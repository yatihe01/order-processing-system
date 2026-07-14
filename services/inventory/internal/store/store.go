// Package store persists inventory and reservations to MySQL.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
)

type Item struct {
	ProductID string
	Quantity  int32
}

type Store struct {
	db *sql.DB
}

func New(db *sql.DB) *Store {
	return &Store{db: db}
}

// Reserve attempts to atomically reserve every item for orderID, all-or-nothing.
// ok=false with a reason means insufficient stock (a business outcome, not an error);
// err is reserved for unexpected failures (DB down, etc).
//
// Decision #2 (concurrency control): SELECT ... FOR UPDATE row lock, decided in
// services/inventory/migrations/0001_init.up.sql's companion design discussion (see
// CLAUDE.md Decision Log). Implementation notes for whoever writes this:
//
//   - Run the whole reservation in one transaction (all-or-nothing across items).
//   - Lock rows in a deterministic order (e.g. sort items by ProductID first) before
//     checking/decrementing, so two orders reserving the same two SKUs in opposite
//     order can't deadlock each other.
//   - For each item: SELECT quantity FROM inventory WHERE product_id=? FOR UPDATE;
//     if quantity < requested, roll back and return ok=false with a reason; otherwise
//     UPDATE inventory SET quantity=quantity-? WHERE product_id=? and INSERT INTO
//     reservations (order_id, product_id, quantity).
//   - Commit only if every item cleared.
func (s *Store) Reserve(ctx context.Context, orderID string, items []Item) (ok bool, reason string, err error) {
	// (1) Deadlock avoidance: always lock rows in the SAME order every time.
	// Sorting by ProductID guarantees two concurrent orders touching the same
	// SKUs grab them in identical sequence, so they can't freeze waiting on
	// each other. (More on why this works below.)
	sort.Slice(items, func(i, j int) bool {
		return items[i].ProductID < items[j].ProductID
	})

	// (2) Open the all-or-nothing bubble.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, "", fmt.Errorf("begin tx: %w", err)
	}
	// The safety net: if we return early for ANY reason without committing,
	// this undoes everything. If we DID commit, Rollback is a harmless no-op.
	defer tx.Rollback()

	// (3) Walk each item: check stock, then decrement + record the reservation.
	for _, item := range items {
		var available int32
		// FOR UPDATE is the magic words: it locks THIS row until we commit or
		// roll back, so no other order can read-then-write it underneath us.
		err := tx.QueryRowContext(ctx, `SELECT quantity FROM inventory WHERE product_id=? FOR UPDATE`, item.ProductID).Scan(&available)

		if errors.Is(err, sql.ErrNoRows) {
			// No such product — bail cleanly (defer rolls back).
			return false, "unknown product: " + item.ProductID, nil
		}
		if err != nil {
			return false, "", fmt.Errorf("select for update: %w", err)
		}

		if available < item.Quantity {
			// Not enough stock. Returning here triggers the deferred Rollback,
			// which un-does any items we already reserved earlier in this loop.
			return false, "insufficient stock for " + item.ProductID, nil
		}

		// Enough stock: subtract it.
		_, err = tx.ExecContext(ctx,
			`UPDATE inventory SET quantity = quantity - ? WHERE product_id = ?`,
			item.Quantity, item.ProductID,
		)
		if err != nil {
			return false, "", fmt.Errorf("decrement: %w", err)
		}

		// Record that this order holds that stock (so Release can give it back).
		_, err = tx.ExecContext(ctx,
			`INSERT INTO reservations (order_id, product_id, quantity) VALUES (?, ?, ?)`,
			orderID, item.ProductID, item.Quantity,
		)
		if err != nil {
			return false, "", fmt.Errorf("insert reservation: %w", err)
		}
	}

	// (4) All items cleared: commit the bubble and return success.
	if err := tx.Commit(); err != nil {
		return false, "", fmt.Errorf("commit: %w", err)
	}
	return true, "", nil
}

// Release restores every item previously reserved for orderID (looked up from the
// reservations table, since Release's caller only has the order id) and deletes the
// reservation rows. Same row-lock discipline as Reserve.
func (s *Store) Release(ctx context.Context, orderID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	// Find what this order reserved. ORDER BY product_id keeps our locking
	// order consistent with Reserve, for the same deadlock-avoidance reason.
	rows, err := tx.QueryContext(ctx,
		`SELECT product_id, quantity FROM reservations WHERE order_id = ? ORDER BY product_id`,
		orderID,
	)
	if err != nil {
		return fmt.Errorf("read reservations: %w", err)
	}

	// Read ALL rows into memory and close the cursor BEFORE running more
	// statements on this tx. (Go gotcha: you can't keep a query open and fire
	// new statements on the same connection at once.)
	type resv struct {
		productID string
		quantity  int32
	}
	var held []resv
	for rows.Next() {
		var r resv
		if err := rows.Scan(&r.productID, &r.quantity); err != nil {
			rows.Close()
			return fmt.Errorf("scan reservation: %w", err)
		}
		held = append(held, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate reservations: %w", err)
	}

	// Give each quantity back.
	for _, r := range held {
		_, err := tx.ExecContext(ctx,
			`UPDATE inventory SET quantity = quantity + ? WHERE product_id = ?`,
			r.quantity, r.productID,
		)
		if err != nil {
			return fmt.Errorf("restore stock: %w", err)
		}
	}

	// Remove the reservation records now that the stock is back.
	if _, err = tx.ExecContext(ctx,
		`DELETE FROM reservations WHERE order_id = ?`,
		orderID,
	); err != nil {
		return fmt.Errorf("delete reservations: %w", err)
	}

	return tx.Commit()
}
