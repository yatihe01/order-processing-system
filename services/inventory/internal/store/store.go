// Package store persists inventory and reservations to MySQL.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/go-sql-driver/mysql"
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
	// (0) Aggregate by product_id first. An order can legitimately carry more
	// than one line item for the same SKU (nothing upstream merges duplicate
	// product_ids before calling Reserve). Without this, the second line item
	// for a given product collides with the first on the (order_id, product_id)
	// dedup key below and gets silently treated as "already reserved" -- which
	// understates the real decrement instead of protecting against it.
	items = mergeByProduct(items)

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
		// (A) Lock the inventory row FIRST. This still serializes concurrent
		//     DIFFERENT orders racing for the same product (the Phase 2 guarantee).
		err := tx.QueryRowContext(ctx,
			`SELECT quantity FROM inventory WHERE product_id=? FOR UPDATE`,
			item.ProductID).Scan(&available)
		if errors.Is(err, sql.ErrNoRows) {
			return false, "unknown product: " + item.ProductID, nil
		}
		if err != nil {
			return false, "", fmt.Errorf("select for update: %w", err)
		}

		// (B) Try to record the reservation BEFORE touching stock. The primary key
		//     (order_id, product_id) means this can only succeed ONCE per order+product.
		_, err = tx.ExecContext(ctx,
			`INSERT INTO reservations (order_id, product_id, quantity) VALUES (?, ?, ?)`,
			orderID, item.ProductID, item.Quantity)

		// (C) If the insert hit a duplicate key, this exact reservation already
		//     exists — a redelivered/retried Reserve. Idempotent skip: do NOT
		//     decrement again, just move to the next item.
		// Note: a redelivery is assumed to carry the same quantity as the original;
		// same order_id means same order, so we intentionally keep the first values.
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			continue
		}
		if err != nil {
			// Any other insert error is a real failure.
			return false, "", fmt.Errorf("insert reservation: %w", err)
		}

		// (D) Insert succeeded, so this is a genuinely new reservation. NOW it's
		//     safe to check stock and decrement — this code only runs once per order.
		if available < item.Quantity {
			return false, "insufficient stock for " + item.ProductID, nil
		}
		_, err = tx.ExecContext(ctx,
			`UPDATE inventory SET quantity = quantity - ? WHERE product_id = ?`,
			item.Quantity, item.ProductID)
		if err != nil {
			return false, "", fmt.Errorf("decrement: %w", err)
		}
	}

	// (4) All items cleared: commit the bubble and return success.
	if err := tx.Commit(); err != nil {
		return false, "", fmt.Errorf("commit: %w", err)
	}
	return true, "", nil
}

// mergeByProduct sums quantities for repeated product_ids, preserving first-seen
// order. A caller-supplied []Item{{A,2},{A,3}} becomes []Item{{A,5}}.
func mergeByProduct(items []Item) []Item {
	totals := make(map[string]int32, len(items))
	order := make([]string, 0, len(items))
	for _, item := range items {
		if _, seen := totals[item.ProductID]; !seen {
			order = append(order, item.ProductID)
		}
		totals[item.ProductID] += item.Quantity
	}

	merged := make([]Item, len(order))
	for i, productID := range order {
		merged[i] = Item{ProductID: productID, Quantity: totals[productID]}
	}
	return merged
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
