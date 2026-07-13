// Package store persists inventory and reservations to MySQL.
package store

import (
	"context"
	"database/sql"
	"errors"
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
	return false, "", errors.New("not implemented")
}

// Release restores every item previously reserved for orderID (looked up from the
// reservations table, since Release's caller only has the order id) and deletes the
// reservation rows. Same row-lock discipline as Reserve.
func (s *Store) Release(ctx context.Context, orderID string) error {
	return errors.New("not implemented")
}
