// Package store persists payment records to MySQL.
package store

import (
	"context"
	"database/sql"
	"fmt"
)

type Store struct {
	db *sql.DB
}

func New(db *sql.DB) *Store {
	return &Store{db: db}
}

// RecordPayment inserts the outcome of a charge attempt for orderID.
func (s *Store) RecordPayment(ctx context.Context, paymentID, orderID, status string) error {
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO payments (order_id, payment_id, status) VALUES (?, ?, ?)`,
		orderID, paymentID, status,
	); err != nil {
		return fmt.Errorf("store: record payment: %w", err)
	}
	return nil
}
