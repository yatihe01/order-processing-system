// Package store persists payment records to MySQL.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

var ErrNotFound = errors.New("payment not found")

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

// GetPayment reads back a previously recorded charge outcome for orderID.
// Returns ErrNotFound if no payment has been recorded for it yet.
func (s *Store) GetPayment(ctx context.Context, orderID string) (paymentID, status string, err error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT payment_id, status FROM payments WHERE order_id = ?`, orderID)
	if err := row.Scan(&paymentID, &status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", ErrNotFound
		}
		return "", "", fmt.Errorf("store: get payment: %w", err)
	}
	return paymentID, status, nil
}
