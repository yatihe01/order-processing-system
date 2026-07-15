//go:build integration

// Package store tests. This file lives next to store.go in the same package,
// so it can reach unexported fields like Store.db (a "white-box" test).
// Requires a live MySQL (make up && make migrate-inventory) -- gated behind
// the integration tag so `make test` stays infra-free, matching Order's
// store_integration_test.go.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	// Blank import: runs the driver's init() so it registers itself with
	// database/sql under the name "mysql". We never call it directly, hence _.
	_ "github.com/go-sql-driver/mysql"
)

// ---- Helpers -------------------------------------------------------------

// setupTestStore connects to the Dockerized MySQL and wipes the tables so each
// run starts from a known, empty state.
//
// RECONCILE WITH YOUR SETUP:
//   - dsn: match the user, password, and database name from your compose file.
//     (You saw inventory_db in SHOW DATABASES; adjust user/pass if not root/root.)
//   - table names: match your migration (0001_init.up.sql).
func setupTestStore(t *testing.T) *Store {
	t.Helper()

	dsn := "root:root@tcp(localhost:3306)/inventory_db?parseTime=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("cannot reach MySQL (is `docker compose up` running?): %v", err)
	}

	// clean() wipes both tables in child-before-parent order (foreign key safe).
	clean := func() {
		for _, tbl := range []string{"reservations", "inventory"} {
			if _, err := db.Exec("DELETE FROM " + tbl); err != nil {
				t.Fatalf("delete from %s: %v", tbl, err)
			}
		}
	}

	clean()          // clean state BEFORE the test runs
	t.Cleanup(clean) // and clean up AFTER the test finishes, pass or fail

	return New(db)
}

// seedProduct inserts one product with a known starting quantity.
// RECONCILE: column names (product_id, quantity) with your schema.
func seedProduct(t *testing.T, s *Store, productID string, qty int32) {
	t.Helper()
	_, err := s.db.Exec(
		"INSERT INTO inventory (product_id, quantity) VALUES (?, ?)",
		productID, qty,
	)
	if err != nil {
		t.Fatalf("seed product: %v", err)
	}
}

// currentStock reads the current quantity straight from the DB — the source of
// truth for the "did we land at exactly 0?" assertion.
func currentStock(t *testing.T, s *Store, productID string) int32 {
	t.Helper()
	var qty int32
	err := s.db.QueryRow(
		"SELECT quantity FROM inventory WHERE product_id = ?",
		productID,
	).Scan(&qty)
	if err != nil {
		t.Fatalf("read stock: %v", err)
	}
	return qty
}

// ---- The test ------------------------------------------------------------

// TestReserve_Concurrent_NoOversell fires far more concurrent reservations than
// there is stock, and proves the row lock prevents overselling: exactly
// startingStock reservations succeed, and the final quantity is exactly 0.
//
// Run it:
//
//	go test ./services/inventory/internal/store/ -run TestReserve_Concurrent -v
//
// And under the race detector (should be clean):
//
//	go test ./services/inventory/internal/store/ -run TestReserve_Concurrent -race -v
func TestReserve_Concurrent_NoOversell(t *testing.T) {
	s := setupTestStore(t)

	const product = "SKU-1"
	const startingStock = 10
	const attempts = 100 // far more than stock, so most MUST fail

	seedProduct(t, s, product, startingStock)

	var wg sync.WaitGroup // waits for all goroutines to finish
	var mu sync.Mutex     // protects successCount from concurrent writes
	successCount := 0

	for i := 0; i < attempts; i++ {
		wg.Add(1) // count this goroutine BEFORE launching it
		go func(n int) {
			defer wg.Done() // always decrement, even on early return

			// Unique order id per goroutine so reservations don't collide.
			orderID := fmt.Sprintf("order-%d", n)
			items := []Item{{ProductID: product, Quantity: 1}}

			ok, _, err := s.Reserve(context.Background(), orderID, items)
			if err != nil {
				// A real DB error is a test failure, not a business "no".
				t.Errorf("unexpected error: %v", err)
				return
			}
			if ok {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait() // block here until all 100 goroutines are done

	// Assertion 1: exactly the available stock was reserved — no oversell.
	if successCount != startingStock {
		t.Errorf("got %d successful reservations, want %d (oversold or undersold)",
			successCount, startingStock)
	}

	// Assertion 2: the database agrees — stock landed at exactly 0.
	if final := currentStock(t, s, product); final != 0 {
		t.Errorf("final stock = %d, want 0", final)
	}
}

// TestReserve_Idempotent proves that calling Reserve twice with the SAME order id
// (as happens when Kafka redelivers a message) reserves stock only ONCE.
// Without the primary-key dedupe in Reserve, the second call would decrement
// again and this test would see stock at 98 instead of 99.
func TestReserve_Idempotent(t *testing.T) {
	s := setupTestStore(t)

	const product = "team-badge"
	const startingStock = 100
	seedProduct(t, s, product, startingStock)

	orderID := "order-dup"
	items := []Item{{ProductID: product, Quantity: 1}}

	// First delivery: a normal, new reservation.
	ok, reason, err := s.Reserve(context.Background(), orderID, items)
	if err != nil {
		t.Fatalf("first Reserve errored: %v", err)
	}
	if !ok {
		t.Fatalf("first Reserve should succeed, got ok=false reason=%q", reason)
	}

	// Second delivery: the SAME order id again (a redelivery/retry).
	// It should still report success (the reservation exists and is valid)...
	ok, reason, err = s.Reserve(context.Background(), orderID, items)
	if err != nil {
		t.Fatalf("second Reserve errored: %v", err)
	}
	if !ok {
		t.Fatalf("redelivered Reserve should report success, got ok=false reason=%q", reason)
	}

	// ...but it must NOT have decremented stock a second time.
	if final := currentStock(t, s, product); final != startingStock-1 {
		t.Errorf("stock = %d, want %d (redelivery double-decremented!)",
			final, startingStock-1)
	}
}
