//go:build integration

package store

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/oklog/ulid/v2"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("ORDER_MYSQL_DSN")
	if dsn == "" {
		dsn = "root:root@tcp(localhost:3306)/order_db?parseTime=true"
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatalf("ping db: %v", err)
	}
	return db
}

func TestCreateAndGetOrder(t *testing.T) {
	db := openTestDB(t)
	s := New(db)
	ctx := context.Background()

	items := []Item{
		{ProductID: "sku-1", Quantity: 2},
		{ProductID: "sku-2", Quantity: 1},
	}

	orderID := ulid.Make().String()
	created, err := s.CreateOrder(ctx, orderID, "user-123", items)
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	t.Cleanup(func() {
		db.ExecContext(context.Background(), "DELETE FROM orders WHERE order_id = ?", created.OrderID)
	})

	if created.Status != "PENDING" {
		t.Errorf("created.Status = %q, want PENDING", created.Status)
	}
	if len(created.OrderID) != 26 {
		t.Errorf("created.OrderID = %q, want a 26-char ULID", created.OrderID)
	}

	got, err := s.GetOrder(ctx, created.OrderID)
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if got.UserID != "user-123" {
		t.Errorf("got.UserID = %q, want user-123", got.UserID)
	}
	if got.Status != "PENDING" {
		t.Errorf("got.Status = %q, want PENDING", got.Status)
	}
	if len(got.Items) != 2 {
		t.Fatalf("got.Items = %v, want 2 items", got.Items)
	}
	if got.Items[0].ProductID != "sku-1" || got.Items[0].Quantity != 2 {
		t.Errorf("got.Items[0] = %+v, want {sku-1 2}", got.Items[0])
	}
	if got.Items[1].ProductID != "sku-2" || got.Items[1].Quantity != 1 {
		t.Errorf("got.Items[1] = %+v, want {sku-2 1}", got.Items[1])
	}
}

func TestGetOrder_NotFound(t *testing.T) {
	db := openTestDB(t)
	s := New(db)

	_, err := s.GetOrder(context.Background(), "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if err != ErrNotFound {
		t.Errorf("GetOrder err = %v, want ErrNotFound", err)
	}
}
