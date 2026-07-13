// Command inventory runs the Inventory service: a gRPC server backed by MySQL.
package main

import (
	"context"
	"database/sql"
	"log"
	"net"
	"os/signal"
	"syscall"
	"time"

	inventoryv1 "orderproc/proto/gen/inventory/v1"
	"orderproc/services/inventory/internal/config"
	"orderproc/services/inventory/internal/server"
	"orderproc/services/inventory/internal/store"

	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg := config.Load()

	db, err := sql.Open("mysql", cfg.MySQLDSN)
	if err != nil {
		return err
	}
	defer db.Close()

	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		return err
	}

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	inventoryv1.RegisterInventoryServiceServer(grpcServer, server.New(store.New(db)))

	errCh := make(chan error, 1)
	go func() {
		log.Printf("inventory service listening on %s", cfg.GRPCAddr)
		errCh <- grpcServer.Serve(lis)
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		log.Print("shutting down inventory service")
		grpcServer.GracefulStop()
		return nil
	}
}
