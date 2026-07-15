// Command order runs the Order service: a gRPC server backed by MySQL.
package main

import (
	"context"
	"database/sql"
	"log"
	"net"
	"os/signal"
	"sync"
	"syscall"
	"time"

	inventoryv1 "orderproc/proto/gen/inventory/v1"
	orderv1 "orderproc/proto/gen/order/v1"
	"orderproc/services/order/internal/config"
	"orderproc/services/order/internal/kafka"
	"orderproc/services/order/internal/server"
	"orderproc/services/order/internal/store"

	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	invConn, err := grpc.NewClient(cfg.InventoryAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer invConn.Close()
	invClient := inventoryv1.NewInventoryServiceClient(invConn)

	producer := kafka.NewProducer(cfg.KafkaBrokers)
	defer producer.Close()

	st := store.New(db)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	orderv1.RegisterOrderServiceServer(grpcServer, server.New(st, invClient, producer))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Printf("order service listening on %s", cfg.GRPCAddr)
		errCh <- grpcServer.Serve(lis)
	}()

	resultConsumer := kafka.NewResultConsumer(st, invClient)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := resultConsumer.RunPaymentCompleted(ctx, cfg.KafkaBrokers); err != nil {
			log.Printf("payment-completed consumer stopped: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := resultConsumer.RunPaymentFailed(ctx, cfg.KafkaBrokers); err != nil {
			log.Printf("payment-failed consumer stopped: %v", err)
		}
	}()

	select {
	case err := <-errCh:
		stop()
		wg.Wait()
		return err
	case <-ctx.Done():
		log.Print("shutting down order service")
		grpcServer.GracefulStop()
		wg.Wait()
		return nil
	}
}
