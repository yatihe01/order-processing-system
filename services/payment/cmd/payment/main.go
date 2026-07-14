// Command payment runs the Payment service: a Kafka consumer backed by MySQL.
// It has no synchronous API -- payment happens entirely off the request path.
package main

import (
	"context"
	"database/sql"
	"log"
	"os/signal"
	"syscall"
	"time"

	"orderproc/services/payment/internal/config"
	"orderproc/services/payment/internal/kafka"
	"orderproc/services/payment/internal/store"

	_ "github.com/go-sql-driver/mysql"
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

	producer := kafka.NewProducer(cfg.KafkaBrokers)
	defer producer.Close()

	consumer := kafka.NewConsumer(store.New(db), producer)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Print("payment service consuming order-created")
	if err := consumer.Run(ctx, cfg.KafkaBrokers); err != nil {
		return err
	}
	log.Print("shutting down payment service")
	return nil
}
