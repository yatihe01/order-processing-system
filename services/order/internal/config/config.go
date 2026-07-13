// Package config loads Order service configuration from the environment once at startup.
package config

import "os"

type Config struct {
	GRPCAddr      string
	MySQLDSN      string
	InventoryAddr string
}

func Load() Config {
	return Config{
		GRPCAddr:      getEnv("ORDER_GRPC_ADDR", ":50051"),
		MySQLDSN:      getEnv("ORDER_MYSQL_DSN", "root:root@tcp(localhost:3306)/order_db?parseTime=true"),
		InventoryAddr: getEnv("ORDER_INVENTORY_ADDR", "localhost:50052"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
