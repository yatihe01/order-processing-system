// Package config loads Inventory service configuration from the environment once at startup.
package config

import "os"

type Config struct {
	GRPCAddr string
	MySQLDSN string
}

func Load() Config {
	return Config{
		GRPCAddr: getEnv("INVENTORY_GRPC_ADDR", ":50052"),
		MySQLDSN: getEnv("INVENTORY_MYSQL_DSN", "root:root@tcp(localhost:3306)/inventory_db?parseTime=true"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
