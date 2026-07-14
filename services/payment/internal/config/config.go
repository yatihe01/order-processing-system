// Package config loads Payment service configuration from the environment once at startup.
package config

import (
	"os"
	"strings"
)

type Config struct {
	MySQLDSN     string
	KafkaBrokers []string
}

func Load() Config {
	return Config{
		MySQLDSN:     getEnv("PAYMENT_MYSQL_DSN", "root:root@tcp(localhost:3306)/payment_db?parseTime=true"),
		KafkaBrokers: strings.Split(getEnv("PAYMENT_KAFKA_BROKERS", "localhost:9092"), ","),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
