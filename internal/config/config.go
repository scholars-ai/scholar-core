package config

import (
	"fmt"
	"os"
)

type Config struct {
	// DatabaseURL 形如 postgres://user:pass@host:5432/db?sslmode=disable
	DatabaseURL string
	// Addr 形如 :8080
	Addr string
}

func Load() (Config, error) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	addr := os.Getenv("CORE_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	return Config{DatabaseURL: dbURL, Addr: addr}, nil
}
