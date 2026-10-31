package testdb

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"triggerinbox/internal/store"
)

const defaultURL = "postgres://postgres@localhost:5544/triggerinbox?sslmode=disable"

func URL() string {
	if u := os.Getenv("DATABASE_URL"); u != "" {
		return u
	}
	return defaultURL
}

func New(t testing.TB) (*store.Store, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	schema := fmt.Sprintf("test_%d", rand.Uint64())

	admin, err := pgxpool.New(ctx, URL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatalf("create schema (is Postgres running? make db-up): %v", err)
	}

	pool := Open(t, schema)
	t.Cleanup(func() {
		pool.Close()
		admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	s := store.New(pool)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s, pool
}

func Open(t testing.TB, schema string) *pgxpool.Pool {
	t.Helper()
	cfg, err := store.PoolConfig(URL(), 40)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return pool
}
