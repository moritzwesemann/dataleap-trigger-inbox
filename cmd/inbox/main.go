package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"triggerinbox/internal/agent"
	"triggerinbox/internal/inbox"
	"triggerinbox/internal/store"
	"triggerinbox/internal/worker"
)

func main() {
	addr := flag.String("addr", "localhost:8787", "HTTP listen address")
	dbURL := flag.String("db", envOr("DATABASE_URL", "postgres://postgres@localhost:5544/triggerinbox?sslmode=disable"), "Postgres URL")
	workers := flag.Int("workers", 50, "runs driven at once by this process")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(*addr, *dbURL, *workers, log); err != nil {
		log.Error("stopped", "err", err)
		os.Exit(1)
	}
}

func run(addr, dbURL string, workers int, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	ingestDB, err := openPool(ctx, dbURL, 60)
	if err != nil {
		return err
	}
	defer ingestDB.Close()
	workDB, err := openPool(ctx, dbURL, int32(workers)+10)
	if err != nil {
		return err
	}
	defer workDB.Close()

	if err := store.New(ingestDB).Migrate(ctx); err != nil {
		return err
	}

	sim := &agent.Sim{
		MinDuration: 2 * time.Second,
		MaxDuration: 10 * time.Second,
		FailRate:    0.05,
		CrashRate:   0.01,
		Effects:     agent.NewEffects(),
	}
	workerPool := worker.New(store.New(workDB), sim, worker.Config{
		Workers:      workers,
		Reservation:  30 * time.Second,
		Heartbeat:    10 * time.Second,
		PollInterval: 200 * time.Millisecond,
	}, log)

	in := inbox.New(store.New(ingestDB), log)
	in.Start(ctx)
	srv := &http.Server{
		Addr:              addr,
		Handler:           in.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	var wg sync.WaitGroup
	wg.Go(func() { workerPool.Run(ctx) })
	wg.Go(func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	})

	log.Info("trigger inbox listening", "addr", addr)
	err = srv.ListenAndServe()
	stop()
	wg.Wait()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func openPool(ctx context.Context, url string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := store.PoolConfig(url, maxConns)
	if err != nil {
		return nil, err
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
