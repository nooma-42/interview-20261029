package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"rockets/internal/httpapi"
	"rockets/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("stopped", "error", err)
		os.Exit(1)
	}
}
func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	args := os.Args[1:]
	mode := "serve"
	if len(args) > 0 {
		mode = args[0]
	}
	// Container health check uses the same static binary, with no shell/curl needed.
	if mode == "healthcheck" {
		_, port, err := net.SplitHostPort(env("HTTP_ADDR", ":8088"))
		if err != nil {
			return err
		}
		client := http.Client{Timeout: 3 * time.Second}
		resp, err := client.Get("http://" + net.JoinHostPort("127.0.0.1", port) + "/health/ready")
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return fmt.Errorf("readiness returned %d", resp.StatusCode)
		}
		return nil
	}
	if (mode != "serve" && mode != "migrate") || len(args) > 1 {
		return errors.New("usage: rockets [serve|migrate|healthcheck]")
	}
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return errors.New("DATABASE_URL is required")
	}
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	db, err := store.Open(startup, url)
	if err != nil {
		return err
	}
	defer db.Pool.Close()
	if mode == "migrate" {
		return db.Migrate(startup)
	}
	if err = db.Ready(startup); err != nil {
		return fmt.Errorf("run rockets migrate first: %w", err)
	}
	api := httpapi.New(db, logger)
	replayCtx, stopReplay := context.WithCancel(ctx)
	replayDone := make(chan struct{})
	go func() {
		defer close(replayDone)
		db.RunReplay(replayCtx, logger, api.EventsApplied)
	}()
	defer func() { stopReplay(); <-replayDone }()
	server := &http.Server{Addr: env("HTTP_ADDR", ":8088"), Handler: api, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	result := make(chan error, 1)
	go func() { logger.Info("listening", "address", server.Addr); result <- server.ListenAndServe() }()
	select {
	case err := <-result:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		logger.Info("draining HTTP requests")
		shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			return err
		}
		return nil
	}
}
func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
