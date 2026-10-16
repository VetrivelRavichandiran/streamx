// Command streamx-api runs the STREAMX control plane (REST + SSE).
//
// Configuration is environment-driven (CONTRACTS.md §12):
//
//	STREAMX_API_ADDR          listen addr (default :8080)
//	STREAMX_DB_URL            meta store DSN (postgres:// or sqlite://)
//	STREAMX_ADMIN_USER/PASSWORD, STREAMX_OPERATOR_USER/PASSWORD  bootstrap users
//	STREAMX_RATE_LIMIT_RPS    per-token API rate limit
//	STREAMX_REDIS_URL         optional; empty = in-memory rate limiter
//	STREAMX_BROKER_CLUSTER    id=host:port,... data-plane membership
//
// The API bootstraps the bootstrap users, serves REST + SSE on
// STREAMX_API_ADDR, and talks to the brokers over the data-plane protocol for
// produce/fetch/metadata.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/streamx/streamx/internal/api"
	"github.com/streamx/streamx/internal/config"
	"github.com/streamx/streamx/internal/logging"
	"github.com/streamx/streamx/internal/meta"
)

func main() {
	cfg := config.LoadAPI()
	log := logging.MustSetup("api", cfg.LogLevel)

	store, err := meta.Open(cfg.DBURL)
	if err != nil {
		log.Error("api: open meta store", "err", err)
		os.Exit(1)
	}
	if err := store.Init(context.Background()); err != nil {
		log.Error("api: init meta store", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	srv, err := api.New(cfg, log, store)
	if err != nil {
		log.Error("api: init", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := srv.Run(ctx); err != nil {
		log.Error("api: run", "err", err)
		os.Exit(1)
	}
	log.Info("api: stopped")
}
