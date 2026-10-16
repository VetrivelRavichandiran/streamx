// Command streamx-broker runs a single data-plane broker process.
//
// Configuration is entirely environment-driven (CONTRACTS.md §12):
//
//	STREAMX_BROKER_ID         unique broker id (default broker-1)
//	STREAMX_BROKER_ADDR       data-plane listen addr (default :9092)
//	STREAMX_METRICS_ADDR      metrics/health listen addr (default :9102)
//	STREAMX_CLUSTER           id=host:port,... membership (same for all brokers)
//	STREAMX_DATA_DIR          segment log directory (default ./data/broker-1)
//	STREAMX_DB_URL            shared meta store (default sqlite://./data/streamx.db)
//
// The broker reads topic definitions from the shared meta store (the same
// store the API writes to), opens the partitions it is a replica of, and
// serves produce/fetch/group ops over the data-plane TCP protocol.
package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/streamx/streamx/internal/broker"
	"github.com/streamx/streamx/internal/config"
	"github.com/streamx/streamx/internal/logging"
	"github.com/streamx/streamx/internal/meta"
)

func main() {
	cfg := config.LoadBroker()
	log := logging.MustSetup(cfg.BrokerID, cfg.LogLevel)

	// Open the shared meta store (topics, brokers, groups, offsets).
	store, err := meta.Open(cfg.DBURL)
	if err != nil {
		log.Error("broker: open meta store", "err", err)
		os.Exit(1)
	}
	if err := store.Init(context.Background()); err != nil {
		log.Error("broker: init meta store", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	b := broker.New(cfg, store, log)
	srv := broker.NewServer(b)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := srv.Listen(ctx); err != nil {
		log.Error("broker: listen", "err", err)
		os.Exit(1)
	}
	log.Info("broker: data plane listening", "addr", cfg.Addr, "metrics", cfg.MetricsAddr, "data_dir", cfg.DataDir)

	// Start blocks until ctx is cancelled (signal) or an admin stop.
	if err := b.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("broker: start", "err", err)
	}
	srv.Close()
	log.Info("broker: stopped", "id", cfg.BrokerID)
}
