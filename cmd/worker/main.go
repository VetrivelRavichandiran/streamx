// Command streamx-worker runs a consumer-group member with a pluggable
// handler that exercises the retry + DLQ path.
//
// Configuration is environment-driven (CONTRACTS.md §12):
//
//	STREAMX_WORKER_GROUP       consumer group name (default orders-group)
//	STREAMX_WORKER_TOPIC       topic to consume (default orders)
//	STREAMX_WORKER_HANDLER     noop | log | fail:<p> | slow:<ms> (default log)
//	STREAMX_WORKER_MAX_RETRIES retries before DLQ (default 3)
//	STREAMX_WORKER_CONCURRENCY handler concurrency (default 8)
//	STREAMX_WORKER_MEMBER_ID   stable member id (default random)
//	STREAMX_WORKER_START_AT    earliest | latest (default earliest)
//	STREAMX_CLUSTER            id=host:port,... (default local 3-broker cluster)
//	STREAMX_API_URL            control-plane URL for DLQ reporting (optional)
//	STREAMX_API_TOKEN          bearer token for DLQ reporting (optional)
//	STREAMX_WORKER_METRICS_ADDR optional metrics listen addr (e.g. :9110)
package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/streamx/streamx/internal/config"
	"github.com/streamx/streamx/internal/logging"
	"github.com/streamx/streamx/internal/metrics"
	"github.com/streamx/streamx/internal/protocol"
	sdk "github.com/streamx/streamx/sdk/streamx"
)

func main() {
	group := config.Get("STREAMX_WORKER_GROUP", "orders-group")
	topic := config.Get("STREAMX_WORKER_TOPIC", "orders")
	handler := config.Get("STREAMX_WORKER_HANDLER", "log")
	maxRetries := config.GetInt("STREAMX_WORKER_MAX_RETRIES", 3)
	concurrency := config.GetInt("STREAMX_WORKER_CONCURRENCY", 8)
	memberID := config.Get("STREAMX_WORKER_MEMBER_ID", "")
	startAt := config.Get("STREAMX_WORKER_START_AT", "earliest")
	apiURL := config.Get("STREAMX_API_URL", "")
	apiToken := config.Get("STREAMX_API_TOKEN", "")
	metricsAddr := config.Get("STREAMX_WORKER_METRICS_ADDR", "")
	logLevel := config.Get("STREAMX_LOG_LEVEL", "info")

	log := logging.MustSetup("worker", logLevel)
	log.Info("worker: starting", "group", group, "topic", topic, "handler", handler, "member", memberID)

	cluster := config.ParseCluster(config.Get("STREAMX_CLUSTER",
		"broker-1=127.0.0.1:9092,broker-2=127.0.0.1:9093,broker-3=127.0.0.1:9094"))

	client, err := sdk.NewClient(sdk.Config{
		Brokers:        cluster,
		ProducerID:     "worker-" + group,
		APIURL:         apiURL,
		APIToken:       apiToken,
		RequestTimeout: 10 * time.Second,
	})
	if err != nil {
		log.Error("worker: new client", "err", err)
		os.Exit(1)
	}

	// Optional consumer metrics endpoint.
	if metricsAddr != "" {
		cm := metrics.NewConsumer()
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.HandlerFor(cm.Registry, promhttp.HandlerOpts{}))
			srv := &http.Server{Addr: metricsAddr, Handler: mux}
			_ = srv.ListenAndServe()
		}()
	}

	h := buildHandler(handler, log)
	consumer, err := sdk.NewConsumer(sdk.GroupConfig{
		Client:      client,
		Group:       group,
		Topic:       topic,
		MemberID:    memberID,
		StartAt:     startAt,
		MaxRetries:  maxRetries,
		Concurrency: concurrency,
		Handler:     h,
		OnDLQ: func(ctx context.Context, ev *protocol.Event, reason string) {
			log.Warn("worker: event to DLQ", "offset", ev.Offset, "partition", ev.Partition, "reason", reason)
		},
	})
	if err != nil {
		log.Error("worker: new consumer", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("worker: consuming", "member", consumer.MemberID())
	if err := consumer.Start(ctx); err != nil && ctx.Err() == nil {
		log.Error("worker: consume", "err", err)
	}
	log.Info("worker: stopped")
}

// buildHandler returns the handler for the given spec:
//
//	noop        — drop the event (no-op)
//	log         — log each event
//	fail:<p>    — fail with probability p (0..1) to exercise retry + DLQ
//	slow:<ms>   — sleep ms (builds lag) then succeed
func buildHandler(spec string, log *slog.Logger) func(ctx context.Context, ev *protocol.Event) error {
	switch {
	case spec == "noop" || spec == "":
		return func(ctx context.Context, ev *protocol.Event) error { return nil }
	case spec == "log":
		return func(ctx context.Context, ev *protocol.Event) error {
			log.Info("worker: event", "topic", ev.Topic, "partition", ev.Partition, "offset", ev.Offset, "key", ev.Key)
			return nil
		}
	case strings.HasPrefix(spec, "fail:"):
		p, err := strconv.ParseFloat(strings.TrimPrefix(spec, "fail:"), 64)
		if err != nil || p < 0 || p > 1 {
			log.Warn("worker: bad fail spec, defaulting to 0.1", "spec", spec)
			p = 0.1
		}
		return func(ctx context.Context, ev *protocol.Event) error {
			if rand.Float64() < p {
				return fmt.Errorf("simulated handler failure (p=%.2f)", p)
			}
			return nil
		}
	case strings.HasPrefix(spec, "slow:"):
		ms, err := strconv.Atoi(strings.TrimPrefix(spec, "slow:"))
		if err != nil || ms < 0 {
			ms = 100
		}
		return func(ctx context.Context, ev *protocol.Event) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(ms) * time.Millisecond):
			}
			return nil
		}
	default:
		log.Warn("worker: unknown handler, using log", "spec", spec)
		return func(ctx context.Context, ev *protocol.Event) error {
			log.Info("worker: event", "partition", ev.Partition, "offset", ev.Offset)
			return nil
		}
	}
}
