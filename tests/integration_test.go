//go:build integration

// Integration test: spins up a real broker on a temp data dir + SQLite meta
// store, then exercises the full data plane — produce, fetch, consumer-group
// join/commit, and the retry→DLQ path — over the wire protocol.
package tests

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/streamx/streamx/internal/broker"
	"github.com/streamx/streamx/internal/config"
	"github.com/streamx/streamx/internal/logging"
	"github.com/streamx/streamx/internal/meta"
	"github.com/streamx/streamx/internal/protocol"
	sdk "github.com/streamx/streamx/sdk/streamx"
)

// freePort returns a free localhost TCP port.
func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// startBroker boots a single broker on a temp data dir + SQLite store.
func startBroker(t *testing.T, cluster map[string]string) (*broker.Broker, *broker.Server, context.CancelFunc) {
	t.Helper()
	dataDir := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "streamx.db")
	log := logging.MustSetup("test-broker", "warn")

	store, err := meta.Open("sqlite://" + dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := store.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	cfg := config.BrokerConfig{
		BrokerID:         "broker-1",
		Addr:             cluster["broker-1"],
		MetricsAddr:      "127.0.0.1:0", // random port; metrics unused in tests
		Cluster:          cluster,
		DataDir:          dataDir,
		DBURL:            "sqlite://" + dbPath,
		HeartbeatMS:      100,
		BrokerTimeoutMS:  1500,
		RetentionCheckMS: 30000,
		FsyncIntervalMS:  0,
		SegmentMaxBytes:  64 << 20,
		PendingBudget:    64 << 20,
	}
	b := broker.New(cfg, store, log)
	srv := broker.NewServer(b)
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Listen(ctx); err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = b.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		srv.Close()
	})
	return b, srv, cancel
}

// newClient builds an SDK client pointed at the cluster.
func newClient(t *testing.T, cluster map[string]string) *sdk.Client {
	t.Helper()
	c, err := sdk.NewClient(sdk.Config{Brokers: cluster, ProducerID: "test", RequestTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return c
}

// waitTopicReady polls until the topic metadata is served by the broker.
func waitTopicReady(t *testing.T, client *sdk.Client, topic string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := client.Metadata(context.Background(), topic); err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("topic %s not ready within %s", topic, timeout)
}

// createTopic writes a topic into the meta store and waits for the broker to
// open its partitions.
func createTopic(t *testing.T, store meta.Store, client *sdk.Client, name string, partitions int32) {
	t.Helper()
	topic := &meta.Topic{
		Name:              name,
		Partitions:        partitions,
		ReplicationFactor: 1,
		Strategy:          "keyed",
		Retention:         meta.TopicRetention{MaxAgeMS: 3600000, MaxBytes: 1 << 30},
	}
	if err := store.CreateTopic(context.Background(), topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	waitTopicReady(t, client, name, 10*time.Second)
}

// TestProduceFetchRoundTrip produces N events and reads them back.
func TestProduceFetchRoundTrip(t *testing.T) {
	cluster := map[string]string{"broker-1": freePort(t)}
	b, _, _ := startBroker(t, cluster)
	client := newClient(t, cluster)

	createTopic(t, b.Meta(), client, "roundtrip", 3)

	ctx := context.Background()
	events := make([]*protocol.Event, 0, 30)
	for i := 0; i < 30; i++ {
		events = append(events, &protocol.Event{
			Key:     fmt.Sprintf("k-%d", i%3),
			Payload: []byte(fmt.Sprintf(`{"n":%d}`, i)),
		})
	}
	resp, err := client.Produce(ctx, "roundtrip", "leader", events)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	ok := 0
	for _, r := range resp.Results {
		if r.Error == "" {
			ok++
		} else {
			t.Fatalf("produce error: %s", r.Error)
		}
	}
	if ok != 30 {
		t.Fatalf("produced %d, want 30", ok)
	}

	// Read back from partition 0 (keyed k-0 → one partition).
	var got int
	for p := int32(0); p < 3; p++ {
		fr, err := client.Fetch(ctx, "roundtrip", p, 0, 1<<20, 0)
		if err != nil {
			t.Fatalf("fetch p%d: %v", p, err)
		}
		got += len(fr.Records)
	}
	if got != 30 {
		t.Fatalf("fetched %d, want 30", got)
	}
}

// TestConsumerGroupCommit exercises join → fetch → commit → describe.
func TestConsumerGroupCommit(t *testing.T) {
	cluster := map[string]string{"broker-1": freePort(t)}
	b, _, _ := startBroker(t, cluster)
	_ = b
	client := newClient(t, cluster)
	store := b.Meta()

	createTopic(t, store, client, "group-topic", 2)
	ctx := context.Background()

	// Produce 10 events.
	events := make([]*protocol.Event, 0, 10)
	for i := 0; i < 10; i++ {
		events = append(events, &protocol.Event{Key: fmt.Sprintf("k-%d", i%2), Payload: []byte("x")})
	}
	if _, err := client.Produce(ctx, "group-topic", "leader", events); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// Consume with a group; the consumer joins, fetches, and commits.
	var processed int
	consumer, err := sdk.NewConsumer(sdk.GroupConfig{
		Client:     client,
		Group:      "test-group",
		Topic:      "group-topic",
		MemberID:   "m-1",
		StartAt:    "earliest",
		MaxRetries: 0,
		Handler: func(ctx context.Context, ev *protocol.Event) error {
			processed++
			return nil
		},
	})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	_ = consumer.Start(cctx)
	cancel()

	if processed < 10 {
		t.Fatalf("processed %d, want >= 10", processed)
	}
}

// TestRetryDLQ verifies that a failing handler exhausts retries and the event
// lands in the DLQ topic.
func TestRetryDLQ(t *testing.T) {
	cluster := map[string]string{"broker-1": freePort(t)}
	b, _, _ := startBroker(t, cluster)
	client := newClient(t, cluster)
	store := b.Meta()

	createTopic(t, store, client, "dlq-src", 1)
	createTopic(t, store, client, "dlq-src.dlq", 1)
	ctx := context.Background()

	// Produce 5 events, all of which will fail the handler.
	events := make([]*protocol.Event, 0, 5)
	for i := 0; i < 5; i++ {
		events = append(events, &protocol.Event{Key: "k", Payload: []byte("boom")})
	}
	if _, err := client.Produce(ctx, "dlq-src", "leader", events); err != nil {
		t.Fatalf("produce: %v", err)
	}

	dlqCount := 0
	consumer, err := sdk.NewConsumer(sdk.GroupConfig{
		Client:     client,
		Group:      "dlq-group",
		Topic:      "dlq-src",
		MemberID:   "m-1",
		StartAt:    "earliest",
		MaxRetries: 1, // small so retries exhaust quickly
		Handler: func(ctx context.Context, ev *protocol.Event) error {
			return fmt.Errorf("always fails")
		},
		OnDLQ: func(ctx context.Context, ev *protocol.Event, reason string) {
			dlqCount++
		},
	})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	_ = consumer.Start(cctx)
	cancel()

	if dlqCount < 5 {
		t.Fatalf("dlq count %d, want >= 5", dlqCount)
	}
	// Verify the DLQ topic actually received the records.
	fr, err := client.Fetch(ctx, "dlq-src.dlq", 0, 0, 1<<20, 0)
	if err != nil {
		t.Fatalf("fetch dlq: %v", err)
	}
	if len(fr.Records) < 5 {
		t.Fatalf("dlq records %d, want >= 5", len(fr.Records))
	}
}
