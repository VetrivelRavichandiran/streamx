// Package bench implements the STREAMX benchmark runner: it creates a
// dedicated topic, produces N events with P producers, consumes with C
// consumers, and reports throughput + latency percentiles. It is used by both
// the standalone streamx-bench binary and the `streamx bench` CLI command.
package bench

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/streamx/streamx/internal/protocol"
	sdk "github.com/streamx/streamx/sdk/streamx"
)

// Config configures a Run.
type Config struct {
	Brokers      map[string]string
	APIURL       string
	APIToken     string
	Scenario     string
	Events       int
	PayloadBytes int
	Producers    int
	Consumers    int
	Partitions   int
	Concurrency  int
	Topic        string // explicit topic (skip create/cleanup); default auto
}

// Result is the benchmark outcome.
type Result struct {
	ThroughputEventsPerSec float64
	ThroughputBytesPerSec  float64
	ProducerLatencyMS      Latiles
	ConsumerLatencyMS      Latiles
	ErrorRate              float64
	Errors                 int
	Produced               int
	Consumed               int
	DurationMS             int64
	MemoryBytes            uint64
	Topic                  string
}

// Latiles holds p50/p95/p99 in milliseconds.
type Latiles struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

// Run executes the benchmark and returns the result. It creates a dedicated
// topic (unless cfg.Topic is set), produces, drains, and cleans up.
func Run(ctx context.Context, cfg Config) (*Result, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("bench: no brokers configured")
	}
	if cfg.Events <= 0 {
		cfg.Events = 10000
	}
	if cfg.PayloadBytes <= 0 {
		cfg.PayloadBytes = 512
	}
	if cfg.Producers <= 0 {
		cfg.Producers = 2
	}
	if cfg.Consumers <= 0 {
		cfg.Consumers = 2
	}
	if cfg.Partitions <= 0 {
		cfg.Partitions = 3
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 8
	}
	if cfg.Scenario == "" {
		cfg.Scenario = "throughput"
	}

	client, err := sdk.NewClient(sdk.Config{
		Brokers:        cfg.Brokers,
		ProducerID:     "bench-runner",
		APIURL:         cfg.APIURL,
		APIToken:       cfg.APIToken,
		RequestTimeout: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	defer client.Close()

	// Determine the topic. If not given, create one via the API (needs token)
	// or use a timestamped name and hope it exists.
	topic := cfg.Topic
	cleanup := false
	if topic == "" {
		topic = "bench-" + strconv.FormatInt(time.Now().UnixNano(), 36)
		if cfg.APIToken != "" {
			if err := ensureTopic(ctx, cfg.APIURL, cfg.APIToken, topic, cfg.Partitions, cfg.Brokers); err != nil {
				return nil, fmt.Errorf("bench: create topic: %w", err)
			}
			cleanup = true
		}
	}
	if cleanup {
		defer deleteTopic(ctx, cfg.APIURL, cfg.APIToken, topic)
	}

	// Wait for the topic to be visible to the brokers.
	if err := waitTopicReady(ctx, client, topic, 10*time.Second); err != nil {
		return nil, fmt.Errorf("bench: topic not ready: %w", err)
	}

	group := topic + "-bench-group"

	// Consumer latency samples.
	var consMu sync.Mutex
	var consLat []float64
	var consCount int
	consCtx, consCancel := context.WithCancel(ctx)
	defer consCancel()
	var consWG sync.WaitGroup
	for c := 0; c < cfg.Consumers; c++ {
		consWG.Add(1)
		go func(id int) {
			defer consWG.Done()
			runConsumer(consCtx, cfg, topic, group, id, cfg.Concurrency, func(d time.Duration) {
				consMu.Lock()
				consLat = append(consLat, float64(d.Milliseconds()))
				consCount++
				consMu.Unlock()
			})
		}(c)
	}

	// Producer phase.
	var prodMu sync.Mutex
	var prodLat []float64
	var prodErrs int
	var produced int
	start := time.Now()
	payload := make([]byte, cfg.PayloadBytes)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	var prodWG sync.WaitGroup
	for p := 0; p < cfg.Producers; p++ {
		prodWG.Add(1)
		go func(id int) {
			defer prodWG.Done()
			pclient, err := sdk.NewClient(sdk.Config{
				Brokers:        cfg.Brokers,
				ProducerID:     "bench-p-" + strconv.Itoa(id),
				RequestTimeout: 10 * time.Second,
			})
			if err != nil {
				return
			}
			defer pclient.Close()
			batch := make([]*protocol.Event, 0, 100)
			for n := 0; n < cfg.Events/cfg.Producers; n++ {
				key := fmt.Sprintf("k-%d", n%cfg.Partitions)
				batch = append(batch, &protocol.Event{Key: key, Payload: payload})
				if len(batch) >= 100 {
					produceBatch(ctx, pclient, topic, batch, &prodLat, &prodErrs, &prodMu, &produced)
					batch = batch[:0]
				}
			}
			if len(batch) > 0 {
				produceBatch(ctx, pclient, topic, batch, &prodLat, &prodErrs, &prodMu, &produced)
			}
		}(p)
	}
	prodWG.Wait()
	duration := time.Since(start)

	// Let consumers drain (bounded), then stop.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		consMu.Lock()
		n := consCount
		consMu.Unlock()
		if n >= produced {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	consCancel()
	consWG.Wait()

	consMu.Lock()
	consLatCopy := append([]float64(nil), consLat...)
	consumedN := consCount
	consMu.Unlock()
	prodMu.Lock()
	prodLatCopy := append([]float64(nil), prodLat...)
	prodErrsN := prodErrs
	producedN := produced
	prodMu.Unlock()

	res := &Result{
		ThroughputEventsPerSec: float64(producedN) / duration.Seconds(),
		ThroughputBytesPerSec:  float64(producedN*cfg.PayloadBytes) / duration.Seconds(),
		ProducerLatencyMS:      percentiles(prodLatCopy),
		ConsumerLatencyMS:      percentiles(consLatCopy),
		ErrorRate:              0,
		Errors:                 prodErrsN,
		Produced:               producedN,
		Consumed:               consumedN,
		DurationMS:             duration.Milliseconds(),
		Topic:                  topic,
	}
	if producedN > 0 {
		res.ErrorRate = float64(prodErrsN) / float64(producedN)
	}
	return res, nil
}

func produceBatch(ctx context.Context, client *sdk.Client, topic string, batch []*protocol.Event,
	lat *[]float64, errs *int, mu *sync.Mutex, produced *int) {
	start := time.Now()
	resp, err := client.Produce(ctx, topic, "leader", batch)
	d := time.Since(start)
	mu.Lock()
	defer mu.Unlock()
	if err != nil || (resp != nil && len(resp.Results) != len(batch)) {
		*errs += len(batch)
		return
	}
	for _, r := range resp.Results {
		if r.Error != "" {
			*errs++
		}
	}
	*produced += len(batch)
	*lat = append(*lat, float64(d.Milliseconds()))
}

func runConsumer(ctx context.Context, cfg Config, topic, group string, id, concurrency int, onLatency func(time.Duration)) {
	client, err := sdk.NewClient(sdk.Config{
		Brokers:        cfg.Brokers,
		ProducerID:     "bench-c-" + strconv.Itoa(id),
		RequestTimeout: 10 * time.Second,
	})
	if err != nil {
		return
	}
	defer client.Close()
	consumer, err := sdk.NewConsumer(sdk.GroupConfig{
		Client:      client,
		Group:       group,
		Topic:       topic,
		MemberID:    "bench-c-" + strconv.Itoa(id),
		StartAt:     "earliest",
		Concurrency: concurrency,
		Handler: func(ctx context.Context, ev *protocol.Event) error {
			start := time.Now()
			onLatency(time.Since(start))
			return nil
		},
	})
	if err != nil {
		return
	}
	_ = consumer.Start(ctx)
}

func waitTopicReady(ctx context.Context, client *sdk.Client, topic string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := client.Metadata(ctx, topic); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("topic %s not ready within %s", topic, timeout)
}

// percentiles computes p50/p95/p99 from a slice of values (ms).
func percentiles(vals []float64) Latiles {
	if len(vals) == 0 {
		return Latiles{}
	}
	sorted := append([]float64(nil), vals...)
	sort.Float64s(sorted)
	pick := func(p float64) float64 {
		idx := int(p * float64(len(sorted)-1))
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		return sorted[idx]
	}
	return Latiles{P50: pick(0.50), P95: pick(0.95), P99: pick(0.99)}
}
