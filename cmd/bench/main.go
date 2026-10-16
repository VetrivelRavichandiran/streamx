// Command streamx-bench runs a load benchmark against a live cluster and
// prints throughput + latency percentiles. Optionally registers the result
// with the control plane for comparison in the dashboard.
//
// Configuration is environment-driven (CONTRACTS.md §12):
//
//	STREAMX_CLUSTER          id=host:port,... (default local 3-broker cluster)
//	STREAMX_BENCH_API        control-plane URL (default http://localhost:8080)
//	STREAMX_BENCH_TOKEN      bearer token (obtain via POST /api/v1/auth/login)
//	STREAMX_BENCH_EVENTS     default 10000
//	STREAMX_BENCH_PAYLOAD    default 512
//	STREAMX_BENCH_PRODUCERS  default 2
//	STREAMX_BENCH_CONSUMERS  default 2
//	STREAMX_BENCH_PARTITIONS default 3
//	STREAMX_BENCH_CONCURRENCY default 8
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/streamx/streamx/internal/bench"
	"github.com/streamx/streamx/internal/config"
	"github.com/streamx/streamx/internal/logging"
)

func main() {
	log := logging.MustSetup("bench", config.Get("STREAMX_LOG_LEVEL", "info"))
	cluster := config.ParseCluster(config.Get("STREAMX_CLUSTER",
		"broker-1=127.0.0.1:9092,broker-2=127.0.0.1:9093,broker-3=127.0.0.1:9094"))
	apiURL := config.Get("STREAMX_BENCH_API", "http://localhost:8080")
	token := config.Get("STREAMX_BENCH_TOKEN", "")

	cfg := bench.Config{
		Brokers:      cluster,
		APIURL:       apiURL,
		APIToken:     token,
		Scenario:     "throughput",
		Events:       config.GetInt("STREAMX_BENCH_EVENTS", 10000),
		PayloadBytes: config.GetInt("STREAMX_BENCH_PAYLOAD", 512),
		Producers:    config.GetInt("STREAMX_BENCH_PRODUCERS", 2),
		Consumers:    config.GetInt("STREAMX_BENCH_CONSUMERS", 2),
		Partitions:   config.GetInt("STREAMX_BENCH_PARTITIONS", 3),
		Concurrency:  config.GetInt("STREAMX_BENCH_CONCURRENCY", 8),
	}

	log.Info("bench: starting", "events", cfg.Events, "payload", cfg.PayloadBytes,
		"producers", cfg.Producers, "consumers", cfg.Consumers, "partitions", cfg.Partitions)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	res, err := bench.Run(ctx, cfg)
	if err != nil {
		log.Error("bench: run", "err", err)
		os.Exit(1)
	}

	printResult(res)

	// Register with the API if we have a token.
	if token != "" {
		if runID, err := register(apiURL, token, cfg, res); err != nil {
			log.Warn("bench: register", "err", err)
		} else {
			log.Info("bench: registered", "run_id", runID)
		}
	}
}

func printResult(r *bench.Result) {
	w := 40
	fmt.Println()
	fmt.Println(strings.Repeat("=", w))
	fmt.Println("  STREAMX benchmark result")
	fmt.Println(strings.Repeat("=", w))
	fmt.Printf("  events:            %d\n", r.Produced)
	fmt.Printf("  consumed:          %d\n", r.Consumed)
	fmt.Printf("  duration:          %d ms\n", r.DurationMS)
	fmt.Printf("  throughput:        %.0f events/s (%.0f bytes/s)\n", r.ThroughputEventsPerSec, r.ThroughputBytesPerSec)
	fmt.Printf("  producer p50/p95/p99: %.2f / %.2f / %.2f ms\n", r.ProducerLatencyMS.P50, r.ProducerLatencyMS.P95, r.ProducerLatencyMS.P99)
	fmt.Printf("  consumer p50/p95/p99: %.2f / %.2f / %.2f ms\n", r.ConsumerLatencyMS.P50, r.ConsumerLatencyMS.P95, r.ConsumerLatencyMS.P99)
	fmt.Printf("  errors:            %d (%.3f%%)\n", r.Errors, r.ErrorRate*100)
	fmt.Println(strings.Repeat("=", w))
}

// register posts the result to the API's benchmark register endpoint.
func register(apiURL, token string, cfg bench.Config, r *bench.Result) (string, error) {
	body, _ := json.Marshal(map[string]any{
		"scenario":      cfg.Scenario,
		"events":        cfg.Events,
		"payload_bytes": cfg.PayloadBytes,
		"producers":     cfg.Producers,
		"consumers":     cfg.Consumers,
		"partitions":    cfg.Partitions,
		"concurrency":   cfg.Concurrency,
		"result": map[string]any{
			"throughput_events_per_sec": r.ThroughputEventsPerSec,
			"throughput_bytes_per_sec":  r.ThroughputBytesPerSec,
			"producer_latency_p50_ms":   r.ProducerLatencyMS.P50,
			"producer_latency_p95_ms":   r.ProducerLatencyMS.P95,
			"producer_latency_p99_ms":   r.ProducerLatencyMS.P99,
			"consumer_latency_p50_ms":   r.ConsumerLatencyMS.P50,
			"consumer_latency_p95_ms":   r.ConsumerLatencyMS.P95,
			"consumer_latency_p99_ms":   r.ConsumerLatencyMS.P99,
			"error_rate":                r.ErrorRate,
			"produced":                  float64(r.Produced),
			"consumed":                  float64(r.Consumed),
			"duration_ms":               float64(r.DurationMS),
		},
	})
	req, _ := http.NewRequest(http.MethodPost, strings.TrimRight(apiURL, "/")+"/api/v1/benchmarks/register", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("register status %d", resp.StatusCode)
	}
	var out struct {
		RunID string `json:"run_id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out.RunID, nil
}
