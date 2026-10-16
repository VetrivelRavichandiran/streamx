package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/streamx/streamx/internal/bench"
)

var cliHTTP = &http.Client{Timeout: 30 * time.Second}

// apiGet performs an authenticated GET against the control plane.
func apiGet(path, token string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, strings.TrimRight(apiURL(), "/")+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := cliHTTP.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// apiPost performs an authenticated POST with a JSON body.
func apiPost(path string, body []byte, token string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(apiURL(), "/")+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := cliHTTP.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// apiLogin logs in and returns the bearer token.
func apiLogin(username, password string) (string, error) {
	body, _ := json.Marshal(map[string]string{"username": username, "password": password})
	req, _ := http.NewRequest(http.MethodPost, strings.TrimRight(apiURL(), "/")+"/api/v1/auth/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := cliHTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("login status %d: %s", resp.StatusCode, string(b))
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.Token, nil
}

// registerBenchmark posts a benchmark result to the API register endpoint.
func registerBenchmark(cfg bench.Config, r *bench.Result) (string, error) {
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
	resp, err := apiPost("/api/v1/benchmarks/register", body, cfg.APIToken)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 && resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("register status %d: %s", resp.StatusCode, string(b))
	}
	var out struct {
		RunID string `json:"run_id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out.RunID, nil
}
