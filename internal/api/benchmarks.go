package api

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/streamx/streamx/internal/meta"
	"github.com/streamx/streamx/internal/protocol"
	"github.com/streamx/streamx/sdk/streamx"
)

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

type benchmarkReq struct {
	Scenario     string `json:"scenario"`
	Events       int    `json:"events"`
	PayloadBytes int    `json:"payload_bytes"`
	Producers    int    `json:"producers"`
	Consumers    int    `json:"consumers"`
	Partitions   int    `json:"partitions"`
	Concurrency  int    `json:"concurrency"`
}

type benchmarkResult struct {
	ThroughputEventsPerSec float64 `json:"throughput_events_per_sec"`
	ThroughputBytesPerSec  float64 `json:"throughput_bytes_per_sec"`
	ProducerLatencyMS      latiles `json:"producer_latency_ms"`
	ConsumerLatencyMS      latiles `json:"consumer_latency_ms"`
	ErrorRate              float64 `json:"error_rate"`
	Errors                 int     `json:"errors"`
	Produced               int     `json:"produced"`
	Consumed               int     `json:"consumed"`
	DurationMS             int64   `json:"duration_ms"`
	CPUPercent             float64 `json:"cpu_percent"`
	MemoryBytes            uint64  `json:"memory_bytes"`
	DiskBytesPerSec        float64 `json:"disk_bytes_per_sec"`
}

// latiles holds p50/p95/p99 in milliseconds.
type latiles struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

func (s *Server) handleCreateBenchmark(w http.ResponseWriter, r *http.Request) {
	var req benchmarkReq
	if err := s.decodeBody(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "validation", "bad request: "+err.Error())
		return
	}
	applyDefaults(&req)
	runID := "run-" + randomHex(4)
	b := &meta.Benchmark{
		ID:           runID,
		Scenario:     req.Scenario,
		Events:       req.Events,
		PayloadBytes: req.PayloadBytes,
		Producers:    req.Producers,
		Consumers:    req.Consumers,
		Partitions:   req.Partitions,
		Concurrency:  req.Concurrency,
		Status:       "running",
		StartedAt:    time.Now(),
	}
	if err := s.store.CreateBenchmark(r.Context(), b); err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	// Run in the background; the client polls for the result.
	go s.runBenchmark(context.Background(), runID, req)
	s.writeJSON(w, http.StatusAccepted, map[string]string{"run_id": runID})
}

func applyDefaults(req *benchmarkReq) {
	if req.Scenario == "" {
		req.Scenario = "throughput"
	}
	if req.Events <= 0 {
		req.Events = 10000
	}
	if req.PayloadBytes <= 0 {
		req.PayloadBytes = 512
	}
	if req.Producers <= 0 {
		req.Producers = 2
	}
	if req.Consumers <= 0 {
		req.Consumers = 2
	}
	if req.Partitions <= 0 {
		req.Partitions = 3
	}
	if req.Concurrency <= 0 {
		req.Concurrency = 8
	}
}

// runBenchmark executes a benchmark: create a topic, produce with N producers,
// consume with M consumers, measure, and persist the result.
func (s *Server) runBenchmark(ctx context.Context, runID string, req benchmarkReq) {
	topic := "bench-" + runID
	group := "bench-" + runID + "-group"
	b := &meta.Benchmark{ID: runID, Status: "running"}
	finish := func(status string, result *benchmarkResult) {
		now := time.Now()
		b.Status = status
		b.FinishedAt = &now
		_ = s.store.UpdateBenchmark(ctx, b)
		if result != nil {
			s.persistResult(ctx, runID, result)
		}
	}

	// Create the benchmark topic.
	t := &meta.Topic{
		Name:              topic,
		Partitions:        int32(req.Partitions),
		ReplicationFactor: s.replicationFactor(),
		Strategy:          "keyed",
		Retention:         meta.TopicRetention{MaxAgeMS: 60_000, MaxBytes: 1 << 30},
	}
	if err := s.store.CreateTopic(ctx, t); err != nil {
		finish("failed", nil)
		return
	}
	// Wait for the brokers to open the partitions.
	if !s.waitTopicReady(ctx, topic, time.Second*5) {
		finish("failed", nil)
		return
	}
	defer func() {
		// Clean up the benchmark topic.
		for id, addr := range s.cfg.BrokerCluster {
			s.callBrokerAdmin(context.Background(), id, addr, "/admin/delete-topic?topic="+topic)
		}
		_ = s.store.DeleteTopic(context.Background(), topic)
	}()

	// Producer latency samples.
	var prodMu sync.Mutex
	var prodLat []float64
	var prodErrs int
	// Consumer state.
	var consMu sync.Mutex
	var consLat []float64

	// Start consumers (stopped after the produce phase + drain window).
	consCtx, consCancel := context.WithCancel(ctx)
	var consWG sync.WaitGroup
	for c := 0; c < req.Consumers; c++ {
		consWG.Add(1)
		go func(id int) {
			defer consWG.Done()
			s.runBenchmarkConsumer(consCtx, topic, group, id, req.Concurrency, func(d time.Duration) {
				consMu.Lock()
				consLat = append(consLat, float64(d.Milliseconds()))
				consMu.Unlock()
			})
		}(c)
	}

	// Produce.
	start := time.Now()
	var prodWG sync.WaitGroup
	payload := make([]byte, req.PayloadBytes)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	for p := 0; p < req.Producers; p++ {
		prodWG.Add(1)
		go func(id int) {
			defer prodWG.Done()
			client, err := streamx.NewClient(streamx.Config{
				Brokers:        s.cfg.BrokerCluster,
				ProducerID:     "bench-p-" + strconv.Itoa(id),
				RequestTimeout: 10 * time.Second,
			})
			if err != nil {
				return
			}
			batch := make([]*protocol.Event, 0, 100)
			for n := 0; n < req.Events/req.Producers; n++ {
				key := fmt.Sprintf("k-%d", n%req.Partitions)
				batch = append(batch, &protocol.Event{Key: key, Payload: payload})
				if len(batch) >= 100 {
					s.produceBatch(ctx, client, topic, batch, &prodLat, &prodErrs, &prodMu)
					batch = batch[:0]
				}
			}
			if len(batch) > 0 {
				s.produceBatch(ctx, client, topic, batch, &prodLat, &prodErrs, &prodMu)
			}
		}(p)
	}
	prodWG.Wait()
	duration := time.Since(start)

	// Let consumers drain the produced events (bounded), then stop them.
	produced := req.Events
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		consMu.Lock()
		n := len(consLat)
		consMu.Unlock()
		if n >= produced {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	consCancel()
	consWG.Wait()

	consumedN := 0
	consMu.Lock()
	consumedN = len(consLat)
	consLatCopy := append([]float64(nil), consLat...)
	consMu.Unlock()
	prodMu.Lock()
	prodLatCopy := append([]float64(nil), prodLat...)
	prodErrsN := prodErrs
	prodMu.Unlock()

	result := &benchmarkResult{
		ThroughputEventsPerSec: float64(produced) / duration.Seconds(),
		ThroughputBytesPerSec:  float64(produced*req.PayloadBytes) / duration.Seconds(),
		ProducerLatencyMS:      percentiles(prodLatCopy),
		ConsumerLatencyMS:      percentiles(consLatCopy),
		ErrorRate:              float64(prodErrsN) / float64(produced),
		Errors:                 prodErrsN,
		Produced:               produced,
		Consumed:               consumedN,
		DurationMS:             duration.Milliseconds(),
		MemoryBytes:            memAlloc(),
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	result.MemoryBytes = m.Alloc
	finish("done", result)
}

// produceBatch produces a batch and records latency/errors.
func (s *Server) produceBatch(ctx context.Context, client *streamx.Client, topic string, batch []*protocol.Event, lat *[]float64, errs *int, mu *sync.Mutex) {
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
	*lat = append(*lat, float64(d.Milliseconds()))
}

// runBenchmarkConsumer consumes from the group until ctx is cancelled,
// recording per-event handler latency.
func (s *Server) runBenchmarkConsumer(ctx context.Context, topic, group string, id int, concurrency int, onLatency func(time.Duration)) {
	client, err := streamx.NewClient(streamx.Config{
		Brokers:        s.cfg.BrokerCluster,
		ProducerID:     "bench-c-" + strconv.Itoa(id),
		RequestTimeout: 10 * time.Second,
	})
	if err != nil {
		return
	}
	consumer, err := streamx.NewConsumer(streamx.GroupConfig{
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

// replicationFactor returns the RF for benchmark topics (min 1, up to cluster
// size, capped at 3).
func (s *Server) replicationFactor() int32 {
	n := int32(len(s.cfg.BrokerCluster))
	if n > 3 {
		n = 3
	}
	if n < 1 {
		n = 1
	}
	return n
}

// waitTopicReady polls until the topic's metadata is available from the
// brokers.
func (s *Server) waitTopicReady(ctx context.Context, topic string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := s.client.Metadata(ctx, topic); err == nil {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(200 * time.Millisecond):
		}
	}
	return false
}

// persistResult stores the benchmark result metrics.
func (s *Server) persistResult(ctx context.Context, runID string, r *benchmarkResult) {
	now := time.Now()
	rows := []*meta.BenchmarkResult{
		{ID: randomHex(4), RunID: runID, Metric: "throughput_events_per_sec", Value: r.ThroughputEventsPerSec, Unit: "events/s", CreatedAt: now},
		{ID: randomHex(4), RunID: runID, Metric: "throughput_bytes_per_sec", Value: r.ThroughputBytesPerSec, Unit: "bytes/s", CreatedAt: now},
		{ID: randomHex(4), RunID: runID, Metric: "producer_latency_p50_ms", Value: r.ProducerLatencyMS.P50, Unit: "ms", CreatedAt: now},
		{ID: randomHex(4), RunID: runID, Metric: "producer_latency_p95_ms", Value: r.ProducerLatencyMS.P95, Unit: "ms", CreatedAt: now},
		{ID: randomHex(4), RunID: runID, Metric: "producer_latency_p99_ms", Value: r.ProducerLatencyMS.P99, Unit: "ms", CreatedAt: now},
		{ID: randomHex(4), RunID: runID, Metric: "consumer_latency_p50_ms", Value: r.ConsumerLatencyMS.P50, Unit: "ms", CreatedAt: now},
		{ID: randomHex(4), RunID: runID, Metric: "consumer_latency_p95_ms", Value: r.ConsumerLatencyMS.P95, Unit: "ms", CreatedAt: now},
		{ID: randomHex(4), RunID: runID, Metric: "consumer_latency_p99_ms", Value: r.ConsumerLatencyMS.P99, Unit: "ms", CreatedAt: now},
		{ID: randomHex(4), RunID: runID, Metric: "error_rate", Value: r.ErrorRate, Unit: "ratio", CreatedAt: now},
		{ID: randomHex(4), RunID: runID, Metric: "produced", Value: float64(r.Produced), Unit: "events", CreatedAt: now},
		{ID: randomHex(4), RunID: runID, Metric: "consumed", Value: float64(r.Consumed), Unit: "events", CreatedAt: now},
		{ID: randomHex(4), RunID: runID, Metric: "duration_ms", Value: float64(r.DurationMS), Unit: "ms", CreatedAt: now},
		{ID: randomHex(4), RunID: runID, Metric: "memory_bytes", Value: float64(r.MemoryBytes), Unit: "bytes", CreatedAt: now},
	}
	for _, row := range rows {
		_ = s.store.InsertBenchmarkResult(ctx, row)
	}
}

// registerBenchmarkReq is a pre-computed benchmark result submitted by an
// external runner (e.g. the streamx CLI bench) for persistence + comparison.
type registerBenchmarkReq struct {
	Scenario     string         `json:"scenario"`
	Events       int            `json:"events"`
	PayloadBytes int            `json:"payload_bytes"`
	Producers    int            `json:"producers"`
	Consumers    int            `json:"consumers"`
	Partitions   int            `json:"partitions"`
	Concurrency  int            `json:"concurrency"`
	Result       map[string]any `json:"result"`
}

// handleRegisterBenchmark stores a pre-computed benchmark run + its metrics.
func (s *Server) handleRegisterBenchmark(w http.ResponseWriter, r *http.Request) {
	var req registerBenchmarkReq
	if err := s.decodeBody(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "validation", "bad request: "+err.Error())
		return
	}
	d := benchmarkReq{
		Scenario: req.Scenario, Events: req.Events, PayloadBytes: req.PayloadBytes,
		Producers: req.Producers, Consumers: req.Consumers, Partitions: req.Partitions,
		Concurrency: req.Concurrency,
	}
	applyDefaults(&d)
	runID := "run-" + randomHex(4)
	now := time.Now()
	b := &meta.Benchmark{
		ID: runID, Scenario: d.Scenario, Events: d.Events, PayloadBytes: d.PayloadBytes,
		Producers: d.Producers, Consumers: d.Consumers, Partitions: d.Partitions,
		Concurrency: d.Concurrency, Status: "done", StartedAt: now, FinishedAt: &now,
	}
	if err := s.store.CreateBenchmark(r.Context(), b); err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	// Persist the result metrics.
	metricKeys := []string{
		"throughput_events_per_sec", "throughput_bytes_per_sec",
		"producer_latency_p50_ms", "producer_latency_p95_ms", "producer_latency_p99_ms",
		"consumer_latency_p50_ms", "consumer_latency_p95_ms", "consumer_latency_p99_ms",
		"error_rate", "produced", "consumed", "duration_ms", "memory_bytes",
	}
	for _, k := range metricKeys {
		if v, ok := req.Result[k]; ok {
			if f, ok := v.(float64); ok {
				_ = s.store.InsertBenchmarkResult(r.Context(), &meta.BenchmarkResult{
					ID: randomHex(4), RunID: runID, Metric: k, Value: f, Unit: "auto", CreatedAt: now,
				})
			}
		}
	}
	s.audit(r, "benchmark.register", runID, "events="+strconv.Itoa(req.Events))
	s.writeJSON(w, http.StatusCreated, map[string]string{"run_id": runID})
}

func (s *Server) handleListBenchmarks(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		limit, _ = strconv.Atoi(v)
	}
	runs, err := s.store.ListBenchmarks(r.Context(), limit)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	out := make([]map[string]any, 0, len(runs))
	for _, run := range runs {
		out = append(out, benchmarkView(run, nil))
	}
	s.writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleGetBenchmark(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	run, err := s.store.GetBenchmark(r.Context(), id)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "not_found", "benchmark not found: "+id)
		return
	}
	results, _ := s.store.ListBenchmarkResults(r.Context(), id)
	s.writeJSON(w, http.StatusOK, benchmarkView(run, results))
}

func (s *Server) handleCompareBenchmarks(w http.ResponseWriter, r *http.Request) {
	ids := r.URL.Query().Get("ids")
	parts := strings.Split(ids, ",")
	out := []map[string]any{}
	for _, id := range parts {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		run, err := s.store.GetBenchmark(r.Context(), id)
		if err != nil {
			continue
		}
		results, _ := s.store.ListBenchmarkResults(r.Context(), id)
		out = append(out, benchmarkView(run, results))
	}
	s.writeJSON(w, http.StatusOK, out)
}

// benchmarkView shapes a benchmark run + its results.
func benchmarkView(b *meta.Benchmark, results []*meta.BenchmarkResult) map[string]any {
	view := map[string]any{
		"run_id":     b.ID,
		"status":     b.Status,
		"scenario":   b.Scenario,
		"started_at": b.StartedAt,
		"config": map[string]any{
			"scenario":      b.Scenario,
			"events":        b.Events,
			"payload_bytes": b.PayloadBytes,
			"producers":     b.Producers,
			"consumers":     b.Consumers,
			"partitions":    b.Partitions,
			"concurrency":   b.Concurrency,
		},
	}
	if b.FinishedAt != nil {
		view["finished_at"] = *b.FinishedAt
	}
	if results != nil {
		m := map[string]float64{}
		for _, r := range results {
			m[r.Metric] = r.Value
		}
		view["result"] = map[string]any{
			"throughput_events_per_sec": m["throughput_events_per_sec"],
			"throughput_bytes_per_sec":  m["throughput_bytes_per_sec"],
			"producer_latency_ms": map[string]float64{
				"p50": m["producer_latency_p50_ms"], "p95": m["producer_latency_p95_ms"], "p99": m["producer_latency_p99_ms"],
			},
			"consumer_latency_ms": map[string]float64{
				"p50": m["consumer_latency_p50_ms"], "p95": m["consumer_latency_p95_ms"], "p99": m["consumer_latency_p99_ms"],
			},
			"error_rate":   m["error_rate"],
			"produced":     m["produced"],
			"consumed":     m["consumed"],
			"duration_ms":  m["duration_ms"],
			"memory_bytes": m["memory_bytes"],
		}
	}
	return view
}

// percentiles computes p50/p95/p99 from a slice of values (ms).
func percentiles(vals []float64) latiles {
	if len(vals) == 0 {
		return latiles{}
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
	return latiles{P50: pick(0.50), P95: pick(0.95), P99: pick(0.99)}
}

// memAlloc returns the current allocation in bytes.
func memAlloc() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc
}
