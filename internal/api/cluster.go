package api

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/streamx/streamx/internal/meta"
)

// ---------------------------------------------------------------------------
// Cluster & broker views
// ---------------------------------------------------------------------------

// brokerSample is the last-scraped counter values for a broker (for windowed
// rate computation).
type brokerSample struct {
	ts       time.Time
	counters map[string]float64
}

var (
	rateMu      sync.Mutex
	rateSamples = map[string]*brokerSample{}
)

// handleCluster returns brokers, topology, and a summary.
func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	brokers := s.brokerViews(ctx)
	topics, _ := s.store.ListTopics(ctx)
	groups, _ := s.store.ListGroups(ctx)
	var partitions int32
	for _, t := range topics {
		partitions += t.Partitions
	}
	up := 0
	for _, b := range brokers {
		if st, _ := b["status"].(string); st == "up" {
			up++
		}
	}
	summary := map[string]any{
		"brokers_total":    len(brokers),
		"brokers_up":       up,
		"topics":           len(topics),
		"partitions":       partitions,
		"consumer_groups":  len(groups),
		"active_consumers": s.countActiveConsumers(ctx, groups),
		"events_per_sec":   0.0,
		"bytes_per_sec":    0.0,
	}
	if snap, err := s.metricsSummary(ctx); err == nil {
		summary["events_per_sec"] = snap["events_per_sec"]
		summary["bytes_per_sec"] = snap["bytes_per_sec"]
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"brokers": brokers,
		"summary": summary,
	})
}

// handleBrokers lists brokers with stats.
func (s *Server) handleBrokers(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, s.brokerViews(r.Context()))
}

// handleBroker returns a single broker + leadership + replication.
func (s *Server) handleBroker(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ctx := r.Context()
	for _, b := range s.brokerViews(ctx) {
		if b["id"] == id {
			// Leadership + replication from the brokers' cluster state.
			b["leadership"] = s.leadershipFor(ctx, id)
			b["replication"] = s.replicationFor(ctx, id)
			s.writeJSON(w, http.StatusOK, b)
			return
		}
	}
	s.writeError(w, http.StatusNotFound, "not_found", "broker not found: "+id)
}

// brokerViews builds the per-broker view (status from meta + stats from
// scraping the broker's metrics endpoint).
func (s *Server) brokerViews(ctx context.Context) []map[string]any {
	rows, _ := s.store.ListBrokers(ctx)
	// Leadership counts per broker (from metadata across all topics).
	led, rep := s.leadershipCounts(ctx)
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		view := map[string]any{
			"id":                 row.ID,
			"addr":               row.Addr,
			"status":             row.Status,
			"live":               row.Status == "up",
			"last_seen":          row.LastSeen,
			"cpu_percent":        0.0,
			"memory_bytes":       0.0,
			"disk_used_bytes":    0.0,
			"connections":        0.0,
			"events_per_sec":     0.0,
			"bytes_per_sec":      0.0,
			"partitions_led":     led[row.ID],
			"partitions_replica": rep[row.ID],
		}
		text, err := s.scrapeBroker(ctx, row.ID)
		if err == nil {
			counters := parsePrometheus(text)
			// Windowed rates.
			ep, bp := s.windowRates(row.ID, counters)
			view["events_per_sec"] = ep
			view["bytes_per_sec"] = bp
			view["memory_bytes"] = counters["streamx_broker_memory_bytes"]
			view["disk_used_bytes"] = counters["streamx_broker_disk_used_bytes"]
			view["connections"] = counters["streamx_broker_connections"]
			// CPU percent from process CPU seconds delta.
			view["cpu_percent"] = s.cpuPercent(row.ID, counters)
		}
		out = append(out, view)
	}
	return out
}

// leadershipCounts returns per-broker leader and follower counts.
func (s *Server) leadershipCounts(ctx context.Context) (map[string]int, map[string]int) {
	led := map[string]int{}
	rep := map[string]int{}
	topics, _ := s.store.ListTopics(ctx)
	for _, t := range topics {
		md, err := s.client.Metadata(ctx, t.Name)
		if err != nil {
			continue
		}
		for _, pm := range md.Partitions {
			if pm.Leader != "" {
				led[pm.Leader]++
			}
			for _, r := range pm.Replicas {
				if r != pm.Leader {
					rep[r]++
				}
			}
		}
	}
	return led, rep
}

// leadershipFor returns the partitions a broker leads (from its cluster state
// via the metadata op on each topic).
func (s *Server) leadershipFor(ctx context.Context, brokerID string) []map[string]any {
	out := []map[string]any{}
	topics, _ := s.store.ListTopics(ctx)
	for _, t := range topics {
		md, err := s.client.Metadata(ctx, t.Name)
		if err != nil {
			continue
		}
		for _, pm := range md.Partitions {
			if pm.Leader == brokerID {
				out = append(out, map[string]any{
					"topic":     t.Name,
					"partition": pm.Partition,
					"role":      "leader",
				})
			}
		}
	}
	return out
}

// replicationFor returns the partitions a broker replicates (non-leader).
func (s *Server) replicationFor(ctx context.Context, brokerID string) []map[string]any {
	out := []map[string]any{}
	topics, _ := s.store.ListTopics(ctx)
	for _, t := range topics {
		md, err := s.client.Metadata(ctx, t.Name)
		if err != nil {
			continue
		}
		for _, pm := range md.Partitions {
			if pm.Leader == brokerID {
				continue
			}
			for _, rep := range pm.Replicas {
				if rep == brokerID {
					out = append(out, map[string]any{
						"topic":     t.Name,
						"partition": pm.Partition,
						"role":      "follower",
						"leader":    pm.Leader,
					})
					break
				}
			}
		}
	}
	return out
}

// countActiveConsumers counts consumers with status "up" across groups.
func (s *Server) countActiveConsumers(ctx context.Context, groups []*meta.Group) int {
	n := 0
	for _, g := range groups {
		consumers, err := s.store.ListConsumers(ctx, g.ID)
		if err != nil {
			continue
		}
		for _, c := range consumers {
			if c.Status == "up" {
				n++
			}
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// Metrics summary (windowed rates from broker scrapes)
// ---------------------------------------------------------------------------

// metricsSummary computes windowed cluster rates by scraping all brokers and
// comparing against the previous sample.
func (s *Server) metricsSummary(ctx context.Context) (map[string]any, error) {
	var totalEvents, totalBytes float64
	var windowMS float64
	anySample := false
	for id := range s.cfg.BrokerCluster {
		text, err := s.scrapeBroker(ctx, id)
		if err != nil {
			continue
		}
		counters := parsePrometheus(text)
		ep, bp := s.windowRates(id, counters)
		totalEvents += ep
		totalBytes += bp
		if prev, ok := rateSamples[id]; ok {
			windowMS = time.Since(prev.ts).Seconds() * 1000
			anySample = true
		}
	}
	if !anySample {
		windowMS = 2000
	}
	return map[string]any{
		"events_per_sec": totalEvents,
		"bytes_per_sec":  totalBytes,
		"errors_per_sec": 0.0,
		"window_ms":      windowMS,
		"ts":             time.Now().UnixMilli(),
	}, nil
}

// windowRates computes per-broker events/sec and bytes/sec from the delta
// between this sample and the previous, and stores this sample.
func (s *Server) windowRates(brokerID string, counters map[string]float64) (float64, float64) {
	rateMu.Lock()
	defer rateMu.Unlock()
	prev, ok := rateSamples[brokerID]
	now := time.Now()
	var ep, bp float64
	if ok && now.Sub(prev.ts) > time.Millisecond {
		dt := now.Sub(prev.ts).Seconds()
		ev := counters["streamx_events_produced_total"] - prev.counters["streamx_events_produced_total"]
		by := counters["streamx_bytes_produced_total"] - prev.counters["streamx_bytes_produced_total"]
		if ev >= 0 {
			ep = ev / dt
		}
		if by >= 0 {
			bp = by / dt
		}
	}
	rateSamples[brokerID] = &brokerSample{ts: now, counters: counters}
	return ep, bp
}

// cpuPercent computes CPU percent from the process CPU-seconds delta.
func (s *Server) cpuPercent(brokerID string, counters map[string]float64) float64 {
	rateMu.Lock()
	prev, ok := rateSamples[brokerID]
	now := time.Now()
	rateMu.Unlock()
	if !ok || now.Sub(prev.ts) <= time.Millisecond {
		return 0
	}
	dt := now.Sub(prev.ts).Seconds()
	dcpu := counters["streamx_broker_cpu_seconds_total"] - prev.counters["streamx_broker_cpu_seconds_total"]
	if dcpu < 0 {
		return 0
	}
	return dcpu / dt * 100
}

// parsePrometheus parses a Prometheus text exposition into a map of
// metric-name (summed over label sets) -> value.
func parsePrometheus(text string) map[string]float64 {
	out := map[string]float64{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// Split name{labels} value
		name := line
		valStr := ""
		if i := strings.LastIndex(line, " "); i >= 0 {
			valStr = line[i+1:]
		}
		if j := strings.Index(line, "{"); j >= 0 {
			name = line[:j]
		} else {
			name = strings.TrimSpace(line[:strings.LastIndex(line, " ")])
		}
		v, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			continue
		}
		out[name] += v
	}
	return out
}

// handleMetricsSummary returns the windowed cluster rates.
func (s *Server) handleMetricsSummary(w http.ResponseWriter, r *http.Request) {
	snap, err := s.metricsSummary(r.Context())
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	s.writeJSON(w, http.StatusOK, snap)
}
