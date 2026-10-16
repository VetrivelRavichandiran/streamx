// Package config centralizes environment-based configuration.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func Get(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func GetInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return def
}

func GetInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return n
		}
	}
	return def
}

func GetBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return def
}

func GetDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return def
}

// ParseCluster parses "id1=host1:port1,id2=host2:port2" into an ordered map.
func ParseCluster(s string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) == 2 {
			out[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}
	return out
}

// SortedCluster returns broker ids in sorted order (deterministic).
func SortedCluster(m map[string]string) []string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sortStrings(ids)
	return ids
}

// AssignReplicas returns the deterministic replica set for a partition.
// sortedCluster must be sorted. RF replicas are chosen by stepping through
// the cluster starting at partition mod len, wrapping around, so replicas are
// spread across brokers. Both the API (at topic creation) and the brokers
// (at partition discovery) use this, so they always agree.
func AssignReplicas(sortedCluster []string, partition, rf int32) []string {
	n := int32(len(sortedCluster))
	if n == 0 {
		return nil
	}
	if rf > n {
		rf = n
	}
	seen := map[string]bool{}
	out := make([]string, 0, rf)
	for i := int32(0); i < rf; i++ {
		id := sortedCluster[(partition+i)%n]
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// BrokerConfig holds broker process settings.
type BrokerConfig struct {
	BrokerID         string
	Addr             string
	MetricsAddr      string
	Cluster          map[string]string
	DataDir          string
	DBURL            string // shared meta store (same as the API)
	FsyncIntervalMS  int64  // 0 = every batch, >0 = batched, -1 = never
	SegmentMaxBytes  int64
	RetentionCheckMS int64
	HeartbeatMS      int64
	BrokerTimeoutMS  int64
	ProduceRateLimit int // events/sec per producer
	TopicRateLimit   int // events/sec per topic
	MaxBatchBytes    int
	PendingBudget    int64 // per-partition pending append budget (bytes)
	LogLevel         string
}

func LoadBroker() BrokerConfig {
	return BrokerConfig{
		BrokerID:         Get("STREAMX_BROKER_ID", "broker-1"),
		Addr:             Get("STREAMX_BROKER_ADDR", ":9092"),
		MetricsAddr:      Get("STREAMX_METRICS_ADDR", ":9102"),
		Cluster:          ParseCluster(Get("STREAMX_CLUSTER", "broker-1=127.0.0.1:9092")),
		DataDir:          Get("STREAMX_DATA_DIR", "./data/broker-1"),
		DBURL:            Get("STREAMX_DB_URL", "sqlite://./data/streamx.db"),
		FsyncIntervalMS:  GetInt64("STREAMX_FSYNC_INTERVAL_MS", 0),
		SegmentMaxBytes:  GetInt64("STREAMX_SEGMENT_MAX_BYTES", 64<<20),
		RetentionCheckMS: GetInt64("STREAMX_RETENTION_CHECK_INTERVAL_MS", 30000),
		HeartbeatMS:      GetInt64("STREAMX_HEARTBEAT_INTERVAL_MS", 500),
		BrokerTimeoutMS:  GetInt64("STREAMX_BROKER_TIMEOUT_MS", 1500),
		ProduceRateLimit: GetInt("STREAMX_PRODUCE_RATE_LIMIT", 50000),
		TopicRateLimit:   GetInt("STREAMX_TOPIC_RATE_LIMIT", 200000),
		MaxBatchBytes:    GetInt("STREAMX_MAX_BATCH_BYTES", 16<<20),
		PendingBudget:    GetInt64("STREAMX_PENDING_APPEND_BUDGET", 64<<20),
		LogLevel:         Get("STREAMX_LOG_LEVEL", "info"),
	}
}

// APIConfig holds control-plane settings.
type APIConfig struct {
	Addr          string
	DBURL         string
	AdminUser     string
	AdminPassword string
	OperatorUser  string
	OperatorPass  string
	RateLimitRPS  int
	RedisURL      string
	BrokerCluster map[string]string
	LogLevel      string
}

func LoadAPI() APIConfig {
	return APIConfig{
		Addr:          Get("STREAMX_API_ADDR", ":8080"),
		DBURL:         Get("STREAMX_DB_URL", "sqlite://./data/streamx.db"),
		AdminUser:     Get("STREAMX_ADMIN_USER", "admin"),
		AdminPassword: Get("STREAMX_ADMIN_PASSWORD", "admin123"),
		OperatorUser:  Get("STREAMX_OPERATOR_USER", "operator"),
		OperatorPass:  Get("STREAMX_OPERATOR_PASSWORD", "operator123"),
		RateLimitRPS:  GetInt("STREAMX_RATE_LIMIT_RPS", 500),
		RedisURL:      Get("STREAMX_REDIS_URL", ""),
		BrokerCluster: ParseCluster(Get("STREAMX_BROKER_CLUSTER", "broker-1=127.0.0.1:9092,broker-2=127.0.0.1:9093,broker-3=127.0.0.1:9094")),
		LogLevel:      Get("STREAMX_LOG_LEVEL", "info"),
	}
}

// MustParseDBURL validates the DSN prefix.
func MustParseDBURL(dsn string) (kind, path string, err error) {
	switch {
	case strings.HasPrefix(dsn, "postgres://"), strings.HasPrefix(dsn, "postgresql://"):
		return "postgres", dsn, nil
	case strings.HasPrefix(dsn, "sqlite://"):
		return "sqlite", strings.TrimPrefix(dsn, "sqlite://"), nil
	default:
		return "", "", fmt.Errorf("unsupported DB_URL %q (want postgres:// or sqlite://)", dsn)
	}
}
