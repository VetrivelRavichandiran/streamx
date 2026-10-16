// Package metrics provides shared Prometheus collectors for STREAMX.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Broker holds the broker-side collectors.
type Broker struct {
	Registry *prometheus.Registry

	EventsProduced  *prometheus.CounterVec
	BytesProduced   *prometheus.CounterVec
	EventsConsumed  *prometheus.CounterVec
	BytesConsumed   *prometheus.CounterVec
	ProduceLatency  *prometheus.HistogramVec
	FetchRequests   *prometheus.CounterVec
	ProduceRequests *prometheus.CounterVec
	PartitionOffset *prometheus.GaugeVec
	HighWatermark   *prometheus.GaugeVec
	LogStartOffset  *prometheus.GaugeVec
	ReplicationLag  *prometheus.GaugeVec
	Connections     *prometheus.GaugeVec
	Up              prometheus.Gauge
	CPUSecs         prometheus.Counter
	MemBytes        prometheus.Gauge
	DiskUsedBytes   prometheus.Gauge
	RateLimited     *prometheus.CounterVec
	Backpressure    *prometheus.CounterVec
}

// NewBroker creates and registers all broker collectors.
func NewBroker() *Broker {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	b := &Broker{Registry: reg}
	b.EventsProduced = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "streamx_events_produced_total", Help: "Events appended to the log.",
	}, []string{"topic", "partition", "broker"})
	b.BytesProduced = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "streamx_bytes_produced_total", Help: "Payload bytes appended.",
	}, []string{"topic"})
	b.EventsConsumed = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "streamx_events_consumed_total", Help: "Events served to consumers.",
	}, []string{"topic", "partition"})
	b.BytesConsumed = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "streamx_bytes_consumed_total", Help: "Payload bytes served.",
	}, []string{"topic"})
	b.ProduceLatency = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name: "streamx_producer_latency_seconds", Help: "Produce end-to-end latency.",
		Buckets: []float64{.0001, .0005, .001, .005, .01, .05, .1, .5, 1},
	}, []string{"topic"})
	b.FetchRequests = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "streamx_fetch_requests_total", Help: "Fetch requests served.",
	}, []string{"topic", "partition"})
	b.ProduceRequests = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "streamx_produce_requests_total", Help: "Produce requests served.",
	}, []string{"topic", "partition", "acks"})
	b.PartitionOffset = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "streamx_partition_offset", Help: "Log end offset per partition.",
	}, []string{"topic", "partition"})
	b.HighWatermark = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "streamx_partition_high_watermark", Help: "High watermark per partition.",
	}, []string{"topic", "partition"})
	b.LogStartOffset = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "streamx_partition_log_start_offset", Help: "Earliest available offset.",
	}, []string{"topic", "partition"})
	b.ReplicationLag = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "streamx_replication_lag", Help: "Follower LEO lag behind leader.",
	}, []string{"topic", "partition", "leader", "follower"})
	b.Connections = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "streamx_broker_connections", Help: "Active TCP connections.",
	}, []string{"broker"})
	b.Up = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "streamx_up", Help: "1 when broker is up.",
	})
	b.CPUSecs = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "streamx_broker_cpu_seconds_total", Help: "Process CPU seconds.",
	})
	b.MemBytes = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "streamx_broker_memory_bytes", Help: "Process RSS bytes.",
	})
	b.DiskUsedBytes = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "streamx_broker_disk_used_bytes", Help: "Data dir usage bytes.",
	})
	b.RateLimited = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "streamx_rate_limited_total", Help: "Requests rejected by rate limits.",
	}, []string{"scope"})
	b.Backpressure = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "streamx_backpressure_rejects_total", Help: "Appends rejected by backpressure.",
	}, []string{"topic", "partition"})
	return b
}

// API holds the control-plane collectors.
type API struct {
	Registry        *prometheus.Registry
	RequestsTotal   *prometheus.CounterVec
	RequestDuration *prometheus.HistogramVec
	ActiveTokens    prometheus.Gauge
}

func NewAPI() *API {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	a := &API{Registry: reg}
	a.RequestsTotal = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "streamx_api_requests_total", Help: "API requests by route/status.",
	}, []string{"method", "route", "status"})
	a.RequestDuration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name: "streamx_api_request_duration_seconds", Help: "API request latency.",
		Buckets: []float64{.001, .005, .01, .05, .1, .5, 1, 5},
	}, []string{"method", "route"})
	a.ActiveTokens = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "streamx_api_active_tokens", Help: "Valid unexpired tokens.",
	})
	return a
}

// Consumer holds consumer-side collectors (SDK/worker).
type Consumer struct {
	Registry     *prometheus.Registry
	Consumed     *prometheus.CounterVec
	Errors       *prometheus.CounterVec
	Processing   *prometheus.HistogramVec
	Lag          *prometheus.GaugeVec
	ActiveGroups prometheus.Gauge
}

func NewConsumer() *Consumer {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector())
	c := &Consumer{Registry: reg}
	c.Consumed = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "streamx_events_consumed_total", Help: "Events processed by this consumer.",
	}, []string{"topic", "partition", "group"})
	c.Errors = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "streamx_event_processing_errors_total", Help: "Handler failures.",
	}, []string{"topic", "group"})
	c.Processing = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name: "streamx_consumer_latency_seconds", Help: "Handler processing latency.",
		Buckets: []float64{.0001, .0005, .001, .005, .01, .05, .1, .5, 1, 5},
	}, []string{"topic", "group"})
	c.Lag = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "streamx_consumer_lag", Help: "Committed vs latest offset gap.",
	}, []string{"topic", "group", "partition"})
	c.ActiveGroups = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "streamx_active_consumers", Help: "Consumer groups this process participates in.",
	})
	return c
}
