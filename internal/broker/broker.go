// Package broker assembles the data-plane broker: partitions, replication,
// group coordination, cluster gossip, and the TCP server (CONTRACTS.md §5-7,
// BROKER.md).
package broker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/streamx/streamx/internal/broker/cluster"
	"github.com/streamx/streamx/internal/broker/groups"
	"github.com/streamx/streamx/internal/broker/storage"
	"github.com/streamx/streamx/internal/config"
	"github.com/streamx/streamx/internal/meta"
	"github.com/streamx/streamx/internal/metrics"
	"github.com/streamx/streamx/internal/protocol"
	"github.com/streamx/streamx/internal/security"
)

// Broker is a single data-plane broker process.
type Broker struct {
	cfg     config.BrokerConfig
	log     *slog.Logger
	meta    meta.Store
	metrics *metrics.Broker
	cluster *cluster.Cluster
	coordin *groups.Coordinator

	mu         sync.RWMutex
	partitions map[string]*Partition // "topic/partition" -> partition
	topics     map[string]*meta.Topic

	// acks=all wait timeout.
	acksAllTimeout time.Duration
	// produce rate limits (per producer + per topic token buckets).
	producerLimits *security.KeyedBucket
	topicLimits    *security.KeyedBucket
	// slowProduceMS simulates slow disk (admin failure injection).
	slowProduceMS atomic.Int64
	// shutdownOnce guards a single graceful shutdown.
	shutdownOnce sync.Once
	// stopped is closed once the broker has shut down (for cmd to exit).
	stopped chan struct{}
}

// New constructs a Broker from config.
func New(cfg config.BrokerConfig, store meta.Store, log *slog.Logger) *Broker {
	b := &Broker{
		cfg:            cfg,
		log:            log,
		meta:           store,
		metrics:        metrics.NewBroker(),
		partitions:     map[string]*Partition{},
		topics:         map[string]*meta.Topic{},
		acksAllTimeout: 5 * time.Second,
		stopped:        make(chan struct{}),
	}
	// Per-producer and per-topic produce rate limits (token buckets).
	pr := cfg.ProduceRateLimit
	if pr <= 0 {
		pr = 50000
	}
	tr := cfg.TopicRateLimit
	if tr <= 0 {
		tr = 200000
	}
	b.producerLimits = security.NewKeyedBucket(float64(pr), pr)
	b.topicLimits = security.NewKeyedBucket(float64(tr), tr)
	// Register self in the meta store.
	_ = store.UpsertBroker(context.Background(), &meta.Broker{ID: cfg.BrokerID, Addr: cfg.Addr, Status: "up"})
	b.cluster = cluster.New(cluster.Config{
		SelfID:   cfg.BrokerID,
		Peers:    cfg.Cluster,
		Timeout:  time.Duration(cfg.BrokerTimeoutMS) * time.Millisecond,
		Interval: time.Duration(cfg.HeartbeatMS) * time.Millisecond,
		StateFunc: func() protocol.ClusterHeartbeat {
			return b.state()
		},
		Logger: log,
	})
	b.coordin = groups.NewCoordinator(groups.Config{
		Meta:      store,
		PartCount: b.partCount,
		Logger:    log,
	})
	b.metrics.Up.Set(1)
	return b
}

// partCount returns the partition count for a topic (from the meta store).
func (b *Broker) partCount(topic string) (int32, bool) {
	b.mu.RLock()
	t, ok := b.topics[topic]
	b.mu.RUnlock()
	if ok {
		return t.Partitions, true
	}
	t, err := b.meta.GetTopic(context.Background(), topic)
	if err != nil {
		return 0, false
	}
	b.mu.Lock()
	b.topics[topic] = t
	b.mu.Unlock()
	return t.Partitions, true
}

// Start runs all broker loops until ctx is cancelled.
func (b *Broker) Start(ctx context.Context) error {
	// Initial topic sync + partition open.
	b.syncTopics(ctx)
	// cluster.Start blocks on its gossip loop, so run it in the background.
	go b.cluster.Start(ctx)
	// Only the coordinator broker runs the group coordinator.
	if b.cluster.Coordinator() == b.cfg.BrokerID {
		b.coordin.Start(ctx)
	}
	// Background loops.
	go b.loop(ctx, time.Duration(b.cfg.HeartbeatMS)*time.Millisecond, func() { b.syncTopics(ctx) })
	go b.loop(ctx, 100*time.Millisecond, b.replicationTick)
	go b.loop(ctx, time.Duration(b.cfg.RetentionCheckMS)*time.Millisecond, b.retentionTick)
	go b.loop(ctx, 5*time.Second, b.resourceTick)
	go b.loop(ctx, 5*time.Second, b.touchMeta)
	<-ctx.Done()
	return b.Shutdown()
}

// loop runs fn every interval until ctx is cancelled.
func (b *Broker) loop(ctx context.Context, interval time.Duration, fn func()) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn()
		}
	}
}

// Shutdown performs a graceful shutdown: stop loops, close logs, unregister.
func (b *Broker) Shutdown() error {
	b.shutdownOnce.Do(func() {
		b.cluster.Stop()
		b.coordin.Stop()
		b.mu.Lock()
		for _, p := range b.partitions {
			p.Close()
		}
		b.mu.Unlock()
		_ = b.meta.SetBrokerStatus(context.Background(), b.cfg.BrokerID, "down")
		b.metrics.Up.Set(0)
		close(b.stopped)
	})
	return nil
}

// Stopped returns a channel that is closed once the broker has shut down
// (used by cmd/broker to exit when an admin stop is triggered).
func (b *Broker) Stopped() <-chan struct{} { return b.stopped }

// ---------------------------------------------------------------------------
// Admin (failure injection, driven by the control plane)
// ---------------------------------------------------------------------------

// GracefulStop shuts the broker down cleanly.
func (b *Broker) GracefulStop() error {
	return b.Shutdown()
}

// SetSlowProduceMS simulates a slow disk by delaying produces by the given
// number of milliseconds (0 disables it).
func (b *Broker) SetSlowProduceMS(ms int64) { b.slowProduceMS.Store(ms) }

// SlowProduceMS returns the current simulated produce delay.
func (b *Broker) SlowProduceMS() int64 { return b.slowProduceMS.Load() }

// DeleteTopic closes and removes this broker's copies of a topic's partitions
// and deletes the topic's data directory.
func (b *Broker) DeleteTopic(name string) error {
	b.mu.Lock()
	for key, p := range b.partitions {
		if p.Topic == name {
			p.Close()
			delete(b.partitions, key)
		}
	}
	delete(b.topics, name)
	b.mu.Unlock()
	_ = os.RemoveAll(filepath.Join(b.cfg.DataDir, name))
	return nil
}

// PartitionsLed returns the number of partitions this broker leads.
func (b *Broker) PartitionsLed() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	n := 0
	for _, p := range b.partitions {
		if b.isLeader(p) {
			n++
		}
	}
	return n
}

// CheckProduceLimits enforces the per-producer and per-topic produce rate
// limits, consuming one token per event, returning the retry-after hint when
// limited.
func (b *Broker) CheckProduceLimits(producer, topic string, count int) (bool, int) {
	if b.producerLimits != nil && producer != "" {
		for i := 0; i < count; i++ {
			if !b.producerLimits.Allow(producer) {
				return false, 100
			}
		}
	}
	if b.topicLimits != nil && topic != "" {
		for i := 0; i < count; i++ {
			if !b.topicLimits.Allow(topic) {
				return false, 100
			}
		}
	}
	return true, 0
}

// ---------------------------------------------------------------------------
// Topic sync & partition management
// ---------------------------------------------------------------------------

// syncTopics loads topics from the meta store and opens partitions this
// broker is a replica of.
func (b *Broker) syncTopics(ctx context.Context) {
	topics, err := b.meta.ListTopics(ctx)
	if err != nil {
		b.log.Warn("broker: list topics", "err", err)
		return
	}
	b.mu.Lock()
	b.topics = map[string]*meta.Topic{}
	for _, t := range topics {
		b.topics[t.Name] = t
	}
	b.mu.Unlock()
	sortedCluster := config.SortedCluster(b.cfg.Cluster)
	for _, t := range topics {
		for p := int32(0); p < t.Partitions; p++ {
			replicas := config.AssignReplicas(sortedCluster, p, t.ReplicationFactor)
			if !contains(replicas, b.cfg.BrokerID) {
				continue
			}
			b.ensurePartition(t, p, replicas)
		}
	}
}

func (b *Broker) ensurePartition(t *meta.Topic, p int32, replicas []string) {
	key := partKey(t.Name, p)
	b.mu.Lock()
	if _, ok := b.partitions[key]; ok {
		b.mu.Unlock()
		return
	}
	b.mu.Unlock()
	dir := filepath.Join(b.cfg.DataDir, t.Name, fmt.Sprintf("%d", p))
	lg, err := storage.OpenLog(dir, storage.LogConfig{
		FsyncIntervalMS: b.cfg.FsyncIntervalMS,
		SegmentMaxBytes: b.cfg.SegmentMaxBytes,
		PendingBudget:   b.cfg.PendingBudget,
		PendingBudgetN:  100000,
	})
	if err != nil {
		b.log.Error("broker: open partition log", "topic", t.Name, "partition", p, "err", err)
		return
	}
	// Initialize HW to LEO (nothing in flight).
	leo := lg.LEO()
	np := &Partition{
		Topic:    t.Name,
		ID:       p,
		Replicas: replicas,
		Log:      lg,
	}
	np.ret.MaxAgeMS = t.Retention.MaxAgeMS
	np.ret.MaxBytes = t.Retention.MaxBytes
	np.SetHW(leo)
	b.mu.Lock()
	b.partitions[key] = np
	b.mu.Unlock()
	b.log.Info("broker: opened partition", "topic", t.Name, "partition", p, "replicas", replicas, "leo", leo)
}

func partKey(topic string, p int32) string { return fmt.Sprintf("%s/%d", topic, p) }

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// getPartition returns a partition hosted by this broker.
func (b *Broker) getPartition(topic string, p int32) *Partition {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.partitions[partKey(topic, p)]
}

// isLeader reports whether this broker leads the partition.
func (b *Broker) isLeader(p *Partition) bool {
	leader := b.cluster.LeaderFor(p.Replicas)
	return leader == b.cfg.BrokerID
}

// ---------------------------------------------------------------------------
// Replication & high watermark
// ---------------------------------------------------------------------------

// replicationTick runs the follower fetch loop and updates HW on leaders.
func (b *Broker) replicationTick() {
	ctx := context.Background()
	b.mu.RLock()
	ps := make([]*Partition, 0, len(b.partitions))
	for _, p := range b.partitions {
		ps = append(ps, p)
	}
	b.mu.RUnlock()
	for _, p := range ps {
		leader := b.cluster.LeaderFor(p.Replicas)
		if leader == b.cfg.BrokerID {
			b.updateHW(p)
		} else if leader != "" {
			b.followerFetch(ctx, p, leader)
		}
	}
}

// updateHW sets HW = min(follower LEOs, own LEO) on the leader.
func (b *Broker) updateHW(p *Partition) {
	leo := p.LEO()
	hw := leo
	for _, r := range p.Replicas {
		if r == b.cfg.BrokerID {
			continue
		}
		if !b.cluster.IsLive(r) {
			continue
		}
		st := b.cluster.PeerState(r)
		if ts, ok := st.Topics[p.Topic]; ok {
			if ps, ok := ts.Partitions[p.ID]; ok {
				if ps.LEO < hw {
					hw = ps.LEO
				}
				// Replication lag metric.
				lag := leo - ps.LEO
				if lag < 0 {
					lag = 0
				}
				b.metrics.ReplicationLag.WithLabelValues(p.Topic, fmt.Sprintf("%d", p.ID), b.cfg.BrokerID, r).Set(float64(lag))
			}
		}
	}
	p.SetHW(hw)
	// Update offset gauges for observability.
	b.metrics.PartitionOffset.WithLabelValues(p.Topic, fmt.Sprintf("%d", p.ID)).Set(float64(leo))
	b.metrics.HighWatermark.WithLabelValues(p.Topic, fmt.Sprintf("%d", p.ID)).Set(float64(hw))
	b.metrics.LogStartOffset.WithLabelValues(p.Topic, fmt.Sprintf("%d", p.ID)).Set(float64(p.LSO()))
}

// followerFetch fetches from the leader starting at own LEO and appends.
func (b *Broker) followerFetch(ctx context.Context, p *Partition, leader string) {
	leo := p.LEO()
	recs, err := b.fetchFrom(ctx, leader, p.Topic, p.ID, leo, 1<<20, 0)
	if err != nil {
		return
	}
	for _, r := range recs {
		if _, err := p.Append(storage.Record{
			Key:         r.Record.Key,
			Payload:     r.Record.Payload,
			Headers:     r.Record.Headers,
			ProducerSeq: r.Record.ProducerSeq,
			ProducerID:  r.Record.ProducerID,
		}); err != nil {
			b.log.Warn("broker: follower append", "err", err)
			break
		}
	}
}

// fetchFrom fetches records from a specific broker.
func (b *Broker) fetchFrom(ctx context.Context, brokerID, topic string, partition int32, offset int64, maxBytes int, maxWaitMS int) ([]storage.StoredRecord, error) {
	addr, ok := b.cfg.Cluster[brokerID]
	if !ok {
		return nil, fmt.Errorf("unknown broker %s", brokerID)
	}
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	req := &protocol.Request{
		ID:        fmt.Sprintf("fr-%d", time.Now().UnixNano()),
		Op:        protocol.OpFetch,
		Topic:     topic,
		Partition: partition,
		Body:      mustJSON(&protocol.FetchRequest{Offset: &offset, MaxBytes: maxBytes, MaxWait: maxWaitMS, MinBytes: 1}),
	}
	if err := protocol.WriteFrame(conn, req); err != nil {
		return nil, err
	}
	resp := &protocol.Response{}
	if err := protocol.ReadFrame(bufio.NewReader(conn), resp); err != nil {
		return nil, err
	}
	if resp.Status != protocol.OK {
		return nil, resp.Err()
	}
	var fr protocol.FetchResponse
	if err := json.Unmarshal(resp.Body, &fr); err != nil {
		return nil, err
	}
	out := make([]storage.StoredRecord, 0, len(fr.Records))
	for _, ev := range fr.Records {
		out = append(out, storage.StoredRecord{
			Offset: ev.Offset,
			Record: storage.Record{
				Key:         ev.Key,
				Payload:     ev.Payload,
				Headers:     ev.Headers,
				ProducerSeq: ev.Seq,
				ProducerID:  ev.Producer,
			},
		})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// State & metrics
// ---------------------------------------------------------------------------

// state builds the cluster heartbeat state this broker advertises.
func (b *Broker) state() protocol.ClusterHeartbeat {
	b.mu.RLock()
	defer b.mu.RUnlock()
	topics := map[string]protocol.TopicState{}
	for _, p := range b.partitions {
		ts, ok := topics[p.Topic]
		if !ok {
			ts = protocol.TopicState{Partitions: map[int32]protocol.PartitionState{}}
		}
		ts.Partitions[p.ID] = protocol.PartitionState{
			IsLeader: b.isLeader(p),
			LEO:      p.LEO(),
			HW:       p.HW(),
		}
		topics[p.Topic] = ts
	}
	return protocol.ClusterHeartbeat{BrokerID: b.cfg.BrokerID, Topics: topics, Ts: time.Now().UnixMilli()}
}

// resourceTick updates process resource gauges.
func (b *Broker) resourceTick() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	b.metrics.MemBytes.Set(float64(m.Alloc))
	disk, _ := dirSize(b.cfg.DataDir)
	b.metrics.DiskUsedBytes.Set(float64(disk))
}

// touchMeta keeps the broker's last_seen fresh in the meta store.
func (b *Broker) touchMeta() {
	_ = b.meta.TouchBroker(context.Background(), b.cfg.BrokerID)
}

// retentionTick runs retention sweeps.
func (b *Broker) retentionTick() {
	b.mu.RLock()
	ps := make([]*Partition, 0, len(b.partitions))
	for _, p := range b.partitions {
		ps = append(ps, p)
	}
	b.mu.RUnlock()
	for _, p := range ps {
		if _, err := p.Retention(); err != nil {
			b.log.Warn("broker: retention", "topic", p.Topic, "partition", p.ID, "err", err)
		}
	}
}

// Metrics returns the broker's Prometheus registry.
func (b *Broker) Metrics() *prometheus.Registry { return b.metrics.Registry }

// Cluster returns the cluster manager.
func (b *Broker) Cluster() *cluster.Cluster { return b.cluster }

// Coordinator returns the group coordinator.
func (b *Broker) Coordinator() *groups.Coordinator { return b.coordin }

// Config returns the broker config.
func (b *Broker) Config() config.BrokerConfig { return b.cfg }

// Meta returns the meta store.
func (b *Broker) Meta() meta.Store { return b.meta }

// IsCoordinator reports whether this broker is the group coordinator.
func (b *Broker) IsCoordinator() bool { return b.cluster.Coordinator() == b.cfg.BrokerID }

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}

func dirSize(dir string) (int64, error) {
	var total int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	for _, e := range entries {
		if e.IsDir() {
			sz, _ := dirSize(filepath.Join(dir, e.Name()))
			total += sz
		} else {
			if fi, err := e.Info(); err == nil {
				total += fi.Size()
			}
		}
	}
	return total, nil
}
