// Package streamx is the STREAMX Go client SDK (docs/SDK_SPEC.md).
//
// It speaks the broker data-plane protocol (length-prefixed JSON over TCP) and
// implements the producer, consumer-group, and retry/DLQ semantics described in
// docs/CONTRACTS.md. It depends only on internal/protocol.
package streamx

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/streamx/streamx/internal/protocol"
)

// Config configures a Client.
type Config struct {
	// Brokers maps broker id -> address (e.g. "broker-1" -> "127.0.0.1:9092").
	// The order is not significant; the client routes by broker id.
	Brokers        map[string]string
	ProducerID     string
	APIURL         string
	APIToken       string
	DialTimeout    time.Duration
	RequestTimeout time.Duration
	MaxRetries     int
}

// BrokersFromCluster converts a "id=host:port,..." cluster string into a
// broker id -> address map.
func BrokersFromCluster(s string) map[string]string {
	return parseCluster(s)
}

func parseCluster(s string) map[string]string {
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

// Client is a STREAMX data-plane client.
type Client struct {
	cfg         Config
	producersMu sync.Mutex
	// metadata cache per topic.
	metaMu sync.Mutex
	meta   map[string]*metaEntry
	corr   int64
}

type metaEntry struct {
	resp    *protocol.MetadataResponse
	fetched time.Time
}

const metaTTL = 5 * time.Second

// NewClient creates a client.
func NewClient(cfg Config) (*Client, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("streamx: no brokers configured")
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 10 * time.Second
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 2
	}
	if cfg.ProducerID == "" {
		cfg.ProducerID = "p-" + randomSuffix()
	}
	return &Client{cfg: cfg, meta: map[string]*metaEntry{}}, nil
}

// brokerAddrs returns all configured broker addresses.
func (c *Client) brokerAddrs() []string {
	out := make([]string, 0, len(c.cfg.Brokers))
	for _, a := range c.cfg.Brokers {
		out = append(out, a)
	}
	return out
}

// Close is a no-op (the client is stateless across connections).
func (c *Client) Close() error { return nil }

// dial connects to a broker address.
func (c *Client) dial(addr string) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, c.cfg.DialTimeout)
}

// pickBroker returns the address of a broker that hosts the given partition
// (prefers the leader, then any live replica).
func (c *Client) pickBroker(ctx context.Context, topic string, partition int32) (string, *protocol.MetadataResponse, error) {
	md, err := c.Metadata(ctx, topic)
	if err != nil {
		return "", nil, err
	}
	var leader, replica string
	for _, pm := range md.Partitions {
		if pm.Partition == partition {
			leader = pm.Leader
			if len(pm.Replicas) > 0 {
				replica = pm.Replicas[0]
			}
			break
		}
	}
	if leader != "" {
		if a, ok := c.cfg.Brokers[leader]; ok {
			return a, md, nil
		}
	}
	if replica != "" {
		if a, ok := c.cfg.Brokers[replica]; ok {
			return a, md, nil
		}
	}
	addrs := c.brokerAddrs()
	if len(addrs) > 0 {
		return addrs[0], md, nil
	}
	return "", md, fmt.Errorf("streamx: no broker address for partition %d", partition)
}

// Metadata returns the topic's partition layout, using a 5 s cache.
func (c *Client) Metadata(ctx context.Context, topic string) (*protocol.MetadataResponse, error) {
	c.metaMu.Lock()
	if e, ok := c.meta[topic]; ok && time.Since(e.fetched) < metaTTL {
		c.metaMu.Unlock()
		return e.resp, nil
	}
	c.metaMu.Unlock()
	resp, err := c.metadataFresh(ctx, topic)
	if err != nil {
		// Serve a stale cache if available.
		c.metaMu.Lock()
		if e, ok := c.meta[topic]; ok {
			c.metaMu.Unlock()
			return e.resp, nil
		}
		c.metaMu.Unlock()
		return nil, err
	}
	c.metaMu.Lock()
	c.meta[topic] = &metaEntry{resp: resp, fetched: time.Now()}
	c.metaMu.Unlock()
	return resp, nil
}

func (c *Client) metadataFresh(ctx context.Context, topic string) (*protocol.MetadataResponse, error) {
	var lastErr error
	for _, addr := range c.brokerAddrs() {
		resp, err := c.call(ctx, addr, &protocol.Request{
			Op:    protocol.OpMetadata,
			Topic: topic,
			Body:  mustJSON(&protocol.MetadataRequest{Topic: topic}),
		})
		if err == nil {
			var md protocol.MetadataResponse
			if err := json.Unmarshal(resp.Body, &md); err != nil {
				return nil, err
			}
			return &md, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// InvalidateMetadata drops the cached metadata for a topic.
func (c *Client) InvalidateMetadata(topic string) {
	c.metaMu.Lock()
	delete(c.meta, topic)
	c.metaMu.Unlock()
}

// Ping measures round-trip time to a broker.
func (c *Client) Ping(ctx context.Context) (time.Duration, error) {
	addrs := c.brokerAddrs()
	if len(addrs) == 0 {
		return 0, fmt.Errorf("streamx: no brokers")
	}
	start := time.Now()
	_, err := c.call(ctx, addrs[0], &protocol.Request{Op: protocol.OpPing, Body: mustJSON(&protocol.PingBody{Ts: time.Now().UnixMilli()})})
	return time.Since(start), err
}

// call performs a single request/response on the given broker address, with
// network-level retries and exponential backoff.
func (c *Client) call(ctx context.Context, addr string, req *protocol.Request) (*protocol.Response, error) {
	req.Correlation = atomic.AddInt64(&c.corr, 1)
	if req.ID == "" {
		req.ID = fmt.Sprintf("req-%d", req.Correlation)
	}
	var lastErr error
	backoff := 250 * time.Millisecond
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
			if backoff < 5*time.Second {
				backoff *= 2
				if backoff > 5*time.Second {
					backoff = 5 * time.Second
				}
			}
		}
		resp, err := c.callOnce(ctx, addr, req)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		// Retry only on network errors, not protocol errors.
		if _, ok := err.(*protocol.StatusError); ok {
			return nil, err
		}
	}
	return nil, lastErr
}

func (c *Client) callOnce(ctx context.Context, addr string, req *protocol.Request) (*protocol.Response, error) {
	conn, err := c.dial(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	deadline := time.Now().Add(c.cfg.RequestTimeout)
	_ = conn.SetDeadline(deadline)
	if err := protocol.WriteFrame(conn, req); err != nil {
		return nil, err
	}
	resp := &protocol.Response{}
	if err := protocol.ReadFrame(bufio.NewReader(conn), resp); err != nil {
		return nil, err
	}
	if resp.Status != protocol.OK {
		return resp, resp.Err()
	}
	return resp, nil
}

// Offsets returns the earliest or latest offset for a partition.
func (c *Client) Offsets(ctx context.Context, topic string, partition int32, which string) (int64, error) {
	addr, _, err := c.pickBroker(ctx, topic, partition)
	if err != nil {
		return 0, err
	}
	resp, err := c.call(ctx, addr, &protocol.Request{
		Op:        protocol.OpOffsets,
		Topic:     topic,
		Partition: partition,
		Body:      mustJSON(&protocol.OffsetsRequest{Which: which}),
	})
	if err != nil {
		return 0, err
	}
	var or protocol.OffsetsResponse
	if err := json.Unmarshal(resp.Body, &or); err != nil {
		return 0, err
	}
	return or.Offset, nil
}

// Fetch reads records from a partition. offset 0 reads from the beginning,
// -1 reads from the earliest available offset.
func (c *Client) Fetch(ctx context.Context, topic string, partition int32, offset int64, maxBytes int, maxWait time.Duration) (*protocol.FetchResponse, error) {
	addr, _, err := c.pickBroker(ctx, topic, partition)
	if err != nil {
		return nil, err
	}
	resp, err := c.call(ctx, addr, &protocol.Request{
		Op:        protocol.OpFetch,
		Topic:     topic,
		Partition: partition,
		Body: mustJSON(&protocol.FetchRequest{
			Offset: &offset, MaxBytes: maxBytes, MaxWait: int(maxWait.Milliseconds()), MinBytes: 1,
		}),
	})
	if err != nil {
		return nil, err
	}
	var fr protocol.FetchResponse
	if err := json.Unmarshal(resp.Body, &fr); err != nil {
		return nil, err
	}
	return &fr, nil
}

// mustJSON marshals v, returning {} on error.
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}

// randomSuffix returns a short random hex string.
func randomSuffix() string {
	return fmt.Sprintf("%08x", time.Now().UnixNano()&0xffffffff)
}

// PartitionForKey routes a key to a partition using xxhash64.
func PartitionForKey(key string, n int32) int32 {
	if n <= 0 {
		return 0
	}
	return int32(xxhash.Sum64String(key)) % n
}

// Ensure sync import is used.
var _ = sync.Once{}
