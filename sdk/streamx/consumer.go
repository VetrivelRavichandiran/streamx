package streamx

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/streamx/streamx/internal/protocol"
)

// GroupConfig configures a Consumer.
type GroupConfig struct {
	Client      *Client
	Group       string
	Topic       string
	MemberID    string
	StartAt     string // "earliest" | "latest"
	Handler     func(ctx context.Context, ev *protocol.Event) error
	MaxRetries  int
	Concurrency int
	QueueSize   int
	DLQTopic    string
	OnDLQ       func(ctx context.Context, ev *protocol.Event, reason string)
}

// Consumer is a consumer-group member.
type Consumer struct {
	cfg        GroupConfig
	memberID   string
	assignment []int32
	generation int32

	// per-partition fetch cursor (next offset to read).
	commitMu sync.Mutex
	offsets  map[int32]int64
	// per-partition in-flight offsets (enqueued, not yet processed). The
	// committed offset for a partition is the smallest in-flight offset, or
	// the cursor when none are in flight (at-least-once).
	inflight map[int32]map[int64]bool
	// per-partition latest (for lag).
	latestMu sync.Mutex
	latest   map[int32]int64

	dlqClient *http.Client
}

// NewConsumer creates a consumer.
func NewConsumer(cfg GroupConfig) (*Consumer, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("streamx: client required")
	}
	if cfg.Group == "" || cfg.Topic == "" {
		return nil, fmt.Errorf("streamx: group and topic required")
	}
	if cfg.Handler == nil {
		return nil, fmt.Errorf("streamx: handler required")
	}
	if cfg.MemberID == "" {
		cfg.MemberID = "m-" + randomSuffix()
	}
	if cfg.StartAt == "" {
		cfg.StartAt = "earliest"
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 8
	}
	if cfg.QueueSize == 0 {
		cfg.QueueSize = 10000
	}
	if cfg.DLQTopic == "" {
		cfg.DLQTopic = cfg.Topic + ".dlq"
	}
	return &Consumer{
		cfg:       cfg,
		memberID:  cfg.MemberID,
		offsets:   map[int32]int64{},
		inflight:  map[int32]map[int64]bool{},
		latest:    map[int32]int64{},
		dlqClient: &http.Client{Timeout: 10 * time.Second},
	}, nil
}

// MemberID returns the consumer's member id.
func (c *Consumer) MemberID() string { return c.memberID }

// Assignment returns the current partition assignment.
func (c *Consumer) Assignment() []int32 {
	c.commitMu.Lock()
	defer c.commitMu.Unlock()
	out := make([]int32, len(c.assignment))
	copy(out, c.assignment)
	return out
}

// Lag returns per-partition lag (latest - committed). The committed offset is
// the smallest in-flight offset, or the read cursor when none are in flight.
func (c *Consumer) Lag() map[int32]int64 {
	c.commitMu.Lock()
	defer c.commitMu.Unlock()
	out := map[int32]int64{}
	for p, cursor := range c.offsets {
		committed := cursor
		if m, ok := c.inflight[p]; ok {
			for off := range m {
				if off < committed {
					committed = off
				}
			}
		}
		c.latestMu.Lock()
		latest := c.latest[p]
		c.latestMu.Unlock()
		lag := latest - committed
		if lag < 0 {
			lag = 0
		}
		out[p] = lag
	}
	return out
}

// Start runs the consumer loop until ctx is cancelled. It blocks.
func (c *Consumer) Start(ctx context.Context) error {
	c.join(ctx)
	// Heartbeat loop.
	hbDone := make(chan struct{})
	go c.heartbeatLoop(ctx, hbDone)
	// Fetch loop.
	c.fetchLoop(ctx)
	// Clean shutdown: commit + leave.
	c.commitAll(ctx)
	c.leave(ctx)
	close(hbDone)
	return ctx.Err()
}

// join joins the group and sets the assignment + committed offsets.
func (c *Consumer) join(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		addr, md, err := c.coordinatorAddr(ctx)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		resp, err := c.cfg.Client.call(ctx, addr, &protocol.Request{
			Op:   protocol.OpGroupJoin,
			Body: mustJSON(&protocol.GroupJoinRequest{Group: c.cfg.Group, MemberID: c.memberID, Topic: c.cfg.Topic}),
		})
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		var gj protocol.GroupJoinResponse
		if err := json.Unmarshal(resp.Body, &gj); err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		c.commitMu.Lock()
		c.assignment = gj.Assignment
		c.generation = gj.Generation
		c.commitMu.Unlock()
		// Load committed offsets; for uncommitted partitions use the start position.
		c.loadOffsets(ctx, md)
		return
	}
}

// coordinatorAddr returns the address of the group coordinator (the lowest-id
// live broker). We approximate it by trying brokers in id order.
func (c *Consumer) coordinatorAddr(ctx context.Context) (string, *protocol.MetadataResponse, error) {
	// Get metadata to know the cluster, then pick the lowest-id broker.
	md, err := c.cfg.Client.Metadata(ctx, c.cfg.Topic)
	if err != nil {
		return "", nil, err
	}
	// Determine the set of live broker ids from any partition's replicas.
	ids := map[string]bool{}
	for _, pm := range md.Partitions {
		for _, r := range pm.Replicas {
			ids[r] = true
		}
	}
	// Sort ids and pick the lowest that we have an address for.
	sorted := sortedKeys(ids)
	for _, id := range sorted {
		if a, ok := c.cfg.Client.cfg.Brokers[id]; ok {
			return a, md, nil
		}
	}
	addrs := c.cfg.Client.brokerAddrs()
	if len(addrs) > 0 {
		return addrs[0], md, nil
	}
	return "", md, fmt.Errorf("streamx: no coordinator address")
}

// loadOffsets sets per-partition start offsets: committed if present, else
// the start position. It resolves earliest/latest via the offsets op so the
// fetch offset is always an explicit value (0 on the wire means "from HW").
func (c *Consumer) loadOffsets(ctx context.Context, md *protocol.MetadataResponse) {
	committed, _ := c.getCommitted(ctx)
	c.commitMu.Lock()
	defer c.commitMu.Unlock()
	for _, p := range c.assignment {
		if off, ok := committed[p]; ok {
			c.offsets[p] = off
			continue
		}
		var start int64
		if c.cfg.StartAt == "latest" {
			if off, err := c.cfg.Client.Offsets(ctx, c.cfg.Topic, p, "latest"); err == nil {
				start = off
			} else {
				for _, pm := range md.Partitions {
					if pm.Partition == p {
						start = pm.LogEndOffset
						break
					}
				}
			}
		} else {
			if off, err := c.cfg.Client.Offsets(ctx, c.cfg.Topic, p, "earliest"); err == nil {
				start = off
			} else {
				for _, pm := range md.Partitions {
					if pm.Partition == p {
						start = pm.LogStart
						break
					}
				}
			}
		}
		c.offsets[p] = start
	}
}

// getCommitted reads the group's committed offsets from the coordinator.
func (c *Consumer) getCommitted(ctx context.Context) (map[int32]int64, error) {
	addr, _, err := c.coordinatorAddr(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := c.cfg.Client.call(ctx, addr, &protocol.Request{
		Op: protocol.OpGroupDescr,
		Body: mustJSON(struct {
			Group string `json:"group"`
		}{Group: c.cfg.Group}),
	})
	if err != nil {
		return nil, err
	}
	var gd protocol.GroupDescribeResponse
	if err := json.Unmarshal(resp.Body, &gd); err != nil {
		return nil, err
	}
	return gd.Offsets, nil
}

// heartbeatLoop sends a heartbeat every 2 s; on generation mismatch it
// triggers a rejoin.
func (c *Consumer) heartbeatLoop(ctx context.Context, done chan struct{}) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			c.heartbeatOnce(ctx)
		}
	}
}

func (c *Consumer) heartbeatOnce(ctx context.Context) {
	c.commitMu.Lock()
	gen := c.generation
	c.commitMu.Unlock()
	addr, _, err := c.coordinatorAddr(ctx)
	if err != nil {
		return
	}
	resp, err := c.cfg.Client.call(ctx, addr, &protocol.Request{
		Op:   protocol.OpGroupHeart,
		Body: mustJSON(&protocol.GroupHeartbeatRequest{Group: c.cfg.Group, MemberID: c.memberID, Generation: gen}),
	})
	if err != nil {
		return
	}
	var gh protocol.GroupHeartbeatResponse
	if err := json.Unmarshal(resp.Body, &gh); err != nil {
		return
	}
	if gh.RebalanceRequired || !gh.OK {
		// Rejoin with the new generation.
		c.join(ctx)
	}
}

// fetchLoop fetches from assigned partitions and dispatches to the handler
// queue.
func (c *Consumer) fetchLoop(ctx context.Context) {
	queue := make(chan *protocol.Event, c.cfg.QueueSize)
	// Handler workers.
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ev := range queue {
				c.process(ctx, ev)
			}
		}()
	}
	defer wg.Wait()
	defer close(queue) // close first (LIFO) so workers drain and exit, then Wait returns

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	lastCommit := time.Now()

	for {
		if ctx.Err() != nil {
			return
		}
		c.commitMu.Lock()
		assignment := make([]int32, len(c.assignment))
		copy(assignment, c.assignment)
		c.commitMu.Unlock()
		// Fetch each assigned partition.
		for _, p := range assignment {
			c.commitMu.Lock()
			off := c.offsets[p]
			c.commitMu.Unlock()
			addr, _, err := c.cfg.Client.pickBroker(ctx, c.cfg.Topic, p)
			if err != nil {
				continue
			}
			resp, err := c.cfg.Client.call(ctx, addr, &protocol.Request{
				Op:        protocol.OpFetch,
				Topic:     c.cfg.Topic,
				Partition: p,
				Body:      mustJSON(&protocol.FetchRequest{Offset: &off, MaxBytes: 1 << 20, MaxWait: 500, MinBytes: 1}),
			})
			if err != nil {
				continue
			}
			var fr protocol.FetchResponse
			if err := json.Unmarshal(resp.Body, &fr); err != nil {
				continue
			}
			// Track latest for lag.
			c.latestMu.Lock()
			c.latest[p] = fr.HighWatermark
			c.latestMu.Unlock()
			// Enqueue records (blocks when the queue is full → backpressure).
			for _, ev := range fr.Records {
				if ctx.Err() != nil {
					return
				}
				queue <- ev
				c.commitMu.Lock()
				c.offsets[p] = ev.Offset + 1
				if c.inflight[p] == nil {
					c.inflight[p] = map[int64]bool{}
				}
				c.inflight[p][ev.Offset] = true
				c.commitMu.Unlock()
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if time.Since(lastCommit) >= time.Second {
				c.commitAll(ctx)
				lastCommit = time.Now()
			}
		}
	}
}

// process runs the handler with in-place retries, then the DLQ on exhaustion.
// On completion (success or DLQ) the event is removed from the partition's
// in-flight set so the committed offset can advance past it.
func (c *Consumer) process(ctx context.Context, ev *protocol.Event) {
	backoff := 100 * time.Millisecond
	var lastErr error
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 5*time.Second {
				backoff *= 2
				if backoff > 5*time.Second {
					backoff = 5 * time.Second
				}
			}
		}
		if err := c.cfg.Handler(ctx, ev); err != nil {
			lastErr = err
			continue
		}
		c.done(ev)
		return // success
	}
	// Exhausted → DLQ.
	reason := "handler error"
	if lastErr != nil {
		reason = lastErr.Error()
	}
	c.toDLQ(ctx, ev, reason)
	c.done(ev)
}

// done marks an event as processed, releasing it from the in-flight set.
func (c *Consumer) done(ev *protocol.Event) {
	c.commitMu.Lock()
	defer c.commitMu.Unlock()
	if m, ok := c.inflight[ev.Partition]; ok {
		delete(m, ev.Offset)
	}
}

// toDLQ publishes the event to the DLQ topic, calls the OnDLQ hook, and
// reports to the API if configured.
func (c *Consumer) toDLQ(ctx context.Context, ev *protocol.Event, reason string) {
	dlqHeaders := map[string]string{
		"dlq_original_topic":     ev.Topic,
		"dlq_original_partition": fmt.Sprintf("%d", ev.Partition),
		"dlq_original_offset":    fmt.Sprintf("%d", ev.Offset),
		"dlq_key":                ev.Key,
		"dlq_retry_count":        fmt.Sprintf("%d", c.cfg.MaxRetries),
		"dlq_reason":             reason,
		"dlq_ts":                 fmt.Sprintf("%d", time.Now().UnixMilli()),
	}
	// Publish to the DLQ topic (auto-create via API if configured).
	if c.cfg.Client.cfg.APIURL != "" {
		c.ensureTopic(ctx, c.cfg.DLQTopic)
	}
	_, _ = c.cfg.Client.ProduceOne(ctx, c.cfg.DLQTopic, ev.Key, ev.Payload, dlqHeaders, "leader")
	// Hook.
	if c.cfg.OnDLQ != nil {
		c.cfg.OnDLQ(ctx, ev, reason)
	}
	// Report to the API.
	if c.cfg.Client.cfg.APIURL != "" && c.cfg.Client.cfg.APIToken != "" {
		c.reportDLQ(ctx, ev, reason)
	}
}

// reportDLQ POSTs the DLQ entry to the API.
func (c *Consumer) reportDLQ(ctx context.Context, ev *protocol.Event, reason string) {
	body := map[string]any{
		"topic":           c.cfg.DLQTopic,
		"group":           c.cfg.Group,
		"original_topic":  ev.Topic,
		"original_part":   ev.Partition,
		"original_offset": ev.Offset,
		"key":             ev.Key,
		"payload":         ev.Payload,
		"reason":          reason,
		"retry_count":     c.cfg.MaxRetries,
		"status":          "pending",
	}
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(c.cfg.Client.cfg.APIURL, "/")+"/api/v1/dlq", strings.NewReader(string(b)))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.cfg.Client.cfg.APIToken)
	resp, err := c.dlqClient.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// ensureTopic creates a topic via the API if it does not exist.
func (c *Consumer) ensureTopic(ctx context.Context, name string) {
	if c.cfg.Client.cfg.APIToken == "" {
		return
	}
	url := strings.TrimRight(c.cfg.Client.cfg.APIURL, "/") + "/api/v1/topics/" + name
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.Client.cfg.APIToken)
	resp, err := c.dlqClient.Do(req)
	if err == nil {
		resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			// Create it.
			body := map[string]any{"name": name, "partitions": 1, "replication_factor": 1}
			b, _ := json.Marshal(body)
			creq, _ := http.NewRequestWithContext(ctx, http.MethodPost,
				strings.TrimRight(c.cfg.Client.cfg.APIURL, "/")+"/api/v1/topics", strings.NewReader(string(b)))
			creq.Header.Set("Content-Type", "application/json")
			creq.Header.Set("Authorization", "Bearer "+c.cfg.Client.cfg.APIToken)
			if r, err := c.dlqClient.Do(creq); err == nil {
				r.Body.Close()
			}
		}
	}
}

// commitAll commits the current per-partition offsets to the coordinator. For
// each partition the committed offset is the smallest in-flight offset (the
// first event not yet processed), or the read cursor when none are in flight.
func (c *Consumer) commitAll(ctx context.Context) {
	c.commitMu.Lock()
	if len(c.offsets) == 0 {
		c.commitMu.Unlock()
		return
	}
	offsets := make(map[int32]int64, len(c.offsets))
	for p, cursor := range c.offsets {
		commit := cursor
		if m, ok := c.inflight[p]; ok {
			for off := range m {
				if off < commit {
					commit = off
				}
			}
		}
		offsets[p] = commit
	}
	c.commitMu.Unlock()
	addr, _, err := c.coordinatorAddr(ctx)
	if err != nil {
		return
	}
	_, _ = c.cfg.Client.call(ctx, addr, &protocol.Request{
		Op:   protocol.OpGroupCommit,
		Body: mustJSON(&protocol.GroupCommitRequest{Group: c.cfg.Group, MemberID: c.memberID, Offsets: offsets}),
	})
}

// leave removes the member from the group.
func (c *Consumer) leave(ctx context.Context) {
	addr, _, err := c.coordinatorAddr(ctx)
	if err != nil {
		return
	}
	_, _ = c.cfg.Client.call(ctx, addr, &protocol.Request{
		Op:   protocol.OpGroupLeave,
		Body: mustJSON(&protocol.GroupLeaveRequest{Group: c.cfg.Group, MemberID: c.memberID}),
	})
}

// sortedKeys returns the sorted keys of a map.
func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
