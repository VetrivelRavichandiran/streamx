// Package cluster implements broker gossip, membership, and deterministic
// leader election (CONTRACTS.md §5).
//
// Model (deliberately simplified, documented in REPLICATION.md):
//   - Brokers gossip over the data-plane TCP protocol: every HeartbeatMS each
//     broker dials every other broker and exchanges cluster_heartbeat frames
//     carrying {broker_id, topics, per-partition {leader?, leo, hw}}.
//   - A broker is dead after TimeoutMS without a heartbeat.
//   - Leader election is deterministic: for each partition, leader = lowest
//     broker-id among live replicas. No consensus.
//   - The group coordinator is the lowest-id live broker in the cluster.
package cluster

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/streamx/streamx/internal/protocol"
)

// MemberInfo is the last-known state of a cluster member.
type MemberInfo struct {
	ID       string
	LastSeen time.Time
	State    protocol.ClusterHeartbeat
}

// Live reports whether the member has been seen within the timeout.
func (m *MemberInfo) Live(timeout time.Duration) bool {
	return time.Since(m.LastSeen) < timeout
}

// Config configures a Cluster.
type Config struct {
	SelfID    string
	Peers     map[string]string // id -> addr (from STREAMX_CLUSTER)
	Timeout   time.Duration     // dead after this (BROKER_TIMEOUT_MS)
	Interval  time.Duration     // heartbeat period (HEARTBEAT_INTERVAL_MS)
	StateFunc func() protocol.ClusterHeartbeat
	Logger    *slog.Logger
	Dial      func(addr string) (net.Conn, error) // injectable for tests
	OnLeader  func(topic string, partition int32, leader string)
	OnMember  func()
}

// Cluster manages gossip, membership, and election.
type Cluster struct {
	selfID   string
	peers    map[string]string
	timeout  time.Duration
	interval time.Duration
	stateFn  func() protocol.ClusterHeartbeat
	dial     func(addr string) (net.Conn, error)
	log      *slog.Logger
	onLeader func(topic string, partition int32, leader string)
	onMember func()

	mu      sync.RWMutex
	members map[string]*MemberInfo

	stop chan struct{}
	done chan struct{}
	wg   sync.WaitGroup
}

// New creates a Cluster. The self broker is always a live member.
func New(cfg Config) *Cluster {
	if cfg.Dial == nil {
		cfg.Dial = func(addr string) (net.Conn, error) {
			return net.DialTimeout("tcp", addr, 2*time.Second)
		}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	c := &Cluster{
		selfID:   cfg.SelfID,
		peers:    cfg.Peers,
		timeout:  cfg.Timeout,
		interval: cfg.Interval,
		stateFn:  cfg.StateFunc,
		dial:     cfg.Dial,
		log:      cfg.Logger,
		onLeader: cfg.OnLeader,
		onMember: cfg.OnMember,
		members:  map[string]*MemberInfo{},
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	c.members[c.selfID] = &MemberInfo{ID: c.selfID, LastSeen: time.Now()}
	return c
}

// Start launches the gossip loop. It blocks until ctx is cancelled or Stop.
func (c *Cluster) Start(ctx context.Context) {
	c.wg.Add(1)
	defer c.wg.Done()
	defer close(c.done)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	// Send an immediate heartbeat so the cluster converges fast.
	c.gossipOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stop:
			return
		case <-ticker.C:
			c.gossipOnce(ctx)
		}
	}
}

// Stop signals the gossip loop to stop and waits for it.
func (c *Cluster) Stop() {
	select {
	case <-c.stop:
	default:
		close(c.stop)
	}
	<-c.done
}

// gossipOnce dials every peer and exchanges heartbeats.
func (c *Cluster) gossipOnce(ctx context.Context) {
	for id, addr := range c.peers {
		if id == c.selfID {
			continue
		}
		if err := c.exchange(ctx, id, addr); err != nil {
			c.log.Debug("gossip: exchange failed", "peer", id, "err", err)
		}
	}
}

// exchange dials a peer, sends our state, and reads its state.
func (c *Cluster) exchange(ctx context.Context, peerID, addr string) error {
	conn, err := c.dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	req := &protocol.Request{
		ID:    fmt.Sprintf("hb-%s-%d", c.selfID, time.Now().UnixNano()),
		Op:    protocol.OpClusterHeart,
		Topic: c.selfID,
		Body:  mustJSON(c.stateFn()),
	}
	if err := protocol.WriteFrame(conn, req); err != nil {
		return err
	}
	r := &protocol.Response{}
	if err := protocol.ReadFrame(bufio.NewReader(conn), r); err != nil {
		return err
	}
	if r.Status != protocol.OK {
		return fmt.Errorf("peer %s returned status %d", peerID, r.Status)
	}
	var hb protocol.ClusterHeartbeat
	if err := json.Unmarshal(r.Body, &hb); err != nil {
		return err
	}
	c.ApplyHeartbeat(hb)
	return nil
}

// ApplyHeartbeat records a peer's state and updates membership. It is called
// by the broker server when it receives a cluster_heartbeat request, and by
// the gossip loop when it receives a peer's response.
func (c *Cluster) ApplyHeartbeat(hb protocol.ClusterHeartbeat) {
	c.mu.Lock()
	m, ok := c.members[hb.BrokerID]
	if !ok {
		m = &MemberInfo{ID: hb.BrokerID}
		c.members[hb.BrokerID] = m
	}
	wasLive := m.Live(c.timeout)
	m.LastSeen = time.Now()
	m.State = hb
	nowLive := m.Live(c.timeout)
	c.mu.Unlock()
	if !wasLive && nowLive {
		c.log.Info("cluster: member back", "broker", hb.BrokerID)
		if c.onMember != nil {
			c.onMember()
		}
	}
}

// LocalState returns the state this broker advertises.
func (c *Cluster) LocalState() protocol.ClusterHeartbeat {
	if c.stateFn == nil {
		return protocol.ClusterHeartbeat{BrokerID: c.selfID, Ts: nowMS()}
	}
	return c.stateFn()
}

// LiveBrokers returns the sorted list of live broker ids (including self).
func (c *Cluster) LiveBrokers() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []string
	for id, m := range c.members {
		if m.Live(c.timeout) {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// IsLive reports whether a broker has been seen within the timeout.
func (c *Cluster) IsLive(id string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if id == c.selfID {
		return true
	}
	m, ok := c.members[id]
	return ok && m.Live(c.timeout)
}

// LeaderFor returns the lowest live broker-id among replicas, or "" if none
// of the replicas are live.
func (c *Cluster) LeaderFor(replicas []string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	live := map[string]bool{}
	for id, m := range c.members {
		if m.Live(c.timeout) {
			live[id] = true
		}
	}
	live[c.selfID] = true
	var best string
	for _, r := range replicas {
		if !live[r] {
			continue
		}
		if best == "" || r < best {
			best = r
		}
	}
	return best
}

// Coordinator returns the lowest live broker-id in the cluster (the group
// coordinator), or the self id if no other broker is live.
func (c *Cluster) Coordinator() string {
	live := c.LiveBrokers()
	if len(live) == 0 {
		return c.selfID
	}
	return live[0]
}

// PeerState returns the last-known state of a peer (its gossiped partition
// states). Returns a zero value if unknown.
func (c *Cluster) PeerState(id string) protocol.ClusterHeartbeat {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if m, ok := c.members[id]; ok {
		return m.State
	}
	return protocol.ClusterHeartbeat{}
}

// MemberCount returns the number of known members (live or not).
func (c *Cluster) MemberCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.members)
}

// AllMembers returns a snapshot of all members (for debugging/monitoring).
func (c *Cluster) AllMembers() []MemberInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]MemberInfo, 0, len(c.members))
	for _, m := range c.members {
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func nowMS() int64 { return time.Now().UnixMilli() }

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}
