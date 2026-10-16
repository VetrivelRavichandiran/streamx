// Package groups implements the consumer-group coordinator (CONTRACTS.md §7).
//
// The coordinator runs on the lowest-id live broker. It keeps membership in
// memory (rebuilt from heartbeats) and persists committed offsets to the meta
// store so they survive restarts. Assignment uses the range strategy: members
// are sorted by id and partitions are split evenly (remainder to the earliest
// ids). Every membership change bumps the generation, which is how consumers
// detect a rebalance.
package groups

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/streamx/streamx/internal/meta"
	"github.com/streamx/streamx/internal/protocol"
)

// Member is an in-memory group member.
type Member struct {
	ID         string
	Assignment []int32
	LastSeen   time.Time
	Generation int32
}

// Group is an in-memory consumer group.
type Group struct {
	ID         string
	Topic      string
	Generation int32
	Members    map[string]*Member // member id -> member
}

// Config configures a Coordinator.
type Config struct {
	Meta      meta.Store
	PartCount func(topic string) (int32, bool) // topic -> (partition count, ok)
	ExpireMS  time.Duration                    // member expired after this (default 10s)
	CheckMS   time.Duration                    // expiry sweep period (default 2s)
	Logger    *slog.Logger
}

// Coordinator manages consumer groups on the coordinator broker.
type Coordinator struct {
	meta      meta.Store
	partCount func(topic string) (int32, bool)
	expire    time.Duration
	check     time.Duration
	log       *slog.Logger

	mu     sync.Mutex
	groups map[string]*Group

	stop chan struct{}
	done chan struct{}
}

// NewCoordinator creates a coordinator.
func NewCoordinator(cfg Config) *Coordinator {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.ExpireMS <= 0 {
		cfg.ExpireMS = 10 * time.Second
	}
	if cfg.CheckMS <= 0 {
		cfg.CheckMS = 2 * time.Second
	}
	return &Coordinator{
		meta:      cfg.Meta,
		partCount: cfg.PartCount,
		expire:    cfg.ExpireMS,
		check:     cfg.CheckMS,
		log:       cfg.Logger,
		groups:    map[string]*Group{},
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// Start runs the expiry sweep loop until Stop is called.
func (c *Coordinator) Start(ctx context.Context) {
	go func() {
		defer close(c.done)
		ticker := time.NewTicker(c.check)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stop:
				return
			case <-ticker.C:
				c.expireSweep()
			}
		}
	}()
}

// Stop stops the sweep loop.
func (c *Coordinator) Stop() {
	select {
	case <-c.stop:
	default:
		close(c.stop)
	}
	<-c.done
}

// Join adds a member to a group and returns its assignment.
func (c *Coordinator) Join(ctx context.Context, group, memberID string, topic string) (*protocol.GroupJoinResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[group]
	if !ok {
		if _, ok := c.partCount(topic); !ok {
			return nil, &protocol.StatusError{Status: protocol.TopicNotFound, Message: "topic not found: " + topic}
		}
		g = &Group{ID: group, Topic: topic, Generation: 1, Members: map[string]*Member{}}
		c.groups[group] = g
		if err := c.meta.UpsertGroup(ctx, &meta.Group{ID: group, Generation: 1, Topic: topic}); err != nil {
			c.log.Warn("groups: upsert group", "err", err)
		}
	} else if topic != "" && topic != g.Topic {
		return nil, &protocol.StatusError{Status: protocol.ValidationFailed, Message: "group " + group + " consumes " + g.Topic + ", not " + topic}
	} else if topic == "" {
		topic = g.Topic
	}
	if _, ok := c.partCount(topic); !ok {
		return nil, &protocol.StatusError{Status: protocol.TopicNotFound, Message: "topic not found: " + topic}
	}
	if _, exists := g.Members[memberID]; !exists {
		// New member: register it, bump generation, reassign all partitions.
		g.Members[memberID] = &Member{ID: memberID, LastSeen: time.Now()}
		g.Generation++
		if err := c.meta.SetGroupGeneration(ctx, group, g.Generation); err != nil {
			c.log.Warn("groups: set generation", "err", err)
		}
		c.reassignLocked(g)
	}
	// Persist the consumer.
	assign := g.Members[memberID].Assignment
	_ = c.meta.UpsertConsumer(ctx, &meta.Consumer{ID: memberID, Group: group, Status: "up", Assignment: assign})
	_ = c.meta.TouchConsumer(ctx, group, memberID)
	return &protocol.GroupJoinResponse{
		Generation: g.Generation,
		Assignment: assign,
		IsLeader:   isLeader(g, memberID),
	}, nil
}

// Heartbeat refreshes a member and reports the current generation.
func (c *Coordinator) Heartbeat(ctx context.Context, group, memberID string, gen int32) (*protocol.GroupHeartbeatResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[group]
	if !ok {
		return nil, &protocol.StatusError{Status: protocol.GroupNotFound, Message: "group not found: " + group}
	}
	m, ok := g.Members[memberID]
	if !ok {
		// Member was expired and removed; force a rejoin.
		return &protocol.GroupHeartbeatResponse{OK: false, Generation: g.Generation, RebalanceRequired: true}, nil
	}
	m.LastSeen = time.Now()
	_ = c.meta.TouchConsumer(ctx, group, memberID)
	if gen != g.Generation {
		return &protocol.GroupHeartbeatResponse{OK: true, Generation: g.Generation, RebalanceRequired: true}, nil
	}
	return &protocol.GroupHeartbeatResponse{OK: true, Generation: g.Generation, RebalanceRequired: false}, nil
}

// Leave removes a member from a group.
func (c *Coordinator) Leave(ctx context.Context, group, memberID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[group]
	if !ok {
		return nil
	}
	if _, exists := g.Members[memberID]; exists {
		delete(g.Members, memberID)
		if len(g.Members) == 0 {
			delete(c.groups, group)
			_ = c.meta.DeleteGroup(ctx, group)
			return nil
		}
		g.Generation++
		_ = c.meta.SetGroupGeneration(ctx, group, g.Generation)
		c.reassignLocked(g)
	}
	_ = c.meta.SetConsumerStatus(ctx, group, memberID, "down")
	return nil
}

// ForceRebalance bumps the generation (used by the rebalance op).
func (c *Coordinator) ForceRebalance(ctx context.Context, group string) (int32, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[group]
	if !ok {
		return 0, &protocol.StatusError{Status: protocol.GroupNotFound, Message: "group not found: " + group}
	}
	g.Generation++
	_ = c.meta.SetGroupGeneration(ctx, group, g.Generation)
	c.reassignLocked(g)
	return g.Generation, nil
}

// Describe returns the group's generation, members, and committed offsets.
func (c *Coordinator) Describe(ctx context.Context, group string) (*protocol.GroupDescribeResponse, error) {
	c.mu.Lock()
	g, ok := c.groups[group]
	if !ok {
		c.mu.Unlock()
		return nil, &protocol.StatusError{Status: protocol.GroupNotFound, Message: "group not found: " + group}
	}
	members := make([]protocol.GroupMember, 0, len(g.Members))
	ids := make([]string, 0, len(g.Members))
	for id := range g.Members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		members = append(members, protocol.GroupMember{MemberID: id, Assignment: g.Members[id].Assignment})
	}
	gen := g.Generation
	topic := g.Topic
	c.mu.Unlock()
	offsets, _ := c.meta.GetOffsets(ctx, group, topic)
	return &protocol.GroupDescribeResponse{Generation: gen, Members: members, Offsets: offsets}, nil
}

// Commit persists committed offsets.
func (c *Coordinator) Commit(ctx context.Context, group, topic string, offsets map[int32]int64) error {
	return c.meta.CommitOffsets(ctx, group, topic, offsets)
}

// GroupInfo is a snapshot of a group for the API.
type GroupInfo struct {
	Group      string
	Topic      string
	Generation int32
	Members    []protocol.GroupMember
	Offsets    map[int32]int64
}

// ListGroups returns a snapshot of all groups.
func (c *Coordinator) ListGroups(ctx context.Context) []GroupInfo {
	c.mu.Lock()
	out := make([]GroupInfo, 0, len(c.groups))
	for _, g := range c.groups {
		ids := make([]string, 0, len(g.Members))
		for id := range g.Members {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		members := make([]protocol.GroupMember, 0, len(ids))
		for _, id := range ids {
			members = append(members, protocol.GroupMember{MemberID: id, Assignment: g.Members[id].Assignment})
		}
		out = append(out, GroupInfo{Group: g.ID, Topic: g.Topic, Generation: g.Generation, Members: members})
	}
	c.mu.Unlock()
	for i := range out {
		out[i].Offsets, _ = c.meta.GetOffsets(ctx, out[i].Group, out[i].Topic)
	}
	return out
}

// GetGroup returns a snapshot of one group, or nil if absent.
func (c *Coordinator) GetGroup(ctx context.Context, group string) *GroupInfo {
	c.mu.Lock()
	g, ok := c.groups[group]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	ids := make([]string, 0, len(g.Members))
	for id := range g.Members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	members := make([]protocol.GroupMember, 0, len(ids))
	for _, id := range ids {
		members = append(members, protocol.GroupMember{MemberID: id, Assignment: g.Members[id].Assignment})
	}
	info := GroupInfo{Group: g.ID, Topic: g.Topic, Generation: g.Generation, Members: members}
	c.mu.Unlock()
	info.Offsets, _ = c.meta.GetOffsets(ctx, group, g.Topic)
	return &info
}

// expireSweep removes members that have not heartbeated within the timeout,
// triggering a rebalance for affected groups.
func (c *Coordinator) expireSweep() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for _, g := range c.groups {
		changed := false
		for id, m := range g.Members {
			if now.Sub(m.LastSeen) > c.expire {
				delete(g.Members, id)
				changed = true
			}
		}
		if changed {
			if len(g.Members) == 0 {
				delete(c.groups, g.ID)
				continue
			}
			g.Generation++
			c.reassignLocked(g)
			c.log.Info("groups: rebalance (expiry)", "group", g.ID, "gen", g.Generation)
		}
	}
}

// reassignLocked assigns partitions to members using the range strategy.
// Caller must hold c.mu.
func (c *Coordinator) reassignLocked(g *Group) {
	n, ok := c.partCount(g.Topic)
	if !ok {
		n = 0
	}
	// Members sorted by id (deterministic).
	ids := make([]string, 0, len(g.Members))
	for id := range g.Members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	// Range assignment: split n partitions evenly across len(ids) members,
	// remainder to the earliest ids.
	assign := rangeAssign(int(n), len(ids))
	for i, id := range ids {
		start := assign[i]
		end := assign[i+1]
		var parts []int32
		for p := start; p < end; p++ {
			parts = append(parts, int32(p))
		}
		g.Members[id].Assignment = parts
		_ = c.meta.SetConsumerAssignment(context.Background(), g.ID, id, parts)
	}
}

// rangeAssign returns n+1 cut points splitting n items into k contiguous
// ranges, with the remainder distributed to the earliest ranges.
func rangeAssign(n, k int) []int {
	if k <= 0 {
		return []int{0, n}
	}
	if n == 0 {
		cuts := make([]int, k+1)
		return cuts
	}
	base := n / k
	rem := n % k
	cuts := make([]int, k+1)
	pos := 0
	for i := 0; i < k; i++ {
		cuts[i] = pos
		size := base
		if i < rem {
			size++
		}
		pos += size
	}
	cuts[k] = pos
	return cuts
}

// isLeader reports whether the member is the first (leader) in the group.
func isLeader(g *Group, memberID string) bool {
	ids := make([]string, 0, len(g.Members))
	for id := range g.Members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return len(ids) > 0 && ids[0] == memberID
}
