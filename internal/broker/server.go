package broker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/streamx/streamx/internal/protocol"
)

// Server is the broker's TCP data-plane server.
type Server struct {
	b        *Broker
	listener net.Listener
	metricsL net.Listener
	httpSrv  *http.Server
	conns    int64
	mu       sync.Mutex
	serveWg  sync.WaitGroup
}

// NewServer creates the TCP + metrics servers (not yet listening).
func NewServer(b *Broker) *Server {
	return &Server{b: b}
}

// Listen starts both the data-plane and metrics servers.
func (s *Server) Listen(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.b.cfg.Addr)
	if err != nil {
		return fmt.Errorf("broker: listen data plane: %w", err)
	}
	s.listener = ln
	mux := promhttp.HandlerFor(s.b.Metrics(), promhttp.HandlerOpts{})
	mln, err := net.Listen("tcp", s.b.cfg.MetricsAddr)
	if err != nil {
		ln.Close()
		return fmt.Errorf("broker: listen metrics: %w", err)
	}
	s.metricsL = mln
	s.serveWg.Add(2)
	go s.serveData(ctx, ln)
	go s.serveHTTP(ctx, mln, mux)
	return nil
}

// Close shuts down both servers.
func (s *Server) Close() {
	if s.listener != nil {
		s.listener.Close()
	}
	if s.metricsL != nil {
		s.metricsL.Close()
	}
	if s.httpSrv != nil {
		_ = s.httpSrv.Close()
	}
	s.serveWg.Wait()
}

func (s *Server) serveHTTP(ctx context.Context, ln net.Listener, h http.Handler) {
	go func() {
		defer s.serveWg.Done()
		srv := newHTTPServer(s.b, h)
		s.mu.Lock()
		s.httpSrv = srv
		s.mu.Unlock()
		if err := srv.Serve(ln); err != nil && ctx.Err() == nil {
			s.b.log.Warn("broker: metrics server", "err", err)
		}
	}()
}

func (s *Server) serveData(ctx context.Context, ln net.Listener) {
	defer s.serveWg.Done()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		s.mu.Lock()
		s.conns++
		s.mu.Unlock()
		s.b.metrics.Connections.WithLabelValues(s.b.cfg.BrokerID).Inc()
		go s.handleConn(ctx, conn)
	}
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer func() {
		conn.Close()
		s.mu.Lock()
		s.conns--
		s.mu.Unlock()
		s.b.metrics.Connections.WithLabelValues(s.b.cfg.BrokerID).Dec()
	}()
	r := bufio.NewReader(conn)
	for {
		if ctx.Err() != nil {
			return
		}
		req := &protocol.Request{}
		if err := protocol.ReadFrame(r, req); err != nil {
			return
		}
		resp := s.dispatch(ctx, req)
		if err := protocol.WriteFrame(conn, resp); err != nil {
			return
		}
	}
}

// dispatch handles a single request and returns a response.
func (s *Server) dispatch(ctx context.Context, req *protocol.Request) *protocol.Response {
	switch req.Op {
	case protocol.OpPing:
		return protocol.NewResponseOK(req, protocol.PingBody{Ts: time.Now().UnixMilli()})
	case protocol.OpMetadata:
		return s.handleMetadata(req)
	case protocol.OpProduce:
		return s.handleProduce(ctx, req)
	case protocol.OpFetch:
		return s.handleFetch(ctx, req)
	case protocol.OpOffsets:
		return s.handleOffsets(req)
	case protocol.OpGroupJoin:
		return s.handleGroupJoin(ctx, req)
	case protocol.OpGroupHeart:
		return s.handleGroupHeartbeat(ctx, req)
	case protocol.OpGroupLeave:
		return s.handleGroupLeave(ctx, req)
	case protocol.OpGroupCommit:
		return s.handleGroupCommit(ctx, req)
	case protocol.OpGroupDescr:
		return s.handleGroupDescribe(ctx, req)
	case protocol.OpRebalance:
		return s.handleRebalance(ctx, req)
	case protocol.OpClusterHeart:
		return s.handleClusterHeartbeat(req)
	default:
		return protocol.NewError(req, protocol.UnknownOp, "unknown op "+req.Op)
	}
}

// handleMetadata returns the topic's partition layout.
func (s *Server) handleMetadata(req *protocol.Request) *protocol.Response {
	var body protocol.MetadataRequest
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &body)
	}
	topic := body.Topic
	if topic == "" {
		topic = req.Topic
	}
	t, err := s.b.meta.GetTopic(ctx0(), topic)
	if err != nil {
		return protocol.NewError(req, protocol.TopicNotFound, "topic not found: "+topic)
	}
	resp := protocol.MetadataResponse{Topic: topic, Partitions: make([]protocol.PartitionMeta, 0, t.Partitions)}
	for p := int32(0); p < t.Partitions; p++ {
		replicas := s.b.replicasFor(topic, p)
		pm := protocol.PartitionMeta{
			Partition: p,
			Leader:    s.b.cluster.LeaderFor(replicas),
			Replicas:  replicas,
		}
		if part := s.b.getPartition(topic, p); part != nil {
			pm.HighWatermark = part.HW()
			pm.LogEndOffset = part.LEO()
			pm.LogStart = part.LSO()
		}
		resp.Partitions = append(resp.Partitions, pm)
	}
	return protocol.NewResponseOK(req, resp)
}

// replicasFor returns the deterministic replica set for a partition.
func (b *Broker) replicasFor(topic string, p int32) []string {
	t, err := b.meta.GetTopic(ctx0(), topic)
	if err != nil {
		return []string{b.cfg.BrokerID}
	}
	sorted := configSortedCluster(b.cfg.Cluster)
	return assignReplicas(sorted, p, t.ReplicationFactor)
}

// handleProduce appends events to their partitions and acks per the acks mode.
func (s *Server) handleProduce(ctx context.Context, req *protocol.Request) *protocol.Response {
	produceStart := time.Now()
	var body protocol.ProduceRequest
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return protocol.NewError(req, protocol.ValidationFailed, "bad produce body: "+err.Error())
	}
	if body.Acks == "" {
		body.Acks = "leader"
	}
	if body.Acks != "none" && body.Acks != "leader" && body.Acks != "all" {
		return protocol.NewError(req, protocol.ValidationFailed, "invalid acks: "+body.Acks)
	}
	if _, err := s.b.meta.GetTopic(ctx0(), req.Topic); err != nil {
		return protocol.NewError(req, protocol.TopicNotFound, "topic not found: "+req.Topic)
	}
	// Enforce produce rate limits (per producer + per topic).
	producer := ""
	if len(body.Events) > 0 {
		producer = body.Events[0].Producer
	}
	if ok, retry := s.b.CheckProduceLimits(producer, req.Topic, len(body.Events)); !ok {
		s.b.metrics.RateLimited.WithLabelValues("produce").Inc()
		return protocol.NewError(req, protocol.RateLimited, fmt.Sprintf("rate limited (retry_after_ms=%d)", retry))
	}
	// Simulated slow disk (admin failure injection).
	if d := s.b.slowProduceMS.Load(); d > 0 {
		time.Sleep(time.Duration(d) * time.Millisecond)
	}
	// Group events by partition.
	byPart := map[int32][]*protocol.Event{}
	for _, ev := range body.Events {
		byPart[ev.Partition] = append(byPart[ev.Partition], ev)
	}
	results := make([]protocol.ProduceResult, 0, len(body.Events))
	// Process each partition in order.
	parts := make([]int32, 0, len(byPart))
	for p := range byPart {
		parts = append(parts, p)
	}
	sortInt32s(parts)
	for _, p := range parts {
		part := s.b.getPartition(req.Topic, p)
		if part == nil {
			for range byPart[p] {
				results = append(results, protocol.ProduceResult{Partition: p, Error: "partition not found"})
			}
			continue
		}
		if !s.b.isLeader(part) {
			leader := s.b.cluster.LeaderFor(part.Replicas)
			for range byPart[p] {
				results = append(results, protocol.ProduceResult{Partition: p, Error: "not leader (leader=" + leader + ")"})
			}
			continue
		}
		for _, ev := range byPart[p] {
			off, err := part.Append(storageRecord(ev))
			if err != nil {
				results = append(results, protocol.ProduceResult{Partition: p, Error: err.Error()})
				continue
			}
			ev.Offset = off
			if body.Acks == "all" {
				if !s.b.waitForReplicas(ctx, part, off) {
					results = append(results, protocol.ProduceResult{Partition: p, Offset: off, Error: "not_enough_replicas"})
					continue
				}
			}
			// Single-replica partitions have no replication lag, so make the
			// record visible (HW = LEO) immediately instead of waiting for the
			// next replication tick.
			if len(part.Replicas) <= 1 {
				part.SetHW(part.LEO())
			}
			s.b.metrics.EventsProduced.WithLabelValues(req.Topic, strconv.Itoa(int(p)), s.b.cfg.BrokerID).Inc()
			s.b.metrics.BytesProduced.WithLabelValues(req.Topic).Add(float64(len(ev.Payload)))
			results = append(results, protocol.ProduceResult{Partition: p, Offset: off})
		}
	}
	resp := protocol.ProduceResponse{Leader: s.b.cfg.BrokerID, Results: results}
	// Record produce latency + request count.
	s.b.metrics.ProduceLatency.WithLabelValues(req.Topic).Observe(time.Since(produceStart).Seconds())
	s.b.metrics.ProduceRequests.WithLabelValues(req.Topic, strconv.Itoa(int(req.Partition)), body.Acks).Inc()
	return protocol.NewResponseOK(req, resp)
}

// waitForReplicas waits (up to acksAllTimeout) for all live replicas to have
// LEO > offset.
func (b *Broker) waitForReplicas(ctx context.Context, p *Partition, offset int64) bool {
	deadline := time.Now().Add(b.acksAllTimeout)
	for {
		ok := true
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
					if ps.LEO <= offset {
						ok = false
						break
					}
				} else {
					ok = false
					break
				}
			} else {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// handleFetch reads committed records up to HW.
func (s *Server) handleFetch(ctx context.Context, req *protocol.Request) *protocol.Response {
	var body protocol.FetchRequest
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &body)
	}
	part := s.b.getPartition(req.Topic, req.Partition)
	if part == nil {
		return protocol.NewError(req, protocol.PartitionNotFound, "partition not found")
	}
	if body.MaxBytes <= 0 {
		body.MaxBytes = 1 << 20
	}
	var start int64
	switch {
	case body.Offset == nil:
		start = part.HW() // absent → from high watermark (latest)
	case *body.Offset == -1:
		start = part.LSO() // -1 → earliest
	default:
		start = *body.Offset
	}
	deadline := time.Now().Add(time.Duration(body.MaxWait) * time.Millisecond)
	for {
		if start < part.LSO() {
			start = part.LSO()
		}
		hw := part.HW()
		if start < hw {
			recs, err := part.ReadAt(start, body.MaxBytes)
			if err != nil {
				return protocol.NewError(req, protocol.Internal, err.Error())
			}
			// Only return committed records (offset < HW).
			out := make([]*protocol.Event, 0, len(recs))
			for _, r := range recs {
				if r.Offset >= hw {
					break
				}
				out = append(out, eventFromStored(r))
			}
			s.b.metrics.FetchRequests.WithLabelValues(req.Topic, strconv.Itoa(int(req.Partition))).Inc()
			if len(out) > 0 {
				s.b.metrics.EventsConsumed.WithLabelValues(req.Topic, strconv.Itoa(int(req.Partition))).Add(float64(len(out)))
			}
			return protocol.NewResponseOK(req, protocol.FetchResponse{
				Records: out, HighWatermark: hw, LogStartOffset: part.LSO(),
			})
		}
		// No committed records yet.
		if body.MaxWait > 0 && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		return protocol.NewResponseOK(req, protocol.FetchResponse{
			Records: nil, HighWatermark: hw, LogStartOffset: part.LSO(),
		})
	}
}

// handleOffsets returns earliest (LSO) or latest (LEO) offset.
func (s *Server) handleOffsets(req *protocol.Request) *protocol.Response {
	var body protocol.OffsetsRequest
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &body)
	}
	part := s.b.getPartition(req.Topic, req.Partition)
	if part == nil {
		return protocol.NewError(req, protocol.PartitionNotFound, "partition not found")
	}
	off := part.LSO()
	if body.Which == "latest" {
		off = part.LEO()
	}
	return protocol.NewResponseOK(req, protocol.OffsetsResponse{Offset: off})
}

// handleClusterHeartbeat applies the peer's state and returns ours.
func (s *Server) handleClusterHeartbeat(req *protocol.Request) *protocol.Response {
	var hb protocol.ClusterHeartbeat
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &hb)
	}
	if hb.BrokerID != "" {
		s.b.cluster.ApplyHeartbeat(hb)
	}
	return protocol.NewResponseOK(req, s.b.state())
}

// ---- group ops (forward to the coordinator if we are one) ----

func (s *Server) handleGroupJoin(ctx context.Context, req *protocol.Request) *protocol.Response {
	if !s.b.IsCoordinator() {
		return protocol.NewError(req, protocol.NotLeader, "not the group coordinator")
	}
	var body protocol.GroupJoinRequest
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return protocol.NewError(req, protocol.ValidationFailed, "bad body")
	}
	topic := body.Topic
	if topic == "" {
		topic = s.topicForGroup(body.Group)
	}
	resp, err := s.b.coordin.Join(ctx, body.Group, body.MemberID, topic)
	if err != nil {
		return statusResponse(req, err)
	}
	return protocol.NewResponseOK(req, resp)
}

func (s *Server) handleGroupHeartbeat(ctx context.Context, req *protocol.Request) *protocol.Response {
	if !s.b.IsCoordinator() {
		return protocol.NewError(req, protocol.NotLeader, "not the group coordinator")
	}
	var body protocol.GroupHeartbeatRequest
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return protocol.NewError(req, protocol.ValidationFailed, "bad body")
	}
	resp, err := s.b.coordin.Heartbeat(ctx, body.Group, body.MemberID, body.Generation)
	if err != nil {
		return statusResponse(req, err)
	}
	return protocol.NewResponseOK(req, resp)
}

func (s *Server) handleGroupLeave(ctx context.Context, req *protocol.Request) *protocol.Response {
	if !s.b.IsCoordinator() {
		return protocol.NewError(req, protocol.NotLeader, "not the group coordinator")
	}
	var body protocol.GroupLeaveRequest
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return protocol.NewError(req, protocol.ValidationFailed, "bad body")
	}
	if err := s.b.coordin.Leave(ctx, body.Group, body.MemberID); err != nil {
		return statusResponse(req, err)
	}
	return protocol.NewResponseOK(req, map[string]bool{"ok": true})
}

func (s *Server) handleGroupCommit(ctx context.Context, req *protocol.Request) *protocol.Response {
	if !s.b.IsCoordinator() {
		return protocol.NewError(req, protocol.NotLeader, "not the group coordinator")
	}
	var body protocol.GroupCommitRequest
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return protocol.NewError(req, protocol.ValidationFailed, "bad body")
	}
	topic := s.topicForGroup(body.Group)
	if err := s.b.coordin.Commit(ctx, body.Group, topic, body.Offsets); err != nil {
		return statusResponse(req, err)
	}
	return protocol.NewResponseOK(req, map[string]bool{"ok": true})
}

func (s *Server) handleGroupDescribe(ctx context.Context, req *protocol.Request) *protocol.Response {
	if !s.b.IsCoordinator() {
		return protocol.NewError(req, protocol.NotLeader, "not the group coordinator")
	}
	var body struct {
		Group string `json:"group"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return protocol.NewError(req, protocol.ValidationFailed, "bad body")
	}
	resp, err := s.b.coordin.Describe(ctx, body.Group)
	if err != nil {
		return statusResponse(req, err)
	}
	return protocol.NewResponseOK(req, resp)
}

func (s *Server) handleRebalance(ctx context.Context, req *protocol.Request) *protocol.Response {
	if !s.b.IsCoordinator() {
		return protocol.NewError(req, protocol.NotLeader, "not the group coordinator")
	}
	var body protocol.RebalanceRequest
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return protocol.NewError(req, protocol.ValidationFailed, "bad body")
	}
	gen, err := s.b.coordin.ForceRebalance(ctx, body.Group)
	if err != nil {
		return statusResponse(req, err)
	}
	return protocol.NewResponseOK(req, protocol.RebalanceResponse{Generation: gen})
}

// topicForGroup returns the topic a group consumes (from the meta store).
func (s *Server) topicForGroup(group string) string {
	if g := s.b.coordin.GetGroup(ctx0(), group); g != nil {
		return g.Topic
	}
	if g, err := s.b.meta.GetGroup(ctx0(), group); err == nil {
		return g.Topic
	}
	return ""
}

// statusResponse converts a *protocol.StatusError into a response.
func statusResponse(req *protocol.Request, err error) *protocol.Response {
	if se, ok := err.(*protocol.StatusError); ok {
		return protocol.NewError(req, se.Status, se.Message)
	}
	return protocol.NewError(req, protocol.Internal, err.Error())
}

// ctx0 is a background context for quick lookups.
func ctx0() context.Context { return context.Background() }

func sortInt32s(s []int32) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
