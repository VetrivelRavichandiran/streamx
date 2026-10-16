package api

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/streamx/streamx/internal/meta"
	"github.com/streamx/streamx/internal/protocol"
)

// ---------------------------------------------------------------------------
// Topics
// ---------------------------------------------------------------------------

type retentionReq struct {
	MaxAgeMS int64 `json:"max_age_ms"`
	MaxBytes int64 `json:"max_bytes"`
}

type schemaFieldReq struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
}

// schemaReq accepts the schema as either a bare array of fields or the
// frontend's {fields: [...]} envelope.
type schemaReq struct {
	Fields []schemaFieldReq `json:"fields"`
}

func (s *schemaReq) fields() []schemaFieldReq {
	if s == nil {
		return nil
	}
	return s.Fields
}

type createTopicReq struct {
	Name              string        `json:"name"`
	Partitions        int32         `json:"partitions"`
	ReplicationFactor int32         `json:"replication_factor"`
	Retention         *retentionReq `json:"retention"`
	Schema            *schemaReq    `json:"schema"`
}

func (s *Server) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	var req createTopicReq
	if err := s.decodeBody(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "validation", "bad request: "+err.Error())
		return
	}
	if req.Name == "" {
		s.writeError(w, http.StatusBadRequest, "validation", "name required")
		return
	}
	if req.Partitions <= 0 || req.Partitions > 64 {
		s.writeError(w, http.StatusBadRequest, "validation", "partitions must be 1-64")
		return
	}
	if req.ReplicationFactor <= 0 || req.ReplicationFactor > 3 {
		s.writeError(w, http.StatusBadRequest, "validation", "replication_factor must be 1-3")
		return
	}
	if int(req.ReplicationFactor) > len(s.cfg.BrokerCluster) {
		s.writeError(w, http.StatusBadRequest, "validation",
			fmt.Sprintf("replication_factor %d exceeds cluster size %d", req.ReplicationFactor, len(s.cfg.BrokerCluster)))
		return
	}
	// Validate schema types.
	for _, f := range req.Schema.fields() {
		switch f.Type {
		case "string", "number", "boolean", "object", "array":
		default:
			s.writeError(w, http.StatusBadRequest, "validation", "invalid schema field type: "+f.Type)
			return
		}
	}
	t := &meta.Topic{
		Name:              req.Name,
		Partitions:        req.Partitions,
		ReplicationFactor: req.ReplicationFactor,
		Strategy:          "keyed",
	}
	if req.Retention != nil {
		t.Retention = meta.TopicRetention{MaxAgeMS: req.Retention.MaxAgeMS, MaxBytes: req.Retention.MaxBytes}
	} else {
		t.Retention = meta.TopicRetention{MaxAgeMS: 7 * 24 * 3600 * 1000, MaxBytes: 1 << 30}
	}
	if fields := req.Schema.fields(); len(fields) > 0 {
		t.SchemaEnabled = true
		for _, f := range fields {
			t.Schema = append(t.Schema, meta.SchemaField{Name: f.Name, Type: f.Type, Required: f.Required})
		}
	}
	if err := s.store.CreateTopic(r.Context(), t); err != nil {
		if err == meta.ErrTopicExists {
			s.writeError(w, http.StatusConflict, "conflict", "topic already exists: "+req.Name)
			return
		}
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	s.audit(r, "topic.create", req.Name, "")
	s.writeJSON(w, http.StatusCreated, t)
}

func (s *Server) handleListTopics(w http.ResponseWriter, r *http.Request) {
	topics, err := s.store.ListTopics(r.Context())
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	out := make([]map[string]any, 0, len(topics))
	for _, t := range topics {
		view := map[string]any{
			"name":               t.Name,
			"partitions":         t.Partitions,
			"replication_factor": t.ReplicationFactor,
			"retention":          t.Retention,
			"schema_enabled":     t.SchemaEnabled,
			"created_at":         t.CreatedAt,
		}
		// Per-topic event totals from broker metrics (best effort).
		md, err := s.client.Metadata(r.Context(), t.Name)
		if err == nil {
			var total int64
			for _, pm := range md.Partitions {
				total += pm.LogEndOffset
			}
			view["events_total"] = total
		}
		out = append(out, view)
	}
	s.writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleGetTopic(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	t, err := s.store.GetTopic(r.Context(), name)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "not_found", "topic not found: "+name)
		return
	}
	s.writeJSON(w, http.StatusOK, t)
}

type updateTopicReq struct {
	Retention *retentionReq `json:"retention"`
	Schema    *schemaReq    `json:"schema"`
}

func (s *Server) handleUpdateTopic(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	t, err := s.store.GetTopic(r.Context(), name)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "not_found", "topic not found: "+name)
		return
	}
	var req updateTopicReq
	if err := s.decodeBody(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "validation", "bad request: "+err.Error())
		return
	}
	if req.Retention != nil {
		t.Retention = meta.TopicRetention{MaxAgeMS: req.Retention.MaxAgeMS, MaxBytes: req.Retention.MaxBytes}
	}
	if req.Schema != nil {
		t.Schema = nil
		for _, f := range req.Schema.fields() {
			t.Schema = append(t.Schema, meta.SchemaField{Name: f.Name, Type: f.Type, Required: f.Required})
		}
		if len(t.Schema) > 0 {
			t.SchemaEnabled = true
		} else {
			t.SchemaEnabled = false
		}
	}
	t.UpdatedAt = time.Now()
	if err := s.store.UpdateTopic(r.Context(), t); err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	s.audit(r, "topic.update", name, "")
	s.writeJSON(w, http.StatusOK, t)
}

func (s *Server) handleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if _, err := s.store.GetTopic(r.Context(), name); err != nil {
		s.writeError(w, http.StatusNotFound, "not_found", "topic not found: "+name)
		return
	}
	// Tell every broker to drop its local copies (best effort).
	for id, addr := range s.cfg.BrokerCluster {
		s.callBrokerAdmin(r.Context(), id, addr, "/admin/delete-topic?topic="+name)
	}
	if err := s.store.DeleteTopic(r.Context(), name); err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	s.audit(r, "topic.delete", name, "")
	s.writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleTopicPartitions returns per-partition leader/replicas/HW/LEO/LSO.
func (s *Server) handleTopicPartitions(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	t, err := s.store.GetTopic(r.Context(), name)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "not_found", "topic not found: "+name)
		return
	}
	md, err := s.client.Metadata(r.Context(), name)
	if err != nil {
		s.writeError(w, http.StatusBadGateway, "internal", "cluster unreachable: "+err.Error())
		return
	}
	// Consumer lag per partition (max across groups consuming this topic).
	lag := map[int32]int64{}
	groups, _ := s.store.ListGroups(r.Context())
	for _, g := range groups {
		if g.Topic != name {
			continue
		}
		offsets, _ := s.store.GetOffsets(r.Context(), g.ID, name)
		for p, committed := range offsets {
			if l := pmLEO(md, p) - committed; l > lag[p] {
				lag[p] = l
			}
		}
	}
	out := make([]map[string]any, 0, len(md.Partitions))
	for _, pm := range md.Partitions {
		l := lag[pm.Partition]
		if l < 0 {
			l = 0
		}
		out = append(out, map[string]any{
			"partition":        pm.Partition,
			"leader":           pm.Leader,
			"replicas":         pm.Replicas,
			"high_watermark":   pm.HighWatermark,
			"log_end_offset":   pm.LogEndOffset,
			"log_start_offset": pm.LogStart,
			"rate_per_sec":     0.0,
			"consumer_lag":     l,
		})
	}
	_ = t
	s.writeJSON(w, http.StatusOK, out)
}

// handlePeekEvents reads records from a partition (offset + max, optional key).
func (s *Server) handlePeekEvents(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	pStr := r.PathValue("p")
	p, err := strconv.Atoi(pStr)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "validation", "bad partition")
		return
	}
	q := r.URL.Query()
	off := int64(0)
	if v := q.Get("offset"); v != "" {
		off, _ = strconv.ParseInt(v, 10, 64)
	}
	maxN := 100
	if v := q.Get("max"); v != "" {
		maxN, _ = strconv.Atoi(v)
		if maxN <= 0 || maxN > 500 {
			maxN = 500
		}
	}
	keyFilter := q.Get("key")
	if off < 0 {
		off = 0
	}
	fr, err := s.client.Fetch(r.Context(), name, int32(p), off, 1<<20, 0)
	if err != nil {
		s.writeError(w, http.StatusBadGateway, "internal", err.Error())
		return
	}
	out := make([]map[string]any, 0, maxN)
	for _, ev := range fr.Records {
		if keyFilter != "" && ev.Key != keyFilter {
			continue
		}
		out = append(out, eventView(ev, name, int32(p)))
		if len(out) >= maxN {
			break
		}
	}
	s.writeJSON(w, http.StatusOK, out)
}

// eventView shapes an event for the API.
func eventView(ev *protocol.Event, topic string, p int32) map[string]any {
	return map[string]any{
		"id":          ev.ID,
		"topic":       topic,
		"partition":   p,
		"offset":      ev.Offset,
		"key":         ev.Key,
		"payload":     ev.Payload, // base64 in JSON
		"headers":     ev.Headers,
		"ts":          ev.Ts,
		"producer_id": ev.Producer,
		"seq":         ev.Seq,
	}
}

// ---------------------------------------------------------------------------
// Produce
// ---------------------------------------------------------------------------

type produceEventReq struct {
	Key string `json:"key"`
	// Payload is a plain string in the REST API (see docs/API.md): the
	// dashboard and demo send the raw event body (e.g. a JSON object as a
	// string). It is converted to bytes before hitting the data plane.
	// (The internal wire protocol base64-encodes payloads; that is a
	// separate concern from this control-plane contract.)
	Payload string            `json:"payload"`
	Headers map[string]string `json:"headers"`
	Acks    string            `json:"acks"`
}

func (s *Server) handleProduceEvent(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var req produceEventReq
	if err := s.decodeBody(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "validation", "bad request: "+err.Error())
		return
	}
	if req.Acks == "" {
		req.Acks = "leader"
	}
	resp, err := s.client.ProduceOne(r.Context(), name, req.Key, []byte(req.Payload), req.Headers, req.Acks)
	if err != nil {
		s.produceError(w, err)
		return
	}
	if len(resp.Results) != 1 {
		s.writeError(w, http.StatusInternalServerError, "internal", "unexpected produce result")
		return
	}
	res := resp.Results[0]
	if res.Error != "" {
		s.writeError(w, http.StatusInternalServerError, "internal", res.Error)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"offset":    res.Offset,
		"partition": res.Partition,
		"ts":        time.Now().UnixMilli(),
	})
}

type produceBatchReq struct {
	Events []produceEventReq `json:"events"`
	Acks   string            `json:"acks"`
}

func (s *Server) handleProduceBatch(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var req produceBatchReq
	if err := s.decodeBody(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "validation", "bad request: "+err.Error())
		return
	}
	if req.Acks == "" {
		req.Acks = "leader"
	}
	if len(req.Events) == 0 {
		s.writeJSON(w, http.StatusOK, []map[string]any{})
		return
	}
	events := make([]*protocol.Event, 0, len(req.Events))
	for _, e := range req.Events {
		events = append(events, &protocol.Event{Key: e.Key, Payload: []byte(e.Payload), Headers: e.Headers})
	}
	resp, err := s.client.Produce(r.Context(), name, req.Acks, events)
	if err != nil {
		s.produceError(w, err)
		return
	}
	out := make([]map[string]any, 0, len(resp.Results))
	for _, res := range resp.Results {
		if res.Error != "" {
			s.writeError(w, http.StatusInternalServerError, "internal", res.Error)
			return
		}
		out = append(out, map[string]any{
			"offset":    res.Offset,
			"partition": res.Partition,
			"ts":        time.Now().UnixMilli(),
		})
	}
	s.writeJSON(w, http.StatusOK, out)
}

// produceError maps a produce error to an HTTP response.
func (s *Server) produceError(w http.ResponseWriter, err error) {
	if se, ok := err.(*protocol.StatusError); ok {
		switch se.Status {
		case protocol.TopicNotFound:
			s.writeError(w, http.StatusNotFound, "not_found", "topic not found")
			return
		case protocol.RateLimited:
			s.writeError(w, http.StatusTooManyRequests, "rate_limited", "produce rate limited")
			return
		case protocol.Backpressure:
			s.writeError(w, http.StatusTooManyRequests, "rate_limited", "backpressure")
			return
		}
	}
	s.writeError(w, http.StatusBadGateway, "internal", err.Error())
}

// callBrokerAdmin posts to a broker's admin endpoint (best effort).
func (s *Server) callBrokerAdmin(ctx context.Context, brokerID, addr, path string) {
	url := "http://" + addr + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return
	}
	resp, err := brokerHTTP.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// pmLEO returns the log-end offset for a partition from metadata.
func pmLEO(md *protocol.MetadataResponse, p int32) int64 {
	for _, pm := range md.Partitions {
		if pm.Partition == p {
			return pm.LogEndOffset
		}
	}
	return 0
}
