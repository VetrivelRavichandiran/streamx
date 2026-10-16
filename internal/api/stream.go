package api

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/streamx/streamx/internal/meta"
)

// ---------------------------------------------------------------------------
// SSE: live event stream
// ---------------------------------------------------------------------------

// eventStreamPoller tails every topic's partitions and broadcasts new events
// to the event hub. It keeps a per-partition cursor so each event is emitted
// once.
func (s *Server) eventStreamPoller(ctx context.Context) {
	var mu sync.Mutex
	cursors := map[string]int64{} // "topic/partition" -> next offset to read
	ticker := time.NewTicker(1500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.pollEventsOnce(ctx, cursors, &mu)
		}
	}
}

func (s *Server) pollEventsOnce(ctx context.Context, cursors map[string]int64, mu *sync.Mutex) {
	topics, err := s.store.ListTopics(ctx)
	if err != nil {
		return
	}
	for _, t := range topics {
		md, err := s.client.Metadata(ctx, t.Name)
		if err != nil {
			continue
		}
		for _, pm := range md.Partitions {
			key := t.Name + "/" + strconv.Itoa(int(pm.Partition))
			mu.Lock()
			cursor, seen := cursors[key]
			if !seen {
				// Start at the latest so we only stream new events.
				cursor = pm.LogEndOffset
			}
			mu.Unlock()
			if pm.LogEndOffset <= cursor {
				continue
			}
			fr, err := s.client.Fetch(ctx, t.Name, pm.Partition, cursor, 1<<20, 0)
			if err != nil {
				continue
			}
			if len(fr.Records) == 0 {
				continue
			}
			batch := make([]map[string]any, 0, len(fr.Records))
			next := cursor
			for _, ev := range fr.Records {
				batch = append(batch, eventView(ev, t.Name, pm.Partition))
				next = ev.Offset + 1
			}
			mu.Lock()
			cursors[key] = next
			mu.Unlock()
			s.hub.publish(jsonPayload(batch))
		}
	}
}

func (s *Server) handleStreamEvents(w http.ResponseWriter, r *http.Request) {
	ch, unsub := s.hub.subscribe()
	defer unsub()
	s.serveSSE(w, r, ch, unsub, 15*time.Second)
}

// ---------------------------------------------------------------------------
// SSE: metrics stream
// ---------------------------------------------------------------------------

// metricsStreamPoller emits a metrics snapshot to the metrics hub every 2 s.
func (s *Server) metricsStreamPoller(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snap, err := s.metricsSummary(ctx)
			if err != nil {
				continue
			}
			s.metricsHub.publish(jsonPayload(snap))
		}
	}
}

func (s *Server) handleStreamMetrics(w http.ResponseWriter, r *http.Request) {
	ch, unsub := s.metricsHub.subscribe()
	defer unsub()
	s.serveSSE(w, r, ch, unsub, 15*time.Second)
}

// ---------------------------------------------------------------------------
// DLQ: report endpoint (used by workers to record DLQ entries)
// ---------------------------------------------------------------------------

type dlqReportReq struct {
	Topic          string `json:"topic"`
	Group          string `json:"group"`
	OriginalTopic  string `json:"original_topic"`
	OriginalPart   int32  `json:"original_part"`
	OriginalOffset int64  `json:"original_offset"`
	Key            string `json:"key"`
	Payload        []byte `json:"payload"`
	Reason         string `json:"reason"`
	RetryCount     int    `json:"retry_count"`
	Status         string `json:"status"`
}

// handleInsertDLQ accepts a DLQ entry report from a worker and stores it.
func (s *Server) handleInsertDLQ(w http.ResponseWriter, r *http.Request) {
	var req dlqReportReq
	if err := s.decodeBody(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "validation", "bad request: "+err.Error())
		return
	}
	if req.Status == "" {
		req.Status = "pending"
	}
	e := &meta.DLQEntry{
		ID:             "dlq-" + randomHex(4),
		Topic:          req.Topic,
		Group:          req.Group,
		OriginalTopic:  req.OriginalTopic,
		OriginalPart:   req.OriginalPart,
		OriginalOffset: req.OriginalOffset,
		Key:            req.Key,
		Payload:        req.Payload,
		Reason:         req.Reason,
		RetryCount:     req.RetryCount,
		Status:         req.Status,
	}
	if err := s.store.InsertDLQ(r.Context(), e); err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	s.writeJSON(w, http.StatusCreated, map[string]any{"id": e.ID})
}
