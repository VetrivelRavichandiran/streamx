package api

import (
	"net/http"
	"strconv"

	"github.com/streamx/streamx/internal/meta"
)

// ---------------------------------------------------------------------------
// DLQ
// ---------------------------------------------------------------------------

func (s *Server) handleListDLQ(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	topic := q.Get("topic")
	group := q.Get("group")
	limit := 100
	if v := q.Get("limit"); v != "" {
		limit, _ = strconv.Atoi(v)
		if limit <= 0 || limit > 500 {
			limit = 500
		}
	}
	entries, err := s.store.ListDLQ(r.Context(), topic, group, limit)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	out := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		out = append(out, dlqView(e))
	}
	s.writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleGetDLQ(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	e, err := s.store.GetDLQ(r.Context(), id)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "not_found", "dlq entry not found: "+id)
		return
	}
	s.writeJSON(w, http.StatusOK, dlqView(e))
}

func dlqView(e *meta.DLQEntry) map[string]any {
	return map[string]any{
		"id":          e.ID,
		"topic":       e.Topic,
		"group":       e.Group,
		"partition":   e.OriginalPart,
		"offset":      e.OriginalOffset,
		"key":         e.Key,
		"payload":     e.Payload, // base64 in JSON
		"retry_count": e.RetryCount,
		"reason":      e.Reason,
		"status":      e.Status,
		"ts":          e.CreatedAt.UnixMilli(),
	}
}

// handleReplayDLQ republishes a DLQ entry to its original topic and marks it
// replayed.
func (s *Server) handleReplayDLQ(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	e, err := s.store.GetDLQ(r.Context(), id)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "not_found", "dlq entry not found: "+id)
		return
	}
	if e.Status != "pending" {
		s.writeError(w, http.StatusConflict, "conflict", "entry already "+e.Status)
		return
	}
	// Republish to the original topic (best effort).
	_, _ = s.client.ProduceOne(r.Context(), e.OriginalTopic, e.Key, e.Payload, nil, "leader")
	if err := s.store.SetDLQStatus(r.Context(), id, "replayed"); err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	s.audit(r, "dlq.replay", id, "topic="+e.OriginalTopic)
	s.writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": "replayed"})
}

// handleDropDLQ marks a DLQ entry dropped.
func (s *Server) handleDropDLQ(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	e, err := s.store.GetDLQ(r.Context(), id)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "not_found", "dlq entry not found: "+id)
		return
	}
	if e.Status != "pending" {
		s.writeError(w, http.StatusConflict, "conflict", "entry already "+e.Status)
		return
	}
	if err := s.store.SetDLQStatus(r.Context(), id, "dropped"); err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	s.audit(r, "dlq.drop", id, "")
	s.writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": "dropped"})
}
