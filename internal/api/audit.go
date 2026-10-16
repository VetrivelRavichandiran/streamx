package api

import (
	"context"
	"net/http"

	"github.com/streamx/streamx/internal/meta"
)

// audit records an audit-log entry for an authenticated request. It is best
// effort: a failure to write the audit row never fails the request.
func (s *Server) audit(r *http.Request, action, target, detail string) {
	u, _ := r.Context().Value(ctxUserKey).(*meta.User)
	userID := ""
	if u != nil {
		userID = u.ID
	}
	e := &meta.AuditEntry{
		ID:     "a-" + randomHex(4),
		UserID: userID,
		Action: action,
		Target: target,
		Detail: detail,
	}
	if err := s.store.InsertAudit(r.Context(), e); err != nil {
		s.log.Warn("api: audit insert", "action", action, "err", err)
	}
}

// auditView shapes an audit entry for the API, resolving the username.
func (s *Server) auditView(ctx context.Context, e *meta.AuditEntry) map[string]any {
	username := ""
	if u, err := s.store.GetUserByID(ctx, e.UserID); err == nil && u != nil {
		username = u.Username
	}
	return map[string]any{
		"id":       e.ID,
		"user_id":  e.UserID,
		"username": username,
		"action":   e.Action,
		"target":   e.Target,
		"detail":   e.Detail,
		"ts":       e.CreatedAt.UnixMilli(),
	}
}

// clientIP extracts the request's source IP (best effort, honors X-Forwarded-For).
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return xff
	}
	host := r.RemoteAddr
	if i := len(host) - 1; i > 0 && host[i-1] == ':' {
		return host[:i]
	}
	return host
}
