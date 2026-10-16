package api

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/streamx/streamx/internal/auth"
	"github.com/streamx/streamx/internal/meta"
)

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

type loginReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	// Per-IP login rate limit (brute-force protection).
	if !s.loginLimit.Allow(clientIP(r)) {
		s.writeError(w, http.StatusTooManyRequests, "rate_limited", "too many login attempts")
		return
	}
	var req loginReq
	if err := s.decodeBody(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "validation", "bad request: "+err.Error())
		return
	}
	u, token, err := s.auth.Login(r.Context(), req.Username, req.Password)
	if err != nil {
		s.writeError(w, http.StatusUnauthorized, "auth", "invalid credentials")
		return
	}
	s.audit(r, "auth.login", req.Username, "ip="+clientIP(r))
	s.writeJSON(w, http.StatusOK, map[string]any{
		"token": token,
		"role":  u.Role,
		"user":  map[string]any{"id": u.ID, "username": u.Username, "org_id": u.OrgID},
	})
}

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	u, _ := r.Context().Value(ctxUserKey).(*meta.User)
	role, _ := r.Context().Value(ctxRoleKey).(string)
	if u == nil {
		s.writeError(w, http.StatusUnauthorized, "auth", "no user in context")
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"user": map[string]any{"id": u.ID, "username": u.Username, "org_id": u.OrgID},
		"role": role,
	})
}

// ---------------------------------------------------------------------------
// Admin: users
// ---------------------------------------------------------------------------

func (s *Server) handleListUsers(w http.ResponseWriter, r *http.Request) {
	users, err := s.store.ListUsers(r.Context())
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	out := make([]map[string]any, 0, len(users))
	for _, u := range users {
		out = append(out, map[string]any{
			"id":         u.ID,
			"username":   u.Username,
			"org_id":     u.OrgID,
			"role":       u.Role,
			"created_at": u.CreatedAt.UnixMilli(),
		})
	}
	s.writeJSON(w, http.StatusOK, out)
}

type createUserReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Role     string `json:"role"`
}

func (s *Server) handleCreateUser(w http.ResponseWriter, r *http.Request) {
	var req createUserReq
	if err := s.decodeBody(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "validation", "bad request: "+err.Error())
		return
	}
	if req.Username == "" || req.Password == "" {
		s.writeError(w, http.StatusBadRequest, "validation", "username and password required")
		return
	}
	if req.Role != auth.RoleAdmin && req.Role != auth.RoleOperator && req.Role != auth.RoleViewer {
		s.writeError(w, http.StatusBadRequest, "validation", "role must be admin, operator, or viewer")
		return
	}
	u := &meta.User{
		ID:       "u-" + randomHex(4),
		OrgID:    "org-1",
		Username: req.Username,
		Role:     req.Role,
	}
	hash, err := bcryptHash(req.Password)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	u.Password = hash
	if err := s.store.CreateUser(r.Context(), u); err != nil {
		if err == meta.ErrUserExists {
			s.writeError(w, http.StatusConflict, "conflict", "user already exists: "+req.Username)
			return
		}
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	s.audit(r, "user.create", req.Username, "role="+req.Role)
	s.writeJSON(w, http.StatusCreated, map[string]any{
		"id":         u.ID,
		"username":   u.Username,
		"org_id":     u.OrgID,
		"role":       u.Role,
		"created_at": time.Now().UnixMilli(),
	})
}

// ---------------------------------------------------------------------------
// Admin: audit
// ---------------------------------------------------------------------------

func (s *Server) handleAudit(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}
	entries, err := s.store.ListAudit(r.Context(), limit)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	out := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		out = append(out, s.auditView(r.Context(), e))
	}
	s.writeJSON(w, http.StatusOK, out)
}

// ---------------------------------------------------------------------------
// Admin: failure injection
// ---------------------------------------------------------------------------

// failureRecord is an in-memory failure-injection event (newest first).
type failureRecord struct {
	ID         string `json:"id"`
	Ts         int64  `json:"ts"`
	Kind       string `json:"kind"`
	Target     string `json:"target"`
	Action     string `json:"action"`
	Detail     string `json:"detail"`
	Status     string `json:"status"`
	BrokerID   string `json:"broker_id,omitempty"`
	ConsumerID string `json:"consumer_id,omitempty"`
	DelayMS    int    `json:"delay_ms,omitempty"`
	DurationMS int    `json:"duration_ms,omitempty"`
}

var (
	failMu   sync.Mutex
	failures []failureRecord
	failSeq  int64
)

// recordFailure prepends a failure event to the in-memory history.
func (s *Server) recordFailure(f failureRecord) {
	failMu.Lock()
	failSeq++
	f.ID = "f-" + strconv.FormatInt(failSeq, 10)
	f.Ts = time.Now().UnixMilli()
	failures = append([]failureRecord{f}, failures...)
	if len(failures) > 200 {
		failures = failures[:200]
	}
	failMu.Unlock()
}

func (s *Server) handleFailureBroker(w http.ResponseWriter, r *http.Request) {
	var req struct {
		BrokerID string `json:"broker_id"`
		Action   string `json:"action"`
	}
	if err := s.decodeBody(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "validation", "bad request: "+err.Error())
		return
	}
	if req.BrokerID == "" {
		s.writeError(w, http.StatusBadRequest, "validation", "broker_id required")
		return
	}
	addr, ok := s.cfg.BrokerCluster[req.BrokerID]
	if !ok {
		s.writeError(w, http.StatusNotFound, "not_found", "unknown broker: "+req.BrokerID)
		return
	}
	status := "accepted"
	switch req.Action {
	case "stop":
		s.callBrokerAdmin(r.Context(), req.BrokerID, addr, "/admin/stop")
	case "restart":
		// Graceful stop; the orchestrator (docker-compose / make dev) is
		// expected to relaunch the process.
		s.callBrokerAdmin(r.Context(), req.BrokerID, addr, "/admin/stop")
		status = "stop_sent"
	case "kill":
		// SIGKILL is a host-level operation the API cannot perform on its own;
		// record the intent so the demo/ops tooling can act on it.
		status = "recorded"
	default:
		s.writeError(w, http.StatusBadRequest, "validation", "action must be stop, restart, or kill")
		return
	}
	s.audit(r, "failure.broker", req.BrokerID, "action="+req.Action)
	s.recordFailure(failureRecord{Kind: "broker", Target: req.BrokerID, BrokerID: req.BrokerID, Action: req.Action, Status: status, Detail: "broker failure injection"})
	s.writeJSON(w, http.StatusOK, map[string]any{"ok": true, "status": status})
}

func (s *Server) handleFailureConsumer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ConsumerID string `json:"consumer_id"`
		Action     string `json:"action"`
	}
	if err := s.decodeBody(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "validation", "bad request: "+err.Error())
		return
	}
	if req.ConsumerID == "" {
		s.writeError(w, http.StatusBadRequest, "validation", "consumer_id required")
		return
	}
	// Find the group that owns this member.
	groups, _ := s.store.ListGroups(r.Context())
	group := ""
	for _, g := range groups {
		consumers, _ := s.store.ListConsumers(r.Context(), g.ID)
		for _, c := range consumers {
			if c.ID == req.ConsumerID {
				group = g.ID
				break
			}
		}
		if group != "" {
			break
		}
	}
	status := "accepted"
	switch req.Action {
	case "stop", "restart":
		// Mark the consumer down; the coordinator's expiry sweep will rebalance.
		if group != "" {
			_ = s.store.SetConsumerStatus(r.Context(), group, req.ConsumerID, "down")
		}
		status = "marked_down"
	case "slow", "pause":
		// Latency/pause is applied by the worker process itself; record intent.
		status = "recorded"
	default:
		s.writeError(w, http.StatusBadRequest, "validation", "action must be stop, restart, slow, or pause")
		return
	}
	s.audit(r, "failure.consumer", req.ConsumerID, "action="+req.Action+" group="+group)
	s.recordFailure(failureRecord{Kind: "consumer", Target: req.ConsumerID, ConsumerID: req.ConsumerID, Action: req.Action, Status: status, Detail: "consumer failure injection"})
	s.writeJSON(w, http.StatusOK, map[string]any{"ok": true, "status": status})
}

func (s *Server) handleFailureNetwork(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Target     string `json:"target"`
		DelayMS    int    `json:"delay_ms"`
		DurationMS int    `json:"duration_ms"`
	}
	if err := s.decodeBody(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "validation", "bad request: "+err.Error())
		return
	}
	if req.Target == "" {
		s.writeError(w, http.StatusBadRequest, "validation", "target required")
		return
	}
	// True network fault injection requires host-level tooling (tc/iptables);
	// record the intent and surface it in the failure history.
	s.audit(r, "failure.network", req.Target, "delay_ms="+strconv.Itoa(req.DelayMS)+" duration_ms="+strconv.Itoa(req.DurationMS))
	s.recordFailure(failureRecord{Kind: "network", Target: req.Target, Action: "delay", Status: "recorded", DelayMS: req.DelayMS, DurationMS: req.DurationMS, Detail: "network delay injection"})
	s.writeJSON(w, http.StatusOK, map[string]any{"ok": true, "status": "recorded"})
}

func (s *Server) handleFailureHistory(w http.ResponseWriter, r *http.Request) {
	failMu.Lock()
	out := make([]map[string]any, 0, len(failures))
	for _, f := range failures {
		out = append(out, map[string]any{
			"id":          f.ID,
			"ts":          f.Ts,
			"kind":        f.Kind,
			"target":      f.Target,
			"action":      f.Action,
			"detail":      f.Detail,
			"status":      f.Status,
			"broker_id":   f.BrokerID,
			"consumer_id": f.ConsumerID,
			"delay_ms":    f.DelayMS,
			"duration_ms": f.DurationMS,
		})
	}
	failMu.Unlock()
	s.writeJSON(w, http.StatusOK, out)
}

// bcryptHash hashes a password with bcrypt (used when creating users).
func bcryptHash(password string) (string, error) {
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(h), nil
}
