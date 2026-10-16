// Package api implements the STREAMX control plane (CONTRACTS.md §3, API.md):
// REST + SSE over HTTP, bearer-token auth with roles, and a data-plane client
// (the streamx SDK) for produce/fetch/offsets against the brokers.
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/streamx/streamx/internal/auth"
	"github.com/streamx/streamx/internal/config"
	"github.com/streamx/streamx/internal/meta"
	"github.com/streamx/streamx/internal/metrics"
	"github.com/streamx/streamx/internal/security"
	"github.com/streamx/streamx/sdk/streamx"
)

// Server is the control-plane HTTP server.
type Server struct {
	cfg     config.APIConfig
	log     *slog.Logger
	store   meta.Store
	auth    *auth.Auth
	metrics *metrics.API
	client  *streamx.Client

	// rate limiters.
	apiLimit   *security.KeyedBucket // per token
	loginLimit *security.KeyedBucket // per IP

	hub        *sseHub // SSE event stream
	metricsHub *sseHub // SSE metrics stream
}

// New builds an API server from config.
func New(cfg config.APIConfig, log *slog.Logger, store meta.Store) (*Server, error) {
	if len(cfg.BrokerCluster) == 0 {
		return nil, fmt.Errorf("api: no broker cluster configured")
	}
	client, err := streamx.NewClient(streamx.Config{
		Brokers:        cfg.BrokerCluster,
		ProducerID:     "api-" + randomHex(3),
		RequestTimeout: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	s := &Server{
		cfg:        cfg,
		log:        log,
		store:      store,
		auth:       auth.New(store),
		metrics:    metrics.NewAPI(),
		client:     client,
		apiLimit:   security.NewKeyedBucket(float64(cfg.RateLimitRPS), cfg.RateLimitRPS),
		loginLimit: security.NewKeyedBucket(5, 5), // 5/min/IP
		hub:        newSSEHub(),
		metricsHub: newSSEHub(),
	}
	return s, nil
}

// Run bootstraps users, starts background loops, and serves HTTP until ctx is
// cancelled.
func (s *Server) Run(ctx context.Context) error {
	if err := s.auth.Bootstrap(ctx, s.cfg.AdminUser, s.cfg.AdminPassword, s.cfg.OperatorUser, s.cfg.OperatorPass); err != nil {
		return fmt.Errorf("api: bootstrap: %w", err)
	}
	go s.eventStreamPoller(ctx)
	go s.metricsStreamPoller(ctx)

	mux := s.routes()
	h := security.CORS(config.Get("STREAMX_CORS_ORIGIN", "http://localhost:3000"))(security.SecureHeaders(mux))
	srv := &http.Server{
		Addr:              s.cfg.Addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		s.log.Info("api: listening", "addr", s.cfg.Addr)
		errCh <- srv.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shCtx)
		s.hub.close()
		s.metricsHub.close()
		return nil
	case err := <-errCh:
		return err
	}
}

// routes wires the HTTP mux. Public routes are registered directly; /api/v1/*
// goes through the auth middleware.
func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	// Public.
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.metrics.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("POST /api/v1/auth/login", s.handleLogin)
	// Authenticated (role checked per route).
	mux.Handle("/api/v1/", s.authMiddleware(s.apiMux()))
	return mux
}

// apiMux registers the authenticated routes.
func (s *Server) apiMux() http.Handler {
	mux := http.NewServeMux()
	// Auth.
	mux.HandleFunc("GET /api/v1/auth/me", s.requireRole(auth.RoleViewer, s.handleMe))
	// Topics.
	mux.HandleFunc("POST /api/v1/topics", s.requireRole(auth.RoleOperator, s.handleCreateTopic))
	mux.HandleFunc("GET /api/v1/topics", s.requireRole(auth.RoleViewer, s.handleListTopics))
	mux.HandleFunc("GET /api/v1/topics/{name}", s.requireRole(auth.RoleViewer, s.handleGetTopic))
	mux.HandleFunc("PATCH /api/v1/topics/{name}", s.requireRole(auth.RoleOperator, s.handleUpdateTopic))
	mux.HandleFunc("DELETE /api/v1/topics/{name}", s.requireRole(auth.RoleOperator, s.handleDeleteTopic))
	mux.HandleFunc("GET /api/v1/topics/{name}/partitions", s.requireRole(auth.RoleViewer, s.handleTopicPartitions))
	mux.HandleFunc("GET /api/v1/topics/{name}/partitions/{p}/events", s.requireRole(auth.RoleViewer, s.handlePeekEvents))
	mux.HandleFunc("POST /api/v1/topics/{name}/events", s.requireRole(auth.RoleOperator, s.handleProduceEvent))
	mux.HandleFunc("POST /api/v1/topics/{name}/events/batch", s.requireRole(auth.RoleOperator, s.handleProduceBatch))
	// Consumer groups.
	mux.HandleFunc("GET /api/v1/consumer-groups", s.requireRole(auth.RoleViewer, s.handleListGroups))
	mux.HandleFunc("GET /api/v1/consumer-groups/{group}", s.requireRole(auth.RoleViewer, s.handleGetGroup))
	mux.HandleFunc("GET /api/v1/consumer-groups/{group}/offsets", s.requireRole(auth.RoleViewer, s.handleGroupOffsets))
	mux.HandleFunc("DELETE /api/v1/consumer-groups/{group}", s.requireRole(auth.RoleOperator, s.handleDeleteGroup))
	// Cluster & brokers.
	mux.HandleFunc("GET /api/v1/cluster", s.requireRole(auth.RoleViewer, s.handleCluster))
	mux.HandleFunc("GET /api/v1/brokers", s.requireRole(auth.RoleViewer, s.handleBrokers))
	mux.HandleFunc("GET /api/v1/brokers/{id}", s.requireRole(auth.RoleViewer, s.handleBroker))
	mux.HandleFunc("GET /api/v1/metrics/summary", s.requireRole(auth.RoleViewer, s.handleMetricsSummary))
	// DLQ.
	mux.HandleFunc("POST /api/v1/dlq", s.requireRole(auth.RoleOperator, s.handleInsertDLQ))
	mux.HandleFunc("GET /api/v1/dlq", s.requireRole(auth.RoleViewer, s.handleListDLQ))
	mux.HandleFunc("GET /api/v1/dlq/{id}", s.requireRole(auth.RoleViewer, s.handleGetDLQ))
	mux.HandleFunc("POST /api/v1/dlq/{id}/replay", s.requireRole(auth.RoleOperator, s.handleReplayDLQ))
	mux.HandleFunc("POST /api/v1/dlq/{id}/drop", s.requireRole(auth.RoleOperator, s.handleDropDLQ))
	// Benchmarks.
	mux.HandleFunc("POST /api/v1/benchmarks", s.requireRole(auth.RoleOperator, s.handleCreateBenchmark))
	mux.HandleFunc("POST /api/v1/benchmarks/register", s.requireRole(auth.RoleOperator, s.handleRegisterBenchmark))
	mux.HandleFunc("GET /api/v1/benchmarks", s.requireRole(auth.RoleViewer, s.handleListBenchmarks))
	mux.HandleFunc("GET /api/v1/benchmarks/compare", s.requireRole(auth.RoleViewer, s.handleCompareBenchmarks))
	mux.HandleFunc("GET /api/v1/benchmarks/{id}", s.requireRole(auth.RoleViewer, s.handleGetBenchmark))
	// Admin.
	mux.HandleFunc("POST /api/v1/admin/failures/broker", s.requireRole(auth.RoleAdmin, s.handleFailureBroker))
	mux.HandleFunc("POST /api/v1/admin/failures/consumer", s.requireRole(auth.RoleAdmin, s.handleFailureConsumer))
	mux.HandleFunc("POST /api/v1/admin/failures/network", s.requireRole(auth.RoleAdmin, s.handleFailureNetwork))
	mux.HandleFunc("GET /api/v1/admin/failures", s.requireRole(auth.RoleAdmin, s.handleFailureHistory))
	mux.HandleFunc("GET /api/v1/admin/users", s.requireRole(auth.RoleAdmin, s.handleListUsers))
	mux.HandleFunc("POST /api/v1/admin/users", s.requireRole(auth.RoleAdmin, s.handleCreateUser))
	mux.HandleFunc("GET /api/v1/admin/audit", s.requireRole(auth.RoleAdmin, s.handleAudit))
	// SSE.
	mux.HandleFunc("GET /api/v1/stream/events", s.requireRole(auth.RoleViewer, s.handleStreamEvents))
	mux.HandleFunc("GET /api/v1/stream/metrics", s.requireRole(auth.RoleViewer, s.handleStreamMetrics))
	return mux
}

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

// handleHealth is the public health endpoint (no auth). It reports the API's
// status and whether the meta store is reachable.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	// Probe the meta store with a cheap query.
	dbOK := true
	if _, err := s.store.ListTopics(r.Context()); err != nil {
		dbOK = false
	}
	status := "ok"
	code := http.StatusOK
	if !dbOK {
		status = "degraded"
		code = http.StatusServiceUnavailable
	}
	s.writeJSON(w, code, map[string]any{
		"status":  status,
		"db":      dbOK,
		"version": "streamx",
		"ts":      time.Now().UnixMilli(),
	})
}

// authMiddleware validates the bearer token (header or ?token=) and attaches
// the user + role to the request context. It also applies the per-token rate
// limit.
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := auth.ExtractToken(r)
		if token == "" {
			s.writeError(w, http.StatusUnauthorized, "auth", "missing token")
			return
		}
		if !s.apiLimit.Allow(token) {
			s.writeError(w, http.StatusTooManyRequests, "rate_limited", "rate limit exceeded")
			return
		}
		u, role, err := s.auth.Validate(r.Context(), token)
		if err != nil {
			s.writeError(w, http.StatusUnauthorized, "auth", "invalid or expired token")
			return
		}
		ctx := context.WithValue(r.Context(), ctxUserKey, u)
		ctx = context.WithValue(ctx, ctxRoleKey, role)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// requireRole wraps a handler with a minimum-role check.
func (s *Server) requireRole(min string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		role, _ := r.Context().Value(ctxRoleKey).(string)
		if !auth.HasRole(role, min) {
			s.writeError(w, http.StatusForbidden, "auth", "insufficient role")
			return
		}
		h(w, r)
	}
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

type ctxKey string

const (
	ctxUserKey ctxKey = "user"
	ctxRoleKey ctxKey = "role"
)

func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) writeError(w http.ResponseWriter, status int, code, msg string) {
	s.writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": msg}})
}

func (s *Server) decodeBody(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// randomHex returns n random bytes as hex.
func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// metricsAddrFor maps a broker's data-plane address (host:port) to its metrics
// address (host:metricsPort) using the standard port plan (data 9092-9094 →
// metrics 9102-9104).
func (s *Server) metricsAddrFor(dataAddr string) string {
	host, port, err := net.SplitHostPort(dataAddr)
	if err != nil {
		return ""
	}
	var p int
	if _, err := fmt.Sscanf(port, "%d", &p); err != nil {
		return ""
	}
	if p >= 9092 && p <= 9099 {
		return net.JoinHostPort(host, fmt.Sprintf("%d", 9100+(p-9092)))
	}
	// Fallback: +10 offset.
	return net.JoinHostPort(host, fmt.Sprintf("%d", p+10))
}

// brokerHTTP is a short-timeout client for broker admin/metrics calls.
var brokerHTTP = &http.Client{Timeout: 3 * time.Second}

// scrapeBroker fetches a broker's Prometheus metrics text.
func (s *Server) scrapeBroker(ctx context.Context, brokerID string) (string, error) {
	addr, ok := s.cfg.BrokerCluster[brokerID]
	if !ok {
		return "", fmt.Errorf("unknown broker %s", brokerID)
	}
	maddr := s.metricsAddrFor(addr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+maddr+"/metrics", nil)
	if err != nil {
		return "", err
	}
	resp, err := brokerHTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metrics status %d", resp.StatusCode)
	}
	var buf strings.Builder
	buf.Grow(64 << 10)
	_, _ = io.Copy(&buf, resp.Body)
	return buf.String(), nil
}
