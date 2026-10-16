// Package security provides rate limiting, secure headers, and CORS for the
// control plane (CONTRACTS.md §13, SECURITY.md).
package security

import (
	"net/http"
	"sync"
	"time"
)

// TokenBucket is a simple in-memory token-bucket rate limiter.
type TokenBucket struct {
	mu     sync.Mutex
	rate   float64 // tokens per second
	burst  float64 // bucket capacity
	tokens float64
	last   time.Time
}

// NewTokenBucket creates a bucket with the given rate (tokens/sec) and burst.
func NewTokenBucket(rate float64, burst int) *TokenBucket {
	return &TokenBucket{rate: rate, burst: float64(burst), tokens: float64(burst), last: time.Now()}
}

// Allow consumes one token and reports whether the request is allowed.
func (b *TokenBucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(b.last).Seconds()
	b.last = now
	b.tokens += elapsed * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// KeyedBucket is a per-key token-bucket limiter (e.g. per token or per IP).
type KeyedBucket struct {
	mu      sync.Mutex
	rate    float64
	burst   int
	buckets map[string]*TokenBucket
}

// NewKeyedBucket creates a keyed limiter.
func NewKeyedBucket(rate float64, burst int) *KeyedBucket {
	return &KeyedBucket{rate: rate, burst: burst, buckets: map[string]*TokenBucket{}}
}

// Allow reports whether the key may proceed.
func (k *KeyedBucket) Allow(key string) bool {
	k.mu.Lock()
	b, ok := k.buckets[key]
	if !ok {
		b = NewTokenBucket(k.rate, k.burst)
		k.buckets[key] = b
	}
	k.mu.Unlock()
	return b.Allow()
}

// SecureHeaders middleware sets the documented security headers.
func SecureHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		if isAPIPath(r.URL.Path) {
			w.Header().Set("Cache-Control", "no-store")
		}
		next.ServeHTTP(w, r)
	})
}

func isAPIPath(p string) bool {
	return len(p) >= 5 && p[:5] == "/api/"
}

// CORS restricts cross-origin requests to the given web origin.
func CORS(origin string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqOrigin := r.Header.Get("Origin")
			if origin != "" && reqOrigin != "" && reqOrigin == origin {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
				w.Header().Set("Access-Control-Allow-Credentials", "true")
				w.Header().Set("Vary", "Origin")
			}
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
