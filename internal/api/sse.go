package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// sseHub is a fan-out hub for Server-Sent Events. Each subscriber gets a
// buffered channel; slow subscribers are dropped (EventSource reconnects).
// The hub is used for the live event stream and the metrics stream.
type sseHub struct {
	mu   sync.Mutex
	subs map[chan string]struct{}
}

func newSSEHub() *sseHub {
	return &sseHub{subs: map[chan string]struct{}{}}
}

// subscribe registers a subscriber and returns its channel plus an unsubscribe
// function.
func (h *sseHub) subscribe() (<-chan string, func()) {
	ch := make(chan string, 256)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		if _, ok := h.subs[ch]; ok {
			delete(h.subs, ch)
			close(ch)
		}
		h.mu.Unlock()
	}
}

// publish sends a payload to every subscriber, dropping any that are full.
func (h *sseHub) publish(payload string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- payload:
		default:
			// Slow subscriber: drop it; EventSource will reconnect.
			delete(h.subs, ch)
			close(ch)
		}
	}
}

// close shuts down all subscribers.
func (h *sseHub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		delete(h.subs, ch)
		close(ch)
	}
}

// sseWrite sets the SSE headers and writes a single event.
func sseWrite(w http.ResponseWriter, event, data string) {
	if event != "" {
		fmt.Fprintf(w, "event: %s\n", event)
	}
	fmt.Fprintf(w, "data: %s\n\n", data)
	if fl, ok := w.(http.Flusher); ok {
		fl.Flush()
	}
}

// serveSSE is the shared SSE handler: it sets headers, then calls writeLoop
// with a per-subscriber channel until the client disconnects.
func (s *Server) serveSSE(w http.ResponseWriter, r *http.Request, ch <-chan string, unsubscribe func(), keepalive time.Duration) {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	fl, ok := w.(http.Flusher)
	if !ok {
		s.writeError(w, http.StatusInternalServerError, "internal", "streaming unsupported")
		return
	}
	fl.Flush()
	defer unsubscribe()
	ka := time.NewTicker(keepalive)
	defer ka.Stop()
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-ch:
			if !ok {
				return
			}
			sseWrite(w, "", data)
		case <-ka.C:
			// SSE comment keepalive so proxies don't time the connection out.
			fmt.Fprint(w, ": keepalive\n\n")
			fl.Flush()
		}
	}
}

// jsonPayload marshals v into an SSE data payload.
func jsonPayload(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}
