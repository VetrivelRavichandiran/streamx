package api

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestProduceEventPlainStringPayload is a regression test for the REST produce
// contract (docs/API.md §POST /topics/{name}/events): the control plane accepts
// `payload` as a PLAIN string (the raw event body, e.g. a JSON object as a
// string) — the dashboard and scripts/demo.sh both send it this way.
//
// The old implementation typed the field as []byte, so Go base64-decoded the
// incoming string and rejected any non-base64 body (e.g. `{"order":"demo-0"}`)
// with a 400. This test pins the documented plain-string behavior.
func TestProduceEventPlainStringPayload(t *testing.T) {
	// Exactly what the web client (lib/api.ts produceEvent) and demo.sh send.
	body := `{"key":"cust-123","payload":"{\"order\":\"demo-0\",\"amount\":42}","acks":"leader"}`

	var req produceEventReq
	if err := json.NewDecoder(strings.NewReader(body)).Decode(&req); err != nil {
		t.Fatalf("decode plain-string payload failed (was the base64 bug): %v", err)
	}
	if req.Key != "cust-123" {
		t.Errorf("key = %q, want cust-123", req.Key)
	}
	if req.Payload != `{"order":"demo-0","amount":42}` {
		t.Errorf("payload = %q, want the original plain string", req.Payload)
	}
	if got := string([]byte(req.Payload)); got != `{"order":"demo-0","amount":42}` {
		t.Errorf("converted bytes = %q, want the original plain string", got)
	}
}

// TestProduceBatchPlainStringPayloads covers the batch endpoint, which the
// dashboard uses for multi-event produces.
func TestProduceBatchPlainStringPayloads(t *testing.T) {
	body := `{"events":[{"key":"k1","payload":"{\"n\":1}"},{"payload":"{\"n\":2}"}],"acks":"all"}`

	var req produceBatchReq
	if err := json.NewDecoder(strings.NewReader(body)).Decode(&req); err != nil {
		t.Fatalf("decode batch plain-string payloads failed: %v", err)
	}
	if req.Acks != "all" {
		t.Errorf("acks = %q, want all", req.Acks)
	}
	if len(req.Events) != 2 {
		t.Fatalf("events = %d, want 2", len(req.Events))
	}
	if string([]byte(req.Events[0].Payload)) != `{"n":1}` {
		t.Errorf("event[0] payload = %q, want {\"n\":1}", req.Events[0].Payload)
	}
	if string([]byte(req.Events[1].Payload)) != `{"n":2}` {
		t.Errorf("event[1] payload = %q, want {\"n\":2}", req.Events[1].Payload)
	}
}

// TestProduceEventEmptyPayload ensures an absent payload decodes to "" (not an
// error), so clients that omit the field still work.
func TestProduceEventEmptyPayload(t *testing.T) {
	var req produceEventReq
	if err := json.NewDecoder(strings.NewReader(`{"key":"k"}`)).Decode(&req); err != nil {
		t.Fatalf("decode payload-less request failed: %v", err)
	}
	if req.Payload != "" {
		t.Errorf("payload = %q, want empty", req.Payload)
	}
}
