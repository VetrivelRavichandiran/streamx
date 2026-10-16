package broker

import (
	"net/http"
	"strconv"
	"time"

	"github.com/streamx/streamx/internal/broker/storage"
	"github.com/streamx/streamx/internal/config"
	"github.com/streamx/streamx/internal/protocol"
)

// storageRecord converts a wire Event into a storage Record.
func storageRecord(ev *protocol.Event) storage.Record {
	return storage.Record{
		Key:         ev.Key,
		Payload:     ev.Payload,
		Headers:     ev.Headers,
		ProducerSeq: ev.Seq,
		ProducerID:  ev.Producer,
	}
}

// eventFromStored converts a stored record into a wire Event.
func eventFromStored(r storage.StoredRecord) *protocol.Event {
	ts := time.Now().UnixMilli()
	return &protocol.Event{
		ID:        "",
		Topic:     "",
		Partition: 0,
		Offset:    r.Offset,
		Key:       r.Record.Key,
		Payload:   r.Record.Payload,
		Headers:   r.Record.Headers,
		Ts:        ts,
		Producer:  r.Record.ProducerID,
		Seq:       r.Record.ProducerSeq,
	}
}

// configSortedCluster returns sorted broker ids from a cluster map.
func configSortedCluster(m map[string]string) []string {
	return config.SortedCluster(m)
}

// assignReplicas returns the deterministic replica set for a partition.
func assignReplicas(sorted []string, partition, rf int32) []string {
	return config.AssignReplicas(sorted, partition, rf)
}

// newHTTPServer builds the metrics/health/admin HTTP server.
func newHTTPServer(b *Broker, h http.Handler) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", h)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	// Admin failure-injection endpoints (called by the control plane).
	mux.HandleFunc("/admin/stop", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("stopping"))
		go func() {
			time.Sleep(50 * time.Millisecond) // let the response flush
			_ = b.GracefulStop()
		}()
	})
	mux.HandleFunc("/admin/slow-produce", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ms, _ := strconv.ParseInt(r.URL.Query().Get("ms"), 10, 64)
		b.SetSlowProduceMS(ms)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/admin/delete-topic", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := r.URL.Query().Get("topic")
		if name == "" {
			http.Error(w, "topic required", http.StatusBadRequest)
			return
		}
		if err := b.DeleteTopic(name); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	return &http.Server{Handler: mux}
}
