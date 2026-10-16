package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

var benchHTTP = &http.Client{}

// ensureTopic creates a topic via the API if it does not exist.
func ensureTopic(ctx context.Context, apiURL, token, topic string, partitions int, brokers map[string]string) error {
	if apiURL == "" {
		return fmt.Errorf("no API URL to create topic")
	}
	// Check existence first.
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimRight(apiURL, "/")+"/api/v1/topics/"+topic, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := benchHTTP.Do(req)
	if err == nil {
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil // exists
		}
		if resp.StatusCode != http.StatusNotFound {
			return fmt.Errorf("topic check status %d", resp.StatusCode)
		}
	}
	// Create it. RF = min(3, len(brokers)).
	rf := len(brokers)
	if rf > 3 {
		rf = 3
	}
	if rf < 1 {
		rf = 1
	}
	body, _ := json.Marshal(map[string]any{
		"name":               topic,
		"partitions":         partitions,
		"replication_factor": rf,
		"retention":          map[string]int64{"max_age_ms": 60000, "max_bytes": 1 << 30},
	})
	creq, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(apiURL, "/")+"/api/v1/topics", bytes.NewReader(body))
	creq.Header.Set("Content-Type", "application/json")
	creq.Header.Set("Authorization", "Bearer "+token)
	cresp, err := benchHTTP.Do(creq)
	if err != nil {
		return err
	}
	cresp.Body.Close()
	if cresp.StatusCode != http.StatusCreated && cresp.StatusCode != http.StatusOK {
		return fmt.Errorf("create topic status %d", cresp.StatusCode)
	}
	return nil
}

// deleteTopic removes a topic via the API (best effort).
func deleteTopic(ctx context.Context, apiURL, token, topic string) {
	if apiURL == "" || token == "" {
		return
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodDelete,
		strings.TrimRight(apiURL, "/")+"/api/v1/topics/"+topic, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := benchHTTP.Do(req)
	if err == nil {
		resp.Body.Close()
	}
}
