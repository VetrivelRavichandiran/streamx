package streamx

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/streamx/streamx/internal/protocol"
)

// Produce routes events to their partitions (keyed: xxhash64(key)%N; unkeyed:
// round-robin), sends per-partition produce requests to the leader broker, and
// merges the per-event results. acks: "none" | "leader" | "all".
func (c *Client) Produce(ctx context.Context, topic string, acks string, events []*protocol.Event) (*protocol.ProduceResponse, error) {
	if len(events) == 0 {
		return &protocol.ProduceResponse{Leader: c.cfg.ProducerID, Results: nil}, nil
	}
	if acks == "" {
		acks = "leader"
	}
	md, err := c.Metadata(ctx, topic)
	if err != nil {
		return nil, err
	}
	n := int32(len(md.Partitions))
	if n == 0 {
		return nil, fmt.Errorf("streamx: topic %s has no partitions", topic)
	}
	// Assign a partition to each event.
	byPart := map[int32][]*protocol.Event{}
	var rr int32
	for i, ev := range events {
		if ev.ID == "" {
			ev.ID = fmt.Sprintf("evt-%d-%d", time.Now().UnixNano(), i)
		}
		ev.Topic = topic
		ev.Producer = c.cfg.ProducerID
		var p int32
		if ev.Key != "" {
			p = PartitionForKey(ev.Key, n)
		} else {
			p = rr % n
			rr++
		}
		ev.Partition = p
		byPart[p] = append(byPart[p], ev)
	}
	// Send per-partition produce requests to the leader.
	results := make([]protocol.ProduceResult, 0, len(events))
	leaderID := ""
	for p, evs := range byPart {
		addr, _, err := c.pickBroker(ctx, topic, p)
		if err != nil {
			for range evs {
				results = append(results, protocol.ProduceResult{Partition: p, Error: err.Error()})
			}
			continue
		}
		body := &protocol.ProduceRequest{Acks: acks, Events: evs}
		resp, err := c.call(ctx, addr, &protocol.Request{
			Op:        protocol.OpProduce,
			Topic:     topic,
			Partition: p,
			Body:      mustJSON(body),
		})
		if err != nil {
			// NotLeader / TopicNotFound → refresh metadata and retry once.
			if isRetryable(err) {
				c.InvalidateMetadata(topic)
				addr, _, err2 := c.pickBroker(ctx, topic, p)
				if err2 == nil {
					resp, err = c.call(ctx, addr, &protocol.Request{
						Op: protocol.OpProduce, Topic: topic, Partition: p, Body: mustJSON(body),
					})
				}
			}
			if err != nil {
				for range evs {
					results = append(results, protocol.ProduceResult{Partition: p, Error: err.Error()})
				}
				continue
			}
		}
		var pr protocol.ProduceResponse
		if err := json.Unmarshal(resp.Body, &pr); err != nil {
			for range evs {
				results = append(results, protocol.ProduceResult{Partition: p, Error: "bad produce response"})
			}
			continue
		}
		leaderID = pr.Leader
		// Map results back to the events (order preserved per partition).
		for i, r := range pr.Results {
			if i < len(evs) {
				evs[i].Offset = r.Offset
				if r.Error != "" {
					results = append(results, protocol.ProduceResult{Partition: p, Offset: r.Offset, Error: r.Error})
				} else {
					results = append(results, protocol.ProduceResult{Partition: p, Offset: r.Offset})
				}
			}
		}
	}
	return &protocol.ProduceResponse{Leader: leaderID, Results: results}, nil
}

// isRetryable reports whether a produce error is worth a metadata refresh.
func isRetryable(err error) bool {
	se, ok := err.(*protocol.StatusError)
	if !ok {
		return false
	}
	return se.Status == protocol.NotLeader || se.Status == protocol.TopicNotFound ||
		se.Status == protocol.LeaderNotAvailable || se.Status == protocol.PartitionNotFound
}

// ProduceOne is a convenience for producing a single event.
func (c *Client) ProduceOne(ctx context.Context, topic, key string, payload []byte, headers map[string]string, acks string) (*protocol.ProduceResponse, error) {
	ev := &protocol.Event{Key: key, Payload: payload, Headers: headers}
	return c.Produce(ctx, topic, acks, []*protocol.Event{ev})
}
