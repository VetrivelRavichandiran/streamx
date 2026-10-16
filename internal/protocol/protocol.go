// Package protocol defines the STREAMX broker wire protocol: JSON frames
// over TCP, request/response ops, and the Event record type.
package protocol

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	MaxFrameSize = 64 << 20 // 64 MiB

	// Ops
	OpPing         = "ping"
	OpMetadata     = "metadata"
	OpProduce      = "produce"
	OpFetch        = "fetch"
	OpOffsets      = "offsets"
	OpGroupJoin    = "group_join"
	OpGroupHeart   = "group_heartbeat"
	OpGroupLeave   = "group_leave"
	OpGroupCommit  = "group_commit"
	OpGroupDescr   = "group_describe"
	OpRebalance    = "rebalance"
	OpClusterHeart = "cluster_heartbeat" // broker-to-broker gossip
)

// Status codes (see docs/CONTRACTS.md §2).
const (
	OK                 int32 = 0
	UnknownOp          int32 = 1
	NotFound           int32 = 2
	NotLeader          int32 = 3
	TopicNotFound      int32 = 4
	PartitionNotFound  int32 = 5
	GroupNotFound      int32 = 6
	OffsetOutOfRange   int32 = 7
	ValidationFailed   int32 = 8
	RateLimited        int32 = 9
	QuotaExceeded      int32 = 10
	LeaderNotAvailable int32 = 11
	NotEnoughReplicas  int32 = 12
	GroupFenced        int32 = 13
	AuthFailed         int32 = 14
	Internal           int32 = 15
	Timeout            int32 = 16
	Backpressure       int32 = 17
	TopicExists        int32 = 18
)

var StatusNames = map[int32]string{
	OK: "ok", UnknownOp: "unknown_op", NotFound: "not_found",
	NotLeader: "not_leader", TopicNotFound: "topic_not_found",
	PartitionNotFound: "partition_not_found", GroupNotFound: "group_not_found",
	OffsetOutOfRange: "offset_out_of_range", ValidationFailed: "validation_failed",
	RateLimited: "rate_limited", QuotaExceeded: "quota_exceeded",
	LeaderNotAvailable: "leader_not_available", NotEnoughReplicas: "not_enough_replicas",
	GroupFenced: "group_fenced", AuthFailed: "auth_failed", Internal: "internal",
	Timeout: "timeout", Backpressure: "backpressure", TopicExists: "topic_exists",
}

// Event is a single stream record.
type Event struct {
	ID        string            `json:"id"`
	Topic     string            `json:"topic"`
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Key       string            `json:"key,omitempty"`
	Payload   []byte            `json:"payload"` // base64 in JSON
	Headers   map[string]string `json:"headers,omitempty"`
	Ts        int64             `json:"ts"` // ms epoch, broker-assigned
	Producer  string            `json:"producer_id,omitempty"`
	Seq       int64             `json:"seq,omitempty"`
}

// Request is an inbound operation.
type Request struct {
	ID          string          `json:"id"`
	Op          string          `json:"op"`
	Correlation int64           `json:"correlation"`
	Topic       string          `json:"topic,omitempty"`
	Partition   int32           `json:"partition"`
	Body        json.RawMessage `json:"body,omitempty"`
}

// Response is an outbound operation result.
type Response struct {
	ID          string          `json:"id"`
	Correlation int64           `json:"correlation"`
	Status      int32           `json:"status"`
	Error       string          `json:"error,omitempty"`
	Body        json.RawMessage `json:"body,omitempty"`
}

func (r *Response) Err() error {
	if r.Status == OK {
		return nil
	}
	msg := r.Error
	if msg == "" {
		msg = StatusNames[r.Status]
	}
	return &StatusError{Status: r.Status, Message: msg}
}

// StatusError carries a protocol status code.
type StatusError struct {
	Status  int32
	Message string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("streamx: %s (%d)", e.Message, e.Status)
}

func (e *StatusError) Is(target error) bool {
	t, ok := target.(*StatusError)
	return ok && t.Status == e.Status
}

// NewResponseOK builds an OK response with an encoded body, or an Internal
// error response if encoding fails.
func NewResponseOK(req *Request, body any) *Response {
	r, err := NewResponse(req, body)
	if err != nil {
		return NewError(req, Internal, err.Error())
	}
	return r
}

// NewResponse builds an OK response with an encoded body.
func NewResponse(req *Request, body any) (*Response, error) {
	var raw json.RawMessage
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		raw = b
	}
	return &Response{ID: req.ID, Correlation: req.Correlation, Status: OK, Body: raw}, nil
}

// NewError builds an error response.
func NewError(req *Request, status int32, msg string) *Response {
	return &Response{ID: req.ID, Correlation: req.Correlation, Status: status, Error: msg}
}

// WriteFrame writes [len][json] to w.
func WriteFrame(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) > MaxFrameSize {
		return fmt.Errorf("frame too large: %d", len(b))
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(b)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// ReadFrame reads [len][json] from r and unmarshals into v.
func ReadFrame(r *bufio.Reader, v any) error {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 {
		return errors.New("empty frame")
	}
	if n > MaxFrameSize {
		return fmt.Errorf("frame too large: %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	return json.Unmarshal(buf, v)
}

// ---- typed bodies ----

type PingBody struct {
	Ts int64 `json:"ts"`
}

type MetadataRequest struct {
	Topic string `json:"topic"`
}

type PartitionMeta struct {
	Partition     int32    `json:"partition"`
	Leader        string   `json:"leader"`
	Replicas      []string `json:"replicas"`
	HighWatermark int64    `json:"high_watermark"`
	LogEndOffset  int64    `json:"log_end_offset"`
	LogStart      int64    `json:"log_start_offset"`
}

type MetadataResponse struct {
	Topic      string          `json:"topic"`
	Partitions []PartitionMeta `json:"partitions"`
}

type ProduceRequest struct {
	Acks   string   `json:"acks"` // "none" | "leader" | "all"
	Events []*Event `json:"events"`
}

type ProduceResult struct {
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Error     string `json:"error,omitempty"`
}

type ProduceResponse struct {
	Leader  string          `json:"leader"`
	Results []ProduceResult `json:"results"`
}

type FetchRequest struct {
	// Offset is the start offset. nil = from high watermark (latest);
	// 0 = from the beginning; -1 = earliest (log start). Using a pointer
	// keeps "absent" (default to latest) distinct from an explicit 0.
	Offset   *int64 `json:"offset,omitempty"`
	MaxBytes int    `json:"max_bytes"`
	MaxWait  int    `json:"max_wait_ms"`
	MinBytes int    `json:"min_bytes"`
}

type FetchResponse struct {
	Records        []*Event `json:"records"`
	HighWatermark  int64    `json:"high_watermark"`
	LogStartOffset int64    `json:"log_start_offset"`
}

type OffsetsRequest struct {
	Which string `json:"which"` // "earliest" | "latest"
}

type OffsetsResponse struct {
	Offset int64 `json:"offset"`
}

type GroupJoinRequest struct {
	Group    string `json:"group"`
	MemberID string `json:"member_id"`
	// Topic is the topic the member subscribes to. Required for a brand-new
	// group so the coordinator can size the assignment; for an existing group
	// it must match the group's recorded topic.
	Topic string `json:"topic,omitempty"`
}

type GroupJoinResponse struct {
	Generation int32   `json:"generation"`
	Assignment []int32 `json:"assignment"`
	IsLeader   bool    `json:"is_leader"`
}

type GroupHeartbeatRequest struct {
	Group      string `json:"group"`
	MemberID   string `json:"member_id"`
	Generation int32  `json:"generation"`
}

type GroupHeartbeatResponse struct {
	OK                bool  `json:"ok"`
	Generation        int32 `json:"generation"`
	RebalanceRequired bool  `json:"rebalance_required"`
}

type GroupLeaveRequest struct {
	Group    string `json:"group"`
	MemberID string `json:"member_id"`
}

type GroupCommitRequest struct {
	Group    string          `json:"group"`
	MemberID string          `json:"member_id"`
	Offsets  map[int32]int64 `json:"offsets"` // partition -> committed offset (next to read)
}

type GroupDescribeResponse struct {
	Generation int32           `json:"generation"`
	Members    []GroupMember   `json:"members"`
	Offsets    map[int32]int64 `json:"offsets"`
}

type GroupMember struct {
	MemberID   string  `json:"member_id"`
	Assignment []int32 `json:"assignment"`
}

type RebalanceRequest struct {
	Group string `json:"group"`
}

type RebalanceResponse struct {
	Generation int32 `json:"generation"`
}

// ClusterHeartbeat is broker gossip.
type PartitionState struct {
	IsLeader bool  `json:"is_leader"`
	LEO      int64 `json:"leo"`
	HW       int64 `json:"hw"`
}

type TopicState struct {
	Partitions map[int32]PartitionState `json:"partitions"`
}

type ClusterHeartbeat struct {
	BrokerID string                `json:"broker_id"`
	Topics   map[string]TopicState `json:"topics"`
	Ts       int64                 `json:"ts"`
}
