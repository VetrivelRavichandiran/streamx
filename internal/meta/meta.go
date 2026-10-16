// Package meta defines the control-plane metadata store.
// Implementations: PostgreSQL (production/Docker) and SQLite (local dev).
package meta

import (
	"context"
	"errors"
	"time"
)

type TopicRetention struct {
	MaxAgeMS int64 `json:"max_age_ms"` // 0 = unlimited
	MaxBytes int64 `json:"max_bytes"`  // 0 = unlimited
}

type SchemaField struct {
	Name     string `json:"name"`
	Type     string `json:"type"` // string|number|boolean|object|array
	Required bool   `json:"required"`
}

type Topic struct {
	Name              string         `json:"name"`
	Partitions        int32          `json:"partitions"`
	ReplicationFactor int32          `json:"replication_factor"`
	Retention         TopicRetention `json:"retention"`
	Schema            []SchemaField  `json:"schema,omitempty"`
	SchemaEnabled     bool           `json:"schema_enabled"`
	Strategy          string         `json:"strategy"` // keyed|round_robin
	CreatedAt         time.Time      `json:"created_at"`
	UpdatedAt         time.Time      `json:"updated_at"`
}

type Broker struct {
	ID       string    `json:"id"`
	Addr     string    `json:"addr"`
	Status   string    `json:"status"` // up|down|unknown
	LastSeen time.Time `json:"last_seen"`
}

type Consumer struct {
	ID         string    `json:"id"`
	Group      string    `json:"group"`
	Status     string    `json:"status"` // up|down
	Assignment []int32   `json:"assignment"`
	LastSeen   time.Time `json:"last_seen"`
}

type Group struct {
	ID         string    `json:"id"`
	Generation int32     `json:"generation"`
	Topic      string    `json:"topic"`
	CreatedAt  time.Time `json:"created_at"`
}

type Offset struct {
	Group     string    `json:"group"`
	Topic     string    `json:"topic"`
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	UpdatedAt time.Time `json:"updated_at"`
}

type DLQEntry struct {
	ID             string    `json:"id"`
	Topic          string    `json:"topic"`
	Group          string    `json:"group"`
	OriginalTopic  string    `json:"original_topic"`
	OriginalPart   int32     `json:"original_partition"`
	OriginalOffset int64     `json:"original_offset"`
	Key            string    `json:"key,omitempty"`
	Payload        []byte    `json:"payload"`
	Reason         string    `json:"reason"`
	RetryCount     int       `json:"retry_count"`
	Status         string    `json:"status"` // pending|replayed|dropped
	CreatedAt      time.Time `json:"created_at"`
}

type Benchmark struct {
	ID           string     `json:"id"`
	Scenario     string     `json:"scenario"`
	Events       int        `json:"events"`
	PayloadBytes int        `json:"payload_bytes"`
	Producers    int        `json:"producers"`
	Consumers    int        `json:"consumers"`
	Partitions   int        `json:"partitions"`
	Concurrency  int        `json:"concurrency"`
	Status       string     `json:"status"` // running|done|failed
	StartedAt    time.Time  `json:"started_at"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
}

type BenchmarkResult struct {
	ID        string    `json:"id"`
	RunID     string    `json:"run_id"`
	Metric    string    `json:"metric"`
	Value     float64   `json:"value"`
	Unit      string    `json:"unit"`
	Labels    string    `json:"labels,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type User struct {
	ID        string    `json:"id"`
	OrgID     string    `json:"org_id"`
	Username  string    `json:"username"`
	Role      string    `json:"role"` // admin|operator|viewer
	Password  string    `json:"-"`    // bcrypt hash, never serialized
	CreatedAt time.Time `json:"created_at"`
}

type Token struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	Role      string    `json:"role"`
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
	CreatedAt time.Time `json:"created_at"`
}

type AuditEntry struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	Action    string    `json:"action"`
	Target    string    `json:"target"`
	Detail    string    `json:"detail"`
	CreatedAt time.Time `json:"created_at"`
}

// Store is the metadata store contract (see docs/CONTRACTS.md §4).
type Store interface {
	Init(ctx context.Context) error
	Close() error

	// EnsureOrganization creates the organization if absent (idempotent).
	EnsureOrganization(ctx context.Context, id, name string) error

	CreateTopic(ctx context.Context, t *Topic) error
	GetTopic(ctx context.Context, name string) (*Topic, error)
	ListTopics(ctx context.Context) ([]*Topic, error)
	UpdateTopic(ctx context.Context, t *Topic) error
	DeleteTopic(ctx context.Context, name string) error

	UpsertBroker(ctx context.Context, b *Broker) error
	ListBrokers(ctx context.Context) ([]*Broker, error)
	TouchBroker(ctx context.Context, id string) error
	SetBrokerStatus(ctx context.Context, id, status string) error

	UpsertGroup(ctx context.Context, g *Group) error
	GetGroup(ctx context.Context, id string) (*Group, error)
	ListGroups(ctx context.Context) ([]*Group, error)
	DeleteGroup(ctx context.Context, id string) error
	SetGroupGeneration(ctx context.Context, id string, gen int32) error

	UpsertConsumer(ctx context.Context, c *Consumer) error
	ListConsumers(ctx context.Context, group string) ([]*Consumer, error)
	SetConsumerAssignment(ctx context.Context, group, id string, assignment []int32) error
	TouchConsumer(ctx context.Context, group, id string) error
	SetConsumerStatus(ctx context.Context, group, id, status string) error
	ExpireConsumers(ctx context.Context, olderThan time.Duration) (int, error)

	CommitOffsets(ctx context.Context, group, topic string, offsets map[int32]int64) error
	GetOffsets(ctx context.Context, group, topic string) (map[int32]int64, error)

	InsertDLQ(ctx context.Context, e *DLQEntry) error
	ListDLQ(ctx context.Context, topic, group string, limit int) ([]*DLQEntry, error)
	GetDLQ(ctx context.Context, id string) (*DLQEntry, error)
	SetDLQStatus(ctx context.Context, id, status string) error

	CreateBenchmark(ctx context.Context, b *Benchmark) error
	UpdateBenchmark(ctx context.Context, b *Benchmark) error
	GetBenchmark(ctx context.Context, id string) (*Benchmark, error)
	ListBenchmarks(ctx context.Context, limit int) ([]*Benchmark, error)
	InsertBenchmarkResult(ctx context.Context, r *BenchmarkResult) error
	ListBenchmarkResults(ctx context.Context, runID string) ([]*BenchmarkResult, error)

	CreateUser(ctx context.Context, u *User) error
	GetUserByUsername(ctx context.Context, username string) (*User, error)
	GetUserByID(ctx context.Context, id string) (*User, error)
	ListUsers(ctx context.Context) ([]*User, error)

	CreateToken(ctx context.Context, t *Token) error
	ValidateToken(ctx context.Context, token string) (*Token, error)
	RevokeToken(ctx context.Context, token string) error

	InsertAudit(ctx context.Context, e *AuditEntry) error
	ListAudit(ctx context.Context, limit int) ([]*AuditEntry, error)
}

// ErrNotFound is returned when a row does not exist.
var ErrNotFound = errNotFound{}

type errNotFound struct{}

func (errNotFound) Error() string { return "meta: not found" }

func IsNotFound(err error) bool {
	_, ok := err.(errNotFound)
	return ok
}

// ErrTopicExists is returned when creating a topic that already exists.
var ErrTopicExists = errors.New("meta: topic already exists")

// ErrUserExists is returned when creating a user that already exists.
var ErrUserExists = errors.New("meta: user already exists")
