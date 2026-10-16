# STREAMX Go SDK Contract (sdk/streamx)

Module path: `github.com/streamx/streamx/sdk/streamx` (same module, package `streamx`).
Depends ONLY on `internal/protocol` — no other internal packages.

## API surface

```go
type Config struct {
    Brokers      []string // e.g. ["127.0.0.1:9092", ...]
    ProducerID   string   // default: "p-<rand>"
    APIURL       string   // optional control-plane URL for DLQ reporting
    APIToken     string   // optional
    DialTimeout  time.Duration // default 5s
    RequestTimeout time.Duration // default 10s
    MaxRetries   int // network-level retries, default 2
}

type Client struct{ ... }
func NewClient(cfg Config) (*Client, error)
func (c *Client) Close() error
func (c *Client) Ping(ctx context.Context) (time.Duration, error)
func (c *Client) Metadata(ctx context.Context, topic string) (*protocol.MetadataResponse, error)
func (c *Client) Produce(ctx context.Context, topic string, acks string, events []*protocol.Event) (*protocol.ProduceResponse, error)
    // routes each event to its partition (keyed: xxhash64(key)%N; unkeyed: round-robin),
    // sends per-partition produce requests to the LEADER broker (via metadata),
    // merges results. Acks: "none"|"leader"|"all".
func (c *Client) Fetch(ctx context.Context, topic string, partition int32, offset int64, maxBytes int, maxWait time.Duration) (*protocol.FetchResponse, error)
func (c *Client) Offsets(ctx context.Context, topic string, partition int32, which string) (int64, error)

// ---- consumer groups ----
type GroupConfig struct {
    Client       *Client
    Group        string
    Topic        string
    MemberID     string // default "m-<rand>"
    StartAt      string // "earliest" | "latest"
    Handler      func(ctx context.Context, ev *protocol.Event) error
    MaxRetries   int  // default 3 (in-place retries w/ backoff, then DLQ)
    Concurrency  int  // default 8 handler goroutines
    QueueSize    int  // default 10000 bounded fetch->handler queue
    DLQTopic     string // default <topic>.dlq
    OnDLQ        func(ctx context.Context, ev *protocol.Event, reason string) // optional hook
}
type Consumer struct{ ... }
func NewConsumer(cfg GroupConfig) (*Consumer, error)
func (c *Consumer) Start(ctx context.Context) error   // blocking; returns on ctx cancel
func (c *Consumer) MemberID() string
func (c *Consumer) Assignment() []int32
func (c *Consumer) Lag() map[int32]int64 // partition -> latest - committed
```

## Behavior (MUST match docs/CONTRACTS.md)

1. **Broker selection:** keep a `MetadataResponse` cache per topic, refreshed on
   `NotLeader`/`TopicNotFound`/connection errors and at most every 5 s.
   Produce goes to the leader broker for each partition; fetch may go to any
   live replica (prefer leader).
2. **Reconnect:** on connection error, redial with exponential backoff
   (250 ms base, 5 s cap) respecting ctx.
3. **Group loop:** join → (fetch loop over assigned partitions from
   committed offset, or start position if none) → heartbeat every 2 s in a
   goroutine → on generation mismatch: stop fetching, re-join, re-read
   committed offsets. On `rebalance_required`: same.
4. **Offsets:** commit every 1 s or every 100 processed events (whichever
   first), per partition, offset = next-to-read. On clean shutdown: commit +
   group_leave.
5. **Retry/DLQ:** handler error → retry in place (100 ms * 2^i, cap 5 s) up to
   MaxRetries; then: (a) publish event to DLQTopic (auto-create via API if
   APIURL set, else skip), (b) call OnDLQ hook, (c) POST to
   `APIURL/api/v1/dlq` if APIURL+APIToken set. Continue with next event.
6. **Backpressure:** fetch loop blocks when handler queue full (bounded
   channel). Never unbounded.
7. **No global state.** All state in structs. All goroutines must exit when
   ctx is cancelled — verified by tests (no leaks: use
   `golang.org/x/sync/errgroup` or explicit WaitGroup + ctx).
8. **xxhash64** via `github.com/cespare/xxhash/v2` for keyed partitioning.

## Files

- `sdk/streamx/client.go` — Client, dial, request/response, metadata cache
- `sdk/streamx/produce.go` — Produce routing
- `sdk/streamx/consumer.go` — Consumer group loop, heartbeat, commit, retry/DLQ
- `sdk/streamx/client_test.go` — framing/metadata unit tests (no broker needed:
  use in-memory fake broker server from a test helper)
- `sdk/streamx/fake_test.go` — minimal in-process broker stub for tests