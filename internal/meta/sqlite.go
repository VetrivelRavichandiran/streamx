// Package meta — SQLite metadata store (dev fallback, CONTRACTS.md §4).
//
// Uses the pure-Go modernc.org/sqlite driver so the binary stays
// CGO_ENABLED=0 and the Docker images stay distroless.
package meta

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite" // registers the "sqlite" driver (pure Go, CGO-free)
)

//go:embed schema_sqlite.sql
var sqliteSchema string

// SQLiteStore implements Store backed by a single SQLite file.
type SQLiteStore struct {
	db *sql.DB
}

// OpenSQLite opens (creating if needed) the SQLite file at path and applies
// the schema. The file path is relative to the process CWD.
func OpenSQLite(path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("meta: open sqlite: %w", err)
	}
	// Single-writer mode keeps the dev store simple and avoids SQLITE_BUSY
	// under the broker+api concurrency pattern of `make dev`.
	if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		db.Close()
		return nil, fmt.Errorf("meta: pragma wal: %w", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000;"); err != nil {
		db.Close()
		return nil, fmt.Errorf("meta: pragma busy_timeout: %w", err)
	}
	if _, err := db.Exec(sqliteSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("meta: apply schema: %w", err)
	}
	return &SQLiteStore{db: db}, nil
}

func (s *SQLiteStore) Close() error { return s.db.Close() }

// Init is a no-op for SQLite (schema applied at open).
func (s *SQLiteStore) Init(ctx context.Context) error { return nil }

func ts(t time.Time) int64     { return t.UnixMilli() }
func fromTS(v int64) time.Time { return time.UnixMilli(v) }

func isNoRows(err error) bool { return errors.Is(err, sql.ErrNoRows) }

// ---------------------------------------------------------------------------
// Topics
// ---------------------------------------------------------------------------

func (s *SQLiteStore) CreateTopic(ctx context.Context, t *Topic) error {
	now := ts(time.Now())
	schemaJSON, _ := json.Marshal(t.Schema)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO topics (name, partitions, replication_factor, retention_max_age_ms,
			retention_max_bytes, schema_json, schema_enabled, strategy, created_at, updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?)`,
		t.Name, t.Partitions, t.ReplicationFactor, t.Retention.MaxAgeMS, t.Retention.MaxBytes,
		string(schemaJSON), boolToInt(t.SchemaEnabled), t.Strategy, now, now)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return ErrTopicExists
		}
		return err
	}
	return nil
}

func (s *SQLiteStore) GetTopic(ctx context.Context, name string) (*Topic, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT name, partitions, replication_factor, retention_max_age_ms, retention_max_bytes,
			schema_json, schema_enabled, strategy, created_at, updated_at
		FROM topics WHERE name=?`, name)
	return scanTopic(row)
}

func (s *SQLiteStore) ListTopics(ctx context.Context) ([]*Topic, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT name, partitions, replication_factor, retention_max_age_ms, retention_max_bytes,
			schema_json, schema_enabled, strategy, created_at, updated_at
		FROM topics ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Topic
	for rows.Next() {
		t, err := scanTopic(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) UpdateTopic(ctx context.Context, t *Topic) error {
	schemaJSON, _ := json.Marshal(t.Schema)
	res, err := s.db.ExecContext(ctx, `
		UPDATE topics SET partitions=?, replication_factor=?, retention_max_age_ms=?,
			retention_max_bytes=?, schema_json=?, schema_enabled=?, strategy=?, updated_at=?
		WHERE name=?`,
		t.Partitions, t.ReplicationFactor, t.Retention.MaxAgeMS, t.Retention.MaxBytes,
		string(schemaJSON), boolToInt(t.SchemaEnabled), t.Strategy, ts(time.Now()), t.Name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLiteStore) DeleteTopic(ctx context.Context, name string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM topics WHERE name=?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func scanTopic(sc scanner) (*Topic, error) {
	var (
		t        Topic
		schemaJS string
		enabled  int
		created  int64
		updated  int64
	)
	err := sc.Scan(&t.Name, &t.Partitions, &t.ReplicationFactor, &t.Retention.MaxAgeMS,
		&t.Retention.MaxBytes, &schemaJS, &enabled, &t.Strategy, &created, &updated)
	if err != nil {
		if isNoRows(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	_ = json.Unmarshal([]byte(schemaJS), &t.Schema)
	t.SchemaEnabled = enabled == 1
	t.CreatedAt = fromTS(created)
	t.UpdatedAt = fromTS(updated)
	return &t, nil
}

// ---------------------------------------------------------------------------
// Brokers
// ---------------------------------------------------------------------------

func (s *SQLiteStore) UpsertBroker(ctx context.Context, b *Broker) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO brokers (id, addr, status, last_seen) VALUES (?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET addr=excluded.addr, status=excluded.status, last_seen=excluded.last_seen`,
		b.ID, b.Addr, b.Status, ts(time.Now()))
	return err
}

func (s *SQLiteStore) ListBrokers(ctx context.Context) ([]*Broker, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, addr, status, last_seen FROM brokers ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Broker
	for rows.Next() {
		var b Broker
		var seen int64
		if err := rows.Scan(&b.ID, &b.Addr, &b.Status, &seen); err != nil {
			return nil, err
		}
		b.LastSeen = fromTS(seen)
		out = append(out, &b)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) TouchBroker(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE brokers SET last_seen=?, status='up' WHERE id=?`, ts(time.Now()), id)
	return err
}

func (s *SQLiteStore) SetBrokerStatus(ctx context.Context, id, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE brokers SET status=? WHERE id=?`, status, id)
	return err
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

func (s *SQLiteStore) UpsertGroup(ctx context.Context, g *Group) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO consumer_groups (id, generation, topic, created_at) VALUES (?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET topic=excluded.topic`,
		g.ID, g.Generation, g.Topic, ts(time.Now()))
	return err
}

func (s *SQLiteStore) GetGroup(ctx context.Context, id string) (*Group, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, generation, topic, created_at FROM consumer_groups WHERE id=?`, id)
	var g Group
	var created int64
	if err := row.Scan(&g.ID, &g.Generation, &g.Topic, &created); err != nil {
		if isNoRows(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	g.CreatedAt = fromTS(created)
	return &g, nil
}

func (s *SQLiteStore) ListGroups(ctx context.Context) ([]*Group, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, generation, topic, created_at FROM consumer_groups ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Group
	for rows.Next() {
		var g Group
		var created int64
		if err := rows.Scan(&g.ID, &g.Generation, &g.Topic, &created); err != nil {
			return nil, err
		}
		g.CreatedAt = fromTS(created)
		out = append(out, &g)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) DeleteGroup(ctx context.Context, id string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM consumers WHERE "group"=?`, id); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM offsets WHERE "group"=?`, id); err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM consumer_groups WHERE id=?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLiteStore) SetGroupGeneration(ctx context.Context, id string, gen int32) error {
	_, err := s.db.ExecContext(ctx, `UPDATE consumer_groups SET generation=? WHERE id=?`, gen, id)
	return err
}

// ---------------------------------------------------------------------------
// Consumers
// ---------------------------------------------------------------------------

func (s *SQLiteStore) UpsertConsumer(ctx context.Context, c *Consumer) error {
	assign, _ := json.Marshal(c.Assignment)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO consumers (id, "group", status, assignment, last_seen) VALUES (?,?,?,?,?)
		ON CONFLICT("group", id) DO UPDATE SET status=excluded.status, assignment=excluded.assignment, last_seen=excluded.last_seen`,
		c.ID, c.Group, c.Status, string(assign), ts(time.Now()))
	return err
}

func (s *SQLiteStore) ListConsumers(ctx context.Context, group string) ([]*Consumer, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, "group", status, assignment, last_seen FROM consumers WHERE "group"=? ORDER BY id`, group)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Consumer
	for rows.Next() {
		var c Consumer
		var assign string
		var seen int64
		if err := rows.Scan(&c.ID, &c.Group, &c.Status, &assign, &seen); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(assign), &c.Assignment)
		c.LastSeen = fromTS(seen)
		out = append(out, &c)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) SetConsumerAssignment(ctx context.Context, group, id string, assignment []int32) error {
	assign, _ := json.Marshal(assignment)
	_, err := s.db.ExecContext(ctx, `UPDATE consumers SET assignment=? WHERE "group"=? AND id=?`, string(assign), group, id)
	return err
}

func (s *SQLiteStore) TouchConsumer(ctx context.Context, group, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE consumers SET last_seen=?, status='up' WHERE "group"=? AND id=?`, ts(time.Now()), group, id)
	return err
}

func (s *SQLiteStore) SetConsumerStatus(ctx context.Context, group, id, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE consumers SET status=? WHERE "group"=? AND id=?`, status, group, id)
	return err
}

func (s *SQLiteStore) ExpireConsumers(ctx context.Context, olderThan time.Duration) (int, error) {
	cutoff := ts(time.Now().Add(-olderThan))
	res, err := s.db.ExecContext(ctx, `UPDATE consumers SET status='down' WHERE last_seen < ? AND status='up'`, cutoff)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// ---------------------------------------------------------------------------
// Offsets
// ---------------------------------------------------------------------------

func (s *SQLiteStore) CommitOffsets(ctx context.Context, group, topic string, offsets map[int32]int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := ts(time.Now())
	for p, off := range offsets {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO offsets ("group", topic, partition, offset, updated_at) VALUES (?,?,?,?,?)
			ON CONFLICT("group", topic, partition) DO UPDATE SET offset=excluded.offset, updated_at=excluded.updated_at`,
			group, topic, p, off, now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *SQLiteStore) GetOffsets(ctx context.Context, group, topic string) (map[int32]int64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT partition, offset FROM offsets WHERE "group"=? AND topic=?`, group, topic)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int32]int64{}
	for rows.Next() {
		var p int32
		var off int64
		if err := rows.Scan(&p, &off); err != nil {
			return nil, err
		}
		out[p] = off
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// DLQ
// ---------------------------------------------------------------------------

func (s *SQLiteStore) InsertDLQ(ctx context.Context, e *DLQEntry) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO dlq_entries (id, topic, "group", original_topic, original_part, original_offset,
			key, payload, reason, retry_count, status, created_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
		e.ID, e.Topic, e.Group, e.OriginalTopic, e.OriginalPart, e.OriginalOffset,
		e.Key, e.Payload, e.Reason, e.RetryCount, e.Status, ts(time.Now()))
	return err
}

func (s *SQLiteStore) ListDLQ(ctx context.Context, topic, group string, limit int) ([]*DLQEntry, error) {
	q := `SELECT id, topic, "group", original_topic, original_part, original_offset, key, payload, reason, retry_count, status, created_at FROM dlq_entries WHERE 1=1`
	var args []any
	if topic != "" {
		q += ` AND topic=?`
		args = append(args, topic)
	}
	if group != "" {
		q += ` AND "group"=?`
		args = append(args, group)
	}
	q += ` ORDER BY created_at DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*DLQEntry
	for rows.Next() {
		e, err := scanDLQ(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) GetDLQ(ctx context.Context, id string) (*DLQEntry, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, topic, "group", original_topic, original_part, original_offset, key, payload, reason, retry_count, status, created_at
		FROM dlq_entries WHERE id=?`, id)
	e, err := scanDLQ(row)
	if err != nil && isNoRows(err) {
		return nil, ErrNotFound
	}
	return e, err
}

func (s *SQLiteStore) SetDLQStatus(ctx context.Context, id, status string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE dlq_entries SET status=? WHERE id=?`, status, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func scanDLQ(sc scanner) (*DLQEntry, error) {
	var e DLQEntry
	var created int64
	err := sc.Scan(&e.ID, &e.Topic, &e.Group, &e.OriginalTopic, &e.OriginalPart, &e.OriginalOffset,
		&e.Key, &e.Payload, &e.Reason, &e.RetryCount, &e.Status, &created)
	if err != nil {
		return nil, err
	}
	e.CreatedAt = fromTS(created)
	return &e, nil
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func (s *SQLiteStore) CreateBenchmark(ctx context.Context, b *Benchmark) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO benchmarks (id, scenario, events, payload_bytes, producers, consumers, partitions, concurrency, status, started_at, finished_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		b.ID, b.Scenario, b.Events, b.PayloadBytes, b.Producers, b.Consumers, b.Partitions, b.Concurrency,
		b.Status, ts(time.Now()), nullInt64(b.FinishedAt))
	return err
}

func (s *SQLiteStore) UpdateBenchmark(ctx context.Context, b *Benchmark) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE benchmarks SET status=?, finished_at=? WHERE id=?`,
		b.Status, nullInt64(b.FinishedAt), b.ID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLiteStore) GetBenchmark(ctx context.Context, id string) (*Benchmark, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, scenario, events, payload_bytes, producers, consumers, partitions, concurrency, status, started_at, finished_at
		FROM benchmarks WHERE id=?`, id)
	return scanBenchmark(row)
}

func (s *SQLiteStore) ListBenchmarks(ctx context.Context, limit int) ([]*Benchmark, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, scenario, events, payload_bytes, producers, consumers, partitions, concurrency, status, started_at, finished_at
		FROM benchmarks ORDER BY started_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Benchmark
	for rows.Next() {
		b, err := scanBenchmark(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) InsertBenchmarkResult(ctx context.Context, r *BenchmarkResult) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO benchmark_results (id, run_id, metric, value, unit, labels, created_at) VALUES (?,?,?,?,?,?,?)`,
		r.ID, r.RunID, r.Metric, r.Value, r.Unit, r.Labels, ts(time.Now()))
	return err
}

func (s *SQLiteStore) ListBenchmarkResults(ctx context.Context, runID string) ([]*BenchmarkResult, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, run_id, metric, value, unit, labels, created_at FROM benchmark_results WHERE run_id=? ORDER BY created_at`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*BenchmarkResult
	for rows.Next() {
		var r BenchmarkResult
		var created int64
		if err := rows.Scan(&r.ID, &r.RunID, &r.Metric, &r.Value, &r.Unit, &r.Labels, &created); err != nil {
			return nil, err
		}
		r.CreatedAt = fromTS(created)
		out = append(out, &r)
	}
	return out, rows.Err()
}

func scanBenchmark(sc scanner) (*Benchmark, error) {
	var b Benchmark
	var started int64
	var finished sql.NullInt64
	err := sc.Scan(&b.ID, &b.Scenario, &b.Events, &b.PayloadBytes, &b.Producers, &b.Consumers,
		&b.Partitions, &b.Concurrency, &b.Status, &started, &finished)
	if err != nil {
		if isNoRows(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	b.StartedAt = fromTS(started)
	if finished.Valid {
		t := fromTS(finished.Int64)
		b.FinishedAt = &t
	}
	return &b, nil
}

// ---------------------------------------------------------------------------
// Users / tokens / audit
// ---------------------------------------------------------------------------

func (s *SQLiteStore) CreateUser(ctx context.Context, u *User) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO users (id, org_id, username, role, password, created_at) VALUES (?,?,?,?,?,?)`,
		u.ID, u.OrgID, u.Username, u.Role, u.Password, ts(time.Now()))
	if err != nil && strings.Contains(err.Error(), "UNIQUE") {
		return ErrUserExists
	}
	return err
}

func (s *SQLiteStore) GetUserByUsername(ctx context.Context, username string) (*User, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, org_id, username, role, password, created_at FROM users WHERE username=?`, username)
	var u User
	var created int64
	if err := row.Scan(&u.ID, &u.OrgID, &u.Username, &u.Role, &u.Password, &created); err != nil {
		if isNoRows(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	u.CreatedAt = fromTS(created)
	return &u, nil
}

func (s *SQLiteStore) GetUserByID(ctx context.Context, id string) (*User, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, org_id, username, role, password, created_at FROM users WHERE id=?`, id)
	var u User
	var created int64
	if err := row.Scan(&u.ID, &u.OrgID, &u.Username, &u.Role, &u.Password, &created); err != nil {
		if isNoRows(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	u.CreatedAt = fromTS(created)
	return &u, nil
}

func (s *SQLiteStore) EnsureOrganization(ctx context.Context, id, name string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO organizations (id, name, created_at) VALUES (?,?,?)
		ON CONFLICT(id) DO NOTHING`, id, name, ts(time.Now()))
	return err
}

func (s *SQLiteStore) ListUsers(ctx context.Context) ([]*User, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, org_id, username, role, password, created_at FROM users ORDER BY username`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*User
	for rows.Next() {
		var u User
		var created int64
		if err := rows.Scan(&u.ID, &u.OrgID, &u.Username, &u.Role, &u.Password, &created); err != nil {
			return nil, err
		}
		u.CreatedAt = fromTS(created)
		out = append(out, &u)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) CreateToken(ctx context.Context, t *Token) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO tokens (id, user_id, role, token, expires_at, created_at) VALUES (?,?,?,?,?,?)`,
		t.ID, t.UserID, t.Role, t.Token, ts(t.ExpiresAt), ts(time.Now()))
	return err
}

func (s *SQLiteStore) ValidateToken(ctx context.Context, token string) (*Token, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, user_id, role, token, expires_at, created_at FROM tokens WHERE token=?`, token)
	var t Token
	var expires, created int64
	if err := row.Scan(&t.ID, &t.UserID, &t.Role, &t.Token, &expires, &created); err != nil {
		if isNoRows(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	t.ExpiresAt = fromTS(expires)
	t.CreatedAt = fromTS(created)
	if time.Now().After(t.ExpiresAt) {
		return nil, ErrNotFound
	}
	return &t, nil
}

func (s *SQLiteStore) RevokeToken(ctx context.Context, token string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM tokens WHERE token=?`, token)
	return err
}

func (s *SQLiteStore) InsertAudit(ctx context.Context, e *AuditEntry) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO audit_logs (id, user_id, action, target, detail, created_at) VALUES (?,?,?,?,?,?)`,
		e.ID, e.UserID, e.Action, e.Target, e.Detail, ts(time.Now()))
	return err
}

func (s *SQLiteStore) ListAudit(ctx context.Context, limit int) ([]*AuditEntry, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, user_id, action, target, detail, created_at FROM audit_logs ORDER BY created_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*AuditEntry
	for rows.Next() {
		var e AuditEntry
		var created int64
		if err := rows.Scan(&e.ID, &e.UserID, &e.Action, &e.Target, &e.Detail, &created); err != nil {
			return nil, err
		}
		e.CreatedAt = fromTS(created)
		out = append(out, &e)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

type scanner interface {
	Scan(dest ...any) error
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func nullInt64(t *time.Time) any {
	if t == nil {
		return nil
	}
	return ts(*t)
}
