// Package meta — PostgreSQL metadata store (production, CONTRACTS.md §4).
//
// Uses the pgx/v5 driver through database/sql (stdlib adapter) so the query
// shape matches the SQLite store. DSN form: postgres://user:pass@host:5432/db
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

	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed schema.sql
var pgSchema string

// PostgresStore implements Store backed by PostgreSQL.
type PostgresStore struct {
	db *sql.DB
}

// OpenPostgres connects to PostgreSQL and applies the schema.
func OpenPostgres(dsn string) (*PostgresStore, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("meta: open postgres: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("meta: ping postgres: %w", err)
	}
	if _, err := db.ExecContext(ctx, pgSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("meta: apply schema: %w", err)
	}
	return &PostgresStore{db: db}, nil
}

func (s *PostgresStore) Close() error { return s.db.Close() }

func (s *PostgresStore) Init(ctx context.Context) error { return nil }

// ---------------------------------------------------------------------------
// Topics
// ---------------------------------------------------------------------------

func (s *PostgresStore) CreateTopic(ctx context.Context, t *Topic) error {
	schemaJSON, _ := json.Marshal(t.Schema)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO topics (name, partitions, replication_factor, retention_max_age_ms,
			retention_max_bytes, schema_json, schema_enabled, strategy)
		VALUES (?,?,?,?,?,?,?,?)`,
		t.Name, t.Partitions, t.ReplicationFactor, t.Retention.MaxAgeMS, t.Retention.MaxBytes,
		string(schemaJSON), t.SchemaEnabled, t.Strategy)
	if err != nil && (strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "23505")) {
		return ErrTopicExists
	}
	return err
}

func (s *PostgresStore) GetTopic(ctx context.Context, name string) (*Topic, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT name, partitions, replication_factor, retention_max_age_ms, retention_max_bytes,
			schema_json, schema_enabled, strategy, created_at, updated_at
		FROM topics WHERE name=$1`, name)
	return scanTopicPG(row)
}

func (s *PostgresStore) ListTopics(ctx context.Context) ([]*Topic, error) {
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
		t, err := scanTopicPG(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *PostgresStore) UpdateTopic(ctx context.Context, t *Topic) error {
	schemaJSON, _ := json.Marshal(t.Schema)
	res, err := s.db.ExecContext(ctx, `
		UPDATE topics SET partitions=$1, replication_factor=$2, retention_max_age_ms=$3,
			retention_max_bytes=$4, schema_json=$5, schema_enabled=$6, strategy=$7, updated_at=now()
		WHERE name=$8`,
		t.Partitions, t.ReplicationFactor, t.Retention.MaxAgeMS, t.Retention.MaxBytes,
		string(schemaJSON), t.SchemaEnabled, t.Strategy, t.Name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) DeleteTopic(ctx context.Context, name string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM topics WHERE name=$1`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func scanTopicPG(sc scanner) (*Topic, error) {
	var (
		t        Topic
		schemaJS string
		created  time.Time
		updated  time.Time
	)
	err := sc.Scan(&t.Name, &t.Partitions, &t.ReplicationFactor, &t.Retention.MaxAgeMS,
		&t.Retention.MaxBytes, &schemaJS, &t.SchemaEnabled, &t.Strategy, &created, &updated)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	_ = json.Unmarshal([]byte(schemaJS), &t.Schema)
	t.CreatedAt = created
	t.UpdatedAt = updated
	return &t, nil
}

// ---------------------------------------------------------------------------
// Brokers
// ---------------------------------------------------------------------------

func (s *PostgresStore) UpsertBroker(ctx context.Context, b *Broker) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO brokers (id, addr, status, last_seen) VALUES ($1,$2,$3,now())
		ON CONFLICT(id) DO UPDATE SET addr=EXCLUDED.addr, status=EXCLUDED.status, last_seen=now()`,
		b.ID, b.Addr, b.Status)
	return err
}

func (s *PostgresStore) ListBrokers(ctx context.Context) ([]*Broker, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, addr, status, last_seen FROM brokers ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Broker
	for rows.Next() {
		var b Broker
		if err := rows.Scan(&b.ID, &b.Addr, &b.Status, &b.LastSeen); err != nil {
			return nil, err
		}
		out = append(out, &b)
	}
	return out, rows.Err()
}

func (s *PostgresStore) TouchBroker(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE brokers SET last_seen=now(), status='up' WHERE id=$1`, id)
	return err
}

func (s *PostgresStore) SetBrokerStatus(ctx context.Context, id, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE brokers SET status=$1 WHERE id=$2`, status, id)
	return err
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

func (s *PostgresStore) UpsertGroup(ctx context.Context, g *Group) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO consumer_groups (id, generation, topic) VALUES ($1,$2,$3)
		ON CONFLICT(id) DO UPDATE SET topic=EXCLUDED.topic`,
		g.ID, g.Generation, g.Topic)
	return err
}

func (s *PostgresStore) GetGroup(ctx context.Context, id string) (*Group, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, generation, topic, created_at FROM consumer_groups WHERE id=$1`, id)
	var g Group
	if err := row.Scan(&g.ID, &g.Generation, &g.Topic, &g.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &g, nil
}

func (s *PostgresStore) ListGroups(ctx context.Context) ([]*Group, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, generation, topic, created_at FROM consumer_groups ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Group
	for rows.Next() {
		var g Group
		if err := rows.Scan(&g.ID, &g.Generation, &g.Topic, &g.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, &g)
	}
	return out, rows.Err()
}

func (s *PostgresStore) DeleteGroup(ctx context.Context, id string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM consumers WHERE "group"=$1`, id); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM offsets WHERE "group"=$1`, id); err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM consumer_groups WHERE id=$1`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) SetGroupGeneration(ctx context.Context, id string, gen int32) error {
	_, err := s.db.ExecContext(ctx, `UPDATE consumer_groups SET generation=$1 WHERE id=$2`, gen, id)
	return err
}

// ---------------------------------------------------------------------------
// Consumers
// ---------------------------------------------------------------------------

func (s *PostgresStore) UpsertConsumer(ctx context.Context, c *Consumer) error {
	assign, _ := json.Marshal(c.Assignment)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO consumers (id, "group", status, assignment, last_seen) VALUES ($1,$2,$3,$4,now())
		ON CONFLICT("group", id) DO UPDATE SET status=EXCLUDED.status, assignment=EXCLUDED.assignment, last_seen=now()`,
		c.ID, c.Group, c.Status, string(assign))
	return err
}

func (s *PostgresStore) ListConsumers(ctx context.Context, group string) ([]*Consumer, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, "group", status, assignment, last_seen FROM consumers WHERE "group"=$1 ORDER BY id`, group)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Consumer
	for rows.Next() {
		var c Consumer
		var assign string
		if err := rows.Scan(&c.ID, &c.Group, &c.Status, &assign, &c.LastSeen); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(assign), &c.Assignment)
		out = append(out, &c)
	}
	return out, rows.Err()
}

func (s *PostgresStore) SetConsumerAssignment(ctx context.Context, group, id string, assignment []int32) error {
	assign, _ := json.Marshal(assignment)
	_, err := s.db.ExecContext(ctx, `UPDATE consumers SET assignment=$1 WHERE "group"=$2 AND id=$3`, string(assign), group, id)
	return err
}

func (s *PostgresStore) TouchConsumer(ctx context.Context, group, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE consumers SET last_seen=now(), status='up' WHERE "group"=$1 AND id=$2`, group, id)
	return err
}

func (s *PostgresStore) SetConsumerStatus(ctx context.Context, group, id, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE consumers SET status=$1 WHERE "group"=$2 AND id=$3`, status, group, id)
	return err
}

func (s *PostgresStore) ExpireConsumers(ctx context.Context, olderThan time.Duration) (int, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE consumers SET status='down' WHERE last_seen < now() - $1 AND status='up'`, olderThan)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// ---------------------------------------------------------------------------
// Offsets
// ---------------------------------------------------------------------------

func (s *PostgresStore) CommitOffsets(ctx context.Context, group, topic string, offsets map[int32]int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for p, off := range offsets {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO offsets ("group", topic, partition, offset, updated_at) VALUES ($1,$2,$3,$4,now())
			ON CONFLICT("group", topic, partition) DO UPDATE SET offset=EXCLUDED.offset, updated_at=now()`,
			group, topic, p, off); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *PostgresStore) GetOffsets(ctx context.Context, group, topic string) (map[int32]int64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT partition, offset FROM offsets WHERE "group"=$1 AND topic=$2`, group, topic)
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

func (s *PostgresStore) InsertDLQ(ctx context.Context, e *DLQEntry) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO dlq_entries (id, topic, "group", original_topic, original_part, original_offset,
			key, payload, reason, retry_count, status)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		e.ID, e.Topic, e.Group, e.OriginalTopic, e.OriginalPart, e.OriginalOffset,
		e.Key, e.Payload, e.Reason, e.RetryCount, e.Status)
	return err
}

func (s *PostgresStore) ListDLQ(ctx context.Context, topic, group string, limit int) ([]*DLQEntry, error) {
	q := `SELECT id, topic, "group", original_topic, original_part, original_offset, key, payload, reason, retry_count, status, created_at FROM dlq_entries WHERE 1=1`
	var args []any
	n := 1
	if topic != "" {
		q += fmt.Sprintf(` AND topic=$%d`, n)
		args = append(args, topic)
		n++
	}
	if group != "" {
		q += fmt.Sprintf(` AND "group"=$%d`, n)
		args = append(args, group)
		n++
	}
	q += fmt.Sprintf(` ORDER BY created_at DESC LIMIT $%d`, n)
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*DLQEntry
	for rows.Next() {
		e, err := scanDLQPG(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *PostgresStore) GetDLQ(ctx context.Context, id string) (*DLQEntry, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, topic, "group", original_topic, original_part, original_offset, key, payload, reason, retry_count, status, created_at
		FROM dlq_entries WHERE id=$1`, id)
	e, err := scanDLQPG(row)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return e, err
}

func (s *PostgresStore) SetDLQStatus(ctx context.Context, id, status string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE dlq_entries SET status=$1 WHERE id=$2`, status, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func scanDLQPG(sc scanner) (*DLQEntry, error) {
	var e DLQEntry
	err := sc.Scan(&e.ID, &e.Topic, &e.Group, &e.OriginalTopic, &e.OriginalPart, &e.OriginalOffset,
		&e.Key, &e.Payload, &e.Reason, &e.RetryCount, &e.Status, &e.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &e, nil
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func (s *PostgresStore) CreateBenchmark(ctx context.Context, b *Benchmark) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO benchmarks (id, scenario, events, payload_bytes, producers, consumers, partitions, concurrency, status, started_at, finished_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,now(),$10)`,
		b.ID, b.Scenario, b.Events, b.PayloadBytes, b.Producers, b.Consumers, b.Partitions, b.Concurrency,
		b.Status, nullTime(b.FinishedAt))
	return err
}

func (s *PostgresStore) UpdateBenchmark(ctx context.Context, b *Benchmark) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE benchmarks SET status=$1, finished_at=$2 WHERE id=$3`,
		b.Status, nullTime(b.FinishedAt), b.ID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) GetBenchmark(ctx context.Context, id string) (*Benchmark, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, scenario, events, payload_bytes, producers, consumers, partitions, concurrency, status, started_at, finished_at
		FROM benchmarks WHERE id=$1`, id)
	return scanBenchmarkPG(row)
}

func (s *PostgresStore) ListBenchmarks(ctx context.Context, limit int) ([]*Benchmark, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, scenario, events, payload_bytes, producers, consumers, partitions, concurrency, status, started_at, finished_at
		FROM benchmarks ORDER BY started_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Benchmark
	for rows.Next() {
		b, err := scanBenchmarkPG(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *PostgresStore) InsertBenchmarkResult(ctx context.Context, r *BenchmarkResult) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO benchmark_results (id, run_id, metric, value, unit, labels) VALUES ($1,$2,$3,$4,$5,$6)`,
		r.ID, r.RunID, r.Metric, r.Value, r.Unit, r.Labels)
	return err
}

func (s *PostgresStore) ListBenchmarkResults(ctx context.Context, runID string) ([]*BenchmarkResult, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, run_id, metric, value, unit, labels, created_at FROM benchmark_results WHERE run_id=$1 ORDER BY created_at`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*BenchmarkResult
	for rows.Next() {
		var r BenchmarkResult
		if err := rows.Scan(&r.ID, &r.RunID, &r.Metric, &r.Value, &r.Unit, &r.Labels, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, &r)
	}
	return out, rows.Err()
}

func scanBenchmarkPG(sc scanner) (*Benchmark, error) {
	var b Benchmark
	var finished sql.NullTime
	if err := sc.Scan(&b.ID, &b.Scenario, &b.Events, &b.PayloadBytes, &b.Producers, &b.Consumers,
		&b.Partitions, &b.Concurrency, &b.Status, &b.StartedAt, &finished); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if finished.Valid {
		t := finished.Time
		b.FinishedAt = &t
	}
	return &b, nil
}

// ---------------------------------------------------------------------------
// Users / tokens / audit
// ---------------------------------------------------------------------------

func (s *PostgresStore) CreateUser(ctx context.Context, u *User) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO users (id, org_id, username, role, password) VALUES ($1,$2,$3,$4,$5)`,
		u.ID, u.OrgID, u.Username, u.Role, u.Password)
	if err != nil && (strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "23505")) {
		return ErrUserExists
	}
	return err
}

func (s *PostgresStore) GetUserByUsername(ctx context.Context, username string) (*User, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, org_id, username, role, password, created_at FROM users WHERE username=$1`, username)
	var u User
	if err := row.Scan(&u.ID, &u.OrgID, &u.Username, &u.Role, &u.Password, &u.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &u, nil
}

func (s *PostgresStore) GetUserByID(ctx context.Context, id string) (*User, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, org_id, username, role, password, created_at FROM users WHERE id=$1`, id)
	var u User
	if err := row.Scan(&u.ID, &u.OrgID, &u.Username, &u.Role, &u.Password, &u.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &u, nil
}

func (s *PostgresStore) EnsureOrganization(ctx context.Context, id, name string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO organizations (id, name) VALUES ($1,$2)
		ON CONFLICT(id) DO NOTHING`, id, name)
	return err
}

func (s *PostgresStore) ListUsers(ctx context.Context) ([]*User, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, org_id, username, role, password, created_at FROM users ORDER BY username`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.OrgID, &u.Username, &u.Role, &u.Password, &u.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, &u)
	}
	return out, rows.Err()
}

func (s *PostgresStore) CreateToken(ctx context.Context, t *Token) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO tokens (id, user_id, role, token, expires_at) VALUES ($1,$2,$3,$4,$5)`,
		t.ID, t.UserID, t.Role, t.Token, t.ExpiresAt)
	return err
}

func (s *PostgresStore) ValidateToken(ctx context.Context, token string) (*Token, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, user_id, role, token, expires_at, created_at FROM tokens WHERE token=$1 AND expires_at > now()`, token)
	var t Token
	if err := row.Scan(&t.ID, &t.UserID, &t.Role, &t.Token, &t.ExpiresAt, &t.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &t, nil
}

func (s *PostgresStore) RevokeToken(ctx context.Context, token string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM tokens WHERE token=$1`, token)
	return err
}

func (s *PostgresStore) InsertAudit(ctx context.Context, e *AuditEntry) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO audit_logs (id, user_id, action, target, detail) VALUES ($1,$2,$3,$4,$5)`,
		e.ID, e.UserID, e.Action, e.Target, e.Detail)
	return err
}

func (s *PostgresStore) ListAudit(ctx context.Context, limit int) ([]*AuditEntry, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, user_id, action, target, detail, created_at FROM audit_logs ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*AuditEntry
	for rows.Next() {
		var e AuditEntry
		if err := rows.Scan(&e.ID, &e.UserID, &e.Action, &e.Target, &e.Detail, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, &e)
	}
	return out, rows.Err()
}

func nullTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return *t
}
