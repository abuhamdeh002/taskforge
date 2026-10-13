package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/abuhamdeh002/taskforge/internal/job"
	"modernc.org/sqlite"
)

// SQLite is a file-backed store. OpenSQLite enables WAL so readers do not
// block the single writer, sets a busy timeout so a locked writer waits,
// and starts every transaction with BEGIN IMMEDIATE.
type SQLite struct {
	db *sql.DB
}

const jobColumns = "id, type, payload, state, attempts, max_retries, last_error, created_at, updated_at, run_at"

const createTableSQL = `
CREATE TABLE IF NOT EXISTS jobs (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    id          TEXT NOT NULL UNIQUE,
    type        TEXT NOT NULL,
    payload     BLOB NOT NULL,
    state       TEXT NOT NULL CHECK (state IN ('pending', 'running', 'succeeded', 'failed', 'cancelled')),
    attempts    INTEGER NOT NULL CHECK (attempts >= 0),
    max_retries INTEGER NOT NULL CHECK (max_retries >= 0),
    last_error  TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    run_at      INTEGER NOT NULL
)`

const createIndexSQL = `
CREATE INDEX IF NOT EXISTS idx_jobs_state_seq ON jobs (state, seq)`

// OpenSQLite opens path, creates the schema if needed, and returns a store
// that is safe for concurrent use. path is a filesystem path, not a URI.
func OpenSQLite(ctx context.Context, path string) (*SQLite, error) {
	const op = "open sqlite"
	if err := ctxErr(op, ctx); err != nil {
		return nil, err
	}
	if path == "" {
		return nil, fmt.Errorf("%s: path is required", op)
	}

	// More than one connection so concurrent ClaimNext calls actually
	// contend inside SQLite. _busy_timeout makes the loser wait instead
	// of failing with SQLITE_BUSY. _txlock=immediate makes Begin start
	// with the write lock already held.
	dsn := path + "?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)

	s := &SQLite{db: db}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := s.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return s, nil
}

func (s *SQLite) migrate(ctx context.Context) error {
	for _, stmt := range []string{createTableSQL, createIndexSQL} {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

// Close releases the database file.
func (s *SQLite) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("close sqlite: %w", err)
	}
	return nil
}

// Create inserts j. See Store.Create.
func (s *SQLite) Create(ctx context.Context, j job.Job) (job.Job, error) {
	const op = "create job"
	if err := ctxErr(op, ctx); err != nil {
		return job.Job{}, err
	}
	prepared, err := prepareNew(j, time.Now())
	if err != nil {
		return job.Job{}, fmt.Errorf("%s: %w", op, err)
	}
	if prepared.ID == "" {
		id, err := newID()
		if err != nil {
			return job.Job{}, fmt.Errorf("%s: %w", op, err)
		}
		prepared.ID = id
	}

	_, err = s.db.ExecContext(ctx, `
INSERT INTO jobs (id, type, payload, state, attempts, max_retries, last_error, created_at, updated_at, run_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		prepared.ID,
		prepared.Type,
		prepared.Payload,
		prepared.State,
		prepared.Attempts,
		prepared.MaxRetries,
		prepared.LastError,
		prepared.CreatedAt.UnixNano(),
		prepared.UpdatedAt.UnixNano(),
		prepared.RunAt.UnixNano(),
	)
	if isConstraint(err) {
		return job.Job{}, fmt.Errorf("%s: %w", op, ErrAlreadyExists)
	}
	if err != nil {
		return job.Job{}, fmt.Errorf("%s: %w", op, err)
	}
	return prepared, nil
}

// Get returns the job with id.
func (s *SQLite) Get(ctx context.Context, id string) (job.Job, error) {
	const op = "get job"
	if err := ctxErr(op, ctx); err != nil {
		return job.Job{}, err
	}
	j, err := getJob(ctx, s.db, id)
	if err != nil {
		return job.Job{}, fmt.Errorf("%s: %w", op, err)
	}
	return j, nil
}

// List returns matching jobs, oldest insertion first.
func (s *SQLite) List(ctx context.Context, filter ListFilter) ([]job.Job, error) {
	const op = "list jobs"
	if err := ctxErr(op, ctx); err != nil {
		return nil, err
	}
	if err := filter.validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT `+jobColumns+`
FROM jobs
WHERE (? = '' OR state = ?)
ORDER BY seq ASC
LIMIT ?`, string(filter.State), string(filter.State), filter.limit())
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	defer rows.Close()

	out := []job.Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return out, nil
}

// ClaimNext marks the oldest pending job running and increments Attempts.
//
// Atomicity: the read and the write are one UPDATE, inside a transaction
// opened with BEGIN IMMEDIATE (the DSN sets _txlock=immediate).
// BEGIN IMMEDIATE takes SQLite's reserved lock before the subquery reads,
// so a second connection cannot start its own claim until this transaction
// commits. The subquery then picks the oldest pending row and the UPDATE
// marks that same row running before the lock is released. The second
// connection's subquery therefore cannot return the row this claim took.
//
// AND state = 'pending' is a second guard. If the chosen row were no
// longer pending, the UPDATE would match nothing and ClaimNext would
// return ErrNoJob instead of handing that job to two workers. BEGIN
// IMMEDIATE already closes that window; the guard keeps the statement
// correct even if the lock mode changes.
func (s *SQLite) ClaimNext(ctx context.Context) (job.Job, error) {
	const op = "claim next"
	if err := ctxErr(op, ctx); err != nil {
		return job.Job{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return job.Job{}, fmt.Errorf("%s: %w", op, err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC()
	row := tx.QueryRowContext(ctx, `
UPDATE jobs
SET state = ?,
    attempts = attempts + 1,
    updated_at = ?
WHERE state = ?
  AND run_at <= ?
  AND seq = (
    SELECT seq FROM jobs
    WHERE state = ? AND run_at <= ?
    ORDER BY seq ASC
    LIMIT 1
  )
RETURNING `+jobColumns,
		job.StateRunning,
		now.UnixNano(),
		job.StatePending,
		now.UnixNano(),
		job.StatePending,
		now.UnixNano(),
	)
	j, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return job.Job{}, fmt.Errorf("%s: %w", op, ErrNoJob)
	}
	if err != nil {
		return job.Job{}, fmt.Errorf("%s: %w", op, err)
	}
	if err := tx.Commit(); err != nil {
		return job.Job{}, fmt.Errorf("%s: %w", op, err)
	}
	return j, nil
}

// Update writes state, attempts, and last error for an existing job.
func (s *SQLite) Update(ctx context.Context, j job.Job) error {
	const op = "update job"
	if err := ctxErr(op, ctx); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	defer func() { _ = tx.Rollback() }()

	existing, err := getJob(ctx, tx, j.ID)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	updated, err := applyUpdate(existing, j, time.Now())
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	res, err := tx.ExecContext(ctx, `
UPDATE jobs
SET state = ?, attempts = ?, last_error = ?, updated_at = ?, run_at = ?
WHERE id = ? AND state = ?`,
		updated.State,
		updated.Attempts,
		updated.LastError,
		updated.UpdatedAt.UnixNano(),
		updated.RunAt.UnixNano(),
		updated.ID,
		existing.State,
	)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if n == 0 {
		return fmt.Errorf("%s: %w", op, ErrConflict)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

// Cancel moves a pending or running job to cancelled.
func (s *SQLite) Cancel(ctx context.Context, id string) error {
	const op = "cancel job"
	if err := ctxErr(op, ctx); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	defer func() { _ = tx.Rollback() }()

	existing, err := getJob(ctx, tx, id)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if existing.State == job.StateCancelled {
		return nil
	}
	if !existing.State.CanTransition(job.StateCancelled) {
		return fmt.Errorf("%s: %w", op, ErrNotCancellable)
	}

	res, err := tx.ExecContext(ctx, `
UPDATE jobs
SET state = ?, updated_at = ?
WHERE id = ? AND state = ?`,
		job.StateCancelled,
		time.Now().UTC().UnixNano(),
		id,
		existing.State,
	)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if n == 0 {
		return fmt.Errorf("%s: %w", op, ErrConflict)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

type rowQuery interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func getJob(ctx context.Context, q rowQuery, id string) (job.Job, error) {
	j, err := scanJob(q.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return job.Job{}, ErrNotFound
	}
	if err != nil {
		return job.Job{}, err
	}
	return j, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanJob(sc scanner) (job.Job, error) {
	var (
		j                     job.Job
		state                 string
		payload               []byte
		created, updated, run int64
	)
	if err := sc.Scan(
		&j.ID,
		&j.Type,
		&payload,
		&state,
		&j.Attempts,
		&j.MaxRetries,
		&j.LastError,
		&created,
		&updated,
		&run,
	); err != nil {
		return job.Job{}, err
	}
	j.Payload = clonePayload(payload)
	j.State = job.State(state)
	j.CreatedAt = time.Unix(0, created).UTC()
	j.UpdatedAt = time.Unix(0, updated).UTC()
	j.RunAt = time.Unix(0, run).UTC()
	return j, nil
}

// SQLITE_CONSTRAINT is 19. Extended codes such as SQLITE_CONSTRAINT_UNIQUE
// keep that primary code in the low 8 bits.
func isConstraint(err error) bool {
	if err == nil {
		return false
	}
	var se *sqlite.Error
	if errors.As(err, &se) && se.Code()&0xff == 19 {
		return true
	}
	return false
}
