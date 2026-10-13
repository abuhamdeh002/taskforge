// Package store persists jobs. Memory is process-local. SQLite survives restarts.
package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/abuhamdeh002/taskforge/internal/job"
)

const (
	defaultListLimit = 100
	maxListLimit     = 1000
	maxIDLen         = 128
)

var (
	// ErrNotFound is returned when no job has the given id.
	ErrNotFound = errors.New("job not found")
	// ErrNoJob is returned when ClaimNext finds nothing pending.
	ErrNoJob = errors.New("no pending job")
	// ErrAlreadyExists is returned when Create is given an id that is already stored.
	ErrAlreadyExists = errors.New("job already exists")
	// ErrNotCancellable is returned when Cancel is called on a succeeded or failed job.
	ErrNotCancellable = errors.New("job cannot be cancelled")
	// ErrImmutable is returned when Update tries to change type, payload, or max retries.
	ErrImmutable = errors.New("job type, payload, and max retries are immutable")
	// ErrInvalidID is returned when an id is empty, too long, or contains whitespace.
	ErrInvalidID = errors.New("invalid job id")
	// ErrConflict is returned when the job changed between the read and the write.
	ErrConflict = errors.New("conflicting update")
)

// Store persists jobs. Implementations must be safe for concurrent use.
//
// ClaimNext is the only supported way to move a job from pending to running.
// Concurrent callers must each receive a distinct job.
type Store interface {
	// Create inserts a pending job. An empty ID is generated. An empty State
	// is treated as pending. CreatedAt and UpdatedAt are set by the store.
	Create(ctx context.Context, j job.Job) (job.Job, error)

	// Get returns a copy of the job with that id.
	Get(ctx context.Context, id string) (job.Job, error)

	// List returns jobs in insertion order, oldest first.
	List(ctx context.Context, filter ListFilter) ([]job.Job, error)

	// ClaimNext moves the oldest due pending job to running and increments Attempts.
	// A job is due when RunAt is zero or not after the current time.
	ClaimNext(ctx context.Context) (job.Job, error)

	// Update writes state, attempt count, last error, and RunAt.
	// A zero RunAt leaves the stored RunAt unchanged.
	// Type, payload, max retries, and CreatedAt stay as stored.
	// The state change must be a legal transition.
	Update(ctx context.Context, j job.Job) error

	// Cancel moves a pending or running job to cancelled.
	// Cancelling a job that is already cancelled succeeds.
	Cancel(ctx context.Context, id string) error

	// RequeueRunning moves every running job back to pending and returns how
	// many changed. A restart uses this so a crash does not leave a job
	// claimed forever. Attempts is left as stored.
	RequeueRunning(ctx context.Context) (int, error)
}

// Compile-time check that both implementations satisfy Store.
var (
	_ Store = (*Memory)(nil)
	_ Store = (*SQLite)(nil)
)

// ListFilter narrows List. A zero State means every state.
// Limit <= 0 uses the default of 100. Limits above 1000 are capped.
type ListFilter struct {
	State job.State
	Limit int
}

func (f ListFilter) validate() error {
	if f.State != "" && !f.State.Valid() {
		return fmt.Errorf("%w: %q", job.ErrInvalidState, f.State)
	}
	return nil
}

func (f ListFilter) limit() int {
	switch {
	case f.Limit <= 0:
		return defaultListLimit
	case f.Limit > maxListLimit:
		return maxListLimit
	default:
		return f.Limit
	}
}

func validID(id string) bool {
	if id == "" || len(id) > maxIDLen {
		return false
	}
	for _, r := range id {
		if r <= ' ' || r == 0x7f {
			return false
		}
	}
	return true
}

// prepareNew fills the fields the store owns and rejects a job that
// cannot be inserted.
func prepareNew(j job.Job, now time.Time) (job.Job, error) {
	if j.State == "" {
		j.State = job.StatePending
	}
	j.Payload = clonePayload(j.Payload)
	if err := j.ValidateNew(); err != nil {
		return job.Job{}, err
	}
	if j.ID != "" && !validID(j.ID) {
		return job.Job{}, ErrInvalidID
	}
	now = now.UTC()
	j.CreatedAt = now
	j.UpdatedAt = now
	if j.RunAt.IsZero() {
		j.RunAt = now
	} else {
		j.RunAt = j.RunAt.UTC()
	}
	j.LastError = ""
	return j, nil
}

// applyUpdate checks the transition and returns the record to store.
// Type, payload, max retries, id, and CreatedAt come from existing.
func applyUpdate(existing, next job.Job, now time.Time) (job.Job, error) {
	if !validID(next.ID) || next.ID != existing.ID {
		return job.Job{}, ErrInvalidID
	}
	if err := next.Validate(); err != nil {
		return job.Job{}, err
	}
	if existing.Type != next.Type || existing.MaxRetries != next.MaxRetries || !samePayload(existing.Payload, next.Payload) {
		return job.Job{}, ErrImmutable
	}
	if !existing.State.CanTransition(next.State) {
		return job.Job{}, fmt.Errorf("%w: %s -> %s", job.ErrInvalidTransition, existing.State, next.State)
	}
	existing.State = next.State
	existing.Attempts = next.Attempts
	existing.LastError = next.LastError
	if !next.RunAt.IsZero() {
		existing.RunAt = next.RunAt.UTC()
	}
	existing.UpdatedAt = now.UTC()
	return existing, nil
}

func clonePayload(p []byte) []byte {
	if len(p) == 0 {
		return []byte{}
	}
	return bytes.Clone(p)
}

func cloneJob(j job.Job) job.Job {
	j.Payload = clonePayload(j.Payload)
	return j
}

func samePayload(a, b []byte) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return bytes.Equal(a, b)
}

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	// UUID version 4, variant RFC 4122.
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

func ctxErr(op string, ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}
