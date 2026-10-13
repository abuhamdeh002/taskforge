package store

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/abuhamdeh002/taskforge/internal/job"
)

// Memory is an in-process store. It keeps jobs in a map guarded by mu.
type Memory struct {
	mu   sync.Mutex
	next int64
	jobs map[string]memJob
}

type memJob struct {
	seq int64
	job job.Job
}

// NewMemory returns an empty store.
func NewMemory() *Memory {
	return &Memory{jobs: make(map[string]memJob)}
}

// Create inserts j. See Store.Create.
func (m *Memory) Create(ctx context.Context, j job.Job) (job.Job, error) {
	const op = "create job"
	if err := ctxErr(op, ctx); err != nil {
		return job.Job{}, err
	}
	prepared, err := prepareNew(j, time.Now())
	if err != nil {
		return job.Job{}, fmt.Errorf("%s: %w", op, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if prepared.ID == "" {
		id, err := newID()
		if err != nil {
			return job.Job{}, fmt.Errorf("%s: %w", op, err)
		}
		prepared.ID = id
	}
	if _, ok := m.jobs[prepared.ID]; ok {
		return job.Job{}, fmt.Errorf("%s: %w", op, ErrAlreadyExists)
	}

	m.next++
	stored := cloneJob(prepared)
	m.jobs[stored.ID] = memJob{seq: m.next, job: stored}
	return cloneJob(stored), nil
}

// Get returns a copy of the job. The caller can edit the payload
// without changing the stored job.
func (m *Memory) Get(ctx context.Context, id string) (job.Job, error) {
	const op = "get job"
	if err := ctxErr(op, ctx); err != nil {
		return job.Job{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.jobs[id]
	if !ok {
		return job.Job{}, fmt.Errorf("%s: %w", op, ErrNotFound)
	}
	return cloneJob(e.job), nil
}

// List returns a copy of each matching job, oldest insertion first.
func (m *Memory) List(ctx context.Context, filter ListFilter) ([]job.Job, error) {
	const op = "list jobs"
	if err := ctxErr(op, ctx); err != nil {
		return nil, err
	}
	if err := filter.validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	matched := make([]memJob, 0, len(m.jobs))
	for _, e := range m.jobs {
		if filter.State != "" && e.job.State != filter.State {
			continue
		}
		matched = append(matched, e)
	}
	sort.Slice(matched, func(i, k int) bool {
		return matched[i].seq < matched[k].seq
	})
	if len(matched) > filter.limit() {
		matched = matched[:filter.limit()]
	}

	out := make([]job.Job, len(matched))
	for i, e := range matched {
		out[i] = cloneJob(e.job)
	}
	return out, nil
}

// ClaimNext marks the oldest pending job running and increments Attempts.
//
// Atomicity: mu is held for the whole find-and-update. That is the same
// guarantee as a synchronized block. The goroutine that wins the lock sees
// a pending job, sets it to running, and only then releases the lock. Every
// other caller waits, then sees that job as running and skips it. There is
// no gap between "this job is pending" and "this job is running" that
// another goroutine can enter, so two workers cannot claim the same job.
func (m *Memory) ClaimNext(ctx context.Context) (job.Job, error) {
	const op = "claim next"
	if err := ctxErr(op, ctx); err != nil {
		return job.Job{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var (
		found bool
		best  memJob
	)
	for _, e := range m.jobs {
		if e.job.State != job.StatePending || e.job.RunAt.After(now) {
			continue
		}
		if !found || e.seq < best.seq {
			found = true
			best = e
		}
	}
	if !found {
		return job.Job{}, fmt.Errorf("%s: %w", op, ErrNoJob)
	}

	best.job.State = job.StateRunning
	best.job.Attempts++
	best.job.UpdatedAt = time.Now().UTC()
	m.jobs[best.job.ID] = best
	return cloneJob(best.job), nil
}

// Update writes state, attempts, and last error for an existing job.
func (m *Memory) Update(ctx context.Context, j job.Job) error {
	const op = "update job"
	if err := ctxErr(op, ctx); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.jobs[j.ID]
	if !ok {
		return fmt.Errorf("%s: %w", op, ErrNotFound)
	}
	updated, err := applyUpdate(e.job, j, time.Now())
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	e.job = updated
	m.jobs[j.ID] = e
	return nil
}

// Cancel moves a pending or running job to cancelled.
func (m *Memory) Cancel(ctx context.Context, id string) error {
	const op = "cancel job"
	if err := ctxErr(op, ctx); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.jobs[id]
	if !ok {
		return fmt.Errorf("%s: %w", op, ErrNotFound)
	}
	if e.job.State == job.StateCancelled {
		return nil
	}
	if !e.job.State.CanTransition(job.StateCancelled) {
		return fmt.Errorf("%s: %w", op, ErrNotCancellable)
	}
	e.job.State = job.StateCancelled
	e.job.UpdatedAt = time.Now().UTC()
	m.jobs[id] = e
	return nil
}
