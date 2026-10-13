// Package worker runs claimed jobs on a fixed set of goroutines.
package worker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/abuhamdeh002/taskforge/internal/job"
	"github.com/abuhamdeh002/taskforge/internal/store"
)

const (
	// DefaultJobTimeout bounds each handler call.
	DefaultJobTimeout = 30 * time.Second
	// DefaultPollInterval is how long a worker waits after an empty claim.
	DefaultPollInterval = 20 * time.Millisecond
	// DefaultBaseDelay is the first retry wait before jitter.
	DefaultBaseDelay = 100 * time.Millisecond
	// DefaultMaxDelay caps the exponential retry wait.
	DefaultMaxDelay = 30 * time.Second
)

var (
	errAlreadyRegistered = errors.New("handler already registered")
	errUnknownType       = errors.New("unknown job type")
	errNilHandler        = errors.New("nil handler")
)

// Option changes a pool built by New.
type Option func(*Pool) error

// WithJobTimeout sets the timeout passed to each handler.
func WithJobTimeout(d time.Duration) Option {
	return func(p *Pool) error {
		if d <= 0 {
			return fmt.Errorf("job timeout must be positive")
		}
		p.jobTimeout = d
		return nil
	}
}

// WithPollInterval sets how long a worker sleeps when no job is due.
func WithPollInterval(d time.Duration) Option {
	return func(p *Pool) error {
		if d <= 0 {
			return fmt.Errorf("poll interval must be positive")
		}
		p.poll = d
		return nil
	}
}

// WithBaseDelay sets the first retry wait. Zero retries as soon as the
// job is pending again. Jitter still applies when the delay is positive.
func WithBaseDelay(d time.Duration) Option {
	return func(p *Pool) error {
		if d < 0 {
			return fmt.Errorf("base delay must be >= 0")
		}
		p.base = d
		return nil
	}
}

// WithMaxDelay caps the exponential wait.
func WithMaxDelay(d time.Duration) Option {
	return func(p *Pool) error {
		if d < 0 {
			return fmt.Errorf("max delay must be >= 0")
		}
		p.maxDelay = d
		return nil
	}
}

// Pool claims jobs and runs the handler registered for each job type.
type Pool struct {
	store      store.Store
	workers    int
	jobTimeout time.Duration
	poll       time.Duration
	base       time.Duration
	maxDelay   time.Duration

	mu       sync.RWMutex
	handlers map[string]Handler
}

// New returns a pool with workers goroutines. The store must be safe for
// concurrent ClaimNext calls. Options override the default timeouts.
func New(st store.Store, workers int, opts ...Option) (*Pool, error) {
	if st == nil {
		return nil, errors.New("new worker pool: store is nil")
	}
	if workers < 1 {
		return nil, fmt.Errorf("new worker pool: worker count must be positive")
	}
	p := &Pool{
		store:      st,
		workers:    workers,
		jobTimeout: DefaultJobTimeout,
		poll:       DefaultPollInterval,
		base:       DefaultBaseDelay,
		maxDelay:   DefaultMaxDelay,
		handlers:   make(map[string]Handler),
	}
	for _, opt := range opts {
		if opt == nil {
			return nil, errors.New("new worker pool: nil option")
		}
		if err := opt(p); err != nil {
			return nil, fmt.Errorf("new worker pool: %w", err)
		}
	}
	if p.maxDelay > 0 && p.base > p.maxDelay {
		return nil, fmt.Errorf("new worker pool: base delay exceeds max delay")
	}
	return p, nil
}

// Register binds h to jobType. Registering the same type twice fails.
func (p *Pool) Register(jobType string, h Handler) error {
	if strings.TrimSpace(jobType) == "" || strings.TrimSpace(jobType) != jobType {
		return fmt.Errorf("register handler: %w", job.ErrInvalidType)
	}
	if h == nil {
		return fmt.Errorf("register handler: %w", errNilHandler)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.handlers[jobType]; ok {
		return fmt.Errorf("register handler: %w", errAlreadyRegistered)
	}
	p.handlers[jobType] = h
	return nil
}

// Run claims jobs until ctx is cancelled, then waits for the jobs already
// in a handler to finish. Cancelling ctx does not cancel those handlers.
// Each handler gets its own timeout context.
func (p *Pool) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("run worker pool: nil context")
	}
	var wg sync.WaitGroup
	for range p.workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.loop(ctx)
		}()
	}
	wg.Wait()
	return nil
}

func (p *Pool) loop(ctx context.Context) {
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		j, err := p.store.ClaimNext(ctx)
		if err == nil {
			p.dispatch(ctx, j)
			continue
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		if !errors.Is(err, store.ErrNoJob) {
			if !p.wait(ctx) {
				return
			}
			continue
		}
		if !p.wait(ctx) {
			return
		}
	}
}

func (p *Pool) wait(ctx context.Context) bool {
	timer := time.NewTimer(p.poll)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (p *Pool) dispatch(ctx context.Context, j job.Job) {
	h := p.handler(j.Type)
	if h == nil {
		_ = p.settle(ctx, j, fmt.Errorf("%w: %q", errUnknownType, j.Type))
		return
	}
	jobCtx, cancel := context.WithTimeout(context.Background(), p.jobTimeout)
	herr := call(jobCtx, h, j)
	cancel()
	_ = p.settle(ctx, j, herr)
}

func (p *Pool) handler(jobType string) Handler {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.handlers[jobType]
}

func call(ctx context.Context, h Handler, j job.Job) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("handler panic: %v", rec)
		}
	}()
	if err = h(ctx, j); err != nil {
		return fmt.Errorf("handle %s: %w", j.Type, err)
	}
	return nil
}

// settle writes the outcome. A handler error is retried while Attempts is
// still within MaxRetries: the job goes back to pending with a future RunAt.
// Attempts counts claims, so MaxRetries 0 fails on the first error and
// MaxRetries 3 allows three retries after the first failure.
func (p *Pool) settle(ctx context.Context, j job.Job, herr error) error {
	writeCtx := context.WithoutCancel(ctx)
	if herr == nil {
		j.State = job.StateSucceeded
		j.LastError = ""
		if err := p.store.Update(writeCtx, j); err != nil {
			return fmt.Errorf("settle job: %w", err)
		}
		return nil
	}
	j.LastError = herr.Error()
	if errors.Is(herr, errUnknownType) || j.Attempts > j.MaxRetries {
		j.State = job.StateFailed
		if err := p.store.Update(writeCtx, j); err != nil {
			return fmt.Errorf("settle job: %w", err)
		}
		return nil
	}
	j.State = job.StatePending
	j.RunAt = time.Now().UTC().Add(p.retryDelay(j.Attempts))
	if err := p.store.Update(writeCtx, j); err != nil {
		return fmt.Errorf("settle job: %w", err)
	}
	return nil
}

func (p *Pool) retryDelay(attempt int) time.Duration {
	exp := exponential(attempt, p.base, p.maxDelay)
	half := exp / 2
	if half <= 0 {
		return Delay(attempt, p.base, p.maxDelay, 0)
	}
	roll := time.Duration(rand.Int64N(int64(half) + 1))
	return Delay(attempt, p.base, p.maxDelay, roll)
}
