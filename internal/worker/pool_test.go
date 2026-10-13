package worker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abuhamdeh002/taskforge/internal/job"
	"github.com/abuhamdeh002/taskforge/internal/store"
)

func TestRegister(t *testing.T) {
	p := newTestPool(t, store.NewMemory(), 1)
	if err := p.Register("", Echo); !errors.Is(err, job.ErrInvalidType) {
		t.Fatalf("empty type: %v", err)
	}
	if err := p.Register("echo", nil); !errors.Is(err, errNilHandler) {
		t.Fatalf("nil handler: %v", err)
	}
	if err := p.Register("echo", Echo); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := p.Register("echo", Echo); !errors.Is(err, errAlreadyRegistered) {
		t.Fatalf("duplicate: %v", err)
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	if _, err := New(nil, 1); err == nil {
		t.Fatal("nil store")
	}
	if _, err := New(store.NewMemory(), 0); err == nil {
		t.Fatal("zero workers")
	}
	if _, err := New(store.NewMemory(), 1, nil); err == nil {
		t.Fatal("nil option")
	}
	if _, err := New(store.NewMemory(), 1, WithBaseDelay(time.Minute), WithMaxDelay(time.Second)); err == nil {
		t.Fatal("base above max")
	}
}

func TestEchoSucceeds(t *testing.T) {
	st := store.NewMemory()
	p := newTestPool(t, st, 2)
	if err := p.Register("echo", Echo); err != nil {
		t.Fatalf("register: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runPool(ctx, p)

	created := mustCreate(t, st, job.Job{Type: "echo", Payload: []byte("hi"), MaxRetries: 1})
	got := waitFor(t, st, created.ID, job.StateSucceeded)
	if got.Attempts != 1 || got.LastError != "" {
		t.Fatalf("job = %+v", got)
	}
	cancel()
	waitDone(t, done)
}

func TestRetryThenFail(t *testing.T) {
	st := store.NewMemory()
	p := newTestPool(t, st, 1)
	if err := p.Register("work", func(context.Context, job.Job) error {
		return errors.New("nope")
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runPool(ctx, p)

	created := mustCreate(t, st, job.Job{Type: "work", MaxRetries: 2})
	got := waitFor(t, st, created.ID, job.StateFailed)
	if got.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3 (1 try + 2 retries)", got.Attempts)
	}
	if got.LastError == "" {
		t.Fatal("missing last error")
	}
	cancel()
	waitDone(t, done)
}

func TestRetryWaitsUntilRunAt(t *testing.T) {
	st := store.NewMemory()
	p := mustPool(t, st, 1, WithBaseDelay(300*time.Millisecond), WithMaxDelay(time.Second), WithPollInterval(5*time.Millisecond))
	var calls atomic.Int32
	if err := p.Register("work", func(context.Context, job.Job) error {
		calls.Add(1)
		return errors.New("later")
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runPool(ctx, p)

	created := mustCreate(t, st, job.Job{Type: "work", MaxRetries: 3})
	deadline := time.Now().Add(time.Second)
	var got job.Job
	for time.Now().Before(deadline) {
		var err error
		got, err = st.Get(context.Background(), created.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.State == job.StatePending && got.Attempts == 1 {
			break
		}
		if got.State == job.StateFailed || got.Attempts > 1 {
			t.Fatalf("retried before the delay: %+v", got)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got.State != job.StatePending || got.Attempts != 1 {
		t.Fatalf("job = %+v", got)
	}
	// UpdatedAt is stamped inside Update, a moment after RunAt is chosen,
	// so compare against a bound below the minimum jittered delay (150ms).
	if got.RunAt.Sub(got.UpdatedAt) < 100*time.Millisecond {
		t.Fatalf("delay = %s, want at least 100ms", got.RunAt.Sub(got.UpdatedAt))
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d", calls.Load())
	}
	cancel()
	waitDone(t, done)
}

func TestJobTimeout(t *testing.T) {
	st := store.NewMemory()
	p := mustPool(t, st, 1, WithJobTimeout(30*time.Millisecond))
	if err := p.Register("slow", func(ctx context.Context, _ job.Job) error {
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runPool(ctx, p)

	created := mustCreate(t, st, job.Job{Type: "slow", MaxRetries: 0})
	got := waitFor(t, st, created.ID, job.StateFailed)
	if got.Attempts != 1 {
		t.Fatalf("attempts = %d", got.Attempts)
	}
	if !strings.Contains(got.LastError, "deadline exceeded") {
		t.Fatalf("last error = %q", got.LastError)
	}
	cancel()
	waitDone(t, done)
}

func TestUnknownTypeFailsWithoutRetry(t *testing.T) {
	st := store.NewMemory()
	p := newTestPool(t, st, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runPool(ctx, p)

	created := mustCreate(t, st, job.Job{Type: "missing", MaxRetries: 5})
	got := waitFor(t, st, created.ID, job.StateFailed)
	if got.Attempts != 1 || !strings.Contains(got.LastError, "unknown job type") {
		t.Fatalf("job = %+v", got)
	}
	cancel()
	waitDone(t, done)
}

func TestHandlerPanicIsFailure(t *testing.T) {
	st := store.NewMemory()
	p := newTestPool(t, st, 1)
	if err := p.Register("boom", func(context.Context, job.Job) error {
		panic("boom")
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runPool(ctx, p)

	created := mustCreate(t, st, job.Job{Type: "boom", MaxRetries: 0})
	got := waitFor(t, st, created.ID, job.StateFailed)
	if !strings.Contains(got.LastError, "boom") {
		t.Fatalf("last error = %q", got.LastError)
	}
	cancel()
	waitDone(t, done)
}

func TestWorkersDoNotDoubleHandle(t *testing.T) {
	st := store.NewMemory()
	p := newTestPool(t, st, 4)
	var mu sync.Mutex
	seen := map[string]int{}
	if err := p.Register("echo", func(_ context.Context, j job.Job) error {
		mu.Lock()
		seen[j.ID]++
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runPool(ctx, p)

	const n = 20
	ids := make([]string, n)
	for i := range ids {
		ids[i] = mustCreate(t, st, job.Job{Type: "echo"}).ID
	}
	for _, id := range ids {
		waitFor(t, st, id, job.StateSucceeded)
	}
	mu.Lock()
	if len(seen) != n {
		t.Fatalf("handled %d jobs, want %d", len(seen), n)
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("job %s handled %d times", id, count)
		}
	}
	mu.Unlock()
	cancel()
	waitDone(t, done)
}

func TestFlakyOutcomes(t *testing.T) {
	if err := flaky(context.Background(), false); err != nil {
		t.Fatalf("success path: %v", err)
	}
	if err := flaky(context.Background(), true); err == nil {
		t.Fatal("expected a failure")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := flaky(ctx, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
	if err := Echo(ctx, job.Job{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("echo canceled: %v", err)
	}
}

func newTestPool(t *testing.T, st store.Store, workers int, opts ...Option) *Pool {
	t.Helper()
	return mustPool(t, st, workers, opts...)
}

func mustPool(t *testing.T, st store.Store, workers int, opts ...Option) *Pool {
	t.Helper()
	opts = append([]Option{
		WithBaseDelay(0),
		WithMaxDelay(0),
		WithPollInterval(5 * time.Millisecond),
		WithJobTimeout(time.Second),
	}, opts...)
	p, err := New(st, workers, opts...)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	return p
}

func runPool(ctx context.Context, p *Pool) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- p.Run(ctx)
	}()
	return done
}

func waitDone(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pool did not stop")
	}
}

func mustCreate(t *testing.T, st store.Store, j job.Job) job.Job {
	t.Helper()
	got, err := st.Create(context.Background(), j)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return got
}

func waitFor(t *testing.T, st store.Store, id string, state job.State) job.Job {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var last job.Job
	for time.Now().Before(deadline) {
		got, err := st.Get(context.Background(), id)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		last = got
		if got.State == state {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s state = %s, want %s (last error %q)", id, last.State, state, last.LastError)
	return job.Job{}
}
