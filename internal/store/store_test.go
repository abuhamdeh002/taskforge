package store_test

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/abuhamdeh002/taskforge/internal/job"
	"github.com/abuhamdeh002/taskforge/internal/store"
)

// TestStore runs one suite against the in-memory store and SQLite.
func TestStore(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		runStoreSuite(t, func(t *testing.T) store.Store {
			t.Helper()
			return store.NewMemory()
		})
	})
	t.Run("sqlite", func(t *testing.T) {
		runStoreSuite(t, openSQLite)
	})
}

func runStoreSuite(t *testing.T, open func(t *testing.T) store.Store) {
	t.Helper()
	tests := []struct {
		name string
		fn   func(*testing.T, store.Store)
	}{
		{"create and get", testCreateAndGet},
		{"supplied id", testSuppliedID},
		{"reject invalid create", testRejectInvalidCreate},
		{"list filter and limit", testList},
		{"claim order", testClaimOrder},
		{"update", testUpdate},
		{"cancel", testCancel},
		{"copies are independent", testCopies},
		{"canceled context", testCanceledContext},
		{"run at", testRunAt},
		{"requeue running", testRequeueRunning},
		{"concurrent claim", testConcurrentClaim},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(t, open(t))
		})
	}
}

func testCreateAndGet(t *testing.T, s store.Store) {
	ctx := context.Background()
	created, err := s.Create(ctx, job.Job{
		Type:       "email",
		Payload:    []byte("hello"),
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if len(created.ID) != 36 {
		t.Fatalf("id = %q, want a UUID", created.ID)
	}
	if created.State != job.StatePending || created.Attempts != 0 || created.MaxRetries != 3 {
		t.Fatalf("created = %+v", created)
	}
	if created.CreatedAt.IsZero() || !created.UpdatedAt.Equal(created.CreatedAt) || !created.RunAt.Equal(created.CreatedAt) {
		t.Fatalf("timestamps created=%s updated=%s run_at=%s", created.CreatedAt, created.UpdatedAt, created.RunAt)
	}

	got, err := s.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	assertJob(t, got, created)

	if _, err := s.Get(ctx, "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("get missing: %v", err)
	}
}

func testSuppliedID(t *testing.T, s store.Store) {
	ctx := context.Background()
	created, err := s.Create(ctx, job.Job{ID: "job-1", Type: "email"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.ID != "job-1" {
		t.Fatalf("id = %q", created.ID)
	}

	_, err = s.Create(ctx, job.Job{ID: "job-1", Type: "email"})
	if !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("duplicate: %v", err)
	}

	_, err = s.Create(ctx, job.Job{ID: " has space", Type: "email"})
	if !errors.Is(err, store.ErrInvalidID) {
		t.Fatalf("bad id: %v", err)
	}
}

func testRejectInvalidCreate(t *testing.T, s store.Store) {
	ctx := context.Background()
	cases := []struct {
		name string
		job  job.Job
		want error
	}{
		{name: "empty type", job: job.Job{}, want: job.ErrInvalidType},
		{name: "not pending", job: job.Job{Type: "email", State: job.StateRunning}, want: job.ErrInvalidState},
		{name: "attempts", job: job.Job{Type: "email", Attempts: 2}, want: job.ErrInvalidAttempts},
		{
			name: "payload",
			job:  job.Job{Type: "email", Payload: bytes.Repeat([]byte{'x'}, job.MaxPayload+1)},
			want: job.ErrPayloadTooLarge,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.Create(ctx, tt.job)
			if !errors.Is(err, tt.want) {
				t.Fatalf("create error = %v, want %v", err, tt.want)
			}
		})
	}
}

func testList(t *testing.T, s store.Store) {
	ctx := context.Background()
	var ids []string
	for i := 0; i < 3; i++ {
		created, err := s.Create(ctx, job.Job{Type: "email", Payload: []byte{byte('a' + i)}})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		ids = append(ids, created.ID)
	}

	all, err := s.List(ctx, store.ListFilter{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 3 || all[0].ID != ids[0] || all[2].ID != ids[2] {
		t.Fatalf("list order = %s, want %s", jobIDs(all), ids)
	}

	page, err := s.List(ctx, store.ListFilter{Limit: 2})
	if err != nil {
		t.Fatalf("list limit: %v", err)
	}
	if len(page) != 2 || page[0].ID != ids[0] || page[1].ID != ids[1] {
		t.Fatalf("page = %s", jobIDs(page))
	}

	if _, err := s.ClaimNext(ctx); err != nil {
		t.Fatalf("claim: %v", err)
	}
	pending, err := s.List(ctx, store.ListFilter{State: job.StatePending})
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	if len(pending) != 2 || pending[0].ID != ids[1] {
		t.Fatalf("pending = %s", jobIDs(pending))
	}
	running, err := s.List(ctx, store.ListFilter{State: job.StateRunning})
	if err != nil {
		t.Fatalf("list running: %v", err)
	}
	if len(running) != 1 || running[0].ID != ids[0] || running[0].Attempts != 1 {
		t.Fatalf("running = %+v", running)
	}

	empty, err := s.List(ctx, store.ListFilter{State: job.StateFailed})
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("empty = %#v, want a non-nil empty slice", empty)
	}

	if _, err := s.List(ctx, store.ListFilter{State: "bogus"}); !errors.Is(err, job.ErrInvalidState) {
		t.Fatalf("bad filter: %v", err)
	}
}

func testClaimOrder(t *testing.T, s store.Store) {
	ctx := context.Background()
	var ids []string
	for i := 0; i < 3; i++ {
		created, err := s.Create(ctx, job.Job{Type: "email"})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		ids = append(ids, created.ID)
	}

	for i, id := range ids {
		got, err := s.ClaimNext(ctx)
		if err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		if got.ID != id || got.State != job.StateRunning || got.Attempts != 1 {
			t.Fatalf("claimed %+v, want id %s running attempts 1", got, id)
		}
	}
	if _, err := s.ClaimNext(ctx); !errors.Is(err, store.ErrNoJob) {
		t.Fatalf("empty claim: %v", err)
	}

	// A finished job must not be claimed again. A newer pending job is next.
	done, err := s.Get(ctx, ids[0])
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	done.State = job.StateSucceeded
	if err := s.Update(ctx, done); err != nil {
		t.Fatalf("update: %v", err)
	}
	fresh, err := s.Create(ctx, job.Job{Type: "email"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := s.ClaimNext(ctx)
	if err != nil {
		t.Fatalf("claim fresh: %v", err)
	}
	if got.ID != fresh.ID {
		t.Fatalf("claimed %s, want the new pending job %s", got.ID, fresh.ID)
	}
}

func testUpdate(t *testing.T, s store.Store) {
	ctx := context.Background()
	created, err := s.Create(ctx, job.Job{Type: "email", Payload: []byte("body"), MaxRetries: 2})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	created.State = job.StateSucceeded
	if err := s.Update(ctx, created); !errors.Is(err, job.ErrInvalidTransition) {
		t.Fatalf("skip running: %v", err)
	}

	claimed, err := s.ClaimNext(ctx)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	claimed.LastError = "still going"
	if err := s.Update(ctx, claimed); err != nil {
		t.Fatalf("same-state update: %v", err)
	}

	claimed.State = job.StateFailed
	claimed.LastError = "boom"
	if err := s.Update(ctx, claimed); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, err := s.Get(ctx, claimed.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.State != job.StateFailed || got.LastError != "boom" || got.Attempts != 1 {
		t.Fatalf("updated = %+v", got)
	}
	if !got.CreatedAt.Equal(created.CreatedAt) {
		t.Fatalf("created_at changed: %s -> %s", created.CreatedAt, got.CreatedAt)
	}
	if !bytes.Equal(got.Payload, []byte("body")) || got.MaxRetries != 2 {
		t.Fatalf("immutable fields changed: %+v", got)
	}

	changed := got
	changed.Type = "other"
	if err := s.Update(ctx, changed); !errors.Is(err, store.ErrImmutable) {
		t.Fatalf("change type: %v", err)
	}

	if err := s.Update(ctx, job.Job{ID: "missing", Type: "email", State: job.StatePending}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
}

func testCancel(t *testing.T, s store.Store) {
	ctx := context.Background()
	pending, err := s.Create(ctx, job.Job{Type: "email"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.Cancel(ctx, pending.ID); err != nil {
		t.Fatalf("cancel pending: %v", err)
	}
	if err := s.Cancel(ctx, pending.ID); err != nil {
		t.Fatalf("cancel again: %v", err)
	}
	got, err := s.Get(ctx, pending.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.State != job.StateCancelled {
		t.Fatalf("state = %s", got.State)
	}
	if _, err := s.ClaimNext(ctx); !errors.Is(err, store.ErrNoJob) {
		t.Fatalf("claim cancelled: %v", err)
	}

	running, err := s.Create(ctx, job.Job{Type: "email"})
	if err != nil {
		t.Fatalf("create running: %v", err)
	}
	claimed, err := s.ClaimNext(ctx)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.ID != running.ID {
		t.Fatalf("claimed %s", claimed.ID)
	}
	if err := s.Cancel(ctx, running.ID); err != nil {
		t.Fatalf("cancel running: %v", err)
	}

	claimed.State = job.StateSucceeded
	if err := s.Update(ctx, claimed); !errors.Is(err, job.ErrInvalidTransition) {
		t.Fatalf("finish cancelled job: %v", err)
	}

	done, err := s.Create(ctx, job.Job{Type: "email"})
	if err != nil {
		t.Fatalf("create done: %v", err)
	}
	finished, err := s.ClaimNext(ctx)
	if err != nil {
		t.Fatalf("claim done: %v", err)
	}
	finished.State = job.StateSucceeded
	if err := s.Update(ctx, finished); err != nil {
		t.Fatalf("succeed: %v", err)
	}
	if err := s.Cancel(ctx, done.ID); !errors.Is(err, store.ErrNotCancellable) {
		t.Fatalf("cancel succeeded: %v", err)
	}
	if err := s.Cancel(ctx, "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cancel missing: %v", err)
	}
}

func testCopies(t *testing.T, s store.Store) {
	ctx := context.Background()
	payload := []byte("abc")
	created, err := s.Create(ctx, job.Job{Type: "email", Payload: payload})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	payload[0] = 'Z'

	got, err := s.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got.Payload, []byte("abc")) {
		t.Fatalf("stored payload changed to %q", got.Payload)
	}
	got.Payload[0] = 'Q'
	got.Type = "changed"

	again, err := s.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("get again: %v", err)
	}
	if again.Type != "email" || !bytes.Equal(again.Payload, []byte("abc")) {
		t.Fatalf("get returned a live reference: %+v", again)
	}
}

func testRunAt(t *testing.T, s store.Store) {
	ctx := context.Background()
	future := time.Now().Add(time.Hour).UTC()
	delayed, err := s.Create(ctx, job.Job{Type: "email", RunAt: future})
	if err != nil {
		t.Fatalf("create delayed: %v", err)
	}
	if !delayed.RunAt.Equal(future) {
		t.Fatalf("run_at = %s, want %s", delayed.RunAt, future)
	}
	if _, err := s.ClaimNext(ctx); !errors.Is(err, store.ErrNoJob) {
		t.Fatalf("claim before run_at: %v", err)
	}

	ready, err := s.Create(ctx, job.Job{Type: "email"})
	if err != nil {
		t.Fatalf("create ready: %v", err)
	}
	got, err := s.ClaimNext(ctx)
	if err != nil {
		t.Fatalf("claim ready: %v", err)
	}
	if got.ID != ready.ID {
		t.Fatalf("claimed %s, want %s", got.ID, ready.ID)
	}
}

func testRequeueRunning(t *testing.T, s store.Store) {
	ctx := context.Background()
	first, err := s.Create(ctx, job.Job{Type: "email"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.Create(ctx, job.Job{Type: "email"}); err != nil {
		t.Fatalf("create second: %v", err)
	}
	claimed, err := s.ClaimNext(ctx)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.ID != first.ID || claimed.Attempts != 1 {
		t.Fatalf("claimed %+v", claimed)
	}
	n, err := s.RequeueRunning(ctx)
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if n != 1 {
		t.Fatalf("requeued %d, want 1", n)
	}
	got, err := s.Get(ctx, first.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.State != job.StatePending || got.Attempts != 1 {
		t.Fatalf("requeued job = %+v", got)
	}
	again, err := s.RequeueRunning(ctx)
	if err != nil || again != 0 {
		t.Fatalf("second requeue = %d, %v", again, err)
	}
}

func testCanceledContext(t *testing.T, s store.Store) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := s.Create(ctx, job.Job{Type: "email"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.Get(ctx, "id"); !errors.Is(err, context.Canceled) {
		t.Fatalf("get: %v", err)
	}
	if _, err := s.List(ctx, store.ListFilter{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("list: %v", err)
	}
	if _, err := s.ClaimNext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("claim: %v", err)
	}
	if err := s.Update(ctx, job.Job{ID: "id", Type: "email", State: job.StatePending}); !errors.Is(err, context.Canceled) {
		t.Fatalf("update: %v", err)
	}
	if err := s.Cancel(ctx, "id"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
}

func testConcurrentClaim(t *testing.T, s store.Store) {
	const (
		n       = 50
		workers = 8
	)
	ctx := context.Background()
	ids := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		created, err := s.Create(ctx, job.Job{Type: "work", Payload: []byte{byte(i)}})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		ids[created.ID] = struct{}{}
	}

	var (
		mu       sync.Mutex
		claimed  = make(map[string]int, n)
		claimErr error
	)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				got, err := s.ClaimNext(ctx)
				if errors.Is(err, store.ErrNoJob) {
					return
				}
				if err != nil {
					mu.Lock()
					if claimErr == nil {
						claimErr = err
					}
					mu.Unlock()
					return
				}
				mu.Lock()
				claimed[got.ID]++
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	if claimErr != nil {
		t.Fatalf("claim: %v", claimErr)
	}
	if len(claimed) != n {
		t.Fatalf("claimed %d jobs, want %d", len(claimed), n)
	}
	for id, count := range claimed {
		if _, ok := ids[id]; !ok {
			t.Errorf("claimed unknown id %s", id)
		}
		if count != 1 {
			t.Errorf("job %s claimed %d times", id, count)
		}
		got, err := s.Get(ctx, id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if got.State != job.StateRunning || got.Attempts != 1 {
			t.Errorf("job %s state=%s attempts=%d", id, got.State, got.Attempts)
		}
	}
}

func TestSQLitePersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "jobs.db")

	s, err := store.OpenSQLite(ctx, path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	created, err := s.Create(ctx, job.Job{ID: "keep", Type: "email", Payload: []byte("saved"), MaxRetries: 4})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := store.OpenSQLite(ctx, path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	got, err := reopened.Get(ctx, "keep")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	assertJob(t, got, created)
}

func TestOpenSQLiteRequiresPath(t *testing.T) {
	if _, err := store.OpenSQLite(context.Background(), ""); err == nil {
		t.Fatal("expected an error for an empty path")
	}
}

func openSQLite(t *testing.T) store.Store {
	t.Helper()
	s, err := store.OpenSQLite(context.Background(), filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close sqlite: %v", err)
		}
	})
	return s
}

func assertJob(t *testing.T, got, want job.Job) {
	t.Helper()
	if got.ID != want.ID || got.Type != want.Type || got.State != want.State ||
		got.Attempts != want.Attempts || got.MaxRetries != want.MaxRetries ||
		got.LastError != want.LastError || !bytes.Equal(got.Payload, want.Payload) ||
		!got.CreatedAt.Equal(want.CreatedAt) || !got.UpdatedAt.Equal(want.UpdatedAt) ||
		!got.RunAt.Equal(want.RunAt) {
		t.Fatalf("job mismatch\ngot  %+v payload=%q\nwant %+v payload=%q", got, got.Payload, want, want.Payload)
	}
}

func jobIDs(jobs []job.Job) []string {
	ids := make([]string, len(jobs))
	for i, j := range jobs {
		ids[i] = j.ID
	}
	return ids
}
