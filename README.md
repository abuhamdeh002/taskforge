# taskforge

taskforge is a small persistent job queue. You submit a job over HTTP, a pool of workers claims it, and the result is stored in SQLite. If the process dies while a job is running, the next start puts that job back on the queue.

## Run

```bash
go run ./cmd/taskforge -port 8080 -db taskforge.db -workers 4
```

Flags:

| Flag | Default | Meaning |
| --- | --- | --- |
| `-port` | `8080` | HTTP port |
| `-db` | `taskforge.db` | SQLite file |
| `-workers` | `4` | Number of worker goroutines |
| `-drain` | `15s` | How long in-flight jobs may finish after Ctrl+C |

`make build` writes `bin/taskforge`. `make test` runs `go vet` and `go test -race`. `make run` starts the server with the defaults.

On PowerShell, `curl` is an alias for `Invoke-WebRequest`. The examples below use `curl.exe`.

## API

Create a job. `echo` succeeds. `flaky` fails about half the time and is retried.

```bash
curl.exe -s -X POST http://localhost:8080/jobs -H "Content-Type: application/json" -d "{\"type\":\"echo\",\"payload\":\"hello\",\"max_retries\":3}"
```

Example response:

```json
{
  "id": "6f1c0c2e-7b1a-4e0a-9c2d-1a2b3c4d5e6f",
  "type": "echo",
  "payload": "hello",
  "state": "pending",
  "attempts": 0,
  "max_retries": 3,
  "run_at": "2026-10-14T00:00:00Z",
  "created_at": "2026-10-14T00:00:00Z",
  "updated_at": "2026-10-14T00:00:00Z"
}
```

Fetch one job:

```bash
curl.exe -s http://localhost:8080/jobs/6f1c0c2e-7b1a-4e0a-9c2d-1a2b3c4d5e6f
```

List jobs. `status` is optional (`pending`, `running`, `succeeded`, `failed`, `cancelled`). `limit` defaults to 100 and is capped at 1000.

```bash
curl.exe -s "http://localhost:8080/jobs?status=pending&limit=10"
```

Cancel a pending or running job. Cancelling an already cancelled job returns 204. Cancelling a finished job returns 409.

```bash
curl.exe -s -X DELETE http://localhost:8080/jobs/6f1c0c2e-7b1a-4e0a-9c2d-1a2b3c4d5e6f
```

Health check:

```bash
curl.exe -s http://localhost:8080/healthz
```

## Design decisions

Claiming a job is one critical section. The in-memory store holds a mutex across the find and the state change. SQLite does it in one `UPDATE ... RETURNING` inside `BEGIN IMMEDIATE`, and the row must still be `pending`. Two workers cannot observe the same job as available.

`RunAt` is the earliest time a pending job may be claimed. A retry sets `RunAt` in the future and moves the job from `running` back to `pending`, so the wait survives a restart and does not occupy a worker.

The retry wait doubles each attempt and is capped. Jitter then keeps a value in the upper half of that wait, so retries do not all fire on the same tick. `max_retries` is the number of extra tries after the first failure. Zero means the first error fails the job.

Each handler runs with its own timeout context. Shutdown stops new claims immediately. In-flight handlers are left alone until they return, or until `-drain` expires, and then they are cancelled.

Startup moves every `running` job back to `pending` without incrementing its attempt count. The attempt that was in flight did not finish, so it gets to run again.

The only dependency outside the standard library is `modernc.org/sqlite`, which is a pure-Go SQLite driver. That keeps the build free of CGO except for `go test -race`.

## What I learned coming from Java/C#

I kept reaching for a class with getters. Go wants the fields exported on `Job`, and the store returns a copy so the caller cannot change the stored payload through the slice it still holds.

Errors are values. I return `fmt.Errorf("claim next: %w", ErrNoJob)` and the caller uses `errors.Is`, instead of throwing a custom exception type and catching it three frames up.

`context.Context` is the cancellation token, and it is the first argument of anything that blocks. There is no ambient transaction or request scope hiding in a thread local.

The worker pool is goroutines and a mutex, not an `ExecutorService` I configure with a thread factory. The mutex in the memory store is the synchronized block. SQLite's write lock is the other one, and it lives in the database rather than in the process.

Interfaces are satisfied without a `implements` clause. `Memory` and `SQLite` both match `Store` because their methods do. I check that with `var _ Store = (*Memory)(nil)` so a missing method fails at compile time.
