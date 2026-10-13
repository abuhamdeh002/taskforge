package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/abuhamdeh002/taskforge/internal/job"
	"github.com/abuhamdeh002/taskforge/internal/store"
)

func TestParseFlags(t *testing.T) {
	cfg, err := parseFlags([]string{"-port", "9090", "-db", "queue.db", "-workers", "2", "-drain", "3s"})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.addr != ":9090" || cfg.dbPath != "queue.db" || cfg.workers != 2 || cfg.drain != 3*time.Second {
		t.Fatalf("cfg = %+v", cfg)
	}
	if _, err := parseFlags([]string{"-workers", "0"}); err == nil {
		t.Fatal("expected workers error")
	}
	if _, err := parseFlags([]string{"-port", "70000"}); err == nil {
		t.Fatal("expected port error")
	}
}

func TestRunRecoversAndServes(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "jobs.db")
	db, err := store.OpenSQLite(ctx, path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Create(ctx, job.Job{ID: "stuck", Type: "echo", Payload: []byte("hi")}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.ClaimNext(ctx); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	errCh := make(chan error, 1)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	go func() {
		errCh <- run(runCtx, config{
			addr:    "127.0.0.1:0",
			dbPath:  path,
			workers: 1,
			drain:   time.Second,
		}, log, func(addr string) {
			ready <- addr
		})
	}()

	var addr string
	select {
	case addr = <-ready:
	case err := <-errCh:
		t.Fatalf("run: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("server did not start")
	}

	health, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	defer health.Body.Close()
	if health.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d", health.StatusCode)
	}

	deadline := time.Now().Add(3 * time.Second)
	var body map[string]any
	for time.Now().Before(deadline) {
		res, err := http.Get("http://" + addr + "/jobs/stuck")
		if err != nil {
			t.Fatalf("get job: %v", err)
		}
		if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
			res.Body.Close()
			t.Fatalf("json: %v", err)
		}
		res.Body.Close()
		if body["state"] == string(job.StateSucceeded) {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if body["state"] != string(job.StateSucceeded) {
		t.Fatalf("stuck job state = %v, want succeeded after recovery", body["state"])
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown timed out")
	}
}
