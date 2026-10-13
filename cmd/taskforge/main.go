package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/abuhamdeh002/taskforge/internal/api"
	"github.com/abuhamdeh002/taskforge/internal/store"
	"github.com/abuhamdeh002/taskforge/internal/worker"
)

type config struct {
	addr    string
	dbPath  string
	workers int
	drain   time.Duration
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, log, nil); err != nil {
		log.Error("stopped", "err", err)
		os.Exit(1)
	}
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("taskforge", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	port := fs.Int("port", 8080, "HTTP port")
	dbPath := fs.String("db", "taskforge.db", "SQLite database path")
	workers := fs.Int("workers", 4, "number of workers")
	drain := fs.Duration("drain", 15*time.Second, "time allowed for in-flight jobs to finish on shutdown")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if *port < 0 || *port > 65535 {
		return config{}, fmt.Errorf("port %d is out of range", *port)
	}
	if *workers < 1 {
		return config{}, fmt.Errorf("workers must be positive")
	}
	if *drain < 0 {
		return config{}, fmt.Errorf("drain must be >= 0")
	}
	if *dbPath == "" {
		return config{}, fmt.Errorf("db path is required")
	}
	return config{
		addr:    fmt.Sprintf(":%d", *port),
		dbPath:  *dbPath,
		workers: *workers,
		drain:   *drain,
	}, nil
}

func run(ctx context.Context, cfg config, log *slog.Logger, ready func(addr string)) error {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	db, err := store.OpenSQLite(ctx, cfg.dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	n, err := db.RequeueRunning(ctx)
	if err != nil {
		return err
	}
	if n > 0 {
		log.Info("requeued running jobs", "count", n)
	}

	pool, err := worker.New(db, cfg.workers)
	if err != nil {
		return err
	}
	if err := pool.Register("echo", worker.Echo); err != nil {
		return err
	}
	if err := pool.Register("flaky", worker.Flaky); err != nil {
		return err
	}

	srv := &http.Server{
		Handler:           api.NewHandler(db, log),
		ReadHeaderTimeout: 5 * time.Second,
	}
	ln, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	if ready != nil {
		ready(ln.Addr().String())
	}
	log.Info("listening", "addr", ln.Addr().String())

	serveErr := make(chan error, 1)
	go func() {
		err := srv.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	claimCtx, cancelClaims := context.WithCancel(context.Background())
	defer cancelClaims()
	poolDone := make(chan struct{})
	go func() {
		_ = pool.Run(claimCtx)
		close(poolDone)
	}()

	select {
	case <-ctx.Done():
	case err := <-serveErr:
		cancelClaims()
		<-poolDone
		return err
	}

	drain := cfg.drain
	if drain == 0 {
		drain = time.Nanosecond
	}
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), drain)
	defer cancelDrain()
	cancelClaims()
	if err := srv.Shutdown(drainCtx); err != nil {
		log.Error("http shutdown", "err", err)
	}
	select {
	case <-poolDone:
	case <-drainCtx.Done():
		log.Info("drain expired, cancelling in-flight jobs")
		pool.StopHandlers()
		<-poolDone
	}
	return nil
}
