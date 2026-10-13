package worker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"

	"github.com/abuhamdeh002/taskforge/internal/job"
)

// Handler runs one job. A nil error marks the job succeeded.
// The context is cancelled when the job's timeout expires.
type Handler func(ctx context.Context, j job.Job) error

// Echo records a successful no-op. The payload is the message.
func Echo(ctx context.Context, _ job.Job) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("echo: %w", err)
	}
	return nil
}

// Flaky fails about half the time. The worker retries it like any other error.
func Flaky(ctx context.Context, _ job.Job) error {
	return flaky(ctx, rand.IntN(2) == 0)
}

func flaky(ctx context.Context, fail bool) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("flaky: %w", err)
	}
	if fail {
		return errors.New("flaky: temporary failure")
	}
	return nil
}
