// Package job defines a unit of work and the states it can be in.
package job

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	// MaxTypeLen is the maximum length of Type, in bytes.
	MaxTypeLen = 128
	// MaxPayload is the maximum payload size accepted by Validate.
	MaxPayload = 1 << 20
)

// State is where a job is in its lifecycle.
//
//	pending -> running -> succeeded
//	                   -> failed
//	                   -> pending (retry; RunAt says when it may be claimed again)
//	pending or running -> cancelled
//
// succeeded, failed, and cancelled are terminal: they stay where they are.
type State string

const (
	StatePending   State = "pending"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateFailed    State = "failed"
	StateCancelled State = "cancelled"
)

// Valid reports whether s is one of the known states.
func (s State) Valid() bool {
	switch s {
	case StatePending, StateRunning, StateSucceeded, StateFailed, StateCancelled:
		return true
	default:
		return false
	}
}

// CanTransition reports whether a job in s may move to next.
// Staying in the same state is allowed so a caller can record a new
// last error without otherwise changing the job.
func (s State) CanTransition(next State) bool {
	if s == next {
		return true
	}
	switch s {
	case StatePending:
		return next == StateRunning || next == StateCancelled
	case StateRunning:
		return next == StateSucceeded || next == StateFailed || next == StatePending || next == StateCancelled
	default:
		return false
	}
}

var (
	ErrInvalidType       = errors.New("invalid job type")
	ErrInvalidState      = errors.New("invalid job state")
	ErrInvalidMaxRetries = errors.New("invalid max retries")
	ErrInvalidAttempts   = errors.New("invalid attempts")
	ErrInvalidTransition = errors.New("invalid state transition")
	ErrPayloadTooLarge   = errors.New("payload too large")
)

// Job is one unit of work. The store assigns ID and timestamps when
// they are left zero on create.
type Job struct {
	ID         string
	Type       string
	Payload    []byte
	State      State
	Attempts   int
	MaxRetries int
	LastError  string
	// RunAt is the earliest time a pending job may be claimed.
	// A zero RunAt on create is filled in by the store.
	RunAt     time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Validate checks the fields a caller is allowed to set.
// An empty ID is valid: the store generates one on create.
// A job that is already running or finished can be valid; use ValidateNew
// before inserting.
func (j Job) Validate() error {
	switch {
	case strings.TrimSpace(j.Type) == "" || strings.TrimSpace(j.Type) != j.Type:
		return ErrInvalidType
	case len(j.Type) > MaxTypeLen:
		return fmt.Errorf("%w: length %d", ErrInvalidType, len(j.Type))
	case !j.State.Valid():
		return fmt.Errorf("%w: %q", ErrInvalidState, j.State)
	case j.MaxRetries < 0:
		return fmt.Errorf("%w: %d", ErrInvalidMaxRetries, j.MaxRetries)
	case j.Attempts < 0:
		return fmt.Errorf("%w: %d", ErrInvalidAttempts, j.Attempts)
	case len(j.Payload) > MaxPayload:
		return fmt.Errorf("%w: %d bytes", ErrPayloadTooLarge, len(j.Payload))
	default:
		return nil
	}
}

// ValidateNew reports whether j can be inserted.
// State must be pending and Attempts must be zero.
func (j Job) ValidateNew() error {
	if err := j.Validate(); err != nil {
		return err
	}
	if j.State != StatePending {
		return fmt.Errorf("%w: new job must be pending", ErrInvalidState)
	}
	if j.Attempts != 0 {
		return fmt.Errorf("%w: new job must start at zero attempts", ErrInvalidAttempts)
	}
	return nil
}
