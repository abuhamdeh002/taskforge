package job

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	ok := Job{Type: "email", State: StatePending, MaxRetries: 3, Payload: []byte("hi")}

	tests := []struct {
		name string
		job  Job
		want error
	}{
		{name: "valid", job: ok},
		{name: "empty type", job: Job{Type: "  ", State: StatePending}, want: ErrInvalidType},
		{name: "padded type", job: Job{Type: " email", State: StatePending}, want: ErrInvalidType},
		{
			name: "type too long",
			job:  Job{Type: strings.Repeat("a", MaxTypeLen+1), State: StatePending},
			want: ErrInvalidType,
		},
		{name: "bad state", job: Job{Type: "email", State: "nope"}, want: ErrInvalidState},
		{name: "negative retries", job: Job{Type: "email", State: StatePending, MaxRetries: -1}, want: ErrInvalidMaxRetries},
		{name: "negative attempts", job: Job{Type: "email", State: StatePending, Attempts: -1}, want: ErrInvalidAttempts},
		{
			name: "payload too large",
			job:  Job{Type: "email", State: StatePending, Payload: bytes.Repeat([]byte{'x'}, MaxPayload+1)},
			want: ErrPayloadTooLarge,
		},
		{
			name: "payload at limit",
			job:  Job{Type: "email", State: StatePending, Payload: bytes.Repeat([]byte{'x'}, MaxPayload)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.job.Validate()
			if tt.want == nil {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("Validate() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestCanTransition(t *testing.T) {
	all := []State{StatePending, StateRunning, StateSucceeded, StateFailed, StateCancelled}
	allowed := map[State][]State{
		StatePending:   {StatePending, StateRunning, StateCancelled},
		StateRunning:   {StateRunning, StateSucceeded, StateFailed, StatePending, StateCancelled},
		StateSucceeded: {StateSucceeded},
		StateFailed:    {StateFailed},
		StateCancelled: {StateCancelled},
	}

	for _, from := range all {
		for _, to := range all {
			want := false
			for _, ok := range allowed[from] {
				if ok == to {
					want = true
					break
				}
			}
			if got := from.CanTransition(to); got != want {
				t.Errorf("%s -> %s = %v, want %v", from, to, got, want)
			}
		}
	}

	if State("nope").CanTransition(StatePending) {
		t.Fatal("invalid state transitioned")
	}
}

func TestValidateNew(t *testing.T) {
	pending := Job{Type: "email", State: StatePending}
	if err := pending.ValidateNew(); err != nil {
		t.Fatalf("pending: %v", err)
	}

	running := pending
	running.State = StateRunning
	if err := running.ValidateNew(); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("running: %v", err)
	}

	started := pending
	started.Attempts = 1
	if err := started.ValidateNew(); !errors.Is(err, ErrInvalidAttempts) {
		t.Fatalf("attempts: %v", err)
	}
}

func TestStateValid(t *testing.T) {
	if !StatePending.Valid() || State("").Valid() {
		t.Fatal("Valid() mismatch")
	}
}
