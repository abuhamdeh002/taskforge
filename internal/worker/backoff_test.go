package worker

import (
	"testing"
	"time"
)

func TestDelay(t *testing.T) {
	ms := time.Millisecond
	tests := []struct {
		name                  string
		attempt               int
		base, cap, roll, want time.Duration
	}{
		{name: "first attempt, no extra jitter", attempt: 1, base: 100 * ms, cap: 10 * time.Second, roll: 0, want: 50 * ms},
		{name: "first attempt, full jitter", attempt: 1, base: 100 * ms, cap: 10 * time.Second, roll: 50 * ms, want: 100 * ms},
		{name: "second attempt doubles", attempt: 2, base: 100 * ms, cap: 10 * time.Second, roll: 0, want: 100 * ms},
		{name: "third attempt doubles", attempt: 3, base: 100 * ms, cap: 10 * time.Second, roll: 0, want: 200 * ms},
		{name: "fourth attempt doubles", attempt: 4, base: 100 * ms, cap: 10 * time.Second, roll: 25 * ms, want: 425 * ms},
		{name: "cap stops the growth", attempt: 10, base: 100 * ms, cap: 500 * ms, roll: 0, want: 250 * ms},
		{name: "zero base waits nothing", attempt: 5, base: 0, cap: time.Second, roll: time.Second, want: 0},
		{name: "attempt below 1 matches the first", attempt: 0, base: 100 * ms, cap: time.Second, roll: 0, want: 50 * ms},
		{name: "negative roll is clamped", attempt: 1, base: 100 * ms, cap: time.Second, roll: -1, want: 50 * ms},
		{name: "roll above half is clamped", attempt: 1, base: 100 * ms, cap: time.Second, roll: time.Second, want: 100 * ms},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Delay(tt.attempt, tt.base, tt.cap, tt.roll)
			if got != tt.want {
				t.Fatalf("Delay(%d, %s, %s, %s) = %s, want %s", tt.attempt, tt.base, tt.cap, tt.roll, got, tt.want)
			}
		})
	}
}
