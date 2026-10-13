package worker

import "time"

// Delay is the wait before another attempt. attempt is the attempt that
// just failed, starting at 1. The wait doubles each attempt, is capped,
// then equal jitter keeps a value in [half, full] of that wait.
// roll is the random part and must be in [0, half]; values outside that
// range are clamped. A zero base waits not at all.
func Delay(attempt int, base, cap, roll time.Duration) time.Duration {
	return withJitter(exponential(attempt, base, cap), roll)
}

func exponential(attempt int, base, cap time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if base <= 0 {
		return 0
	}
	delay := base
	for n := 1; n < attempt; n++ {
		if cap > 0 && delay >= cap {
			return cap
		}
		if delay > maxDuration/2 {
			if cap > 0 && cap < maxDuration {
				return cap
			}
			return maxDuration
		}
		delay *= 2
	}
	if cap > 0 && delay > cap {
		return cap
	}
	return delay
}

func withJitter(delay, roll time.Duration) time.Duration {
	if delay <= 0 {
		return 0
	}
	half := delay / 2
	if roll < 0 {
		roll = 0
	}
	if roll > half {
		roll = half
	}
	return half + roll
}

const maxDuration = time.Duration(int64(^uint64(0) >> 1))
