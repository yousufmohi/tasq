package backoff

import (
	"math"
	"testing"
	"time"
)

func TestCeilingDoublesAndCaps(t *testing.T) {
	base, cap := time.Second, 10*time.Second
	want := []time.Duration{1, 2, 4, 8, 10, 10, 10}
	for i, w := range want {
		if got := Ceiling(i+1, base, cap); got != w*time.Second {
			t.Errorf("attempt %d: %v, want %v", i+1, got, w*time.Second)
		}
	}
	if got := Ceiling(0, base, cap); got != base {
		t.Errorf("attempt 0 treated as 1: got %v", got)
	}
}

func TestCeilingNoOverflow(t *testing.T) {
	cap := time.Duration(math.MaxInt64)
	if got := Ceiling(1000, time.Second, cap); got <= 0 {
		t.Fatalf("overflowed: %v", got)
	}
	if got := Ceiling(1<<30, time.Nanosecond, time.Hour); got != time.Hour {
		t.Fatalf("got %v, want cap", got)
	}
}

func TestFullStaysWithinWindow(t *testing.T) {
	base, cap := 100*time.Millisecond, 5*time.Second
	for attempt := 1; attempt <= 10; attempt++ {
		ceil := Ceiling(attempt, base, cap)
		lo := Full(attempt, base, cap, func(int64) int64 { return 0 })
		hi := Full(attempt, base, cap, func(n int64) int64 { return n - 1 })
		if lo != 0 || hi != ceil {
			t.Errorf("attempt %d: range [%v,%v], want [0,%v]", attempt, lo, hi, ceil)
		}
		for range 1000 {
			if d := Full(attempt, base, cap, nil); d < 0 || d > ceil {
				t.Fatalf("attempt %d: %v outside [0,%v]", attempt, d, ceil)
			}
		}
	}
}

// Full jitter should actually spread: the mean of many samples is near
// ceil/2, not clustered at either end.
func TestFullIsSpread(t *testing.T) {
	ceil := Ceiling(4, 100*time.Millisecond, time.Minute) // 800ms
	var sum time.Duration
	const n = 20000
	for range n {
		sum += Full(4, 100*time.Millisecond, time.Minute, nil)
	}
	mean := sum / n
	if mean < ceil*45/100 || mean > ceil*55/100 {
		t.Fatalf("mean %v not within 45-55%% of %v", mean, ceil)
	}
}

func TestZeroConfig(t *testing.T) {
	if d := Full(3, 0, time.Second, nil); d != 0 {
		t.Fatalf("zero base: %v", d)
	}
}
