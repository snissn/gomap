package compaction

import (
	"testing"
	"time"
)

func TestLimiter_DisabledReturnsImmediately(t *testing.T) {
	l := newLimiter(0, 0)
	start := time.Now()
	l.Wait(10_000_000)
	if time.Since(start) > 10*time.Millisecond {
		t.Fatalf("expected disabled limiter to return immediately")
	}
}

func TestLimiter_SleepsWhenExceedingBurst(t *testing.T) {
	// rate=1000 bytes/sec, burst defaults to 1000 bytes.
	// Waiting for 1100 bytes should incur ~100ms sleep.
	l := newLimiter(1000, 0)
	start := time.Now()
	l.Wait(1100)
	elapsed := time.Since(start)

	// Allow wide tolerance for scheduler jitter on loaded systems.
	if elapsed < 60*time.Millisecond {
		t.Fatalf("expected limiter to sleep, got %s", elapsed)
	}
	if elapsed > 750*time.Millisecond {
		t.Fatalf("unexpectedly long sleep: %s", elapsed)
	}
}
