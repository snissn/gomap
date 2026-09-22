package caching

import "testing"

func TestComputeBackpressureThresholds_MaxBytesOnly(t *testing.T) {
	slow, stop, resume := computeBackpressureThresholds(backpressureParams{
		flushBps:           0,
		flushThreshold:     64,
		maxBacklogBytes:    1000,
		stopResumeFraction: 0.7,
	})
	if slow != 0 {
		t.Fatalf("slowdown=%d want 0", slow)
	}
	if stop != 1000 {
		t.Fatalf("stop=%d want 1000", stop)
	}
	if resume != 700 {
		t.Fatalf("resume=%d want 700", resume)
	}
}

func TestComputeBackpressureThresholds_Seconds(t *testing.T) {
	slow, stop, resume := computeBackpressureThresholds(backpressureParams{
		flushBps:               100,
		flushThreshold:         1,
		slowdownBacklogSeconds: 2,
		stopBacklogSeconds:     5,
		stopResumeFraction:     0.7,
	})
	if slow != 200 {
		t.Fatalf("slowdown=%d want 200", slow)
	}
	if stop != 500 {
		t.Fatalf("stop=%d want 500", stop)
	}
	if resume != 350 {
		t.Fatalf("resume=%d want 350", resume)
	}
}

func TestComputeBackpressureThresholds_MaxBytesCapsSeconds(t *testing.T) {
	_, stop, _ := computeBackpressureThresholds(backpressureParams{
		flushBps:           100,
		flushThreshold:     1,
		stopBacklogSeconds: 100,
		maxBacklogBytes:    1234,
		stopResumeFraction: 0.7,
	})
	if stop != 1234 {
		t.Fatalf("stop=%d want 1234", stop)
	}
}

func TestComputeBackpressureThresholds_StopClampsSlowdown(t *testing.T) {
	slow, stop, _ := computeBackpressureThresholds(backpressureParams{
		flushBps:               100,
		flushThreshold:         1,
		slowdownBacklogSeconds: 10, // 1000
		stopBacklogSeconds:     5,  // 500
		stopResumeFraction:     0.7,
	})
	if stop != 500 {
		t.Fatalf("stop=%d want 500", stop)
	}
	if slow != 500 {
		t.Fatalf("slowdown=%d want 500 (clamped to stop)", slow)
	}
}
