package caching

type backpressureParams struct {
	flushBps               float64
	flushThreshold         int64
	slowdownBacklogSeconds float64
	stopBacklogSeconds     float64
	maxBacklogBytes        int64
	stopResumeFraction     float64
}

const defaultLegacyBacklogBytes int64 = 256 * 1024 * 1024

func defaultMaxQueuedMemtables(flushThreshold int64) int {
	if flushThreshold <= 0 {
		return 4
	}
	n := int((defaultLegacyBacklogBytes + flushThreshold - 1) / flushThreshold)
	if n < 1 {
		n = 1
	}
	// Guard against pathological configurations where FlushThreshold is set
	// extremely small: queue length backpressure is cheap, but iterators may
	// degrade with thousands of in-memory sources. Keep the default bounded.
	if n > 1024 {
		n = 1024
	}
	return n
}

func computeBackpressureThresholds(p backpressureParams) (slowdownBytes, stopBytes, resumeBytes int64) {
	// Convert seconds -> bytes via the observed (EWMA) flush throughput.
	if p.flushBps > 0 {
		if p.slowdownBacklogSeconds > 0 {
			slowdownBytes = int64(p.flushBps * p.slowdownBacklogSeconds)
		}
		if p.stopBacklogSeconds > 0 {
			stopBytes = int64(p.flushBps * p.stopBacklogSeconds)
		}
	}

	// Absolute cap (safety net) always applies if set.
	if p.maxBacklogBytes > 0 {
		if stopBytes == 0 || stopBytes > p.maxBacklogBytes {
			stopBytes = p.maxBacklogBytes
		}
		if slowdownBytes > p.maxBacklogBytes {
			slowdownBytes = p.maxBacklogBytes
		}
	}

	// Clamp to at least one memtable when enabled, so the thresholds don't collapse
	// to 0 due to rounding or tiny durations.
	if p.flushThreshold > 0 {
		if slowdownBytes > 0 && slowdownBytes < p.flushThreshold {
			slowdownBytes = p.flushThreshold
		}
		if stopBytes > 0 && stopBytes < p.flushThreshold {
			stopBytes = p.flushThreshold
		}
	}

	// Ensure stop >= slowdown if both enabled.
	if stopBytes > 0 && slowdownBytes > stopBytes {
		slowdownBytes = stopBytes
	}

	if stopBytes > 0 {
		f := p.stopResumeFraction
		if f <= 0 || f >= 1 {
			f = 0.70
		}
		resumeBytes = int64(float64(stopBytes) * f)
		if resumeBytes >= stopBytes {
			resumeBytes = stopBytes - 1
		}
		if resumeBytes < 0 {
			resumeBytes = 0
		}
	}

	return slowdownBytes, stopBytes, resumeBytes
}
