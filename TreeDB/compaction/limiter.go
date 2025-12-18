package compaction

import (
	"time"
)

// limiter is a minimal token-bucket limiter for compaction copy IO.
type limiter struct {
	rate  int64 // tokens per second
	burst int64 // max tokens

	tokens   float64
	lastTime time.Time
}

func newLimiter(bytesPerSec int64, burstBytes int64) *limiter {
	if bytesPerSec <= 0 {
		return &limiter{rate: 0}
	}
	if burstBytes <= 0 {
		burstBytes = bytesPerSec // 1s burst by default
	}
	return &limiter{
		rate:     bytesPerSec,
		burst:    burstBytes,
		tokens:   float64(burstBytes),
		lastTime: time.Now(),
	}
}

func (l *limiter) Wait(n int) {
	if l.rate <= 0 || n <= 0 {
		return
	}

	now := time.Now()
	elapsed := now.Sub(l.lastTime).Seconds()
	l.lastTime = now

	l.tokens += elapsed * float64(l.rate)
	if l.tokens > float64(l.burst) {
		l.tokens = float64(l.burst)
	}

	need := float64(n)
	if l.tokens >= need {
		l.tokens -= need
		return
	}

	deficit := need - l.tokens
	sleep := time.Duration(deficit / float64(l.rate) * float64(time.Second)) // ceil-ish not needed
	if sleep > 0 {
		time.Sleep(sleep)
	}
	l.tokens = 0
}
