package compaction

import "time"

// limiter is a simple leaky-bucket rate limiter for copy IO.
type limiter struct {
	rateBytesPerSec int64
	last            time.Time
	allowance       int64
}

func newLimiter(rate int64) *limiter {
	if rate <= 0 {
		rate = defaultMaxCopyBPS
	}
	return &limiter{
		rateBytesPerSec: rate,
		last:            time.Now(),
		allowance:       rate,
	}
}

// Wait sleeps as needed to keep average throughput under rateBytesPerSec.
func (l *limiter) Wait(bytes int) {
	if l == nil || l.rateBytesPerSec <= 0 || bytes <= 0 {
		return
	}
	now := time.Now()
	elapsed := now.Sub(l.last)
	if elapsed > 0 {
		l.allowance += int64(float64(l.rateBytesPerSec) * elapsed.Seconds())
		if l.allowance > l.rateBytesPerSec {
			l.allowance = l.rateBytesPerSec
		}
	}

	need := int64(bytes)
	if need <= l.allowance {
		l.allowance -= need
		l.last = now
		return
	}

	deficit := need - l.allowance
	sleep := time.Duration(float64(deficit)/float64(l.rateBytesPerSec)*float64(time.Second))
	if sleep > 0 {
		time.Sleep(sleep)
	}
	l.allowance = 0
	l.last = time.Now()
}

