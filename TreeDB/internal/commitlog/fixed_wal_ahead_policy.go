package commitlog

import "time"

const (
	// Amortize clock reads over physical progress. Include the same quantum in
	// the preparation lead so sampling cannot consume an unbudgeted interval.
	fixedWALAheadSampleBytes = int64(64 << 10)
	fixedWALAheadRateDecay   = time.Second
)

// fixedWALAheadPolicy is only a scheduling estimate, never publication authority.
// All state belongs to the journal mutex. The worker reports completed private
// preparation before waiting for adoption; residence time is not preparation.
// No per-command clock read, history allocation, or buffered-frame scan is needed.
type fixedWALAheadPolicy struct {
	current        *fixedWAL
	at             time.Time
	physical       int64
	rate           float64 // Physical bytes/ns; a recent peak decays with elapsed time.
	preparePerByte float64
	lastPrepare    time.Duration
	lead           int64
}

func (p *fixedWALAheadPolicy) prepared(capacity int64, elapsed time.Duration) {
	if capacity <= 0 || elapsed <= 0 {
		return
	}
	p.preparePerByte = max(p.preparePerByte, float64(elapsed)/float64(capacity))
	p.lastPrepare = elapsed
}

func (p *fixedWALAheadPolicy) leadBytes(capacity, incoming int64) int64 {
	// Scale measured work upward for the next capacity, retaining the latest
	// actual full duration as a floor. Two preparation budgets allow scheduling
	// and storage variability; an unobserved stall can still miss safely.
	budget := 2 * max(float64(p.lastPrepare), p.preparePerByte*float64(capacity))
	lead := p.rate*budget + float64(fixedWALAheadSampleBytes) + float64(incoming)
	if lead >= float64(int64(^uint64(0)>>1)) {
		return int64(^uint64(0) >> 1)
	}
	return int64(lead)
}

func (p *fixedWALAheadPolicy) shouldStart(current *fixedWAL, buffered, frameBytes, nextCapacity int64, now func() time.Time) bool {
	if current == nil {
		return false
	}
	end, err := fixedWALExtentEnd(current.physical, frameBytes)
	if err != nil || buffered < 0 {
		return true
	}
	incoming := end - current.physical
	remaining := current.capacity - current.physical
	// Reuse fixedFitsAppend's conservative admission bound, without multiplying
	// an unbounded value or counting accepted buffer bytes in the rate as well.
	if incoming > remaining || buffered > (remaining-incoming)/4 {
		return true
	}
	remaining -= 4 * buffered
	if p.current != current {
		t := now()
		if elapsed := t.Sub(p.at); !p.at.IsZero() && elapsed > 0 {
			p.rate /= 1 + float64(elapsed)/float64(fixedWALAheadRateDecay)
		}
		p.current, p.at, p.physical = current, t, current.physical
	}
	// Cold start keeps the former half-full lead. First successful ahead work
	// calibrates subsequent pressure segments without changing Writer ownership.
	if p.lastPrepare == 0 {
		return remaining <= (current.capacity-fixedWALHeaderSize+1)/2
	}
	// Predict only the exact shared pressure capacity; adoption still validates it.
	p.lead = p.leadBytes(nextCapacity, incoming)
	if current.physical-p.physical >= fixedWALAheadSampleBytes || remaining <= p.lead {
		t := now()
		if elapsed := t.Sub(p.at); elapsed > 0 {
			instant := float64(max(int64(0), current.physical-p.physical)) / float64(elapsed)
			p.rate = max(instant, p.rate/(1+float64(elapsed)/float64(fixedWALAheadRateDecay)))
			p.at, p.physical = t, current.physical
		}
	}
	p.lead = p.leadBytes(nextCapacity, incoming)
	return remaining <= p.lead
}
