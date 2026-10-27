package commitlog

import (
	"math"
	"testing"
	"time"
)

func TestFixedWALAheadDemandPolicy(t *testing.T) {
	newState := func() *fixedWAL { return &fixedWAL{capacity: 8 << 20, physical: fixedWALHeaderSize} }
	t.Run("cold_then_low_demand", func(t *testing.T) {
		s := newState()
		p := fixedWALAheadPolicy{}
		now := time.Unix(100, 0)
		clock := func() time.Time { return now }
		s.physical = 5 << 20
		if !p.shouldStart(s, 0, 1024, 16<<20, clock) {
			t.Fatal("cold start lost halfway lead")
		}
		p.prepared(8<<20, 10*time.Millisecond)
		if p.shouldStart(s, 0, 1024, 16<<20, clock) {
			t.Fatal("calibrated low demand reserves at halfway")
		}
		s.physical = s.capacity - 32<<10
		now = now.Add(time.Hour)
		if !p.shouldStart(s, 0, 1024, 16<<20, clock) {
			t.Fatal("did not reserve at sampling headroom")
		}
	})
	t.Run("physical_rate_and_latency", func(t *testing.T) {
		s := newState()
		p := fixedWALAheadPolicy{}
		p.prepared(8<<20, 100*time.Millisecond)
		now := time.Unix(100, 0)
		clock := func() time.Time { return now }
		p.shouldStart(s, 0, 1024, 16<<20, clock)
		s.physical += 1 << 20
		now = now.Add(10 * time.Millisecond)
		if !p.shouldStart(s, 0, 1024, 16<<20, clock) {
			t.Fatal("high demand missed early preparation")
		}
		if p.lead < 16<<20 {
			t.Fatalf("lead=%d", p.lead)
		}
	})
	t.Run("elapsed_idle_decays_without_byte_sample", func(t *testing.T) {
		s := newState()
		s.physical = 5 << 20
		now := time.Unix(100, 0)
		clock := func() time.Time { return now }
		p := fixedWALAheadPolicy{current: s, at: now, physical: s.physical, rate: 1}
		p.prepared(8<<20, 10*time.Millisecond)
		now = now.Add(time.Hour)
		if p.shouldStart(s, 0, 1024, 16<<20, clock) {
			t.Fatal("idle retained inflated lead")
		}
		if p.rate >= .001 {
			t.Fatalf("idle rate=%g", p.rate)
		}
	})
	t.Run("buffer_and_atomic_extent", func(t *testing.T) {
		s := newState()
		p := fixedWALAheadPolicy{}
		p.prepared(8<<20, time.Millisecond)
		clock := func() time.Time { return time.Unix(100, 0) }
		if p.shouldStart(s, 0, 1024, 16<<20, clock) {
			t.Fatal("empty reserves")
		}
		buffered := (s.capacity - s.physical - (32 << 10)) / 4
		if !p.shouldStart(s, buffered, 1024, 16<<20, clock) {
			t.Fatal("accepted buffer ignored")
		}
		if !p.shouldStart(s, 0, s.capacity, 16<<20, clock) {
			t.Fatal("oversized incoming ignored")
		}
		if !p.shouldStart(s, math.MaxInt64, 1024, 16<<20, clock) {
			t.Fatal("buffer overflow")
		}
	})
	t.Run("clock_amortized_and_rotation_cursor", func(t *testing.T) {
		s := newState()
		p := fixedWALAheadPolicy{}
		p.prepared(8<<20, time.Millisecond)
		now := time.Unix(100, 0)
		calls := 0
		clock := func() time.Time { calls++; return now }
		for i := 0; i < 100; i++ {
			p.shouldStart(s, 0, 64, 16<<20, clock)
			s.physical += 64
		}
		if calls != 1 {
			t.Fatalf("clock calls=%d", calls)
		}
		oldRate := p.rate
		next := newState()
		now = now.Add(time.Second)
		p.shouldStart(next, 0, 64, 16<<20, clock)
		if p.physical != next.physical || p.rate > oldRate {
			t.Fatal("rotation inherited physical cursor")
		}
	})
	t.Run("capacity_scaling_and_saturation", func(t *testing.T) {
		p := fixedWALAheadPolicy{rate: 1}
		p.prepared(8<<20, 10*time.Millisecond)
		if p.leadBytes(16<<20, 0) < 40*int64(time.Millisecond) {
			t.Fatal("next capacity not scaled")
		}
		p.rate = math.MaxFloat64
		if p.leadBytes(16<<20, 1024) != math.MaxInt64 {
			t.Fatal("lead overflow")
		}
	})
}

func TestFixedWALAheadCompletedPreparationObservation(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	j, err := openFixedTestCommandJournal(t.TempDir(), CommandJournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	aheadFill(t, j)
	aheadWait(t, j, func(s *fixedWALAheadSlot) bool { return s != nil && s.ready })
	j.mu.Lock()
	defer j.mu.Unlock()
	p := j.fixedAheadPolicy
	if p.lastPrepare <= 0 || p.preparePerByte <= 0 {
		t.Fatal("completed preparation not observed")
	}
	if j.fixedAhead.reserve.state.capacity != 2*j.writer.fixed.capacity {
		t.Fatal("shared successor capacity changed")
	}
}
