package caching

import (
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type constructionPressureTestState struct {
	*testSemanticState
	observe func(*backenddb.Snapshot) (backenddb.NativeSemanticProducerPressure, error)
}

func (s *constructionPressureTestState) NativeProducerPressure(snapshot *backenddb.Snapshot) (backenddb.NativeSemanticProducerPressure, error) {
	return s.observe(snapshot)
}
func (s *constructionPressureTestState) PrepareRollover() (backenddb.NativeSemanticState, error) {
	s.Retain()
	return s, nil
}

func TestCOWNativeConstructionPressureThresholds(t *testing.T) {
	limits := memtable.DefaultCOWLimits()
	base := cowNativePressureState{semantic: backenddb.NativeSemanticProducerPressure{
		ConstructionBytes: 74, MaxInFlightBytes: 100, MaxTotalBytes: 200, Sources: 1, MaxSources: 8,
	}}
	if base.underPressure(limits) {
		t.Fatal("construction below watermark requested maintenance")
	}
	for _, kind := range []string{
		"in-flight", "total", "sources", "raw-bytes",
		"raw-total-bytes", "raw-retired-bytes", "raw-retired-overflow-sum1", "raw-retired-overflow-sum2",
		"raw-sources", "raw-generations",
	} {
		t.Run(kind, func(t *testing.T) {
			s := base
			switch kind {
			case "in-flight":
				s.semantic.ConstructionBytes = 75
			case "total":
				s.semantic.MaxTotalBytes = 96 // 96 - 96/4 == 72.
			case "sources":
				s.semantic.Sources = s.semantic.MaxSources
			case "raw-bytes":
				s.stats.ReservedBytes = limits.MaxInFlightBytes - limits.MaxInFlightBytes/4
				if s.nonReservationPressure(limits) || s.totalSoftPressure(limits) {
					t.Fatal("raw-bytes tripped non-reservation or total pressure")
				}
			case "raw-total-bytes":
				watermark := limits.MaxTotalBytes - limits.MaxTotalBytes/4
				s.stats.TotalBytes = watermark - 1
				if s.underPressure(limits) {
					t.Fatal("below total-bytes watermark requested maintenance")
				}
				s.stats.TotalBytes = watermark
				if !s.totalSoftPressure(limits) || s.totalHardPressure(limits) || s.reservationSoftPressure(limits) {
					t.Fatalf("unexpected total-bytes soft/hard state at watermark=%d", watermark)
				}
				s.stats.TotalBytes = limits.MaxTotalBytes - limits.MaxTotalBytes/8
				if !s.totalHardPressure(limits) {
					t.Fatal("hard total-bytes watermark did not trip totalHardPressure")
				}
				s.stats.TotalBytes = watermark
			case "raw-retired-bytes":
				watermark := limits.MaxRetiredBytes - limits.MaxRetiredBytes/4
				// Keep ReservedBytes well below MaxInFlightBytes*3/4 so reservationSoftPressure stays false.
				s.stats.HistoryBytes = watermark - 101
				s.stats.ReservedBytes = 50
				s.stats.DeferredBytes = 50
				if s.underPressure(limits) {
					t.Fatal("below retired-bytes watermark requested maintenance")
				}
				s.stats.DeferredBytes = 51
				if s.reservationSoftPressure(limits) {
					t.Fatal("test fixture accidentally tripped reservationSoftPressure")
				}
				if !s.totalSoftPressure(limits) || s.totalHardPressure(limits) {
					t.Fatal("retired-bytes watermark did not trip soft-only totalSoftPressure")
				}
				hardWatermark := limits.MaxRetiredBytes - limits.MaxRetiredBytes/8
				s.stats.HistoryBytes = hardWatermark - 101
				if !s.totalHardPressure(limits) {
					t.Fatal("hard retired-bytes watermark did not trip totalHardPressure")
				}
				s.stats.HistoryBytes = watermark - 101
			case "raw-retired-overflow-sum1":
				s.stats.HistoryBytes = ^uint64(0)
				s.stats.ReservedBytes = 1
				if s.reservationSoftPressure(limits) {
					t.Fatal("test fixture accidentally tripped reservationSoftPressure")
				}
				if !s.totalSoftPressure(limits) || !s.totalHardPressure(limits) {
					t.Fatal("HistoryBytes+ReservedBytes wrap-around did not trip total pressure")
				}
			case "raw-retired-overflow-sum2":
				s.stats.HistoryBytes = 1
				s.stats.ReservedBytes = 0
				s.stats.DeferredBytes = ^uint64(0)
				if s.reservationSoftPressure(limits) {
					t.Fatal("test fixture accidentally tripped reservationSoftPressure")
				}
				if !s.totalSoftPressure(limits) || !s.totalHardPressure(limits) {
					t.Fatal("used+DeferredBytes wrap-around did not trip total pressure")
				}
			case "raw-sources":
				s.stats.Sources = limits.MaxSources
			case "raw-generations":
				s.stats.Generations = limits.MaxGenerations
			}
			if !s.underPressure(limits) {
				t.Fatal("independent pressure limit ignored")
			}
		})
	}
	if (cowNativePressureState{}).underPressure(limits) {
		t.Fatal("optional zero inventory requested maintenance")
	}
	// Match the existing subtraction policy even when the limit is not a
	// multiple of four, and avoid overflow near the largest finite limit.
	for _, limit := range []uint64{1, 3, 5, ^uint64(0)} {
		s := base
		s.semantic.MaxInFlightBytes, s.semantic.MaxTotalBytes = limit, limit
		watermark := limit - limit/4
		s.semantic.ConstructionBytes = watermark
		if !s.underPressure(limits) {
			t.Fatalf("watermark=%d limit=%d", watermark, limit)
		}
		s.semantic.ConstructionBytes = watermark - 1
		if s.underPressure(limits) {
			t.Fatalf("below watermark=%d limit=%d", watermark, limit)
		}
	}
}

func TestCOWNativeConstructionPressureBorrowsExactCutOffLatch(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			cache, backend := nativeBridgeFixture(t, false)
			c := cache.cow
			cut := c.cut
			refs := cut.refs.Load()
			before := c.budget.Stats()
			failure := errors.New("pressure inventory failure")
			calls := 0
			s := &constructionPressureTestState{testSemanticState: newTestSemanticState(nil)}
			s.observe = func(snapshot *backenddb.Snapshot) (backenddb.NativeSemanticProducerPressure, error) {
				calls++
				if !c.cutMu.TryLock() {
					t.Fatal("participant observation under cut latch")
				}
				paired := snapshot == cut.basis.snapshot && cut.refs.Load() == refs+1
				c.cutMu.Unlock()
				if !paired {
					t.Fatal("observation replaced basis or did not retain original cut")
				}
				if _, valid := snapshot.StateToken(); !valid {
					t.Fatal("borrowed basis ended during observation")
				}
				if fail {
					return backenddb.NativeSemanticProducerPressure{}, failure
				}
				return backenddb.NativeSemanticProducerPressure{}, nil
			}
			c.cutMu.Lock()
			cut.semanticRootSet = s
			c.cutMu.Unlock()
			runs := cache.checkpointRuns.Load()
			err := backend.PrepareNativeProducer()
			if fail && !errors.Is(err, failure) || !fail && err != nil {
				t.Fatalf("observer error=%v", err)
			}
			if calls != 1 || cut.refs.Load() != refs || cache.checkpointRuns.Load() != runs || c.nativePressureRemembered {
				t.Fatal("low/error observation retained an edge, checkpointed, or remembered a drain")
			}
			after := c.budget.Stats()
			before.PeakBytes, after.PeakBytes = 0, 0
			if before != after {
				t.Fatalf("observer allocated a second capture owner: before=%+v after=%+v", before, after)
			}
		})
	}
}

func TestCOWNativeConstructionPressureOneDrainAndCurrentIdentity(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, false)
	c := cache.cow
	pressure := backenddb.NativeSemanticProducerPressure{ConstructionBytes: 75, MaxInFlightBytes: 100, MaxTotalBytes: 100, Sources: 1, MaxSources: 8}
	s := &constructionPressureTestState{testSemanticState: newTestSemanticState(nil)}
	s.observe = func(*backenddb.Snapshot) (backenddb.NativeSemanticProducerPressure, error) { return pressure, nil }
	c.cutMu.Lock()
	c.cut.semanticRootSet = s
	c.cutMu.Unlock()
	runs, next := cache.checkpointRuns.Load(), backend.CommandWALNextLSN()
	frontiers := c.prefix.LogicalFrontiers()
	if err := backend.PrepareNativeProducer(); err != nil {
		t.Fatal(err)
	}
	if cache.checkpointRuns.Load() != runs+1 || !c.nativePressureRemembered {
		t.Fatal("current constructor debt did not request one existing checkpoint")
	}
	if backend.CommandWALNextLSN() != next || c.prefix.LogicalFrontiers() != frontiers {
		t.Fatal("advisory maintenance assigned or published logical work")
	}
	for i := 0; i < 3; i++ {
		if err := backend.PrepareNativeProducer(); err != nil {
			t.Fatal(err)
		}
	}
	if cache.checkpointRuns.Load() != runs+1 {
		t.Fatal("unchanged retained constructor debt repeated no-op maintenance")
	}
	// Change only the semantic scalar signature, preserving raw statistics and
	// logical sequence. This fixture emulates a different immutable inventory.
	pressure.ConstructionBytes++
	if err := backend.PrepareNativeProducer(); err != nil {
		t.Fatal(err)
	}
	if cache.checkpointRuns.Load() != runs+2 {
		t.Fatal("changed current construction debt hidden by no-progress memory")
	}
	before := c.nativePressureState
	after := before
	after.basis.CommitSeq++
	if before == after {
		t.Fatal("physical basis identity excluded from pressure memory")
	}
}

func TestCOWNativeProducerWithoutPressureHookKeepsOriginalOwners(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, false)
	c := cache.cow
	for _, semantic := range []bool{false, true} {
		if semantic {
			c.cutMu.Lock()
			c.cut.semanticRootSet = newTestSemanticState(nil) // No optional hook.
			c.cutMu.Unlock()
		}
		cut, refs, runs := c.cut, c.cut.refs.Load(), cache.checkpointRuns.Load()
		before := c.budget.Stats()
		for i := 0; i < 3; i++ {
			if err := backend.PrepareNativeProducer(); err != nil {
				t.Fatal(err)
			}
		}
		after := c.budget.Stats()
		before.PeakBytes, after.PeakBytes = 0, 0
		if before != after || c.cut != cut || cut.refs.Load() != refs || cache.checkpointRuns.Load() != runs {
			t.Fatal("plain/nonparticipating producer added capture ownership or maintenance")
		}
	}
}
