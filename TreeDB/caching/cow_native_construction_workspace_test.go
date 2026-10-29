package caching

import (
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestCOWNativeConstructionWorkspaceWatermarks(t *testing.T) {
	for _, test := range []struct {
		name                string
		total, fixed, limit uint64
		pressure            bool
	}{
		{"original-below", 74, 0, 100, false},
		{"original-at", 75, 0, 100, true},
		{"shared-below", 84, 40, 100, false},
		{"shared-at", 85, 40, 100, true},
		{"odd-residual-below", 7, 2, 9, false},
		{"odd-residual-at", 8, 2, 9, true},
		{"zero-variable", 40, 40, 100, false},
		{"fixed-equals-limit", 100, 100, 100, true},
		{"fixed-exceeds-limit", 101, 101, 100, true},
		{"fixed-exceeds-total", 39, 40, 100, true},
		{"zero-limit", 1, 0, 0, true},
		{"overflow-below", ^uint64(0) - 3, ^uint64(0) - 7, ^uint64(0), false},
		{"overflow-at", ^uint64(0) - 1, ^uint64(0) - 7, ^uint64(0), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := nativeConstructionUnderPressure(test.total, test.fixed, test.limit); got != test.pressure {
				t.Fatalf("total=%d fixed=%d limit=%d pressure=%v want=%v", test.total, test.fixed, test.limit, got, test.pressure)
			}
		})
	}
	limits := memtable.DefaultCOWLimits()
	base := cowNativePressureState{semantic: backenddb.NativeSemanticProducerPressure{
		ConstructionBytes: 84, FixedConstructionBytes: 40,
		MaxInFlightBytes: 100, MaxTotalBytes: 100, Sources: 1, MaxSources: 8,
	}}
	if base.underPressure(limits) {
		t.Fatal("shared fixed workspace still fills variable watermark")
	}
	for _, operand := range []string{"in-flight", "total", "members", "reserved", "sources", "generations"} {
		t.Run(operand, func(t *testing.T) {
			s := base
			switch operand {
			case "in-flight":
				s.semantic.MaxInFlightBytes = 98
			case "total":
				s.semantic.MaxTotalBytes = 98
			case "members":
				s.semantic.Sources = s.semantic.MaxSources
			case "reserved":
				s.stats.ReservedBytes = limits.MaxInFlightBytes - limits.MaxInFlightBytes/4
			case "sources":
				s.stats.Sources = limits.MaxSources
			case "generations":
				s.stats.Generations = limits.MaxGenerations
			}
			if !s.underPressure(limits) {
				t.Fatal("shared workspace hid an independent pressure operand")
			}
		})
	}
}

// Exercise the real off-gate checkpoint bridge, including its unchanged
// no-progress identity. A fixed-term change must remain visible even when the
// full total, physical basis, logical sequence and raw budget are unchanged.
func TestCOWNativeConstructionWorkspaceCurrentIdentity(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, false)
	c := cache.cow
	pressure := backenddb.NativeSemanticProducerPressure{
		ConstructionBytes: 84, FixedConstructionBytes: 40,
		MaxInFlightBytes: 100, MaxTotalBytes: 100, Sources: 1, MaxSources: 8,
	}
	s := &constructionPressureTestState{testSemanticState: newTestSemanticState(nil)}
	s.observe = func(*backenddb.Snapshot) (backenddb.NativeSemanticProducerPressure, error) { return pressure, nil }
	c.cutMu.Lock()
	c.cut.semanticRootSet = s
	c.cutMu.Unlock()
	runs, next := cache.checkpointRuns.Load(), backend.CommandWALNextLSN()
	frontiers := c.prefix.LogicalFrontiers()
	before := c.budget.Stats()
	observation, err := backend.PrepareNativeProducerObservation()
	if err != nil {
		t.Fatal(err)
	}
	after := c.budget.Stats()
	before.PeakBytes, after.PeakBytes = 0, 0
	if observation.UnderPressure || observation.Semantic != pressure || before != after || cache.checkpointRuns.Load() != runs {
		t.Fatal("below variable watermark changed owners, inventory or checkpoint count")
	}
	pressure.ConstructionBytes = 85
	for i := 0; i < 3; i++ {
		if err := backend.PrepareNativeProducer(); err != nil {
			t.Fatal(err)
		}
	}
	if cache.checkpointRuns.Load() != runs+1 {
		t.Fatal("changed variable debt must drain once; unchanged debt must not repeat")
	}
	pressure.FixedConstructionBytes = 39
	if err := backend.PrepareNativeProducer(); err != nil {
		t.Fatal(err)
	}
	if cache.checkpointRuns.Load() != runs+2 {
		t.Fatal("fixed workspace omitted from current-state revalidation identity")
	}
	if backend.CommandWALNextLSN() != next || c.prefix.LogicalFrontiers() != frontiers {
		t.Fatal("advisory scheduling assigned or published logical work")
	}
}
