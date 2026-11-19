package caching

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCOWGenerationSealQueueDoesNotLoseSecondDuringFirst(t *testing.T) {
	var queue cowGenerationSealQueue
	now := time.Unix(10, 0)
	first := cowGenerationSealRequest{through: rootpublication.SealedPrefix{LogicalSequence: 1, CanonicalLSN: 2}}
	second := cowGenerationSealRequest{
		through: rootpublication.SealedPrefix{LogicalSequence: 3, CanonicalLSN: 5}, deadline: now.Add(time.Second),
	}
	if !queue.add(first) {
		t.Fatal("the first installed rotation was lost")
	}
	if pending, count := queue.next(now); pending != first || count != 1 {
		t.Fatalf("immediate first request = %+v count=%d", pending, count)
	}
	if !queue.add(second) || !queue.full() {
		t.Fatal("the second rotation was lost while the first was in flight")
	}
	queue.complete(1)
	for tick := now; tick.Before(second.deadline); tick = tick.Add(time.Millisecond) {
		if pending, count := queue.next(tick); pending != second || count != 0 {
			t.Fatalf("waiting second request = %+v count=%d, want its original deadline", pending, count)
		}
	}
	if pending, count := queue.next(second.deadline); pending != second || count != 1 {
		t.Fatalf("expired second request = %+v count=%d", pending, count)
	}
	queue.complete(1)
	if pending, count := queue.next(second.deadline); pending != (cowGenerationSealRequest{}) || count != 0 {
		t.Fatalf("completed requests still own scheduling slots: %+v count=%d", pending, count)
	}
}

func TestCOWGenerationSealRequiresOriginalBudgetHeadroom(t *testing.T) {
	limits := memtable.COWLimits{
		MaxInFlightBytes: 1024, MaxTotalBytes: 2048, MaxRetiredBytes: 1024,
		MaxGenerationBytes: 128, MaxSources: 4, MaxGenerations: 8,
	}
	baseline := memtable.COWStats{ReservedBytes: 640, TotalBytes: 1920, HistoryBytes: 200, DeferredBytes: 56, Sources: 3, Generations: 7}
	if !cowGenerationSealHasHeadroom(limits, baseline) {
		t.Fatal("one generation of space should be enough at each original boundary")
	}
	for _, probe := range []struct {
		name string
		bump func(*memtable.COWStats)
	}{
		{"original early parent threshold", func(s *memtable.COWStats) { s.ReservedBytes++ }},
		{"parent total", func(s *memtable.COWStats) { s.TotalBytes++ }},
		{"retirement history and deferred", func(s *memtable.COWStats) { s.DeferredBytes++ }},
		{"source slots", func(s *memtable.COWStats) { s.Sources++ }},
		{"generation slots", func(s *memtable.COWStats) { s.Generations++ }},
	} {
		t.Run(probe.name, func(t *testing.T) {
			stats := baseline
			probe.bump(&stats)
			if cowGenerationSealHasHeadroom(limits, stats) {
				t.Fatalf("can delay without another generation of headroom: %+v", stats)
			}
		})
	}
}

// Another maintenance pass can cover a scheduled request before it wakes.
// Compare against a nonzero real receipt so this exercises the stale path.
func TestCOWGenerationSealCoveredRequestReportsAcceptedError(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, true)
	cache.SetCommandWALCheckpointCutoverHook(func() {})
	writeCOWAutoFallbackRaw(t, cache, backend, "scheduled interval")
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	through := cache.cow.prefix.LogicalFrontiers().S
	if through.LogicalSequence == 0 || through.CanonicalLSN == 0 {
		t.Fatalf("missing original materialization receipt: %+v", through)
	}
	failure := errors.New("accepted prefix refusal after scheduled rotation was covered")
	cache.cow.prefix.PoisonAccepted(failure)
	if err := cache.materializeCOWPrefixThrough(context.Background(), false, nil, cowGenerationPressureMaterialization, through); !errors.Is(err, failure) {
		t.Fatalf("covered request lost the original accepted error: %v", err)
	}
	if err := cache.Close(); !errors.Is(err, failure) {
		t.Fatalf("checked Close lost the original accepted error: %v", err)
	}
}
