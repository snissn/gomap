package db

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/zipper"
)

type ordinaryCensusCapacityBudget struct {
	sync.Mutex
	limit, bytes, refusals, refusedGrowth uint64
	refusal                               error
}

type ordinaryCensusCapacityLease struct {
	budget *ordinaryCensusCapacityBudget
	bytes  uint64
}

func (b *ordinaryCensusCapacityBudget) configure(limit uint64, refusal error) {
	b.Lock()
	defer b.Unlock()
	b.limit, b.refusal = limit, refusal
}

func (b *ordinaryCensusCapacityBudget) snapshot() (bytes, refusals, refusedGrowth uint64) {
	b.Lock()
	defer b.Unlock()
	return b.bytes, b.refusals, b.refusedGrowth
}

func (b *ordinaryCensusCapacityBudget) AcquireRetention(n uint64) (retainedalloc.Lease, error) {
	l := &ordinaryCensusCapacityLease{budget: b}
	if err := l.Resize(n); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *ordinaryCensusCapacityLease) Resize(n uint64) error {
	b := l.budget
	b.Lock()
	defer b.Unlock()
	if n > l.bytes && n-l.bytes > b.limit-b.bytes {
		b.refusals++
		b.refusedGrowth = n - l.bytes
		return b.refusal
	}
	b.bytes = b.bytes - l.bytes + n
	l.bytes = n
	return nil
}

func (l *ordinaryCensusCapacityLease) Close() {
	b := l.budget
	b.Lock()
	defer b.Unlock()
	b.bytes -= l.bytes
	l.bytes = 0
}

// Exercise the closure-capture caller with a real successful zipper result.
// The old tree references 256 persistent segments; the private candidate
// overwrites those values inline. Its original full scan counts only the final
// raw-leaf segment, while copying the optional old logical census exceeds the
// same finite cap after admitting the temporary raw census clone.
func TestOrdinaryCensusCapacityFallsBackToOriginalCapture(t *testing.T) {
	testOrdinaryCensusCapacityFallsBackToOriginalCapture(t, false)
}

func TestOrdinaryCOWCensusCapacityFallsBackToOriginalCapture(t *testing.T) {
	testOrdinaryCensusCapacityFallsBackToOriginalCapture(t, true)
}

func testOrdinaryCensusCapacityFallsBackToOriginalCapture(t *testing.T, realCOW bool) {
	d, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	log := &registeredLeafPageLog{db: d, dir: d.dir}
	if err := log.ensureWriter(); err != nil {
		t.Fatal(err)
	}
	d.SetLeafPageLog(log)
	defer closeVacuumTestLeafPageLog(t, d, log)
	seed := d.NewBatch().(*Batch)
	defer seed.Close()
	update := d.NewBatch().(*Batch)
	defer update.Close()
	const segments = 256
	for i := 0; i < segments; i++ {
		_, ptr := writeValueLogRecord(t, d.dir, 0, uint32(i+1), []byte("persistent predecessor value"), uint64(i+1))
		key := []byte(fmt.Sprintf("key/%02d", i))
		if err := seed.SetPointer(key, ptr); err != nil {
			t.Fatal(err)
		}
		if err := update.Set(key, []byte("inline")); err != nil {
			t.Fatal(err)
		}
	}
	if err := seed.WriteSync(); err != nil {
		t.Fatal(err)
	}
	// The first insertion may use predecessor membership without scanning.
	// An overwrite establishes the ordinary raw census from the real closure.
	if err := seed.WriteSync(); err != nil {
		t.Fatal(err)
	}
	snapshot := d.AcquireSnapshot()
	defer snapshot.Close()
	allocations := newAllocTracker(snapshot.idx.allocator)
	defer func() {
		if err := allocations.FreeAll(); err != nil {
			t.Error(err)
		}
	}()
	z := snapshot.idx.zipper.CloneWithAllocator(allocations)
	result, err := z.ApplyWithOptions(snapshot.state.RootPageID, update.batch, zipper.ApplyOptions{CollectOldPointerRefs: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Flush(); err != nil {
		t.Fatal(err)
	}
	entries, ranges := update.batch.ApplyPlan()
	delta, err := d.buildValueLogRefDeltaForApply(snapshot.idx, snapshot.state.RootPageID, result.RootID, snapshot.state.CommitSeq, entries, ranges, &result)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseValueLogRefDelta(delta)
	if delta == nil || !delta.rawExact || !delta.requiresCandidateProjection {
		t.Fatal("control needs a real exact destructive ordinary DATA receipt")
	}
	next := page.MetaPageBody{CommitSeq: snapshot.state.CommitSeq + 1, UserRootPageID: result.RootID, SystemRootPageID: snapshot.state.SystemRootPageID}
	tracker := d.valueLogRefTracker
	tracker.mu.RLock()
	eligible := tracker.valid && tracker.rawValid && tracker.counts.used == segments && tracker.rawCounts.used == 1
	oldLogical, oldRaw := tracker.counts.toMap(), tracker.rawCounts.toMap()
	tracker.mu.RUnlock()
	if !eligible {
		t.Fatal("public seed did not activate the exact predecessor census")
	}
	owner := d.StableResourceIdentityPinRegistry().MetadataOwner()
	var budget retainedalloc.Budget
	var custom *ordinaryCensusCapacityBudget
	var cow *memtable.COWBudget
	var filler *memtable.COWExternalLease
	if realCOW {
		limits := memtable.DefaultCOWLimits()
		limits.MaxTotalBytes, limits.MaxGenerationBytes = 32<<20, 32<<20
		limits.MaxRetiredBytes, limits.MaxInFlightBytes = 32<<20, 32<<20
		cow, err = memtable.NewCOWBudget(limits)
		if err != nil {
			t.Fatal(err)
		}
		filler, err = cow.AcquireExternal(0)
		if err != nil {
			t.Fatal(err)
		}
		budget = cow
	} else {
		custom = &ordinaryCensusCapacityBudget{limit: math.MaxUint64, refusal: retainedalloc.ErrCapacity}
		budget = custom
	}
	// Use the same owner enrollment as admitCOWSnapshotRetention. In the COW
	// case all refusal and rollback effects come from real COWExternalLeases.
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		enrollment.Close()
		if realCOW {
			filler.Close()
			cow.Close()
			if stats := cow.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
				t.Errorf("last COW metadata enrollment retained charges: %+v", stats)
			}
		} else if bytes, _, _ := custom.snapshot(); bytes != 0 {
			t.Errorf("last metadata enrollment retained %d bytes", bytes)
		}
	}()
	before := owner.Bytes()
	// Fill the real budget permanently, rather than replacing the governor or
	// allowing a later allocation after a one-shot refusal. Resize includes the
	// filler lease wrapper already present in Stats, so only payload is added.
	setCOWHeadroom := func(headroom uint64) memtable.COWStats {
		t.Helper()
		if err := filler.Resize(0); err != nil {
			t.Fatal(err)
		}
		stats := cow.Stats()
		available := cow.Limits().MaxTotalBytes - stats.TotalBytes
		if available < headroom {
			t.Fatalf("headroom=%d exceeds available=%d", headroom, available)
		}
		if err := filler.Resize(available - headroom); err != nil {
			t.Fatal(err)
		}
		return cow.Stats()
	}
	assertCOWRestored := func(want memtable.COWStats) {
		t.Helper()
		got := cow.Stats()
		got.PeakBytes = want.PeakBytes
		if got != want || owner.Bytes() != before {
			t.Fatalf("COW capture retained charges: got=%+v want=%+v owner=%d want=%d", got, want, owner.Bytes(), before)
		}
	}
	scans := 0
	d.testScanCandidateExternalReferencesHook = func() { scans++ }
	defer func() { d.testScanCandidateExternalReferencesHook = nil }()
	capture := func() error {
		var candidate candidateValueLogRefCountsV1
		defer candidate.counts.close()
		defer candidate.rawCounts.close()
		resources, captureErr := d.captureDurableRootResourcesFromBaseWithRefCountsV1(
			snapshot.idx, next, delta, nil, nil,
			rootpublication.StableLogicalObligationRequirements{}, rootpublication.StableLogicalObligationMutation{},
			rootpublication.StableLogicalObligationMutation{}, rootpublication.StableResourceClosureWork{}, nil, false, nil, &candidate, nil,
		)
		if err := resources.ReleaseWithError(); err != nil {
			t.Error(err)
		}
		if captureErr == nil && (!candidate.rawEligible || candidate.counts.used != 0 || candidate.rawCounts.used != 1 || candidate.rawCounts.get(log.fileID) == 0) {
			t.Fatal("original scan did not capture the final raw-only external closure")
		}
		if captureErr != nil && (candidate.rawEligible || candidate.counts.used != 0 || candidate.rawCounts.used != 0) {
			t.Fatal("failed optional projection retained partial candidate evidence")
		}
		return captureErr
	}
	if realCOW {
		stats := setCOWHeadroom(8192)
		// Prove a real raw clone is admitted before the logical clone is refused,
		// then both the owner and every live budget counter are restored exactly.
		var partial candidateValueLogRefCountsV1
		defer partial.counts.close()
		defer partial.rawCounts.close()
		_, reused, projectionErr := d.projectOrdinaryRawReferences(snapshot.idx, next, delta, &partial)
		if !errors.Is(projectionErr, memtable.ErrCOWCapacity) || reused || partial.rawEligible || partial.counts.used != 0 || partial.rawCounts.used != 0 {
			t.Fatalf("real COW clone refusal: reused=%t candidate=%+v err=%v", reused, partial, projectionErr)
		}
		tracker.mu.RLock()
		rawCloneCharge := tracker.rawCounts.charge
		tracker.mu.RUnlock()
		if peak := cow.Stats().PeakBytes; peak != stats.TotalBytes+rawCloneCharge {
			t.Fatalf("partial raw clone admission peak=%d want=%d", peak, stats.TotalBytes+rawCloneCharge)
		}
		assertCOWRestored(stats)
		// The exact original caller, final root and permanent governing cap fit.
		delta.rawExact = false
		if err := capture(); err != nil {
			t.Fatalf("original capture under real COW cap: %v", err)
		}
		if scans != 1 {
			t.Fatalf("original capture scans=%d want 1", scans)
		}
		assertCOWRestored(stats)
		delta.rawExact = true
		if err := capture(); err != nil {
			t.Fatalf("optional real COW census capacity must fall back to viable original capture: %v", err)
		}
		if scans != 2 {
			t.Fatalf("real COW fallback scans=%d want 2", scans)
		}
		assertCOWRestored(stats)
		// Required scanner capacity still fails through the same real governor.
		stats = setCOWHeadroom(64)
		if err := capture(); !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("required real COW capture error=%v want capacity refusal", err)
		}
		if scans != 3 {
			t.Fatalf("required real COW refusal scans=%d want 3", scans)
		}
		assertCOWRestored(stats)
		setCOWHeadroom(1 << 20)
		// A closed real governing budget is not capacity and remains fatal.
		closed, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
		if err != nil {
			t.Fatal(err)
		}
		closedEnrollment, err := owner.Enroll(closed)
		if err != nil {
			t.Fatal(err)
		}
		defer closed.Close()
		defer closedEnrollment.Close()
		closed.Close()
		closedBefore := closed.Stats()
		if err := capture(); !errors.Is(err, memtable.ErrCOWClosed) {
			t.Fatalf("closed governing COW budget error=%v want closed", err)
		}
		if scans != 3 || closed.Stats() != closedBefore {
			t.Fatal("closed COW governor scanned or changed retained charges")
		}
		closedEnrollment.Close()
		if stats := closed.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
			t.Fatalf("closed COW governor retained charges: %+v", stats)
		}
		if owner.Bytes() != before {
			t.Fatal("closed governor enrollment retained owner metadata")
		}
	} else {
		custom.configure(before+8192, retainedalloc.ErrCapacity)
		// The original route succeeds under this exact cap before trying the
		// optional path; this is not a one-shot budget that forgives later growth.
		delta.rawExact = false
		if err := capture(); err != nil {
			t.Fatalf("original capture under finite cap: %v", err)
		}
		if admitted, refusals, _ := custom.snapshot(); scans != 1 || refusals != 0 || admitted != before || owner.Bytes() != before {
			t.Fatalf("original capture scans=%d refusals=%d lease/owner=%d/%d want 1/0/%d/%d", scans, refusals, admitted, owner.Bytes(), before, before)
		}
		delta.rawExact = true
		if err := capture(); err != nil {
			t.Fatalf("optional census capacity must fall back to viable original capture: %v", err)
		}
		if admitted, refusals, growth := custom.snapshot(); scans != 2 || refusals != 1 || growth != recordsChargeForSlots(segments*2) || admitted != before || owner.Bytes() != before {
			t.Fatalf("fallback scans=%d refusals=%d refused growth=%d lease/owner=%d/%d want 2/1/%d/%d/%d", scans, refusals, growth, admitted, owner.Bytes(), recordsChargeForSlots(segments*2), before, before)
		}
		// Other admission/owner failures remain fatal and cannot be hidden by a
		// successful original scanner.
		brokenOwner := errors.New("governing metadata lease failed")
		custom.configure(before+8192, brokenOwner)
		if err := capture(); !errors.Is(err, brokenOwner) {
			t.Fatalf("non-capacity owner error=%v want %v", err, brokenOwner)
		}
		if scans != 2 || owner.Bytes() != before {
			t.Fatal("non-capacity refusal scanned or retained optional backing")
		}
		// Capacity required by the final scanner is still a write-blocking error.
		custom.configure(before+64, retainedalloc.ErrCapacity)
		if err := capture(); !errors.Is(err, retainedalloc.ErrCapacity) {
			t.Fatalf("required capture admission error=%v want capacity refusal", err)
		}
		if scans != 3 || owner.Bytes() != before {
			t.Fatal("required capture refusal skipped the scanner or retained backing")
		}
	}
	// A second real private apply points at an unregistered producer segment.
	// With enough census capacity, registration must fail before any fallback.
	if !realCOW {
		custom.configure(math.MaxUint64, retainedalloc.ErrCapacity)
	}
	missingID, missingPtr, _ := writeUnregisteredValueLogRecord(t, d.dir, 0, segments+1, []byte("unregistered private value"), segments+1)
	if d.valueLogManager.HasSegment(missingID) {
		t.Fatal("unregistered producer control was already registered")
	}
	if err := update.SetPointer([]byte("key/00"), missingPtr); err != nil {
		t.Fatal(err)
	}
	result, err = z.ApplyWithOptions(snapshot.state.RootPageID, update.batch, zipper.ApplyOptions{CollectOldPointerRefs: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Flush(); err != nil {
		t.Fatal(err)
	}
	entries, ranges = update.batch.ApplyPlan()
	delta, err = d.buildValueLogRefDeltaForApply(snapshot.idx, snapshot.state.RootPageID, result.RootID, snapshot.state.CommitSeq, entries, ranges, &result)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseValueLogRefDelta(delta)
	next.UserRootPageID = result.RootID
	if err := capture(); !errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("unregistered producer error=%v want unresolved resource", err)
	}
	if scans != 3 || owner.Bytes() != before {
		t.Fatal("registration failure scanned or retained optional backing")
	}
	tracker.mu.RLock()
	unchanged := tracker.valid && tracker.rawValid && tracker.commitSeq == snapshot.state.CommitSeq && maps.Equal(tracker.counts.toMap(), oldLogical) && maps.Equal(tracker.rawCounts.toMap(), oldRaw)
	tracker.mu.RUnlock()
	if !unchanged {
		t.Fatal("private capture mutated the accepted predecessor census")
	}
	if got, err := snapshot.Get([]byte("key/00")); err != nil || string(got) != "persistent predecessor value" {
		t.Fatalf("accepted predecessor read=%q err=%v", got, err)
	}
}
