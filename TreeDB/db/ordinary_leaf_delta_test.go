package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	"maps"
	"math"
	"path/filepath"
	"testing"
)

func TestOrdinaryDATAOuterLeafOverwritesAvoidCandidateScan(t *testing.T) {
	testOrdinaryDATAOuterLeafOverwritesAvoidCandidateScan(t, false)
}

func TestOrdinaryDATASpanNativeOverwritesAvoidCandidateScan(t *testing.T) {
	testOrdinaryDATAOuterLeafOverwritesAvoidCandidateScan(t, true)
}

func testOrdinaryDATAOuterLeafOverwritesAvoidCandidateScan(t *testing.T, spanNative bool) {
	t.Helper()
	opts := Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true}
	if spanNative {
		opts.FlushAdmissionPolicy = FlushAdmissionPolicyExplicit
		opts.FlushApplySpanNative = true
		opts.FlushApplyConcurrency = 4
		opts.FlushApplyMinEntries = 1
		opts.FlushApplyMinSpans = 1
		opts.FlushApplyMinBytes = 1
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	log := &registeredLeafPageLog{db: d, dir: d.dir}
	if err = log.ensureWriter(); err != nil {
		t.Fatal(err)
	}
	d.SetLeafPageLog(log)
	defer closeVacuumTestLeafPageLog(t, d, log)
	put := func(round int) {
		t.Helper()
		b := d.NewBatch()
		defer b.Close()
		for i := 0; i < 1000; i++ {
			if e := b.Set([]byte(fmt.Sprintf("key/%05d", i)), bytes.Repeat([]byte{byte(round)}, 128)); e != nil {
				t.Fatal(e)
			}
		}
		if e := b.WriteSync(); e != nil {
			t.Fatal(e)
		}
	}
	put(1)
	put(2)
	old := d.AcquireSnapshot()
	defer old.Close()
	usedBefore := rawSpanNativeRouteStatUint(t, d.Stats(), "treedb.raw.span_native.route.point_put.used_ops_total")
	scans := 0
	d.testScanCandidateExternalReferencesHook = func() { scans++ }
	for round := 3; round < 6; round++ {
		put(round)
	}
	d.testScanCandidateExternalReferencesHook = nil
	if spanNative {
		usedAfter := rawSpanNativeRouteStatUint(t, d.Stats(), "treedb.raw.span_native.route.point_put.used_ops_total")
		if usedAfter-usedBefore != 3000 {
			t.Fatalf("span-native consumed %d overwrite ops, want 3000", usedAfter-usedBefore)
		}
	}
	if scans != 0 {
		t.Fatalf("ordinary DATA overwrite performed %d full candidate scans", scans)
	}
	if got, e := old.Get([]byte("key/00001")); e != nil || !bytes.Equal(got, bytes.Repeat([]byte{2}, 128)) {
		t.Fatalf("old snapshot: %x %v", got, e)
	}
	assertCandidateTrackerMatchesFullScan(t, d)
	assertOrdinaryRawCensus(t, d)
	assertOrdinaryRawProjectionBoundaries(t, d)
}

func assertOrdinaryRawCensus(t *testing.T, d *DB) {
	t.Helper()
	snap := d.AcquireSnapshot()
	defer snap.Close()
	want := map[uint32]uint64{}
	if err := leafrefscan.Walk(context.Background(), snap.state.RootPageID, snap.idx.pager.Get, nil, func(ptr page.LeafLogPtr) error { want[ptr.ValueLogFileID()]++; return nil }); err != nil {
		t.Fatal(err)
	}
	tracker := d.valueLogRefTracker
	tracker.mu.RLock()
	got := tracker.rawCounts.toMap()
	valid := tracker.rawValid && tracker.rawCommitSeq == snap.state.CommitSeq && tracker.rawUserRoot == snap.state.RootPageID
	tracker.mu.RUnlock()
	if !valid || !maps.Equal(got, want) {
		t.Fatalf("raw census valid=%v got=%v want=%v", valid, got, want)
	}
	resources, _, err := d.rootPublication.cloneVisibleResourcesWithWorkMax(-1)
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Release()
	diag, err := resources.AcquirePhysicalDiagnostics()
	if err != nil {
		t.Fatal(err)
	}
	defer diag.CloseWithError()
	gotFiles := map[uint32]struct{}{}
	for _, desc := range diag.Physical() {
		if desc.Kind == rootpublication.ResourceOuterLeafLog {
			gotFiles[uint32(desc.Generation)] = struct{}{}
		}
	}
	if len(gotFiles) != len(want) {
		t.Fatalf("raw dependency membership got=%v want=%v", gotFiles, want)
	}
	for id := range want {
		if _, ok := gotFiles[id]; !ok {
			t.Fatalf("missing raw dependency %d", id)
		}
	}
}

func TestOrdinaryDATARawGroupCancellationRotationAndFallback(t *testing.T) {
	testOrdinaryDATARawGroupCancellationRotationAndFallback(t, false)
}

func TestOrdinaryDATASpanNativeRawGroupCancellationRotationAndFallback(t *testing.T) {
	testOrdinaryDATARawGroupCancellationRotationAndFallback(t, true)
}

func testOrdinaryDATARawGroupCancellationRotationAndFallback(t *testing.T, spanNative bool) {
	t.Helper()
	opts := Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true}
	if spanNative {
		opts.FlushAdmissionPolicy = FlushAdmissionPolicyExplicit
		opts.FlushApplySpanNative = true
		opts.FlushApplyConcurrency = 4
		opts.FlushApplyMinEntries = 1
		opts.FlushApplyMinSpans = 1
		opts.FlushApplyMinBytes = 1
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	log := &registeredLeafPageLog{db: d, dir: d.dir}
	if err = log.ensureWriter(); err != nil {
		t.Fatal(err)
	}
	d.SetLeafPageLog(log)
	defer closeVacuumTestLeafPageLog(t, d, log)
	write := func(round, count int) {
		t.Helper()
		b := d.NewBatch()
		defer b.Close()
		for i := 0; i < count; i++ {
			if err := b.Set([]byte(fmt.Sprintf("key/%05d", i)), bytes.Repeat([]byte{byte(round)}, 128)); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatal(err)
		}
	}
	write(1, 1000)
	write(2, 1000)
	assertOrdinaryRawCensus(t, d)
	old := d.AcquireSnapshot()
	defer old.Close()
	previous := log.fileID
	if err = log.Close(); err != nil {
		t.Fatal(err)
	}
	log.fileID, err = valuelog.EncodeFileID(rewriteLeafLogLaneID, 2)
	if err != nil {
		t.Fatal(err)
	}
	log.path = filepath.Join(LeafLogDirPath(d.dir), fmt.Sprintf("value-l%d-000002.log", rewriteLeafLogLaneID))
	log.w, err = valuelog.NewWriter(log.path, log.fileID)
	if err != nil {
		t.Fatal(err)
	}
	if err = d.RegisterValueLogSegment(log.path, log.fileID); err != nil {
		t.Fatal(err)
	}
	scans := 0
	d.testScanCandidateExternalReferencesHook = func() { scans++ }
	defer func() { d.testScanCandidateExternalReferencesHook = nil }()
	usedBefore := rawSpanNativeRouteStatUint(t, d.Stats(), "treedb.raw.span_native.route.point_put.used_ops_total")
	group, err := d.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	for round := 3; round <= 5; round++ {
		b := d.NewPhysicalBatch().(*Batch)
		for i := 0; i < 1200; i++ {
			if err = b.Set([]byte(fmt.Sprintf("key/%05d", i)), bytes.Repeat([]byte{byte(round)}, 128)); err != nil {
				t.Fatal(err)
			}
		}
		if err = b.SetRootPublicationBuildGroup(group, round == 5); err != nil {
			t.Fatal(err)
		}
		if round == 5 {
			err = b.WriteSync()
		} else {
			err = b.Write()
		}
		if err != nil {
			t.Fatal(err)
		}
		if err = b.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if spanNative {
		usedAfter := rawSpanNativeRouteStatUint(t, d.Stats(), "treedb.raw.span_native.route.point_put.used_ops_total")
		if usedAfter-usedBefore != 3600 {
			t.Fatalf("span-native consumed %d grouped put ops, want 3600", usedAfter-usedBefore)
		}
	}
	if scans != 0 {
		t.Fatalf("group rescanned %d times", scans)
	}
	assertOrdinaryRawCensus(t, d)
	d.valueLogRefTracker.mu.RLock()
	remaining := d.valueLogRefTracker.rawCounts.get(previous)
	d.valueLogRefTracker.mu.RUnlock()
	if remaining != 0 {
		t.Fatalf("retained old raw file edge count %d", remaining)
	}
	if got, e := old.Get([]byte("key/00001")); e != nil || !bytes.Equal(got, bytes.Repeat([]byte{2}, 128)) {
		t.Fatalf("held old snapshot %x %v", got, e)
	}
	// Unsupported delete maintenance rebuilds a fresh exact census.
	if err = d.DeleteSync([]byte("key/00001")); err != nil {
		t.Fatal(err)
	}
	if scans == 0 {
		t.Fatal("delete bypassed exact fallback")
	}
	assertOrdinaryRawCensus(t, d)
	// Invalid derived evidence must force one exact repair, then resume.
	d.valueLogRefTracker.invalidate()
	scans = 0
	write(6, 1200)
	if scans == 0 {
		t.Fatal("invalid tracker bypassed exact fallback")
	}
	assertOrdinaryRawCensus(t, d)
	scans = 0
	write(7, 1200)
	if scans != 0 {
		t.Fatalf("repaired census rescanned %d", scans)
	}
	assertOrdinaryRawCensus(t, d)
	// A census from another physical incarnation cannot authorize omission.
	d.valueLogRefTracker.mu.Lock()
	d.valueLogRefTracker.rawIncarnation.indexID++
	d.valueLogRefTracker.mu.Unlock()
	scans = 0
	write(8, 1200)
	if scans == 0 {
		t.Fatal("stale raw incarnation bypassed exact fallback")
	}
	assertOrdinaryRawCensus(t, d)
	// A descriptor may keep an older user root alive. The ordinary census is
	// deliberately ineligible, including a descriptor alias of today's root.
	aliased := d.State().RootPageID
	if _, err := d.PublishSystemRootIterator(mustFrozenRawMemtable(t,
		maintenanceTestCollectionRootKey, encodeMaintenanceRootID(aliased),
	).NewIterator(nil, nil)); err != nil {
		t.Fatal(err)
	}
	scans = 0
	write(9, 1200)
	if scans == 0 {
		t.Fatal("descriptor alias bypassed exact fallback")
	}
	d.valueLogRefTracker.mu.RLock()
	rawValid := d.valueLogRefTracker.rawValid
	d.valueLogRefTracker.mu.RUnlock()
	if rawValid {
		t.Fatal("descriptor alias accepted ordinary raw census")
	}
	assertCandidateTrackerMatchesFullScan(t, d)
}

// Broken arithmetic evidence and finite clone admission cannot authorize an
// omitted dependency or mutate the accepted tracker. Exercise the real census
// and existing metadata owner after public writes activate the receipt.
func assertOrdinaryRawProjectionBoundaries(t *testing.T, d *DB) {
	t.Helper()
	snap := d.AcquireSnapshot()
	defer snap.Close()
	tracker := d.valueLogRefTracker
	tracker.mu.RLock()
	var id uint32
	var count uint64
	for _, slot := range tracker.rawCounts.slots {
		if slot.fileID != 0 {
			id, count = slot.fileID, slot.count
			break
		}
	}
	tracker.mu.RUnlock()
	if id == 0 || count >= math.MaxInt64 {
		t.Fatal("missing finite raw census")
	}
	next := page.MetaPageBody{CommitSeq: snap.state.CommitSeq + 1, UserRootPageID: snap.state.RootPageID, SystemRootPageID: snap.state.SystemRootPageID}
	delta := newValueLogRefDelta()
	defer releaseValueLogRefDelta(delta)
	delta.rawExact, delta.rawIndexID = true, snap.idx.id
	delta.rawInput, delta.rawOutput = snap.state.RootPageID, snap.state.RootPageID
	delta.rawChanges = newValueLogRefDelta()
	owner := d.StableResourceIdentityPinRegistry().MetadataOwner()
	project := func(wantCapacity bool) {
		t.Helper()
		before := owner.Bytes()
		var candidate candidateValueLogRefCountsV1
		refs, exact, err := d.projectOrdinaryRawReferences(snap.idx, next, delta, &candidate)
		if wantCapacity && !errors.Is(err, retainedalloc.ErrCapacity) || !wantCapacity && err != nil || exact || refs != nil {
			t.Fatalf("unsafe projection exact=%v refs=%v err=%v capacity=%v", exact, refs, err, wantCapacity)
		}
		if owner.Bytes() != before || candidate.rawEligible || candidate.rawCounts.used != 0 || candidate.counts.used != 0 {
			t.Fatal("rejected projection retained candidate backing")
		}
	}
	delta.rawChanges.addChange(id, -int64(count)-1)
	project(false)
	delta.rawChanges.resetForReuse()
	delta.rawChanges.addChange(id, 1)
	tracker.mu.Lock()
	err := tracker.rawCounts.set(id, math.MaxUint64)
	tracker.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	project(false)
	tracker.mu.Lock()
	err = tracker.rawCounts.set(id, count)
	tracker.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	budget := &originalClosureAdmissionBudget{limit: math.MaxUint64}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	budget.setLimit(owner.Bytes())
	project(true)
	budget.setLimit(math.MaxUint64)
	assertOrdinaryRawCensus(t, d)
}
