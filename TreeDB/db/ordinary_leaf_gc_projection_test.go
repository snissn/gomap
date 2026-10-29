package db

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/page"
)

// Refreshing logical counts for unchanged roots must retain the independently
// proven raw census, so the following exact append can project its dependencies.
func TestOrdinaryDATAGCScanPreservesSameRootRawCensus(t *testing.T) {
	opts := Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true}
	d, err := Open(opts)
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
	b := d.NewBatch()
	for i := 0; i < 1000; i++ {
		if err := b.Set([]byte(fmt.Sprintf("key/%05d", i)), bytes.Repeat([]byte{byte(i)}, 128)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.WriteSync(); err != nil {
		t.Fatal(err)
	}
	b.Close()
	assertOrdinaryRawCensus(t, d)
	old := d.AcquireSnapshot()
	defer old.Close()
	tracker := d.valueLogRefTracker
	tracker.mu.RLock()
	seq := tracker.commitSeq
	incarnation := tracker.incarnation
	beforeRaw := tracker.rawValid
	tracker.mu.RUnlock()
	_, source, scannedSeq, err := d.referencedValueLogSegmentsForGCAtSeq(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tracker.mu.RLock()
	afterSeq := tracker.commitSeq
	afterIncarnation := tracker.incarnation
	afterRaw := tracker.rawValid
	tracker.mu.RUnlock()
	if scannedSeq != seq || afterSeq != seq || afterIncarnation != incarnation {
		t.Fatalf("generation changed: before=%d scanned=%d after=%d", seq, scannedSeq, afterSeq)
	}
	if !beforeRaw || !afterRaw {
		t.Fatalf("real resolver discarded same-root raw census: before=%t after=%t", beforeRaw, afterRaw)
	}
	scans := 0
	d.testScanCandidateExternalReferencesHook = func() { scans++ }
	if err := d.SetSync([]byte("tail/key"), []byte("next")); err != nil {
		t.Fatal(err)
	}
	d.testScanCandidateExternalReferencesHook = nil
	if scans != 0 {
		t.Fatalf("following append scans=%d want0", scans)
	}
	assertOrdinaryRawCensus(t, d)
	assertCandidateTrackerMatchesFullScan(t, d)
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key/%05d", i))
		want := bytes.Repeat([]byte{byte(i)}, 128)
		for _, s := range []*Snapshot{old, d.AcquireSnapshot()} {
			got, err := s.Get(key)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("row%d mismatch err=%v", i, err)
			}
			if s != old {
				s.Close()
			}
		}
	}
	t.Logf("same generation=%d source=%v rawValid %t→%t following append full scans=%d; tracker/full closure and retained values agree", seq, source, beforeRaw, afterRaw, scans)
}

// Metadata matching, not merely a sequence match, retains the original raw
// owner. Every absent/mismatched proof uses the existing invalidation path.
func TestValueLogRefTrackerRawCensusRetentionRequiresExactScanBasis(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	inc, err := captureValueLogRefIncarnation(d.idx.Load())
	if err != nil {
		t.Fatal(err)
	}
	type mutate func(*valueLogRefTracker, *valueLogRefScanBasis)
	for _, tc := range []struct {
		name     string
		change   mutate
		preserve bool
	}{
		{"same-roots", func(*valueLogRefTracker, *valueLogRefScanBasis) {}, true},
		{"missing-basis", func(*valueLogRefTracker, *valueLogRefScanBasis) {}, false},
		{"different-user", func(_ *valueLogRefTracker, b *valueLogRefScanBasis) { b.userRootID++ }, false},
		{"different-system", func(_ *valueLogRefTracker, b *valueLogRefScanBasis) { b.systemRootID++ }, false},
		{"zero-user", func(_ *valueLogRefTracker, b *valueLogRefScanBasis) { b.userRootID = 0 }, false},
		{"aliased-roots", func(_ *valueLogRefTracker, b *valueLogRefScanBasis) { b.userRootID = b.systemRootID }, false},
		{"different-basis-sequence", func(_ *valueLogRefTracker, b *valueLogRefScanBasis) { b.commitSeq++ }, false},
		{"different-basis-incarnation", func(_ *valueLogRefTracker, b *valueLogRefScanBasis) { b.incarnation.indexID++ }, false},
		{"different-tracker-incarnation", func(t *valueLogRefTracker, _ *valueLogRefScanBasis) { t.incarnation.indexID++ }, false},
		{"different-raw-incarnation", func(t *valueLogRefTracker, _ *valueLogRefScanBasis) { t.rawIncarnation.indexID++ }, false},
		{"different-raw-sequence", func(t *valueLogRefTracker, _ *valueLogRefScanBasis) { t.rawCommitSeq++ }, false},
		{"invalid-logical", func(t *valueLogRefTracker, _ *valueLogRefScanBasis) { t.valid = false }, false},
		{"invalid-raw", func(t *valueLogRefTracker, _ *valueLogRefScanBasis) { t.rawValid = false }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := d.valueLogRefTracker.metadata
			before := owner.Bytes()
			defer func() {
				if got := owner.Bytes(); got != before {
					t.Errorf("collector lease leak: before=%d after=%d", before, got)
				}
			}()
			tracker := newValueLogRefTracker()
			defer tracker.close()
			tracker.counts, err = valueLogRefCountsFromMap(owner, map[uint32]uint64{12: 1})
			if err != nil {
				t.Fatal(err)
			}
			tracker.rawCounts, err = valueLogRefCountsFromMap(owner, map[uint32]uint64{99: 3})
			if err != nil {
				t.Fatal(err)
			}
			tracker.commitSeq, tracker.rawCommitSeq = 7, 7
			tracker.incarnation, tracker.rawIncarnation = inc, inc
			tracker.valid, tracker.rawValid = true, true
			tracker.rawUserRoot, tracker.rawSystemRoot = 101, 102
			basis := valueLogRefScanBasis{commitSeq: 7, incarnation: inc, userRootID: 101, systemRootID: 102}
			tc.change(tracker, &basis)
			incoming, err := valueLogRefCountsFromMap(owner, map[uint32]uint64{12: 2})
			if err != nil {
				t.Fatal(err)
			}
			defer incoming.close()
			rawBacking, incomingBacking := &tracker.rawCounts.slots[0], &incoming.slots[0]
			basisArg := &basis
			if tc.name == "missing-basis" {
				basisArg = nil
			}
			if !tracker.replaceUnlessAdvancedOwnedWithBasis(&incoming, 7, inc, true, tracker.revision, basisArg) {
				t.Fatal("unchanged replacement declined")
			}
			if tracker.rawValid != tc.preserve {
				t.Fatalf("rawValid=%t want%t", tracker.rawValid, tc.preserve)
			}
			if tc.preserve && (tracker.rawCounts.get(99) != 3 || &tracker.rawCounts.slots[0] != rawBacking) {
				t.Fatal("original raw counts lost")
			}
			if !tc.preserve && tracker.rawCounts.used != 0 {
				t.Fatal("invalid raw counts retained")
			}
			if incoming.slots != nil || tracker.counts.get(12) != 2 || &tracker.counts.slots[0] != incomingBacking {
				t.Fatal("incoming logical collector ownership not transferred")
			}
		})
	}
}

// A completed scan is installed concurrently with a later publication. The
// existing advancement guard must either retain the exact predecessor before
// publication or refuse the stale replacement afterward; both yield exact roots.
func TestOrdinaryDATAGCScanConcurrentPublicationPreservesNewCensus(t *testing.T) {
	opts := Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true}
	d, err := Open(opts)
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
	if err := d.SetSync([]byte("seed"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 20; round++ {
		tracker := d.valueLogRefTracker
		revision := tracker.revisionSnapshot()
		counts, basis, err := d.scanValueLogRefCountsOwnedWithBasis(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		done := make(chan error, 1)
		go func() { <-start; done <- d.SetSync([]byte(fmt.Sprintf("tail/%d", round)), []byte("next")) }()
		close(start)
		tracker.replaceUnlessAdvancedOwnedWithBasis(&counts, basis.commitSeq, basis.incarnation, true, revision, &basis)
		counts.close()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		assertOrdinaryRawCensus(t, d)
		assertCandidateTrackerMatchesFullScan(t, d)
	}
}

// Channels impose both orderings, rather than relying on the scheduler to pick
// a different winner across repetitions of the concurrent control above.
func TestOrdinaryDATAGCScanForcedPublicationOrderings(t *testing.T) {
	for _, gcFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("gc-first=%t", gcFirst), func(t *testing.T) {
			d, _ := openOrdinaryGCProjectionFixture(t)
			tracker := d.valueLogRefTracker
			revision := tracker.revisionSnapshot()
			counts, basis, err := d.scanValueLogRefCountsOwnedWithBasis(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer counts.close()
			tracker.mu.RLock()
			rawBacking := &tracker.rawCounts.slots[0]
			tracker.mu.RUnlock()
			start := make(chan struct{})
			done := make(chan error, 1)
			go func() { <-start; done <- d.SetSync([]byte("ordered/tail"), []byte("next")) }()
			published := false
			publish := func() {
				published = true
				close(start)
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
			defer func() {
				if !published {
					publish()
				}
			}()
			if !gcFirst {
				publish()
			}
			// In the publication-first case, save the exact successor before the
			// stale installer runs, including both collectors and their backing.
			tracker.mu.RLock()
			beforeSeq, beforeInc, beforeRev := tracker.commitSeq, tracker.incarnation, tracker.revision
			beforeLogical, beforeRaw := tracker.counts.toMap(), tracker.rawCounts.toMap()
			logicalBacking, successorRawBacking := &tracker.counts.slots[0], &tracker.rawCounts.slots[0]
			tracker.mu.RUnlock()
			installed := tracker.replaceUnlessAdvancedOwnedWithBasis(&counts, basis.commitSeq, basis.incarnation, true, revision, &basis)
			if installed != gcFirst {
				t.Fatalf("installed=%t want %t", installed, gcFirst)
			}
			tracker.mu.RLock()
			if gcFirst {
				if counts.slots != nil || !tracker.rawValid || &tracker.rawCounts.slots[0] != rawBacking {
					t.Error("GC-first install did not transfer logical ownership and preserve original raw backing")
				}
			} else if counts.slots == nil || tracker.commitSeq != beforeSeq || tracker.incarnation != beforeInc || tracker.revision != beforeRev ||
				!tracker.rawValid || !maps.Equal(tracker.counts.toMap(), beforeLogical) || !maps.Equal(tracker.rawCounts.toMap(), beforeRaw) ||
				&tracker.counts.slots[0] != logicalBacking || &tracker.rawCounts.slots[0] != successorRawBacking {
				t.Error("stale GC install consumed caller ownership or changed successor census")
			}
			tracker.mu.RUnlock()
			if gcFirst {
				publish()
			}
			assertOrdinaryRawCensus(t, d)
			assertCandidateTrackerMatchesFullScan(t, d)
			if got, err := d.Get([]byte("ordered/tail")); err != nil || !bytes.Equal(got, []byte("next")) {
				t.Fatalf("published value=%q err=%v", got, err)
			}
		})
	}
}

func openOrdinaryGCProjectionFixture(t *testing.T) (*DB, *registeredLeafPageLog) {
	t.Helper()
	d, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true, KeepRecent: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	log := &registeredLeafPageLog{db: d, dir: d.dir}
	if err := log.ensureWriter(); err != nil {
		t.Fatal(err)
	}
	d.SetLeafPageLog(log)
	t.Cleanup(func() { closeVacuumTestLeafPageLog(t, d, log) })
	// Include a real logical ValuePtr as well as raw outer-leaf references, so
	// ownership assertions exercise nonempty backing for both collectors.
	ptr := appendPointersInNewSegment(t, d.dir, 101, 1, 1, 1, func(int) []byte { return []byte("seed pointer value") })[0]
	b := d.NewBatch()
	defer b.Close()
	if err := b.(interface {
		SetPointer([]byte, page.ValuePtr) error
	}).SetPointer([]byte("pointer/seed"), ptr); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		if err := b.Set([]byte(fmt.Sprintf("key/%05d", i)), bytes.Repeat([]byte{byte(i)}, 128)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.WriteSync(); err != nil {
		t.Fatal(err)
	}
	b.Close()
	assertOrdinaryRawCensus(t, d)
	return d, log
}

// A real same-DB physical cutover advances CommitSeq by one. The scanner's
// retained old generation must never overwrite that successor. A same-sequence
// public cutover is not supported by VacuumIndexOnline's publication contract.
func TestOrdinaryDATAGCScanRejectsPriorPhysicalVacuumGeneration(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	d, _ := openOrdinaryGCProjectionFixture(t)
	old := d.AcquireSnapshot()
	defer old.Close()
	tracker := d.valueLogRefTracker
	revision := tracker.revisionSnapshot()
	counts, basis, err := d.scanValueLogRefCountsOwnedWithBasis(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer counts.close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.VacuumIndexOnline(ctx); err != nil {
		t.Fatal(err)
	}
	nextInc, err := captureValueLogRefIncarnation(d.idx.Load())
	if err != nil {
		t.Fatal(err)
	}
	if nextInc.indexID == basis.incarnation.indexID || nextInc.samePhysical(basis.incarnation) {
		t.Fatal("fixture did not replace the physical index generation")
	}
	if got := d.State().CommitSeq; got != basis.commitSeq+1 {
		t.Fatalf("physical cutover sequence=%d want %d", got, basis.commitSeq+1)
	}
	tracker.mu.RLock()
	beforeSeq, beforeInc, beforeRevision := tracker.commitSeq, tracker.incarnation, tracker.revision
	beforeLogical, beforeRaw := tracker.counts.toMap(), tracker.rawCounts.toMap()
	beforeRawValid := tracker.rawValid
	tracker.mu.RUnlock()
	if tracker.replaceUnlessAdvancedOwnedWithBasis(&counts, basis.commitSeq, basis.incarnation, true, revision, &basis) {
		t.Fatal("prior physical generation scan installed after real cutover")
	}
	tracker.mu.RLock()
	unchanged := tracker.commitSeq == beforeSeq && tracker.incarnation == beforeInc && tracker.revision == beforeRevision &&
		tracker.rawValid == beforeRawValid && maps.Equal(tracker.counts.toMap(), beforeLogical) && maps.Equal(tracker.rawCounts.toMap(), beforeRaw)
	tracker.mu.RUnlock()
	if !unchanged || counts.slots == nil {
		t.Fatal("rejected old scan mutated successor or consumed caller's collector")
	}
	assertCandidateTrackerMatchesFullScan(t, d)
	// The first append repairs any old raw basis conservatively. Then the new
	// physical generation has independently checked logical and raw censuses.
	if err := d.SetSync([]byte("after/vacuum"), []byte("next")); err != nil {
		t.Fatal(err)
	}
	assertOrdinaryRawCensus(t, d)
	assertCandidateTrackerMatchesFullScan(t, d)
	current := d.AcquireSnapshot()
	defer current.Close()
	for _, snap := range []*Snapshot{old, current} {
		for i := 0; i < 1000; i++ {
			got, err := snap.Get([]byte(fmt.Sprintf("key/%05d", i)))
			if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{byte(i)}, 128)) {
				t.Fatalf("retained/current value %d mismatch err=%v", i, err)
			}
		}
	}
	t.Logf("same DB physical cutover index %d→%d sequence %d→%d; old scan refused", basis.incarnation.indexID, nextInc.indexID, basis.commitSeq, beforeSeq)
}

func TestOrdinaryDATAGCScanPreservedCensusDeletesAndReopens(t *testing.T) {
	d, log := openOrdinaryGCProjectionFixture(t)
	dead := appendPointersInNewSegment(t, d.dir, 100, 1, 1, 1, func(int) []byte { return []byte("dead value") })[0]
	liveValue := bytes.Repeat([]byte("live value"), 128)
	live := appendPointersInNewSegment(t, d.dir, 100, 2, 2, 1, func(int) []byte { return liveValue })[0]
	// Keep both referenced producers inactive, so GC must use reachability.
	appendPointersInNewSegment(t, d.dir, 100, 3, 3, 1, func(int) []byte { return []byte("active value") })
	b := d.NewBatch()
	ptrBatch := b.(interface {
		SetPointer([]byte, page.ValuePtr) error
	})
	if err := ptrBatch.SetPointer([]byte("dead"), dead); err != nil {
		t.Fatal(err)
	}
	if err := ptrBatch.SetPointer([]byte("live"), live); err != nil {
		t.Fatal(err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatal(err)
	}
	b.Close()
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if _, ok := recoverableValueLogSegmentsForTest(t, d)[dead.FileID]; !ok {
		t.Fatal("dead pointer was never recovery-selectable")
	}
	assertOrdinaryRawCensus(t, d)
	tracker := d.valueLogRefTracker
	tracker.mu.RLock()
	rawBacking := &tracker.rawCounts.slots[0]
	tracker.mu.RUnlock()
	if _, _, _, err := d.referencedValueLogSegmentsForGCAtSeq(context.Background()); err != nil {
		t.Fatal(err)
	}
	tracker.mu.RLock()
	preserved := tracker.rawValid && &tracker.rawCounts.slots[0] == rawBacking
	tracker.mu.RUnlock()
	if !preserved {
		t.Fatal("same-root resolver did not preserve original raw census")
	}
	if err := d.DeleteSync([]byte("dead")); err != nil {
		t.Fatal(err)
	}
	deleteSeq := d.State().CommitSeq
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// An ordinary successor overwrites the older pointer-bearing durable slot
	// without introducing a nonempty system tree that disables raw projection.
	if err := d.SetSync([]byte("after/delete"), []byte("settled")); err != nil {
		t.Fatal(err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	settled := captureValueLogGCDurableSlotState(t, d, "ordinary deletion settled")
	for slot, seq := range settled.slotCommits {
		if seq < deleteSeq {
			t.Fatalf("slot %d still predates deletion: %d < %d", slot, seq, deleteSeq)
		}
	}
	recoverable := recoverableValueLogSegmentsForTest(t, d)
	if _, ok := recoverable[dead.FileID]; ok {
		t.Fatal("dead producer remains in recoverable closure")
	}
	if _, ok := recoverable[live.FileID]; !ok {
		t.Fatal("live producer missing from recoverable closure")
	}
	assertOrdinaryRawCensus(t, d)
	stats, err := d.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	deadPath := filepath.Join(ValueLogDirPath(d.dir), "value-l100-000001.log")
	livePath := filepath.Join(ValueLogDirPath(d.dir), "value-l100-000002.log")
	if stats.SegmentsDeleted == 0 {
		t.Fatalf("no producer deleted: %+v", stats)
	}
	if _, err := os.Stat(deadPath); !os.IsNotExist(err) {
		t.Fatalf("dead producer not deleted: %v", err)
	}
	if _, err := os.Stat(livePath); err != nil {
		t.Fatalf("live producer not retained: %v", err)
	}
	assertOrdinaryRawCensus(t, d)
	assertCandidateTrackerMatchesFullScan(t, d)
	if got, err := d.Get([]byte("live")); err != nil || !bytes.Equal(got, liveValue) {
		t.Fatalf("live before reopen=%q err=%v", got, err)
	}
	// Close the externally owned writer before DB close; its cleanup is
	// idempotent and still runs on a failure earlier in this test.
	closeVacuumTestLeafPageLog(t, d, log)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(Options{Dir: d.dir, Durability: DurabilityWALOffRelaxed, IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true, KeepRecent: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Get([]byte("dead")); err != nil || got != nil {
		t.Fatalf("deleted key after reopen=%q err=%v", got, err)
	}
	if got, err := reopened.Get([]byte("live")); err != nil || !bytes.Equal(got, liveValue) {
		t.Fatalf("live after reopen=%q err=%v", got, err)
	}
	for i := 0; i < 1000; i++ {
		got, err := reopened.Get([]byte(fmt.Sprintf("key/%05d", i)))
		if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{byte(i)}, 128)) {
			t.Fatalf("ordinary value %d after reopen mismatch err=%v", i, err)
		}
	}
	if _, err := os.Stat(deadPath); !os.IsNotExist(err) {
		t.Fatalf("dead producer resurrected on reopen: %v", err)
	}
	t.Logf("real GC deleted %d segments/%d bytes, live producer and 1000 ordinary values survive reopen", stats.SegmentsDeleted, stats.BytesDeleted)
}
