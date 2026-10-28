package db

import (
	"bytes"
	"context"
	"fmt"
	"testing"
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
