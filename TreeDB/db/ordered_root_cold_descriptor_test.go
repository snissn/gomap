package db

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

// Exercise the command-WAL batch entry used by buffered indexed collection
// flushes. Cold roots must not require reading their outer leaves back merely
// to rediscover the dependencies already reported by their builders.
func TestPublishOrderedRootCommandWALColdBatchDescriptorsAvoidCandidateScan(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	opts := Options{Dir: dir, CommandWAL: true, Durability: DurabilityDurable,
		DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true}
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	db.SetLeafPageLog(leafLog)
	t.Cleanup(func() {
		_ = db.Close()
		_ = leafLog.Close()
	})
	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 610_000, 2, func(int) []byte { return []byte("persistent-value") })
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	ordered := make([]OrderedRootDeltaBatchPublishInput, len(ptrs))
	for i, ptr := range ptrs {
		it := mustFrozenSystemPointerMemtable(t, "doc/p", ptr).NewIterator(nil, nil)
		delta, err := OrderedRootDeltaBatchFromIterator(it)
		_ = it.Close()
		if err != nil {
			t.Fatal(err)
		}
		defer delta.Close()
		ordered[i] = OrderedRootDeltaBatchPublishInput{Delta: delta, StoragePolicy: OrderedRootStorageValueLogLeaves, ParallelApply: true}
	}
	var scans atomic.Int64
	db.testScanCandidateExternalReferencesHook = func() { scans.Add(1) }
	_, _, err = db.PublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder(
		ordered, mustRawKVCommandWALIntent(t, db, "cmd/cold-batch", "1"),
		func(roots []uint64) (iterator.UnsafeIterator, error) {
			return mustFrozenRawMemtable(t,
				collectionRootDescriptorPrefix+"cold-a", encodeMaintenanceRootID(roots[0]),
				collectionRootDescriptorPrefix+"cold-alias", encodeMaintenanceRootID(roots[0]),
				collectionRootDescriptorPrefix+"cold-b", encodeMaintenanceRootID(roots[1]),
			).NewIterator(nil, nil), nil
		})
	db.testScanCandidateExternalReferencesHook = nil
	if err != nil {
		t.Fatal(err)
	}
	if got := scans.Load(); got != 0 {
		t.Fatalf("cold covered batch candidate scans=%d want 0", got)
	}
	assertCandidateTrackerMatchesFullScan(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := leafLog.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	for _, key := range []string{collectionRootDescriptorPrefix + "cold-a", collectionRootDescriptorPrefix + "cold-b"} {
		got := readCollectionRootValue(t, reopened, key, []byte("doc/p"))
		if string(got) != "persistent-value" {
			t.Fatalf("descriptor %q Get=%q", key, got)
		}
	}
	if _, err := reopened.referencedValueLogSegments(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertCandidateTrackerMatchesFullScan(t, reopened)
}

func TestOrderedRootColdDescriptorCoverage(t *testing.T) {
	entry := func(roots ...uint64) []collectionEntry {
		return []collectionEntry{{key: []byte(collectionRootDescriptorPrefix + "cold"), sourceRootIDs: roots}}
	}
	for _, tc := range []struct {
		name       string
		base, next []uint64
		entries    []collectionEntry
		user       uint64
		want       bool
	}{
		{name: "independent empty roots", base: []uint64{0, 0}, next: []uint64{11, 12}, entries: entry(11, 12), want: true},
		{name: "descriptor aliases count root once", base: []uint64{0}, next: []uint64{11}, entries: entry(11, 11, 0), want: true},
		{name: "unconsumed output", base: []uint64{0, 0}, next: []uint64{11, 12}, entries: entry(11)},
		{name: "duplicate output", base: []uint64{0, 0}, next: []uint64{11, 11}, entries: entry(11)},
		{name: "foreign attachment", base: []uint64{0}, next: []uint64{11}, entries: entry(11, 12)},
		{name: "nonzero predecessor", base: []uint64{10}, next: []uint64{11}, entries: entry(11)},
		{name: "zero output", base: []uint64{0}, next: []uint64{0}, entries: entry(0)},
		{name: "missing descriptors", base: []uint64{0}, next: []uint64{11}},
		{name: "base system alias", base: []uint64{0}, next: []uint64{1}, entries: entry(1)},
		{name: "new system alias", base: []uint64{0}, next: []uint64{2}, entries: entry(2)},
		{name: "user alias", base: []uint64{0}, next: []uint64{11}, entries: entry(11), user: 11},
		{name: "implicit roots alias", base: []uint64{0}, next: []uint64{11}, entries: entry(11), user: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := orderedRootCollectionDescriptorTransitionsCoveredEntries(nil, tc.entries, tc.user, 1, 2, tc.base, tc.next); got != tc.want {
				t.Fatalf("covered=%t want %t", got, tc.want)
			}
		})
	}
}

func TestPublishOrderedRootCommandWALBatchReferenceDeltaWarmRetargetAndAbort(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer func() {
		if err := db.Close(); err != nil && !errors.Is(err, ErrRecoveryRequired) {
			t.Error(err)
		}
	}()
	first := appendPointersInNewSegment(t, dir, 0, 1, 620_000, 1, func(int) []byte { return []byte("first") })[0]
	second := appendPointersInNewSegment(t, dir, 0, 2, 630_000, 1, func(int) []byte { return []byte("second") })[0]
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	publish := func(base uint64, ptr page.ValuePtr, commandKey string) (uint64, error) {
		t.Helper()
		it := mustFrozenSystemPointerMemtable(t, "doc/p", ptr).NewIterator(nil, nil)
		delta, err := OrderedRootDeltaBatchFromIterator(it)
		_ = it.Close()
		if err != nil {
			t.Fatal(err)
		}
		defer delta.Close()
		_, roots, err := db.PublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder(
			[]OrderedRootDeltaBatchPublishInput{{BaseRoot: base, Delta: delta}},
			mustRawKVCommandWALIntent(t, db, commandKey, "1"),
			func(roots []uint64) (iterator.UnsafeIterator, error) {
				return mustFrozenRawMemtable(t, maintenanceTestCollectionRootKey, encodeMaintenanceRootID(roots[0])).NewIterator(nil, nil), nil
			})
		if err != nil {
			return 0, err
		}
		return roots[0], nil
	}
	root, err := publish(0, first, "cmd/cold")
	if err != nil {
		t.Fatal(err)
	}
	assertCandidateTrackerMatchesFullScan(t, db)
	var scans atomic.Int64
	db.testScanCandidateExternalReferencesHook = func() { scans.Add(1) }
	root, err = publish(root, second, "cmd/warm")
	db.testScanCandidateExternalReferencesHook = nil
	if err != nil {
		t.Fatal(err)
	}
	if scans.Load() != 0 {
		t.Fatalf("warm covered batch scans=%d want 0", scans.Load())
	}
	assertCandidateTrackerMatchesFullScan(t, db)
	counts := snapshotCandidateTracker(db).counts
	if counts[first.FileID] != 0 || counts[second.FileID] != 1 {
		t.Fatalf("warm replacement counts=%v", counts)
	}
	if got := readCollectionRootValue(t, db, maintenanceTestCollectionRootKey, []byte("doc/p")); string(got) != "second" {
		t.Fatalf("warm replacement value=%q", got)
	}
	before := snapshotCandidateTracker(db)
	beforeSystemRoot := db.meta.SystemRootPageID
	db.testFailDurableRootVisibleInstall.Store(true)
	_, err = publish(root, first, "cmd/abort")
	db.testFailDurableRootVisibleInstall.Store(false)
	if !errors.Is(err, errTestDurableRootVisibleInstallFailpoint) {
		t.Fatalf("publish error=%v, want visible-install failpoint", err)
	}
	if got := snapshotCandidateTracker(db); !reflect.DeepEqual(got, before) {
		t.Fatalf("aborted candidate changed tracker: before=%+v after=%+v", before, got)
	}
	if db.currentCommitSeq() != before.sequence || db.meta.SystemRootPageID != beforeSystemRoot {
		t.Fatal("aborted candidate changed visible sequence or system root")
	}
	// A command-WAL post-append failure requires reopen; it must never turn
	// into a same-handle retry that applies the private delta a second time.
	if _, err := publish(root, first, "cmd/retry"); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("retry error=%v, want recovery required", err)
	}
}

func TestOrderedRootColdDescriptorDeltaGuards(t *testing.T) {
	key := []byte(collectionRootDescriptorPrefix + "cold")
	for _, tc := range []struct {
		name string
		edit func(*batch.Batch) error
		want bool
	}{
		{name: "inline attachment", edit: func(b *batch.Batch) error { return b.Set(key, encodeMaintenanceRootID(11)) }, want: true},
		{name: "malformed descriptor", edit: func(b *batch.Batch) error { return b.Set(key, []byte("bad")) }},
		{name: "pointer descriptor", edit: func(b *batch.Batch) error {
			return b.SetPointer(key, page.ValuePtr{FileID: page.ValueLogFileID(1), Offset: 10, Length: 8})
		}},
		{name: "delete descriptor", edit: func(b *batch.Batch) error { return b.Delete(key) }},
		{name: "descriptor range", edit: func(b *batch.Batch) error {
			if err := b.DeleteRange(collectionRootDescriptorPrefixBytes, collectionRootDescriptorPrefixEnd()); err != nil {
				return err
			}
			return b.Set(key, encodeMaintenanceRootID(11))
		}},
		{name: "overlay range", edit: func(b *batch.Batch) error {
			if err := b.DeleteRange(collectionRootOverlayDescriptorPrefixBytes, collectionRootOverlayDescriptorPrefixEnd()); err != nil {
				return err
			}
			return b.Set(key, encodeMaintenanceRootID(11))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := batch.New(nil, 1024)
			defer b.Close()
			if err := tc.edit(b); err != nil {
				t.Fatal(err)
			}
			if got := orderedRootCollectionDescriptorTransitionsCoveredByDelta(nil, b, 3, 1, 2, []uint64{0}, []uint64{11}); got != tc.want {
				t.Fatalf("covered=%t want %t", got, tc.want)
			}
		})
	}
}
