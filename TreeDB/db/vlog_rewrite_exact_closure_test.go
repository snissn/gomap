package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// This fixture exercises compressed physical outer leaves containing logical
// value pointers. Paged-leaf rewrite fixtures cannot expose candidate decoding.
func TestRewriteCompressedOuterLeavesExactClosure(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("rotated producer creation requires stable relative namespace authority")
	}
	const count, batchSize = 2048, 64
	for _, serialized := range []bool{false, true} {
		t.Run(fmt.Sprintf("serialized=%v", serialized), func(t *testing.T) {
			dir := t.TempDir()
			db, err := Open(Options{Dir: dir, Durability: DurabilityWALOffRelaxed,
				DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true,
				LeafPrefixCompression: true, IndexColumnarLeaves: true, IndexPackedValuePtr: true,
				LeafPageReadCacheEntries: -1, ValueLog: ValueLogOptions{ForcePointers: true}})
			if err != nil {
				t.Fatal(err)
			}
			log, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{Compression: ValueLogCompressionAuto})
			if err != nil {
				t.Fatal(err)
			}
			// Keep every size win so the fixture does not depend on timing estimates.
			log.(*rewriteWriter).SetKeepPolicy(0, 0, 0)
			db.SetLeafPageLog(log)
			defer log.Close()
			defer db.Close()
			old := appendPointersInNewSegment(t, dir, 0, 1, 10_000, count/2, func(int) []byte { return bytes.Repeat([]byte("old-value|"), 64) })
			old = append(old, appendPointersInNewSegment(t, dir, 0, 2, 20_000, count/2, func(int) []byte { return bytes.Repeat([]byte("old-value|"), 64) })...)
			fresh := appendPointersInNewSegment(t, dir, 0, 3, 30_000, count, func(int) []byte { return bytes.Repeat([]byte("new-value|"), 64) })
			if err := db.RefreshValueLogSet(); err != nil {
				t.Fatal(err)
			}
			key := func(i int) []byte { return []byte(fmt.Sprintf("rewrite-compressed-shared-prefix/%08d", i)) }
			for start := 0; start < count; start += count / 2 {
				if start != 0 {
					if err := log.(*rewriteWriter).rotateLeaf(); err != nil {
						t.Fatal(err)
					}
				}
				b := db.NewBatch().(*Batch)
				for i := start; i < start+count/2; i++ {
					if err := b.SetPointer(key(i), old[i]); err != nil {
						t.Fatal(err)
					}
				}
				if err := b.WriteSync(); err != nil {
					t.Fatal(err)
				}
				closeNoErr(t, b)
			}
			assertCandidateTrackerMatchesFullScan(t, db)
			snap := db.AcquireSnapshot()
			raw := make(map[uint32]struct{})
			compressed := make(map[uint32]bool)
			if err := leafrefscan.WalkRoots(context.Background(), []uint64{snap.state.RootPageID}, snap.idx.pager.Get, nil, func(ptr page.LeafLogPtr) error {
				raw[ptr.ValueLogFileID()] = struct{}{}
				if !page.ValuePtrIsGrouped(ptr.ValuePtr()) {
					return fmt.Errorf("fixture outer pointer is not grouped: %+v", ptr)
				}
				f, err := os.Open(db.valueLogManager.SegmentPath(ptr.ValueLogFileID()))
				if err != nil {
					return err
				}
				var header [valuelog.FrameHeaderSize]byte
				_, err = f.ReadAt(header[:], int64(ptr.Offset)-4+valuelog.HeaderSize)
				_ = f.Close()
				if err != nil {
					return err
				}
				if header[0] != valuelog.FrameVersion || header[2] == 0 {
					return fmt.Errorf("fixture has invalid physical frame header: %v", header)
				}
				if header[1]&valuelog.FrameFlagCompressed != 0 {
					compressed[ptr.ValueLogFileID()] = true
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(raw) < 2 || len(compressed) != len(raw) {
				t.Fatalf("fixture raw generations=%v compressed generations=%v", raw, compressed)
			}
			closeNoErr(t, snap)
			// A third producer generation ensures inherited raw membership must
			// disappear after its final reference is changed.
			if err := log.(*rewriteWriter).rotateLeaf(); err != nil {
				t.Fatal(err)
			}
			fullScans := 0
			db.testScanCandidateExternalReferencesHook = func() { fullScans++ }
			for start := 0; start < count; start += batchSize {
				swaps := make([]rewriteSwap, 0, batchSize)
				for i := start; i < start+batchSize; i++ {
					swaps = append(swaps, rewriteSwap{key: key(i), oldPtr: old[i], newPtr: fresh[i]})
				}
				beforeBodies := db.durableRootCandidateOuterBodies.Load()
				beforeLeafScans := db.durableRootCandidateLeafOnlyScans.Load()
				beforePages := db.durableRootCandidatePagesVisited.Load()
				if serialized {
					err = db.applyRewriteSwapBatchSerialized(swaps, true)
				} else {
					var committed bool
					committed, err = db.applyRewriteSwapBatchOptimistic(swaps, true)
					if err == nil && !committed {
						t.Fatal("uncontended optimistic rewrite declined")
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				if fullScans != 0 {
					t.Fatalf("supported fixed-B rewrite ran %d full candidate scans after batch %d, want 0", fullScans, start/batchSize)
				}
				if got := db.durableRootCandidateOuterBodies.Load() - beforeBodies; got != 0 {
					t.Fatalf("batch %d candidate projected %d outer bodies", start/batchSize, got)
				}
				if got := db.durableRootCandidateLeafOnlyScans.Load() - beforeLeafScans; got != 2 {
					t.Fatalf("batch %d leaf-only scans=%d want 2", start/batchSize, got)
				}
				if db.durableRootCandidatePagesVisited.Load() <= beforePages {
					t.Fatal("candidate did not visit pager topology")
				}
				db.testScanCandidateExternalReferencesHook = nil
				snap := db.AcquireSnapshot()
				beforeOracleBodies := db.durableRootCandidateOuterBodies.Load()
				want, err := db.scanCandidateExternalReferencesV1(snap)
				closeNoErr(t, snap)
				if err != nil {
					t.Fatal(err)
				}
				if db.durableRootCandidateOuterBodies.Load() <= beforeOracleBodies {
					t.Fatal("unchanged scanner did not project outer bodies")
				}
				got := make(map[uint32]struct{})
				for _, descriptor := range db.durableRoot.slotResources[db.durableRoot.slot].PhysicalDescriptors() {
					if descriptor.Kind == rootpublication.ResourceValueLog || descriptor.Kind == rootpublication.ResourceOuterLeafLog || descriptor.Kind == rootpublication.ResourceOuterLeafPack {
						got[uint32(descriptor.Generation)] = struct{}{}
					}
				}
				if !maps.Equal(got, want) {
					t.Fatalf("batch %d exact closure=%v full scanner=%v", start/batchSize, got, want)
				}
				assertCandidateTrackerMatchesFullScan(t, db)
				db.testScanCandidateExternalReferencesHook = func() { fullScans++ }
			}
			db.testScanCandidateExternalReferencesHook = nil
			if fullScans != 0 {
				t.Fatalf("supported fixed-B rewrite ran %d full candidate scans, want 0", fullScans)
			}
			final := make(map[uint32]bool)
			for _, descriptor := range db.durableRoot.slotResources[db.durableRoot.slot].PhysicalDescriptors() {
				final[uint32(descriptor.Generation)] = true
			}
			for id := range raw {
				if final[id] {
					t.Fatalf("dead raw generation %d remains in candidate", id)
				}
			}
			for _, ptr := range []page.ValuePtr{old[0], old[count-1]} {
				if final[ptr.FileID] {
					t.Fatalf("dead value segment %d remains in candidate", ptr.FileID)
				}
			}
			if !db.valueLogManager.ReadChecksumEnabled() {
				t.Fatal("owned read fixture must verify record CRCs")
			}
			beforeCRC := db.valueLogManager.ReadStats().RecordCRCChecks
			for i := 0; i < count; i++ {
				got, err := db.Get(key(i))
				if err != nil || !bytes.Equal(got, bytes.Repeat([]byte("new-value|"), 64)) {
					t.Fatalf("owned read %d: %q %v", i, got, err)
				}
			}
			if db.valueLogManager.ReadStats().RecordCRCChecks <= beforeCRC {
				t.Fatal("owned reads did not check record CRCs")
			}
			closeNoErr(t, db)
			closeNoErr(t, log.(*rewriteWriter))
			reopened, err := Open(Options{Dir: dir, Durability: DurabilityWALOffRelaxed,
				DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true,
				LeafPrefixCompression: true, IndexColumnarLeaves: true, IndexPackedValuePtr: true,
				ValueLog: ValueLogOptions{ForcePointers: true}})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			for i := 0; i < count; i++ {
				got, err := reopened.Get(key(i))
				if err != nil || !bytes.Equal(got, bytes.Repeat([]byte("new-value|"), 64)) {
					t.Fatalf("reopened owned read %d: %q %v", i, got, err)
				}
			}

		})
	}
}

func setupExactRewritePair(t *testing.T) (*DB, *rewriteWriter, page.ValuePtr, page.ValuePtr) {
	return setupExactRewritePairWithValueLogOptions(t, ValueLogOptions{ForcePointers: true})
}

func setupExactRewritePairWithValueLogOptions(t *testing.T, valueLogOptions ValueLogOptions) (*DB, *rewriteWriter, page.ValuePtr, page.ValuePtr) {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true,
		IndexOuterLeavesInValueLog: true, LeafPrefixCompression: true, IndexColumnarLeaves: true, IndexPackedValuePtr: true,
		ValueLog: valueLogOptions})
	if err != nil {
		t.Fatal(err)
	}
	log, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{Compression: ValueLogCompressionAuto})
	if err != nil {
		t.Fatal(err)
	}
	db.SetLeafPageLog(log)
	t.Cleanup(func() { _ = db.Close(); _ = log.Close() })
	old := appendPointersInNewSegment(t, dir, 0, 1, 40_000, 1, func(int) []byte { return []byte("old") })[0]
	fresh := appendPointersInNewSegment(t, dir, 0, 2, 50_000, 1, func(int) []byte { return []byte("new") })[0]
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	b := db.NewBatch().(*Batch)
	defer b.Close()
	for _, key := range []string{"a", "b"} {
		if err := b.SetPointer([]byte(key), old); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.WriteSync(); err != nil {
		t.Fatal(err)
	}
	assertCandidateTrackerMatchesFullScan(t, db)
	return db, log.(*rewriteWriter), old, fresh
}

func TestRewriteExactClosureFallbackRepairsOnlyOnActivation(t *testing.T) {
	for _, condition := range []string{"unknown", "stale", "underflow"} {
		t.Run(condition, func(t *testing.T) {
			db, _, old, fresh := setupExactRewritePair(t)
			tracker := db.valueLogRefTracker
			tracker.mu.Lock()
			switch condition {
			case "unknown":
				tracker.valid = false
			case "stale":
				tracker.commitSeq--
			case "underflow":
				tracker.counts[old.FileID] = 0
			}
			tracker.mu.Unlock()
			before := snapshotCandidateTracker(db)
			beforeSeq := db.currentCommitSeq()
			scans := 0
			db.testScanCandidateExternalReferencesHook = func() { scans++ }
			db.testFailDurableRootVisibleInstall.Store(true)
			err := db.applyRewriteSwapBatchSerialized([]rewriteSwap{{key: []byte("a"), oldPtr: old, newPtr: fresh}}, true)
			db.testFailDurableRootVisibleInstall.Store(false)
			if !errors.Is(err, errTestDurableRootVisibleInstallFailpoint) {
				t.Fatalf("abort error=%v", err)
			}
			if got := snapshotCandidateTracker(db); !reflect.DeepEqual(got, before) {
				t.Fatalf("abort changed tracker: before=%+v after=%+v", before, got)
			}
			if db.currentCommitSeq() != beforeSeq {
				t.Fatal("abort changed visible sequence")
			}
			if got, err := db.Get([]byte("a")); err != nil || !bytes.Equal(got, []byte("old")) {
				t.Fatalf("aborted read=%q %v", got, err)
			}
			if scans != 1 {
				t.Fatalf("aborted fallback scans=%d", scans)
			}
			if err := db.applyRewriteSwapBatchSerialized([]rewriteSwap{{key: []byte("a"), oldPtr: old, newPtr: fresh}}, true); err != nil {
				t.Fatal(err)
			}
			db.testScanCandidateExternalReferencesHook = nil
			if scans != 2 {
				t.Fatalf("fallback retry scans=%d", scans)
			}
			assertCandidateTrackerMatchesFullScan(t, db)
		})
	}
}

func TestRewriteExactClosureSharedNetZeroAndMismatch(t *testing.T) {
	db, _, old, fresh := setupExactRewritePair(t)
	scans := 0
	db.testScanCandidateExternalReferencesHook = func() { scans++ }
	// A same-pointer rewrite has a net-zero segment delta, still admitting the
	// exact raw topology. A mismatching old pointer must not publish anything.
	if err := db.applyRewriteSwapBatchSerialized([]rewriteSwap{{key: []byte("a"), oldPtr: old, newPtr: old}}, true); err != nil {
		t.Fatal(err)
	}
	before := snapshotCandidateTracker(db)
	if err := db.applyRewriteSwapBatchSerialized([]rewriteSwap{{key: []byte("a"), oldPtr: fresh, newPtr: old}}, true); err != nil {
		t.Fatal(err)
	}
	if got := snapshotCandidateTracker(db); !reflect.DeepEqual(got, before) {
		t.Fatalf("mismatch published: before=%+v after=%+v", before, got)
	}
	if err := db.applyRewriteSwapBatchSerialized([]rewriteSwap{{key: []byte("a"), oldPtr: old, newPtr: fresh}}, true); err != nil {
		t.Fatal(err)
	}
	counts := snapshotCandidateTracker(db).counts
	if counts[old.FileID] != 1 || counts[fresh.FileID] != 1 {
		t.Fatalf("shared-pointer counts=%v", counts)
	}
	if err := db.applyRewriteSwapBatchSerialized([]rewriteSwap{{key: []byte("b"), oldPtr: old, newPtr: fresh}}, true); err != nil {
		t.Fatal(err)
	}
	counts = snapshotCandidateTracker(db).counts
	if _, ok := counts[old.FileID]; ok || counts[fresh.FileID] != 2 {
		t.Fatalf("final shared-pointer counts=%v", counts)
	}
	db.testScanCandidateExternalReferencesHook = nil
	if scans != 0 {
		t.Fatalf("supported rewrites used %d full scans", scans)
	}
	assertCandidateTrackerMatchesFullScan(t, db)
}

func TestRewriteExactClosureCollectionDescriptorFallback(t *testing.T) {
	for _, policy := range []OrderedRootStoragePolicy{OrderedRootStoragePagerLeaves, OrderedRootStorageValueLogLeaves} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			db, _, old, fresh := setupExactRewritePair(t)
			_, roots, err := db.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{BaseRoot: 0,
				Iter: mustFrozenSystemPointerMemtable(t, "doc/p", old).NewIterator(nil, nil), StoragePolicy: policy}}, func(roots []uint64) (iterator.UnsafeIterator, error) {
				return mustFrozenRawMemtable(t, maintenanceTestCollectionRootKey, encodeMaintenanceRootID(roots[0]), "collections/root/users/alias", encodeMaintenanceRootID(roots[0])).NewIterator(nil, nil), nil
			})
			if err != nil {
				t.Fatal(err)
			}
			alias := "collections/root/users/alias"
			// Both aliases share one pointer-backed descriptor record. Replacing them
			// removes two logical descriptor references outside the collection delta.
			descriptor := appendPointersInNewSegment(t, db.dir, 0, 3, 60_000, 1, func(int) []byte { return encodeMaintenanceRootID(roots[0]) })[0]
			if err := db.RefreshValueLogSet(); err != nil {
				t.Fatal(err)
			}
			table := memtable.NewAppendOnlyWithEntryCapacity(2)
			table.SetEntry([]byte(maintenanceTestCollectionRootKey), nil, descriptor, node.FlagPointer)
			table.SetEntry([]byte(alias), nil, descriptor, node.FlagPointer)
			table.Freeze()
			if _, err := db.PublishSystemRootIterator(table.NewIterator(nil, nil)); err != nil {
				t.Fatal(err)
			}
			assertCandidateTrackerMatchesFullScan(t, db)
			if snapshotCandidateTracker(db).counts[descriptor.FileID] != 2 {
				t.Fatal("pointer-backed alias fixture did not retain both descriptors")
			}
			target := &collectionRewriteRootState{descriptorKey: []byte(maintenanceTestCollectionRootKey), descriptorAliases: [][]byte{[]byte(maintenanceTestCollectionRootKey), []byte(alias)}, rootID: roots[0], systemRoot: db.State().SystemRootPageID}
			scans := 0
			db.testScanCandidateExternalReferencesHook = func() { scans++ }
			err = db.applyRewriteSwapBatchToCollectionRoot(target, []rewriteSwap{{key: []byte("doc/p"), oldPtr: old, newPtr: fresh}}, true)
			db.testScanCandidateExternalReferencesHook = nil
			if err != nil {
				t.Fatal(err)
			}
			if scans != 1 {
				t.Fatalf("alias whole-publication fallback scans=%d want 1", scans)
			}
			assertCandidateTrackerMatchesFullScan(t, db)
			if _, ok := snapshotCandidateTracker(db).counts[descriptor.FileID]; ok {
				t.Fatal("removed descriptor segment retained in logical tracker")
			}
			for _, key := range []string{maintenanceTestCollectionRootKey, alias} {
				if got := readCollectionRootValue(t, db, key, []byte("doc/p")); !bytes.Equal(got, []byte("new")) {
					t.Fatalf("alias %s read=%q", key, got)
				}
			}
		})
	}
}

func TestRewriteExactClosureOrdinarySystemValue(t *testing.T) {
	db, _, old, fresh := setupExactRewritePair(t)
	if _, err := db.PublishSystemRootIterator(mustFrozenSystemPointerMemtable(t, "system/value", old).NewIterator(nil, nil)); err != nil {
		t.Fatal(err)
	}
	assertCandidateTrackerMatchesFullScan(t, db)
	scans := 0
	db.testScanCandidateExternalReferencesHook = func() { scans++ }
	err := db.applyRewriteSwapBatchToSystemRoot([]rewriteSwap{{key: []byte("system/value"), oldPtr: old, newPtr: fresh}}, true)
	db.testScanCandidateExternalReferencesHook = nil
	if err != nil {
		t.Fatal(err)
	}
	if scans != 0 {
		t.Fatalf("ordinary system value used %d full scans", scans)
	}
	assertCandidateTrackerMatchesFullScan(t, db)
	counts := snapshotCandidateTracker(db).counts
	if counts[old.FileID] != 2 || counts[fresh.FileID] != 1 {
		t.Fatalf("system rewrite counts=%v", counts)
	}
}

func TestRewriteExactClosureOptimisticConflict(t *testing.T) {
	db, _, old, fresh := setupExactRewritePair(t)
	beforeSeq := db.currentCommitSeq()
	var competing candidateTrackerSnapshot
	var competingDescriptors []rootpublication.StableResourcePhysicalDescriptor
	db.testAfterOptimisticPublishPrepareHook = func() {
		db.testAfterOptimisticPublishPrepareHook = nil
		// This is a real competing public publication after rewrite COW Apply.
		b := db.NewBatch().(*Batch)
		if err := b.SetPointer([]byte("b"), fresh); err != nil {
			t.Fatal(err)
		}
		if err := b.WriteSync(); err != nil {
			t.Fatal(err)
		}
		closeNoErr(t, b)
		competing = snapshotCandidateTracker(db)
		competingDescriptors = db.durableRoot.slotResources[db.durableRoot.slot].PhysicalDescriptors()
	}
	defer func() { db.testAfterOptimisticPublishPrepareHook = nil }()
	committed, err := db.applyRewriteSwapBatchOptimistic([]rewriteSwap{{key: []byte("a"), oldPtr: old, newPtr: fresh}}, true)
	if err != nil || committed {
		t.Fatalf("conflicting rewrite committed=%v err=%v", committed, err)
	}
	if db.currentCommitSeq() != beforeSeq+1 || !reflect.DeepEqual(snapshotCandidateTracker(db), competing) {
		t.Fatal("declined rewrite changed the competing publication tracker")
	}
	if !reflect.DeepEqual(db.durableRoot.slotResources[db.durableRoot.slot].PhysicalDescriptors(), competingDescriptors) {
		t.Fatal("declined rewrite changed the competing publication resources")
	}
	if got, err := db.Get([]byte("a")); err != nil || !bytes.Equal(got, []byte("old")) {
		t.Fatalf("conflicting rewrite read=%q %v", got, err)
	}
	assertCandidateTrackerMatchesFullScan(t, db)
	committed, err = db.applyRewriteSwapBatchOptimistic([]rewriteSwap{{key: []byte("a"), oldPtr: old, newPtr: fresh}}, true)
	if err != nil || !committed {
		t.Fatalf("retry committed=%v err=%v", committed, err)
	}
	assertCandidateTrackerMatchesFullScan(t, db)
}

func TestRewriteExactClosureInvalidNewestReopensOlderRawGeneration(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("rotated producer creation requires stable relative namespace authority")
	}
	db, writer, old, fresh := setupExactRewritePair(t)
	beforeSeq := db.currentCommitSeq()
	oldRaw := make(map[uint64]bool)
	for _, descriptor := range db.durableRoot.slotResources[db.durableRoot.slot].PhysicalDescriptors() {
		if descriptor.Kind == rootpublication.ResourceOuterLeafLog {
			oldRaw[descriptor.Generation] = true
		}
	}
	if err := writer.rotateLeaf(); err != nil {
		t.Fatal(err)
	}
	if err := db.applyRewriteSwapBatchSerialized([]rewriteSwap{{key: []byte("a"), oldPtr: old, newPtr: fresh}}, true); err != nil {
		t.Fatal(err)
	}
	var newestPath string
	for _, descriptor := range db.durableRoot.slotResources[db.durableRoot.slot].PhysicalDescriptors() {
		if descriptor.Kind == rootpublication.ResourceOuterLeafLog && !oldRaw[descriptor.Generation] {
			newestPath = db.valueLogManager.SegmentPath(uint32(descriptor.Generation))
		}
	}
	if newestPath == "" {
		t.Fatal("fixture did not switch raw producer generation")
	}
	older := db.durableRoot.slotResources[1-db.durableRoot.slot]
	foundOlder := false
	for _, descriptor := range older.PhysicalDescriptors() {
		if descriptor.Kind == rootpublication.ResourceOuterLeafLog && oldRaw[descriptor.Generation] {
			foundOlder = true
		}
	}
	if !foundOlder {
		t.Fatal("older recoverable slot lost its raw closure")
	}
	dir := db.dir
	closeNoErr(t, db)
	closeNoErr(t, writer)
	if err := os.Remove(newestPath); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(Options{Dir: dir, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true,
		IndexOuterLeavesInValueLog: true, LeafPrefixCompression: true, IndexColumnarLeaves: true, IndexPackedValuePtr: true,
		ValueLog: ValueLogOptions{ForcePointers: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.currentCommitSeq() != beforeSeq {
		t.Fatalf("fallback sequence=%d want %d", reopened.currentCommitSeq(), beforeSeq)
	}
	for _, key := range []string{"a", "b"} {
		if got, err := reopened.Get([]byte(key)); err != nil || !bytes.Equal(got, []byte("old")) {
			t.Fatalf("older raw root read %s=%q %v", key, got, err)
		}
	}
}
