package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

func buildSparsePagerTreeForTest(t *testing.T, p *pager.Pager, alloc vacuumAllocator, leafCount int, emptyEvery int, entryRevisions bool, valPtrs map[string]page.ValuePtr) (uint64, map[string][]byte, map[string]page.EntryRevision) {
	t.Helper()
	expectedVals := make(map[string][]byte)
	expectedRevs := make(map[string]page.EntryRevision)

	type childEntry struct {
		sep    []byte
		pageID uint64
	}
	var leaves []childEntry
	for i := 0; i < leafCount; i++ {
		pid, err := alloc.Alloc(0)
		if err != nil {
			t.Fatalf("alloc sparse leaf %d: %v", i, err)
		}
		buf, err := p.GetForWrite(pid)
		if err != nil {
			t.Fatalf("get sparse leaf %d: %v", i, err)
		}
		b := node.NewBuilderWithOptions(buf, page.PageTypeLeaf, node.BuilderOptions{
			EntryRevisions: entryRevisions,
		})
		b.SetPageID(pid)
		sep := []byte(fmt.Sprintf("k-%06d-0", i))
		if i == 0 {
			sep = []byte{}
		}
		isEmpty := i > 0 && emptyEvery > 0 && i%emptyEvery == 0
		if !isEmpty {
			for j := 0; j < 2; j++ {
				k := fmt.Sprintf("k-%06d-%d", i, j)
				rev := page.EntryRevision(0)
				if entryRevisions {
					rev = page.EntryRevision(i*10 + j + 1)
				}
				if ptr, ok := valPtrs[k]; ok {
					if err := b.AddLeafEntryWithRevision([]byte(k), nil, node.FlagPointer, ptr, rev); err != nil {
						t.Fatalf("add ptr entry %s: %v", k, err)
					}
				} else {
					v := []byte(fmt.Sprintf("val-%06d-%d", i, j))
					if err := b.AddLeafEntryWithRevision([]byte(k), v, node.FlagInline, page.ValuePtr{}, rev); err != nil {
						t.Fatalf("add inline entry %s: %v", k, err)
					}
					expectedVals[k] = v
				}
				if entryRevisions {
					expectedRevs[k] = rev
				}
			}
		}
		b.Finish()
		leaves = append(leaves, childEntry{sep: sep, pageID: pid})
	}

	// Build internal level(s) bottom-up.
	level := leaves
	for len(level) > 1 {
		var nextLevel []childEntry
		idx := 0
		for idx < len(level) {
			pid, err := alloc.Alloc(0)
			if err != nil {
				t.Fatalf("alloc internal: %v", err)
			}
			buf, err := p.GetForWrite(pid)
			if err != nil {
				t.Fatalf("get internal: %v", err)
			}
			b := node.NewBuilder(buf, page.PageTypeInternal)
			b.SetPageID(pid)
			firstSep := level[idx].sep
			if len(nextLevel) == 0 {
				firstSep = []byte{}
			}
			added := 0
			for idx < len(level) && added < 64 {
				k := level[idx].sep
				if added == 0 {
					k = []byte{}
				}
				if err := b.AddInternalChildRef(k, page.PageChildRef(level[idx].pageID)); err != nil {
					if err == node.ErrNodeFull && added > 0 {
						break
					}
					t.Fatalf("add internal child: %v", err)
				}
				idx++
				added++
			}
			b.Finish()
			nextLevel = append(nextLevel, childEntry{sep: firstSep, pageID: pid})
		}
		level = nextLevel
	}
	return level[0].pageID, expectedVals, expectedRevs
}

func inspectPagerRootLeafStats(t *testing.T, p *pager.Pager, rootID uint64) (totalPages, leafPages, emptyLeaves int, avgFillPct float64, anyRevisions bool) {
	t.Helper()
	var fillSum float64
	seen := make(map[uint64]struct{})
	var walk func(id uint64)
	walk = func(id uint64) {
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		img, err := p.Get(id)
		if err != nil {
			t.Fatalf("pager.Get(%d): %v", id, err)
		}
		totalPages++
		n := node.NewNode(img)
		switch n.Type() {
		case page.PageTypeLeaf:
			leafPages++
			if n.Count() == 0 {
				emptyLeaves++
			}
			if n.LeafEntryRevisionsEnabled() {
				anyRevisions = true
			}
			fillSum += float64(page.PageSize-n.FreeSpace()) / float64(page.PageSize)
		case page.PageTypeInternal:
			for i := uint16(0); i < n.Count(); i++ {
				_, ref, err := n.GetInternalEntryRefView(i)
				if err != nil {
					t.Fatalf("GetInternalEntryRefView: %v", err)
				}
				if ref.Kind == page.ChildRefPage {
					walk(ref.Page)
				}
			}
		}
	}
	walk(rootID)
	if leafPages > 0 {
		avgFillPct = (fillSum / float64(leafPages)) * 100.0
	}
	return
}

func TestVacuumIndexOnlineRepacksSparseCollectionRootsAndReclaimsSpace(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}

	t.Run("repacks_sparse_pager_root_with_pointers_revisions_and_concurrent_updates", func(t *testing.T) {
		dir := t.TempDir()
		opts := Options{
			Dir:                        dir,
			DisableBackgroundPrune:     true,
			IndexOuterLeavesInValueLog: false,
			LeafPrefixCompression:      true,
			ValueLog:                   ValueLogOptions{PointerThreshold: 128},
		}
		d, err := Open(opts)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}

		// Seed a real value-log pointer so the sparse collection root holds
		// genuine reachable ValueLog pointers.
		largeVal := bytes.Repeat([]byte("P"), 1024)
		seededPtr := appendPointersInNewSegment(t, dir, 0, 77, 770_000, 1, func(int) []byte {
			return largeVal
		})[0]
		if err := d.valueLogManager.Refresh(); err != nil {
			_ = d.Close()
			t.Fatalf("valueLogManager.Refresh: %v", err)
		}
		seedBatch := d.NewBatch().(*Batch)
		if err := seedBatch.SetPointer([]byte("ptr-seed"), seededPtr); err != nil {
			_ = seedBatch.Close()
			_ = d.Close()
			t.Fatalf("SetPointer ptr-seed: %v", err)
		}
		if err := seedBatch.WriteSync(); err != nil {
			_ = seedBatch.Close()
			_ = d.Close()
			t.Fatalf("WriteSync ptr-seed: %v", err)
		}
		_ = seedBatch.Close()

		// Create a sparse collection root with 180 leaves (every 3rd leaf empty,
		// others ~2% full) with EntryRevisions enabled + value-log pointer entries,
		// and publish it into the system tree.
		ptrMap := map[string]page.ValuePtr{
			"k-000001-0": seededPtr,
			"k-000010-1": seededPtr,
			"k-000100-0": seededPtr,
		}
		sparseRootID, expectedVals, expectedRevs := buildSparsePagerTreeForTest(t, d.idx.Load().pager, d.idx.Load().allocator, 180, 3, true, ptrMap)
		// Also create a second pager-backed collection root that will receive a
		// concurrent update during VacuumIndexOnline preclone.
		const (
			sparseDescKey     = "collections/root/docs/sparse_revs"
			concurrentDescKey = "collections/root/docs/concurrent"
		)
		mtConcurrent, err := memtable.NewWithCapacityMode(4<<10, memtable.ModeBTree)
		if err != nil {
			_ = d.Close()
			t.Fatal(err)
		}
		for i := 0; i < 40; i++ {
			mtConcurrent.Set([]byte(fmt.Sprintf("c-%04d", i)), []byte("v0"))
		}
		mtConcurrent.Freeze()
		var concurrentRootID uint64
		if _, _, err := d.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
			BaseRoot:      0,
			Iter:          mtConcurrent.NewIterator(nil, nil),
			StoragePolicy: OrderedRootStoragePagerLeaves,
		}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			concurrentRootID = rootIDs[0]
			return mustFrozenRawMemtable(t,
				sparseDescKey, encodeCollectionRootDescriptorRootID(sparseRootID),
				concurrentDescKey, encodeCollectionRootDescriptorRootID(rootIDs[0]),
			).NewIterator(nil, nil), nil
		}); err != nil {
			_ = d.Close()
			t.Fatalf("PublishOrderedRootGroupWithSystemBuilder: %v", err)
		}

		if err := d.Checkpoint(); err != nil {
			_ = d.Close()
			t.Fatalf("Checkpoint: %v", err)
		}

		beforePages, beforeLeaves, beforeEmpty, beforeFill, beforeRevs := inspectPagerRootLeafStats(t, d.idx.Load().pager, sparseRootID)
		if beforeLeaves != 180 || beforeEmpty == 0 || beforeFill >= 25.0 || !beforeRevs {
			_ = d.Close()
			t.Fatalf("unexpected initial sparse root: pages=%d leaves=%d empty=%d fill=%.1f%% revs=%v",
				beforePages, beforeLeaves, beforeEmpty, beforeFill, beforeRevs)
		}

		// Install preclone hook to inject a concurrent collection root update
		// during VacuumIndexOnline (exercising currentCollectionTokenLocked() ->
		// deferred cutover -> incremental reclone).
		reached := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once
		d.vacuumCollectionClonePageHook = func(phase vacuumCollectionClonePhase, _ uint64) {
			if phase != vacuumCollectionClonePreclone {
				return
			}
			once.Do(func() {
				close(reached)
				<-release
			})
		}

		vacErrCh := make(chan error, 1)
		go func() {
			vacErrCh <- d.VacuumIndexOnline(context.Background())
		}()

		select {
		case <-reached:
		case <-time.After(10 * time.Second):
			_ = d.Close()
			t.Fatal("timed out waiting for collection preclone hook")
		}

		mtUpdate, err := memtable.NewWithCapacityMode(4<<10, memtable.ModeBTree)
		if err != nil {
			close(release)
			_ = d.Close()
			t.Fatal(err)
		}
		mtUpdate.Set([]byte("c-0000"), []byte("v1-updated"))
		mtUpdate.Set([]byte("c-9999"), []byte("v1-inserted"))
		mtUpdate.Freeze()
		var updatedConcRoot uint64
		_, _, err = d.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder([]OrderedRootDeltaPublishInput{{
			BaseRoot:      concurrentRootID,
			Iter:          mtUpdate.NewIterator(nil, nil),
			StoragePolicy: OrderedRootStoragePagerLeaves,
		}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			updatedConcRoot = rootIDs[0]
			return mustFrozenRawMemtable(t,
				concurrentDescKey, encodeCollectionRootDescriptorRootID(rootIDs[0]),
			).NewIterator(nil, nil), nil
		})
		close(release)
		if err != nil {
			_ = d.Close()
			t.Fatalf("concurrent collection publish during vacuum: %v", err)
		}
		if updatedConcRoot == 0 {
			_ = d.Close()
			t.Fatalf("updatedConcRoot=0")
		}

		if err := <-vacErrCh; err != nil {
			_ = d.Close()
			t.Fatalf("VacuumIndexOnline: %v", err)
		}
		d.vacuumCollectionClonePageHook = nil

		vacStats := d.VacuumOnlineStats()
		if vacStats.PrecloneTraversalPages == 0 || vacStats.RecloneTraversalPages == 0 || vacStats.CutoverCloneTraversalPages != 0 || vacStats.DeferredCutovers == 0 {
			_ = d.Close()
			t.Fatalf("unexpected vacuum traversal stats: %+v", vacStats)
		}

		// Verify sparse root was repacked tightly with 0 empty leaves, >80% fill,
		// and exact values + ValuePtrs + EntryRevisions preserved.
		verifyAfter := func(dbHandle *DB, label string) {
			snap := dbHandle.AcquireSnapshot()
			if snap == nil {
				t.Fatalf("%s: missing snapshot", label)
			}
			defer func() { _ = snap.Close() }()
			descs, err := vacuumCollectCollectionRootDescriptors(snap.idx.pager, &snap.reader, snap.state.SystemRootPageID)
			if err != nil {
				t.Fatalf("%s: collect descriptors: %v", label, err)
			}
			rootByKey := make(map[string]uint64, len(descs))
			for _, desc := range descs {
				rootByKey[string(desc.key)] = desc.rootID
			}
			repackedSparse := rootByKey[sparseDescKey]
			if repackedSparse == 0 {
				t.Fatalf("%s: missing %s descriptor", label, sparseDescKey)
			}
			afterPages, afterLeaves, afterEmpty, afterFill, afterRevs := inspectPagerRootLeafStats(t, snap.idx.pager, repackedSparse)
			if afterEmpty != 0 {
				t.Fatalf("%s: repacked sparse root still has %d empty leaves (before=%d)", label, afterEmpty, beforeEmpty)
			}
			if afterPages >= beforePages/4 {
				t.Fatalf("%s: sparse root did not shrink via bulk repack: beforePages=%d afterPages=%d (beforeLeaves=%d afterLeaves=%d fill=%.1f%%)",
					label, beforePages, afterPages, beforeLeaves, afterLeaves, afterFill)
			}
			if afterLeaves > 1 && afterFill < 75.0 {
				t.Fatalf("%s: repacked avg leaf fill=%.1f%% want >= 75.0%% (afterLeaves=%d)", label, afterFill, afterLeaves)
			}
			if !afterRevs {
				t.Fatalf("%s: repacked root dropped LeafEntryRevisionsEnabled", label)
			}

			// Verify every key, inline value, ValuePtr, and EntryRevision verbatim.
			it, err := snap.IteratorAtRootWithOptions(repackedSparse, nil, nil, IteratorOptions{Mode: IteratorModePointerProjection})
			if err != nil {
				t.Fatalf("%s: iterator sparse root: %v", label, err)
			}
			seenCount := 0
			for ; it.Valid(); it.Next() {
				k := string(it.UnsafeKey())
				val, ptr, flags, rev := iterator.UnsafeEntryWithRevision(it)
				seenCount++
				wantRev, okRev := expectedRevs[k]
				if !okRev {
					t.Fatalf("%s: unexpected key %q in repacked root", label, k)
				}
				if rev != wantRev {
					t.Fatalf("%s: key %q rev=%d want %d", label, k, rev, wantRev)
				}
				if wantPtr, isPtr := ptrMap[k]; isPtr {
					if flags&node.FlagPointer == 0 || ptr != wantPtr {
						t.Fatalf("%s: key %q ptr=%+v flags=%x want ptr=%+v", label, k, ptr, flags, wantPtr)
					}
					resolved, err := snap.reader.ReadUnsafe(ptr)
					if err != nil || !bytes.Equal(resolved, largeVal) {
						t.Fatalf("%s: key %q resolve ptr err=%v len=%d", label, k, err, len(resolved))
					}
				} else {
					wantVal := expectedVals[k]
					if flags&node.FlagPointer != 0 || !bytes.Equal(val, wantVal) {
						t.Fatalf("%s: key %q val=%q flags=%x want %q", label, k, val, flags, wantVal)
					}
				}
			}
			if err := it.Error(); err != nil {
				t.Fatalf("%s: iter error: %v", label, err)
			}
			_ = it.Close()
			if seenCount != len(expectedRevs) {
				t.Fatalf("%s: seen %d keys want %d", label, seenCount, len(expectedRevs))
			}

			// Verify concurrent collection updates survived vacuum.
			repackedConc := rootByKey[concurrentDescKey]
			if repackedConc == 0 {
				t.Fatalf("%s: missing %s descriptor", label, concurrentDescKey)
			}
			trConc := tree.New(snap.idx.pager, &snap.reader, repackedConc)
			for _, tc := range []struct{ k, want string }{
				{"c-0000", "v1-updated"},
				{"c-0001", "v0"},
				{"c-9999", "v1-inserted"},
			} {
				got, err := trConc.Get([]byte(tc.k))
				if err != nil || string(got) != tc.want {
					t.Fatalf("%s: concurrent root Get(%q)=(%q,%v) want %q", label, tc.k, got, err, tc.want)
				}
			}
		}

		verifyAfter(d, "live-after-vacuum")

		// Supersede slot^1 with one more commit + checkpoint and verify 0 unaccounted pages!
		if err := d.SetSync([]byte("post-vac-supersede"), []byte("ok")); err != nil {
			_ = d.Close()
			t.Fatalf("SetSync post-vac-supersede: %v", err)
		}
		if err := d.Checkpoint(); err != nil {
			_ = d.Close()
			t.Fatalf("Checkpoint after vacuum: %v", err)
		}
		if err := d.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		// Inspect index.db for unaccounted pages after superseding slot^1!
		if leaked := countUnaccountedIndexPagesForTest(t, filepath.Join(dir, indexFileName)); leaked != 0 {
			t.Fatalf("unaccounted (leaked) pages after VacuumIndexOnline + superseding slot^1 = %d, want 0", leaked)
		}

		reopened, err := Open(opts)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer func() { _ = reopened.Close() }()
		verifyAfter(reopened, "after-reopen")
	})

	t.Run("mixed_leafref_root_falls_back_cleanly_and_offline_vacuum_repacks", func(t *testing.T) {
		dir := t.TempDir()
		opts := Options{
			Dir:                        dir,
			DisableBackgroundPrune:     true,
			IndexOuterLeavesInValueLog: false,
			LeafPrefixCompression:      true,
		}
		d, err := Open(opts)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}

		// Build a pure-pager sparse root in the live DB and a synthetic mixed
		// internal root in an isolated source pager (containing one pager-leaf
		// branch + one ChildRefLeafLog branch) to verify the pure-pager predicate
		// rejects mixed roots and falls back cleanly.
		sparseRootID, _, _ := buildSparsePagerTreeForTest(t, d.idx.Load().pager, d.idx.Load().allocator, 90, 3, false, nil)

		srcPager, err := pager.Open(filepath.Join(t.TempDir(), "src.db"), 64*1024)
		if err != nil {
			_ = d.Close()
			t.Fatal(err)
		}
		defer func() { _ = srcPager.Close() }()
		if _, err := srcPager.Alloc(2); err != nil {
			_ = d.Close()
			t.Fatal(err)
		}
		p := srcPager

		pagerLeafID, err := p.Alloc(1)
		if err != nil {
			t.Fatal(err)
		}
		leafBuf, err := p.GetForWrite(pagerLeafID)
		if err != nil {
			t.Fatal(err)
		}
		lb := node.NewBuilder(leafBuf, page.PageTypeLeaf)
		lb.SetPageID(pagerLeafID)
		if err := lb.AddLeafEntry([]byte("a"), []byte("1"), node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatal(err)
		}
		lb.Finish()

		pagerBranchID, err := p.Alloc(1)
		if err != nil {
			t.Fatal(err)
		}
		pbBuf, err := p.GetForWrite(pagerBranchID)
		if err != nil {
			t.Fatal(err)
		}
		pb := node.NewBuilder(pbBuf, page.PageTypeInternal)
		pb.SetPageID(pagerBranchID)
		if err := pb.AddInternalChildRef([]byte{}, page.PageChildRef(pagerLeafID)); err != nil {
			t.Fatal(err)
		}
		pb.Finish()

		leafRefBranchID, err := p.Alloc(1)
		if err != nil {
			t.Fatal(err)
		}
		lrbBuf, err := p.GetForWrite(leafRefBranchID)
		if err != nil {
			t.Fatal(err)
		}
		lrb := node.NewBuilder(lrbBuf, page.PageTypeInternal)
		lrb.SetPageID(leafRefBranchID)
		dummyLeafRef := page.LeafLogChildRef(page.LeafLogPtr{FileID: 7, Offset: 4096})
		if err := lrb.AddInternalChildRef([]byte("m"), dummyLeafRef); err != nil {
			t.Fatal(err)
		}
		lrb.Finish()

		mixedRootID, err := p.Alloc(1)
		if err != nil {
			t.Fatal(err)
		}
		mixBuf, err := p.GetForWrite(mixedRootID)
		if err != nil {
			t.Fatal(err)
		}
		mb := node.NewBuilder(mixBuf, page.PageTypeInternal)
		mb.SetPageID(mixedRootID)
		if err := mb.AddInternalChildRef([]byte{}, page.PageChildRef(pagerBranchID)); err != nil {
			t.Fatal(err)
		}
		if err := mb.AddInternalChildRef([]byte("m"), page.PageChildRef(leafRefBranchID)); err != nil {
			t.Fatal(err)
		}
		mb.Finish()

		// Directly exercise vacuumCopyCollectionRootWithObserver on the mixed root
		// into a separate destination pager: must fall back without error and
		// preserve both pager and ChildRefLeafLog branches verbatim!
		dstPager, err := pager.Open(filepath.Join(t.TempDir(), "dst.db"), 64*1024)
		if err != nil {
			_ = d.Close()
			t.Fatal(err)
		}
		defer func() { _ = dstPager.Close() }()
		if _, err := dstPager.Alloc(2); err != nil {
			_ = d.Close()
			t.Fatal(err)
		}
		dstAlloc := &pagerAllocator{p: dstPager}
		visitedPages := 0
		clonedMixed, err := vacuumCopyCollectionRootWithObserver(p, mixedRootID, dstAlloc, dstPager, func(uint64) { visitedPages++ })
		if err != nil {
			_ = d.Close()
			t.Fatalf("mixed root copy failed: %v", err)
		}
		if visitedPages != 4 {
			_ = d.Close()
			t.Fatalf("mixed root fallback visitSourcePage calls=%d want 4 (1x per source page)", visitedPages)
		}
		dstRootImg, err := dstPager.Get(clonedMixed)
		if err != nil {
			_ = d.Close()
			t.Fatal(err)
		}
		dstRootNode := node.NewNode(dstRootImg)
		if dstRootNode.Type() != page.PageTypeInternal || dstRootNode.Count() != 2 {
			_ = d.Close()
			t.Fatalf("mixed clone root type=%d count=%d want internal/2", dstRootNode.Type(), dstRootNode.Count())
		}
		_, branch1Ref, err := dstRootNode.GetInternalEntryRefView(1)
		if err != nil || branch1Ref.Kind != page.ChildRefPage {
			_ = d.Close()
			t.Fatalf("mixed clone branch[1]=%+v err=%v", branch1Ref, err)
		}
		branch1Img, err := dstPager.Get(branch1Ref.Page)
		if err != nil {
			_ = d.Close()
			t.Fatal(err)
		}
		_, ref1, err := node.NewNode(branch1Img).GetInternalEntryRefView(0)
		if err != nil || ref1 != dummyLeafRef {
			_ = d.Close()
			t.Fatalf("mixed clone leafref=%+v err=%v want %+v", ref1, err, dummyLeafRef)
		}

		// Publish sparse root and test VacuumIndexOffline repacking.
		const offlineDescKey = "collections/root/docs/offline_sparse"
		mt := mustFrozenRawMemtable(t, offlineDescKey, encodeCollectionRootDescriptorRootID(sparseRootID))
		if _, _, err := d.PublishOrderedRootGroupWithSystemBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
			return mt.NewIterator(nil, nil), nil
		}); err != nil {
			_ = d.Close()
			t.Fatalf("publish offline sparse root: %v", err)
		}
		if err := d.Checkpoint(); err != nil {
			_ = d.Close()
			t.Fatal(err)
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}

		if err := VacuumIndexOffline(opts); err != nil {
			t.Fatalf("VacuumIndexOffline: %v", err)
		}

		reopened, err := Open(opts)
		if err != nil {
			t.Fatalf("reopen after offline vacuum: %v", err)
		}
		defer func() { _ = reopened.Close() }()
		snap := reopened.AcquireSnapshot()
		defer func() { _ = snap.Close() }()
		descs, err := vacuumCollectCollectionRootDescriptors(snap.idx.pager, &snap.reader, snap.state.SystemRootPageID)
		if err != nil {
			t.Fatal(err)
		}
		var repacked uint64
		for _, desc := range descs {
			if string(desc.key) == offlineDescKey {
				repacked = desc.rootID
			}
		}
		if repacked == 0 {
			t.Fatalf("missing %s after offline vacuum", offlineDescKey)
		}
		afterPages, _, afterEmpty, _, _ := inspectPagerRootLeafStats(t, snap.idx.pager, repacked)
		if afterEmpty != 0 || afterPages >= 25 {
			t.Fatalf("offline vacuum did not repack sparse collection root: afterPages=%d afterEmpty=%d (before=90+ leaves)", afterPages, afterEmpty)
		}
	})
}

type rawTestIndexSource []byte

func (s rawTestIndexSource) ReadPage(pageID uint64) ([]byte, error) {
	off := int(pageID) * page.PageSize
	if off < 0 || off+page.PageSize > len(s) {
		return nil, fmt.Errorf("page %d OOB", pageID)
	}
	return s[off : off+page.PageSize], nil
}

func countUnaccountedIndexPagesForTest(t *testing.T, indexPath string) uint64 {
	t.Helper()
	raw, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("read %s: %v", indexPath, err)
	}
	src := rawTestIndexSource(raw)
	totalPages := uint64(len(raw) / page.PageSize)

	reachable := make(map[uint64]struct{})
	var walkTree func(uint64)
	walkTree = func(rootID uint64) {
		if rootID == 0 || rootID >= totalPages {
			return
		}
		if _, ok := reachable[rootID]; ok {
			return
		}
		reachable[rootID] = struct{}{}
		img, err := src.ReadPage(rootID)
		if err != nil {
			return
		}
		n := node.NewNode(img)
		if n.Type() == page.PageTypeInternal {
			for i := uint16(0); i < n.Count(); i++ {
				_, ref, err := n.GetInternalEntryRefView(i)
				if err == nil && ref.Kind == page.ChildRefPage {
					walkTree(ref.Page)
				}
			}
		}
	}

	var bestRec rootpublication.DurableRootRecordV1
	var bestSeq uint64
	for slot := uint64(0); slot < 2; slot++ {
		img, err := src.ReadPage(slot)
		if err != nil || !page.VerifyChecksumNonMutating(img) {
			continue
		}
		meta, err := page.DecodeDurableMetaV1(img[page.PageHeaderSize:])
		if err != nil || meta.CommitSeq == 0 {
			continue
		}
		recImg, err := src.ReadPage(meta.RootRecordPageID)
		if err != nil {
			continue
		}
		rec, err := rootpublication.DecodeDurableRootRecordV1(recImg, meta.RootRecordPageID, meta.RootRecordDigest)
		if err != nil {
			continue
		}
		walkTree(rec.UserRootPageID)
		walkTree(rec.SystemRootPageID)
		// Walk collection root descriptors in SystemRootPageID.
		sysSeen := make(map[uint64]struct{})
		var walkSys func(uint64)
		walkSys = func(id uint64) {
			if id == 0 || id >= totalPages {
				return
			}
			if _, ok := sysSeen[id]; ok {
				return
			}
			sysSeen[id] = struct{}{}
			pImg, err := src.ReadPage(id)
			if err != nil {
				return
			}
			n := node.NewNode(pImg)
			switch n.Type() {
			case page.PageTypeLeaf:
				for i := uint16(0); i < n.Count(); i++ {
					k, v, _, flags, err := n.GetLeafEntryView(i)
					if err != nil || flags&(node.FlagTombstone|node.FlagPointer) != 0 {
						continue
					}
					if bytes.HasPrefix(k, vacuumCollectionRootDescriptorPrefixBytes) && len(v) == 8 {
						walkTree(binary.BigEndian.Uint64(v))
					} else if bytes.HasPrefix(k, vacuumCollectionRootOverlayDescriptorPrefixBytes) && len(v)%8 == 0 {
						for j := 0; j < len(v); j += 8 {
							walkTree(binary.BigEndian.Uint64(v[j : j+8]))
						}
					}
				}
			case page.PageTypeInternal:
				for i := uint16(0); i < n.Count(); i++ {
					_, ref, err := n.GetInternalEntryRefView(i)
					if err == nil && ref.Kind == page.ChildRefPage {
						walkSys(ref.Page)
					}
				}
			}
		}
		walkSys(rec.SystemRootPageID)
		if meta.CommitSeq > bestSeq {
			bestSeq = meta.CommitSeq
			bestRec = rec
		}
	}

	gen, err := freelist.LoadGenerationV1(src, bestRec.Freelist)
	if err != nil {
		t.Fatalf("LoadGenerationV1: %v", err)
	}
	highWater := gen.HighWater()
	if totalPages < highWater {
		highWater = totalPages
	}
	var leaked uint64
	for id := uint64(2); id < highWater; id++ {
		if _, ok := reachable[id]; ok {
			continue
		}
		if gen.Allocatable(id) {
			continue
		}
		if unused, err := gen.SnapshotPageUnusedV1(id, ^uint64(0)); err == nil && unused {
			continue
		}
		img, err := src.ReadPage(id)
		if err != nil {
			continue
		}
		switch node.NewNode(img).Type() {
		case page.PageTypeFreelistGeneration,
			page.PageTypeFreelistIndex,
			page.PageTypeFreelistChunk,
			page.PageTypeFreelistReservation,
			page.PageTypeDependencyManifest,
			page.PageTypeDurableRootRecord:
			continue
		}
		leaked++
	}
	return leaked
}

func TestVacuumIndexOnlineMatchedSlotsConcurrentWrite(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	for _, hookKind := range []string{"after_base_snapshot", "during_preflush"} {
		t.Run(hookKind, func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{
				Dir:                        dir,
				DisableBackgroundPrune:     true,
				IndexOuterLeavesInValueLog: false,
			}
			d, err := Open(opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			closed := false
			defer func() {
				if !closed {
					_ = d.Close()
				}
			}()

			for i := 0; i < 64; i++ {
				k := []byte(fmt.Sprintf("init-%04d", i))
				v := bytes.Repeat([]byte{byte('a' + i%26)}, 128)
				if err := d.Set(k, v); err != nil {
					t.Fatalf("Set %d: %v", i, err)
				}
			}
			if err := d.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			// Converge both durable slots onto identical roots so VacuumIndexOnline
			// enters with olderReplacementSharedPending = true.
			if err := d.refreshDurableRootFallback(); err != nil {
				t.Fatalf("refreshDurableRootFallback: %v", err)
			}

			concurrentKey := []byte("concurrent-" + hookKind)
			concurrentVal := bytes.Repeat([]byte("Z"), 128)
			var injected atomic.Bool
			injectWrite := func() {
				if !injected.CompareAndSwap(false, true) {
					return
				}
				if err := d.SetSync(concurrentKey, concurrentVal); err != nil {
					t.Errorf("concurrent SetSync (%s): %v", hookKind, err)
				}
			}

			switch hookKind {
			case "after_base_snapshot":
				prev := testHookVacuumAfterBaseSnapshot
				testHookVacuumAfterBaseSnapshot = injectWrite
				defer func() { testHookVacuumAfterBaseSnapshot = prev }()
			case "during_preflush":
				d.vacuumPreflushHook = func() error {
					injectWrite()
					return nil
				}
				defer func() { d.vacuumPreflushHook = nil }()
			}

			if err := d.VacuumIndexOnline(context.Background()); err != nil {
				t.Fatalf("VacuumIndexOnline with concurrent write on matched slots (%s) failed: %v", hookKind, err)
			}
			if !injected.Load() {
				t.Fatalf("concurrent write hook %s never fired", hookKind)
			}
			stats := d.vacuumOnlineStatsSnapshot()
			if stats.DeferredCutovers == 0 {
				t.Fatalf("expected DeferredCutovers > 0 after concurrent write (%s), got %+v", hookKind, stats)
			}
			if stats.OlderRootRebuilds == 0 {
				t.Fatalf("expected OlderRootRebuilds > 0 when concurrent write diverges matched slots (%s), got %+v", hookKind, stats)
			}

			verify := func(stage string, cur *DB) {
				t.Helper()
				got, err := cur.Get(concurrentKey)
				if err != nil || !bytes.Equal(got, concurrentVal) {
					t.Fatalf("%s: Get(%q) len=%d err=%v want len=%d", stage, concurrentKey, len(got), err, len(concurrentVal))
				}
				for i := 0; i < 64; i++ {
					k := []byte(fmt.Sprintf("init-%04d", i))
					want := bytes.Repeat([]byte{byte('a' + i%26)}, 128)
					got, err := cur.Get(k)
					if err != nil || !bytes.Equal(got, want) {
						t.Fatalf("%s: Get(%q) mismatch err=%v", stage, k, err)
					}
				}
			}
			verify("after-vacuum", d)

			if err := d.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			closed = true

			reopened, err := Open(opts)
			if err != nil {
				t.Fatalf("Reopen: %v", err)
			}
			defer func() { _ = reopened.Close() }()
			verify("after-reopen", reopened)
		})
	}
}
