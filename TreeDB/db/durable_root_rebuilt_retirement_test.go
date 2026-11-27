package db

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestWriteRebuiltDurableRootsV1ExcludesSuccessorReachablePages(t *testing.T) {
	dir := t.TempDir()
	indexPath := filepath.Join(dir, indexFileName)
	p, err := pager.Open(indexPath, 64*1024)
	if err != nil {
		t.Fatalf("pager.Open: %v", err)
	}
	defer func() { _ = p.Close() }()

	if _, err := p.Alloc(2); err != nil {
		t.Fatalf("alloc meta pages: %v", err)
	}

	buildLeaf := func(entries [][2][]byte) uint64 {
		id, err := p.Alloc(1)
		if err != nil {
			t.Fatalf("alloc leaf: %v", err)
		}
		buf, err := p.GetForWrite(id)
		if err != nil {
			t.Fatalf("get leaf %d: %v", id, err)
		}
		b := node.NewBuilder(buf, page.PageTypeLeaf)
		b.SetPageID(id)
		for _, kv := range entries {
			if err := b.AddLeafEntry(kv[0], kv[1], node.FlagInline, page.ValuePtr{}); err != nil {
				t.Fatalf("add leaf entry %q: %v", kv[0], err)
			}
		}
		b.Finish()
		return id
	}
	buildInternal := func(children []struct {
		key  []byte
		page uint64
	}) uint64 {
		id, err := p.Alloc(1)
		if err != nil {
			t.Fatalf("alloc internal: %v", err)
		}
		buf, err := p.GetForWrite(id)
		if err != nil {
			t.Fatalf("get internal %d: %v", id, err)
		}
		b := node.NewBuilder(buf, page.PageTypeInternal)
		b.SetPageID(id)
		for _, ch := range children {
			if err := b.AddInternalChildRef(ch.key, page.PageChildRef(ch.page)); err != nil {
				t.Fatalf("add internal child %d: %v", ch.page, err)
			}
		}
		b.Finish()
		return id
	}

	// Build two distinct user roots that share a subtree leaf page (`sharedUserLeaf`).
	olderOnlyUserLeaf := buildLeaf([][2][]byte{{[]byte("a-old"), []byte("v-old")}})
	nextOnlyUserLeaf := buildLeaf([][2][]byte{{[]byte("a-new"), []byte("v-new")}})
	sharedUserLeaf := buildLeaf([][2][]byte{{[]byte("m-shared"), []byte("v-shared")}})

	olderUserRoot := buildInternal([]struct {
		key  []byte
		page uint64
	}{
		{key: []byte{}, page: olderOnlyUserLeaf},
		{key: []byte("m-shared"), page: sharedUserLeaf},
	})
	nextUserRoot := buildInternal([]struct {
		key  []byte
		page uint64
	}{
		{key: []byte{}, page: nextOnlyUserLeaf},
		{key: []byte("m-shared"), page: sharedUserLeaf},
	})

	// Build two distinct collection roots that share a collection leaf page (`sharedColLeaf`).
	olderOnlyColLeaf := buildLeaf([][2][]byte{{[]byte("ca-old"), []byte("cv-old")}})
	nextOnlyColLeaf := buildLeaf([][2][]byte{{[]byte("ca-new"), []byte("cv-new")}})
	sharedColLeaf := buildLeaf([][2][]byte{{[]byte("cm-shared"), []byte("cv-shared")}})

	olderColRoot := buildInternal([]struct {
		key  []byte
		page uint64
	}{
		{key: []byte{}, page: olderOnlyColLeaf},
		{key: []byte("cm-shared"), page: sharedColLeaf},
	})
	nextColRoot := buildInternal([]struct {
		key  []byte
		page uint64
	}{
		{key: []byte{}, page: nextOnlyColLeaf},
		{key: []byte("cm-shared"), page: sharedColLeaf},
	})

	// Prepare non-nil dependency-directory V2 resources so both slots build
	// independent dependency directory trees (`directory.RootPageID != 0`),
	// proving that successor directory pages are excluded from retirement while
	// replaced older directory pages are retired at older.CommitSeq.
	srcDB, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open dependency directory source db: %v", err)
	}
	defer func() { _ = srcDB.Close() }()
	srcDB.rootReuseMu.Lock()
	boundResources, _, err := srcDB.prepareDependencyDirectoryV2(srcDB.idx.Load(), 2, nil, nil)
	srcDB.rootReuseMu.Unlock()
	if err != nil {
		t.Fatalf("prepareDependencyDirectoryV2: %v", err)
	}
	defer boundResources.Release()

	// Build two distinct 2-level system roots that share a child leaf (`sharedSysLeaf`)
	// while holding distinct collection root descriptors (`olderColRoot` vs `nextColRoot`).
	olderOnlySysLeaf := buildLeaf([][2][]byte{
		{[]byte("collections/root/shared_guard/primary"), encodeCollectionRootDescriptorRootID(olderColRoot)},
	})
	nextOnlySysLeaf := buildLeaf([][2][]byte{
		{[]byte("collections/root/shared_guard/primary"), encodeCollectionRootDescriptorRootID(nextColRoot)},
	})
	sharedSysLeaf := buildLeaf([][2][]byte{
		{[]byte("sys/shared-meta"), []byte("shared-sys-val")},
	})

	olderSysRoot := buildInternal([]struct {
		key  []byte
		page uint64
	}{
		{key: []byte{}, page: olderOnlySysLeaf},
		{key: []byte("sys/shared-meta"), page: sharedSysLeaf},
	})
	nextSysRoot := buildInternal([]struct {
		key  []byte
		page uint64
	}{
		{key: []byte{}, page: nextOnlySysLeaf},
		{key: []byte("sys/shared-meta"), page: sharedSysLeaf},
	})

	// Allocate genuinely unused spare pages (enough that some remain in
	// FreelistFree even after slot 0/1 auxiliary metadata placement) to verify
	// filteredFree keeps unrelated free pages while defensively excluding
	// roots[0].pages.
	var unusedSparePages []uint64
	for i := 0; i < 16; i++ {
		unusedSparePages = append(unusedSparePages, buildLeaf([][2][]byte{{[]byte(fmt.Sprintf("spare-%02d", i)), []byte("unused")}}))
	}

	olderOnlyPages := []uint64{
		olderUserRoot,
		olderOnlyUserLeaf,
		olderSysRoot,
		olderOnlySysLeaf,
		olderColRoot,
		olderOnlyColLeaf,
	}
	sharedPages := []uint64{
		sharedUserLeaf,
		sharedSysLeaf,
		sharedColLeaf,
	}
	allOlderPages := append(append([]uint64(nil), olderOnlyPages...), sharedPages...)

	older := rebuiltDurableRootV1{
		meta: page.MetaPageBody{
			CommitSeq:        10,
			UserRootPageID:   olderUserRoot,
			SystemRootPageID: olderSysRoot,
		},
		resources: boundResources,
		pages:     allOlderPages,
	}
	next := rebuiltDurableRootV1{
		meta: page.MetaPageBody{
			CommitSeq:        11,
			UserRootPageID:   nextUserRoot,
			SystemRootPageID: nextSysRoot,
		},
		resources: boundResources,
	}

	// Pass an initialFree containing BOTH an older-slot page (must be defensively
	// filtered out!) and unrelated spare pages (must remain allocatable/usable).
	initialFree := append([]uint64{olderOnlyUserLeaf}, unusedSparePages...)
	if err := writeRebuiltDurableRootsV1WithFree(dir, indexPath, p, []rebuiltDurableRootV1{older, next}, initialFree); err != nil {
		t.Fatalf("writeRebuiltDurableRootsV1WithFree: %v", err)
	}

	selected, err := selectDurableRootV1(p, p.PageCount(), nil, dependencyDirectoryStructureValidatorV2(p))
	if err != nil {
		t.Fatalf("selectDurableRootV1: %v", err)
	}
	if selected.Slot != MetaPage1ID || selected.Record.CommitSeq != next.meta.CommitSeq {
		t.Fatalf("selected slot=%d commit=%d want slot=%d commit=%d", selected.Slot, selected.Record.CommitSeq, MetaPage1ID, next.meta.CommitSeq)
	}
	olderDirRoot := selected.SlotRecords[MetaPage0ID].Directory.RootPageID
	nextDirRoot := selected.SlotRecords[MetaPage1ID].Directory.RootPageID
	if olderDirRoot == 0 || nextDirRoot == 0 || olderDirRoot == nextDirRoot {
		t.Fatalf("expected distinct non-zero dependency directory roots: older=%d next=%d", olderDirRoot, nextDirRoot)
	}
	olderDirPages, err := tree.New(p, nil, olderDirRoot).CollectPageIDs()
	if err != nil || len(olderDirPages) == 0 {
		t.Fatalf("collect older directory pages: pages=%v err=%v", olderDirPages, err)
	}
	nextDirPages, err := tree.New(p, nil, nextDirRoot).CollectPageIDs()
	if err != nil || len(nextDirPages) == 0 {
		t.Fatalf("collect next directory pages: pages=%v err=%v", nextDirPages, err)
	}

	// Verify collectRebuiltSuccessorReachablePagesV1 covers user, system,
	// collection, and dependency-directory pages of the successor.
	successorReachable, err := collectRebuiltSuccessorReachablePagesV1(p, nil, next.meta, selected.Record.Directory)
	if err != nil {
		t.Fatalf("collectRebuiltSuccessorReachablePagesV1: %v", err)
	}
	for _, id := range append(append([]uint64(nil), sharedPages...), nextDirPages...) {
		if _, ok := successorReachable[id]; !ok {
			t.Fatalf("collectRebuiltSuccessorReachablePagesV1 omitted successor page %d", id)
		}
	}

	// Verify filteredFree defensively excluded olderOnlyUserLeaf (so slot 0's
	// live B-tree leaf was never overwritten by metadata allocation and still
	// reads back intact) while keeping unrelated spare pages in the freelist.
	if got, err := tree.New(p, nil, olderUserRoot).Get([]byte("a-old")); err != nil || string(got) != "v-old" {
		t.Fatalf("olderOnlyUserLeaf corrupted (leaked into filteredFree): got=%q err=%v", got, err)
	}
	gen0, err := freelist.LoadGenerationV1(p, selected.SlotRecords[MetaPage0ID].Freelist)
	if err != nil {
		t.Fatalf("LoadGenerationV1(slot0): %v", err)
	}
	if gen0.Allocatable(olderOnlyUserLeaf) {
		t.Fatalf("olderOnlyUserLeaf %d leaked into slot0 FreelistFree", olderOnlyUserLeaf)
	}
	spareFreeCount := 0
	for _, id := range unusedSparePages {
		if selected.Freelist.Allocatable(id) {
			spareFreeCount++
		}
	}
	if spareFreeCount == 0 {
		t.Fatalf("expected unrelated initialFree spare pages to remain allocatable in FreelistFree (freeCount=%d)", selected.Freelist.FreeCount())
	}

	// Shared pages AND successor dependency-directory pages MUST NOT be retired or allocatable.
	protectedSuccessorPages := append(append([]uint64(nil), sharedPages...), nextDirPages...)
	for _, id := range protectedSuccessorPages {
		if selected.Freelist.Allocatable(id) {
			t.Fatalf("successor-reachable page %d is unexpectedly allocatable", id)
		}
		unused, err := selected.Freelist.SnapshotPageUnusedV1(id, ^uint64(0))
		if err != nil {
			t.Fatalf("SnapshotPageUnusedV1(successor page %d): %v", id, err)
		}
		if unused {
			t.Fatalf("successor-reachable page %d was erroneously retired in appendRebuiltDurableRootV1", id)
		}
	}

	// Strictly older-only pages AND replaced older dependency-directory pages
	// MUST be retired at older.meta.CommitSeq (protected while oldestCommit ==
	// older.CommitSeq, reusable once oldestCommit > older.CommitSeq).
	expectedRetiredOlderPages := append(append([]uint64(nil), olderOnlyPages...), olderDirPages...)
	for _, id := range expectedRetiredOlderPages {
		if selected.Freelist.Allocatable(id) {
			t.Fatalf("older-only page %d became allocatable while older slot is still recoverable", id)
		}
		unusedAtOlder, err := selected.Freelist.SnapshotPageUnusedV1(id, older.meta.CommitSeq)
		if err != nil {
			t.Fatalf("SnapshotPageUnusedV1(older page %d, olderSeq): %v", id, err)
		}
		if unusedAtOlder {
			t.Fatalf("older-only page %d reported reusable at older.CommitSeq=%d", id, older.meta.CommitSeq)
		}
		unusedAfterSupersede, err := selected.Freelist.SnapshotPageUnusedV1(id, older.meta.CommitSeq+1)
		if err != nil {
			t.Fatalf("SnapshotPageUnusedV1(older page %d, olderSeq+1): %v", id, err)
		}
		if !unusedAfterSupersede {
			t.Fatalf("older-only page %d was not retired at older.CommitSeq=%d", id, older.meta.CommitSeq)
		}
	}

	// Simulate superseding slot 0 with a reuse capability whose OldestCommitSeq > older.CommitSeq:
	// older-only pages (including replaced older directory pages) must promote
	// to FreelistFree while shared and successor directory pages remain non-allocatable.
	allocator := freelist.New(p, 0)
	if err := allocator.EnableCOWV1(selected.Freelist, freelist.NewReservationLedger()); err != nil {
		t.Fatalf("EnableCOWV1: %v", err)
	}
	capAfterSupersede, err := freelist.NewReuseCapability(next.meta.CommitSeq, next.meta.CommitSeq, 0)
	if err != nil {
		t.Fatalf("NewReuseCapability: %v", err)
	}
	var candidateID freelist.CandidateIDV1
	candidateID[0] = 42
	prepared, err := allocator.PrepareCOWCandidateV1(
		selected.Freelist.GenerationID()+1,
		next.meta.CommitSeq+1,
		candidateID,
		capAfterSupersede,
		0,
		freelist.NewMemoryPageStoreV1(),
	)
	if err != nil {
		t.Fatalf("PrepareCOWCandidateV1: %v", err)
	}
	promotedGen := prepared.Candidate().Generation()
	for _, id := range expectedRetiredOlderPages {
		if !promotedGen.Allocatable(id) {
			t.Fatalf("older-only page %d did not promote to FreelistFree after superseding older slot", id)
		}
	}
	for _, id := range protectedSuccessorPages {
		if promotedGen.Allocatable(id) {
			t.Fatalf("successor-reachable page %d became allocatable after superseding older slot", id)
		}
	}
}

func collectSlotReachablePagesForTest(t *testing.T, p *pager.Pager, reader tree.SlabReader, rec rootpublication.DurableRootRecordV1) map[uint64]struct{} {
	t.Helper()
	out := make(map[uint64]struct{})
	walk := func(rootID uint64) {
		if rootID == 0 {
			return
		}
		if err := tree.New(p, nil, rootID).WalkPages(func(id uint64, _ node.Node) error {
			out[id] = struct{}{}
			return nil
		}); err != nil {
			t.Fatalf("WalkPages(root=%d): %v", rootID, err)
		}
	}
	walk(rec.UserRootPageID)
	walk(rec.SystemRootPageID)
	entries, err := vacuumCollectCollectionEntriesFromRoot(context.Background(), p, reader, rec.SystemRootPageID)
	if err != nil {
		t.Fatalf("vacuumCollectCollectionEntriesFromRoot(sys=%d): %v", rec.SystemRootPageID, err)
	}
	for _, entry := range entries {
		for _, rid := range entry.sourceRootIDs {
			walk(rid)
		}
	}
	return out
}

func copyDirExceptLockForTest(t *testing.T, srcDir, dstDir string) {
	t.Helper()
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", dstDir, err)
	}
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", srcDir, err)
	}
	for _, ent := range entries {
		name := ent.Name()
		if name == "LOCK" {
			continue
		}
		srcPath := filepath.Join(srcDir, name)
		dstPath := filepath.Join(dstDir, name)
		if ent.IsDir() {
			copyDirExceptLockForTest(t, srcPath, dstPath)
			continue
		}
		in, err := os.Open(srcPath)
		if err != nil {
			t.Fatalf("Open(%s): %v", srcPath, err)
		}
		out, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			_ = in.Close()
			t.Fatalf("Create(%s): %v", dstPath, err)
		}
		if _, err := io.Copy(out, in); err != nil {
			_ = in.Close()
			_ = out.Close()
			t.Fatalf("Copy(%s -> %s): %v", srcPath, dstPath, err)
		}
		_ = in.Close()
		if err := out.Close(); err != nil {
			t.Fatalf("Close(%s): %v", dstPath, err)
		}
	}
}

func TestVacuumIndexOnlineRebuiltSlotRetirementAndFallbackRecovery(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}

	const descKey = "collections/root/rebuilt_recovery/primary"

	seedFixture := func(t *testing.T, dir string) (
		d *DB,
		opts Options,
		expectedV1User map[string][]byte,
		expectedV1Col map[string][]byte,
		expectedV2User map[string][]byte,
		expectedV2Col map[string][]byte,
	) {
		t.Helper()
		opts = Options{
			Dir:                        dir,
			ResolvedProfile:            ProfileNoWALFast,
			Durability:                 DurabilityWALOffRelaxed,
			IndexOuterLeavesInValueLog: false,
			DisableBackgroundPrune:     true,
			LeafPrefixCompression:      true,
		}
		var err error
		d, err = Open(opts)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}

		buildColRoot := func(prefix string, count int) (uint64, map[string][]byte) {
			gen := d.idx.Load()
			mt, err := memtable.NewWithCapacityMode(64*1024, memtable.ModeHashSorted)
			if err != nil {
				t.Fatalf("memtable: %v", err)
			}
			want := make(map[string][]byte, count)
			payload := bytes.Repeat([]byte("C"), 180)
			for i := 0; i < count; i++ {
				k := fmt.Sprintf("col-%04d", i)
				v := append([]byte(fmt.Sprintf("%s-%04d:", prefix, i)), payload...)
				mt.Set([]byte(k), v)
				want[k] = v
			}
			mt.Freeze()
			it := mt.NewIterator(nil, nil)
			rootID, err := bulk.BuildWithOptions(it, gen.allocator, gen.pager, bulk.BuildOptions{
				LeafPrefixCompression: true,
			})
			_ = it.Close()
			if err != nil {
				t.Fatalf("bulk build col root (%s): %v", prefix, err)
			}
			return rootID, want
		}

		// Generation 1: user keys + multi-leaf pager-backed collection root.
		colRoot1, col1 := buildColRoot("v1", 120)
		expectedV1Col = col1
		expectedV1User = make(map[string][]byte, 160)
		userPayload := bytes.Repeat([]byte("U"), 140)
		b1 := d.NewBatch()
		for i := 0; i < 160; i++ {
			k := fmt.Sprintf("user-%04d", i)
			v := append([]byte(fmt.Sprintf("v1-%04d:", i)), userPayload...)
			if err := b1.Set([]byte(k), v); err != nil {
				t.Fatalf("b1.Set(%s): %v", k, err)
			}
			expectedV1User[k] = v
		}
		if err := b1.WriteSync(); err != nil {
			_ = b1.Close()
			t.Fatalf("b1.WriteSync: %v", err)
		}
		_ = b1.Close()
		if _, _, err := d.PublishOrderedRootGroupWithSystemBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
			return mustFrozenRawMemtable(t, descKey, encodeCollectionRootDescriptorRootID(colRoot1)).NewIterator(nil, nil), nil
		}); err != nil {
			t.Fatalf("publish system root v1: %v", err)
		}
		if err := d.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint v1: %v", err)
		}

		// Generation 2: mutate user keys AND replace collection root so slot0 and slot1 diverge.
		colRoot2, col2 := buildColRoot("v2", 140)
		expectedV2Col = col2
		expectedV2User = make(map[string][]byte, 200)
		for k, v := range expectedV1User {
			expectedV2User[k] = v
		}
		b2 := d.NewBatch()
		for i := 0; i < 100; i++ {
			k := fmt.Sprintf("user-%04d", i)
			v := append([]byte(fmt.Sprintf("v2-%04d:", i)), userPayload...)
			if err := b2.Set([]byte(k), v); err != nil {
				t.Fatalf("b2.Set(%s): %v", k, err)
			}
			expectedV2User[k] = v
		}
		for i := 160; i < 200; i++ {
			k := fmt.Sprintf("user-%04d", i)
			v := append([]byte(fmt.Sprintf("v2-%04d:", i)), userPayload...)
			if err := b2.Set([]byte(k), v); err != nil {
				t.Fatalf("b2.Set(%s): %v", k, err)
			}
			expectedV2User[k] = v
		}
		if err := b2.WriteSync(); err != nil {
			_ = b2.Close()
			t.Fatalf("b2.WriteSync: %v", err)
		}
		_ = b2.Close()
		if _, _, err := d.PublishOrderedRootGroupWithSystemBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
			return mustFrozenRawMemtable(t, descKey, encodeCollectionRootDescriptorRootID(colRoot2)).NewIterator(nil, nil), nil
		}); err != nil {
			t.Fatalf("publish system root v2: %v", err)
		}
		if err := d.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint v2: %v", err)
		}

		// Ensure slot 0 and slot 1 are both valid and distinct before returning.
		d.durablePublishMu.Lock()
		s0 := d.durableRoot.slotRecord[0]
		s1 := d.durableRoot.slotRecord[1]
		d.durablePublishMu.Unlock()
		if s0.CommitSeq == 0 || s1.CommitSeq == 0 || s0.UserRootPageID == s1.UserRootPageID {
			// Ensure the active/fallback slots differ in user root as well.
			if err := d.SetSync([]byte("user-0000"), append([]byte("v2-extra:"), userPayload...)); err != nil {
				t.Fatalf("SetSync diverge: %v", err)
			}
			expectedV2User["user-0000"] = append([]byte("v2-extra:"), userPayload...)
		}
		return d, opts, expectedV1User, expectedV1Col, expectedV2User, expectedV2Col
	}

	verifyDBContents := func(t *testing.T, label string, d *DB, wantUser map[string][]byte, wantCol map[string][]byte) {
		t.Helper()
		for k, want := range wantUser {
			got, err := d.Get([]byte(k))
			if err != nil {
				t.Fatalf("%s: Get(%q): %v", label, k, err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("%s: Get(%q) mismatch: got len=%d want len=%d", label, k, len(got), len(want))
			}
		}
		snap := d.AcquireSnapshot()
		if snap == nil {
			t.Fatalf("%s: AcquireSnapshot nil", label)
		}
		defer func() { _ = snap.Close() }()
		descs, err := vacuumCollectCollectionRootDescriptors(snap.idx.pager, &snap.reader, snap.state.SystemRootPageID)
		if err != nil || len(descs) != 1 {
			t.Fatalf("%s: descriptors=%+v err=%v want 1", label, descs, err)
		}
		it, err := snap.IteratorAtRootWithOptions(descs[0].rootID, nil, nil, IteratorOptions{})
		if err != nil {
			t.Fatalf("%s: IteratorAtRootWithOptions: %v", label, err)
		}
		seen := 0
		for ; it.Valid(); it.Next() {
			k := string(it.UnsafeKey())
			val, _, flags := it.UnsafeEntry()
			if flags&node.FlagPointer != 0 {
				_ = it.Close()
				t.Fatalf("%s: unexpected pointer flag on col key %q", label, k)
			}
			want, ok := wantCol[k]
			if !ok || !bytes.Equal(val, want) {
				_ = it.Close()
				t.Fatalf("%s: col key %q mismatch (ok=%v)", label, k, ok)
			}
			seen++
		}
		if err := it.Close(); err != nil {
			t.Fatalf("%s: close col iter: %v", label, err)
		}
		if seen != len(wantCol) {
			t.Fatalf("%s: col key count=%d want %d", label, seen, len(wantCol))
		}
	}

	t.Run("distinct_slot_fallback_protection", func(t *testing.T) {
		dir := t.TempDir()
		d, opts, _, _, _, _ := seedFixture(t, dir)

		// Capture exact expected contents of the older durable slot BEFORE vacuum so
		// fallback assertions match whatever generation resides in slot^1.
		d.durablePublishMu.Lock()
		olderSlotBefore := d.durableRoot.slot ^ 1
		olderBeforeRec := d.durableRoot.slotRecord[olderSlotBefore]
		newerBeforeRec := d.durableRoot.slotRecord[d.durableRoot.slot]
		d.durablePublishMu.Unlock()
		if olderBeforeRec.CommitSeq == 0 || newerBeforeRec.CommitSeq == 0 ||
			(olderBeforeRec.UserRootPageID == newerBeforeRec.UserRootPageID && olderBeforeRec.SystemRootPageID == newerBeforeRec.SystemRootPageID) {
			_ = d.Close()
			t.Fatalf("expected two distinct durable slots before vacuum: older=%+v newer=%+v", olderBeforeRec, newerBeforeRec)
		}

		// Snapshot older slot's exact user and collection key-values prior to vacuum.
		olderWantUser := make(map[string][]byte)
		olderWantCol := make(map[string][]byte)
		{
			gen := d.idx.Load()
			it := tree.New(gen.pager, d.valueLogManager, olderBeforeRec.UserRootPageID).Iterator(nil, nil)
			for ; it.Valid(); it.Next() {
				olderWantUser[string(it.Key())] = append([]byte(nil), it.Value()...)
			}
			if err := it.Close(); err != nil {
				_ = d.Close()
				t.Fatalf("close older user iter: %v", err)
			}
			descs, err := vacuumCollectCollectionRootDescriptors(gen.pager, d.valueLogManager, olderBeforeRec.SystemRootPageID)
			if err != nil || len(descs) != 1 {
				_ = d.Close()
				t.Fatalf("older collection descriptors=%+v err=%v", descs, err)
			}
			cit := tree.New(gen.pager, d.valueLogManager, descs[0].rootID).Iterator(nil, nil)
			for ; cit.Valid(); cit.Next() {
				olderWantCol[string(cit.Key())] = append([]byte(nil), cit.Value()...)
			}
			if err := cit.Close(); err != nil {
				_ = d.Close()
				t.Fatalf("close older col iter: %v", err)
			}
		}

		vacStats, err := d.VacuumIndexOnlineWithStats(context.Background())
		if err != nil {
			_ = d.Close()
			t.Fatalf("VacuumIndexOnlineWithStats: %v", err)
		}
		if vacStats.OlderRootRebuilds == 0 {
			_ = d.Close()
			t.Fatalf("expected distinct-slot older rebuild, got OlderRootRebuilds=0")
		}

		d.durablePublishMu.Lock()
		activeSlot := d.durableRoot.slot
		rec0 := d.durableRoot.slotRecord[MetaPage0ID]
		rec1 := d.durableRoot.slotRecord[MetaPage1ID]
		d.durablePublishMu.Unlock()
		if activeSlot != MetaPage1ID || rec0.CommitSeq == 0 || rec1.CommitSeq <= rec0.CommitSeq {
			_ = d.Close()
			t.Fatalf("unexpected post-vacuum slots: active=%d rec0=%+v rec1=%+v", activeSlot, rec0, rec1)
		}
		if rec0.UserRootPageID == rec1.UserRootPageID && rec0.SystemRootPageID == rec1.SystemRootPageID {
			_ = d.Close()
			t.Fatalf("expected distinct rebuilt slots: rec0=%+v rec1=%+v", rec0, rec1)
		}

		p := d.idx.Load().pager
		slot0Reachable := collectSlotReachablePagesForTest(t, p, d.valueLogManager, rec0)
		slot1Reachable := collectSlotReachablePagesForTest(t, p, d.valueLogManager, rec1)
		olderOnly := make([]uint64, 0)
		for id := range slot0Reachable {
			if _, inSlot1 := slot1Reachable[id]; !inSlot1 {
				olderOnly = append(olderOnly, id)
			}
		}
		if len(olderOnly) == 0 {
			_ = d.Close()
			t.Fatalf("expected non-empty strictly older-only pages in rebuilt slot0")
		}
		gen1, err := freelist.LoadGenerationV1(p, rec1.Freelist)
		if err != nil {
			_ = d.Close()
			t.Fatalf("LoadGenerationV1(slot1): %v", err)
		}
		for _, id := range olderOnly {
			if gen1.Allocatable(id) {
				_ = d.Close()
				t.Fatalf("older-only page %d is allocatable while MetaPage0ID is recoverable", id)
			}
			unusedNow, err := gen1.SnapshotPageUnusedV1(id, rec0.CommitSeq)
			if err != nil || unusedNow {
				_ = d.Close()
				t.Fatalf("older-only page %d not protected at older CommitSeq=%d: unused=%v err=%v", id, rec0.CommitSeq, unusedNow, err)
			}
			unusedLater, err := gen1.SnapshotPageUnusedV1(id, rec0.CommitSeq+1)
			if err != nil || !unusedLater {
				_ = d.Close()
				t.Fatalf("older-only page %d not retired at older CommitSeq=%d: unused=%v err=%v", id, rec0.CommitSeq, unusedLater, err)
			}
		}

		if err := d.Close(); err != nil {
			t.Fatalf("Close before corrupting MetaPage1ID: %v", err)
		}

		// Corrupt MetaPage1ID so recovery must select fallback MetaPage0ID.
		corruptIndexPageByte(t, dir, MetaPage1ID)

		recovered, err := Open(opts)
		if err != nil {
			t.Fatalf("Open fallback after corrupting MetaPage1ID: %v", err)
		}
		defer closeNoErr(t, recovered)

		if recovered.metaPageID != MetaPage0ID {
			t.Fatalf("recovered slot=%d want fallback MetaPage0ID=%d", recovered.metaPageID, MetaPage0ID)
		}
		if recovered.State().CommitSeq != rec0.CommitSeq {
			t.Fatalf("recovered CommitSeq=%d want fallback %d", recovered.State().CommitSeq, rec0.CommitSeq)
		}
		verifyDBContents(t, "distinct-slot-fallback", recovered, olderWantUser, olderWantCol)

		// Verify all fallback-reachable pages remain protected in the recovered freelist.
		recoveredGen, err := freelist.LoadGenerationV1(recovered.idx.Load().pager, recovered.durableRoot.record.Freelist)
		if err != nil {
			t.Fatalf("LoadGenerationV1(recovered): %v", err)
		}
		for id := range slot0Reachable {
			if recoveredGen.Allocatable(id) {
				t.Fatalf("fallback-reachable page %d is allocatable after fallback recovery", id)
			}
		}
	})

	t.Run("post_supersede_reuse", func(t *testing.T) {
		dir := t.TempDir()
		d, opts, _, _, expectedV2User, expectedV2Col := seedFixture(t, dir)

		vacStats, err := d.VacuumIndexOnlineWithStats(context.Background())
		if err != nil {
			_ = d.Close()
			t.Fatalf("VacuumIndexOnlineWithStats: %v", err)
		}
		if vacStats.OlderRootRebuilds == 0 {
			_ = d.Close()
			t.Fatalf("expected distinct-slot older rebuild, got OlderRootRebuilds=0")
		}

		d.durablePublishMu.Lock()
		rec0 := d.durableRoot.slotRecord[MetaPage0ID]
		rec1 := d.durableRoot.slotRecord[MetaPage1ID]
		d.durablePublishMu.Unlock()

		p := d.idx.Load().pager
		slot0Reachable := collectSlotReachablePagesForTest(t, p, d.valueLogManager, rec0)
		slot1Reachable := collectSlotReachablePagesForTest(t, p, d.valueLogManager, rec1)
		olderOnlySet := make(map[uint64]struct{})
		for id := range slot0Reachable {
			if _, inSlot1 := slot1Reachable[id]; !inSlot1 {
				olderOnlySet[id] = struct{}{}
			}
		}
		if len(olderOnlySet) == 0 {
			_ = d.Close()
			t.Fatalf("expected non-empty older-only pages")
		}
		retiredBefore := rec1.FreelistRetiredCount
		if retiredBefore < uint64(len(olderOnlySet)) {
			_ = d.Close()
			t.Fatalf("FreelistRetiredCount=%d < olderOnly=%d", retiredBefore, len(olderOnlySet))
		}

		// Commit 1 overwrites MetaPage0ID (superseding rec0); commit 2 runs with
		// OldestCommitSeq > rec0.CommitSeq and promotes olderOnly pages to FreelistFree.
		payload := bytes.Repeat([]byte("P"), 140)
		for step := 1; step <= 2; step++ {
			k := fmt.Sprintf("supersede-step-%d", step)
			v := append([]byte(fmt.Sprintf("step-%d:", step)), payload...)
			if err := d.SetSync([]byte(k), v); err != nil {
				_ = d.Close()
				t.Fatalf("SetSync step %d: %v", step, err)
			}
			expectedV2User[k] = v
		}

		d.durablePublishMu.Lock()
		afterSupersedeRec := d.durableRoot.record
		d.durablePublishMu.Unlock()
		genAfterSupersede, err := freelist.LoadGenerationV1(d.idx.Load().pager, afterSupersedeRec.Freelist)
		if err != nil {
			_ = d.Close()
			t.Fatalf("LoadGenerationV1(after supersede): %v", err)
		}
		promotedOrReused := 0
		for id := range olderOnlySet {
			if genAfterSupersede.Allocatable(id) {
				promotedOrReused++
				continue
			}
			// Could already have been reused during commit 2!
			unused, err := genAfterSupersede.SnapshotPageUnusedV1(id, ^uint64(0))
			if err == nil && !unused {
				promotedOrReused++
			}
		}
		if promotedOrReused == 0 {
			_ = d.Close()
			t.Fatalf("no older-only retired pages promoted to FreelistFree after superseding MetaPage0ID (retiredBefore=%d retiredAfter=%d freeAfter=%d)",
				retiredBefore, afterSupersedeRec.FreelistRetiredCount, afterSupersedeRec.FreelistFreeCount)
		}

		// Now write enough new keys across two commits to trigger page allocations
		// and verify actual physical reuse of superseded older-only page IDs
		// (either as live B-tree pages or low-placed durable metadata/auxiliary
		// pages owned by the active generation).
		beforeReuseCounters := d.idx.Load().allocator.Counters()
		bReuse := d.NewBatch()
		for i := 0; i < 200; i++ {
			k := fmt.Sprintf("reuse-key-%04d", i)
			v := append([]byte(fmt.Sprintf("reuse-%04d:", i)), payload...)
			if err := bReuse.Set([]byte(k), v); err != nil {
				_ = bReuse.Close()
				_ = d.Close()
				t.Fatalf("bReuse.Set(%s): %v", k, err)
			}
			expectedV2User[k] = v
		}
		if err := bReuse.WriteSync(); err != nil {
			_ = bReuse.Close()
			_ = d.Close()
			t.Fatalf("bReuse.WriteSync: %v", err)
		}
		_ = bReuse.Close()
		afterReuseCounters := d.idx.Load().allocator.Counters()

		d.durablePublishMu.Lock()
		finalMeta0 := d.durableRoot.slotMeta[MetaPage0ID]
		finalMeta1 := d.durableRoot.slotMeta[MetaPage1ID]
		finalRec0 := d.durableRoot.slotRecord[MetaPage0ID]
		finalRec1 := d.durableRoot.slotRecord[MetaPage1ID]
		finalActiveRec := d.durableRoot.record
		d.durablePublishMu.Unlock()

		liveOrOwnedAfterReuse := make(map[uint64]struct{})
		for _, rec := range []rootpublication.DurableRootRecordV1{finalRec0, finalRec1} {
			if rec.CommitSeq == 0 {
				continue
			}
			for id := range collectSlotReachablePagesForTest(t, d.idx.Load().pager, d.valueLogManager, rec) {
				liveOrOwnedAfterReuse[id] = struct{}{}
			}
			if rec.Directory.RootPageID != 0 {
				if dirPages, err := tree.New(d.idx.Load().pager, nil, rec.Directory.RootPageID).CollectPageIDs(); err == nil {
					for _, id := range dirPages {
						liveOrOwnedAfterReuse[id] = struct{}{}
					}
				}
			} else if rec.Manifest.PageCount > 0 {
				for off := uint64(0); off < uint64(rec.Manifest.PageCount); off++ {
					liveOrOwnedAfterReuse[rec.Manifest.FirstPageID+off] = struct{}{}
				}
			}
			if flGen, err := freelist.LoadGenerationV1(d.idx.Load().pager, rec.Freelist); err == nil {
				liveOrOwnedAfterReuse[rec.Freelist.HeaderPageID] = struct{}{}
				for _, ext := range flGen.ReservationRecord().Extents {
					if ext.Kind == freelist.ReservationTargetMetadata {
						for off := uint64(0); off < uint64(ext.Count); off++ {
							liveOrOwnedAfterReuse[ext.StartPageID+off] = struct{}{}
						}
					}
				}
			}
		}
		if finalMeta0.RootRecordPageID >= 2 {
			liveOrOwnedAfterReuse[finalMeta0.RootRecordPageID] = struct{}{}
		}
		if finalMeta1.RootRecordPageID >= 2 {
			liveOrOwnedAfterReuse[finalMeta1.RootRecordPageID] = struct{}{}
		}

		reusedCount := 0
		for id := range olderOnlySet {
			if _, ok := liveOrOwnedAfterReuse[id]; ok {
				reusedCount++
			}
		}
		if reusedCount == 0 {
			_ = d.Close()
			t.Fatalf("expected superseded older-only pages to be physically reused by subsequent allocations (olderOnly=%d, freeAfter=%d, reuseDelta=%d)",
				len(olderOnlySet), finalActiveRec.FreelistFreeCount, afterReuseCounters.ReuseAllocPages-beforeReuseCounters.ReuseAllocPages)
		}
		if afterReuseCounters.ReuseAllocPages <= beforeReuseCounters.ReuseAllocPages {
			_ = d.Close()
			t.Fatalf("expected allocator ReuseAllocPages to increase after superseding older slot: before=%+v after=%+v",
				beforeReuseCounters, afterReuseCounters)
		}

		if err := d.Close(); err != nil {
			t.Fatalf("Close after reuse: %v", err)
		}
		reopened, err := Open(opts)
		if err != nil {
			t.Fatalf("reopen after reuse: %v", err)
		}
		defer closeNoErr(t, reopened)
		verifyDBContents(t, "post-supersede-reopen", reopened, expectedV2User, expectedV2Col)
	})

	t.Run("shared_slot_fallback_recovery", func(t *testing.T) {
		dir := t.TempDir()
		d, opts, _, _, expectedV2User, expectedV2Col := seedFixture(t, dir)

		if err := d.refreshDurableRootFallback(); err != nil {
			_ = d.Close()
			t.Fatalf("refreshDurableRootFallback: %v", err)
		}

		vacStats, err := d.VacuumIndexOnlineWithStats(context.Background())
		if err != nil {
			_ = d.Close()
			t.Fatalf("VacuumIndexOnlineWithStats(shared): %v", err)
		}
		if vacStats.OlderRootRebuilds != 0 {
			_ = d.Close()
			t.Fatalf("expected shared-slot vacuum (OlderRootRebuilds=0), got %d", vacStats.OlderRootRebuilds)
		}

		d.durablePublishMu.Lock()
		meta0 := d.durableRoot.slotMeta[MetaPage0ID]
		meta1 := d.durableRoot.slotMeta[MetaPage1ID]
		rec0 := d.durableRoot.slotRecord[MetaPage0ID]
		rec1 := d.durableRoot.slotRecord[MetaPage1ID]
		d.durablePublishMu.Unlock()

		if rec0.CommitSeq == 0 || rec1.CommitSeq <= rec0.CommitSeq {
			_ = d.Close()
			t.Fatalf("invalid shared-slot commit seqs: rec0=%+v rec1=%+v", rec0, rec1)
		}
		if rec0.UserRootPageID != rec1.UserRootPageID || rec0.SystemRootPageID != rec1.SystemRootPageID {
			_ = d.Close()
			t.Fatalf("expected shared roots across slots: rec0=%d/%d rec1=%d/%d",
				rec0.UserRootPageID, rec0.SystemRootPageID, rec1.UserRootPageID, rec1.SystemRootPageID)
		}
		if meta0.RootRecordPageID == meta1.RootRecordPageID {
			_ = d.Close()
			t.Fatalf("shared slots must retain distinct DurableRootRecordV1 pages: both=%d", meta0.RootRecordPageID)
		}
		if rec0.Directory.RootPageID != 0 {
			if rec0.Directory.RootPageID == rec1.Directory.RootPageID {
				_ = d.Close()
				t.Fatalf("shared slots must retain independent dependency directories: both=%d", rec0.Directory.RootPageID)
			}
		} else if rec0.Manifest == rec1.Manifest {
			_ = d.Close()
			t.Fatalf("shared slots must retain independent dependency manifests: both=%+v", rec0.Manifest)
		}

		if err := d.Close(); err != nil {
			t.Fatalf("Close before corrupting MetaPage1ID: %v", err)
		}
		corruptIndexPageByte(t, dir, MetaPage1ID)

		recovered, err := Open(opts)
		if err != nil {
			t.Fatalf("Open shared-slot fallback: %v", err)
		}
		defer closeNoErr(t, recovered)

		if recovered.metaPageID != MetaPage0ID || recovered.State().CommitSeq != rec0.CommitSeq {
			t.Fatalf("shared-slot fallback slot=%d seq=%d want slot=%d seq=%d",
				recovered.metaPageID, recovered.State().CommitSeq, MetaPage0ID, rec0.CommitSeq)
		}
		if _, err := freelist.LoadGenerationV1(recovered.idx.Load().pager, recovered.durableRoot.record.Freelist); err != nil {
			t.Fatalf("LoadGenerationV1(shared fallback): %v", err)
		}
		if recovered.durableRoot.record.Directory.RootPageID == 0 {
			if _, err := rootpublication.LoadDependencyManifestV1(recovered.idx.Load().pager, recovered.durableRoot.record.Manifest); err != nil {
				t.Fatalf("LoadDependencyManifestV1(shared fallback): %v", err)
			}
		}
		verifyDBContents(t, "shared-slot-fallback", recovered, expectedV2User, expectedV2Col)
	})

	t.Run("shared_slot_crash_between_final_sync_and_swap", func(t *testing.T) {
		dir := t.TempDir()
		d, opts, _, _, expectedV2User, expectedV2Col := seedFixture(t, dir)

		if err := d.refreshDurableRootFallback(); err != nil {
			_ = d.Close()
			t.Fatalf("refreshDurableRootFallback: %v", err)
		}

		crashPreSwapDir := t.TempDir()
		crashMidSwapDir := t.TempDir()
		capturedPreSwap := false
		capturedMidSwap := false

		restore := durabilitycut.Install(func(event durabilitycut.Event) error {
			if event.Namespace == durabilitycut.NamespaceCreate &&
				filepath.Clean(event.NewPath) == filepath.Join(dir, indexReadyFileName) &&
				!capturedPreSwap {
				copyDirExceptLockForTest(t, dir, crashPreSwapDir)
				capturedPreSwap = true
			}
			if event.Namespace == durabilitycut.NamespaceRename &&
				filepath.Clean(event.OldPath) == filepath.Join(dir, indexFileName) &&
				filepath.Clean(event.NewPath) == filepath.Join(dir, indexBakFileName) &&
				!capturedMidSwap {
				copyDirExceptLockForTest(t, dir, crashMidSwapDir)
				capturedMidSwap = true
			}
			return nil
		})
		var restoreOnce sync.Once
		restoreCut := func() { restoreOnce.Do(restore) }
		defer restoreCut()
		vacStats, err := d.VacuumIndexOnlineWithStats(context.Background())
		restoreCut()
		if err != nil {
			_ = d.Close()
			t.Fatalf("VacuumIndexOnlineWithStats(crash cut): %v", err)
		}
		if vacStats.OlderRootRebuilds != 0 {
			_ = d.Close()
			t.Fatalf("expected shared-slot vacuum (OlderRootRebuilds=0), got %d", vacStats.OlderRootRebuilds)
		}
		if err := d.Close(); err != nil {
			t.Fatalf("Close live db: %v", err)
		}
		if !capturedPreSwap || !capturedMidSwap {
			t.Fatalf("expected both crash snapshots captured: preSwap=%v midSwap=%v", capturedPreSwap, capturedMidSwap)
		}

		// 1. Crash after final sync + ready marker, before old index rename:
		// recovery cleans up staging files and keeps original index.db intact.
		preSwapOpts := opts
		preSwapOpts.Dir = crashPreSwapDir
		preSwapDB, err := Open(preSwapOpts)
		if err != nil {
			t.Fatalf("Open pre-swap crash image: %v", err)
		}
		verifyDBContents(t, "shared-crash-pre-swap", preSwapDB, expectedV2User, expectedV2Col)
		if err := preSwapDB.Close(); err != nil {
			t.Fatalf("Close pre-swap crash image: %v", err)
		}
		for _, artifact := range []string{indexNewFileName, indexReadyFileName} {
			if _, err := os.Stat(filepath.Join(crashPreSwapDir, artifact)); !os.IsNotExist(err) {
				t.Fatalf("pre-swap recovery left stale artifact %s: %v", artifact, err)
			}
		}

		// 2. Crash after index.db -> index.db.bak rename, before index.db.new -> index.db:
		// recovery rolls forward the synced shared-root replacement and both slots remain recoverable.
		midSwapOpts := opts
		midSwapOpts.Dir = crashMidSwapDir
		midSwapDB, err := Open(midSwapOpts)
		if err != nil {
			t.Fatalf("Open mid-swap crash image: %v", err)
		}
		midSwapDB.durablePublishMu.Lock()
		midRec0 := midSwapDB.durableRoot.slotRecord[MetaPage0ID]
		midRec1 := midSwapDB.durableRoot.slotRecord[MetaPage1ID]
		midSwapDB.durablePublishMu.Unlock()
		if midRec0.CommitSeq == 0 || midRec1.CommitSeq <= midRec0.CommitSeq ||
			midRec0.UserRootPageID != midRec1.UserRootPageID || midRec0.SystemRootPageID != midRec1.SystemRootPageID {
			_ = midSwapDB.Close()
			t.Fatalf("mid-swap rolled-forward shared slots invalid: rec0=%+v rec1=%+v", midRec0, midRec1)
		}
		verifyDBContents(t, "shared-crash-mid-swap", midSwapDB, expectedV2User, expectedV2Col)
		if err := midSwapDB.Close(); err != nil {
			t.Fatalf("Close mid-swap crash image: %v", err)
		}

		// Corrupt MetaPage1ID on the rolled-forward image and verify fallback to MetaPage0ID also succeeds.
		corruptIndexPageByte(t, crashMidSwapDir, MetaPage1ID)
		midFallbackDB, err := Open(midSwapOpts)
		if err != nil {
			t.Fatalf("Open mid-swap fallback after corrupting MetaPage1ID: %v", err)
		}
		defer closeNoErr(t, midFallbackDB)
		if midFallbackDB.metaPageID != MetaPage0ID || midFallbackDB.State().CommitSeq != midRec0.CommitSeq {
			t.Fatalf("mid-swap fallback slot=%d seq=%d want slot=%d seq=%d",
				midFallbackDB.metaPageID, midFallbackDB.State().CommitSeq, MetaPage0ID, midRec0.CommitSeq)
		}
		verifyDBContents(t, "shared-crash-mid-swap-fallback", midFallbackDB, expectedV2User, expectedV2Col)
	})
}
