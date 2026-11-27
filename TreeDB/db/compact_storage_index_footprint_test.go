package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

func buildLeadingHoleIndexFixtureForTest(t *testing.T, dir string, leadingDeadPages int, liveLeafTarget int) (opts Options, descKey string, expectedKeys map[string][]byte) {
	t.Helper()
	opts = Options{
		Dir:                        dir,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: false,
		LeafPrefixCompression:      true,
	}
	descKey = "collections/root/docs/leading_hole"

	// Create a valid initial DB so format.json and layout directories exist.
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open init: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close init: %v", err)
	}

	// Rebuild index.db directly with a large dead leading prefix [2 .. 2+leadingDeadPages-1]
	// followed by a tightly-packed live pager-backed collection root (so
	// CollectionRootSpan == CollectionRootPages, SpanRatioPPM == 1_000_000,
	// FreelistReclaimablePages == 0, and 0 empty/underfull leaves).
	indexPath := filepath.Join(dir, indexFileName)
	if err := os.Remove(indexPath); err != nil {
		t.Fatalf("remove init index.db: %v", err)
	}
	p, err := pager.Open(indexPath, defaultChunkSize)
	if err != nil {
		t.Fatalf("pager.Open: %v", err)
	}
	if _, err := p.Alloc(2); err != nil {
		_ = p.Close()
		t.Fatalf("alloc meta pages: %v", err)
	}
	// Allocate leading dead pages that are neither reachable nor in FreelistFree.
	if _, err := p.Alloc(leadingDeadPages); err != nil {
		_ = p.Close()
		t.Fatalf("alloc leading dead pages: %v", err)
	}

	alloc := &pagerAllocator{p: p}
	buildOpts := bulk.BuildOptions{LeafPrefixCompression: true}

	// Build a dense pager-backed collection root with >= liveLeafTarget pages.
	// Each entry ~260 bytes => ~14 entries per 4 KiB leaf => liveLeafTarget*14 keys.
	numKeys := liveLeafTarget * 14
	mtCol, err := memtable.NewWithCapacityMode(numKeys*300, memtable.ModeBTree)
	if err != nil {
		_ = p.Close()
		t.Fatalf("new col memtable: %v", err)
	}
	expectedKeys = make(map[string][]byte, numKeys)
	payload := bytes.Repeat([]byte("X"), 220)
	for i := 0; i < numKeys; i++ {
		k := fmt.Sprintf("lh-%06d", i)
		v := append([]byte(fmt.Sprintf("%06d:", i)), payload...)
		mtCol.Set([]byte(k), v)
		expectedKeys[k] = v
	}
	mtCol.Freeze()
	colIt := mtCol.NewIterator(nil, nil)
	colRootID, err := bulk.BuildWithOptions(colIt, alloc, p, buildOpts)
	_ = colIt.Close()
	if err != nil {
		_ = p.Close()
		t.Fatalf("bulk build col root: %v", err)
	}

	mtSys := mustFrozenRawMemtable(t, descKey, encodeCollectionRootDescriptorRootID(colRootID))
	sysIt := mtSys.NewIterator(nil, nil)
	sysRootID, err := bulk.BuildWithOptions(sysIt, alloc, p, buildOpts)
	_ = sysIt.Close()
	if err != nil {
		_ = p.Close()
		t.Fatalf("bulk build sys root: %v", err)
	}

	mtUser := mustFrozenRawMemtable(t, "u0", "v0")
	userIt := mtUser.NewIterator(nil, nil)
	userRootID, err := bulk.BuildWithOptions(userIt, alloc, p, buildOpts)
	_ = userIt.Close()
	if err != nil {
		_ = p.Close()
		t.Fatalf("bulk build user root: %v", err)
	}

	meta := page.MetaPageBody{
		CommitSeq:        10,
		UserRootPageID:   userRootID,
		SystemRootPageID: sysRootID,
		TotalPages:       p.PageCount(),
	}
	if err := writeRebuiltDurableRootV1(dir, indexPath, p, meta, nil); err != nil {
		_ = p.Close()
		t.Fatalf("writeRebuiltDurableRootV1: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("pager.Close: %v", err)
	}
	return opts, descKey, expectedKeys
}

func buildSparseCollectionLeavesFixtureForTest(t *testing.T, dir string, leafCount int, emptyEvery int) (opts Options, descKey string, expectedVals map[string][]byte) {
	t.Helper()
	opts = Options{
		Dir:                        dir,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: false,
		LeafPrefixCompression:      true,
	}
	descKey = "collections/root/docs/sparse_leaves"

	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open init: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close init: %v", err)
	}

	indexPath := filepath.Join(dir, indexFileName)
	if err := os.Remove(indexPath); err != nil {
		t.Fatalf("remove init index.db: %v", err)
	}
	p, err := pager.Open(indexPath, defaultChunkSize)
	if err != nil {
		t.Fatalf("pager.Open: %v", err)
	}
	if _, err := p.Alloc(2); err != nil {
		_ = p.Close()
		t.Fatalf("alloc meta pages: %v", err)
	}

	alloc := &pagerAllocator{p: p}
	colRootID, expectedVals, _ := buildSparsePagerTreeForTest(t, p, alloc, leafCount, emptyEvery, false, nil)

	buildOpts := bulk.BuildOptions{LeafPrefixCompression: true}
	mtSys := mustFrozenRawMemtable(t, descKey, encodeCollectionRootDescriptorRootID(colRootID))
	sysIt := mtSys.NewIterator(nil, nil)
	sysRootID, err := bulk.BuildWithOptions(sysIt, alloc, p, buildOpts)
	_ = sysIt.Close()
	if err != nil {
		_ = p.Close()
		t.Fatalf("bulk build sys root: %v", err)
	}

	mtUser := mustFrozenRawMemtable(t, "u0", "v0")
	userIt := mtUser.NewIterator(nil, nil)
	userRootID, err := bulk.BuildWithOptions(userIt, alloc, p, buildOpts)
	_ = userIt.Close()
	if err != nil {
		_ = p.Close()
		t.Fatalf("bulk build user root: %v", err)
	}

	meta := page.MetaPageBody{
		CommitSeq:        10,
		UserRootPageID:   userRootID,
		SystemRootPageID: sysRootID,
		TotalPages:       p.PageCount(),
	}
	if err := writeRebuiltDurableRootV1(dir, indexPath, p, meta, nil); err != nil {
		_ = p.Close()
		t.Fatalf("writeRebuiltDurableRootV1: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("pager.Close: %v", err)
	}
	return opts, descKey, expectedVals
}

func assertNoCompactStorageSettlePhase(t *testing.T, stats CompactStorageStats) {
	t.Helper()
	for _, p := range stats.Phases {
		if p.Name == "index-vacuum-settle" {
			t.Fatalf("unexpected redundant index-vacuum-settle phase: %+v (all phases=%+v)", p, stats.Phases)
		}
	}
}

func TestCompactStorageTriggersVacuumOnLeadingHoleAndSparseCollectionLeaves(t *testing.T) {
	t.Run("policy_thresholds_and_convergence", func(t *testing.T) {
		// Exact host185 geometry: 755k-page leading dead prefix, span ratio 1.082 < 1.20.
		host185 := IndexVacuumTriggerReport{
			TotalPages:                   1_600_512,
			UserPages:                    1,
			SystemPages:                  7,
			CollectionRootPages:          757_263,
			CollectionRootMinPageID:      755_215,
			CollectionRootMaxPageID:      1_574_911,
			CollectionRootSpan:           819_697,
			CollectionRootSpanRatioPPM:   1_082_446,
			CollectionRootSpanRatioValid: true,
			ReplacementChunkPages:        16_384,
		}
		var host185Full CompactStorageDebt
		populateCompactStorageIndexDebtWithSlotCopies(CompactStorageOptions{Mode: CompactStorageFull}, host185, 1, &host185Full)
		if !host185Full.IndexVacuumRequired || host185Full.IndexVacuumReason != "whole_file_slack" {
			t.Fatalf("host185(full): required=%v reason=%q want true/whole_file_slack", host185Full.IndexVacuumRequired, host185Full.IndexVacuumReason)
		}
		// Tight-span leading hole (Span == Pages) must trigger whole_file_slack on BOTH Full and Exhaustive.
		tightLeadingHole := host185
		tightLeadingHole.CollectionRootSpan = tightLeadingHole.CollectionRootPages
		tightLeadingHole.CollectionRootMaxPageID = tightLeadingHole.CollectionRootMinPageID + tightLeadingHole.CollectionRootPages - 1
		tightLeadingHole.CollectionRootSpanRatioPPM = 1_000_000
		for _, mode := range []CompactStorageMode{CompactStorageFull, CompactStorageExhaustive} {
			var debt CompactStorageDebt
			populateCompactStorageIndexDebtWithSlotCopies(CompactStorageOptions{Mode: mode}, tightLeadingHole, 1, &debt)
			if !debt.IndexVacuumRequired || debt.IndexVacuumReason != "whole_file_slack" {
				t.Fatalf("tightLeadingHole(%s): required=%v reason=%q want true/whole_file_slack", mode, debt.IndexVacuumRequired, debt.IndexVacuumReason)
			}
		}

		// Freshly vacuumed 1-copy and 2-copy DBs must converge without re-triggering.
		for _, slotCopies := range []uint64{1, 2} {
			live := uint64(500)
			expected := compactStorageExpectedPostVacuumPages(IndexVacuumTriggerReport{
				UserPages:             1,
				SystemPages:           2,
				CollectionRootPages:   live - 3,
				ReplacementChunkPages: 64,
			}, slotCopies)
			fresh := IndexVacuumTriggerReport{
				TotalPages:                   expected,
				UserPages:                    1,
				UserSpan:                     1,
				UserSpanRatioPPM:             1_000_000,
				SystemPages:                  2,
				CollectionRootPages:          live - 3,
				CollectionRootSpan:           live - 3,
				CollectionRootSpanRatioPPM:   1_000_000,
				CollectionRootSpanRatioValid: true,
				CollectionRootLeafPages:      live - 10,
				ReplacementChunkPages:        64,
			}
			for _, mode := range []CompactStorageMode{CompactStorageFull, CompactStorageExhaustive} {
				var debt CompactStorageDebt
				populateCompactStorageIndexDebtWithSlotCopies(CompactStorageOptions{Mode: mode}, fresh, slotCopies, &debt)
				if debt.IndexVacuumRequired {
					t.Fatalf("fresh(slotCopies=%d, mode=%s) falsely triggered: reason=%q", slotCopies, mode, debt.IndexVacuumReason)
				}
			}
		}

		// Exhaustive triggers on >=1 empty non-root leaf, whereas Full respects >=64 & >=10%.
		oneEmpty := IndexVacuumTriggerReport{
			TotalPages:                   400,
			UserPages:                    1,
			UserSpan:                     1,
			SystemPages:                  1,
			CollectionRootPages:          360,
			CollectionRootSpan:           360,
			CollectionRootSpanRatioPPM:   1_000_000,
			CollectionRootSpanRatioValid: true,
			CollectionRootLeafPages:      350,
			CollectionRootEmptyLeafPages: 1,
			ReplacementChunkPages:        64,
		}
		var fullDebt, exDebt CompactStorageDebt
		populateCompactStorageIndexDebtWithSlotCopies(CompactStorageOptions{Mode: CompactStorageFull}, oneEmpty, 1, &fullDebt)
		if fullDebt.IndexVacuumRequired {
			t.Fatalf("Full should not trigger on 1/350 empty leaf, got reason=%q", fullDebt.IndexVacuumReason)
		}
		populateCompactStorageIndexDebtWithSlotCopies(CompactStorageOptions{Mode: CompactStorageExhaustive}, oneEmpty, 1, &exDebt)
		if !exDebt.IndexVacuumRequired || exDebt.IndexVacuumReason != "collection_leaf_fill" {
			t.Fatalf("Exhaustive should trigger collection_leaf_fill on 1 empty leaf, got req=%v reason=%q", exDebt.IndexVacuumRequired, exDebt.IndexVacuumReason)
		}
	})

	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}

	for _, mode := range []CompactStorageMode{CompactStorageFull, CompactStorageExhaustive} {
		t.Run("leading_hole/"+string(mode), func(t *testing.T) {
			dir := t.TempDir()
			opts, descKey, expected := buildLeadingHoleIndexFixtureForTest(t, dir, 600, 360)
			d, err := Open(opts)
			if err != nil {
				t.Fatalf("Open leading hole fixture: %v", err)
			}
			// Trigger report must preserve tight CollectionRootSpan = max - min + 1
			// while detecting the massive leading dead prefix via whole_file_slack.
			beforeRep, err := d.IndexVacuumTriggerReport()
			if err != nil {
				_ = d.Close()
				t.Fatalf("IndexVacuumTriggerReport: %v", err)
			}
			if beforeRep.SystemPages == 0 {
				_ = d.Close()
				t.Fatalf("expected SystemPages > 0, got 0")
			}
			if beforeRep.UserPages+beforeRep.SystemPages+beforeRep.CollectionRootPages < 256 {
				_ = d.Close()
				t.Fatalf("expected >= 256 live pages for hard convergence check, got %d",
					beforeRep.UserPages+beforeRep.SystemPages+beforeRep.CollectionRootPages)
			}
			if beforeRep.CollectionRootMinPageID < 600 {
				_ = d.Close()
				t.Fatalf("expected leading hole minPageID >= 600, got %d", beforeRep.CollectionRootMinPageID)
			}
			if wantSpan := (beforeRep.CollectionRootMaxPageID - beforeRep.CollectionRootMinPageID) + 1; beforeRep.CollectionRootSpan != wantSpan {
				_ = d.Close()
				t.Fatalf("CollectionRootSpan=%d want (max-min+1)=%d", beforeRep.CollectionRootSpan, wantSpan)
			}
			if beforeRep.CollectionRootSpanRatioPPM >= compactStorageIndexCollectionRootSpanRatioPPM {
				_ = d.Close()
				t.Fatalf("expected tight span ratio < %d, got %d", compactStorageIndexCollectionRootSpanRatioPPM, beforeRep.CollectionRootSpanRatioPPM)
			}
			if beforeRep.FreelistReclaimablePages != 0 {
				_ = d.Close()
				t.Fatalf("expected FreelistReclaimablePages=0, got %d", beforeRep.FreelistReclaimablePages)
			}

			beforeInfo, err := os.Stat(filepath.Join(dir, indexFileName))
			if err != nil {
				_ = d.Close()
				t.Fatal(err)
			}
			plan, err := d.CompactStoragePlan(context.Background(), CompactStorageOptions{Mode: mode})
			if err != nil {
				_ = d.Close()
				t.Fatalf("CompactStoragePlan(%s): %v", mode, err)
			}
			if !plan.RemainingDebt.IndexVacuumRequired || plan.RemainingDebt.IndexVacuumReason != "whole_file_slack" {
				_ = d.Close()
				t.Fatalf("CompactStoragePlan(%s): required=%v reason=%q want true/whole_file_slack (report=%+v)",
					mode, plan.RemainingDebt.IndexVacuumRequired, plan.RemainingDebt.IndexVacuumReason, beforeRep)
			}

			stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{Mode: mode})
			if err != nil {
				_ = d.Close()
				t.Fatalf("CompactStorage(%s): %v", mode, err)
			}
			assertNoCompactStorageSettlePhase(t, stats)
			if stats.RemainingDebt.IndexVacuumRequired {
				_ = d.Close()
				t.Fatalf("CompactStorage(%s) did not converge: RemainingDebt=%+v", mode, stats.RemainingDebt)
			}
			if !stats.FullyCompacted || !stats.PolicyFullyCompacted {
				_ = d.Close()
				t.Fatalf("CompactStorage(%s) completion flags false: fully=%v policy=%v debt=%+v",
					mode, stats.FullyCompacted, stats.PolicyFullyCompacted, stats.RemainingDebt)
			}
			if got, want := stats.ByteMinimized, mode == CompactStorageExhaustive; got != want {
				_ = d.Close()
				t.Fatalf("CompactStorage(%s) ByteMinimized=%v want %v", mode, got, want)
			}
			afterInfo, err := os.Stat(filepath.Join(dir, indexFileName))
			if err != nil {
				_ = d.Close()
				t.Fatal(err)
			}
			if afterInfo.Size()*100 >= beforeInfo.Size()*65 {
				_ = d.Close()
				t.Fatalf("CompactStorage(%s) did not shrink leading-hole index.db: before=%d after=%d", mode, beforeInfo.Size(), afterInfo.Size())
			}

			// Close and reopen, verifying all collection entries survived verbatim.
			if err := d.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			reopened, err := Open(opts)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer func() { _ = reopened.Close() }()
			snap := reopened.AcquireSnapshot()
			defer func() { _ = snap.Close() }()
			descs, err := vacuumCollectCollectionRootDescriptors(snap.idx.pager, &snap.reader, snap.state.SystemRootPageID)
			if err != nil || len(descs) != 1 || string(descs[0].key) != descKey {
				t.Fatalf("reopen descriptors=%+v err=%v", descs, err)
			}
			tr := tree.New(snap.idx.pager, &snap.reader, descs[0].rootID)
			it := tr.Iterator(nil, nil)
			seen := 0
			for ; it.Valid(); it.Next() {
				k := string(it.UnsafeKey())
				want, ok := expected[k]
				if !ok || !bytes.Equal(it.UnsafeValue(), want) {
					t.Fatalf("reopen mismatch for key %q", k)
				}
				seen++
			}
			_ = it.Close()
			if seen != len(expected) {
				t.Fatalf("reopen key count=%d want %d", seen, len(expected))
			}
		})

		t.Run("sparse_collection_leaves/"+string(mode), func(t *testing.T) {
			dir := t.TempDir()
			// 320 leaves, every 4th leaf empty (79 empty non-first leaves = 24.7%,
			// remaining leaves ~2% full = 100% <25% fill), tight span, 0 freelist!
			opts, descKey, expected := buildSparseCollectionLeavesFixtureForTest(t, dir, 320, 4)
			d, err := Open(opts)
			if err != nil {
				t.Fatalf("Open sparse leaves fixture: %v", err)
			}
			beforeRep, err := d.IndexVacuumTriggerReport()
			if err != nil {
				_ = d.Close()
				t.Fatalf("IndexVacuumTriggerReport: %v", err)
			}
			if beforeRep.SystemPages == 0 || beforeRep.CollectionRootEmptyLeafPages < 64 || beforeRep.CollectionRootUnderfullLeafPages < 256 {
				_ = d.Close()
				t.Fatalf("unexpected trigger report leaf stats: sys=%d empty=%d underfull=%d",
					beforeRep.SystemPages, beforeRep.CollectionRootEmptyLeafPages, beforeRep.CollectionRootUnderfullLeafPages)
			}
			if beforeRep.CollectionRootSpanRatioPPM >= compactStorageIndexCollectionRootSpanRatioPPM {
				_ = d.Close()
				t.Fatalf("expected tight span ratio, got %d", beforeRep.CollectionRootSpanRatioPPM)
			}
			if beforeRep.FreelistReclaimablePages != 0 {
				_ = d.Close()
				t.Fatalf("expected FreelistReclaimablePages=0, got %d", beforeRep.FreelistReclaimablePages)
			}

			beforeInfo, err := os.Stat(filepath.Join(dir, indexFileName))
			if err != nil {
				_ = d.Close()
				t.Fatal(err)
			}
			plan, err := d.CompactStoragePlan(context.Background(), CompactStorageOptions{Mode: mode})
			if err != nil {
				_ = d.Close()
				t.Fatalf("CompactStoragePlan(%s): %v", mode, err)
			}
			if !plan.RemainingDebt.IndexVacuumRequired || plan.RemainingDebt.IndexVacuumReason != "collection_leaf_fill" {
				_ = d.Close()
				t.Fatalf("CompactStoragePlan(%s): required=%v reason=%q want true/collection_leaf_fill (report=%+v)",
					mode, plan.RemainingDebt.IndexVacuumRequired, plan.RemainingDebt.IndexVacuumReason, beforeRep)
			}

			stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{Mode: mode})
			if err != nil {
				_ = d.Close()
				t.Fatalf("CompactStorage(%s): %v", mode, err)
			}
			assertNoCompactStorageSettlePhase(t, stats)
			if stats.RemainingDebt.IndexVacuumRequired {
				_ = d.Close()
				t.Fatalf("CompactStorage(%s) did not converge: RemainingDebt=%+v", mode, stats.RemainingDebt)
			}
			if !stats.FullyCompacted || !stats.PolicyFullyCompacted {
				_ = d.Close()
				t.Fatalf("CompactStorage(%s) completion flags false: fully=%v policy=%v debt=%+v",
					mode, stats.FullyCompacted, stats.PolicyFullyCompacted, stats.RemainingDebt)
			}
			if got, want := stats.ByteMinimized, mode == CompactStorageExhaustive; got != want {
				_ = d.Close()
				t.Fatalf("CompactStorage(%s) ByteMinimized=%v want %v", mode, got, want)
			}

			afterInfo, err := os.Stat(filepath.Join(dir, indexFileName))
			if err != nil {
				_ = d.Close()
				t.Fatal(err)
			}
			if afterInfo.Size()*100 >= beforeInfo.Size()*50 {
				_ = d.Close()
				t.Fatalf("CompactStorage(%s) did not shrink sparse-leaf index.db: before=%d after=%d", mode, beforeInfo.Size(), afterInfo.Size())
			}

			// Verify zero empty leaves remain and all data reads back after reopen.
			if err := d.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			reopened, err := Open(opts)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer func() { _ = reopened.Close() }()
			snap := reopened.AcquireSnapshot()
			defer func() { _ = snap.Close() }()
			descs, err := vacuumCollectCollectionRootDescriptors(snap.idx.pager, &snap.reader, snap.state.SystemRootPageID)
			if err != nil || len(descs) != 1 || string(descs[0].key) != descKey {
				t.Fatalf("reopen descriptors=%+v err=%v", descs, err)
			}
			afterPages, _, afterEmpty, _, _ := inspectPagerRootLeafStats(t, snap.idx.pager, descs[0].rootID)
			if afterEmpty != 0 || afterPages >= 40 {
				t.Fatalf("reopen sparse root not repacked: afterPages=%d afterEmpty=%d", afterPages, afterEmpty)
			}
			it, err := snap.IteratorAtRootWithOptions(descs[0].rootID, nil, nil, IteratorOptions{})
			if err != nil {
				t.Fatal(err)
			}
			seen := 0
			for ; it.Valid(); it.Next() {
				k := string(it.UnsafeKey())
				val, _, flags := it.UnsafeEntry()
				if flags&node.FlagPointer != 0 {
					t.Fatalf("unexpected pointer flag on %q", k)
				}
				if !bytes.Equal(val, expected[k]) {
					t.Fatalf("reopen value mismatch on %q", k)
				}
				seen++
			}
			_ = it.Close()
			if seen != len(expected) {
				t.Fatalf("reopen key count=%d want %d", seen, len(expected))
			}
		})
	}
}

func TestCompactStorageConvergesOnManySmallTwoLeafCollections(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	// 256 small 2-leaf pure-pager collection roots: each root has 5 x ~900B
	// inline entries so bulk-build produces 2 leaves per root (leaf 0 full,
	// leaf 1 trailing <25% fill). That yields 512 total leaves with 256
	// irreducible trailing <25% leaves (50% >= 40%). Both Full and Exhaustive
	// MUST converge cleanly after vacuum without re-triggering collection_leaf_fill.
	for _, mode := range []CompactStorageMode{CompactStorageFull, CompactStorageExhaustive} {
		t.Run(string(mode), func(t *testing.T) {
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
			defer closeNoErr(t, d)

			const numCollections = 256
			const entriesPerCollection = 5
			val := bytes.Repeat([]byte("v"), 900)
			sysEntries := make([]struct {
				key string
				val []byte
			}, 0, numCollections)

			gen := d.idx.Load()
			alloc := gen.allocator
			p := gen.pager
			for c := 0; c < numCollections; c++ {
				mt, err := memtable.NewWithCapacityMode(8*1024, memtable.ModeHashSorted)
				if err != nil {
					t.Fatalf("memtable: %v", err)
				}
				for e := 0; e < entriesPerCollection; e++ {
					k := []byte(fmt.Sprintf("k-%03d-%02d", c, e))
					mt.Set(k, val)
				}
				mt.Freeze()
				it := mt.NewIterator(nil, nil)
				rootID, err := bulk.BuildWithOptions(it, alloc, p, bulk.BuildOptions{
					LeafPrefixCompression: true,
				})
				_ = it.Close()
				if err != nil {
					t.Fatalf("bulk.Build col %d: %v", c, err)
				}
				sysEntries = append(sysEntries, struct {
					key string
					val []byte
				}{
					key: fmt.Sprintf("collections/root/small_%03d/primary", c),
					val: encodeCollectionRootDescriptorRootID(rootID),
				})
			}

			if _, _, err := d.PublishOrderedRootGroupWithSystemBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
				mt, err := memtable.NewWithCapacityMode(64*1024, memtable.ModeHashSorted)
				if err != nil {
					return nil, err
				}
				for _, e := range sysEntries {
					mt.Set([]byte(e.key), e.val)
				}
				mt.Freeze()
				return mt.NewIterator(nil, nil), nil
			}); err != nil {
				t.Fatalf("PublishOrderedRootGroupWithSystemBuilder: %v", err)
			}
			if err := d.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}

			beforeRep, err := d.IndexVacuumTriggerReport()
			if err != nil {
				t.Fatalf("before IndexVacuumTriggerReport: %v", err)
			}
			if beforeRep.CollectionRootLeafPages < numCollections*2 {
				t.Fatalf("expected >= %d leaves across %d 2-leaf roots, got %d", numCollections*2, numCollections, beforeRep.CollectionRootLeafPages)
			}

			stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{Mode: mode})
			if err != nil {
				t.Fatalf("CompactStorage(%s): %v", mode, err)
			}
			for _, ph := range stats.Phases {
				if ph.Name == "index-vacuum-settle" {
					t.Fatalf("unexpected index-vacuum-settle on %d small 2-leaf collections (%s): phases=%+v", numCollections, mode, stats.Phases)
				}
			}
			if stats.RemainingDebt.IndexVacuumRequired {
				t.Fatalf("CompactStorage(%s) failed to converge on %d small 2-leaf collections: RemainingDebt=%+v", mode, numCollections, stats.RemainingDebt)
			}
			if !stats.FullyCompacted || !stats.PolicyFullyCompacted {
				t.Fatalf("CompactStorage(%s) flags fully=%v policy=%v byte=%v debt=%+v",
					mode, stats.FullyCompacted, stats.PolicyFullyCompacted, stats.ByteMinimized, stats.RemainingDebt)
			}
			if mode == CompactStorageExhaustive && !stats.ByteMinimized {
				t.Fatalf("CompactStorage(Exhaustive) ByteMinimized=false debt=%+v", stats.RemainingDebt)
			}
			afterRep, err := d.IndexVacuumTriggerReport()
			if err != nil {
				t.Fatalf("after IndexVacuumTriggerReport: %v", err)
			}
			if afterRep.CollectionRootUnderfullLeafPages != 0 || afterRep.CollectionRootEmptyLeafPages != 0 {
				t.Fatalf("expected 0 reducible underfull/empty leaves after vacuum, got underfull=%d empty=%d (totalLeaves=%d)",
					afterRep.CollectionRootUnderfullLeafPages, afterRep.CollectionRootEmptyLeafPages, afterRep.CollectionRootLeafPages)
			}
		})
	}

	t.Run("mixed_internal_leafref_at_nonzero_index_excluded_from_leaf_fill", func(t *testing.T) {
		p, err := pager.Open(filepath.Join(t.TempDir(), "mixed.db"), 64*1024)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = p.Close() }()
		if _, err := p.Alloc(2); err != nil {
			t.Fatal(err)
		}
		// Build a height-2 mixed root whose branch 0 is a pager internal node
		// holding an empty pager leaf and branch 1 is a leaf-log internal node
		// holding a ChildRefLeafLog. Because the tree mixes pager and leaf-log
		// leaves, vacuum cannot bulk-repack it, so fragmentation must exclude it
		// from empty/underfull leaf-fill debt.
		emptyLeafID, err := p.Alloc(1)
		if err != nil {
			t.Fatal(err)
		}
		buf, err := p.GetForWrite(emptyLeafID)
		if err != nil {
			t.Fatal(err)
		}
		lb := node.NewBuilder(buf, page.PageTypeLeaf)
		lb.SetPageID(emptyLeafID)
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
		if err := pb.AddInternalChildRef([]byte{}, page.PageChildRef(emptyLeafID)); err != nil {
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
		if err := lrb.AddInternalChildRef([]byte("z"), page.LeafLogChildRef(page.LeafLogPtr{FileID: 9, Offset: 4096})); err != nil {
			t.Fatal(err)
		}
		lrb.Finish()

		rootID, err := p.Alloc(1)
		if err != nil {
			t.Fatal(err)
		}
		rbuf, err := p.GetForWrite(rootID)
		if err != nil {
			t.Fatal(err)
		}
		ib := node.NewBuilder(rbuf, page.PageTypeInternal)
		ib.SetPageID(rootID)
		if err := ib.AddInternalChildRef([]byte{}, page.PageChildRef(pagerBranchID)); err != nil {
			t.Fatal(err)
		}
		if err := ib.AddInternalChildRef([]byte("z"), page.PageChildRef(leafRefBranchID)); err != nil {
			t.Fatal(err)
		}
		ib.Finish()

		// Build a system root pointing to rootID as a collection root descriptor.
		sysRootID, err := p.Alloc(1)
		if err != nil {
			t.Fatal(err)
		}
		sbuf, err := p.GetForWrite(sysRootID)
		if err != nil {
			t.Fatal(err)
		}
		sb := node.NewBuilder(sbuf, page.PageTypeLeaf)
		sb.SetPageID(sysRootID)
		if err := sb.AddLeafEntry([]byte("collections/root/mixed/primary"), encodeCollectionRootDescriptorRootID(rootID), node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatal(err)
		}
		sb.Finish()

		got, err := collectionRootFragmentationStatsWithContext(context.Background(), &indexGen{pager: p}, &DBState{SystemRootPageID: sysRootID})
		if err != nil {
			t.Fatalf("collectionRootFragmentationStatsWithContext: %v", err)
		}
		if got.emptyLeafPages != 0 || got.underfullLeafPages != 0 {
			t.Fatalf("mixed height-2 root with ChildRefLeafLog branch misclassified as pure-pager: empty=%d underfull=%d",
				got.emptyLeafPages, got.underfullLeafPages)
		}
	})
}

func TestCompactStorageVacuumIndexOnlineCanceledBeforeFallbackRefresh(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	dir := t.TempDir()
	d, err := Open(Options{
		Dir:                        dir,
		ResolvedProfile:            ProfileNoWALFast,
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: false,
		DisableBackgroundPrune:     true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer closeNoErr(t, d)

	if err := d.SetSync([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("SetSync k1: %v", err)
	}
	if err := d.SetSync([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("SetSync k2: %v", err)
	}

	d.durablePublishMu.Lock()
	beforeSlot := d.durableRoot.slot
	beforeRec0 := d.durableRoot.slotRecord[0]
	beforeRec1 := d.durableRoot.slotRecord[1]
	d.durablePublishMu.Unlock()
	if beforeRec0.CommitSeq == 0 || beforeRec1.CommitSeq == 0 || beforeRec0.UserRootPageID == beforeRec1.UserRootPageID {
		t.Fatalf("expected two distinct durable slots before canceled call: rec0=%+v rec1=%+v", beforeRec0, beforeRec1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := d.compactStorageVacuumIndexOnline(ctx, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("compactStorageVacuumIndexOnline(canceled) err=%v want context.Canceled", err)
	}

	d.durablePublishMu.Lock()
	afterSlot := d.durableRoot.slot
	afterRec0 := d.durableRoot.slotRecord[0]
	afterRec1 := d.durableRoot.slotRecord[1]
	d.durablePublishMu.Unlock()
	if afterSlot != beforeSlot || afterRec0 != beforeRec0 || afterRec1 != beforeRec1 {
		t.Fatalf("canceled compactStorageVacuumIndexOnline mutated durable slots: before=(slot=%d r0=%d/%d r1=%d/%d) after=(slot=%d r0=%d/%d r1=%d/%d)",
			beforeSlot, beforeRec0.CommitSeq, beforeRec0.UserRootPageID, beforeRec1.CommitSeq, beforeRec1.UserRootPageID,
			afterSlot, afterRec0.CommitSeq, afterRec0.UserRootPageID, afterRec1.CommitSeq, afterRec1.UserRootPageID)
	}
}

func TestCompactStorageExpectedVacuumSlotCopiesReflectsSlotConvergence(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	dir := t.TempDir()
	d, err := Open(Options{
		Dir:                        dir,
		ResolvedProfile:            ProfileNoWALFast,
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: false,
		DisableBackgroundPrune:     true,
		LeafPrefixCompression:      true,
		ChunkSize:                  64 * 1024,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer closeNoErr(t, d)

	// Populate enough live pages (>256) so whole_file_slack thresholds are active.
	payload := bytes.Repeat([]byte("S"), 240)
	b1 := d.NewBatch()
	for i := 0; i < 4200; i++ {
		if err := b1.Set([]byte(fmt.Sprintf("k-%06d", i)), payload); err != nil {
			_ = b1.Close()
			t.Fatalf("b1.Set: %v", err)
		}
	}
	if err := b1.WriteSync(); err != nil {
		_ = b1.Close()
		t.Fatalf("b1.WriteSync: %v", err)
	}
	_ = b1.Close()
	// One more distinct commit so slot0 and slot1 diverge.
	if err := d.SetSync([]byte("k-000000"), bytes.Repeat([]byte("T"), 240)); err != nil {
		t.Fatalf("SetSync diverge: %v", err)
	}

	if got := d.compactStorageExpectedVacuumSlotCopies(); got != 2 {
		t.Fatalf("compactStorageExpectedVacuumSlotCopies with distinct non-command-WAL slots = %d, want 2", got)
	}

	// Run a 2-slot online vacuum directly (distinct durable slots -> 2 copies in index.db).
	vacStats, err := d.VacuumIndexOnlineWithStats(context.Background())
	if err != nil {
		t.Fatalf("VacuumIndexOnlineWithStats: %v", err)
	}
	if vacStats.OlderRootRebuilds == 0 {
		t.Fatalf("expected 2-slot rebuild with distinct slots, got OlderRootRebuilds=0")
	}
	if got := d.compactStorageExpectedVacuumSlotCopies(); got != 2 {
		t.Fatalf("compactStorageExpectedVacuumSlotCopies after 2-slot vacuum = %d, want 2", got)
	}

	// While the two durable slots remain distinct, CompactStoragePlan must NOT
	// report whole_file_slack on a file whose size matches a 2-copy rebuild
	// (preventing futile repeated 2-slot vacuums under concurrent writers).
	planDistinct, err := d.CompactStoragePlan(context.Background(), CompactStorageOptions{Mode: CompactStorageFull})
	if err != nil {
		t.Fatalf("CompactStoragePlan(distinct): %v", err)
	}
	if planDistinct.RemainingDebt.IndexVacuumRequired && planDistinct.RemainingDebt.IndexVacuumReason == "whole_file_slack" {
		t.Fatalf("CompactStoragePlan falsely reported whole_file_slack on 2-copy distinct-slot index: debt=%+v", planDistinct.RemainingDebt)
	}

	// On a quiet DB, CompactStorage(Full) should converge the distinct slots via
	// fallback refresh, shrink the 2-copy file down to 1 copy, and converge
	// cleanly in a single pass.
	beforeSize, err := os.Stat(filepath.Join(dir, indexFileName))
	if err != nil {
		t.Fatalf("stat before CompactStorage: %v", err)
	}
	csStats, err := d.CompactStorage(context.Background(), CompactStorageOptions{Mode: CompactStorageFull})
	if err != nil {
		t.Fatalf("CompactStorage(Full) on quiet 2-copy DB: %v", err)
	}
	assertNoCompactStorageSettlePhase(t, csStats)
	if csStats.RemainingDebt.IndexVacuumRequired {
		t.Fatalf("CompactStorage(Full) did not converge: debt=%+v", csStats.RemainingDebt)
	}
	if got := d.compactStorageExpectedVacuumSlotCopies(); got != 1 {
		t.Fatalf("compactStorageExpectedVacuumSlotCopies after convergence = %d, want 1", got)
	}
	afterSize, err := os.Stat(filepath.Join(dir, indexFileName))
	if err != nil {
		t.Fatalf("stat after CompactStorage: %v", err)
	}
	if afterSize.Size() >= beforeSize.Size() {
		t.Fatalf("quiet CompactStorage(Full) failed to shrink 2-copy index: before=%d after=%d", beforeSize.Size(), afterSize.Size())
	}
}

func TestCompactStoragePrivateCOWAndNativeSemanticSkipFallbackRefreshInPlan(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	dir := t.TempDir()
	d, err := Open(Options{
		Dir:                        dir,
		ResolvedProfile:            ProfileNoWALFast,
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: false,
		DisableBackgroundPrune:     true,
		LeafPrefixCompression:      true,
		ChunkSize:                  64 * 1024,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer closeNoErr(t, d)

	// Build a 2-copy diverged-slot index where slotCopies=2 reports no debt,
	// while slotCopies=1 would report whole_file_slack — the exact branch that
	// triggers pre-vacuum fallback convergence on unowned DBs.
	payload := bytes.Repeat([]byte("N"), 240)
	b1 := d.NewBatch()
	for i := 0; i < 4200; i++ {
		if err := b1.Set([]byte(fmt.Sprintf("k-%06d", i)), payload); err != nil {
			_ = b1.Close()
			t.Fatalf("b1.Set: %v", err)
		}
	}
	if err := b1.WriteSync(); err != nil {
		_ = b1.Close()
		t.Fatalf("b1.WriteSync: %v", err)
	}
	_ = b1.Close()
	if err := d.SetSync([]byte("k-000000"), bytes.Repeat([]byte("M"), 240)); err != nil {
		t.Fatalf("SetSync diverge: %v", err)
	}
	vacStats, err := d.VacuumIndexOnlineWithStats(context.Background())
	if err != nil {
		t.Fatalf("VacuumIndexOnlineWithStats: %v", err)
	}
	if vacStats.OlderRootRebuilds == 0 {
		t.Fatalf("expected 2-slot rebuild, got OlderRootRebuilds=0")
	}

	// Verify the precondition: slotCopies=2 has no debt, slotCopies=1 has whole_file_slack.
	rep, err := d.IndexVacuumTriggerReport()
	if err != nil {
		t.Fatalf("IndexVacuumTriggerReport: %v", err)
	}
	debt2 := d.compactStorageIndexVacuumDebtFromReport(CompactStorageOptions{Mode: CompactStorageFull}, rep, 2)
	debt1 := d.compactStorageIndexVacuumDebtFromReport(CompactStorageOptions{Mode: CompactStorageFull}, rep, 1)
	if debt2.IndexVacuumRequired || !debt1.IndexVacuumRequired {
		t.Fatalf("precondition failed: debt2=%+v debt1=%+v", debt2, debt1)
	}

	owner := transactionowner.New()
	if err := d.BindPrivateCOWOwner(owner); err != nil {
		t.Fatalf("BindPrivateCOWOwner: %v", err)
	}

	snapshotSlots := func() (uint64, [2]uint64, [2]uint64) {
		d.durablePublishMu.Lock()
		defer d.durablePublishMu.Unlock()
		return d.durableRoot.slot,
			[2]uint64{d.durableRoot.slotRecord[0].CommitSeq, d.durableRoot.slotRecord[1].CommitSeq},
			[2]uint64{d.durableRoot.slotRecord[0].UserRootPageID, d.durableRoot.slotRecord[1].UserRootPageID}
	}

	// Case 1: CompactStorageForPrivateCOW with nativeSemanticActive=false must
	// not run unowned fallback refresh or mutate durable slots during planning.
	beforeSlot, beforeSeqs, beforeRoots := snapshotSlots()
	stats1, err := d.CompactStorageForPrivateCOW(owner, context.Background(), CompactStorageOptions{Mode: CompactStorageFull})
	if err != nil {
		t.Fatalf("CompactStorageForPrivateCOW(nativeSemanticActive=false): %v", err)
	}
	afterSlot, afterSeqs, afterRoots := snapshotSlots()
	if afterSlot != beforeSlot || afterSeqs != beforeSeqs || afterRoots != beforeRoots {
		t.Fatalf("CompactStorageForPrivateCOW mutated durable slots during index-vacuum-plan: before=(slot=%d seq=%v roots=%v) after=(slot=%d seq=%v roots=%v)",
			beforeSlot, beforeSeqs, beforeRoots, afterSlot, afterSeqs, afterRoots)
	}
	for _, ph := range stats1.Phases {
		if ph.Name == "index-vacuum" && ph.Status == CompactStoragePhaseStatusFailed {
			t.Fatalf("unexpected failed index-vacuum phase: %+v", ph)
		}
	}

	// Case 2: With nativeSemanticActive=true, CompactStorageForPrivateCOW must
	// NOT fail with ErrNativeSemanticUnsupported inside index-vacuum-plan and
	// must leave durable slots untouched.
	d.nativeSemanticActive.Store(true)
	defer d.nativeSemanticActive.Store(false)

	beforeSlot, beforeSeqs, beforeRoots = snapshotSlots()
	stats2, err := d.CompactStorageForPrivateCOW(owner, context.Background(), CompactStorageOptions{Mode: CompactStorageFull})
	if err != nil {
		t.Fatalf("CompactStorageForPrivateCOW(nativeSemanticActive=true): %v", err)
	}
	afterSlot, afterSeqs, afterRoots = snapshotSlots()
	if afterSlot != beforeSlot || afterSeqs != beforeSeqs || afterRoots != beforeRoots {
		t.Fatalf("CompactStorageForPrivateCOW(nativeSemanticActive=true) mutated durable slots: before=(slot=%d seq=%v roots=%v) after=(slot=%d seq=%v roots=%v)",
			beforeSlot, beforeSeqs, beforeRoots, afterSlot, afterSeqs, afterRoots)
	}
	for _, ph := range stats2.Phases {
		if ph.Name == "index-vacuum" && ph.Status == CompactStoragePhaseStatusFailed {
			t.Fatalf("unexpected failed index-vacuum phase under nativeSemanticActive: %+v", ph)
		}
	}
}
