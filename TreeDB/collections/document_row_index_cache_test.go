package collections

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

func TestCollectionReadViewFreshViewsReuseRowIndex(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(map[bool]string{false: "mapped", true: "read_at"}[fallback], func(t *testing.T) {
			resetColumnAssetVerifiedChecksumCacheForTest(t)
			db, col := newDocumentMaterializerTestCollection(t)
			defer db.Close()
			ids := [][]byte{[]byte("a"), []byte("b")}
			docs := [][]byte{[]byte(`{"row_id":1,"kind":"old","score":1}`), []byte(`{"row_id":2,"kind":"other","score":2}`)}
			if _, err := col.InsertBatch(ids, docs); err != nil {
				t.Fatal(err)
			}
			before := workstats.Read().RowIndexCache
			var first []int
			var stableIdentity bool
			for phase := range 3 {
				fresh, err := NewCollectionManager(db).OpenCollection("docs")
				if err != nil {
					t.Fatal(err)
				}
				view, err := fresh.OpenCollectionReadView()
				if err != nil {
					t.Fatal(err)
				}
				view.forceAssetReadAtFallbackForTest = fallback
				got, err := view.FetchDocumentsByID([][]byte{ids[1], ids[0], ids[1], []byte("missing")}, DocumentFetchOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if len(got.Results) != 4 || got.Results[3].Found || !bytes.Contains(got.Results[1].Document, []byte(`"old"`)) || !bytes.Equal(got.Results[0].Document, got.Results[2].Document) {
					t.Fatalf("fetch=%+v", got)
				}
				if len(view.pointRowBlocks) != 1 {
					t.Fatalf("blocks=%d", len(view.pointRowBlocks))
				}
				if phase == 0 {
					stableIdentity = rowIndexReadCacheHasStableIdentityForTest(view.rowAssetReadCache)
				}
				for _, block := range view.pointRowBlocks {
					if phase == 0 {
						first = block.rowOffsets
					} else if stableIdentity && &first[0] != &block.rowOffsets[0] {
						t.Fatal("fresh view rebuilt immutable row offsets")
					}
				}
				if err := view.Close(); err != nil {
					t.Fatal(err)
				}
			}
			after := workstats.Read().RowIndexCache
			wantBuilds := uint64(3)
			if stableIdentity {
				wantBuilds = 1
			}
			if after.Builds-before.Builds != wantBuilds || after.RowsVisited-before.RowsVisited != 2*wantBuilds {
				t.Fatalf("before=%+v after=%+v", before, after)
			}
			if !stableIdentity && (after.Hits != before.Hits || after.Misses != before.Misses) {
				t.Fatal("unsupported identity used row memo")
			}
		})
	}
}

func rowIndexReadCacheHasStableIdentityForTest(cache *columnPhysicalAssetReadCache) bool {
	found := false
	if cache.file != nil {
		found = true
		if !cache.file.identity.valid {
			return false
		}
	}
	for _, file := range cache.files {
		found = true
		if !file.identity.valid {
			return false
		}
	}
	return found
}

func TestColumnPhysicalRowReaderFreshReadersReuseRowIndex(t *testing.T) {
	resetColumnAssetVerifiedChecksumCacheForTest(t)
	cfg := testColumnPhysicalRowReaderConfigV1(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	ref := writeColumnPhysicalRowReaderAssetForTestV1(t, root, cfg, 7, 1, testColumnPhysicalRowReaderRowsV1(0, 3, cfg))
	view := columnPhysicalRowReaderViewForTestV1(root, cfg, ref)
	before := workstats.Read().RowIndexCache
	var offsets []int
	var stableIdentity bool
	for range 3 {
		reader, err := newColumnPhysicalRowReaderFromSnapshotView(view, columnPhysicalRowReaderOptions{})
		if err != nil {
			t.Fatal(err)
		}
		var scratch columnPhysicalRowReaderScratch
		row, err := reader.FetchRow(1, &scratch)
		if err != nil {
			t.Fatal(err)
		}
		if string(row.ID) != "doc-001" {
			t.Fatalf("row=%+v", row)
		}
		current := reader.blocks[0].rowOffsets
		if offsets == nil {
			offsets = current
			stableIdentity = rowIndexReadCacheHasStableIdentityForTest(&reader.readCache)
		} else if stableIdentity && &offsets[0] != &current[0] {
			t.Fatal("generic reader rebuilt row offsets")
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
	after := workstats.Read().RowIndexCache
	wantBuilds, wantHits := uint64(3), uint64(0)
	if stableIdentity {
		wantBuilds, wantHits = 1, 2
	}
	if after.Builds-before.Builds != wantBuilds || after.Hits-before.Hits != wantHits {
		t.Fatalf("before=%+v after=%+v", before, after)
	}
	// Exercise the unsupported-identity fallback even on this host.
	before = workstats.Read().RowIndexCache
	for range 2 {
		reader, err := newColumnPhysicalRowReaderFromSnapshotView(view, columnPhysicalRowReaderOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if reader.readCache.file != nil {
			reader.readCache.file.identity = columnAssetVerifiedChecksumFileIdentity{}
		}
		for _, file := range reader.readCache.files {
			file.identity = columnAssetVerifiedChecksumFileIdentity{}
		}
		var scratch columnPhysicalRowReaderScratch
		row, err := reader.FetchRow(1, &scratch)
		if err != nil || string(row.ID) != "doc-001" {
			t.Fatalf("identity fallback row=%+v err=%v", row, err)
		}
		reader.Close()
	}
	after = workstats.Read().RowIndexCache
	if after.Builds-before.Builds != 2 || after.RowsVisited-before.RowsVisited != 6 || after.Hits != before.Hits || after.Misses != before.Misses {
		t.Fatalf("identity fallback before=%+v after=%+v", before, after)
	}
	// A warm memo does not bypass a fresh schema/header validation.
	badView := view
	badView.Config.Columns = append([]ColumnStoreColumn(nil), view.Config.Columns...)
	badView.Config.Columns[0].Name = "wrong"
	if reader, err := newColumnPhysicalRowReaderFromSnapshotView(badView, columnPhysicalRowReaderOptions{}); err == nil {
		reader.Close()
		t.Fatal("mismatching schema accepted")
	}
	for _, mode := range []ColumnAssetReadIntegrity{ColumnAssetReadIntegrityCachedVerify, ColumnAssetReadIntegritySkipChecksums} {
		before := workstats.Read().RowIndexCache
		for range 2 {
			reader, err := newColumnPhysicalRowReaderFromSnapshotView(view, columnPhysicalRowReaderOptions{})
			if err != nil {
				t.Fatal(err)
			}
			reader.readCache.readIntegrity = mode
			reader.readCache.verifyChecksum = mode != ColumnAssetReadIntegritySkipChecksums
			var scratch columnPhysicalRowReaderScratch
			if _, err := reader.FetchRow(1, &scratch); err != nil {
				t.Fatal(err)
			}
			reader.Close()
		}
		after := workstats.Read().RowIndexCache
		if after.Hits != before.Hits || after.Builds-before.Builds != 2 {
			t.Fatalf("mode=%s before=%+v after=%+v", mode, before, after)
		}
	}
	corruptColumnAssetPayloadByte(t, root, ref)
	if reader, err := newColumnPhysicalRowReaderFromSnapshotView(view, columnPhysicalRowReaderOptions{}); err == nil {
		reader.Close()
		t.Fatal("strict warm corruption accepted")
	}
}

func TestColumnAssetRowIndexCacheBudgetAndProofIndependence(t *testing.T) {
	resetColumnAssetVerifiedChecksumCacheForTest(t)
	identity := columnAssetVerifiedChecksumFileIdentity{valid: true, dev: 1, ino: 2, size: 3, modTimeUnixNano: 4}
	ref := ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: "memo", Generation: 1, PartID: 1, FileID: 1, Length: 3, Checksum: 5}
	key := columnAssetVerifiedChecksumKeyForRef("memo-root", ref, identity)
	// Four 20 MB tables exceed 64 MiB on both 32-bit and 64-bit platforms.
	const tableBytes = 20_000_000
	tableRows := tableBytes / int(unsafe.Sizeof(int(0)))
	memo := &columnAssetVerifiedRowIndex{version: 6, offsets: make([]int, tableRows)}
	memo.offsets[0] = 42
	storeColumnAssetRowIndex(key, memo)
	if columnAssetVerifiedChecksumCacheContains("memo-root", ref, identity) {
		t.Fatal("offset memo created checksum proof")
	}
	columnAssetVerifiedChecksumCacheStore("memo-root", ref, identity)
	if !columnAssetVerifiedChecksumCacheContains("memo-root", ref, identity) {
		t.Fatal("checksum store lost proof")
	}
	if got := lookupColumnAssetRowIndex(key, 6, 0, columnPhysicalAssetScanHeader{RowCount: len(memo.offsets)}); got != memo {
		t.Fatal("same-key checksum store discarded offsets")
	}
	for _, change := range []func(*columnAssetVerifiedChecksumKey){
		func(k *columnAssetVerifiedChecksumKey) { k.rootDir += "-other" },
		func(k *columnAssetVerifiedChecksumKey) { k.namespace += "-other" },
		func(k *columnAssetVerifiedChecksumKey) { k.generation++ },
		func(k *columnAssetVerifiedChecksumKey) { k.partID++ },
		func(k *columnAssetVerifiedChecksumKey) { k.fileID++ },
		func(k *columnAssetVerifiedChecksumKey) { k.offset++ },
		func(k *columnAssetVerifiedChecksumKey) { k.length++ },
		func(k *columnAssetVerifiedChecksumKey) { k.checksum++ },
		func(k *columnAssetVerifiedChecksumKey) { k.fileDev++ },
		func(k *columnAssetVerifiedChecksumKey) { k.fileIno++ },
		func(k *columnAssetVerifiedChecksumKey) { k.fileSize++ },
		func(k *columnAssetVerifiedChecksumKey) { k.fileModNS++ },
	} {
		changed := key
		change(&changed)
		if lookupColumnAssetRowIndex(changed, 6, 0, columnPhysicalAssetScanHeader{RowCount: len(memo.offsets)}) != nil {
			t.Fatal("different resource identity reused offsets")
		}
	}
	before := workstats.Read().RowIndexCache
	used := map[int]bool{columnAssetVerifiedChecksumCacheIndex(key): true}
	for i := uint64(2); len(used) < 4; i++ {
		next := key
		next.partID = i
		slot := columnAssetVerifiedChecksumCacheIndex(next)
		if used[slot] {
			continue
		}
		used[slot] = true
		storeColumnAssetRowIndex(next, &columnAssetVerifiedRowIndex{version: 6, offsets: make([]int, tableRows)})
	}
	after := workstats.Read().RowIndexCache
	if after.RetainedBytes > columnAssetRowIndexCacheMaxBytes || after.Entries >= 4 || after.Evictions <= before.Evictions {
		t.Fatalf("budget=%+v", after)
	}
	if memo.offsets[0] != 42 {
		t.Fatal("eviction modified borrowed offsets")
	}
	// Capacity, not length, controls oversized admission.
	huge := &columnAssetVerifiedRowIndex{offsets: make([]int, 1, columnAssetRowIndexCacheMaxBytes/int(unsafe.Sizeof(int(0)))+1)}
	next := key
	next.partID = 999999
	storeColumnAssetRowIndex(next, huge)
	final := workstats.Read().RowIndexCache
	if final.RetainedBytes != after.RetainedBytes || final.OversizedBypasses != after.OversizedBypasses+1 {
		t.Fatalf("oversized before=%+v after=%+v", after, final)
	}
	// A collision from the existing checksum-only path releases memo residency.
	for candidate := uint64(2); ; candidate++ {
		other := ref
		other.PartID = candidate
		otherKey := columnAssetVerifiedChecksumKeyForRef("memo-root", other, identity)
		if columnAssetVerifiedChecksumCacheIndex(otherKey) != columnAssetVerifiedChecksumCacheIndex(key) {
			continue
		}
		storeColumnAssetRowIndex(key, &columnAssetVerifiedRowIndex{offsets: []int{42}})
		previous := workstats.Read().RowIndexCache
		columnAssetVerifiedChecksumCacheStore("memo-root", other, identity)
		current := workstats.Read().RowIndexCache
		if current.Entries+1 != previous.Entries || current.RetainedBytes >= previous.RetainedBytes {
			t.Fatalf("collision before=%+v after=%+v", previous, current)
		}
		break
	}
}

func TestColumnAssetRowIndexCacheConcurrentOwnership(t *testing.T) {
	resetColumnAssetVerifiedChecksumCacheForTest(t)
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Go(func() {
			for i := range 64 {
				key := columnAssetVerifiedChecksumKey{rootDir: fmt.Sprint(worker), partID: uint64(i)}
				memo := storeColumnAssetRowIndex(key, &columnAssetVerifiedRowIndex{version: 6, offsets: []int{i}})
				if got := lookupColumnAssetRowIndex(key, 6, 0, columnPhysicalAssetScanHeader{RowCount: 1}); got != nil && got.offsets[0] != i {
					t.Error("corrupt concurrent lookup")
				}
				if memo.offsets[0] != i {
					t.Error("corrupt borrowed offsets")
				}
			}
		})
	}
	wg.Wait()
	if got := workstats.Read().RowIndexCache; got.RetainedBytes > got.ByteLimit {
		t.Fatalf("budget=%+v", got)
	}
}

func TestColumnAssetRowIndexFailedBuildCountsVisitedPrefix(t *testing.T) {
	resetColumnAssetVerifiedChecksumCacheForTest(t)
	cfg := testColumnPhysicalRowReaderConfigV1(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	ref := writeColumnPhysicalRowReaderAssetForTestV1(t, root, cfg, 7, 1, testColumnPhysicalRowReaderRowsV1(0, 3, cfg))
	raw, err := readColumnPhysicalAssetFromManager(root, ref)
	if err != nil {
		t.Fatal(err)
	}
	header, version, rowsOffset, err := parseColumnPhysicalAssetScanHeader(raw, ref, "events", cfg, ColumnPublishOperationInsert)
	if err != nil {
		t.Fatal(err)
	}
	index, err := indexColumnPhysicalAssetReaderRows(raw, version, rowsOffset, header, cfg)
	if err != nil {
		t.Fatal(err)
	}
	bad, err := writeColumnPhysicalAssetToManager(root, *cfg, raw[:index.offsets[1]+1], 7, 1)
	if err != nil {
		t.Fatal(err)
	}
	before := workstats.Read().RowIndexCache
	for range 2 {
		reader, err := newColumnPhysicalRowReaderFromSnapshotView(columnPhysicalRowReaderViewForTestV1(root, cfg, bad), columnPhysicalRowReaderOptions{})
		if err != nil {
			t.Fatal(err)
		}
		var scratch columnPhysicalRowReaderScratch
		if _, err := reader.FetchRow(0, &scratch); err == nil {
			t.Fatal("truncated second row accepted")
		}
		reader.Close()
	}
	after := workstats.Read().RowIndexCache
	if after.Builds-before.Builds != 2 || after.RowsVisited-before.RowsVisited != 4 || after.Entries != before.Entries || after.Hits != before.Hits {
		t.Fatalf("failed prefix before=%+v after=%+v", before, after)
	}
}
