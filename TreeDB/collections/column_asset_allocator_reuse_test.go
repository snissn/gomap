package collections

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestColumnAssetAllocatorReusesExhaustedHint(t *testing.T) {
	dir := t.TempDir()
	for _, id := range []uint32{1, 2, columnAssetDirectViewSegmentFileIDBase - 1, columnAssetDirectViewSegmentFileIDBase} {
		if err := os.WriteFile(filepath.Join(dir, columnAssetSegmentFileName(id)), []byte("occupied"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	namespace := columnAssetManagerNamespace{SegmentDir: dir}
	for _, next := range []uint32{0, columnAssetDirectViewSegmentFileIDBase} {
		cache := columnAssetSegmentAllocationCache{segmentDir: dir, nextFileID: next, valid: true}
		id, err := nextColumnAssetSegmentFileIDCached(namespace, dir, &cache)
		if err != nil || id != 3 {
			t.Fatalf("exhausted hint %d: id=%d err=%v, want reclaimed gap 3", next, id, err)
		}
	}
	id, err := nextColumnAssetSegmentFileID(namespace)
	if err != nil || id != 3 {
		t.Fatalf("cold scan id=%d err=%v, want gap 3", id, err)
	}
}

func TestColumnAssetAllocatorSmallCompleteBoundary(t *testing.T) {
	for _, tc := range []struct {
		ids  []uint32
		want uint32
	}{{[]uint32{0, 1, 2, 3, 4, 5, 8}, 0}, {[]uint32{1, 2, 2, 4, 5, 8}, 3}, {[]uint32{1, 2, 3, 4}, 5}} {
		var segments []columnAssetReachabilitySegment
		for _, id := range tc.ids {
			segments = append(segments, columnAssetReachabilitySegment{fileID: id})
		}
		id, err := nextColumnAssetSegmentFileIDFromSorted(segments, 6)
		if id != tc.want || (err != nil) != (tc.want == 0) {
			t.Fatalf("ids=%v got=%d err=%v want=%d", tc.ids, id, err, tc.want)
		}
	}
}

func TestColumnAssetAllocatorReusesOnlyAfterReaderAndExactGC(t *testing.T) {
	requireColumnAssetExactDestructiveGCTest(t)
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer d.Close()
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatal(err)
	}
	old := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 2, []byte("old payload"))
	edge := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, columnAssetDirectViewSegmentFileIDBase-1, []byte("edge"))
	cache, err := newColumnPhysicalAssetReadCacheWithIntegrity(d.ColumnAssetRootDir(), old.Namespace, ColumnAssetReadIntegrityCachedVerify)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.close()
	if err := cache.useMappedResourceManager(mappedresource.NewManager(), mappedresource.Scope{Kind: mappedresource.ScopeSnapshot, ID: "allocator-reuse", Namespace: old.Namespace, Collection: "events", Generation: old.Generation}, "allocator-reuse"); err != nil {
		t.Fatal(err)
	}
	if raw, err := cache.read(old, nil); err != nil || string(raw) != "old payload" {
		t.Fatalf("old read=%q err=%v", raw, err)
	}
	opts := ColumnAssetGCOptions{CandidateRefs: []ColumnAssetRef{old}, PinnedRefs: []ColumnAssetRef{edge}}
	stats, err := col.ColumnAssetGC(context.Background(), opts)
	if err != nil || stats.SegmentsDeleted != 0 {
		t.Fatalf("pinned GC=%+v err=%v", stats, err)
	}
	first, err := newNextColumnPhysicalAssetSegmentAppenderWithStableResources(d.ColumnAssetRootDir(), *col.Meta().Options.ColumnStore, d.StableResourceIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer first.abort()
	if first.fileID != 3 {
		t.Fatalf("protected file reused: id=%d", first.fileID)
	}
	if raw, err := cache.read(old, nil); err != nil || string(raw) != "old payload" {
		t.Fatalf("held read=%q err=%v", raw, err)
	}
	if err := cache.close(); err != nil {
		t.Fatal(err)
	}
	stats, err = col.ColumnAssetGC(context.Background(), opts)
	if err != nil || stats.SegmentsDeleted != 1 {
		t.Fatalf("released GC=%+v err=%v", stats, err)
	}
	ns, err := columnAssetManagerNamespaceForRoot(d.ColumnAssetRootDir(), old.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	idx := columnAssetSegmentAllocationLockIndex(ns.SegmentDir)
	columnAssetSegmentAllocationLocks[idx].Lock()
	columnAssetSegmentAllocationCaches[idx] = columnAssetSegmentAllocationCache{} // cold reopen-equivalent hint
	columnAssetSegmentAllocationLocks[idx].Unlock()
	retry, err := newNextColumnPhysicalAssetSegmentAppenderWithStableResources(d.ColumnAssetRootDir(), *col.Meta().Options.ColumnStore, d.StableResourceIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer retry.abort()
	if retry.fileID != old.FileID {
		t.Fatalf("reclaimed ID=%d want=%d", retry.fileID, old.FileID)
	}
	ref, err := retry.append([]byte("new payload"), old.Generation, old.PartID)
	if err != nil {
		t.Fatal(err)
	}
	if err := retry.close(); err != nil {
		t.Fatal(err)
	}
	if raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref); err != nil || string(raw) != "new payload" {
		t.Fatalf("new read=%q err=%v", raw, err)
	}
	if _, err := readColumnPhysicalAssetFromManagerIntoWithIntegrity(d.ColumnAssetRootDir(), old, nil, ColumnAssetReadIntegrityCachedVerify); err == nil {
		t.Fatal("stale logical reference bypassed new file checksum")
	}
	if err := first.abort(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openCollectionCommandWALDB(t, dir)
	defer reopened.Close()
	if raw, err := readColumnPhysicalAssetFromManager(reopened.ColumnAssetRootDir(), ref); err != nil || string(raw) != "new payload" {
		t.Fatalf("reopened reused ref=%q err=%v", raw, err)
	}
}

func TestColumnAssetAllocatorHoleCollisionAndConcurrent(t *testing.T) {
	requireColumnAssetExactDestructiveGCTest(t)
	cfg, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	ns, err := columnAssetManagerNamespaceForRoot(root, cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureColumnAssetManagerNamespace(ns); err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint32{1, columnAssetDirectViewSegmentFileIDBase - 1} {
		if err := os.WriteFile(filepath.Join(ns.SegmentDir, columnAssetSegmentFileName(id)), []byte("occupied"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(ns.SegmentDir, columnAssetSegmentFileName(2)), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(ns.SegmentDir, columnAssetSegmentFileName(3))); err != nil {
		t.Fatal(err)
	}
	idx := columnAssetSegmentAllocationLockIndex(ns.SegmentDir)
	columnAssetSegmentAllocationLocks[idx].Lock()
	columnAssetSegmentAllocationCaches[idx] = columnAssetSegmentAllocationCache{segmentDir: ns.SegmentDir, nextFileID: 2, valid: true}
	columnAssetSegmentAllocationLocks[idx].Unlock()
	var wg sync.WaitGroup
	ids := make(chan uint32, 8)
	registry := rootpublication.NewIdentityPinRegistry()
	first, err := newNextColumnPhysicalAssetSegmentAppenderWithStableResources(root, *cfg, registry)
	if err != nil {
		t.Fatal(err)
	}
	ids <- first.fileID
	if err := first.abort(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 7; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var a *columnPhysicalAssetSegmentAppender
			var err error
			if i%2 == 0 {
				a, err = newNextColumnPhysicalAssetSegmentAppenderWithStableResources(root, *cfg, registry)
			} else {
				a, err = newNextColumnPhysicalAssetSegmentAppender(root, *cfg)
			}
			if err != nil {
				t.Error(err)
				return
			}
			ids <- a.fileID
			if err := a.abort(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	close(ids)
	seen := make(map[uint32]bool)
	for id := range ids {
		if id < 4 || id >= columnAssetDirectViewSegmentFileIDBase || seen[id] {
			t.Fatalf("invalid or reused occupied ID %d", id)
		}
		seen[id] = true
	}
	if len(seen) != 8 {
		t.Fatalf("created %d files", len(seen))
	}
}
