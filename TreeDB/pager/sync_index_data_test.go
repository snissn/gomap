package pager

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestSyncIndexDataExcludesMetaPages(t *testing.T) {
	p, err := Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	if _, err := p.Alloc(4); err != nil {
		t.Fatal(err)
	}
	data, err := p.GetForWrite(2)
	if err != nil {
		t.Fatal(err)
	}
	data[0] = 0x42

	ids := p.DirtyIndexPages()
	if len(ids) != 1 || ids[0] != 2 {
		t.Fatalf("DirtyIndexPages=%v want [2]", ids)
	}
	if err := p.SyncIndexFileSize(); err != nil {
		t.Fatal(err)
	}
	originalRanges := syncPageRangesFn
	originalFile := syncPageFileFn
	t.Cleanup(func() {
		syncPageRangesFn = originalRanges
		syncPageFileFn = originalFile
	})
	var captured []syncPageRange
	fileSyncCalls := 0
	syncPageRangesFn = func(_ *os.File, _ [][]byte, ranges []syncPageRange, _ int64) (bool, error) {
		captured = append(captured, ranges...)
		return true, nil
	}
	syncPageFileFn = func(_ *os.File) error {
		fileSyncCalls++
		return nil
	}
	if err := p.SyncIndexData(); err != nil {
		t.Fatal(err)
	}
	if fileSyncCalls != 0 {
		t.Fatalf("SyncIndexData used file-wide sync %d times", fileSyncCalls)
	}
	if len(captured) == 0 {
		t.Fatal("SyncIndexData did not issue a page range")
	}
	for _, r := range captured {
		start := int64(r.chunk)*p.chunkSize + int64(r.start)
		end := int64(r.chunk)*p.chunkSize + int64(r.end)
		if start < int64(2*page.PageSize) && end > 0 {
			t.Fatalf("range [%d,%d) overlaps redundant meta pages", start, end)
		}
	}
	if got := p.DirtyIndexPages(); len(got) != 0 {
		t.Fatalf("dirty index pages after sync=%v", got)
	}
	if err := p.SyncPages([]uint64{0}); err != nil {
		t.Fatal(err)
	}
}

func TestSyncIndexPagesDoesNotConsumeLaterBuilderDirtyPage(t *testing.T) {
	p, err := Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	if _, err := p.Alloc(4); err != nil {
		t.Fatal(err)
	}
	committed, err := p.GetForWrite(2)
	if err != nil {
		t.Fatal(err)
	}
	committed[0] = 0x22
	laterBuilder, err := p.GetForWrite(3)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.SyncIndexFileSize(); err != nil {
		t.Fatal(err)
	}

	originalRanges := syncPageRangesFn
	t.Cleanup(func() { syncPageRangesFn = originalRanges })
	entered := make(chan struct{})
	release := make(chan struct{})
	syncPageRangesFn = func(_ *os.File, _ [][]byte, ranges []syncPageRange, _ int64) (bool, error) {
		if len(ranges) != 1 {
			t.Fatalf("candidate ranges=%v want one", ranges)
		}
		close(entered)
		<-release
		return true, nil
	}
	done := make(chan error, 1)
	go func() { done <- p.SyncIndexPages([]uint64{2}) }()
	<-entered
	// The later builder already owns its mmap slice and can continue mutating it
	// without taking pager.mu while the prior candidate is in its durability cut.
	laterBuilder[0] = 0x33
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := p.DirtyIndexPages(); !reflect.DeepEqual(got, []uint64{3}) {
		t.Fatalf("dirty pages after prior candidate sync=%v want [3]", got)
	}
}

func TestSyncDirtyPageCleanupPreservesNewGeneration(t *testing.T) {
	p := &Pager{dirtyPages: map[uint64]uint64{2: 1, 3: 2}}
	p.clearDirtyPageGenerations(map[uint64]uint64{2: 1, 3: 1})
	if got := p.DirtyIndexPages(); !reflect.DeepEqual(got, []uint64{3}) {
		t.Fatalf("dirty pages after stale sync cleanup=%v want [3]", got)
	}
}

func TestSyncIndexDataRejectsDirtyMetaInvariant(t *testing.T) {
	p, err := Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	if _, err := p.Alloc(4); err != nil {
		t.Fatal(err)
	}
	meta, err := p.GetForWrite(0)
	if err != nil {
		t.Fatal(err)
	}
	meta[0] = 0x7f
	if err := p.SyncIndexFileSize(); err == nil {
		t.Fatal("SyncIndexFileSize accepted a dirty meta slot")
	}
}
