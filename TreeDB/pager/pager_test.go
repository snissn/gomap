package pager

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestPagerLifecycle(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index.db")

	chunkSize := mmapOffsetGranularity()
	if chunkSize < int64(page.PageSize) {
		chunkSize = int64(page.PageSize)
	}

	pagesPerChunk := int(chunkSize / int64(page.PageSize))

	// 1. Open new
	p, err := Open(path, chunkSize)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// 2. Alloc 1 page (fits in Chunk 0)
	id, err := p.Alloc(1)
	if err != nil {
		t.Fatalf("Alloc failed: %v", err)
	}
	if id != 0 {
		t.Errorf("Expected ID 0, got %d", id)
	}

	// Verify file size
	info, _ := os.Stat(path)
	if info.Size() != chunkSize {
		t.Errorf("Expected file size %d, got %d", chunkSize, info.Size())
	}

	// Write data
	data := make([]byte, page.PageSize)
	data[0] = 0xAA
	data[page.PageSize-1] = 0xBB
	if err := p.Write(0, data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Read back
	readBuf, err := p.Get(0)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if readBuf[0] != 0xAA || readBuf[page.PageSize-1] != 0xBB {
		t.Errorf("Read data mismatch")
	}

	// 3. Alloc enough pages to cross into Chunk 1.
	// We have 1 page used. Capacity is pagesPerChunk.
	// We need to alloc `pagesPerChunk` more pages.
	// Total will be 1 + pagesPerChunk.
	// Example: 16KB chunk (4 pages). Used 1. Alloc 4. Total 5.
	// 5 pages requires 2 chunks (capacity 8 pages).

	countToAlloc := pagesPerChunk
	id, err = p.Alloc(countToAlloc)
	if err != nil {
		t.Fatalf("Alloc %d failed: %v", countToAlloc, err)
	}
	if id != 1 {
		t.Errorf("Expected ID 1, got %d", id)
	}

	expectedTotal := 1 + countToAlloc
	if p.PageCount() != uint64(expectedTotal) {
		t.Errorf("Expected %d pages, got %d", expectedTotal, p.PageCount())
	}

	// Verify file size (should be 2 chunks)
	info, _ = os.Stat(path)
	if info.Size() != chunkSize*2 {
		t.Errorf("Expected file size %d, got %d", chunkSize*2, info.Size())
	}

	// Write to the last page (which should be in the new chunk)
	lastPageID := uint64(expectedTotal - 1)
	data2 := make([]byte, page.PageSize)
	data2[0] = 0xCC
	if err := p.Write(lastPageID, data2); err != nil {
		t.Fatalf("Write page %d failed: %v", lastPageID, err)
	}

	// Verify reads
	rb0, _ := p.Get(0)
	rbLast, _ := p.Get(lastPageID)
	if rb0[0] != 0xAA {
		t.Error("Page 0 corrupted")
	}
	if rbLast[0] != 0xCC {
		t.Error("Last Page corrupted")
	}

	// 4. Close
	if err := p.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 5. Reopen
	p2, err := Open(path, chunkSize)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}

	// Set Page Count
	p2.SetPageCount(uint64(expectedTotal))

	// Verify data persistence
	rb0_2, _ := p2.Get(0)
	rbLast_2, _ := p2.Get(lastPageID)
	if rb0_2[0] != 0xAA {
		t.Error("Reopen: Page 0 corrupted")
	}
	if rbLast_2[0] != 0xCC {
		t.Error("Reopen: Last Page corrupted")
	}

	p2.Close()
}

func TestFlushDirtyChunksFromRetainsLowerChunksForFinalSync(t *testing.T) {
	chunkSize := mmapOffsetGranularity()
	if chunkSize < int64(page.PageSize) {
		chunkSize = int64(page.PageSize)
	}
	pagesPerChunk := int(chunkSize / int64(page.PageSize))
	p, err := Open(filepath.Join(t.TempDir(), "selective-sync.db"), chunkSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = p.Close() }()
	if _, err := p.Alloc(pagesPerChunk + 1); err != nil {
		t.Fatalf("alloc: %v", err)
	}
	data := make([]byte, page.PageSize)
	if err := p.Write(0, data); err != nil {
		t.Fatalf("write chunk 0: %v", err)
	}
	if err := p.Write(uint64(pagesPerChunk), data); err != nil {
		t.Fatalf("write chunk 1: %v", err)
	}
	if err := p.FlushDirtyChunksFrom(1); err != nil {
		t.Fatalf("selective flush: %v", err)
	}
	p.mu.RLock()
	_, lowerDirty := p.dirtyChunks[0]
	_, upperDirty := p.dirtyChunks[1]
	p.mu.RUnlock()
	if !lowerDirty || upperDirty {
		t.Fatalf("dirty chunks after selective flush: lower=%t upper=%t", lowerDirty, upperDirty)
	}
	if err := p.Sync(); err != nil {
		t.Fatalf("final sync: %v", err)
	}
	p.mu.RLock()
	dirtyCount := len(p.dirtyChunks)
	p.mu.RUnlock()
	if dirtyCount != 0 {
		t.Fatalf("dirty chunks after final sync=%d want 0", dirtyCount)
	}
}

func TestPagerSyncPagesPersistsRequestedPagesWithoutClearingDirtyChunks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index_sync_pages.db")
	chunkSize := int64(page.PageSize * 4)
	if gran := mmapOffsetGranularity(); gran > 0 && chunkSize%gran != 0 {
		chunkSize = ((chunkSize + gran - 1) / gran) * gran
	}
	pagesPerChunk := int(chunkSize / int64(page.PageSize))
	pageCount := pagesPerChunk + 1

	p, err := Open(path, chunkSize)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := p.Alloc(pageCount); err != nil {
		t.Fatalf("Alloc: %v", err)
	}
	for _, pageID := range []uint64{0, 1, 2, uint64(pagesPerChunk)} {
		buf := make([]byte, page.PageSize)
		buf[0] = byte(pageID + 1)
		if err := p.Write(pageID, buf); err != nil {
			t.Fatalf("Write(%d): %v", pageID, err)
		}
	}
	dirtyBefore := len(p.dirtyChunks)
	if dirtyBefore < 2 {
		t.Fatalf("dirty chunks=%d want at least 2", dirtyBefore)
	}

	if err := p.SyncPages([]uint64{uint64(pagesPerChunk), 2, 1, 1, 0}); err != nil {
		t.Fatalf("SyncPages: %v", err)
	}
	if got := len(p.dirtyChunks); got != dirtyBefore {
		t.Fatalf("dirty chunks=%d want unchanged %d", got, dirtyBefore)
	}
	if err := p.SyncPages([]uint64{uint64(pageCount)}); !errors.Is(err, ErrPageOutOfBounds) {
		t.Fatalf("SyncPages out of bounds error=%v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(path, chunkSize)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	reopened.SetPageCount(uint64(pageCount))
	for _, pageID := range []uint64{0, 1, 2, uint64(pagesPerChunk)} {
		buf, err := reopened.Get(pageID)
		if err != nil {
			t.Fatalf("Get(%d): %v", pageID, err)
		}
		if got, want := buf[0], byte(pageID+1); got != want {
			t.Fatalf("Get(%d)[0]=%d want %d", pageID, got, want)
		}
	}
}

func TestPagerTruncate(t *testing.T) {

	dir := t.TempDir()

	path := filepath.Join(dir, "index_trunc.db")

	chunkSize := int64(page.PageSize * 4) // 4 pages per chunk
	if gran := mmapOffsetGranularity(); gran > 0 && chunkSize%gran != 0 {
		chunkSize = gran
	}

	p, err := Open(path, chunkSize)

	if err != nil {

		t.Fatalf("Open failed: %v", err)

	}

	defer p.Close()

	// 1. Alloc 2 pages

	if _, err := p.Alloc(2); err != nil {

		t.Fatalf("Alloc failed: %v", err)

	}

	if p.PageCount() != 2 {

		t.Errorf("Expected 2 pages, got %d", p.PageCount())

	}

	// 2. Try to truncate to 1 (Shrink) -> Should fail

	if err := p.Truncate(1); err == nil {

		t.Error("Expected error when shrinking, got nil")

	}

	// 3. Truncate to 5 (Grow) -> Should succeed

	if err := p.Truncate(5); err != nil {

		t.Fatalf("Truncate(5) failed: %v", err)

	}

	if p.PageCount() != 5 {

		t.Errorf("Expected 5 pages, got %d", p.PageCount())

	}

	// Verify file size grew

	info, _ := os.Stat(path)

	pagesPerChunk := chunkSize / int64(page.PageSize)
	chunks := (int64(5) + pagesPerChunk - 1) / pagesPerChunk
	expectedSize := chunkSize * chunks

	if info.Size() != expectedSize {
		t.Errorf("Expected file size %d, got %d", expectedSize, info.Size())
	}

}

func TestPagerTruncateSerializesConcurrentGrowth(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index_truncate_concurrent.db")
	chunkSize := int64(page.PageSize * 4)
	if gran := mmapOffsetGranularity(); gran > 0 && chunkSize%gran != 0 {
		chunkSize = gran
	}

	p, err := Open(path, chunkSize)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer p.Close()
	if _, err := p.Alloc(2); err != nil {
		t.Fatalf("initial Alloc: %v", err)
	}

	observed := make(chan uint64, 1)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var snapshots atomic.Int32
	previousHook := testHookTruncateAfterPageCount
	testHookTruncateAfterPageCount = func(current uint64) {
		if snapshots.Add(1) != 1 {
			return
		}
		observed <- current
		<-release
	}
	t.Cleanup(func() { testHookTruncateAfterPageCount = previousHook })

	truncateErr := make(chan error, 1)
	go func() { truncateErr <- p.Truncate(5) }()
	select {
	case current := <-observed:
		if current != 2 {
			t.Fatalf("truncate observed pages=%d want 2", current)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("truncate did not reach page-count snapshot")
	}

	// Both calls observed 2 pages. The old implementation serialized only the
	// later allocations, so their stale +3 and +4 deltas produced 9 pages.
	if err := p.Truncate(6); err != nil {
		t.Fatalf("concurrent Truncate: %v", err)
	}
	close(release)
	select {
	case err := <-truncateErr:
		if err != nil {
			t.Fatalf("Truncate: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("truncate did not finish")
	}
	if got := p.PageCount(); got != 6 {
		t.Fatalf("page count=%d want later target 6", got)
	}
}

func TestPagerAsyncPreGrow_DefaultChunkEnabled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index_pregrow.db")

	chunkSize := int64(256 << 10) // TreeDB default main chunk size
	if gran := mmapOffsetGranularity(); gran > 0 && chunkSize%gran != 0 {
		chunkSize = ((chunkSize + gran - 1) / gran) * gran
	}

	p, err := Open(path, chunkSize)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer p.Close()

	pagesPerChunk := int(chunkSize / int64(page.PageSize))
	if pagesPerChunk < 2 {
		t.Fatalf("unexpected pagesPerChunk=%d for chunkSize=%d", pagesPerChunk, chunkSize)
	}

	// Fill almost an entire chunk. This should trigger async pre-grow and map
	// one additional chunk in the background.
	if _, err := p.Alloc(pagesPerChunk - 1); err != nil {
		t.Fatalf("Alloc failed: %v", err)
	}

	wantCapacity := int64(2) * chunkSize
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := p.currentCapacityBytes(); got >= wantCapacity {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("async pre-grow did not reach capacity=%d within timeout; got=%d", wantCapacity, p.currentCapacityBytes())
}
