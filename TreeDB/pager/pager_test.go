package pager

import (
	"os"
	"path/filepath"
	"testing"

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

func TestPagerCloseTrimsUnusedTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index.db")

	const chunkSize = 4 * 1024 * 1024 // match TreeDB default

	p, err := Open(path, chunkSize)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Grow to >1 chunk, then simulate recovery correcting the logical page count
	// to a smaller value. Close should trim the trailing unused capacity down to
	// the minimal chunk-aligned size needed for numPages.
	if _, err := p.Alloc(2000); err != nil {
		_ = p.Close()
		t.Fatalf("Alloc: %v", err)
	}
	p.SetPageCount(1000)
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	want := int64(chunkSize) // 1000 pages (~4MiB) rounds up to 1 chunk
	if st.Size() != want {
		t.Fatalf("unexpected size after Close trim: got=%d want=%d", st.Size(), want)
	}
}
