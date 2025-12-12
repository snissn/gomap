package pager

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap-gemini/TreeDB/page"
)

func TestPagerLifecycle(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index.db")
	
	osPageSize := int64(os.Getpagesize())
	chunkSize := osPageSize
	// Ensure chunkSize is at least PageSize (it usually is)
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

func TestPagerOutOfBounds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index_oob.db")
	
	osPageSize := int64(os.Getpagesize())
	chunkSize := osPageSize
	
	p, _ := Open(path, chunkSize)
	defer p.Close()

	p.Alloc(1)

	_, err := p.Get(1)
	if err != ErrPageOutOfBounds {
		t.Errorf("Expected ErrPageOutOfBounds, got %v", err)
	}
}