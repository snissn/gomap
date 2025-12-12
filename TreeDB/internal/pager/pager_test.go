package pager

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"treedb/internal/crc"
	"treedb/internal/page"
)

func TestChunkAlignmentAndBoundaryCrossing(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(dir, 2*page.PageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = p.Close() }()

	// Allocate enough pages to force mapping multiple chunks.
	var ids []page.PageID
	for i := 0; i < 6; i++ {
		id, err := p.AllocPage()
		if err != nil {
			t.Fatalf("alloc %d: %v", i, err)
		}
		ids = append(ids, id)
	}

	// Ensure all pages are readable/writable without bounds errors.
	for _, id := range ids {
		b, err := p.ReadPage(id)
		if err != nil {
			t.Fatalf("read %d: %v", id, err)
		}
		if len(b) != page.PageSize {
			t.Fatalf("unexpected page size for %d: %d", id, len(b))
		}
	}

	if len(p.chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(p.chunks))
	}
	for i, c := range p.chunks {
		if c.startOffset%p.chunkSize != 0 {
			t.Fatalf("chunk %d startOffset misaligned: %d", i, c.startOffset)
		}
		if int64(len(c.data)) != p.chunkSize {
			t.Fatalf("chunk %d data length: got %d want %d", i, len(c.data), p.chunkSize)
		}
		if int64(len(c.data))%page.PageSize != 0 {
			t.Fatalf("chunk %d data length not page-aligned: %d", i, len(c.data))
		}
	}

	// Invalid chunk size should fail.
	if _, err := Open(t.TempDir(), page.PageSize+1); err != ErrInvalidChunkSize {
		t.Fatalf("expected ErrInvalidChunkSize, got %v", err)
	}
}

func TestGrowthSafetyWithPinnedChunk(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(dir, 2*page.PageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = p.Close() }()

	pin, err := p.pinChunk(0)
	if err != nil {
		t.Fatalf("pin: %v", err)
	}
	oldAddr := uintptr(unsafe.Pointer(&pin.c.data[0]))

	// Grow well beyond current size.
	if err := p.GrowToPages(p.TotalPages() + 10); err != nil {
		t.Fatalf("grow: %v", err)
	}

	newAddr := uintptr(unsafe.Pointer(&pin.c.data[0]))
	if oldAddr != newAddr {
		t.Fatalf("old chunk remapped unexpectedly")
	}
	pin.Release()
}

func TestShrinkForbidden(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(dir, 2*page.PageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = p.Close() }()

	cur := p.TotalPages()
	if err := p.GrowToPages(cur - 1); err != ErrShrinkForbidden {
		t.Fatalf("expected ErrShrinkForbidden, got %v", err)
	}
}

func TestRefcountedUnmapSafety(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(dir, 2*page.PageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	pin, err := p.pinChunk(0)
	if err != nil {
		t.Fatalf("pin: %v", err)
	}
	if err := p.Close(); err != ErrChunkPinned {
		t.Fatalf("expected ErrChunkPinned, got %v", err)
	}
	pin.Release()
	if err := p.Close(); err != nil {
		t.Fatalf("close after release: %v", err)
	}
}

func TestSyncIndexSmoke(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(dir, 2*page.PageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = p.Close() }()

	id, err := p.AllocPage()
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	data := make([]byte, page.PageSize)
	data[0] = 0xAA
	if err := p.WritePage(id, data); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := p.SyncIndex(); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

func TestMetaSelectionHighestCommitSeq(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(dir, 2*page.PageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	meta := p.ReadActiveMeta()
	meta.CommitSeq = 2
	if err := p.WriteInactiveMeta(meta); err != nil {
		t.Fatalf("write inactive meta: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := Open(dir, 2*page.PageSize)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = p2.Close() }()

	if got := p2.ReadActiveMeta().CommitSeq; got != 2 {
		t.Fatalf("expected commit seq 2, got %d", got)
	}
}

func TestFreelistReuse(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(dir, 2*page.PageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = p.Close() }()

	id1, _ := p.AllocPage()
	id2, _ := p.AllocPage()
	if err := p.FreePages([]page.PageID{id1, id2}); err != nil {
		t.Fatalf("free: %v", err)
	}
	ra, err := p.AllocPage()
	if err != nil {
		t.Fatalf("realloc: %v", err)
	}
	rb, _ := p.AllocPage()
	if (ra != id2 && ra != id1) || (rb != id2 && rb != id1) || ra == rb {
		t.Fatalf("expected reuse of freed pages, got %d and %d", ra, rb)
	}
}

func TestPreallocCacheStalenessFailsFast(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(dir, 2*page.PageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = p.Close() }()

	fi, err := os.Stat(filepath.Join(dir, "index.db"))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	p.mu.Lock()
	p.preallocSize = fi.Size() + p.chunkSize
	wantGrowSize := p.preallocSize + p.chunkSize
	p.mu.Unlock()

	if err := p.preallocate(wantGrowSize); err != ErrFileCorrupt {
		t.Fatalf("expected ErrFileCorrupt, got %v", err)
	}
}

func TestAllocReserveCapEnforced(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(dir, 2*page.PageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = p.Close() }()

	// chunkPages=2, maxChunks=2 => cap=4 pages.
	if err := p.BeginAllocReserve(1_000_000, 2); err != nil {
		t.Fatalf("begin reserve: %v", err)
	}
	p.mu.Lock()
	gotBatch := p.reserveBatch
	p.mu.Unlock()
	if gotBatch != 4 {
		t.Fatalf("reserve batch: got %d want %d", gotBatch, 4)
	}
	if err := p.EndAllocReserve(); err != nil {
		t.Fatalf("end reserve: %v", err)
	}
}

func TestAllocReserveUndershootFallback(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(dir, 2*page.PageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = p.Close() }()

	start := p.TotalPages()
	if err := p.BeginAllocReserve(1, 4); err != nil {
		t.Fatalf("begin reserve: %v", err)
	}

	var ids []page.PageID
	for i := 0; i < 5; i++ {
		id, err := p.AllocPage()
		if err != nil {
			t.Fatalf("alloc %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	for i, id := range ids {
		want := page.PageID(uint64(start) + uint64(i))
		if id != want {
			t.Fatalf("id[%d]=%d want %d (ids=%v)", i, id, want, ids)
		}
	}
	if err := p.EndAllocReserve(); err != nil {
		t.Fatalf("end reserve: %v", err)
	}
}

func TestFreelistPopSkipsEmptyHeadPage(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(dir, 2*page.PageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = p.Close() }()

	// Create a second freelist page and a single free ID on it.
	fl2, err := p.AllocPage()
	if err != nil {
		t.Fatalf("alloc fl2: %v", err)
	}
	freeID, err := p.AllocPage()
	if err != nil {
		t.Fatalf("alloc freeID: %v", err)
	}
	headPid, err := p.AllocPage()
	if err != nil {
		t.Fatalf("alloc head: %v", err)
	}

	p.mu.Lock()
	buf2, err := p.pageSliceLocked(fl2)
	if err != nil {
		p.mu.Unlock()
		t.Fatalf("slice fl2: %v", err)
	}
	if err := initFreelistPage(fl2, 0, buf2); err != nil {
		p.mu.Unlock()
		t.Fatalf("init fl2: %v", err)
	}
	if n, err := appendFreelistIDs(buf2, []page.PageID{freeID}); err != nil || n != 1 {
		p.mu.Unlock()
		t.Fatalf("append fl2: n=%d err=%v", n, err)
	}
	bufHead, err := p.pageSliceLocked(headPid)
	if err != nil {
		p.mu.Unlock()
		t.Fatalf("slice head: %v", err)
	}
	if err := initFreelistPage(headPid, fl2, bufHead); err != nil {
		p.mu.Unlock()
		t.Fatalf("init head: %v", err)
	}
	p.meta.FreelistHeadID = headPid
	p.freelistCount = 1
	p.mu.Unlock()

	got, err := p.AllocPage()
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	if got != freeID {
		t.Fatalf("expected %d, got %d", freeID, got)
	}

	p.mu.Lock()
	if p.meta.FreelistHeadID != fl2 {
		p.mu.Unlock()
		t.Fatalf("expected head advanced to %d, got %d", fl2, p.meta.FreelistHeadID)
	}
	p.mu.Unlock()
}

func TestFreelistAppendAllocatesNewHeadWhenFull(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(dir, 2*page.PageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = p.Close() }()

	capacity := freelistCapacity()
	ids := make([]page.PageID, 0, capacity+2)
	for i := 0; i < capacity+2; i++ {
		id, err := p.AllocPage()
		if err != nil {
			t.Fatalf("alloc %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	if err := p.FreePages(ids[:capacity]); err != nil {
		t.Fatalf("free fill: %v", err)
	}
	p.mu.Lock()
	oldHead := p.meta.FreelistHeadID
	p.mu.Unlock()

	if err := p.FreePages(ids[capacity : capacity+1]); err != nil {
		t.Fatalf("free overflow: %v", err)
	}

	p.mu.Lock()
	newHead := p.meta.FreelistHeadID
	if newHead == oldHead {
		p.mu.Unlock()
		t.Fatalf("expected new freelist head allocated")
	}
	buf, err := p.pageSliceLocked(newHead)
	if err != nil {
		p.mu.Unlock()
		t.Fatalf("slice new head: %v", err)
	}
	_, _, next, count, err := openFreelistPage(buf)
	p.mu.Unlock()
	if err != nil {
		t.Fatalf("open new head: %v", err)
	}
	if next != oldHead {
		t.Fatalf("new head next=%d want %d", next, oldHead)
	}
	if count != 1 {
		t.Fatalf("new head count=%d want %d", count, 1)
	}
}

func TestFreelistCRCMismatchOnAlloc(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(dir, 2*page.PageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = p.Close() }()

	id, err := p.AllocPage()
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	if err := p.FreePages([]page.PageID{id}); err != nil {
		t.Fatalf("free: %v", err)
	}

	p.mu.Lock()
	head := p.meta.FreelistHeadID
	buf, err := p.pageSliceLocked(head)
	if err != nil {
		p.mu.Unlock()
		t.Fatalf("slice head: %v", err)
	}
	_, body, err := page.SplitPage(buf)
	if err != nil {
		p.mu.Unlock()
		t.Fatalf("split: %v", err)
	}
	body[freelistHeaderExtra] ^= 0xFF
	p.mu.Unlock()

	if _, err := p.AllocPage(); !errors.Is(err, crc.ErrChecksumMismatch) {
		t.Fatalf("expected crc mismatch, got %v", err)
	}
}
