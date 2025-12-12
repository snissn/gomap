package pager

import (
	"testing"
	"unsafe"

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

