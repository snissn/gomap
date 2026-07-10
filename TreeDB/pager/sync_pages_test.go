package pager

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestPlanSyncPageRangesAlignsTo64KiBAllocationGranularity(t *testing.T) {
	const (
		chunkSize   = int64(256 << 10)
		granularity = int64(64 << 10)
	)
	ranges, err := planSyncPageRanges(
		[]uint64{33, 1, 2, 2, 47},
		0,
		64,
		chunkSize,
		granularity,
		[]int{int(chunkSize)},
	)
	if err != nil {
		t.Fatalf("planSyncPageRanges: %v", err)
	}
	want := []syncPageRange{
		{chunk: 0, start: 0, end: 64 << 10},
		{chunk: 0, start: 128 << 10, end: 192 << 10},
	}
	if len(ranges) != len(want) {
		t.Fatalf("ranges=%v want=%v", ranges, want)
	}
	for i := range want {
		if ranges[i] != want[i] {
			t.Fatalf("range[%d]=%+v want=%+v", i, ranges[i], want[i])
		}
		if ranges[i].start%int(granularity) != 0 {
			t.Fatalf("range[%d] start=%d is not granularity aligned", i, ranges[i].start)
		}
	}
}

func TestPlanSyncPageRangesUsesOverlayLocalOffsets(t *testing.T) {
	const base = uint64(1 << 63)
	ranges, err := planSyncPageRanges(
		[]uint64{base + 1, base + 2},
		base,
		base+4,
		64<<10,
		64<<10,
		[]int{64 << 10},
	)
	if err != nil {
		t.Fatalf("planSyncPageRanges: %v", err)
	}
	if len(ranges) != 1 || ranges[0] != (syncPageRange{chunk: 0, start: 0, end: 64 << 10}) {
		t.Fatalf("ranges=%v", ranges)
	}
	if _, err := planSyncPageRanges([]uint64{1}, base, base+4, 64<<10, 64<<10, []int{64 << 10}); !errors.Is(err, ErrPageOutOfBounds) {
		t.Fatalf("fallback write range error=%v want ErrPageOutOfBounds", err)
	}
}

func TestMemoryOverlayPagerKeepsPrivatePagesOutOfFallback(t *testing.T) {
	dir := t.TempDir()
	chunkSize := mmapOffsetGranularity()
	if chunkSize < int64(page.PageSize) {
		chunkSize = int64(page.PageSize)
	}
	basePager, err := Open(filepath.Join(dir, "base.db"), chunkSize)
	if err != nil {
		t.Fatalf("Open base: %v", err)
	}
	defer func() { _ = basePager.Close() }()
	baseID, err := basePager.Alloc(1)
	if err != nil {
		t.Fatalf("Alloc base: %v", err)
	}
	wantBase := bytes.Repeat([]byte{0x31}, page.PageSize)
	if err := basePager.Write(baseID, wantBase); err != nil {
		t.Fatalf("Write base: %v", err)
	}

	const overlayBase = uint64(1 << 63)
	overlay, err := NewOverlay(chunkSize, overlayBase, basePager)
	if err != nil {
		t.Fatalf("NewOverlay: %v", err)
	}
	defer func() { _ = overlay.Close() }()
	gotBase, err := overlay.Get(baseID)
	if err != nil || !bytes.Equal(gotBase, wantBase) {
		t.Fatalf("overlay fallback Get error=%v equal=%t", err, bytes.Equal(gotBase, wantBase))
	}
	privateID, err := overlay.Alloc(1)
	if err != nil {
		t.Fatalf("Alloc overlay: %v", err)
	}
	if privateID != overlayBase {
		t.Fatalf("privateID=%d want=%d", privateID, overlayBase)
	}
	wantPrivate := bytes.Repeat([]byte{0x73}, page.PageSize)
	if err := overlay.Write(privateID, wantPrivate); err != nil {
		t.Fatalf("Write overlay: %v", err)
	}
	gotPrivate, err := overlay.Get(privateID)
	if err != nil || !bytes.Equal(gotPrivate, wantPrivate) {
		t.Fatalf("overlay private Get error=%v equal=%t", err, bytes.Equal(gotPrivate, wantPrivate))
	}
	if got := basePager.PageCount(); got != 1 {
		t.Fatalf("fallback PageCount=%d want=1", got)
	}
	if err := overlay.SyncPages([]uint64{privateID}); err != nil {
		t.Fatalf("SyncPages overlay: %v", err)
	}
	if err := overlay.SyncPages([]uint64{privateID + 1}); !errors.Is(err, ErrPageOutOfBounds) {
		t.Fatalf("SyncPages unallocated error=%v want ErrPageOutOfBounds", err)
	}
}

func TestSyncPagesPersistsNonzeroPageWithinAllocationGranularity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "aligned-sync.db")
	chunkSize := mmapOffsetGranularity()
	if chunkSize < int64(2*page.PageSize) {
		chunkSize = int64(2 * page.PageSize)
	}
	p, err := Open(path, chunkSize)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	start, err := p.Alloc(2)
	if err != nil {
		_ = p.Close()
		t.Fatalf("Alloc: %v", err)
	}
	want := bytes.Repeat([]byte{0x5d}, page.PageSize)
	if err := p.Write(start+1, want); err != nil {
		_ = p.Close()
		t.Fatalf("Write: %v", err)
	}
	if err := p.SyncPages([]uint64{start + 1}); err != nil {
		_ = p.Close()
		t.Fatalf("SyncPages: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(path, chunkSize)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopened.SetPageCount(2)
	got, err := reopened.Get(start + 1)
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("synced page changed after reopen")
	}
}
