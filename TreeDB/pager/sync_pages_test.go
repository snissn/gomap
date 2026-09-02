package pager

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func syncPagesTestChunkSize(minPages int) int64 {
	size := int64(minPages * page.PageSize)
	size = alignUp(size, int64(os.Getpagesize()))
	return alignUp(size, mmapOffsetGranularity())
}

func TestSyncPagesUsesDataDurabilityBoundary(t *testing.T) {
	originalRanges := syncPageRangesFn
	originalFile := syncPageFileFn
	rangeCalls := 0
	fileCalls := 0
	syncPageRangesFn = func(file *os.File, chunks [][]byte, ranges []syncPageRange, chunkSize int64) (bool, error) {
		if file == nil || len(chunks) == 0 || len(ranges) == 0 || chunkSize <= 0 {
			t.Fatalf("invalid range durability input: file=%v chunks=%d ranges=%d chunk_size=%d", file, len(chunks), len(ranges), chunkSize)
		}
		rangeCalls++
		return true, nil
	}
	syncPageFileFn = func(file *os.File) error {
		fileCalls++
		return originalFile(file)
	}
	t.Cleanup(func() {
		syncPageRangesFn = originalRanges
		syncPageFileFn = originalFile
	})

	dir := t.TempDir()
	p, err := Open(filepath.Join(dir, "data-sync.db"), syncPagesTestChunkSize(1))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = p.Close() }()
	id, err := p.Alloc(1)
	if err != nil {
		t.Fatalf("Alloc: %v", err)
	}
	p.durableFileSize.Store(int64(len(p.chunks)) * p.chunkSize)
	if err := p.Write(id, bytes.Repeat([]byte{0x2a}, page.PageSize)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := p.SyncPages([]uint64{id}); err != nil {
		t.Fatalf("SyncPages: %v", err)
	}
	if rangeCalls != 1 || fileCalls != 0 {
		t.Fatalf("range/file durability calls=%d/%d want 1/0", rangeCalls, fileCalls)
	}
}

func TestSyncPagesFallsBackWhenRangeDurabilityIsUnavailable(t *testing.T) {
	originalRanges := syncPageRangesFn
	originalFile := syncPageFileFn
	fileCalls := 0
	syncPageRangesFn = func(*os.File, [][]byte, []syncPageRange, int64) (bool, error) { return false, nil }
	syncPageFileFn = func(*os.File) error {
		fileCalls++
		return nil
	}
	t.Cleanup(func() {
		syncPageRangesFn = originalRanges
		syncPageFileFn = originalFile
	})

	p, err := Open(filepath.Join(t.TempDir(), "fallback.db"), syncPagesTestChunkSize(1))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = p.Close() }()
	id, err := p.Alloc(1)
	if err != nil {
		t.Fatalf("Alloc: %v", err)
	}
	p.durableFileSize.Store(int64(len(p.chunks)) * p.chunkSize)
	if err := p.SyncPages([]uint64{id}); err != nil {
		t.Fatalf("SyncPages: %v", err)
	}
	if fileCalls != 1 {
		t.Fatalf("fallback durability calls=%d want 1", fileCalls)
	}
}

func TestSyncPagesFallsBackUntilFileGrowthIsDurable(t *testing.T) {
	originalRanges := syncPageRangesFn
	originalFile := syncPageFileFn
	rangeCalls := 0
	fileCalls := 0
	syncPageRangesFn = func(*os.File, [][]byte, []syncPageRange, int64) (bool, error) {
		rangeCalls++
		return true, nil
	}
	syncPageFileFn = func(*os.File) error {
		fileCalls++
		return nil
	}
	t.Cleanup(func() {
		syncPageRangesFn = originalRanges
		syncPageFileFn = originalFile
	})

	chunkSize := syncPagesTestChunkSize(2)
	p, err := Open(filepath.Join(t.TempDir(), "growth.db"), chunkSize)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = p.Close() }()
	first, err := p.Alloc(1)
	if err != nil {
		t.Fatalf("Alloc first: %v", err)
	}
	if err := p.SyncPages([]uint64{first}); err != nil {
		t.Fatalf("SyncPages growth: %v", err)
	}
	if rangeCalls != 0 || fileCalls != 1 {
		t.Fatalf("growth range/file durability calls=%d/%d want 0/1", rangeCalls, fileCalls)
	}

	second, err := p.Alloc(1)
	if err != nil {
		t.Fatalf("Alloc second: %v", err)
	}
	if err := p.SyncPages([]uint64{second}); err != nil {
		t.Fatalf("SyncPages durable capacity: %v", err)
	}
	if rangeCalls != 1 || fileCalls != 1 {
		t.Fatalf("steady-state range/file durability calls=%d/%d want 1/1", rangeCalls, fileCalls)
	}
}

func TestUseDurableRangeWritesBoundsSparseDurabilityWork(t *testing.T) {
	unit := syncPageRange{chunk: 0, start: 0, end: page.PageSize}
	if !useDurableRangeWrites([]syncPageRange{unit}) {
		t.Fatal("single-page publication should use range durability")
	}
	tooMany := make([]syncPageRange, maxDurableRangeWrites+1)
	for i := range tooMany {
		tooMany[i] = unit
	}
	if useDurableRangeWrites(tooMany) {
		t.Fatalf("%d ranges should use the file durability fallback", len(tooMany))
	}
	if useDurableRangeWrites([]syncPageRange{{chunk: 0, start: 0, end: maxDurableRangeBytes + 1}}) {
		t.Fatal("oversized range should use the file durability fallback")
	}
}

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
	if err := p.Sync(); err != nil {
		_ = p.Close()
		t.Fatalf("Sync allocated file size: %v", err)
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
