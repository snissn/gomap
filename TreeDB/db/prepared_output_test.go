package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

type preparedOutputTestLeafLog struct {
	ptrs []page.LeafLogPtr
}

func (l *preparedOutputTestLeafLog) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	ptr := page.LeafLogPtr{
		FileID:           uint32(len(l.ptrs) + 1),
		Offset:           uint64(len(l.ptrs)+1) * 100,
		RecordLengthHint: uint32(len(leafPage)),
	}
	l.ptrs = append(l.ptrs, ptr)
	return ptr, nil
}

func (l *preparedOutputTestLeafLog) Flush() error {
	return nil
}

func (l *preparedOutputTestLeafLog) Sync() error {
	return nil
}

func TestPreparedOutputAllocTrackerAbandonsOwnedPagesOnFree(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	idx := db.idx.Load()
	if idx == nil {
		t.Fatal("missing index")
	}
	tracker := db.newPreparedOutputAllocTracker(idx.allocator)
	if got := tracker.PreparedOutputID(); got == 0 {
		t.Fatal("prepared output ID is zero")
	}

	pageID, err := tracker.Alloc(0)
	if err != nil {
		t.Fatalf("alloc prepared page: %v", err)
	}
	before := tracker.PreparedOutputSnapshot()
	if before.State != preparedOutputStatePrepared {
		t.Fatalf("state=%v want prepared", before.State)
	}
	if len(before.Pages) != 1 || before.Pages[0] != pageID {
		t.Fatalf("pages=%v want [%d]", before.Pages, pageID)
	}

	if err := tracker.FreeAll(); err != nil {
		t.Fatalf("free prepared pages: %v", err)
	}
	after := tracker.PreparedOutputSnapshot()
	if after.State != preparedOutputStateAbandoned {
		t.Fatalf("state=%v want abandoned", after.State)
	}
	if len(after.Pages) != 0 {
		t.Fatalf("abandoned tracker retained pages: %v", after.Pages)
	}
}

func TestPreparedOutputAllocTrackerInstallPreventsAbandonFree(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	idx := db.idx.Load()
	if idx == nil {
		t.Fatal("missing index")
	}
	tracker := db.newPreparedOutputAllocTracker(idx.allocator)
	pageID, err := tracker.Alloc(0)
	if err != nil {
		t.Fatalf("alloc prepared page: %v", err)
	}

	tracker.MarkInstalled()
	if err := tracker.FreeAll(); err != nil {
		t.Fatalf("free installed prepared output: %v", err)
	}
	after := tracker.PreparedOutputSnapshot()
	if after.State != preparedOutputStateInstalled {
		t.Fatalf("state=%v want installed", after.State)
	}
	if len(after.Pages) != 1 || after.Pages[0] != pageID {
		t.Fatalf("installed tracker pages=%v want [%d]", after.Pages, pageID)
	}
}

func TestPreparedOutputAllocTrackerAbandonDoesNotBecomeInstalled(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	idx := db.idx.Load()
	if idx == nil {
		t.Fatal("missing index")
	}
	tracker := db.newPreparedOutputAllocTracker(idx.allocator)
	if _, err := tracker.Alloc(0); err != nil {
		t.Fatalf("alloc prepared page: %v", err)
	}
	if err := tracker.FreeAll(); err != nil {
		t.Fatalf("free prepared pages: %v", err)
	}

	tracker.MarkInstalled()
	after := tracker.PreparedOutputSnapshot()
	if after.State != preparedOutputStateAbandoned {
		t.Fatalf("state=%v want abandoned", after.State)
	}
	if len(after.Pages) != 0 {
		t.Fatalf("abandoned tracker retained pages: %v", after.Pages)
	}
}

func TestPreparedOutputAllocTrackerEmptyFreeLeavesPreparedState(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	idx := db.idx.Load()
	if idx == nil {
		t.Fatal("missing index")
	}
	tracker := db.newPreparedOutputAllocTracker(idx.allocator)
	if err := tracker.FreeAll(); err != nil {
		t.Fatalf("free empty prepared output: %v", err)
	}

	after := tracker.PreparedOutputSnapshot()
	if after.State != preparedOutputStatePrepared {
		t.Fatalf("state=%v want prepared", after.State)
	}
	if len(after.Pages) != 0 {
		t.Fatalf("empty tracker pages=%v want none", after.Pages)
	}
}

func TestPreparedOutputLeafPageLogTracksPointers(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	idx := db.idx.Load()
	if idx == nil {
		t.Fatal("missing index")
	}
	tracker := db.newPreparedOutputAllocTracker(idx.allocator)
	log := preparedOutputLeafPageLog{
		inner:   &preparedOutputTestLeafLog{},
		tracker: tracker,
	}
	first, err := log.AppendLeafPage(make([]byte, page.PageSize))
	if err != nil {
		t.Fatalf("append first leaf page: %v", err)
	}
	second, err := log.AppendLeafPage(make([]byte, page.PageSize))
	if err != nil {
		t.Fatalf("append second leaf page: %v", err)
	}

	before := tracker.PreparedOutputSnapshot()
	if before.State != preparedOutputStatePrepared {
		t.Fatalf("state=%v want prepared", before.State)
	}
	if len(before.LeafLogPtrs) != 2 || before.LeafLogPtrs[0] != first || before.LeafLogPtrs[1] != second {
		t.Fatalf("leaf log ptrs=%v want [%v %v]", before.LeafLogPtrs, first, second)
	}

	if err := tracker.FreeAll(); err != nil {
		t.Fatalf("free prepared leaf-log output: %v", err)
	}
	after := tracker.PreparedOutputSnapshot()
	if after.State != preparedOutputStateAbandoned {
		t.Fatalf("state=%v want abandoned", after.State)
	}
	if len(after.LeafLogPtrs) != 0 {
		t.Fatalf("abandoned tracker retained leaf-log ptrs: %v", after.LeafLogPtrs)
	}
}

func TestPreparedOutputLeafPageLogInstalledRetainsPointers(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	idx := db.idx.Load()
	if idx == nil {
		t.Fatal("missing index")
	}
	tracker := db.newPreparedOutputAllocTracker(idx.allocator)
	log := preparedOutputLeafPageLog{
		inner:   &preparedOutputTestLeafLog{},
		tracker: tracker,
	}
	ptr, err := log.AppendLeafPage(make([]byte, page.PageSize))
	if err != nil {
		t.Fatalf("append leaf page: %v", err)
	}

	tracker.MarkInstalled()
	if err := tracker.FreeAll(); err != nil {
		t.Fatalf("free installed prepared leaf-log output: %v", err)
	}
	after := tracker.PreparedOutputSnapshot()
	if after.State != preparedOutputStateInstalled {
		t.Fatalf("state=%v want installed", after.State)
	}
	if len(after.LeafLogPtrs) != 1 || after.LeafLogPtrs[0] != ptr {
		t.Fatalf("installed tracker leaf-log ptrs=%v want [%v]", after.LeafLogPtrs, ptr)
	}
}
