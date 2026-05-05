package db

import "testing"

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
