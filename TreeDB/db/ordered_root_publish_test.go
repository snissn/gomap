package db

import "testing"

func TestPublishOrderedRootIterator_WarmSparseDelta_PreservesPages(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	db.testSystemRootWarmMaxDeltaOps = 8

	initial := mustFrozenSystemMemtable(t, systemRangeKVs(2048, nil)...)
	baseRoot, _, _, _, err := db.publishOrderedRootIterator(0, initial.NewIterator(nil, nil), systemRootOrderedPublishOptions(db))
	if err != nil {
		t.Fatalf("initial publish ordered root: %v", err)
	}
	oldPages := collectRootPageIDs(t, db, baseRoot)

	sparse := mustFrozenSystemMemtable(t, systemRangeKVs(2048, map[int]string{
		1024: "value-1024-updated",
	})...)
	newRoot, retired, _, stats, err := db.publishOrderedRootIterator(baseRoot, sparse.NewIterator(nil, nil), systemRootOrderedPublishOptions(db))
	if err != nil {
		t.Fatalf("sparse publish ordered root: %v", err)
	}
	if newRoot == baseRoot {
		t.Fatalf("expected new root id after sparse publish")
	}
	if stats.warmAttempts != 1 {
		t.Fatalf("warmAttempts=%d want 1", stats.warmAttempts)
	}
	if stats.warmNativeApplyAttempts != 1 {
		t.Fatalf("warmNativeApplyAttempts=%d want 1", stats.warmNativeApplyAttempts)
	}
	if stats.warmRebuildFallbacks != 0 {
		t.Fatalf("warmRebuildFallbacks=%d want 0", stats.warmRebuildFallbacks)
	}
	if stats.warmPreservedPages == 0 {
		t.Fatalf("warmPreservedPages=%d want >0", stats.warmPreservedPages)
	}
	if len(retired) >= len(oldPages) {
		t.Fatalf("retired=%d want <%d", len(retired), len(oldPages))
	}
}

func TestPublishOrderedRootIterator_WarmDenseDelta_FallsBackToRebuild(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	db.testSystemRootWarmMaxDeltaOps = 8

	initial := mustFrozenSystemMemtable(t, systemRangeKVs(2048, nil)...)
	baseRoot, _, _, _, err := db.publishOrderedRootIterator(0, initial.NewIterator(nil, nil), systemRootOrderedPublishOptions(db))
	if err != nil {
		t.Fatalf("initial publish ordered root: %v", err)
	}

	denseOverrides := make(map[int]string, 1024)
	for i := 0; i < 1024; i++ {
		denseOverrides[i] = "dense-updated"
	}
	dense := mustFrozenSystemMemtable(t, systemRangeKVs(2048, denseOverrides)...)
	newRoot, retired, _, stats, err := db.publishOrderedRootIterator(baseRoot, dense.NewIterator(nil, nil), systemRootOrderedPublishOptions(db))
	if err != nil {
		t.Fatalf("dense publish ordered root: %v", err)
	}
	if newRoot == 0 {
		t.Fatal("expected non-zero root id")
	}
	if stats.warmAttempts != 1 {
		t.Fatalf("warmAttempts=%d want 1", stats.warmAttempts)
	}
	if stats.warmNativeApplyAttempts != 0 {
		t.Fatalf("warmNativeApplyAttempts=%d want 0", stats.warmNativeApplyAttempts)
	}
	if stats.warmRebuildFallbacks != 1 {
		t.Fatalf("warmRebuildFallbacks=%d want 1", stats.warmRebuildFallbacks)
	}
	if len(retired) == 0 {
		t.Fatal("expected rebuild fallback to retire old pages")
	}
}

func TestPublishOrderedRootIterator_ColdBuild_SkipsWarmCounters(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	initial := mustFrozenSystemMemtable(t, "sys/a", "sv-a")
	newRoot, retired, _, stats, err := db.publishOrderedRootIterator(0, initial.NewIterator(nil, nil), systemRootOrderedPublishOptions(db))
	if err != nil {
		t.Fatalf("cold publish ordered root: %v", err)
	}
	if newRoot == 0 {
		t.Fatal("expected non-zero root id")
	}
	if len(retired) != 0 {
		t.Fatalf("retired=%d want 0", len(retired))
	}
	if stats.warmAttempts != 0 || stats.warmNativeApplyAttempts != 0 || stats.warmRebuildFallbacks != 0 {
		t.Fatalf("unexpected warm stats: %+v", stats)
	}
}

func TestPublishOrderedRootIterator_PersistsAndPreservesMetaRoots(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()

	if err := db.Set([]byte("user/a"), []byte("uv")); err != nil {
		t.Fatalf("set user key: %v", err)
	}
	sys := mustFrozenSystemMemtable(t, "sys/a", "sv")
	if _, err := db.PublishSystemRootIterator(sys.NewIterator(nil, nil)); err != nil {
		t.Fatalf("publish system root: %v", err)
	}
	before := db.State()
	if before == nil {
		t.Fatal("expected backend state")
	}

	rootTable := mustFrozenSystemMemtable(t, "iter/a", "iv")
	newRoot, err := db.PublishOrderedRootIterator(0, rootTable.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish ordered root: %v", err)
	}
	after := db.State()
	if after == nil {
		t.Fatal("expected backend state after publish")
	}
	if after.RootPageID != before.RootPageID {
		t.Fatalf("user root changed: got %d want %d", after.RootPageID, before.RootPageID)
	}
	if after.SystemRootPageID != before.SystemRootPageID {
		t.Fatalf("system root changed: got %d want %d", after.SystemRootPageID, before.SystemRootPageID)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	entry, err := snap.GetEntryAtRoot(newRoot, []byte("iter/a"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(iter): %v", err)
	}
	if got := string(entry.Value); got != "iv" {
		t.Fatalf("iter value=%q want %q", got, "iv")
	}
	_ = snap.Close()

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	closed = true

	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()
	reopenSnap := reopened.AcquireSnapshot()
	if reopenSnap == nil {
		t.Fatal("expected reopen snapshot")
	}
	defer reopenSnap.Close()
	entry, err = reopenSnap.GetEntryAtRoot(newRoot, []byte("iter/a"))
	if err != nil {
		t.Fatalf("reopen GetEntryAtRoot(iter): %v", err)
	}
	if got := string(entry.Value); got != "iv" {
		t.Fatalf("reopen iter value=%q want %q", got, "iv")
	}
}
