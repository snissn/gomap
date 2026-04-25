package db

import (
	"context"
	"reflect"
	"testing"
)

func TestPublishOrderedRootIterator_WarmSparseDelta_PreservesPages(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	db.testSystemRootWarmMaxDeltaOps = 8

	initial := mustFrozenSystemMemtable(t, systemRangeKVs(2048, nil)...)
	baseRoot, _, _, _, _, err := db.publishOrderedRootIterator(0, initial.NewIterator(nil, nil), systemRootOrderedPublishOptions(db), false)
	if err != nil {
		t.Fatalf("initial publish ordered root: %v", err)
	}
	oldPages := collectRootPageIDs(t, db, baseRoot)

	sparse := mustFrozenSystemMemtable(t, systemRangeKVs(2048, map[int]string{
		1024: "value-1024-updated",
	})...)
	newRoot, retired, _, stats, _, err := db.publishOrderedRootIterator(baseRoot, sparse.NewIterator(nil, nil), systemRootOrderedPublishOptions(db), false)
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
	baseRoot, _, _, _, _, err := db.publishOrderedRootIterator(0, initial.NewIterator(nil, nil), systemRootOrderedPublishOptions(db), false)
	if err != nil {
		t.Fatalf("initial publish ordered root: %v", err)
	}

	denseOverrides := make(map[int]string, 1024)
	for i := 0; i < 1024; i++ {
		denseOverrides[i] = "dense-updated"
	}
	dense := mustFrozenSystemMemtable(t, systemRangeKVs(2048, denseOverrides)...)
	newRoot, retired, _, stats, _, err := db.publishOrderedRootIterator(baseRoot, dense.NewIterator(nil, nil), systemRootOrderedPublishOptions(db), false)
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
	newRoot, retired, _, stats, _, err := db.publishOrderedRootIterator(0, initial.NewIterator(nil, nil), systemRootOrderedPublishOptions(db), false)
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

func TestPublishOrderedRootGroup_PersistsSystemAndOrderedRoots(t *testing.T) {
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
	before := db.State()
	if before == nil {
		t.Fatal("expected backend state")
	}

	systemTable := mustFrozenSystemMemtable(t, "sys/a", "sv")
	pointTable := mustFrozenSystemMemtable(t, "root/a", "rv")
	iterTable := mustFrozenSystemMemtable(t, "iter/a", "iv")
	newSystemRoot, rootIDs, err := db.PublishOrderedRootGroup(systemTable.NewIterator(nil, nil), []OrderedRootPublishInput{
		{BaseRoot: 0, Iter: pointTable.NewIterator(nil, nil)},
		{BaseRoot: 0, Iter: iterTable.NewIterator(nil, nil)},
	})
	if err != nil {
		t.Fatalf("publish ordered root group: %v", err)
	}
	if len(rootIDs) != 2 {
		t.Fatalf("rootIDs=%d want 2", len(rootIDs))
	}
	after := db.State()
	if after == nil {
		t.Fatal("expected backend state after publish")
	}
	if after.RootPageID != before.RootPageID {
		t.Fatalf("user root changed: got %d want %d", after.RootPageID, before.RootPageID)
	}
	if after.SystemRootPageID != newSystemRoot {
		t.Fatalf("system root changed: got %d want %d", after.SystemRootPageID, newSystemRoot)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	sysEntry, err := snap.GetEntryAtRoot(newSystemRoot, []byte("sys/a"))
	if err != nil {
		t.Fatalf("GetEntry(sys): %v", err)
	}
	if got := string(sysEntry.Value); got != "sv" {
		t.Fatalf("system value=%q want %q", got, "sv")
	}
	entry, err := snap.GetEntryAtRoot(rootIDs[0], []byte("root/a"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(root): %v", err)
	}
	if got := string(entry.Value); got != "rv" {
		t.Fatalf("root value=%q want %q", got, "rv")
	}
	entry, err = snap.GetEntryAtRoot(rootIDs[1], []byte("iter/a"))
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
	sysEntry, err = reopenSnap.GetEntryAtRoot(newSystemRoot, []byte("sys/a"))
	if err != nil {
		t.Fatalf("reopen GetEntry(sys): %v", err)
	}
	if got := string(sysEntry.Value); got != "sv" {
		t.Fatalf("reopen system value=%q want %q", got, "sv")
	}
	entry, err = reopenSnap.GetEntryAtRoot(rootIDs[0], []byte("root/a"))
	if err != nil {
		t.Fatalf("reopen GetEntryAtRoot(root): %v", err)
	}
	if got := string(entry.Value); got != "rv" {
		t.Fatalf("reopen root value=%q want %q", got, "rv")
	}
	entry, err = reopenSnap.GetEntryAtRoot(rootIDs[1], []byte("iter/a"))
	if err != nil {
		t.Fatalf("reopen GetEntryAtRoot(iter): %v", err)
	}
	if got := string(entry.Value); got != "iv" {
		t.Fatalf("reopen iter value=%q want %q", got, "iv")
	}
}

func TestPublishOrderedRootGroup_SystemWarmApplyUpdatesValueLogRefTrackerInline(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	oldPtr := appendPointersInNewSegment(t, dir, 0, 21, 30_000, 1, func(int) []byte {
		return []byte("old-grouped-system-pointer")
	})[0]
	newPtr := appendPointersInNewSegment(t, dir, 0, 22, 40_000, 1, func(int) []byte {
		return []byte("new-grouped-system-pointer")
	})[0]

	initialSystem := mustFrozenSystemPointerMemtable(t, "sys/p", oldPtr)
	if _, err := db.PublishSystemRootIterator(initialSystem.NewIterator(nil, nil)); err != nil {
		t.Fatalf("publish initial system root: %v", err)
	}

	pointTable := mustFrozenSystemMemtable(t, "root/a", "rv")
	iterTable := mustFrozenSystemMemtable(t, "iter/a", "iv")
	if _, _, err := db.PublishOrderedRootGroup(mustFrozenSystemPointerMemtable(t, "sys/p", newPtr).NewIterator(nil, nil), []OrderedRootPublishInput{
		{BaseRoot: 0, Iter: pointTable.NewIterator(nil, nil)},
		{BaseRoot: 0, Iter: iterTable.NewIterator(nil, nil)},
	}); err != nil {
		t.Fatalf("publish ordered root group: %v", err)
	}

	seq := db.currentCommitSeq()
	incRefs, ok := db.valueLogRefTracker.referencedSet(seq)
	if !ok {
		t.Fatalf("expected incremental ref set for seq=%d", seq)
	}
	if _, ok := incRefs[newPtr.FileID]; !ok {
		t.Fatalf("expected new pointer file %d in ref set", newPtr.FileID)
	}
	if _, ok := incRefs[oldPtr.FileID]; ok {
		t.Fatalf("expected old pointer file %d to be removed", oldPtr.FileID)
	}

	fullCounts, fullSeq, err := db.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatalf("scanValueLogRefCounts: %v", err)
	}
	if fullSeq != seq {
		t.Fatalf("scan seq mismatch: got=%d want=%d", fullSeq, seq)
	}
	fullRefs := valueLogRefSetFromCounts(fullCounts)
	if !reflect.DeepEqual(incRefs, fullRefs) {
		t.Fatalf("incremental/full-scan mismatch: incremental=%v full=%v", incRefs, fullRefs)
	}
}

func TestPublishOrderedRootIterator_NonSystemWarmApplyPreservesValueLogRefTracker(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	livePtr := appendPointersInNewSegment(t, dir, 0, 31, 50_000, 1, func(int) []byte {
		return []byte("live-user-pointer")
	})[0]
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("user/live"), livePtr); err != nil {
		t.Fatalf("SetPointer(user/live): %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write live pointer: %v", err)
	}
	_ = b.Close()

	oldPtr := appendPointersInNewSegment(t, dir, 0, 32, 60_000, 1, func(int) []byte {
		return []byte("old-non-system-pointer")
	})[0]
	newPtr := appendPointersInNewSegment(t, dir, 0, 33, 70_000, 1, func(int) []byte {
		return []byte("new-non-system-pointer")
	})[0]

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemPointerMemtable(t, "root/p", oldPtr).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish initial non-system root: %v", err)
	}
	if _, err := db.referencedValueLogSegments(context.Background()); err != nil {
		t.Fatalf("refresh value-log refs: %v", err)
	}

	newRoot, err := db.PublishOrderedRootIterator(baseRoot, mustFrozenSystemPointerMemtable(t, "root/p", newPtr).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("warm publish non-system root: %v", err)
	}
	if newRoot == baseRoot {
		t.Fatalf("expected warm publish to produce a new root")
	}

	assertValueLogRefTrackerMatchesFullScan(t, db)
}

func TestPublishOrderedRootGroup_NonSystemWarmApplyPreservesValueLogRefTracker(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	livePtr := appendPointersInNewSegment(t, dir, 0, 41, 80_000, 1, func(int) []byte {
		return []byte("group-live-user-pointer")
	})[0]
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("user/live"), livePtr); err != nil {
		t.Fatalf("SetPointer(user/live): %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write live pointer: %v", err)
	}
	_ = b.Close()

	oldPtr := appendPointersInNewSegment(t, dir, 0, 42, 90_000, 1, func(int) []byte {
		return []byte("old-group-non-system-pointer")
	})[0]
	newPtr := appendPointersInNewSegment(t, dir, 0, 43, 100_000, 1, func(int) []byte {
		return []byte("new-group-non-system-pointer")
	})[0]

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemPointerMemtable(t, "root/p", oldPtr).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish initial non-system root: %v", err)
	}
	if _, err := db.referencedValueLogSegments(context.Background()); err != nil {
		t.Fatalf("refresh value-log refs: %v", err)
	}

	_, rootIDs, err := db.PublishOrderedRootGroup(nil, []OrderedRootPublishInput{{
		BaseRoot: baseRoot,
		Iter:     mustFrozenSystemPointerMemtable(t, "root/p", newPtr).NewIterator(nil, nil),
	}})
	if err != nil {
		t.Fatalf("warm publish non-system root group: %v", err)
	}
	if len(rootIDs) != 1 {
		t.Fatalf("rootIDs len=%d want 1", len(rootIDs))
	}
	if rootIDs[0] == baseRoot {
		t.Fatalf("expected grouped warm publish to produce a new root")
	}

	assertValueLogRefTrackerMatchesFullScan(t, db)
}

func assertValueLogRefTrackerMatchesFullScan(t *testing.T, db *DB) {
	t.Helper()
	seq := db.currentCommitSeq()
	incRefs, ok := db.valueLogRefTracker.referencedSet(seq)
	if !ok {
		t.Fatalf("expected incremental ref set for seq=%d", seq)
	}
	fullCounts, fullSeq, err := db.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatalf("scanValueLogRefCounts: %v", err)
	}
	if fullSeq != seq {
		t.Fatalf("scan seq mismatch: got=%d want=%d", fullSeq, seq)
	}
	fullRefs := valueLogRefSetFromCounts(fullCounts)
	if !reflect.DeepEqual(incRefs, fullRefs) {
		t.Fatalf("incremental/full-scan mismatch: incremental=%v full=%v", incRefs, fullRefs)
	}
}
