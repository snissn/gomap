package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

const (
	vacuumSnapshotPrimaryKey  = "collections/root/snapshot/primary"
	vacuumSnapshotAliasKey    = "collections/root/snapshot/alias"
	vacuumSnapshotNewAliasKey = "collections/root/snapshot/new-alias"
	vacuumSnapshotOverlayKey  = "collections/root-overlay/snapshot/primary"
	vacuumSnapshotEmptyKey    = "collections/root-overlay/snapshot/empty"
)

func TestVacuumCollectionCatalogPreservesAliasesAndEmptyOverlays(t *testing.T) {
	db, err := Open(vacuumSnapshotTestOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	rootID, err := publishVacuumSnapshotCollectionVersion(db, 0, 0, 64)
	if err != nil {
		t.Fatalf("publish collection: %v", err)
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()

	entries, err := vacuumCollectCollectionEntries(context.Background(), snap)
	if err != nil {
		t.Fatalf("collect catalog: %v", err)
	}
	want := []collectionEntry{
		{key: []byte(vacuumSnapshotEmptyKey), sourceRootIDs: nil},
		{key: []byte(vacuumSnapshotOverlayKey), sourceRootIDs: []uint64{rootID, rootID}},
		{key: []byte(vacuumSnapshotAliasKey), sourceRootIDs: []uint64{rootID}},
		{key: []byte(vacuumSnapshotPrimaryKey), sourceRootIDs: []uint64{rootID}},
	}
	if !reflect.DeepEqual(entries, want) {
		t.Fatalf("catalog=%#v want %#v", entries, want)
	}
}

func TestReconcileCollectionBasisEntriesReusesOnlyAdjacentUnchangedEntries(t *testing.T) {
	old := &collectionBasis{
		token: collectionToken{indexGenerationID: 1, systemRootPageID: 10},
		entries: []collectionEntry{
			{key: []byte("collections/root/a"), sourceRootIDs: []uint64{7}, clonedRootIDs: []uint64{70}},
			{key: []byte("collections/root/alias"), sourceRootIDs: []uint64{7}, clonedRootIDs: []uint64{70}},
			{key: []byte("collections/root-overlay/empty"), sourceRootIDs: nil, clonedRootIDs: nil},
			{key: []byte("collections/root-overlay/mixed"), sourceRootIDs: []uint64{8, 7}, clonedRootIDs: []uint64{80, 70}},
		},
		byKey:        map[string]int{"collections/root/a": 0, "collections/root/alias": 1, "collections/root-overlay/empty": 2, "collections/root-overlay/mixed": 3},
		destRefCount: map[uint64]int{70: 3, 80: 1},
		sourceToDest: map[uint64]uint64{7: 70, 8: 80},
		destPages:    map[uint64][]uint64{70: {70, 71}, 80: {80}},
	}
	successor := []collectionEntry{
		{key: []byte("collections/root/a"), sourceRootIDs: []uint64{7}},
		{key: []byte("collections/root/alias"), sourceRootIDs: []uint64{9}},
		{key: []byte("collections/root/new-alias"), sourceRootIDs: []uint64{9, 9}},
		{key: []byte("collections/root-overlay/empty"), sourceRootIDs: nil},
		{key: []byte("collections/root-overlay/mixed"), sourceRootIDs: []uint64{8, 7}},
	}

	var cloned []uint64
	var reclaimed []uint64
	next, dirty, err := reconcileCollectionBasisEntries(old, successor, collectionToken{indexGenerationID: 1, systemRootPageID: 11}, func(sourceRootID uint64) (uint64, []uint64, error) {
		cloned = append(cloned, sourceRootID)
		return sourceRootID * 10, []uint64{sourceRootID * 10}, nil
	}, func(destRootID uint64, _ []uint64) error {
		reclaimed = append(reclaimed, destRootID)
		return nil
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if dirty != 2 {
		t.Fatalf("dirty=%d want 2", dirty)
	}
	if !reflect.DeepEqual(cloned, []uint64{9}) {
		t.Fatalf("cloned sources=%v want [9]", cloned)
	}
	if len(reclaimed) != 0 {
		t.Fatalf("reclaimed=%v want none", reclaimed)
	}
	if got := next.entries[next.byKey["collections/root/alias"]].clonedRootIDs; !reflect.DeepEqual(got, []uint64{90}) {
		t.Fatalf("changed alias clones=%v want [90]", got)
	}
	if got := next.entries[next.byKey["collections/root/new-alias"]].clonedRootIDs; !reflect.DeepEqual(got, []uint64{90, 90}) {
		t.Fatalf("new aliases clones=%v want [90 90]", got)
	}
	if got := next.entries[next.byKey["collections/root-overlay/mixed"]].clonedRootIDs; !reflect.DeepEqual(got, []uint64{80, 70}) {
		t.Fatalf("unchanged mixed clones=%v want [80 70]", got)
	}

	finalEntries := []collectionEntry{
		{key: []byte("collections/root/alias"), sourceRootIDs: []uint64{9}},
		{key: []byte("collections/root/new-alias"), sourceRootIDs: []uint64{9, 9}},
		{key: []byte("collections/root-overlay/empty"), sourceRootIDs: nil},
	}
	_, dirty, err = reconcileCollectionBasisEntries(next, finalEntries, collectionToken{indexGenerationID: 1, systemRootPageID: 12}, func(sourceRootID uint64) (uint64, []uint64, error) {
		return 0, nil, fmt.Errorf("unexpected clone of %d", sourceRootID)
	}, func(destRootID uint64, _ []uint64) error {
		reclaimed = append(reclaimed, destRootID)
		return nil
	})
	if err != nil {
		t.Fatalf("reconcile drops: %v", err)
	}
	if dirty != 2 {
		t.Fatalf("drop dirty=%d want 2", dirty)
	}
	if !reflect.DeepEqual(reclaimed, []uint64{70, 80}) {
		t.Fatalf("reclaimed=%v want [70 80]", reclaimed)
	}
}

func TestReconcileCollectionBasisEntriesReclonesRootIDAcrossGenerationChange(t *testing.T) {
	old := &collectionBasis{
		token:        collectionToken{indexGenerationID: 1, systemRootPageID: 10},
		entries:      []collectionEntry{{key: []byte(vacuumSnapshotPrimaryKey), sourceRootIDs: []uint64{7}, clonedRootIDs: []uint64{70}}},
		byKey:        map[string]int{vacuumSnapshotPrimaryKey: 0},
		destRefCount: map[uint64]int{70: 1},
		sourceToDest: map[uint64]uint64{7: 70},
		destPages:    map[uint64][]uint64{70: {70}},
	}
	var cloned []uint64
	var reclaimed []uint64
	next, dirty, err := reconcileCollectionBasisEntries(old, []collectionEntry{{
		key:           []byte(vacuumSnapshotPrimaryKey),
		sourceRootIDs: []uint64{7},
	}}, collectionToken{indexGenerationID: 2, systemRootPageID: 10}, func(sourceRootID uint64) (uint64, []uint64, error) {
		cloned = append(cloned, sourceRootID)
		return 170, []uint64{170}, nil
	}, func(destRootID uint64, _ []uint64) error {
		reclaimed = append(reclaimed, destRootID)
		return nil
	})
	if err != nil {
		t.Fatalf("reconcile generation change: %v", err)
	}
	if dirty != 1 {
		t.Fatalf("dirty=%d want 1", dirty)
	}
	if !reflect.DeepEqual(cloned, []uint64{7}) {
		t.Fatalf("cloned=%v want [7]", cloned)
	}
	if !reflect.DeepEqual(reclaimed, []uint64{70}) {
		t.Fatalf("reclaimed=%v want [70]", reclaimed)
	}
	if got := next.entries[0].clonedRootIDs; !reflect.DeepEqual(got, []uint64{170}) {
		t.Fatalf("cloned roots=%v want [170]", got)
	}
}

func TestReconcileCollectionBasisEntriesCleansPartialCloneOnFailure(t *testing.T) {
	old := &collectionBasis{
		token:        collectionToken{indexGenerationID: 1, systemRootPageID: 10},
		entries:      []collectionEntry{{key: []byte(vacuumSnapshotPrimaryKey), sourceRootIDs: []uint64{7}, clonedRootIDs: []uint64{70}}},
		byKey:        map[string]int{vacuumSnapshotPrimaryKey: 0},
		destRefCount: map[uint64]int{70: 1},
		sourceToDest: map[uint64]uint64{7: 70},
		destPages:    map[uint64][]uint64{70: {70}},
	}
	var reclaimed []uint64
	_, _, err := reconcileCollectionBasisEntries(old, []collectionEntry{{
		key:           []byte(vacuumSnapshotPrimaryKey),
		sourceRootIDs: []uint64{8, 9},
	}}, collectionToken{indexGenerationID: 1, systemRootPageID: 11}, func(sourceRootID uint64) (uint64, []uint64, error) {
		if sourceRootID == 9 {
			return 0, nil, errors.New("injected clone failure")
		}
		return 80, []uint64{80, 81}, nil
	}, func(destRootID uint64, _ []uint64) error {
		reclaimed = append(reclaimed, destRootID)
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "injected clone failure") {
		t.Fatalf("reconcile err=%v want injected failure", err)
	}
	if !reflect.DeepEqual(reclaimed, []uint64{80}) {
		t.Fatalf("reclaimed=%v want only partial clone [80]", reclaimed)
	}
}

type vacuumReservedAllocatorTestBase struct {
	next  uint64
	freed []uint64
}

func (a *vacuumReservedAllocatorTestBase) Alloc(uint64) (uint64, error) {
	id := a.next
	a.next++
	return id, nil
}

func (a *vacuumReservedAllocatorTestBase) Free(id uint64) error {
	a.freed = append(a.freed, id)
	return nil
}

func TestVacuumReservedAllocatorUsesAndReclaimsReservedPages(t *testing.T) {
	base := &vacuumReservedAllocatorTestBase{next: 100}
	alloc := newVacuumReservedAllocator(base, 10, 3)

	for i, want := range []uint64{10, 11, 12, 100} {
		got, err := alloc.Alloc(0)
		if err != nil {
			t.Fatalf("alloc %d: %v", i, err)
		}
		if got != want {
			t.Fatalf("alloc %d=%d want %d", i, got, want)
		}
	}
	if err := alloc.ReleaseUnused(); err != nil {
		t.Fatalf("release exhausted reservation: %v", err)
	}
	if len(base.freed) != 0 {
		t.Fatalf("freed exhausted reservation=%v want none", base.freed)
	}

	base = &vacuumReservedAllocatorTestBase{next: 200}
	alloc = newVacuumReservedAllocator(base, 20, 4)
	if got, err := alloc.Alloc(0); err != nil || got != 20 {
		t.Fatalf("reserved alloc=%d err=%v want 20", got, err)
	}
	if err := alloc.ReleaseUnused(); err != nil {
		t.Fatalf("release unused reservation: %v", err)
	}
	if !reflect.DeepEqual(base.freed, []uint64{21, 22, 23}) {
		t.Fatalf("freed=%v want [21 22 23]", base.freed)
	}
}

func TestVacuumIndexOnlinePagerSyncRunsOutsideWriteMu(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	db, err := Open(vacuumSnapshotTestOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := publishVacuumSnapshotCollectionVersion(db, 0, 0, 2048); err != nil {
		t.Fatalf("publish collection: %v", err)
	}
	// The helper publishes through the activated coordinator. Drain that exact
	// root before invoking production VacuumIndexOnline so an outstanding stable
	// index pin cannot race the RecoverableRootSet-fenced replacement below.
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint collection: %v", err)
	}

	var preflushCalls, finalCalls int
	var hookErr error
	db.vacuumPagerSyncHook = func(phase vacuumPagerSyncPhase) {
		writeMuAvailable := db.writeMu.TryRLock()
		if writeMuAvailable {
			db.writeMu.RUnlock()
		}
		switch phase {
		case vacuumPagerSyncPrecutover:
			preflushCalls++
			if !writeMuAvailable && hookErr == nil {
				hookErr = errors.New("precutover pager sync ran under writeMu")
			}
		case vacuumPagerSyncFinal:
			finalCalls++
			if !writeMuAvailable && hookErr == nil {
				hookErr = errors.New("final pager sync ran under writeMu")
			}
		default:
			if hookErr == nil {
				hookErr = fmt.Errorf("unexpected pager sync phase %d", phase)
			}
		}
	}

	if err := db.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	if preflushCalls == 0 || finalCalls != 1 {
		t.Fatalf("pager sync calls preflush=%d final=%d want preflush>0 final=1", preflushCalls, finalCalls)
	}
	stats := db.vacuumOnlineStatsSnapshot()
	if stats.PrecloneTraversalPages <= 1 {
		t.Fatalf("preclone traversal pages=%d want multi-page clone", stats.PrecloneTraversalPages)
	}
	if stats.CutoverCloneTraversalPages != 0 {
		t.Fatalf("cutover clone traversal pages=%d want 0", stats.CutoverCloneTraversalPages)
	}
}

func TestVacuumIndexOnlineFinalSyncGateWaitsWriterAndPublishesItAfterCutover(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	dir := t.TempDir()
	opts := Options{Dir: dir, ChunkSize: 64 << 10}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.SetSync([]byte("before-cutover"), []byte("o")); err != nil {
		_ = db.Close()
		t.Fatalf("seed: %v", err)
	}

	reached := make(chan struct{})
	release := make(chan struct{})
	initialWriterReady := make(chan struct{})
	releaseInitialHold := make(chan struct{})
	initialHoldStarted := make(chan time.Time, 1)
	writeErr := make(chan error, 1)
	db.vacuumAfterCutoverLockHook = func() {
		initialHoldStarted <- time.Now()
		go func() {
			close(initialWriterReady)
			writeErr <- db.SetSync([]byte("during-final-sync"), []byte("n"))
		}()
		<-initialWriterReady
		<-releaseInitialHold
	}
	defer func() { db.vacuumAfterCutoverLockHook = nil }()
	var once sync.Once
	db.vacuumPagerSyncHook = func(phase vacuumPagerSyncPhase) {
		if phase == vacuumPagerSyncFinal {
			once.Do(func() {
				close(reached)
				<-release
			})
		}
	}
	vacuumErr := make(chan error, 1)
	go func() { vacuumErr <- db.VacuumIndexOnline(context.Background()) }()
	waitVacuumTestSignal(t, initialWriterReady, "writer blocked by cutover write lock")
	initialStartedAt := <-initialHoldStarted
	select {
	case err := <-writeErr:
		close(releaseInitialHold)
		_ = db.Close()
		t.Fatalf("writer passed initial cutover hold: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	initialHoldFor := time.Since(initialStartedAt)
	close(releaseInitialHold)
	waitVacuumTestSignal(t, reached, "vacuum final sync")
	finalSyncBlockedAt := time.Now()

	select {
	case err := <-writeErr:
		close(release)
		<-vacuumErr
		_ = db.Close()
		t.Fatalf("writer passed active cutover gate: %v", err)
	case <-time.After(250 * time.Millisecond):
	}
	finalSyncBlockedFor := time.Since(finalSyncBlockedAt)
	close(release)
	if err := <-vacuumErr; err != nil {
		_ = db.Close()
		t.Fatalf("vacuum: %v", err)
	}
	if err := <-writeErr; err != nil {
		_ = db.Close()
		t.Fatalf("writer after cutover: %v", err)
	}
	if stats := db.VacuumOnlineStats(); stats.MaxWriterPause < initialHoldFor+finalSyncBlockedFor {
		_ = db.Close()
		t.Fatalf("max writer pause=%s want initial+final blocked interval at least %s", stats.MaxWriterPause, initialHoldFor+finalSyncBlockedFor)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	value, err := reopened.Get([]byte("during-final-sync"))
	if err != nil {
		t.Fatalf("get writer value after reopen: %v", err)
	}
	if string(value) != "n" {
		t.Fatalf("writer value after reopen=%q, want n", value)
	}
}

func TestVacuumIndexOnlinePreflushFailureLeavesOldIndexAuthoritative(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	dir := t.TempDir()
	opts := vacuumSnapshotTestOptions(dir)
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	rootID, err := publishVacuumSnapshotCollectionVersion(db, 0, 0, 2048)
	if err != nil {
		_ = db.Close()
		t.Fatalf("publish collection: %v", err)
	}

	preflushErr := errors.New("injected preflush failure")
	var calls atomic.Int32
	db.vacuumPreflushHook = func() error {
		calls.Add(1)
		return preflushErr
	}
	if err := db.VacuumIndexOnline(context.Background()); !errors.Is(err, preflushErr) {
		_ = db.Close()
		t.Fatalf("vacuum error=%v, want %v", err, preflushErr)
	}
	if got := calls.Load(); got != 1 {
		_ = db.Close()
		t.Fatalf("preflush failpoint calls=%d, want 1", got)
	}
	for _, name := range []string{indexNewFileName, indexReadyFileName} {
		if _, statErr := os.Stat(filepath.Join(dir, name)); !errors.Is(statErr, os.ErrNotExist) {
			_ = db.Close()
			t.Fatalf("artifact %s remains after preflush failure: %v", name, statErr)
		}
	}
	verifyVacuumSnapshotCollectionRootUnchanged(t, db, rootID)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	verifyVacuumSnapshotCollectionRootUnchanged(t, reopened, rootID)
}

func TestVacuumIndexOnlineCollectionPrecloneAllowsMutationAndReopens(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	dir := t.TempDir()
	opts := vacuumSnapshotTestOptions(dir)
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	rootID, err := publishVacuumSnapshotCollectionVersion(db, 0, 0, 512)
	if err != nil {
		t.Fatalf("publish initial collection: %v", err)
	}

	reached := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	db.vacuumCollectionClonePageHook = func(phase vacuumCollectionClonePhase, _ uint64) {
		if phase != vacuumCollectionClonePreclone {
			return
		}
		once.Do(func() {
			close(reached)
			<-release
		})
	}

	vacuumErr := make(chan error, 1)
	go func() { vacuumErr <- db.VacuumIndexOnline(context.Background()) }()
	waitVacuumTestSignal(t, reached, "collection preclone")

	publishDone := make(chan error, 1)
	var nextRoot atomic.Uint64
	go func() {
		root, publishErr := publishVacuumSnapshotCollectionVersion(db, rootID, 1, 0)
		nextRoot.Store(root)
		publishDone <- publishErr
	}()
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("publish during preclone: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("collection publish blocked while preclone was paused")
	}
	close(release)
	if err := <-vacuumErr; err != nil {
		t.Fatalf("vacuum: %v", err)
	}

	stats := db.vacuumOnlineStatsSnapshot()
	if stats.PrecloneTraversalPages == 0 {
		t.Fatal("preclone traversal pages=0")
	}
	if stats.RecloneTraversalPages == 0 {
		t.Fatal("reclone traversal pages=0 after concurrent collection publish")
	}
	if stats.CutoverCloneTraversalPages != 0 {
		t.Fatalf("cutover clone traversal pages=%d want 0", stats.CutoverCloneTraversalPages)
	}
	if stats.DirtyDescriptors == 0 || stats.DeferredCutovers == 0 {
		t.Fatalf("dirty=%d defers=%d want both positive", stats.DirtyDescriptors, stats.DeferredCutovers)
	}
	verifyVacuumSnapshotCollectionVersion(t, db, nextRoot.Load(), 1)

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	verifyVacuumSnapshotCollectionVersion(t, reopened, 0, 1)
}

func TestVacuumIndexOnlineCollectionRecloneAllowsMutation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	db, err := Open(vacuumSnapshotTestOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	rootID, err := publishVacuumSnapshotCollectionVersion(db, 0, 0, 256)
	if err != nil {
		t.Fatalf("publish initial collection: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint initial collection: %v", err)
	}

	var publishOnce sync.Once
	var hookErr error
	db.vacuumBeforeCutoverHook = func(_ int) {
		publishOnce.Do(func() {
			rootID, hookErr = publishVacuumSnapshotCollectionVersion(db, rootID, 1, 0)
		})
	}
	reached := make(chan struct{})
	release := make(chan struct{})
	var cloneOnce sync.Once
	db.vacuumCollectionClonePageHook = func(phase vacuumCollectionClonePhase, _ uint64) {
		if phase == vacuumCollectionCloneReclone {
			cloneOnce.Do(func() {
				close(reached)
				<-release
			})
		}
	}

	vacuumErr := make(chan error, 1)
	go func() { vacuumErr <- db.VacuumIndexOnline(context.Background()) }()
	waitVacuumTestSignal(t, reached, "collection reclone")
	publishDone := make(chan error, 1)
	go func() {
		var publishErr error
		rootID, publishErr = publishVacuumSnapshotCollectionVersion(db, rootID, 2, 0)
		publishDone <- publishErr
	}()
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("publish during reclone: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("collection publish blocked while reclone was paused")
	}
	close(release)
	if err := <-vacuumErr; err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if hookErr != nil {
		t.Fatalf("first cutover publish: %v", hookErr)
	}
	stats := db.vacuumOnlineStatsSnapshot()
	if stats.DeferredCutovers < 2 || stats.RecloneTraversalPages == 0 {
		t.Fatalf("defers=%d reclone-pages=%d want at least two defers and reclone work", stats.DeferredCutovers, stats.RecloneTraversalPages)
	}
	if stats.CutoverCloneTraversalPages != 0 {
		t.Fatalf("cutover clone traversal pages=%d want 0", stats.CutoverCloneTraversalPages)
	}
	verifyVacuumSnapshotCollectionVersion(t, db, rootID, 2)
}

func TestVacuumIndexOnlineDefersRangeOnlyTailOverLimit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	db, err := Open(vacuumSnapshotTestOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "user/present", "value").NewIterator(nil, nil)); err != nil {
		t.Fatalf("publish user root: %v", err)
	}
	// Settle the activated setup publication before the legacy vacuum seam
	// directly replaces its stable index identity.
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint user root: %v", err)
	}

	var once sync.Once
	db.vacuumBeforeCutoverHook = func(_ int) {
		once.Do(func() {
			ranges := make([]batchpkg.DeleteRange, vacuumCutoverMaxKeys+1)
			for i := range ranges {
				start := []byte(fmt.Sprintf("absent/%06d", i))
				ranges[i] = batchpkg.DeleteRange{Start: start, End: append(append([]byte(nil), start...), 0)}
			}
			db.vacuum.RecordApplyPlan(nil, ranges)
		})
	}
	if err := db.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	stats := db.vacuumOnlineStatsSnapshot()
	if stats.DeferredCutovers != 1 {
		t.Fatalf("deferred cutovers=%d want 1", stats.DeferredCutovers)
	}
	if stats.UserTailMutations != vacuumCutoverMaxKeys+1 {
		t.Fatalf("tail mutations=%d want %d", stats.UserTailMutations, vacuumCutoverMaxKeys+1)
	}
}

func TestVacuumIndexOnlineReplaysProductionRangeMutation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	dir := t.TempDir()
	opts := Options{Dir: dir, DisableBackgroundPrune: true}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	seed := db.NewBatch()
	for i := 0; i < 32; i++ {
		key := []byte(fmt.Sprintf("range/%03d", i))
		if err := seed.Set(key, append([]byte("value/"), key...)); err != nil {
			t.Fatalf("seed set %d: %v", i, err)
		}
	}
	if err := seed.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = seed.Close()

	var once sync.Once
	var rangeErr error
	db.vacuumBeforeCutoverHook = func(_ int) {
		once.Do(func() {
			mutation := db.NewBatch()
			defer func() { _ = mutation.Close() }()
			if err := mutation.DeleteRange([]byte("range/008"), []byte("range/024")); err != nil {
				rangeErr = err
				return
			}
			rangeErr = mutation.Write()
		})
	}
	if err := db.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if rangeErr != nil {
		t.Fatalf("range mutation: %v", rangeErr)
	}
	stats := db.vacuumOnlineStatsSnapshot()
	if stats.UserTailMutations != 1 || stats.UserTailPointMutations != 0 || stats.UserTailRangeMutations != 1 {
		t.Fatalf("tail mutations=%d points=%d ranges=%d want 1/0/1 production range", stats.UserTailMutations, stats.UserTailPointMutations, stats.UserTailRangeMutations)
	}
	assertVacuumProductionRangeState(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	assertVacuumProductionRangeState(t, reopened)
}

func assertVacuumProductionRangeState(t *testing.T, db *DB) {
	t.Helper()
	for i := 0; i < 32; i++ {
		key := []byte(fmt.Sprintf("range/%03d", i))
		if i >= 8 && i < 24 {
			has, err := db.Has(key)
			if err != nil {
				t.Fatalf("has deleted key %q: %v", key, err)
			}
			if has {
				t.Fatalf("deleted key %q remains", key)
			}
			continue
		}
		value, err := db.Get(key)
		want := append([]byte("value/"), key...)
		if err != nil || !bytes.Equal(value, want) {
			t.Fatalf("key %q=(%q,%v) want %q", key, value, err, want)
		}
	}
}

func TestVacuumIndexOnlineCollectionChurnExhaustsRetriesBeforeRename(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	dir := t.TempDir()
	opts := vacuumSnapshotTestOptions(dir)
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	rootID, err := publishVacuumSnapshotCollectionVersion(db, 0, 0, 256)
	if err != nil {
		t.Fatalf("publish initial collection: %v", err)
	}

	var hookErr error
	var version int
	db.vacuumBeforeCutoverHook = func(_ int) {
		if hookErr != nil {
			return
		}
		version++
		rootID, hookErr = publishVacuumSnapshotCollectionVersion(db, rootID, version, 0)
	}
	err = db.VacuumIndexOnline(context.Background())
	if !errors.Is(err, ErrVacuumConcurrentMutation) {
		t.Fatalf("vacuum err=%v want %v", err, ErrVacuumConcurrentMutation)
	}
	if hookErr != nil {
		t.Fatalf("cutover mutation: %v", hookErr)
	}
	stats := db.vacuumOnlineStatsSnapshot()
	if stats.DeferredCutovers != vacuumCutoverMaxDefers {
		t.Fatalf("deferred cutovers=%d want %d", stats.DeferredCutovers, vacuumCutoverMaxDefers)
	}
	if stats.ConcurrentMutationAborts != 1 {
		t.Fatalf("concurrent mutation aborts=%d want 1", stats.ConcurrentMutationAborts)
	}
	for _, name := range []string{indexNewFileName, indexReadyFileName} {
		if _, statErr := os.Stat(filepath.Join(db.dir, name)); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("artifact %s remains after abort: %v", name, statErr)
		}
	}
	verifyVacuumSnapshotCollectionVersion(t, db, 0, version)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen old authoritative DB: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	verifyVacuumSnapshotCollectionVersion(t, reopened, 0, version)
}

func TestVacuumIndexOnlineCollectionCatalogTransitionsAreExact(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	dir := t.TempDir()
	opts := vacuumSnapshotTestOptions(dir)
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	rootID, err := publishVacuumSnapshotCollectionVersion(db, 0, 0, 128)
	if err != nil {
		t.Fatalf("publish initial collection: %v", err)
	}

	var once sync.Once
	var publishErr error
	db.vacuumBeforeCutoverHook = func(_ int) {
		once.Do(func() {
			rootID, publishErr = publishVacuumSnapshotCollectionTransition(db, rootID, 1)
		})
	}
	if err := db.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if publishErr != nil {
		t.Fatalf("publish transition: %v", publishErr)
	}
	assertVacuumSnapshotTransitionCatalog(t, db, 1)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	assertVacuumSnapshotTransitionCatalog(t, reopened, 1)
}

func TestPublishMalformedCollectionDescriptorAbortsBeforeVacuumArtifacts(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	dir := t.TempDir()
	opts := vacuumSnapshotTestOptions(dir)
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	malformed := []byte{1, 2, 3}
	beforeSeq := db.currentCommitSeq()
	if _, err := db.PublishSystemRootIterator(mustFrozenSystemMemtable(t, vacuumSnapshotPrimaryKey, string(malformed)).NewIterator(nil, nil)); err == nil {
		t.Fatal("published malformed collection descriptor")
	}
	for _, name := range []string{indexNewFileName, indexReadyFileName} {
		if _, statErr := os.Stat(filepath.Join(dir, name)); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("artifact %s remains after malformed abort: %v", name, statErr)
		}
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil after rejected publication")
	}
	if snap.state.CommitSeq != beforeSeq {
		t.Fatalf("commit sequence advanced after rejected descriptor: got=%d want=%d", snap.state.CommitSeq, beforeSeq)
	}
	if got, err := snap.GetAtRoot(snap.state.SystemRootPageID, []byte(vacuumSnapshotPrimaryKey)); err == nil {
		t.Fatalf("rejected malformed descriptor became visible: %x", got)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestVacuumIndexOnlineSerializesCloseThroughMaintenance(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	opts := vacuumSnapshotTestOptions(t.TempDir())
	opts.CommandWAL = true
	opts.IndexOuterLeavesInValueLog = true
	opts.ValueLog = ValueLogOptions{}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	appender, ok := db.currentValueLogAppender().(*replayInlineAppender)
	if !ok || appender == nil {
		t.Fatalf("production value-log appender=%T, want *replayInlineAppender", db.currentValueLogAppender())
	}
	for i := 0; i < 256; i++ {
		if err := db.Set([]byte(fmt.Sprintf("close/%06d", i)), bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 8)); err != nil {
			t.Fatalf("seed key %d: %v", i, err)
		}
	}

	reached := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	db.vacuumPagerSyncHook = func(phase vacuumPagerSyncPhase) {
		if phase == vacuumPagerSyncPrecutover {
			once.Do(func() {
				close(reached)
				<-release
			})
		}
	}
	vacuumErr := make(chan error, 1)
	go func() { vacuumErr <- db.VacuumIndexOnline(context.Background()) }()
	waitVacuumTestSignal(t, reached, "vacuum precutover sync")

	closeHookRan := make(chan struct{})
	db.RegisterCloseHook(func() error {
		close(closeHookRan)
		return nil
	})
	closeStarted := make(chan struct{})
	closeErr := make(chan error, 1)
	go func() {
		close(closeStarted)
		closeErr <- db.Close()
	}()
	waitVacuumTestSignal(t, closeStarted, "Close start")

	waitVacuumTestSignal(t, closeHookRan, "user close hook outside maintenance")
	select {
	case err := <-closeErr:
		close(release)
		<-vacuumErr
		t.Fatalf("Close returned while vacuum held maintenanceMu: %v", err)
	case <-time.After(250 * time.Millisecond):
	}
	if got := db.currentValueLogAppender(); got != appender {
		close(release)
		<-vacuumErr
		<-closeErr
		t.Fatalf("production value-log appender changed during maintenance: got %T (%p), want %p", got, got, appender)
	}
	appender.mu.Lock()
	writerOpen := appender.writer != nil
	appender.mu.Unlock()
	if !writerOpen || db.leafPageLog == nil {
		close(release)
		<-vacuumErr
		<-closeErr
		t.Fatalf("production outer-leaf appender torn down during maintenance: writerOpen=%t leafPageLog=%T", writerOpen, db.leafPageLog)
	}

	close(release)
	if err := <-vacuumErr; err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if err := <-closeErr; err != nil {
		t.Fatalf("close: %v", err)
	}
	appender.mu.Lock()
	writerOpen = appender.writer != nil
	appender.mu.Unlock()
	if writerOpen || db.currentValueLogAppender() != nil {
		t.Fatalf("production value-log appender remained open after Close: writerOpen=%t appender=%T", writerOpen, db.currentValueLogAppender())
	}
}

func TestCollectionPublicationAdvancesSystemRootPublishEpoch(t *testing.T) {
	db, err := Open(vacuumSnapshotTestOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	before := db.systemRootPublishEpoch.Load()
	rootID, err := publishVacuumSnapshotCollectionVersion(db, 0, 0, 8)
	if err != nil {
		t.Fatalf("publish collection: %v", err)
	}
	after := db.systemRootPublishEpoch.Load()
	if after <= before {
		t.Fatalf("publish epoch before=%d after=%d", before, after)
	}

	if _, err := db.PublishOrderedRootIterator(rootID, mustFrozenSystemMemtable(t, "doc/epoch", "user-only").NewIterator(nil, nil)); err != nil {
		t.Fatalf("publish detached root: %v", err)
	}
	if got := db.systemRootPublishEpoch.Load(); got != after {
		t.Fatalf("detached root publish advanced system epoch: got %d want %d", got, after)
	}
}

func TestCollectionSnapshotTokenRetainsPublishedEpoch(t *testing.T) {
	db, err := Open(vacuumSnapshotTestOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	rootID, err := publishVacuumSnapshotCollectionVersion(db, 0, 0, 8)
	if err != nil {
		t.Fatalf("publish initial collection: %v", err)
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	wantEpoch := db.systemRootPublishEpoch.Load()

	if _, err := publishVacuumSnapshotCollectionVersion(db, rootID, 1, 0); err != nil {
		t.Fatalf("publish successor collection: %v", err)
	}
	if current := db.systemRootPublishEpoch.Load(); current == wantEpoch {
		t.Fatalf("successor publication did not advance epoch %d", current)
	}

	token, err := db.collectionTokenForSnapshot(snap)
	if err != nil {
		t.Fatalf("snapshot token: %v", err)
	}
	if token.publishEpoch != wantEpoch {
		t.Fatalf("snapshot token epoch=%d want captured epoch %d", token.publishEpoch, wantEpoch)
	}
}

func vacuumSnapshotTestOptions(dir string) Options {
	return Options{
		Dir:       dir,
		ChunkSize: 64 << 10,
		ValueLog: ValueLogOptions{
			ForcePointers:    true,
			PointerThreshold: 1,
		},
	}
}

func publishVacuumSnapshotCollectionVersion(db *DB, baseRoot uint64, version, initialEntries int) (uint64, error) {
	if baseRoot != 0 {
		delta := batchpkg.New(nil, vacuumInlineThresholdMax)
		defer func() { _ = delta.Close() }()
		if err := delta.Set([]byte("doc/000000"), []byte(fmt.Sprintf("updated-%d", version))); err != nil {
			return 0, err
		}
		if err := delta.Set([]byte(fmt.Sprintf("doc/inserted-%06d", version)), []byte(fmt.Sprintf("inserted-%d", version))); err != nil {
			return 0, err
		}
		if err := delta.Delete([]byte("doc/000001")); err != nil {
			return 0, err
		}
		_, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
			BaseRoot:      baseRoot,
			Delta:         delta,
			StoragePolicy: OrderedRootStoragePagerLeaves,
		}}, vacuumSnapshotCatalogBuilder)
		if err != nil {
			return 0, err
		}
		return rootIDs[0], nil
	}

	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		return 0, err
	}
	if baseRoot == 0 {
		for i := 0; i < initialEntries; i++ {
			mt.Set([]byte(fmt.Sprintf("doc/%06d", i)), bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 64))
		}
	}
	mt.Freeze()

	_, rootIDs, err := db.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot:      baseRoot,
		Iter:          mt.NewIterator(nil, nil),
		StoragePolicy: OrderedRootStoragePagerLeaves,
	}}, vacuumSnapshotCatalogBuilder)
	if err != nil {
		return 0, err
	}
	return rootIDs[0], nil
}

func publishVacuumSnapshotCollectionTransition(db *DB, baseRoot uint64, version int) (uint64, error) {
	delta := batchpkg.New(nil, vacuumInlineThresholdMax)
	defer func() { _ = delta.Close() }()
	if err := delta.Set([]byte("doc/000000"), []byte(fmt.Sprintf("updated-%d", version))); err != nil {
		return 0, err
	}
	if err := delta.Set([]byte(fmt.Sprintf("doc/inserted-%06d", version)), []byte(fmt.Sprintf("inserted-%d", version))); err != nil {
		return 0, err
	}
	if err := delta.Delete([]byte("doc/000001")); err != nil {
		return 0, err
	}
	_, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
		BaseRoot:      baseRoot,
		Delta:         delta,
		StoragePolicy: OrderedRootStoragePagerLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 || rootIDs[0] == 0 {
			return nil, fmt.Errorf("unexpected collection roots %v", rootIDs)
		}
		catalog, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
		if err != nil {
			return nil, err
		}
		encoded := encodeCollectionRootDescriptorRootID(rootIDs[0])
		catalog.Set([]byte(vacuumSnapshotPrimaryKey), encoded)
		catalog.Delete([]byte(vacuumSnapshotAliasKey))
		catalog.Set([]byte(vacuumSnapshotNewAliasKey), encoded)
		catalog.Set([]byte(vacuumSnapshotOverlayKey), nil)
		catalog.Set([]byte(vacuumSnapshotEmptyKey), encoded)
		catalog.Freeze()
		return catalog.NewIterator(nil, nil), nil
	})
	if err != nil {
		return 0, err
	}
	return rootIDs[0], nil
}

func vacuumSnapshotCatalogBuilder(rootIDs []uint64) (iterator.UnsafeIterator, error) {
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		return nil, fmt.Errorf("unexpected collection roots %v", rootIDs)
	}
	catalog, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		return nil, err
	}
	encoded := encodeCollectionRootDescriptorRootID(rootIDs[0])
	catalog.Set([]byte(vacuumSnapshotPrimaryKey), encoded)
	catalog.Set([]byte(vacuumSnapshotAliasKey), encoded)
	catalog.Set([]byte(vacuumSnapshotOverlayKey), encodeCollectionRootDescriptorRootIDs([]uint64{rootIDs[0], rootIDs[0]}))
	catalog.Set([]byte(vacuumSnapshotEmptyKey), nil)
	catalog.Freeze()
	return catalog.NewIterator(nil, nil), nil
}

func verifyVacuumSnapshotCollectionVersion(t *testing.T, db *DB, expectedRoot uint64, version int) {
	t.Helper()
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	encoded, err := snap.GetAtRoot(snap.state.SystemRootPageID, []byte(vacuumSnapshotPrimaryKey))
	if err != nil {
		t.Fatalf("read primary descriptor: %v", err)
	}
	rootIDs, err := decodeCollectionRootDescriptorRootIDs([]byte(vacuumSnapshotPrimaryKey), encoded, false)
	if err != nil || len(rootIDs) != 1 {
		t.Fatalf("decode primary descriptor roots=%v err=%v", rootIDs, err)
	}
	rootID := rootIDs[0]
	if expectedRoot != 0 && rootID == expectedRoot {
		t.Fatalf("vacuum did not rewrite collection root: root=%d", rootID)
	}
	entry, err := snap.GetEntryAtRoot(rootID, []byte("doc/000000"))
	if err != nil || string(entry.Value) != fmt.Sprintf("updated-%d", version) {
		t.Fatalf("updated entry=%q err=%v", entry.Value, err)
	}
	entry, err = snap.GetEntryAtRoot(rootID, []byte(fmt.Sprintf("doc/inserted-%06d", version)))
	if err != nil || string(entry.Value) != fmt.Sprintf("inserted-%d", version) {
		t.Fatalf("inserted entry=%q err=%v", entry.Value, err)
	}
	if _, err := snap.GetEntryAtRoot(rootID, []byte("doc/000001")); err == nil {
		t.Fatal("deleted collection entry remains")
	}
	empty, err := snap.GetAtRoot(snap.state.SystemRootPageID, []byte(vacuumSnapshotEmptyKey))
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty overlay=%x err=%v", empty, err)
	}
}

func verifyVacuumSnapshotCollectionRootUnchanged(t *testing.T, db *DB, expectedRoot uint64) {
	t.Helper()
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	encoded, err := snap.GetAtRoot(snap.state.SystemRootPageID, []byte(vacuumSnapshotPrimaryKey))
	if err != nil {
		t.Fatalf("read primary descriptor: %v", err)
	}
	rootIDs, err := decodeCollectionRootDescriptorRootIDs([]byte(vacuumSnapshotPrimaryKey), encoded, false)
	if err != nil || len(rootIDs) != 1 || rootIDs[0] != expectedRoot {
		t.Fatalf("primary descriptor roots=%v err=%v, want [%d]", rootIDs, err, expectedRoot)
	}
	entry, err := snap.GetEntryAtRoot(expectedRoot, []byte("doc/000000"))
	if err != nil || len(entry.Value) == 0 {
		t.Fatalf("original collection entry len=%d err=%v", len(entry.Value), err)
	}
}

func assertVacuumSnapshotTransitionCatalog(t *testing.T, db *DB, version int) {
	t.Helper()
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	entries, err := vacuumCollectCollectionEntries(context.Background(), snap)
	if err != nil {
		t.Fatalf("collect transition catalog: %v", err)
	}
	if len(entries) != 4 {
		t.Fatalf("transition catalog entries=%#v want 4", entries)
	}
	byKey := make(map[string]collectionEntry, len(entries))
	for _, entry := range entries {
		byKey[string(entry.key)] = entry
	}
	if _, exists := byKey[vacuumSnapshotAliasKey]; exists {
		t.Fatalf("dropped alias %q remains", vacuumSnapshotAliasKey)
	}
	if roots := byKey[vacuumSnapshotOverlayKey].sourceRootIDs; len(roots) != 0 {
		t.Fatalf("overlay transition roots=%v want empty", roots)
	}
	primary := byKey[vacuumSnapshotPrimaryKey].sourceRootIDs
	newAlias := byKey[vacuumSnapshotNewAliasKey].sourceRootIDs
	formerEmpty := byKey[vacuumSnapshotEmptyKey].sourceRootIDs
	if len(primary) != 1 || !reflect.DeepEqual(primary, newAlias) || !reflect.DeepEqual(primary, formerEmpty) {
		t.Fatalf("transition roots primary=%v alias=%v former-empty=%v", primary, newAlias, formerEmpty)
	}
	entry, err := snap.GetEntryAtRoot(primary[0], []byte("doc/000000"))
	if err != nil || string(entry.Value) != fmt.Sprintf("updated-%d", version) {
		t.Fatalf("transition value=%q err=%v", entry.Value, err)
	}
}

func waitVacuumTestSignal(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}
