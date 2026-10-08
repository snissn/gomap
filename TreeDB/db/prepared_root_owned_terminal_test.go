package db

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func scopedSnapshotTerminalFixture(t *testing.T) (*DB, *Snapshot, *valuelog.Manager, uint32, string) {
	t.Helper()
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	dir := t.TempDir()
	id, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")
	writer, err := valuelog.NewWriter(path, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Append(0, nil, 1, []byte("actual retained segment")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	manager, err := valuelog.NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	snapshot := database.AcquireSnapshot()
	if snapshot.vlogPinned {
		if err := snapshot.vlogManager.Release(snapshot.state.ValueLogSet); err != nil {
			t.Fatal(err)
		}
	}
	state := *snapshot.state
	state.ValueLogSet = manager.CurrentSetNoRefresh()
	snapshot.state = &state
	snapshot.vlogManager, snapshot.vlogPinned = manager, true
	return database, snapshot, manager, id, path
}

func TestPreparedOwnedPointOpenSnapshotTerminalKeepsCreatorThroughDrainedRoots(t *testing.T) {
	database, snapshot, manager, id, path := scopedSnapshotTerminalFixture(t)
	account := &scopedPointTestCredit{}
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("doc/1")}}
	first, err := database.captureScopedPreparedOwnedPointRoot(snapshot, 0, OrderedRootStoragePagerLeaves, ops, ownedPointTestLimits(), account)
	if err != nil {
		t.Fatal(err)
	}
	second, err := database.captureScopedPreparedOwnedPointRoot(snapshot, 0, OrderedRootStoragePagerLeaves, ops, ownedPointTestLimits(), account)
	if err != nil {
		t.Fatal(err)
	}
	control := snapshot.ownedPointTerminalRetentions
	if control == nil || control.Count() != 1 {
		t.Fatal("actual Snapshot did not retain its exact segment")
	}
	if err := manager.MarkZombie(id); err != nil {
		t.Fatal(err)
	}
	account.retire()
	if err := database.closeScopedOwnedPointRoot(first, snapshot); err != nil {
		t.Fatal(err)
	}
	if account.closed || snapshot.ownedPointTerminalRetentions != control {
		t.Fatal("creator-first-close discarded terminal control")
	}
	if err := database.closeScopedOwnedPointRoot(second, snapshot); err != nil {
		t.Fatal(err)
	}
	if account.closed || snapshot.ownedPointRegistration != nil || snapshot.ownedPointTerminalRetentions != control {
		t.Fatal("drained roots discarded actual held Snapshot credit")
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if !account.closed || account.refs != 0 || manager.RegisteredFileCountNoRefresh() != 0 {
		t.Fatal("normal-open Snapshot terminal did not fully drain")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("exact zombie was not removed", err)
	}
}

func TestPreparedOwnedPointOpenSnapshotSyncFailureRetainsExactlyOnceCleanup(t *testing.T) {
	database, snapshot, manager, id, path := scopedSnapshotTerminalFixture(t)
	account := &scopedPointTestCredit{}
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("doc/1")}}
	owner, err := database.captureScopedPreparedOwnedPointRoot(snapshot, 0, OrderedRootStoragePagerLeaves, ops, ownedPointTestLimits(), account)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkZombie(id); err != nil {
		t.Fatal(err)
	}
	set := snapshot.state.ValueLogSet
	control := snapshot.ownedPointTerminalRetentions
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("actual Snapshot namespace sync failure")
	original := syncDirFn
	defer func() { syncDirFn = original }()
	calls := 0
	syncDirFn = func(string) error {
		calls++
		if !database.maintenanceMu.TryLock() {
			t.Fatal("terminal IO inherited maintenance gate")
		}
		database.maintenanceMu.Unlock()
		if !database.teardownMu.TryLock() {
			t.Fatal("terminal IO inherited teardown gate")
		}
		database.teardownMu.Unlock()
		if !database.writeMu.TryLock() {
			t.Fatal("terminal IO inherited writer gate")
		}
		database.writeMu.Unlock()
		if !database.commitMu.TryLock() {
			t.Fatal("terminal IO inherited commit gate")
		}
		database.commitMu.Unlock()
		return injected
	}
	notified := false
	database.notifyError = func(error) { notified = true }
	if err := database.closeScopedOwnedPointRoot(owner, snapshot); !errors.Is(err, injected) {
		t.Fatal("actual terminal failure was hidden", err)
	}
	if notified || owner.closed || account.closed || snapshot.db != database || snapshot.ownedPointTerminalRetentions != control || !control.SetReleased() || set.RefCount.Load() != 0 {
		t.Fatal("failed cleanup discarded actual pending owner or replayed its Set")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("fixture did not reach committed physical deletion", err)
	}
	if manager.RegisteredFileCountNoRefresh() != 1 {
		t.Fatal("failed sync silently forgot the retained registration")
	}
	account.retire()
	syncDirFn = func(string) error { calls++; return nil }
	if err := database.closeScopedOwnedPointRoot(owner, snapshot); err != nil {
		t.Fatal(err)
	}
	if !owner.closed || !account.closed || account.refs != 0 || set.RefCount.Load() != 0 || calls != 2 || manager.RegisteredFileCountNoRefresh() != 0 {
		t.Fatal("retry did not finish exact cleanup without Set/COW reconsume")
	}
}

func TestPreparedOwnedPointPublicSnapshotCloseRetriesActualCleanup(t *testing.T) {
	database, snapshot, manager, id, path := scopedSnapshotTerminalFixture(t)
	account := &scopedPointTestCredit{}
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("doc/1")}}
	owner, err := database.captureScopedPreparedOwnedPointRoot(snapshot, 0, OrderedRootStoragePagerLeaves, ops, ownedPointTestLimits(), account)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.closeScopedOwnedPointRoot(owner, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkZombie(id); err != nil {
		t.Fatal(err)
	}
	control := snapshot.ownedPointTerminalRetentions
	set := snapshot.state.ValueLogSet
	account.retire()
	injected := errors.New("public Snapshot Close namespace failure")
	original := syncDirFn
	defer func() { syncDirFn = original }()
	calls := 0
	syncDirFn = func(string) error { calls++; return injected }
	if err := snapshot.Close(); !errors.Is(err, injected) {
		t.Fatal("public Close hid actual cleanup failure", err)
	}
	if snapshot.finalized.Load() || snapshot.db != database || snapshot.ownedPointTerminalRetentions != control || account.closed || set.RefCount.Load() != 0 {
		t.Fatal("failed public Close discarded its actual terminal owner")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("failure did not follow actual unlink", err)
	}
	syncDirFn = func(string) error { calls++; return nil }
	if err := snapshot.Close(); err != nil {
		t.Fatal("public Close did not retry exact cleanup", err)
	}
	if calls != 2 || !snapshot.finalized.Load() || !account.closed || account.refs != 0 || set.RefCount.Load() != 0 || manager.RegisteredFileCountNoRefresh() != 0 {
		t.Fatal("public retry did not finish once without Set replay")
	}
	if err := snapshot.Close(); err != nil || calls != 2 {
		t.Fatal("finalized repeated Close was not idempotent", err)
	}
}

func TestPreparedOwnedPointCloseJoinsOnlyExecutingTerminal(t *testing.T) {
	database, snapshot, manager, id, path := scopedSnapshotActualManagerTerminalFixture(t)
	account := &scopedPointTestCredit{}
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("doc/1")}}
	owner, err := database.captureScopedPreparedOwnedPointRoot(snapshot, 0, OrderedRootStoragePagerLeaves, ops, ownedPointTestLimits(), account)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkZombie(id); err != nil {
		t.Fatal(err)
	}
	if err := database.publishValueLogSetNoRefresh(); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	releaseIO := func() { unblock.Do(func() { close(resume) }) }
	defer releaseIO()
	original := syncDirFn
	defer func() { syncDirFn = original }()
	syncDirFn = func(dir string) error {
		if dir != filepath.Dir(path) {
			return original(dir)
		}
		close(entered)
		<-resume
		return nil
	}
	terminal := make(chan error, 1)
	go func() { terminal <- database.closeScopedOwnedPointRoot(owner, snapshot) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("terminal did not reach actual namespace IO")
	}
	closed := make(chan error, 1)
	go func() { closed <- database.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for !database.closing.Load() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !database.closing.Load() {
		releaseIO()
		t.Fatal("Close never reached the shared admission transition")
	}
	select {
	case err := <-closed:
		releaseIO()
		t.Fatal("Close tore down during admitted terminal IO", err)
	default:
	}
	// Close has changed admission under maintenanceMu, yet waits without that
	// gate. An unresolved retained Snapshot is not part of this invocation join.
	if !database.maintenanceMu.TryLock() {
		releaseIO()
		t.Fatal("Close joined while holding maintenanceMu")
	}
	database.maintenanceMu.Unlock()
	releaseIO()
	select {
	case <-terminal:
	case <-time.After(5 * time.Second):
		t.Fatal("terminal invocation did not leave its join")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close waited for unresolved Snapshot lifetime")
	}
	if !owner.closed {
		if err := database.closeScopedOwnedPointRoot(owner, snapshot); err != nil {
			t.Fatal("checked metadata retry after shutdown", err)
		}
	}
	account.retire()
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if !account.closed || account.refs != 0 {
		t.Fatal("actual control creator survived completed terminal")
	}
}

// This witness uses the actual Manager installed and later saved by DB.Close;
// no independent registrar or synthetic Manager edge substitutes for it.
func scopedSnapshotActualManagerTerminalFixture(t *testing.T) (*DB, *Snapshot, *valuelog.Manager, uint32, string) {
	t.Helper()
	dir := t.TempDir()
	vlogDir := ValueLogDirPath(dir)
	if err := os.MkdirAll(vlogDir, 0755); err != nil {
		t.Fatal(err)
	}
	id, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := valuelog.SegmentPath(vlogDir, id)
	writer, err := valuelog.NewWriter(path, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Append(0, nil, 1, []byte("unreferenced exact installed segment")); err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	manager := database.valueLogManager
	// Bounded recovery intentionally does not discover orphan names. Register
	// the exact fixture segment through the real installed Manager, then publish
	// its actual immutable Set before taking the public Snapshot.
	if err := manager.RegisterSegment(path, id); err != nil {
		t.Fatal(err)
	}
	if err := database.publishValueLogSetNoRefresh(); err != nil {
		t.Fatal(err)
	}
	snapshot := database.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("actual Snapshot capture failed")
	}
	if manager == nil || snapshot.vlogManager != manager || !snapshot.vlogPinned || snapshot.state.ValueLogSet.Files[id] == nil {
		_ = snapshot.Close()
		t.Fatal("fixture did not hold the actual installed Manager/File/Set")
	}
	return database, snapshot, manager, id, path
}

func TestPreparedOwnedPointPublicCloseRetryUsesActualSavedManager(t *testing.T) {
	database, snapshot, manager, id, path := scopedSnapshotActualManagerTerminalFixture(t)
	account := &scopedPointTestCredit{}
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("doc/1")}}
	owner, err := database.captureScopedPreparedOwnedPointRoot(snapshot, 0, OrderedRootStoragePagerLeaves, ops, ownedPointTestLimits(), account)
	if err != nil {
		_ = snapshot.Close()
		t.Fatal(err)
	}
	if err := database.closeScopedOwnedPointRoot(owner, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkZombie(id); err != nil {
		t.Fatal(err)
	}
	// Use the actual DB state publication edge to retire its old Set. The public
	// Snapshot retains that old immutable Set and its exact scalar terminal hold.
	if err := database.publishValueLogSetNoRefresh(); err != nil {
		t.Fatal(err)
	}
	control := snapshot.ownedPointTerminalRetentions
	set := snapshot.state.ValueLogSet
	if control == nil || set.RefCount.Load() != 1 {
		t.Fatal("actual DB state did not transfer the old Set lifetime to Snapshot")
	}
	account.retire()
	injected := errors.New("actual saved Manager namespace sync failure")
	original := syncDirFn
	defer func() { syncDirFn = original }()
	calls := 0
	syncDirFn = func(dir string) error {
		if dir != filepath.Dir(path) {
			return original(dir)
		}
		calls++
		return injected
	}
	if err := snapshot.Close(); !errors.Is(err, injected) {
		t.Fatal("public first Close hid actual Manager cleanup failure", err)
	}
	if snapshot.finalized.Load() || account.closed || snapshot.vlogManager != manager || snapshot.ownedPointTerminalRetentions != control || set.RefCount.Load() != 0 {
		t.Fatal("failed actual public Close lost saved Manager/control or replayed Set")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("first Close did not reach actual unlink", err)
	}
	// Retry the actual public API while the Manager is still open. The exact
	// consumed Set stays consumed and one outstanding namespace sync is retried.
	syncDirFn = func(dir string) error {
		if dir == filepath.Dir(path) {
			calls++
		}
		return original(dir)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal("public actual-Manager retry", err)
	}
	if !snapshot.finalized.Load() || !account.closed || account.refs != 0 || calls != 2 || manager.RegisteredFileCountNoRefresh() != 0 || set.RefCount.Load() != 0 {
		t.Fatal("actual-Manager public retry did not finish once")
	}
	if err := snapshot.Close(); err != nil || calls != 2 {
		t.Fatal("finalized public Close was not idempotent", err)
	}
	// Close must save and close this exact Manager after terminal invocations
	// leave, and must never replace it with a newly created registrar.
	if err := database.Close(); err != nil && !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if database.valueLogManager != nil {
		t.Fatal("DB shutdown did not detach the actual saved Manager")
	}
}

func TestPreparedOwnedPointClosedPublicSnapshotCannotReenterLaterBorrower(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	first := database.AcquireSnapshot()
	if first == nil {
		t.Fatal("first capture failed")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	next := database.AcquireSnapshot()
	if next == nil {
		t.Fatal("next capture failed")
	}
	defer next.Close()
	if first == next {
		t.Fatal("public Snapshot address was reused")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := first.Get([]byte("key")); err == nil && got != nil {
		t.Fatal("closed handle read later borrower")
	}
	if got, err := next.Get([]byte("key")); err != nil || string(got) != "value" {
		t.Fatalf("old Close/read changed live borrower: %q %v", got, err)
	}
	if next.closed.Load() || next.finalized.Load() {
		t.Fatal("old public handle finalized later borrower")
	}
}
