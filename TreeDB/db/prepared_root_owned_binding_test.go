package db

import (
	"bytes"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"reflect"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
)

func assertScopedOwnedPointDetached(t *testing.T, o *PreparedOwnedPointRoot) {
	t.Helper()
	if o.snapshot != nil || !o.scopedBinding || o.registration == nil {
		t.Fatal("scoped root retained broad snapshot or lost its terminal registration")
	}
	for _, item := range []struct {
		value  any
		fields []string
	}{
		{o.zipper, []string{"pager", "allocator", "leafPageLog", "parallelMergePressure"}},
		{o.workspace, []string{"pager", "writer"}},
	} {
		v := reflect.ValueOf(item.value).Elem()
		for _, name := range item.fields {
			if !v.FieldByName(name).IsNil() {
				t.Fatalf("retained operational edge %T.%s", item.value, name)
			}
		}
	}
}

func TestPreparedOwnedPointScopedBindingsAndSingleApply(t *testing.T) {
	database := openOrderedRootSpanNativeTestDB(t, t.TempDir(), false, 0)
	defer database.Close()
	base := newOrderedRootSpanNativeBatch(t, 1024, "scoped-base")
	root := publishOrderedRootSpanNativeBatch(t, database, 0, base, OrderedRootStoragePagerLeaves)
	base.Close()
	snapshot := database.AcquireSnapshot()
	defer snapshot.Close()
	ops := []batch.Entry{{Type: batch.OpDelete, Key: []byte("key-000030")}, {Type: batch.OpPut, Key: []byte("key-000031"), Value: []byte("replacement")}}
	account := &scopedPointTestCredit{}
	owner, err := database.captureScopedPreparedOwnedPointRoot(snapshot, root, OrderedRootStoragePagerLeaves, ops, ownedPointTestLimits(), account)
	if err != nil {
		t.Fatal(err)
	}
	defer database.closeScopedOwnedPointRoot(owner, snapshot)
	assertScopedOwnedPointDetached(t, owner)
	if account.bytes != owner.BackingBytes() {
		t.Fatalf("class ledger=%d actual admission=%d", owner.BackingBytes(), account.bytes)
	}
	if _, err := owner.Prepare(ops); err == nil {
		t.Fatal("detached generic Prepare acquired operational authority")
	}
	if _, err := database.prepareScopedOwnedPointRoot(owner, snapshot, OrderedRootStoragePagerLeaves, ops); err != nil {
		t.Fatal(err)
	}
	assertScopedOwnedPointDetached(t, owner)
	wrong := []batch.Entry{{Type: batch.OpPut, Key: []byte("unowned")}}
	if _, err := database.prepareScopedOwnedPointRoot(owner, snapshot, OrderedRootStoragePagerLeaves, wrong); err == nil {
		t.Fatal("different canonical root keys were admitted")
	}
	assertScopedOwnedPointDetached(t, owner)
	delta := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	defer delta.Close()
	if err := delta.Delete(ops[0].Key); err != nil {
		t.Fatal(err)
	}
	if err := delta.Set(ops[1].Key, ops[1].Value); err != nil {
		t.Fatal(err)
	}
	ordinary := snapshot.idx.zipper.CloneWithAllocator(snapshot.idx.allocator)
	wantRoot, _, _, err := ordinary.Apply(root, delta)
	if err != nil {
		t.Fatal(err)
	}
	gotRoot, _, _, err := database.applyScopedOwnedPointRoot(owner, snapshot, OrderedRootStoragePagerLeaves, delta, snapshot.idx.allocator, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertScopedOwnedPointDetached(t, owner)
	for _, key := range []string{"key-000029", "key-000030", "key-000031", "key-000032"} {
		want, wantErr := snapshot.GetAtRoot(wantRoot, []byte(key))
		got, gotErr := snapshot.GetAtRoot(gotRoot, []byte(key))
		if (wantErr == nil) != (gotErr == nil) || !bytes.Equal(want, got) {
			t.Fatalf("key=%s owned=%q/%v ordinary=%q/%v", key, got, gotErr, want, wantErr)
		}
	}
	if _, _, _, err := database.applyScopedOwnedPointRoot(owner, snapshot, OrderedRootStoragePagerLeaves, delta, snapshot.idx.allocator, nil); err == nil {
		t.Fatal("same actual Apply authority was consumed twice")
	}
	assertScopedOwnedPointDetached(t, owner)
}

func TestPreparedOwnedPointScopedTerminalRequiresExactContext(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	snapshot := database.AcquireSnapshot()
	defer snapshot.Close()
	other := database.AcquireSnapshot()
	defer other.Close()
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("doc/1")}}
	account := &scopedPointTestCredit{}
	before := snapshot.readState.Load()
	first, err := database.captureScopedPreparedOwnedPointRoot(snapshot, 0, OrderedRootStoragePagerLeaves, ops, ownedPointTestLimits(), account)
	if err != nil {
		t.Fatal(err)
	}
	second, err := database.captureScopedPreparedOwnedPointRoot(snapshot, 0, OrderedRootStoragePagerLeaves, ops, ownedPointTestLimits(), account)
	if err != nil {
		t.Fatal(err)
	}
	cell := first.registration
	if cell != second.registration || cell.roots != 2 || snapshot.readState.Load() != before+2 {
		t.Fatal("actual snapshot registration did not retain both readers")
	}
	if err := first.Close(); err == nil {
		t.Fatal("no-context terminal silently released finite root")
	}
	if err := database.closeScopedOwnedPointRoot(first, other); err == nil {
		t.Fatal("same scalar state from another snapshot authorized terminal")
	}
	if first.closed || cell.roots != 2 || snapshot.readState.Load() != before+2 {
		t.Fatal("refused terminal changed actual readers or owner")
	}
	if err := database.closeScopedOwnedPointRoot(first, snapshot); err != nil {
		t.Fatal(err)
	}
	if cell.roots != 1 || snapshot.ownedPointRegistration != cell {
		t.Fatal("first terminal erased remaining reader")
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if snapshot.finalized.Load() {
		t.Fatal("snapshot finalized while actual scoped reader remains")
	}
	if err := database.closeScopedOwnedPointRoot(second, other); err == nil {
		t.Fatal("shutdown mismatch lost pending owner")
	}
	if second.closed || cell.roots != 1 {
		t.Fatal("shutdown refusal dropped pending registration")
	}
	if err := database.closeScopedOwnedPointRoot(second, snapshot); err != nil {
		t.Fatal(err)
	}
	if cell.roots != 0 || snapshot.db != nil || snapshot.idx != nil || snapshot.readState.Load() != snapshotReadClosedBit || second.registration != nil || second.snapshot != nil {
		t.Fatal("exact terminal did not finish the real snapshot reader")
	}
}

// Mirrors the canonical facet lifetime, including cumulative nonrefundable
// birth debit and request retirement while real creator consumers remain.
type scopedPointTestCredit struct {
	mu              sync.Mutex
	bytes, refs     uint64
	retired, closed bool
}

func (a *scopedPointTestCredit) ReserveStableMetadata(n uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return ErrPreparedRootPointProfileLimit
	}
	a.bytes += n
	return nil
}
func (a *scopedPointTestCredit) RetainStableMetadata() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return ErrPreparedRootPointProfileLimit
	}
	a.refs++
	return nil
}
func (a *scopedPointTestCredit) ReleaseStableMetadata() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.refs == 0 {
		panic("test creator imbalance")
	}
	a.refs--
	if a.retired && a.refs == 0 {
		a.closed = true
	}
}
func (a *scopedPointTestCredit) retire() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.retired = true
	if a.refs == 0 {
		a.closed = true
	}
}

func TestPreparedOwnedPointScopedCellRetainsCreatorAfterFirstRootClose(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	snapshot := database.AcquireSnapshot()
	defer snapshot.Close()
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("doc/1")}}
	account := &scopedPointTestCredit{}
	first, err := database.captureScopedPreparedOwnedPointRoot(snapshot, 0, OrderedRootStoragePagerLeaves, ops, ownedPointTestLimits(), account)
	if err != nil {
		t.Fatal(err)
	}
	second, err := database.captureScopedPreparedOwnedPointRoot(snapshot, 0, OrderedRootStoragePagerLeaves, ops, ownedPointTestLimits(), account)
	if err != nil {
		t.Fatal(err)
	}
	cell := first.registration
	if account.refs != 3 {
		t.Fatalf("two roots plus actual shared-cell creator refs=%d", account.refs)
	}
	other := &scopedPointTestCredit{}
	before := snapshot.readState.Load()
	if _, err := database.captureScopedPreparedOwnedPointRoot(snapshot, 0, OrderedRootStoragePagerLeaves, ops, ownedPointTestLimits(), other); err == nil {
		t.Fatal("different creator crossed Snapshot ownership boundary")
	}
	if other.bytes != 0 || other.refs != 0 || snapshot.readState.Load() != before || cell.roots != 2 {
		t.Fatal("different creator refused after effects")
	}
	if first.Workspace() != nil {
		t.Fatal("scoped workspace escaped through generic getter")
	}
	if err := database.closeScopedOwnedPointRoot(first, snapshot); err != nil {
		t.Fatal(err)
	}
	account.retire()
	if account.closed || account.refs != 2 || snapshot.ownedPointCreatorCredit != account || cell.roots != 1 {
		t.Fatal("creator root/request retirement released still-shared cell backing")
	}
	if _, err := database.prepareScopedOwnedPointRoot(second, snapshot, OrderedRootStoragePagerLeaves, ops); err != nil {
		t.Fatal(err)
	}
	if err := database.closeScopedOwnedPointRoot(second, snapshot); err != nil {
		t.Fatal(err)
	}
	if !account.closed || account.refs != 0 || snapshot.ownedPointCreatorCredit != nil || cell.roots != 0 {
		t.Fatal("last actual cell consumer did not release creator")
	}
}

func TestPreparedOwnedPointScopedFailedSnapshotTerminalRetainsExactOwner(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	snapshot := database.AcquireSnapshot()
	manager, err := valuelog.NewManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	// Attach a real independently installed Manager Set with no master alias.
	// This exercises the last-Set branch, rather than a fabricated ref counter.
	if snapshot.vlogPinned {
		if err := snapshot.vlogManager.Release(snapshot.state.ValueLogSet); err != nil {
			t.Fatal(err)
		}
	}
	state := *snapshot.state
	state.ValueLogSet = manager.CurrentSetNoRefresh()
	snapshot.state = &state
	snapshot.vlogManager, snapshot.vlogPinned = manager, true
	account := &scopedPointTestCredit{}
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("doc/1")}}
	owner, err := database.captureScopedPreparedOwnedPointRoot(snapshot, 0, OrderedRootStoragePagerLeaves, ops, ownedPointTestLimits(), account)
	if err != nil {
		t.Fatal(err)
	}
	cell := owner.registration
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	beforeRead, beforeRefs := snapshot.readState.Load(), account.refs
	if err := database.closeScopedOwnedPointRoot(owner, snapshot); !errors.Is(err, rootpublication.ErrStableMetadataShapeUnsupported) {
		t.Fatalf("open last-Set terminal error=%v", err)
	}
	if owner.closed || owner.registration != cell || cell.roots != 1 || snapshot.readState.Load() != beforeRead ||
		snapshot.db != database || snapshot.ownedPointCreatorCredit != account || account.refs != beforeRefs || state.ValueLogSet.RefCount.Load() != 1 {
		t.Fatal("failed terminal laundered pending read/owner/account")
	}
	account.retire()
	if account.closed {
		t.Fatal("failed pending owner lost creator credit")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.closeScopedOwnedPointRoot(owner, snapshot); err != nil {
		t.Fatal(err)
	}
	if !owner.closed || !account.closed || account.refs != 0 || state.ValueLogSet.RefCount.Load() != 0 || snapshot.db != nil {
		t.Fatal("exact Manager-closed retry did not discharge actual pending owner")
	}
}

func TestPreparedOwnedPointScopedTerminalAfterActualDBShutdown(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := database.AcquireSnapshot()
	account := &scopedPointTestCredit{}
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("doc/1")}}
	owner, err := database.captureScopedPreparedOwnedPointRoot(snapshot, 0, OrderedRootStoragePagerLeaves, ops, ownedPointTestLimits(), account)
	if err != nil {
		t.Fatal(err)
	}
	index := snapshot.idx
	account.retire()
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if !index.handlesClosed.Load() || account.closed || owner.closed {
		t.Fatal("shutdown did not close exact index or dropped held scoped creator")
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.closeScopedOwnedPointRoot(owner, snapshot); err != nil {
		t.Fatal(err)
	}
	if !owner.closed || !account.closed || snapshot.db != nil {
		t.Fatal("saved exact shutdown Snapshot did not join terminal")
	}
}

func TestPreparedOwnedPointScopedTerminalRefusesForegroundCallbackBeforeEffects(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	calls := 0
	unregister := database.RegisterForegroundReadObserver(func() {}, func() func() { return func() { calls++ } })
	defer unregister()
	snapshot := database.AcquireSnapshot()
	snapshot.MarkForegroundRead()
	account := &scopedPointTestCredit{}
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("doc/1")}}
	owner, err := database.captureScopedPreparedOwnedPointRoot(snapshot, 0, OrderedRootStoragePagerLeaves, ops, ownedPointTestLimits(), account)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	beforeRead, beforeRefs := snapshot.readState.Load(), account.refs
	if err := database.closeScopedOwnedPointRoot(owner, snapshot); err == nil {
		t.Fatal("arbitrary retained callback admitted to checked terminal")
	}
	if calls != 0 || owner.closed || owner.registration.roots != 1 || snapshot.readState.Load() != beforeRead || account.refs != beforeRefs {
		t.Fatal("callback refusal occurred after terminal effects")
	}
	snapshot.DetachForegroundRead()
	if calls != 1 {
		t.Fatal("foreground caller did not discharge its actual callback")
	}
	if err := database.closeScopedOwnedPointRoot(owner, snapshot); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || account.refs != 0 {
		t.Fatal("checked terminal repeated callback or retained completed creator")
	}
}

func TestPreparedOwnedPointScopedTerminalRefusesInheritedGates(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	snapshot := database.AcquireSnapshot()
	defer snapshot.Close()
	account := &scopedPointTestCredit{}
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("doc/1")}}
	owner, err := database.captureScopedPreparedOwnedPointRoot(snapshot, 0, OrderedRootStoragePagerLeaves, ops, ownedPointTestLimits(), account)
	if err != nil {
		t.Fatal(err)
	}
	for _, gate := range []struct {
		name         string
		lock, unlock func()
	}{
		{"maintenance", database.maintenanceMu.Lock, database.maintenanceMu.Unlock},
		{"teardown-read", database.teardownMu.RLock, database.teardownMu.RUnlock},
	} {
		gate.lock()
		err := database.closeScopedOwnedPointRoot(owner, snapshot)
		gate.unlock()
		if err == nil || owner.closed || owner.registration.roots != 1 || account.refs != 2 || snapshot.readState.Load() != 1 {
			t.Fatalf("%s inherited gate changed checked owner: %v", gate.name, err)
		}
	}
	if err := database.closeScopedOwnedPointRoot(owner, snapshot); err != nil {
		t.Fatal(err)
	}
}
