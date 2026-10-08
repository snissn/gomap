package db

import (
	"bytes"
	"math"
	"reflect"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/zipper"
)

// This is the actual selected Snapshot registration, not another reader
// registry. All fields are scalar values. The existing Snapshot owns the real
// beginRead/index/leaf-pin edges; each registered root consumes exactly one
// beginRead through checked terminal Close. Only iteratorMu mutates roots.
// Clearing the last root removes the Snapshot's cell edge before releasing
// that read or the packet's creator credit.
type ownedPointCaptureRegistration struct {
	token      StateToken
	generation uint64
	indexID    uint64
	roots      uint64
}

// The selected packet keeps the real snapshot on its actual pending/stack
// owner. No prepared root retains that broad graph between concrete calls.
// Public generic capture preserves its established retaining semantics.
func (db *DB) captureScopedPreparedOwnedPointRoot(snapshot *Snapshot, root uint64, policy OrderedRootStoragePolicy, ops []batch.Entry, limits PreparedOwnedPointLimits, account rootpublication.StableMetadataAccount) (_ *PreparedOwnedPointRoot, retErr error) {
	if db == nil || snapshot == nil || account == nil || !reflect.TypeOf(account).Comparable() || reflect.ValueOf(account).Kind() == reflect.Pointer && reflect.ValueOf(account).IsNil() {
		return nil, ErrPreparedRootPointProfileLimit
	}
	// Serialize registration with Snapshot.Close and other captures. A rejected
	// creator never reserves, clones or observes; the read acquired by Capture
	// keeps all exact broad state alive after this lock is released.
	snapshot.iteratorMu.Lock()
	defer snapshot.iteratorMu.Unlock()
	if snapshot.db != db || snapshot.closed.Load() || snapshot.idx == nil || snapshot.ownedPointTerminalStarted || snapshot.ownedPointTerminalExecuting ||
		snapshot.ownedPointRegistration != nil && snapshot.ownedPointCreatorCredit != account {
		return nil, ErrPreparedRootPointProfileLimit
	}
	if control := snapshot.ownedPointTerminalRetentions; control != nil {
		if err := control.RequireAccount(account); err != nil {
			return nil, err
		}
	}
	if err := account.RetainStableMetadata(); err != nil {
		return nil, err
	}
	rootRetained := true
	defer func() {
		if retErr != nil && rootRetained {
			account.ReleaseStableMetadata()
		}
	}()
	callbackClass, err := rootpublication.StableBackingClassBytes(16, true)
	if err != nil {
		return nil, err
	}
	if err := account.ReserveStableMetadata(callbackClass); err != nil {
		return nil, err
	}
	// This one escaping method value is owned by the retained root creator.
	o, err := db.CapturePreparedOwnedPointRoot(snapshot, root, policy, ops, limits, account.ReserveStableMetadata)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			_ = o.Close()
		}
	}()
	if o.backing > math.MaxUint64-callbackClass {
		return nil, ErrPreparedRootPointProfileLimit
	}
	o.backing += callbackClass
	cell := snapshot.ownedPointRegistration
	if cell != nil && (cell.token != o.token || cell.generation != snapshot.generation.Load() || cell.indexID != snapshot.idx.id || cell.roots == math.MaxUint64) {
		return nil, ErrPreparedRootPointProfileLimit
	}
	cellRetained := false
	defer func() {
		if retErr != nil && cellRetained {
			account.ReleaseStableMetadata()
		}
	}()
	if cell == nil {
		if err := account.RetainStableMetadata(); err != nil {
			return nil, err
		}
		cellRetained = true
		if err := o.chargeClass(uint64(unsafe.Sizeof(ownedPointCaptureRegistration{})), false); err != nil {
			return nil, err
		}
		cell = &ownedPointCaptureRegistration{token: o.token, generation: snapshot.generation.Load(), indexID: snapshot.idx.id}
	}
	if err := o.workspace.DetachOperationalBindings(o.zipper); err != nil {
		return nil, err
	}
	if snapshot.ownedPointTerminalRetentions == nil && snapshot.vlogPinned && snapshot.state != nil && snapshot.state.ValueLogSet != nil && len(snapshot.state.ValueLogSet.Files) != 0 {
		if snapshot.vlogManager == nil {
			return nil, ErrPreparedRootPointProfileLimit
		}
		control, err := snapshot.vlogManager.AcquireSnapshotSetTerminalRetentions(snapshot.state.ValueLogSet, 32, account)
		if err != nil {
			return nil, err
		}
		// The actual Snapshot owns this complete scalar closure before any
		// eventual WAL/Apply. Its own creator retain survives creator-root Close.
		snapshot.ownedPointTerminalRetentions = control
		n := control.BackingBytes()
		if o.backing > math.MaxUint64-n {
			panic("admitted owned point backing overflow")
		}
		o.backing += n
	}
	cell.roots++
	snapshot.ownedPointRegistration = cell
	if cellRetained {
		snapshot.ownedPointCreatorCredit = account
	}
	o.registration, o.snapshot, o.scopedBinding, o.scopedAccount = cell, nil, true, account
	rootRetained = false
	return o, nil
}

func (o *PreparedOwnedPointRoot) checkedScopedContextLocked(db *DB, s *Snapshot, allowClosed bool) error {
	if o == nil || o.closed || !o.scopedBinding || o.scopedApplying || o.registration == nil || o.scopedAccount == nil ||
		db == nil || s == nil || s.db != db || s.idx == nil || s.ownedPointTerminalExecuting ||
		!allowClosed && (s.closed.Load() || s.ownedPointTerminalStarted || s.ownedPointTerminalExecuting || s.idx.pager == nil || s.idx.allocator == nil) {
		return ErrPreparedRootPointProfileLimit
	}
	if s.ownedPointRegistration != o.registration || o.registration.roots == 0 ||
		s.ownedPointCreatorCredit != o.scopedAccount || s.readState.Load()&^snapshotReadClosedBit == 0 ||
		o.registration.generation != s.generation.Load() || o.registration.indexID != s.idx.id || o.registration.token != o.token {
		return ErrPreparedRootPointProfileLimit
	}
	token, ok := stateTokenFromState(s.state)
	if !ok || token != o.token {
		return ErrPreparedRootPointProfileLimit
	}
	return nil
}

func (o *PreparedOwnedPointRoot) checkedScopedContext(db *DB, s *Snapshot, allowClosed bool) error {
	if s == nil {
		return ErrPreparedRootPointProfileLimit
	}
	s.iteratorMu.Lock()
	defer s.iteratorMu.Unlock()
	return o.checkedScopedContextLocked(db, s, allowClosed)
}

// Prepare uses the exact same C13 old images/header facts. Operational aliases
// exist only through this synchronous call, and are cleared on every return.
func (db *DB) prepareScopedOwnedPointRoot(o *PreparedOwnedPointRoot, s *Snapshot, policy OrderedRootStoragePolicy, ops []batch.Entry) (result zipper.ReadOnlyPrepareResult, retErr error) {
	if err := o.checkedScopedContext(db, s, false); err != nil {
		return result, err
	}
	if policy != o.storagePolicy {
		return result, ErrPreparedRootPointProfileLimit
	}
	opts, err := db.orderedRootPublishOptionsForPolicy(policy)
	if err != nil {
		return result, err
	}
	if err := o.workspace.BindOperationalContext(o.zipper, s.idx.pager, s.idx.allocator, opts.leafPageLog); err != nil {
		return result, err
	}
	o.snapshot = s
	defer func() {
		o.snapshot = nil
		if err := o.workspace.DetachOperationalBindings(o.zipper); retErr == nil {
			retErr = err
		}
	}()
	if err := o.ValidateCapturedBaseline(o.token); err != nil {
		return result, err
	}
	result, retErr = o.Prepare(ops)
	if retErr == nil {
		o.scopedPrepared = true
	}
	return result, retErr
}

// applyScopedOwnedPointRoot is the single serial Apply for this exact
// Prepare result. Even a partially failed Apply cannot be replayed; actual COW
// terminal ownership remains the existing candidate/runtime's responsibility.
// The allocator and concrete leaf producer are call-local arguments only.
func (db *DB) applyScopedOwnedPointRoot(o *PreparedOwnedPointRoot, s *Snapshot, policy OrderedRootStoragePolicy, delta *batch.Batch, allocator zipper.PageAllocator, writer zipper.LeafPageLog) (root uint64, retired []uint64, metrics adaptive.Metrics, retErr error) {
	return db.applyScopedOwnedPointRootMode(o, s, policy, delta, allocator, writer, false)
}

// Staging changes the destination of this SAME Apply's pager writes. The
// caller supplies the actual no-growth allocator and a separately staged leaf
// producer; neither authority is inferred from interface satisfaction. The
// actual publication packet retains the root/images through storage uncertainty.
func (db *DB) applyScopedOwnedPointRootMode(o *PreparedOwnedPointRoot, s *Snapshot, policy OrderedRootStoragePolicy, delta *batch.Batch, allocator zipper.PageAllocator, writer zipper.LeafPageLog, stagePager bool) (root uint64, retired []uint64, metrics adaptive.Metrics, retErr error) {
	if err := o.checkedScopedContext(db, s, false); err != nil {
		return 0, nil, metrics, err
	}
	if policy != o.storagePolicy || !o.scopedPrepared || o.scopedApplied || delta == nil || allocator == nil || delta.HasDeleteRanges() {
		return 0, nil, metrics, ErrPreparedRootPointProfileLimit
	}
	ops := delta.SortedEntries()
	if len(ops) != len(o.ops) {
		return 0, nil, metrics, ErrPreparedRootPointProfileLimit
	}
	for i := range ops {
		if ops[i].Type != o.ops[i].Type || !bytes.Equal(ops[i].Key, o.ops[i].Key) {
			return 0, nil, metrics, ErrPreparedRootPointProfileLimit
		}
	}
	// Policy is checked against the same captured configuration, not a current
	// index zipper or inherited generic decoder. The selected caller supplies
	// its real synchronous finite writer; nil is valid only for pager leaves.
	opts, err := db.orderedRootPublishOptionsForPolicy(policy)
	if err != nil {
		return 0, nil, metrics, err
	}
	if opts.outerLeavesInValueLog && writer == nil || !opts.outerLeavesInValueLog && writer != nil {
		return 0, nil, metrics, ErrPreparedRootPointProfileLimit
	}
	if err := o.workspace.BindOperationalContext(o.zipper, s.idx.pager, allocator, writer); err != nil {
		return 0, nil, metrics, err
	}
	o.snapshot = s
	defer func() {
		o.snapshot = nil
		o.scopedApplying = false
		if err := o.workspace.DetachOperationalBindings(o.zipper); retErr == nil {
			retErr = err
		}
	}()
	if err := o.ValidateCapturedBaseline(o.token); err != nil {
		return 0, nil, metrics, err
	}
	if stagePager {
		if err := o.workspace.EnablePagerStaging(); err != nil {
			return 0, nil, metrics, err
		}
	}
	o.scopedApplying, o.scopedApplied = true, true
	root, retired, metrics, retErr = o.zipper.Apply(o.root, delta)
	if retErr == nil && stagePager {
		o.scopedPagerStaged = true
	}
	return root, retired, metrics, retErr
}

// closeScopedOwnedPointRoot performs no implicit current-snapshot lookup. A
// mismatched caller retains every pin/read/credit edge for a later correct
// terminal consumer. A closed but not finalized exact Snapshot is accepted;
// this is how the actual pending owner joins Snapshot/DB shutdown.
func (db *DB) closeScopedOwnedPointRoot(o *PreparedOwnedPointRoot, s *Snapshot) error {
	if o == nil || o.closed {
		return nil
	}
	if db == nil || s == nil {
		return ErrPreparedRootPointProfileLimit
	}
	s.iteratorMu.Lock()
	needsTerminal := s.ownedPointTerminalRetentions != nil && s.readState.Load() == snapshotReadClosedBit|1 && len(s.iterators) == 0
	s.iteratorMu.Unlock()
	if needsTerminal {
		if err := db.releaseScopedSnapshotValueLog(s, o); err != nil {
			return err
		}
	}
	// The existing maintenance gate serializes vacuum index replacement and
	// shutdown handle closure through this no-I/O terminal join. Actual future
	// callers must arrive without an inherited maintenance/teardown gate.
	if !db.maintenanceMu.TryLock() {
		return ErrPreparedRootPointProfileLimit
	}
	defer db.maintenanceMu.Unlock()
	if !db.teardownMu.TryLock() {
		return ErrPreparedRootPointProfileLimit
	}
	defer db.teardownMu.Unlock()
	s.iteratorMu.Lock()
	if err := o.checkedScopedContextLocked(db, s, true); err != nil {
		s.iteratorMu.Unlock()
		return err
	}
	// Last-read finalization must not consume a live last Set and then silently
	// pool a failed Snapshot. The restricted checked Manager seam is no-I/O.
	if s.readState.Load() == snapshotReadClosedBit|1 && len(s.iterators) == 0 {
		// Retired-index close can perform fallible I/O or install a ghost. Its
		// checked complete terminal integration remains unsupported here.
		if s.foregroundReadEnd != nil || s.idx != db.idx.Load() && !s.idx.handlesClosed.Load() {
			s.iteratorMu.Unlock()
			return ErrPreparedRootPointProfileLimit
		}
		if s.vlogPinned && s.state != nil && s.state.ValueLogSet != nil {
			if s.vlogManager == nil {
				s.iteratorMu.Unlock()
				return ErrPreparedRootPointProfileLimit
			}
			if err := s.vlogManager.ReleaseSnapshotSetChecked(s.state.ValueLogSet); err != nil {
				s.iteratorMu.Unlock()
				return err
			}
			s.vlogPinned = false
		}
	}
	if s.readState.Load() == snapshotReadClosedBit|1 && s.ownedPointTerminalRetentions != nil {
		if err := s.ownedPointTerminalRetentions.Close(); err != nil {
			s.iteratorMu.Unlock()
			return err
		}
		s.ownedPointTerminalRetentions = nil
	}
	cell := o.registration
	cell.roots--
	var creator rootpublication.StableMetadataAccount
	if cell.roots == 0 {
		s.ownedPointRegistration = nil
		creator = s.ownedPointCreatorCredit
		s.ownedPointCreatorCredit = nil
	}
	account := o.scopedAccount
	o.registration, o.scopedBinding, o.scopedAccount, o.snapshot = nil, false, nil, s
	s.iteratorMu.Unlock()
	// Every remaining ordinary terminal action is nonfallible in the admitted
	// branch; the real read still pins s until Close reaches endRead.
	if err := o.Close(); err != nil {
		return err
	}
	if creator != nil {
		creator.ReleaseStableMetadata()
	}
	account.ReleaseStableMetadata()
	return nil
}
