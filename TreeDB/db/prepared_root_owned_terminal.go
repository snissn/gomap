package db

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// releaseScopedSnapshotValueLog is the synchronous terminal half of the actual
// Snapshot. Its reservation remains on that Snapshot; no transient consumer,
// plan, Manager binding or callback is installed in a prepared owner.
//
// root is either the one last registered reader or nil for Snapshot.Close
// after all registered roots have drained. The checked metadata half remains
// the caller's responsibility, so a failed index/finalize check retains the
// control creator even after physical cleanup succeeds.
func (db *DB) releaseScopedSnapshotValueLog(s *Snapshot, root *PreparedOwnedPointRoot) (retErr error) {
	if db == nil || s == nil {
		return ErrPreparedRootPointProfileLimit
	}
	if !db.maintenanceMu.TryLock() {
		return ErrPreparedRootPointProfileLimit
	}
	if !db.teardownMu.TryLock() {
		db.maintenanceMu.Unlock()
		return ErrPreparedRootPointProfileLimit
	}
	s.iteratorMu.Lock()
	expected := snapshotReadClosedBit
	if root != nil {
		expected++
	}
	if s.db != db || !s.closed.Load() || s.idx == nil || s.readState.Load() != expected ||
		len(s.iterators) != 0 || s.ownedPointTerminalExecuting ||
		s.foregroundReadEnd != nil || s.idx != db.idx.Load() && !s.idx.handlesClosed.Load() {
		s.iteratorMu.Unlock()
		db.teardownMu.Unlock()
		db.maintenanceMu.Unlock()
		return ErrPreparedRootPointProfileLimit
	}
	if root != nil {
		if err := root.checkedScopedContextLocked(db, s, true); err != nil {
			s.iteratorMu.Unlock()
			db.teardownMu.Unlock()
			db.maintenanceMu.Unlock()
			return err
		}
	}
	control := s.ownedPointTerminalRetentions
	if control == nil {
		s.iteratorMu.Unlock()
		db.teardownMu.Unlock()
		db.maintenanceMu.Unlock()
		return nil
	}
	manager := s.vlogManager
	if manager == nil || s.state == nil || s.state.ValueLogSet == nil {
		s.iteratorMu.Unlock()
		db.teardownMu.Unlock()
		db.maintenanceMu.Unlock()
		return ErrPreparedRootPointProfileLimit
	}
	// Shutdown metadata-only proof checks ALL exact holds before effects.
	allClosed := true
	for i := 0; i < control.Count(); i++ {
		_, hold, released, err := control.TerminalRegistration(i)
		if err != nil {
			s.iteratorMu.Unlock()
			db.teardownMu.Unlock()
			db.maintenanceMu.Unlock()
			return err
		}
		if !released {
			checked, ok := hold.(rootpublication.StableSegmentTerminalRetention)
			if !ok || checked.ValidateTerminalRelease(nil) != nil {
				allClosed = false
			}
		}
	}
	if allClosed {
		if s.vlogPinned {
			if err := manager.ReleaseSnapshotSetChecked(s.state.ValueLogSet); err != nil {
				s.iteratorMu.Unlock()
				db.teardownMu.Unlock()
				db.maintenanceMu.Unlock()
				return err
			}
			s.vlogPinned = false
		}
		for i := 0; i < control.Count(); i++ {
			_, hold, released, err := control.TerminalRegistration(i)
			if err != nil {
				s.iteratorMu.Unlock()
				db.teardownMu.Unlock()
				db.maintenanceMu.Unlock()
				return err
			}
			if !released {
				if err := hold.Release(); err != nil {
					s.iteratorMu.Unlock()
					db.teardownMu.Unlock()
					db.maintenanceMu.Unlock()
					return err
				}
			}
		}
		s.ownedPointTerminalStarted = true
		s.iteratorMu.Unlock()
		db.teardownMu.Unlock()
		db.maintenanceMu.Unlock()
		return nil
	}
	if db.closing.Load() {
		s.iteratorMu.Unlock()
		db.teardownMu.Unlock()
		db.maintenanceMu.Unlock()
		return ErrPreparedRootPointProfileLimit
	}
	// Add and the Close transition share maintenanceMu. The wait occurs outside
	// every engine gate and tracks this invocation only, including refusals.
	db.ownedPointTerminalExecutions.Add(1)
	s.ownedPointTerminalExecuting = true
	s.iteratorMu.Unlock()
	db.teardownMu.Unlock()
	db.maintenanceMu.Unlock()
	defer db.ownedPointTerminalExecutions.Done()
	defer func() {
		s.iteratorMu.Lock()
		s.ownedPointTerminalExecuting = false
		s.iteratorMu.Unlock()
	}()
	account := s.ownedPointCreatorCredit
	if account == nil && root != nil {
		account = root.scopedAccount
	}
	// The control itself is the retained creator when all roots closed first.
	if account == nil {
		account = control.TerminalAccount()
	}
	count := control.Count()
	pinCount := 0
	if root != nil {
		for i := range root.files {
			if root.files[i].pin != nil {
				pinCount++
			}
		}
	}
	groupClass, err := rootpublication.StableBackingClassBytes(uint64(count)*uint64(unsafe.Sizeof(rootpublication.StableSegmentTerminalGroup{})), true)
	if err != nil {
		return err
	}
	pinClass, err := rootpublication.StableBackingClassBytes(uint64(pinCount)*uint64(unsafe.Sizeof((*rootpublication.IdentityPin)(nil))), true)
	if err != nil || ^uint64(0)-groupClass < pinClass {
		return rootpublication.ErrStableMetadataShapeUnsupported
	}
	consumerClass, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(stableSegmentTerminalConsumerV1{})), true)
	if err != nil || ^uint64(0)-groupClass-pinClass < consumerClass {
		return rootpublication.ErrStableMetadataShapeUnsupported
	}
	if err := account.ReserveStableMetadata(groupClass + pinClass + consumerClass); err != nil {
		return err
	}
	consumer := stableSegmentTerminalConsumerV1{db: db, manager: manager}
	joined, err := consumer.BeginTerminalRelease()
	if err != nil {
		return err
	}
	defer consumer.EndTerminalRelease(joined)
	groups := make([]rootpublication.StableSegmentTerminalGroup, 0, count)
	pins := make([]*rootpublication.IdentityPin, 0, pinCount)
	for i := 0; i < count; i++ {
		id, hold, released, err := control.TerminalRegistration(i)
		if err != nil {
			return err
		}
		if released {
			continue
		}
		start := len(pins)
		if root != nil {
			for j := range root.files {
				if root.files[j].id == id && root.files[j].pin != nil {
					pins = append(pins, root.files[j].pin)
				}
			}
		}
		groups = append(groups, rootpublication.StableSegmentTerminalGroup{Retention: hold, OwnedPins: pins[start:len(pins)]})
	}
	if len(pins) != pinCount {
		return rootpublication.ErrStableTerminalConsumerMismatch
	}
	if len(groups) == 0 {
		return nil
	}
	var planErr error
	if !control.SetReleased() {
		consumer.plan, planErr = manager.PrepareStableSegmentTerminalReleaseWithSnapshotSet(groups, s.state.ValueLogSet)
	} else {
		consumer.plan, planErr = manager.PrepareStableSegmentTerminalRelease(groups)
	}
	if planErr != nil {
		return planErr
	}
	// ALL real holds, pins and conditional Set edges have now passed the same
	// whole-group preflight. Pin draining and Set consumption are exact-once.
	s.iteratorMu.Lock()
	s.ownedPointTerminalStarted = true
	if root != nil {
		for i := range root.files {
			if root.files[i].pin != nil {
				root.files[i].pin.Release()
				root.files[i].pin = nil
			}
		}
	}
	s.iteratorMu.Unlock()
	if !control.SetReleased() {
		if err := control.ReleaseSnapshotSet(consumer.plan); err != nil {
			return err
		}
		s.iteratorMu.Lock()
		s.vlogPinned = false
		s.iteratorMu.Unlock()
	}
	for _, group := range groups {
		if err := consumer.ReleaseSegmentRetention(group.Retention); err != nil {
			db.recordTerminalErrorV1(err)
			return err
		}
	}
	return nil
}
