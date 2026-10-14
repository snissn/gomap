package valuelog

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"unsafe"
)

// ManagerWrapperReleaseWork belongs to the original disposing Snapshot. It
// holds the exact borrowed Manager until aliases can clear; its external
// removal receipt survives disposal of the Manager's containing contribution.
type ManagerWrapperReleaseWork struct {
	manager        *Manager
	consumesBorrow bool
	phase          uint8
	removal        retainedalloc.RemovalWork
}

func (m *Manager) RetainSnapshotWrapper() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing {
		return false
	}
	m.wrapperBorrows.Add(1)
	return true
}

func (state *ManagerWrapperReleaseWork) BindSnapshotWrapper(m *Manager, w *iterator.OrdinalScanWork) (bool, error) {
	if state == nil || m == nil || w == nil {
		return false, rootpublication.ErrResourceOwnership
	}
	if state.manager != nil || state.phase != 0 {
		return false, rootpublication.ErrResourceOwnership
	}
	if !reserveManagerWrapperWork(w, 1, 2*uint64(unsafe.Sizeof(*state))+64) {
		return false, nil
	}
	state.manager = m
	state.consumesBorrow = true
	return true, nil
}

// The original Set releases no Snapshot borrow. Its embedded receipt merely
// completes the original terminal Manager after its final File aliases clear.
func (state *ManagerWrapperReleaseWork) bindSetWrapper(m *Manager, w *iterator.OrdinalScanWork) (bool, error) {
	if state == nil || m == nil || state.manager != nil || state.phase != 0 {
		return false, rootpublication.ErrResourceOwnership
	}
	if !reserveManagerWrapperWork(w, 1, 2*uint64(unsafe.Sizeof(*state))+2*uint64(unsafe.Sizeof(*m))+64) {
		return false, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// The original Set still retains its actual File/Manager aliases here.
	// Establish this containing edge before any final File alias can detach.
	m.wrapperBorrows.Add(1)
	state.manager = m
	state.consumesBorrow = true
	return true, nil
}

func (state *ManagerWrapperReleaseWork) ReleaseWork(w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		return false, rootpublication.ErrResourceOwnership
	}
	return state.releaseStep(true, w)
}

// ReleaseOriginal uses the actual disposing Snapshot's preborn receipt. Capture
// refusal may reuse a Snapshot only after every receipt alias has been consumed.
func (state *ManagerWrapperReleaseWork) ReleaseOriginal(m *Manager) {
	if m == nil {
		return
	}
	if state.manager != nil || state.phase != 0 {
		panic("Manager wrapper receipt already active")
	}
	state.manager = m
	state.consumesBorrow = true
	for {
		ready, err := state.releaseStep(false, nil)
		if err != nil {
			panic(err)
		}
		if ready {
			*state = ManagerWrapperReleaseWork{}
			return
		}
	}
}

// Standalone original consumers preserve the same synchronous release API.
func (m *Manager) ReleaseSnapshotWrapper() {
	var state ManagerWrapperReleaseWork
	state.ReleaseOriginal(m)
}
func (state *ManagerWrapperReleaseWork) releaseStep(native bool, w *iterator.OrdinalScanWork) (bool, error) {
	if state == nil {
		return false, rootpublication.ErrResourceOwnership
	}
	if state.phase == 3 {
		return true, nil
	}
	if state.phase == 2 {
		ready, err := state.continueRemoval(native, w)
		if ready {
			state.phase = 3
		}
		return ready, err
	}
	m := state.manager
	if m == nil {
		return false, rootpublication.ErrResourceOwnership
	}
	if state.phase == 0 {
		if !reserveManagerWrapperWork(w, 1, 2*uint64(unsafe.Sizeof(*m))+2*uint64(unsafe.Sizeof(*state))+64) {
			return false, nil
		}
		m.mu.Lock()
		if state.consumesBorrow && m.wrapperBorrows.Add(-1) < 0 {
			m.mu.Unlock()
			panic("value-log Manager Snapshot borrow underflow")
		}
		if native && m.closing && m.wrapperBorrows.Load() == 0 {
			m.wrapperNativeCleanup = true
		}
		state.phase = 1
		m.mu.Unlock()
	}
	if !reserveManagerWrapperWork(w, 1, 2*uint64(unsafe.Sizeof(*m))+64) {
		return false, nil
	}
	m.mu.Lock()
	if native && m.closing && m.closeRunning {
		// The closing original owner may still be draining its physical walker.
		// Keep this claimed native receipt and containing custody across the pause.
		m.mu.Unlock()
		return false, nil
	}
	if !m.closing || m.closeRunning || len(m.files) != 0 || m.closeErr != nil || m.wrapperBorrows.Load() != 0 || m.fileAliases.Load() != 0 || m.producerMetadata == nil {
		m.wrapperNativeCleanup = false
		state.manager = nil
		state.phase = 3
		m.mu.Unlock()
		return true, nil
	}
	ready, err := state.bindRemoval(m.producerMetadata, m.producerCharge, native, w)
	if !ready || err != nil {
		m.mu.Unlock()
		return false, err
	}
	if !reserveManagerWrapperWork(w, 1, 2*uint64(unsafe.Sizeof(*m))+64) {
		m.mu.Unlock()
		return false, nil
	}
	m.clearTerminalAliasesLocked()
	m.wrapperNativeCleanup = false
	state.manager = nil
	state.phase = 2
	m.mu.Unlock()
	m = nil
	ready, err = state.continueRemoval(native, w)
	if ready {
		state.phase = 3
	}
	return ready, err
}
func (m *Manager) clearTerminalAliasesLocked() {
	m.producerMetadata, m.producerCharge = nil, 0
	m.files, m.currentWritableByLane, m.multiCurrentWritableLanes = nil, nil, nil
	m.retirementParents, m.recoveredStableDeleteDirs = nil, nil
	m.dictLookup, m.templateLookup, m.templateDefCache = nil, nil, nil
	m.deferredDeletionSync = nil
	m.groupedFrameCacheBudget = nil
	m.stableResourcePins = nil
	m.dir, m.extraScanDirs = "", nil
}

func reserveManagerWrapperWork(w *iterator.OrdinalScanWork, records, bytes uint64) bool {
	return w == nil || w.Reserve(records, bytes)
}
func (state *ManagerWrapperReleaseWork) bindRemoval(owner *retainedalloc.Owner, charge uint64, native bool, w *iterator.OrdinalScanWork) (bool, error) {
	if native {
		return state.removal.BindPendingWork(owner, charge, w)
	}
	return state.removal.BindPendingOriginal(owner, charge)
}
func (state *ManagerWrapperReleaseWork) continueRemoval(native bool, w *iterator.OrdinalScanWork) (bool, error) {
	if native {
		return state.removal.ContinueWork(w)
	}
	return state.removal.ContinueOriginal()
}
