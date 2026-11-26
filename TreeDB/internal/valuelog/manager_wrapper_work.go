package valuelog

// RetainSnapshotWrapper keeps the original Manager containing allocation
// charged while a Snapshot can still name it, even after Manager.Close.
func (m *Manager) RetainSnapshotWrapper() bool {
	if m == nil {
		return false
	}
	if m.closingAtomic.Load() {
		return false
	}
	m.wrapperBorrows.Add(1)
	if !m.closingAtomic.Load() {
		return true
	}
	// Close raced with the speculative increment. Roll back through the terminal
	// release path so Close cannot miss a final refund after seeing the transient
	// non-zero borrow count.
	m.ReleaseSnapshotWrapper()
	return false
}

// ReleaseSnapshotWrapper consumes the same original borrow after the disposing
// Snapshot/Set has cleared its Manager aliases and physical cleanup completed.
func (m *Manager) ReleaseSnapshotWrapper() {
	if m == nil {
		return
	}
	rem := m.wrapperBorrows.Add(-1)
	if rem < 0 {
		panic("value-log Manager Snapshot borrow underflow")
	}
	if rem > 0 || !m.closingAtomic.Load() {
		return
	}
	m.mu.Lock()
	owner, charge := m.detachTerminalMetadataLocked()
	m.mu.Unlock()
	if owner != nil {
		owner.RemovePending(charge)
	}
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
