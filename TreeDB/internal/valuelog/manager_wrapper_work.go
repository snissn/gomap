package valuelog

// RetainSnapshotWrapper keeps the original Manager containing allocation
// charged while a Snapshot can still name it, even after Manager.Close.
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

// ReleaseSnapshotWrapper consumes the same original borrow after the disposing
// Snapshot/Set has cleared its Manager aliases and physical cleanup completed.
func (m *Manager) ReleaseSnapshotWrapper() {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.wrapperBorrows.Add(-1) < 0 {
		m.mu.Unlock()
		panic("value-log Manager Snapshot borrow underflow")
	}
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
