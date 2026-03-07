package caching

import (
	"fmt"
	"sort"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/rootfmt"
)

func (m *rootDomainManager) pendingNamedRoots() bool {
	if m == nil {
		return false
	}
	m.namedRootMu.RLock()
	defer m.namedRootMu.RUnlock()
	return len(m.namedRootsByID) > 0
}

func (m *rootDomainManager) debugNamedRootStateByID(rootID uint64) (NamedRootDebugState, bool) {
	if m == nil {
		return NamedRootDebugState{}, false
	}
	m.namedRootMu.RLock()
	defer m.namedRootMu.RUnlock()
	state := m.namedRootsByID[rootID]
	return namedRootDebugState(state)
}

func (m *rootDomainManager) debugNamedRootStateByKey(rootKey []byte) (NamedRootDebugState, bool) {
	if m == nil {
		return NamedRootDebugState{}, false
	}
	m.namedRootMu.RLock()
	defer m.namedRootMu.RUnlock()
	state := m.namedRootsByKey[string(rootKey)]
	return namedRootDebugState(state)
}

func namedRootDebugState(state *namedRootOverlayState) (NamedRootDebugState, bool) {
	if state == nil {
		return NamedRootDebugState{}, false
	}
	return NamedRootDebugState{
		VirtualRootID:    state.virtualRootID,
		BaseRootID:       state.baseRootID,
		HasDomain:        state.domain != nil && state.domain.table != nil,
		LegacyEntryCount: 0,
	}, true
}

func (m *rootDomainManager) namedRootState(rootID uint64) (*namedRootOverlayState, error) {
	if m == nil {
		return nil, fmt.Errorf("treedb: named root %d not buffered", rootID)
	}
	m.namedRootMu.RLock()
	state := m.namedRootsByID[rootID]
	m.namedRootMu.RUnlock()
	if state == nil {
		return nil, fmt.Errorf("treedb: named root %d not buffered", rootID)
	}
	return state, nil
}

func (m *rootDomainManager) resolvedNamedRootID(rootID uint64) uint64 {
	if m == nil || rootID == 0 {
		return rootID
	}
	m.namedRootMu.RLock()
	defer m.namedRootMu.RUnlock()
	return m.resolvePublishedNamedRootIDLocked(rootID)
}

func (m *rootDomainManager) resolvePublishedNamedRootIDLocked(rootID uint64) uint64 {
	if rootID == 0 || m == nil {
		return rootID
	}
	if state := m.namedRootsByID[rootID]; state != nil {
		return state.baseRootID
	}
	if m.namedRootPublishedByVirtual != nil {
		if resolved, ok := m.namedRootPublishedByVirtual[rootID]; ok {
			return resolved
		}
	}
	return rootID
}

func (m *rootDomainManager) nextVirtualNamedRootIDLocked() uint64 {
	return virtualNamedRootMask | m.nextNamedRootID.Add(1)
}

func (m *rootDomainManager) bindPendingNamedRootKeysLocked(pending []namedRootPendingMutation, sysEntries []batch.Entry) error {
	return m.bindPendingNamedRootRootKeysLocked(
		len(pending),
		func(i int) uint64 { return pending[i].virtualRootID },
		func(i int) bool { return len(pending[i].rootKey) > 0 },
		func(i int, rootKey []byte) { pending[i].rootKey = rootKey },
		sysEntries,
	)
}

func (m *rootDomainManager) bindPendingNamedRootIteratorKeysLocked(pending []namedRootPendingIteratorMutation, sysEntries []batch.Entry) error {
	return m.bindPendingNamedRootRootKeysLocked(
		len(pending),
		func(i int) uint64 { return pending[i].virtualRootID },
		func(i int) bool { return len(pending[i].rootKey) > 0 },
		func(i int, rootKey []byte) { pending[i].rootKey = rootKey },
		sysEntries,
	)
}

func (m *rootDomainManager) bindPendingNamedRootTableKeysLocked(pending []namedRootPendingTableMutation, sysEntries []batch.Entry) error {
	return m.bindPendingNamedRootRootKeysLocked(
		len(pending),
		func(i int) uint64 { return pending[i].virtualRootID },
		func(i int) bool { return len(pending[i].rootKey) > 0 },
		func(i int, rootKey []byte) { pending[i].rootKey = rootKey },
		sysEntries,
	)
}

func (m *rootDomainManager) bindPendingNamedRootRootKeysLocked(pendingLen int, virtualRootID func(int) uint64, hasRootKey func(int) bool, setRootKey func(int, []byte), sysEntries []batch.Entry) error {
	for _, entry := range sysEntries {
		if entry.Type != batch.OpPut || len(entry.Value) == 0 {
			continue
		}
		var desc collections.CollectionRootDescriptor
		if err := desc.Decode(entry.Value); err != nil {
			continue
		}
		for i := 0; i < pendingLen; i++ {
			if virtualRootID(i) != desc.RootPageID || hasRootKey(i) {
				continue
			}
			setRootKey(i, append([]byte(nil), entry.Key...))
			break
		}
	}
	for i := 0; i < pendingLen; i++ {
		if !hasRootKey(i) {
			return fmt.Errorf("treedb: missing root key for buffered root %d", virtualRootID(i))
		}
	}
	return nil
}

func (m *rootDomainManager) applyPendingNamedRootLocked(pending *namedRootPendingMutation) {
	if pending == nil {
		return
	}
	state, beforeBytes, err := m.ensureNamedRootStateLocked(pending.virtualRootID, pending.baseRootID, pending.rootKey, pending.hasFormat, pending.format, nil)
	if err != nil {
		panic(fmt.Sprintf("treedb: create named-root domain: %v", err))
	}
	for _, entry := range pending.entries {
		switch {
		case entry.Type == batch.OpDelete:
			runNamedRootOwnedWriteTestHook("delete_key", entry.Key)
		case len(entry.Value) == 0:
			runNamedRootOwnedWriteTestHook("set_key", entry.Key)
		default:
			runNamedRootOwnedWriteTestHook("set_bytes", entry.Key)
		}
		runNamedRootMemtableWriteTestHook(pending.virtualRootID, "domain", entry.Key)
	}
	if err := state.domain.applyEntriesOwned(pending.entries, true); err != nil {
		panic(fmt.Sprintf("treedb: apply named-root entries: %v", err))
	}
	m.namedRootBufferedBytes.Add(namedRootStateBufferedBytes(state) - beforeBytes)
}

func (m *rootDomainManager) applyPendingNamedRootTableLocked(pending *namedRootPendingTableMutation) error {
	if pending == nil {
		return nil
	}
	state, beforeBytes, err := m.ensureNamedRootStateLocked(pending.virtualRootID, pending.baseRootID, pending.rootKey, pending.hasFormat, pending.format, pending.table)
	if err != nil {
		return err
	}
	defer func() {
		m.namedRootBufferedBytes.Add(namedRootStateBufferedBytes(state) - beforeBytes)
	}()
	if pending.table == nil {
		return nil
	}
	if state.domain != nil && state.domain.table == pending.table {
		return nil
	}
	return applyNamedRootTableToDomain(pending.virtualRootID, state.domain, pending.table)
}

func (m *rootDomainManager) applyPendingNamedRootIteratorLocked(pending *namedRootPendingIteratorMutation) error {
	if pending == nil {
		return nil
	}
	sourceTable, hasSourceTable := pending.iter.(namedRootMutationTableProvider)
	var source memtable.Table
	if hasSourceTable {
		source = sourceTable.RootMutationTable()
	}
	state, beforeBytes, err := m.ensureNamedRootStateLocked(pending.virtualRootID, pending.baseRootID, pending.rootKey, pending.hasFormat, pending.format, source)
	if err != nil {
		return err
	}
	defer func() {
		m.namedRootBufferedBytes.Add(namedRootStateBufferedBytes(state) - beforeBytes)
	}()
	if pending.iter == nil {
		return nil
	}
	defer func() { _ = pending.iter.Close() }()
	if hasSourceTable && state.domain != nil && state.domain.table == source {
		return nil
	}
	if state.domain == nil {
		domain, err := newRootDomain()
		if err != nil {
			return err
		}
		state.domain = domain
	}
	stable := false
	if stableIter, ok := pending.iter.(stableNamedRootEntryIterator); ok {
		stable = stableIter.StableUnsafeIteratorSlices()
	}
	return applyNamedRootIteratorToDomain(pending.virtualRootID, state.domain, pending.iter, source, stable)
}

func (m *rootDomainManager) ensureNamedRootStateLocked(virtualRootID, baseRootID uint64, rootKey []byte, hasFormat bool, format rootfmt.Format, sourceTable memtable.Table) (*namedRootOverlayState, int64, error) {
	state := m.namedRootsByID[virtualRootID]
	beforeBytes := namedRootStateBufferedBytes(state)
	if state == nil {
		var domain *rootDomain
		if sourceTable != nil {
			domain = &rootDomain{table: sourceTable}
		} else {
			var err error
			domain, err = newRootDomain()
			if err != nil {
				return nil, 0, err
			}
		}
		state = &namedRootOverlayState{
			virtualRootID: virtualRootID,
			baseRootID:    baseRootID,
			rootKey:       append([]byte(nil), rootKey...),
			hasFormat:     hasFormat,
			format:        format,
			domain:        domain,
		}
		if m.namedRootsByID == nil {
			m.namedRootsByID = make(map[uint64]*namedRootOverlayState)
		}
		if m.namedRootsByKey == nil {
			m.namedRootsByKey = make(map[string]*namedRootOverlayState)
		}
		m.namedRootsByID[virtualRootID] = state
		m.namedRootsByKey[string(rootKey)] = state
	}
	if hasFormat {
		state.hasFormat = true
		state.format = format
	}
	if state.domain == nil {
		domain, err := newRootDomain()
		if err != nil {
			return nil, 0, err
		}
		state.domain = domain
	}
	return state, beforeBytes, nil
}

func applyNamedRootTableToDomain(virtualRootID uint64, domain *rootDomain, table memtable.Table) error {
	if table == nil || domain == nil {
		return nil
	}
	if applier, ok := domain.table.(memtable.SortedBatchApplier); ok {
		iter := table.NewIterator(nil, nil)
		defer func() { _ = iter.Close() }()
		stable := false
		if stableTable, ok := table.(memtable.StableUnsafeIteratorTable); ok {
			stable = stableTable.StableUnsafeIteratorSlices()
		}
		ops, err := collectNamedRootIteratorOps(iter, table.Len(), stable)
		if err != nil {
			return err
		}
		applier.ApplyStealSortedBatch(ops, func(key []byte) {
			runNamedRootMemtableWriteTestHook(virtualRootID, "domain", key)
		})
		domain.version.Add(1)
		domain.bytes.Store(domain.table.Size())
		return nil
	}
	iter := table.NewIterator(nil, nil)
	defer func() { _ = iter.Close() }()
	return applyNamedRootIteratorEntries(virtualRootID, domain, iter, false)
}

func applyNamedRootIteratorToDomain(virtualRootID uint64, domain *rootDomain, iter iterator.UnsafeIterator, sourceTable memtable.Table, stable bool) error {
	if iter == nil || domain == nil {
		return nil
	}
	if applier, ok := domain.table.(memtable.SortedBatchApplier); ok {
		hint := 0
		if sourceTable != nil {
			hint = sourceTable.Len()
		}
		ops, err := collectNamedRootIteratorOps(iter, hint, stable)
		if err != nil {
			return err
		}
		applier.ApplyStealSortedBatch(ops, func(key []byte) {
			runNamedRootMemtableWriteTestHook(virtualRootID, "domain", key)
		})
		domain.version.Add(1)
		domain.bytes.Store(domain.table.Size())
		return nil
	}
	return applyNamedRootIteratorEntries(virtualRootID, domain, iter, stable)
}

func applyNamedRootIteratorEntries(virtualRootID uint64, domain *rootDomain, iter iterator.UnsafeIterator, stable bool) error {
	for iter.Valid() {
		key := iter.UnsafeKey()
		value, ptr, flags := iter.UnsafeEntry()
		if !stable {
			key = append([]byte(nil), key...)
			if flags&node.FlagTombstone == 0 && len(value) > 0 {
				value = append([]byte(nil), value...)
			}
		}
		switch {
		case flags&node.FlagTombstone != 0:
			runNamedRootOwnedWriteTestHook("delete_key", key)
			domain.hasDeletes = true
		case len(value) == 0:
			runNamedRootOwnedWriteTestHook("set_key", key)
		default:
			runNamedRootOwnedWriteTestHook("set_bytes", key)
		}
		runNamedRootMemtableWriteTestHook(virtualRootID, "domain", key)
		domain.table.SetEntrySteal(key, value, ptr, flags)
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return err
	}
	domain.version.Add(1)
	domain.bytes.Store(domain.table.Size())
	return nil
}

func (m *rootDomainManager) snapshotPendingNamedRootDomainsLocked(getSystem func([]byte) ([]byte, error)) ([]namedRootDomainSnapshot, error) {
	if len(m.namedRootsByID) == 0 {
		return nil, nil
	}
	states := make([]*namedRootOverlayState, 0, len(m.namedRootsByID))
	for _, state := range m.namedRootsByID {
		states = append(states, state)
	}
	sort.Slice(states, func(i, j int) bool {
		return states[i].virtualRootID < states[j].virtualRootID
	})
	snapshots := make([]namedRootDomainSnapshot, 0, len(states))
	for _, state := range states {
		flushTable := namedRootFlushTable(state)
		if flushTable == nil {
			return nil, fmt.Errorf("treedb: missing named-root domain for %q", state.rootKey)
		}
		raw, err := getSystem(state.rootKey)
		if err != nil {
			return nil, err
		}
		if len(raw) == 0 {
			return nil, fmt.Errorf("treedb: missing cached root descriptor for %q", state.rootKey)
		}
		snapshots = append(snapshots, namedRootDomainSnapshot{
			state:         state,
			table:         flushTable,
			descriptorRaw: raw,
		})
	}
	return snapshots, nil
}
