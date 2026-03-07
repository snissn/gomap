package caching

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/rootfmt"
)

type rootDomainFlushKind uint8

const (
	rootDomainFlushKindNamedRoots rootDomainFlushKind = iota + 1
	rootDomainFlushKindSystem
)

type rootDomainFlushUnit struct {
	kind rootDomainFlushKind
}

type namedRootDomainSnapshot struct {
	state         *namedRootOverlayState
	table         memtable.Table
	descriptorRaw []byte
}

var rootDomainPublishHook struct {
	mu sync.RWMutex
	fn func(rootDomainFlushKind, int)
}

type rootDomain struct {
	table      memtable.Table
	hasDeletes bool
	version    atomic.Uint64
	bytes      atomic.Int64
}

type rootDomainManager struct {
	systemMu             sync.RWMutex
	systemOverlay        map[string]systemOverlayValue
	systemOverlayVersion atomic.Uint64
	systemDomain         *rootDomain

	namedRootMu                 sync.RWMutex
	namedRootsByID              map[uint64]*namedRootOverlayState
	namedRootsByKey             map[string]*namedRootOverlayState
	namedRootPublishedByVirtual map[uint64]uint64
	namedRootBufferedBytes      atomic.Int64
	nextNamedRootID             atomic.Uint64
}

type RootDomainManagerDebugState struct {
	HasSystemMutable       bool
	PendingNamedRoots      int
	PublishedNamedRoots    int
	BufferedNamedRootBytes int64
}

type SystemRootDebugState struct {
	HasMutable       bool
	QueueLen         int
	LegacyEntryCount int
}

func newRootDomainTable() (memtable.Table, error) {
	return memtable.NewWithCapacityMode(0, memtable.ModeBTree)
}

func newRootDomain() (*rootDomain, error) {
	table, err := newRootDomainTable()
	if err != nil {
		return nil, err
	}
	return &rootDomain{table: table}, nil
}

func setRootDomainPublishTestHook(fn func(rootDomainFlushKind, int)) func() {
	rootDomainPublishHook.mu.Lock()
	prev := rootDomainPublishHook.fn
	rootDomainPublishHook.fn = fn
	rootDomainPublishHook.mu.Unlock()
	return func() {
		rootDomainPublishHook.mu.Lock()
		rootDomainPublishHook.fn = prev
		rootDomainPublishHook.mu.Unlock()
	}
}

func runRootDomainPublishTestHook(kind rootDomainFlushKind, units int) {
	rootDomainPublishHook.mu.RLock()
	fn := rootDomainPublishHook.fn
	rootDomainPublishHook.mu.RUnlock()
	if fn != nil {
		fn(kind, units)
	}
}

func (d *rootDomain) pending() bool {
	return d != nil && d.table != nil && d.table.Len() > 0
}

func (d *rootDomain) bufferedBytes() int64 {
	if d == nil || d.table == nil {
		return 0
	}
	return d.table.Size()
}

func (d *rootDomain) applyEntriesOwned(entries []batch.Entry, allowPointers bool) error {
	if d == nil || len(entries) == 0 {
		return nil
	}
	if d.table == nil {
		table, err := newRootDomainTable()
		if err != nil {
			return err
		}
		d.table = table
	}
	for _, entry := range entries {
		switch entry.Type {
		case batch.OpDelete:
			d.hasDeletes = true
			d.table.SetEntrySteal(entry.Key, nil, page.ValuePtr{}, node.FlagTombstone)
		case batch.OpPut:
			if entry.IsPtr {
				if !allowPointers {
					return batch.ErrValueTooLarge
				}
				d.table.SetEntrySteal(entry.Key, entry.Value, entry.ValuePtr, node.FlagPointer)
				continue
			}
			d.table.SetEntrySteal(entry.Key, entry.Value, page.ValuePtr{}, node.FlagInline)
		}
	}
	d.version.Add(1)
	d.bytes.Store(d.table.Size())
	return nil
}

func (d *rootDomain) getEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	if d == nil || d.table == nil {
		return nil, page.ValuePtr{}, 0, false
	}
	return d.table.GetEntry(key)
}

func (d *rootDomain) newIterator(start, end []byte) (iterator.UnsafeIterator, error) {
	if d == nil || d.table == nil {
		return nil, fmt.Errorf("treedb: missing root domain table")
	}
	iter := d.table.NewIterator(start, end)
	if stable, ok := d.table.(memtable.StableUnsafeIteratorTable); ok && stable.StableUnsafeIteratorSlices() {
		return namedRootStableIterator{UnsafeIterator: iter}, nil
	}
	return iter, nil
}

func (db *DB) DebugRootDomainManagerState() RootDomainManagerDebugState {
	if db == nil {
		return RootDomainManagerDebugState{}
	}
	state := RootDomainManagerDebugState{
		BufferedNamedRootBytes: db.rootDomainManager.namedRootBufferedBytes.Load(),
	}
	db.rootDomainManager.systemMu.RLock()
	if db.rootDomainManager.systemDomain != nil && db.rootDomainManager.systemDomain.table != nil && db.rootDomainManager.systemDomain.table.Len() > 0 {
		state.HasSystemMutable = true
	}
	db.rootDomainManager.systemMu.RUnlock()

	db.rootDomainManager.namedRootMu.RLock()
	state.PendingNamedRoots = len(db.rootDomainManager.namedRootsByID)
	state.PublishedNamedRoots = len(db.rootDomainManager.namedRootPublishedByVirtual)
	db.rootDomainManager.namedRootMu.RUnlock()
	return state
}

func (db *DB) PendingRootDomains() bool {
	return db.pendingRootDomainUnitCount() > 0
}

func (db *DB) pendingRootDomainUnits() []rootDomainFlushUnit {
	if db == nil {
		return nil
	}
	units := make([]rootDomainFlushUnit, 0, 2)
	if db.PendingNamedRoots() {
		units = append(units, rootDomainFlushUnit{kind: rootDomainFlushKindNamedRoots})
	}
	if db.PendingSystemOverlay() {
		units = append(units, rootDomainFlushUnit{kind: rootDomainFlushKindSystem})
	}
	return units
}

func (db *DB) pendingRootDomainUnitCount() int {
	if db == nil {
		return 0
	}
	count := 0
	if db.PendingNamedRoots() {
		count++
	}
	if db.PendingSystemOverlay() {
		count++
	}
	return count
}

func (db *DB) flushPendingRootDomainUnits(sync bool) error {
	if db == nil {
		return errDBClosing
	}
	if db.pendingRootDomainUnitCount() == 0 {
		return nil
	}
	db.waitForCheckpoint()
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	bridge, err := db.directBridge()
	if err != nil {
		return err
	}
	return db.flushPendingRootDomainUnitsLocked(bridge, sync)
}

func (db *DB) flushPendingRootDomainUnitsLocked(bridge BackendDirectBridge, sync bool) error {
	for {
		flushed, err := db.flushOnePendingRootDomainUnitLocked(bridge, sync)
		if err != nil {
			return err
		}
		if !flushed {
			return nil
		}
	}
}

func (db *DB) flushOnePendingRootDomainUnitLocked(bridge BackendDirectBridge, sync bool) (bool, error) {
	count := db.pendingRootDomainUnitCount()
	if count == 0 {
		return false, nil
	}
	unit := rootDomainFlushUnit{kind: rootDomainFlushKindNamedRoots}
	if !db.PendingNamedRoots() {
		unit.kind = rootDomainFlushKindSystem
	}
	if err := db.flushRootDomainUnitLocked(bridge, unit, sync); err != nil {
		return false, err
	}
	return true, nil
}

func (db *DB) flushRootDomainUnitLocked(bridge BackendDirectBridge, unit rootDomainFlushUnit, sync bool) error {
	switch unit.kind {
	case rootDomainFlushKindNamedRoots:
		return db.publishNamedRootDomainsLocked(bridge, sync)
	case rootDomainFlushKindSystem:
		return db.publishSystemRootDomainLocked(bridge, sync)
	default:
		return fmt.Errorf("treedb: unknown root-domain flush unit kind %d", unit.kind)
	}
}

func (db *DB) publishSystemRootDomainLocked(bridge BackendDirectBridge, sync bool) error {
	if db == nil {
		return errDBClosing
	}
	db.rootDomainManager.systemMu.Lock()
	defer db.rootDomainManager.systemMu.Unlock()
	if db.rootDomainManager.systemDomain == nil || !db.rootDomainManager.systemDomain.pending() {
		return nil
	}
	runRootDomainPublishTestHook(rootDomainFlushKindSystem, 1)
	flushTable := db.rootDomainManager.systemDomain.table
	if err := bridge.ApplySystemTable(sync, flushTable); err != nil {
		return err
	}
	nextDomain, err := newRootDomain()
	if err != nil {
		return err
	}
	db.rootDomainManager.systemDomain = nextDomain
	db.rootDomainManager.systemDomain.version.Add(1)
	return nil
}

func (db *DB) publishNamedRootDomainsLocked(bridge BackendDirectBridge, sync bool) error {
	if db == nil {
		return errDBClosing
	}
	db.rootDomainManager.namedRootMu.Lock()
	defer db.rootDomainManager.namedRootMu.Unlock()
	snapshots, err := db.snapshotPendingNamedRootDomainsLocked()
	if err != nil {
		return err
	}
	if len(snapshots) == 0 {
		return nil
	}
	if err := runNamedRootPublishTestHook("before_publish"); err != nil {
		return err
	}
	runRootDomainPublishTestHook(rootDomainFlushKindNamedRoots, len(snapshots))

	var (
		updatedSystemEntries []batch.Entry
		publishedRootIDs     []uint64
	)
	rootIDs := make([]uint64, len(snapshots))
	formats := make([]*rootfmt.Format, len(snapshots))
	rootTables := make([]memtable.Table, len(snapshots))
	for i := range snapshots {
		snapshot := snapshots[i]
		rootIDs[i] = snapshot.state.baseRootID
		if snapshot.state.hasFormat {
			format := snapshot.state.format
			formats[i] = &format
		}
		rootTables[i] = snapshot.table
	}

	runNamedRootBulkPublishOpsTestHook(len(rootIDs))
	runNamedRootBulkPublishTablesTestHook(len(rootIDs))
	_, err = bridge.MutateRootsWithFormatTables(sync, rootIDs, formats, rootTables, func(newRootIDs []uint64) ([]batch.Entry, error) {
		if len(newRootIDs) != len(snapshots) {
			return nil, fmt.Errorf("treedb: named-root checkpoint publish mismatch")
		}
		publishedRootIDs = append(publishedRootIDs[:0], newRootIDs...)
		updatedSystemEntries = make([]batch.Entry, 0, len(newRootIDs))
		for i := range snapshots {
			encoded, err := collections.UpdateEncodedCollectionRootDescriptorRootPageID(snapshots[i].descriptorRaw, newRootIDs[i])
			if err != nil {
				return nil, err
			}
			updatedSystemEntries = append(updatedSystemEntries, batch.Entry{
				Type:  batch.OpPut,
				Key:   append([]byte(nil), snapshots[i].state.rootKey...),
				Value: encoded,
			})
		}
		return updatedSystemEntries, nil
	})
	if err != nil {
		return err
	}
	if err := db.refreshBackendDirectValueLogRetention(bridge); err != nil {
		return err
	}
	if err := db.ApplySystemOverlayEntriesOwned(updatedSystemEntries); err != nil {
		return err
	}
	if len(publishedRootIDs) > 0 {
		if db.rootDomainManager.namedRootPublishedByVirtual == nil {
			db.rootDomainManager.namedRootPublishedByVirtual = make(map[uint64]uint64, len(publishedRootIDs))
		}
		for i := range snapshots {
			db.rootDomainManager.namedRootPublishedByVirtual[snapshots[i].state.virtualRootID] = publishedRootIDs[i]
		}
	}
	clear(db.rootDomainManager.namedRootsByID)
	clear(db.rootDomainManager.namedRootsByKey)
	db.rootDomainManager.namedRootBufferedBytes.Store(0)
	return nil
}

func (db *DB) snapshotPendingNamedRootDomainsLocked() ([]namedRootDomainSnapshot, error) {
	if len(db.rootDomainManager.namedRootsByID) == 0 {
		return nil, nil
	}
	states := make([]*namedRootOverlayState, 0, len(db.rootDomainManager.namedRootsByID))
	for _, state := range db.rootDomainManager.namedRootsByID {
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
		raw, err := db.GetSystem(state.rootKey)
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
