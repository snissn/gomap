package caching

import (
	"fmt"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type rootDomainFlushKind uint8

const (
	rootDomainFlushKindNamedRoots rootDomainFlushKind = iota + 1
	rootDomainFlushKindSystem
)

type rootDomainFlushUnit struct {
	kind rootDomainFlushKind
}

type rootDomain struct {
	table      memtable.Table
	hasDeletes bool
	version    atomic.Uint64
	bytes      atomic.Int64
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
		return db.flushNamedRootOverlaysLocked(bridge, sync)
	case rootDomainFlushKindSystem:
		return db.flushSystemOverlayLocked(bridge, sync)
	default:
		return fmt.Errorf("treedb: unknown root-domain flush unit kind %d", unit.kind)
	}
}
