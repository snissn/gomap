package caching

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/rootfmt"
)

const virtualNamedRootMask uint64 = 1 << 63

type NamedRootDebugState struct {
	HasPointState    bool
	HasPrefixState   bool
	SharedState      bool
	LegacyEntryCount int
}

type namedRootOverlayState struct {
	virtualRootID uint64
	baseRootID    uint64
	rootKey       []byte
	hasFormat     bool
	format        rootfmt.Format
	pointState    memtable.Table
	prefixState   memtable.Table
	entries       map[string]namedRootOverlayValue
}

type namedRootOverlayValue struct {
	key     []byte
	value   []byte
	deleted bool
}

type namedRootPendingMutation struct {
	virtualRootID uint64
	baseRootID    uint64
	rootKey       []byte
	hasFormat     bool
	format        rootfmt.Format
	entries       []batch.Entry
	entryScratch  [8]batch.Entry
}

type overlayEntryBatch struct {
	entries []batch.Entry
	closed  bool
}

type namedRootIterEntry struct {
	key     []byte
	value   []byte
	deleted bool
}

type namedRootOverlayIterator struct {
	entries []namedRootIterEntry
	index   int
	start   []byte
	end     []byte
}

type namedRootMergeSource uint8

const (
	namedRootMergeSourceNone namedRootMergeSource = iota
	namedRootMergeSourceOverlay
	namedRootMergeSourceBase
)

type namedRootMergeIterator struct {
	overlay        iterator.UnsafeIterator
	base           iterator.UnsafeIterator
	current        namedRootMergeSource
	skipBaseOnNext bool
	start          []byte
	end            []byte
}

var namedRootPublishHook struct {
	mu sync.RWMutex
	fn func(string) error
}

var namedRootBufferedIteratorHook struct {
	mu sync.RWMutex
	fn func(rootID uint64, start, end []byte)
}

var namedRootOverlayEntryReadHook struct {
	mu sync.RWMutex
	fn func(op string, rootID uint64, key []byte)
}

var namedRootOverlayPrefixScanHook struct {
	mu sync.RWMutex
	fn func(rootID uint64, prefix []byte)
}

var namedRootLegacyIteratorMaterializeHook struct {
	mu sync.RWMutex
	fn func(rootID uint64, start, end []byte)
}

var namedRootLegacyFlushSnapshotHook struct {
	mu sync.RWMutex
	fn func(rootID uint64)
}

var namedRootMemtableWriteHook struct {
	mu sync.RWMutex
	fn func(rootID uint64, kind string, key []byte)
}

var namedRootOwnedWriteHook struct {
	mu sync.RWMutex
	fn func(kind string, key []byte)
}

var namedRootBulkPublishOpsHook struct {
	mu sync.RWMutex
	fn func(int)
}

func setNamedRootPublishTestHook(fn func(string) error) func() {
	namedRootPublishHook.mu.Lock()
	prev := namedRootPublishHook.fn
	namedRootPublishHook.fn = fn
	namedRootPublishHook.mu.Unlock()
	return func() {
		namedRootPublishHook.mu.Lock()
		namedRootPublishHook.fn = prev
		namedRootPublishHook.mu.Unlock()
	}
}

func SetNamedRootPublishTestHook(fn func(string) error) func() {
	return setNamedRootPublishTestHook(fn)
}

func runNamedRootPublishTestHook(stage string) error {
	namedRootPublishHook.mu.RLock()
	fn := namedRootPublishHook.fn
	namedRootPublishHook.mu.RUnlock()
	if fn == nil {
		return nil
	}
	return fn(stage)
}

func setNamedRootBufferedIteratorTestHook(fn func(rootID uint64, start, end []byte)) func() {
	namedRootBufferedIteratorHook.mu.Lock()
	prev := namedRootBufferedIteratorHook.fn
	namedRootBufferedIteratorHook.fn = fn
	namedRootBufferedIteratorHook.mu.Unlock()
	return func() {
		namedRootBufferedIteratorHook.mu.Lock()
		namedRootBufferedIteratorHook.fn = prev
		namedRootBufferedIteratorHook.mu.Unlock()
	}
}

func SetNamedRootBufferedIteratorTestHook(fn func(rootID uint64, start, end []byte)) func() {
	return setNamedRootBufferedIteratorTestHook(fn)
}

func runNamedRootBufferedIteratorTestHook(rootID uint64, start, end []byte) {
	namedRootBufferedIteratorHook.mu.RLock()
	fn := namedRootBufferedIteratorHook.fn
	namedRootBufferedIteratorHook.mu.RUnlock()
	if fn != nil {
		fn(rootID, start, end)
	}
}

func setNamedRootOverlayEntryReadTestHook(fn func(op string, rootID uint64, key []byte)) func() {
	namedRootOverlayEntryReadHook.mu.Lock()
	prev := namedRootOverlayEntryReadHook.fn
	namedRootOverlayEntryReadHook.fn = fn
	namedRootOverlayEntryReadHook.mu.Unlock()
	return func() {
		namedRootOverlayEntryReadHook.mu.Lock()
		namedRootOverlayEntryReadHook.fn = prev
		namedRootOverlayEntryReadHook.mu.Unlock()
	}
}

func SetNamedRootOverlayEntryReadTestHook(fn func(op string, rootID uint64, key []byte)) func() {
	return setNamedRootOverlayEntryReadTestHook(fn)
}

func runNamedRootOverlayEntryReadTestHook(op string, rootID uint64, key []byte) {
	namedRootOverlayEntryReadHook.mu.RLock()
	fn := namedRootOverlayEntryReadHook.fn
	namedRootOverlayEntryReadHook.mu.RUnlock()
	if fn != nil {
		fn(op, rootID, key)
	}
}

func setNamedRootOverlayPrefixScanTestHook(fn func(rootID uint64, prefix []byte)) func() {
	namedRootOverlayPrefixScanHook.mu.Lock()
	prev := namedRootOverlayPrefixScanHook.fn
	namedRootOverlayPrefixScanHook.fn = fn
	namedRootOverlayPrefixScanHook.mu.Unlock()
	return func() {
		namedRootOverlayPrefixScanHook.mu.Lock()
		namedRootOverlayPrefixScanHook.fn = prev
		namedRootOverlayPrefixScanHook.mu.Unlock()
	}
}

func SetNamedRootOverlayPrefixScanTestHook(fn func(rootID uint64, prefix []byte)) func() {
	return setNamedRootOverlayPrefixScanTestHook(fn)
}

func runNamedRootOverlayPrefixScanTestHook(rootID uint64, prefix []byte) {
	namedRootOverlayPrefixScanHook.mu.RLock()
	fn := namedRootOverlayPrefixScanHook.fn
	namedRootOverlayPrefixScanHook.mu.RUnlock()
	if fn != nil {
		fn(rootID, prefix)
	}
}

func setNamedRootLegacyIteratorMaterializeTestHook(fn func(rootID uint64, start, end []byte)) func() {
	namedRootLegacyIteratorMaterializeHook.mu.Lock()
	prev := namedRootLegacyIteratorMaterializeHook.fn
	namedRootLegacyIteratorMaterializeHook.fn = fn
	namedRootLegacyIteratorMaterializeHook.mu.Unlock()
	return func() {
		namedRootLegacyIteratorMaterializeHook.mu.Lock()
		namedRootLegacyIteratorMaterializeHook.fn = prev
		namedRootLegacyIteratorMaterializeHook.mu.Unlock()
	}
}

func SetNamedRootLegacyIteratorMaterializeTestHook(fn func(rootID uint64, start, end []byte)) func() {
	return setNamedRootLegacyIteratorMaterializeTestHook(fn)
}

func runNamedRootLegacyIteratorMaterializeTestHook(rootID uint64, start, end []byte) {
	namedRootLegacyIteratorMaterializeHook.mu.RLock()
	fn := namedRootLegacyIteratorMaterializeHook.fn
	namedRootLegacyIteratorMaterializeHook.mu.RUnlock()
	if fn != nil {
		fn(rootID, start, end)
	}
}

func setNamedRootLegacyFlushSnapshotTestHook(fn func(rootID uint64)) func() {
	namedRootLegacyFlushSnapshotHook.mu.Lock()
	prev := namedRootLegacyFlushSnapshotHook.fn
	namedRootLegacyFlushSnapshotHook.fn = fn
	namedRootLegacyFlushSnapshotHook.mu.Unlock()
	return func() {
		namedRootLegacyFlushSnapshotHook.mu.Lock()
		namedRootLegacyFlushSnapshotHook.fn = prev
		namedRootLegacyFlushSnapshotHook.mu.Unlock()
	}
}

func SetNamedRootLegacyFlushSnapshotTestHook(fn func(rootID uint64)) func() {
	return setNamedRootLegacyFlushSnapshotTestHook(fn)
}

func runNamedRootLegacyFlushSnapshotTestHook(rootID uint64) {
	namedRootLegacyFlushSnapshotHook.mu.RLock()
	fn := namedRootLegacyFlushSnapshotHook.fn
	namedRootLegacyFlushSnapshotHook.mu.RUnlock()
	if fn != nil {
		fn(rootID)
	}
}

func setNamedRootMemtableWriteTestHook(fn func(rootID uint64, kind string, key []byte)) func() {
	namedRootMemtableWriteHook.mu.Lock()
	prev := namedRootMemtableWriteHook.fn
	namedRootMemtableWriteHook.fn = fn
	namedRootMemtableWriteHook.mu.Unlock()
	return func() {
		namedRootMemtableWriteHook.mu.Lock()
		namedRootMemtableWriteHook.fn = prev
		namedRootMemtableWriteHook.mu.Unlock()
	}
}

func SetNamedRootMemtableWriteTestHook(fn func(rootID uint64, kind string, key []byte)) func() {
	return setNamedRootMemtableWriteTestHook(fn)
}

func runNamedRootMemtableWriteTestHook(rootID uint64, kind string, key []byte) {
	namedRootMemtableWriteHook.mu.RLock()
	fn := namedRootMemtableWriteHook.fn
	namedRootMemtableWriteHook.mu.RUnlock()
	if fn != nil {
		fn(rootID, kind, key)
	}
}

func setNamedRootOwnedWriteTestHook(fn func(kind string, key []byte)) func() {
	namedRootOwnedWriteHook.mu.Lock()
	prev := namedRootOwnedWriteHook.fn
	namedRootOwnedWriteHook.fn = fn
	namedRootOwnedWriteHook.mu.Unlock()
	return func() {
		namedRootOwnedWriteHook.mu.Lock()
		namedRootOwnedWriteHook.fn = prev
		namedRootOwnedWriteHook.mu.Unlock()
	}
}

func SetNamedRootOwnedWriteTestHook(fn func(kind string, key []byte)) func() {
	return setNamedRootOwnedWriteTestHook(fn)
}

func setNamedRootBulkPublishOpsTestHook(fn func(int)) func() {
	namedRootBulkPublishOpsHook.mu.Lock()
	prev := namedRootBulkPublishOpsHook.fn
	namedRootBulkPublishOpsHook.fn = fn
	namedRootBulkPublishOpsHook.mu.Unlock()
	return func() {
		namedRootBulkPublishOpsHook.mu.Lock()
		namedRootBulkPublishOpsHook.fn = prev
		namedRootBulkPublishOpsHook.mu.Unlock()
	}
}

func SetNamedRootBulkPublishOpsTestHook(fn func(int)) func() {
	return setNamedRootBulkPublishOpsTestHook(fn)
}

func runNamedRootBulkPublishOpsTestHook(rootCount int) {
	namedRootBulkPublishOpsHook.mu.RLock()
	fn := namedRootBulkPublishOpsHook.fn
	namedRootBulkPublishOpsHook.mu.RUnlock()
	if fn != nil {
		fn(rootCount)
	}
}

func runNamedRootOwnedWriteTestHook(kind string, key []byte) {
	namedRootOwnedWriteHook.mu.RLock()
	fn := namedRootOwnedWriteHook.fn
	namedRootOwnedWriteHook.mu.RUnlock()
	if fn != nil {
		fn(kind, key)
	}
}

func (db *DB) HasBufferedNamedRoot(rootID uint64) bool {
	if db == nil || rootID == 0 {
		return false
	}
	db.namedRootMu.RLock()
	defer db.namedRootMu.RUnlock()
	_, ok := db.namedRootsByID[rootID]
	return ok
}

func (db *DB) BufferedGetAtRoot(rootID uint64, key []byte) ([]byte, error) {
	state, err := db.namedRootState(rootID)
	if err != nil {
		return nil, err
	}
	if state.pointState != nil {
		value, _, flags, found := state.pointState.GetEntry(key)
		if found {
			if flags&node.FlagTombstone != 0 {
				return nil, nil
			}
			return append([]byte(nil), value...), nil
		}
	} else {
		runNamedRootOverlayEntryReadTestHook("get", rootID, key)
		if entry, ok := state.entries[string(key)]; ok {
			if entry.deleted {
				return nil, nil
			}
			return append([]byte(nil), entry.value...), nil
		}
	}
	bridge, err := db.directBridge()
	if err != nil {
		return nil, err
	}
	return bridge.GetAtRoot(state.baseRootID, key)
}

func (db *DB) BufferedGetAtRootAppend(rootID uint64, key, dst []byte) ([]byte, error) {
	state, err := db.namedRootState(rootID)
	if err != nil {
		return dst, err
	}
	if state.pointState != nil {
		value, _, flags, found := state.pointState.GetEntry(key)
		if found {
			if flags&node.FlagTombstone != 0 {
				return dst, nil
			}
			return append(dst, value...), nil
		}
	} else {
		runNamedRootOverlayEntryReadTestHook("get_append", rootID, key)
		if entry, ok := state.entries[string(key)]; ok {
			if entry.deleted {
				return dst, nil
			}
			return append(dst, entry.value...), nil
		}
	}
	bridge, err := db.directBridge()
	if err != nil {
		return dst, err
	}
	return bridge.GetAtRootAppend(state.baseRootID, key, dst)
}

func (db *DB) BufferedHasAtRoot(rootID uint64, key []byte) (bool, error) {
	state, err := db.namedRootState(rootID)
	if err != nil {
		return false, err
	}
	if state.pointState != nil {
		_, _, flags, found := state.pointState.GetEntry(key)
		if found {
			return flags&node.FlagTombstone == 0, nil
		}
	} else {
		runNamedRootOverlayEntryReadTestHook("has", rootID, key)
		if entry, ok := state.entries[string(key)]; ok {
			return !entry.deleted, nil
		}
	}
	bridge, err := db.directBridge()
	if err != nil {
		return false, err
	}
	return bridge.HasAtRoot(state.baseRootID, key)
}

func (db *DB) BufferedHasPrefixAtRoot(rootID uint64, prefix []byte) (bool, error) {
	state, err := db.namedRootState(rootID)
	if err != nil {
		return false, err
	}

	if state.prefixState != nil {
		overlayIter := state.prefixState.NewIterator(prefix, nil)
		defer func() { _ = overlayIter.Close() }()
		for overlayIter.Valid() {
			key := overlayIter.UnsafeKey()
			if !bytes.HasPrefix(key, prefix) {
				break
			}
			if !overlayIter.IsDeleted() {
				return true, nil
			}
			overlayIter.Next()
		}
		if err := overlayIter.Error(); err != nil {
			return false, err
		}
	} else {
		runNamedRootOverlayPrefixScanTestHook(rootID, prefix)
		db.namedRootMu.RLock()
		for _, entry := range state.entries {
			if entry.deleted || !bytes.HasPrefix(entry.key, prefix) {
				continue
			}
			db.namedRootMu.RUnlock()
			return true, nil
		}
		db.namedRootMu.RUnlock()
	}

	bridge, err := db.directBridge()
	if err != nil {
		return false, err
	}
	baseIter, err := bridge.IteratorAtRoot(state.baseRootID, prefix, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = baseIter.Close() }()
	for baseIter.Valid() {
		key := baseIter.UnsafeKey()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		if state.pointState != nil {
			_, _, flags, found := state.pointState.GetEntry(key)
			if found {
				if flags&node.FlagTombstone != 0 {
					baseIter.Next()
					continue
				}
				return true, nil
			}
		} else {
			db.namedRootMu.RLock()
			overlayEntry, ok := state.entries[string(key)]
			db.namedRootMu.RUnlock()
			if ok {
				if overlayEntry.deleted {
					baseIter.Next()
					continue
				}
				return true, nil
			}
		}
		if !baseIter.IsDeleted() {
			return true, nil
		}
		baseIter.Next()
	}
	if err := baseIter.Error(); err != nil {
		return false, err
	}
	return false, nil
}

func (db *DB) BufferedIteratorAtRoot(rootID uint64, start, end []byte) (iterator.UnsafeIterator, error) {
	runNamedRootBufferedIteratorTestHook(rootID, start, end)
	state, err := db.namedRootState(rootID)
	if err != nil {
		return nil, err
	}
	bridge, err := db.directBridge()
	if err != nil {
		return nil, err
	}
	if state.prefixState != nil {
		overlayIter := state.prefixState.NewIterator(start, end)
		baseIter, err := bridge.IteratorAtRoot(state.baseRootID, start, end)
		if err != nil {
			_ = overlayIter.Close()
			return nil, err
		}
		return newNamedRootMergeIterator(overlayIter, baseIter, start, end), nil
	}

	runNamedRootLegacyIteratorMaterializeTestHook(rootID, start, end)
	merged := make(map[string]namedRootIterEntry, len(state.entries))
	baseIter, err := bridge.IteratorAtRoot(state.baseRootID, start, end)
	if err != nil {
		return nil, err
	}
	defer func() { _ = baseIter.Close() }()
	for baseIter.Valid() {
		key := baseIter.KeyCopy(nil)
		entry := namedRootIterEntry{key: key}
		if baseIter.IsDeleted() {
			entry.deleted = true
		} else {
			entry.value = baseIter.ValueCopy(nil)
		}
		merged[string(key)] = entry
		baseIter.Next()
	}
	if err := baseIter.Error(); err != nil {
		return nil, err
	}

	db.namedRootMu.RLock()
	for _, entry := range state.entries {
		if !namedRootKeyInRange(entry.key, start, end) {
			continue
		}
		merged[string(entry.key)] = namedRootIterEntry{
			key:     append([]byte(nil), entry.key...),
			value:   append([]byte(nil), entry.value...),
			deleted: entry.deleted,
		}
	}
	db.namedRootMu.RUnlock()

	keys := make([]string, 0, len(merged))
	for key := range merged {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare([]byte(keys[i]), []byte(keys[j])) < 0
	})
	out := make([]namedRootIterEntry, 0, len(keys))
	for _, key := range keys {
		out = append(out, merged[key])
	}
	return &namedRootOverlayIterator{
		entries: out,
		start:   append([]byte(nil), start...),
		end:     append([]byte(nil), end...),
	}, nil
}

func (db *DB) BufferNamedRootMutations(sync bool, rootIDs []uint64, formats []*rootfmt.Format, mutateRoots []func(batch.Interface) error, updateSystem func(batch.Interface, []uint64) error) ([]uint64, error) {
	if db == nil {
		return nil, errDBClosing
	}
	if len(rootIDs) != len(mutateRoots) {
		return nil, fmt.Errorf("named root mutation length mismatch")
	}

	rootOps := make([][]batch.Entry, len(rootIDs))
	for i := range rootIDs {
		var opScratch [8]batch.Entry
		b := newOverlayEntryBatch(opScratch[:0])
		if mutateRoots[i] != nil {
			if err := mutateRoots[i](&b); err != nil {
				return nil, err
			}
		}
		rootOps[i] = b.entries
	}
	return db.BufferNamedRootMutationsOps(sync, rootIDs, formats, rootOps, func(newRootIDs []uint64) ([]batch.Entry, error) {
		if updateSystem == nil {
			return nil, nil
		}
		var sysScratch [8]batch.Entry
		sys := newOverlayEntryBatch(sysScratch[:0])
		if err := updateSystem(&sys, newRootIDs); err != nil {
			return nil, err
		}
		return sys.entries, nil
	})
}

func (db *DB) BufferNamedRootMutationsOps(sync bool, rootIDs []uint64, formats []*rootfmt.Format, rootOps [][]batch.Entry, buildSystemOps func([]uint64) ([]batch.Entry, error)) ([]uint64, error) {
	if db == nil {
		return nil, errDBClosing
	}
	if len(rootIDs) != len(rootOps) {
		return nil, fmt.Errorf("named root ops length mismatch")
	}

	db.namedRootMu.Lock()
	virtualRootIDs := make([]uint64, len(rootIDs))
	pending := make([]namedRootPendingMutation, len(rootIDs))
	for i := range rootIDs {
		existing := db.namedRootsByID[rootIDs[i]]
		if existing != nil {
			virtualRootIDs[i] = existing.virtualRootID
			pending[i] = namedRootPendingMutation{
				virtualRootID: existing.virtualRootID,
				baseRootID:    existing.baseRootID,
				rootKey:       append([]byte(nil), existing.rootKey...),
				hasFormat:     existing.hasFormat,
				format:        existing.format,
			}
		} else {
			virtualRootIDs[i] = db.nextVirtualNamedRootIDLocked()
			pending[i] = namedRootPendingMutation{
				virtualRootID: virtualRootIDs[i],
				baseRootID:    rootIDs[i],
			}
		}
		if format := rootFormatAt(formats, i); format != nil {
			pending[i].hasFormat = true
			pending[i].format = *format
		}
		pending[i].entries = rootOps[i]
	}

	var sysEntries []batch.Entry
	if buildSystemOps != nil {
		ops, err := buildSystemOps(virtualRootIDs)
		if err != nil {
			db.namedRootMu.Unlock()
			return nil, err
		}
		sysEntries = ops
	}
	if err := db.bindPendingNamedRootKeysLocked(pending, sysEntries); err != nil {
		db.namedRootMu.Unlock()
		return nil, err
	}
	for i := range pending {
		db.applyPendingNamedRootLocked(&pending[i])
	}
	if err := db.ApplySystemOverlayEntriesOwned(sysEntries); err != nil {
		db.namedRootMu.Unlock()
		return nil, err
	}
	db.namedRootMu.Unlock()

	if sync {
		if err := db.FlushNamedRootOverlays(true); err != nil {
			return nil, err
		}
		if err := db.Checkpoint(); err != nil {
			return nil, err
		}
	}
	return virtualRootIDs, nil
}

func (db *DB) FlushNamedRootOverlays(sync bool) error {
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
	return db.flushNamedRootOverlaysLocked(bridge, sync)
}

func (db *DB) flushNamedRootOverlaysLocked(bridge BackendDirectBridge, sync bool) error {
	if db == nil {
		return errDBClosing
	}
	db.namedRootMu.Lock()
	defer db.namedRootMu.Unlock()
	if len(db.namedRootsByID) == 0 {
		return nil
	}
	if err := runNamedRootPublishTestHook("before_publish"); err != nil {
		return err
	}

	states := make([]*namedRootOverlayState, 0, len(db.namedRootsByID))
	for _, state := range db.namedRootsByID {
		states = append(states, state)
	}
	sort.Slice(states, func(i, j int) bool {
		return states[i].virtualRootID < states[j].virtualRootID
	})

	type rootFlushSnapshot struct {
		state         *namedRootOverlayState
		entries       []batch.Entry
		descriptorRaw []byte
	}
	snapshots := make([]rootFlushSnapshot, 0, len(states))
	for _, state := range states {
		var entries []batch.Entry
		if state.prefixState != nil {
			iter := state.prefixState.NewIterator(nil, nil)
			entries = make([]batch.Entry, 0, state.prefixState.Len())
			for iter.Valid() {
				next := batch.Entry{
					Key: append([]byte(nil), iter.UnsafeKey()...),
				}
				if iter.IsDeleted() {
					next.Type = batch.OpDelete
				} else {
					next.Type = batch.OpPut
					next.Value = append([]byte(nil), iter.UnsafeValue()...)
				}
				entries = append(entries, next)
				iter.Next()
			}
			if err := iter.Error(); err != nil {
				_ = iter.Close()
				return err
			}
			if err := iter.Close(); err != nil {
				return err
			}
		} else {
			runNamedRootLegacyFlushSnapshotTestHook(state.virtualRootID)
			entries = make([]batch.Entry, 0, len(state.entries))
			keys := make([]string, 0, len(state.entries))
			for key := range state.entries {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				entry := state.entries[key]
				next := batch.Entry{Key: append([]byte(nil), entry.key...)}
				if entry.deleted {
					next.Type = batch.OpDelete
				} else {
					next.Type = batch.OpPut
					next.Value = append([]byte(nil), entry.value...)
				}
				entries = append(entries, next)
			}
		}
		raw, err := db.GetSystem(state.rootKey)
		if err != nil {
			return err
		}
		if len(raw) == 0 {
			return fmt.Errorf("treedb: missing cached root descriptor for %q", state.rootKey)
		}
		snapshots = append(snapshots, rootFlushSnapshot{
			state:         state,
			entries:       entries,
			descriptorRaw: raw,
		})
	}

	var updatedSystemEntries []batch.Entry
	err := func(bridge BackendDirectBridge) error {
		rootIDs := make([]uint64, len(snapshots))
		formats := make([]*rootfmt.Format, len(snapshots))
		rootOps := make([][]batch.Entry, len(snapshots))
		for i := range snapshots {
			snapshot := snapshots[i]
			rootIDs[i] = snapshot.state.baseRootID
			if snapshot.state.hasFormat {
				format := snapshot.state.format
				formats[i] = &format
			}
			rootOps[i] = snapshot.entries
		}

		runNamedRootBulkPublishOpsTestHook(len(rootIDs))
		_, err := bridge.MutateRootsWithFormatOps(sync, rootIDs, formats, rootOps, func(newRootIDs []uint64) ([]batch.Entry, error) {
			if len(newRootIDs) != len(snapshots) {
				return nil, fmt.Errorf("treedb: named-root checkpoint publish mismatch")
			}
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
		return err
	}(bridge)
	if err != nil {
		return err
	}
	if err := db.refreshBackendDirectValueLogRetention(bridge); err != nil {
		return err
	}
	if err := db.ApplySystemOverlayEntriesOwned(updatedSystemEntries); err != nil {
		return err
	}
	clear(db.namedRootsByID)
	clear(db.namedRootsByKey)
	return nil
}

func (db *DB) PendingNamedRoots() bool {
	if db == nil {
		return false
	}
	db.namedRootMu.RLock()
	defer db.namedRootMu.RUnlock()
	return len(db.namedRootsByID) > 0
}

func (db *DB) DebugNamedRootStateByID(rootID uint64) (NamedRootDebugState, bool) {
	if db == nil {
		return NamedRootDebugState{}, false
	}
	db.namedRootMu.RLock()
	defer db.namedRootMu.RUnlock()
	state := db.namedRootsByID[rootID]
	if state == nil {
		return NamedRootDebugState{}, false
	}
	return NamedRootDebugState{
		HasPointState:    state.pointState != nil,
		HasPrefixState:   state.prefixState != nil,
		SharedState:      state.pointState != nil && state.pointState == state.prefixState,
		LegacyEntryCount: len(state.entries),
	}, true
}

func (db *DB) namedRootState(rootID uint64) (*namedRootOverlayState, error) {
	db.namedRootMu.RLock()
	state := db.namedRootsByID[rootID]
	db.namedRootMu.RUnlock()
	if state == nil {
		return nil, fmt.Errorf("treedb: named root %d not buffered", rootID)
	}
	return state, nil
}

func (db *DB) nextVirtualNamedRootIDLocked() uint64 {
	return virtualNamedRootMask | db.nextNamedRootID.Add(1)
}

func (db *DB) bindPendingNamedRootKeysLocked(pending []namedRootPendingMutation, sysEntries []batch.Entry) error {
	for _, entry := range sysEntries {
		if entry.Type != batch.OpPut || len(entry.Value) == 0 {
			continue
		}
		var desc collections.CollectionRootDescriptor
		if err := desc.Decode(entry.Value); err != nil {
			continue
		}
		for i := range pending {
			if pending[i].virtualRootID != desc.RootPageID || len(pending[i].rootKey) > 0 {
				continue
			}
			pending[i].rootKey = append([]byte(nil), entry.Key...)
			break
		}
	}
	for i := range pending {
		if len(pending[i].rootKey) == 0 {
			return fmt.Errorf("treedb: missing root key for buffered root %d", pending[i].virtualRootID)
		}
	}
	return nil
}

func (db *DB) applyPendingNamedRootLocked(pending *namedRootPendingMutation) {
	if pending == nil {
		return
	}
	state := db.namedRootsByID[pending.virtualRootID]
	if state == nil {
		sharedState, err := memtable.NewWithCapacityMode(0, memtable.ModeBTree)
		if err != nil {
			panic(fmt.Sprintf("treedb: create named-root shared state: %v", err))
		}
		state = &namedRootOverlayState{
			virtualRootID: pending.virtualRootID,
			baseRootID:    pending.baseRootID,
			rootKey:       append([]byte(nil), pending.rootKey...),
			hasFormat:     pending.hasFormat,
			format:        pending.format,
			pointState:    sharedState,
			prefixState:   sharedState,
		}
		if db.namedRootsByID == nil {
			db.namedRootsByID = make(map[uint64]*namedRootOverlayState)
		}
		if db.namedRootsByKey == nil {
			db.namedRootsByKey = make(map[string]*namedRootOverlayState)
		}
		db.namedRootsByID[pending.virtualRootID] = state
		db.namedRootsByKey[string(pending.rootKey)] = state
	}
	if pending.hasFormat {
		state.hasFormat = true
		state.format = pending.format
	}
	for _, entry := range pending.entries {
		if entry.Type == batch.OpDelete {
			if state.pointState != nil {
				runNamedRootMemtableWriteTestHook(pending.virtualRootID, "point", entry.Key)
				state.pointState.SetEntrySteal(entry.Key, nil, page.ValuePtr{}, node.FlagTombstone)
			}
			if state.prefixState != nil && state.prefixState != state.pointState {
				runNamedRootMemtableWriteTestHook(pending.virtualRootID, "prefix", entry.Key)
				state.prefixState.SetEntrySteal(entry.Key, nil, page.ValuePtr{}, node.FlagTombstone)
			}
			continue
		}
		if state.pointState != nil {
			runNamedRootMemtableWriteTestHook(pending.virtualRootID, "point", entry.Key)
			state.pointState.SetEntrySteal(entry.Key, entry.Value, page.ValuePtr{}, node.FlagInline)
		}
		if state.prefixState != nil && state.prefixState != state.pointState {
			runNamedRootMemtableWriteTestHook(pending.virtualRootID, "prefix", entry.Key)
			state.prefixState.SetEntrySteal(entry.Key, entry.Value, page.ValuePtr{}, node.FlagInline)
		}
	}
}

func rootFormatAt(formats []*rootfmt.Format, idx int) *rootfmt.Format {
	if idx < 0 || idx >= len(formats) {
		return nil
	}
	return formats[idx]
}

func namedRootKeyInRange(key, start, end []byte) bool {
	if len(start) > 0 && bytes.Compare(key, start) < 0 {
		return false
	}
	if len(end) > 0 && bytes.Compare(key, end) >= 0 {
		return false
	}
	return true
}

func newOverlayEntryBatch(backing []batch.Entry) overlayEntryBatch {
	return overlayEntryBatch{entries: backing[:0]}
}

func (b *overlayEntryBatch) Set(key, value []byte) error {
	if b.closed {
		return batch.ErrBatchClosed
	}
	b.entries = append(b.entries, batch.Entry{
		Type:  batch.OpPut,
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), value...),
	})
	return nil
}

func (b *overlayEntryBatch) SetView(key, value []byte) error {
	return b.Set(key, value)
}

func (b *overlayEntryBatch) SetOwnedBytes(key, value []byte) error {
	if b.closed {
		return batch.ErrBatchClosed
	}
	runNamedRootOwnedWriteTestHook("set_bytes", key)
	b.entries = append(b.entries, batch.Entry{
		Type:  batch.OpPut,
		Key:   key,
		Value: value,
	})
	return nil
}

func (b *overlayEntryBatch) SetAuto(key, value []byte) error {
	return b.Set(key, value)
}

func (b *overlayEntryBatch) SetAutoView(key, value []byte) error {
	return b.Set(key, value)
}

func (b *overlayEntryBatch) Delete(key []byte) error {
	if b.closed {
		return batch.ErrBatchClosed
	}
	b.entries = append(b.entries, batch.Entry{
		Type: batch.OpDelete,
		Key:  append([]byte(nil), key...),
	})
	return nil
}

func (b *overlayEntryBatch) DeleteView(key []byte) error {
	return b.Delete(key)
}

func (b *overlayEntryBatch) SetOwnedKey(key []byte) error {
	if b.closed {
		return batch.ErrBatchClosed
	}
	runNamedRootOwnedWriteTestHook("set_key", key)
	b.entries = append(b.entries, batch.Entry{
		Type: batch.OpPut,
		Key:  key,
	})
	return nil
}

func (b *overlayEntryBatch) DeleteOwnedKey(key []byte) error {
	if b.closed {
		return batch.ErrBatchClosed
	}
	runNamedRootOwnedWriteTestHook("delete_key", key)
	b.entries = append(b.entries, batch.Entry{
		Type: batch.OpDelete,
		Key:  key,
	})
	return nil
}

func (b *overlayEntryBatch) SetOps(ops []batch.Entry) error {
	for _, entry := range ops {
		if entry.IsPtr {
			return batch.ErrValueTooLarge
		}
		switch entry.Type {
		case batch.OpPut:
			if err := b.Set(entry.Key, entry.Value); err != nil {
				return err
			}
		case batch.OpDelete:
			if err := b.Delete(entry.Key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *overlayEntryBatch) Write() error {
	b.closed = true
	return nil
}

func (b *overlayEntryBatch) WriteSync() error {
	b.closed = true
	return nil
}

func (b *overlayEntryBatch) Close() error {
	b.closed = true
	b.entries = nil
	return nil
}

func (b *overlayEntryBatch) Replay(fn func(batch.Entry) error) error {
	for _, entry := range b.entries {
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func (b *overlayEntryBatch) GetByteSize() (int, error) {
	size := 0
	for _, entry := range b.entries {
		size += len(entry.Key) + len(entry.Value)
	}
	return size, nil
}

func (it *namedRootOverlayIterator) Valid() bool {
	return it != nil && it.index < len(it.entries)
}

func (it *namedRootOverlayIterator) Next() {
	if it != nil && it.index < len(it.entries) {
		it.index++
	}
}

func (it *namedRootOverlayIterator) Seek(key []byte) {
	if it == nil {
		return
	}
	it.index = sort.Search(len(it.entries), func(i int) bool {
		return bytes.Compare(it.entries[i].key, key) >= 0
	})
}

func (it *namedRootOverlayIterator) UnsafeKey() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.index].key
}

func (it *namedRootOverlayIterator) UnsafeValue() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.index].value
}

func (it *namedRootOverlayIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.Valid() {
		return nil, page.ValuePtr{}, 0
	}
	entry := it.entries[it.index]
	if entry.deleted {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	return entry.value, page.ValuePtr{}, node.FlagInline
}

func (it *namedRootOverlayIterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *namedRootOverlayIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *namedRootOverlayIterator) KeyCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst[:0]
	}
	return append(dst[:0], it.entries[it.index].key...)
}

func (it *namedRootOverlayIterator) ValueCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst[:0]
	}
	return append(dst[:0], it.entries[it.index].value...)
}

func (it *namedRootOverlayIterator) IsDeleted() bool {
	return it.Valid() && it.entries[it.index].deleted
}

func (it *namedRootOverlayIterator) Error() error {
	return nil
}

func (it *namedRootOverlayIterator) Close() error {
	if it != nil {
		it.entries = nil
	}
	return nil
}

func (it *namedRootOverlayIterator) Domain() ([]byte, []byte) {
	if it == nil {
		return nil, nil
	}
	return it.start, it.end
}

func newNamedRootMergeIterator(overlay, base iterator.UnsafeIterator, start, end []byte) *namedRootMergeIterator {
	it := &namedRootMergeIterator{
		overlay: overlay,
		base:    base,
		start:   append([]byte(nil), start...),
		end:     append([]byte(nil), end...),
	}
	it.selectCurrent()
	return it
}

func (it *namedRootMergeIterator) selectCurrent() {
	it.current = namedRootMergeSourceNone
	it.skipBaseOnNext = false
	overlayValid := it.overlay != nil && it.overlay.Valid()
	baseValid := it.base != nil && it.base.Valid()
	if !overlayValid && !baseValid {
		return
	}
	if !overlayValid {
		it.current = namedRootMergeSourceBase
		return
	}
	if !baseValid {
		it.current = namedRootMergeSourceOverlay
		return
	}
	cmp := bytes.Compare(it.overlay.UnsafeKey(), it.base.UnsafeKey())
	switch {
	case cmp < 0:
		it.current = namedRootMergeSourceOverlay
	case cmp > 0:
		it.current = namedRootMergeSourceBase
	default:
		it.current = namedRootMergeSourceOverlay
		it.skipBaseOnNext = true
	}
}

func (it *namedRootMergeIterator) Valid() bool {
	switch it.current {
	case namedRootMergeSourceOverlay:
		return it.overlay != nil && it.overlay.Valid()
	case namedRootMergeSourceBase:
		return it.base != nil && it.base.Valid()
	default:
		return false
	}
}

func (it *namedRootMergeIterator) Next() {
	switch it.current {
	case namedRootMergeSourceOverlay:
		if it.overlay != nil {
			it.overlay.Next()
		}
		if it.skipBaseOnNext && it.base != nil {
			it.base.Next()
		}
	case namedRootMergeSourceBase:
		if it.base != nil {
			it.base.Next()
		}
	}
	it.selectCurrent()
}

func (it *namedRootMergeIterator) Seek(key []byte) {
	if it.overlay != nil {
		it.overlay.Seek(key)
	}
	if it.base != nil {
		it.base.Seek(key)
	}
	it.selectCurrent()
}

func (it *namedRootMergeIterator) UnsafeKey() []byte {
	switch it.current {
	case namedRootMergeSourceOverlay:
		return it.overlay.UnsafeKey()
	case namedRootMergeSourceBase:
		return it.base.UnsafeKey()
	default:
		return nil
	}
}

func (it *namedRootMergeIterator) UnsafeValue() []byte {
	switch it.current {
	case namedRootMergeSourceOverlay:
		return it.overlay.UnsafeValue()
	case namedRootMergeSourceBase:
		return it.base.UnsafeValue()
	default:
		return nil
	}
}

func (it *namedRootMergeIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	switch it.current {
	case namedRootMergeSourceOverlay:
		return it.overlay.UnsafeEntry()
	case namedRootMergeSourceBase:
		return it.base.UnsafeEntry()
	default:
		return nil, page.ValuePtr{}, 0
	}
}

func (it *namedRootMergeIterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *namedRootMergeIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *namedRootMergeIterator) KeyCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst[:0]
	}
	return append(dst[:0], it.UnsafeKey()...)
}

func (it *namedRootMergeIterator) ValueCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst[:0]
	}
	return append(dst[:0], it.UnsafeValue()...)
}

func (it *namedRootMergeIterator) IsDeleted() bool {
	switch it.current {
	case namedRootMergeSourceOverlay:
		return it.overlay.IsDeleted()
	case namedRootMergeSourceBase:
		return it.base.IsDeleted()
	default:
		return false
	}
}

func (it *namedRootMergeIterator) Error() error {
	if it.overlay != nil {
		if err := it.overlay.Error(); err != nil {
			return err
		}
	}
	if it.base != nil {
		return it.base.Error()
	}
	return nil
}

func (it *namedRootMergeIterator) Close() error {
	var firstErr error
	if it.overlay != nil {
		if err := it.overlay.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		it.overlay = nil
	}
	if it.base != nil {
		if err := it.base.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		it.base = nil
	}
	it.current = namedRootMergeSourceNone
	return firstErr
}

func (it *namedRootMergeIterator) Domain() ([]byte, []byte) {
	if it == nil {
		return nil, nil
	}
	return it.start, it.end
}
