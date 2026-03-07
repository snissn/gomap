package caching

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/rootfmt"
)

const virtualNamedRootMask uint64 = 1 << 63

type NamedRootDebugState struct {
	VirtualRootID    uint64
	BaseRootID       uint64
	HasDomain        bool
	LegacyEntryCount int
}

type namedRootOverlayState struct {
	virtualRootID uint64
	baseRootID    uint64
	rootKey       []byte
	hasFormat     bool
	format        rootfmt.Format
	domain        *rootDomain
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

type namedRootPendingIteratorMutation struct {
	virtualRootID uint64
	baseRootID    uint64
	rootKey       []byte
	hasFormat     bool
	format        rootfmt.Format
	iter          iterator.UnsafeIterator
}

type namedRootPendingTableMutation struct {
	virtualRootID uint64
	baseRootID    uint64
	rootKey       []byte
	hasFormat     bool
	format        rootfmt.Format
	table         memtable.Table
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

type namedRootStableIterator struct {
	iterator.UnsafeIterator
}

func (namedRootStableIterator) StableUnsafeIteratorSlices() bool { return true }

type batchEntryIterator struct {
	entries []batch.Entry
	index   int
}

type stableNamedRootEntryIterator interface {
	iterator.UnsafeIterator
	StableUnsafeIteratorSlices() bool
}

type namedRootMutationTableProvider interface {
	RootMutationTable() memtable.Table
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

var namedRootBulkPublishIteratorsHook struct {
	mu sync.RWMutex
	fn func(int)
}

var namedRootBulkPublishTablesHook struct {
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

func setNamedRootBulkPublishIteratorsTestHook(fn func(int)) func() {
	namedRootBulkPublishIteratorsHook.mu.Lock()
	prev := namedRootBulkPublishIteratorsHook.fn
	namedRootBulkPublishIteratorsHook.fn = fn
	namedRootBulkPublishIteratorsHook.mu.Unlock()
	return func() {
		namedRootBulkPublishIteratorsHook.mu.Lock()
		namedRootBulkPublishIteratorsHook.fn = prev
		namedRootBulkPublishIteratorsHook.mu.Unlock()
	}
}

func SetNamedRootBulkPublishIteratorsTestHook(fn func(int)) func() {
	return setNamedRootBulkPublishIteratorsTestHook(fn)
}

func runNamedRootBulkPublishIteratorsTestHook(rootCount int) {
	namedRootBulkPublishIteratorsHook.mu.RLock()
	fn := namedRootBulkPublishIteratorsHook.fn
	namedRootBulkPublishIteratorsHook.mu.RUnlock()
	if fn != nil {
		fn(rootCount)
	}
}

func setNamedRootBulkPublishTablesTestHook(fn func(int)) func() {
	namedRootBulkPublishTablesHook.mu.Lock()
	prev := namedRootBulkPublishTablesHook.fn
	namedRootBulkPublishTablesHook.fn = fn
	namedRootBulkPublishTablesHook.mu.Unlock()
	return func() {
		namedRootBulkPublishTablesHook.mu.Lock()
		namedRootBulkPublishTablesHook.fn = prev
		namedRootBulkPublishTablesHook.mu.Unlock()
	}
}

func SetNamedRootBulkPublishTablesTestHook(fn func(int)) func() {
	return setNamedRootBulkPublishTablesTestHook(fn)
}

func runNamedRootBulkPublishTablesTestHook(rootCount int) {
	namedRootBulkPublishTablesHook.mu.RLock()
	fn := namedRootBulkPublishTablesHook.fn
	namedRootBulkPublishTablesHook.mu.RUnlock()
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
		bridge, bridgeErr := db.directBridge()
		if bridgeErr != nil {
			return nil, bridgeErr
		}
		return bridge.GetAtRoot(db.ResolvedNamedRootID(rootID), key)
	}
	if state.domain != nil {
		if value, _, flags, found := state.domain.getEntry(key); found {
			if flags&node.FlagTombstone != 0 {
				return nil, nil
			}
			return append([]byte(nil), value...), nil
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
		bridge, bridgeErr := db.directBridge()
		if bridgeErr != nil {
			return dst, bridgeErr
		}
		return bridge.GetAtRootAppend(db.ResolvedNamedRootID(rootID), key, dst)
	}
	if state.domain != nil {
		if value, _, flags, found := state.domain.getEntry(key); found {
			if flags&node.FlagTombstone != 0 {
				return dst, nil
			}
			return append(dst, value...), nil
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
		bridge, bridgeErr := db.directBridge()
		if bridgeErr != nil {
			return false, bridgeErr
		}
		return bridge.HasAtRoot(db.ResolvedNamedRootID(rootID), key)
	}
	if state.domain != nil {
		if _, _, flags, found := state.domain.getEntry(key); found {
			return flags&node.FlagTombstone == 0, nil
		}
	}
	bridge, err := db.directBridge()
	if err != nil {
		return false, err
	}
	return bridge.HasAtRoot(state.baseRootID, key)
}

func (db *DB) BufferedHasManyAtRoot(rootID uint64, keys [][]byte) ([]bool, error) {
	state, err := db.namedRootState(rootID)
	if err != nil {
		bridge, bridgeErr := db.directBridge()
		if bridgeErr != nil {
			return nil, bridgeErr
		}
		return bridge.HasManyAtRoot(db.ResolvedNamedRootID(rootID), keys)
	}
	out := make([]bool, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	pendingIndexes := make([]int, 0, len(keys))
	pendingKeys := make([][]byte, 0, len(keys))
	for i, key := range keys {
		if state.domain != nil {
			if _, _, flags, found := state.domain.getEntry(key); found {
				out[i] = flags&node.FlagTombstone == 0
				continue
			}
		}
		pendingIndexes = append(pendingIndexes, i)
		pendingKeys = append(pendingKeys, key)
	}
	if len(pendingKeys) == 0 {
		return out, nil
	}
	bridge, err := db.directBridge()
	if err != nil {
		return nil, err
	}
	baseResults, err := bridge.HasManyAtRoot(state.baseRootID, pendingKeys)
	if err != nil {
		return nil, err
	}
	for i, idx := range pendingIndexes {
		out[idx] = baseResults[i]
	}
	return out, nil
}

func (db *DB) BufferedHasPrefixAtRoot(rootID uint64, prefix []byte) (bool, error) {
	state, err := db.namedRootState(rootID)
	if err != nil {
		bridge, bridgeErr := db.directBridge()
		if bridgeErr != nil {
			return false, bridgeErr
		}
		return bridge.HasPrefixAtRoot(db.ResolvedNamedRootID(rootID), prefix)
	}

	if state.domain != nil && state.domain.table != nil {
		overlayIter := state.domain.table.NewIterator(prefix, nil)
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
		if state.domain != nil {
			if _, _, flags, found := state.domain.getEntry(key); found {
				if flags&node.FlagTombstone != 0 {
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

func (db *DB) BufferedHasPrefixesAtRoot(rootID uint64, prefixes [][]byte) ([]bool, error) {
	state, err := db.namedRootState(rootID)
	if err != nil {
		bridge, bridgeErr := db.directBridge()
		if bridgeErr != nil {
			return nil, bridgeErr
		}
		return bridge.HasPrefixesAtRoot(db.ResolvedNamedRootID(rootID), prefixes)
	}
	out := make([]bool, len(prefixes))
	if len(prefixes) == 0 {
		return out, nil
	}
	type pendingPrefix struct {
		index  int
		prefix []byte
	}
	pending := make([]pendingPrefix, 0, len(prefixes))
	for i, prefix := range prefixes {
		hasOverlayMatch := false
		if state.domain != nil && state.domain.table != nil {
			overlayIter := state.domain.table.NewIterator(prefix, nil)
			for overlayIter.Valid() {
				key := overlayIter.UnsafeKey()
				if !bytes.HasPrefix(key, prefix) {
					break
				}
				if !overlayIter.IsDeleted() {
					hasOverlayMatch = true
					break
				}
				overlayIter.Next()
			}
			if err := overlayIter.Error(); err != nil {
				_ = overlayIter.Close()
				return nil, err
			}
			if err := overlayIter.Close(); err != nil {
				return nil, err
			}
		}
		if hasOverlayMatch {
			out[i] = true
			continue
		}
		pending = append(pending, pendingPrefix{index: i, prefix: prefix})
	}
	if len(pending) == 0 {
		return out, nil
	}
	bridge, err := db.directBridge()
	if err != nil {
		return nil, err
	}
	if state.domain == nil || !state.domain.hasDeletes {
		batchPrefixes := make([][]byte, len(pending))
		for i := range pending {
			batchPrefixes[i] = pending[i].prefix
		}
		baseResults, err := bridge.HasPrefixesAtRoot(state.baseRootID, batchPrefixes)
		if err != nil {
			return nil, err
		}
		for i := range pending {
			out[pending[i].index] = baseResults[i]
		}
		return out, nil
	}
	for _, item := range pending {
		has, err := db.BufferedHasPrefixAtRoot(rootID, item.prefix)
		if err != nil {
			return nil, err
		}
		out[item.index] = has
	}
	return out, nil
}

func (db *DB) BufferedIteratorAtRoot(rootID uint64, start, end []byte) (iterator.UnsafeIterator, error) {
	runNamedRootBufferedIteratorTestHook(rootID, start, end)
	state, err := db.namedRootState(rootID)
	if err != nil {
		bridge, bridgeErr := db.directBridge()
		if bridgeErr != nil {
			return nil, bridgeErr
		}
		return bridge.IteratorAtRoot(db.ResolvedNamedRootID(rootID), start, end)
	}
	bridge, err := db.directBridge()
	if err != nil {
		return nil, err
	}
	if state.domain != nil && state.domain.pending() {
		overlayIter, err := state.domain.newIterator(start, end)
		if err != nil {
			return nil, err
		}
		baseIter, err := bridge.IteratorAtRoot(state.baseRootID, start, end)
		if err != nil {
			_ = overlayIter.Close()
			return nil, err
		}
		return newNamedRootMergeIterator(overlayIter, baseIter, start, end), nil
	}
	return bridge.IteratorAtRoot(state.baseRootID, start, end)
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

	db.rootDomainManager.namedRootMu.Lock()
	virtualRootIDs := make([]uint64, len(rootIDs))
	pending := make([]namedRootPendingMutation, len(rootIDs))
	for i := range rootIDs {
		existing := db.rootDomainManager.namedRootsByID[rootIDs[i]]
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
			resolvedRootID := db.rootDomainManager.resolvePublishedNamedRootIDLocked(rootIDs[i])
			virtualRootIDs[i] = db.rootDomainManager.nextVirtualNamedRootIDLocked()
			if rootIDs[i]&virtualNamedRootMask != 0 && resolvedRootID != rootIDs[i] {
				virtualRootIDs[i] = rootIDs[i]
			}
			pending[i] = namedRootPendingMutation{
				virtualRootID: virtualRootIDs[i],
				baseRootID:    resolvedRootID,
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
			db.rootDomainManager.namedRootMu.Unlock()
			return nil, err
		}
		sysEntries = ops
	}
	if err := db.rootDomainManager.bindPendingNamedRootKeysLocked(pending, sysEntries); err != nil {
		db.rootDomainManager.namedRootMu.Unlock()
		return nil, err
	}
	for i := range pending {
		db.rootDomainManager.applyPendingNamedRootLocked(&pending[i])
	}
	if err := db.ApplySystemOverlayEntriesOwned(sysEntries); err != nil {
		db.rootDomainManager.namedRootMu.Unlock()
		return nil, err
	}
	db.rootDomainManager.namedRootMu.Unlock()
	if !sync {
		db.maybeTriggerNamedRootFlush()
	}

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

func (db *DB) BufferNamedRootMutationsIterators(sync bool, rootIDs []uint64, formats []*rootfmt.Format, rootIters []iterator.UnsafeIterator, buildSystemOps func([]uint64) ([]batch.Entry, error)) ([]uint64, error) {
	if db == nil {
		return nil, errDBClosing
	}
	if len(rootIDs) != len(rootIters) {
		return nil, fmt.Errorf("named root iterators length mismatch")
	}

	db.rootDomainManager.namedRootMu.Lock()
	virtualRootIDs := make([]uint64, len(rootIDs))
	pending := make([]namedRootPendingIteratorMutation, len(rootIDs))
	for i := range rootIDs {
		existing := db.rootDomainManager.namedRootsByID[rootIDs[i]]
		if existing != nil {
			virtualRootIDs[i] = existing.virtualRootID
			pending[i] = namedRootPendingIteratorMutation{
				virtualRootID: existing.virtualRootID,
				baseRootID:    existing.baseRootID,
				rootKey:       append([]byte(nil), existing.rootKey...),
				hasFormat:     existing.hasFormat,
				format:        existing.format,
				iter:          rootIters[i],
			}
		} else {
			resolvedRootID := db.rootDomainManager.resolvePublishedNamedRootIDLocked(rootIDs[i])
			virtualRootIDs[i] = db.rootDomainManager.nextVirtualNamedRootIDLocked()
			if rootIDs[i]&virtualNamedRootMask != 0 && resolvedRootID != rootIDs[i] {
				virtualRootIDs[i] = rootIDs[i]
			}
			pending[i] = namedRootPendingIteratorMutation{
				virtualRootID: virtualRootIDs[i],
				baseRootID:    resolvedRootID,
				iter:          rootIters[i],
			}
		}
		if format := rootFormatAt(formats, i); format != nil {
			pending[i].hasFormat = true
			pending[i].format = *format
		}
	}

	var sysEntries []batch.Entry
	if buildSystemOps != nil {
		ops, err := buildSystemOps(virtualRootIDs)
		if err != nil {
			db.rootDomainManager.namedRootMu.Unlock()
			return nil, err
		}
		sysEntries = ops
	}
	if err := db.rootDomainManager.bindPendingNamedRootIteratorKeysLocked(pending, sysEntries); err != nil {
		db.rootDomainManager.namedRootMu.Unlock()
		return nil, err
	}
	for i := range pending {
		if err := db.rootDomainManager.applyPendingNamedRootIteratorLocked(&pending[i]); err != nil {
			db.rootDomainManager.namedRootMu.Unlock()
			return nil, err
		}
	}
	if err := db.ApplySystemOverlayEntriesOwned(sysEntries); err != nil {
		db.rootDomainManager.namedRootMu.Unlock()
		return nil, err
	}
	db.rootDomainManager.namedRootMu.Unlock()
	if !sync {
		db.maybeTriggerNamedRootFlush()
	}

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

func (db *DB) BufferNamedRootMutationsTables(sync bool, rootIDs []uint64, formats []*rootfmt.Format, rootTables []memtable.Table, buildSystemOps func([]uint64) ([]batch.Entry, error)) ([]uint64, error) {
	if db == nil {
		return nil, errDBClosing
	}
	if len(rootIDs) != len(rootTables) {
		return nil, fmt.Errorf("named root tables length mismatch")
	}

	db.rootDomainManager.namedRootMu.Lock()
	virtualRootIDs := make([]uint64, len(rootIDs))
	pending := make([]namedRootPendingTableMutation, len(rootIDs))
	for i := range rootIDs {
		existing := db.rootDomainManager.namedRootsByID[rootIDs[i]]
		if existing != nil {
			virtualRootIDs[i] = existing.virtualRootID
			pending[i] = namedRootPendingTableMutation{
				virtualRootID: existing.virtualRootID,
				baseRootID:    existing.baseRootID,
				rootKey:       append([]byte(nil), existing.rootKey...),
				hasFormat:     existing.hasFormat,
				format:        existing.format,
				table:         rootTables[i],
			}
		} else {
			resolvedRootID := db.rootDomainManager.resolvePublishedNamedRootIDLocked(rootIDs[i])
			virtualRootIDs[i] = db.rootDomainManager.nextVirtualNamedRootIDLocked()
			if rootIDs[i]&virtualNamedRootMask != 0 && resolvedRootID != rootIDs[i] {
				virtualRootIDs[i] = rootIDs[i]
			}
			pending[i] = namedRootPendingTableMutation{
				virtualRootID: virtualRootIDs[i],
				baseRootID:    resolvedRootID,
				table:         rootTables[i],
			}
		}
		if format := rootFormatAt(formats, i); format != nil {
			pending[i].hasFormat = true
			pending[i].format = *format
		}
	}

	var sysEntries []batch.Entry
	if buildSystemOps != nil {
		ops, err := buildSystemOps(virtualRootIDs)
		if err != nil {
			db.rootDomainManager.namedRootMu.Unlock()
			return nil, err
		}
		sysEntries = ops
	}
	if err := db.rootDomainManager.bindPendingNamedRootTableKeysLocked(pending, sysEntries); err != nil {
		db.rootDomainManager.namedRootMu.Unlock()
		return nil, err
	}
	for i := range pending {
		if err := db.rootDomainManager.applyPendingNamedRootTableLocked(&pending[i]); err != nil {
			db.rootDomainManager.namedRootMu.Unlock()
			return nil, err
		}
	}
	if err := db.ApplySystemOverlayEntriesOwned(sysEntries); err != nil {
		db.rootDomainManager.namedRootMu.Unlock()
		return nil, err
	}
	db.rootDomainManager.namedRootMu.Unlock()
	if !sync {
		db.maybeTriggerNamedRootFlush()
	}

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
	return db.publishNamedRootDomainsLocked(bridge, sync)
}

func (db *DB) PendingNamedRoots() bool {
	if db == nil {
		return false
	}
	return db.rootDomainManager.pendingNamedRoots()
}

func (db *DB) DebugNamedRootStateByID(rootID uint64) (NamedRootDebugState, bool) {
	if db == nil {
		return NamedRootDebugState{}, false
	}
	return db.rootDomainManager.debugNamedRootStateByID(rootID)
}

func (db *DB) DebugNamedRootStateByKey(rootKey []byte) (NamedRootDebugState, bool) {
	if db == nil {
		return NamedRootDebugState{}, false
	}
	return db.rootDomainManager.debugNamedRootStateByKey(rootKey)
}

func (db *DB) namedRootState(rootID uint64) (*namedRootOverlayState, error) {
	return db.rootDomainManager.namedRootState(rootID)
}

func (db *DB) ResolvedNamedRootID(rootID uint64) uint64 {
	if db == nil || rootID == 0 {
		return rootID
	}
	return db.rootDomainManager.resolvedNamedRootID(rootID)
}

func (db *DB) resolvePublishedNamedRootIDLocked(rootID uint64) uint64 {
	return db.rootDomainManager.resolvePublishedNamedRootIDLocked(rootID)
}

func (db *DB) nextVirtualNamedRootIDLocked() uint64 {
	return db.rootDomainManager.nextVirtualNamedRootIDLocked()
}

func (db *DB) bindPendingNamedRootKeysLocked(pending []namedRootPendingMutation, sysEntries []batch.Entry) error {
	return db.rootDomainManager.bindPendingNamedRootKeysLocked(pending, sysEntries)
}

func (db *DB) bindPendingNamedRootIteratorKeysLocked(pending []namedRootPendingIteratorMutation, sysEntries []batch.Entry) error {
	return db.rootDomainManager.bindPendingNamedRootIteratorKeysLocked(pending, sysEntries)
}

func (db *DB) bindPendingNamedRootTableKeysLocked(pending []namedRootPendingTableMutation, sysEntries []batch.Entry) error {
	return db.rootDomainManager.bindPendingNamedRootTableKeysLocked(pending, sysEntries)
}

func (db *DB) applyPendingNamedRootLocked(pending *namedRootPendingMutation) {
	db.rootDomainManager.applyPendingNamedRootLocked(pending)
}

func (db *DB) applyPendingNamedRootTableLocked(pending *namedRootPendingTableMutation) error {
	return db.rootDomainManager.applyPendingNamedRootTableLocked(pending)
}

func (db *DB) applyPendingNamedRootIteratorLocked(pending *namedRootPendingIteratorMutation) error {
	return db.rootDomainManager.applyPendingNamedRootIteratorLocked(pending)
}

func collectNamedRootIteratorOps(iter iterator.UnsafeIterator, hint int, stable bool) ([]batch.Entry, error) {
	if iter == nil {
		return nil, nil
	}
	if hint < 0 {
		hint = 0
	}
	ops := make([]batch.Entry, 0, hint)
	for iter.Valid() {
		key := iter.UnsafeKey()
		value, ptr, flags := iter.UnsafeEntry()
		if !stable {
			key = append([]byte(nil), key...)
			if flags&node.FlagTombstone == 0 && len(value) > 0 {
				value = append([]byte(nil), value...)
			}
		}
		entry := batch.Entry{Key: key}
		switch {
		case flags&node.FlagTombstone != 0:
			entry.Type = batch.OpDelete
		case flags&node.FlagPointer != 0:
			entry.Type = batch.OpPut
			entry.IsPtr = true
			entry.ValuePtr = ptr
			entry.Value = value
		default:
			entry.Type = batch.OpPut
			entry.Value = value
		}
		ops = append(ops, entry)
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return ops, nil
}

func namedRootStateBufferedBytes(state *namedRootOverlayState) int64 {
	if state == nil || state.domain == nil {
		return 0
	}
	return state.domain.bufferedBytes()
}

func namedRootFlushTable(state *namedRootOverlayState) memtable.Table {
	if state == nil || state.domain == nil {
		return nil
	}
	return state.domain.table
}

func (db *DB) maybeTriggerNamedRootFlush() {
	if db == nil {
		return
	}
	threshold := db.mutableFlushThreshold()
	if threshold <= 0 {
		return
	}
	if db.namedRootBufferedBytes.Load() > threshold {
		db.TriggerFlush()
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

func (it *batchEntryIterator) Valid() bool {
	return it != nil && it.index < len(it.entries)
}

func (it *batchEntryIterator) Next() {
	if it != nil && it.index < len(it.entries) {
		it.index++
	}
}

func (it *batchEntryIterator) Seek(key []byte) {
	if it == nil {
		return
	}
	it.index = 0
	for it.index < len(it.entries) && bytes.Compare(it.entries[it.index].Key, key) < 0 {
		it.index++
	}
}

func (it *batchEntryIterator) UnsafeKey() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.index].Key
}

func (it *batchEntryIterator) UnsafeValue() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.index].Value
}

func (it *batchEntryIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.Valid() {
		return nil, page.ValuePtr{}, 0
	}
	entry := it.entries[it.index]
	switch {
	case entry.Type == batch.OpDelete:
		return nil, page.ValuePtr{}, node.FlagTombstone
	case entry.IsPtr:
		return entry.Value, entry.ValuePtr, node.FlagPointer
	default:
		return entry.Value, page.ValuePtr{}, node.FlagInline
	}
}

func (it *batchEntryIterator) Key() []byte { return it.UnsafeKey() }

func (it *batchEntryIterator) Value() []byte { return it.UnsafeValue() }

func (it *batchEntryIterator) KeyCopy(dst []byte) []byte {
	return append(dst[:0], it.UnsafeKey()...)
}

func (it *batchEntryIterator) ValueCopy(dst []byte) []byte {
	return append(dst[:0], it.UnsafeValue()...)
}

func (it *batchEntryIterator) IsDeleted() bool {
	return it.Valid() && it.entries[it.index].Type == batch.OpDelete
}

func (it *batchEntryIterator) Error() error { return nil }

func (it *batchEntryIterator) Close() error { return nil }

func (it *batchEntryIterator) Domain() (start, end []byte) { return nil, nil }

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
