package db

import (
	"bytes"
	"fmt"
	"sync"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/rootfmt"
	"github.com/snissn/gomap/TreeDB/tree"
)

// RootMutation describes a detached-root mutation to publish in one commit
// boundary alongside any system catalog updates.
type RootMutation struct {
	RootID uint64
	Format *rootfmt.Format
	Mutate func(batchpkg.Interface) error
}

type detachedBatch struct {
	Batch
}

var detachedBatchPool = sync.Pool{
	New: func() any {
		return &detachedBatch{}
	},
}

var rootIteratorBulkBuildHook struct {
	mu sync.RWMutex
	fn func(int)
}

var rootIteratorWarmMergeHook struct {
	mu sync.RWMutex
	fn func(int)
}

var rootIteratorCollectEntriesHook struct {
	mu sync.RWMutex
	fn func(int)
}

type stableRootEntryIterator interface {
	iterator.UnsafeIterator
	StableUnsafeIteratorSlices() bool
}

type rootMutationTableProvider interface {
	RootMutationTable() memtable.Table
}

const rootIteratorWarmMergeMinEntries = 128

func setRootIteratorBulkBuildTestHook(fn func(int)) func() {
	rootIteratorBulkBuildHook.mu.Lock()
	prev := rootIteratorBulkBuildHook.fn
	rootIteratorBulkBuildHook.fn = fn
	rootIteratorBulkBuildHook.mu.Unlock()
	return func() {
		rootIteratorBulkBuildHook.mu.Lock()
		rootIteratorBulkBuildHook.fn = prev
		rootIteratorBulkBuildHook.mu.Unlock()
	}
}

func runRootIteratorBulkBuildTestHook(rootIndex int) {
	rootIteratorBulkBuildHook.mu.RLock()
	fn := rootIteratorBulkBuildHook.fn
	rootIteratorBulkBuildHook.mu.RUnlock()
	if fn != nil {
		fn(rootIndex)
	}
}

func setRootIteratorWarmMergeTestHook(fn func(int)) func() {
	rootIteratorWarmMergeHook.mu.Lock()
	prev := rootIteratorWarmMergeHook.fn
	rootIteratorWarmMergeHook.fn = fn
	rootIteratorWarmMergeHook.mu.Unlock()
	return func() {
		rootIteratorWarmMergeHook.mu.Lock()
		rootIteratorWarmMergeHook.fn = prev
		rootIteratorWarmMergeHook.mu.Unlock()
	}
}

func runRootIteratorWarmMergeTestHook(rootIndex int) {
	rootIteratorWarmMergeHook.mu.RLock()
	fn := rootIteratorWarmMergeHook.fn
	rootIteratorWarmMergeHook.mu.RUnlock()
	if fn != nil {
		fn(rootIndex)
	}
}

func setRootIteratorCollectEntriesTestHook(fn func(int)) func() {
	rootIteratorCollectEntriesHook.mu.Lock()
	prev := rootIteratorCollectEntriesHook.fn
	rootIteratorCollectEntriesHook.fn = fn
	rootIteratorCollectEntriesHook.mu.Unlock()
	return func() {
		rootIteratorCollectEntriesHook.mu.Lock()
		rootIteratorCollectEntriesHook.fn = prev
		rootIteratorCollectEntriesHook.mu.Unlock()
	}
}

func runRootIteratorCollectEntriesTestHook(rootIndex int) {
	rootIteratorCollectEntriesHook.mu.RLock()
	fn := rootIteratorCollectEntriesHook.fn
	rootIteratorCollectEntriesHook.mu.RUnlock()
	if fn != nil {
		fn(rootIndex)
	}
}

func (*DB) PreferWarmIteratorBatchPublish() bool { return true }

// GetAtRoot returns the value for a key in the specified root page.
func (db *DB) GetAtRoot(rootID uint64, key []byte) ([]byte, error) {
	if rootID == 0 {
		return nil, nil
	}
	snap, err := db.acquireSnapshotWithRoot(rootID)
	if err != nil {
		return nil, err
	}
	defer snap.Close()
	val, err := snap.Get(key)
	if err == tree.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

// GetAtRootAppend appends the value for a key in the specified root page to
// dst and returns the grown slice. If the key is not found, it returns dst, nil.
func (db *DB) GetAtRootAppend(rootID uint64, key, dst []byte) ([]byte, error) {
	if rootID == 0 {
		return dst, nil
	}
	snap, err := db.acquireSnapshotWithRoot(rootID)
	if err != nil {
		return dst, err
	}
	defer snap.Close()
	val, err := snap.GetAppend(key, dst)
	if err == tree.ErrKeyNotFound {
		return dst, nil
	}
	if err != nil {
		return dst, err
	}
	return val, nil
}

// HasAtRoot reports whether a key exists in the specified root page.
func (db *DB) HasAtRoot(rootID uint64, key []byte) (bool, error) {
	if rootID == 0 {
		return false, nil
	}
	snap, err := db.acquireSnapshotWithRoot(rootID)
	if err != nil {
		return false, err
	}
	defer snap.Close()
	return snap.Has(key)
}

// HasManyAtRoot reports whether each key exists in the specified root page.
func (db *DB) HasManyAtRoot(rootID uint64, keys [][]byte) ([]bool, error) {
	out := make([]bool, len(keys))
	if rootID == 0 || len(keys) == 0 {
		return out, nil
	}
	snap, err := db.acquireSnapshotWithRoot(rootID)
	if err != nil {
		return nil, err
	}
	defer snap.Close()
	seen := make(map[string]bool, len(keys))
	for i, key := range keys {
		cacheKey := string(key)
		if cached, ok := seen[cacheKey]; ok {
			out[i] = cached
			continue
		}
		has, err := snap.Has(key)
		if err != nil {
			return nil, err
		}
		seen[cacheKey] = has
		out[i] = has
	}
	return out, nil
}

// HasPrefixAtRoot reports whether any non-deleted key with the provided prefix
// exists in the specified root page.
func (db *DB) HasPrefixAtRoot(rootID uint64, prefix []byte) (bool, error) {
	if rootID == 0 {
		return false, nil
	}
	it, err := db.IteratorAtRoot(rootID, prefix, nil)
	if err != nil {
		return false, err
	}
	defer it.Close()
	for it.Valid() {
		key := it.UnsafeKey()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		if !it.IsDeleted() {
			return true, nil
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return false, err
	}
	return false, nil
}

// HasPrefixesAtRoot reports whether any non-deleted key with each prefix exists
// in the specified root page.
func (db *DB) HasPrefixesAtRoot(rootID uint64, prefixes [][]byte) ([]bool, error) {
	out := make([]bool, len(prefixes))
	if rootID == 0 || len(prefixes) == 0 {
		return out, nil
	}
	snap, err := db.acquireSnapshotWithRoot(rootID)
	if err != nil {
		return nil, err
	}
	defer snap.Close()
	seen := make(map[string]bool, len(prefixes))
	for i, prefix := range prefixes {
		cacheKey := string(prefix)
		if cached, ok := seen[cacheKey]; ok {
			out[i] = cached
			continue
		}
		it := snap.tree.IteratorWithOptions(prefix, nil, tree.IteratorOptions{Mode: tree.IteratorModeKeysOnly})
		has := false
		for it.Valid() {
			key := it.UnsafeKey()
			if !bytes.HasPrefix(key, prefix) {
				break
			}
			if !it.IsDeleted() {
				has = true
				break
			}
			it.Next()
		}
		if err := it.Error(); err != nil {
			_ = it.Close()
			return nil, err
		}
		if err := it.Close(); err != nil {
			return nil, err
		}
		seen[cacheKey] = has
		out[i] = has
	}
	return out, nil
}

// IteratorAtRoot returns an iterator over the specified root page.
func (db *DB) IteratorAtRoot(rootID uint64, start, end []byte) (iterator.UnsafeIterator, error) {
	return db.IteratorAtRootWithOptions(rootID, start, end, IteratorOptions{})
}

// IteratorAtRootWithOptions returns a root-local iterator with explicit
// materialization controls.
func (db *DB) IteratorAtRootWithOptions(rootID uint64, start, end []byte, opts IteratorOptions) (iterator.UnsafeIterator, error) {
	if rootID == 0 {
		return &emptyIterator{}, nil
	}
	snap, err := db.acquireSnapshotWithRoot(rootID)
	if err != nil {
		return nil, err
	}
	it := snap.tree.IteratorWithOptions(start, end, opts)
	return &DBIterator{snap: snap, iter: it}, nil
}

// MutateRoot applies a batch to the provided root page and updates the system
// catalog in the same durable commit.
func (db *DB) MutateRoot(rootID uint64, sync bool, mutateRoot func(batchpkg.Interface) error, updateSystem func(batchpkg.Interface, uint64) error) (uint64, error) {
	return db.MutateRootWithFormat(rootID, nil, sync, mutateRoot, updateSystem)
}

// MutateRootWithFormat applies a batch to the provided root page using the
// supplied root-local format overrides and updates the system catalog in the
// same durable commit.
func (db *DB) MutateRootWithFormat(rootID uint64, format *rootfmt.Format, sync bool, mutateRoot func(batchpkg.Interface) error, updateSystem func(batchpkg.Interface, uint64) error) (uint64, error) {
	return db.mutateRootInternal(rootID, format, sync, mutateRoot, nil, updateSystem)
}

// MutateRootAndUser atomically applies mutations to a dedicated root, the user
// root, and the system root in one commit boundary.
func (db *DB) MutateRootAndUser(rootID uint64, sync bool, mutateRoot func(batchpkg.Interface) error, mutateUser func(batchpkg.Interface) error, updateSystem func(batchpkg.Interface, uint64) error) (uint64, error) {
	return db.MutateRootAndUserWithFormat(rootID, nil, sync, mutateRoot, mutateUser, updateSystem)
}

// MutateRootAndUserWithFormat atomically applies mutations to a dedicated root,
// the user root, and the system root in one commit boundary using the supplied
// root-local format overrides for the detached root.
func (db *DB) MutateRootAndUserWithFormat(rootID uint64, format *rootfmt.Format, sync bool, mutateRoot func(batchpkg.Interface) error, mutateUser func(batchpkg.Interface) error, updateSystem func(batchpkg.Interface, uint64) error) (uint64, error) {
	return db.mutateRootInternal(rootID, format, sync, mutateRoot, mutateUser, updateSystem)
}

// MutateRoots atomically applies mutations to multiple detached roots and the
// system root in one commit boundary. The user root remains unchanged.
func (db *DB) MutateRoots(sync bool, roots []RootMutation, updateSystem func(batchpkg.Interface, []uint64) error) ([]uint64, error) {
	rootIDs := make([]uint64, len(roots))
	formats := make([]*rootfmt.Format, len(roots))
	mutators := make([]func(batchpkg.Interface) error, len(roots))
	for i := range roots {
		rootIDs[i] = roots[i].RootID
		formats[i] = roots[i].Format
		mutators[i] = roots[i].Mutate
	}
	return db.MutateRootsWithFormats(sync, rootIDs, formats, mutators, updateSystem)
}

// MutateRootsWithFuncs is the function-based form of MutateRoots. It is used
// by higher-level packages that cannot import db.RootMutation without creating
// package import cycles in tests.
func (db *DB) MutateRootsWithFuncs(sync bool, rootIDs []uint64, mutateRoots []func(batchpkg.Interface) error, updateSystem func(batchpkg.Interface, []uint64) error) ([]uint64, error) {
	return db.MutateRootsWithFormats(sync, rootIDs, nil, mutateRoots, updateSystem)
}

// MutateRootsWithFormatOps is the bulk-entry form of MutateRootsWithFormats.
// Root entries are applied through detached root batches using view/auto-pointer
// fast paths. System entries are built after root publication so callers can
// encode descriptor updates with the published root ids.
func (db *DB) MutateRootsWithFormatOps(sync bool, rootIDs []uint64, formats []*rootfmt.Format, rootOps [][]batchpkg.Entry, buildSystemOps func([]uint64) ([]batchpkg.Entry, error)) ([]uint64, error) {
	if len(rootIDs) != len(rootOps) {
		return nil, fmt.Errorf("mutate root ops length mismatch")
	}
	mutators := make([]func(batchpkg.Interface) error, len(rootOps))
	for i := range rootOps {
		ops := rootOps[i]
		mutators[i] = func(target batchpkg.Interface) error {
			return applyBulkRootOps(target, ops)
		}
	}
	return db.MutateRootsWithFormats(sync, rootIDs, formats, mutators, func(sys batchpkg.Interface, newRootIDs []uint64) error {
		if buildSystemOps == nil {
			return nil
		}
		ops, err := buildSystemOps(newRootIDs)
		if err != nil {
			return err
		}
		return applyBulkRootOps(sys, ops)
	})
}

// MutateRootsWithFormatIterators is the streaming form of MutateRootsWithFormats.
// Root entries are consumed from sorted iterators, allowing callers like cached
// named-root flush to publish memtable state without first materializing a full
// []Entry snapshot.
func (db *DB) MutateRootsWithFormatIterators(sync bool, rootIDs []uint64, formats []*rootfmt.Format, rootIters []iterator.UnsafeIterator, buildSystemOps func([]uint64) ([]batchpkg.Entry, error)) ([]uint64, error) {
	if db == nil {
		return nil, fmt.Errorf("missing db")
	}
	if db.readOnly {
		return nil, ErrReadOnly
	}
	if len(rootIDs) != len(rootIters) {
		return nil, fmt.Errorf("mutate root iterators length mismatch")
	}
	if len(formats) > 0 && len(formats) != len(rootIDs) {
		return nil, fmt.Errorf("mutate root formats length mismatch")
	}

	rootBatches := make([]*detachedBatch, len(rootIDs))
	mergeWarmTables := make([]memtable.Table, len(rootIDs))
	for i := range rootIDs {
		if provider, ok := rootIters[i].(rootMutationTableProvider); ok {
			if table := provider.RootMutationTable(); table != nil {
				mergeWarmTables[i] = table
				if rootIDs[i] != 0 && table.Len() >= rootIteratorWarmMergeMinEntries {
					continue
				}
			}
		}
		if rootIDs[i] == 0 {
			continue
		}
		rootBatch, err := newDetachedBatch(db, false)
		if err != nil {
			return nil, err
		}
		rootBatches[i] = rootBatch
		defer func(batch *detachedBatch) { _ = batch.Close() }(rootBatch)
		if err := applyBulkRootIterator(rootBatch, rootIters[i]); err != nil {
			return nil, err
		}
	}

	for _, rootBatch := range rootBatches {
		if rootBatch != nil && rootBatch.usedAutoPtr {
			if err := db.flushInlineAppender(sync); err != nil {
				return nil, err
			}
			break
		}
	}

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	idx := db.idx.Load()
	if idx == nil {
		return nil, fmt.Errorf("missing index")
	}

	db.mu.RLock()
	baseSeq := db.meta.CommitSeq
	currentUserRoot := db.meta.UserRootPageID
	currentSystemRoot := db.meta.SystemRootPageID
	regID := idx.registry.Register(baseSeq)
	db.mu.RUnlock()
	defer idx.registry.Unregister(regID)

	newRootIDs := make([]uint64, len(rootIDs))
	retired := make([]uint64, 0, len(rootIDs)*2)
	metrics := adaptive.Metrics{}
	valueLogDeltas := make([]*valueLogRefDelta, 0, len(rootIDs))
	forceValueLogRefresh := db.indexOuterLeavesInValueLog
	var touchedValueLogSegments []uint32

	for i := range rootIDs {
		rootID := rootIDs[i]
		format := rootMutationFormatAt(formats, i)
		if rootID == 0 {
			if mergeWarmTables[i] != nil {
				newRootID, rootTouched, rootNeedsRefresh, err := db.buildRootFromTableLocked(idx, format, mergeWarmTables[i], i)
				if err != nil {
					return nil, err
				}
				newRootIDs[i] = newRootID
				touchedValueLogSegments = mergeValueLogSegmentIDs(touchedValueLogSegments, rootTouched)
				if rootNeedsRefresh {
					forceValueLogRefresh = true
				}
				valueLogDeltas = append(valueLogDeltas, nil)
				continue
			}
			newRootID, rootTouched, rootNeedsRefresh, err := db.buildRootFromIteratorLocked(idx, format, rootIters[i], i)
			if err != nil {
				return nil, err
			}
			newRootIDs[i] = newRootID
			touchedValueLogSegments = mergeValueLogSegmentIDs(touchedValueLogSegments, rootTouched)
			if rootNeedsRefresh {
				forceValueLogRefresh = true
			}
			valueLogDeltas = append(valueLogDeltas, nil)
			continue
		}
		if mergeWarmTables[i] != nil && mergeWarmTables[i].Len() >= rootIteratorWarmMergeMinEntries {
			newRootID, rootRetired, rootTouched, rootNeedsRefresh, rootDelta, err := db.mergeRootFromTableLocked(idx, rootID, baseSeq, format, mergeWarmTables[i], i)
			if err != nil {
				return nil, err
			}
			newRootIDs[i] = newRootID
			retired = append(retired, rootRetired...)
			valueLogDeltas = append(valueLogDeltas, rootDelta)
			touchedValueLogSegments = mergeValueLogSegmentIDs(touchedValueLogSegments, rootTouched)
			if rootNeedsRefresh {
				forceValueLogRefresh = true
			}
			continue
		}

		rootBatch := rootBatches[i]
		newRootID, rootRetired, rootMetrics, err := db.zipperForRootFormat(idx, format).Apply(rootID, rootBatch.batch)
		if err != nil {
			return nil, err
		}
		rootDelta, err := db.buildValueLogRefDelta(idx.pager, rootID, baseSeq, rootBatch.batch.SortedEntries())
		if err != nil {
			return nil, err
		}
		newRootIDs[i] = newRootID
		retired = append(retired, rootRetired...)
		metrics = mergeAdaptiveMetrics(metrics, rootMetrics)
		valueLogDeltas = append(valueLogDeltas, rootDelta)
		if db.rootFormatUsesLeafPageValueLog(format) {
			forceValueLogRefresh = true
		}
	}

	systemBatch, err := newDetachedBatch(db, true)
	if err != nil {
		return nil, err
	}
	defer func() { _ = systemBatch.Close() }()
	if buildSystemOps != nil {
		ops, err := buildSystemOps(newRootIDs)
		if err != nil {
			return nil, err
		}
		if err := applyBulkRootOps(systemBatch, ops); err != nil {
			return nil, err
		}
	}

	newSystemRoot := currentSystemRoot
	systemRetired := []uint64(nil)
	systemMetrics := adaptive.Metrics{}
	var systemDelta *valueLogRefDelta
	if len(systemBatch.batch.SortedEntries()) > 0 {
		systemDelta, err = db.buildValueLogRefDelta(idx.pager, currentSystemRoot, baseSeq, systemBatch.batch.SortedEntries())
		if err != nil {
			return nil, err
		}
		newSystemRoot, systemRetired, systemMetrics, err = idx.zipper.Apply(currentSystemRoot, systemBatch.batch)
		if err != nil {
			return nil, err
		}
	}

	retired = append(retired, systemRetired...)
	metrics = mergeAdaptiveMetrics(metrics, systemMetrics)
	valueLogDeltas = append(valueLogDeltas, systemDelta)
	vlogRefDelta := mergeValueLogRefDeltas(valueLogDeltas...)
	touchedValueLogSegments = mergeValueLogSegmentIDs(touchedValueLogSegments, combineTouchedValueLogSegments(append(rootBatches, systemBatch)...))
	if err := db.finalizeCommit(currentUserRoot, newSystemRoot, retired, sync, metrics, touchedValueLogSegments, forceValueLogRefresh, vlogRefDelta); err != nil {
		return nil, err
	}
	return newRootIDs, nil
}

// MutateRootsWithFormatTables publishes root-local memtables directly. This
// avoids iterator -> batch-entry replay for callers that already hold ordered
// table state, such as cached named-root flush and collection batch ingest.
func (db *DB) MutateRootsWithFormatTables(sync bool, rootIDs []uint64, formats []*rootfmt.Format, rootTables []memtable.Table, buildSystemOps func([]uint64) ([]batchpkg.Entry, error)) ([]uint64, error) {
	if db == nil {
		return nil, fmt.Errorf("missing db")
	}
	if db.readOnly {
		return nil, ErrReadOnly
	}
	if len(rootIDs) != len(rootTables) {
		return nil, fmt.Errorf("mutate root tables length mismatch")
	}
	if len(formats) > 0 && len(formats) != len(rootIDs) {
		return nil, fmt.Errorf("mutate root formats length mismatch")
	}

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	idx := db.idx.Load()
	if idx == nil {
		return nil, fmt.Errorf("missing index")
	}

	db.mu.RLock()
	baseSeq := db.meta.CommitSeq
	currentUserRoot := db.meta.UserRootPageID
	currentSystemRoot := db.meta.SystemRootPageID
	regID := idx.registry.Register(baseSeq)
	db.mu.RUnlock()
	defer idx.registry.Unregister(regID)

	newRootIDs := make([]uint64, len(rootIDs))
	retired := make([]uint64, 0, len(rootIDs)*2)
	valueLogDeltas := make([]*valueLogRefDelta, 0, len(rootIDs))
	forceValueLogRefresh := db.indexOuterLeavesInValueLog
	var touchedValueLogSegments []uint32

	for i := range rootIDs {
		rootID := rootIDs[i]
		format := rootMutationFormatAt(formats, i)
		table := rootTables[i]
		if rootID == 0 {
			newRootID, rootTouched, rootNeedsRefresh, err := db.buildRootFromTableLocked(idx, format, table, i)
			if err != nil {
				return nil, err
			}
			newRootIDs[i] = newRootID
			touchedValueLogSegments = mergeValueLogSegmentIDs(touchedValueLogSegments, rootTouched)
			if rootNeedsRefresh {
				forceValueLogRefresh = true
			}
			valueLogDeltas = append(valueLogDeltas, nil)
			continue
		}

		newRootID, rootRetired, rootTouched, rootNeedsRefresh, rootDelta, err := db.mergeRootFromTableLocked(idx, rootID, baseSeq, format, table, i)
		if err != nil {
			return nil, err
		}
		newRootIDs[i] = newRootID
		retired = append(retired, rootRetired...)
		valueLogDeltas = append(valueLogDeltas, rootDelta)
		touchedValueLogSegments = mergeValueLogSegmentIDs(touchedValueLogSegments, rootTouched)
		if rootNeedsRefresh {
			forceValueLogRefresh = true
		}
	}

	systemBatch, err := newDetachedBatch(db, true)
	if err != nil {
		return nil, err
	}
	defer func() { _ = systemBatch.Close() }()
	if buildSystemOps != nil {
		ops, err := buildSystemOps(newRootIDs)
		if err != nil {
			return nil, err
		}
		if err := applyBulkRootOps(systemBatch, ops); err != nil {
			return nil, err
		}
	}

	newSystemRoot := currentSystemRoot
	systemRetired := []uint64(nil)
	systemMetrics := adaptive.Metrics{}
	var systemDelta *valueLogRefDelta
	if len(systemBatch.batch.SortedEntries()) > 0 {
		systemDelta, err = db.buildValueLogRefDelta(idx.pager, currentSystemRoot, baseSeq, systemBatch.batch.SortedEntries())
		if err != nil {
			return nil, err
		}
		newSystemRoot, systemRetired, systemMetrics, err = idx.zipper.Apply(currentSystemRoot, systemBatch.batch)
		if err != nil {
			return nil, err
		}
	}

	retired = append(retired, systemRetired...)
	vlogRefDelta := mergeValueLogRefDeltas(append(valueLogDeltas, systemDelta)...)
	touchedValueLogSegments = mergeValueLogSegmentIDs(touchedValueLogSegments, combineTouchedValueLogSegments(systemBatch))
	if err := db.finalizeCommit(currentUserRoot, newSystemRoot, retired, sync, systemMetrics, touchedValueLogSegments, forceValueLogRefresh, vlogRefDelta); err != nil {
		return nil, err
	}
	return newRootIDs, nil
}

// MutateRootsWithFormats is the function-based form of MutateRoots with
// explicit per-root format overrides.
func (db *DB) MutateRootsWithFormats(sync bool, rootIDs []uint64, formats []*rootfmt.Format, mutateRoots []func(batchpkg.Interface) error, updateSystem func(batchpkg.Interface, []uint64) error) ([]uint64, error) {
	if db == nil {
		return nil, fmt.Errorf("missing db")
	}
	if db.readOnly {
		return nil, ErrReadOnly
	}
	if len(rootIDs) != len(mutateRoots) {
		return nil, fmt.Errorf("mutate roots length mismatch")
	}
	if len(formats) > 0 && len(formats) != len(rootIDs) {
		return nil, fmt.Errorf("mutate root formats length mismatch")
	}

	rootBatches := make([]*detachedBatch, len(rootIDs))
	for i := range rootIDs {
		rootBatch, err := newDetachedBatch(db, false)
		if err != nil {
			return nil, err
		}
		rootBatches[i] = rootBatch
		defer func(batch *detachedBatch) { _ = batch.Close() }(rootBatch)
		if mutateRoots[i] == nil {
			continue
		}
		if err := mutateRoots[i](rootBatch); err != nil {
			return nil, err
		}
	}

	for _, rootBatch := range rootBatches {
		if rootBatch != nil && rootBatch.usedAutoPtr {
			if err := db.flushInlineAppender(sync); err != nil {
				return nil, err
			}
			break
		}
	}

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	idx := db.idx.Load()
	if idx == nil {
		return nil, fmt.Errorf("missing index")
	}

	db.mu.RLock()
	baseSeq := db.meta.CommitSeq
	currentUserRoot := db.meta.UserRootPageID
	currentSystemRoot := db.meta.SystemRootPageID
	regID := idx.registry.Register(baseSeq)
	db.mu.RUnlock()
	defer idx.registry.Unregister(regID)

	newRootIDs := make([]uint64, len(rootIDs))
	retired := make([]uint64, 0, len(rootIDs)*2)
	metrics := adaptive.Metrics{}
	valueLogDeltas := make([]*valueLogRefDelta, 0, len(rootIDs))
	forceValueLogRefresh := db.indexOuterLeavesInValueLog

	for i := range rootIDs {
		rootID := rootIDs[i]
		rootBatch := rootBatches[i]
		format := rootMutationFormatAt(formats, i)
		if rootID == 0 && rootBatch != nil && len(rootBatch.batch.SortedEntries()) > 0 {
			var err error
			rootID, err = db.allocateDetachedRootLocked(idx, format)
			if err != nil {
				return nil, err
			}
		}

		newRootID, rootRetired, rootMetrics, err := db.zipperForRootFormat(idx, format).Apply(rootID, rootBatch.batch)
		if err != nil {
			return nil, err
		}
		rootDelta, err := db.buildValueLogRefDelta(idx.pager, rootID, baseSeq, rootBatch.batch.SortedEntries())
		if err != nil {
			return nil, err
		}
		newRootIDs[i] = newRootID
		retired = append(retired, rootRetired...)
		metrics = mergeAdaptiveMetrics(metrics, rootMetrics)
		valueLogDeltas = append(valueLogDeltas, rootDelta)
		if db.rootFormatUsesLeafPageValueLog(format) {
			forceValueLogRefresh = true
		}
	}

	systemBatch, err := newDetachedBatch(db, true)
	if err != nil {
		return nil, err
	}
	defer func() { _ = systemBatch.Close() }()
	if updateSystem != nil {
		if err := updateSystem(systemBatch, newRootIDs); err != nil {
			return nil, err
		}
	}

	newSystemRoot := currentSystemRoot
	systemRetired := []uint64(nil)
	systemMetrics := adaptive.Metrics{}
	var systemDelta *valueLogRefDelta
	if len(systemBatch.batch.SortedEntries()) > 0 {
		systemDelta, err = db.buildValueLogRefDelta(idx.pager, currentSystemRoot, baseSeq, systemBatch.batch.SortedEntries())
		if err != nil {
			return nil, err
		}
		newSystemRoot, systemRetired, systemMetrics, err = idx.zipper.Apply(currentSystemRoot, systemBatch.batch)
		if err != nil {
			return nil, err
		}
	}

	retired = append(retired, systemRetired...)
	metrics = mergeAdaptiveMetrics(metrics, systemMetrics)
	valueLogDeltas = append(valueLogDeltas, systemDelta)
	vlogRefDelta := mergeValueLogRefDeltas(valueLogDeltas...)
	touchedValueLogSegments := combineTouchedValueLogSegments(append(rootBatches, systemBatch)...)
	if err := db.finalizeCommit(currentUserRoot, newSystemRoot, retired, sync, metrics, touchedValueLogSegments, forceValueLogRefresh, vlogRefDelta); err != nil {
		return nil, err
	}
	return newRootIDs, nil
}

func applyBulkRootOps(target batchpkg.Interface, ops []batchpkg.Entry) error {
	if len(ops) == 0 {
		return nil
	}
	if reserve, ok := target.(interface{ Reserve(int) }); ok {
		reserve.Reserve(len(ops))
	}
	if autoOps, ok := target.(interface{ SetAutoOpsView([]batchpkg.Entry) error }); ok {
		return autoOps.SetAutoOpsView(ops)
	}
	if setOps, ok := target.(interface{ SetOps([]batchpkg.Entry) error }); ok {
		return setOps.SetOps(ops)
	}
	for _, op := range ops {
		switch op.Type {
		case batchpkg.OpDelete:
			if deleteView, ok := target.(interface{ DeleteView([]byte) error }); ok {
				if err := deleteView.DeleteView(op.Key); err != nil {
					return err
				}
			} else if err := target.Delete(op.Key); err != nil {
				return err
			}
		case batchpkg.OpPut:
			if op.IsPtr {
				if setPtr, ok := target.(interface {
					SetPointerView([]byte, page.ValuePtr) error
				}); ok {
					if err := setPtr.SetPointerView(op.Key, op.ValuePtr); err != nil {
						return err
					}
					continue
				}
			}
			if autoView, ok := target.(interface{ SetAutoView([]byte, []byte) error }); ok {
				if err := autoView.SetAutoView(op.Key, op.Value); err != nil {
					return err
				}
			} else if setView, ok := target.(interface{ SetView([]byte, []byte) error }); ok {
				if err := setView.SetView(op.Key, op.Value); err != nil {
					return err
				}
			} else if err := target.Set(op.Key, op.Value); err != nil {
				return err
			}
		}
	}
	return nil
}

func applyBulkRootIterator(target batchpkg.Interface, iter iterator.UnsafeIterator) (err error) {
	if iter == nil {
		return nil
	}
	defer func() {
		closeErr := iter.Close()
		if err == nil {
			err = closeErr
		}
	}()

	const chunkCap = 1024
	stable := false
	if st, ok := iter.(stableRootEntryIterator); ok {
		stable = st.StableUnsafeIteratorSlices()
	}

	ops := make([]batchpkg.Entry, 0, chunkCap)
	flush := func() error {
		if len(ops) == 0 {
			return nil
		}
		if err := applyBulkRootOps(target, ops); err != nil {
			return err
		}
		ops = ops[:0]
		return nil
	}

	for iter.Valid() {
		key := iter.UnsafeKey()
		value, ptr, flags := iter.UnsafeEntry()
		if !stable {
			key = append([]byte(nil), key...)
			if flags&node.FlagTombstone == 0 && len(value) > 0 {
				value = append([]byte(nil), value...)
			}
		}

		entry := batchpkg.Entry{Key: key}
		switch {
		case flags&node.FlagTombstone != 0:
			entry.Type = batchpkg.OpDelete
		case flags&node.FlagPointer != 0:
			entry.Type = batchpkg.OpPut
			entry.IsPtr = true
			entry.ValuePtr = ptr
			entry.Value = value
		default:
			entry.Type = batchpkg.OpPut
			entry.Value = value
		}
		ops = append(ops, entry)
		iter.Next()
		if len(ops) == cap(ops) {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	return flush()
}

func (db *DB) buildValueLogRefDeltaFromIterator(p *pager.Pager, rootID uint64, baseSeq uint64, iter iterator.UnsafeIterator) (*valueLogRefDelta, error) {
	if db == nil || db.valueLogRefTracker == nil || !db.valueLogRefTracker.canTrack(baseSeq) {
		return nil, nil
	}
	delta := newValueLogRefDelta()
	if p == nil || iter == nil {
		return delta, nil
	}
	reader := ValueReaderForState(db.State())
	tr := tree.New(p, reader, rootID)
	for iter.Valid() {
		key := iter.UnsafeKey()
		_, ptr, flags := iter.UnsafeEntry()
		oldFileID, oldRef, err := lookupValueLogRefAtKey(tr, key)
		if err != nil {
			return nil, err
		}
		if oldRef {
			delta.add(oldFileID, -1)
		}
		if flags&node.FlagPointer != 0 && page.IsValueLogFileID(ptr.FileID) {
			delta.add(ptr.FileID, 1)
		}
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return delta, nil
}

type rootDeltaMergeSource uint8

const (
	rootDeltaMergeSourceNone rootDeltaMergeSource = iota
	rootDeltaMergeSourceDelta
	rootDeltaMergeSourceBase
)

type rootDeltaMergeIterator struct {
	delta          iterator.UnsafeIterator
	base           iterator.UnsafeIterator
	current        rootDeltaMergeSource
	skipBaseOnNext bool
}

func newRootDeltaMergeIterator(delta, base iterator.UnsafeIterator) *rootDeltaMergeIterator {
	it := &rootDeltaMergeIterator{
		delta: delta,
		base:  base,
	}
	it.advance()
	return it
}

func (it *rootDeltaMergeIterator) advance() {
	it.current = rootDeltaMergeSourceNone
	if it == nil {
		return
	}
	deltaValid := it.delta != nil && it.delta.Valid()
	baseValid := it.base != nil && it.base.Valid()
	switch {
	case deltaValid && baseValid:
		cmp := bytes.Compare(it.delta.UnsafeKey(), it.base.UnsafeKey())
		switch {
		case cmp < 0:
			it.current = rootDeltaMergeSourceDelta
		case cmp == 0:
			it.current = rootDeltaMergeSourceDelta
			it.skipBaseOnNext = true
		default:
			it.current = rootDeltaMergeSourceBase
		}
	case deltaValid:
		it.current = rootDeltaMergeSourceDelta
	case baseValid:
		it.current = rootDeltaMergeSourceBase
	}
}

func (it *rootDeltaMergeIterator) Valid() bool {
	return it != nil && it.current != rootDeltaMergeSourceNone
}

func (it *rootDeltaMergeIterator) Next() {
	if it == nil {
		return
	}
	switch it.current {
	case rootDeltaMergeSourceDelta:
		if it.delta != nil {
			it.delta.Next()
		}
		if it.skipBaseOnNext {
			it.skipBaseOnNext = false
			if it.base != nil && it.base.Valid() {
				it.base.Next()
			}
		}
	case rootDeltaMergeSourceBase:
		if it.base != nil {
			it.base.Next()
		}
	}
	it.advance()
}

func (it *rootDeltaMergeIterator) Seek(key []byte) {
	if it == nil {
		return
	}
	it.skipBaseOnNext = false
	if it.delta != nil {
		it.delta.Seek(key)
	}
	if it.base != nil {
		it.base.Seek(key)
	}
	it.advance()
}

func (it *rootDeltaMergeIterator) source() iterator.UnsafeIterator {
	switch it.current {
	case rootDeltaMergeSourceDelta:
		return it.delta
	case rootDeltaMergeSourceBase:
		return it.base
	default:
		return nil
	}
}

func (it *rootDeltaMergeIterator) UnsafeKey() []byte {
	src := it.source()
	if src == nil {
		return nil
	}
	return src.UnsafeKey()
}

func (it *rootDeltaMergeIterator) UnsafeValue() []byte {
	src := it.source()
	if src == nil {
		return nil
	}
	return src.UnsafeValue()
}

func (it *rootDeltaMergeIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	src := it.source()
	if src == nil {
		return nil, page.ValuePtr{}, 0
	}
	return src.UnsafeEntry()
}

func (it *rootDeltaMergeIterator) Key() []byte { return it.UnsafeKey() }

func (it *rootDeltaMergeIterator) Value() []byte { return it.UnsafeValue() }

func (it *rootDeltaMergeIterator) KeyCopy(dst []byte) []byte {
	src := it.source()
	if src == nil {
		return dst[:0]
	}
	return src.KeyCopy(dst)
}

func (it *rootDeltaMergeIterator) ValueCopy(dst []byte) []byte {
	src := it.source()
	if src == nil {
		return dst[:0]
	}
	return src.ValueCopy(dst)
}

func (it *rootDeltaMergeIterator) IsDeleted() bool {
	src := it.source()
	return src != nil && src.IsDeleted()
}

func (it *rootDeltaMergeIterator) Error() error {
	if it == nil {
		return nil
	}
	if it.delta != nil {
		if err := it.delta.Error(); err != nil {
			return err
		}
	}
	if it.base != nil {
		return it.base.Error()
	}
	return nil
}

func (it *rootDeltaMergeIterator) Close() error {
	if it == nil {
		return nil
	}
	var err error
	if it.delta != nil {
		err = it.delta.Close()
	}
	if it.base != nil {
		if closeErr := it.base.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}

func (it *rootDeltaMergeIterator) Domain() (start, end []byte) {
	return nil, nil
}

func collectRootIteratorEntries(iter iterator.UnsafeIterator) ([]batchpkg.Entry, error) {
	if iter == nil {
		return nil, nil
	}
	stable := false
	if st, ok := iter.(stableRootEntryIterator); ok {
		stable = st.StableUnsafeIteratorSlices()
	}
	entries := make([]batchpkg.Entry, 0, 128)
	for iter.Valid() {
		key := iter.UnsafeKey()
		value, ptr, flags := iter.UnsafeEntry()
		if !stable {
			key = append([]byte(nil), key...)
			if len(value) > 0 {
				value = append([]byte(nil), value...)
			}
		}
		entry := batchpkg.Entry{Key: key}
		switch {
		case flags&node.FlagTombstone != 0:
			entry.Type = batchpkg.OpDelete
		case flags&node.FlagPointer != 0:
			entry.Type = batchpkg.OpPut
			entry.IsPtr = true
			entry.ValuePtr = ptr
			entry.Value = value
		default:
			entry.Type = batchpkg.OpPut
			entry.Value = value
		}
		entries = append(entries, entry)
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return entries, nil
}

func (db *DB) mutateRootInternal(rootID uint64, format *rootfmt.Format, sync bool, mutateRoot func(batchpkg.Interface) error, mutateUser func(batchpkg.Interface) error, updateSystem func(batchpkg.Interface, uint64) error) (uint64, error) {
	if db == nil {
		return 0, fmt.Errorf("missing db")
	}
	if db.readOnly {
		return 0, ErrReadOnly
	}

	rootBatch, err := newDetachedBatch(db, false)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rootBatch.Close() }()
	if mutateRoot != nil {
		if err := mutateRoot(rootBatch); err != nil {
			return 0, err
		}
	}

	var userBatch *detachedBatch
	if mutateUser != nil {
		userBatch, err = newDetachedBatch(db, false)
		if err != nil {
			return 0, err
		}
		defer func() { _ = userBatch.Close() }()
		if err := mutateUser(userBatch); err != nil {
			return 0, err
		}
	}

	if rootBatch.usedAutoPtr {
		if err := db.flushInlineAppender(sync); err != nil {
			return 0, err
		}
	}
	if userBatch != nil && userBatch.usedAutoPtr {
		if err := db.flushInlineAppender(sync); err != nil {
			return 0, err
		}
	}

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	idx := db.idx.Load()
	if idx == nil {
		return 0, fmt.Errorf("missing index")
	}

	db.mu.RLock()
	baseSeq := db.meta.CommitSeq
	currentUserRoot := db.meta.UserRootPageID
	currentSystemRoot := db.meta.SystemRootPageID
	regID := idx.registry.Register(baseSeq)
	db.mu.RUnlock()
	defer idx.registry.Unregister(regID)

	if rootID == 0 && len(rootBatch.batch.SortedEntries()) > 0 {
		rootID, err = db.allocateDetachedRootLocked(idx, format)
		if err != nil {
			return 0, err
		}
	}

	newRootID, rootRetired, rootMetrics, err := db.zipperForRootFormat(idx, format).Apply(rootID, rootBatch.batch)
	if err != nil {
		return 0, err
	}
	rootDelta, err := db.buildValueLogRefDelta(idx.pager, rootID, baseSeq, rootBatch.batch.SortedEntries())
	if err != nil {
		return 0, err
	}

	newUserRoot := currentUserRoot
	userRetired := []uint64(nil)
	userMetrics := adaptive.Metrics{}
	var userDelta *valueLogRefDelta
	if userBatch != nil {
		newUserRoot, userRetired, userMetrics, err = idx.zipper.Apply(currentUserRoot, userBatch.batch)
		if err != nil {
			return 0, err
		}
		userDelta, err = db.buildValueLogRefDelta(idx.pager, currentUserRoot, baseSeq, userBatch.batch.SortedEntries())
		if err != nil {
			return 0, err
		}
	}

	systemBatch, err := newDetachedBatch(db, true)
	if err != nil {
		return 0, err
	}
	defer func() { _ = systemBatch.Close() }()
	if updateSystem != nil {
		if err := updateSystem(systemBatch, newRootID); err != nil {
			return 0, err
		}
	}

	newSystemRoot := currentSystemRoot
	systemRetired := []uint64(nil)
	systemMetrics := adaptive.Metrics{}
	if len(systemBatch.batch.SortedEntries()) > 0 {
		newSystemRoot, systemRetired, systemMetrics, err = idx.zipper.Apply(currentSystemRoot, systemBatch.batch)
		if err != nil {
			return 0, err
		}
	}

	retired := append([]uint64{}, rootRetired...)
	retired = append(retired, userRetired...)
	retired = append(retired, systemRetired...)

	metrics := mergeAdaptiveMetrics(rootMetrics, userMetrics, systemMetrics)
	touchedValueLogSegments := combineTouchedValueLogSegments(rootBatch, userBatch, systemBatch)
	vlogRefDelta := mergeValueLogRefDeltas(rootDelta, userDelta)
	if err := db.finalizeCommit(newUserRoot, newSystemRoot, retired, sync, metrics, touchedValueLogSegments, db.indexOuterLeavesInValueLog || db.rootFormatUsesLeafPageValueLog(format), vlogRefDelta); err != nil {
		return 0, err
	}
	if db.vacuum.Active() && userBatch != nil {
		db.vacuum.RecordOps(userBatch.batch.Ops())
	}
	return newRootID, nil
}

type rootBuildIterator struct {
	inner           iterator.UnsafeIterator
	touchedSegments []uint32
	sawPointers     bool
}

func newRootBuildIterator(inner iterator.UnsafeIterator) *rootBuildIterator {
	it := &rootBuildIterator{inner: inner}
	it.skipDeletes()
	return it
}

func (it *rootBuildIterator) skipDeletes() {
	for it != nil && it.inner != nil && it.inner.Valid() && it.inner.IsDeleted() {
		it.inner.Next()
	}
}

func (it *rootBuildIterator) Valid() bool { return it != nil && it.inner != nil && it.inner.Valid() }

func (it *rootBuildIterator) Next() {
	if it == nil || it.inner == nil {
		return
	}
	it.inner.Next()
	it.skipDeletes()
}

func (it *rootBuildIterator) Seek(key []byte) {
	if it == nil || it.inner == nil {
		return
	}
	it.inner.Seek(key)
	it.skipDeletes()
}

func (it *rootBuildIterator) UnsafeKey() []byte {
	if !it.Valid() {
		return nil
	}
	return it.inner.UnsafeKey()
}

func (it *rootBuildIterator) UnsafeValue() []byte {
	value, _, _ := it.UnsafeEntry()
	return value
}

func (it *rootBuildIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.Valid() {
		return nil, page.ValuePtr{}, 0
	}
	value, ptr, flags := it.inner.UnsafeEntry()
	if flags&node.FlagPointer != 0 && page.IsValueLogFileID(ptr.FileID) {
		it.sawPointers = true
		it.touchedSegments = mergeValueLogSegmentIDs(it.touchedSegments, []uint32{ptr.FileID})
	}
	return value, ptr, flags &^ node.FlagTombstone
}

func (it *rootBuildIterator) Key() []byte { return it.UnsafeKey() }

func (it *rootBuildIterator) Value() []byte { return it.UnsafeValue() }

func (it *rootBuildIterator) KeyCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst[:0]
	}
	return it.inner.KeyCopy(dst)
}

func (it *rootBuildIterator) ValueCopy(dst []byte) []byte {
	value, _, _ := it.UnsafeEntry()
	return append(dst[:0], value...)
}

func (it *rootBuildIterator) IsDeleted() bool { return false }

func (it *rootBuildIterator) Error() error {
	if it == nil || it.inner == nil {
		return nil
	}
	return it.inner.Error()
}

func (it *rootBuildIterator) Close() error {
	if it == nil || it.inner == nil {
		return nil
	}
	return it.inner.Close()
}

func (it *rootBuildIterator) Domain() (start, end []byte) {
	if it == nil || it.inner == nil {
		return nil, nil
	}
	return it.inner.Domain()
}

func (db *DB) mergeRootFromIteratorLocked(idx *indexGen, rootID uint64, baseSeq uint64, format *rootfmt.Format, deltaIter iterator.UnsafeIterator, deltaTable memtable.Table, rootIndex int) (uint64, []uint64, []uint32, bool, *valueLogRefDelta, error) {
	if idx == nil {
		return 0, nil, nil, false, nil, fmt.Errorf("missing index")
	}
	if deltaIter == nil {
		return 0, nil, nil, false, nil, fmt.Errorf("missing delta iterator")
	}
	if deltaTable == nil {
		return 0, nil, nil, false, nil, fmt.Errorf("missing delta table")
	}
	_ = deltaIter

	runRootIteratorWarmMergeTestHook(rootIndex)

	return db.mergeRootFromTableLocked(idx, rootID, baseSeq, format, deltaTable, rootIndex)
}

func (db *DB) buildRootFromIteratorLocked(idx *indexGen, format *rootfmt.Format, iter iterator.UnsafeIterator, rootIndex int) (uint64, []uint32, bool, error) {
	if iter == nil {
		iter = &emptyIterator{}
	}
	buildIter := newRootBuildIterator(iter)
	defer func() { _ = buildIter.Close() }()

	rootFormat := db.normalizeRootMutationFormat(format)
	buildOpts := bulk.BuildOptions{
		LeafPrefixCompression: rootFormat.LeafPrefixCompression,
		LeafColumnar:          db.indexColumnarLeaves,
		PackedValuePtr:        db.indexPackedValuePtr,
		InternalBaseDelta:     db.indexInternalBaseDelta,
	}
	if rootFormat.OuterLeavesInValueLog {
		buildOpts.LeafPageLog = db.leafPageLog
	}
	runRootIteratorBulkBuildTestHook(rootIndex)
	newRootID, err := bulk.BuildWithOptions(buildIter, &pagerAllocator{p: idx.pager}, idx.pager, buildOpts)
	if err != nil {
		return 0, nil, false, err
	}
	return newRootID, buildIter.touchedSegments, rootFormat.OuterLeavesInValueLog || buildIter.sawPointers, nil
}

func (db *DB) buildRootFromTableLocked(idx *indexGen, format *rootfmt.Format, table memtable.Table, rootIndex int) (uint64, []uint32, bool, error) {
	if table == nil {
		return db.buildRootFromIteratorLocked(idx, format, &emptyIterator{}, rootIndex)
	}
	return db.buildRootFromIteratorLocked(idx, format, table.NewIterator(nil, nil), rootIndex)
}

func (db *DB) mergeRootFromTableLocked(idx *indexGen, rootID uint64, baseSeq uint64, format *rootfmt.Format, deltaTable memtable.Table, rootIndex int) (uint64, []uint64, []uint32, bool, *valueLogRefDelta, error) {
	if idx == nil {
		return 0, nil, nil, false, nil, fmt.Errorf("missing index")
	}
	if deltaTable == nil {
		return 0, nil, nil, false, nil, fmt.Errorf("missing delta table")
	}

	runRootIteratorWarmMergeTestHook(rootIndex)

	deltaScanIter := deltaTable.NewIterator(nil, nil)
	delta, err := db.buildValueLogRefDeltaFromIterator(idx.pager, rootID, baseSeq, deltaScanIter)
	if closeErr := deltaScanIter.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return 0, nil, nil, false, nil, err
	}

	reader := ValueReaderForState(db.State())
	tr := tree.New(idx.pager, reader, rootID)
	retired, err := tr.CollectPageIDs()
	if err != nil {
		return 0, nil, nil, false, nil, err
	}
	baseIter := tr.Iterator(nil, nil)
	mergedIter := newRootDeltaMergeIterator(deltaTable.NewIterator(nil, nil), baseIter)
	newRootID, touched, needsRefresh, err := db.buildRootFromIteratorLocked(idx, format, mergedIter, rootIndex)
	if err != nil {
		return 0, nil, nil, false, nil, err
	}
	return newRootID, retired, touched, needsRefresh, delta, nil
}

func newDetachedBatch(db *DB, system bool) (*detachedBatch, error) {
	if db == nil {
		return nil, fmt.Errorf("missing db")
	}
	target := batchRootUser
	if system {
		target = batchRootSystem
	}
	b := detachedBatchPool.Get().(*detachedBatch)
	b.db = db
	b.batch = db.newInternalBatch(0)
	b.targetRoot = target
	b.usedAutoPtr = false
	return b, nil
}

func (b *detachedBatch) Close() error {
	if b == nil {
		return nil
	}
	var err error
	if b.batch != nil {
		err = b.batch.Close()
	}
	b.db = nil
	b.batch = nil
	b.targetRoot = batchRootUser
	b.usedAutoPtr = false
	detachedBatchPool.Put(b)
	return err
}

func combineTouchedValueLogSegments(batches ...*detachedBatch) []uint32 {
	var out []uint32
	for _, b := range batches {
		if b == nil || b.batch == nil {
			continue
		}
		touched := b.batch.TouchedValueLogSegments()
		if len(touched) == 0 {
			continue
		}
		if out == nil {
			out = touched
			continue
		}
		for _, fileID := range touched {
			if containsValueLogSegmentID(out, fileID) {
				continue
			}
			out = append(out, fileID)
		}
	}
	return out
}

func containsValueLogSegmentID(ids []uint32, target uint32) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}

func mergeValueLogRefDeltas(deltas ...*valueLogRefDelta) *valueLogRefDelta {
	var merged *valueLogRefDelta
	for _, delta := range deltas {
		if delta == nil || len(delta.changes) == 0 {
			continue
		}
		if merged == nil {
			merged = newValueLogRefDelta()
		}
		for fileID, change := range delta.changes {
			merged.add(fileID, change)
		}
	}
	if merged != nil && len(merged.changes) == 0 {
		return nil
	}
	return merged
}

func mergeAdaptiveMetrics(metrics ...adaptive.Metrics) adaptive.Metrics {
	var out adaptive.Metrics
	for _, metric := range metrics {
		if metric.LeafFill > out.LeafFill {
			out.LeafFill = metric.LeafFill
		}
		out.Splits += metric.Splits
		out.IndexWriteBytes += metric.IndexWriteBytes
		out.SlabWriteBytes += metric.SlabWriteBytes
		out.SlabDeadBytes += metric.SlabDeadBytes
		if len(metric.SlabWriteBytesByFile) > 0 {
			if out.SlabWriteBytesByFile == nil {
				out.SlabWriteBytesByFile = make(map[uint32]int64, len(metric.SlabWriteBytesByFile))
			}
			for fileID, bytesWritten := range metric.SlabWriteBytesByFile {
				out.SlabWriteBytesByFile[fileID] += bytesWritten
			}
		}
		if len(metric.SlabDeadBytesByFile) > 0 {
			if out.SlabDeadBytesByFile == nil {
				out.SlabDeadBytesByFile = make(map[uint32]int64, len(metric.SlabDeadBytesByFile))
			}
			for fileID, deadBytes := range metric.SlabDeadBytesByFile {
				out.SlabDeadBytesByFile[fileID] += deadBytes
			}
		}
	}
	return out
}

func (db *DB) allocateDetachedRootLocked(idx *indexGen, format *rootfmt.Format) (uint64, error) {
	if db == nil || idx == nil || idx.pager == nil {
		return 0, fmt.Errorf("missing index pager")
	}
	rootFormat := db.normalizeRootMutationFormat(format)
	rootID, err := idx.pager.Alloc(1)
	if err != nil {
		return 0, err
	}
	data, err := idx.pager.GetForWrite(rootID)
	if err != nil {
		return 0, err
	}
	builder := node.NewBuilderWithOptions(data, page.PageTypeLeaf, node.BuilderOptions{
		LeafPrefixCompression: rootFormat.LeafPrefixCompression,
		LeafColumnar:          db.indexColumnarLeaves,
		PackedValuePtr:        db.indexPackedValuePtr,
		InternalBaseDelta:     db.indexInternalBaseDelta,
	})
	builder.SetPageID(rootID)
	builder.Finish()
	return rootID, nil
}

func rootMutationFormatAt(formats []*rootfmt.Format, index int) *rootfmt.Format {
	if len(formats) == 0 || index < 0 || index >= len(formats) {
		return nil
	}
	return formats[index]
}

func (db *DB) normalizeRootMutationFormat(format *rootfmt.Format) rootfmt.Format {
	if format != nil {
		return *format
	}
	return rootfmt.Format{
		OuterLeavesInValueLog: db.indexOuterLeavesInValueLog,
		LeafPrefixCompression: db.leafPrefixCompression,
		AllowValues:           true,
	}
}

func (db *DB) rootFormatUsesLeafPageValueLog(format *rootfmt.Format) bool {
	return db.normalizeRootMutationFormat(format).OuterLeavesInValueLog
}

func (db *DB) zipperForRootFormat(idx *indexGen, format *rootfmt.Format) interface {
	Apply(rootID uint64, b *batchpkg.Batch) (uint64, []uint64, adaptive.Metrics, error)
} {
	if idx == nil || idx.zipper == nil {
		return nil
	}
	rootFormat := db.normalizeRootMutationFormat(format)
	if rootFormat.OuterLeavesInValueLog == db.indexOuterLeavesInValueLog && rootFormat.LeafPrefixCompression == db.leafPrefixCompression {
		return idx.zipper
	}
	clone := idx.zipper.CloneWithAllocator(idx.allocator)
	clone.SetLeafPrefixCompression(rootFormat.LeafPrefixCompression)
	clone.SetIndexInternalBaseDelta(db.indexInternalBaseDelta)
	clone.SetOuterLeavesInValueLog(rootFormat.OuterLeavesInValueLog)
	if rootFormat.OuterLeavesInValueLog {
		clone.SetLeafPageReader(db.valueLogManager)
		clone.SetLeafPageLog(db.leafPageLog)
	} else {
		clone.SetLeafPageLog(nil)
	}
	return clone
}

type emptyIterator struct{}

func (it *emptyIterator) Valid() bool                                { return false }
func (it *emptyIterator) Next()                                      {}
func (it *emptyIterator) Seek(key []byte)                            {}
func (it *emptyIterator) Key() []byte                                { return nil }
func (it *emptyIterator) Value() []byte                              { return nil }
func (it *emptyIterator) KeyCopy(dst []byte) []byte                  { return nil }
func (it *emptyIterator) ValueCopy(dst []byte) []byte                { return nil }
func (it *emptyIterator) UnsafeKey() []byte                          { return nil }
func (it *emptyIterator) UnsafeValue() []byte                        { return nil }
func (it *emptyIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) { return nil, page.ValuePtr{}, 0 }
func (it *emptyIterator) IsDeleted() bool                            { return false }
func (it *emptyIterator) Error() error                               { return nil }
func (it *emptyIterator) Close() error                               { return nil }
func (it *emptyIterator) Domain() ([]byte, []byte)                   { return nil, nil }
