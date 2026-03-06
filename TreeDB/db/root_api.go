package db

import (
	"fmt"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
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

	rootBatches := make([]*Batch, len(rootIDs))
	for i := range rootIDs {
		rootBatch, err := newDetachedBatch(db, false)
		if err != nil {
			return nil, err
		}
		rootBatches[i] = rootBatch
		defer func(batch *Batch) { _ = batch.Close() }(rootBatch)
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

	var userBatch *Batch
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

func newDetachedBatch(db *DB, system bool) (*Batch, error) {
	if db == nil {
		return nil, fmt.Errorf("missing db")
	}
	threshold := db.InlineThreshold()
	domains := db.valueLogDomainThresholds
	internal := batchpkg.New(db.valueLogManager, threshold)
	if threshold > 0 {
		internal.SetInlineThresholdResolver(func(key []byte) int {
			return ResolveInlineThresholdForKey(threshold, key, domains)
		})
	}
	target := batchRootUser
	if system {
		target = batchRootSystem
	}
	return &Batch{
		db:         db,
		batch:      internal,
		targetRoot: target,
	}, nil
}

func combineTouchedValueLogSegments(batches ...*Batch) []uint32 {
	if len(batches) == 0 {
		return nil
	}
	seen := make(map[uint32]struct{}, 8)
	out := make([]uint32, 0, 8)
	for _, b := range batches {
		if b == nil || b.batch == nil {
			continue
		}
		for _, fileID := range b.batch.TouchedValueLogSegments() {
			if _, ok := seen[fileID]; ok {
				continue
			}
			seen[fileID] = struct{}{}
			out = append(out, fileID)
		}
	}
	return out
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
