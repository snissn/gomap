package db

import (
	"bytes"
	"errors"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/TreeDB/zipper"
)

type orderedRootPublishPlan uint8

const (
	orderedRootPublishPlanColdBuild orderedRootPublishPlan = iota
	orderedRootPublishPlanWarmFallbackRebuild
	orderedRootPublishPlanWarmNativeApply
)

// orderedRootDeltaBatchInlineThreshold is intentionally not the page/value-log
// placement threshold. These batches are transient root-local mutation streams
// consumed by zipper.Apply; they do not decide durable value placement. Large
// collection documents must remain valid here because the destination ordered
// root policy decides whether rebuilt leaves live in pager pages or value-log
// LeafRefs. Pointer entries still flow through SetPointer, and non-stable inline
// iterators may copy values into this short-lived batch, so callers should keep
// delta streams bounded rather than using this as a bulk-load accumulator.
var orderedRootDeltaBatchInlineThreshold = int(^uint(0) >> 1)

type orderedRootPublishStats struct {
	warmAttempts                           uint64
	warmNativeApplyAttempts                uint64
	warmRebuildFallbacks                   uint64
	warmPreservedPages                     uint64
	warmRewrittenPages                     uint64
	collectionRootDescriptorBaseContains   bool
	collectionRootDescriptorTargetContains bool
}

type orderedRootPublishOptions struct {
	maxWarmDeltaOps       int
	leafPrefixCompression bool
	leafColumnar          bool
	packedValuePtr        bool
	internalBaseDelta     bool
	outerLeavesInValueLog bool
	leafPageLog           bulk.LeafPageAppender
}

// OrderedRootStoragePolicy selects the physical storage policy for a published
// ordered root. The zero value keeps the DB-level default.
type OrderedRootStoragePolicy uint8

const (
	OrderedRootStorageDefault OrderedRootStoragePolicy = iota
	// OrderedRootStoragePagerLeaves stores root leaves in index.db pages. It is
	// the fast index policy and can use internal base-delta child encodings.
	OrderedRootStoragePagerLeaves
	// OrderedRootStorageValueLogLeaves stores root leaves as value-log LeafRefs.
	// It is the compressed policy; internal base-delta is disabled because
	// LeafRef child IDs use the same child-id space.
	OrderedRootStorageValueLogLeaves
)

type OrderedRootPublishInput struct {
	BaseRoot      uint64
	Iter          iterator.UnsafeIterator
	StoragePolicy OrderedRootStoragePolicy
}

// OrderedRootDeltaPublishInput describes a sorted root-local mutation stream.
// Unlike OrderedRootPublishInput, Iter contains only keys changed by this
// publish; omitted base-root keys are preserved.
type OrderedRootDeltaPublishInput struct {
	BaseRoot      uint64
	Iter          iterator.UnsafeIterator
	StoragePolicy OrderedRootStoragePolicy
}

func closeUnconsumedOrderedRootPublishIterators(ordered []OrderedRootPublishInput, consumed []bool) {
	for idx := range ordered {
		if idx < len(consumed) && consumed[idx] {
			continue
		}
		if ordered[idx].Iter != nil {
			_ = ordered[idx].Iter.Close()
		}
	}
}

func closeUnconsumedOrderedRootDeltaPublishIterators(ordered []OrderedRootDeltaPublishInput, consumed []bool) {
	for idx := range ordered {
		if idx < len(consumed) && consumed[idx] {
			continue
		}
		if ordered[idx].Iter != nil {
			_ = ordered[idx].Iter.Close()
		}
	}
}

// OrderedRootGroupSystemBuilder builds a target system-root iterator after the
// non-system roots in a group have been built. The rootIDs slice is ordered to
// match the OrderedRootPublishInput slice passed to
// PublishOrderedRootGroupWithSystemBuilder.
type OrderedRootGroupSystemBuilder func(rootIDs []uint64) (iterator.UnsafeIterator, error)

type orderedRootStableUnsafeIterator interface {
	StableUnsafeIteratorSlices() bool
}

type orderedRootLenHintIterator interface {
	Len() int
}

func selectOrderedRootWarmPublishPlan(hasExistingEntries bool, deltaOps int, maxDeltaOps int) orderedRootPublishPlan {
	if !hasExistingEntries {
		return orderedRootPublishPlanColdBuild
	}
	if deltaOps <= maxDeltaOps {
		return orderedRootPublishPlanWarmNativeApply
	}
	return orderedRootPublishPlanWarmFallbackRebuild
}

func (db *DB) orderedRootPublishOptionsForPolicy(policy OrderedRootStoragePolicy) (orderedRootPublishOptions, error) {
	opts := systemRootOrderedPublishOptions(db)
	switch policy {
	case OrderedRootStorageDefault:
		return opts, nil
	case OrderedRootStoragePagerLeaves:
		opts.outerLeavesInValueLog = false
		opts.leafPageLog = nil
		opts.internalBaseDelta = true
		return opts, nil
	case OrderedRootStorageValueLogLeaves:
		opts.outerLeavesInValueLog = true
		opts.internalBaseDelta = false
		opts.leafPageLog = db.leafPageLog
		if opts.leafPageLog == nil {
			return opts, errors.New("ordered root value-log leaf storage requires a leaf page log")
		}
		return opts, nil
	default:
		return opts, errors.New("unknown ordered root storage policy")
	}
}

func (db *DB) orderedRootZipperForOptions(idx *indexGen, opts orderedRootPublishOptions) (*zipper.Zipper, error) {
	if idx == nil || idx.zipper == nil {
		return nil, errors.New("missing index")
	}
	if db != nil && db.orderedRootOptionsUseDefaultZipper(opts) {
		return idx.zipper, nil
	}
	z := idx.zipper.CloneWithAllocator(idx.allocator)
	z.SetOuterLeavesInValueLog(opts.outerLeavesInValueLog)
	z.SetIndexInternalBaseDelta(opts.internalBaseDelta && !opts.outerLeavesInValueLog)
	if opts.outerLeavesInValueLog {
		if opts.leafPageLog == nil {
			return nil, errors.New("ordered root value-log leaf storage requires a leaf page log")
		}
		z.SetLeafPageLog(opts.leafPageLog)
	}
	return z, nil
}

func (db *DB) orderedRootOptionsUseDefaultZipper(opts orderedRootPublishOptions) bool {
	if db == nil {
		return false
	}
	if opts.leafPrefixCompression != db.leafPrefixCompression ||
		opts.leafColumnar != db.indexColumnarLeaves ||
		opts.packedValuePtr != db.indexPackedValuePtr ||
		opts.outerLeavesInValueLog != db.indexOuterLeavesInValueLog ||
		(opts.internalBaseDelta && !opts.outerLeavesInValueLog) != db.indexInternalBaseDelta {
		return false
	}
	if opts.outerLeavesInValueLog && opts.leafPageLog != db.leafPageLog {
		return false
	}
	return true
}

func materializeOrderedRootTable(iter iterator.UnsafeIterator) (memtable.Table, error) {
	entries := 0
	if hint, ok := iter.(orderedRootLenHintIterator); ok {
		entries = hint.Len()
	}
	table := memtable.NewAppendOnlyWithEntryCapacity(entries)
	for iter.Valid() {
		val, ptr, flags := iter.UnsafeEntry()
		table.SetEntry(iter.UnsafeKey(), val, ptr, flags)
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	table.Freeze()
	return table, nil
}

func orderedRootTableContainsCollectionRootDescriptor(table memtable.Table) (bool, error) {
	iter := table.NewIterator(collectionRootDescriptorPrefixBytes, collectionRootDescriptorPrefixEnd())
	defer func() { _ = iter.Close() }()
	return orderedRootIteratorContainsCollectionRootDescriptor(iter)
}

func orderedRootIteratorContainsCollectionRootDescriptor(iter iterator.UnsafeIterator) (bool, error) {
	if iter == nil {
		return false, nil
	}
	if iter.Valid() && bytes.HasPrefix(iter.UnsafeKey(), collectionRootDescriptorPrefixBytes) {
		return true, nil
	}
	return false, iter.Error()
}

func (s orderedRootPublishStats) collectionRootDescriptorReachabilityMayChange() bool {
	return s.collectionRootDescriptorBaseContains || s.collectionRootDescriptorTargetContains
}

func orderedRootEntryEqual(baseIter, targetIter iterator.UnsafeIterator) bool {
	baseVal, basePtr, baseFlags := baseIter.UnsafeEntry()
	targetVal, targetPtr, targetFlags := targetIter.UnsafeEntry()
	if baseFlags != targetFlags {
		return false
	}
	if baseFlags&node.FlagPointer != 0 {
		return basePtr == targetPtr
	}
	return bytes.Equal(baseVal, targetVal)
}

func orderedRootEntryValueLogFileID(iter iterator.UnsafeIterator) (uint32, bool) {
	if iter == nil || !iter.Valid() {
		return 0, false
	}
	_, ptr, flags := iter.UnsafeEntry()
	if flags&node.FlagPointer == 0 || !page.IsValueLogFileID(ptr.FileID) {
		return 0, false
	}
	return ptr.FileID, true
}

func orderedRootBatchPut(delta *batch.Batch, iter iterator.UnsafeIterator, borrowEntryViews bool) error {
	if delta == nil || iter == nil || !iter.Valid() {
		return nil
	}
	val, ptr, flags := iter.UnsafeEntry()
	if flags&node.FlagPointer != 0 && page.IsValueLogFileID(ptr.FileID) {
		if borrowEntryViews {
			return delta.SetPointerView(iter.UnsafeKey(), ptr)
		}
		return delta.SetPointer(iter.UnsafeKey(), ptr)
	}
	if borrowEntryViews {
		return delta.SetView(iter.UnsafeKey(), val)
	}
	return delta.Set(iter.UnsafeKey(), val)
}

func orderedRootDeltaBatchFromIterator(iter iterator.UnsafeIterator) (*batch.Batch, error) {
	delta := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	if hint, ok := iter.(orderedRootLenHintIterator); ok {
		delta.Reserve(hint.Len())
	}
	borrowEntryViews := false
	if stable, ok := iter.(orderedRootStableUnsafeIterator); ok {
		borrowEntryViews = stable.StableUnsafeIteratorSlices()
	}
	for iter.Valid() {
		if iter.IsDeleted() {
			var err error
			if borrowEntryViews {
				err = delta.DeleteView(iter.UnsafeKey())
			} else {
				err = delta.Delete(iter.UnsafeKey())
			}
			if err != nil {
				_ = delta.Close()
				return nil, err
			}
		} else if err := orderedRootBatchPut(delta, iter, borrowEntryViews); err != nil {
			_ = delta.Close()
			return nil, err
		}
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		_ = delta.Close()
		return nil, err
	}
	return delta, nil
}

func collectValueLogRefDeltaFromIterator(iter iterator.UnsafeIterator) (*valueLogRefDelta, error) {
	if iter == nil {
		return nil, nil
	}
	defer iter.Close()
	delta := newValueLogRefDelta()
	for iter.Valid() {
		if fileID, ok := orderedRootEntryValueLogFileID(iter); ok {
			delta.add(fileID, 1)
		}
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return delta, nil
}

func (db *DB) publishOrderedRootDeltaIterator(baseRoot uint64, iter iterator.UnsafeIterator, opts orderedRootPublishOptions) (newRoot uint64, retired []uint64, metrics adaptive.Metrics, err error) {
	if db == nil {
		err = ErrClosed
		return
	}
	if iter == nil {
		err = errors.New("nil ordered root delta iterator")
		return
	}

	if baseRoot == 0 {
		newRoot, retired, metrics, _, _, err = db.publishOrderedRootIterator(0, iter, opts, false)
		return
	}
	defer iter.Close()

	idx := db.idx.Load()
	if idx == nil {
		err = errors.New("missing index")
		return
	}
	if opts.outerLeavesInValueLog && opts.leafPageLog == nil {
		err = errors.New("ordered root value-log leaf storage requires a leaf page log")
		return
	}
	delta, err := orderedRootDeltaBatchFromIterator(iter)
	if err != nil {
		return 0, nil, metrics, err
	}
	defer delta.Close()
	if len(delta.SortedEntries()) == 0 {
		return baseRoot, nil, metrics, nil
	}
	rootZipper, err := db.orderedRootZipperForOptions(idx, opts)
	if err != nil {
		return 0, nil, metrics, err
	}
	newRoot, retired, metrics, err = rootZipper.Apply(baseRoot, delta)
	return
}

func buildOrderedRootDeltaBatch(baseIter, targetIter iterator.UnsafeIterator, trackRefs bool) (*batch.Batch, int, *valueLogRefDelta, error) {
	delta := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	baseValid := baseIter.Valid()
	targetValid := targetIter.Valid()
	deltaOps := 0
	var vlogRefDelta *valueLogRefDelta
	if trackRefs {
		vlogRefDelta = newValueLogRefDelta()
	}
	for baseValid || targetValid {
		switch {
		case !targetValid:
			if vlogRefDelta != nil {
				if fileID, ok := orderedRootEntryValueLogFileID(baseIter); ok {
					vlogRefDelta.add(fileID, -1)
				}
			}
			if err := delta.Delete(baseIter.UnsafeKey()); err != nil {
				_ = delta.Close()
				return nil, 0, nil, err
			}
			deltaOps++
			baseIter.Next()
			baseValid = baseIter.Valid()
		case !baseValid:
			if vlogRefDelta != nil {
				if fileID, ok := orderedRootEntryValueLogFileID(targetIter); ok {
					vlogRefDelta.add(fileID, 1)
				}
			}
			if err := orderedRootBatchPut(delta, targetIter, false); err != nil {
				_ = delta.Close()
				return nil, 0, nil, err
			}
			deltaOps++
			targetIter.Next()
			targetValid = targetIter.Valid()
		default:
			switch cmp := bytes.Compare(baseIter.UnsafeKey(), targetIter.UnsafeKey()); {
			case cmp < 0:
				if vlogRefDelta != nil {
					if fileID, ok := orderedRootEntryValueLogFileID(baseIter); ok {
						vlogRefDelta.add(fileID, -1)
					}
				}
				if err := delta.Delete(baseIter.UnsafeKey()); err != nil {
					_ = delta.Close()
					return nil, 0, nil, err
				}
				deltaOps++
				baseIter.Next()
				baseValid = baseIter.Valid()
			case cmp > 0:
				if vlogRefDelta != nil {
					if fileID, ok := orderedRootEntryValueLogFileID(targetIter); ok {
						vlogRefDelta.add(fileID, 1)
					}
				}
				if err := orderedRootBatchPut(delta, targetIter, false); err != nil {
					_ = delta.Close()
					return nil, 0, nil, err
				}
				deltaOps++
				targetIter.Next()
				targetValid = targetIter.Valid()
			default:
				if !orderedRootEntryEqual(baseIter, targetIter) {
					if vlogRefDelta != nil {
						if fileID, ok := orderedRootEntryValueLogFileID(baseIter); ok {
							vlogRefDelta.add(fileID, -1)
						}
						if fileID, ok := orderedRootEntryValueLogFileID(targetIter); ok {
							vlogRefDelta.add(fileID, 1)
						}
					}
					if err := orderedRootBatchPut(delta, targetIter, false); err != nil {
						_ = delta.Close()
						return nil, 0, nil, err
					}
					deltaOps++
				}
				baseIter.Next()
				targetIter.Next()
				baseValid = baseIter.Valid()
				targetValid = targetIter.Valid()
			}
		}
	}
	if err := baseIter.Error(); err != nil {
		_ = delta.Close()
		return nil, 0, nil, err
	}
	if err := targetIter.Error(); err != nil {
		_ = delta.Close()
		return nil, 0, nil, err
	}
	return delta, deltaOps, vlogRefDelta, nil
}

func (db *DB) publishOrderedRootIterator(baseRoot uint64, iter iterator.UnsafeIterator, opts orderedRootPublishOptions, trackValueLogRefs bool) (newRoot uint64, retired []uint64, metrics adaptive.Metrics, stats orderedRootPublishStats, vlogRefDelta *valueLogRefDelta, err error) {
	if db == nil {
		err = ErrClosed
		return
	}
	if iter == nil {
		err = errors.New("nil ordered root iterator")
		return
	}
	if db.testOrderedRootPublishHook != nil {
		db.testOrderedRootPublishHook(baseRoot)
	}
	defer iter.Close()

	idx := db.idx.Load()
	if idx == nil {
		err = errors.New("missing index")
		return
	}
	state := db.state.Load()
	if state == nil {
		err = errors.New("missing backend state")
		return
	}

	newRoot = baseRoot
	var buildIter iterator.UnsafeIterator
	if opts.outerLeavesInValueLog && opts.leafPageLog == nil {
		err = errors.New("ordered root value-log leaf storage requires a leaf page log")
		return
	}
	trackValueLogRefs = trackValueLogRefs && db.valueLogRefTracker != nil && db.valueLogRefTracker.canTrack(db.currentCommitSeq()) && !opts.outerLeavesInValueLog
	if baseRoot != 0 {
		rootTree := tree.New(idx.pager, newValueReader(state.ValueLogSet), baseRoot)
		collectBasePageIDs := func() ([]uint64, error) {
			return rootTree.CollectPageIDs()
		}
		if trackValueLogRefs {
			baseDescriptorIter := rootTree.Iterator(collectionRootDescriptorPrefixBytes, collectionRootDescriptorPrefixEnd())
			stats.collectionRootDescriptorBaseContains, err = orderedRootIteratorContainsCollectionRootDescriptor(baseDescriptorIter)
			_ = baseDescriptorIter.Close()
			if err != nil {
				return
			}
		}

		baseProbe := rootTree.Iterator(nil, nil)
		hasExistingEntries := baseProbe.Valid()
		iterErr := baseProbe.Error()
		baseProbe.Close()
		if iterErr != nil {
			err = iterErr
			return
		}

		targetTable, materializeErr := materializeOrderedRootTable(iter)
		if materializeErr != nil {
			err = materializeErr
			return
		}
		if trackValueLogRefs {
			stats.collectionRootDescriptorTargetContains, err = orderedRootTableContainsCollectionRootDescriptor(targetTable)
			if err != nil {
				return
			}
		}
		if !hasExistingEntries {
			if trackValueLogRefs {
				vlogRefDelta, err = collectValueLogRefDeltaFromIterator(targetTable.NewIterator(nil, nil))
				if err != nil {
					return
				}
			}
			pageIDs, collectErr := collectBasePageIDs()
			if collectErr != nil {
				err = collectErr
				return
			}
			retired = append(retired, pageIDs...)
			buildIter = targetTable.NewIterator(nil, nil)
		} else {
			baseIter := rootTree.Iterator(nil, nil)
			targetIter := targetTable.NewIterator(nil, nil)
			delta, deltaOps, refDelta, deltaErr := buildOrderedRootDeltaBatch(baseIter, targetIter, trackValueLogRefs)
			baseIter.Close()
			targetIter.Close()
			if deltaErr != nil {
				err = deltaErr
				return
			}
			defer delta.Close()
			vlogRefDelta = refDelta
			switch selectOrderedRootWarmPublishPlan(hasExistingEntries, deltaOps, opts.maxWarmDeltaOps) {
			case orderedRootPublishPlanWarmNativeApply:
				stats.warmAttempts++
				stats.warmNativeApplyAttempts++
				rootZipper, zipperErr := db.orderedRootZipperForOptions(idx, opts)
				if zipperErr != nil {
					err = zipperErr
					return
				}
				newRoot, retired, metrics, err = rootZipper.Apply(baseRoot, delta)
				if err != nil {
					return
				}
				// Avoid a full old-tree page scan on the warm apply path. The
				// retired page list is exact; preserved pages are tracked as a
				// lower bound so the public counter still proves warm apply
				// avoided a full rebuild without making every write walk the root.
				if newRoot != 0 {
					stats.warmPreservedPages = 1
				}
				stats.warmRewrittenPages = uint64(len(retired))
				return
			case orderedRootPublishPlanWarmFallbackRebuild:
				stats.warmAttempts++
				stats.warmRebuildFallbacks++
				if vlogRefDelta != nil {
					vlogRefDelta = nil
				}
				pageIDs, collectErr := collectBasePageIDs()
				if collectErr != nil {
					err = collectErr
					return
				}
				retired = append(retired, pageIDs...)
				buildIter = targetTable.NewIterator(nil, nil)
			case orderedRootPublishPlanColdBuild:
				err = errors.New("ordered root warm publish selected cold build for a non-empty base root")
				return
			}
		}
	} else {
		if trackValueLogRefs {
			targetTable, materializeErr := materializeOrderedRootTable(iter)
			if materializeErr != nil {
				err = materializeErr
				return
			}
			stats.collectionRootDescriptorTargetContains, err = orderedRootTableContainsCollectionRootDescriptor(targetTable)
			if err != nil {
				return
			}
			vlogRefDelta, err = collectValueLogRefDeltaFromIterator(targetTable.NewIterator(nil, nil))
			if err != nil {
				return
			}
			buildIter = targetTable.NewIterator(nil, nil)
		} else {
			buildIter = iter
		}
	}

	if buildIter != nil && buildIter != iter {
		defer buildIter.Close()
	}
	if buildIter != nil {
		var leafPageLog bulk.LeafPageAppender
		if opts.outerLeavesInValueLog {
			leafPageLog = opts.leafPageLog
		}
		newRoot, err = bulk.BuildWithOptions(buildIter, &pagerAllocator{p: idx.pager}, idx.pager, bulk.BuildOptions{
			LeafPrefixCompression: opts.leafPrefixCompression,
			LeafColumnar:          opts.leafColumnar,
			PackedValuePtr:        opts.packedValuePtr,
			InternalBaseDelta:     opts.internalBaseDelta && !opts.outerLeavesInValueLog,
			LeafPageLog:           leafPageLog,
		})
	}
	return
}

func mergeOrderedRootPublishMetrics(dst *adaptive.Metrics, src adaptive.Metrics) {
	if dst == nil {
		return
	}
	if src.LeafFill > 0 {
		if dst.LeafFill == 0 {
			dst.LeafFill = src.LeafFill
		} else {
			dst.LeafFill = (dst.LeafFill + src.LeafFill) / 2
		}
	}
	dst.Splits += src.Splits
	dst.IndexWriteBytes += src.IndexWriteBytes
	dst.SlabWriteBytes += src.SlabWriteBytes
	dst.SlabDeadBytes += src.SlabDeadBytes
	if len(src.SlabWriteBytesByFile) != 0 {
		if dst.SlabWriteBytesByFile == nil {
			dst.SlabWriteBytesByFile = make(map[uint32]int64, len(src.SlabWriteBytesByFile))
		}
		for fileID, bytes := range src.SlabWriteBytesByFile {
			dst.SlabWriteBytesByFile[fileID] += bytes
		}
	}
	if len(src.SlabDeadBytesByFile) != 0 {
		if dst.SlabDeadBytesByFile == nil {
			dst.SlabDeadBytesByFile = make(map[uint32]int64, len(src.SlabDeadBytesByFile))
		}
		for fileID, bytes := range src.SlabDeadBytesByFile {
			dst.SlabDeadBytesByFile[fileID] += bytes
		}
	}
}

// PublishOrderedRootIterator builds and commits a non-meta root from an ordered
// iterator while preserving the current user and system roots in the commit.
func (db *DB) PublishOrderedRootIterator(baseRoot uint64, iter iterator.UnsafeIterator) (uint64, error) {
	if db == nil {
		return 0, ErrClosed
	}
	if db.closing.Load() {
		return 0, ErrClosed
	}
	if iter == nil {
		return 0, errors.New("nil ordered root iterator")
	}

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	if db.readOnly {
		return 0, ErrReadOnly
	}

	db.mu.RLock()
	userRoot := db.meta.UserRootPageID
	systemRoot := db.meta.SystemRootPageID
	baseSeq := db.meta.CommitSeq
	db.mu.RUnlock()

	newRoot, retired, metrics, _, _, err := db.publishOrderedRootIterator(baseRoot, iter, systemRootOrderedPublishOptions(db), false)
	if err != nil {
		return 0, err
	}

	db.mu.RLock()
	curUserRoot := db.meta.UserRootPageID
	curSystemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if curUserRoot != userRoot || curSystemRoot != systemRoot {
		return 0, errors.New("concurrent modification detected during ordered root publish")
	}

	vlogRefDelta := db.newNoopValueLogRefDeltaIfTrackable(baseSeq)
	defer func() {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
	}()

	if err := db.finalizeCommit(userRoot, systemRoot, retired, false, metrics, nil, true, vlogRefDelta, nil, nil); err != nil {
		return 0, err
	}
	vlogRefDelta = nil
	return newRoot, nil
}

// PublishOrderedRootGroup builds and commits a mixed system/non-system root
// group in one backend commit. Non-system roots are built from ordered
// iterators and become durable when the grouped commit finalizes.
func (db *DB) PublishOrderedRootGroup(systemIter iterator.UnsafeIterator, ordered []OrderedRootPublishInput) (uint64, []uint64, error) {
	return db.publishOrderedRootGroup(systemIter, ordered, nil)
}

// PublishOrderedRootGroupWithSystemBuilder builds non-system roots first, then
// calls buildSystemIter with the produced root IDs and commits the system root
// plus all non-system roots in one backend commit. This is intended for callers
// whose system descriptors must store the new root IDs produced by the group.
func (db *DB) PublishOrderedRootGroupWithSystemBuilder(ordered []OrderedRootPublishInput, buildSystemIter OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	if buildSystemIter == nil {
		return 0, nil, errors.New("nil ordered root group system builder")
	}
	return db.publishOrderedRootGroup(nil, ordered, buildSystemIter)
}

// PublishOrderedRootDeltaGroupWithSystemBuilder applies root-local mutation
// streams to non-system roots, then builds and commits a system-root iterator
// that can persist the produced root IDs in the same backend commit.
func (db *DB) PublishOrderedRootDeltaGroupWithSystemBuilder(ordered []OrderedRootDeltaPublishInput, buildSystemIter OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	if buildSystemIter == nil {
		return 0, nil, errors.New("nil ordered root group system builder")
	}
	if db == nil {
		return 0, nil, ErrClosed
	}
	if db.closing.Load() {
		return 0, nil, ErrClosed
	}

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	if db.readOnly {
		return 0, nil, ErrReadOnly
	}

	db.mu.RLock()
	userRoot := db.meta.UserRootPageID
	baseSystemRoot := db.meta.SystemRootPageID
	baseSeq := db.meta.CommitSeq
	db.mu.RUnlock()

	systemOpts := systemRootOrderedPublishOptions(db)
	rootIDs := make([]uint64, len(ordered))
	orderedConsumed := make([]bool, len(ordered))
	defer closeUnconsumedOrderedRootDeltaPublishIterators(ordered, orderedConsumed)
	var retired []uint64
	var merged adaptive.Metrics
	for idx := range ordered {
		opts, err := db.orderedRootPublishOptionsForPolicy(ordered[idx].StoragePolicy)
		if err != nil {
			return 0, nil, err
		}
		orderedConsumed[idx] = true
		rootID, rootRetired, metrics, err := db.publishOrderedRootDeltaIterator(ordered[idx].BaseRoot, ordered[idx].Iter, opts)
		if err != nil {
			return 0, nil, err
		}
		rootIDs[idx] = rootID
		retired = append(retired, rootRetired...)
		mergeOrderedRootPublishMetrics(&merged, metrics)
	}

	iter, err := buildSystemIter(append([]uint64(nil), rootIDs...))
	if err != nil {
		return 0, nil, err
	}
	if iter == nil {
		return 0, nil, errors.New("nil system root iterator")
	}
	rootID, rootRetired, metrics, _, refDelta, err := db.publishOrderedRootIterator(baseSystemRoot, iter, systemOpts, true)
	if err != nil {
		return 0, nil, err
	}
	newSystemRoot := rootID
	retired = append(retired, rootRetired...)
	mergeOrderedRootPublishMetrics(&merged, metrics)
	vlogRefDelta := refDelta
	if len(ordered) > 0 {
		// Non-system roots were applied from deltas, so this commit has no
		// exact value-log ref delta for their pointer changes. Keep GC
		// reachability conservative by invalidating the tracker after commit.
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
		vlogRefDelta = nil
	}
	defer func() {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
	}()

	db.mu.RLock()
	curUserRoot := db.meta.UserRootPageID
	curSystemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if curUserRoot != userRoot || curSystemRoot != baseSystemRoot {
		return 0, nil, errors.New("concurrent modification detected during ordered root group publish")
	}

	if len(ordered) == 0 && vlogRefDelta == nil {
		vlogRefDelta = db.newNoopValueLogRefDeltaIfTrackable(baseSeq)
	}
	if err := db.finalizeCommit(userRoot, newSystemRoot, retired, false, merged, nil, true, vlogRefDelta, nil, nil); err != nil {
		return 0, nil, err
	}
	vlogRefDelta = nil
	return newSystemRoot, rootIDs, nil
}

// PublishOrderedRootDeltaGroupWithSystemDeltaBuilder applies root-local
// mutation streams to non-system roots, then applies a root-local mutation
// stream to the system root. The system delta should contain only changed
// system-root entries; omitted system entries are preserved.
func (db *DB) PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered []OrderedRootDeltaPublishInput, buildSystemDeltaIter OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	if buildSystemDeltaIter == nil {
		return 0, nil, errors.New("nil ordered root group system delta builder")
	}
	if db == nil {
		return 0, nil, ErrClosed
	}
	if db.closing.Load() {
		return 0, nil, ErrClosed
	}

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	if db.readOnly {
		return 0, nil, ErrReadOnly
	}

	db.mu.RLock()
	userRoot := db.meta.UserRootPageID
	baseSystemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()

	systemOpts := systemRootOrderedPublishOptions(db)
	rootIDs := make([]uint64, len(ordered))
	orderedConsumed := make([]bool, len(ordered))
	defer closeUnconsumedOrderedRootDeltaPublishIterators(ordered, orderedConsumed)
	var retired []uint64
	var merged adaptive.Metrics
	for idx := range ordered {
		opts, err := db.orderedRootPublishOptionsForPolicy(ordered[idx].StoragePolicy)
		if err != nil {
			return 0, nil, err
		}
		orderedConsumed[idx] = true
		rootID, rootRetired, metrics, err := db.publishOrderedRootDeltaIterator(ordered[idx].BaseRoot, ordered[idx].Iter, opts)
		if err != nil {
			return 0, nil, err
		}
		rootIDs[idx] = rootID
		retired = append(retired, rootRetired...)
		mergeOrderedRootPublishMetrics(&merged, metrics)
	}

	iter, err := buildSystemDeltaIter(append([]uint64(nil), rootIDs...))
	if err != nil {
		return 0, nil, err
	}
	if iter == nil {
		return 0, nil, errors.New("nil system root delta iterator")
	}
	rootID, rootRetired, metrics, err := db.publishOrderedRootDeltaIterator(baseSystemRoot, iter, systemOpts)
	if err != nil {
		return 0, nil, err
	}
	newSystemRoot := rootID
	retired = append(retired, rootRetired...)
	mergeOrderedRootPublishMetrics(&merged, metrics)

	db.mu.RLock()
	curUserRoot := db.meta.UserRootPageID
	curSystemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if curUserRoot != userRoot || curSystemRoot != baseSystemRoot {
		return 0, nil, errors.New("concurrent modification detected during ordered root group publish")
	}

	// The system root was applied as a delta, so we do not have an exact
	// value-log ref delta for system-root pointer changes. Passing nil keeps the
	// tracker conservative by invalidating it after commit.
	var vlogRefDelta *valueLogRefDelta
	if err := db.finalizeCommit(userRoot, newSystemRoot, retired, false, merged, nil, true, vlogRefDelta, nil, nil); err != nil {
		return 0, nil, err
	}
	return newSystemRoot, rootIDs, nil
}

func (db *DB) publishOrderedRootGroup(systemIter iterator.UnsafeIterator, ordered []OrderedRootPublishInput, buildSystemIter OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	if systemIter != nil && buildSystemIter != nil {
		return 0, nil, errors.New("ordered root group cannot use both system iterator and system builder")
	}
	if db == nil {
		return 0, nil, ErrClosed
	}
	if db.closing.Load() {
		return 0, nil, ErrClosed
	}

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	if db.readOnly {
		return 0, nil, ErrReadOnly
	}

	db.mu.RLock()
	userRoot := db.meta.UserRootPageID
	baseSystemRoot := db.meta.SystemRootPageID
	baseSeq := db.meta.CommitSeq
	db.mu.RUnlock()

	systemOpts := systemRootOrderedPublishOptions(db)
	newSystemRoot := baseSystemRoot
	var retired []uint64
	var merged adaptive.Metrics
	var systemStats orderedRootPublishStats
	var vlogRefDelta *valueLogRefDelta
	defer func() {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
	}()

	if systemIter != nil {
		rootID, rootRetired, metrics, publishStats, refDelta, err := db.publishOrderedRootIterator(baseSystemRoot, systemIter, systemOpts, true)
		if err != nil {
			return 0, nil, err
		}
		newSystemRoot = rootID
		retired = append(retired, rootRetired...)
		mergeOrderedRootPublishMetrics(&merged, metrics)
		systemStats = publishStats
		vlogRefDelta = refDelta
	}

	rootIDs := make([]uint64, len(ordered))
	orderedConsumed := make([]bool, len(ordered))
	defer closeUnconsumedOrderedRootPublishIterators(ordered, orderedConsumed)
	for idx := range ordered {
		opts, err := db.orderedRootPublishOptionsForPolicy(ordered[idx].StoragePolicy)
		if err != nil {
			return 0, nil, err
		}
		orderedConsumed[idx] = true
		rootID, rootRetired, metrics, _, _, err := db.publishOrderedRootIterator(ordered[idx].BaseRoot, ordered[idx].Iter, opts, false)
		if err != nil {
			return 0, nil, err
		}
		rootIDs[idx] = rootID
		retired = append(retired, rootRetired...)
		mergeOrderedRootPublishMetrics(&merged, metrics)
	}

	if buildSystemIter != nil {
		builtRootIDs := append([]uint64(nil), rootIDs...)
		iter, err := buildSystemIter(builtRootIDs)
		if err != nil {
			return 0, nil, err
		}
		if iter == nil {
			return 0, nil, errors.New("nil system root iterator")
		}
		rootID, rootRetired, metrics, publishStats, refDelta, err := db.publishOrderedRootIterator(baseSystemRoot, iter, systemOpts, true)
		if err != nil {
			return 0, nil, err
		}
		newSystemRoot = rootID
		retired = append(retired, rootRetired...)
		mergeOrderedRootPublishMetrics(&merged, metrics)
		systemStats = publishStats
		vlogRefDelta = refDelta
	}

	db.mu.RLock()
	curUserRoot := db.meta.UserRootPageID
	curSystemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if curUserRoot != userRoot || curSystemRoot != baseSystemRoot {
		return 0, nil, errors.New("concurrent modification detected during ordered root group publish")
	}

	forceRefTrackerRebuild := systemStats.collectionRootDescriptorReachabilityMayChange()
	if forceRefTrackerRebuild {
		// Collection descriptors make non-system roots part of value-log
		// reachability. The system-root ref delta alone is not an exact commit
		// delta for those roots, so force the tracker to rebuild from the full
		// maintenance root set.
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
		vlogRefDelta = nil
	}
	if vlogRefDelta == nil && !forceRefTrackerRebuild {
		vlogRefDelta = db.newNoopValueLogRefDeltaIfTrackable(baseSeq)
	}
	if err := db.finalizeCommit(userRoot, newSystemRoot, retired, false, merged, nil, true, vlogRefDelta, nil, nil); err != nil {
		return 0, nil, err
	}
	vlogRefDelta = nil
	if systemIter != nil {
		db.systemRootWarmPublishAttempts.Add(systemStats.warmAttempts)
		db.systemRootWarmNativeApplyAttempts.Add(systemStats.warmNativeApplyAttempts)
		db.systemRootWarmPublishRebuildFallbacks.Add(systemStats.warmRebuildFallbacks)
		db.systemRootWarmPreservedPages.Add(systemStats.warmPreservedPages)
		db.systemRootWarmRewrittenPages.Add(systemStats.warmRewrittenPages)
	}
	return newSystemRoot, rootIDs, nil
}
