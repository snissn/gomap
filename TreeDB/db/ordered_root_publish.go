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
)

type orderedRootPublishPlan uint8

const (
	orderedRootPublishPlanColdBuild orderedRootPublishPlan = iota
	orderedRootPublishPlanWarmFallbackRebuild
	orderedRootPublishPlanWarmNativeApply
)

type orderedRootPublishStats struct {
	warmAttempts            uint64
	warmNativeApplyAttempts uint64
	warmRebuildFallbacks    uint64
	warmPreservedPages      uint64
	warmRewrittenPages      uint64
}

type orderedRootPublishOptions struct {
	maxWarmDeltaOps       int
	leafPrefixCompression bool
	leafColumnar          bool
	packedValuePtr        bool
	internalBaseDelta     bool
}

type OrderedRootPublishInput struct {
	BaseRoot uint64
	Iter     iterator.UnsafeIterator
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

func materializeOrderedRootTable(iter iterator.UnsafeIterator) (memtable.Table, error) {
	table, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		return nil, err
	}
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

func orderedRootBatchPut(delta *batch.Batch, iter iterator.UnsafeIterator) error {
	if delta == nil || iter == nil || !iter.Valid() {
		return nil
	}
	val, ptr, flags := iter.UnsafeEntry()
	if flags&node.FlagPointer != 0 && page.IsValueLogFileID(ptr.FileID) {
		return delta.SetPointer(iter.UnsafeKey(), ptr)
	}
	return delta.Set(iter.UnsafeKey(), val)
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

func buildOrderedRootDeltaBatch(baseIter, targetIter iterator.UnsafeIterator, trackRefs bool) (*batch.Batch, int, *valueLogRefDelta, error) {
	delta := batch.New(nil, page.DefaultInlineThreshold)
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
			if err := orderedRootBatchPut(delta, targetIter); err != nil {
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
				if err := orderedRootBatchPut(delta, targetIter); err != nil {
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
					if err := orderedRootBatchPut(delta, targetIter); err != nil {
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
	trackValueLogRefs = trackValueLogRefs && db.valueLogRefTracker != nil && db.valueLogRefTracker.canTrack(db.currentCommitSeq()) && !db.indexOuterLeavesInValueLog
	if baseRoot != 0 {
		rootTree := tree.New(idx.pager, newValueReader(state.ValueLogSet), baseRoot)
		pageIDs, collectErr := rootTree.CollectPageIDs()
		if collectErr != nil {
			err = collectErr
			return
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
		if !hasExistingEntries {
			if trackValueLogRefs {
				vlogRefDelta, err = collectValueLogRefDeltaFromIterator(targetTable.NewIterator(nil, nil))
				if err != nil {
					return
				}
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
				newRoot, retired, metrics, err = idx.zipper.Apply(baseRoot, delta)
				if err != nil {
					return
				}
				if len(pageIDs) >= len(retired) {
					stats.warmPreservedPages = uint64(len(pageIDs) - len(retired))
				}
				stats.warmRewrittenPages = uint64(len(retired))
				return
			case orderedRootPublishPlanWarmFallbackRebuild:
				stats.warmAttempts++
				stats.warmRebuildFallbacks++
				if vlogRefDelta != nil {
					vlogRefDelta = nil
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
		newRoot, err = bulk.BuildWithOptions(buildIter, &pagerAllocator{p: idx.pager}, idx.pager, bulk.BuildOptions{
			LeafPrefixCompression: opts.leafPrefixCompression,
			LeafColumnar:          opts.leafColumnar,
			PackedValuePtr:        opts.packedValuePtr,
			InternalBaseDelta:     opts.internalBaseDelta,
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

	opts := systemRootOrderedPublishOptions(db)
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
		rootID, rootRetired, metrics, publishStats, refDelta, err := db.publishOrderedRootIterator(baseSystemRoot, systemIter, opts, true)
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
	for idx := range ordered {
		rootID, rootRetired, metrics, _, _, err := db.publishOrderedRootIterator(ordered[idx].BaseRoot, ordered[idx].Iter, opts, false)
		if err != nil {
			return 0, nil, err
		}
		rootIDs[idx] = rootID
		retired = append(retired, rootRetired...)
		mergeOrderedRootPublishMetrics(&merged, metrics)
	}

	db.mu.RLock()
	curUserRoot := db.meta.UserRootPageID
	curSystemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if curUserRoot != userRoot || curSystemRoot != baseSystemRoot {
		return 0, nil, errors.New("concurrent modification detected during ordered root group publish")
	}

	if vlogRefDelta == nil {
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
