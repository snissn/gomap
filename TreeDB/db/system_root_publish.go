package db

import (
	"bytes"
	"errors"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type systemRootPublishStats struct {
	warmAttempts            uint64
	warmNativeApplyAttempts uint64
	warmRebuildFallbacks    uint64
	warmPreservedPages      uint64
	warmRewrittenPages      uint64
}

type systemRootPublishPlan uint8

const (
	systemRootPublishPlanColdBuild systemRootPublishPlan = iota
	systemRootPublishPlanWarmFallbackRebuild
	systemRootPublishPlanWarmNativeApply
)

func (db *DB) systemRootPublishStatsSnapshot() systemRootPublishStats {
	if db == nil {
		return systemRootPublishStats{}
	}
	return systemRootPublishStats{
		warmAttempts:            db.systemRootWarmPublishAttempts.Load(),
		warmNativeApplyAttempts: db.systemRootWarmNativeApplyAttempts.Load(),
		warmRebuildFallbacks:    db.systemRootWarmPublishRebuildFallbacks.Load(),
		warmPreservedPages:      db.systemRootWarmPreservedPages.Load(),
		warmRewrittenPages:      db.systemRootWarmRewrittenPages.Load(),
	}
}

func selectSystemRootPublishPlan(hasExistingEntries bool) systemRootPublishPlan {
	if !hasExistingEntries {
		return systemRootPublishPlanColdBuild
	}
	// R4 scaffolding: until warm native apply exists, non-empty steady-state
	// system-root publishes explicitly select rebuild fallback.
	return systemRootPublishPlanWarmFallbackRebuild
}

const defaultSystemRootWarmMaxDeltaOps = 256

func (db *DB) systemRootWarmMaxDeltaOps() int {
	if db != nil && db.testSystemRootWarmMaxDeltaOps > 0 {
		return db.testSystemRootWarmMaxDeltaOps
	}
	return defaultSystemRootWarmMaxDeltaOps
}

func selectSystemRootWarmPublishPlan(hasExistingEntries bool, deltaOps int, maxDeltaOps int) systemRootPublishPlan {
	if !hasExistingEntries {
		return systemRootPublishPlanColdBuild
	}
	if deltaOps <= maxDeltaOps {
		return systemRootPublishPlanWarmNativeApply
	}
	return systemRootPublishPlanWarmFallbackRebuild
}

func materializeSystemRootTable(iter iterator.UnsafeIterator) (memtable.Table, error) {
	table, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		return nil, err
	}
	for iter.Valid() {
		table.Set(iter.UnsafeKey(), iter.UnsafeValue())
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	table.Freeze()
	return table, nil
}

func buildSystemRootDeltaBatch(baseIter, targetIter iterator.UnsafeIterator) (*batch.Batch, int, error) {
	delta := batch.New(nil, page.DefaultInlineThreshold)
	baseValid := baseIter.Valid()
	targetValid := targetIter.Valid()
	deltaOps := 0
	for baseValid || targetValid {
		switch {
		case !targetValid:
			if err := delta.Delete(baseIter.UnsafeKey()); err != nil {
				_ = delta.Close()
				return nil, 0, err
			}
			deltaOps++
			baseIter.Next()
			baseValid = baseIter.Valid()
		case !baseValid:
			if err := delta.Set(targetIter.UnsafeKey(), targetIter.UnsafeValue()); err != nil {
				_ = delta.Close()
				return nil, 0, err
			}
			deltaOps++
			targetIter.Next()
			targetValid = targetIter.Valid()
		default:
			switch cmp := bytes.Compare(baseIter.UnsafeKey(), targetIter.UnsafeKey()); {
			case cmp < 0:
				if err := delta.Delete(baseIter.UnsafeKey()); err != nil {
					_ = delta.Close()
					return nil, 0, err
				}
				deltaOps++
				baseIter.Next()
				baseValid = baseIter.Valid()
			case cmp > 0:
				if err := delta.Set(targetIter.UnsafeKey(), targetIter.UnsafeValue()); err != nil {
					_ = delta.Close()
					return nil, 0, err
				}
				deltaOps++
				targetIter.Next()
				targetValid = targetIter.Valid()
			default:
				if !bytes.Equal(baseIter.UnsafeValue(), targetIter.UnsafeValue()) {
					if err := delta.Set(targetIter.UnsafeKey(), targetIter.UnsafeValue()); err != nil {
						_ = delta.Close()
						return nil, 0, err
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
		return nil, 0, err
	}
	if err := targetIter.Error(); err != nil {
		_ = delta.Close()
		return nil, 0, err
	}
	return delta, deltaOps, nil
}

// PublishSystemRootIterator builds and commits a new system root from an
// ordered iterator without detached-batch replay. The current user root is
// preserved across the commit.
func (db *DB) PublishSystemRootIterator(iter iterator.UnsafeIterator) (uint64, error) {
	if db == nil {
		return 0, ErrClosed
	}
	if iter == nil {
		return 0, errors.New("nil system root iterator")
	}
	defer iter.Close()

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	if db.readOnly {
		return 0, ErrReadOnly
	}
	idx := db.idx.Load()
	if idx == nil {
		return 0, errors.New("missing index")
	}

	db.mu.RLock()
	state := db.state.Load()
	userRoot := db.meta.UserRootPageID
	baseSystemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()

	retired := make([]uint64, 0)
	metrics := adaptive.Metrics{}
	newSystemRoot := baseSystemRoot
	var buildIter iterator.UnsafeIterator
	if baseSystemRoot != 0 {
		sysTree := tree.New(idx.pager, newValueReader(state.ValueLogSet), baseSystemRoot)
		pageIDs, err := sysTree.CollectPageIDs()
		if err != nil {
			return 0, err
		}

		sysIter := sysTree.Iterator(nil, nil)
		warm := sysIter.Valid()
		iterErr := sysIter.Error()
		sysIter.Close()
		if iterErr != nil {
			return 0, iterErr
		}

		targetTable, err := materializeSystemRootTable(iter)
		if err != nil {
			return 0, err
		}
		if !warm {
			retired = append(retired, pageIDs...)
			buildIter = targetTable.NewIterator(nil, nil)
		} else {
			baseIter := sysTree.Iterator(nil, nil)
			targetIter := targetTable.NewIterator(nil, nil)
			delta, deltaOps, err := buildSystemRootDeltaBatch(baseIter, targetIter)
			baseIter.Close()
			targetIter.Close()
			if err != nil {
				return 0, err
			}
			defer delta.Close()
			switch selectSystemRootWarmPublishPlan(warm, deltaOps, db.systemRootWarmMaxDeltaOps()) {
			case systemRootPublishPlanWarmNativeApply:
				db.systemRootWarmPublishAttempts.Add(1)
				db.systemRootWarmNativeApplyAttempts.Add(1)
				retiredPages, applyMetrics := []uint64(nil), adaptive.Metrics{}
				newSystemRoot, retiredPages, applyMetrics, err = idx.zipper.Apply(baseSystemRoot, delta)
				if err != nil {
					return 0, err
				}
				retired = retiredPages
				metrics = applyMetrics
				if len(pageIDs) >= len(retiredPages) {
					db.systemRootWarmPreservedPages.Add(uint64(len(pageIDs) - len(retiredPages)))
				}
				db.systemRootWarmRewrittenPages.Add(uint64(len(retiredPages)))
			case systemRootPublishPlanWarmFallbackRebuild:
				db.systemRootWarmPublishAttempts.Add(1)
				db.systemRootWarmPublishRebuildFallbacks.Add(1)
				retired = append(retired, pageIDs...)
				buildIter = targetTable.NewIterator(nil, nil)
			default:
				retired = append(retired, pageIDs...)
				buildIter = targetTable.NewIterator(nil, nil)
			}
		}
	} else {
		buildIter = iter
	}

	if buildIter != nil && buildIter != iter {
		defer buildIter.Close()
	}
	if buildIter != nil {
		var err error
		newSystemRoot, err = bulk.BuildWithOptions(buildIter, &pagerAllocator{p: idx.pager}, idx.pager, bulk.BuildOptions{
			LeafPrefixCompression: db.leafPrefixCompression,
			LeafColumnar:          db.indexColumnarLeaves,
			PackedValuePtr:        db.indexPackedValuePtr,
			InternalBaseDelta:     db.indexInternalBaseDelta,
		})
		if err != nil {
			return 0, err
		}
	}

	db.mu.Lock()
	curUserRoot := db.meta.UserRootPageID
	curSystemRoot := db.meta.SystemRootPageID
	db.mu.Unlock()
	if curUserRoot != userRoot || curSystemRoot != baseSystemRoot {
		return 0, errors.New("concurrent modification detected during system root publish")
	}

	if err := db.finalizeCommit(userRoot, newSystemRoot, retired, false, metrics, nil, true, nil); err != nil {
		return 0, err
	}
	return newSystemRoot, nil
}
