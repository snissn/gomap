package treedb

import (
	"fmt"

	"treedb/internal/adaptive"
	"treedb/internal/mvcc"
	"treedb/internal/page"
	"treedb/internal/slab"
	"treedb/internal/tree"
)

func (db *DB) writeBatch(b *Batch, sync bool) error {
	if db == nil || b == nil {
		return fmt.Errorf("treedb: nil batch/db")
	}
	if db.closed.Load() {
		return fmt.Errorf("treedb: db closed")
	}
	db.writerMu.Lock()
	defer db.writerMu.Unlock()

	st := db.state.Load()
	if st == nil {
		return fmt.Errorf("treedb: no state")
	}

	// Phase 1: prewrite large values.
	threshold := db.opts.InlineThreshold
	if db.adaptive != nil && db.adaptive.Enabled() {
		threshold = db.adaptive.Latch()
	}
	var observe func(int)
	if db.hooks != nil && db.hooks.thresholdObserved != nil {
		observe = db.hooks.thresholdObserved
	}
	keys, modSlabs, slabWriteBytes, err := b.prepare(threshold, db.slabs, observe)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}

	// Best-effort: if the freelist is empty, set a bounded grow quantum so the
	// commit can amortize pager growth work.
	reserveActive := false
	if est := estimateCommitReservePages(len(keys)); est > 0 {
		hasFree, err := db.pager.HasFreePages()
		if err != nil {
			return err
		}
		if !hasFree {
			if err := db.pager.BeginAllocReserve(est, 4); err != nil {
				return err
			}
			reserveActive = true
			defer func() {
				if reserveActive {
					_ = db.pager.EndAllocReserve()
				}
			}()
		}
	}

	// Local COW roots.
	userTree := tree.NewUserTree(db.pager, st.UserRootPageID)
	systemTree := tree.NewSystemTree(db.pager, st.SystemRootPageID)

	var retired []page.PageID
	modifiedSlabs := modSlabs
	if modifiedSlabs == nil {
		modifiedSlabs = make(map[uint32]struct{})
	}

	// Phase 2: zipper merge into user tree.
	for _, k := range keys {
		op := b.ops[string(k)]
		if op == nil {
			continue
		}
		ids, oldEnt, err := userTree.SetRaw(k, op.leafEntry())
		if err != nil {
			return err
		}
		// Track old pointer dead bytes if overwritten/deleted.
		if oldEnt != nil && oldEnt.Flags == page.LeafFlagPointer {
			if op.del || (op.inline || op.ptr != oldEnt.Ptr) {
				if set := db.slabs.SlabSet(); set != nil {
					if f, ok := set.Get(oldEnt.Ptr.FileID); ok && f != nil {
						f.MarkDead(oldEnt.Ptr)
						modifiedSlabs[oldEnt.Ptr.FileID] = struct{}{}
					}
				}
			}
		}
		retired = append(retired, ids...)

		if db.hooks != nil && db.hooks.panicAfterOps > 0 {
			n := db.hooks.opApplied.Add(1)
			if int(n) >= db.hooks.panicAfterOps {
				panic("treedb: injected panic")
			}
		}
	}

	// Update slab stats in system tree.
	for id := range modifiedSlabs {
		if set := db.slabs.SlabSet(); set != nil {
			if f, ok := set.Get(id); ok && f != nil {
				statsVal := slab.EncodeStatsValue(f.Stats())
				ids, _, err := systemTree.SetRaw(slab.StatsKey(id), tree.LeafEntry{
					Flags:       page.LeafFlagInline,
					InlineValue: statsVal,
				})
				if err != nil {
					return err
				}
				retired = append(retired, ids...)
			}
		}
	}

	if reserveActive {
		if err := db.pager.EndAllocReserve(); err != nil {
			return err
		}
		reserveActive = false
	}

	return db.finishCommit(st, userTree, systemTree, retired, modifiedSlabs, slabWriteBytes, len(keys), sync)
}

func estimateCommitReservePages(ops int) uint64 {
	if ops < 64 {
		return 0
	}
	// When the freelist is empty, the commit must allocate many fresh pages. Use
	// a (capped) estimate to reduce incremental pager growth calls.
	est := uint64(ops) * 2
	est += est / 10
	return est
}

// writeOne applies a single Set/Delete operation under the global writer lock.
// It bypasses Batch allocation/sorting while preserving identical commit semantics.
func (db *DB) writeOne(key, value []byte, del bool, sync bool) error {
	if db == nil {
		return fmt.Errorf("treedb: nil db")
	}
	if db.closed.Load() {
		return fmt.Errorf("treedb: db closed")
	}
	if key == nil || len(key) == 0 {
		return ErrKeyEmpty
	}
	if !del && value == nil {
		return ErrValueNil
	}

	db.writerMu.Lock()
	defer db.writerMu.Unlock()

	st := db.state.Load()
	if st == nil {
		return fmt.Errorf("treedb: no state")
	}

	threshold := db.opts.InlineThreshold
	if db.adaptive != nil && db.adaptive.Enabled() {
		threshold = db.adaptive.Latch()
	}
	if db.hooks != nil && db.hooks.thresholdObserved != nil {
		db.hooks.thresholdObserved(threshold)
	}

	modifiedSlabs := make(map[uint32]struct{})
	var slabWriteBytes uint64
	var opInline bool
	var opPtr page.ValuePtr

	leaf := tree.LeafEntry{Flags: page.LeafFlagTombstone}
	if !del {
		if len(value) > page.InlineHardMax || len(value) > threshold {
			ptr, err := db.slabs.AppendLarge(key, value)
			if err != nil {
				return err
			}
			opPtr = ptr
			opInline = false
			leaf = tree.LeafEntry{Flags: page.LeafFlagPointer, Ptr: ptr}
			modifiedSlabs[ptr.FileID] = struct{}{}
			slabWriteBytes = uint64(4 + ptr.Length)
		} else {
			opInline = true
			leaf = tree.LeafEntry{Flags: page.LeafFlagInline, InlineValue: append([]byte(nil), value...)}
		}
	}

	userTree := tree.NewUserTree(db.pager, st.UserRootPageID)
	systemTree := tree.NewSystemTree(db.pager, st.SystemRootPageID)

	retired, oldEnt, err := userTree.SetRaw(key, leaf)
	if err != nil {
		return err
	}

	// Track old pointer dead bytes if overwritten/deleted.
	if oldEnt != nil && oldEnt.Flags == page.LeafFlagPointer {
		if del || (opInline || opPtr != oldEnt.Ptr) {
			if set := db.slabs.SlabSet(); set != nil {
				if f, ok := set.Get(oldEnt.Ptr.FileID); ok && f != nil {
					f.MarkDead(oldEnt.Ptr)
					modifiedSlabs[oldEnt.Ptr.FileID] = struct{}{}
				}
			}
		}
	}

	// Update slab stats in system tree.
	for id := range modifiedSlabs {
		if set := db.slabs.SlabSet(); set != nil {
			if f, ok := set.Get(id); ok && f != nil {
				statsVal := slab.EncodeStatsValue(f.Stats())
				ids, _, err := systemTree.SetRaw(slab.StatsKey(id), tree.LeafEntry{
					Flags:       page.LeafFlagInline,
					InlineValue: statsVal,
				})
				if err != nil {
					return err
				}
				retired = append(retired, ids...)
			}
		}
	}

	return db.finishCommit(st, userTree, systemTree, retired, modifiedSlabs, slabWriteBytes, 1, sync)
}

// finishCommit persists meta, applies durability if requested, publishes DBState, and records adaptive metrics.
// Caller must hold db.writerMu.
func (db *DB) finishCommit(st *mvcc.DBState, userTree, systemTree *tree.Tree, retired []page.PageID, modifiedSlabs map[uint32]struct{}, slabWriteBytes uint64, ops int, sync bool) error {
	newSeq := st.CommitSeq + 1

	// Ensure any buffered slab bytes are written before publishing pointers and
	// before any durability boundary (WriteSync).
	if db.slabs != nil {
		if err := db.slabs.Flush(); err != nil {
			return err
		}
	}

	// Build new meta based on current pager meta.
	meta := db.pager.ReadActiveMeta()
	meta.CommitSeq = newSeq
	meta.UserRootPageID = userTree.Root()
	meta.SystemRootPageID = systemTree.Root()
	meta.ActiveSlabID = db.slabs.ActiveID()
	meta.ActiveSlabTail = db.slabs.ActiveTail()

	metaPid := page.PageID(newSeq % 2)
	metaBuf, err := encodeMetaPage(metaPid, meta)
	if err != nil {
		return err
	}
	if err := db.pager.WritePage(metaPid, metaBuf); err != nil {
		return err
	}

	if sync {
		if err := db.syncSlabs(modifiedSlabs); err != nil {
			return err
		}
		if db.hooks != nil && db.hooks.slabSynced != nil {
			db.hooks.slabSynced()
		}
		if err := db.pager.SyncIndex(); err != nil {
			return err
		}
		if db.hooks != nil && db.hooks.indexSynced != nil {
			db.hooks.indexSynced()
		}
	}

	// Publish new state and record retired pages.
	db.grave.Record(newSeq, retired)
	db.state.Publish(&mvcc.DBState{
		CommitSeq:        newSeq,
		UserRootPageID:   userTree.Root(),
		SystemRootPageID: systemTree.Root(),
		SlabSet:          db.slabs.SlabSet(),
	})

	if db.adaptive != nil && db.adaptive.Enabled() {
		fillAvg, leafCount, _ := adaptive.ComputeLeafStats(db.pager, userTree.Root())
		slabDeadRatio := 0.0
		if set := db.slabs.SlabSet(); set != nil {
			var dead, total uint64
			for _, id := range set.IDs() {
				f, ok := set.Get(id)
				if !ok || f == nil {
					continue
				}
				sts := f.Stats()
				dead += sts.DeadBytes
				total += sts.TotalBytes
			}
			if total > 0 {
				slabDeadRatio = float64(dead) / float64(total)
			}
		}
		db.adaptive.RecordCommit(adaptive.CommitMetrics{
			LeafFillAvg:     fillAvg,
			LeafCount:       leafCount,
			IndexWriteBytes: uint64(len(retired)) * page.PageSize,
			SlabWriteBytes:  slabWriteBytes,
			SlabDeadRatio:   slabDeadRatio,
			CompactionIOBPS: 0,
			Ops:             ops,
		})
	}

	_ = db.pruner.Prune(newSeq)
	return nil
}

func (db *DB) syncSlabs(modified map[uint32]struct{}) error {
	if db == nil || db.slabs == nil {
		return nil
	}
	set := db.slabs.SlabSet()
	if set == nil {
		return nil
	}
	// Always sync active slab when syncing.
	activeID := db.slabs.ActiveID()
	modified[activeID] = struct{}{}
	for id := range modified {
		f, ok := set.Get(id)
		if !ok || f == nil || f.Handle == nil {
			continue
		}
		if err := f.Handle.Sync(); err != nil {
			return err
		}
	}
	return nil
}
