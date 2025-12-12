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

		// Track old pointer dead bytes if overwritten/deleted.
		old, err := userTree.GetRaw(k)
		if err == nil && old.Flags == page.LeafFlagPointer {
			if op.del || (op.inline || op.ptr != old.Ptr) {
				if set := db.slabs.SlabSet(); set != nil {
					if f, ok := set.Get(old.Ptr.FileID); ok && f != nil {
						f.MarkDead(old.Ptr)
						modifiedSlabs[old.Ptr.FileID] = struct{}{}
					}
				}
			}
		}

		ids, err := userTree.SetRaw(k, op.leafEntry())
		if err != nil {
			return err
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
				ids, err := systemTree.SetRaw(slab.StatsKey(id), tree.LeafEntry{
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

	newSeq := st.CommitSeq + 1

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
			Ops:             len(keys),
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
