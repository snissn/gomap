package db

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/TreeDB/zipper"
)

var ErrVacuumInProgress = errors.New("online vacuum already in progress")

const (
	vacuumDeltaBatchSize     = 4096
	vacuumCatchupPassesMax   = 3
	vacuumCatchupKeyTarget   = 4096
	vacuumCutoverMaxKeys     = 8192
	vacuumCutoverMaxDefers   = 3
	vacuumRetireBatchSize    = 4096
	vacuumInlineThresholdMax = int(^uint(0) >> 1)
)

type vacuumRecorder struct {
	active atomicBool
	mu     sync.Mutex
	keys   map[string]struct{}
}

func (r *vacuumRecorder) Active() bool {
	return r.active.Load()
}

func (r *vacuumRecorder) Start() {
	r.mu.Lock()
	r.keys = make(map[string]struct{}, 1024)
	r.mu.Unlock()
	r.active.Store(true)
}

func (r *vacuumRecorder) Stop() {
	r.active.Store(false)
}

func (r *vacuumRecorder) RecordOps(ops map[string]batch.Entry) {
	if !r.active.Load() || len(ops) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.active.Load() {
		return
	}
	if r.keys == nil {
		r.keys = make(map[string]struct{}, len(ops))
	}
	for k := range ops {
		r.keys[k] = struct{}{}
	}
}

func (r *vacuumRecorder) Drain() map[string]struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.keys) == 0 {
		return nil
	}
	out := r.keys
	r.keys = make(map[string]struct{}, 1024)
	return out
}

type atomicBool struct{ v uint32 }

func (b *atomicBool) Load() bool {
	return atomic.LoadUint32(&b.v) == 1
}

func (b *atomicBool) Store(v bool) {
	if v {
		atomic.StoreUint32(&b.v, 1)
		return
	}
	atomic.StoreUint32(&b.v, 0)
}

// VacuumIndexOnline rebuilds the user index in the background and swaps it in
// with a short writer pause. It records backend writes during the build and
// applies a delta replay before the swap. Old tree pages are retired
// asynchronously after the swap.
func (db *DB) VacuumIndexOnline(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	db.vacuumMu.Lock()
	defer db.vacuumMu.Unlock()

	if db.vacuum.Active() {
		return ErrVacuumInProgress
	}
	db.vacuum.Start()
	defer db.vacuum.Stop()

	// Build a fresh tree from a stable snapshot.
	snap := db.AcquireSnapshot()
	iter := snap.tree.Iterator(nil, nil)
	alloc := &pagerAllocator{p: db.pager}
	newRoot, err := bulk.Build(iter, alloc, db.pager)
	_ = iter.Close()
	snap.Close()
	if err != nil {
		return err
	}

	z := zipper.New(db.pager, alloc)
	z.SetFillTargets(db.leafFillTargetPPM, db.internalFillTargetPPM)
	z.SetPiggybackCompaction(db.piggybackCompaction)

	var retired []uint64
	for pass := 0; pass < vacuumCatchupPassesMax; pass++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		keys := db.vacuum.Drain()
		if len(keys) == 0 {
			break
		}
		newRoot, retired, err = db.applyVacuumDelta(newRoot, keys, z, retired)
		if err != nil {
			return err
		}
		if len(keys) <= vacuumCatchupKeyTarget {
			break
		}
	}

	// Final cutover: stop recording, apply the tail, then swap roots.
	defers := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		db.writeMu.Lock()
		db.vacuum.Stop()
		finalKeys := db.vacuum.Drain()
		if len(finalKeys) > vacuumCutoverMaxKeys && defers < vacuumCutoverMaxDefers {
			db.vacuum.Start()
			db.writeMu.Unlock()
			defers++
			newRoot, retired, err = db.applyVacuumDelta(newRoot, finalKeys, z, retired)
			if err != nil {
				return err
			}
			continue
		}

		if len(finalKeys) > 0 {
			newRoot, retired, err = db.applyVacuumDelta(newRoot, finalKeys, z, retired)
			if err != nil {
				db.writeMu.Unlock()
				return err
			}
		}

		oldSnap := db.AcquireSnapshot()
		oldRoot := oldSnap.state.RootPageID
		sysRoot := oldSnap.state.SystemRootPageID

		if err := db.finalizeCommit(newRoot, sysRoot, retired, true, adaptive.Metrics{}); err != nil {
			db.writeMu.Unlock()
			oldSnap.Close()
			return err
		}
		db.writeMu.Unlock()

		seq := db.state.Load().CommitSeq
		if err := db.retireVacuumRoot(oldSnap, oldRoot, seq); err != nil {
			return err
		}

		return nil
	}
}

func (db *DB) applyVacuumDelta(root uint64, keys map[string]struct{}, z *zipper.Zipper, retired []uint64) (uint64, []uint64, error) {
	if len(keys) == 0 {
		return root, retired, nil
	}

	snap := db.AcquireSnapshot()
	defer snap.Close()
	tr := tree.New(db.pager, snap.state.SlabSet, snap.state.RootPageID)

	ops := make([]batch.Entry, 0, vacuumDeltaBatchSize)
	applyOps := func() error {
		if len(ops) == 0 {
			return nil
		}
		b := batch.New(db.slabManager, vacuumInlineThresholdMax)
		if err := b.SetOps(ops); err != nil {
			return err
		}
		newRoot, newRetired, _, err := z.Apply(root, b)
		if err != nil {
			return err
		}
		root = newRoot
		if len(newRetired) > 0 {
			retired = append(retired, newRetired...)
		}
		ops = ops[:0]
		return nil
	}

	for key := range keys {
		entry, err := tr.GetEntry([]byte(key))
		if err != nil {
			if err == tree.ErrKeyNotFound {
				ops = append(ops, batch.Entry{
					Type: batch.OpDelete,
					Key:  []byte(key),
				})
			} else {
				return 0, nil, err
			}
		} else if entry.Flags&node.FlagTombstone != 0 {
			ops = append(ops, batch.Entry{
				Type: batch.OpDelete,
				Key:  append([]byte(nil), entry.Key...),
			})
		} else if entry.Flags&node.FlagPointer != 0 {
			ops = append(ops, batch.Entry{
				Type:     batch.OpPut,
				Key:      append([]byte(nil), entry.Key...),
				ValuePtr: entry.ValuePtr,
				IsPtr:    true,
			})
		} else {
			val := append([]byte(nil), entry.Value...)
			ops = append(ops, batch.Entry{
				Type:  batch.OpPut,
				Key:   append([]byte(nil), entry.Key...),
				Value: val,
			})
		}

		if len(ops) >= vacuumDeltaBatchSize {
			if err := applyOps(); err != nil {
				return 0, nil, err
			}
		}
	}

	if err := applyOps(); err != nil {
		return 0, nil, err
	}

	return root, retired, nil
}

func (db *DB) retireVacuumRoot(snap *Snapshot, rootID uint64, seq uint64) error {
	defer snap.Close()

	tr := tree.New(db.pager, snap.state.SlabSet, rootID)
	batchIDs := make([]uint64, 0, vacuumRetireBatchSize)

	err := tr.WalkPages(func(pageID uint64, _ node.Node) error {
		batchIDs = append(batchIDs, pageID)
		if len(batchIDs) >= vacuumRetireBatchSize {
			db.graveyard.Add(seq, batchIDs)
			batchIDs = batchIDs[:0]
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("vacuum retire walk: %w", err)
	}
	if len(batchIDs) > 0 {
		db.graveyard.Add(seq, batchIDs)
	}

	if db.pruner.Enabled() {
		db.pruner.Kick()
	} else {
		db.Prune()
	}
	return nil
}
