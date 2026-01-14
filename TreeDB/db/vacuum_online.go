package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/TreeDB/zipper"
)

var ErrVacuumInProgress = errors.New("online vacuum already in progress")
var ErrVacuumUnsupported = errors.New("online vacuum unsupported on this platform")

const (
	vacuumDeltaBatchSize     = 4096
	vacuumCatchupPassesMax   = 3
	vacuumCatchupKeyTarget   = 4096
	vacuumCutoverMaxKeys     = 8192
	vacuumCutoverMaxDefers   = 3
	vacuumInlineThresholdMax = int(^uint(0) >> 1)
	vacuumMaxGrowthFactor    = 8
)

type vacuumRecorder struct {
	active atomic.Bool
	mu     sync.Mutex
	ops    map[string]batch.Entry
}

func (r *vacuumRecorder) Active() bool {
	return r.active.Load()
}

func (r *vacuumRecorder) Start() {
	r.mu.Lock()
	r.ops = make(map[string]batch.Entry, 1024)
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
	if r.ops == nil {
		r.ops = make(map[string]batch.Entry, len(ops))
	}
	for k, v := range ops {
		// Copy entry to ensure it's detached from the batch memory
		copied := v
		copied.Key = append([]byte(nil), v.Key...)
		if v.Value != nil {
			copied.Value = append([]byte(nil), v.Value...)
		}
		r.ops[k] = copied
	}
}

func (r *vacuumRecorder) Drain() map[string]batch.Entry {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.ops) == 0 {
		return nil
	}
	out := r.ops
	r.ops = make(map[string]batch.Entry, 1024)
	return out
}

// VacuumIndexOnline rebuilds the index into a new file and swaps it in with a
// short writer pause. Old snapshots remain valid by pinning the previous index
// generation until readers drain; disk space is reclaimed once the old mmap is
// closed.
func (db *DB) VacuumIndexOnline(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if db.readOnly {
		return ErrReadOnly
	}
	if runtime.GOOS == "windows" {
		return ErrVacuumUnsupported
	}

	if !db.vacuumInProgress.CompareAndSwap(false, true) {
		return ErrVacuumInProgress
	}
	defer db.vacuumInProgress.Store(false)

	if db.dir == "" {
		return errors.New("vacuum: missing db dir")
	}

	indexPath := filepath.Join(db.dir, indexFileName)
	newPath := filepath.Join(db.dir, indexNewFileName)
	bakPath := filepath.Join(db.dir, indexBakFileName)
	readyPath := filepath.Join(db.dir, indexReadyFileName)

	// Clean up any previous partial artifacts (best-effort).
	_ = os.Remove(newPath)
	_ = os.Remove(readyPath)

	newPager, err := pager.Open(newPath, db.chunkSize)
	if err != nil {
		return err
	}
	cleanupNewPager := func() {
		_ = newPager.Close()
		_ = os.Remove(newPath)
		_ = os.Remove(readyPath)
	}

	oldGen := db.idx.Load()
	oldPages := uint64(0)
	if oldGen != nil && oldGen.pager != nil {
		oldPages = oldGen.pager.PageCount()
	}
	maxPages := uint64(0)
	if oldPages > 0 {
		maxPages = oldPages * uint64(vacuumMaxGrowthFactor)
	}
	checkGrowth := func() error {
		if maxPages == 0 {
			return nil
		}
		newPages := newPager.PageCount()
		if newPages > maxPages {
			return fmt.Errorf("vacuum: new index page count %d exceeds %dx old (%d)", newPages, vacuumMaxGrowthFactor, oldPages)
		}
		return nil
	}

	if _, err := newPager.Alloc(2); err != nil {
		cleanupNewPager()
		return err
	}

	newAlloc := freelist.New(newPager, 0)
	newAlloc.SetPreferAppend(db.preferAppendAlloc)
	newAlloc.SetFreelistRegion(db.freelistRegionPages, db.freelistRegionRadius)

	newZ := zipper.New(newPager, newAlloc)
	newZ.SetFillTargets(db.leafFillTargetPPM, db.internalFillTargetPPM)
	newZ.SetPiggybackCompaction(db.piggybackCompaction)
	newZ.SetLeafPrefixCompression(db.leafPrefixCompression)
	newZ.SetMaintenanceDeleteMinRatio(db.maintenanceDeleteMinRatio)

	db.vacuum.Start()
	defer db.vacuum.Stop()

	// Build a fresh user tree from a stable snapshot.
	baseSnap := db.AcquireSnapshot()
	baseIter := baseSnap.tree.Iterator(nil, nil)

	buildOpts := bulk.BuildOptions{
		LeafPrefixCompression: db.leafPrefixCompression,
		ValueWriter:           db.slabManager,
	}
	if db.forceValuePointers {
		buildOpts.ForceValuePointers = true
	}

	newRoot, err := bulk.BuildWithOptions(baseIter, newAlloc, newPager, buildOpts)
	_ = baseIter.Close()
	_ = baseSnap.Close()
	if err != nil {
		cleanupNewPager()
		return err
	}
	if err := checkGrowth(); err != nil {
		cleanupNewPager()
		return err
	}

	freeRetired := func(retired []uint64) {
		for _, id := range retired {
			_ = newAlloc.Free(id)
		}
	}

	// Online catch-up: replay recorded keys in bounded passes.
	for pass := 0; pass < vacuumCatchupPassesMax; pass++ {
		if err := ctx.Err(); err != nil {
			cleanupNewPager()
			return err
		}
		ops := db.vacuum.Drain()
		if len(ops) == 0 {
			break
		}
		var retired []uint64
		newRoot, retired, err = db.applyVacuumDelta(newRoot, ops, newZ, nil)
		if err != nil {
			cleanupNewPager()
			return err
		}
		freeRetired(retired)
		if err := checkGrowth(); err != nil {
			cleanupNewPager()
			return err
		}
		if len(ops) <= vacuumCatchupKeyTarget {
			break
		}
	}

	// Final cutover: stop recording, apply the tail, rebuild the System tree in
	// the new file, then swap index.db on disk and publish a new index generation.
	defers := 0
	for {
		if err := ctx.Err(); err != nil {
			cleanupNewPager()
			return err
		}

		db.writeMu.Lock()
		db.vacuum.Stop()
		finalOps := db.vacuum.Drain()
		if len(finalOps) > vacuumCutoverMaxKeys && defers < vacuumCutoverMaxDefers {
			db.vacuum.Start()
			db.writeMu.Unlock()
			defers++

			var retired []uint64
			newRoot, retired, err = db.applyVacuumDelta(newRoot, finalOps, newZ, nil)
			if err != nil {
				cleanupNewPager()
				return err
			}
			freeRetired(retired)
			if err := checkGrowth(); err != nil {
				cleanupNewPager()
				return err
			}
			continue
		}

		if len(finalOps) > 0 {
			var retired []uint64
			newRoot, retired, err = db.applyVacuumDelta(newRoot, finalOps, newZ, nil)
			if err != nil {
				db.writeMu.Unlock()
				cleanupNewPager()
				return err
			}
			freeRetired(retired)
			if err := checkGrowth(); err != nil {
				db.writeMu.Unlock()
				cleanupNewPager()
				return err
			}
		}

		// Snapshot current roots/meta while writers are paused.
		db.mu.RLock()
		oldGen := db.idx.Load()
		state := db.state.Load()
		baseMeta := db.meta
		db.mu.RUnlock()
		if oldGen == nil || state == nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return errors.New("vacuum: missing db state")
		}

		sysIter := tree.New(oldGen.pager, valueReader{slabs: state.SlabSet, vlogs: state.ValueLogSet}, state.SystemRootPageID).Iterator(nil, nil)
		newSysRoot, err := bulk.BuildWithOptions(sysIter, newAlloc, newPager, bulk.BuildOptions{
			LeafPrefixCompression: db.leafPrefixCompression,
		})
		_ = sysIter.Close()
		if err != nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		}
		if err := checkGrowth(); err != nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		}

		// Prepare new meta.
		nextMeta := baseMeta
		nextMeta.CommitSeq++
		nextMeta.UserRootPageID = newRoot
		nextMeta.SystemRootPageID = newSysRoot
		nextMeta.FreelistHeadID = newAlloc.Head()
		nextMeta.ActiveSlabID = db.slabManager.ActiveSlabID()
		nextMeta.ActiveSlabTail = db.slabManager.ActiveSlabTail()
		nextMeta.TotalPages = newPager.PageCount()
		nextMeta.LastSeq = baseMeta.LastSeq // Explicitly copy LastSeq

		// Ensure slab tail referenced by meta is durable before publishing the
		// new index. Vacuum is treated as a durability boundary.
		if err := db.slabManager.Sync(); err != nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		}

		// Write redundant Meta pages (0/1) to the new file and sync it.
		if err := writeMetaToPager(newPager, MetaPage0ID, nextMeta); err != nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		}
		if err := writeMetaToPager(newPager, MetaPage1ID, nextMeta); err != nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		}
		if err := newPager.Sync(); err != nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		}

		if err := os.WriteFile(readyPath, []byte("ready\n"), 0o644); err != nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		}
		if runtime.GOOS != "windows" {
			if dir, err := os.Open(db.dir); err == nil {
				_ = dir.Sync()
				_ = dir.Close()
			}
		}

		// Swap index.db -> index.db.bak, index.db.new -> index.db.
		_ = os.Remove(bakPath)
		if err := os.Rename(indexPath, bakPath); err != nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		}
		if err := os.Rename(newPath, indexPath); err != nil {
			_ = os.Rename(bakPath, indexPath)
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		}

		_ = os.Remove(readyPath)
		_ = os.Remove(bakPath)
		if runtime.GOOS != "windows" {
			if dir, err := os.Open(db.dir); err == nil {
				_ = dir.Sync()
				_ = dir.Close()
			}
		}

		// Publish the new index generation (old readers keep oldGen pinned).
		newGen := newIndexGen(db.nextIndexID(), newPager, newAlloc, newZ)
		db.trackIndex(newGen)

		var oldState *DBState
		db.mu.Lock()
		oldState = db.state.Load()
		db.idx.Store(newGen)
		db.meta = nextMeta
		db.metaPageID = MetaPage0ID
		db.state.Store(&DBState{
			CommitSeq:        nextMeta.CommitSeq,
			RootPageID:       nextMeta.UserRootPageID,
			SystemRootPageID: nextMeta.SystemRootPageID,
			SlabSet:          db.slabManager.CurrentSlabSet(),
			ValueLogSet:      db.valueLogManager.CurrentSet(),
			LastSeq:          nextMeta.LastSeq,
		})
		db.mu.Unlock()

		db.writeMu.Unlock()

		if oldState != nil {
			_ = db.slabManager.ReleaseSlabs(oldState.SlabSet)
			_ = db.valueLogManager.Release(oldState.ValueLogSet)
		}

		// Drop the DB-held reference to the previous generation outside the
		// writer pause; closing the old mmap can be expensive.
		db.releaseIndex(oldGen)

		return nil
	}
}

func (db *DB) applyVacuumDelta(root uint64, opsMap map[string]batch.Entry, z *zipper.Zipper, retired []uint64) (uint64, []uint64, error) {
	if len(opsMap) == 0 {
		return root, retired, nil
	}

	snap := db.AcquireSnapshot()
	defer snap.Close()
	// tr := tree.New(snap.idx.pager, valueReader{slabs: snap.state.SlabSet, vlogs: snap.state.ValueLogSet}, snap.state.RootPageID)

	ops := make([]batch.Entry, 0, vacuumDeltaBatchSize)
	applyOps := func() error {
		if len(ops) == 0 {
			return nil
		}
		b := batch.New(db.slabManager, vacuumInlineThresholdMax)
		defer func() { _ = b.Close() }()
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

	// Sort keys to ensure sequential updates. Random map iteration causes
	// massive write amplification (random COW path rewrites) and can balloon
	// the index file.
	sortedKeys := make([]string, 0, len(opsMap))
	for k := range opsMap {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	for _, key := range sortedKeys {
		// We trust the entry from the recorder because it was captured from a
		// committed batch. We do not need to look it up in the old index, which
		// avoids race conditions where the old index snapshot might be stale.
		//
		// Note: The entry contains full Value or ValuePtr.
		entry := opsMap[key]
		ops = append(ops, entry)

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
