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
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
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

func vacuumRecordCopyEntry(entry batch.Entry) batch.Entry {
	out := entry
	out.Key = append([]byte(nil), entry.Key...)
	if entry.Type == batch.OpPut && !entry.IsPtr {
		out.Value = append([]byte(nil), entry.Value...)
	} else {
		out.Value = nil
	}
	return out
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
	for k, entry := range ops {
		r.ops[k] = vacuumRecordCopyEntry(entry)
	}
}

func (r *vacuumRecorder) RecordEntries(entries []batch.Entry) {
	if !r.active.Load() || len(entries) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.active.Load() {
		return
	}
	if r.ops == nil {
		r.ops = make(map[string]batch.Entry, len(entries))
	}
	for i := range entries {
		entry := entries[i]
		if len(entry.Key) == 0 {
			continue
		}
		r.ops[string(entry.Key)] = vacuumRecordCopyEntry(entry)
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
	return db.vacuumIndexOnline(ctx, true)
}

func (db *DB) vacuumIndexOnline(ctx context.Context, lockMaintenance bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if db.readOnly {
		return ErrReadOnly
	}
	if runtime.GOOS == "windows" {
		return ErrVacuumUnsupported
	}
	if lockMaintenance {
		db.maintenanceMu.Lock()
		defer db.maintenanceMu.Unlock()
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
	newZ.SetIndexColumnarLeaves(db.indexColumnarLeaves)
	newZ.SetIndexPackedValuePtr(db.indexPackedValuePtr)
	newZ.SetIndexInternalBaseDelta(db.indexInternalBaseDelta)
	newZ.SetAdaptiveLeafEncoding(db.indexAdaptiveLeafEncoding)
	newZ.SetMaintenanceOpsPerCoalesce(db.maintenanceOpsPerCoalesce)
	newZ.SetLeafPageReader(db.leafPageReader(db.valueLogManager))
	newZ.SetLeafPageLog(db.leafPageLog)
	newZ.SetOuterLeavesInValueLog(db.indexOuterLeavesInValueLog)
	db.idxMu.Lock()
	parallelMergePressureSource := db.zipperParallelMergeSource
	db.idxMu.Unlock()
	newZ.SetParallelMergePressureSource(parallelMergePressureSource)

	db.vacuum.Start()
	defer db.vacuum.Stop()

	// Build a fresh user tree from a stable snapshot.
	baseSnap := db.AcquireSnapshot()
	var newRoot uint64
	if db.indexOuterLeavesInValueLog {
		if db.leafPageLog == nil {
			_ = baseSnap.Close()
			cleanupNewPager()
			return fmt.Errorf("vacuum: leaf page log not configured")
		}

		baseState := baseSnap.State()
		basePager := baseSnap.Pager()
		if baseState == nil || basePager == nil {
			_ = baseSnap.Close()
			cleanupNewPager()
			return errors.New("vacuum: missing base snapshot state")
		}
		rootData, err := basePager.Get(baseState.RootPageID)
		if err != nil {
			_ = baseSnap.Close()
			cleanupNewPager()
			return err
		}
		effectiveInternalBaseDelta := db.indexInternalBaseDelta && !db.indexOuterLeavesInValueLog
		rootNode := node.NewNode(rootData)
		if rootNode.Type() == page.PageTypeLeaf {
			newRoot, err = vacuumClonePagerTreeWithLeafRefs(basePager, baseState.RootPageID, newAlloc, newPager, effectiveInternalBaseDelta)
			if err != nil {
				_ = baseSnap.Close()
				cleanupNewPager()
				return err
			}
		} else {
			leafChildren, err := vacuumCollectLeafRefChildren(basePager, baseState.RootPageID)
			if err != nil {
				_ = baseSnap.Close()
				cleanupNewPager()
				return err
			}
			newRoot, err = vacuumBuildInternalTreeFromChildren(newPager, newAlloc, leafChildren, effectiveInternalBaseDelta)
			if err != nil {
				_ = baseSnap.Close()
				cleanupNewPager()
				return err
			}
		}
	} else {
		baseIter := baseSnap.tree.IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
		buildOpts := bulk.BuildOptions{
			LeafPrefixCompression: db.leafPrefixCompression,
			LeafColumnar:          db.indexColumnarLeaves,
			PackedValuePtr:        db.indexPackedValuePtr,
			InternalBaseDelta:     db.indexInternalBaseDelta,
		}
		newRoot, err = bulk.BuildWithOptions(baseIter, newAlloc, newPager, buildOpts)
		_ = baseIter.Close()
	}
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
		opsMap := db.vacuum.Drain()
		if len(opsMap) == 0 {
			break
		}
		var retired []uint64
		newRoot, retired, err = db.applyVacuumDelta(newRoot, opsMap, newZ, nil)
		if err != nil {
			cleanupNewPager()
			return err
		}
		freeRetired(retired)
		if db.indexOuterLeavesInValueLog && db.leafPageLog != nil {
			if err := db.leafPageLog.Flush(); err != nil {
				cleanupNewPager()
				return err
			}
		}
		if err := checkGrowth(); err != nil {
			cleanupNewPager()
			return err
		}
		if len(opsMap) <= vacuumCatchupKeyTarget {
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
			if db.indexOuterLeavesInValueLog && db.leafPageLog != nil {
				if err := db.leafPageLog.Flush(); err != nil {
					cleanupNewPager()
					return err
				}
			}
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
			if db.indexOuterLeavesInValueLog && db.leafPageLog != nil {
				if err := db.leafPageLog.Flush(); err != nil {
					db.writeMu.Unlock()
					cleanupNewPager()
					return err
				}
			}
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

		reader := newValueReader(state.ValueLogSet)
		collectionRootReplacements, err := vacuumRewriteCollectionRoots(oldGen.pager, reader, state.SystemRootPageID, newAlloc, newPager)
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

		var newSysRoot uint64
		effectiveInternalBaseDelta := db.indexInternalBaseDelta && !db.indexOuterLeavesInValueLog
		if db.indexOuterLeavesInValueLog && len(collectionRootReplacements) == 0 {
			newSysRoot, err = vacuumClonePagerTreeWithLeafRefs(oldGen.pager, state.SystemRootPageID, newAlloc, newPager, effectiveInternalBaseDelta)
		} else {
			newSysRoot, err = vacuumBuildSystemRoot(oldGen.pager, reader, state.SystemRootPageID, newAlloc, newPager, bulk.BuildOptions{
				LeafPrefixCompression: db.leafPrefixCompression,
				LeafColumnar:          db.indexColumnarLeaves,
				PackedValuePtr:        db.indexPackedValuePtr,
				InternalBaseDelta:     effectiveInternalBaseDelta,
			}, collectionRootReplacements)
		}
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
		nextMeta.TotalPages = newPager.PageCount()

		if db.indexOuterLeavesInValueLog && db.leafPageLog != nil {
			if err := db.leafPageLog.Sync(); err != nil {
				db.writeMu.Unlock()
				cleanupNewPager()
				return err
			}
		}
		leafPageLogSegmentsRegistered := true
		if db.indexOuterLeavesInValueLog && db.leafPageLog != nil {
			var err error
			leafPageLogSegmentsRegistered, err = db.registerLeafPageLogSegmentsForPublish(nextMeta.CommitSeq)
			if err != nil {
				db.writeMu.Unlock()
				cleanupNewPager()
				return err
			}
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

		// Ensure the new generation preserves leaf-page-in-value-log wiring for
		// subsequent writes.
		newZ.SetLeafPageReader(db.leafPageReader(db.valueLogManager))
		newZ.SetLeafPageLog(db.leafPageLog)
		newZ.SetOuterLeavesInValueLog(db.indexOuterLeavesInValueLog)

		// Publish the new index generation (old readers keep oldGen pinned).
		newGen := newIndexGen(db.nextIndexID(), newPager, newAlloc, newZ)
		db.trackIndex(newGen)

		var oldState *DBState
		db.mu.Lock()
		oldState = db.state.Load()
		db.idx.Store(newGen)
		db.meta = nextMeta
		db.metaPageID = MetaPage0ID
		valueLogSet := db.valueLogManager.CurrentSetNoRefresh()
		if !leafPageLogSegmentsRegistered {
			if valueLogSet != nil {
				_ = db.valueLogManager.Release(valueLogSet)
				valueLogSet = nil
			}
			if err := db.valueLogManager.Refresh(); err != nil {
				db.mu.Unlock()
				db.writeMu.Unlock()
				cleanupNewPager()
				return err
			}
			valueLogSet = db.valueLogManager.CurrentSetNoRefresh()
		}
		newState := &DBState{
			CommitSeq:                  nextMeta.CommitSeq,
			RootPageID:                 nextMeta.UserRootPageID,
			SystemRootPageID:           nextMeta.SystemRootPageID,
			AppliedCommandLSN:          nextMeta.AppliedCommandLSN,
			ValueLogSet:                valueLogSet,
			LeafGenerations:            oldState.LeafGenerations,
			LeafGenerationStateVersion: oldState.LeafGenerationStateVersion,
		}
		db.state.Store(newState)
		db.publishSnapshotView(newGen, newState, db.valueLogManager)
		db.mu.Unlock()
		db.clearLeafGenerationReachabilityCaches()

		db.writeMu.Unlock()

		if oldState != nil {
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

	keys := make([]string, 0, len(opsMap))
	for k := range opsMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	ops := make([]batch.Entry, 0, vacuumDeltaBatchSize)
	applyOps := func() error {
		if len(ops) == 0 {
			return nil
		}
		b := batch.New(db.valueLogManager, vacuumInlineThresholdMax)
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

	for _, key := range keys {
		entry := opsMap[key]
		if len(entry.Key) == 0 {
			entry.Key = []byte(key)
		}
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

type vacuumCloneCtx struct {
	oldPager          *pager.Pager
	newPager          *pager.Pager
	internalBaseDelta bool
	alloc             interface {
		Alloc(hint uint64) (uint64, error)
	}
	remap map[uint64]uint64
}

type vacuumLeafChild struct {
	key      []byte
	childRef page.ChildRef
}

func vacuumCollectLeafRefChildren(p *pager.Pager, rootID uint64) ([]vacuumLeafChild, error) {
	if p == nil {
		return nil, errors.New("vacuum: missing pager")
	}
	if rootID == 0 {
		return nil, errors.New("vacuum: missing root id")
	}
	out := make([]vacuumLeafChild, 0, 1024)
	var walk func(uint64) error
	walk = func(id uint64) error {
		data, err := p.Get(id)
		if err != nil {
			return err
		}
		n := node.NewNode(data)
		switch n.Type() {
		case page.PageTypeInternal:
			count := n.Count()
			for i := uint16(0); i < count; i++ {
				keyView, childRef, err := n.GetInternalEntryRefView(i)
				if err != nil {
					return err
				}
				if childRef.Kind == page.ChildRefLeafLog {
					out = append(out, vacuumLeafChild{
						key:      append([]byte(nil), keyView...),
						childRef: childRef,
					})
					continue
				}
				if err := walk(childRef.Page); err != nil {
					return err
				}
			}
			return nil
		case page.PageTypeLeaf:
			// In outer-leaf-in-vlog mode, leaves should be leafrefs, not pager pages.
			return fmt.Errorf("vacuum: unexpected pager-backed leaf page %d while collecting leafrefs", id)
		default:
			return fmt.Errorf("vacuum: unexpected page type %d at page %d", n.Type(), id)
		}
	}
	if err := walk(rootID); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, errors.New("vacuum: collected zero leafref children")
	}
	return out, nil
}

func vacuumBuildInternalTreeFromChildren(p *pager.Pager, alloc interface {
	Alloc(hint uint64) (uint64, error)
}, children []vacuumLeafChild, internalBaseDelta bool) (uint64, error) {
	if p == nil || alloc == nil {
		return 0, errors.New("vacuum: missing pager/allocator")
	}
	if len(children) == 0 {
		return 0, errors.New("vacuum: missing children")
	}

	type levelBuilder struct {
		builder  *node.Builder
		startKey []byte
	}
	var levels []*levelBuilder

	newBuilder := func() (*node.Builder, error) {
		buf := make([]byte, page.PageSize)
		var b *node.Builder
		if internalBaseDelta {
			b = node.NewBuilderWithOptions(buf, page.PageTypeInternal, node.BuilderOptions{InternalBaseDelta: true})
		} else {
			b = node.NewBuilder(buf, page.PageTypeInternal)
		}
		pid, err := alloc.Alloc(0)
		if err != nil {
			return nil, err
		}
		b.SetPageID(pid)
		return b, nil
	}

	ensureLevel := func(lvl int) error {
		for len(levels) <= lvl {
			b, err := newBuilder()
			if err != nil {
				return err
			}
			levels = append(levels, &levelBuilder{builder: b})
		}
		return nil
	}
	if err := ensureLevel(0); err != nil {
		return 0, err
	}

	var flush func(int) error
	flush = func(lvl int) error {
		lb := levels[lvl]
		n := lb.builder.Finish()
		childID := lb.builder.PageID()
		if err := p.Write(childID, n.Data()); err != nil {
			return err
		}

		if err := ensureLevel(lvl + 1); err != nil {
			return err
		}
		parent := levels[lvl+1]
		key := lb.startKey
		if key == nil {
			key = []byte{}
		}
		if parent.startKey == nil {
			parent.startKey = append([]byte(nil), key...)
		}

		err := parent.builder.AddInternalChild(key, childID)
		if err == node.ErrNodeFull {
			if err := flush(lvl + 1); err != nil {
				return err
			}
			parent = levels[lvl+1]
			if parent.startKey == nil {
				parent.startKey = append([]byte(nil), key...)
			}
			if err := parent.builder.AddInternalChild(key, childID); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}

		// Reset this level.
		b, err := newBuilder()
		if err != nil {
			return err
		}
		lb.builder = b
		lb.startKey = nil
		return nil
	}

	for _, child := range children {
		key := child.key
		lb := levels[0]
		if lb.startKey == nil {
			lb.startKey = append([]byte(nil), key...)
		}
		err := lb.builder.AddInternalChildRef(key, child.childRef)
		if err == node.ErrNodeFull {
			if err := flush(0); err != nil {
				return 0, err
			}
			lb = levels[0]
			if lb.startKey == nil {
				lb.startKey = append([]byte(nil), key...)
			}
			err = lb.builder.AddInternalChildRef(key, child.childRef)
		}
		if err != nil {
			return 0, err
		}
	}

	// Finalize all levels (same promotion logic as bulk builder).
	currID := uint64(0)
	for i := 0; i < len(levels); i++ {
		lb := levels[i]
		n := lb.builder.Finish()
		childID := lb.builder.PageID()
		if err := p.Write(childID, n.Data()); err != nil {
			return 0, err
		}
		currID = childID

		if i < len(levels)-1 {
			parent := levels[i+1]
			key := lb.startKey
			if key == nil {
				key = []byte{}
			}
			if parent.startKey == nil {
				parent.startKey = append([]byte(nil), key...)
			}
			err := parent.builder.AddInternalChild(key, currID)
			if err == node.ErrNodeFull {
				if err := flush(i + 1); err != nil {
					return 0, err
				}
				parent = levels[i+1]
				if parent.startKey == nil {
					parent.startKey = append([]byte(nil), key...)
				}
				if err := parent.builder.AddInternalChild(key, currID); err != nil {
					return 0, err
				}
			} else if err != nil {
				return 0, err
			}
		}
	}

	// Reduce root if possible.
	if len(levels) > 1 {
		root := levels[len(levels)-1].builder.Finish()
		if root.Count() == 1 {
			childRef, err := root.GetInternalChildRef(0)
			if err == nil && childRef.Kind == page.ChildRefPage {
				return childRef.Page, nil
			}
		}
	}

	return currID, nil
}

func vacuumClonePagerTreeWithLeafRefs(oldPager *pager.Pager, rootID uint64, alloc interface {
	Alloc(hint uint64) (uint64, error)
}, newPager *pager.Pager, internalBaseDelta bool) (uint64, error) {
	if oldPager == nil {
		return 0, errors.New("vacuum: missing source pager")
	}
	if newPager == nil {
		return 0, errors.New("vacuum: missing destination pager")
	}
	c := &vacuumCloneCtx{
		oldPager:          oldPager,
		newPager:          newPager,
		internalBaseDelta: internalBaseDelta,
		alloc:             alloc,
		remap:             make(map[uint64]uint64, 1024),
	}
	return c.cloneNode(rootID)
}

func (c *vacuumCloneCtx) cloneNode(oldID uint64) (uint64, error) {
	if newID, ok := c.remap[oldID]; ok {
		return newID, nil
	}
	if c.oldPager == nil || c.newPager == nil || c.alloc == nil {
		return 0, errors.New("vacuum: clone missing pager/allocator")
	}

	data, err := c.oldPager.Get(oldID)
	if err != nil {
		return 0, err
	}
	n := node.NewNode(data)

	switch n.Type() {
	case page.PageTypeInternal:
		newID, err := c.alloc.Alloc(oldID)
		if err != nil {
			return 0, err
		}
		c.remap[oldID] = newID

		buf := make([]byte, page.PageSize)
		b := node.NewBuilderWithOptions(buf, page.PageTypeInternal, node.BuilderOptions{
			InternalBaseDelta: c.internalBaseDelta || n.InternalBaseDeltaEnabled(),
		})
		b.SetPageID(newID)
		if low, high, ok, err := n.InternalFenceBounds(); err != nil {
			return 0, err
		} else if ok {
			b.SetInternalFenceBounds(low, high)
		}

		count := n.Count()
		for i := uint16(0); i < count; i++ {
			keyView, childRef, err := n.GetInternalEntryRefView(i)
			if err != nil {
				return 0, err
			}
			if childRef.Kind == page.ChildRefPage {
				childNew, err := c.cloneNode(childRef.Page)
				if err != nil {
					return 0, err
				}
				childRef = page.PageChildRef(childNew)
			}
			if err := b.AddInternalChildRef(keyView, childRef); err != nil {
				return 0, err
			}
		}

		out := b.Finish()
		if err := c.newPager.Write(newID, out.Data()); err != nil {
			return 0, err
		}
		return newID, nil

	case page.PageTypeLeaf:
		newID, err := c.alloc.Alloc(oldID)
		if err != nil {
			return 0, err
		}
		c.remap[oldID] = newID

		buf := make([]byte, page.PageSize)
		copy(buf, data)
		out := node.NewNode(buf)
		out.SetPageID(newID)
		out.UpdateChecksum()
		if err := c.newPager.Write(newID, out.Data()); err != nil {
			return 0, err
		}
		return newID, nil

	default:
		return 0, fmt.Errorf("vacuum: unexpected page type %d at page %d", n.Type(), oldID)
	}
}
