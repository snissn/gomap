package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/TreeDB/zipper"
)

// IndexSwapCompactionOptions configures compaction that rebuilds the index into
// a second file and swaps it in atomically.
type IndexSwapCompactionOptions struct {
	// CopyBytesPerSec limits slab copy IO. 0 disables throttling.
	CopyBytesPerSec int64
	// CopyBurstBytes is the limiter burst size. 0 uses a 1-second burst.
	CopyBurstBytes int64

	// Assist is an optional hook invoked periodically during compaction work.
	// It must be fast and must not assume any DB locks are held.
	Assist func()

	// Stats captures timing and byte counters for the compaction run.
	Stats *IndexSwapCompactionStats
}

// IndexSwapCompactionStats summarizes compaction work for observability.
type IndexSwapCompactionStats struct {
	TotalNanos    uint64
	BuildNanos    uint64
	CatchupNanos  uint64
	FinalizeNanos uint64

	RemapCount uint64
	RemapBytes uint64

	SlabWriteBytes int
	SlabDeadBytes  int
}

// CompactSlabsIndexSwap compacts one or more slab files by rebuilding the user
// index into a new file and swapping it in with a short writer pause.
//
// This avoids large COW B-Tree churn during compaction pointer updates by
// materializing the post-compaction pointer view directly in index.db.new.
//
// Behavior notes:
//   - The active slab cannot be compacted.
//   - Missing slab IDs are ignored (idempotent).
//   - Any concurrent writes are replayed into the new index, but only if the key
//     actually changed since the base snapshot (to avoid undoing pointer remaps).
func (db *DB) CompactSlabsIndexSwap(ctx context.Context, slabIDs []uint32, opts IndexSwapCompactionOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if runtime.GOOS == "windows" {
		return ErrVacuumUnsupported
	}
	if !db.vacuumInProgress.CompareAndSwap(false, true) {
		return ErrVacuumInProgress
	}
	defer db.vacuumInProgress.Store(false)

	if db.dir == "" {
		return errors.New("compaction: missing db dir")
	}

	stats := opts.Stats
	var totalStart time.Time
	if stats != nil {
		totalStart = time.Now()
		defer func() {
			stats.TotalNanos = uint64(time.Since(totalStart))
		}()
	}

	activeID := db.slabManager.ActiveSlabID()
	targets := make(map[uint32]struct{}, len(slabIDs))
	for _, id := range slabIDs {
		if id == activeID {
			return fmt.Errorf("compaction: cannot compact active slab %d", id)
		}
		if _, ok := targets[id]; ok {
			continue
		}
		if _, err := os.Stat(db.slabManager.GetSlabPath(id)); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		targets[id] = struct{}{}
	}
	if len(targets) == 0 {
		return nil
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
	newZ.SetIndexInternalBaseDelta(db.indexInternalBaseDelta)

	db.vacuum.Start()
	defer db.vacuum.Stop()

	baseSnap := db.AcquireSnapshot()
	defer func() { _ = baseSnap.Close() }()

	lim := newIndexSwapLimiter(opts.CopyBytesPerSec, opts.CopyBurstBytes)
	remapIter := newIndexSwapRemapIterator(ctx, baseSnap.tree.Iterator(nil, nil), db, targets, lim, opts.Assist)
	buildStart := time.Now()
	newRoot, buildErr := bulk.BuildWithOptions(remapIter, newAlloc, newPager, bulk.BuildOptions{
		LeafPrefixCompression: db.leafPrefixCompression,
		LeafColumnar:          db.indexColumnarLeaves,
		InternalBaseDelta:     db.indexInternalBaseDelta,
	})
	if stats != nil {
		stats.BuildNanos = uint64(time.Since(buildStart))
		stats.RemapCount = remapIter.remapCount
		stats.RemapBytes = remapIter.remapBytes
		stats.SlabWriteBytes = remapIter.metrics.SlabWriteBytes
		stats.SlabDeadBytes = remapIter.metrics.SlabDeadBytes
	}
	iterErr := remapIter.Error()
	_ = remapIter.Close()
	if buildErr != nil || iterErr != nil {
		cleanupNewPager()
		return errors.Join(buildErr, iterErr)
	}

	metrics := remapIter.metrics
	remap := remapIter.remap
	adjusted := make(map[page.ValuePtr]struct{}, 128)

	freeRetired := func(retired []uint64) {
		for _, id := range retired {
			_ = newAlloc.Free(id)
		}
	}

	// Online catch-up: replay recorded keys in bounded passes. Filter out keys
	// whose state did not change since the base snapshot so we never override a
	// slab-pointer remap with the old pointer value.
	catchupStart := time.Now()
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
		newRoot, retired, err = db.applyIndexSwapDelta(newRoot, opsMap, newZ, baseSnap, targets, remap, adjusted, &metrics)
		if err != nil {
			cleanupNewPager()
			return err
		}
		freeRetired(retired)
		if len(opsMap) <= vacuumCatchupKeyTarget {
			break
		}
	}
	if stats != nil {
		stats.CatchupNanos = uint64(time.Since(catchupStart))
	}

	// Final cutover: stop recording, apply the tail, rebuild the System tree in
	// the new file, then swap index.db on disk and publish a new index generation.
	finalizeStart := time.Now()
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
			newRoot, retired, err = db.applyIndexSwapDelta(newRoot, finalOps, newZ, baseSnap, targets, remap, adjusted, &metrics)
			if err != nil {
				cleanupNewPager()
				return err
			}
			freeRetired(retired)
			continue
		}

		if len(finalOps) > 0 {
			var retired []uint64
			newRoot, retired, err = db.applyIndexSwapDelta(newRoot, finalOps, newZ, baseSnap, targets, remap, adjusted, &metrics)
			if err != nil {
				db.writeMu.Unlock()
				cleanupNewPager()
				return err
			}
			freeRetired(retired)
		}

		db.mu.RLock()
		oldGen := db.idx.Load()
		state := db.state.Load()
		baseMeta := db.meta
		db.mu.RUnlock()
		if oldGen == nil || state == nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return errors.New("compaction: missing db state")
		}

		sysIter := tree.New(oldGen.pager, valueReader{slabs: state.SlabSet, vlogs: state.ValueLogSet}, state.SystemRootPageID).Iterator(nil, nil)
		newSysRoot, err := bulk.BuildWithOptions(sysIter, newAlloc, newPager, bulk.BuildOptions{
			LeafPrefixCompression: db.leafPrefixCompression,
			LeafColumnar:          db.indexColumnarLeaves,
			InternalBaseDelta:     db.indexInternalBaseDelta,
		})
		_ = sysIter.Close()
		if err != nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		}

		if nextSysRoot, sysRetired, err := applySystemStatsUpdatesOnPager(newSysRoot, metrics, newZ, newPager, db); err != nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		} else if nextSysRoot != newSysRoot || len(sysRetired) > 0 {
			newSysRoot = nextSysRoot
			freeRetired(sysRetired)
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

		// Ensure slab tail referenced by meta is durable before publishing the
		// new index. Index swap compaction is treated as a durability boundary.
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
		if err := syncDirFn(db.dir); err != nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		}

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
		_ = syncDirFn(db.dir)

		// Publish the new index generation (old readers keep oldGen pinned).
		newGen := newIndexGen(db.nextIndexID(), newPager, newAlloc, newZ)
		db.trackIndex(newGen)

		// Mark compacted slabs as zombies so future snapshots stop pinning them.
		for id := range targets {
			if err := db.slabManager.MarkZombie(id); err != nil {
				// Best effort: if this fails, keep the slab pinned; the index no
				// longer references it so safety holds, but space reclamation will
				// be delayed.
				_ = err
			}
		}

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
		})
		db.mu.Unlock()

		db.writeMu.Unlock()

		if oldState != nil {
			_ = db.slabManager.ReleaseSlabs(oldState.SlabSet)
			_ = db.valueLogManager.Release(oldState.ValueLogSet)
		}

		db.releaseIndex(oldGen)
		if stats != nil {
			stats.FinalizeNanos = uint64(time.Since(finalizeStart))
		}
		return nil
	}
}

// indexSwapLimiter is a minimal token-bucket limiter for slab copy IO.
type indexSwapLimiter struct {
	rate  int64
	burst int64

	tokens   float64
	lastTime time.Time
}

func newIndexSwapLimiter(bytesPerSec int64, burstBytes int64) *indexSwapLimiter {
	if bytesPerSec <= 0 {
		return &indexSwapLimiter{rate: 0}
	}
	if burstBytes <= 0 {
		burstBytes = bytesPerSec
	}
	return &indexSwapLimiter{
		rate:     bytesPerSec,
		burst:    burstBytes,
		tokens:   float64(burstBytes),
		lastTime: time.Now(),
	}
}

func (l *indexSwapLimiter) Wait(ctx context.Context, n int) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if l.rate <= 0 || n <= 0 {
		return nil
	}

	now := time.Now()
	elapsed := now.Sub(l.lastTime).Seconds()
	l.lastTime = now

	l.tokens += elapsed * float64(l.rate)
	if l.tokens > float64(l.burst) {
		l.tokens = float64(l.burst)
	}

	need := float64(n)
	if l.tokens >= need {
		l.tokens -= need
		return nil
	}

	deficit := need - l.tokens
	sleep := time.Duration(deficit / float64(l.rate) * float64(time.Second)) // ceil-ish not needed
	if sleep > 0 {
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	l.tokens = 0
	return nil
}

type indexSwapRemapIterator struct {
	ctx     context.Context
	under   iterator.UnsafeIterator
	db      *DB
	targets map[uint32]struct{}
	lim     *indexSwapLimiter
	assist  func()

	metrics adaptive.Metrics
	remap   map[page.ValuePtr]page.ValuePtr
	err     error

	remapCount uint64
	remapBytes uint64

	lastAssist       time.Time
	bytesSinceAssist int64
}

func newIndexSwapRemapIterator(ctx context.Context, under iterator.UnsafeIterator, db *DB, targets map[uint32]struct{}, lim *indexSwapLimiter, assist func()) *indexSwapRemapIterator {
	it := &indexSwapRemapIterator{
		ctx:     ctx,
		under:   under,
		db:      db,
		targets: targets,
		lim:     lim,
		assist:  assist,
	}
	it.lastAssist = time.Now()
	return it
}

func (it *indexSwapRemapIterator) maybeAssist(force bool) {
	if it.assist == nil {
		return
	}
	if !force {
		const assistEveryBytes = 4 << 20
		const assistEveryDur = 250 * time.Millisecond
		if it.bytesSinceAssist < assistEveryBytes && time.Since(it.lastAssist) < assistEveryDur {
			return
		}
	}
	it.assist()
	it.lastAssist = time.Now()
	it.bytesSinceAssist = 0
}

func (it *indexSwapRemapIterator) Valid() bool {
	return it.err == nil && it.under.Valid()
}

func (it *indexSwapRemapIterator) Next() {
	it.under.Next()
}

func (it *indexSwapRemapIterator) Seek(key []byte) {
	it.under.Seek(key)
}

func (it *indexSwapRemapIterator) UnsafeKey() []byte {
	return it.under.UnsafeKey()
}

func (it *indexSwapRemapIterator) UnsafeValue() []byte {
	return it.under.UnsafeValue()
}

func (it *indexSwapRemapIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	val, ptr, flags := it.under.UnsafeEntry()
	if it.err != nil {
		return nil, page.ValuePtr{}, 0
	}

	if flags&node.FlagPointer == 0 {
		return val, ptr, flags
	}

	// Ensure builders never inline pointer values, even if we loaded them.
	val = nil

	if _, ok := it.targets[ptr.FileID]; !ok {
		return nil, ptr, flags
	}

	select {
	case <-it.ctx.Done():
		it.err = it.ctx.Err()
		return nil, page.ValuePtr{}, 0
	default:
	}

	// Read the pointed-to value bytes from the base snapshot and append the record
	// to the current active slab.
	key := it.under.UnsafeKey()
	value := it.under.UnsafeValue()

	recordBytes := int(page.ValuePtrRecordLength(ptr)) + 4 // include CRC
	if err := it.lim.Wait(it.ctx, recordBytes); err != nil {
		it.err = err
		return nil, page.ValuePtr{}, 0
	}
	it.bytesSinceAssist += int64(recordBytes)
	it.maybeAssist(false)

	newPtr, err := it.db.slabManager.Append(key, value)
	if err != nil {
		it.err = err
		return nil, page.ValuePtr{}, 0
	}

	it.remapCount++
	it.remapBytes += uint64(recordBytes)

	if it.remap == nil {
		it.remap = make(map[page.ValuePtr]page.ValuePtr, 1024)
	}
	it.remap[ptr] = newPtr

	if it.metrics.SlabWriteBytesByFile == nil {
		it.metrics.SlabWriteBytesByFile = make(map[uint32]int64, 4)
	}
	it.metrics.SlabWriteBytesByFile[newPtr.FileID] += int64(page.ValuePtrRecordLength(newPtr))

	if it.metrics.SlabDeadBytesByFile == nil {
		it.metrics.SlabDeadBytesByFile = make(map[uint32]int64, 4)
	}
	it.metrics.SlabDeadBytesByFile[ptr.FileID] += int64(page.ValuePtrRecordLength(ptr))

	it.metrics.SlabWriteBytes += int(page.ValuePtrRecordLength(newPtr))
	it.metrics.SlabDeadBytes += int(page.ValuePtrRecordLength(ptr))

	return nil, newPtr, flags
}

func (it *indexSwapRemapIterator) Key() []byte   { return it.under.Key() }
func (it *indexSwapRemapIterator) Value() []byte { return it.under.Value() }
func (it *indexSwapRemapIterator) KeyCopy(dst []byte) []byte {
	return it.under.KeyCopy(dst)
}
func (it *indexSwapRemapIterator) ValueCopy(dst []byte) []byte {
	return it.under.ValueCopy(dst)
}
func (it *indexSwapRemapIterator) IsDeleted() bool { return it.under.IsDeleted() }
func (it *indexSwapRemapIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.under.Error()
}
func (it *indexSwapRemapIterator) Close() error { return it.under.Close() }
func (it *indexSwapRemapIterator) Domain() (start, end []byte) {
	return it.under.Domain()
}

func entriesEquivalent(base node.LeafEntry, baseErr error, curr node.LeafEntry, currErr error) bool {
	baseMissing := baseErr != nil
	currMissing := currErr != nil
	if baseMissing && currMissing {
		return true
	}

	// Treat tombstones as missing; iteration never includes them.
	if !baseMissing && base.Flags&node.FlagTombstone != 0 {
		baseMissing = true
	}
	if !currMissing && curr.Flags&node.FlagTombstone != 0 {
		currMissing = true
	}
	if baseMissing || currMissing {
		return baseMissing && currMissing
	}

	basePtr := base.Flags&node.FlagPointer != 0
	currPtr := curr.Flags&node.FlagPointer != 0
	if basePtr != currPtr {
		return false
	}
	if basePtr {
		return base.ValuePtr == curr.ValuePtr
	}
	return bytes.Equal(base.Value, curr.Value)
}

func recordedEquivalentToBase(base node.LeafEntry, baseErr error, rec batch.Entry) bool {
	baseMissing := baseErr != nil
	if !baseMissing && base.Flags&node.FlagTombstone != 0 {
		baseMissing = true
	}

	recMissing := rec.Type == batch.OpDelete
	if baseMissing || recMissing {
		return baseMissing && recMissing
	}

	basePtr := base.Flags&node.FlagPointer != 0
	if basePtr != rec.IsPtr {
		return false
	}
	if basePtr {
		return base.ValuePtr == rec.ValuePtr
	}
	return bytes.Equal(base.Value, rec.Value)
}

func (db *DB) applyIndexSwapDelta(root uint64, opsMap map[string]batch.Entry, z *zipper.Zipper, baseSnap *Snapshot, targets map[uint32]struct{}, remap map[page.ValuePtr]page.ValuePtr, adjusted map[page.ValuePtr]struct{}, metrics *adaptive.Metrics) (uint64, []uint64, error) {
	if len(opsMap) == 0 {
		return root, nil, nil
	}
	if baseSnap == nil || baseSnap.idx == nil || baseSnap.state == nil {
		return 0, nil, errors.New("compaction: missing base snapshot")
	}

	ops := make([]batch.Entry, 0, vacuumDeltaBatchSize)
	var retired []uint64

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

	adjustForBasePtr := func(ptr page.ValuePtr) {
		if metrics == nil || remap == nil {
			return
		}
		if _, ok := adjusted[ptr]; ok {
			return
		}
		newPtr, ok := remap[ptr]
		if !ok {
			return
		}

		if metrics.SlabWriteBytesByFile != nil {
			if cur := metrics.SlabWriteBytesByFile[newPtr.FileID]; cur > 0 {
				cur -= int64(page.ValuePtrRecordLength(newPtr))
				if cur <= 0 {
					delete(metrics.SlabWriteBytesByFile, newPtr.FileID)
				} else {
					metrics.SlabWriteBytesByFile[newPtr.FileID] = cur
				}
			}
		}
		if metrics.SlabDeadBytesByFile != nil {
			if cur := metrics.SlabDeadBytesByFile[ptr.FileID]; cur > 0 {
				cur -= int64(page.ValuePtrRecordLength(ptr))
				if cur <= 0 {
					delete(metrics.SlabDeadBytesByFile, ptr.FileID)
				} else {
					metrics.SlabDeadBytesByFile[ptr.FileID] = cur
				}
			}
		}
		if metrics.SlabWriteBytes > 0 {
			metrics.SlabWriteBytes -= int(page.ValuePtrRecordLength(newPtr))
			if metrics.SlabWriteBytes < 0 {
				metrics.SlabWriteBytes = 0
			}
		}
		if metrics.SlabDeadBytes > 0 {
			metrics.SlabDeadBytes -= int(page.ValuePtrRecordLength(ptr))
			if metrics.SlabDeadBytes < 0 {
				metrics.SlabDeadBytes = 0
			}
		}
		adjusted[ptr] = struct{}{}
	}

	keys := make([]string, 0, len(opsMap))
	for k := range opsMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		rec := opsMap[key]
		if len(rec.Key) == 0 {
			rec.Key = []byte(key)
		}

		baseEntry, baseErr := baseSnap.tree.GetEntry(rec.Key)
		if baseErr != nil && !errors.Is(baseErr, tree.ErrKeyNotFound) {
			return 0, nil, baseErr
		}

		if recordedEquivalentToBase(baseEntry, baseErr, rec) {
			continue
		}

		// If the base snapshot pointed at a to-be-compacted slab, this key's
		// remap should not be counted anymore (it was overwritten/deleted).
		if baseErr == nil && baseEntry.Flags&node.FlagPointer != 0 {
			if _, ok := targets[baseEntry.ValuePtr.FileID]; ok {
				adjustForBasePtr(baseEntry.ValuePtr)
			}
		}

		ops = append(ops, rec)

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

func applySystemStatsUpdatesOnPager(sysRootID uint64, metrics adaptive.Metrics, z *zipper.Zipper, p *pager.Pager, db *DB) (uint64, []uint64, error) {
	if len(metrics.SlabWriteBytesByFile) == 0 && len(metrics.SlabDeadBytesByFile) == 0 {
		return sysRootID, nil, nil
	}
	if db == nil || z == nil || p == nil {
		return 0, nil, errors.New("missing dependencies")
	}

	// Determine which slab IDs are touched this commit.
	ids := make([]uint32, 0, len(metrics.SlabWriteBytesByFile)+len(metrics.SlabDeadBytesByFile))
	seen := make(map[uint32]struct{}, len(metrics.SlabWriteBytesByFile)+len(metrics.SlabDeadBytesByFile))
	for id := range metrics.SlabWriteBytesByFile {
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	for id := range metrics.SlabDeadBytesByFile {
		if _, ok := seen[id]; ok {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	sysTree := tree.New(p, valueReader{slabs: db.slabManager, vlogs: db.valueLogManager}, sysRootID)
	sysBatch := batch.New(db.slabManager, page.DefaultInlineThreshold)
	defer func() { _ = sysBatch.Close() }()

	for _, id := range ids {
		key := slabStatsKey(id)

		var dead, total uint64
		if raw, err := sysTree.Get(key); err == nil && raw != nil {
			d, t, err := decodeSlabStatsValue(raw)
			if err == nil {
				dead, total = d, t
			}
		}

		if delta := metrics.SlabWriteBytesByFile[id]; delta > 0 {
			total += uint64(delta)
		}
		if delta := metrics.SlabDeadBytesByFile[id]; delta > 0 {
			dead += uint64(delta)
		}
		if dead > total {
			dead = total
		}

		if err := sysBatch.Set(key, encodeSlabStatsValue(dead, total)); err != nil {
			return 0, nil, err
		}
	}

	newSysRoot, sysRetired, _, err := z.Apply(sysRootID, sysBatch)
	if err != nil {
		return 0, nil, err
	}
	return newSysRoot, sysRetired, nil
}
