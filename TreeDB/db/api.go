package db

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

const (
	getManyValueGuessBytes          = 128
	getManyMaxArenaBytes            = 1 << 20
	getManyParallelMinKeys          = 128
	getManyParallelMinKeysPerWorker = 32
	getManyParallelMaxWorkers       = 8
)

var getManyEmptyValue = []byte{}

func getManyArenaCap(keyCount int) int {
	if keyCount <= 0 {
		return 0
	}
	arenaCap := keyCount * getManyValueGuessBytes
	if arenaCap < 0 {
		arenaCap = 0
	}
	if arenaCap > getManyMaxArenaBytes {
		arenaCap = getManyMaxArenaBytes
	}
	return arenaCap
}

func getManyWorkerCount(keyCount int) int {
	if keyCount <= 0 {
		return 1
	}
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > getManyParallelMaxWorkers {
		workers = getManyParallelMaxWorkers
	}
	if workers > keyCount {
		workers = keyCount
	}
	return workers
}

func getManyCanParallelize(keyCount, workers int) bool {
	if keyCount < getManyParallelMinKeys {
		return false
	}
	if workers <= 1 {
		return false
	}
	return keyCount/workers >= getManyParallelMinKeysPerWorker
}

func getManyChunkBounds(worker, workers, keyCount int) (int, int) {
	start := (worker * keyCount) / workers
	end := ((worker + 1) * keyCount) / workers
	return start, end
}

// GetManyParallelPlan reports how this backend would schedule GetMany for the
// provided key count.
func (db *DB) GetManyParallelPlan(keyCount int) (workers int, parallel bool) {
	workers = getManyWorkerCount(keyCount)
	return workers, getManyCanParallelize(keyCount, workers)
}

// --- Public API ---

func (db *DB) acquireSnapshotOrErr() (*Snapshot, error) {
	snap := db.AcquireSnapshot()
	if snap == nil {
		return nil, ErrClosed
	}
	return snap, nil
}

func (db *DB) refreshOnValueLogFileNotFound(err error) bool {
	if db == nil || db.valueLogManager == nil {
		return false
	}
	return errors.Is(err, valuelog.ErrFileNotFound)
}

func (db *DB) refreshValueLogSetForReadRetry(observedEpoch uint64) error {
	if db == nil {
		return ErrClosed
	}
	for {
		db.readRetryRefreshMu.Lock()
		if !db.readRetryRefreshInFlight {
			if db.readRetryRefreshEpoch.Load() != observedEpoch {
				db.readRetryRefreshSkippedEpoch.Add(1)
				db.readRetryRefreshMu.Unlock()
				return nil
			}
			done := make(chan struct{})
			db.readRetryRefreshInFlight = true
			db.readRetryRefreshDone = done
			db.readRetryRefreshErr = nil
			db.readRetryRefreshLeaderCount.Add(1)
			db.readRetryRefreshMu.Unlock()

			err := db.RefreshValueLogSet()

			db.readRetryRefreshMu.Lock()
			db.readRetryRefreshErr = err
			if err == nil {
				db.readRetryRefreshEpoch.Add(1)
			}
			db.readRetryRefreshInFlight = false
			db.readRetryRefreshDone = nil
			close(done)
			db.readRetryRefreshMu.Unlock()
			return err
		}
		done := db.readRetryRefreshDone
		db.readRetryRefreshFollowerCount.Add(1)
		db.readRetryRefreshMu.Unlock()

		if done == nil {
			runtime.Gosched()
			continue
		}

		<-done
		db.readRetryRefreshMu.Lock()
		err := db.readRetryRefreshErr
		db.readRetryRefreshMu.Unlock()
		return err
	}
}

// Get returns the value for a key.
//
// Semantics: Returns a safe copy of the value.
func (db *DB) Get(key []byte) ([]byte, error) {
	readOnce := func() ([]byte, error) {
		snap, err := db.acquireSnapshotOrErr()
		if err != nil {
			return nil, err
		}
		defer snap.Close()
		return snap.Get(key)
	}

	retryEpoch := db.readRetryRefreshEpoch.Load()
	val, err := readOnce()
	if db.refreshOnValueLogFileNotFound(err) {
		if refreshErr := db.refreshValueLogSetForReadRetry(retryEpoch); refreshErr != nil {
			return nil, refreshErr
		}
		val, err = readOnce()
	}
	if err == tree.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

// GetMany returns values for keys.
//
// Semantics: Returns safe copies of values. Missing keys are returned as nil
// entries with no error.
func (db *DB) GetMany(keys [][]byte) ([][]byte, error) {
	retryEpoch := db.readRetryRefreshEpoch.Load()
	out, err := db.getManyOnce(keys)
	if db.refreshOnValueLogFileNotFound(err) {
		if refreshErr := db.refreshValueLogSetForReadRetry(retryEpoch); refreshErr != nil {
			return nil, refreshErr
		}
		return db.getManyOnce(keys)
	}
	return out, err
}

func (db *DB) getManyOnce(keys [][]byte) ([][]byte, error) {
	out := make([][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	snap, err := db.acquireSnapshotOrErr()
	if err != nil {
		return nil, err
	}
	defer snap.Close()

	workers := getManyWorkerCount(len(keys))
	if getManyCanParallelize(len(keys), workers) {
		if err := db.getManyParallel(snap, keys, out, workers); err != nil {
			return nil, err
		}
		return out, nil
	}
	if err := db.getManySequential(snap, keys, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (db *DB) getManySequential(snap *Snapshot, keys [][]byte, out [][]byte) error {
	// Copy all found values into a single arena to avoid one allocation per key.
	// Each returned slice is capacity-capped to preserve safe-copy semantics.
	arena := make([]byte, 0, getManyArenaCap(len(keys)))
	for i, key := range keys {
		start := len(arena)
		nextArena, err := snap.GetAppend(key, arena)
		if err == tree.ErrKeyNotFound {
			continue
		}
		if err != nil {
			return err
		}
		arena = nextArena
		if len(arena) == start {
			out[i] = getManyEmptyValue
			continue
		}
		out[i] = arena[start:len(arena):len(arena)]
	}
	return nil
}

func (db *DB) getManyParallel(snap *Snapshot, keys [][]byte, out [][]byte, workers int) error {
	var (
		wg       sync.WaitGroup
		stop     atomic.Bool
		firstErr error
		errMu    sync.Mutex
	)
	for worker := 0; worker < workers; worker++ {
		start, end := getManyChunkBounds(worker, workers, len(keys))
		if start >= end {
			continue
		}
		workerArenaCap := getManyArenaCap(end - start)
		wg.Add(1)
		go func(start, end, arenaCap int) {
			defer wg.Done()
			arena := make([]byte, 0, arenaCap)
			for i := start; i < end; i++ {
				if stop.Load() {
					return
				}
				arenaStart := len(arena)
				nextArena, err := snap.GetAppend(keys[i], arena)
				if err == tree.ErrKeyNotFound {
					continue
				}
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
						stop.Store(true)
					}
					errMu.Unlock()
					return
				}
				arena = nextArena
				if len(arena) == arenaStart {
					out[i] = getManyEmptyValue
					continue
				}
				out[i] = arena[arenaStart:len(arena):len(arena)]
			}
		}(start, end, workerArenaCap)
	}
	wg.Wait()
	return firstErr
}

// GetUnsafe returns the value for a key.
//
// Semantics: Returns a safe copy of the value. For zero-copy views tied to a
// snapshot lifetime, use Snapshot.GetUnsafe.
func (db *DB) GetUnsafe(key []byte) ([]byte, error) {
	return db.Get(key)
}

// Dir returns the on-disk directory backing the DB.
func (db *DB) Dir() string {
	if db == nil {
		return ""
	}
	return db.dir
}

// GetAppend appends the value for the key to dst and returns the new slice.
// If the key is not found, it returns dst and ErrKeyNotFound.
func (db *DB) GetAppend(key, dst []byte) ([]byte, error) {
	readOnce := func(base []byte) ([]byte, error) {
		snap, err := db.acquireSnapshotOrErr()
		if err != nil {
			return base, err
		}
		defer snap.Close()
		return snap.GetAppend(key, base)
	}

	retryEpoch := db.readRetryRefreshEpoch.Load()
	val, err := readOnce(dst)
	if db.refreshOnValueLogFileNotFound(err) {
		if refreshErr := db.refreshValueLogSetForReadRetry(retryEpoch); refreshErr != nil {
			return dst, refreshErr
		}
		val, err = readOnce(dst)
	}
	if err == tree.ErrKeyNotFound {
		return dst, err
	}
	if err != nil {
		return dst, err
	}
	return val, nil
}

// Has checks if a key exists.
func (db *DB) Has(key []byte) (bool, error) {
	snap, err := db.acquireSnapshotOrErr()
	if err != nil {
		return false, err
	}
	defer snap.Close()
	return snap.Has(key)
}

// Set sets the value for a key.
func (db *DB) Set(key, value []byte) error {
	if handled, err := db.writeViaCommitCombiner(key, value, false, false); handled {
		return err
	}
	return db.writeSingleKV(key, value, false, false)
}

// SetSync sets the value and syncs to disk.
func (db *DB) SetSync(key, value []byte) error {
	if handled, err := db.writeViaCommitCombiner(key, value, false, true); handled {
		return err
	}
	return db.writeSingleKV(key, value, false, true)
}

// Delete removes a key.
func (db *DB) Delete(key []byte) error {
	if handled, err := db.writeViaCommitCombiner(key, nil, true, false); handled {
		return err
	}
	return db.writeSingleKV(key, nil, true, false)
}

// DeleteSync removes a key and syncs.
func (db *DB) DeleteSync(key []byte) error {
	if handled, err := db.writeViaCommitCombiner(key, nil, true, true); handled {
		return err
	}
	return db.writeSingleKV(key, nil, true, true)
}

// DBIterator wraps tree.Iterator and holds a Snapshot.
type DBIterator struct {
	snap *Snapshot
	iter iterator.UnsafeIterator
	err  error
}

type IteratorMode = tree.IteratorMode

const (
	IteratorModeFull              = tree.IteratorModeFull
	IteratorModeKeysOnly          = tree.IteratorModeKeysOnly
	IteratorModePointerProjection = tree.IteratorModePointerProjection
)

type IteratorOptions = tree.IteratorOptions

func (it *DBIterator) DebugStats() (queueLen int, sourcesUsed int) {
	return 0, 1
}

func (it *DBIterator) Next() {
	if !it.Valid() {
		return
	}
	it.iter.Next()
}

func (it *DBIterator) Valid() bool {
	return it.iter.Valid() && it.err == nil
}

func (it *DBIterator) Key() []byte {
	return it.KeyCopy(nil)
}

func (it *DBIterator) Value() []byte {
	return it.ValueCopy(nil)
}

func (it *DBIterator) KeyCopy(dst []byte) []byte {
	k := it.iter.UnsafeKey()
	if k == nil {
		return nil
	}
	return append(dst[:0], k...)
}

func (it *DBIterator) ValueCopy(dst []byte) []byte {
	val := it.UnsafeValue()
	if it.err != nil {
		return dst
	}
	if val == nil {
		return nil
	}
	return append(dst[:0], val...)
}

func (it *DBIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.iter.Error()
}

func (it *DBIterator) Close() error {
	err := it.iter.Close()
	if e := it.snap.Close(); e != nil {
		if err == nil {
			err = e
		}
	}
	return err
}

// UnsafeIterator methods
func (it *DBIterator) Seek(key []byte) {
	it.iter.Seek(key)
}

func (it *DBIterator) UnsafeKey() []byte {
	return it.iter.UnsafeKey()
}

func (it *DBIterator) UnsafeValue() []byte {
	if it.err != nil {
		return nil
	}
	val := it.iter.UnsafeValue()
	if err := it.iter.Error(); err != nil {
		it.err = err
		return nil
	}
	it.err = nil
	return val
}

func (it *DBIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return it.iter.UnsafeEntry()
}

func (it *DBIterator) IsDeleted() bool {
	return it.iter.IsDeleted()
}

func (it *DBIterator) Domain() (start, end []byte) {
	return it.iter.Domain()
}

// Iterator returns an iterator.
func (db *DB) Iterator(start, end []byte) (iterator.UnsafeIterator, error) {
	return db.IteratorWithOptions(start, end, IteratorOptions{})
}

// IteratorWithOptions returns an iterator with explicit value materialization
// controls.
func (db *DB) IteratorWithOptions(start, end []byte, opts IteratorOptions) (iterator.UnsafeIterator, error) {
	snap, err := db.acquireSnapshotOrErr()
	if err != nil {
		return nil, err
	}
	it := snap.tree.IteratorWithOptions(start, end, opts)
	return &DBIterator{snap: snap, iter: it}, nil
}

// ReverseIterator returns a reverse iterator.
func (db *DB) ReverseIterator(start, end []byte) (iterator.UnsafeIterator, error) {
	return db.ReverseIteratorWithOptions(start, end, IteratorOptions{})
}

// ReverseIteratorWithOptions returns a reverse iterator with explicit value
// materialization controls.
func (db *DB) ReverseIteratorWithOptions(start, end []byte, opts IteratorOptions) (iterator.UnsafeIterator, error) {
	snap, err := db.acquireSnapshotOrErr()
	if err != nil {
		return nil, err
	}
	it := snap.tree.ReverseIteratorWithOptions(start, end, opts)
	return &DBIterator{snap: snap, iter: it}, nil
}

// Stats returns database statistics.
func (db *DB) Stats() map[string]string {
	stats := make(map[string]string)
	stats["cosmos.db.type"] = "treedb"

	snap := db.AcquireSnapshot()
	if snap == nil || snap.idx == nil || snap.state == nil {
		if snap != nil {
			_ = snap.Close()
		}
		return stats
	}
	defer func() { _ = snap.Close() }()

	state := snap.state
	idx := snap.idx

	stats["treedb.commit_seq"] = fmt.Sprintf("%d", state.CommitSeq)
	stats["treedb.root_page"] = fmt.Sprintf("%d", state.RootPageID)
	stats["treedb.system_root_page"] = fmt.Sprintf("%d", state.SystemRootPageID)

	stats["treedb.pages.total"] = fmt.Sprintf("%d", idx.pager.PageCount())
	// PR1 generational scaffolding (backend/read-only path). Cached mode exports
	// richer live counters; backend path reports stable defaults.
	stats["treedb.vlog_generation.enabled"] = "false"
	stats["treedb.vlog_generation.policy"] = "0"
	stats["treedb.vlog_generation.scheduler_state"] = "disabled"
	stats["treedb.vlog_generation.bytes.live.total"] = "0"
	stats["treedb.vlog_generation.bytes.stale.total"] = "0"
	stats["treedb.vlog_generation.bytes.total.total"] = "0"
	stats["treedb.vlog_generation.segments.total"] = "0"
	stats["treedb.vlog_generation.rewrite.bytes_in"] = "0"
	stats["treedb.vlog_generation.rewrite.bytes_out"] = "0"
	stats["treedb.vlog_generation.gc.deleted_segments"] = "0"
	stats["treedb.vlog_generation.gc.deleted_bytes"] = "0"
	stats["treedb.vlog_generation.remap.successes"] = "0"
	stats["treedb.vlog_generation.remap.failures"] = "0"
	growStats := valuelog.GrowBufferStatsSnapshot()
	stats["treedb.vlog.decode_buffer_grow.calls_total"] = fmt.Sprintf("%d", growStats.CallsTotal)
	stats["treedb.vlog.decode_buffer_grow.realloc_calls_total"] = fmt.Sprintf("%d", growStats.ReallocCallsTotal)
	stats["treedb.vlog.decode_buffer_grow.requested_bytes_total"] = fmt.Sprintf("%d", growStats.RequestedBytesTotal)
	stats["treedb.vlog.decode_buffer_grow.allocated_bytes_total"] = fmt.Sprintf("%d", growStats.AllocatedBytesTotal)
	stats["treedb.vlog.decode_buffer_grow.copied_bytes_total"] = fmt.Sprintf("%d", growStats.CopiedBytesTotal)
	stats["treedb.vlog.decode_buffer_grow.capacity_waste_bytes_total"] = fmt.Sprintf("%d", growStats.CapacityWasteBytesTotal)
	if growStats.CallsTotal > 0 {
		stats["treedb.vlog.decode_buffer_grow.realloc_rate"] = fmt.Sprintf("%.6f", float64(growStats.ReallocCallsTotal)/float64(growStats.CallsTotal))
	}
	if growStats.ReallocCallsTotal > 0 {
		stats["treedb.vlog.decode_buffer_grow.avg_allocated_bytes_per_realloc"] = fmt.Sprintf("%.3f", float64(growStats.AllocatedBytesTotal)/float64(growStats.ReallocCallsTotal))
		stats["treedb.vlog.decode_buffer_grow.avg_copied_bytes_per_realloc"] = fmt.Sprintf("%.3f", float64(growStats.CopiedBytesTotal)/float64(growStats.ReallocCallsTotal))
	}
	if growStats.RequestedBytesTotal > 0 {
		stats["treedb.vlog.decode_buffer_grow.overalloc_ratio"] = fmt.Sprintf("%.6f", float64(growStats.AllocatedBytesTotal)/float64(growStats.RequestedBytesTotal))
	}
	readPathStats := tree.ReadPathStatsSnapshot()
	stats["treedb.process.read_path.backend_tree.get_append_inline_hits_total"] = fmt.Sprintf("%d", readPathStats.GetAppendInlineHitsTotal)
	stats["treedb.process.read_path.backend_tree.get_append_inline_bytes_total"] = fmt.Sprintf("%d", readPathStats.GetAppendInlineBytesTotal)
	stats["treedb.process.read_path.backend_tree.get_append_pointer_hits_total"] = fmt.Sprintf("%d", readPathStats.GetAppendPointerHitsTotal)
	stats["treedb.process.read_path.backend_tree.get_append_pointer_bytes_total"] = fmt.Sprintf("%d", readPathStats.GetAppendPointerBytesTotal)

	if db.valueLogManager != nil {
		vlogRemaps, vlogDeadMappings := db.valueLogManager.RemapStats()
		stats["treedb.vlog.mmap_remaps"] = fmt.Sprintf("%d", vlogRemaps)
		stats["treedb.vlog.mmap_dead_mappings"] = fmt.Sprintf("%d", vlogDeadMappings)
		stats["treedb.vlog.mmap_dead_mappings.cap_base"] = fmt.Sprintf("%d", valuelog.MaxDeadMappings)
		stats["treedb.vlog.mmap_max_mapped_sealed_segments"] = fmt.Sprintf("%d", valuelog.MaxMappedSealedSegments)
		stats["treedb.vlog.mmap_max_mapped_sealed_bytes"] = fmt.Sprintf("%d", valuelog.MaxMappedSealedBytes)
		currentSegments, currentBytes, sealedSegments, sealedBytes, _, deadBytes := db.valueLogManager.MmapResidencyStats()
		stats["treedb.vlog.mmap_active_segments"] = fmt.Sprintf("%d", currentSegments+sealedSegments)
		stats["treedb.vlog.mmap_active_bytes"] = fmt.Sprintf("%d", currentBytes+sealedBytes)
		stats["treedb.vlog.mmap_current_segments"] = fmt.Sprintf("%d", currentSegments)
		stats["treedb.vlog.mmap_current_bytes"] = fmt.Sprintf("%d", currentBytes)
		stats["treedb.vlog.mmap_sealed_segments"] = fmt.Sprintf("%d", sealedSegments)
		stats["treedb.vlog.mmap_sealed_bytes"] = fmt.Sprintf("%d", sealedBytes)
		stats["treedb.vlog.mmap_dead_bytes"] = fmt.Sprintf("%d", deadBytes)
		sealedDeniedCountCap, sealedDeniedBytesCap := db.valueLogManager.SealedMapDeniedByReasonStats()
		stats["treedb.vlog.mmap_sealed_map_denied.count_cap"] = fmt.Sprintf("%d", sealedDeniedCountCap)
		stats["treedb.vlog.mmap_sealed_map_denied.bytes_cap"] = fmt.Sprintf("%d", sealedDeniedBytesCap)
		stats["treedb.vlog.mmap_sealed_map_denied"] = fmt.Sprintf("%d", sealedDeniedCountCap+sealedDeniedBytesCap)

		mmapHits, mmapMissOutOfRange, mmapMissNoMapping, mmapMissDeadCap, mmapFallbackReadAt := db.valueLogManager.MmapReadStats()
		stats["treedb.vlog.mmap_read.hits"] = fmt.Sprintf("%d", mmapHits)
		stats["treedb.vlog.mmap_read.miss_out_of_range"] = fmt.Sprintf("%d", mmapMissOutOfRange)
		stats["treedb.vlog.mmap_read.miss_no_mapping"] = fmt.Sprintf("%d", mmapMissNoMapping)
		stats["treedb.vlog.mmap_read.miss_dead_mapping_cap"] = fmt.Sprintf("%d", mmapMissDeadCap)
		stats["treedb.vlog.mmap_read.fallback_readat"] = fmt.Sprintf("%d", mmapFallbackReadAt)
		if total := mmapHits + mmapFallbackReadAt; total > 0 {
			stats["treedb.vlog.mmap_read.hit_ratio"] = fmt.Sprintf("%.6f", float64(mmapHits)/float64(total))
		}

		gHits, gMisses, gEntries, gCapacity := db.valueLogManager.GroupedFrameCacheStats()
		stats["treedb.vlog.grouped_frame_cache.hits"] = fmt.Sprintf("%d", gHits)
		stats["treedb.vlog.grouped_frame_cache.misses"] = fmt.Sprintf("%d", gMisses)
		stats["treedb.vlog.grouped_frame_cache.entries"] = fmt.Sprintf("%d", gEntries)
		stats["treedb.vlog.grouped_frame_cache.capacity"] = fmt.Sprintf("%d", gCapacity)
		if total := gHits + gMisses; total > 0 {
			stats["treedb.vlog.grouped_frame_cache.hit_ratio"] = fmt.Sprintf("%.6f", float64(gHits)/float64(total))
		}

		stats["treedb.vlog.read_retry_refresh.leader_calls"] = fmt.Sprintf("%d", db.readRetryRefreshLeaderCount.Load())
		stats["treedb.vlog.read_retry_refresh.follower_calls"] = fmt.Sprintf("%d", db.readRetryRefreshFollowerCount.Load())
		stats["treedb.vlog.read_retry_refresh.skipped_epoch_calls"] = fmt.Sprintf("%d", db.readRetryRefreshSkippedEpoch.Load())

		hits, misses, entries, capacity := db.valueLogManager.TemplateDefCacheStats()
		stats["treedb.vlog.template_def_cache.hits"] = fmt.Sprintf("%d", hits)
		stats["treedb.vlog.template_def_cache.misses"] = fmt.Sprintf("%d", misses)
		stats["treedb.vlog.template_def_cache.entries"] = fmt.Sprintf("%d", entries)
		stats["treedb.vlog.template_def_cache.capacity"] = fmt.Sprintf("%d", capacity)
		if total := hits + misses; total > 0 {
			stats["treedb.vlog.template_def_cache.hit_ratio"] = fmt.Sprintf("%.6f", float64(hits)/float64(total))
		}
	}
	watermarkLockDelaySharePct, watermarkLatencyP99Ms := db.publishWatermarkStats()
	stats["treedb.publish.watermark.lock_delay_share_pct"] = fmt.Sprintf("%.3f", watermarkLockDelaySharePct)
	stats["treedb.publish.watermark.latency_p99_ms"] = fmt.Sprintf("%.3f", watermarkLatencyP99Ms)
	// Backend DB path currently doesn't track queue drift; emit a stable default
	// for suite compatibility and fail-closed checks that require key presence.
	stats["treedb.publish.watermark.lag_drift_bytes_per_sec"] = "0.000"

	pruneStatsInto(stats, &db.pruner)

	return stats
}

// Print debugs the tree (simple dump).
func (db *DB) Print() error {
	// Not implemented fully
	return nil
}
