package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// valueLogKeepRecentSegmentsPerLane bounds how aggressively GC/rewrite may mark
// segments zombie while writers are online. In cached mode, new value-log
// segments can be created/rotated after a protected-path snapshot is taken but
// before reachability is re-evaluated; keeping a small recent window prevents
// deleting freshly rotated segments that may still back in-memory pointers.
const valueLogKeepRecentSegmentsPerLane = 2

// ErrValueLogZombieDeferred reports that MarkValueLogZombie retained the
// requested segment because it is still reachable or otherwise protected.
// Callers should keep their cleanup record and retry after recovery roots or
// other lifecycle pins advance.
var ErrValueLogZombieDeferred = errors.New("treedb: value-log zombie marking deferred")

// valueLogGCPostRefreshCurrentSetNoRefresh is a narrow test seam for the
// second no-refresh read after the fallback Refresh call.
var valueLogGCPostRefreshCurrentSetNoRefresh = func(vm *valuelog.Manager) *valuelog.Set {
	return vm.CurrentSetNoRefresh()
}

var valueLogGCPostScanHook struct {
	mu sync.Mutex
	fn func()
}

func registerValueLogGCPostScanHook(hook func()) func() {
	valueLogGCPostScanHook.mu.Lock()
	prev := valueLogGCPostScanHook.fn
	valueLogGCPostScanHook.fn = hook
	valueLogGCPostScanHook.mu.Unlock()
	return func() {
		valueLogGCPostScanHook.mu.Lock()
		valueLogGCPostScanHook.fn = prev
		valueLogGCPostScanHook.mu.Unlock()
	}
}

func runValueLogGCPostScanHook() {
	valueLogGCPostScanHook.mu.Lock()
	hook := valueLogGCPostScanHook.fn
	valueLogGCPostScanHook.mu.Unlock()
	if hook != nil {
		hook()
	}
}

// ValueLogGCOptions controls value-log garbage collection.
type ValueLogGCOptions struct {
	DryRun bool
	// ProtectedPaths preserves legacy callers that provide a single merged set
	// of protected paths. Prefer the specific ProtectedInUsePaths and
	// ProtectedRetainedPaths fields for blocker classification.
	ProtectedPaths []string
	// ProtectedInUsePaths are paths that may still be referenced by mutable
	// in-memory state during online maintenance.
	ProtectedInUsePaths []string
	// ProtectedRetainedPaths are paths pinned by pointer lifecycle retention.
	ProtectedRetainedPaths []string
	// ObservedSourceFileIDs enables per-classification probe counters for a
	// caller-provided subset of segment IDs (for example, rewrite-selected
	// source segments). IDs not present in the current set are ignored.
	ObservedSourceFileIDs []uint32
	// ObservedSourceAssumeUnreferenced indicates ObservedSourceFileIDs are
	// already known to be unreferenced. When true, ValueLogGC skips the
	// reachability scan and only classifies (and, if !DryRun, zombifies) the
	// observed IDs; it does not attempt to reclaim other segments.
	ObservedSourceAssumeUnreferenced bool
	// ObservedSourceReclaimActive permits observed-only GC to reclaim an
	// otherwise-active segment. Callers must first prove the source is
	// unreferenced and fence cached writers past the source.
	ObservedSourceReclaimActive bool
	// observedSourceMissingIsError preserves MarkValueLogZombie's historical
	// missing-manager signal without changing rewrite cleanup's documented
	// best-effort treatment of already-retired observed IDs.
	observedSourceMissingIsError bool
}

// ValueLogGCStats summarizes value-log GC work.
type ValueLogGCStats struct {
	SegmentsTotal                           int
	SegmentsReferenced                      int
	SegmentsActive                          int
	SegmentsProtected                       int
	SegmentsProtectedInUse                  int
	SegmentsProtectedRetained               int
	SegmentsProtectedOverlap                int
	SegmentsProtectedOther                  int
	SegmentsEligible                        int
	SegmentsDeleted                         int
	SegmentsPending                         int
	BytesTotal                              int64
	BytesReferenced                         int64
	BytesActive                             int64
	BytesProtected                          int64
	BytesProtectedInUse                     int64
	BytesProtectedRetained                  int64
	BytesProtectedOverlap                   int64
	BytesProtectedOther                     int64
	BytesEligible                           int64
	BytesDeleted                            int64
	BytesPending                            int64
	ObservedSourceSegments                  int
	ObservedSourceSegmentsReferenced        int
	ObservedSourceSegmentsActive            int
	ObservedSourceSegmentsProtected         int
	ObservedSourceSegmentsProtectedInUse    int
	ObservedSourceSegmentsProtectedRetained int
	ObservedSourceSegmentsProtectedOverlap  int
	ObservedSourceSegmentsProtectedOther    int
	ObservedSourceSegmentsEligible          int
	ObservedSourceSegmentsDeleted           int
	ObservedSourceSegmentsPending           int
	ObservedSourceBytes                     int64
	ObservedSourceBytesReferenced           int64
	ObservedSourceBytesActive               int64
	ObservedSourceBytesProtected            int64
	ObservedSourceBytesProtectedInUse       int64
	ObservedSourceBytesProtectedRetained    int64
	ObservedSourceBytesProtectedOverlap     int64
	ObservedSourceBytesProtectedOther       int64
	ObservedSourceBytesEligible             int64
	ObservedSourceBytesDeleted              int64
	ObservedSourceBytesPending              int64
}

// ValueLogGC deletes fully-unreferenced value-log segments from value_vlog.
//
// It scans the user + system trees for value-log pointers, computes referenced
// value_vlog segments, and removes segments that are:
//   - not referenced,
//   - not the currently-active segment per lane,
//   - and not pinned by active snapshots.
func (db *DB) ValueLogGC(ctx context.Context, opts ValueLogGCOptions) (ValueLogGCStats, error) {
	return db.valueLogGC(ctx, opts, true)
}

func (db *DB) valueLogGC(ctx context.Context, opts ValueLogGCOptions, lockMaintenance bool) (ValueLogGCStats, error) {
	var stats ValueLogGCStats
	if db == nil {
		return stats, fmt.Errorf("missing db")
	}
	if db.readOnly && !opts.DryRun {
		return stats, ErrReadOnly
	}
	if !opts.DryRun {
		if err := db.commandWALPoisonedError(); err != nil {
			return stats, err
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if lockMaintenance {
		if hook := db.testStorageMaintenanceBeforeLockHook; hook != nil {
			hook("value-log-gc")
		}
		db.maintenanceMu.Lock()
		defer db.maintenanceMu.Unlock()
	}
	if !opts.DryRun {
		if err := db.CheckStorageMaintenanceReady(); err != nil {
			return stats, err
		}
		if hook := db.testStorageMaintenanceAfterLockHook; hook != nil {
			if err := hook("value-log-gc"); err != nil {
				return stats, err
			}
		}
	}
	vm := db.valueLogManager
	if vm == nil {
		return stats, fmt.Errorf("value log manager unavailable")
	}
	observedOnly := len(opts.ObservedSourceFileIDs) > 0 && opts.ObservedSourceAssumeUnreferenced
	missingObservedSourceError := func() error {
		if !observedOnly || !opts.observedSourceMissingIsError {
			return nil
		}
		for _, id := range opts.ObservedSourceFileIDs {
			if id != 0 {
				return fmt.Errorf("%w: file_id=%d", valuelog.ErrFileNotFound, id)
			}
		}
		return nil
	}
	var referenced map[uint32]struct{}
	var scannedSeq uint64
	var recoverableRoots *RecoverableRootSet
	if !observedOnly {
		// A full scan is O(N), so it must never run while publishPrepareMu is
		// exclusively held. Safety invariant: GC never zombifies a segment from a
		// referenced set older than the root visible while the exclusive lock is
		// held. A post-scan root can first-reference an old segment, so a changed
		// commit sequence is retried outside the lock once and then conservatively
		// aborts this GC pass.
		const maxStaleScanRetries = 1
		for attempt := 0; ; attempt++ {
			var err error
			referenced, _, scannedSeq, err = db.referencedValueLogSegmentsForGCAtSeq(ctx)
			if err != nil {
				return stats, err
			}
			runValueLogGCPostScanHook()
			if opts.DryRun {
				break
			}

			db.publishPrepareMu.Lock()
			if db.currentCommitSeq() == scannedSeq {
				db.publishPrepareMu.Unlock()
				break
			}
			db.publishPrepareMu.Unlock()
			if attempt == maxStaleScanRetries {
				return stats, nil
			}
		}
	}
	{
		var err error
		if opts.DryRun {
			recoverableRoots, err = db.captureRecoverableRootSetForInspectionWithMaintenanceLockHeld(ctx)
		} else {
			recoverableRoots, err = db.captureRecoverableRootSetWithMaintenanceLockHeld(ctx)
		}
		if err != nil {
			return stats, err
		}
		defer recoverableRoots.Release()
		recoverableReferenced, err := db.referencedValueLogSegmentsForRecoverableRootSet(ctx, recoverableRoots)
		if err != nil {
			return stats, err
		}
		if referenced == nil {
			referenced = make(map[uint32]struct{}, len(recoverableReferenced))
		}
		for fileID := range recoverableReferenced {
			referenced[fileID] = struct{}{}
		}

		if !opts.DryRun {
			db.publishPrepareMu.Lock()
			defer db.publishPrepareMu.Unlock()
			if !observedOnly && db.currentCommitSeq() != scannedSeq {
				return stats, ErrRecoverableRootSetStale
			}
		}
	}

	// Commit-sequence equality fences published roots, not prepared output. The
	// current/recent and pending-append keep sets below are therefore computed
	// from the value-log topology while publish preparation remains excluded.
	// A full GC pass is already a directory-wide maintenance operation, so it
	// must refresh even when publication registered a non-empty reachable
	// subset: externally created unreferenced siblings are precisely what GC is
	// expected to discover. Observed-source reclamation remains bounded to the
	// caller's already-registered identities and avoids that scan.
	var set *valuelog.Set
	if observedOnly {
		set = vm.CurrentSetNoRefresh()
	} else {
		if err := vm.Refresh(); err != nil {
			return stats, err
		}
		set = valueLogGCPostRefreshCurrentSetNoRefresh(vm)
	}
	if observedOnly && (set == nil || len(set.Files) == 0) {
		if set != nil {
			_ = vm.Release(set)
		}
		if err := vm.Refresh(); err != nil {
			return stats, err
		}
		set = valueLogGCPostRefreshCurrentSetNoRefresh(vm)
	}
	if set == nil || len(set.Files) == 0 {
		if set != nil {
			_ = vm.Release(set)
		}
		if err := missingObservedSourceError(); err != nil {
			return stats, err
		}
		if !opts.DryRun && !db.readOnly {
			db.persistValueLogRefTrackerBestEffort()
		}
		return stats, nil
	}
	files := db.valueOnlyValueLogFiles(set.Files)
	if len(files) == 0 {
		if set != nil {
			_ = vm.Release(set)
		}
		if err := missingObservedSourceError(); err != nil {
			return stats, err
		}
		if !opts.DryRun && !db.readOnly {
			db.persistValueLogRefTrackerBestEffort()
		}
		return stats, nil
	}
	defer func() {
		if set != nil {
			_ = vm.Release(set)
		}
	}()
	valueSet := &valuelog.Set{Files: files}
	keptIDs := currentValueLogIDs(valueSet)
	protectedAll := mergeUniqueNonEmptyPaths(opts.ProtectedPaths, opts.ProtectedInUsePaths, opts.ProtectedRetainedPaths)
	if len(protectedAll) > 0 {
		if recent := recentValueLogIDsForProtectedPaths(valueSet, valueLogKeepRecentSegmentsPerLane, protectedAll); len(recent) > 0 {
			// Protected-path mode should keep a narrow recent window only for the
			// protected lanes so historical rewrite lanes remain eligible. Keep the
			// current primary-lane segment as a safety guard for live writes.
			keptIDs = recent
			for id := range currentValueLogIDs(valueSet) {
				lane, _ := valuelog.DecodeFileID(id)
				if lane == 0 {
					keptIDs[id] = struct{}{}
				}
			}
		}
	}
	pendingAppendIDs := db.pendingValueLogAppendFileIDs()
	for id := range pendingAppendIDs {
		keptIDs[id] = struct{}{}
	}
	protectedPaths := make(map[string]struct{}, len(opts.ProtectedPaths))
	for _, path := range opts.ProtectedPaths {
		if path == "" {
			continue
		}
		protectedPaths[path] = struct{}{}
	}
	protectedInUsePaths := make(map[string]struct{}, len(opts.ProtectedInUsePaths))
	for _, path := range opts.ProtectedInUsePaths {
		if path == "" {
			continue
		}
		protectedInUsePaths[path] = struct{}{}
	}
	protectedRetainedPaths := make(map[string]struct{}, len(opts.ProtectedRetainedPaths))
	for _, path := range opts.ProtectedRetainedPaths {
		if path == "" {
			continue
		}
		protectedRetainedPaths[path] = struct{}{}
	}
	type candidate struct {
		path     string
		size     int64
		observed bool
	}
	candidates := make(map[uint32]candidate)
	observedSourceIDs := make(map[uint32]struct{}, len(opts.ObservedSourceFileIDs))
	for _, id := range opts.ObservedSourceFileIDs {
		if id == 0 {
			continue
		}
		observedSourceIDs[id] = struct{}{}
	}

	if observedOnly {
		for id := range observedSourceIDs {
			if err := ctx.Err(); err != nil {
				return stats, err
			}
			f, ok := files[id]
			if !ok {
				if opts.observedSourceMissingIsError {
					// MarkValueLogZombie historically delegated directly to the
					// manager, whose missing-ID result is part of the caching backend
					// contract: callers use ErrFileNotFound to remove an orphaned
					// retained path that the manager no longer tracks.
					return stats, fmt.Errorf("%w: file_id=%d", valuelog.ErrFileNotFound, id)
				}
				continue
			}
			size := fileSize(f)
			stats.ObservedSourceSegments++
			stats.ObservedSourceBytes += size
			stats.SegmentsTotal++
			stats.BytesTotal += size

			if _, ok := referenced[id]; ok {
				stats.SegmentsReferenced++
				stats.BytesReferenced += size
				stats.ObservedSourceSegmentsReferenced++
				stats.ObservedSourceBytesReferenced += size
				continue
			}

			if _, ok := pendingAppendIDs[id]; ok {
				stats.SegmentsProtected++
				stats.BytesProtected += size
				stats.SegmentsProtectedOther++
				stats.BytesProtectedOther += size
				stats.ObservedSourceSegmentsProtected++
				stats.ObservedSourceBytesProtected += size
				stats.ObservedSourceSegmentsProtectedOther++
				stats.ObservedSourceBytesProtectedOther += size
				continue
			}
			if _, ok := keptIDs[id]; ok && !opts.ObservedSourceReclaimActive {
				stats.SegmentsActive++
				stats.BytesActive += size
				stats.ObservedSourceSegmentsActive++
				stats.ObservedSourceBytesActive += size
				continue
			}
			_, inUseProtected := protectedInUsePaths[f.Path]
			_, retainedProtected := protectedRetainedPaths[f.Path]
			if inUseProtected || retainedProtected {
				stats.SegmentsProtected++
				stats.BytesProtected += size
				stats.ObservedSourceSegmentsProtected++
				stats.ObservedSourceBytesProtected += size
				switch {
				case inUseProtected && retainedProtected:
					stats.SegmentsProtectedOverlap++
					stats.BytesProtectedOverlap += size
					stats.ObservedSourceSegmentsProtectedOverlap++
					stats.ObservedSourceBytesProtectedOverlap += size
				case inUseProtected:
					stats.SegmentsProtectedInUse++
					stats.BytesProtectedInUse += size
					stats.ObservedSourceSegmentsProtectedInUse++
					stats.ObservedSourceBytesProtectedInUse += size
				default:
					stats.SegmentsProtectedRetained++
					stats.BytesProtectedRetained += size
					stats.ObservedSourceSegmentsProtectedRetained++
					stats.ObservedSourceBytesProtectedRetained += size
				}
				continue
			}
			if _, ok := protectedPaths[f.Path]; ok {
				stats.SegmentsProtected++
				stats.BytesProtected += size
				stats.SegmentsProtectedOther++
				stats.BytesProtectedOther += size
				stats.ObservedSourceSegmentsProtected++
				stats.ObservedSourceBytesProtected += size
				stats.ObservedSourceSegmentsProtectedOther++
				stats.ObservedSourceBytesProtectedOther += size
				continue
			}

			stats.SegmentsEligible++
			stats.BytesEligible += size
			stats.ObservedSourceSegmentsEligible++
			stats.ObservedSourceBytesEligible += size

			if opts.DryRun {
				stats.SegmentsPending++
				stats.BytesPending += size
				stats.ObservedSourceSegmentsPending++
				stats.ObservedSourceBytesPending += size
				continue
			}
			candidates[id] = candidate{path: f.Path, size: size, observed: true}
		}
	} else {
		for id, f := range files {
			if err := ctx.Err(); err != nil {
				return stats, err
			}
			size := fileSize(f)
			observed := false
			if _, ok := observedSourceIDs[id]; ok {
				observed = true
				stats.ObservedSourceSegments++
				stats.ObservedSourceBytes += size
			}
			stats.SegmentsTotal++
			stats.BytesTotal += size

			if _, ok := referenced[id]; ok {
				stats.SegmentsReferenced++
				stats.BytesReferenced += size
				if observed {
					stats.ObservedSourceSegmentsReferenced++
					stats.ObservedSourceBytesReferenced += size
				}
				continue
			}
			if _, ok := keptIDs[id]; ok {
				stats.SegmentsActive++
				stats.BytesActive += size
				if observed {
					stats.ObservedSourceSegmentsActive++
					stats.ObservedSourceBytesActive += size
				}
				continue
			}
			_, inUseProtected := protectedInUsePaths[f.Path]
			_, retainedProtected := protectedRetainedPaths[f.Path]
			if inUseProtected || retainedProtected {
				stats.SegmentsProtected++
				stats.BytesProtected += size
				if observed {
					stats.ObservedSourceSegmentsProtected++
					stats.ObservedSourceBytesProtected += size
				}
				switch {
				case inUseProtected && retainedProtected:
					stats.SegmentsProtectedOverlap++
					stats.BytesProtectedOverlap += size
					if observed {
						stats.ObservedSourceSegmentsProtectedOverlap++
						stats.ObservedSourceBytesProtectedOverlap += size
					}
				case inUseProtected:
					stats.SegmentsProtectedInUse++
					stats.BytesProtectedInUse += size
					if observed {
						stats.ObservedSourceSegmentsProtectedInUse++
						stats.ObservedSourceBytesProtectedInUse += size
					}
				default:
					stats.SegmentsProtectedRetained++
					stats.BytesProtectedRetained += size
					if observed {
						stats.ObservedSourceSegmentsProtectedRetained++
						stats.ObservedSourceBytesProtectedRetained += size
					}
				}
				continue
			}
			if _, ok := protectedPaths[f.Path]; ok {
				stats.SegmentsProtected++
				stats.BytesProtected += size
				stats.SegmentsProtectedOther++
				stats.BytesProtectedOther += size
				if observed {
					stats.ObservedSourceSegmentsProtected++
					stats.ObservedSourceBytesProtected += size
					stats.ObservedSourceSegmentsProtectedOther++
					stats.ObservedSourceBytesProtectedOther += size
				}
				continue
			}

			stats.SegmentsEligible++
			stats.BytesEligible += size
			if observed {
				stats.ObservedSourceSegmentsEligible++
				stats.ObservedSourceBytesEligible += size
			}

			if opts.DryRun {
				stats.SegmentsPending++
				stats.BytesPending += size
				if observed {
					stats.ObservedSourceSegmentsPending++
					stats.ObservedSourceBytesPending += size
				}
				continue
			}
			candidates[id] = candidate{path: f.Path, size: size, observed: observed}
		}
	}

	if opts.DryRun {
		if set != nil {
			_ = vm.Release(set)
			set = nil
		}
		return stats, nil
	}
	if len(candidates) > 0 {
		// Consume the exact recovery authority only after classification is
		// complete. publishPrepareMu remains held, so no new visible root can make
		// one of these segments reachable between the final validation and zombie
		// mutation. Segments selected by an older recoverable root were excluded
		// during classification; eligible rewritten sources leave the current
		// ValueLogSet here, while snapshot/resource pins retain their exact physical
		// identities until every remaining reader releases them.
		if hook := db.testValueLogGCBeforeRevalidateHook; hook != nil {
			hook()
		}
		if err := recoverableRoots.Revalidate(); err != nil {
			return stats, err
		}
		for id := range candidates {
			if err := vm.MarkZombie(id); err != nil {
				return stats, err
			}
		}
		if err := db.publishValueLogSetNoRefresh(); err != nil {
			return stats, err
		}
		// The new current topology no longer selects these files. Persistent
		// durable-root resource tokens and snapshot ValueLogSets now own any
		// required physical retention, so the capability's cloned pins can drop
		// before releasing this operation's local manager set.
		recoverableRoots.Release()
	}

	if set != nil {
		_ = vm.Release(set)
		set = nil
	}

	if len(candidates) == 0 {
		if err := db.publishValueLogSetNoRefresh(); err != nil {
			return stats, err
		}
	}

	for _, info := range candidates {
		if info.path == "" {
			continue
		}
		if _, err := os.Stat(info.path); err != nil {
			if os.IsNotExist(err) {
				stats.SegmentsDeleted++
				stats.BytesDeleted += info.size
				if info.observed {
					stats.ObservedSourceSegmentsDeleted++
					stats.ObservedSourceBytesDeleted += info.size
				}
			} else {
				return stats, err
			}
		}
	}
	if stats.SegmentsEligible > stats.SegmentsDeleted {
		stats.SegmentsPending = stats.SegmentsEligible - stats.SegmentsDeleted
	}
	if stats.BytesEligible > stats.BytesDeleted {
		stats.BytesPending = stats.BytesEligible - stats.BytesDeleted
	}
	if stats.ObservedSourceSegmentsEligible > stats.ObservedSourceSegmentsDeleted {
		stats.ObservedSourceSegmentsPending = stats.ObservedSourceSegmentsEligible - stats.ObservedSourceSegmentsDeleted
	}
	if stats.ObservedSourceBytesEligible > stats.ObservedSourceBytesDeleted {
		stats.ObservedSourceBytesPending = stats.ObservedSourceBytesEligible - stats.ObservedSourceBytesDeleted
	}

	if !observedOnly {
		currentSet := vm.CurrentSetNoRefresh()
		if currentSet != nil {
			if err := updateValueLogHealthAfterGC(db.dir, &valuelog.Set{Files: db.valueOnlyValueLogFiles(currentSet.Files)}, referenced); err != nil {
				if db.notifyError != nil {
					db.notifyError(fmt.Errorf("value-log health update after gc: %w", err))
				}
			}
			_ = vm.Release(currentSet)
		}
	}

	if !opts.DryRun && !db.readOnly {
		db.persistValueLogRefTrackerBestEffort()
	}
	return stats, nil
}

func currentValueLogIDs(set *valuelog.Set) map[uint32]struct{} {
	active := make(map[uint32]struct{})
	if set == nil || len(set.Files) == 0 {
		return active
	}
	maxByLane := make(map[uint32]uint32)
	for id := range set.Files {
		lane, seq := valuelog.DecodeFileID(id)
		if cur, ok := maxByLane[lane]; !ok || seq > cur {
			maxByLane[lane] = seq
		}
	}
	for id := range set.Files {
		lane, seq := valuelog.DecodeFileID(id)
		if maxByLane[lane] == seq || set.Files[id].IsCurrentWritable() {
			active[id] = struct{}{}
		}
	}
	return active
}

func mergeUniqueNonEmptyPaths(pathSets ...[]string) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, paths := range pathSets {
		for _, path := range paths {
			if path == "" {
				continue
			}
			if _, ok := seen[path]; ok {
				continue
			}
			seen[path] = struct{}{}
			out = append(out, path)
		}
	}
	return out
}

func recentValueLogIDs(set *valuelog.Set, keepPerLane int) map[uint32]struct{} {
	if keepPerLane <= 1 {
		return currentValueLogIDs(set)
	}
	if set == nil || len(set.Files) == 0 {
		return nil
	}
	kept := make(map[uint32]struct{})
	maxByLane := make(map[uint32]uint32)
	for id := range set.Files {
		lane, seq := valuelog.DecodeFileID(id)
		if cur, ok := maxByLane[lane]; !ok || seq > cur {
			maxByLane[lane] = seq
		}
	}
	for id := range set.Files {
		lane, seq := valuelog.DecodeFileID(id)
		maxSeq := maxByLane[lane]
		if maxSeq <= seq || int64(maxSeq)-int64(seq) < int64(keepPerLane) {
			kept[id] = struct{}{}
		}
	}
	return kept
}

func recentValueLogIDsForProtectedPaths(set *valuelog.Set, keepPerLane int, protectedPaths []string) map[uint32]struct{} {
	if keepPerLane <= 1 || set == nil || len(set.Files) == 0 {
		return nil
	}
	if len(protectedPaths) == 0 {
		return nil
	}
	protected := make(map[string]struct{}, len(protectedPaths))
	for _, path := range protectedPaths {
		if path == "" {
			continue
		}
		protected[path] = struct{}{}
	}
	if len(protected) == 0 {
		return nil
	}
	protectedLanes := make(map[uint32]struct{})
	for id, f := range set.Files {
		if f == nil || f.Path == "" {
			continue
		}
		if _, ok := protected[f.Path]; !ok {
			continue
		}
		lane, _ := valuelog.DecodeFileID(id)
		protectedLanes[lane] = struct{}{}
	}
	if len(protectedLanes) == 0 {
		return nil
	}
	kept := make(map[uint32]struct{})
	maxByLane := make(map[uint32]uint32)
	for id := range set.Files {
		lane, seq := valuelog.DecodeFileID(id)
		if _, ok := protectedLanes[lane]; !ok {
			continue
		}
		if cur, ok := maxByLane[lane]; !ok || seq > cur {
			maxByLane[lane] = seq
		}
	}
	for id := range set.Files {
		lane, seq := valuelog.DecodeFileID(id)
		if _, ok := protectedLanes[lane]; !ok {
			continue
		}
		maxSeq := maxByLane[lane]
		if maxSeq <= seq {
			kept[id] = struct{}{}
			continue
		}
		delta := int64(maxSeq) - int64(seq)
		if delta < int64(keepPerLane) {
			kept[id] = struct{}{}
		}
	}
	return kept
}

func fileSize(f *valuelog.File) int64 {
	if f == nil {
		return 0
	}
	if f.Path != "" {
		if info, err := os.Stat(f.Path); err == nil {
			return info.Size()
		}
	}
	return f.SizeBestEffort()
}
