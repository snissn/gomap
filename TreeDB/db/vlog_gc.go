package db

import (
	"context"
	"fmt"
	"os"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// valueLogKeepRecentSegmentsPerLane bounds how aggressively GC/rewrite may mark
// segments zombie while writers are online. In cached mode, new value-log
// segments can be created/rotated after a protected-path snapshot is taken but
// before reachability is re-evaluated; keeping a small recent window prevents
// deleting freshly rotated segments that may still back in-memory pointers.
const valueLogKeepRecentSegmentsPerLane = 2

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

// ValueLogGC deletes fully-unreferenced value-log segments.
//
// It scans the user + system trees for value-log pointers, computes referenced
// segments, and removes segments that are:
//   - not referenced,
//   - not the currently-active segment per lane,
//   - and not pinned by active snapshots.
func (db *DB) ValueLogGC(ctx context.Context, opts ValueLogGCOptions) (ValueLogGCStats, error) {
	var stats ValueLogGCStats
	if db == nil {
		return stats, fmt.Errorf("missing db")
	}
	if db.readOnly {
		return stats, ErrReadOnly
	}
	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	vm := db.valueLogManager
	if vm == nil {
		return stats, fmt.Errorf("value log manager unavailable")
	}

	observedOnly := len(opts.ObservedSourceFileIDs) > 0 && opts.ObservedSourceAssumeUnreferenced
	var referenced map[uint32]struct{}
	if !observedOnly {
		var err error
		referenced, err = db.referencedValueLogSegments(ctx)
		if err != nil {
			return stats, err
		}
	}

	// Prefer no-refresh snapshots to avoid repeated filesystem scans on the hot
	// path. Fall back to a refresh if the manager has not yet discovered any
	// segments (or if another process created segments on disk).
	set := vm.CurrentSetNoRefresh()
	if set == nil || len(set.Files) == 0 {
		if set != nil {
			_ = vm.Release(set)
		}
		if err := vm.Refresh(); err != nil {
			return stats, err
		}
		set = vm.CurrentSetNoRefresh()
	}
	keptIDs := currentValueLogIDs(set)
	protectedAll := mergeUniqueNonEmptyPaths(opts.ProtectedPaths, opts.ProtectedInUsePaths, opts.ProtectedRetainedPaths)
	if len(protectedAll) > 0 {
		if recent := recentValueLogIDsForProtectedPaths(set, valueLogKeepRecentSegmentsPerLane, protectedAll); len(recent) > 0 {
			// Protected-path mode should keep a narrow recent window only for the
			// protected lanes so historical rewrite lanes remain eligible. Keep the
			// current primary-lane segment as a safety guard for live writes.
			keptIDs = recent
			for id := range currentValueLogIDs(set) {
				lane, _ := valuelog.DecodeFileID(id)
				if lane == 0 {
					keptIDs[id] = struct{}{}
				}
			}
		}
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
			f, ok := set.Files[id]
			if !ok {
				continue
			}
			size := fileSize(f)
			stats.ObservedSourceSegments++
			stats.ObservedSourceBytes += size
			stats.SegmentsTotal++
			stats.BytesTotal += size

			if _, ok := keptIDs[id]; ok {
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
			if err := vm.MarkZombie(id); err != nil {
				return stats, err
			}
			candidates[id] = candidate{path: f.Path, size: size, observed: true}
		}
	} else {
		for id, f := range set.Files {
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
			if err := vm.MarkZombie(id); err != nil {
				return stats, err
			}
			candidates[id] = candidate{path: f.Path, size: size, observed: observed}
		}
	}

	if opts.DryRun {
		if set != nil {
			_ = vm.Release(set)
		}
		db.persistValueLogRefTrackerBestEffort()
		return stats, nil
	}

	if set != nil {
		_ = vm.Release(set)
	}

	if err := db.publishValueLogSetNoRefresh(); err != nil {
		return stats, err
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
			if err := updateValueLogHealthAfterGC(db.dir, currentSet, referenced); err != nil {
				if db.notifyError != nil {
					db.notifyError(fmt.Errorf("value-log health update after gc: %w", err))
				}
			}
			_ = vm.Release(currentSet)
		}
	}

	db.persistValueLogRefTrackerBestEffort()
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
		if maxByLane[lane] == seq {
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
	return f.SizeBestEffort()
}
