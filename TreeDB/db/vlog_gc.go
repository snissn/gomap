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
	DryRun         bool
	ProtectedPaths []string
	ReferencedIDs  map[uint32]struct{}
}

// ValueLogGCStats summarizes value-log GC work.
type ValueLogGCStats struct {
	SegmentsTotal      int
	SegmentsReferenced int
	SegmentsActive     int
	SegmentsProtected  int
	SegmentsEligible   int
	SegmentsDeleted    int
	BytesTotal         int64
	BytesReferenced    int64
	BytesActive        int64
	BytesProtected     int64
	BytesEligible      int64
	BytesDeleted       int64
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
	releaseSealedMmapBudget := db.acquireValueLogMaintenanceSealedMmapBudget()
	defer releaseSealedMmapBudget()
	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	vm := db.valueLogManager
	if vm == nil {
		return stats, fmt.Errorf("value log manager unavailable")
	}

	referenced := opts.ReferencedIDs
	if len(referenced) == 0 {
		var err error
		referenced, err = db.referencedValueLogSegments(ctx)
		if err != nil {
			return stats, err
		}
	} else {
		referenced = cloneReferencedValueLogSegments(referenced)
	}
	db.cacheLastValueLogGCReferencedSegments(db.currentCommitSeq(), referenced)

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
	if len(opts.ProtectedPaths) > 0 {
		if recent := recentValueLogIDsForProtectedPaths(set, valueLogKeepRecentSegmentsPerLane, opts.ProtectedPaths); len(recent) > 0 {
			keptIDs = recent
		}
	}
	protectedPaths := make(map[string]struct{}, len(opts.ProtectedPaths))
	for _, path := range opts.ProtectedPaths {
		if path == "" {
			continue
		}
		protectedPaths[path] = struct{}{}
	}
	type candidate struct {
		path string
		size int64
	}
	candidates := make(map[uint32]candidate)

	for id, f := range set.Files {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		size := fileSize(f)
		stats.SegmentsTotal++
		stats.BytesTotal += size

		if _, ok := referenced[id]; ok {
			stats.SegmentsReferenced++
			stats.BytesReferenced += size
			continue
		}
		if _, ok := keptIDs[id]; ok {
			stats.SegmentsActive++
			stats.BytesActive += size
			continue
		}
		if _, ok := protectedPaths[f.Path]; ok {
			stats.SegmentsProtected++
			stats.BytesProtected += size
			continue
		}

		stats.SegmentsEligible++
		stats.BytesEligible += size

		if opts.DryRun {
			continue
		}
		if err := vm.MarkZombie(id); err != nil {
			return stats, err
		}
		candidates[id] = candidate{path: f.Path, size: size}
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
			} else {
				return stats, err
			}
		}
	}

	currentSet := vm.CurrentSetNoRefresh()
	if currentSet != nil {
		if err := updateValueLogHealthAfterGC(db.dir, currentSet, referenced); err != nil {
			if db.notifyError != nil {
				db.notifyError(fmt.Errorf("value-log health update after gc: %w", err))
			}
		}
		_ = vm.Release(currentSet)
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
	if f.File != nil {
		if info, err := f.File.Stat(); err == nil {
			return info.Size()
		}
	}
	if f.Path != "" {
		if info, err := os.Stat(f.Path); err == nil {
			return info.Size()
		}
	}
	return 0
}
