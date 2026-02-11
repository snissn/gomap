package db

import (
	"context"
	"fmt"
	"os"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// ValueLogGCOptions controls value-log garbage collection.
type ValueLogGCOptions struct {
	DryRun         bool
	ProtectedPaths []string
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
	if ctx == nil {
		ctx = context.Background()
	}
	if db.valueLogManager == nil {
		return stats, fmt.Errorf("value log manager unavailable")
	}

	snap := db.AcquireSnapshot()
	if snap == nil || snap.idx == nil || snap.state == nil {
		if snap != nil {
			_ = snap.Close()
		}
		return stats, fmt.Errorf("missing db state")
	}

	referenced := make(map[uint32]struct{})

	userIter := snap.tree.Iterator(nil, nil)
	if err := collectValueLogRefs(userIter, referenced); err != nil {
		_ = userIter.Close()
		return stats, err
	}
	_ = userIter.Close()

	sysIter := tree.New(snap.idx.pager, valueReader{vlogs: snap.state.ValueLogSet}, snap.state.SystemRootPageID).Iterator(nil, nil)
	if err := collectValueLogRefs(sysIter, referenced); err != nil {
		_ = sysIter.Close()
		return stats, err
	}
	_ = sysIter.Close()

	if err := snap.Close(); err != nil {
		return stats, err
	}

	set := db.valueLogManager.CurrentSet()
	activeIDs := currentValueLogIDs(set)
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
		if _, ok := activeIDs[id]; ok {
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
		if err := db.valueLogManager.MarkZombie(id); err != nil {
			return stats, err
		}
		candidates[id] = candidate{path: f.Path, size: size}
	}

	if opts.DryRun {
		if set != nil {
			_ = db.valueLogManager.Release(set)
		}
		return stats, nil
	}

	if set != nil {
		_ = db.valueLogManager.Release(set)
	}

	if err := db.RefreshValueLogSet(); err != nil {
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

	return stats, nil
}

func collectValueLogRefs(it iterator.UnsafeIterator, refs map[uint32]struct{}) error {
	for it.Valid() {
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer != 0 && page.IsValueLogFileID(ptr.FileID) {
			refs[ptr.FileID] = struct{}{}
		}
		it.Next()
	}
	return it.Error()
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
