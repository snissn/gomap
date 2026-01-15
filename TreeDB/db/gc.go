package db

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// GCStats contains statistics about the last garbage collection run.
type GCStats struct {
	LastRunTime     time.Time
	LastRunDuration time.Duration
	ReclaimedBytes  int64
	LastError       error
}

// GC performs a refcounted garbage collection of vlog and slab segments.
// It identifies segments that are no longer reachable from the latest root
// and marks them for deletion. Actual deletion occurs once all pinned
// snapshots referencing these segments are closed.
//
// It returns the number of bytes reclaimed (best-effort estimate).
func (db *DB) GC() (reclaimed int64, err error) {
	start := time.Now()

	// 1. Snapshot current state for consistent scanning.
	snap := db.AcquireSnapshot()
	defer snap.Close()

	state := snap.State()
	if state == nil {
		return 0, nil
	}

	liveValueIDs := make(map[uint64]struct{})
	liveFileIDs := make(map[uint32]struct{})

	// 2. Scan User Tree for reachable ValueIDs and legacy pointers.
	// We use the snapshot tree which is consistent.
	userIter := snap.tree.Iterator(nil, nil)
	defer userIter.Close()

	for userIter.Valid() {
		val, ptr, flags := userIter.UnsafeEntry()
		if flags&node.FlagValueID != 0 {
			if len(val) == 8 {
				vid := binary.BigEndian.Uint64(val)
				liveValueIDs[vid] = struct{}{}
			}
		} else if flags&node.FlagPointer != 0 {
			liveFileIDs[ptr.FileID] = struct{}{}
		}
		userIter.Next()
	}
	if err := userIter.Error(); err != nil {
		return 0, fmt.Errorf("GC: user tree scan failed: %w", err)
	}

	// 3. Scan Value Index (System Tree) for live vlog/slab references.
	// Also identify unreachable ValueIDs for pruning.
	sysTree := tree.New(snap.idx.pager, ValueReaderForState(state), state.SystemRootPageID)
	sysIter := sysTree.Iterator(ValueIndexPrefix, ValueIndexPrefixEnd())
	defer sysIter.Close()

	var deadValueIDOps []batch.Entry
	for sysIter.Valid() {
		key := sysIter.Key()
		if len(key) >= 11 { // 3 bytes prefix + 8 bytes ID
			vid := binary.BigEndian.Uint64(key[3:])
			val := sysIter.Value()

			if _, live := liveValueIDs[vid]; live {
				if len(val) == page.ValuePtrSize {
					ptr := page.DecodeValuePtr(val)
					liveFileIDs[ptr.FileID] = struct{}{}
				}
			} else {
				deadValueIDOps = append(deadValueIDOps, batch.Entry{
					Type: batch.OpDelete,
					Key:  append([]byte(nil), key...),
				})
			}
		}
		sysIter.Next()
	}

	if err := sysIter.Error(); err != nil {
		return 0, fmt.Errorf("GC: sys tree scan failed: %w", err)
	}

	// 4. Prune Value Index if dead mappings found.
	// We must take the writer lock now to apply updates to the LATEST state.
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	latestState := db.state.Load()
	if latestState == nil {
		return 0, nil
	}

	sysRoot := latestState.SystemRootPageID
	if len(deadValueIDOps) > 0 {
		newSysRoot, retired, err := db.applySystemUpdates(sysRoot, deadValueIDOps, adaptive.Metrics{}, db.slabManager, db.valueLogManager)
		if err != nil {
			return 0, fmt.Errorf("GC: failed to prune value index: %w", err)
		}
		// Commit the pruned system tree.
		if err := db.finalizeCommit(latestState.RootPageID, newSysRoot, retired, true, nil, adaptive.Metrics{}, 0); err != nil {
			return 0, fmt.Errorf("GC: failed to commit pruned index: %w", err)
		}
		sysRoot = newSysRoot
		latestState = db.state.Load()
	}

	// 5. Mark dead segments as zombies.
	var candidateBytes int64

	// Value Logs
	// We only mark files that were present in the snapshot we scanned.
	for id := range state.ValueLogSet.Files {
		if _, live := liveFileIDs[id]; !live {
			sz, _ := db.valueLogManager.SegmentSize(id)
			candidateBytes += sz
			_ = db.valueLogManager.MarkZombie(id)
		}
	}

	// Slabs
	// We only mark files that were present in the snapshot we scanned.
	activeSlabID := db.slabManager.ActiveSlabID()
	for id, sf := range state.SlabSet.Files {
		if id == activeSlabID {
			continue
		}
		if _, live := liveFileIDs[id]; !live {
			candidateBytes += sf.Size()
			_ = db.slabManager.MarkZombie(id)
		}
	}

	// 6. Trigger cleanup of zero-ref zombies.
	if err := db.RefreshSlabSet(); err != nil {
		stats := &GCStats{
			LastRunTime:     start,
			LastRunDuration: time.Since(start),
			LastError:       err,
		}
		db.lastGCStats.Store(stats)
		return 0, err
	}

	duration := time.Since(start)
	stats := &GCStats{
		LastRunTime:     start,
		LastRunDuration: duration,
		ReclaimedBytes:  candidateBytes,
	}
	db.lastGCStats.Store(stats)

	return candidateBytes, nil
}
