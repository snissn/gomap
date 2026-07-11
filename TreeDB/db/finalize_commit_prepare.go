package db

import (
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/pager"
)

// finalizeCommitPrepareGuard keeps value-log GC from scanning reachability and
// deleting freshly flushed publish output before the root that references it is
// either installed or abandoned.
type finalizeCommitPrepareGuard struct {
	db *DB
}

func (g *finalizeCommitPrepareGuard) transferTo(candidate *PreparedRootCandidate) bool {
	if g == nil || g.db == nil || candidate == nil {
		return false
	}
	candidate.holdsPreparePin = true
	g.db = nil
	return true
}

func (g *finalizeCommitPrepareGuard) Release() {
	if g == nil || g.db == nil {
		return
	}
	g.db.publishPrepareMu.RUnlock()
	g.db = nil
}

func (db *DB) flushFinalizeCommitDurability(idx *indexGen, valueLogAppender ValueLogAppender, sync bool) error {
	if idx == nil {
		return errors.New("missing index")
	}
	var dependencyEvent durabilitycut.Event
	var hasDependencyEvent bool
	if sync && durabilitycut.Enabled() {
		var err error
		dependencyEvent, hasDependencyEvent, err = db.finalizeDependencySyncEvent(valueLogAppender, true)
		if err != nil {
			return err
		}
	}
	if !sync {
		if err := durabilitycut.EmitBasic(durabilitycut.BeforeUserspaceFlush, "", db.dir); err != nil {
			return err
		}
	} else if hasDependencyEvent {
		dependencyEvent.Point = durabilitycut.BeforeDependencyFileSync
		if err := durabilitycut.Emit(dependencyEvent); err != nil {
			return err
		}
	}
	// Ensure value-log-backed leaf pages are flushed before we publish an index
	// commit that references them. Per-root storage policies can use the leaf
	// page log even when the DB-level default stores outer leaves in index pages.
	if db.leafPageLog != nil {
		if sync {
			if err := db.leafPageLog.Sync(); err != nil {
				return err
			}
		} else {
			if err := db.leafPageLog.Flush(); err != nil {
				return err
			}
		}
	}
	if valueLogAppender != nil {
		if sync {
			if err := valueLogAppender.Sync(); err != nil {
				return err
			}
		} else {
			if err := valueLogAppender.Flush(); err != nil {
				return err
			}
		}
	}
	if !sync {
		if err := durabilitycut.EmitBasic(durabilitycut.AfterUserspaceFlush, "", db.dir); err != nil {
			return err
		}
	} else if hasDependencyEvent {
		dependencyEvent.Point = durabilitycut.AfterDependencyFileSync
		if err := durabilitycut.Emit(dependencyEvent); err != nil {
			return err
		}
	}
	// Sync data pages before the meta-page install for synchronous commits. This
	// can run before the final publish validation: a later mismatch merely leaves
	// extra unreferenced data pages/records behind, while the root swap still
	// happens only after validation under commit serialization.
	if sync {
		if err := idx.pager.SyncIndexData(); err != nil {
			return err
		}
	}
	return nil
}

// flushRootPublicationClosureDurability makes the complete dependency union
// for a coalesced publication durable. In particular, a later candidate may
// use a different value-log appender/lane after rotation, so syncing only the
// frontier appender is insufficient.
func (db *DB) flushRootPublicationClosureDurability(idx *indexGen, candidates []*PreparedRootCandidate, indexSnapshots ...*pager.IndexPageSnapshot) error {
	if idx == nil {
		return errors.New("missing index")
	}
	dependencySet := make(map[string]struct{})
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		if candidate.DependencyPath != "" {
			dependencySet[candidate.DependencyPath] = struct{}{}
		}
		for _, path := range candidate.DependencyPaths {
			if path != "" {
				dependencySet[path] = struct{}{}
			}
		}
	}
	dependencyPaths := make([]string, 0, len(dependencySet))
	for path := range dependencySet {
		dependencyPaths = append(dependencyPaths, path)
	}
	sort.Strings(dependencyPaths)
	if len(dependencyPaths) != 0 {
		if err := durabilitycut.Emit(durabilitycut.Event{Point: durabilitycut.BeforeDependencyFileSync, Root: db.dir, Paths: dependencyPaths}); err != nil {
			return err
		}
		for _, path := range dependencyPaths {
			f, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				return fmt.Errorf("sync root publication dependency %q: %w", path, err)
			}
			syncErr := f.Sync()
			closeErr := f.Close()
			if syncErr != nil || closeErr != nil {
				return fmt.Errorf("sync root publication dependency %q: %w", path, errors.Join(syncErr, closeErr))
			}
		}
	}
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		// Candidate preparation already drained userspace buffers before the
		// dependency path snapshot was captured. Sync those immutable path
		// identities directly: the installed appender objects remain live and a
		// later visible commit may be appending through them concurrently.
		// A drained leaf log is fenced through its captured paths above. Calling
		// Sync on the installed writer would race a successor commit appending to
		// that same mutable object. Legacy/test candidates without a flushed path
		// snapshot retain the conservative live-writer fallback.
		if candidate.LeafPageLog != nil && (!candidate.DependenciesFlushed || len(candidate.DependencyPaths) == 0) {
			var err error
			if durable, ok := candidate.LeafPageLog.(LeafPageLogDurableSyncer); ok {
				err = durable.SyncLeafPageLogDurable()
			} else {
				err = candidate.LeafPageLog.Sync()
			}
			if err != nil {
				return err
			}
		}
		if candidate.ValueLogAppender == nil {
			continue
		}
		fileIDs := append([]uint32(nil), candidate.TouchedValueLogFiles...)
		// A dependency-complete candidate with no external value-log records
		// owns no value-log durability work. Passing an empty list to the
		// appender means "sync every pending lane", which is only the intended
		// conservative fallback for legacy/incomplete candidates.
		if len(fileIDs) == 0 && candidate.DependenciesFlushed {
			continue
		}
		if flusher, ok := candidate.ValueLogAppender.(ValueLogDurableExternalRefFlusher); ok {
			if err := flusher.SyncValueLogExternalRefsDurable(fileIDs); err != nil {
				return err
			}
			continue
		}
		if flusher, ok := candidate.ValueLogAppender.(ValueLogExternalRefFlusher); ok {
			if err := flusher.FlushValueLogExternalRefs(fileIDs, true); err != nil {
				return err
			}
			continue
		}
		if err := candidate.ValueLogAppender.Sync(); err != nil {
			return err
		}
	}
	if len(dependencyPaths) != 0 {
		if err := durabilitycut.Emit(durabilitycut.Event{Point: durabilitycut.AfterDependencyFileSync, Root: db.dir, Paths: dependencyPaths}); err != nil {
			return err
		}
	}
	if len(indexSnapshots) != 0 && indexSnapshots[0] != nil {
		// The immutable snapshot path writes exact non-meta ranges and ends with
		// a file sync. That one boundary makes both a newly grown file size and
		// the captured data stable before the target meta is written; a separate
		// size sync would duplicate the same file-wide durability work.
		return idx.pager.SyncIndexPageSnapshot(indexSnapshots[0])
	}
	// Legacy/test callers that sync mapped ranges still need a durable file size
	// before a range operation can safely cover newly grown pages.
	if err := idx.pager.SyncIndexFileSize(); err != nil {
		return err
	}
	return idx.pager.SyncIndexPages(rootPublicationIndexPages(candidates))
}

func rootPublicationIndexPages(candidates []*PreparedRootCandidate) []uint64 {
	pageSet := make(map[uint64]struct{})
	var frontierFreelistHeadID uint64
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		frontierFreelistHeadID = candidate.FreelistHeadID
		for _, pageID := range candidate.TouchedIndexPages {
			pageSet[pageID] = struct{}{}
		}
		// A coalesced frontier only needs the pages that remain reachable at
		// that frontier. A later candidate can retire a page first touched by
		// an earlier candidate; writing the earlier image after publication
		// could otherwise overwrite a page that has already been recycled.
		for _, pageID := range candidate.RetiredPages {
			delete(pageSet, pageID)
		}
	}
	// Freelist heads are mutable metadata, not COW tree output. Capturing every
	// historical head in a coalesced closure is unsafe because a later candidate
	// may have recycled an old head as a tree page. The frontier head contains the
	// complete allocator state needed by the target meta.
	if frontierFreelistHeadID != 0 {
		pageSet[frontierFreelistHeadID] = struct{}{}
	}
	pageIDs := make([]uint64, 0, len(pageSet))
	for pageID := range pageSet {
		pageIDs = append(pageIDs, pageID)
	}
	sort.Slice(pageIDs, func(i, j int) bool { return pageIDs[i] < pageIDs[j] })
	return pageIDs
}

func (db *DB) finalizeDependencySyncEvent(valueLogAppender ValueLogAppender, includeValueLog bool) (durabilitycut.Event, bool, error) {
	// The ordinary KV path has no outer-leaf log and exactly one current value
	// log lane. Preserve that singular identity without allocating a temporary
	// map and slice on every visible commit.
	if db.leafPageLog == nil {
		if valueLogAppender == nil || !includeValueLog {
			return durabilitycut.Event{}, false, nil
		}
		if path, _, ok := valueLogAppender.CurrentValueLogSegment(); ok && path != "" {
			return durabilitycut.Event{
				Resource: durabilitycut.ResourceValueLog,
				Root:     db.dir,
				Path:     path,
			}, true, nil
		}
		return durabilitycut.Event{}, false, nil
	}
	eventsByPath := make(map[string]durabilitycut.Event)
	if db.leafPageLog != nil {
		segments, err := leafPageLogCurrentSegments(db.leafPageLog)
		if err != nil {
			return durabilitycut.Event{}, false, fmt.Errorf("durability cut: list leaf-page dependency paths: %w", err)
		}
		for _, segment := range segments {
			if segment.Path != "" {
				eventsByPath[segment.Path] = durabilitycut.Event{Resource: durabilitycut.ResourceOuterLeaf, Root: db.dir, Path: segment.Path}
			}
		}
	}
	if valueLogAppender != nil && includeValueLog {
		if path, _, ok := valueLogAppender.CurrentValueLogSegment(); ok && path != "" {
			eventsByPath[path] = durabilitycut.Event{Resource: durabilitycut.ResourceValueLog, Root: db.dir, Path: path}
		}
	}
	paths := make([]string, 0, len(eventsByPath))
	for path := range eventsByPath {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return durabilitycut.Event{}, false, nil
	}
	event := durabilitycut.Event{Root: db.dir, Paths: paths}
	if len(paths) == 1 {
		event.Resource = eventsByPath[paths[0]].Resource
	}
	return event, true, nil
}

func (db *DB) prepareFinalizeCommitDurability(sync bool) (*finalizeCommitPrepareGuard, error) {
	if db == nil {
		return nil, ErrClosed
	}
	if db.readOnly {
		return nil, ErrReadOnly
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return nil, err
	}
	idx := db.idx.Load()
	if idx == nil {
		return nil, errors.New("missing index")
	}
	valueLogAppender := db.currentValueLogAppender()
	db.publishPrepareMu.RLock()
	guard := &finalizeCommitPrepareGuard{db: db}
	// Candidate preparation drains userspace buffers only. Stable file and index
	// fences belong exclusively to RootPublicationCoordinator after all commit
	// and writer serialization has been released.
	if err := db.flushFinalizeCommitDurability(idx, valueLogAppender, false); err != nil {
		guard.Release()
		return nil, wrapFinalizeCommitError(err, true)
	}
	return guard, nil
}
