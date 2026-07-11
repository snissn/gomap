package db

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// finalizeCommitPrepareGuard keeps value-log GC from scanning reachability and
// deleting freshly flushed publish output before the root that references it is
// either installed or abandoned.
type finalizeCommitPrepareGuard struct {
	db *DB
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
		dependencyEvent, hasDependencyEvent, err = db.finalizeDependencySyncEvent(valueLogAppender)
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
		indexPath := filepath.Join(db.dir, "index.db")
		if err := durabilitycut.EmitPath(durabilitycut.BeforeIndexDataSync, durabilitycut.ResourceIndex, db.dir, indexPath); err != nil {
			return err
		}
		if err := idx.pager.Sync(); err != nil {
			return err
		}
		if err := durabilitycut.EmitPath(durabilitycut.AfterIndexDataSync, durabilitycut.ResourceIndex, db.dir, indexPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) finalizeDependencySyncEvent(valueLogAppender ValueLogAppender) (durabilitycut.Event, bool, error) {
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
	if valueLogAppender != nil {
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
	if err := db.flushFinalizeCommitDurability(idx, valueLogAppender, sync); err != nil {
		guard.Release()
		return nil, wrapFinalizeCommitError(err, true)
	}
	return guard, nil
}
