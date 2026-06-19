package db

import "errors"

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
	// Sync data pages before the meta-page install for synchronous commits. This
	// can run before the final publish validation: a later mismatch merely leaves
	// extra unreferenced data pages/records behind, while the root swap still
	// happens only after validation under commit serialization.
	if sync {
		if err := idx.pager.Sync(); err != nil {
			return err
		}
	}
	return nil
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
