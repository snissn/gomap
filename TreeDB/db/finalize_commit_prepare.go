package db

import "errors"

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

func (db *DB) prepareFinalizeCommitDurability(sync bool) error {
	if db == nil {
		return ErrClosed
	}
	if db.readOnly {
		return ErrReadOnly
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return err
	}
	idx := db.idx.Load()
	if idx == nil {
		return errors.New("missing index")
	}
	valueLogAppender := db.currentValueLogAppender()
	db.publishPrepareMu.Lock()
	defer db.publishPrepareMu.Unlock()
	if err := db.flushFinalizeCommitDurability(idx, valueLogAppender, sync); err != nil {
		return wrapFinalizeCommitError(err, true)
	}
	return nil
}
