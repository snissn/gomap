package db

import "github.com/snissn/gomap/TreeDB/zipper"

const flushApplyReadOnlyPrepareBufferKeep = 4

type flushApplyReadOnlyPrepareBuffer struct {
	opts zipper.ReadOnlyPrepareOptions
}

func (db *DB) acquireFlushApplyReadOnlyPrepareBuffer(opts zipper.ApplyOptions) *flushApplyReadOnlyPrepareBuffer {
	if db == nil || (!opts.PrepareReadOnly && opts.ParallelApplyConcurrency <= 1) {
		return nil
	}
	db.flushApplyReadOnlyPrepareMu.Lock()
	n := len(db.flushApplyReadOnlyPrepareFree)
	if n > 0 {
		buf := db.flushApplyReadOnlyPrepareFree[n-1]
		db.flushApplyReadOnlyPrepareFree[n-1] = nil
		db.flushApplyReadOnlyPrepareFree = db.flushApplyReadOnlyPrepareFree[:n-1]
		db.flushApplyReadOnlyPrepareMu.Unlock()
		if buf != nil {
			return buf
		}
		return &flushApplyReadOnlyPrepareBuffer{}
	}
	db.flushApplyReadOnlyPrepareMu.Unlock()
	return &flushApplyReadOnlyPrepareBuffer{}
}

func (db *DB) clearFlushApplyReadOnlyPrepareBuffers() {
	if db == nil {
		return
	}
	db.flushApplyReadOnlyPrepareMu.Lock()
	clear(db.flushApplyReadOnlyPrepareFree)
	db.flushApplyReadOnlyPrepareFree = nil
	db.flushApplyReadOnlyPrepareMu.Unlock()
}

func (db *DB) releaseFlushApplyReadOnlyPrepareBuffer(buf *flushApplyReadOnlyPrepareBuffer, result *zipper.ApplyResult) {
	if db == nil || buf == nil {
		return
	}
	if result == nil {
		buf.opts = zipper.ReadOnlyPrepareOptions{}
	} else {
		prepared := result.ReadOnlyPrepare
		prepared.ResetForReuse()
		buf.opts = prepared.ReuseOptions()
		// Drop the caller's reference to buffers that may now be reused by a later
		// apply attempt. Callers must observe any read-only prepare stats before
		// returning the buffer.
		result.ReadOnlyPrepare = zipper.ReadOnlyPrepareResult{}
	}
	db.flushApplyReadOnlyPrepareMu.Lock()
	if len(db.flushApplyReadOnlyPrepareFree) < flushApplyReadOnlyPrepareBufferKeep {
		db.flushApplyReadOnlyPrepareFree = append(db.flushApplyReadOnlyPrepareFree, buf)
		db.flushApplyReadOnlyPrepareMu.Unlock()
		return
	}
	db.flushApplyReadOnlyPrepareMu.Unlock()
}
