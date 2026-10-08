package caching

import (
	"context"
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type cachingLeafPageLogGenerationHandoff struct {
	db *DB
}

func (db *DB) beginLeafPageLogGenerationHandoff(ctx context.Context) (backenddb.LeafPageLogGenerationHandoff, error) {
	if db == nil || !db.indexOuterLeavesInValueLog {
		return nil, nil
	}
	// COW's installed maintenance owner already drains its frontier and holds
	// flushMu; its existing handoff must not be reacquired recursively here.
	if db.cow != nil {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := lockCheckpointMutexContext(ctx, &db.flushMu); err != nil {
		return nil, err
	}
	if err := lockCheckpointWriteMutexContext(ctx, &db.writeMu); err != nil {
		db.flushMu.Unlock()
		return nil, err
	}
	if db.closing.Load() {
		db.writeMu.Unlock()
		db.flushMu.Unlock()
		return nil, backenddb.ErrClosed
	}
	// This short owner lease also works inside an existing append-only
	// maintenance callback. Do not recursively wait for maintenanceActive or
	// checkpoint, and do not hold these locks across pack/GC's live scan.
	return &cachingLeafPageLogGenerationHandoff{db: db}, nil
}

func (h *cachingLeafPageLogGenerationHandoff) AdvanceLeafPageLogGeneration(ctx context.Context) error {
	if h == nil || h.db == nil {
		return backenddb.ErrClosed
	}
	db := h.db
	if err := ctx.Err(); err != nil {
		return err
	}
	if db.closing.Load() {
		return backenddb.ErrClosed
	}
	maxSeq := db.leafLogAppendSeq.Load()
	dirty := false
	for _, l := range db.leafLogAppendLanesSnapshot() {
		if l == nil {
			continue
		}
		l.vlogMu.Lock()
		if l.vlog != nil && l.vlogLiveBytes.Load() > 0 {
			dirty = true
		}
		if l.vlogSeq > 0 && uint32(l.vlogSeq) > maxSeq {
			maxSeq = uint32(l.vlogSeq)
		}
		l.vlogMu.Unlock()
	}
	// Repeated no-work maintenance must not manufacture empty generations.
	// A dirty frontier seals every physical writer, including idle peers.
	if !dirty {
		return nil
	}
	// The existing advance skips equality. Reserve an UNUSED floor above the
	// complete current/shared frontier, then advance BEFORE enumerating lanes.
	// A lane created afterwards cannot open a writer on the old frontier.
	floor, err := db.reserveLeafLogAppendSequence(maxSeq)
	if err != nil {
		return err
	}
	db.advanceLeafLogAppendSeqAtLeast(int(floor))
	for _, l := range db.leafLogAppendLanesSnapshot() {
		if l == nil {
			continue
		}
		l.vlogMu.Lock()
		oldSeq, hadWriter := l.vlogSeq, l.vlog != nil
		l.vlogMu.Unlock()
		err := db.advanceLeafLogAppendWriterPastObservedSeq(l, int(floor))
		l.vlogMu.Lock()
		if hadWriter && l.vlogSeq > oldSeq {
			l.vlogHandoffTotal.Add(1)
		}
		l.vlogMu.Unlock()
		if err != nil {
			return err
		}
	}
	for _, l := range db.leafLogAppendLanesSnapshot() {
		if l == nil {
			continue
		}
		l.vlogMu.Lock()
		invalid := l.vlog != nil && l.vlogPath != "" && uint32(l.vlogSeq) <= floor
		l.vlogMu.Unlock()
		if invalid {
			return fmt.Errorf("cachingdb: leaf producer writer did not cross generation floor %d", floor)
		}
	}
	return nil
}

func (h *cachingLeafPageLogGenerationHandoff) Release() {
	if h == nil || h.db == nil {
		return
	}
	db := h.db
	h.db = nil
	db.writeMu.Unlock()
	db.flushMu.Unlock()
}
