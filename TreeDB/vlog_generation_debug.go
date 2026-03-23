package treedb

import (
	"fmt"
	"time"

	"github.com/snissn/gomap/TreeDB/caching"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

// ValueLogRewritePlanSegment is re-exported for maintenance/debug tooling.
type ValueLogRewritePlanSegment = treedbdb.ValueLogRewritePlanSegment

// DebugValueLogGenerationState exposes cached rewrite-debt state for offline
// forensics against copied DB directories.
type DebugValueLogGenerationState struct {
	RewriteSourceFileIDs     []uint32
	RewriteDebtLedger        []ValueLogRewritePlanSegment
	RewriteStagePending      bool
	RewriteStageObservedAtNS int64
}

// DebugValueLogGenerationMaintenanceOptions drives one cached maintenance pass.
// This is intended for offline analysis on copied DB directories.
type DebugValueLogGenerationMaintenanceOptions struct {
	RunGC                 bool
	BypassQuiet           bool
	SkipCheckpoint        bool
	SkipRetainedPruneWait bool
	RewriteDebtDrain      bool
	SuppressFollowOn      bool
	Source                string
}

func (db *DB) debugRequireCachedRW() error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.cached == nil {
		return fmt.Errorf("debug value-log generation maintenance requires cached read-write mode")
	}
	return nil
}

func (db *DB) DebugValueLogGenerationState() (DebugValueLogGenerationState, error) {
	if err := db.debugRequireCachedRW(); err != nil {
		return DebugValueLogGenerationState{}, err
	}
	state, err := db.cached.DebugValueLogGenerationState()
	if err != nil {
		return DebugValueLogGenerationState{}, err
	}
	return DebugValueLogGenerationState{
		RewriteSourceFileIDs:     state.RewriteSourceFileIDs,
		RewriteDebtLedger:        state.RewriteDebtLedger,
		RewriteStagePending:      state.RewriteStagePending,
		RewriteStageObservedAtNS: state.RewriteStageObservedAtNS,
	}, nil
}

func (db *DB) DebugSetValueLogGenerationRewriteQueue(ids []uint32) error {
	if err := db.debugRequireCachedRW(); err != nil {
		return err
	}
	return db.cached.DebugSetValueLogGenerationRewriteQueue(ids)
}

func (db *DB) DebugSetValueLogGenerationRewriteLedger(segments []ValueLogRewritePlanSegment, stagePending bool, stageObservedAtNS int64) error {
	if err := db.debugRequireCachedRW(); err != nil {
		return err
	}
	return db.cached.DebugSetValueLogGenerationRewriteLedger(segments, stagePending, stageObservedAtNS)
}

func (db *DB) DebugRunValueLogGenerationMaintenanceOnce(opts DebugValueLogGenerationMaintenanceOptions) (bool, error) {
	if err := db.debugRequireCachedRW(); err != nil {
		return false, err
	}
	return db.cached.DebugRunValueLogGenerationMaintenanceOnce(caching.DebugValueLogGenerationMaintenanceOptions{
		RunGC:                 opts.RunGC,
		BypassQuiet:           opts.BypassQuiet,
		SkipCheckpoint:        opts.SkipCheckpoint,
		SkipRetainedPruneWait: opts.SkipRetainedPruneWait,
		RewriteDebtDrain:      opts.RewriteDebtDrain,
		SuppressFollowOn:      opts.SuppressFollowOn,
		Source:                opts.Source,
	})
}

func (db *DB) DebugWaitValueLogGenerationIdle(timeout time.Duration) error {
	if err := db.debugRequireCachedRW(); err != nil {
		return err
	}
	return db.cached.DebugWaitValueLogGenerationIdle(timeout)
}
