package caching

import (
	"fmt"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// DebugValueLogGenerationState exposes cached rewrite-debt state for offline
// forensics against copied DB directories.
type DebugValueLogGenerationState struct {
	RewriteSourceFileIDs     []uint32
	RewriteDebtLedger        []backenddb.ValueLogRewritePlanSegment
	RewriteStagePending      bool
	RewriteStageObservedAtNS int64
}

// DebugValueLogGenerationMaintenanceOptions drives a single cached maintenance
// pass for offline analysis tools.
type DebugValueLogGenerationMaintenanceOptions struct {
	RunGC                 bool
	BypassQuiet           bool
	SkipCheckpoint        bool
	SkipRetainedPruneWait bool
	RewriteDebtDrain      bool
	Source                string
}

func (db *DB) DebugValueLogGenerationState() (DebugValueLogGenerationState, error) {
	if db == nil {
		return DebugValueLogGenerationState{}, fmt.Errorf("cachingdb: nil db")
	}
	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		return DebugValueLogGenerationState{}, err
	}
	ledger, err := db.currentVlogGenerationRewriteLedger()
	if err != nil {
		return DebugValueLogGenerationState{}, err
	}
	stagePending, stageObservedAt, err := db.currentVlogGenerationRewriteStage()
	if err != nil {
		return DebugValueLogGenerationState{}, err
	}
	return DebugValueLogGenerationState{
		RewriteSourceFileIDs:     queue,
		RewriteDebtLedger:        ledger,
		RewriteStagePending:      stagePending,
		RewriteStageObservedAtNS: stageObservedAt,
	}, nil
}

func (db *DB) DebugSetValueLogGenerationRewriteQueue(ids []uint32) error {
	if db == nil {
		return fmt.Errorf("cachingdb: nil db")
	}
	return db.setVlogGenerationRewriteQueue(ids)
}

func (db *DB) DebugSetValueLogGenerationRewriteLedger(segments []backenddb.ValueLogRewritePlanSegment, stagePending bool, stageObservedAtNS int64) error {
	if db == nil {
		return fmt.Errorf("cachingdb: nil db")
	}
	if !stagePending {
		stageObservedAtNS = 0
	} else if stageObservedAtNS <= 0 {
		stageObservedAtNS = time.Now().UnixNano()
	}
	return db.setVlogGenerationRewriteLedgerWithStage(segments, stagePending, stageObservedAtNS)
}

func (db *DB) DebugRunValueLogGenerationMaintenanceOnce(opts DebugValueLogGenerationMaintenanceOptions) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("cachingdb: nil db")
	}
	acquired := db.maybeRunVlogGenerationMaintenanceWithOptions(opts.RunGC, vlogGenerationMaintenanceOptions{
		bypassQuiet:           opts.BypassQuiet,
		skipCheckpoint:        opts.SkipCheckpoint,
		skipRetainedPruneWait: opts.SkipRetainedPruneWait,
		rewriteDebtDrain:      opts.RewriteDebtDrain,
		debugSource:           opts.Source,
	})
	return acquired, nil
}

func (db *DB) DebugWaitValueLogGenerationIdle(timeout time.Duration) error {
	return db.waitValueLogGenerationIdle(timeout)
}
