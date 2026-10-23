package db

import (
	"errors"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// CommandWALCapturedPrefix is a single-use complete-frame durability target.
// Capture owns teardown and independent physical-resource pins until Sync.
// It never borrows mutable journal buffers after capture returns.
type CommandWALCapturedPrefix struct {
	db           *DB
	journal      *commitlog.CapturedSync
	dependencies *rootpublication.StableResourceSet
	result       CommandWALBarrierResult
	once         sync.Once
	err          error
}

// CaptureCommandWALPrefix drains publishers, appends the existing durable V2
// barrier when necessary, flushes its complete bytes and pins the dependency
// closure under append authority. Captured WAL sync is deferred to target.Sync.
// Every successful capture must be consumed by Sync; abandoning it pins Close.
func (db *DB) CaptureCommandWALPrefix() (_ *CommandWALCapturedPrefix, result CommandWALBarrierResult, err error) {
	unlock, err := db.LockCommandWALPublishWithBarriers()
	if err != nil {
		return nil, result, err
	}
	retained := false
	defer func() {
		if !retained {
			unlock()
		}
	}()
	if db == nil || !db.commandWAL || db.commandJournal == nil {
		return nil, result, ErrCommandWALUnsupported
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return nil, result, err
	}
	next := db.CommandWALNextLSN()
	if next > 1 && db.commandWALDurableLSN.Load() < next-1 {
		result.AttemptedDependencyEntries = db.commandWALDebt.entryCountThrough(next - 1)
		// Existing V2 durable markers can reach storage before their final
		// fsync. Preserve the format's dependency-before-marker ordering.
		// A future format may defer this I/O behind a separate certificate.
		if err := db.syncCommandWALDependenciesThrough(next-1, nil); err != nil {
			return nil, result, err
		}
	}
	// Union is a borrowed view; the independent clone owns every producer's
	// flush/sync implementation and exact content certificate during overlap.
	frontier := db.CommandWALNextLSN() - 1
	view, err := db.commandWALDebt.resourceViewThrough(frontier)
	if err != nil {
		return nil, result, err
	}
	dependencies, err := rootpublication.CloneStableResourceSetExcludingKinds(view)
	if err != nil {
		return nil, result, err
	}
	if next > 1 && db.commandWALDurableLSN.Load() < next-1 {
		result.LSN, err = db.appendCommandWALPrefixBarrier(true)
		if err != nil {
			dependencies.Release()
			return nil, result, err
		}
	}
	frontier = db.CommandWALNextLSN() - 1
	rotations, err := db.commandWALDebt.rotationFileViewThrough(frontier)
	if err != nil {
		dependencies.Release()
		return nil, result, err
	}
	journal, err := db.commandJournal.CaptureSync(rotations)
	if err != nil {
		dependencies.Release()
		db.commandWALFlushPoisoned.Store(true)
		return nil, result, err
	}
	target := &CommandWALCapturedPrefix{db: db, journal: journal, dependencies: dependencies, result: result}
	// Keep teardown pinned, but admit new prepared publishers and raw appends.
	db.commandWALRawPublishMu.Unlock()
	db.commandWALRawAdmissionMu.Unlock()
	retained = true
	return target, result, nil
}

// Sync certifies only the captured prefix after all captured persistent value
// and auxiliary dependencies. A failure is commit-ambiguous and poisons later
// acceptance; every caller receives the same single-use result.
func (target *CommandWALCapturedPrefix) Sync() (CommandWALBarrierResult, error) {
	target.once.Do(func() {
		db := target.db
		defer db.teardownMu.RUnlock()
		defer target.journal.Release()
		defer target.dependencies.Release()
		started := time.Now()
		err := target.journal.Sync()
		if err == nil && db.testFailCommandWALFlush.Load() {
			err = errTestCommandWALFlushFailpoint
		}
		db.observeCommandWALFlush(commandWALAppendStatsBarrier, true, time.Since(started))
		// Publish completion under the same append authority used by capture. Pins
		// remain alive until this update; no successor can lose borrowed debt.
		db.commandWALRawPublishMu.Lock()
		if err != nil {
			db.commandWALFlushPoisoned.Store(true)
			target.err = errors.Join(err, ErrRecoveryRequired)
		} else if poison := db.commandWALPoisonedError(); poison != nil {
			target.err = poison
		} else {
			db.closeCommandWALDurablePrefixThrough(target.journal.LSN)
			target.result.CoveredDependencyEntries = target.result.AttemptedDependencyEntries
		}
		db.commandWALRawPublishMu.Unlock()
	})
	return target.result, target.err
}
