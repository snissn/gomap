package db

import (
	"errors"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

// CommandWALCapturedPrefix is a single-use complete-frame durability target.
// Capture owns teardown, the original dependency-debt retention lease and an
// exact active-file pin until Sync.
// It never borrows mutable journal buffers after capture returns.
type CommandWALCapturedPrefix struct {
	db             *DB
	journal        *commitlog.CapturedSync
	journalCapture commitlog.CapturedSync
	result         CommandWALBarrierResult
	once           sync.Once
	err            error
}

// CaptureCommandWALPrefix drains publishers, appends the existing durable V2
// barrier when necessary, flushes its complete bytes and pins the dependency
// closure under append authority. Captured WAL sync is deferred to target.Sync.
// Every successful capture must be consumed by Sync; abandoning it pins Close.
func (db *DB) CaptureCommandWALPrefix() (_ *CommandWALCapturedPrefix, result CommandWALBarrierResult, err error) {
	return db.captureCommandWALPrefix(nil, nil)
}

// CaptureCommandWALPrefixInto uses caller-owned, unused storage. The caller
// must own it exclusively until Sync returns, including every deferred pin
// release. Only then may the caller zero it and start a new capture. No waiter
// or other consumer may retain an earlier pointer across that reuse boundary.
// A nil owner selects ordinary authority; native semantic mode still requires
// the original private COW owner. That owner's fully prepared commands need
// teardown and raw append authority, without legacy publisher admission held
// by an overlapping physical checkpoint.
func (db *DB) CaptureCommandWALPrefixInto(target *CommandWALCapturedPrefix, owner *transactionowner.Owner) (*CommandWALCapturedPrefix, CommandWALBarrierResult, error) {
	if target == nil || target.db != nil {
		return nil, CommandWALBarrierResult{}, errors.New("command WAL capture target must be unused")
	}
	if owner != nil && (db == nil || owner != db.privateCOWOwner) {
		return nil, CommandWALBarrierResult{}, ErrNativeSemanticUnsupported
	}
	return db.captureCommandWALPrefix(owner, target)
}

func (db *DB) captureCommandWALPrefix(owner *transactionowner.Owner, target *CommandWALCapturedPrefix) (_ *CommandWALCapturedPrefix, result CommandWALBarrierResult, err error) {
	return db.captureCommandWALPrefixAfterAppend(owner, target, 0)
}

// The exact already-assigned identity enables only failure poisoning. Capture
// always reacquires its original authority, reruns hooks and samples a fresh cut.
func (db *DB) captureCommandWALPrefixAfterAppend(owner *transactionowner.Owner, target *CommandWALCapturedPrefix, assignedLSN uint64) (_ *CommandWALCapturedPrefix, result CommandWALBarrierResult, err error) {
	// The fixed COW owner appends only fully prepared commands under its typed
	// staging guard. As with LockPrivateCOWStagingGuard, it needs raw append
	// authority, not legacy prepared-publisher admission. A physical checkpoint
	// can retain that admission through I/O while COW successor cuts advance.
	privateOwner := db != nil && owner != nil && owner == db.privateCOWOwner
	if privateOwner {
		db.teardownMu.RLock()
		if db.closing.Load() {
			db.teardownMu.RUnlock()
			return nil, result, ErrClosed
		}
		db.commandWALRawPublishMu.Lock()
	} else if err = db.acquireCommandWALPublishWithBarriersAfterAppend(owner, assignedLSN); err != nil {
		return nil, result, err
	}
	retained := false
	defer func() {
		recovered := recover()
		if !retained {
			if assignedLSN != 0 && (err != nil || recovered != nil) {
				db.commandWALFlushPoisoned.Store(true)
			}
			db.commandWALRawPublishMu.Unlock()
			if !privateOwner {
				db.commandWALRawAdmissionMu.Unlock()
			}
			db.teardownMu.RUnlock()
		}
		if recovered != nil {
			panic(recovered)
		}
	}()
	target, result, err = db.captureCommandWALPrefixLocked(owner, target)
	if err != nil {
		return nil, result, err
	}
	// Transfer the original teardown lease; Sync releases it exactly once.
	db.commandWALRawPublishMu.Unlock()
	if !privateOwner {
		db.commandWALRawAdmissionMu.Unlock()
	}
	retained = true
	return target, result, nil
}

// captureCommandWALPrefixLocked samples the actual journal/dependency frontier
// under existing teardown/raw publication custody. The caller retains ownership
// of those locks on failure; success transfers only the captured resources, and
// the caller must transfer its original teardown lease to target.Sync.
func (db *DB) captureCommandWALPrefixLocked(owner *transactionowner.Owner, target *CommandWALCapturedPrefix) (_ *CommandWALCapturedPrefix, result CommandWALBarrierResult, err error) {
	if db.closing.Load() {
		return nil, result, ErrClosed
	}
	if db == nil || !db.commandWAL || db.commandJournal == nil {
		return nil, result, ErrCommandWALUnsupported
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return nil, result, err
	}
	frontier := db.commandJournal.AppendedLSN()
	if frontier != 0 {
		result.AttemptedDependencyEntries = db.commandWALDebt.entryCountThrough(frontier)
	}
	if frontier != 0 && db.commandWALDurableLSN.Load() < frontier && db.commandWALDurableMarkerLSN.Load() < frontier {
		// Existing V2 durable markers can reach storage before their final
		// fsync. Preserve the format's dependency-before-marker ordering.
		// A future format may defer this I/O behind a separate certificate.
		if err := db.syncCommandWALDependenciesThrough(frontier, nil); err != nil {
			return nil, result, err
		}
	}

	if frontier != 0 && db.commandWALDurableLSN.Load() < frontier && db.commandWALDurableMarkerLSN.Load() < frontier {
		result.LSN, err = db.appendCommandWALPrefixBarrierForOwner(true, owner)
		if err != nil {
			return nil, result, err
		}
	}
	// Every earlier rotation was stabilized before the V2 marker (including
	// a rotation performed while appending that marker). Only the active file
	// still needs a physical sync; resyncing retained rotations is redundant.
	if target == nil {
		target = new(CommandWALCapturedPrefix)
	}
	*target = CommandWALCapturedPrefix{db: db, result: result}
	flushStarted := time.Now()
	journal, err := db.commandJournal.CaptureSyncInto(&target.journalCapture, nil)
	db.observeCommandWALFlush(commandWALAppendStatsBarrier, false, time.Since(flushStarted))
	if err != nil {
		db.commandWALFlushPoisoned.Store(true)
		return nil, result, err
	}
	db.commandWALDebt.retainCapture()
	target.journal = journal
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
		defer db.commandWALDebt.releaseCapture()
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
