package db

import (
	"context"
)

// LeafPageLogGenerationHandoff owns only the short physical producer cut.
// The backend calls Advance while teardown, command publication and root
// builders are quiescent. Release must also run before an unlocked staged drain.
// A handoff is transient and must never be stored in a prepared publication.
type LeafPageLogGenerationHandoff interface {
	AdvanceLeafPageLogGeneration(context.Context) error
	Release()
}

// LeafPageLogGenerationHandoffProvider is the installed cached producer's
// maintenance bridge. Begin drains cached flush and foreground owners before
// the backend takes its publication boundary. A nil handoff preserves an
// owner's existing generation handoff (for example the COW frontier).
type LeafPageLogGenerationHandoffProvider interface {
	BeginLeafPageLogGenerationHandoff(context.Context) (LeafPageLogGenerationHandoff, error)
}

// LeafPageLogRetiredSegmentObserver forgets producer accounting only after
// existing GC has confirmed physical removal. It grants no deletion authority
// and must perform no storage I/O or publication.
type LeafPageLogRetiredSegmentObserver interface {
	LeafPageLogSegmentsRetired([]LeafPageLogSegment)
}

func leafPageLogInstalledProducer(log LeafPageLog) LeafPageLog {
	if wrapped, ok := log.(*leafPageLogWithRecordLengthHints); ok {
		return leafPageLogInstalledProducer(wrapped.inner)
	}
	return log
}

func leafPageLogGenerationHandoffProvider(log LeafPageLog) LeafPageLogGenerationHandoffProvider {
	provider, _ := leafPageLogInstalledProducer(log).(LeafPageLogGenerationHandoffProvider)
	return provider
}

// AdvanceLeafPageLogGeneration seals the installed cached native producer's
// current physical writers before explicit leaf maintenance. It changes no
// rollover threshold and runs no live-tree scan, checkpoint, pack or GC while
// foreground publication is stopped. Caller-owned leaf logs keep their own
// generation policy.
func (db *DB) AdvanceLeafPageLogGeneration(ctx context.Context) error {
	if err := db.CheckStorageMaintenanceReady(); err != nil {
		return err
	}
	if !db.indexOuterLeavesInValueLog {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Installation can change during an unlocked drain. Read its version
		// under the same lock as SetLeafPageLog, then revalidate the final cut.
		db.writeMu.Lock()
		provider := leafPageLogGenerationHandoffProvider(db.leafPageLog)
		version := db.leafPageLogVersion
		db.writeMu.Unlock()
		if provider == nil {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		owner, err := provider.BeginLeafPageLogGenerationHandoff(ctx)
		if err != nil || owner == nil {
			return err
		}
		// Match checkpoint/native publisher order. Cached ownership precedes
		// these leases: foreground cached command append owns cached writeMu
		// before raw publication, while background flush owns flushMu.
		db.teardownMu.RLock()
		unlockAdmission := db.lockCommandWALQuiescentAdmission()
		unlockRaw := db.lockCommandWALRawPublish()
		release := func() {
			unlockRaw()
			unlockAdmission()
			db.teardownMu.RUnlock()
			owner.Release()
		}
		if err := ctx.Err(); err != nil {
			release()
			return err
		}
		if err := db.CheckStorageMaintenanceReady(); err != nil {
			release()
			return err
		}
		if err := db.runCommandWALRawPublishBarriers(); err != nil {
			// The collection drain may need cached or native publication. Drop
			// BOTH owners, drain normally, then reacquire and revalidate the cut.
			release()
			drain := commandWALPendingDrain(err)
			if drain == nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := drain.Drain(); err != nil {
				return err
			}
			continue
		}
		db.writeMu.Lock()
		if db.leafPageLogVersion != version {
			db.writeMu.Unlock()
			release()
			continue
		}
		err = db.CheckStorageMaintenanceReady()
		if err == nil {
			err = owner.AdvanceLeafPageLogGeneration(ctx)
		}
		registered := false
		if err == nil {
			registered, err = db.registerLeafPageLogSegmentsForPublish()
		}
		if err == nil && registered {
			// Consume the exact created/current inventory before reclamation can
			// remove an old file. Pending registration must not resurrect it.
			// This uses registered files, not a directory or live-tree scan.
			commitSeq := uint64(1)
			if state, ok := db.StateToken(); ok && state.CommitSeq != 0 {
				commitSeq = state.CommitSeq
			}
			err = db.noteLeafGenerationPendingFileIDs(0, commitSeq)
		}
		if err == nil && registered {
			err = db.publishValueLogSetNoRefresh()
		}
		db.writeMu.Unlock()
		release()
		// Partial rotation/registration failures retain their existing owners
		// and abort this maintenance call. Never turn an error into GC authority.
		return err
	}
}

// advanceLeafPageLogGenerationForMaintenance preserves the caller's footprint
// refusal before any producer effect. Admission runs outside the short producer
// cut; planning, pack and GC still repeat their own captured-input checks.
func (db *DB) advanceLeafPageLogGenerationForMaintenance(ctx context.Context, limits LeafGenerationMaintenanceLimits) error {
	if err := db.CheckStorageMaintenanceReady(); err != nil {
		return err
	}
	if !db.indexOuterLeavesInValueLog {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := db.admitLeafGenerationMaintenance(ctx, limits); err != nil {
		return err
	}
	return db.AdvanceLeafPageLogGeneration(ctx)
}
