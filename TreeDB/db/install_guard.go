package db

import (
	"errors"
	"fmt"
	"time"
)

// ErrInstallGuardMismatch marks a guarded install whose captured root state no
// longer matches the DB's current roots. Callers can retry after replanning.
var ErrInstallGuardMismatch = errors.New("treedb: install guard mismatch")

type dbInstallGuardKind string

const (
	dbInstallGuardRawBatch         dbInstallGuardKind = "raw_batch"
	dbInstallGuardOrderedRootGroup dbInstallGuardKind = "ordered_root_delta_group"
)

type dbInstallGuard struct {
	kind            dbInstallGuardKind
	userRoot        uint64
	systemRoot      uint64
	checkUserRoot   bool
	checkSystemRoot bool
}

type dbInstallGuardHookEvent struct {
	Kind            dbInstallGuardKind
	UserRoot        uint64
	SystemRoot      uint64
	CheckUserRoot   bool
	CheckSystemRoot bool
}

func rawBatchInstallGuard(userRoot uint64) dbInstallGuard {
	return dbInstallGuard{
		kind:          dbInstallGuardRawBatch,
		userRoot:      userRoot,
		checkUserRoot: true,
	}
}

func orderedRootDeltaGroupInstallGuard(userRoot, systemRoot uint64) dbInstallGuard {
	return dbInstallGuard{
		kind:            dbInstallGuardOrderedRootGroup,
		userRoot:        userRoot,
		systemRoot:      systemRoot,
		checkUserRoot:   true,
		checkSystemRoot: true,
	}
}

func orderedRootDeltaGroupSystemInstallGuard(systemRoot uint64) dbInstallGuard {
	return dbInstallGuard{
		kind:            dbInstallGuardOrderedRootGroup,
		systemRoot:      systemRoot,
		checkSystemRoot: true,
	}
}

func (db *DB) runInstallGuard(guard dbInstallGuard) (uint64, error) {
	start := time.Now()
	var err error
	if db == nil {
		err = ErrClosed
	} else if hook := db.testInstallGuardHook; hook != nil {
		err = hook(dbInstallGuardHookEvent{
			Kind:            guard.kind,
			UserRoot:        guard.userRoot,
			SystemRoot:      guard.systemRoot,
			CheckUserRoot:   guard.checkUserRoot,
			CheckSystemRoot: guard.checkSystemRoot,
		})
	}
	if err == nil {
		err = db.checkInstallGuard(guard)
	}
	elapsed := elapsedDurationNs(start)
	if db != nil {
		db.publishInstallGuardCalls.Add(1)
		db.publishInstallGuardNs.Add(elapsed)
		if err != nil {
			db.publishInstallGuardFailures.Add(1)
		}
	}
	return elapsed, err
}

func (db *DB) checkInstallGuard(guard dbInstallGuard) error {
	if db == nil {
		return ErrClosed
	}
	db.mu.RLock()
	currentUserRoot := db.meta.UserRootPageID
	currentSystemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if guard.checkUserRoot && currentUserRoot != guard.userRoot {
		return fmt.Errorf("%w: user root changed from %d to %d", ErrInstallGuardMismatch, guard.userRoot, currentUserRoot)
	}
	if guard.checkSystemRoot && currentSystemRoot != guard.systemRoot {
		return fmt.Errorf("%w: system root changed from %d to %d", ErrInstallGuardMismatch, guard.systemRoot, currentSystemRoot)
	}
	return nil
}
