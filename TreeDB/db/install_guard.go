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
type dbInstallGuardFailureCause uint8

const (
	dbInstallGuardRawBatch         dbInstallGuardKind = "raw_batch"
	dbInstallGuardOrderedRootGroup dbInstallGuardKind = "ordered_root_delta_group"
)

const (
	dbInstallGuardFailureNone dbInstallGuardFailureCause = iota
	dbInstallGuardFailureHook
	dbInstallGuardFailureUserRoot
	dbInstallGuardFailureSystemRoot
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
	cause := dbInstallGuardFailureNone
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
		if err != nil {
			cause = dbInstallGuardFailureHook
		}
	}
	if err == nil {
		cause, err = db.checkInstallGuard(guard)
	}
	elapsed := elapsedDurationNs(start)
	if db != nil {
		db.publishInstallGuardCalls.Add(1)
		db.publishInstallGuardNs.Add(elapsed)
		if err != nil {
			db.publishInstallGuardFailures.Add(1)
			switch cause {
			case dbInstallGuardFailureHook:
				db.publishInstallGuardHookFailures.Add(1)
			case dbInstallGuardFailureUserRoot:
				db.publishInstallGuardUserRootMismatches.Add(1)
			case dbInstallGuardFailureSystemRoot:
				db.publishInstallGuardSystemRootMismatches.Add(1)
			}
		}
	}
	return elapsed, err
}

func (db *DB) checkInstallGuard(guard dbInstallGuard) (dbInstallGuardFailureCause, error) {
	if db == nil {
		return dbInstallGuardFailureNone, ErrClosed
	}
	db.mu.RLock()
	currentUserRoot := db.meta.UserRootPageID
	currentSystemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if guard.checkUserRoot && currentUserRoot != guard.userRoot {
		return dbInstallGuardFailureUserRoot, fmt.Errorf("%w: user root changed from %d to %d", ErrInstallGuardMismatch, guard.userRoot, currentUserRoot)
	}
	if guard.checkSystemRoot && currentSystemRoot != guard.systemRoot {
		return dbInstallGuardFailureSystemRoot, fmt.Errorf("%w: system root changed from %d to %d", ErrInstallGuardMismatch, guard.systemRoot, currentSystemRoot)
	}
	return dbInstallGuardFailureNone, nil
}
