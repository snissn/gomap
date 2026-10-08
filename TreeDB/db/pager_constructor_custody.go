package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/allocclass"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/residentcredit"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/pager"
	"unsafe"
)

// A failed constructor returns a nonnil actual DB only while physical cleanup
// remains pending. Callers must retry Close even when Open also returns an error.
// No constructor debt lives in a global queue or an error-only scalar wrapper.
func constructorPagerFailure(p *pager.Pager, vm *valuelog.Manager, lock *lockfile.Lock, owner *nativePublicationResidentOwner, control *residentcredit.Scope, cause error) (*DB, error) {
	var cleanup error
	if vm != nil {
		if err := vm.Close(); err != nil {
			cleanup = errors.Join(cleanup, err)
		} else {
			vm = nil
		}
	}
	if p != nil {
		if err := p.Close(); err != nil {
			cleanup = errors.Join(cleanup, err)
		} else {
			p = nil
		}
	}
	// Keep the actual namespace lock while any physical constructor owner remains.
	if cleanup == nil && lock != nil {
		if err := lock.Close(); err != nil {
			cleanup = err
		} else {
			lock = nil
		}
	}
	// The calling constructor defer still owns control/master on a clean failure.
	if cleanup == nil {
		return nil, cause
	}
	return &DB{nativePublicationResident: owner, constructorCreator: control, constructorPager: p, constructorOnly: true, valueLogManager: vm, lock: lock}, errors.Join(cause, cleanup)
}

func (db *DB) closePartialConstructor() error {
	db.closing.Store(true)
	var cleanup error
	if db.valueLogManager != nil {
		if err := db.valueLogManager.Close(); err != nil {
			cleanup = errors.Join(cleanup, err)
		} else {
			db.valueLogManager = nil
		}
	}
	if db.constructorPager != nil {
		if err := db.constructorPager.Close(); err != nil {
			cleanup = errors.Join(cleanup, err)
		} else {
			db.constructorPager = nil
		}
	}
	if cleanup == nil && db.lock != nil {
		if err := db.lock.Close(); err != nil {
			cleanup = err
		} else {
			db.lock = nil
		}
	}
	if cleanup == nil {
		db.releaseConstructorControl()
		db.closeNativePublicationResidentOwnerV1()
	}
	return cleanup
}

// failedInstalledOpen propagates the actual fully installed DB whenever Close
// fails. A successfully cleaned constructor failure remains the usual nil DB.
func failedInstalledOpen(database *DB, cause error) (*DB, error) {
	if err := database.Close(); err != nil {
		return database, errors.Join(cause, err)
	}
	return nil, cause
}

func prepayDBConstructorControl(owner *nativePublicationResidentOwner, facadeRaw uint64) (*residentcredit.Scope, error) {
	scope, err := (*residentcredit.Owner)(owner).NewOrdinaryScope()
	if err != nil {
		return nil, err
	}
	class, err := allocclass.ClassBytes(uint64(unsafe.Sizeof(DB{})), true)
	if err != nil {
		class = uint64(unsafe.Sizeof(DB{}))
	}
	if err := scope.ReserveStableMetadata(class); err != nil {
		scope.ReleaseStableMetadata()
		return nil, err
	}
	if facadeRaw != 0 {
		class, err := allocclass.ClassBytes(facadeRaw, true)
		if err != nil {
			class = facadeRaw
		}
		if err := scope.ReserveStableMetadata(class); err != nil {
			scope.ReleaseStableMetadata()
			return nil, err
		}
	}
	return scope, nil
}
func (db *DB) releaseConstructorControl() {
	db.mu.Lock()
	control := db.constructorCreator
	db.constructorCreator = nil
	db.mu.Unlock()
	if control != nil {
		control.ReleaseStableMetadata()
	}
}
