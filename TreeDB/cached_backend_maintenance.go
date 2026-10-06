package treedb

import "errors"

func (db *DB) runCachedBackendMaintenance(fn func() error) error {
	if db != nil && db.cached != nil && db.cached.COWMode() {
		return db.cached.RunBackendMaintenance(fn)
	}
	return db.reconcileCachedBackendMaintenance(fn())
}

func (db *DB) reconcileCachedBackendMaintenance(fnErr error) error {
	if db == nil || db.cached == nil {
		return fnErr
	}
	reconcileErr := db.cached.ReconcileAfterBackendMaintenance()
	if fnErr != nil {
		if reconcileErr != nil {
			return errors.Join(fnErr, reconcileErr)
		}
		return fnErr
	}
	return reconcileErr
}
