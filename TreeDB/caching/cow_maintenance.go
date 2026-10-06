package caching

import "errors"

// RunBackendMaintenance fences cached writers around an existing backend
// operation. Its caller performs the required checkpoint first. COW then
// refreshes the equivalent backend basis before source writes resume.
func (db *DB) RunBackendMaintenance(fn func() error) error {
	return db.runWithBackendMaintenanceOptions(backendMaintenanceOptions{skipCheckpoint: true}, fn)
}

func (db *DB) refreshCOWBackendBasis() error {
	basis, err := db.cow.captureBackendBasis(db.backend.(backendSnapshotProvider))
	if err != nil {
		return err
	}
	old, err := db.cow.installCOWBasis(basis, nil)
	basis.release()
	if err != nil {
		return err
	}
	if old != nil {
		old.drain()
	}
	db.cow.refreshRequired.Store(false)
	return nil
}

func (db *DB) finishCOWBackendMaintenance(fnErr error) error {
	if refresher, ok := db.backend.(valueLogSetRefresher); ok {
		if err := refresher.RefreshValueLogSet(); err != nil {
			return errors.Join(fnErr, err)
		}
	}
	return errors.Join(fnErr, db.refreshCOWBackendBasis())
}
