package db

import (
	"context"
	"runtime"
)

// Test-only access to existing maintenance and RWMutex admission witnesses.
// Installation/restoration happens outside the witnessed operation lifetime.
func (db *DB) SetVectorPrepareMaintenanceWitnessForTestingV1(hook func(string) error) func() {
	previous := db.testStorageMaintenanceAfterLockHook
	db.testStorageMaintenanceAfterLockHook = hook
	return func() { db.testStorageMaintenanceAfterLockHook = previous }
}

func (db *DB) WaitVectorPrepareTeardownWriterForTestingV1(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !db.teardownMu.TryRLock() {
			return nil
		}
		db.teardownMu.RUnlock()
		runtime.Gosched()
	}
}
