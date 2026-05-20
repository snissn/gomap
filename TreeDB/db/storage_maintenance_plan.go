package db

import "github.com/snissn/gomap/TreeDB/internal/storagemaintenance"

// StorageMaintenancePlan authorizes a physical storage-maintenance publish. It
// is intentionally opaque at the DB package boundary; recognized values are
// minted only by TreeDB-internal packages and external callers cannot create a
// value that validates the command-WAL maintenance bypass.
type StorageMaintenancePlan any

func validStorageMaintenancePlan(plan StorageMaintenancePlan) bool {
	return storagemaintenance.IsColumnAssetRewritePlan(plan)
}
