package db

// StorageMaintenancePlan authorizes a physical storage-maintenance publish.
// Values are opaque; callers must use package-provided constructors so the
// storage-maintenance command-WAL bypass cannot be repurposed for arbitrary
// logical mutations.
type StorageMaintenancePlan interface {
	storageMaintenancePlanKind() string
}

type storageMaintenancePlan struct {
	kind string
}

const columnAssetRewriteStorageMaintenanceKind = "column_asset_rewrite"

// ColumnAssetRewriteStorageMaintenancePlan returns the storage-maintenance plan
// used when column asset rewrite remaps manifests to equivalent copied physical
// refs.
func ColumnAssetRewriteStorageMaintenancePlan() StorageMaintenancePlan {
	return storageMaintenancePlan{kind: columnAssetRewriteStorageMaintenanceKind}
}

func (p storageMaintenancePlan) storageMaintenancePlanKind() string {
	return p.kind
}

func validStorageMaintenancePlan(plan StorageMaintenancePlan) bool {
	if plan == nil {
		return false
	}
	switch plan.storageMaintenancePlanKind() {
	case columnAssetRewriteStorageMaintenanceKind:
		return true
	default:
		return false
	}
}
