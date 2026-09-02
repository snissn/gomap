package db

import (
	"reflect"

	"github.com/snissn/gomap/TreeDB/internal/storagemaintenance"
)

// StorageMaintenancePlan authorizes a physical storage-maintenance publish. It
// is intentionally opaque at the DB package boundary; recognized values are
// minted only by TreeDB-internal packages and external callers cannot create a
// value that validates the command-WAL maintenance bypass.
type StorageMaintenancePlan interface {
	StorageMaintenancePlanToken() storagemaintenance.Plan
}

func validStorageMaintenancePlan(plan StorageMaintenancePlan) bool {
	if plan == nil {
		return false
	}
	if token, ok := plan.(storagemaintenance.Plan); ok {
		return storagemaintenance.IsColumnAssetRewritePlan(token)
	}
	value := reflect.ValueOf(plan)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		if value.IsNil() {
			return false
		}
	}
	return validStorageMaintenancePlanToken(plan)
}

func validStorageMaintenancePlanToken(plan StorageMaintenancePlan) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return storagemaintenance.IsColumnAssetRewritePlan(plan.StorageMaintenancePlanToken())
}
