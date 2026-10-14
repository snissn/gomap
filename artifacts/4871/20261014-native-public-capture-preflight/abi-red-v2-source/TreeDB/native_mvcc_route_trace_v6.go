//go:build treedb_test

package treedb

import (
 "unsafe"
 "github.com/snissn/gomap/TreeDB/internal/m7routetrace"
)

// BeginMaintenanceRouteTraceForTest scopes the existing diagnostic observer to
// numeric identities of the real public operation. It grants no work authority.
func (db *DB) BeginMaintenanceRouteTraceForTest(store uintptr) *m7routetrace.Observer {
 db.lifecycleMu.RLock()
 backend := uintptr(unsafe.Pointer(db.backend))
 db.lifecycleMu.RUnlock()
 return m7routetrace.Begin(store, uintptr(unsafe.Pointer(db)), backend)
}
