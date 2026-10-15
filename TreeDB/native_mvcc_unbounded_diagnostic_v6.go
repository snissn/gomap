package treedb

import (
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// PublishNativeMVCCUnboundedDiagnosticV6 is the internal diagnostic bridge;
// bounded public cursors never select it implicitly.
func (db *DB) PublishNativeMVCCUnboundedDiagnosticV6(c *backenddb.NativeMVCCCurrentDeleteV6) error {
	db.lifecycleMu.RLock()
	defer db.lifecycleMu.RUnlock()
	if db.cached == nil {
		return ErrClosed
	}
	return db.cached.PublishNativeMVCCUnboundedDiagnosticV6(c)
}

// StepNativeMVCCUnboundedDiagnosticOutputV6 retains only admitted private DATA
// construction. It is not a bounded final-publication policy.
func (db *DB) StepNativeMVCCUnboundedDiagnosticOutputV6(c *backenddb.NativeMVCCCurrentDeleteV6, w *iterator.OrdinalScanWork) (bool, error) {
	db.lifecycleMu.RLock()
	defer db.lifecycleMu.RUnlock()
	if db.cached == nil {
		return false, ErrClosed
	}
	return db.cached.StepNativeMVCCUnboundedDiagnosticOutputV6(c, w)
}
