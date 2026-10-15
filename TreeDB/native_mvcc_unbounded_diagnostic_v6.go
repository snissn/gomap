package treedb

import backenddb "github.com/snissn/gomap/TreeDB/db"

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
