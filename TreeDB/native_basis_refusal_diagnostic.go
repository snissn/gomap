package treedb

import "github.com/snissn/gomap/TreeDB/internal/valuelog"

// EnableNativeBasisRefusalObserverForTest belongs only to the diagnostic
// overlay. The fixture calls it with sole opening-lifecycle ownership, before
// native work or any concurrent Close. It grants no backend/snapshot authority,
// changes no Open option, and acquires no new observation lock.
func (db *DB) EnableNativeBasisRefusalObserverForTest(observer *valuelog.COWBasisRefusalObserver) bool {
	if db == nil || db.cached == nil {
		return false
	}
	return db.cached.EnableNativeBasisRefusalObserverForTest(db.txnOwner, observer)
}
