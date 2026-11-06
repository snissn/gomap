package caching

import (
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// EnableNativeBasisRefusalObserverForTest is a one-shot diagnostic binding.
// The public owner's private identity must equal this cache's exact identity.
// Installation precedes the fixture's native collection work. The observer is
// never reset or removed; atomic scalar readback needs no borrower-drain guess.
func (db *DB) EnableNativeBasisRefusalObserverForTest(owner *transactionowner.Owner, observer *valuelog.COWBasisRefusalObserver) bool {
	if db == nil || db.cow == nil || owner == nil || db.cow.txnOwner != owner || observer == nil {
		return false
	}
	if !observer.Bind(owner) {
		return false
	}
	return db.nativeBasisRefusalObserver.CompareAndSwap(nil, observer)
}
