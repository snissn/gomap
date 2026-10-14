package caching

import (
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"unsafe"
)

// NativeMVCCBudgetV6 returns the SAME cache-lifetime governor. The original
// cache's Close owns its final admission shutdown. Captures keep their actual
// leases and producer metadata enrollments until physical cleanup completes.
// Calling this method creates no snapshot, retention authority or publisher.
func (db *DB) NativeMVCCBudgetV6(w *iterator.OrdinalScanWork) (*memtable.COWBudget, bool, error) {
	if db == nil {
		return nil, false, backenddb.ErrClosed
	}
	if w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof(db.nativeMVCCBudget))+2*uint64(unsafe.Sizeof(db.nativeMVCCLimits))) {
		return nil, false, nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closing.Load() {
		return nil, false, backenddb.ErrClosed
	}
	if db.cow != nil {
		if db.cow.budget == nil {
			return nil, false, ErrCOWUnsupported
		}
		return db.cow.budget, true, nil
	}
	if db.nativeMVCCBudget != nil {
		return db.nativeMVCCBudget, true, nil
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(memtable.COWBudget{}))+uint64(unsafe.Sizeof(db.nativeMVCCLimits))) {
		return nil, false, nil
	}
	budget, err := memtable.NewCOWBudget(db.nativeMVCCLimits)
	if err != nil {
		return nil, false, err
	}
	db.nativeMVCCBudget = budget
	return budget, true, nil
}

// Close runs after the original cache's physical teardown. An outstanding
// capture/producer binding keeps its charged governor alive; shutdown neither
// disposes those physical resources nor refunds their external leases.
func (db *DB) closeNativeMVCCBudgetV6() {
	db.mu.Lock()
	budget := db.nativeMVCCBudget
	db.mu.Unlock()
	if budget != nil {
		budget.Close()
	}
}
