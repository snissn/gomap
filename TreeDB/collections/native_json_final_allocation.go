package collections

import (
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Census only final outputs of the maintained builders, after encoding has
// established their exact lengths. No inventory backing or key copy is needed.
// The caller admits the sum before uniqueness maps or any final builder runs;
// the original stage receipt remains live through synchronous publication.
func nativeJSONFinalPlanCharge(name string, opts collectionOptions, runtimes []indexRuntime, changed []preparedBatchUpdate) (charge uint64, stateEntries, roots int, resultErr error) {
	add := func(count int, size uint64) bool {
		if count < 0 || size == 0 || uint64(count) > uint64(int(^uint(0)>>1))/size {
			return false
		}
		n := memtable.COWAllocationCharge(uint64(count) * size)
		if n > ^uint64(0)-charge {
			return false
		}
		charge += n
		return true
	}
	capacity := func() (uint64, int, int, error) { return 0, 0, 0, memtable.ErrCOWCapacity }
	if len(name) > int(^uint(0)>>1)-len("/index-state") {
		return capacity()
	}
	if !add(1, uint64(unsafe.Sizeof(directBufferedInsertPlan{}))) ||
		!add(len(changed), uint64(unsafe.Sizeof(directBufferedRootEntry{}))) ||
		!add(len(name)+len("/primary"), 1) || !add(len(name)+len("/index-state"), 1) {
		return capacity()
	}
	for _, item := range changed {
		// The primary builder copies IDs; document values and index-state IDs
		// continue borrowing the original admitted rows.
		if !add(len(item.documentID), 1) {
			return capacity()
		}
		if len(runtimes) == 0 || !persistIndexStateForOptions(opts) || !item.indexStateChanged {
			continue
		}
		stateEntries++
		if !orderedDocumentIndexStateEmpty(item.newState) {
			_, size, err := runtimeOrderedDocumentIndexStateStats(item.newState, runtimes)
			if err != nil {
				return 0, 0, 0, err
			}
			if !add(size, 1) {
				return capacity()
			}
		}
	}
	roots = 1
	if stateEntries > 0 {
		roots++
	}
	if len(changed) > 0 && !add(len(runtimes), uint64(unsafe.Sizeof(directBufferedSecondaryRootPlan{}))) {
		return capacity()
	}
	for runtimeIdx, runtime := range runtimes {
		var entries, keyBytes int
		changedUnique := false
		for _, item := range changed {
			if !item.indexStateChanged || !preparedBatchUpdateIndexChanged(item, runtimeIdx) {
				continue
			}
			changedUnique = runtime.def.unique
			for _, values := range [2][][]byte{item.oldState.valuesAt(runtimeIdx), item.newState.valuesAt(runtimeIdx)} {
				for _, value := range values {
					limit := int(^uint(0) >> 1)
					if entries == limit || len(value) > limit-len(item.documentID) || len(value)+len(item.documentID) > limit-keyBytes {
						return capacity()
					}
					entries++
					keyBytes += len(value) + len(item.documentID)
				}
			}
		}
		if entries > 0 {
			roots++
			if !add(entries, uint64(unsafe.Sizeof(directBufferedSecondaryRootEntry{}))) || !add(keyBytes, 1) {
				return capacity()
			}
		}
		if !changedUnique {
			continue
		}
		// Retain the existing conservative map policy: control backing plus a
		// full small-group/growth allowance per inserted value, rounded on its
		// own, and each copied string key. rejectBatchUniqueConflicts scans ALL
		// new states of a changed unique runtime, including unchanged siblings.
		if !add(256, 1) {
			return capacity()
		}
		for _, item := range changed {
			for _, value := range item.newState.valuesAt(runtimeIdx) {
				if !add(768, 1) || !add(len(value), 1) {
					return capacity()
				}
			}
			if preparedBatchUpdateIndexChanged(item, runtimeIdx) {
				// checkNativeUpdateUnique constructs one prefix and its cloned
				// upper bound. trackDependency admits its independent copies.
				for _, value := range item.newState.valuesAt(runtimeIdx) {
					if len(value) > int(^uint(0)>>1)-2 || !add(len(value)+2, 1) || !add(len(value)+2, 1) {
						return capacity()
					}
				}
			}
		}
	}
	if !add(stateEntries, uint64(unsafe.Sizeof(directBufferedRootEntry{}))) ||
		!add(roots, uint64(unsafe.Sizeof(string("")))) || !add(roots, uint64(unsafe.Sizeof(backenddb.OrderedRootStoragePolicy(0)))) {
		return capacity()
	}
	return charge, stateEntries, roots, nil
}
