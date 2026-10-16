package lifecycle

import (
	"math"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// UnregisterExistingStorageWithWorkV6 consumes the SAME registry registration.
// Fixed shards need no backing growth. Fallback handles require an existing
// free slot; refusal precedes any sequence/borrow mutation and preserves the
// ordinary caller's original registration for its later retirement continuation.
func (r *ReaderRegistry) UnregisterExistingStorageWithWorkV6(id int64, w *iterator.OrdinalScanWork) (bool, error) {
	if r == nil || w == nil {
		return false, retainedalloc.ErrWorkRequired
	}
	if id < 0 {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(readerRegistryShard{}))) {
			return false, nil
		}
		r.Unregister(id)
		return true, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	index := int(id - 1)
	if id > 0 && index >= 0 && index < len(r.seqs) && r.seqs[index] != math.MaxUint64 && len(r.free) == cap(r.free) {
		return false, retainedalloc.ErrWorkRequired
	}
	if !w.Reserve(1, 64) {
		return false, nil
	}
	r.unregisterLockedV6(id)
	return true, nil
}
