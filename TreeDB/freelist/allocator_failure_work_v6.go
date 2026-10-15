package freelist

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// AdmitCOWFailureV6 prices the exact post-unlock original failure operation.
// It is reserved by the SAME authority scope BEFORE any ambiguous write. The
// primitive Broadcast is counted once; scheduler/waiter runtime internals are
// the existing Go runtime service, not a new retained native ownership graph.
func AdmitCOWFailureV6(w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		return false, ErrGenerationFormat
	}
	return appendReserve(w, 3, 2*uint64(unsafe.Sizeof(PreparedCOWCandidateV1{}))+2*uint64(unsafe.Sizeof(allocatorCOWStateV1{}))+2*uint64(unsafe.Sizeof(error(nil))))
}

// FailCOWCandidateAdmittedV6 consumes only the preceding fixed admission. The
// original candidate/state backlink makes current membership exact without a
// mutable vector scan. It is not a captured index or independent failure ticket.
func (a *Allocator) FailCOWCandidateAdmittedV6(prepared *PreparedCOWCandidateV1, cause error) error {
	if a == nil || prepared == nil || cause == nil {
		return ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil || !(a.cow.prepared == prepared || prepared.activationOwner == a.cow && prepared.activated && !prepared.published) {
		return ErrCandidateConsumed
	}
	if a.cow.waitErr == nil {
		a.cow.waitErr = cause
	}
	a.cow.ready.Broadcast()
	return nil
}
