package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"unsafe"
)

// EnterOwnedFinishCallbackV6 reserves entry and exit of the actual DB callback
// marker, including the runtime stack header used for same-caller refusal.
// It neither transfers custody nor creates another retirement lifecycle.
func (r *rootPublicationRuntimeV1) EnterOwnedFinishCallbackV6(w *iterator.OrdinalScanWork) (bool, error) {
	if r == nil || r.db == nil || w == nil {
		return false, rootpublication.ErrDurableRootOwnership
	}
	if !w.Reserve(2, 128+2*uint64(unsafe.Sizeof(DB{}))) {
		return false, nil
	}
	caller := currentGoroutineID()
	if caller == 0 {
		return false, rootpublication.ErrPublisherProtocol
	}
	r.db.closeHooksMu.Lock()
	defer r.db.closeHooksMu.Unlock()
	if r.db.ownedFinishCallerV6 != 0 {
		return false, rootpublication.ErrDurableRootOwnership
	}
	r.db.ownedFinishCallerV6 = caller
	return true, nil
}
func (r *rootPublicationRuntimeV1) LeaveOwnedFinishCallbackV6() {
	r.db.closeHooksMu.Lock()
	r.db.ownedFinishCallerV6 = 0
	r.db.closeHooksMu.Unlock()
}

var errPublicationCallbackCloseV6 = errors.New("treedb: Close from active publication Finish callback")
