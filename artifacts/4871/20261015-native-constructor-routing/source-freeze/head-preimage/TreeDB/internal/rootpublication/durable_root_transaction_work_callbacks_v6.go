package rootpublication

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func (spec *DurableRootTransactionSpec) hasDurableRootCallbacksV6() bool {
	return (spec.Activate != nil || spec.ActivateWithWork != nil) &&
		(spec.Consume != nil || spec.ConsumeWithWork != nil)
}

func (transaction *DurableRootTransaction) requiresNativeCallbackWorkV6() bool {
	c := transaction.constructionV6
	return c != nil && c.nativeWorkOwned && !c.unboundedDiagnostic
}

// One dispatch record covers the by-value input copy and phase/owner writes;
// callback work is separately admitted by the actual typed callback using w.
func reserveDurableRootCallbackDispatchV6(w *iterator.OrdinalScanWork) bool {
	return w.Reserve(1, 2*uint64(unsafe.Sizeof(DurableRootCallbackInput{}))+
		2*uint64(unsafe.Sizeof(durableRootTransactionPhase(0)))+
		2*uint64(unsafe.Sizeof(ResourceOwnerState(0))))
}

// Every original terminal detachment must clear both callback forms before its
// containing metadata allocation can be refunded. Caller holds transaction.mu.
func (transaction *DurableRootTransaction) clearDurableRootCallbacksV6() {
	transaction.activate, transaction.consume = nil, nil
	transaction.activateWithWork, transaction.consumeWithWork = nil, nil
	transaction.abort, transaction.abortComplete, transaction.fail = nil, nil, nil
}
