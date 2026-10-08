package rootpublication

import "errors"
import "github.com/snissn/gomap/TreeDB/internal/iterator"

type OwnedFinishCallbackScopeV6 interface {
	EnterOwnedFinishCallbackV6(*iterator.OrdinalScanWork) (bool, error)
	LeaveOwnedFinishCallbackV6()
}

func (c *Coordinator) finishOrdinaryPrimaryWithScopeV6(txn *DurableRootTransaction) error {
	w := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
	if scope, ok := c.publisher.(OwnedFinishCallbackScopeV6); ok {
		entered, err := scope.EnterOwnedFinishCallbackV6(w)
		if !entered || err != nil {
			return errors.Join(err, ErrDurableRootOwnership)
		}
		defer scope.LeaveOwnedFinishCallbackV6()
	}
	return txn.finishOrdinaryPrimaryV6(w)
}
