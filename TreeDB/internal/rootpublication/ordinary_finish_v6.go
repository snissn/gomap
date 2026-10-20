package rootpublication

import "errors"

type OwnedFinishCallbackScopeV6 interface {
	EnterOwnedFinishCallbackV6() error
	LeaveOwnedFinishCallbackV6()
}

func (c *Coordinator) finishOrdinaryPrimaryWithScopeV6(txn *DurableRootTransaction) (*primaryTerminalCompletionV6, error) {
	if scope, ok := c.publisher.(OwnedFinishCallbackScopeV6); ok {
		err := scope.EnterOwnedFinishCallbackV6()
		if err != nil {
			return nil, errors.Join(err, ErrDurableRootOwnership)
		}
		defer scope.LeaveOwnedFinishCallbackV6()
	}
	return txn.finishOrdinaryPrimaryV6(c.resourceActivePins)
}
