package rootpublication

type OwnedFinishCallbackScopeV6 interface {
	EnterOwnedFinishCallbackV6() error
	LeaveOwnedFinishCallbackV6()
}
