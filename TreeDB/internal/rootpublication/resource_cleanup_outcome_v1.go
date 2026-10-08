package rootpublication

// StableCleanupOutcomeV1 is scalar evidence about the exact original cleanup.
// Started is never a completion certificate. Unknown callbacks remain uncertified.
type StableCleanupPhaseV1 uint8

const (
	StableCleanupLiveV1 StableCleanupPhaseV1 = iota
	StableCleanupWaitingV1
	StableCleanupRunningV1
	StableCleanupPendingV1
	StableCleanupUncertainV1
	StableCleanupCompleteV1
)

type StableCleanupDebtV1 uint8

const (
	StableCleanupNoDebtV1 StableCleanupDebtV1 = iota
	StableCleanupExactHoldsV1
	StableCleanupNamespaceDebtV1
	StableCleanupMetadataDebtV1
	StableCleanupUnknownDebtV1
)

type StableCleanupOutcomeV1 struct {
	Phase                                     StableCleanupPhaseV1
	Debt                                      StableCleanupDebtV1
	StartedRoles, ConsumedRoles, PendingRoles uint64
	FaultCode                                 uint16
}

// Complete proves role consumption and local scrub, not successful namespace
// deletion. NamespaceDebt plus FaultCode records effects-complete-with-error;
// callers must not replay the consumed ref or infer physical durability.
func (o StableCleanupOutcomeV1) Complete() bool {
	return o.Phase == StableCleanupCompleteV1 && o.PendingRoles == 0
}

// StableResourceCleanupEnvironmentV1 is explicitly checked original-only cleanup.
// Implementations return pending while actual reader/control debt remains. It is
// deliberately distinct from the external shared-owned descriptor responsibility.
type StableResourceCleanupEnvironmentV1 interface {
	AdvanceStableResourceCleanupV1() (StableCleanupOutcomeV1, error)
}
