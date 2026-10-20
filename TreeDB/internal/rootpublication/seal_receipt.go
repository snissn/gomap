package rootpublication

import "context"

// PhysicalSealReceipt identifies an actual accepted physical publication. It is
// minted only from EnqueueBuilt's irreversible handoff receipt; callers cannot
// construct authority by sampling a current root or a maximum AppliedLSN.
type PhysicalSealReceipt struct {
	coordinator  *Coordinator
	sequence     uint64
	logicalOwner *Coordinator
	cutNonce     uint64
	through      SealedPrefix
}

func (receipt *BuilderHandoffReceipt) SealReceipt() PhysicalSealReceipt {
	if receipt == nil {
		return PhysicalSealReceipt{}
	}
	return PhysicalSealReceipt{coordinator: receipt.coordinator, sequence: receipt.sequence, logicalOwner: receipt.logicalOwner, cutNonce: receipt.cutNonce, through: receipt.through}
}
func (receipt PhysicalSealReceipt) Valid() bool {
	return receipt.coordinator != nil && receipt.sequence != 0
}
func (receipt PhysicalSealReceipt) Wait(ctx context.Context) error {
	if !receipt.Valid() {
		return ErrAcceptedReceipt
	}
	return receipt.coordinator.WaitThrough(ctx, receipt.sequence)
}
func (receipt PhysicalSealReceipt) Sealed() bool {
	if !receipt.Valid() {
		return false
	}
	c := receipt.coordinator
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.durable.commitSeq >= receipt.sequence
}
