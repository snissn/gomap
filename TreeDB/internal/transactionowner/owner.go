// Package transactionowner carries the private public-DB ownership fence.
// It is not persisted and grants no authority over a caller-owned backend.
package transactionowner

type Owner struct{ identity byte }

func New() *Owner { return &Owner{identity: 1} }

// NativeImportEvidence is an ephemeral, non-owning proof from an internal
// complete physical importer. Its private fields cannot be minted by public
// callers. The original caller must keep receiver and allocator alive during
// this synchronous check; this value is never retained in a cut or state.
type NativeImportEvidence struct {
	receiver  any
	allocator any
	basis     [6]uint64
	sequence  uint64
}

// NewNativeImportEvidence belongs only at the trusted complete-import boundary.
// receiver and allocator are original pointer-valued identities, not callbacks.
func NewNativeImportEvidence(receiver, allocator any, basis [6]uint64, sequence uint64) NativeImportEvidence {
	return NativeImportEvidence{receiver: receiver, allocator: allocator, basis: basis, sequence: sequence}
}

// Matches binds the supplied state itself, so promoting a trusted getter from
// an embedded state cannot authorize a different wrapper receiver. No method
// supplied by the caller decides this comparison.
func (e NativeImportEvidence) Matches(receiver, allocator any, basis [6]uint64, sequence uint64) bool {
	return e.receiver != nil && e.allocator != nil && e.receiver == receiver && e.allocator == allocator && e.basis == basis && e.sequence == sequence
}
