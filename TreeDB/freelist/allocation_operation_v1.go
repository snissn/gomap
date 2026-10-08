package freelist

// allocationOperationV1 is a stack-local receipt for ONE already admitted
// operation. It delegates to the existing creator/facet; it is not an account,
// owner registry or refund mechanism. It contains no current request reference.
// All births, including overlap and scratch, remain cumulatively debited.
type allocationOperationV1 struct {
	creator     *allocationCreditLeaseV1
	bytes, refs uint64
}

func admitAllocationOperationV1(request AllocationRequestCreditV1, creator *allocationCreditLeaseV1, bytes, refs uint64) (allocationOperationV1, error) {
	if err := creator.reserve(request, bytes, refs); err != nil {
		return allocationOperationV1{}, err
	}
	return allocationOperationV1{creator: creator, bytes: bytes, refs: refs}, nil
}

// admitAllocationLoanOperationV1 separates a current request's whole resident
// loan from newly adopted backing. Request debit is irreversible; refusal of
// destination preparation leaves every old creator and reference unchanged.
func admitAllocationLoanOperationV1(request AllocationRequestCreditV1, creator *allocationCreditLeaseV1, loan, births, refs uint64) (allocationOperationV1, error) {
	if creator == nil || request == nil || births > loan {
		return allocationOperationV1{}, ErrAllocationCertificateIncompleteV1
	}
	creator.mu.Lock()
	defer creator.mu.Unlock()
	if creator.refs == 0 || creator.resident == nil || creator.refs > ^uint64(0)-refs {
		return allocationOperationV1{}, ErrCandidateConsumed
	}
	if err := request.ReserveAllocation(loan); err != nil {
		return allocationOperationV1{}, err
	}
	if err := creator.resident.ReserveAllocation(births); err != nil {
		return allocationOperationV1{}, err
	}
	creator.refs += refs
	return allocationOperationV1{creator: creator, bytes: births, refs: refs}, nil
}
func (operation *allocationOperationV1) take(bytes, refs uint64) error {
	if operation == nil {
		return ErrGenerationFormat
	}
	if bytes > operation.bytes || refs > operation.refs {
		return ErrGenerationFormat
	}
	operation.bytes -= bytes
	operation.refs -= refs
	return nil
}
func (operation *allocationOperationV1) close() {
	if operation == nil {
		return
	}
	for operation.refs != 0 {
		operation.creator.release()
		operation.refs--
	}
	operation.creator = nil
}
func reserveBirthV1(request AllocationRequestCreditV1, creator *allocationCreditLeaseV1, bytes, refs uint64, operations ...*allocationOperationV1) error {
	if len(operations) != 0 && operations[0] != nil {
		if operations[0].creator != creator {
			return ErrGenerationFormat
		}
		return operations[0].take(bytes, refs)
	}
	return creator.reserve(request, bytes, refs)
}
