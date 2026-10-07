package freelist

// allocationOperationV1 is a stack-local receipt for ONE already admitted
// operation. It delegates to the existing creator/facet; it is not an account,
// owner registry or refund mechanism. All allocation instances, including
// overlap and abandoned scratch, remain cumulatively debited on that facet.
type allocationOperationV1 struct {
	creator     *allocationCreditLeaseV1
	bytes, refs uint64
}

func admitAllocationOperationV1(creator *allocationCreditLeaseV1, bytes, refs uint64) (allocationOperationV1, error) {
	if err := creator.reserve(bytes, refs); err != nil {
		return allocationOperationV1{}, err
	}
	return allocationOperationV1{creator: creator, bytes: bytes, refs: refs}, nil
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
func reserveBirthV1(creator *allocationCreditLeaseV1, bytes, refs uint64, operations ...*allocationOperationV1) error {
	if len(operations) != 0 && operations[0] != nil {
		if operations[0].creator != creator {
			return ErrGenerationFormat
		}
		return operations[0].take(bytes, refs)
	}
	return creator.reserve(bytes, refs)
}
