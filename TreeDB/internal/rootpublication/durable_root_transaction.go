package rootpublication

import (
	"errors"
	"fmt"
	"sync"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/page"
)

var (
	// ErrDurableRootLineage rejects a candidate before ownership transfer when
	// its allocator transaction is not the next member of the pending DB
	// lineage. A caller may safely abandon a rejected candidate.
	ErrDurableRootLineage = errors.New("durable-root transaction lineage is not consecutive")
	// ErrDurableRootOwnership reports a duplicate, stale, or out-of-order
	// lifecycle transition for one exact allocator transaction.
	ErrDurableRootOwnership = errors.New("durable-root transaction ownership violation")
)

// DurableRootLineageID is an opaque identity for one DB allocator lineage. It
// must remain stable across consecutive visible candidates and change when an
// index/allocator lineage is replaced.
type DurableRootLineageID [16]byte

// DurableRootPayload is an optional immutable, DB-independent input to the one
// stable publisher. Ordinary visible members leave it zero because the target
// slot, durable parent, manifest, record, and meta seal must be selected from
// the current durable base at callback time. Payload returns a copy.
type DurableRootPayload struct {
	TargetMetaPageID uint64
	Meta             page.DurableMetaV1
	Record           DurableRootRecordV1
}

// DurableRootCallbackInput is copied into every exact allocator lifecycle
// callback. PreparedCOW is immutable until Consume, Abort, or Fail transfers
// its allocator ownership according to the callback contract.
type DurableRootCallbackInput struct {
	Lineage     DurableRootLineageID
	Sequence    uint64
	Payload     DurableRootPayload
	PreparedCOW *freelist.PreparedCOWCandidateV1
}

// DurableRootTransactionSpec binds one visible lineage member to the exact COW
// allocator candidate that it owns. Payload is optional; the callbacks are
// deliberately opaque so TreeDB/db can prepare the final seal at publication
// time without introducing a dependency from the coordinator to db types.
//
// Activate is called transactionally by Enqueue after ownership transfer but
// before the candidate/debt becomes visible. Consume is called in lineage order
// only after the stable Publisher reports success. Abort is called only before
// Enqueue made the candidate visible. Fail retains the exact candidate for
// close/recovery after ambiguous publish or shutdown. Lifecycle callbacks must
// not perform stable publication I/O.
type DurableRootTransactionSpec struct {
	Lineage     DurableRootLineageID
	Sequence    uint64
	Payload     DurableRootPayload
	PreparedCOW *freelist.PreparedCOWCandidateV1
	Activate    func(DurableRootCallbackInput) error
	Consume     func(DurableRootCallbackInput) error
	Abort       func(DurableRootCallbackInput) error
	Fail        func(DurableRootCallbackInput, error) error
}

type durableRootTransactionPhase uint8

const (
	durableRootPrepared durableRootTransactionPhase = iota + 1
	durableRootActivated
	durableRootConsumed
	durableRootFailed
)

// DurableRootTransaction is a move-only ownership wrapper. Construction owns
// the exact allocator candidate as Builder; PreparedRootCandidate construction
// transfers it to Candidate, Enqueue transfers it to Coordinator, and terminal
// recovery transfers it to Recovery. Read accessors return immutable inputs.
type DurableRootTransaction struct {
	mu sync.Mutex

	input    DurableRootCallbackInput
	activate func(DurableRootCallbackInput) error
	consume  func(DurableRootCallbackInput) error
	abort    func(DurableRootCallbackInput) error
	fail     func(DurableRootCallbackInput, error) error
	owner    ResourceOwnerState
	phase    durableRootTransactionPhase
}

func NewDurableRootTransaction(spec DurableRootTransactionSpec) (*DurableRootTransaction, error) {
	if spec.Lineage == (DurableRootLineageID{}) || spec.Sequence == 0 || spec.PreparedCOW == nil ||
		spec.Activate == nil || spec.Consume == nil || spec.Abort == nil || spec.Fail == nil {
		return nil, fmt.Errorf("%w: incomplete durable-root transaction", ErrInvalidCandidate)
	}
	if err := validateDurableRootTransactionSpec(spec); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCandidate, err)
	}
	return &DurableRootTransaction{
		input: DurableRootCallbackInput{
			Lineage: spec.Lineage, Sequence: spec.Sequence, Payload: spec.Payload,
			PreparedCOW: spec.PreparedCOW,
		},
		activate: spec.Activate, consume: spec.Consume, abort: spec.Abort, fail: spec.Fail,
		owner: ResourceOwnerBuilder, phase: durableRootPrepared,
	}, nil
}

func validateDurableRootTransactionSpec(spec DurableRootTransactionSpec) error {
	prepared := spec.PreparedCOW
	candidate := prepared.Candidate()
	if candidate == nil || candidate.Generation() == nil || prepared.CandidateID() == (freelist.CandidateIDV1{}) {
		return errors.New("missing exact prepared COW generation")
	}
	generation := candidate.Generation()
	if generation.CommitSeq() != spec.Sequence {
		return errors.New("prepared COW generation does not match the lineage sequence")
	}
	if durableRootPayloadIsZero(spec.Payload) {
		return nil
	}
	if spec.Payload.TargetMetaPageID > 1 || spec.Payload.Meta.CommitSeq != spec.Sequence ||
		spec.Payload.Record.CommitSeq != spec.Sequence || spec.Payload.Record.DurableSeq != spec.Payload.Meta.DurableSeq ||
		spec.Payload.Record.Freelist != generation.GenerationRef() || spec.Payload.Record.TotalPages != generation.HighWater() {
		return errors.New("durable-root payload does not match the exact COW generation")
	}
	metaImage := make([]byte, page.DurableMetaV1BodySize)
	if err := spec.Payload.Meta.Encode(metaImage); err != nil {
		return err
	}
	_, digest, err := spec.Payload.Record.EncodePage(spec.Payload.Meta.RootRecordPageID)
	if err != nil {
		return err
	}
	if digest != spec.Payload.Meta.RootRecordDigest || spec.Payload.Record.MetaProjectionDigest != spec.Payload.Meta.MetaProjectionDigest {
		return errors.New("durable-root record and meta binding differ")
	}
	return nil
}

func durableRootPayloadIsZero(payload DurableRootPayload) bool {
	return payload == (DurableRootPayload{})
}

func (transaction *DurableRootTransaction) Owner() ResourceOwnerState {
	if transaction == nil {
		return ResourceOwnerReleased
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	return transaction.owner
}

func (transaction *DurableRootTransaction) Lineage() DurableRootLineageID {
	if transaction == nil {
		return DurableRootLineageID{}
	}
	return transaction.input.Lineage
}

func (transaction *DurableRootTransaction) Sequence() uint64 {
	if transaction == nil {
		return 0
	}
	return transaction.input.Sequence
}

func (transaction *DurableRootTransaction) Payload() DurableRootPayload {
	if transaction == nil {
		return DurableRootPayload{}
	}
	return transaction.input.Payload
}

func (transaction *DurableRootTransaction) PreparedCOW() *freelist.PreparedCOWCandidateV1 {
	if transaction == nil {
		return nil
	}
	return transaction.input.PreparedCOW
}

// Abort releases a transaction that has not yet been moved into a candidate.
// Once candidate construction succeeds, use PreparedRootCandidate.Abandon if
// Enqueue is not accepted.
func (transaction *DurableRootTransaction) Abort() error {
	return transaction.abortFrom(ResourceOwnerBuilder)
}

func (transaction *DurableRootTransaction) transfer(from, to ResourceOwnerState) error {
	if transaction == nil {
		return nil
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.owner != from || transaction.phase == durableRootConsumed || transaction.owner == ResourceOwnerReleased {
		return ErrDurableRootOwnership
	}
	transaction.owner = to
	return nil
}

func (transaction *DurableRootTransaction) abortFrom(owner ResourceOwnerState) error {
	if transaction == nil {
		return nil
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.owner == ResourceOwnerReleased {
		return nil
	}
	if transaction.owner != owner || transaction.phase != durableRootPrepared {
		return ErrDurableRootOwnership
	}
	if err := transaction.abort(transaction.input); err != nil {
		return err
	}
	transaction.owner = ResourceOwnerReleased
	return nil
}

func (transaction *DurableRootTransaction) activateFromCoordinator() error {
	if transaction == nil {
		return nil
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.owner != ResourceOwnerCoordinator || transaction.phase != durableRootPrepared {
		return ErrDurableRootOwnership
	}
	if err := transaction.activate(transaction.input); err != nil {
		return err
	}
	transaction.phase = durableRootActivated
	return nil
}

func (transaction *DurableRootTransaction) consumeFromCoordinator() error {
	if transaction == nil {
		return nil
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.owner != ResourceOwnerCoordinator || transaction.phase != durableRootActivated {
		return ErrDurableRootOwnership
	}
	if err := transaction.consume(transaction.input); err != nil {
		return err
	}
	transaction.phase = durableRootConsumed
	transaction.owner = ResourceOwnerReleased
	return nil
}

func (transaction *DurableRootTransaction) failFromCoordinator(cause error) error {
	if transaction == nil {
		return nil
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.owner == ResourceOwnerReleased || transaction.phase == durableRootConsumed {
		return nil
	}
	if transaction.owner != ResourceOwnerCoordinator {
		return ErrDurableRootOwnership
	}
	if transaction.phase == durableRootFailed {
		return nil
	}
	if cause == nil {
		cause = ErrRecoveryRequired
	}
	if err := transaction.fail(transaction.input, cause); err != nil {
		return err
	}
	transaction.phase = durableRootFailed
	return nil
}

func (transaction *DurableRootTransaction) releaseFromRecovery() {
	if transaction == nil {
		return
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.owner == ResourceOwnerRecovery {
		transaction.owner = ResourceOwnerReleased
	}
}

type durableRootGroupExtension struct {
	members []*DurableRootTransaction
}

func newDurableRootGroupExtension(transaction *DurableRootTransaction) durableRootGroupExtension {
	if transaction == nil {
		return durableRootGroupExtension{}
	}
	return durableRootGroupExtension{members: []*DurableRootTransaction{transaction}}
}

func (group durableRootGroupExtension) union(other immutableExtension) (immutableExtension, error) {
	next, ok := other.(durableRootGroupExtension)
	if !ok {
		return nil, fmt.Errorf("%w: durable-root extension has type %T", ErrDurableRootLineage, other)
	}
	merged := make([]*DurableRootTransaction, 0, len(group.members)+len(next.members))
	merged = append(merged, group.members...)
	for _, transaction := range next.members {
		if len(merged) != 0 {
			previous := merged[len(merged)-1]
			if err := validateConsecutiveDurableRootTransactions(previous, transaction); err != nil {
				return nil, err
			}
		}
		merged = append(merged, transaction)
	}
	return durableRootGroupExtension{members: merged}, nil
}

func validateConsecutiveDurableRootTransactions(previous, next *DurableRootTransaction) error {
	if previous == nil || next == nil || previous.Lineage() == (DurableRootLineageID{}) ||
		previous.Lineage() != next.Lineage() || previous.Sequence() == ^uint64(0) || next.Sequence() != previous.Sequence()+1 {
		return fmt.Errorf("%w: previous=%d next=%d", ErrDurableRootLineage, sequenceOf(previous), sequenceOf(next))
	}
	return nil
}

func sequenceOf(transaction *DurableRootTransaction) uint64 {
	if transaction == nil {
		return 0
	}
	return transaction.Sequence()
}

// DurableRootGroup is an immutable ordered view supplied to Publisher. Members
// appear in enqueue/allocator-lineage order and Latest is the only durable root
// that the coalesced stable publication may make recovery-selectable.
type DurableRootGroup struct {
	members []*DurableRootTransaction
}

func (group DurableRootGroup) Len() int { return len(group.members) }

func (group DurableRootGroup) Lineage() DurableRootLineageID {
	if len(group.members) == 0 {
		return DurableRootLineageID{}
	}
	return group.members[0].Lineage()
}

func (group DurableRootGroup) Members() []*DurableRootTransaction {
	return append([]*DurableRootTransaction(nil), group.members...)
}

func (group DurableRootGroup) Latest() *DurableRootTransaction {
	if len(group.members) == 0 {
		return nil
	}
	return group.members[len(group.members)-1]
}
