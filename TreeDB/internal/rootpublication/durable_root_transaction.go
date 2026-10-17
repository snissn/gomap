package rootpublication

import (
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
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
	Lineage         DurableRootLineageID
	Sequence        uint64
	Payload         DurableRootPayload
	PreparedCOW     *freelist.PreparedCOWCandidateV1
	PreparedPrimary *PreparedPrimaryRootV5
	// PublicationProjection is the immutable DB descriptor owned by this member.
	// It supplies no second lifecycle or callback authority.
	PublicationProjection any
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
	// PrimaryMetadataBytesV6 transfers already admitted DB projection/callback
	// storage to this SAME constructor; ordinary Finish/Abort refunds it.
	PrimaryMetadataBytesV6 uint64
	// Selected DATA uses the SAME original metadata owner and terminal node.
	TerminalMetadataOwnerV6 *retainedalloc.Owner
	Lineage                 DurableRootLineageID
	Sequence                uint64
	Payload                 DurableRootPayload
	PreparedCOW             *freelist.PreparedCOWCandidateV1
	PreparedPrimary         *PreparedPrimaryRootV5
	Activate                func(DurableRootCallbackInput) error
	Consume                 func(DurableRootCallbackInput) error
	Abort                   func(DurableRootCallbackInput) error
	// AbortComplete consumes callback payload only after BOTH original physical
	// callback cleanup and constructor root cleanup have actually succeeded.
	AbortComplete         func(DurableRootCallbackInput) error
	Fail                  func(DurableRootCallbackInput, error) error
	PublicationProjection any
}

type durableRootTransactionPhase uint8

const (
	durableRootPrepared durableRootTransactionPhase = iota + 1
	durableRootActivated
	durableRootConsumed
	durableRootFinishing
	durableRootFailed
	durableRootConstructingV6
)

// DurableRootTransaction is a move-only ownership wrapper. Construction owns
// the exact allocator candidate as Builder; PreparedRootCandidate construction
// transfers it to Candidate, Enqueue transfers it to Coordinator, and terminal
// recovery transfers it to Recovery. Read accessors return immutable inputs.
type DurableRootTransaction struct {
	mu sync.Mutex

	input                  DurableRootCallbackInput
	activate               func(DurableRootCallbackInput) error
	consume                func(DurableRootCallbackInput) error
	abort                  func(DurableRootCallbackInput) error
	abortComplete          func(DurableRootCallbackInput) error
	physicalCleanupError   error
	physicalCleanupReceipt durableRootCleanupFailureV6
	fail                   func(DurableRootCallbackInput, error) error
	owner                  ResourceOwnerState
	phase                  durableRootTransactionPhase
	finishBusy             bool
	constructionV6         *primaryTransactionConstructionV6
	terminalMetadataV6     *primaryTerminalCompletionV6
	failedCleanupMu        sync.Mutex
	failedCleanupOwner     any
	failedCleanupNext      *DurableRootTransaction
	failedCleanupReceipt   durableRootCleanupFailureV6
}

// This failure edge is embedded in the SAME admitted original transaction.
// It records disposal custody only; existing phase/resource ownership remains
// the sole lifecycle authority and no replacement owner is allocated.
type durableRootCleanupFailureV6 struct {
	mu       sync.Mutex
	original *DurableRootTransaction
	causes   [3]error
}

func (failure *durableRootCleanupFailureV6) LinkPriorCleanupError(prior error) {
	if prior == nil || prior == failure || errors.Is(prior, failure) {
		return
	}
	failure.TryLinkPriorCleanupError(prior)
}
func (failure *durableRootCleanupFailureV6) OriginalCleanupPriorAvailable() bool {
	failure.mu.Lock()
	defer failure.mu.Unlock()
	return failure.causes[2] == nil
}
func (failure *durableRootCleanupFailureV6) TryLinkPriorCleanupError(prior error) bool {
	failure.mu.Lock()
	defer failure.mu.Unlock()
	if failure.causes[2] != nil || prior == nil || prior == failure {
		return false
	}
	failure.causes[2] = prior
	return true
}
func (failure *durableRootCleanupFailureV6) Error() string {
	return "original durable-root cleanup failed"
}
func (failure *durableRootCleanupFailureV6) Is(target error) bool {
	failure.mu.Lock()
	causes := failure.causes
	failure.mu.Unlock()
	for _, cause := range causes {
		if errors.Is(cause, target) {
			return true
		}
	}
	return false
}
func (failure *durableRootCleanupFailureV6) As(target any) bool {
	failure.mu.Lock()
	causes := failure.causes
	failure.mu.Unlock()
	for _, cause := range causes {
		if errors.As(cause, target) {
			return true
		}
	}
	return false
}
func (transaction *DurableRootTransaction) RetainFailedCleanupV6(owner any, next *DurableRootTransaction, cause, prior error) (bool, error) {
	transaction.failedCleanupMu.Lock()
	defer transaction.failedCleanupMu.Unlock()
	if owner == nil || cause == nil {
		return false, ErrDurableRootOwnership
	}
	if transaction.failedCleanupOwner != nil {
		if transaction.failedCleanupOwner != owner {
			return false, ErrDurableRootOwnership
		}
		return false, &transaction.failedCleanupReceipt
	}
	transaction.failedCleanupOwner, transaction.failedCleanupNext = owner, next
	transaction.failedCleanupReceipt.mu.Lock()
	transaction.failedCleanupReceipt.original = transaction
	transaction.failedCleanupReceipt.causes = [3]error{cause, ErrDurableRootOwnership, prior}
	transaction.failedCleanupReceipt.mu.Unlock()
	return true, &transaction.failedCleanupReceipt
}

func NewDurableRootTransaction(spec DurableRootTransactionSpec) (*DurableRootTransaction, error) {
	if spec.Lineage == (DurableRootLineageID{}) || spec.Sequence == 0 || (spec.PreparedCOW == nil && spec.PreparedPrimary == nil) ||
		spec.Abort == nil || spec.Fail == nil || (spec.Activate == nil || spec.Consume == nil) {
		return nil, fmt.Errorf("%w: incomplete durable-root transaction", ErrInvalidCandidate)
	}
	if err := validateDurableRootTransactionSpec(spec); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCandidate, err)
	}
	var terminal *primaryTerminalCompletionV6
	if spec.TerminalMetadataOwnerV6 != nil {
		charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(DurableRootTransaction{}))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryTerminalCompletionV6{})))
		if spec.PrimaryMetadataBytesV6 > ^uint64(0)-charge {
			return nil, retainedalloc.ErrCapacity
		}
		if err := spec.TerminalMetadataOwnerV6.AddPending(charge); err != nil {
			return nil, err
		}
		terminal = &primaryTerminalCompletionV6{owner: spec.TerminalMetadataOwnerV6, charge: charge + spec.PrimaryMetadataBytesV6}
	}
	return &DurableRootTransaction{
		terminalMetadataV6: terminal,
		input: DurableRootCallbackInput{
			Lineage: spec.Lineage, Sequence: spec.Sequence, Payload: spec.Payload,
			PreparedCOW: spec.PreparedCOW, PreparedPrimary: spec.PreparedPrimary, PublicationProjection: spec.PublicationProjection,
		},
		activate: spec.Activate, consume: spec.Consume, abort: spec.Abort, abortComplete: spec.AbortComplete, fail: spec.Fail,
		owner: ResourceOwnerBuilder, phase: durableRootPrepared,
	}, nil
}

func validateDurableRootTransactionSpec(spec DurableRootTransactionSpec) error {
	if spec.PreparedPrimary != nil {
		if spec.PreparedCOW != nil || !durableRootPayloadIsZero(spec.Payload) {
			return errors.New("primary transaction cannot carry V1 COW or payload authority")
		}
		return spec.PreparedPrimary.validateSequence(spec.Sequence)
	}
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
	return transaction.ownerLockedV6()
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

func (transaction *DurableRootTransaction) PreparedPrimary() *PreparedPrimaryRootV5 {
	if transaction == nil {
		return nil
	}
	return transaction.input.PreparedPrimary
}

// Abort releases a transaction that has not yet been moved into a candidate.
// Once candidate construction succeeds, use PreparedRootCandidate.Abandon if
// Enqueue is not accepted.
func (transaction *DurableRootTransaction) Abort() error {
	return transaction.abortFrom(ResourceOwnerBuilder)
}

// AbortPrivatePrimaryConstructionV6 disposes only this transaction's original
// unbound construction. BindPrimaryVisibilityV6 changes the existing phase
// after acquiring the state edge and detaching constructor discovery. That
// transfer is authoritative even when later candidate/publication work fails.
// No group may abort the transaction after that boundary.
func (transaction *DurableRootTransaction) AbortPrivatePrimaryConstructionV6() error {
	if transaction == nil {
		return nil
	}
	var terminal primaryConstructionReleaseState
	err := func() error {
		transaction.mu.Lock()
		defer transaction.mu.Unlock()
		if transaction.physicalCleanupError != nil {
			return transaction.physicalCleanupError
		}
		if transaction.phase != durableRootConstructingV6 {
			return nil
		}
		if transaction.constructionV6 == nil || transaction.ownerLockedV6() != ResourceOwnerBuilder {
			return ErrDurableRootOwnership
		}
		return transaction.abortConstructionLockedV6(&terminal)
	}()
	transaction = nil
	if err != nil {
		return err
	}
	if terminal.owner != nil {
		terminal.owner.RemovePending(terminal.charge)
	}
	return nil
}

func (transaction *DurableRootTransaction) transfer(from, to ResourceOwnerState) error {
	if transaction == nil {
		return nil
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.constructionV6 != nil && transaction.constructionV6.resources != nil {
		setOwner := transaction.constructionV6.resources.Owner()
		if transaction.finishBusy || transaction.phase == durableRootConsumed || transaction.owner != from ||
			(setOwner != to && !(setOwner == ResourceOwnerReleased && (transaction.phase == durableRootFinishing || transaction.physicalCleanupError != nil))) {
			return ErrDurableRootOwnership
		}
		transaction.owner = to
		return nil
	}
	if transaction.ownerLockedV6() != from || (transaction.phase == durableRootConsumed || transaction.phase == durableRootFinishing) || transaction.ownerLockedV6() == ResourceOwnerReleased {
		return ErrDurableRootOwnership
	}
	transaction.owner = to
	return nil
}

func (transaction *DurableRootTransaction) abortFrom(owner ResourceOwnerState) error {
	if transaction == nil {
		return nil
	}
	var terminal primaryConstructionReleaseState
	err := func() error {
		transaction.mu.Lock()
		defer transaction.mu.Unlock()
		if transaction.physicalCleanupError != nil {
			return transaction.physicalCleanupError
		}
		if transaction.ownerLockedV6() == ResourceOwnerReleased {
			return nil
		}
		if transaction.ownerLockedV6() != owner || (transaction.phase != durableRootPrepared && transaction.phase != durableRootConstructingV6) {
			return ErrDurableRootOwnership
		}
		if transaction.phase == durableRootConstructingV6 {
			return transaction.abortConstructionLockedV6(&terminal)
		}
		if err := transaction.abort(transaction.input); err != nil {
			return transaction.recordPhysicalCleanupFailureLocked(fmt.Errorf("abort durable-root visible state: %w", err))
		}
		if transaction.isConstructedPrimaryV6() {
			return transaction.abortConstructionLockedV6(&terminal)
		}
		if transaction.abortComplete != nil {
			if err := transaction.abortComplete(transaction.input); err != nil {
				return transaction.recordPhysicalCleanupFailureLocked(err)
			}
		}
		transaction.releaseOwnerLockedV6()
		if node := transaction.terminalMetadataV6; node != nil {
			transaction.terminalMetadataV6 = nil
			transaction.input = DurableRootCallbackInput{Lineage: transaction.input.Lineage, Sequence: transaction.input.Sequence}
			transaction.activate, transaction.consume, transaction.abort, transaction.abortComplete, transaction.fail = nil, nil, nil, nil, nil
			_, terminal = popPrimaryTerminalCompletionV6(node)
		}
		return nil
	}()
	transaction = nil
	if err != nil {
		return err
	}
	if terminal.owner != nil {
		terminal.owner.RemovePending(terminal.charge)
	}
	return nil
}

// The first original disposal refusal is terminal for this object. A callback
// may already have consumed physical edges before returning the error; neither
// Abort nor Finish/Close may replay those actions or replace the first receipt.
func (transaction *DurableRootTransaction) recordPhysicalCleanupFailureLocked(err error) error {
	if transaction.physicalCleanupError == nil {
		transaction.physicalCleanupReceipt.mu.Lock()
		transaction.physicalCleanupReceipt.original = transaction
		transaction.physicalCleanupReceipt.causes = [3]error{err, ErrDurableRootOwnership}
		transaction.physicalCleanupReceipt.mu.Unlock()
		transaction.physicalCleanupError = &transaction.physicalCleanupReceipt
		if transaction.terminalMetadataV6 != nil {
			transaction.terminalMetadataV6.owner.CleanupFailed()
		}
		if transaction.constructionV6 != nil {
			transaction.constructionV6.arena.MetadataOwner().CleanupFailed()
		}
	}
	return transaction.physicalCleanupError
}

func (transaction *DurableRootTransaction) activateFromCoordinator() error {
	return transaction.activateFromCoordinatorWithWork(nil)
}
func (transaction *DurableRootTransaction) activateFromCoordinatorWithWork(w *iterator.OrdinalScanWork) error {
	if transaction == nil {
		return nil
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.ownerLockedV6() != ResourceOwnerCoordinator || transaction.phase != durableRootPrepared {
		return ErrDurableRootOwnership
	}
	if err := transaction.activate(transaction.input); err != nil {
		return err
	}
	transaction.phase = durableRootActivated
	return nil
}

func (transaction *DurableRootTransaction) consumeFromCoordinator() error {
	return transaction.consumeFromCoordinatorWithWork(nil)
}
func (transaction *DurableRootTransaction) consumeFromCoordinatorWithWork(w *iterator.OrdinalScanWork) error {
	if transaction == nil {
		return nil
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.ownerLockedV6() != ResourceOwnerCoordinator || transaction.phase != durableRootActivated {
		return ErrDurableRootOwnership
	}
	{
		if err := transaction.consume(transaction.input); err != nil {
			return err
		}
		if transaction.isConstructedPrimaryV6() || transaction.terminalMetadataV6 != nil {
			transaction.phase = durableRootFinishing
		} else {
			transaction.phase = durableRootConsumed
			transaction.releaseOwnerLockedV6()
		}
	}
	return nil
}

func (transaction *DurableRootTransaction) failFromCoordinator(cause error) error {
	if transaction == nil {
		return nil
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.ownerLockedV6() == ResourceOwnerReleased || transaction.phase == durableRootConsumed || transaction.phase == durableRootFinishing {
		return nil
	}
	if transaction.ownerLockedV6() != ResourceOwnerCoordinator {
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

func (transaction *DurableRootTransaction) prepareFromRecovery() error {
	if transaction == nil {
		return nil
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.physicalCleanupError != nil {
		return transaction.physicalCleanupError
	}
	if transaction.ownerLockedV6() != ResourceOwnerRecovery {
		return ErrDurableRootOwnership
	}
	if transaction.constructionV6 != nil {
		return transaction.preparePrimaryRecoveryLockedV6()
	}
	return nil
}
func (transaction *DurableRootTransaction) releaseFromRecovery() (*primaryTerminalCompletionV6, error) {
	if transaction == nil {
		return nil, nil
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.physicalCleanupError != nil {
		return nil, transaction.physicalCleanupError
	}
	if transaction.constructionV6 != nil {
		return transaction.releasePrimaryRecoveryLockedV6()
	}
	if transaction.ownerLockedV6() == ResourceOwnerRecovery {
		transaction.input = DurableRootCallbackInput{Lineage: transaction.input.Lineage, Sequence: transaction.input.Sequence}
		transaction.activate, transaction.consume, transaction.abort, transaction.abortComplete, transaction.fail = nil, nil, nil, nil, nil
		transaction.releaseOwnerLockedV6()
		transaction.phase = durableRootConsumed
		node := transaction.terminalMetadataV6
		transaction.terminalMetadataV6 = nil
		return node, nil
	}
	if transaction.ownerLockedV6() == ResourceOwnerReleased {
		return nil, nil
	}
	return nil, ErrDurableRootOwnership
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
