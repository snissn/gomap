package freelist

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var ErrCOWCandidatePrepared = errors.New("COW freelist candidate publication pending")

// TestHookRetireCOWBeforeUnlock is a test-only hook that fires after a COW
// retirement has updated the candidate transaction and counters but before the
// allocator lock is released. It should remain nil in production.
var TestHookRetireCOWBeforeUnlock func()

// TestHookCOWWaitBeforeSleep is a test-only hook that fires after an allocator
// observes a prepared COW candidate and immediately before it waits for that
// candidate to publish or fail. It should remain nil in production.
var TestHookCOWWaitBeforeSleep func()

// TestHookAbortCOWCandidateFailure injects a failure before an owned prepared
// candidate is rolled back. It is test-only and must remain nil in production.
var TestHookAbortCOWCandidateFailure func() error

type allocatorCOWStateV1 struct {
	generation               *FreelistGenerationV1
	txn                      *FreelistTxn
	ledger                   *ReservationLedger
	prepared                 *PreparedCOWCandidateV1
	activated                []*PreparedCOWCandidateV1
	creator                  *allocationCreditLeaseV1 // resident allocator/state/condition owner
	activatedCreator         *allocationCreditLeaseV1 // actual activated-vector backing
	ownedCandidates          *PreparedCOWCandidateV1  // actual backing owners through physical terminal
	residentAdmissionEpoch   uint64
	residentAdmissionBytes   uint64
	residentAdmissionCreator *allocationCreditLeaseV1 // nonowning: txn.buildCreator owns the active edge
	waitErr                  error
	boundedPrune             bool
	pruneWork                BoundedPruneStats
	preparationCopyWork      FreelistStateCopyWorkV1
	ready                    *sync.Cond
	generationLeases         *PublishedGenerationLeaseV1
	rawGenerationEscaped     bool
	closed                   bool
	readyWaiters             uint64 // actual Cond.Wait borrows, including wake/reacquire
}

// COWPrepareProfileV1 is an allocation-free census of the live transaction
// that PrepareCOWCandidateRetiringV1 copies before materializing a candidate.
// It is a snapshot, not an admission limit: callers must also account for
// retirements, pruning, and pages produced after the census.
type COWPrepareProfileV1 struct {
	BoundedPruneWork BoundedPruneStats
	// Cumulative actual preparation work, including failed/aborted attempts.
	// Observation only; neither reuse authority nor a credit exemption.
	PreparationCopyWork      FreelistStateCopyWorkV1
	Valid                    bool
	HighWater                uint64
	RetiredPages             uint64
	FreePages                uint64
	AllocatedPages           uint64
	AbandonedAppendExtents   uint64
	ChangedChunks            uint64
	ReplacedMetadataPages    uint64
	BaseReservationExtents   uint64
	BaseMetadataPages        uint64
	LedgerOwners             uint64
	LedgerCandidates         uint64
	LedgerBurnedTailRanges   uint64
	LedgerHighestReservedEnd uint64
}

// COWPrepareLimitsV1 bounds every live collection copied by prepared COW
// materialization. Zero-valued limits are strict; callers that do not need a
// prepared admission guard must pass nil.
type COWPrepareLimitsV1 struct {
	// AllocationCredit is the allocator facet of the same request owner.
	// Non-nil requires a complete joint pre-WAL certificate.
	AllocationCredit            AllocationCreditV1
	MaxHighWater                uint64
	MaxRetiredPages             uint64
	MaxAllocatedPages           uint64
	MaxAbandonedAppendExtents   uint64
	MaxChangedChunks            uint64
	MaxReplacedMetadataPages    uint64
	MaxBaseReservationExtents   uint64
	MaxBaseMetadataPages        uint64
	MaxLedgerOwners             uint64
	MaxLedgerCandidates         uint64
	MaxLedgerBurnedTailRanges   uint64
	MaxLedgerHighestReservedEnd uint64
	MaxRetirementPages          uint64
	MaxAuxiliaryPages           uint64
}

func (a *Allocator) COWPrepareProfileV1() COWPrepareProfileV1 {
	var profile COWPrepareProfileV1
	if a == nil {
		return profile
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return profile
	}
	return a.cowPrepareProfileLockedV1()
}

func (a *Allocator) cowPrepareProfileLockedV1() COWPrepareProfileV1 {
	var profile COWPrepareProfileV1
	if a.cow == nil || a.cow.txn == nil || a.cow.generation == nil {
		return profile
	}
	transaction, base := a.cow.txn, a.cow.generation
	profile.BoundedPruneWork = a.cow.pruneWork
	profile.PreparationCopyWork = a.cow.preparationCopyWork
	profile.Valid = true
	profile.HighWater = transaction.highWater
	profile.RetiredPages = transaction.root.retiredCount
	profile.FreePages = transaction.root.freeCount
	profile.AllocatedPages = uint64(len(transaction.allocated))
	profile.AbandonedAppendExtents = uint64(len(transaction.abandonedAppends))
	profile.ChangedChunks = uint64(transaction.changedChunks.Len())
	profile.ReplacedMetadataPages = uint64(transaction.replacedMetadata.Len())
	profile.BaseReservationExtents = uint64(len(base.record.Extents))
	profile.BaseMetadataPages = uint64(len(base.metadataPages))
	if ledger := a.cow.ledger; ledger != nil {
		// COW preparation enters the ledger while holding a.mu. Keep the
		// same lock order so the census sees the reservation state it uses.
		ledger.mu.Lock()
		profile.LedgerOwners = uint64(ledger.owners.Len())
		profile.LedgerCandidates = uint64(ledger.candidates.Len())
		profile.LedgerBurnedTailRanges = uint64(len(ledger.burnedTails))
		ledger.owners.Range(func(id uint64, _ CandidateIDV1) bool {
			if id == ^uint64(0) {
				profile.LedgerHighestReservedEnd = ^uint64(0)
			} else {
				profile.LedgerHighestReservedEnd = max(profile.LedgerHighestReservedEnd, id+1)
			}
			return true
		})
		for _, burned := range ledger.burnedTails {
			profile.LedgerHighestReservedEnd = max(profile.LedgerHighestReservedEnd, reservationIntervalEndSaturated(burned))
		}
		ledger.candidates.Range(func(_ CandidateIDV1, reservation *reservation) bool {
			if reservation != nil && reservation.tailReserved {
				profile.LedgerHighestReservedEnd = max(profile.LedgerHighestReservedEnd, reservationIntervalEndSaturated(reservationInterval{start: reservation.tailStart, count: reservation.tailCount}))
			}
			return true
		})
		ledger.mu.Unlock()
	}
	return profile
}

func checkCOWPrepareLimitsV1(profile COWPrepareProfileV1, retirements []COWRetirementV1, auxiliaryPageCount int, limits *COWPrepareLimitsV1) error {
	if limits == nil {
		return nil
	}
	if !profile.Valid || profile.HighWater > limits.MaxHighWater || profile.RetiredPages > limits.MaxRetiredPages ||
		profile.AllocatedPages > limits.MaxAllocatedPages || profile.AbandonedAppendExtents > limits.MaxAbandonedAppendExtents ||
		profile.ChangedChunks > limits.MaxChangedChunks || profile.ReplacedMetadataPages > limits.MaxReplacedMetadataPages ||
		profile.BaseReservationExtents > limits.MaxBaseReservationExtents || profile.BaseMetadataPages > limits.MaxBaseMetadataPages ||
		profile.LedgerOwners > limits.MaxLedgerOwners || profile.LedgerCandidates > limits.MaxLedgerCandidates ||
		profile.LedgerBurnedTailRanges > limits.MaxLedgerBurnedTailRanges ||
		profile.LedgerHighestReservedEnd > limits.MaxLedgerHighestReservedEnd {
		return fmt.Errorf("%w: COW prepare profile exceeds prepared limits", ErrGenerationFormat)
	}
	var retirementPages uint64
	for i := range retirements {
		if uint64(len(retirements[i].PageIDs)) > ^uint64(0)-retirementPages {
			return fmt.Errorf("%w: COW retirement count overflow", ErrGenerationFormat)
		}
		retirementPages += uint64(len(retirements[i].PageIDs))
	}
	if retirementPages > limits.MaxRetirementPages || auxiliaryPageCount < 0 || uint64(auxiliaryPageCount) > limits.MaxAuxiliaryPages {
		return fmt.Errorf("%w: COW prepare growth exceeds prepared limits", ErrGenerationFormat)
	}
	return nil
}

// CheckCOWPrepareProfileLimitsV1 applies the same live-profile check used
// under the allocator lock by candidate preparation. It lets a serialized
// command reject an already oversized base before appending its WAL record;
// candidate preparation repeats the check to catch later growth.
func CheckCOWPrepareProfileLimitsV1(profile COWPrepareProfileV1, limits COWPrepareLimitsV1) error {
	return checkCOWPrepareLimitsV1(profile, nil, 0, &limits)
}

func reservationIntervalEndSaturated(interval reservationInterval) uint64 {
	if interval.count > ^uint64(0)-interval.start {
		return ^uint64(0)
	}
	return interval.start + interval.count
}

// COWRetirementV1 is one retirement set staged atomically with a COW
// candidate. The live allocator transaction is unchanged if preparation fails
// or the caller aborts before publication.
type COWRetirementV1 struct {
	PageIDs                []uint64
	LastReachableCommitSeq uint64
}

// PreparedCOWCandidateV1 is the immutable allocator handoff consumed by the
// durable-root publisher. Auxiliary page IDs are reserved as ordinary data
// pages in the same generation so manifest and root-record pages cannot race
// allocator metadata or later index allocation.
type PreparedCOWCandidateV1 struct {
	backingMu     sync.RWMutex
	ownedNext     *PreparedCOWCandidateV1
	ownedBacking  bool
	creator       *allocationCreditLeaseV1
	allocator     *Allocator
	candidate     *FreelistCandidateV1
	candidateID   CandidateIDV1
	auxiliary     []uint64
	rollbackTxn   *FreelistTxn
	rollbackStats Stats
	activated     bool
	published     bool
}

func (prepared *PreparedCOWCandidateV1) Candidate() *FreelistCandidateV1 {
	if prepared == nil {
		return nil
	}
	if prepared.allocator != nil {
		prepared.allocator.mu.Lock()
		defer prepared.allocator.mu.Unlock()
	}
	prepared.backingMu.RLock()
	defer prepared.backingMu.RUnlock()
	if prepared == nil || prepared.candidate == nil || prepared.candidate.hasFiniteBackingV1() {
		return nil
	}
	prepared.candidate.markOrdinaryEscapeV1()
	if prepared.allocator != nil && prepared.allocator.cow != nil {
		prepared.allocator.cow.rawGenerationEscaped = true
	}
	return prepared.candidate
}

func (prepared *PreparedCOWCandidateV1) CandidateID() CandidateIDV1 {
	if prepared == nil {
		return CandidateIDV1{}
	}
	return prepared.candidateID
}

func (prepared *PreparedCOWCandidateV1) AuxiliaryPageIDs() []uint64 {
	if prepared == nil {
		return nil
	}
	prepared.backingMu.RLock()
	defer prepared.backingMu.RUnlock()
	if prepared == nil || prepared.candidate == nil || prepared.candidate.hasFiniteBackingV1() {
		return nil
	}

	return append([]uint64(nil), prepared.auxiliary...)
}

func (a *Allocator) EnableCOWV1(generation *FreelistGenerationV1, ledger *ReservationLedger) error {
	if generation != nil {
		generation.markOrdinaryEscapeV1()
	}
	return a.enableCOWOwnedV1(generation, ledger)
}

func (a *Allocator) enableCOWOwnedV1(generation *FreelistGenerationV1, ledger *ReservationLedger) error {
	if a == nil || a.pager == nil || generation == nil {
		return ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return ErrCandidateConsumed
	}
	if a.cow != nil {
		return fmt.Errorf("%w: allocator already uses COW freelist", ErrGenerationFormat)
	}
	if err := generation.Validate(); err != nil {
		return err
	}
	if a.pager.PageCount() != generation.HighWater() {
		return fmt.Errorf("%w: pager pages %d do not match generation high-water %d", ErrGenerationFormat, a.pager.PageCount(), generation.HighWater())
	}
	if ledger == nil {
		ledger = newReservationLedgerOwnedV1()
	}
	txn, err := beginCandidateOwnedV1(generation, generation.GenerationRef(), ledger)
	if err != nil {
		return err
	}
	retainReservationLedgerV1(ledger)
	state := &allocatorCOWStateV1{generation: retainGenerationV1(generation), txn: txn, ledger: ledger, rawGenerationEscaped: atomic.LoadUint32(&generation.escaped) != 0}
	state.ready = sync.NewCond(&a.mu)
	a.cow = state
	a.head = 0
	a.stats.Pages = 0
	a.stats.FreeIDs = generation.FreeCount()
	return nil
}

func (a *Allocator) waitCOWReadyLocked() error {
	if a.closed {
		return ErrCandidateConsumed
	}
	state := a.cow
	for state != nil && state.prepared != nil && state.waitErr == nil {
		if TestHookCOWWaitBeforeSleep != nil {
			TestHookCOWWaitBeforeSleep()
		}
		// This exact Cond borrow survives Wait's release/reacquire of a.mu.
		state.readyWaiters++
		state.ready.Wait()
		state.readyWaiters--
		if a.closed {
			a.releaseClosedCOWStateCreatorLockedV1()
			return ErrCandidateConsumed
		}
	}
	if a.closed {
		return ErrCandidateConsumed
	}
	if state != nil && state.waitErr != nil {
		return state.waitErr
	}
	return nil
}

func (a *Allocator) allocCOWLocked(hint uint64) (uint64, error) {
	if err := a.waitCOWReadyLocked(); err != nil {
		return 0, err
	}
	if a.cow == nil || a.cow.txn == nil {
		return 0, ErrGenerationFormat
	}
	var (
		id  uint64
		err error
	)
	if a.preferAppend {
		id, err = a.cow.txn.AllocateAppend()
	} else {
		id, err = a.cow.txn.Allocate(hint)
	}
	if err != nil {
		return 0, err
	}
	if id >= a.pager.PageCount() {
		if err := a.pager.Truncate(id + 1); err != nil {
			return 0, err
		}
		a.stats.AppendAllocPages++
	} else {
		a.stats.ReuseAllocPages++
	}
	a.stats.AllocPages++
	a.lastAlloc = id
	return id, nil
}

// AllocAppend allocates one page above the current logical high-water without
// consulting reusable pages. Unlike toggling SetPreferAppend around Alloc, the
// choice is scoped to this allocation while the allocator lock is held.
func (a *Allocator) AllocAppend() (uint64, error) {
	if a == nil || a.pager == nil {
		return 0, ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return 0, ErrCandidateConsumed
	}
	if a.cow == nil {
		id, err := a.pager.Alloc(1)
		if err == nil {
			a.stats.AllocPages++
			a.stats.AppendAllocPages++
			a.lastAlloc = id
		}
		return id, err
	}
	if err := a.waitCOWReadyLocked(); err != nil {
		return 0, err
	}
	if a.cow == nil || a.cow.txn == nil {
		return 0, ErrGenerationFormat
	}
	// Activated build-ahead generations may reserve a virtual tail in an
	// immutable memory-backed candidate before the publisher materializes that
	// tail in the pager. Append after the transaction's logical high-water so it
	// cannot overlap those reservations. A physical tail beyond the transaction
	// remains invalid because the allocator has no authority for those pages.
	if a.cow.txn.highWater < a.pager.PageCount() {
		return 0, fmt.Errorf("%w: append high-water %d is behind pager pages %d", ErrGenerationFormat, a.cow.txn.highWater, a.pager.PageCount())
	}
	id, err := a.cow.txn.AllocateAppend()
	if err != nil {
		return 0, err
	}
	if id >= a.pager.PageCount() {
		if err := a.pager.Truncate(id + 1); err != nil {
			return 0, err
		}
	}
	a.stats.AllocPages++
	a.stats.AppendAllocPages++
	a.lastAlloc = id
	return id, nil
}

func (a *Allocator) retireCOWLocked(ids []uint64, lastReachableCommitSeq uint64) error {
	if err := a.waitCOWReadyLocked(); err != nil {
		return err
	}
	if a.cow == nil || a.cow.txn == nil || lastReachableCommitSeq == 0 {
		return ErrGenerationFormat
	}
	for _, id := range ids {
		if id < 2 {
			return errCannotFreePageZero
		}
	}
	if len(ids) == 1 {
		a.cow.txn.Retire(ids[0], lastReachableCommitSeq)
	} else {
		retired := make([]retiredPage, len(ids))
		for i, id := range ids {
			retired[i] = retiredPage{id: id, lastReachableCommitSeq: lastReachableCommitSeq}
		}
		a.cow.txn.retireMany(retired)
	}
	if err := a.cow.txn.valid(); err != nil {
		return err
	}
	a.stats.FreePages += uint64(len(ids))
	if TestHookRetireCOWBeforeUnlock != nil {
		TestHookRetireCOWBeforeUnlock()
	}
	return nil
}

func (a *Allocator) RetireCOWV1(ids []uint64, lastReachableCommitSeq uint64) error {
	if len(ids) == 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return ErrCandidateConsumed
	}
	if a.cow == nil {
		return ErrGenerationFormat
	}
	return a.retireCOWLocked(ids, lastReachableCommitSeq)
}

func (a *Allocator) PrepareCOWCandidateV1(generationID, commitSeq uint64, candidateID CandidateIDV1, capability ReuseCapability, auxiliaryPageCount int, sink AppendPageSink) (*PreparedCOWCandidateV1, error) {
	return a.PrepareCOWCandidateRetiringV1(generationID, commitSeq, candidateID, capability, nil, auxiliaryPageCount, sink)
}

// PrepareCOWCandidateRetiringV1 stages retirements and candidate
// materialization as one allocator transaction. The pre-prepare transaction
// remains available for rollback until the candidate publishes.
func (a *Allocator) PrepareCOWCandidateRetiringV1(generationID, commitSeq uint64, candidateID CandidateIDV1, capability ReuseCapability, retirements []COWRetirementV1, auxiliaryPageCount int, sink AppendPageSink) (*PreparedCOWCandidateV1, error) {
	return a.PrepareCOWCandidateRetiringWithLimitsV1(generationID, commitSeq, candidateID, capability, retirements, auxiliaryPageCount, sink, nil)
}

// PrepareCOWCandidateRetiringWithLimitsV1 checks the prepared allocation
// profile while holding the same allocator lock used by the subsequent clone.
// This prevents concurrent allocator growth between admission and allocation.
// Private preparation adds no ownership collection. Before its dirty-tree
// isolation allocates, the same profile bounds HighWater and ChangedChunks.
// A valid tree has at most ceil(HighWater/256) chunks and 15 nodes per chunk
// (plus an empty root); each dirty node/chunk is detached at most once.
// Durable first copies also detach hidden zero-ID child subtrees. All such
// copies and nonnil node visits are attributed; no node is detached twice in
// one exclusive builder. Old and private trees overlap until activation/abort.
// Existing count admission is not a complete caller byte-reservation proof.
func (a *Allocator) PrepareCOWCandidateRetiringWithLimitsV1(generationID, commitSeq uint64, candidateID CandidateIDV1, capability ReuseCapability, retirements []COWRetirementV1, auxiliaryPageCount int, sink AppendPageSink, limits *COWPrepareLimitsV1) (*PreparedCOWCandidateV1, error) {
	return a.prepareCOWCandidateRetiringWithLimitsV1(generationID, commitSeq, candidateID, capability, retirements, auxiliaryPageCount, sink, limits, false)
}

// PrepareOwnedCOWCandidateRetiringWithLimitsV1 transfers sole backing ownership
// to a trusted DB publication control. The caller must invoke the exact terminal
// clear after Consume/Abort, or retain it through ambiguous recovery to shutdown.
// Scalars and the concrete pager are borrows; raw exports permanently taint
// resident admission. This uses the ordinary allocator engine.
func (a *Allocator) PrepareOwnedCOWCandidateRetiringWithLimitsV1(generationID, commitSeq uint64, candidateID CandidateIDV1, capability ReuseCapability, retirements []COWRetirementV1, auxiliaryPageCount int, sink AppendPageSink, limits *COWPrepareLimitsV1) (*PreparedCOWCandidateV1, error) {
	return a.prepareCOWCandidateRetiringWithLimitsV1(generationID, commitSeq, candidateID, capability, retirements, auxiliaryPageCount, sink, limits, true)
}

func (a *Allocator) prepareCOWCandidateRetiringWithLimitsV1(generationID, commitSeq uint64, candidateID CandidateIDV1, capability ReuseCapability, retirements []COWRetirementV1, auxiliaryPageCount int, sink AppendPageSink, limits *COWPrepareLimitsV1, owned bool) (*PreparedCOWCandidateV1, error) {
	if auxiliaryPageCount < 0 || sink == nil {
		return nil, ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil, ErrCandidateConsumed
	}
	if a.cow == nil || a.cow.txn == nil {
		return nil, ErrGenerationFormat
	}
	if a.cow.waitErr != nil {
		return nil, a.cow.waitErr
	}
	if limits != nil && limits.AllocationCredit != nil {
		// A different request facet cannot borrow an ordinary cached candidate.
		// Keep the complete caller/resident certificate closed before every retry.
		return nil, ErrAllocationCertificateIncompleteV1
	}
	if a.cow.prepared != nil {
		generation := a.cow.prepared.candidate.generation
		if generation == nil || generation.GenerationID() != generationID || generation.CommitSeq() != commitSeq ||
			a.cow.prepared.candidateID != candidateID || len(a.cow.prepared.auxiliary) != auxiliaryPageCount {
			return nil, ErrCOWCandidatePrepared
		}
		if owned && !a.cow.prepared.ownedBacking {
			return nil, ErrFiniteAllocationExportV1
		}
		if !owned {
			a.cow.rawGenerationEscaped = true
		}
		return a.cow.prepared, nil
	}
	if err := checkCOWPrepareLimitsV1(a.cowPrepareProfileLockedV1(), retirements, auxiliaryPageCount, limits); err != nil {
		return nil, err
	}
	rollbackTxn := a.cow.txn
	rollbackStats := a.stats
	beforeCopyWork := rollbackTxn.stats.FreelistStateCopyWorkV1
	staged, err := rollbackTxn.cloneForPrivateAllocatorPrepare()
	if err != nil {
		return nil, err
	}
	// Retain actual work even when logical allocator state is rolled back or
	// an already-produced candidate is later aborted. Activation cannot erase
	// this attribution when it begins the next immutable-generation transaction.
	defer func() {
		work := staged.stats.FreelistStateCopyWorkV1
		a.cow.preparationCopyWork.StateNodeCopies += work.StateNodeCopies - beforeCopyWork.StateNodeCopies
		a.cow.preparationCopyWork.StateChunkCopies += work.StateChunkCopies - beforeCopyWork.StateChunkCopies
		a.cow.preparationCopyWork.StateCopyBytes += work.StateCopyBytes - beforeCopyWork.StateCopyBytes
		a.cow.preparationCopyWork.StateIsolationVisits += work.StateIsolationVisits - beforeCopyWork.StateIsolationVisits
	}()
	a.cow.txn = staged
	rollback := func(cause error) (*PreparedCOWCandidateV1, error) {
		a.cow.txn = rollbackTxn
		releaseTxnV1(staged)
		a.stats = rollbackStats
		if rollbackErr := a.cow.ledger.RollbackPreVisible(candidateID); rollbackErr != nil {
			return nil, errors.Join(cause, fmt.Errorf("rollback COW reservation: %w", rollbackErr))
		}
		return nil, cause
	}
	for _, retirement := range retirements {
		if len(retirement.PageIDs) == 0 {
			continue
		}
		if err := a.retireCOWLocked(retirement.PageIDs, retirement.LastReachableCommitSeq); err != nil {
			return rollback(err)
		}
	}
	// Encode every page that the caller's sealed reuse capability permits as
	// free in this exact durable generation. A prepared candidate is immutable;
	// retry returns it above instead of applying a fresher capability after the
	// caller releases its reader-admission gate.
	a.pruneCOWLockedV1(a.cow.txn, capability)
	auxiliary, err := a.cow.txn.allocateContiguousRange(auxiliaryPageCount)
	if err != nil {
		return rollback(err)
	}
	candidate, err := a.cow.txn.materializeCandidateOwnedV1(generationID, commitSeq, candidateID, sink)
	if err != nil {
		return rollback(err)
	}
	prepared := &PreparedCOWCandidateV1{
		allocator: a, candidate: candidate, candidateID: candidateID, auxiliary: auxiliary, ownedBacking: owned,
		rollbackTxn: rollbackTxn, rollbackStats: rollbackStats,
	}
	if owned {
		prepared.ownedNext = a.cow.ownedCandidates
		a.cow.ownedCandidates = prepared
	} else {
		a.cow.rawGenerationEscaped = true
	}
	a.cow.prepared = prepared
	return prepared, nil
}

// AbortCOWCandidateV1 rolls back a candidate that has not crossed visibility.
// Reservation tails accepted by an arbitrary sink remain conservatively
// burned, while the live allocation and retirement transaction is restored.
func (a *Allocator) AbortCOWCandidateV1(prepared *PreparedCOWCandidateV1) error {
	if a == nil || prepared == nil || prepared.rollbackTxn == nil {
		return ErrGenerationFormat
	}
	if TestHookAbortCOWCandidateFailure != nil {
		if err := TestHookAbortCOWCandidateFailure(); err != nil {
			return err
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return ErrCandidateConsumed
	}
	if a.cow == nil || a.cow.prepared != prepared {
		return ErrCandidateConsumed
	}
	if a.cow.waitErr != nil {
		return a.cow.waitErr
	}
	if err := a.cow.ledger.RollbackPreVisible(prepared.candidateID); err != nil {
		return err
	}
	stage := a.cow.txn
	a.cow.txn = prepared.rollbackTxn
	releaseTxnV1(stage)
	replaceStateNodeV1(&a.cow.txn.root, detachUnmaterializedWithStats(a.cow.txn.root, 0, &a.cow.txn.stats))
	a.cow.txn.privatePreparation = true
	a.stats = prepared.rollbackStats
	a.cow.prepared = nil
	prepared.rollbackTxn = nil
	a.cow.ready.Broadcast()
	return nil
}

// ActivateCOWCandidateV1 makes a prepared allocator generation visible to
// subsequent builders without claiming that its durable root has published.
// The exact candidate remains ledger-owned until PublishActivatedCOWThroughV1
// consumes it. This split is what permits several visible COW generations to
// accumulate behind one dependency-closed durable-root publication.
func (a *Allocator) ActivateCOWCandidateV1(prepared *PreparedCOWCandidateV1) error {
	if a == nil || prepared == nil || prepared.candidate == nil || prepared.candidate.generation == nil {
		return ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return ErrCandidateConsumed
	}
	if a.cow == nil || a.cow.prepared != prepared || prepared.activated || prepared.published {
		return ErrCandidateConsumed
	}
	if a.cow.waitErr != nil {
		return a.cow.waitErr
	}
	generation := prepared.candidate.generation
	if err := a.growActivatedBackingLockedV1(prepared.creator); err != nil {
		return err
	}
	next, err := beginCandidateOwnedV1(generation, generation.GenerationRef(), a.cow.ledger)
	if err != nil {
		return err
	}
	if err := a.cow.ledger.MarkVisible(prepared.candidateID); err != nil {
		releaseTxnV1(next)
		return err
	}
	next.pruneCursor = a.cow.txn.pruneCursor
	oldGeneration, oldTxn := a.cow.generation, a.cow.txn
	a.cow.generation = retainGenerationV1(generation)
	a.cow.txn = next
	releaseGenerationV1(oldGeneration)
	releaseTxnV1(oldTxn)
	a.cow.prepared = nil
	a.cow.activated = append(a.cow.activated, prepared)
	releaseTxnV1(prepared.rollbackTxn)
	prepared.rollbackTxn = nil
	prepared.activated = true
	a.stats.FreeIDs = generation.FreeCount()
	a.cow.ready.Broadcast()
	return nil
}

// PublishActivatedCOWPrefixV1 consumes exactly the caller-proven ordered
// visible prefix after the matching durable meta is stable. Missing,
// reordered, or extra members are rejected before the ledger or allocator is
// mutated. Newer activated generations remain reserved.
func (a *Allocator) PublishActivatedCOWPrefixV1(prefix []*PreparedCOWCandidateV1, nextCapability ReuseCapability) error {
	if a == nil || len(prefix) == 0 {
		return ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return ErrCandidateConsumed
	}
	return a.publishActivatedCOWPrefixLockedV1(prefix, nextCapability)
}

func (a *Allocator) publishActivatedCOWPrefixLockedV1(prefix []*PreparedCOWCandidateV1, nextCapability ReuseCapability) error {
	if a.cow == nil {
		return ErrCandidateConsumed
	}
	if a.cow.waitErr != nil {
		return a.cow.waitErr
	}
	if len(prefix) > len(a.cow.activated) {
		return ErrCandidateConsumed
	}
	for i, prepared := range prefix {
		if prepared == nil || !prepared.activated || prepared.published {
			return ErrGenerationFormat
		}
		if a.cow.activated[i] != prepared {
			return fmt.Errorf("%w: activated prefix member %d does not match", ErrCandidateConsumed, i)
		}
	}
	prepared := prefix[len(prefix)-1]
	generation := prepared.candidate.generation
	if generation == nil {
		return ErrGenerationFormat
	}
	if err := a.validateCOWPhysicalTailLockedV1(generation.HighWater()); err != nil {
		return err
	}
	ids := make([]CandidateIDV1, len(prefix))
	for i, candidate := range prefix {
		ids[i] = candidate.candidateID
	}
	if err := a.cow.ledger.PublishBatch(ids); err != nil {
		return err
	}
	for _, candidate := range prefix {
		candidate.published = true
	}
	copy(a.cow.activated, a.cow.activated[len(prefix):])
	clear(a.cow.activated[len(a.cow.activated)-len(prefix):])
	a.cow.activated = a.cow.activated[:len(a.cow.activated)-len(prefix)]
	// The live transaction may already be based on a newer visible generation.
	// Pruning it with the newly advanced recovery horizon is conservative and
	// does not alter any immutable activated generation.
	a.pruneCOWLockedV1(a.cow.txn, nextCapability)
	a.cow.ready.Broadcast()
	return nil
}

// ValidateCOWPhysicalTailV1 proves that the physical pager covers required
// publication state without extending beyond the live allocator transaction's
// logical authority. A physical tail above the published prefix is valid when
// it belongs to a newer activated or in-progress generation.
func (a *Allocator) ValidateCOWPhysicalTailV1(requiredHighWater uint64) error {
	if a == nil || a.pager == nil {
		return ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return ErrCandidateConsumed
	}
	return a.validateCOWPhysicalTailLockedV1(requiredHighWater)
}

func (a *Allocator) validateCOWPhysicalTailLockedV1(requiredHighWater uint64) error {
	if a.cow == nil || a.cow.txn == nil {
		return ErrGenerationFormat
	}
	pageCount := a.pager.PageCount()
	if pageCount < requiredHighWater {
		return fmt.Errorf("%w: pager pages %d do not cover activated generation high-water %d", ErrGenerationFormat, pageCount, requiredHighWater)
	}
	if pageCount > a.cow.txn.highWater {
		return fmt.Errorf("%w: pager pages %d exceed live logical high-water %d", ErrGenerationFormat, pageCount, a.cow.txn.highWater)
	}
	return nil
}

// PublishActivatedCOWThroughV1 is the compatibility form for callers that
// identify an activated prefix by its final member. New durable-root code
// should pass its complete sealed prefix to PublishActivatedCOWPrefixV1.
func (a *Allocator) PublishActivatedCOWThroughV1(prepared *PreparedCOWCandidateV1, nextCapability ReuseCapability) error {
	if a == nil || prepared == nil {
		return ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return ErrCandidateConsumed
	}
	if a.cow == nil {
		return ErrCandidateConsumed
	}
	through := -1
	for i, candidate := range a.cow.activated {
		if candidate == prepared {
			through = i
			break
		}
	}
	if through < 0 {
		return ErrCandidateConsumed
	}
	return a.publishActivatedCOWPrefixLockedV1(a.cow.activated[:through+1], nextCapability)
}

// FailCOWCandidateV1 preserves the exact prepared candidate for close/reopen
// ownership while failing and waking allocator calls that were already
// admitted before durable-root publication became unrecoverable in-process.
// It is idempotent for the same prepared candidate.
func (a *Allocator) FailCOWCandidateV1(prepared *PreparedCOWCandidateV1, cause error) error {
	if a == nil || prepared == nil || cause == nil {
		return ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return ErrCandidateConsumed
	}
	if a.cow == nil {
		return ErrCandidateConsumed
	}
	owned := a.cow.prepared == prepared
	if !owned {
		for _, candidate := range a.cow.activated {
			if candidate == prepared {
				owned = true
				break
			}
		}
	}
	if !owned {
		return ErrCandidateConsumed
	}
	if a.cow.waitErr == nil {
		a.cow.waitErr = cause
	}
	a.cow.ready.Broadcast()
	return nil
}

func (a *Allocator) PublishCOWCandidateV1(prepared *PreparedCOWCandidateV1, nextCapability ReuseCapability) error {
	if prepared == nil || prepared.candidate == nil || prepared.candidate.generation == nil {
		return ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return ErrCandidateConsumed
	}
	if a.cow == nil || a.cow.prepared != prepared || prepared.activated || prepared.published {
		return ErrCandidateConsumed
	}
	if a.cow.waitErr != nil {
		return a.cow.waitErr
	}
	generation := prepared.candidate.generation
	if pageCount := a.pager.PageCount(); pageCount != generation.HighWater() {
		return fmt.Errorf("%w: pager pages %d do not match prepared generation high-water %d", ErrGenerationFormat, pageCount, generation.HighWater())
	}
	if err := a.cow.ledger.MarkVisible(prepared.candidateID); err != nil {
		return err
	}
	if err := a.cow.ledger.Publish(prepared.candidateID); err != nil {
		return err
	}
	next, err := beginCandidateOwnedV1(generation, prepared.candidate.GenerationRef(), a.cow.ledger)
	if err != nil {
		return err
	}
	next.pruneCursor = a.cow.txn.pruneCursor
	a.pruneCOWLockedV1(next, nextCapability)
	oldGeneration, oldTxn := a.cow.generation, a.cow.txn
	a.cow.generation = retainGenerationV1(generation)
	a.cow.txn = next
	releaseGenerationV1(oldGeneration)
	releaseTxnV1(oldTxn)
	a.cow.prepared = nil
	releaseTxnV1(prepared.rollbackTxn)
	prepared.rollbackTxn = nil
	prepared.activated = true
	prepared.published = true
	a.cow.waitErr = nil
	a.stats.FreeIDs = a.cow.generation.FreeCount()
	a.cow.ready.Broadcast()
	return nil
}

func (a *Allocator) COWGenerationV1() *FreelistGenerationV1 {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	if a.cow == nil {
		return nil
	}
	if a.cow.generation.hasFiniteBackingV1() {
		return nil
	}
	a.cow.generation.markOrdinaryEscapeV1()
	a.cow.rawGenerationEscaped = true
	return a.cow.generation
}

// EnableBoundedPruneV1 changes scheduling only. The existing capability remains
// mandatory and legacy allocators retain their original full prune behavior.
func (a *Allocator) EnableBoundedPruneV1() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return
	}
	if a.cow != nil {
		a.cow.boundedPrune = true
	}
}
func (a *Allocator) pruneCOWLockedV1(txn *FreelistTxn, cap ReuseCapability) {
	if !a.cow.boundedPrune {
		txn.PruneWithCapability(cap)
		return
	}
	work := txn.PruneWithCapabilityBounded(cap)
	a.cow.pruneWork.NodeVisits += work.NodeVisits
	a.cow.pruneWork.EntriesExamined += work.EntriesExamined
	a.cow.pruneWork.PromotedPages += work.PromotedPages
	a.cow.pruneWork.MutationPaths += work.MutationPaths
	a.cow.pruneWork.MutationItems += work.MutationItems
	a.cow.pruneWork.PageCredits += work.PageCredits
	a.cow.pruneWork.ByteCredits += work.ByteCredits
	a.cow.pruneWork.Wrapped = work.Wrapped
}
func (a *Allocator) PruneCOWBoundedStepV1(cap ReuseCapability) (BoundedPruneStats, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return BoundedPruneStats{}, ErrCandidateConsumed
	}
	if a.cow == nil || !a.cow.boundedPrune {
		return BoundedPruneStats{}, ErrGenerationFormat
	}
	if a.cow.waitErr != nil {
		return BoundedPruneStats{}, a.cow.waitErr
	}
	if a.cow.prepared != nil {
		return BoundedPruneStats{}, ErrCOWCandidatePrepared
	}
	before := a.cow.pruneWork
	a.pruneCOWLockedV1(a.cow.txn, cap)
	after := a.cow.pruneWork
	return BoundedPruneStats{
		NodeVisits: after.NodeVisits - before.NodeVisits, EntriesExamined: after.EntriesExamined - before.EntriesExamined,
		PromotedPages: after.PromotedPages - before.PromotedPages, MutationPaths: after.MutationPaths - before.MutationPaths,
		MutationItems: after.MutationItems - before.MutationItems, PageCredits: after.PageCredits - before.PageCredits,
		ByteCredits: after.ByteCredits - before.ByteCredits, Wrapped: after.Wrapped,
	}, nil
}
