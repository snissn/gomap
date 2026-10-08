package freelist

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/snissn/gomap/TreeDB/pager"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/page"
)

type FreelistGenerationV1 struct {
	ownedRefs                           uint64
	creator                             *allocationCreditLeaseV1
	escaped                             uint32
	generationID, commitSeq             uint64
	parentGenerationID, parentCommitSeq uint64
	highWater                           uint64
	root                                stateRefV1
	ref                                 GenerationRefV1
	record                              ReservationRecordV1
	metadataPages                       []uint64
}

func NewFreelistGenerationV1(generationID, highWater uint64, free []uint64, retired map[uint64]uint64) (*FreelistGenerationV1, error) {
	g, err := newFreelistGenerationOwnedV1(nil, generationID, highWater, free, retired)
	if err == nil {
		g.markOrdinaryEscapeV1()
	}
	return g, err
}

func newFreelistGenerationOwnedV1(request AllocationRequestCreditV1, generationID, highWater uint64, free []uint64, retired map[uint64]uint64) (*FreelistGenerationV1, error) {
	if generationID == 0 || highWater < 2 {
		return nil, ErrGenerationFormat
	}
	g := &FreelistGenerationV1{generationID: generationID, commitSeq: generationID, highWater: highWater, ownedRefs: 1}
	accepted := false
	defer func() {
		if !accepted {
			releaseGenerationV1(g)
		}
	}()
	seen := newPageRadixV1[struct{}]()
	defer seen.Clear()
	for _, id := range free {
		if err := validateManagedPageID(id, highWater); err != nil {
			return nil, err
		}
		if _, duplicate := seen.Get(id); duplicate {
			return nil, ErrGenerationFormat
		}
		seen.Set(id, struct{}{})
		replaceStateNodeV1(&g.root, mutateChunkForPreparation(request, g.root, id>>freelistChunkShift, 0, func(c *stateChunk) { c.setFree(id&(freelistChunkSize-1), true) }, true, nil))
	}
	for id, seq := range retired {
		if err := validateManagedPageID(id, highWater); err != nil || seq == 0 {
			return nil, ErrGenerationFormat
		}
		if _, duplicate := seen.Get(id); duplicate {
			return nil, ErrGenerationFormat
		}
		seen.Set(id, struct{}{})
		replaceStateNodeV1(&g.root, mutateChunkForPreparation(request, g.root, id>>freelistChunkShift, 0, func(c *stateChunk) { c.retired[id&(freelistChunkSize-1)] = seq }, true, nil))
	}
	if err := g.Validate(); err != nil {
		return nil, err
	}
	accepted = true
	return g, nil
}

func MustNewFreelistGenerationV1(generationID, highWater uint64, free []uint64, retired map[uint64]uint64) *FreelistGenerationV1 {
	g, err := NewFreelistGenerationV1(generationID, highWater, free, retired)
	if err != nil {
		panic(err)
	}
	return g
}

func (g *FreelistGenerationV1) GenerationID() uint64 {
	if g == nil {
		return 0
	}
	return g.generationID
}
func (g *FreelistGenerationV1) CommitSeq() uint64 {
	if g == nil {
		return 0
	}
	return g.commitSeq
}
func (g *FreelistGenerationV1) HighWater() uint64 {
	if g == nil {
		return 0
	}
	return g.highWater
}
func (g *FreelistGenerationV1) FreeCount() uint64 {
	if g == nil {
		return 0
	}
	return g.root.freeCount()
}
func (g *FreelistGenerationV1) RetiredCount() uint64 {
	if g == nil {
		return 0
	}
	return g.root.retiredCount()
}
func (g *FreelistGenerationV1) GenerationRef() GenerationRefV1 {
	if g == nil {
		return GenerationRefV1{}
	}
	return g.ref
}
func (g *FreelistGenerationV1) ReservationRecord() ReservationRecordV1 {
	if g == nil || g.hasFiniteBackingV1() {
		return ReservationRecordV1{}
	}
	g.markOrdinaryEscapeV1()
	return g.record
}
func (g *FreelistGenerationV1) Allocatable(id uint64) bool {
	if g == nil || id >= g.highWater {
		return false
	}
	c := lookupChunk(g.root, id>>freelistChunkShift)
	return c != nil && c.isFree(id&(freelistChunkSize-1))
}

func (g *FreelistGenerationV1) Validate() error {
	if g == nil || g.generationID == 0 || g.highWater < 2 {
		return ErrGenerationFormat
	}
	if err := validateCanonicalStateV2(g.root, true); err != nil {
		return err
	}
	var freeCount, retiredCount uint64
	err := walkState(g.root, 0, func(c *stateChunk) error {
		for offset := uint64(0); offset < freelistChunkSize; offset++ {
			id := c.chunkNo<<freelistChunkShift | offset
			free, seq := c.isFree(offset), c.retired[offset]
			if (free || seq != 0) && (id < 2 || id >= g.highWater) {
				return fmt.Errorf("%w: state page %d outside high-water", ErrGenerationFormat, id)
			}
			if free && seq != 0 {
				return fmt.Errorf("%w: page %d is free and retired", ErrGenerationFormat, id)
			}
			if free {
				freeCount++
			}
			if seq != 0 {
				retiredCount++
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if freeCount != g.root.freeCount() || retiredCount != g.root.retiredCount() {
		return ErrGenerationFormat
	}
	return nil
}

type ReuseCapability struct {
	oldestRecoverableCommitSeq uint64
	minPinnedSnapshotCommitSeq uint64
	historyFloorCommitSeq      uint64
}

func NewReuseCapability(oldestRecoverable, minPinned, historyFloor uint64) (ReuseCapability, error) {
	if oldestRecoverable == 0 {
		return ReuseCapability{}, fmt.Errorf("%w: missing recoverable root horizon", ErrGenerationFormat)
	}
	return ReuseCapability{oldestRecoverableCommitSeq: oldestRecoverable, minPinnedSnapshotCommitSeq: minPinned, historyFloorCommitSeq: historyFloor}, nil
}

func (h ReuseCapability) permits(last uint64) bool {
	return h.oldestRecoverableCommitSeq != 0 && last < h.oldestRecoverableCommitSeq && (h.minPinnedSnapshotCommitSeq == 0 || last < h.minPinnedSnapshotCommitSeq) && (h.historyFloorCommitSeq == 0 || last < h.historyFloorCommitSeq)
}

// RecoveryHorizon is retained for standalone model callers. Production code
// must construct and pass the opaque ReuseCapability returned by recovery.
type RecoveryHorizon struct{ OldestRecoverableCommitSeq, MinPinnedSnapshotCommitSeq, HistoryFloorCommitSeq uint64 }

func (h RecoveryHorizon) capability() ReuseCapability {
	return ReuseCapability{h.OldestRecoverableCommitSeq, h.MinPinnedSnapshotCommitSeq, h.HistoryFloorCommitSeq}
}

// FreelistStateCopyWorkV1 attributes actual structural copies and isolation
// traversal. Bytes are conservative Go allocation-class capacities, not
// serialized output bytes or authority. Reading this value consumes no credit.
type FreelistStateCopyWorkV1 struct {
	StateNodeCopies, StateChunkCopies, StateCopyBytes, StateIsolationVisits uint64
}

type FreelistTxnStats struct {
	COWChunks, COWPages, COWBytes, LogicalDelta uint64
	StateMutationPaths, StateMutationItems      uint64
	FreelistStateCopyWorkV1
	AppendAllocations, ReuseAllocations, Reservations         uint64
	FreeIDs, RetiredIDs, ReuseLag, PageVisits                 uint64
	GenerationID, ReservationRecords                          uint64
	PendingMetadataRetirements                                uint64
	CandidatePageIsolationCopies, CandidatePageIsolationBytes uint64
	OldestRecoverableCommitSeq, MinPinnedSnapshotCommitSeq    uint64
	HistoryFloorCommitSeq                                     uint64
}

type allocatedPage struct {
	id   uint64
	kind ReservationKindV1
}

type FreelistTxn struct {
	base             *FreelistGenerationV1
	ledger           *ReservationLedger
	root             stateRefV1
	highWater        uint64
	allocated        []allocatedPage
	abandonedAppends []ReservationExtentV1
	consumed         bool
	// Set after dirty aliases reachable before a durable boundary are detached.
	// First copies of durable nodes isolate their zero-ID child subtrees too.
	// Cleared at materialization; never copied into immutable generation state.
	privatePreparation bool
	ledgerOwned        bool
	allocationRequired bool // intrinsic credited origin survives ending mutable birth authority
	changedChunks      *numericRadixV1[uint64, struct{}]
	replacedMetadata   *numericRadixV1[uint64, struct{}]
	pruneCursor        uint64 // scheduling only; never reuse authority
	stats              FreelistTxnStats
	creator            *allocationCreditLeaseV1 // immutable transaction-header owner
	buildCreator       *allocationCreditLeaseV1 // mutable future-allocation owner; one real edge
	allocatedCreator   *allocationCreditLeaseV1 // actual allocated-vector backing owner
	abandonedCreator   *allocationCreditLeaseV1 // actual abandoned-vector backing owner
	allocationErr      error
}

// cloneForAllocatorPrepare creates an isolated staging transaction. Tree nodes
// are persistent copy-on-write values, so sharing the root is safe; every
// mutable slice and map must be copied because candidate materialization
// consumes and annotates the staged transaction.
func (t *FreelistTxn) cloneForAllocatorPrepare(request AllocationRequestCreditV1) (*FreelistTxn, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	if err := t.requireBirthCreditV1(request); err != nil {
		return nil, err
	}
	// Sharing a private root revokes the original's edit permission BEFORE
	// publishing the alias. Both persistent transactions now copy every edit;
	// the private wrapper below may re-enable editing only after isolation.
	t.privatePreparation = false
	refs := uint64(2) // new header and mutable build edge
	if len(t.allocated) != 0 {
		refs++
	}
	if len(t.abandonedAppends) != 0 {
		refs++
	}
	maximum := uint64(^uint(0) >> 1)
	if uint64(len(t.allocated)) > maximum/uint64(unsafe.Sizeof(allocatedPage{})) || uint64(len(t.abandonedAppends)) > maximum/uint64(unsafe.Sizeof(ReservationExtentV1{})) {
		return nil, ErrNoAllocatablePage
	}
	bytes := cowSaturatingAddV1(allocationClassV1(uint64(unsafe.Sizeof(*t)), true), cowSaturatingAddV1(allocationClassV1(uint64(len(t.allocated))*uint64(unsafe.Sizeof(allocatedPage{})), false), allocationClassV1(uint64(len(t.abandonedAppends))*uint64(unsafe.Sizeof(ReservationExtentV1{})), false)))
	if bytes == ^uint64(0) {
		return nil, ErrNoAllocatablePage
	}
	if err := t.buildCreator.reserve(request, bytes, refs); err != nil {
		return nil, err
	}
	clone := *t
	clone.creator = t.buildCreator
	clone.buildCreator = t.buildCreator
	clone.allocatedCreator, clone.abandonedCreator = nil, nil
	if len(t.allocated) != 0 {
		clone.allocatedCreator = t.buildCreator
	}
	if len(t.abandonedAppends) != 0 {
		clone.abandonedCreator = t.buildCreator
	}
	if clone.ledgerOwned {
		retainReservationLedgerV1(clone.ledger)
	}
	clone.base = retainGenerationV1(t.base)
	clone.root = retainStateNodeV1(t.root)
	clone.allocated = make([]allocatedPage, len(t.allocated))
	copy(clone.allocated, t.allocated)
	clone.abandonedAppends = make([]ReservationExtentV1, len(t.abandonedAppends))
	copy(clone.abandonedAppends, t.abandonedAppends)
	clone.changedChunks, clone.replacedMetadata = nil, nil
	var err error
	clone.changedChunks, err = t.changedChunks.CloneWithCredit(request, t.buildCreator)
	if err != nil {
		releaseTxnV1(&clone)
		return nil, err
	}
	clone.replacedMetadata, err = t.replacedMetadata.CloneWithCredit(request, t.buildCreator)
	if err != nil {
		releaseTxnV1(&clone)
		return nil, err
	}
	return &clone, nil
}

// cloneForPrivateAllocatorPrepare preserves the rollback tree before enabling
// copy-on-first-write; durable first copies isolate dirty zero-ID descendants.
// The existing persistent clone remains the reference;
// its shared dirty aliases alone do not establish private ownership.
//
// There is no identity table: the only extra ownership state is this
// transaction's flag, within the existing consumed-field padding. The tree
// detachment replaces the isolation previously required at materialization.
func (t *FreelistTxn) cloneForPrivateAllocatorPrepare(request AllocationRequestCreditV1) (*FreelistTxn, error) {
	clone, err := t.cloneForAllocatorPrepare(request)
	if err != nil {
		return nil, err
	}
	plan := dirtyStateBirthPlanV1(clone.root, 0)
	operation, err := admitAllocationOperationV1(request, clone.buildCreator, plan.nodes*stateNodeCopyCapacityV1+plan.chunks*stateChunkCopyCapacityV1, plan.nodes+plan.chunks)
	if err != nil {
		releaseTxnV1(clone)
		return nil, err
	}
	root, err := detachStateOwnedOperationV1(request, clone.root, 0, &clone.stats, &operation)
	operation.close()
	if err != nil {
		releaseTxnV1(clone)
		return nil, err
	}
	replaceStateNodeV1(&clone.root, root)
	clone.privatePreparation = true
	return clone, nil
}

func BeginCandidateV1(base *FreelistGenerationV1, expectedParent GenerationRefV1, ledger *ReservationLedger) (*FreelistTxn, error) {
	if base.hasFiniteBackingV1() {
		return nil, ErrFiniteAllocationExportV1
	}
	base.markOrdinaryEscapeV1()
	return beginCandidateOwnedV1(nil, nil, base, expectedParent, ledger)
}

func beginCandidateOwnedV1(request AllocationRequestCreditV1, scratch *AllocationCreatorV1, base *FreelistGenerationV1, expectedParent GenerationRefV1, ledger *ReservationLedger, creators ...*allocationCreditLeaseV1) (*FreelistTxn, error) {
	if base == nil || len(creators) > 1 {
		return nil, ErrGenerationFormat
	}
	if base.ref.HeaderPageID != 0 && (expectedParent.GenerationID != base.ref.GenerationID || expectedParent.Digest != base.ref.Digest || expectedParent.HeaderPageID != base.ref.HeaderPageID) {
		return nil, ErrGenerationParent
	}
	var creator *allocationCreditLeaseV1
	if len(creators) != 0 {
		creator = creators[0]
	}
	if creator == nil && base.creator != nil {
		return nil, ErrAllocationCertificateIncompleteV1
	}
	if creator != nil {
		// Active birth authority is valid only for the already managed graph.
		// Ordinary initialization still creates its ledger before admission.
		if ledger == nil || atomic.LoadUint32(&base.escaped) != 0 {
			return nil, ErrFiniteAllocationExportV1
		}
		ledger.mu.Lock()
		escaped := ledger.rawEscaped
		ledger.mu.Unlock()
		if escaped {
			return nil, ErrFiniteAllocationExportV1
		}
	}
	if ledger == nil {
		ledger = newReservationLedgerOwnedV1()
	}
	if err := creator.reserve(request, allocationClassV1(uint64(unsafe.Sizeof(FreelistTxn{})), true), 2); err != nil {
		return nil, err
	}
	retainReservationLedgerV1(ledger)
	t := &FreelistTxn{allocationRequired: creator != nil, creator: creator, buildCreator: creator, base: retainGenerationV1(base), ledger: ledger, ledgerOwned: true, root: retainStateNodeV1(base.root), highWater: base.highWater}
	complete := false
	defer func() {
		if !complete {
			releaseTxnV1(t)
		}
	}()
	var err error
	t.changedChunks, err = newPageRadixWithCreditV1[struct{}](request, creator)
	if err != nil {
		return nil, err
	}
	t.replacedMetadata, err = newPageRadixWithCreditV1[struct{}](request, creator)
	if err != nil {
		return nil, err
	}
	root, err := detachUnmaterializedOwnedV1(request, t.root, 0, &t.stats, creator)
	if err != nil {
		return nil, err
	}
	replaceStateNodeV1(&t.root, root)
	t.privatePreparation = true
	if base.ref.HeaderPageID != 0 {
		if err := t.replacedMetadata.PutWithCredit(request, base.ref.HeaderPageID, struct{}{}, creator); err != nil {
			return nil, err
		}
	}
	for _, id := range base.record.pageIDs {
		if err := t.replacedMetadata.PutWithCredit(request, id, struct{}{}, creator); err != nil {
			return nil, err
		}
	}
	retirementCount := base.record.pendingMetadataCountV1()
	if retirementCount > uint64(^uint(0)>>1)/uint64(unsafe.Sizeof(retiredPage{})) {
		return nil, ErrNoAllocatablePage
	}
	if err := reserveAllocationScratchV1(request, scratch, creator, retirementCount, uint64(unsafe.Sizeof(retiredPage{})), false); err != nil {
		return nil, err
	}
	// Exact capacity avoids implicit growth while replaying pending retirement.
	retired := make([]retiredPage, 0, int(retirementCount))
	defer func() { clear(retired[:cap(retired)]) }()
	for _, extent := range base.record.Extents {
		if extent.Kind != ReservationPendingMetadataRetirement {
			continue
		}
		for i := uint64(0); i < uint64(extent.Count); i++ {
			retired = append(retired, retiredPage{extent.StartPageID + i, extent.LastReachableCommitSeq})
		}
	}
	t.retireMany(request, retired)
	if err := t.valid(); err != nil {
		return nil, err
	}
	complete = true
	return t, nil
}

func NewFreelistTxn(base *FreelistGenerationV1, ledger *ReservationLedger) *FreelistTxn {
	t, _ := BeginCandidateV1(base, func() GenerationRefV1 {
		if base == nil {
			return GenerationRefV1{}
		}
		return base.ref
	}(), ledger)
	return t
}

func (t *FreelistTxn) valid() error {
	if t == nil || t.base == nil || t.changedChunks == nil {
		return ErrGenerationFormat
	}
	if t.consumed {
		return ErrCandidateConsumed
	}
	if t.allocationErr != nil {
		return t.allocationErr
	}
	return nil
}

func (t *FreelistTxn) mutateMany(request AllocationRequestCreditV1, chunkNo, items uint64, f func(*stateChunk)) {
	if err := t.mutateManyOwnedV1(request, chunkNo, items, f, 0); err != nil {
		t.allocationErr = err
	}
}
func (t *FreelistTxn) mutate(request AllocationRequestCreditV1, chunkNo uint64, f func(*stateChunk)) {
	t.mutateMany(request, chunkNo, 1, f)
}

// One operation admits tree births, newly tracked metadata/chunk memberships,
// and optional output-vector growth before any tree or ledger-set mutation.
type transactionMutationPlanV1 struct {
	bytes, refs                uint64
	replaced                   [chunkTrieDepth + 2]uint64
	replacementCount, capacity int
	err                        error
	vectorBytes                uint64
}

func (t *FreelistTxn) planMutationV1(chunkNo uint64, allocatedGrowth int) transactionMutationPlanV1 {
	maximum := int(^uint(0) >> 1)
	if allocatedGrowth < 0 || allocatedGrowth > maximum-len(t.allocated) {
		return transactionMutationPlanV1{err: ErrNoAllocatablePage}
	}
	tree := mutationStateBirthPlanV1(t.root, chunkNo, 0, t.privatePreparation, false, false)
	plan := transactionMutationPlanV1{bytes: tree.nodes*stateNodeCopyCapacityV1 + tree.chunks*stateChunkCopyCapacityV1, refs: tree.nodes + tree.chunks}
	add := func(id uint64) {
		if id == 0 {
			return
		}
		if _, exists := t.replacedMetadata.Get(id); exists {
			return
		}
		for i := 0; i < plan.replacementCount; i++ {
			if plan.replaced[i] == id {
				return
			}
		}
		plan.replaced[plan.replacementCount] = id
		plan.replacementCount++
	}
	// A durable empty sentinel is replaced outright by the first chunk.
	// Prefix-mismatching nonempty roots instead remain reachable as a child.
	if t.root.freeCount()+t.root.retiredCount() == 0 {
		add(t.root.pageID())
	}
	for r := t.root; !r.zero() && r.containsChunk(chunkNo); {
		add(r.pageID())
		if r.chunk != nil {
			break
		}
		r = r.branch.child[chunkNibble(chunkNo, int(r.branch.depth))]
	}
	b, n := t.replacedMetadata.insertionCapacityV1(uint64(plan.replacementCount))
	plan.bytes += b
	plan.refs += n
	if _, exists := t.changedChunks.Get(chunkNo); !exists {
		b, n = t.changedChunks.insertionCapacityV1(1)
		plan.bytes += b
		plan.refs += n
	}
	plan.capacity = grownCapacityV1(cap(t.allocated), len(t.allocated)+allocatedGrowth)
	if plan.capacity > maximum/int(unsafe.Sizeof(allocatedPage{})) {
		plan.err = ErrNoAllocatablePage
		return plan
	}
	if plan.capacity > cap(t.allocated) {
		plan.vectorBytes = allocationClassV1(uint64(plan.capacity)*uint64(unsafe.Sizeof(allocatedPage{})), false)
		plan.bytes += plan.vectorBytes
		plan.refs++
	}
	return plan
}
func (t *FreelistTxn) mutateManyOwnedV1(request AllocationRequestCreditV1, chunkNo, items uint64, f func(*stateChunk), allocatedGrowth int) error {
	if err := t.requireBirthCreditV1(request); err != nil {
		return err
	}
	if t.allocationErr != nil {
		return t.allocationErr
	}
	plan := t.planMutationV1(chunkNo, allocatedGrowth)
	if plan.err != nil {
		return plan.err
	}
	operation, err := admitAllocationOperationV1(request, t.buildCreator, plan.bytes, plan.refs)
	if err != nil {
		return err
	}
	defer operation.close()
	return t.applyMutationV1(request, chunkNo, items, f, plan, &operation)
}

// applyMutationV1 consumes an admitted receipt and never calls the facet.
func (t *FreelistTxn) applyMutationV1(request AllocationRequestCreditV1, chunkNo, items uint64, f func(*stateChunk), plan transactionMutationPlanV1, operation *allocationOperationV1) error {
	if plan.vectorBytes != 0 {
		if err := operation.take(plan.vectorBytes, 1); err != nil {
			return err
		}
		next := make([]allocatedPage, len(t.allocated), plan.capacity)
		copy(next, t.allocated)
		oldCreator := t.allocatedCreator
		clear(t.allocated[:cap(t.allocated)])
		t.allocated = next
		t.allocatedCreator = t.buildCreator
		oldCreator.release()
	}
	for _, id := range plan.replaced[:plan.replacementCount] {
		if err := t.replacedMetadata.putAdmittedV1(request, id, struct{}{}, t.buildCreator, operation); err != nil {
			return err
		}
	}
	root, err := mutateStateOwnedOperationV1(request, t.root, chunkNo, 0, f, t.privatePreparation, &t.stats, operation)
	if err != nil {
		return err
	}
	replaceStateNodeV1(&t.root, root)
	if err = t.changedChunks.putAdmittedV1(request, chunkNo, struct{}{}, t.buildCreator, operation); err != nil {
		return err
	}
	t.stats.StateMutationPaths++
	t.stats.StateMutationItems += items
	return nil
}

func (t *FreelistTxn) Allocate(regionHint uint64) (uint64, error) {
	return t.AllocateWithAllocationRequestV1(nil, regionHint)
}

func (t *FreelistTxn) AllocateWithAllocationRequestV1(request AllocationRequestCreditV1, regionHint uint64) (uint64, error) {
	if err := t.valid(); err != nil {
		return 0, err
	}
	if err := t.requireBirthCreditV1(request); err != nil {
		return 0, err
	}
	if id, ok := chooseUnreservedFreePage(t.root, regionHint, t.ledger, &t.stats.PageVisits); ok {
		offset := id & (freelistChunkSize - 1)
		if err := t.mutateManyOwnedV1(request, id>>freelistChunkShift, 1, func(c *stateChunk) { c.setFree(offset, false) }, 1); err != nil {
			return 0, err
		}
		t.allocated = append(t.allocated, allocatedPage{id, ReservationReusedData})
		t.stats.ReuseAllocations++
		return id, nil
	}
	return t.allocateAppend(request)
}

// AllocateAppend reserves a new page at the logical high-water mark without
// consulting the reusable set. It preserves append-only maintenance and the
// public PreferAppendAlloc contract while still recording exact candidate
// ownership in the COW transaction.
func (t *FreelistTxn) AllocateAppend() (uint64, error) {
	return t.AllocateAppendWithAllocationRequestV1(nil)
}

func (t *FreelistTxn) AllocateAppendWithAllocationRequestV1(request AllocationRequestCreditV1) (uint64, error) {
	if err := t.valid(); err != nil {
		return 0, err
	}
	if err := t.requireBirthCreditV1(request); err != nil {
		return 0, err
	}
	return t.allocateAppend(request)
}

func (t *FreelistTxn) allocateAppend(request AllocationRequestCreditV1) (uint64, error) {
	if err := t.requireBirthCreditV1(request); err != nil {
		return 0, err
	}
	if t.highWater == math.MaxUint64 {
		return 0, ErrNoAllocatablePage
	}
	start := t.highWater
	id, ok := t.ledger.firstUnreservedAtOrAfter(start)
	if !ok || id == math.MaxUint64 {
		return 0, ErrNoAllocatablePage
	}
	abandonedGrowth := 0
	if id > start {
		abandonedGrowth = int((id-start-1)/uint64(^uint32(0)) + 1)
	}
	if err := t.growAppendVectorsV1(request, 1, abandonedGrowth); err != nil {
		return 0, err
	}
	if id > start {
		t.abandonedAppends = appendReservationRange(t.abandonedAppends, start, id-start, ReservationAbandonedAppend, 0)
	}
	t.highWater = id + 1
	t.allocated = append(t.allocated, allocatedPage{id, ReservationAppendedData})
	t.stats.AppendAllocations++
	return id, nil
}

// allocateContiguousRange first tries a bounded reusable extent, then appends.
// Durable dependency manifests require the returned positional page chain.
func (t *FreelistTxn) allocateContiguousRange(request AllocationRequestCreditV1, count int) ([]uint64, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	if err := t.requireBirthCreditV1(request); err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, ErrGenerationFormat
	}
	if count == 0 {
		return nil, nil
	}
	if ids, ok := t.allocateReusedRange(request, count); ok {
		return ids, nil
	}
	if t.allocationErr != nil {
		return nil, t.allocationErr
	}
	width := uint64(count)
	start := t.highWater
	for {
		candidate, ok := t.ledger.firstUnreservedAtOrAfter(start)
		if !ok || candidate > math.MaxUint64-width {
			return nil, ErrNoAllocatablePage
		}
		conflict := uint64(0)
		for id := candidate; id < candidate+width; id++ {
			if t.ledger.Reserved(id) {
				conflict = id
				break
			}
		}
		if conflict != 0 {
			if conflict == math.MaxUint64 {
				return nil, ErrNoAllocatablePage
			}
			start = conflict + 1
			continue
		}
		abandonedGrowth := 0
		if candidate > t.highWater {
			abandonedGrowth = int((candidate-t.highWater-1)/uint64(^uint32(0)) + 1)
		}
		if err := reserveMaterializationBackingV1(request, t.buildCreator, uint64(count), 8, false); err != nil {
			return nil, err
		}
		if err := t.growAppendVectorsV1(request, count, abandonedGrowth); err != nil {
			return nil, err
		}
		if candidate > t.highWater {
			t.abandonedAppends = appendReservationRange(t.abandonedAppends, t.highWater, candidate-t.highWater, ReservationAbandonedAppend, 0)
		}
		ids := make([]uint64, count)
		for i := range ids {
			ids[i] = candidate + uint64(i)
			t.allocated = append(t.allocated, allocatedPage{ids[i], ReservationAppendedData})
		}
		t.highWater = candidate + width
		t.stats.AppendAllocations += width
		return ids, nil
	}
}

func (t *FreelistTxn) ReservePage(id uint64) error {
	return t.ReservePageWithAllocationRequestV1(nil, id)
}

func (t *FreelistTxn) ReservePageWithAllocationRequestV1(request AllocationRequestCreditV1, id uint64) error {
	if err := t.valid(); err != nil {
		return err
	}
	if err := t.requireBirthCreditV1(request); err != nil {
		return err
	}
	if !t.base.Allocatable(id) || !t.rootAllocatable(id) || t.ledger.Reserved(id) {
		return ErrPageReserved
	}
	for _, allocation := range t.allocated {
		if allocation.id == id {
			return ErrPageReserved
		}
	}
	offset := id & (freelistChunkSize - 1)
	if err := t.mutateManyOwnedV1(request, id>>freelistChunkShift, 1, func(c *stateChunk) { c.setFree(offset, false) }, 1); err != nil {
		return err
	}
	t.allocated = append(t.allocated, allocatedPage{id, ReservationReusedData})
	return nil
}

func (t *FreelistTxn) rootAllocatable(id uint64) bool {
	c := lookupChunk(t.root, id>>freelistChunkShift)
	return c != nil && c.isFree(id&(freelistChunkSize-1))
}

func (t *FreelistTxn) retire(request AllocationRequestCreditV1, id, seq uint64) {
	if t == nil || t.consumed || id < 2 || id >= t.highWater || seq == 0 {
		return
	}

	if err := t.requireBirthCreditV1(request); err != nil {
		t.allocationErr = err
		return
	}
	offset := id & (freelistChunkSize - 1)
	t.mutate(request, id>>freelistChunkShift, func(c *stateChunk) { c.setFree(offset, false); c.retired[offset] = seq })
}

// retireMany applies all retirements for one state chunk through a single
// persistent-tree mutation. The previous one-ID-at-a-time path cloned the
// same chunk and its full trie path repeatedly when a commit retired adjacent
// index or allocator-metadata pages.
func (t *FreelistTxn) retireMany(request AllocationRequestCreditV1, retired []retiredPage) {
	if t == nil || t.consumed || len(retired) == 0 {
		return
	}

	if err := t.requireBirthCreditV1(request); err != nil {
		t.allocationErr = err
		return
	}
	valid := retired[:0]
	for _, item := range retired {
		if item.id < 2 || item.id >= t.highWater || item.lastReachableCommitSeq == 0 {
			continue
		}
		valid = append(valid, item)
	}
	if len(valid) == 0 {
		return
	}
	if len(valid) == 1 {
		t.retire(request, valid[0].id, valid[0].lastReachableCommitSeq)
		return
	}
	// Stable ordering preserves last-writer behavior for duplicate IDs with
	// different retirement sequences while making every chunk one run.
	slices.SortStableFunc(valid, func(a, b retiredPage) int {
		return cmp.Compare(a.id>>freelistChunkShift, b.id>>freelistChunkShift)
	})
	for start := 0; start < len(valid); {
		chunkNo := valid[start].id >> freelistChunkShift
		end := start + 1
		for end < len(valid) && valid[end].id>>freelistChunkShift == chunkNo {
			end++
		}
		items := valid[start:end]
		t.mutateMany(request, chunkNo, uint64(len(items)), func(c *stateChunk) {
			for _, item := range items {
				offset := item.id & (freelistChunkSize - 1)
				c.setFree(offset, false)
				c.retired[offset] = item.lastReachableCommitSeq
			}
		})
		start = end
	}
}

func (t *FreelistTxn) Retire(id, seq uint64) { t.RetireWithAllocationRequestV1(nil, id, seq) }

func (t *FreelistTxn) RetireWithAllocationRequestV1(request AllocationRequestCreditV1, id, seq uint64) {
	if t != nil {
		t.retire(request, id, seq)
	}
}

func capabilityThreshold(c ReuseCapability) uint64 {
	threshold := c.oldestRecoverableCommitSeq
	for _, value := range []uint64{c.minPinnedSnapshotCommitSeq, c.historyFloorCommitSeq} {
		if value != 0 && (threshold == 0 || value < threshold) {
			threshold = value
		}
	}
	return threshold
}

func collectPrunable(r stateRefV1, _ int, cap ReuseCapability, out *[]retiredPage, visits *uint64) {
	if r.zero() || r.retiredCount() == 0 || r.minRetiredSeq() == 0 || r.minRetiredSeq() >= capabilityThreshold(cap) {
		return
	}
	*visits++
	if r.chunk != nil {
		for offset, seq := range r.chunk.retired {
			if seq != 0 && cap.permits(seq) {
				*out = append(*out, retiredPage{r.chunk.chunkNo<<freelistChunkShift | uint64(offset), seq})
			}
		}
		return
	}
	for _, child := range r.branch.child {
		collectPrunable(child, 0, cap, out, visits)
	}
}

func (t *FreelistTxn) PruneWithCapability(cap ReuseCapability) {
	t.PruneWithCapabilityWithAllocationRequestV1(nil, nil, cap)
}

func (t *FreelistTxn) PruneWithCapabilityWithAllocationRequestV1(request AllocationRequestCreditV1, scratch *AllocationCreatorV1, capability ReuseCapability) {
	if t == nil || t.consumed || capability.oldestRecoverableCommitSeq == 0 {
		return
	}

	if err := t.requireBirthCreditV1(request); err != nil {
		t.allocationErr = err
		return
	}
	// The legacy whole-tree API admits one full promotion capacity before
	// collection. The prepared production caller keeps the bounded API below.
	var promote []retiredPage
	defer func() { clear(promote[:cap(promote)]) }()
	if t.buildCreator != nil {
		capacity := t.root.retiredCount()
		if err := reserveAllocationScratchV1(request, scratch, t.buildCreator, capacity, uint64(unsafe.Sizeof(retiredPage{})), false); err != nil {
			t.allocationErr = err
			return
		}
		if capacity > uint64(^uint(0)>>1) {
			t.allocationErr = ErrNoAllocatablePage
			return
		}
		promote = make([]retiredPage, 0, int(capacity))
	}
	t.stats.OldestRecoverableCommitSeq = capability.oldestRecoverableCommitSeq
	t.stats.MinPinnedSnapshotCommitSeq = capability.minPinnedSnapshotCommitSeq
	t.stats.HistoryFloorCommitSeq = capability.historyFloorCommitSeq
	collectPrunable(t.root, 0, capability, &promote, &t.stats.PageVisits)
	for _, retired := range promote {
		if lag := capability.oldestRecoverableCommitSeq - retired.lastReachableCommitSeq; lag > t.stats.ReuseLag {
			t.stats.ReuseLag = lag
		}
	}
	if len(promote) == 1 {
		item := promote[0]
		offset := item.id & (freelistChunkSize - 1)
		t.mutate(request, item.id>>freelistChunkShift, func(c *stateChunk) {
			c.retired[offset] = 0
			c.setFree(offset, true)
		})
		return
	}
	slices.SortStableFunc(promote, func(a, b retiredPage) int {
		return cmp.Compare(a.id>>freelistChunkShift, b.id>>freelistChunkShift)
	})
	for start := 0; start < len(promote); {
		chunkNo := promote[start].id >> freelistChunkShift
		end := start + 1
		for end < len(promote) && promote[end].id>>freelistChunkShift == chunkNo {
			end++
		}
		items := promote[start:end]
		t.mutateMany(request, chunkNo, uint64(len(items)), func(c *stateChunk) {
			for _, item := range items {
				offset := item.id & (freelistChunkSize - 1)
				c.retired[offset] = 0
				c.setFree(offset, true)
			}
		})
		start = end
	}
}

func (t *FreelistTxn) Prune(h RecoveryHorizon) {
	t.PruneWithCapabilityWithAllocationRequestV1(nil, nil, h.capability())
}

func (t *FreelistTxn) Reserve(candidate CandidateIDV1) error {
	return t.ReserveWithAllocationRequestV1(nil, nil, candidate)
}

func (t *FreelistTxn) ReserveWithAllocationRequestV1(request AllocationRequestCreditV1, scratch *AllocationCreatorV1, candidate CandidateIDV1) error {
	if err := t.valid(); err != nil {
		return err
	}
	if err := t.requireBirthCreditV1(request); err != nil {
		return err
	}
	if err := reserveAllocationScratchV1(request, scratch, t.buildCreator, uint64(len(t.allocated)), 8, false); err != nil {
		return err
	}
	ids := make([]uint64, len(t.allocated))
	defer clear(ids[:cap(ids)])
	for i, allocation := range t.allocated {
		ids[i] = allocation.id
	}
	if err := t.ledger.reserve(request, candidate, ids, t.buildCreator); err != nil {
		return err
	}
	t.stats.Reservations = uint64(len(ids))
	return nil
}

type candidatePageV1 struct {
	PageID uint64
	view   CandidatePageViewV1
}

type FreelistCandidateV1 struct {
	generation *FreelistGenerationV1
	pages      []candidatePageV1
	dirtyIDs   []uint64
	creator    *allocationCreditLeaseV1
	writeMu    sync.Mutex
}

func (c *FreelistCandidateV1) Generation() *FreelistGenerationV1 {
	if c == nil {
		return nil
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c == nil || c.hasFiniteBackingV1() {
		return nil
	}
	c.markOrdinaryEscapeV1()
	return c.generation
}
func (c *FreelistCandidateV1) GenerationRef() GenerationRefV1 {
	if c == nil {
		return GenerationRefV1{}
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.generation.GenerationRef()
}
func (c *FreelistCandidateV1) DirtyPageIDs() []uint64 {
	if c == nil {
		return nil
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c == nil || c.hasFiniteBackingV1() {
		return nil
	}

	return append([]uint64(nil), c.dirtyIDs...)
}
func (c *FreelistCandidateV1) ReservationRecord() ReservationRecordV1 {
	if c == nil {
		return ReservationRecordV1{}
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.generation == nil || c.hasFiniteBackingV1() {
		return ReservationRecordV1{}
	}
	c.markOrdinaryEscapeV1()
	return c.generation.record
}
func (c *FreelistCandidateV1) Pages() []PageImageV1 {
	if c == nil {
		return nil
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c == nil || c.hasFiniteBackingV1() {
		return nil
	}

	out := make([]PageImageV1, len(c.pages))
	for i := range c.pages {
		out[i] = PageImageV1{c.pages[i].PageID, make([]byte, page.PageSize)}
		// Pages predates the fallible opaque writer API. Failure here means an
		// internal immutable-state invariant was violated; never return bad bytes.
		if err := c.pages[i].view.CopyTo(out[i].Data); err != nil {
			panic(err)
		}
	}
	return out
}

func (c *FreelistCandidateV1) PageCount() int {
	if c == nil {
		return 0
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c == nil {
		return 0
	}
	return len(c.pages)
}

// WritePagesToV1 visits candidate-owned pages through opaque read-only views,
// allowing a writer to copy directly into its final destination.
func (c *FreelistCandidateV1) WritePagesToV1(writer CandidatePageWriterV1) error {
	if c == nil || writer == nil {
		return ErrGenerationFormat
	}
	c.writeMu.Lock()
	if c.hasFiniteBackingV1() {
		c.writeMu.Unlock()
		return ErrFiniteAllocationExportV1
	}
	c.markOrdinaryEscapeV1()
	c.writeMu.Unlock()
	// Ordinary candidates are never terminal-scrubbed. Retaining callbacks run
	// without allocator/candidate locks and keep immutable backing indefinitely.
	return c.writePagesV1(writer, nil)
}

func (c *FreelistCandidateV1) WritePagesToPagerV1(dst *pager.Pager) error {
	if c == nil || dst == nil {
		return ErrGenerationFormat
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writePagesV1(nil, dst)
}
func (c *FreelistCandidateV1) writePagesV1(writer CandidatePageWriterV1, dst *pager.Pager) error {
	for i := range c.pages {
		var err error
		if dst != nil {
			err = WriteCandidatePageToPagerV1(dst, c.pages[i].PageID, c.pages[i].view)
		} else {
			err = writer.WriteCandidatePageV1(c.pages[i].PageID, c.pages[i].view)
		}
		if err != nil {
			return fmt.Errorf("write candidate page %d: %w", c.pages[i].PageID, err)
		}
	}
	return nil
}

type recordingSink struct {
	sink          AppendPageSink
	pages         []candidatePageV1
	indexScratch  []byte
	ledger        *ReservationLedger
	candidate     CandidateIDV1
	writeStarted  bool
	metadataStart uint64
	reservedCount uint64
}

// checkNext refuses malformed, repeated, skipped and overflowing IDs before
// invoking any sink or marking a reserved tail as physically attempted.
func (s *recordingSink) checkNext(id uint64) error {
	count := uint64(len(s.pages))
	if s.metadataStart == 0 || s.reservedCount == 0 || s.metadataStart > ^uint64(0)-s.reservedCount || count >= s.reservedCount || len(s.pages) >= cap(s.pages) || id != s.metadataStart+count {
		return ErrGenerationFormat
	}
	return nil
}

func (s *recordingSink) complete() error {
	if s.metadataStart == 0 || s.reservedCount == 0 || s.metadataStart > ^uint64(0)-s.reservedCount || uint64(len(s.pages)) != s.reservedCount {
		return ErrGenerationFormat
	}
	return nil
}

func (s *recordingSink) beforeWrite() error {
	if !s.writeStarted {
		if s.ledger != nil {
			if err := s.ledger.markTailWriteAttempted(s.candidate); err != nil {
				return err
			}
		}
		s.writeStarted = true
	}
	return nil
}

func (s *recordingSink) write(id uint64, data []byte) error {
	if len(data) != page.PageSize {
		return ErrGenerationFormat
	}
	if err := s.checkNext(id); err != nil {
		return err
	}
	if err := s.beforeWrite(); err != nil {
		return err
	}
	sinkData := data
	if !candidateOwnedImagesV1(s.sink) {
		// Arbitrary sinks may retain or mutate their input. Give them an
		// isolated copy while the candidate keeps the fresh encoded buffer.
		sinkData = append([]byte(nil), data...)
	}
	if err := s.sink.WritePage(id, sinkData); err != nil {
		return err
	}
	s.pages = append(s.pages, candidatePageV1{id, CandidatePageViewV1{data: data}})
	return nil
}

// writeIndex retains no scratch alias. Only the exact non-retaining production
// sink receives the temporary encoding; generic sinks use isolated owned images.
func (s *recordingSink) writeIndex(n *stateNode, depth int, generationID uint64) error {
	if n == nil {
		return ErrGenerationFormat
	}
	if err := s.checkNext(n.pageID); err != nil {
		return err
	}
	if !candidateOwnedImagesV1(s.sink) {
		b, err := encodeIndexPage(n.pageID, generationID, n, depth)
		if err != nil {
			return err
		}
		n.checksum = binary.LittleEndian.Uint32(b[8:12])
		return s.write(n.pageID, b)
	}
	if s.indexScratch == nil {
		s.indexScratch = make([]byte, page.PageSize)
	}
	if err := encodeIndexPageInto(s.indexScratch, n.pageID, generationID, n, depth); err != nil {
		return err
	}
	n.checksum = binary.LittleEndian.Uint32(s.indexScratch[8:12])
	if err := s.beforeWrite(); err != nil {
		return err
	}
	if err := s.sink.WritePage(n.pageID, s.indexScratch); err != nil {
		return err
	}
	// Copy only canonical fixed-width descriptors, never a scratch slice or
	// a node pointer. Reserved/unused descriptor bytes are already zero.
	plan := &indexPagePlanV1{}
	copy(plan.prefix[:], s.indexScratch[:len(plan.prefix)])
	view := CandidatePageViewV1{index: plan}
	s.pages = append(s.pages, candidatePageV1{n.pageID, view})
	return nil
}

func emitStatePages(r stateRefV1, _ int, generationID uint64, next *uint64, sink *recordingSink) error {
	if r.zero() || r.pageID() != 0 {
		return nil
	}
	if r.chunk != nil {
		r.chunk.pageID = *next
		*next++
		b := encodeChunkPage(r.chunk.pageID, generationID, r.chunk)
		r.chunk.checksum = binary.LittleEndian.Uint32(b[8:12])
		return sink.write(r.chunk.pageID, b)
	}
	for _, child := range r.branch.child {
		if err := emitStatePages(child, 0, generationID, next, sink); err != nil {
			return err
		}
	}
	r.branch.pageID = *next
	*next++
	return sink.writeIndex(r.branch, int(r.branch.depth), generationID)
}

// Adjacent homogeneous runs are combined BEFORE growing the backing slice.
func appendCoalescedExtentV1(extents []ReservationExtentV1, extent ReservationExtentV1) []ReservationExtentV1 {
	if len(extents) > 0 {
		last := &extents[len(extents)-1]
		if last.Kind == extent.Kind && last.LastReachableCommitSeq == extent.LastReachableCommitSeq && last.StartPageID+uint64(last.Count) == extent.StartPageID && uint64(last.Count)+uint64(extent.Count) <= uint64(^uint32(0)) {
			last.Count += extent.Count
			return extents
		}
	}
	return append(extents, extent)
}
func appendIDExtents(extents []ReservationExtentV1, ids []uint64, kind ReservationKindV1, seq uint64) []ReservationExtentV1 {
	for _, id := range sortedUnique(ids) {
		extents = appendCoalescedExtentV1(extents, ReservationExtentV1{StartPageID: id, Count: 1, Kind: kind, LastReachableCommitSeq: seq})
	}
	return extents
}
func appendReservationRange(extents []ReservationExtentV1, start, count uint64, kind ReservationKindV1, seq uint64) []ReservationExtentV1 {
	for count > 0 {
		chunk := min(count, uint64(^uint32(0)))
		extents = appendCoalescedExtentV1(extents, ReservationExtentV1{StartPageID: start, Count: uint32(chunk), Kind: kind, LastReachableCommitSeq: seq})
		start += chunk
		count -= chunk
	}
	return extents
}

func countUnmaterializedStatePages(r stateRefV1, _ int) uint64 {
	if r.zero() || r.pageID() != 0 {
		return 0
	}
	count := uint64(1)
	if r.branch != nil {
		for _, child := range r.branch.child {
			count += countUnmaterializedStatePages(child, 0)
		}
	}
	return count
}

// The allocated vector is sorted once in private scratch; reused/appended ID
// vectors and sortedUnique copies are unnecessary. Radix traversal is ordered.
func (t *FreelistTxn) reservationExtents(request AllocationRequestCreditV1, scratch *AllocationCreatorV1) ([]ReservationExtentV1, error) {
	if err := reserveAllocationScratchV1(request, scratch, t.buildCreator, uint64(len(t.allocated)), uint64(unsafe.Sizeof(allocatedPage{})), false); err != nil {
		return nil, err
	}
	allocated := make([]allocatedPage, len(t.allocated))
	defer clear(allocated[:cap(allocated)])
	copy(allocated, t.allocated)
	slices.SortFunc(allocated, func(a, b allocatedPage) int {
		if a.kind != b.kind {
			return cmp.Compare(a.kind, b.kind)
		}
		return cmp.Compare(a.id, b.id)
	})
	capacity := uint64(len(allocated)) + uint64(len(t.abandonedAppends)) + uint64(t.replacedMetadata.Len())
	if err := reserveAllocationScratchV1(request, scratch, t.buildCreator, capacity, uint64(unsafe.Sizeof(ReservationExtentV1{})), false); err != nil {
		return nil, err
	}
	if capacity > uint64(^uint(0)>>1) {
		return nil, ErrNoAllocatablePage
	}
	extents := make([]ReservationExtentV1, 0, int(capacity))
	for i, allocation := range allocated {
		if i > 0 && allocation == allocated[i-1] {
			continue
		}
		extents = appendCoalescedExtentV1(extents, ReservationExtentV1{StartPageID: allocation.id, Count: 1, Kind: allocation.kind})
	}
	for _, extent := range t.abandonedAppends {
		extents = appendCoalescedExtentV1(extents, extent)
	}
	t.replacedMetadata.Range(func(id uint64, _ struct{}) bool {
		extents = appendCoalescedExtentV1(extents, ReservationExtentV1{StartPageID: id, Count: 1, Kind: ReservationPendingMetadataRetirement, LastReachableCommitSeq: t.base.commitSeq})
		return true
	})
	return normalizeOwnedExtentsV1(extents)
}

func (t *FreelistTxn) MaterializeCandidate(generationID, commitSeq uint64, candidateID CandidateIDV1, sink AppendPageSink) (*FreelistCandidateV1, error) {
	if t != nil && (t.creator != nil || t.allocatedCreator != nil || t.abandonedCreator != nil || stateTreeFiniteV1(t.root)) {
		t.privatePreparation = false
		return nil, ErrFiniteAllocationExportV1
	}
	return t.materializeCandidateOwnedV1(nil, nil, generationID, commitSeq, candidateID, sink)
}

func (t *FreelistTxn) materializeCandidateOwnedV1(request AllocationRequestCreditV1, scratch *AllocationCreatorV1, generationID, commitSeq uint64, candidateID CandidateIDV1, sink AppendPageSink) (*FreelistCandidateV1, error) {
	// Validation errors that do not consume the transaction still revoke its
	// private edit permission; no error return carries builder authority.
	if t != nil {
		defer func() { t.privatePreparation = false }()
	}
	if err := t.valid(); err != nil {
		return nil, err
	}
	if err := t.requireBirthCreditV1(request); err != nil {
		return nil, err
	}
	if generationID == 0 || commitSeq == 0 || sink == nil {
		return nil, ErrGenerationFormat
	}
	if candidateID == (CandidateIDV1{}) {
		return nil, fmt.Errorf("%w: zero candidate identity", ErrGenerationFormat)
	}
	if t.base.ref.HeaderPageID != 0 && generationID <= t.base.generationID {
		return nil, ErrGenerationParent
	}
	if t.buildCreator != nil {
		if _, owned := sink.(ownedCandidatePageSinkV1); !owned {
			return nil, ErrFiniteAllocationExportV1
		}
	}
	// Both concrete escaping controls are prepaid before any output/reservation
	// effect. This stack receipt retains no request. Failed preparation releases
	// unused creator edges only; cumulative request debit is never refunded.
	generationControlClass := allocationClassV1(uint64(unsafe.Sizeof(FreelistGenerationV1{})), true)
	candidateControlClass := allocationClassV1(uint64(unsafe.Sizeof(FreelistCandidateV1{})), true)
	controlOperation, controlErr := admitAllocationOperationV1(request, t.buildCreator, generationControlClass+candidateControlClass, 2)
	if controlErr != nil {
		return nil, controlErr
	}
	defer controlOperation.close()
	if err := controlOperation.take(generationControlClass+candidateControlClass, 2); err != nil {
		return nil, err
	}
	g := &FreelistGenerationV1{creator: t.buildCreator, ownedRefs: 1}
	candidate := &FreelistCandidateV1{creator: t.buildCreator, generation: g}
	candidateComplete := false
	defer func() {
		if !candidateComplete {
			releaseCandidateOwnedBackingV1(candidate)
		}
	}()
	metadataStart, reservedMetadataCount, extents, reusedMetadata := t.tryReusedMetadata(request, scratch, candidateID)
	placementExtents := extents
	defer func() { clear(placementExtents[:cap(placementExtents)]) }()
	if t.allocationErr != nil {
		return nil, t.allocationErr
	}
	// Page IDs are assigned while writing. Once materialization starts, success
	// or failure consumes this transaction; retry must begin from the immutable
	// base so a partial sink failure cannot retain unwritten page identities.
	t.consumed = true
	if !t.privatePreparation {
		var isolated stateRefV1
		var err error
		if reusedMetadata {
			// tryReusedMetadata copied the selected path; charge and isolate
			// only its dirty siblings before assigning emitted identities.
			isolated, err = detachMetadataSiblingsOwnedV1(request, t.root, 0, metadataStart>>freelistChunkShift, &t.stats, t.buildCreator)
		} else {
			isolated, err = detachUnmaterializedOwnedV1(request, t.root, 0, &t.stats, t.buildCreator)
		}
		if err != nil {
			return nil, err
		}
		replaceStateNodeV1(&t.root, isolated)
	}
	// Exclusive builder ownership ends before any sink receives candidate data.
	// consumed prevents later mutation on both success and partial-write failure.
	t.privatePreparation = false
	if t.root.pageID() != 0 {
		bytes, refs := t.root.class(), uint64(1)
		if _, exists := t.replacedMetadata.Get(t.root.pageID()); !exists {
			b, n := t.replacedMetadata.insertionCapacityV1(1)
			bytes += b
			refs += n
		}
		operation, err := admitAllocationOperationV1(request, t.buildCreator, bytes, refs)
		if err != nil {
			return nil, err
		}
		if err = t.replacedMetadata.putAdmittedV1(request, t.root.pageID(), struct{}{}, t.buildCreator, &operation); err != nil {
			operation.close()
			return nil, err
		}
		root, err := cloneStateRefOwnedV1(request, t.root, false, t.buildCreator, &operation)
		operation.close()
		if err != nil {
			return nil, err
		}
		replaceStateNodeV1(&t.root, root)
		noteStateBirthV1(root, &t.stats)
	}

	// The target metadata extent includes the COW pages, the reservation chain,
	// and the generation header. Its count does not change the number of
	// normalized reservation extents, so compute the chain length first.
	minimumMetadataStart := t.highWater
	if minimumMetadataStart == math.MaxUint64 && !reusedMetadata {
		return nil, ErrNoAllocatablePage
	}
	var err error
	statePageCount := countUnmaterializedStatePages(t.root, 0)
	if t.root.freeCount()+t.root.retiredCount() == 0 {
		statePageCount = 1
	}
	if !reusedMetadata {
		if err := reserveAllocationScratchV1(request, scratch, t.buildCreator, uint64(len(t.allocated)), 8, false); err != nil {
			return nil, err
		}
		dataIDs := make([]uint64, 0, len(t.allocated))
		defer clear(dataIDs[:cap(dataIDs)])
		for _, allocation := range t.allocated {
			dataIDs = append(dataIDs, allocation.id)
		}
		extents, err = t.reservationExtents(request, scratch)
		placementExtents = extents
		if err != nil {
			return nil, err
		}
		metadataStart, reservedMetadataCount, err = t.ledger.reserveTail(request, candidateID, minimumMetadataStart, statePageCount, dataIDs, extents, t.buildCreator)
		if err != nil {
			return nil, err
		}
	}
	// Copy once into exact append capacity. Normalization below consumes this
	// private backing; it cannot grow or retain an alias to a placement plan.
	skippedEntries := uint64(0)
	if metadataStart > minimumMetadataStart {
		skippedEntries = (metadataStart-minimumMetadataStart-1)/uint64(^uint32(0)) + 1
	}
	capacity := uint64(len(extents)) + skippedEntries + 1
	if capacity < uint64(len(extents)) || capacity > uint64(^uint(0)>>1) {
		return nil, ErrNoAllocatablePage
	}
	if err := reserveMaterializationBackingV1(request, t.buildCreator, capacity, uint64(unsafe.Sizeof(ReservationExtentV1{})), false); err != nil {
		return nil, err
	}
	ownedExtents := make([]ReservationExtentV1, len(extents), int(capacity))
	extentsTransferred := false
	defer func() {
		if !extentsTransferred {
			clear(ownedExtents[:cap(ownedExtents)])
		}
	}()
	copy(ownedExtents, extents)
	extents = ownedExtents
	if metadataStart > minimumMetadataStart {
		extents = appendReservationRange(extents, minimumMetadataStart, metadataStart-minimumMetadataStart, ReservationAbandonedAppend, 0)
	}
	extents = append(extents, ReservationExtentV1{StartPageID: metadataStart, Count: 1, Kind: ReservationTargetMetadata})
	pageCount := reservationPagesForEntries(uint64(len(extents)))
	if pageCount > uint64(^uint16(0)) || statePageCount+pageCount+1 != reservedMetadataCount {
		return nil, ErrGenerationFormat
	}
	if err := t.reserveMaterializationOutputsV1(request, scratch, reservedMetadataCount, pageCount); err != nil {
		return nil, err
	}
	next := metadataStart
	recorded := &recordingSink{
		sink:   sink,
		pages:  make([]candidatePageV1, 0, reservedMetadataCount),
		ledger: t.ledger, candidate: candidateID,
		metadataStart: metadataStart, reservedCount: reservedMetadataCount,
	}
	defer func() {
		clear(recorded.indexScratch[:cap(recorded.indexScratch)])
		if !candidateComplete {
			clear(recorded.pages[:cap(recorded.pages)])
		}
		recorded.indexScratch, recorded.sink, recorded.ledger, recorded.pages = nil, nil, nil, nil
	}()
	if t.root.zero() {
		root, birthErr := cloneStateRefOwnedV1(request, stateRefV1{}, false, t.buildCreator)
		if birthErr != nil {
			return nil, birthErr
		}
		replaceStateNodeV1(&t.root, root)
		noteStateBirthV1(root, &t.stats)
	}
	if err := emitStatePages(t.root, 0, generationID, &next, recorded); err != nil {
		return nil, err
	}

	reservationID := next
	headerID := reservationID + pageCount
	next = headerID + 1
	for i := range extents {
		if extents[i].Kind == ReservationTargetMetadata {
			metadataCount := next - metadataStart
			if metadataCount > uint64(^uint32(0)) {
				return nil, ErrGenerationFormat
			}
			extents[i].Count = uint32(metadataCount)
			break
		}
	}
	extents, err = normalizeOwnedExtentsV1(extents)
	if err != nil {
		return nil, err
	}
	record := ReservationRecordV1{CandidateID: candidateID, GenerationID: generationID, BaseID: t.base.generationID, BaseDigest: t.base.ref.Digest, Extents: extents}
	recordPages, record, err := encodeNormalizedReservationPages(reservationID, record)
	defer clear(recordPages[:cap(recordPages)])
	if err != nil {
		return nil, err
	}
	for i, recordPage := range recordPages {
		if err := recorded.write(reservationID+uint64(i), recordPage); err != nil {
			return nil, err
		}
	}
	extentsTransferred = true
	g.generationID, g.commitSeq = generationID, commitSeq
	g.parentGenerationID, g.parentCommitSeq = t.base.generationID, t.base.commitSeq
	g.highWater, g.root, g.record = max(t.highWater, next), retainStateNodeV1(t.root), record
	g.metadataPages = make([]uint64, 0, next-metadataStart)
	for id := metadataStart; id < next; id++ {
		g.metadataPages = append(g.metadataPages, id)
	}
	header := encodeGenerationPage(headerID, g, g.root.checksum())
	if err := recorded.write(headerID, header); err != nil {
		return nil, err
	}
	if err := recorded.complete(); err != nil {
		return nil, err
	}
	copy(g.ref.Digest[:], header[152:184])
	g.ref = GenerationRefV1{HeaderPageID: headerID, GenerationID: generationID, CommitSeq: commitSeq, HighWater: g.highWater, Digest: g.ref.Digest}
	t.stats.LogicalDelta = uint64(t.changedChunks.Len())
	t.stats.COWChunks = uint64(t.changedChunks.Len())
	t.stats.COWPages = uint64(len(recorded.pages))
	t.stats.COWBytes = t.stats.COWPages * page.PageSize
	if !candidateOwnedImagesV1(sink) {
		t.stats.CandidatePageIsolationCopies = t.stats.COWPages
		t.stats.CandidatePageIsolationBytes = t.stats.COWBytes
	}
	t.stats.FreeIDs, t.stats.RetiredIDs = g.root.freeCount(), g.root.retiredCount()
	t.stats.GenerationID = generationID
	t.stats.ReservationRecords = uint64(len(record.pageIDs))
	t.stats.Reservations = uint64(len(t.allocated)) + (next - metadataStart)
	t.stats.PendingMetadataRetirements = record.pendingMetadataCountV1()
	dirty := make([]uint64, len(recorded.pages))
	for i := range recorded.pages {
		dirty[i] = recorded.pages[i].PageID
	}
	candidate.pages, candidate.dirtyIDs = recorded.pages, dirty
	candidateComplete = true
	return candidate, nil
}

func candidateIDFromString(value string) CandidateIDV1 {
	sum := sha256.Sum256([]byte(value))
	var id CandidateIDV1
	copy(id[:], sum[:16])
	return id
}

func (t *FreelistTxn) Materialize(id uint64) (*FreelistGenerationV1, error) {
	store := NewMemoryPageStoreV1()
	candidate, err := t.MaterializeCandidate(id, id, candidateIDFromString(fmt.Sprintf("generation-%d", id)), store)
	if err != nil {
		return nil, err
	}
	candidate.markOrdinaryEscapeV1()
	return candidate.generation, nil
}

func (t *FreelistTxn) Stats() FreelistTxnStats {
	if t == nil {
		return FreelistTxnStats{}
	}
	return t.stats
}
