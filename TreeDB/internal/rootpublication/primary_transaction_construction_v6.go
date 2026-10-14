package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"
)

// This is the construction state inside the existing transaction, not another
// lifecycle. Its logical root has no slot, sequence or durable parent yet.
type primaryTransactionConstructionV6 struct {
	arena          *primaryarena.Arena
	root           primaryarena.Ref
	certificate    *primaryarena.ReadRootCertificateV6
	resources      *StableResourceSet
	components     [49]primaryarena.Ref
	componentCount int
	promotion      *PrimaryPromotionV6
	// Candidate and single-member group are borrowed descriptors of this owner.
	candidate          PreparedRootCandidate
	members            [1]*DurableRootTransaction
	candidateBound     bool
	metadataCharge     uint64
	terminalCompletion *primaryTerminalCompletionV6
	// This is custody on the SAME transaction, admitted in its containing allocation.
	nativeWorkOwned bool
	cleanupPhase    uint8
	cleanupQueue    primaryarena.ReleaseQueue
	cleanupReceipt  *PrimaryConstructionReleaseWork
}
type PrimaryPromotionV6 struct {
	Image        []byte
	View         PrimaryCapsuleViewV6
	Root, Parent primaryarena.Ref
}

func NewPrimaryConstructionTransactionV6(a *primaryarena.Arena, w *iterator.OrdinalScanWork) (primaryarena.ReadRootConstructionV6, bool, error) {
	return newPrimaryConstructionTransactionV6(a, w, false)
}

// Native constructors keep callback and cleanup work with their original caller.
func NewNativeMVCCPrimaryConstructionTransactionV6(a *primaryarena.Arena, w *iterator.OrdinalScanWork) (primaryarena.ReadRootConstructionV6, bool, error) {
	return newPrimaryConstructionTransactionV6(a, w, true)
}

func newPrimaryConstructionTransactionV6(a *primaryarena.Arena, w *iterator.OrdinalScanWork, native bool) (primaryarena.ReadRootConstructionV6, bool, error) {
	if a == nil || !a.CapsuleFormatV6() || w == nil {
		return nil, false, ErrDurableRootOwnership
	}
	if !w.Reserve(3, 2*uint64(unsafe.Sizeof(DurableRootTransaction{}))+2*uint64(unsafe.Sizeof(primaryTransactionConstructionV6{}))+2*uint64(unsafe.Sizeof(primaryTerminalCompletionV6{}))) {
		return nil, false, nil
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(DurableRootTransaction{}))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryTransactionConstructionV6{}))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryTerminalCompletionV6{})))
	if err := a.MetadataOwner().AddPending(charge); err != nil {
		return nil, false, err
	}
	t := &DurableRootTransaction{owner: ResourceOwnerBuilder, phase: durableRootConstructingV6, constructionV6: &primaryTransactionConstructionV6{arena: a, metadataCharge: charge, nativeWorkOwned: native, terminalCompletion: &primaryTerminalCompletionV6{owner: a.MetadataOwner(), charge: charge}}}
	a.InitializeReleaseQueue(&t.constructionV6.cleanupQueue)
	root, ready, err := a.PrepareConstructedReadRootV6(t, w)
	if !ready || err != nil {
		a.MetadataOwner().RemovePending(charge)
		return nil, ready, err
	}
	t.constructionV6.root = root
	return t, true, nil
}
func (t *DurableRootTransaction) PrimaryConstructionReferenceV6() primaryarena.Ref {
	if t == nil {
		return primaryarena.Ref{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	// Reference visibility reports actual retained custody, including a failed
	// or paused abort. Mutation eligibility remains guarded by cleanupPhase.
	if t.constructionV6 == nil {
		return primaryarena.Ref{}
	}
	return t.constructionV6.root
}
func (t *DurableRootTransaction) SealPrimaryConstructionV6(image []byte, groups [1]*primaryarena.Group, w *iterator.OrdinalScanWork) (bool, error) {
	if t == nil || w == nil {
		return false, ErrDurableRootOwnership
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.constructionV6
	if c == nil || c.cleanupPhase != 0 || t.phase != durableRootConstructingV6 || c.certificate != nil {
		return false, ErrDurableRootOwnership
	}
	cert, ready, err := c.arena.SealConstructedReadRootV6(c.root, image, groups, t, w)
	if ready && err == nil {
		c.certificate = cert
		for c.componentCount > 0 {
			r := c.components[c.componentCount-1]
			ok, e := c.arena.Drop(r, w)
			if !ok || e != nil {
				return false, errors.Join(e, ErrDurableRootOwnership)
			}
			c.componentCount--
		}
	}
	return ready, err
}
func (t *DurableRootTransaction) PrimaryConstructionCertificateV6() *primaryarena.ReadRootCertificateV6 {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.constructionV6 == nil || t.constructionV6.cleanupPhase != 0 || t.phase != durableRootConstructingV6 {
		return nil
	}
	return t.constructionV6.certificate
}

// Bind turns the SAME slot-independent constructor into an immutable visible
// member. The resource set's existing atomic cell is the sole handoff owner.
func (t *DurableRootTransaction) BindPrimaryVisibilityV6(spec DurableRootTransactionSpec, resources *StableResourceSet, w *iterator.OrdinalScanWork) (bool, error) {
	if t == nil || w == nil {
		return false, ErrDurableRootOwnership
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(DurableRootTransaction{}))+2*uint64(unsafe.Sizeof(DurableRootTransactionSpec{}))) {
		return false, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.constructionV6
	// Native visibility requires the work-bearing callback contract; the
	// existing synchronous callback binding cannot claim its continuation.
	if c != nil && c.nativeWorkOwned {
		return false, ErrNativeMVCCRootWorkRequiredV6
	}
	if c == nil || c.cleanupPhase != 0 || t.phase != durableRootConstructingV6 || c.certificate == nil || resources == nil || resources.Owner() != ResourceOwnerBuilder ||
		spec.PreparedPrimary == nil || spec.PreparedPrimary.readCertificate != c.certificate || spec.Lineage == (DurableRootLineageID{}) || spec.Sequence == 0 ||
		spec.Abort == nil || spec.Fail == nil {
		return false, ErrDurableRootOwnership
	}
	if err := validateDurableRootTransactionSpec(spec); err != nil {
		return false, err
	}
	// The constructor retains its original root edge through Finish. Visibility
	// acquires a distinct state edge: replacing that state must not free a still
	// pending transaction's immutable logical root.
	if ready, err := c.arena.Acquire(c.root, w); !ready || err != nil {
		return ready, err
	}
	// Private constructor discovery ends before publication. Runtime/coordinator
	// retain this SAME transaction; immutable readers need only its root edge.
	if ready, err := c.arena.DetachReadRootConstructionV6(c.root, t, w); !ready || err != nil {
		cleanup := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
		_, dropErr := c.arena.Drop(c.root, cleanup)
		return false, errors.Join(err, dropErr)
	}
	t.input = DurableRootCallbackInput{Lineage: spec.Lineage, Sequence: spec.Sequence, Payload: spec.Payload, PreparedCOW: spec.PreparedCOW, PreparedPrimary: spec.PreparedPrimary, PublicationProjection: spec.PublicationProjection}
	t.activate, t.consume, t.abort, t.abortComplete, t.fail = spec.Activate, spec.Consume, spec.Abort, spec.AbortComplete, spec.Fail
	c.resources = resources
	c.metadataCharge += spec.PrimaryMetadataBytesV6
	t.phase = durableRootPrepared
	return true, nil
}
func (t *DurableRootTransaction) ownerLockedV6() ResourceOwnerState {
	if t.constructionV6 != nil && t.constructionV6.resources != nil {
		owner := t.constructionV6.resources.Owner()
		if owner != ResourceOwnerReleased {
			return owner
		}
		// The independently consumed Set edge is never revived. This original
		// transaction still owns unfinished descriptors and physical failure custody.
		if t.phase != durableRootConsumed {
			return t.owner
		}
		return ResourceOwnerReleased
	}
	return t.owner
}
func (t *DurableRootTransaction) releaseOwnerLockedV6() {
	t.owner = ResourceOwnerReleased
	// The Set remains sole authority for its resource edge; this original owner
	// separately records completion of transaction/descriptor custody.
}
func (t *DurableRootTransaction) abortConstructionLockedV6(terminal *primaryConstructionReleaseState) error {
	if t.constructionV6 != nil && t.constructionV6.nativeWorkOwned {
		return ErrNativeMVCCRootWorkRequiredV6
	}
	work := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
	for {
		ready, err := t.abortNativeMVCCConstructionStepLockedV6(work, terminal, nil)
		if ready || err != nil {
			return err
		}
	}
}

func (t *DurableRootTransaction) PrimaryPromotionV6() *PrimaryPromotionV6 {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.constructionV6 == nil {
		return nil
	}
	return t.constructionV6.promotion
}

// Fresh promotion selects actual parent/target and acquires independent physical
// closure edges. These head-bound operands never survive as private preparation.
func (t *DurableRootTransaction) PreparePrimaryPromotionV6(record DurableRootRecordV1, projection PrimaryProjectionV5, parent *PrimaryCapsuleViewV6, parentRoot primaryarena.Ref, w *iterator.OrdinalScanWork) (result *PrimaryPromotionV6, completed bool, returnErr error) {
	if t == nil || w == nil {
		return nil, false, ErrDurableRootOwnership
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(DurableRootTransaction{}))+2*uint64(unsafe.Sizeof(PrimaryPromotionV6{}))) {
		return nil, false, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.constructionV6
	if c == nil || t.phase != durableRootActivated || t.ownerLockedV6() != ResourceOwnerCoordinator || c.promotion != nil || parent == nil || parentRoot == (primaryarena.Ref{}) {
		return nil, false, ErrDurableRootOwnership
	}
	target := uint64(1) - parent.Slot()
	charge := retainedalloc.AllocationCharge(PrimaryCapsuleSizeV6) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(PrimaryPromotionV6{})))
	if err := c.arena.MetadataOwner().AddPending(charge); err != nil {
		return nil, false, err
	}
	chargeTransferred := false
	defer func() {
		if !chargeTransferred {
			c.arena.MetadataOwner().RemovePending(charge)
		}
	}()
	image := make([]byte, PrimaryCapsuleSizeV6)
	view, ready, err := ConstructPrimaryCapsuleV6(image, target, DurablePrimaryRootRecordV5{Record: record, Primary: projection}, t.input.PreparedPrimary, parent, w)
	if !ready || err != nil {
		return nil, ready, err
	}
	edge := primaryarena.MetadataEdge{PageID: record.Manifest.FirstPageID, Class: primaryarena.Manifest}
	if record.Directory.RootPageID != 0 {
		edge = primaryarena.MetadataEdge{PageID: record.Directory.RootPageID, Class: primaryarena.Dependency}
	}
	promotionRoot, ready, err := c.arena.CloneReadRootForPromotionV6(c.root, w)
	if !ready || err != nil {
		return nil, ready, err
	}
	retained := false
	defer func() {
		if !retained {
			// Ordinary synchronous unwind owns this exact new image and group edges.
			cleanup := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
			_, dropErr := c.arena.Drop(promotionRoot, cleanup)
			returnErr = errors.Join(returnErr, dropErr)
		}
	}()
	if ready, err = c.arena.BindReadRootDependencyV6(promotionRoot, edge, w); !ready || err != nil {
		return nil, ready, err
	}
	if ready, err = c.arena.Acquire(parentRoot, w); !ready || err != nil {
		return nil, ready, err
	}
	c.promotion = &PrimaryPromotionV6{Image: image, View: view, Root: promotionRoot, Parent: parentRoot}
	c.metadataCharge += charge
	chargeTransferred = true
	retained = true
	return c.promotion, true, nil
}

func (t *DurableRootTransaction) OwnPrimaryComponentV6(ref primaryarena.Ref, w *iterator.OrdinalScanWork) (bool, error) {
	if t == nil || w == nil {
		return false, ErrDurableRootOwnership
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(primaryarena.Ref{}))+2*uint64(unsafe.Sizeof(primaryTransactionConstructionV6{}))) {
		return false, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.constructionV6
	if c == nil || c.cleanupPhase != 0 || t.phase != durableRootConstructingV6 || c.componentCount == len(c.components) || ref == (primaryarena.Ref{}) {
		return false, ErrDurableRootOwnership
	}
	c.components[c.componentCount] = ref
	c.componentCount++
	return true, nil
}

// The ordinary report owns this same synchronous Finish. Its actual ordinary
// release callbacks are deliberately NOT claimed as bounded native retirement.
func (t *DurableRootTransaction) finishOrdinaryPrimaryV6(w *iterator.OrdinalScanWork, activePins map[ResourceKind]uint64) (*primaryTerminalCompletionV6, error) {
	if w == nil {
		return nil, ErrDurableRootOwnership
	}
	t.mu.Lock()
	if t.physicalCleanupError != nil {
		err := t.physicalCleanupError
		t.mu.Unlock()
		return nil, err
	}
	c := t.constructionV6
	if c == nil || c.resources == nil || t.phase != durableRootFinishing || c.resources.Owner() != ResourceOwnerCoordinator || t.finishBusy {
		t.mu.Unlock()
		return nil, ErrDurableRootOwnership
	}
	t.finishBusy = true
	t.mu.Unlock()
	// Runtime/coordinator have been the sole mutable constructor owner since
	// Bind. Synchronous ordinary callbacks release its resources once; readers
	// have no backpointer to this pending graph.
	ready, err := true, error(nil)
	if c.root != (primaryarena.Ref{}) {
		ready, err = c.arena.Drop(c.root, w)
		if ready && err == nil {
			t.mu.Lock()
			c.root = primaryarena.Ref{}
			t.mu.Unlock()
		}
	}
	if ready && err == nil {
		err = c.resources.releaseFromWithActivePins(ResourceOwnerCoordinator, activePins)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.finishBusy = false
	if !ready || err != nil || c.resources.Owner() != ResourceOwnerReleased {
		return nil, t.recordPhysicalCleanupFailureLocked(errors.Join(err, ErrDurableRootOwnership))
	}
	// Immutable state/slot/reader refs independently own the root. Scrub this
	// mutable constructor, then move its accounting custody out of this call.
	t.input = DurableRootCallbackInput{Lineage: t.input.Lineage, Sequence: t.input.Sequence}
	t.activate, t.consume, t.abort, t.abortComplete, t.fail = nil, nil, nil, nil, nil
	t.constructionV6 = nil
	completion := c.terminalCompletion
	completion.charge = c.metadataCharge
	*c = primaryTransactionConstructionV6{}
	t.owner = ResourceOwnerReleased
	t.phase = durableRootConsumed
	return completion, nil
}
func (t *DurableRootTransaction) isConstructedPrimaryV6() bool {
	return t != nil && t.constructionV6 != nil
}

// PublicationProjectionV6 borrows the immutable DB projection published by Bind.
// Callers obtain the transaction through a synchronized current/candidate/runtime
// view; neither this accessor nor the projection can transfer ownership.
func (t *DurableRootTransaction) PublicationProjectionV6() any {
	if t == nil {
		return nil
	}
	return t.input.PublicationProjection
}
func (t *DurableRootTransaction) PublicationCandidateV6() *PreparedRootCandidate {
	if t == nil || t.constructionV6 == nil || !t.constructionV6.candidateBound {
		return nil
	}
	return &t.constructionV6.candidate
}

// adoptPrimaryCandidateV6 installs a descriptor in the existing constructor.
// Physical validation and resource transfer have already succeeded in the shared
// constructor; this consumes no second mutable owner or independent group.
func (t *DurableRootTransaction) adoptPrimaryCandidateV6(candidate PreparedRootCandidate) *PreparedRootCandidate {
	c := t.constructionV6
	c.members[0] = t
	candidate.extensions.durableRootRecord = durableRootGroupExtension{members: c.members[:]}
	c.candidate = candidate
	c.candidateBound = true
	return &c.candidate
}

// TransferPrimaryPromotionImageV6 follows actual slot installation. The slot
// owns the image and copied view; pending callbacks retain only the descriptor.
// No new allocation, physical edge, or admission is manufactured here.
func (t *DurableRootTransaction) TransferPrimaryPromotionImageV6() {
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.constructionV6
	if c == nil || c.promotion == nil || c.promotion.Root == (primaryarena.Ref{}) {
		panic("promotion transfer lost owner")
	}
	charge := retainedalloc.AllocationCharge(PrimaryCapsuleSizeV6)
	c.arena.AdoptPendingRootMetadata(c.promotion.Root, charge)
	c.metadataCharge -= charge
	c.promotion.Image = nil
	c.promotion.View = PrimaryCapsuleViewV6{}
}

// Recovery prepares the SAME constructor before any resource Set is released.
// A physical refusal remains terminal; the original root/descriptor graph and
// first receipt are retained without replaying already consumed edges.
func (t *DurableRootTransaction) preparePrimaryRecoveryLockedV6() error {
	if t.physicalCleanupError != nil {
		return t.physicalCleanupError
	}
	c := t.constructionV6
	if c == nil {
		return nil
	}
	if t.ownerLockedV6() != ResourceOwnerRecovery {
		return ErrDurableRootOwnership
	}
	if c.root != (primaryarena.Ref{}) {
		if !c.arena.MetadataOwner().PhysicalClosed() {
			if ready, err := c.arena.Drop(c.root, nil); !ready || err != nil {
				return t.recordPhysicalCleanupFailureLocked(errors.Join(err, ErrDurableRootOwnership))
			}
		}
		c.root = primaryarena.Ref{}
	}
	return nil
}

// The handoff has consumed the independent Set edge. Its checked original
// completion, rather than the Released scalar, authorizes descriptor disposal.
func (t *DurableRootTransaction) releasePrimaryRecoveryLockedV6() (*primaryTerminalCompletionV6, error) {
	if t.physicalCleanupError != nil {
		return nil, t.physicalCleanupError
	}
	c := t.constructionV6
	if c == nil {
		return nil, nil
	}
	if t.ownerLockedV6() != ResourceOwnerRecovery || c.root != (primaryarena.Ref{}) || c.resources == nil || c.resources.Owner() != ResourceOwnerReleased {
		return nil, ErrDurableRootOwnership
	}
	if err := c.resources.releaseResult(); err != nil {
		return nil, t.recordPhysicalCleanupFailureLocked(err)
	}
	if c.promotion != nil {
		c.promotion.Image = nil
		c.promotion.View = PrimaryCapsuleViewV6{}
	}
	t.input = DurableRootCallbackInput{Lineage: t.input.Lineage, Sequence: t.input.Sequence}
	t.activate, t.consume, t.abort, t.abortComplete, t.fail = nil, nil, nil, nil, nil
	t.constructionV6 = nil
	completion := c.terminalCompletion
	completion.charge = c.metadataCharge
	*c = primaryTransactionConstructionV6{}
	t.owner = ResourceOwnerReleased
	t.phase = durableRootConsumed
	return completion, nil
}
