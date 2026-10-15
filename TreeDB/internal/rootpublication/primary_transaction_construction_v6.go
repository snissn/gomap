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
	nativeWorkOwned       bool
	cleanupPhase          uint8
	cleanupPromotionIndex uint8
	cleanupQueue          primaryarena.ReleaseQueue
	cleanupReceipt        *PrimaryConstructionReleaseWork
	finishReleaseStarted  bool
	finishReleaseDone     bool
	finishRetirementDone  bool
	// SAME constructor owns the original Set wrapper accounting after its
	// protected shared edges are consumed in the publication turn.
	setRelease         StableResourceSetReleaseWorkV6
	setReleaseConsumed bool
}
type PrimaryPromotionV6 struct {
	Image                  []byte
	View                   PrimaryCapsuleViewV6
	Root, Parent           primaryarena.Ref
	Closure, ParentClosure primaryarena.Ref
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
	if c != nil && c.nativeWorkOwned && (spec.ActivateWithWork == nil || spec.ConsumeWithWork == nil) {
		return false, ErrNativeMVCCRootWorkRequiredV6
	}
	if c == nil || c.cleanupPhase != 0 || t.phase != durableRootConstructingV6 || c.certificate == nil || resources == nil || resources.Owner() != ResourceOwnerBuilder ||
		spec.PreparedPrimary == nil || spec.PreparedPrimary.readCertificate != c.certificate || spec.Lineage == (DurableRootLineageID{}) || spec.Sequence == 0 ||
		spec.Abort == nil || spec.Fail == nil || !spec.hasDurableRootCallbacksV6() {
		return false, ErrDurableRootOwnership
	}
	if err := validateDurableRootTransactionSpec(spec); err != nil {
		return false, err
	}
	// The constructor retains its original root edge through Finish. Visibility
	// acquires a distinct state edge: replacing that state must not free a still
	// pending transaction's immutable logical root. Native admission covers BOTH
	// arena stages atomically, before refcount or discovery effects.
	if c.nativeWorkOwned {
		if ready, err := c.arena.AcquireAndDetachReadRootConstructionV6(c.root, t, w); !ready || err != nil {
			return ready, err
		}
	} else {
		if ready, err := c.arena.Acquire(c.root, w); !ready || err != nil {
			return ready, err
		}
		// Preserve ordinary compensation. Native binding never enters this
		// unlimited rollback path when its actual allowance is exhausted.
		if ready, err := c.arena.DetachReadRootConstructionV6(c.root, t, w); !ready || err != nil {
			cleanup := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
			_, dropErr := c.arena.Drop(c.root, cleanup)
			return false, errors.Join(err, dropErr)
		}
	}
	t.input = DurableRootCallbackInput{Lineage: spec.Lineage, Sequence: spec.Sequence, Payload: spec.Payload, PreparedCOW: spec.PreparedCOW, PreparedPrimary: spec.PreparedPrimary, PublicationProjection: spec.PublicationProjection}
	t.activate, t.consume, t.abort, t.abortComplete, t.fail = spec.Activate, spec.Consume, spec.Abort, spec.AbortComplete, spec.Fail
	t.activateWithWork, t.consumeWithWork, t.retireWithWork = spec.ActivateWithWork, spec.ConsumeWithWork, spec.RetireWithWork
	t.abortWithWork, t.abortCompleteWithWork = spec.AbortWithWork, spec.AbortCompleteWithWork
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

// PreparePrimaryPromotionStorageV6 admits and allocates only the original
// transaction's private promotion image. No current root, slot, parent, physical
// fence or visible resource edge is selected here. The SAME constructor owns the
// image through cancellation or later promotion, including allowance refusal.
func (t *DurableRootTransaction) PreparePrimaryPromotionStorageV6(w *iterator.OrdinalScanWork) (bool, error) {
	if t == nil || w == nil {
		return false, ErrDurableRootOwnership
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(PrimaryPromotionV6{}))+2*uint64(unsafe.Sizeof(primaryTransactionConstructionV6{}))) {
		return false, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.constructionV6
	if c == nil || !c.nativeWorkOwned || c.cleanupPhase != 0 || t.phase != durableRootConstructingV6 {
		return false, ErrDurableRootOwnership
	}
	if c.promotion != nil {
		return c.promotion.Root == (primaryarena.Ref{}) && len(c.promotion.Image) == PrimaryCapsuleSizeV6, nil
	}
	promotion, charge, ready, err := allocatePrimaryPromotionStorageV6(c.arena.MetadataOwner(), w)
	if !ready || err != nil {
		return ready, err
	}
	c.promotion = promotion
	c.metadataCharge += charge
	return true, nil
}

// Both ordinary and work-owned preparation allocate the same concrete storage
// under the original arena metadata owner, before any allocation effect.
func allocatePrimaryPromotionStorageV6(owner *retainedalloc.Owner, w *iterator.OrdinalScanWork) (*PrimaryPromotionV6, uint64, bool, error) {
	if !w.Reserve(1, 2*PrimaryCapsuleSizeV6+2*uint64(unsafe.Sizeof(PrimaryPromotionV6{}))) {
		return nil, 0, false, nil
	}
	charge := retainedalloc.AllocationCharge(PrimaryCapsuleSizeV6) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(PrimaryPromotionV6{})))
	if err := owner.AddPending(charge); err != nil {
		return nil, 0, false, err
	}
	return &PrimaryPromotionV6{Image: make([]byte, PrimaryCapsuleSizeV6)}, charge, true, nil
}

// Fresh promotion selects actual parent/target and acquires independent physical
// closure edges. These head-bound operands never survive as private preparation.
func (t *DurableRootTransaction) PreparePrimaryPromotionV6(record DurableRootRecordV1, projection PrimaryProjectionV5, parent *PrimaryCapsuleViewV6, parentRoot primaryarena.Ref, w *iterator.OrdinalScanWork) (result *PrimaryPromotionV6, completed bool, returnErr error) {
	return t.preparePrimaryPromotionV6(record, projection, parent, parentRoot, w, false)
}

// PreparePrivatePrimaryPromotionV6 retains the SAME constructor's immutable
// capsule/closure operands before the native final turn. It grants no slot or
// visibility authority. Its caller must revalidate the exact captured parent,
// target, allocator prefix and expected base sequence before enqueue. Stale
// preparation is disposed by this transaction's original finite abort.
func (t *DurableRootTransaction) PreparePrivatePrimaryPromotionV6(record DurableRootRecordV1, projection PrimaryProjectionV5, parent *PrimaryCapsuleViewV6, parentRoot primaryarena.Ref, w *iterator.OrdinalScanWork) (*PrimaryPromotionV6, bool, error) {
	return t.preparePrimaryPromotionV6(record, projection, parent, parentRoot, w, true)
}

func (t *DurableRootTransaction) preparePrimaryPromotionV6(record DurableRootRecordV1, projection PrimaryProjectionV5, parent *PrimaryCapsuleViewV6, parentRoot primaryarena.Ref, w *iterator.OrdinalScanWork, private bool) (result *PrimaryPromotionV6, completed bool, returnErr error) {
	if t == nil || w == nil {
		return nil, false, ErrDurableRootOwnership
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(DurableRootTransaction{}))+2*uint64(unsafe.Sizeof(PrimaryPromotionV6{}))) {
		return nil, false, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.constructionV6
	eligible := t.phase == durableRootActivated && t.ownerLockedV6() == ResourceOwnerCoordinator
	if private {
		eligible = c != nil && c.nativeWorkOwned && c.cleanupPhase == 0 && c.cleanupReceipt == nil && t.phase == durableRootPrepared && t.ownerLockedV6() == ResourceOwnerCandidate
	}
	if c == nil || !eligible || (c.promotion != nil && c.promotion.Root != (primaryarena.Ref{})) || parent == nil || parentRoot == (primaryarena.Ref{}) {
		return nil, false, ErrDurableRootOwnership
	}
	target := uint64(1) - parent.Slot()
	promotion := c.promotion
	charge := uint64(0)
	if promotion == nil {
		var ready bool
		var err error
		promotion, charge, ready, err = allocatePrimaryPromotionStorageV6(c.arena.MetadataOwner(), w)
		if !ready || err != nil {
			return nil, ready, err
		}
	}
	image := promotion.Image
	chargeTransferred := charge == 0
	defer func() {
		if !chargeTransferred {
			image = nil
			promotion.Image = nil
			promotion = nil
			c.arena.MetadataOwner().RemovePending(charge)
		}
	}()
	view, ready, err := ConstructPrimaryCapsuleV6(image, target, DurablePrimaryRootRecordV5{Record: record, Primary: projection}, t.input.PreparedPrimary, parent, w)
	if !ready || err != nil {
		return nil, ready, err
	}
	edge := primaryarena.MetadataEdge{PageID: record.Manifest.FirstPageID, Class: primaryarena.Manifest}
	if record.Directory.RootPageID != 0 {
		edge = primaryarena.MetadataEdge{PageID: record.Directory.RootPageID, Class: primaryarena.Dependency}
	}
	parentRecord := parent.Current().Record
	parentEdge := primaryarena.MetadataEdge{PageID: parentRecord.Manifest.FirstPageID, Class: primaryarena.Manifest}
	if parentRecord.Directory.RootPageID != 0 {
		parentEdge = primaryarena.MetadataEdge{PageID: parentRecord.Directory.RootPageID, Class: primaryarena.Dependency}
	}
	closure, parentClosure, ready, err := c.arena.AcquirePromotionCustodyV6(c.root, parentRoot, edge, parentEdge, w)
	if !ready || err != nil {
		return nil, ready, err
	}
	*promotion = PrimaryPromotionV6{Image: image, View: view, Root: c.root, Parent: parentRoot, Closure: closure, ParentClosure: parentClosure}
	c.promotion = promotion
	c.metadataCharge += charge
	chargeTransferred = true
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

// The ordinary report drives the SAME Finish stages with its existing
// synchronous resource callback. Native retirement supplies the original
// receipt's work-bearing resource release to the ready-return core below.
func (t *DurableRootTransaction) finishOrdinaryPrimaryV6(w *iterator.OrdinalScanWork, activePins map[ResourceKind]uint64) (*primaryTerminalCompletionV6, error) {
	completion, ready, err := t.finishPrimaryStepWithWorkV6(w, activePins, nil)
	if !ready && err == nil {
		return nil, ErrNativeMVCCRootWorkRequiredV6
	}
	return completion, err
}

// The SAME transaction retains all unfinished physical custody on false, nil.
// release must resume its original Set/receipt and admit its concrete operations
// with w. For native construction it must drain/check this SAME cleanupQueue
// before reporting ready; only the ordinary path may use the synchronous nil callback. The
// returned original terminal is consumed after callers detach aliases and leave
// their callback/lock scopes. This helper does not refund containing metadata.
func (t *DurableRootTransaction) finishPrimaryStepWithWorkV6(w *iterator.OrdinalScanWork, activePins map[ResourceKind]uint64, release func(*StableResourceSet, map[ResourceKind]uint64, *iterator.OrdinalScanWork) (bool, error)) (*primaryTerminalCompletionV6, bool, error) {
	if t == nil || w == nil {
		return nil, false, ErrDurableRootOwnership
	}
	t.mu.Lock()
	if t.physicalCleanupError != nil {
		err := t.physicalCleanupError
		t.mu.Unlock()
		return nil, false, err
	}
	c := t.constructionV6
	if c == nil || c.resources == nil || t.phase != durableRootFinishing || t.owner != ResourceOwnerCoordinator || t.finishBusy {
		t.mu.Unlock()
		return nil, false, ErrDurableRootOwnership
	}
	setOwner := c.resources.Owner()
	if setOwner != ResourceOwnerCoordinator && !(c.finishReleaseStarted && setOwner == ResourceOwnerReleased) {
		t.mu.Unlock()
		return nil, false, ErrDurableRootOwnership
	}
	if c.nativeWorkOwned && release == nil {
		t.mu.Unlock()
		return nil, false, ErrNativeMVCCRootWorkRequiredV6
	}
	// Admit this call's original descriptor updates/scrub before physical work,
	// so a completed Set release never needs replay for a later scrub refusal.
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(DurableRootTransaction{}))+2*uint64(unsafe.Sizeof(primaryTransactionConstructionV6{}))) {
		t.mu.Unlock()
		return nil, false, nil
	}
	t.finishBusy = true
	retire, retireInput := t.retireWithWork, t.input
	t.mu.Unlock()

	ready, err := true, error(nil)
	// finishBusy serializes the original callback while t.mu is released. A
	// superseded descriptor may acquire its own physical-owner locks, and must
	// never run under the publication turn or a transaction lock. Normal work
	// refusal retains BOTH the callback and its original containing custody.
	if !c.finishRetirementDone {
		if retire != nil {
			ready, err = retire(retireInput, w)
		}
		if ready && err == nil {
			t.mu.Lock()
			c.finishRetirementDone = true
			t.mu.Unlock()
		}
	}
	if ready && err == nil && c.root != (primaryarena.Ref{}) {
		if c.nativeWorkOwned {
			ready, err = c.arena.DropToReleaseQueue(c.root, &c.cleanupQueue, w)
		} else {
			ready, err = c.arena.Drop(c.root, w)
		}
		if ready && err == nil {
			t.mu.Lock()
			c.root = primaryarena.Ref{}
			t.mu.Unlock()
		}
	}
	if ready && err == nil && !c.finishReleaseDone {
		t.mu.Lock()
		c.finishReleaseStarted = true
		t.mu.Unlock()
		if release != nil {
			ready, err = release(c.resources, activePins, w)
		} else {
			err = c.resources.releaseFromWithActivePins(ResourceOwnerCoordinator, activePins)
		}
		if ready && err == nil {
			// Released alone cannot authorize containing descriptor disposal.
			// Check the SAME Set's actual completion and first physical outcome.
			if c.resources.Owner() != ResourceOwnerReleased {
				err = ErrDurableRootOwnership
			} else {
				err = c.resources.releaseResult()
			}
			if err == nil {
				t.mu.Lock()
				c.finishReleaseDone = true
				t.mu.Unlock()
			}
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.finishBusy = false
	if err != nil {
		return nil, false, t.recordPhysicalCleanupFailureLocked(err)
	}
	if !ready {
		return nil, false, nil
	}
	if !c.finishReleaseDone || c.terminalCompletion == nil {
		return nil, false, t.recordPhysicalCleanupFailureLocked(ErrDurableRootOwnership)
	}
	// Immutable state/slot/reader refs independently own the root. Scrub this
	// mutable constructor, then move its original accounting custody out of the
	// call; defer unlock runs before the terminal can be consumed by its caller.
	completion := c.terminalCompletion
	completion.charge = c.metadataCharge
	if c.setReleaseConsumed {
		// The disposing callback has returned and mutable parent aliases are
		// gone before transferring by-value accounting into the SAME terminal.
		c.resources = nil
		_, charge, err := c.setRelease.takeCompletionExpected(completion.owner)
		if err != nil {
			return nil, false, t.recordPhysicalCleanupFailureLocked(err)
		}
		completion.charge += charge
	}
	t.input = DurableRootCallbackInput{Lineage: t.input.Lineage, Sequence: t.input.Sequence}
	t.clearDurableRootCallbacksV6()
	t.constructionV6 = nil
	*c = primaryTransactionConstructionV6{}
	t.owner = ResourceOwnerReleased
	t.phase = durableRootConsumed
	return completion, true, nil
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
func (t *DurableRootTransaction) TransferPrimaryPromotionImageV6() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.constructionV6
	if c == nil || c.promotion == nil || c.promotion.Root == (primaryarena.Ref{}) {
		panic("promotion transfer lost owner")
	}
	charge := retainedalloc.AllocationCharge(PrimaryCapsuleSizeV6)
	c.metadataCharge -= charge
	c.promotion.Image = nil
	c.promotion.View = PrimaryCapsuleViewV6{}
	return charge
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
	t.clearDurableRootCallbacksV6()
	t.constructionV6 = nil
	completion := c.terminalCompletion
	completion.charge = c.metadataCharge
	*c = primaryTransactionConstructionV6{}
	t.owner = ResourceOwnerReleased
	t.phase = durableRootConsumed
	return completion, nil
}
