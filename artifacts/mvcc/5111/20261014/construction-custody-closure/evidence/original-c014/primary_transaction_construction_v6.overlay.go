package rootpublication

import (
	"errors"
	"runtime"
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
	candidate      PreparedRootCandidate
	members        [1]*DurableRootTransaction
	candidateBound bool
	metadataCharge uint64
}
type PrimaryPromotionV6 struct {
	Image        []byte
	View         PrimaryCapsuleViewV6
	Root, Parent primaryarena.Ref
}

func NewPrimaryConstructionTransactionV6(a *primaryarena.Arena, w *iterator.OrdinalScanWork) (primaryarena.ReadRootConstructionV6, bool, error) {
	if a == nil || !a.CapsuleFormatV6() || w == nil {
		return nil, false, ErrDurableRootOwnership
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(DurableRootTransaction{}))+2*uint64(unsafe.Sizeof(primaryTransactionConstructionV6{}))) {
		return nil, false, nil
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(DurableRootTransaction{}))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryTransactionConstructionV6{})))
	if err := a.MetadataOwner().AddPending(charge); err != nil {
		return nil, false, err
	}
	t := &DurableRootTransaction{owner: ResourceOwnerBuilder, phase: durableRootConstructingV6, constructionV6: &primaryTransactionConstructionV6{arena: a, metadataCharge: charge}}
	root, ready, err := a.PrepareConstructedReadRootV6(t, w)
	if !ready || err != nil {
		a.MetadataOwner().RemovePending(charge)
		return nil, ready, err
	}
	t.constructionV6.root = root
	t.mu.Lock()
	c := t.constructionV6
	println("COW_TXN_ID", "constructor-success", t, c.arena, c.arena.MetadataOwner(), c.root.PageID, c.root.Incarnation, t.phase, c.metadataCharge, c.metadataCharge, true)
	for diagnosticDepth := 1; diagnosticDepth <= 12; diagnosticDepth++ {
		diagnosticPC, diagnosticFile, diagnosticLine, diagnosticOK := runtime.Caller(diagnosticDepth)
		if !diagnosticOK { break }
		diagnosticSymbol := ""
		if diagnosticFunction := runtime.FuncForPC(diagnosticPC); diagnosticFunction != nil { diagnosticSymbol = diagnosticFunction.Name() }
		println("COW_TXN_CALLER", t, diagnosticDepth, diagnosticPC, diagnosticFile, diagnosticLine, diagnosticSymbol)
	}
	t.mu.Unlock()
	return t, true, nil
}
func (t *DurableRootTransaction) PrimaryConstructionReferenceV6() primaryarena.Ref {
	if t == nil {
		return primaryarena.Ref{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
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
	if c == nil || t.phase != durableRootConstructingV6 || c.certificate != nil {
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
	if t.constructionV6 == nil || t.phase != durableRootConstructingV6 {
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
	if c == nil || t.phase != durableRootConstructingV6 || c.certificate == nil || resources == nil || resources.Owner() != ResourceOwnerBuilder ||
		spec.PreparedPrimary == nil || spec.PreparedPrimary.readCertificate != c.certificate || spec.Lineage == (DurableRootLineageID{}) || spec.Sequence == 0 ||
		spec.Abort == nil || spec.Fail == nil {
		return false, ErrDurableRootOwnership
	}
	if err := validateDurableRootTransactionSpec(spec); err != nil {
		return false, err
	}
	println("COW_TXN_ID", "bind-before", t, c.arena, c.arena.MetadataOwner(), c.root.PageID, c.root.Incarnation, t.phase, c.metadataCharge, c.metadataCharge, true)
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
	t.activate, t.consume, t.abort, t.fail = spec.Activate, spec.Consume, spec.Abort, spec.Fail
	c.resources = resources
	c.metadataCharge += spec.PrimaryMetadataBytesV6
	t.phase = durableRootPrepared
	println("COW_TXN_ID", "bind-after", t, c.arena, c.arena.MetadataOwner(), c.root.PageID, c.root.Incarnation, t.phase, c.metadataCharge, c.metadataCharge, true)
	return true, nil
}
func (t *DurableRootTransaction) ownerLockedV6() ResourceOwnerState {
	if t.constructionV6 != nil && t.constructionV6.resources != nil {
		return t.constructionV6.resources.Owner()
	}
	return t.owner
}
func (t *DurableRootTransaction) releaseOwnerLockedV6() {
	if t.constructionV6 == nil || t.constructionV6.resources == nil {
		t.owner = ResourceOwnerReleased
	}
	// The actual set release is the once-only owner transition, after callback
	// completion and active-pin accounting. No mirrored released cell is written.
}
func (t *DurableRootTransaction) abortConstructionLockedV6() error {
	c := t.constructionV6
	println("COW_TXN_ID", "abort-before", t, c.arena, c.arena.MetadataOwner(), c.root.PageID, c.root.Incarnation, t.phase, c.metadataCharge, c.metadataCharge, true)
	for diagnosticDepth := 1; diagnosticDepth <= 12; diagnosticDepth++ {
		diagnosticPC, diagnosticFile, diagnosticLine, diagnosticOK := runtime.Caller(diagnosticDepth)
		if !diagnosticOK { break }
		diagnosticSymbol := ""
		if diagnosticFunction := runtime.FuncForPC(diagnosticPC); diagnosticFunction != nil { diagnosticSymbol = diagnosticFunction.Name() }
		println("COW_TXN_CALLER", t, diagnosticDepth, diagnosticPC, diagnosticFile, diagnosticLine, diagnosticSymbol)
	}
	work := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
	for c.componentCount > 0 {
		r := c.components[c.componentCount-1]
		ready, err := c.arena.Drop(r, work)
		if !ready || err != nil {
			return errors.Join(err, ErrDurableRootOwnership)
		}
		c.componentCount--
	}
	ready, err := c.arena.Drop(c.root, work)
	if !ready || err != nil {
		return errors.Join(err, ErrDurableRootOwnership)
	}
	t.constructionV6 = nil
	t.input = DurableRootCallbackInput{}
	t.activate, t.consume, t.abort, t.fail = nil, nil, nil, nil
	c.arena.MetadataOwner().RemovePending(c.metadataCharge)
	t.owner = ResourceOwnerReleased
	t.phase = durableRootConsumed
	println("COW_TXN_ID", "abort-after", t, c.arena, c.arena.MetadataOwner(), c.root.PageID, c.root.Incarnation, t.phase, 0, c.metadataCharge, false)
	return nil
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
	if c == nil || t.phase != durableRootConstructingV6 || c.componentCount == len(c.components) || ref == (primaryarena.Ref{}) {
		return false, ErrDurableRootOwnership
	}
	c.components[c.componentCount] = ref
	c.componentCount++
	return true, nil
}

// The ordinary report owns this same synchronous Finish. Its actual ordinary
// release callbacks are deliberately NOT claimed as bounded native retirement.
func (t *DurableRootTransaction) finishOrdinaryPrimaryV6(w *iterator.OrdinalScanWork) error {
	if w == nil {
		return ErrDurableRootOwnership
	}
	t.mu.Lock()
	c := t.constructionV6
	if c == nil || c.resources == nil || t.phase != durableRootFinishing || c.resources.Owner() != ResourceOwnerCoordinator || t.finishBusy {
		t.mu.Unlock()
		return ErrDurableRootOwnership
	}
	println("COW_TXN_ID", "finish-before", t, c.arena, c.arena.MetadataOwner(), c.root.PageID, c.root.Incarnation, t.phase, c.metadataCharge, c.metadataCharge, true)
	t.finishBusy = true
	t.mu.Unlock()
	// Runtime/coordinator have been the sole mutable constructor owner since
	// Bind. Synchronous ordinary callbacks release its resources once; readers
	// have no backpointer to this pending graph.
	c.resources.releaseFrom(ResourceOwnerCoordinator)
	ready, err := c.arena.Drop(c.root, w)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.finishBusy = false
	if !ready || err != nil || c.resources.Owner() != ResourceOwnerReleased {
		return errors.Join(err, ErrDurableRootOwnership)
	}
	// Current DBState and borrowed candidates can outlive Finish. Keep the same
	// transaction identity, but release its completed mutable/callback descriptors;
	// exact state/slot/reader refs independently retain the immutable arena root.
	t.input = DurableRootCallbackInput{Lineage: t.input.Lineage, Sequence: t.input.Sequence}
	t.activate, t.consume, t.abort, t.fail = nil, nil, nil, nil
	t.constructionV6 = nil
	c.arena.MetadataOwner().RemovePending(c.metadataCharge)
	t.owner = ResourceOwnerReleased
	t.phase = durableRootConsumed
	println("COW_TXN_ID", "finish-after", t, c.arena, c.arena.MetadataOwner(), c.root.PageID, c.root.Incarnation, t.phase, 0, c.metadataCharge, false)
	return nil
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

// Recovery releases the SAME constructor only after the handoff has actually
// released its resource set. Closed physical storage has already discarded its
// bank edges; otherwise exact ordinary root custody is dropped here.
func (t *DurableRootTransaction) releasePrimaryRecoveryLockedV6() {
	c := t.constructionV6
	if c == nil || c.resources == nil || c.resources.Owner() != ResourceOwnerReleased {
		return
	}
	println("COW_TXN_ID", "recovery-before", t, c.arena, c.arena.MetadataOwner(), c.root.PageID, c.root.Incarnation, t.phase, c.metadataCharge, c.metadataCharge, true)
	for diagnosticDepth := 1; diagnosticDepth <= 12; diagnosticDepth++ {
		diagnosticPC, diagnosticFile, diagnosticLine, diagnosticOK := runtime.Caller(diagnosticDepth)
		if !diagnosticOK { break }
		diagnosticSymbol := ""
		if diagnosticFunction := runtime.FuncForPC(diagnosticPC); diagnosticFunction != nil { diagnosticSymbol = diagnosticFunction.Name() }
		println("COW_TXN_CALLER", t, diagnosticDepth, diagnosticPC, diagnosticFile, diagnosticLine, diagnosticSymbol)
	}
	if !c.arena.MetadataOwner().PhysicalClosed() {
		if ready, err := c.arena.Drop(c.root, nil); !ready || err != nil {
			c.arena.MetadataOwner().CleanupFailed()
			return
		}
	}
	if c.promotion != nil {
		c.promotion.Image = nil
		c.promotion.View = PrimaryCapsuleViewV6{}
	}
	t.input = DurableRootCallbackInput{Lineage: t.input.Lineage, Sequence: t.input.Sequence}
	t.activate, t.consume, t.abort, t.fail = nil, nil, nil, nil
	t.constructionV6 = nil
	c.arena.MetadataOwner().RemovePending(c.metadataCharge)
	t.owner = ResourceOwnerReleased
	println("COW_TXN_ID", "recovery-after", t, c.arena, c.arena.MetadataOwner(), c.root.PageID, c.root.Incarnation, t.phase, 0, c.metadataCharge, false)
}
