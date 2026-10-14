package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
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
	receipt        OwnedPublicationReceiptV5
	finishDetached bool
	finishQueue    *primaryarena.ReleaseQueue
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
	t := &DurableRootTransaction{owner: ResourceOwnerBuilder, phase: durableRootConstructingV6, constructionV6: &primaryTransactionConstructionV6{arena: a}}
	root, ready, err := a.PrepareConstructedReadRootV6(t, w)
	if !ready || err != nil {
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
	// The constructor retains its original root edge through Finish. Visibility
	// acquires a distinct state edge: replacing that state must not free a still
	// pending transaction's immutable logical root.
	if ready, err := c.arena.Acquire(c.root, w); !ready || err != nil {
		return ready, err
	}
	t.input = DurableRootCallbackInput{Lineage: spec.Lineage, Sequence: spec.Sequence, Payload: spec.Payload, PreparedCOW: spec.PreparedCOW, PreparedPrimary: spec.PreparedPrimary, PublicationProjection: spec.PublicationProjection}
	t.activate, t.consume, t.abort, t.fail = spec.Activate, spec.Consume, spec.Abort, spec.Fail
	t.owned = spec.Owned
	c.resources = resources
	t.phase = durableRootPrepared
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
	t.owner = ResourceOwnerReleased
	t.phase = durableRootConsumed
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
func (t *DurableRootTransaction) PreparePrimaryPromotionV6(record DurableRootRecordV1, projection PrimaryProjectionV5, parent *PrimaryCapsuleViewV6, parentRoot primaryarena.Ref, w *iterator.OrdinalScanWork) (*PrimaryPromotionV6, bool, error) {
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
	image := make([]byte, PrimaryCapsuleSizeV6)
	view, ready, err := ConstructPrimaryCapsuleV6(image, target, DurablePrimaryRootRecordV5{Record: record, Primary: projection}, t.input.PreparedPrimary, parent, w)
	if !ready || err != nil {
		return nil, ready, err
	}
	edge := primaryarena.MetadataEdge{PageID: record.Manifest.FirstPageID, Class: primaryarena.Manifest}
	if record.Directory.RootPageID != 0 {
		edge = primaryarena.MetadataEdge{PageID: record.Directory.RootPageID, Class: primaryarena.Dependency}
	}
	if ready, err = c.arena.BindReadRootDependencyV6(c.root, edge, w); !ready || err != nil {
		return nil, ready, err
	}
	if ready, err = c.arena.Acquire(parentRoot, w); !ready || err != nil {
		return nil, ready, err
	}
	if ready, err = c.arena.Acquire(c.root, w); !ready || err != nil {
		// This synchronous ordinary experiment must retain the unwind in its ledger.
		cleanup := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
		_, dropErr := c.arena.Drop(parentRoot, cleanup)
		return nil, ready, errors.Join(err, dropErr)
	}
	c.promotion = &PrimaryPromotionV6{Image: image, View: view, Root: c.root, Parent: parentRoot}
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
	t.finishBusy = true
	t.mu.Unlock()
	// Existing ordinary release is synchronous; it is recorded as actual ordinary
	// callback work, not disguised by a scalar native reservation.
	// Detach the bank's constructor view before last-owner resource release can
	// physically free that bank. The pending transaction remains the sole Finish
	// owner while its synchronous callbacks execute.
	ready, err := c.arena.DetachReadRootConstructionV6(c.root, t, w)
	if ready && err == nil {
		c.resources.releaseFrom(ResourceOwnerCoordinator)
		ready, err = c.arena.Drop(c.root, w)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.finishBusy = false
	if !ready || err != nil || c.resources.Owner() != ResourceOwnerReleased {
		return errors.Join(err, ErrDurableRootOwnership)
	}
	c.root = primaryarena.Ref{}
	t.phase = durableRootConsumed
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

// retireOwnedConstructorV6 runs only under this transaction's mutex. The same
// receipt retains the queue through every return; ordinary writers cannot drain
// this bank/group/dependency debt. Empty observation is itself charged.
func (t *DurableRootTransaction) retireOwnedConstructorV6(w *iterator.OrdinalScanWork) (bool, bool, error) {
	c := t.constructionV6
	progress := false
	if c.finishQueue == nil {
		q, ready, err := c.arena.NewReleaseQueue(w)
		if !ready || err != nil {
			return false, false, err
		}
		c.finishQueue = q
		progress = true
	}
	if c.root != (primaryarena.Ref{}) {
		ready, err := c.arena.DropToReleaseQueue(c.root, c.finishQueue, w)
		if !ready || err != nil {
			return false, progress, err
		}
		c.root = primaryarena.Ref{}
		progress = true
	}
	for {
		ready, p, err := c.arena.ReleaseQueueStep(c.finishQueue, w)
		progress = progress || p
		if !ready || err != nil {
			return false, progress, err
		}
		if !p {
			break
		}
	}
	c.finishQueue = nil
	t.phase = durableRootConsumed
	t.releaseOwnerLockedV6()
	return true, true, nil
}
