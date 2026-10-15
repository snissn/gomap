package db

import (
	"context"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

// ErrRecoverableRootSetStale reports that a recovery-selectable root changed
// after a maintenance plan captured its authority. Callers must rebuild the
// plan; destructive work must not retry from the stale capability.
var ErrRecoverableRootSetStale = errors.New("treedb: recoverable root set changed")

// RecoverableRoot is an immutable scalar root that was visible, durable, or
// queued when a RecoverableRootSet was captured.
type RecoverableRoot struct {
	CommitSeq         uint64
	UserRootPageID    uint64
	SystemRootPageID  uint64
	AppliedCommandLSN uint64
	MaxEntryRevision  uint64
	Durable           bool
	Visible           bool
}

type recoverableRootKey struct {
	commitSeq         uint64
	userRootPageID    uint64
	systemRootPageID  uint64
	appliedCommandLSN uint64
	maxEntryRevision  uint64
}

func recoverableRootIdentity(root RecoverableRoot) recoverableRootKey {
	return recoverableRootKey{
		commitSeq: root.CommitSeq, userRootPageID: root.UserRootPageID,
		systemRootPageID: root.SystemRootPageID, appliedCommandLSN: root.AppliedCommandLSN,
		maxEntryRevision: root.MaxEntryRevision,
	}
}

type recoverableDurableBasis struct {
	slot             uint64
	slotCommit       [2]uint64
	slotRecord       [2]rootpublication.DurableRootRecordV1
	primaryProof     [2]rootpublication.DurablePrimaryRootRecordV5
	primaryReadRoots [4]uint64
	dataFloor        [4]uint64
	pending          *durableRootPublishCandidateV1
	ambiguous        []*durableRootPublishCandidateV1
}

func (basis recoverableDurableBasis) equal(other recoverableDurableBasis) bool {
	if basis.slot != other.slot || basis.slotCommit != other.slotCommit || basis.slotRecord != other.slotRecord || basis.primaryProof != other.primaryProof || basis.primaryReadRoots != other.primaryReadRoots || basis.dataFloor != other.dataFloor || basis.pending != other.pending || len(basis.ambiguous) != len(other.ambiguous) {
		return false
	}
	for i := range basis.ambiguous {
		if basis.ambiguous[i] != other.ambiguous[i] {
			return false
		}
	}
	return true
}

func (db *DB) recoverableDurableBasisLocked() recoverableDurableBasis {
	basis := recoverableDurableBasis{slot: db.durableRoot.slot, slotCommit: db.durableRoot.slotCommit, slotRecord: db.durableRoot.slotRecord, pending: db.durableRoot.pending, ambiguous: db.durableRoot.ambiguous}
	if primary := db.durableRoot.primary; primary != nil {
		basis.primaryProof = primary.proofRecords
		basis.dataFloor = primary.dataFloor
		if db.idx.Load().primary.CapsuleFormatV6() {
			for i := 0; i < 2; i++ {
				basis.primaryReadRoots[i] = primary.records[i].PageID
				basis.primaryReadRoots[i+2] = primary.proofs[i].PageID
			}
		}
	}
	return basis
}

type recoverableIdentityPin struct {
	identity   rootpublication.StableIdentity
	pin        *rootpublication.IdentityPin
	observed   bool
	cleanupErr error
}

// RecoverableRootSet is an opaque, DB-minted maintenance capability. Its
// fields are deliberately private: callers can inspect copied roots, acquire
// root-bound snapshots, add exact physical pins discovered while traversing a
// root, revalidate, and release, but cannot manufacture authority.
type recoverableRootResource struct {
	key       recoverableRootKey
	resources *rootpublication.StableResourceSet
	present   bool
}
type RecoverableRootSet struct {
	db                *DB
	metadata          *retainedalloc.Owner
	metadataCharge    uint64
	custody           *rootpublication.OriginalCleanupCustody
	failure           originalDBCleanupFailure
	releaseOnce       sync.Once
	releaseErr        error
	resourceStorage   [7]*rootpublication.StableResourceSet
	primaryRefStorage [4]primaryarena.Ref
	rootResources     [5]recoverableRootResource

	roots               []RecoverableRoot
	visible             StateToken
	durable             recoverableDurableBasis
	coordinator         *rootpublication.Coordinator
	coordinatorEpoch    uint64
	idx                 *indexGen
	stableSnapshot      *Snapshot
	oldestRegistryID    int64
	systemRootEpoch     uint64
	resources           []*rootpublication.StableResourceSet
	primaryArena        *primaryarena.Arena
	primaryRefs         []primaryarena.Ref
	rootResourceIndexID uint64
	rootResourceIndex   rootpublication.StableIdentity
	identityPinRegistry *rootpublication.IdentityPinRegistry

	mu           sync.Mutex
	identityPins rootpublication.IdentityCapacityTable[recoverableIdentityPin]
	released     atomic.Bool
}

// CaptureRecoverableRootSet captures the exact bounded recovery closure while
// serializing with storage maintenance admission.
func (db *DB) CaptureRecoverableRootSet(ctx context.Context) (*RecoverableRootSet, error) {
	if db == nil {
		return nil, ErrClosed
	}
	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()
	return db.captureRecoverableRootSetWithMaintenanceLockHeld(ctx)
}

// CaptureRecoverableRootSetForInspection captures root-bound resource authority
// for read-only inspection, including on read-only or command-WAL-poisoned
// handles whose published snapshots remain readable. Ambiguous root publication
// still requires reopen before any new snapshot or inspection capture.
func (db *DB) CaptureRecoverableRootSetForInspection(ctx context.Context) (*RecoverableRootSet, error) {
	if db == nil {
		return nil, ErrClosed
	}
	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()
	return db.captureRecoverableRootSetForInspectionWithMaintenanceLockHeld(ctx)
}

const recoverableRootSetCaptureAttempts = 8

func (db *DB) captureRecoverableRootSetWithMaintenanceLockHeld(ctx context.Context) (*RecoverableRootSet, error) {
	return db.captureRecoverableRootSetWithMaintenanceLockHeldMode(ctx, true)
}

// captureRecoverableRootSetForInspectionWithMaintenanceLockHeld captures the
// same recovery closure for read-only audit paths. Unlike the destructive
// capability path, it remains available on read-only or command-WAL-poisoned
// handles. Root-publication poison still forbids acquiring a new snapshot.
func (db *DB) captureRecoverableRootSetForInspectionWithMaintenanceLockHeld(ctx context.Context) (*RecoverableRootSet, error) {
	return db.captureRecoverableRootSetWithMaintenanceLockHeldMode(ctx, false)
}

func (db *DB) captureRecoverableRootSetWithMaintenanceLockHeldMode(ctx context.Context, requireMaintenanceReady bool) (*RecoverableRootSet, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if requireMaintenanceReady {
		if err := db.CheckStorageMaintenanceReady(); err != nil {
			return nil, err
		}
	} else if db == nil || db.closing.Load() {
		return nil, ErrClosed
	} else if err := db.publicationPoisonedError(); err != nil {
		return nil, err
	}
	stable := db.acquireStableSnapshotWithMaintenanceLockHeld()
	if stable == nil || stable.idx == nil || stable.idx.registry == nil {
		if stable != nil {
			if closeErr := stable.Close(); closeErr != nil {
				db.retainOriginalCleanupError(closeErr)
				return nil, errors.Join(errors.New("treedb: capture recoverable root set stable index"), closeErr)
			}
		}
		return nil, errors.New("treedb: capture recoverable root set stable index")
	}

	for attempt := 0; attempt < recoverableRootSetCaptureAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if closeErr := stable.Close(); closeErr != nil {
				db.retainOriginalCleanupError(closeErr)
				return nil, errors.Join(err, closeErr)
			}
			return nil, err
		}
		capability, retry, err := db.tryCaptureRecoverableRootSet(stable)
		if err != nil {
			if closeErr := stable.Close(); closeErr != nil {
				db.retainOriginalCleanupError(closeErr)
				return nil, errors.Join(err, closeErr)
			}
			return nil, err
		}
		if !retry {
			return capability, nil
		}
	}
	if closeErr := stable.Close(); closeErr != nil {
		db.retainOriginalCleanupError(closeErr)
		return nil, errors.Join(ErrRecoverableRootSetStale, closeErr)
	}
	return nil, fmt.Errorf("%w: capture did not converge", ErrRecoverableRootSetStale)
}

func (db *DB) tryCaptureRecoverableRootSet(stable *Snapshot) (capability *RecoverableRootSet, retry bool, resultErr error) {
	metadata := db.valueLogIdentityPins.MetadataOwner()
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(RecoverableRootSet{})))
	if err := metadata.AddPending(charge); err != nil {
		return nil, false, err
	}
	custody, err := rootpublication.NewOriginalCleanupCustody(db.valueLogIdentityPins, metadata)
	if err != nil {
		metadata.RemovePending(charge)
		return nil, false, err
	}
	set := &RecoverableRootSet{db: db, metadata: metadata, metadataCharge: charge, custody: custody,
		identityPinRegistry: db.valueLogIdentityPins, identityPins: rootpublication.NewIdentityCapacityTable[recoverableIdentityPin](metadata)}
	set.failure.payload = set
	set.resources = set.resourceStorage[:0]
	set.primaryRefs = set.primaryRefStorage[:0]
	defer func() {
		if capability == nil {
			cleanupErr := set.ReleaseWithError()
			resultErr = errors.Join(resultErr, cleanupErr)
			if cleanupErr != nil {
				retry = false
			}
		}
	}()

	var rootResourceIndex rootpublication.StableIdentity
	if err := stable.idx.pager.WithStableResourceFile(func(file *os.File) error {
		var identityErr error
		rootResourceIndex, identityErr = rootpublication.StableIdentityFromFile(file)
		return identityErr
	}); err != nil {
		return nil, false, err
	}
	var coordinator *rootpublication.Coordinator
	if db.rootPublication != nil {
		coordinator = db.rootPublication.coordinator
	}
	var coordinatorView rootpublication.ReachabilitySnapshot
	if coordinator != nil {
		coordinatorView, err = coordinator.CaptureReachability()
		if err != nil {
			return nil, false, publicRootPublicationErrorV1(err)
		}
	}
	if coordinatorView.Resources != nil {
		set.resources = append(set.resources, coordinatorView.Resources)
		coordinatorView.Resources = nil
	}

	var visibleResources *rootpublication.StableResourceSet
	if db.rootPublication != nil {
		visibleResources, err = db.rootPublication.cloneVisibleResources()
		if err != nil {
			return nil, false, err
		}
	}
	if visibleResources != nil {
		set.resources = append(set.resources, visibleResources)
	}

	db.durablePublishMu.Lock()
	db.rootReuseMu.RLock()
	sourceCapacity, capacityErr := checkedRetainedCapacity(5, len(db.durableRoot.ambiguous))
	var ambiguousCharge, sourceCharge uint64
	if capacityErr == nil {
		capacityErr = checkedRetainedPayloadChargeAdd(&ambiguousCharge, uint64(len(db.durableRoot.ambiguous)), uint64(unsafe.Sizeof((*durableRootPublishCandidateV1)(nil))))
	}
	if capacityErr == nil {
		capacityErr = checkedRetainedPayloadChargeAdd(&sourceCharge, uint64(sourceCapacity), uint64(unsafe.Sizeof((*rootpublication.StableResourceSet)(nil))))
	}
	if capacityErr == nil && sourceCharge > ^uint64(0)-ambiguousCharge {
		capacityErr = retainedalloc.ErrCapacity
	}
	if capacityErr == nil {
		capacityErr = metadata.AddPending(ambiguousCharge + sourceCharge)
	}
	if err := capacityErr; err != nil {
		db.rootReuseMu.RUnlock()
		db.durablePublishMu.Unlock()
		return nil, false, err
	}
	set.metadataCharge += ambiguousCharge
	durable := db.recoverableDurableBasisLocked()
	durable.ambiguous = append([]*durableRootPublishCandidateV1(nil), durable.ambiguous...)
	set.durable = durable
	sources := make([]*rootpublication.StableResourceSet, 0, sourceCapacity)
	sources = append(sources, db.durableRoot.slotResources[:]...)
	if primary := db.durableRoot.primary; primary != nil {
		sources = append(sources, primary.proofResources[:]...)
	}
	if pending := db.durableRoot.pending; pending != nil {
		sources = append(sources, pending.resources)
	}
	for _, ambiguous := range db.durableRoot.ambiguous {
		if ambiguous != nil {
			sources = append(sources, ambiguous.resources)
		}
	}
	durableResources, cloneErr := cloneRecoverableResourceUnion(sources...)
	clear(sources)
	sources = nil
	metadata.RemovePending(sourceCharge)
	if durableResources != nil {
		set.resources = append(set.resources, durableResources)
	}
	if cloneErr == nil {
		var recordStorage [4]rootpublication.DurableRootRecordV1
		var recordResourceStorage [4]*rootpublication.StableResourceSet
		records := append(recordStorage[:0], db.durableRoot.slotRecord[:]...)
		recordResources := append(recordResourceStorage[:0], db.durableRoot.slotResources[:]...)
		if primary := db.durableRoot.primary; primary != nil {
			for i, proof := range primary.proofRecords {
				records = append(records, proof.Record)
				recordResources = append(recordResources, primary.proofResources[i])
			}
		}
		for slot, record := range records {
			if durable.primaryReadRoots[slot] != 0 {
				record.UserRootPageID = durable.primaryReadRoots[slot]
			}
			if record.CommitSeq == 0 {
				continue
			}
			resources, resourceErr := rootpublication.CloneStableResourceSetExcludingKinds(recordResources[slot])
			if resourceErr != nil {
				cloneErr = resourceErr
				break
			}
			root := RecoverableRoot{
				CommitSeq: record.CommitSeq, UserRootPageID: record.UserRootPageID,
				SystemRootPageID: record.SystemRootPageID, AppliedCommandLSN: record.AppliedCommandLSN,
				MaxEntryRevision: record.MaxEntryRevision,
			}
			set.putRootResources(recoverableRootIdentity(root), resources)
			if resources != nil {
				set.resources = append(set.resources, resources)
			}
		}
	}
	var primaryArena *primaryarena.Arena
	primaryRefs := set.primaryRefs
	if cloneErr == nil && db.durableRoot.primary != nil {
		primaryArena = stable.idx.primary
		primary := db.durableRoot.primary
		var refStorage [4]primaryarena.Ref
		refs := append(refStorage[:0], primary.records[:]...)
		refs = append(refs, primary.proofs[:]...)
		for _, ref := range refs {
			if ref.PageID == 0 {
				continue
			}
			ready, e := primaryArena.Acquire(ref, nil)
			if e != nil || !ready {
				cloneErr = errors.Join(e, errors.New("treedb: primary capture ownership unavailable"))
				break
			}
			primaryRefs = append(primaryRefs, ref)
			set.primaryRefs = primaryRefs
			set.primaryArena = primaryArena
		}
	}
	db.durablePublishMu.Unlock()
	if cloneErr != nil {
		db.rootReuseMu.RUnlock()
		return nil, false, cloneErr
	}

	state, ok := db.StateToken()
	if !ok || stable.idx != db.idx.Load() {
		db.rootReuseMu.RUnlock()
		return nil, true, nil
	}
	rootCapacity, rootCapacityErr := checkedRetainedCapacity(9, len(coordinatorView.Pending), len(durable.ambiguous))
	var rootCharge uint64
	if rootCapacityErr == nil {
		rootCapacityErr = checkedRetainedPayloadChargeAdd(&rootCharge, uint64(rootCapacity), uint64(unsafe.Sizeof(RecoverableRoot{})))
	}
	if rootCapacityErr == nil {
		rootCapacityErr = metadata.AddPending(rootCharge)
	}
	if err := rootCapacityErr; err != nil {
		db.rootReuseMu.RUnlock()
		return nil, false, err
	}
	set.metadataCharge += rootCharge
	roots := recoverableRootsForBasisInto(make([]RecoverableRoot, 0, rootCapacity), state, durable, coordinatorView)
	set.roots = roots
	oldest := state.CommitSeq
	for _, root := range roots {
		if root.CommitSeq != 0 && (oldest == 0 || root.CommitSeq < oldest) {
			oldest = root.CommitSeq
		}
	}
	for _, floor := range durable.dataFloor {
		if floor != 0 && (oldest == 0 || floor < oldest) {
			oldest = floor
		}
	}
	oldestRegistryID, _ := stable.idx.registry.RegisterWithHint(oldest, -1)
	systemRootEpoch := db.systemRootPublishEpoch.Load()
	db.rootReuseMu.RUnlock()

	set.oldestRegistryID, set.idx = oldestRegistryID, stable.idx
	if coordinator != nil {
		if err := coordinator.RevalidateReachability(coordinatorView.Epoch); err != nil {
			if errors.Is(err, rootpublication.ErrInvalidCandidate) {
				return nil, true, nil
			}
			return nil, false, publicRootPublicationErrorV1(err)
		}
		if coordinatorView.Visible.CommitSeq() != state.CommitSeq || coordinatorView.Visible.UserRootPageID() != state.RootPageID || coordinatorView.Visible.SystemRootPageID() != state.SystemRootPageID || coordinatorView.Visible.AppliedCommandLSN() != state.AppliedCommandLSN || coordinatorView.Visible.MaxEntryRevision() != uint64(state.MaxEntryRevision) || coordinatorView.Durable.CommitSeq() != durable.slotRecord[durable.slot].CommitSeq {
			return nil, true, nil
		}
	}
	current, ok := db.StateToken()
	if !ok || current != state || db.idx.Load() != stable.idx || db.systemRootPublishEpoch.Load() != systemRootEpoch {
		return nil, true, nil
	}
	db.durablePublishMu.Lock()
	currentMatches := durable.equal(db.recoverableDurableBasisLocked())
	db.durablePublishMu.Unlock()
	if !currentMatches {
		return nil, true, nil
	}

	if visibleResources != nil {
		root := RecoverableRoot{CommitSeq: state.CommitSeq, UserRootPageID: state.RootPageID, SystemRootPageID: state.SystemRootPageID, AppliedCommandLSN: state.AppliedCommandLSN, MaxEntryRevision: uint64(state.MaxEntryRevision)}
		key := recoverableRootIdentity(root)
		if _, ok := set.lookupRootResources(key); !ok {
			set.putRootResources(key, visibleResources)
		}
	}
	set.primaryArena, set.primaryRefs = primaryArena, primaryRefs
	set.visible, set.coordinator, set.coordinatorEpoch = state, coordinator, coordinatorView.Epoch
	set.idx, set.stableSnapshot, set.oldestRegistryID = stable.idx, stable, oldestRegistryID
	set.systemRootEpoch = systemRootEpoch
	set.rootResourceIndexID, set.rootResourceIndex = stable.idx.id, rootResourceIndex
	return set, false, nil
}

func (set *RecoverableRootSet) putRootResources(key recoverableRootKey, resources *rootpublication.StableResourceSet) {
	for i := range set.rootResources {
		record := &set.rootResources[i]
		if !record.present || record.key == key {
			*record = recoverableRootResource{key: key, resources: resources, present: true}
			return
		}
	}
	panic("recoverable root resource census exceeds four durable plus one visible")
}
func (set *RecoverableRootSet) removeRootResources(key recoverableRootKey) {
	for i := range set.rootResources {
		if set.rootResources[i].present && set.rootResources[i].key == key {
			set.rootResources[i] = recoverableRootResource{}
			return
		}
	}
}
func (set *RecoverableRootSet) lookupRootResources(key recoverableRootKey) (*rootpublication.StableResourceSet, bool) {
	for _, record := range set.rootResources {
		if record.present && record.key == key {
			return record.resources, true
		}
	}
	return nil, false
}

// CloneStableResourceForRoot selects one independently pinned resource from
// the exact five-scalar root identity captured by this set.
func (set *RecoverableRootSet) CloneStableResourceForRoot(root RecoverableRoot, selector rootpublication.StableResourceSelector) (*rootpublication.StableResourceSet, error) {
	if set == nil {
		return nil, ErrRecoverableRootSetStale
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if set.released.Load() {
		return nil, ErrRecoverableRootSetStale
	}
	resources, ok := set.lookupRootResources(recoverableRootIdentity(root))
	if !ok {
		return nil, ErrRecoverableRootSetStale
	}
	selected, err := rootpublication.CloneStableResourceForSelector(resources, selector)
	if err != nil {
		return nil, err
	}
	return selected, nil
}

func (set *RecoverableRootSet) resourcesForRoot(root RecoverableRoot) *rootpublication.StableResourceSet {
	resources, _, _, _ := set.resourcesForRootExact(root)
	return resources
}

func (set *RecoverableRootSet) resourcesForRootExact(root RecoverableRoot) (*rootpublication.StableResourceSet, uint64, rootpublication.StableIdentity, bool) {
	if set == nil || set.released.Load() {
		return nil, 0, rootpublication.StableIdentity{}, false
	}
	resources, ok := set.lookupRootResources(recoverableRootIdentity(root))
	return resources, set.rootResourceIndexID, set.rootResourceIndex, ok
}

func cloneRecoverableResourceUnion(sources ...*rootpublication.StableResourceSet) (*rootpublication.StableResourceSet, error) {
	filtered := sources[:0]
	for _, source := range sources {
		if source != nil {
			filtered = append(filtered, source)
		}
	}
	if len(filtered) == 0 {
		return nil, nil
	}
	return rootpublication.ClonePhysicalReachabilityUnion(filtered...)
}

func recoverableRootsForBasis(state StateToken, durable recoverableDurableBasis, coordinator rootpublication.ReachabilitySnapshot) []RecoverableRoot {
	return recoverableRootsForBasisInto(make([]RecoverableRoot, 0, 9+len(coordinator.Pending)+len(durable.ambiguous)), state, durable, coordinator)
}
func recoverableRootsForBasisInto(roots []RecoverableRoot, state StateToken, durable recoverableDurableBasis, coordinator rootpublication.ReachabilitySnapshot) []RecoverableRoot {
	add := func(root RecoverableRoot) {
		if root.CommitSeq == 0 || root.UserRootPageID == 0 || root.SystemRootPageID == 0 {
			return
		}
		for i := range roots {
			if roots[i].CommitSeq == root.CommitSeq &&
				roots[i].UserRootPageID == root.UserRootPageID &&
				roots[i].SystemRootPageID == root.SystemRootPageID &&
				roots[i].AppliedCommandLSN == root.AppliedCommandLSN &&
				roots[i].MaxEntryRevision == root.MaxEntryRevision {
				roots[i].Durable = roots[i].Durable || root.Durable
				roots[i].Visible = roots[i].Visible || root.Visible
				return
			}
		}
		roots = append(roots, root)
	}
	for slot, record := range durable.slotRecord {
		if durable.primaryReadRoots[slot] != 0 {
			record.UserRootPageID = durable.primaryReadRoots[slot]
		}
		add(RecoverableRoot{
			CommitSeq: record.CommitSeq, UserRootPageID: record.UserRootPageID, SystemRootPageID: record.SystemRootPageID,
			AppliedCommandLSN: record.AppliedCommandLSN, MaxEntryRevision: record.MaxEntryRevision, Durable: true,
		})
	}
	for slot, proof := range durable.primaryProof {
		record := proof.Record
		if durable.primaryReadRoots[slot+2] != 0 {
			record.UserRootPageID = durable.primaryReadRoots[slot+2]
		}
		add(RecoverableRoot{CommitSeq: record.CommitSeq, UserRootPageID: record.UserRootPageID, SystemRootPageID: record.SystemRootPageID, AppliedCommandLSN: record.AppliedCommandLSN, MaxEntryRevision: record.MaxEntryRevision, Durable: true})
	}
	if durable.pending != nil {
		next := durable.pending.next
		add(recoverableRootFromMeta(next, false, true))
	}
	for _, ambiguous := range durable.ambiguous {
		if ambiguous != nil {
			add(recoverableRootFromMeta(ambiguous.next, false, true))
		}
	}
	add(RecoverableRoot{
		CommitSeq: state.CommitSeq, UserRootPageID: state.RootPageID, SystemRootPageID: state.SystemRootPageID,
		AppliedCommandLSN: state.AppliedCommandLSN, MaxEntryRevision: uint64(state.MaxEntryRevision), Visible: true,
	})
	add(recoverableRootFromFrontier(coordinator.Visible, false, true))
	add(recoverableRootFromFrontier(coordinator.Durable, true, false))
	for _, pending := range coordinator.Pending {
		add(recoverableRootFromFrontier(pending, false, true))
	}
	return roots
}

func recoverableRootFromMeta(meta page.MetaPageBody, durable, visible bool) RecoverableRoot {
	return RecoverableRoot{
		CommitSeq: meta.CommitSeq, UserRootPageID: meta.UserRootPageID, SystemRootPageID: meta.SystemRootPageID,
		AppliedCommandLSN: meta.AppliedCommandLSN, MaxEntryRevision: meta.MaxEntryRevision,
		Durable: durable, Visible: visible,
	}
}

func recoverableRootFromFrontier(frontier rootpublication.Frontier, durable, visible bool) RecoverableRoot {
	return RecoverableRoot{
		CommitSeq: frontier.CommitSeq(), UserRootPageID: frontier.UserRootPageID(), SystemRootPageID: frontier.SystemRootPageID(),
		AppliedCommandLSN: frontier.AppliedCommandLSN(), MaxEntryRevision: frontier.MaxEntryRevision(),
		Durable: durable, Visible: visible,
	}
}

// Roots returns a copy of the captured recovery-selectable scalar roots.
func (set *RecoverableRootSet) Roots() []RecoverableRoot {
	if set == nil || set.released.Load() {
		return nil
	}
	return append([]RecoverableRoot(nil), set.roots...)
}

// RequiresReplayBefore reports whether at least one captured recovery root
// would have to replay through appliedLSN. Maintenance uses this bounded
// scalar proof to retain resources superseded by a command until every
// independently recoverable durable generation has crossed that command.
func (set *RecoverableRootSet) RequiresReplayBefore(appliedLSN uint64) bool {
	if set == nil || set.released.Load() || appliedLSN == 0 {
		return false
	}
	for _, root := range set.roots {
		if root.AppliedCommandLSN < appliedLSN {
			return true
		}
	}
	return false
}

func (set *RecoverableRootSet) leafGenerationIDsForFiles(fileIDs map[uint32]struct{}, current *leafGenerationView) (map[uint64]struct{}, error) {
	live := make(map[uint64]struct{})
	unresolved := make(map[uint32]struct{}, len(fileIDs))
	for fileID := range fileIDs {
		if fileID != 0 {
			unresolved[fileID] = struct{}{}
		}
	}
	consumeManifest := func(manifest *leafGenerationManifest) {
		if manifest == nil {
			return
		}
		for _, generation := range manifest.Generations {
			for _, fileID := range generation.FileIDs {
				if _, ok := fileIDs[fileID]; !ok {
					continue
				}
				live[generation.GenerationID] = struct{}{}
				delete(unresolved, fileID)
			}
		}
	}
	if current != nil {
		consumeManifest(current.sourceManifest)
	}
	seen := make(map[rootpublication.StableIdentity]struct{})
	for _, resources := range set.resources {
		if err := resources.WithScopedTokens(func(tokens []*rootpublication.StableResourceToken) error {
			for _, token := range tokens {
				if token == nil || token.Kind() != rootpublication.ResourceOuterLeafManifest {
					continue
				}
				identity := token.Identity()
				if _, ok := seen[identity]; ok {
					continue
				}
				seen[identity] = struct{}{}
				frontier := token.Frontier()
				if frontier.Bytes == 0 || frontier.Bytes > math.MaxInt64 {
					return fmt.Errorf("%w: recoverable outer-leaf manifest has invalid frontier", rootpublication.ErrFrontierBeyondResource)
				}
				var data []byte
				if err := token.WithPinnedFile(func(file *os.File) error {
					var readErr error
					data, readErr = io.ReadAll(io.NewSectionReader(file, 0, int64(frontier.Bytes)))
					return readErr
				}); err != nil {
					return err
				}
				manifest, err := decodeLeafGenerationManifest(data, token.ResourceID())
				if err != nil {
					return err
				}
				// A recycled file ID can legitimately map to different generations in
				// independently recoverable manifests. Preserve every matching
				// generation rather than collapsing by the scalar file ID.
				consumeManifest(manifest)
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if len(unresolved) != 0 {
		return nil, fmt.Errorf("treedb: recoverable leaf files have no retained generation manifest: %v", unresolved)
	}
	return live, nil
}

// AcquireSnapshotForRoot returns a snapshot rebound to one captured root. The
// capability's oldest-sequence registry pin prevents page reuse while this
// bounded traversal is in progress.
func (set *RecoverableRootSet) AcquireSnapshotForRoot(root RecoverableRoot) *Snapshot {
	return set.acquireSnapshotForRoot(root, false)
}

// acquireSnapshotForRootWithDurablePublishLockHeld retains the same exact root
// and fresh guards while the vacuum cutover gate already holds durablePublishMu.
func (set *RecoverableRootSet) acquireSnapshotForRootWithDurablePublishLockHeld(root RecoverableRoot) *Snapshot {
	return set.acquireSnapshotForRoot(root, true)
}

func (set *RecoverableRootSet) acquireSnapshotForRoot(root RecoverableRoot, durablePublishLockHeld bool) *Snapshot {
	if set == nil || set.released.Load() || set.db == nil || !set.containsRoot(root) {
		return nil
	}
	if err := set.revalidate(durablePublishLockHeld); err != nil {
		return nil
	}
	snapshot := set.db.AcquireSnapshot()
	if snapshot == nil {
		return nil
	}
	if snapshot.idx != set.idx {
		if closeErr := snapshot.Close(); closeErr != nil {
			set.db.retainOriginalCleanupError(closeErr)
		}
		return nil
	}
	state := cloneDBState(snapshot.state)
	if state == nil {
		if closeErr := snapshot.Close(); closeErr != nil {
			set.db.retainOriginalCleanupError(closeErr)
		}
		return nil
	}
	if snapshot.idx.primary != nil {
		ref, ready, e := snapshot.idx.primary.BorrowDirectory(root.UserRootPageID, nil)
		if e != nil || !ready {
			if closeErr := snapshot.Close(); closeErr != nil {
				set.db.retainOriginalCleanupError(closeErr)
			}
			return nil
		}
		// Rebinding uses preborn storage inside the SAME admitted Snapshot;
		// there is no extra heap wrapper or bank-owned allocation per capture.
		// set still retains this exact historical bank during producer binding.
		rebound := &snapshot.primaryRebindRoot
		*rebound = primaryStateRootV5{arena: snapshot.idx.primary, ref: ref}
		if e = bindPrimaryReadRootCertificateV6(rebound); e != nil {
			*rebound = primaryStateRootV5{}
			if closeErr := snapshot.Close(); closeErr != nil {
				set.db.retainOriginalCleanupError(closeErr)
			}
			return nil
		}
		ready, e = snapshot.idx.primary.Acquire(ref, nil)
		if e != nil || !ready {
			*rebound = primaryStateRootV5{}
			if closeErr := snapshot.Close(); closeErr != nil {
				set.db.retainOriginalCleanupError(closeErr)
			}
			return nil
		}
		if snapshot.primaryRoot != nil {
			consumed, dropErr := snapshot.primaryRoot.arena.Drop(snapshot.primaryRoot.ref, nil)
			if !consumed && dropErr == nil {
				dropErr = rootpublication.ErrResourceOwnership
			}
			if dropErr != nil {
				// Preserve the SAME original Snapshot and both operands after the
				// first actual refusal. Its finalizer must not replay that Drop.
				if consumed {
					snapshot.primaryRoot = rebound
				}
				snapshot.primaryRebindErr = dropErr
				if closeErr := snapshot.Close(); closeErr != nil {
					set.db.retainOriginalCleanupError(closeErr)
				}
				return nil
			}
		}
		snapshot.primaryRoot = rebound
		state.primaryRoot = rebound
	}
	state.CommitSeq = root.CommitSeq
	state.RootPageID = root.UserRootPageID
	state.SystemRootPageID = root.SystemRootPageID
	state.AppliedCommandLSN = root.AppliedCommandLSN
	state.MaxEntryRevision = page.EntryRevision(root.MaxEntryRevision)
	snapshot.state = state
	snapshot.tree.Reset(snapshot.idx.pager, &snapshot.reader, state.RootPageID)
	snapshot.treePager = snapshot.idx.pager
	snapshot.treeRoot = state.RootPageID
	return snapshot
}

func (set *RecoverableRootSet) containsRoot(root RecoverableRoot) bool {
	for _, candidate := range set.roots {
		if candidate.CommitSeq == root.CommitSeq &&
			candidate.UserRootPageID == root.UserRootPageID &&
			candidate.SystemRootPageID == root.SystemRootPageID &&
			candidate.AppliedCommandLSN == root.AppliedCommandLSN &&
			candidate.MaxEntryRevision == root.MaxEntryRevision {
			return true
		}
	}
	return false
}

// PinStableFile adds an exact physical deletion pin discovered while walking a
// captured root. Duplicate aliases of the same inode coalesce within the
// capability.
func (set *RecoverableRootSet) PinStableFile(file *os.File) (retErr error) {
	if set == nil || set.released.Load() || set.identityPinRegistry == nil {
		return ErrRecoverableRootSetStale
	}
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		return err
	}
	identity.Generation = 0
	set.mu.Lock()
	var cleanupRefusal error
	defer func() {
		db := set.db
		if cleanupRefusal != nil {
			set.failure.retainCause(0, cleanupRefusal)
			set.metadata.CleanupFailed()
		}
		set.mu.Unlock()
		if cleanupRefusal != nil {
			db.retainOriginalCleanupError(&set.failure)
			retErr = errors.Join(retErr, &set.failure)
		}
	}()
	if set.released.Load() {
		return ErrRecoverableRootSetStale
	}
	if owned, ok := set.identityPins.Lookup(identity); ok {
		return owned.cleanupErr
	}
	if err := set.identityPins.Set(identity, recoverableIdentityPin{identity: identity}); err != nil {
		return err
	}
	if err := set.identityPinRegistry.Observe(identity); err != nil {
		set.identityPins.Remove(identity)
		return err
	}
	pin, err := set.identityPinRegistry.Pin(identity)
	if err != nil {
		cleanupErr := set.identityPinRegistry.Unobserve(identity)
		if cleanupErr == nil {
			set.identityPins.Remove(identity)
		} else {
			cleanupRefusal = cleanupErr
			_ = set.identityPins.Set(identity, recoverableIdentityPin{identity: identity, observed: true, cleanupErr: cleanupErr})
		}
		return errors.Join(err, cleanupErr)
	}
	if err := set.identityPins.Set(identity, recoverableIdentityPin{identity: identity, pin: pin, observed: true}); err != nil {
		pin.Release()
		cleanupRefusal = set.identityPinRegistry.Unobserve(identity)
		return errors.Join(err, cleanupRefusal)
	}
	return nil
}

// Revalidate proves that the captured visible/durable root basis and exact
// coordinator debt are still current immediately before destructive mutation.
func (set *RecoverableRootSet) Revalidate() error {
	return set.revalidate(false)
}

// revalidateWithDurablePublishLockHeld is the cutover form of Revalidate.
// The caller must hold durablePublishMu, which closes the last candidate
// publication window without recursively acquiring the same mutex.
func (set *RecoverableRootSet) revalidateWithDurablePublishLockHeld() error {
	return set.revalidate(true)
}

func (set *RecoverableRootSet) revalidate(durablePublishLockHeld bool) error {
	if set == nil || set.released.Load() || set.db == nil {
		return ErrRecoverableRootSetStale
	}
	if set.coordinator != nil {
		if err := set.coordinator.RevalidateReachability(set.coordinatorEpoch); err != nil {
			return errors.Join(ErrRecoverableRootSetStale, publicRootPublicationErrorV1(err))
		}
	}
	state, ok := set.db.StateToken()
	if !ok || state != set.visible || set.db.idx.Load() != set.idx || set.db.systemRootPublishEpoch.Load() != set.systemRootEpoch {
		return ErrRecoverableRootSetStale
	}
	if !durablePublishLockHeld {
		set.db.durablePublishMu.Lock()
	}
	currentMatches := set.durable.equal(set.db.recoverableDurableBasisLocked())
	if !durablePublishLockHeld {
		set.db.durablePublishMu.Unlock()
	}
	if !currentMatches {
		return ErrRecoverableRootSetStale
	}
	return nil
}

// Release preserves the compatibility API and retains real refusal in DB custody.
func (set *RecoverableRootSet) Release() { _ = set.ReleaseWithError() }
func (set *RecoverableRootSet) ReleaseWithError() error {
	if set == nil {
		return nil
	}
	set.releaseOnce.Do(func() {
		set.released.Store(true)
		// Join the existing mutation latch before callbacks; no callback holds it.
		set.mu.Lock()
		set.mu.Unlock()
		var err error
		for i, ref := range set.primaryRefs {
			if ref.PageID == 0 {
				continue
			}
			consumed, e := set.primaryArena.Drop(ref, nil)
			if consumed {
				set.primaryRefs[i] = primaryarena.Ref{}
			} else if e == nil {
				e = rootpublication.ErrResourceOwnership
			}
			err = errors.Join(err, e)
		}
		for at := -1; ; {
			index, identity, owned, ok := set.identityPins.Next(at)
			if !ok {
				break
			}
			at = index
			owned.pin.Release()
			owned.pin = nil
			if owned.observed {
				e := owned.cleanupErr
				if e == nil {
					e = set.identityPinRegistry.Unobserve(identity)
				}
				err = errors.Join(err, e)
				if e == nil {
					owned.observed = false
				}
			}
			if !owned.observed {
				set.identityPins.Remove(identity)
			} else {
				_ = set.identityPins.Set(identity, owned)
			}
		}
		for i, resources := range set.resources {
			e := resources.ReleaseWithError()
			err = errors.Join(err, e)
			if e == nil {
				set.resources[i] = nil
			}
		}
		if set.oldestRegistryID != 0 && set.idx != nil {
			set.idx.registry.Unregister(set.oldestRegistryID)
			set.oldestRegistryID = 0
			set.idx = nil
		}
		if set.stableSnapshot != nil {
			e := set.stableSnapshot.Close()
			err = errors.Join(err, e)
			if e == nil {
				set.stableSnapshot = nil
			}
		}
		if err != nil {
			set.failure.retainCause(0, err)
			set.releaseErr = &set.failure
			set.metadata.CleanupFailed()
			set.db.retainOriginalCleanupError(set.releaseErr)
			set.custody.Complete(set, set.releaseErr)
			return
		}
		set.identityPins.Close()
		clear(set.roots)
		clear(set.durable.ambiguous)
		set.roots, set.resources, set.primaryRefs = nil, nil, nil
		set.durable = recoverableDurableBasis{}
		set.rootResources = [5]recoverableRootResource{}
		set.primaryArena, set.coordinator, set.identityPinRegistry, set.idx = nil, nil, nil, nil
		owner, charge, custody := set.metadata, set.metadataCharge, set.custody
		set.metadata, set.metadataCharge, set.custody, set.db = nil, 0, nil, nil
		custody.Complete(nil, nil)
		owner.RemovePending(charge)
	})
	return set.releaseErr
}
