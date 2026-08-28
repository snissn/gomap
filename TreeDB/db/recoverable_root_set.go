package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"

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
	slot       uint64
	slotCommit [2]uint64
	slotRecord [2]rootpublication.DurableRootRecordV1
	pending    *durableRootPublishCandidateV1
	ambiguous  []*durableRootPublishCandidateV1
}

func (basis recoverableDurableBasis) equal(other recoverableDurableBasis) bool {
	if basis.slot != other.slot || basis.slotCommit != other.slotCommit || basis.slotRecord != other.slotRecord || basis.pending != other.pending || len(basis.ambiguous) != len(other.ambiguous) {
		return false
	}
	for i := range basis.ambiguous {
		if basis.ambiguous[i] != other.ambiguous[i] {
			return false
		}
	}
	return true
}

type recoverableIdentityPin struct {
	identity rootpublication.StableIdentity
	pin      *rootpublication.IdentityPin
}

// RecoverableRootSet is an opaque, DB-minted maintenance capability. Its
// fields are deliberately private: callers can inspect copied roots, acquire
// root-bound snapshots, add exact physical pins discovered while traversing a
// root, revalidate, and release, but cannot manufacture authority.
type RecoverableRootSet struct {
	db *DB

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
	rootResources       map[recoverableRootKey]*rootpublication.StableResourceSet
	identityPinRegistry *rootpublication.IdentityPinRegistry

	mu           sync.Mutex
	identityPins map[rootpublication.StableIdentity]recoverableIdentityPin
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

const recoverableRootSetCaptureAttempts = 8

func (db *DB) captureRecoverableRootSetWithMaintenanceLockHeld(ctx context.Context) (*RecoverableRootSet, error) {
	return db.captureRecoverableRootSetWithMaintenanceLockHeldMode(ctx, true)
}

// captureRecoverableRootSetForInspectionWithMaintenanceLockHeld captures the
// same recovery closure for read-only audit paths. Unlike the destructive
// capability path, it remains available on read-only or recovery-required
// handles because callers may only inspect the captured roots and resources.
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
	}
	stable := db.acquireStableSnapshotWithMaintenanceLockHeld()
	if stable == nil || stable.idx == nil || stable.idx.registry == nil {
		if stable != nil {
			_ = stable.Close()
		}
		return nil, errors.New("treedb: capture recoverable root set stable index")
	}

	for attempt := 0; attempt < recoverableRootSetCaptureAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			_ = stable.Close()
			return nil, err
		}
		capability, retry, err := db.tryCaptureRecoverableRootSet(stable)
		if err != nil {
			_ = stable.Close()
			return nil, err
		}
		if !retry {
			return capability, nil
		}
	}
	_ = stable.Close()
	return nil, fmt.Errorf("%w: capture did not converge", ErrRecoverableRootSetStale)
}

func (db *DB) tryCaptureRecoverableRootSet(stable *Snapshot) (*RecoverableRootSet, bool, error) {
	var coordinator *rootpublication.Coordinator
	if db.rootPublication != nil {
		coordinator = db.rootPublication.coordinator
	}
	var coordinatorView rootpublication.ReachabilitySnapshot
	var err error
	if coordinator != nil {
		coordinatorView, err = coordinator.CaptureReachability()
		if err != nil {
			return nil, false, publicRootPublicationErrorV1(err)
		}
	}
	releaseCoordinator := true
	defer func() {
		if releaseCoordinator {
			coordinatorView.Release()
		}
	}()

	var visibleResources *rootpublication.StableResourceSet
	if db.rootPublication != nil {
		visibleResources, err = db.rootPublication.cloneVisibleResources()
		if err != nil {
			return nil, false, err
		}
	}
	releaseVisible := true
	defer func() {
		if releaseVisible {
			visibleResources.Release()
		}
	}()

	db.durablePublishMu.Lock()
	db.rootReuseMu.RLock()
	durable := recoverableDurableBasis{
		slot: db.durableRoot.slot, slotCommit: db.durableRoot.slotCommit,
		slotRecord: db.durableRoot.slotRecord, pending: db.durableRoot.pending,
		ambiguous: append([]*durableRootPublishCandidateV1(nil), db.durableRoot.ambiguous...),
	}
	sources := make([]*rootpublication.StableResourceSet, 0, 5+len(db.durableRoot.ambiguous))
	sources = append(sources, db.durableRoot.slotResources[:]...)
	if pending := db.durableRoot.pending; pending != nil {
		sources = append(sources, pending.resources)
	}
	for _, ambiguous := range db.durableRoot.ambiguous {
		if ambiguous != nil {
			sources = append(sources, ambiguous.resources)
		}
	}
	durableResources, cloneErr := cloneRecoverableResourceUnion(sources...)
	rootResources := make(map[recoverableRootKey]*rootpublication.StableResourceSet, 3)
	rootResourceSets := make([]*rootpublication.StableResourceSet, 0, 2)
	if cloneErr == nil {
		for slot, record := range db.durableRoot.slotRecord {
			if record.CommitSeq == 0 {
				continue
			}
			resources, resourceErr := rootpublication.CloneStableResourceSetExcludingKinds(db.durableRoot.slotResources[slot])
			if resourceErr != nil {
				cloneErr = resourceErr
				break
			}
			root := RecoverableRoot{
				CommitSeq: record.CommitSeq, UserRootPageID: record.UserRootPageID,
				SystemRootPageID: record.SystemRootPageID, AppliedCommandLSN: record.AppliedCommandLSN,
				MaxEntryRevision: record.MaxEntryRevision,
			}
			rootResources[recoverableRootIdentity(root)] = resources
			if resources != nil {
				rootResourceSets = append(rootResourceSets, resources)
			}
		}
	}
	db.durablePublishMu.Unlock()
	if cloneErr != nil {
		db.rootReuseMu.RUnlock()
		durableResources.Release()
		for _, resources := range rootResourceSets {
			resources.Release()
		}
		return nil, false, cloneErr
	}

	state, ok := db.StateToken()
	if !ok || stable.idx != db.idx.Load() {
		db.rootReuseMu.RUnlock()
		durableResources.Release()
		for _, resources := range rootResourceSets {
			resources.Release()
		}
		return nil, true, nil
	}
	roots := recoverableRootsForBasis(state, durable, coordinatorView)
	oldest := state.CommitSeq
	for _, root := range roots {
		if root.CommitSeq != 0 && (oldest == 0 || root.CommitSeq < oldest) {
			oldest = root.CommitSeq
		}
	}
	oldestRegistryID, _ := stable.idx.registry.RegisterWithHint(oldest, -1)
	systemRootEpoch := db.systemRootPublishEpoch.Load()
	db.rootReuseMu.RUnlock()

	cleanupRetry := func() {
		if oldestRegistryID != 0 {
			stable.idx.registry.Unregister(oldestRegistryID)
		}
		durableResources.Release()
		for _, resources := range rootResourceSets {
			resources.Release()
		}
	}
	if coordinator != nil {
		if err := coordinator.RevalidateReachability(coordinatorView.Epoch); err != nil {
			cleanupRetry()
			if errors.Is(err, rootpublication.ErrInvalidCandidate) {
				return nil, true, nil
			}
			return nil, false, publicRootPublicationErrorV1(err)
		}
		if coordinatorView.Visible.CommitSeq() != state.CommitSeq || coordinatorView.Visible.UserRootPageID() != state.RootPageID || coordinatorView.Visible.SystemRootPageID() != state.SystemRootPageID || coordinatorView.Visible.AppliedCommandLSN() != state.AppliedCommandLSN || coordinatorView.Visible.MaxEntryRevision() != uint64(state.MaxEntryRevision) || coordinatorView.Durable.CommitSeq() != durable.slotRecord[durable.slot].CommitSeq {
			cleanupRetry()
			return nil, true, nil
		}
	}
	current, ok := db.StateToken()
	if !ok || current != state || db.idx.Load() != stable.idx || db.systemRootPublishEpoch.Load() != systemRootEpoch {
		cleanupRetry()
		return nil, true, nil
	}
	db.durablePublishMu.Lock()
	currentDurable := recoverableDurableBasis{
		slot: db.durableRoot.slot, slotCommit: db.durableRoot.slotCommit,
		slotRecord: db.durableRoot.slotRecord, pending: db.durableRoot.pending,
		ambiguous: append([]*durableRootPublishCandidateV1(nil), db.durableRoot.ambiguous...),
	}
	db.durablePublishMu.Unlock()
	if !durable.equal(currentDurable) {
		cleanupRetry()
		return nil, true, nil
	}

	resources := make([]*rootpublication.StableResourceSet, 0, 3+len(rootResourceSets))
	if durableResources != nil {
		resources = append(resources, durableResources)
	}
	if coordinatorView.Resources != nil {
		resources = append(resources, coordinatorView.Resources)
		coordinatorView.Resources = nil
	}
	if visibleResources != nil {
		resources = append(resources, visibleResources)
		visibleRoot := RecoverableRoot{
			CommitSeq: state.CommitSeq, UserRootPageID: state.RootPageID,
			SystemRootPageID: state.SystemRootPageID, AppliedCommandLSN: state.AppliedCommandLSN,
			MaxEntryRevision: uint64(state.MaxEntryRevision),
		}
		key := recoverableRootIdentity(visibleRoot)
		if _, exists := rootResources[key]; !exists {
			rootResources[key] = visibleResources
		}
		visibleResources = nil
	}
	resources = append(resources, rootResourceSets...)
	releaseCoordinator = false
	releaseVisible = false
	return &RecoverableRootSet{
		db: db, roots: roots, visible: state, durable: durable,
		coordinator: coordinator, coordinatorEpoch: coordinatorView.Epoch,
		idx: stable.idx, stableSnapshot: stable, oldestRegistryID: oldestRegistryID,
		systemRootEpoch: systemRootEpoch, resources: resources, rootResources: rootResources,
		identityPinRegistry: db.StableResourceIdentityPinRegistry(),
		identityPins:        make(map[rootpublication.StableIdentity]recoverableIdentityPin),
	}, false, nil
}

func (set *RecoverableRootSet) resourcesForRoot(root RecoverableRoot) *rootpublication.StableResourceSet {
	resources, _ := set.resourcesForRootExact(root)
	return resources
}

func (set *RecoverableRootSet) resourcesForRootExact(root RecoverableRoot) (*rootpublication.StableResourceSet, bool) {
	if set == nil || set.released.Load() {
		return nil, false
	}
	resources, ok := set.rootResources[recoverableRootIdentity(root)]
	return resources, ok
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
	view, err := rootpublication.UnionStableResourceSets(filtered...)
	if err != nil {
		return nil, err
	}
	return rootpublication.CloneStableResourceSetExcludingKinds(view)
}

func recoverableRootsForBasis(state StateToken, durable recoverableDurableBasis, coordinator rootpublication.ReachabilitySnapshot) []RecoverableRoot {
	roots := make([]RecoverableRoot, 0, 6+len(coordinator.Pending)+len(durable.ambiguous))
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
	for _, record := range durable.slotRecord {
		add(RecoverableRoot{
			CommitSeq: record.CommitSeq, UserRootPageID: record.UserRootPageID, SystemRootPageID: record.SystemRootPageID,
			AppliedCommandLSN: record.AppliedCommandLSN, MaxEntryRevision: record.MaxEntryRevision, Durable: true,
		})
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
		for _, token := range resources.Tokens() {
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
				return nil, fmt.Errorf("%w: recoverable outer-leaf manifest has invalid frontier", rootpublication.ErrFrontierBeyondResource)
			}
			var data []byte
			if err := token.WithPinnedFile(func(file *os.File) error {
				var readErr error
				data, readErr = io.ReadAll(io.NewSectionReader(file, 0, int64(frontier.Bytes)))
				return readErr
			}); err != nil {
				return nil, err
			}
			manifest, err := decodeLeafGenerationManifest(data, token.ResourceID())
			if err != nil {
				return nil, err
			}
			// A recycled file ID can legitimately map to different generations in
			// independently recoverable manifests. Preserve every matching
			// generation rather than collapsing by the scalar file ID.
			consumeManifest(manifest)
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
	if set == nil || set.released.Load() || set.db == nil || !set.containsRoot(root) {
		return nil
	}
	if err := set.Revalidate(); err != nil {
		return nil
	}
	snapshot := set.db.AcquireSnapshot()
	if snapshot == nil {
		return nil
	}
	if snapshot.idx != set.idx {
		_ = snapshot.Close()
		return nil
	}
	state := cloneDBState(snapshot.state)
	if state == nil {
		_ = snapshot.Close()
		return nil
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
func (set *RecoverableRootSet) PinStableFile(file *os.File) error {
	if set == nil || set.released.Load() || set.identityPinRegistry == nil {
		return ErrRecoverableRootSetStale
	}
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		return err
	}
	identity.Generation = 0
	set.mu.Lock()
	defer set.mu.Unlock()
	if set.released.Load() {
		return ErrRecoverableRootSetStale
	}
	if _, ok := set.identityPins[identity]; ok {
		return nil
	}
	if err := set.identityPinRegistry.Observe(identity); err != nil {
		return err
	}
	pin, err := set.identityPinRegistry.Pin(identity)
	if err != nil {
		_ = set.identityPinRegistry.Unobserve(identity)
		return err
	}
	set.identityPins[identity] = recoverableIdentityPin{identity: identity, pin: pin}
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
	current := recoverableDurableBasis{
		slot: set.db.durableRoot.slot, slotCommit: set.db.durableRoot.slotCommit,
		slotRecord: set.db.durableRoot.slotRecord, pending: set.db.durableRoot.pending,
		ambiguous: append([]*durableRootPublishCandidateV1(nil), set.db.durableRoot.ambiguous...),
	}
	if !durablePublishLockHeld {
		set.db.durablePublishMu.Unlock()
	}
	if !set.durable.equal(current) {
		return ErrRecoverableRootSetStale
	}
	return nil
}

// Release drops every stable resource, exact identity, sequence, and index pin
// owned by the capability. It is safe to call repeatedly.
func (set *RecoverableRootSet) Release() {
	if set == nil || !set.released.CompareAndSwap(false, true) {
		return
	}
	set.mu.Lock()
	pins := make([]recoverableIdentityPin, 0, len(set.identityPins))
	for _, pin := range set.identityPins {
		pins = append(pins, pin)
	}
	set.identityPins = nil
	set.mu.Unlock()
	for _, owned := range pins {
		owned.pin.Release()
		_ = set.identityPinRegistry.Unobserve(owned.identity)
	}
	for _, resources := range set.resources {
		resources.Release()
	}
	set.resources = nil
	if set.oldestRegistryID != 0 && set.idx != nil && set.idx.registry != nil {
		set.idx.registry.Unregister(set.oldestRegistryID)
		set.oldestRegistryID = 0
	}
	if set.stableSnapshot != nil {
		_ = set.stableSnapshot.Close()
		set.stableSnapshot = nil
	}
}
