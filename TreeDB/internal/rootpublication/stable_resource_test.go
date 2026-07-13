package rootpublication

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

type testStableHandle struct {
	mu               sync.Mutex
	identity         StableIdentity
	length           uint64
	flushes          []uint64
	syncs            []uint64
	pins             int
	releases         int
	unlinked         bool
	identityAfterPin StableIdentity
}

func newTestStableHandle(identity byte, length uint64) *testStableHandle {
	return &testStableHandle{identity: StableIdentity{Device: 1, File: uint64(identity)}, length: length}
}

func (h *testStableHandle) StableIdentity() (StableIdentity, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.pins != 0 && h.identityAfterPin.valid() {
		return h.identityAfterPin, nil
	}
	return h.identity, nil
}
func (h *testStableHandle) StableLength() (uint64, error) { return h.length, nil }
func (h *testStableHandle) FlushThrough(frontier uint64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.flushes = append(h.flushes, frontier)
	return nil
}
func (h *testStableHandle) SyncThrough(frontier uint64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.syncs = append(h.syncs, frontier)
	return nil
}
func (h *testStableHandle) Pin() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unlinked {
		return errors.New("identity already unlinked")
	}
	h.pins++
	return nil
}
func (h *testStableHandle) Release() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.releases++
	h.pins--
	return nil
}
func (h *testStableHandle) TryUnlink() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.pins != 0 {
		return ErrResourcePinned
	}
	h.unlinked = true
	return nil
}

type testNamespaceHandle struct {
	mu         sync.Mutex
	identity   StableIdentity
	generation uint64
	validates  int
	syncs      int
	pins       int
	releases   int
	supported  bool
	syncErr    error
}

func (h *testNamespaceHandle) StableIdentity() (StableIdentity, error) { return h.identity, nil }
func (h *testNamespaceHandle) StableGeneration() (uint64, error)       { return h.generation, nil }
func (h *testNamespaceHandle) ValidateNamespacePersistence() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.validates++
	if !h.supported {
		return ErrNamespacePersistenceUnsupported
	}
	return nil
}
func (h *testNamespaceHandle) SyncNamespace() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.supported {
		return ErrNamespacePersistenceUnsupported
	}
	h.syncs++
	if h.syncErr != nil {
		return h.syncErr
	}
	return nil
}
func (h *testNamespaceHandle) Pin() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pins++
	return nil
}
func (h *testNamespaceHandle) Release() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pins--
	h.releases++
	return nil
}

func testResourceToken(t testing.TB, handle StableResourceHandle, kind ResourceKind, generation, frontier uint64, digest byte, namespace *StableNamespaceToken) *StableResourceToken {
	return testResourceTokenForField(t, handle, kind, generation, frontier, digest, namespace, "meta.user_root_page_id")
}

func testResourceTokenForField(t testing.TB, handle StableResourceHandle, kind ResourceKind, generation, frontier uint64, digest byte, namespace *StableNamespaceToken, reachabilityField string) *StableResourceToken {
	t.Helper()
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: kind, LogicalNamespace: "primary", ResourceID: fmt.Sprintf("%d", generation),
		DiagnosticPath: "lane/current.data", Generation: generation, Handle: handle,
		RequiredFrontier: frontier, Digest: []byte{digest}, Namespace: namespace,
		ReachabilityField: reachabilityField,
	})
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func TestStableResourceTokenPinsIdentityInsteadOfReopeningDiagnosticPath(t *testing.T) {
	original := newTestStableHandle(1, 128)
	recreated := newTestStableHandle(2, 128)
	token := testResourceToken(t, original, ResourceValueLogSegment, 7, 96, 1, nil)

	// The path now names a different file. The token remains bound to the open
	// identity captured at registration. There is deliberately no path resolver
	// in StableResourceSpec or StableResourceToken.
	if err := token.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if got := token.Identity(); got != original.identity || got == recreated.identity {
		t.Fatalf("token identity=%+v original=%+v recreated=%+v", got, original.identity, recreated.identity)
	}
	if len(original.syncs) != 1 || len(recreated.syncs) != 0 {
		t.Fatalf("syncs original=%v recreated=%v", original.syncs, recreated.syncs)
	}
}

func TestStableNamespaceTokenSeparatesFileDataFromNameDurability(t *testing.T) {
	dir := &testNamespaceHandle{identity: StableIdentity{Device: 1, File: 90}, generation: 3, supported: true}
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Operation: NamespaceCreate, ParentDiagnosticPath: "vlog", ParentGeneration: 3, Parent: dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	if dir.validates != 1 || dir.syncs != 0 {
		t.Fatalf("namespace registration validates=%d syncs=%d", dir.validates, dir.syncs)
	}
	file := newTestStableHandle(3, 64)
	token := testResourceToken(t, file, ResourceValueLogSegment, 8, 64, 2, namespace)
	if err := token.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if dir.syncs != 0 || token.NamespaceStable() {
		t.Fatal("file sync incorrectly certified the new name")
	}
	if err := token.SyncNamespace(); err != nil {
		t.Fatal(err)
	}
	if dir.syncs != 1 || !token.NamespaceStable() {
		t.Fatalf("namespace syncs=%d stable=%t", dir.syncs, token.NamespaceStable())
	}
}

func TestStableResourceSetRetainsRotatedLaneObligation(t *testing.T) {
	oldLane := newTestStableHandle(4, 100)
	newLane := newTestStableHandle(5, 20)
	oldToken := testResourceToken(t, oldLane, ResourceOuterLeafSegment, 11, 100, 3, nil)
	newToken := testResourceToken(t, newLane, ResourceOuterLeafSegment, 12, 20, 4, nil)
	set, err := NewStableResourceSet(oldToken, newToken)
	if err != nil {
		t.Fatal(err)
	}
	if err := set.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if len(oldLane.syncs) != 1 || len(newLane.syncs) != 1 {
		t.Fatalf("rotated/current syncs=%v/%v", oldLane.syncs, newLane.syncs)
	}
}

func TestStableResourceSetRejectsMissingNestedAsset(t *testing.T) {
	present := testResourceTokenForField(t, newTestStableHandle(6, 10), ResourceVectorGraphPack, 1, 10, 5, nil, "meta.vector_catalog_id")
	set, err := NewStableResourceSet(present)
	if err != nil {
		t.Fatal(err)
	}
	if err := set.ValidateReachabilityFields([]string{"meta.vector_catalog_id", "meta.vector_graph_pack_id"}); !errors.Is(err, ErrMissingResourceDependency) {
		t.Fatalf("validate missing nested dependency=%v", err)
	}
}

func TestStableResourceSetCoalescesSameIdentityToGreatestFrontier(t *testing.T) {
	handle := newTestStableHandle(9, 128)
	low := testResourceToken(t, handle, ResourceValueLogSegment, 4, 32, 1, nil)
	high := testResourceToken(t, handle, ResourceValueLogSegment, 4, 96, 1, nil)
	set, err := NewStableResourceSet(low, high)
	if err != nil {
		t.Fatal(err)
	}
	entries := set.Tokens()
	if len(entries) != 1 || entries[0].RequiredFrontier() != 96 {
		t.Fatalf("coalesced tokens=%v", entries)
	}
	if err := set.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if got := handle.syncs; len(got) != 1 || got[0] != 96 {
		t.Fatalf("sync frontiers=%v", got)
	}
}

func TestStableResourceSetTransferAndSupersessionReleaseEveryPinExactlyOnce(t *testing.T) {
	oldHandle := newTestStableHandle(10, 64)
	newHandle := newTestStableHandle(11, 64)
	oldSet, err := NewStableResourceSet(testResourceToken(t, oldHandle, ResourceOuterLeafSegment, 1, 64, 1, nil))
	if err != nil {
		t.Fatal(err)
	}
	newSet, err := NewStableResourceSet(testResourceToken(t, newHandle, ResourceOuterLeafSegment, 2, 64, 2, nil))
	if err != nil {
		t.Fatal(err)
	}
	coalesced, err := TransferUnionStableResourceSets(oldSet, newSet)
	if err != nil {
		t.Fatal(err)
	}
	// Superseded owners are inert after transfer; only the union releases pins.
	if err := oldSet.Release(); err != nil {
		t.Fatal(err)
	}
	if err := newSet.Release(); err != nil {
		t.Fatal(err)
	}
	if oldHandle.releases != 0 || newHandle.releases != 0 {
		t.Fatalf("transferred source released pins old/new=%d/%d", oldHandle.releases, newHandle.releases)
	}
	if err := coalesced.Release(); err != nil {
		t.Fatal(err)
	}
	if err := coalesced.Release(); err != nil {
		t.Fatal(err)
	}
	if oldHandle.releases != 1 || newHandle.releases != 1 || oldHandle.pins != 0 || newHandle.pins != 0 {
		t.Fatalf("release/pins old=%d/%d new=%d/%d", oldHandle.releases, oldHandle.pins, newHandle.releases, newHandle.pins)
	}
}

func TestStableResourceTokenRejectsFrontierAndSetRejectsConflicts(t *testing.T) {
	handle := newTestStableHandle(7, 32)
	if _, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLogSegment, LogicalNamespace: "primary", ResourceID: "7",
		Generation: 7, Handle: handle, RequiredFrontier: 33, ReachabilityField: "meta.value_log_id",
	}); !errors.Is(err, ErrResourceFrontierBeyondLength) {
		t.Fatalf("frontier beyond length=%v", err)
	}
	a := testResourceToken(t, handle, ResourceDictionaryGeneration, 7, 32, 1, nil)
	b := testResourceToken(t, handle, ResourceDictionaryGeneration, 7, 32, 2, nil)
	if _, err := NewStableResourceSet(a, b); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("digest conflict=%v", err)
	}
}

func TestStableResourceTokenUnpinsWhenIdentityChangesDuringRegistration(t *testing.T) {
	handle := newTestStableHandle(12, 32)
	handle.identityAfterPin = StableIdentity{Device: 1, File: 13}
	if _, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLogSegment, LogicalNamespace: "primary", ResourceID: "12",
		Generation: 12, Handle: handle, RequiredFrontier: 32, ReachabilityField: "meta.value_log_id",
	}); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("identity changed during registration=%v", err)
	}
	if handle.pins != 0 || handle.releases != 1 {
		t.Fatalf("failed registration pins=%d releases=%d", handle.pins, handle.releases)
	}
}

func TestImmutableAssetRequiresDigest(t *testing.T) {
	handle := newTestStableHandle(14, 32)
	if _, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceVectorGraphPack, LogicalNamespace: "vectors", ResourceID: "14",
		Generation: 14, Handle: handle, RequiredFrontier: 32, ReachabilityField: "meta.vector_graph_pack_id",
	}); !errors.Is(err, ErrInvalidStableResource) {
		t.Fatalf("immutable asset without digest=%v", err)
	}
	if handle.pins != 0 {
		t.Fatalf("invalid immutable asset acquired pin=%d", handle.pins)
	}
}

func TestCoordinatorStableResourceOwnershipLifecycle(t *testing.T) {
	t.Run("success releases covered originals", func(t *testing.T) {
		handle := newTestStableHandle(15, 32)
		set, err := NewStableResourceSet(testResourceToken(t, handle, ResourceValueLogSegment, 1, 32, 1, nil))
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := NewPreparedRootCandidateWithResources(CandidateSpec{Frontier: NewFrontier(1, 1, 1, 1, 1)}, set)
		if err != nil {
			t.Fatal(err)
		}
		coordinator, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
			return PublishResult{Outcome: PublishSucceeded}
		})})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Enqueue(context.Background(), candidate); err != nil {
			t.Fatal(err)
		}
		if err := coordinator.WaitThrough(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
		if handle.releases != 1 || handle.pins != 0 {
			t.Fatalf("success releases=%d pins=%d", handle.releases, handle.pins)
		}
		stopClean(t, coordinator)
	})

	t.Run("retry retains and stop safely abandons", func(t *testing.T) {
		handle := newTestStableHandle(16, 32)
		set, err := NewStableResourceSet(testResourceToken(t, handle, ResourceValueLogSegment, 1, 32, 1, nil))
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := NewPreparedRootCandidateWithResources(CandidateSpec{Frontier: NewFrontier(1, 1, 1, 1, 1)}, set)
		if err != nil {
			t.Fatal(err)
		}
		coordinator, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
			return PublishResult{Outcome: PublishRetryableFailure, Err: errors.New("injected")}
		})})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Enqueue(context.Background(), candidate); err != nil {
			t.Fatal(err)
		}
		if err := coordinator.WaitThrough(context.Background(), 1); err == nil {
			t.Fatal("retry unexpectedly succeeded")
		}
		if handle.releases != 0 || handle.pins != 1 {
			t.Fatalf("retry releases=%d pins=%d", handle.releases, handle.pins)
		}
		if err := coordinator.Stop(context.Background()); !errors.Is(err, ErrPublicationStopped) {
			t.Fatalf("stop=%v", err)
		}
		if handle.releases != 1 || handle.pins != 0 {
			t.Fatalf("stop abandonment releases=%d pins=%d", handle.releases, handle.pins)
		}
	})

	t.Run("multi candidate retry borrows then success releases originals", func(t *testing.T) {
		firstHandle := newTestStableHandle(21, 32)
		secondHandle := newTestStableHandle(22, 32)
		firstSet, err := NewStableResourceSet(testResourceToken(t, firstHandle, ResourceValueLogSegment, 1, 32, 1, nil))
		if err != nil {
			t.Fatal(err)
		}
		secondSet, err := NewStableResourceSet(testResourceToken(t, secondHandle, ResourceValueLogSegment, 2, 32, 1, nil))
		if err != nil {
			t.Fatal(err)
		}
		first, err := NewPreparedRootCandidateWithResources(CandidateSpec{Frontier: NewFrontier(1, 1, 1, 1, 1)}, firstSet)
		if err != nil {
			t.Fatal(err)
		}
		second, err := NewPreparedRootCandidateWithResources(CandidateSpec{Frontier: NewFrontier(2, 2, 2, 2, 2)}, secondSet)
		if err != nil {
			t.Fatal(err)
		}
		var calls atomic.Int32
		coordinator, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
			if calls.Add(1) == 1 {
				return PublishResult{Outcome: PublishRetryableFailure, Err: errors.New("injected")}
			}
			return PublishResult{Outcome: PublishSucceeded}
		})})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Enqueue(context.Background(), first); err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Enqueue(context.Background(), second); err != nil {
			t.Fatal(err)
		}
		if err := coordinator.WaitThrough(context.Background(), 2); err == nil {
			t.Fatal("first grouped attempt unexpectedly succeeded")
		}
		if firstHandle.pins != 1 || secondHandle.pins != 1 || firstHandle.releases != 0 || secondHandle.releases != 0 {
			t.Fatalf("retry ownership first=%d/%d second=%d/%d", firstHandle.pins, firstHandle.releases, secondHandle.pins, secondHandle.releases)
		}
		if err := coordinator.WaitThrough(context.Background(), 2); err != nil {
			t.Fatal(err)
		}
		if firstHandle.pins != 0 || secondHandle.pins != 0 || firstHandle.releases != 1 || secondHandle.releases != 1 {
			t.Fatalf("success ownership first=%d/%d second=%d/%d", firstHandle.pins, firstHandle.releases, secondHandle.pins, secondHandle.releases)
		}
		stopClean(t, coordinator)
	})
}

func TestStableResourcePinBlocksConcurrentUnlinkAndReleasesExactlyOnce(t *testing.T) {
	handle := newTestStableHandle(8, 32)
	token := testResourceToken(t, handle, ResourceTypedColumnValueAsset, 1, 32, 1, nil)
	if err := handle.TryUnlink(); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("unlink pinned identity=%v", err)
	}
	if err := token.Release(); err != nil {
		t.Fatal(err)
	}
	if err := token.Release(); err != nil {
		t.Fatal(err)
	}
	if handle.releases != 1 || handle.pins != 0 {
		t.Fatalf("releases=%d pins=%d", handle.releases, handle.pins)
	}
	if err := handle.TryUnlink(); err != nil {
		t.Fatalf("unlink after release=%v", err)
	}
}

func TestUnsupportedNamespacePersistenceFailsClosed(t *testing.T) {
	dir := &testNamespaceHandle{identity: StableIdentity{Device: 1, File: 99}, generation: 4, supported: false}
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Operation: NamespaceRename, ParentDiagnosticPath: "assets", ParentGeneration: 4, Parent: dir,
	})
	if !errors.Is(err, ErrNamespacePersistenceUnsupported) || namespace != nil {
		t.Fatalf("unsupported namespace token=(%v,%v)", namespace, err)
	}
}

func TestStableNamespaceSyncSerializesConcurrentCallers(t *testing.T) {
	dir := &testNamespaceHandle{identity: StableIdentity{Device: 1, File: 100}, generation: 5, supported: true}
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Operation: NamespaceCreate, ParentDiagnosticPath: "assets", ParentGeneration: 5, Parent: dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- namespace.Sync()
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if dir.syncs != 1 {
		t.Fatalf("concurrent namespace sync calls=%d want 1", dir.syncs)
	}
	if err := namespace.Release(); err != nil {
		t.Fatal(err)
	}
	if dir.pins != 0 || dir.releases != 1 {
		t.Fatalf("namespace pins=%d releases=%d", dir.pins, dir.releases)
	}
}

func TestStableResourceSetFailedConstructionLeavesUnconsumedTokenOwned(t *testing.T) {
	firstHandle := newTestStableHandle(17, 32)
	first := testResourceToken(t, firstHandle, ResourceValueLogSegment, 1, 32, 1, nil)
	owned, err := NewStableResourceSet(first)
	if err != nil {
		t.Fatal(err)
	}
	secondHandle := newTestStableHandle(18, 32)
	second := testResourceToken(t, secondHandle, ResourceValueLogSegment, 2, 32, 1, nil)
	if _, err := NewStableResourceSet(first, second); !errors.Is(err, ErrStableResourceOwnershipTransferred) {
		t.Fatalf("partial transfer construction=%v", err)
	}
	if err := second.Release(); err != nil {
		t.Fatal(err)
	}
	if secondHandle.releases != 1 || secondHandle.pins != 0 {
		t.Fatalf("unconsumed token releases=%d pins=%d", secondHandle.releases, secondHandle.pins)
	}
	if err := owned.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestTransferUnionDeduplicatesRepeatedSourceSet(t *testing.T) {
	handle := newTestStableHandle(19, 32)
	set, err := NewStableResourceSet(testResourceToken(t, handle, ResourceValueLogSegment, 1, 32, 1, nil))
	if err != nil {
		t.Fatal(err)
	}
	union, err := TransferUnionStableResourceSets(set, set)
	if err != nil {
		t.Fatal(err)
	}
	if err := union.Release(); err != nil {
		t.Fatal(err)
	}
	if handle.releases != 1 || handle.pins != 0 {
		t.Fatalf("duplicate transfer releases=%d pins=%d", handle.releases, handle.pins)
	}
}

func TestStableResourceKindIsClosedAndInventoryChecked(t *testing.T) {
	handle := newTestStableHandle(20, 32)
	if _, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceKind("future_unregistered_kind"), LogicalNamespace: "future", ResourceID: "1",
		Generation: 1, Handle: handle, RequiredFrontier: 32, Digest: []byte{1}, ReachabilityField: "meta.future",
	}); !errors.Is(err, ErrInvalidStableResource) {
		t.Fatalf("unregistered kind=%v", err)
	}
	if handle.pins != 0 {
		t.Fatalf("unregistered kind acquired pins=%d", handle.pins)
	}
}
