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
	onSync           func(uint64)
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
	if h.onSync != nil {
		h.onSync(frontier)
	}
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
	onSync     func()
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
	if h.onSync != nil {
		h.onSync()
	}
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
	present := testResourceTokenForField(t, newTestStableHandle(6, 10), ResourceOuterLeafManifest, 1, 10, 5, nil, "meta.vector_catalog_id")
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

func TestStableResourceSetCoalescesGenericPhysicalSegmentAcrossAuthorityFields(t *testing.T) {
	handle := newTestStableHandle(27, 96)
	first, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLogSegment, LogicalNamespace: "collection/1", ResourceID: "value/7",
		DiagnosticPath: "vlog/27.data", Generation: 4, Handle: handle, RequiredFrontier: 40,
		ReachabilityField: "meta.user_root.values[7]",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLogSegment, LogicalNamespace: "command/9", ResourceID: "external/3",
		DiagnosticPath: "vlog/27.data", Generation: 4, Handle: handle, RequiredFrontier: 80,
		ReachabilityField: "command.external_refs[3]",
	})
	if err != nil {
		t.Fatal(err)
	}
	set, err := NewStableResourceSet(first, second)
	if err != nil {
		t.Fatal(err)
	}
	if got := set.Tokens(); len(got) != 1 || got[0].RequiredFrontier() != 80 {
		t.Fatalf("generic physical coalescing=%v", got)
	}
	if err := set.Release(); err != nil {
		t.Fatal(err)
	}
	if handle.releases != 2 || handle.pins != 0 {
		t.Fatalf("coalesced physical owners releases=%d pins=%d", handle.releases, handle.pins)
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
		DiagnosticPath: "vlog/7.data", Generation: 7, Handle: handle, RequiredFrontier: 33, ReachabilityField: "meta.value_log_id",
	}); !errors.Is(err, ErrResourceFrontierBeyondLength) {
		t.Fatalf("frontier beyond length=%v", err)
	}
	a := testResourceToken(t, handle, ResourceOuterLeafManifest, 7, 32, 1, nil)
	b := testResourceToken(t, handle, ResourceOuterLeafManifest, 7, 32, 2, nil)
	if _, err := NewStableResourceSet(a, b); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("digest conflict=%v", err)
	}
}

func TestStableResourceTokenUnpinsWhenIdentityChangesDuringRegistration(t *testing.T) {
	handle := newTestStableHandle(12, 32)
	handle.identityAfterPin = StableIdentity{Device: 1, File: 13}
	if _, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLogSegment, LogicalNamespace: "primary", ResourceID: "12",
		DiagnosticPath: "vlog/12.data", Generation: 12, Handle: handle, RequiredFrontier: 32, ReachabilityField: "meta.value_log_id",
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
		Kind: ResourceOuterLeafManifest, LogicalNamespace: "leaves", ResourceID: "14",
		DiagnosticPath: "assets/14.pack", Generation: 14, Handle: handle, RequiredFrontier: 32, ReachabilityField: "meta.vector_graph_pack_id",
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

	t.Run("retry and stop retain until explicit safe abandonment", func(t *testing.T) {
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
		if _, err := coordinator.TakePendingResourceOwnership(); !errors.Is(err, ErrInvalidCandidate) {
			t.Fatalf("take before stop=%v", err)
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
		if handle.releases != 0 || handle.pins != 1 {
			t.Fatalf("stop lost recovery debt releases=%d pins=%d", handle.releases, handle.pins)
		}
		handoff, err := coordinator.TakePendingResourceOwnership()
		if err != nil {
			t.Fatal(err)
		}
		if got := handoff.Tokens(); len(got) != 1 || got[0].Identity() != handle.identity || got[0].RequiredFrontier() != 32 {
			t.Fatalf("handoff tokens=%v", got)
		}
		if err := handoff.ReleaseAfterRecovery(); err != nil {
			t.Fatal(err)
		}
		if err := handoff.ReleaseAfterRecovery(); err != nil {
			t.Fatal(err)
		}
		if _, err := coordinator.TakePendingResourceOwnership(); !errors.Is(err, ErrStableResourceOwnershipTransferred) {
			t.Fatalf("second take=%v", err)
		}
		if handle.releases != 1 || handle.pins != 0 {
			t.Fatalf("stop abandonment releases=%d pins=%d", handle.releases, handle.pins)
		}
	})

	t.Run("ambiguous result retains until explicit recovery abandonment", func(t *testing.T) {
		handle := newTestStableHandle(23, 32)
		set, err := NewStableResourceSet(testResourceToken(t, handle, ResourceValueLogSegment, 1, 32, 1, nil))
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := NewPreparedRootCandidateWithResources(CandidateSpec{Frontier: NewFrontier(1, 1, 1, 1, 1)}, set)
		if err != nil {
			t.Fatal(err)
		}
		coordinator, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
			return PublishResult{Outcome: PublishAmbiguous, Err: errors.New("injected ambiguous meta")}
		})})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Enqueue(context.Background(), candidate); err != nil {
			t.Fatal(err)
		}
		if err := coordinator.WaitThrough(context.Background(), 1); !errors.Is(err, ErrRecoveryRequired) {
			t.Fatalf("wait=%v", err)
		}
		if err := coordinator.Stop(context.Background()); !errors.Is(err, ErrPublicationStopped) {
			t.Fatalf("stop=%v", err)
		}
		if handle.releases != 0 || handle.pins != 1 {
			t.Fatalf("ambiguous stop releases=%d pins=%d", handle.releases, handle.pins)
		}
		handoff, err := coordinator.TakePendingResourceOwnership()
		if err != nil {
			t.Fatal(err)
		}
		if err := handoff.ReleaseAfterRecovery(); err != nil {
			t.Fatal(err)
		}
		if handle.releases != 1 || handle.pins != 0 {
			t.Fatalf("abandon releases=%d pins=%d", handle.releases, handle.pins)
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

func TestCoordinatorRetrySupersessionRetainsAdditiveNamespaceDebt(t *testing.T) {
	handle := newTestStableHandle(28, 96)
	dir := &testNamespaceHandle{identity: StableIdentity{Device: 1, File: 280}, generation: 3, supported: true}
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Operation: NamespaceRename, ParentDiagnosticPath: "vlog", ParentGeneration: 3, Parent: dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	low, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLogSegment, LogicalNamespace: "collection/1", ResourceID: "value/1",
		DiagnosticPath: "vlog/28.data", Generation: 4, Handle: handle, RequiredFrontier: 40,
		Namespace: namespace, ReachabilityField: "meta.user_root.values[1]",
	})
	if err != nil {
		t.Fatal(err)
	}
	lowSet, err := NewStableResourceSet(low)
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewPreparedRootCandidateWithResources(CandidateSpec{Frontier: NewFrontier(1, 1, 1, 1, 1)}, lowSet)
	if err != nil {
		t.Fatal(err)
	}
	high, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLogSegment, LogicalNamespace: "command/2", ResourceID: "external/2",
		DiagnosticPath: "vlog/28.data", Generation: 4, Handle: handle, RequiredFrontier: 80,
		ReachabilityField: "command.external_refs[2]",
	})
	if err != nil {
		t.Fatal(err)
	}
	highSet, err := NewStableResourceSet(high)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewPreparedRootCandidateWithResources(CandidateSpec{Frontier: NewFrontier(2, 2, 2, 2, 2)}, highSet)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	coordinator, err := New(Options{Publisher: PublisherFunc(func(_ context.Context, candidate *PreparedRootCandidate) PublishResult {
		tokens := candidate.Resources()
		if len(tokens) != 1 {
			t.Errorf("published physical tokens=%v", tokens)
			return PublishResult{Outcome: PublishRetryableFailure, Err: errors.New("invalid snapshot")}
		}
		switch calls.Add(1) {
		case 1:
			if tokens[0].RequiredFrontier() != 40 || tokens[0].NamespaceStable() {
				t.Errorf("first attempt frontier/stable=%d/%t", tokens[0].RequiredFrontier(), tokens[0].NamespaceStable())
			}
			return PublishResult{Outcome: PublishRetryableFailure, Err: errors.New("injected retry")}
		case 2:
			if tokens[0].RequiredFrontier() != 80 || tokens[0].NamespaceStable() {
				t.Errorf("superseding snapshot dropped debt frontier/stable=%d/%t", tokens[0].RequiredFrontier(), tokens[0].NamespaceStable())
			}
			if err := tokens[0].SyncNamespace(); err != nil {
				t.Errorf("sync retained namespace: %v", err)
				return PublishResult{Outcome: PublishRetryableFailure, Err: err}
			}
			return PublishResult{Outcome: PublishSucceeded}
		default:
			return PublishResult{Outcome: PublishRetryableFailure, Err: errors.New("unexpected publish")}
		}
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Enqueue(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.WaitThrough(context.Background(), 1); err == nil {
		t.Fatal("first attempt unexpectedly succeeded")
	}
	if err := coordinator.Supersede(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.WaitThrough(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	if dir.syncs != 1 || dir.releases != 1 || dir.pins != 0 {
		t.Fatalf("namespace syncs/releases/pins=%d/%d/%d", dir.syncs, dir.releases, dir.pins)
	}
	if handle.releases != 2 || handle.pins != 0 {
		t.Fatalf("resource releases/pins=%d/%d", handle.releases, handle.pins)
	}
	stopClean(t, coordinator)
}

func TestCoordinatorActiveSnapshotExcludesLaterEnqueueAndRetainsSuffixOwnership(t *testing.T) {
	firstHandle := newTestStableHandle(30, 32)
	secondHandle := newTestStableHandle(31, 32)
	makeCandidate := func(seq uint64, handle *testStableHandle) *PreparedRootCandidate {
		t.Helper()
		set, err := NewStableResourceSet(testResourceToken(t, handle, ResourceValueLogSegment, seq, 32, 0, nil))
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := NewPreparedRootCandidateWithResources(CandidateSpec{Frontier: NewFrontier(seq, seq, seq, seq, seq)}, set)
		if err != nil {
			t.Fatal(err)
		}
		return candidate
	}
	first := makeCandidate(1, firstHandle)
	second := makeCandidate(2, secondHandle)
	started := make(chan struct{})
	allowFirst := make(chan struct{})
	var calls atomic.Int32
	coordinator, err := New(Options{Publisher: PublisherFunc(func(_ context.Context, candidate *PreparedRootCandidate) PublishResult {
		call := calls.Add(1)
		tokens := candidate.Resources()
		if len(tokens) != 1 {
			t.Errorf("attempt %d resources=%v", call, tokens)
		} else if call == 1 && tokens[0].Identity() != firstHandle.identity {
			t.Errorf("first snapshot identity=%+v", tokens[0].Identity())
		} else if call == 2 && tokens[0].Identity() != secondHandle.identity {
			t.Errorf("second snapshot identity=%+v", tokens[0].Identity())
		}
		if call == 1 {
			close(started)
			<-allowFirst
		}
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Enqueue(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	firstWait := make(chan error, 1)
	go func() { firstWait <- coordinator.WaitThrough(context.Background(), 1) }()
	<-started
	if err := coordinator.Enqueue(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	close(allowFirst)
	if err := <-firstWait; err != nil {
		t.Fatal(err)
	}
	if firstHandle.releases != 1 || firstHandle.pins != 0 {
		t.Fatalf("captured owner releases/pins=%d/%d", firstHandle.releases, firstHandle.pins)
	}
	if secondHandle.releases != 0 || secondHandle.pins != 1 {
		t.Fatalf("later owner prematurely released=%d/%d", secondHandle.releases, secondHandle.pins)
	}
	if err := coordinator.WaitThrough(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	if secondHandle.releases != 1 || secondHandle.pins != 0 {
		t.Fatalf("suffix owner releases/pins=%d/%d", secondHandle.releases, secondHandle.pins)
	}
	stopClean(t, coordinator)
}

func TestStableResourceSetPreservesLogicalRangesAndSyncsSharedSegmentOnce(t *testing.T) {
	handle := newTestStableHandle(24, 128)
	segment := testResourceToken(t, handle, ResourceColumnAssetSegment, 3, 96, 0, nil)
	part, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceTypedColumnValueAsset, LogicalNamespace: "collection/7", ResourceID: "part/1",
		DiagnosticPath: "columns/3.tca", Generation: 3, Handle: handle, RangeStart: 8,
		RequiredFrontier: 40, BackingKind: ResourceColumnAssetSegment, Digest: []byte{1}, ReachabilityField: "column.parts[1].values",
	})
	if err != nil {
		t.Fatal(err)
	}
	graph, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceVectorGraphPack, LogicalNamespace: "collection/7", ResourceID: "hnsw/2",
		DiagnosticPath: "columns/3.tca", Generation: 3, Handle: handle, RangeStart: 40,
		RequiredFrontier: 96, BackingKind: ResourceColumnAssetSegment, Digest: []byte{2}, ReachabilityField: "column.parts[2].hnsw",
	})
	if err != nil {
		t.Fatal(err)
	}
	set, err := NewStableResourceSet(segment, part, graph)
	if err != nil {
		t.Fatal(err)
	}
	if got := set.Tokens(); len(got) != 3 {
		t.Fatalf("logical obligations collapsed: %v", got)
	}
	if err := set.ValidateReachabilityFields([]string{"meta.user_root_page_id", "column.parts[1].values", "column.parts[2].hnsw"}); err != nil {
		t.Fatal(err)
	}
	if err := set.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if got := handle.syncs; len(got) != 1 || got[0] != 96 {
		t.Fatalf("shared segment syncs=%v", got)
	}
	if err := set.Release(); err != nil {
		t.Fatal(err)
	}
	if handle.releases != 3 || handle.pins != 0 {
		t.Fatalf("range owners releases=%d pins=%d", handle.releases, handle.pins)
	}
}

func TestStableResourceSetRequiresMatchingPhysicalBacking(t *testing.T) {
	handle := newTestStableHandle(25, 64)
	logical, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceCommandWALExternalRID, LogicalNamespace: "commands", ResourceID: "rid/1",
		DiagnosticPath: "vlog/25.data", Generation: 2, Handle: handle, RangeStart: 8,
		RequiredFrontier: 40, BackingKind: ResourceValueLogSegment, Digest: []byte{1},
		ReachabilityField: "command.external_refs[0]",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewStableResourceSet(logical); !errors.Is(err, ErrMissingResourceDependency) {
		t.Fatalf("logical token without backing=%v", err)
	}
	if err := logical.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestStableResourceSetPreservesDistinctLogicalRefsToIdenticalRange(t *testing.T) {
	handle := newTestStableHandle(29, 64)
	backing := testResourceToken(t, handle, ResourceValueLogSegment, 2, 40, 0, nil)
	makeRef := func(resourceID, field string) *StableResourceToken {
		t.Helper()
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceValueLogReference, LogicalNamespace: "collection/1", ResourceID: resourceID,
			DiagnosticPath: "vlog/29.data", Generation: 2, Handle: handle, RangeStart: 8,
			RequiredFrontier: 40, BackingKind: ResourceValueLogSegment, Digest: []byte{1},
			ReachabilityField: field,
		})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	first := makeRef("value/1", "meta.user_root.values[1]")
	second := makeRef("value/2", "meta.system_root.values[2]")
	set, err := NewStableResourceSet(backing, first, second)
	if err != nil {
		t.Fatal(err)
	}
	if got := set.Tokens(); len(got) != 3 {
		t.Fatalf("distinct logical refs collapsed=%v", got)
	}
	if err := set.ValidateReachabilityFields([]string{
		"meta.user_root.values[1]", "meta.system_root.values[2]",
	}); err != nil {
		t.Fatal(err)
	}
	if err := set.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if got := handle.syncs; len(got) != 1 || got[0] != 40 {
		t.Fatalf("identical range physical syncs=%v", got)
	}
	if err := set.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestStableResourceSetRejectsChangedLogicalRangeContract(t *testing.T) {
	tests := []struct {
		name          string
		secondStart   uint64
		secondDigest  byte
		secondBacking ResourceKind
	}{
		{name: "range", secondStart: 9, secondDigest: 1, secondBacking: ResourceValueLogSegment},
		{name: "digest", secondStart: 8, secondDigest: 2, secondBacking: ResourceValueLogSegment},
		{name: "backing kind", secondStart: 8, secondDigest: 1, secondBacking: ResourceOuterLeafSegment},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handle := newTestStableHandle(26, 64)
			valueBacking := testResourceToken(t, handle, ResourceValueLogSegment, 2, 40, 0, nil)
			leafBacking := testResourceToken(t, handle, ResourceOuterLeafSegment, 2, 40, 0, nil)
			first, err := NewStableResourceToken(StableResourceSpec{
				Kind: ResourceCommandWALExternalRID, LogicalNamespace: "commands", ResourceID: "rid/1",
				DiagnosticPath: "segments/26.data", Generation: 2, Handle: handle, RangeStart: 8,
				RequiredFrontier: 40, BackingKind: ResourceValueLogSegment, Digest: []byte{1},
				ReachabilityField: "command.external_refs[0]",
			})
			if err != nil {
				t.Fatal(err)
			}
			second, err := NewStableResourceToken(StableResourceSpec{
				Kind: ResourceCommandWALExternalRID, LogicalNamespace: "commands", ResourceID: "rid/1",
				DiagnosticPath: "segments/26.data", Generation: 2, Handle: handle, RangeStart: tt.secondStart,
				RequiredFrontier: 40, BackingKind: tt.secondBacking, Digest: []byte{tt.secondDigest},
				ReachabilityField: "command.external_refs[0]",
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := NewStableResourceSet(valueBacking, leafBacking, first, second); !errors.Is(err, ErrResourceConflict) {
				t.Fatalf("changed logical contract=%v", err)
			}
			for _, token := range []*StableResourceToken{valueBacking, leafBacking, first, second} {
				if err := token.Release(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestStableResourceSetUsesDeterministicPhysicalAndNamespaceOrder(t *testing.T) {
	var orderMu sync.Mutex
	physicalOrder := make([]uint64, 0, 3)
	namespaceOrder := make([]uint64, 0, 3)
	tokens := make([]*StableResourceToken, 0, 3)
	for _, file := range []uint64{3, 1, 2} {
		handle := newTestStableHandle(byte(file), 32)
		handle.onSync = func(uint64) {
			orderMu.Lock()
			defer orderMu.Unlock()
			physicalOrder = append(physicalOrder, file)
		}
		dir := &testNamespaceHandle{
			identity: StableIdentity{Device: 1, File: file * 10}, generation: 1, supported: true,
		}
		dir.onSync = func() {
			orderMu.Lock()
			defer orderMu.Unlock()
			namespaceOrder = append(namespaceOrder, file*10)
		}
		namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
			Operation: NamespaceCreate, ParentDiagnosticPath: "segments", ParentGeneration: 1, Parent: dir,
		})
		if err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, testResourceToken(t, handle, ResourceValueLogSegment, file, 32, 0, namespace))
	}
	set, err := NewStableResourceSet(tokens...)
	if err != nil {
		t.Fatal(err)
	}
	if err := set.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if err := set.SyncNamespaces(); err != nil {
		t.Fatal(err)
	}
	if got, want := fmt.Sprint(physicalOrder), "[1 2 3]"; got != want {
		t.Fatalf("physical order=%s want=%s", got, want)
	}
	if got, want := fmt.Sprint(namespaceOrder), "[10 20 30]"; got != want {
		t.Fatalf("namespace order=%s want=%s", got, want)
	}
	if err := set.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestStableResourceSetRejectsContradictoryNamespaceDebtForOnePhysicalIdentity(t *testing.T) {
	handle := newTestStableHandle(32, 32)
	makeToken := func(parentFile uint64, resourceID string) *StableResourceToken {
		t.Helper()
		dir := &testNamespaceHandle{
			identity: StableIdentity{Device: 1, File: parentFile}, generation: 1, supported: true,
		}
		namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
			Operation: NamespaceCreate, ParentDiagnosticPath: "vlog", ParentGeneration: 1, Parent: dir,
		})
		if err != nil {
			t.Fatal(err)
		}
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceValueLogSegment, LogicalNamespace: "producer", ResourceID: resourceID,
			DiagnosticPath: "vlog/32.data", Generation: 1, Handle: handle, RequiredFrontier: 32,
			Namespace: namespace, ReachabilityField: "producer.value_log_segment",
		})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	first := makeToken(320, "first")
	second := makeToken(321, "second")
	if _, err := NewStableResourceSet(first, second); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("contradictory namespace debt=%v", err)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	if err := second.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestStableResourcePinBlocksConcurrentUnlinkAndReleasesExactlyOnce(t *testing.T) {
	handle := newTestStableHandle(8, 32)
	token := testResourceToken(t, handle, ResourceOuterLeafManifest, 1, 32, 1, nil)
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
		DiagnosticPath: "future/1.data", Generation: 1, Handle: handle, RequiredFrontier: 32, Digest: []byte{1}, ReachabilityField: "meta.future",
	}); !errors.Is(err, ErrInvalidStableResource) {
		t.Fatalf("unregistered kind=%v", err)
	}
	if handle.pins != 0 {
		t.Fatalf("unregistered kind acquired pins=%d", handle.pins)
	}
}
