package rootpublication

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

type testStableHandle struct {
	mu       sync.Mutex
	identity StableIdentity
	length   uint64
	flushes  []uint64
	syncs    []uint64
	pins     int
	releases int
	unlinked bool
}

func newTestStableHandle(identity byte, length uint64) *testStableHandle {
	return &testStableHandle{identity: StableIdentity{Device: 1, File: uint64(identity)}, length: length}
}

func (h *testStableHandle) StableIdentity() (StableIdentity, error) { return h.identity, nil }
func (h *testStableHandle) StableLength() (uint64, error)           { return h.length, nil }
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
	identity StableIdentity
	syncs    int
	supported bool
}

func (h *testNamespaceHandle) StableIdentity() (StableIdentity, error) { return h.identity, nil }
func (h *testNamespaceHandle) SyncNamespace() error {
	if !h.supported {
		return ErrNamespacePersistenceUnsupported
	}
	h.syncs++
	return nil
}

func testResourceToken(t testing.TB, handle StableResourceHandle, kind ResourceKind, generation, frontier uint64, digest byte, namespace *StableNamespaceToken) *StableResourceToken {
	t.Helper()
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: kind, LogicalNamespace: "primary", ResourceID: fmt.Sprintf("%d", generation),
		DiagnosticPath: "lane/current.data", Generation: generation, Handle: handle,
		RequiredFrontier: frontier, Digest: []byte{digest}, Namespace: namespace,
		ReachabilityField: "meta.user_root_page_id",
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
	// identity captured at registration and never consults the path.
	token.diagnosticPathLookupForTest = func(string) StableResourceHandle { return recreated }
	if err := token.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if len(original.syncs) != 1 || len(recreated.syncs) != 0 {
		t.Fatalf("syncs original=%v recreated=%v", original.syncs, recreated.syncs)
	}
}

func TestStableNamespaceTokenSeparatesFileDataFromNameDurability(t *testing.T) {
	dir := &testNamespaceHandle{identity: StableIdentity{Device: 1, File: 90}, supported: true}
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Operation: NamespaceCreate, ParentDiagnosticPath: "vlog", Parent: dir,
	})
	if err != nil {
		t.Fatal(err)
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
	child := testResourceToken(t, newTestStableHandle(6, 10), ResourceVectorGraphPack, 1, 10, 5, nil)
	set, err := NewStableResourceSet(child)
	if err != nil {
		t.Fatal(err)
	}
	if err := set.ValidateReachabilityFields([]string{"meta.vector_catalog_id", "meta.vector_graph_pack_id"}); !errors.Is(err, ErrMissingResourceDependency) {
		t.Fatalf("validate missing nested dependency=%v", err)
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
	dir := &testNamespaceHandle{identity: StableIdentity{Device: 1, File: 99}, supported: false}
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Operation: NamespaceRename, ParentDiagnosticPath: "assets", Parent: dir,
	})
	if !errors.Is(err, ErrNamespacePersistenceUnsupported) || namespace != nil {
		t.Fatalf("unsupported namespace token=(%v,%v)", namespace, err)
	}
}
