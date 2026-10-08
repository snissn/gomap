package templatedb

import (
	"context"
	"errors"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
	"testing"
)

type templateCustodySnapshotV1 struct{ *stableTestSnapshot }

func (s *templateCustodySnapshotV1) CleanupCompleteV1() bool { return s.snapshot.CleanupCompleteV1() }
func TestTemplateStoreCloseKeepsRealSnapshotWhileIteratorHeld(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = backend.Close() }()
	if err = backend.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	snapshot := backend.AcquireSnapshot()
	it, err := snapshot.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	store := New(stableTestKV{db: backend}, Config{})
	holder := &templateCustodySnapshotV1{&stableTestSnapshot{snapshot: snapshot, dir: backend.Dir()}}
	store.pendingSnapshot = holder
	if err = store.Close(); !errors.Is(err, rootpublication.ErrStableResourceOperationBusy) {
		t.Fatalf("pending template Close: %v", err)
	}
	if store.pendingSnapshot != holder || snapshot.CleanupCompleteV1() {
		t.Fatal("template Store dropped unfinished physical Snapshot")
	}
	if err = it.Close(); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	if store.pendingSnapshot != nil {
		t.Fatal("template holder not scrubbed")
	}
}
func TestTemplateCaptureAddRefusalBalancesOriginalSnapshotAndCanRecapture(t *testing.T) {
	t.Run("claimed-builder-pending-token-alias", testTemplateCaptureCustodyDrainsClaimedBuilderBeforeTokenAlias)
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = backend.Close() }()
	store := New(stableTestKV{db: backend}, Config{})
	id, err := store.PutTemplateDef(context.Background(), []byte("template-cleanup-refusal"), nil)
	if err != nil {
		t.Fatal(err)
	}
	saved := addTemplateStableResourceToken
	defer func() { addTemplateStableResourceToken = saved }()
	fault := errors.New("template index Add refusal")
	addTemplateStableResourceToken = func(_ *rootpublication.StableResourceSetBuilder, _ *rootpublication.StableResourceToken, _ templateStablePhysicalRole) error {
		return fault
	}
	resources, err := store.CaptureTemplateResources(context.Background(), id)
	if resources != nil || !errors.Is(err, fault) {
		t.Fatalf("template refusal: %v", err)
	}
	if store.pendingSnapshot != nil || store.pendingToken != nil || store.pendingBuilder != nil || store.captureRunning {
		t.Fatal("template rollback lost responsibility")
	}
	addTemplateStableResourceToken = saved
	resources, err = store.CaptureTemplateResources(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if err = resources.Release(); err != nil {
		t.Fatal(err)
	}
}

type templateClaimedCleanupV1 struct {
	calls int
	ready bool
}

func (*templateClaimedCleanupV1) ReleaseStableResource() { panic("checked role used void release") }
func (e *templateClaimedCleanupV1) AdvanceStableResourceCleanupV1() (rootpublication.StableCleanupOutcomeV1, error) {
	e.calls++
	if !e.ready {
		return rootpublication.StableCleanupOutcomeV1{Phase: rootpublication.StableCleanupPendingV1, PendingRoles: 1, Debt: rootpublication.StableCleanupExactHoldsV1}, rootpublication.ErrStableResourceOperationBusy
	}
	return rootpublication.StableCleanupOutcomeV1{Phase: rootpublication.StableCleanupCompleteV1, ConsumedRoles: 1}, nil
}
func testTemplateCaptureCustodyDrainsClaimedBuilderBeforeTokenAlias(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "claimed")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	if _, err = file.Write([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	env := &templateClaimedCleanupV1{}
	token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{Kind: rootpublication.ResourceIndex, LogicalLane: "index", ResourceID: "index", Generation: 1, DiagnosticPath: "index.db", File: file, Frontier: rootpublication.DurableFrontier{Bytes: 1}, Digest: [32]byte{1}, Reachability: rootpublication.ReachabilityIndexFile, ReleaseEnvironment: env})
	if err != nil {
		t.Fatal(err)
	}
	builder := rootpublication.NewStableResourceSetBuilder()
	savedAdd := addTemplateStableResourceToken
	defer func() { addTemplateStableResourceToken = savedAdd }()
	fault := errors.New("claimed Add reports refusal")
	addTemplateStableResourceToken = func(b *rootpublication.StableResourceSetBuilder, tok *rootpublication.StableResourceToken, _ templateStablePhysicalRole) error {
		if e := b.Add(tok); e != nil {
			return e
		}
		return fault
	}
	if err = addTemplateStableResourceToken(builder, token, templateStableIndexRole); !errors.Is(err, fault) {
		t.Fatalf("post-claim refusal: %v", err)
	}
	store := &Store{pendingBuilder: builder, pendingToken: token}
	if err = store.drainCaptureCustodyV1(); !errors.Is(err, rootpublication.ErrStableResourceOperationBusy) {
		t.Fatalf("pending builder cleanup: %v", err)
	}
	if env.calls != 1 || store.pendingBuilder != builder || store.pendingToken != token || token.CleanupCompleteV1() {
		t.Fatal("claimed role was bypassed or actual custody dropped")
	}
	env.ready = true
	if err = store.drainCaptureCustodyV1(); err != nil {
		t.Fatal(err)
	}
	if env.calls != 2 || store.pendingBuilder != nil || store.pendingToken != nil || !token.CleanupCompleteV1() {
		t.Fatal("completed token alias replayed or remained stuck")
	}
}
