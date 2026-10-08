package dictdb

import (
	"context"
	"errors"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
	"testing"
)

func TestDictionaryStoreCloseKeepsRealSnapshotWhileIteratorHeld(t *testing.T) {
	store, err := Open(t.TempDir(), db.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.backend.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	snapshot := store.backend.AcquireSnapshot()
	it, err := snapshot.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	store.pendingSnapshot = snapshot
	if err = store.Close(); !errors.Is(err, rootpublication.ErrStableResourceOperationBusy) {
		t.Fatalf("pending Store Close: %v", err)
	}
	if store.pendingSnapshot != snapshot || snapshot.CleanupCompleteV1() {
		t.Fatal("Store lost original unfinished Snapshot")
	}
	if err = it.Close(); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	if store.pendingSnapshot != nil {
		t.Fatal("finished Snapshot still retained")
	}
}
func TestDictionaryCaptureAddRefusalBalancesOriginalSnapshotAndCanRecapture(t *testing.T) {
	t.Run("claimed-builder-pending-token-alias", testDictionaryCaptureCustodyDrainsClaimedBuilderBeforeTokenAlias)
	store, err := Open(t.TempDir(), db.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	id, err := store.PutDictBytes(context.Background(), []byte("dictionary-cleanup-refusal"))
	if err != nil {
		t.Fatal(err)
	}
	saved := addDictionaryStableResourceToken
	defer func() { addDictionaryStableResourceToken = saved }()
	fault := errors.New("dictionary index Add refusal")
	addDictionaryStableResourceToken = func(_ *rootpublication.StableResourceSetBuilder, _ *rootpublication.StableResourceToken, _ dictionaryStablePhysicalRole) error {
		return fault
	}
	resources, err := store.CaptureDictionaryResources(context.Background(), id)
	if resources != nil || !errors.Is(err, fault) {
		t.Fatalf("capture refusal: %v", err)
	}
	if store.pendingSnapshot != nil || store.pendingToken != nil || store.pendingBuilder != nil || store.captureRunning {
		t.Fatal("completed rollback retained phantom custody")
	}
	addDictionaryStableResourceToken = saved
	resources, err = store.CaptureDictionaryResources(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if err = resources.Release(); err != nil {
		t.Fatal(err)
	}
}

type dictionaryClaimedCleanupV1 struct {
	calls int
	ready bool
}

func (*dictionaryClaimedCleanupV1) ReleaseStableResource() { panic("checked role used void release") }
func (e *dictionaryClaimedCleanupV1) AdvanceStableResourceCleanupV1() (rootpublication.StableCleanupOutcomeV1, error) {
	e.calls++
	if !e.ready {
		return rootpublication.StableCleanupOutcomeV1{Phase: rootpublication.StableCleanupPendingV1, PendingRoles: 1, Debt: rootpublication.StableCleanupExactHoldsV1}, rootpublication.ErrStableResourceOperationBusy
	}
	return rootpublication.StableCleanupOutcomeV1{Phase: rootpublication.StableCleanupCompleteV1, ConsumedRoles: 1}, nil
}
func testDictionaryCaptureCustodyDrainsClaimedBuilderBeforeTokenAlias(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "claimed")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	if _, err = file.Write([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	env := &dictionaryClaimedCleanupV1{}
	token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{Kind: rootpublication.ResourceIndex, LogicalLane: "index", ResourceID: "index", Generation: 1, DiagnosticPath: "index.db", File: file, Frontier: rootpublication.DurableFrontier{Bytes: 1}, Digest: [32]byte{1}, Reachability: rootpublication.ReachabilityIndexFile, ReleaseEnvironment: env})
	if err != nil {
		t.Fatal(err)
	}
	builder := rootpublication.NewStableResourceSetBuilder()
	savedAdd := addDictionaryStableResourceToken
	defer func() { addDictionaryStableResourceToken = savedAdd }()
	fault := errors.New("claimed Add reports refusal")
	addDictionaryStableResourceToken = func(b *rootpublication.StableResourceSetBuilder, tok *rootpublication.StableResourceToken, _ dictionaryStablePhysicalRole) error {
		if e := b.Add(tok); e != nil {
			return e
		}
		return fault
	}
	if err = addDictionaryStableResourceToken(builder, token, dictionaryStableIndexRole); !errors.Is(err, fault) {
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
