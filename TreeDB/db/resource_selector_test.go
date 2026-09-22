package db

import (
	"crypto/sha256"
	"errors"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func dbSelectorFixture(t *testing.T) (*rootpublication.StableResourceSet, rootpublication.StableResourceSelector) {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "selector-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	if err := file.Truncate(8); err != nil {
		t.Fatal(err)
	}
	obligation := rootpublication.StableLogicalObligation{Class: "asset", Kind: "part", Namespace: "columns", Generation: 3, PartID: 4, FileID: 7, Length: 8, Reachability: rootpublication.ReachabilityColumnManifest, Digest: sha256.Sum256([]byte("logical"))}
	token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{Kind: rootpublication.ResourceColumnAsset, LogicalLane: "columns", ResourceID: "7", Generation: 7, DiagnosticPath: "columns/7", File: file, Frontier: rootpublication.DurableFrontier{Bytes: 8}, Digest: sha256.Sum256([]byte("physical")), Reachability: rootpublication.ReachabilityColumnManifest, LogicalObligations: []rootpublication.StableLogicalObligation{obligation}, ContentSynced: true})
	if err != nil {
		t.Fatal(err)
	}
	builder := rootpublication.NewStableResourceSetBuilder()
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	resources, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	return resources, rootpublication.StableResourceSelector{Kind: rootpublication.ResourceColumnAsset, LogicalLane: "columns", ResourceID: "7", PhysicalGeneration: 7, Obligation: obligation}
}

func TestRecoverableRootSetCloneStableResourceForExactRoot(t *testing.T) {
	resources, selector := dbSelectorFixture(t)
	root := RecoverableRoot{CommitSeq: 1, UserRootPageID: 2, SystemRootPageID: 3, AppliedCommandLSN: 4, MaxEntryRevision: 5}
	set := &RecoverableRootSet{rootResources: map[recoverableRootKey]*rootpublication.StableResourceSet{recoverableRootIdentity(root): resources}, resources: []*rootpublication.StableResourceSet{resources}}
	selected, err := set.CloneStableResourceForRoot(root, selector)
	if err != nil {
		t.Fatal(err)
	}
	resources.Release()
	set.resources = nil
	if got := selected.Descriptors(); len(got) != 1 || got[0].ResourceID() != "7" {
		selected.Release()
		t.Fatalf("selected=%v", got)
	}
	selected.Release()

	resources, selector = dbSelectorFixture(t)
	set = &RecoverableRootSet{rootResources: map[recoverableRootKey]*rootpublication.StableResourceSet{recoverableRootIdentity(root): resources}, resources: []*rootpublication.StableResourceSet{resources}}
	mismatch := root
	mismatch.MaxEntryRevision++
	if got, err := set.CloneStableResourceForRoot(mismatch, selector); got != nil || !errors.Is(err, ErrRecoverableRootSetStale) {
		t.Fatalf("mismatch selected=%v err=%v", got, err)
	}
	set.released.Store(true)
	if got, err := set.CloneStableResourceForRoot(root, selector); got != nil || !errors.Is(err, ErrRecoverableRootSetStale) {
		t.Fatalf("released selected=%v err=%v", got, err)
	}
	resources.Release()
}

func TestCommandWALPublishContextVisibleSelectorExpires(t *testing.T) {
	if got, err := (CommandWALPublishContext{}).CloneVisibleStableResource(rootpublication.StableResourceSelector{}); got != nil || !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("nil selected=%v err=%v", got, err)
	}
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	database := openCommandWALDB(t, dir)
	defer database.Close()
	var escaped CommandWALPublishContext
	want := errors.New("stop after context capture")
	_, _, err := database.PublishOrderedRootDeltaBatchGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder(nil, mustRawKVCommandWALIntent(t, database, "cmd/selector-expiry", "1"), func(ctx CommandWALPublishContext) ([]OrderedRootDeltaBatchPublishInput, error) {
		escaped = ctx
		if got, selectErr := ctx.CloneVisibleStableResource(rootpublication.StableResourceSelector{}); got != nil || errors.Is(selectErr, rootpublication.ErrResourceOwnership) {
			t.Fatalf("live context selected=%v err=%v", got, selectErr)
		}
		return nil, want
	}, func(CommandWALPublishContext, []uint64) (iterator.UnsafeIterator, error) { return nil, nil })
	if !errors.Is(err, want) {
		t.Fatalf("publish err=%v want %v", err, want)
	}
	if got, err := escaped.CloneVisibleStableResource(rootpublication.StableResourceSelector{}); got != nil || !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("expired selected=%v err=%v", got, err)
	}
}
