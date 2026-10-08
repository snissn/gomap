//go:build unix

package rootpublication

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"golang.org/x/sys/unix"
)

func TestImportedOriginalObservationKeepsFailedLastPhysicalCustody(t *testing.T) {
	file, err := os.Create(filepath.Join(t.TempDir(), "asset"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err = file.Write([]byte("asset")); err != nil {
		t.Fatal(err)
	}
	registry := NewIdentityPinRegistry()
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceColumnAsset, LogicalLane: "assets", ResourceID: "1", Generation: 1,
		DiagnosticPath: "asset", File: file, Frontier: DurableFrontier{Bytes: 5},
		Reachability: ReachabilityColumnManifest, PinRegistry: registry,
		OriginalObservation: registry,
	})
	if err != nil {
		t.Fatal(err)
	}
	builder := NewStableResourceSetBuilder()
	if err = builder.Add(token); err != nil {
		t.Fatal(err)
	}
	source, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	var owner retainedalloc.Owner
	owner.Initialize(0)
	imported, err := ImportStableResourceSetMetadata(&owner, source)
	if err != nil {
		t.Fatal(err)
	}
	sibling, err := CloneStableResourceSetExcludingKinds(imported)
	if err != nil {
		t.Fatal(err)
	}
	source.Release()
	imported.Release()
	if token.released.Load() {
		t.Fatal("original released through held alias")
	}
	if err = unix.Close(int(token.pinned.Fd())); err != nil {
		t.Fatal(err)
	}
	sibling.Release()
	if !errors.Is(registry.CleanupError(), unix.EBADF) {
		t.Fatalf("original close failure lost: %v", registry.CleanupError())
	}
	if registry.failedPinned == nil || registry.MetadataOwner().Bytes() == 0 {
		t.Fatal("original failed backing lost")
	}
	if owner.Bytes() != 0 {
		t.Fatal("independent adapter backing survived disposal")
	}
	before := registry.MetadataOwner().Bytes()
	sibling.Release()
	source.Release()
	token.Release()
	registry.Close()
	if registry.MetadataOwner().Bytes() != before || !errors.Is(registry.CleanupError(), unix.EBADF) {
		t.Fatal("failure retried or refunded")
	}
}

func TestOriginalObservedCloneOwnsExactActionAndSharedPhysicalBacking(t *testing.T) {
	file, err := os.Create(filepath.Join(t.TempDir(), "clone"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	registry := NewIdentityPinRegistry()
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceColumnAsset, LogicalLane: "assets", ResourceID: "1", Generation: 1,
		DiagnosticPath: "clone", File: file, Reachability: ReachabilityColumnManifest,
		PinRegistry: registry, OriginalObservation: registry,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	clone, err := token.cloneSharedPinned("assets", "1", "clone", token.frontier, token.reachability, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	clone.releaseObservation = registry
	backing := token.physicalBacking
	if clone.physicalBacking != backing || backing.refs.Load() != 2 || clone.observationCleanup == nil {
		t.Fatal("original ownership was mirrored or lost")
	}
	token.Release()
	if backing.refs.Load() != 1 {
		t.Fatal("early physical cleanup")
	}
	if _, err = clone.pinned.Stat(); err != nil {
		t.Fatal(err)
	}
	clone.Release()
	clone.Release()
	token.Release()
	if backing.refs.Load() != 0 || registry.Stats().ActivePins != 0 || registry.CleanupError() != nil {
		t.Fatal("once-only clone cleanup failed")
	}
	if token.physicalBacking != nil || clone.physicalBacking != nil || clone.observationCleanup != nil {
		t.Fatal("refunded backing retained through diagnostics")
	}
	registry.Close()
	if registry.MetadataOwner().Bytes() != 0 {
		t.Fatal("successful original backing leaked")
	}
}

func TestOriginalObservedNamespaceFailureAndAdmissionRefusal(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	file, err := os.Create(filepath.Join(dir, "asset"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	registry := NewIdentityPinRegistry()
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: file, ParentGeneration: 1, Operation: NamespaceCreate, NewName: "asset", DiagnosticPath: "assets",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	spec := StableResourceSpec{Kind: ResourceColumnAsset, LogicalLane: "assets", ResourceID: "1", Generation: 1, DiagnosticPath: "asset", File: file, Reachability: ReachabilityColumnManifest, PinRegistry: registry, OriginalObservation: registry, Namespace: namespace}
	token, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	if namespace.originalCleanup == nil {
		t.Fatal("original namespace custody was not preadmitted")
	}
	if err = unix.Close(int(namespace.parent.Fd())); err != nil {
		t.Fatal(err)
	}
	token.Release()
	if !errors.Is(registry.CleanupError(), unix.EBADF) || registry.failedNamespace != namespace || registry.failedPinned == nil {
		t.Fatal("original namespace failure lost")
	}
	before := registry.MetadataOwner().Bytes()
	token.Release()
	namespace.Release()
	registry.Close()
	if registry.MetadataOwner().Bytes() != before || namespace.cleanupFailure.count != 1 {
		t.Fatal("namespace failure replayed or refunded")
	}

	// A closed governor must reject before duplicate handles, pinning, or taking
	// the caller's observed edge. That edge remains the producer's to dispose.
	refused := NewIdentityPinRegistry()
	if err = refused.Observe(identity); err != nil {
		t.Fatal(err)
	}
	refused.Close()
	spec.Namespace = nil
	spec.PinRegistry, spec.OriginalObservation = refused, refused
	bytes := refused.MetadataOwner().Bytes()
	if got, err := NewStableResourceToken(spec); got != nil || !errors.Is(err, retainedalloc.ErrClosed) {
		t.Fatalf("refusal got=%v err=%v", got, err)
	}
	if refused.MetadataOwner().Bytes() != bytes || refused.Stats().ActivePins != 0 {
		t.Fatal("refusal had effects")
	}
	if err = refused.Unobserve(identity); err != nil {
		t.Fatal("refusal consumed producer observation")
	}
	refused.Close()
}
