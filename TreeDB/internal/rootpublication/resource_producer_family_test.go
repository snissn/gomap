package rootpublication

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestStableOuterLeafProducerFamilyFrontierIdentityAndLifetime(t *testing.T) {
	file := selectorSizedTempFile(t, 8)
	defer file.Close()
	registry := NewIdentityPinRegistry()
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	base, err := NewStableProducerResourceTokenForDomain(StableProducerOuterLeaf, StableResourceSpec{
		Kind: ResourceOuterLeafLog, LogicalLane: "outer-leaf-0", ResourceID: "1", Generation: 1,
		DiagnosticPath: "leaf/1.vlog", File: file, Frontier: DurableFrontier{Bytes: 8},
		Reachability: ReachabilityOuterLeafRawPointer, PinRegistry: registry,
	}, "authoritative")
	if err != nil {
		t.Fatal(err)
	}
	defer base.Release()
	other := selectorSizedTempFile(t, 32)
	defer other.Close()
	if token, err := base.CertifyFlushedOuterLeafResource(other); token != nil || !errors.Is(err, ErrResourceConflict) {
		token.Release()
		t.Fatalf("cross-file certificate=%v", err)
	}
	if err := file.Truncate(32); err != nil {
		t.Fatal(err)
	}
	issued, err := base.CertifyFlushedOuterLeafResource(file)
	if err != nil {
		t.Fatal(err)
	}
	defer issued.Release()
	if base.frontier.Bytes != 8 || issued.frontier.Bytes != 32 || base.pinned != issued.pinned {
		t.Fatal("frontier/handle family contract")
	}
	base.Release()
	if _, err := base.CertifyFlushedOuterLeafResource(file); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("released family=%v", err)
	}
	if _, err := issued.pinned.Stat(); err != nil {
		t.Fatalf("independent certificate closed with owner: %v", err)
	}
	if err := file.Truncate(4); err != nil {
		t.Fatal(err)
	}
	if token, err := issued.CertifyFlushedOuterLeafResource(file); token != nil || !errors.Is(err, ErrResourceConflict) {
		token.Release()
		t.Fatalf("short frontier=%v", err)
	}
	issued.Release()
	if registry.Stats().ActivePins != 0 {
		t.Fatal("family leaked registry pins")
	}
	var missing *StableResourceToken
	if _, err := missing.CertifyFlushedOuterLeafResource(file); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("nil family=%v", err)
	}
}

func TestStableOuterLeafProducerFamilyRejectsNamespaceDriftAndCallerLease(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "leaf.vlog")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{Parent: parent, LinkedResource: file, ParentGeneration: 1, Operation: NamespaceCreate, NewName: "leaf.vlog", DiagnosticPath: "leaf"})
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.Release()
	if err := namespace.Stabilize(); err != nil {
		t.Fatal(err)
	}
	registry := NewIdentityPinRegistry()
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	spec := StableResourceSpec{Kind: ResourceOuterLeafLog, LogicalLane: "outer-leaf-0", ResourceID: "1", Generation: 1, DiagnosticPath: "leaf/leaf.vlog", File: file, Reachability: ReachabilityOuterLeafRawPointer, Namespace: namespace, PinRegistry: registry}
	releases := 0
	spec.OnRelease = func() { releases++ }
	custom, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	if token, err := custom.CertifyFlushedOuterLeafResource(file); token != nil || !errors.Is(err, ErrUnresolvedResource) {
		token.Release()
		t.Fatalf("caller lease=%v", err)
	}
	spec.OnRelease = nil
	base, err := NewStableProducerResourceTokenForDomain(StableProducerOuterLeaf, spec, "authoritative")
	if err != nil {
		t.Fatal(err)
	}
	defer base.Release()
	custom.Release()
	if releases != 1 {
		t.Fatalf("caller releases=%d", releases)
	}
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if token, err := base.CertifyFlushedOuterLeafResource(file); token != nil || (!errors.Is(err, ErrResourceConflict) && !errors.Is(err, ErrUnresolvedResource)) {
		token.Release()
		t.Fatalf("namespace drift=%v", err)
	}
}
