package rootpublication

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func testStableIdentity(generation uint64) StableIdentity {
	return StableIdentity{
		Platform:   "test",
		VolumeID:   7,
		ObjectID:   [16]byte{1, 2, 3, 4},
		Generation: generation,
	}
}

func TestIdentityPinRegistrySerializesPinAndDelete(t *testing.T) {
	registry := NewIdentityPinRegistry()
	identity := testStableIdentity(11)
	if err := registry.Observe(identity); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	pin, err := registry.Pin(identity)
	if err != nil {
		t.Fatalf("Pin: %v", err)
	}
	zero := registry.WaitUnpinned(identity)
	if _, err := registry.BeginDelete(identity); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("BeginDelete while pinned = %v, want ErrResourcePinned", err)
	}
	select {
	case <-zero:
		t.Fatal("WaitUnpinned closed before Release")
	default:
	}
	pin.Release()
	<-zero
	lease, err := registry.BeginDelete(identity)
	if err != nil {
		t.Fatalf("BeginDelete after release: %v", err)
	}
	if _, err := registry.Pin(identity); !errors.Is(err, ErrResourceDeletionInProgress) {
		t.Fatalf("Pin during deletion = %v, want ErrResourceDeletionInProgress", err)
	}
	lease.Abort()
	pin, err = registry.Pin(identity)
	if err != nil {
		t.Fatalf("Pin after abort: %v", err)
	}
	pin.Release()
}

func TestIdentityDeleteLeaseCommitForgetsStableNamespaceLinksForDeletedChild(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatalf("open parent: %v", err)
	}
	defer parent.Close()
	childPath := filepath.Join(dir, "000001.vlog")
	child, err := os.OpenFile(childPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatalf("create child: %v", err)
	}
	defer child.Close()

	registry := NewIdentityPinRegistry()
	if err := registry.RememberStableNamespaceLink(parent, child, filepath.Base(childPath)); err != nil {
		t.Fatalf("remember stable namespace link: %v", err)
	}
	identity, err := StableIdentityFromFile(child)
	if err != nil {
		t.Fatalf("child identity: %v", err)
	}
	lease, err := registry.BeginDelete(identity)
	if err != nil {
		t.Fatalf("begin delete: %v", err)
	}
	lease.CommitDeleted()
	if got := registry.ActiveStableNamespaceLinks(); got != 0 {
		t.Fatalf("stable namespace links after delete commit=%d want 0", got)
	}
}

func TestIdentityPinRegistryCoalescesLogicalGenerations(t *testing.T) {
	registry := NewIdentityPinRegistry()
	first := testStableIdentity(1)
	second := testStableIdentity(2)
	if err := registry.Observe(first); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	defer registry.Unobserve(first)
	pin, err := registry.Pin(first)
	if err != nil {
		t.Fatalf("Pin generation 1: %v", err)
	}
	if got := registry.PinCount(second); got != 1 {
		t.Fatalf("PinCount generation 2 = %d, want physical count 1", got)
	}
	if _, err := registry.BeginDelete(second); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("BeginDelete generation 2 = %v, want physical ErrResourcePinned", err)
	}
	pin.Release()
}

func TestIdentityPinRegistryRejectsUnobservedPin(t *testing.T) {
	registry := NewIdentityPinRegistry()
	if _, err := registry.Pin(testStableIdentity(1)); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("Pin without producer observation = %v, want ErrResourceConflict", err)
	}
}

func TestIdentityPinRegistryRejectsLatePinAfterFinalObserverRetires(t *testing.T) {
	registry := NewIdentityPinRegistry()
	identity := testStableIdentity(1)
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	lease, err := registry.BeginDelete(identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Unobserve(identity); err != nil {
		t.Fatal(err)
	}
	lease.Commit()
	if _, err := registry.Pin(identity); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("Pin after final observer retired = %v, want ErrResourceConflict", err)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("active identities after retirement = %d, want 0", got)
	}
}

func TestIdentityPinRegistrySerializesDeleteNamespace(t *testing.T) {
	registry := NewIdentityPinRegistry()
	first := testStableIdentity(1)
	second := testStableIdentity(2)
	second.ObjectID[0] = 9
	lease, err := registry.BeginDeleteAt(first, "value_vlog/value-l1-000001.log")
	if err != nil {
		t.Fatalf("BeginDeleteAt first: %v", err)
	}
	if _, err := registry.BeginDeleteAt(second, "value_vlog/value-l1-000001.log"); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("BeginDeleteAt same namespace = %v, want ErrResourcePinned", err)
	}
	lease.Abort()
	secondLease, err := registry.BeginDeleteAt(second, "value_vlog/value-l1-000001.log")
	if err != nil {
		t.Fatalf("BeginDeleteAt after abort: %v", err)
	}
	secondLease.Abort()
}

func TestIdentityPinRegistryRebindsOnlyExactKnownStableNamespaceLink(t *testing.T) {
	if !StableRelativeNamespaceSupported() {
		t.Skip("known-link replacement requires durable namespace removal")
	}
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	const name = "segment-000001.tca"
	path := filepath.Join(dir, name)
	child, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	spec := StableNamespaceSpec{
		Parent: parent, LinkedResource: child, ParentGeneration: 1,
		Operation: NamespaceCreate, NewName: name, DiagnosticPath: "column-assets/assets/segments",
	}
	registry := NewIdentityPinRegistry()
	if _, err := registry.NewStableNamespaceTokenForKnownLink(spec); !errors.Is(err, ErrNamespaceUnstable) {
		t.Fatalf("unremembered link error=%v want ErrNamespaceUnstable", err)
	}
	if err := parent.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := registry.RememberStableNamespaceLink(parent, child, name); err != nil {
		t.Fatal(err)
	}
	token, err := registry.NewStableNamespaceTokenForKnownLink(spec)
	if err != nil {
		t.Fatalf("known exact link: %v", err)
	}
	if err := token.validateStable(); err != nil {
		t.Fatalf("known exact link token is not stable: %v", err)
	}
	token.Release()

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	replacement, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	spec.LinkedResource = replacement
	if _, err := registry.NewStableNamespaceTokenForKnownLink(spec); !errors.Is(err, ErrNamespaceUnstable) {
		t.Fatalf("replacement link error=%v want ErrNamespaceUnstable", err)
	}
}

func TestIdentityPinRegistryRejectsUnbalancedRelease(t *testing.T) {
	registry := NewIdentityPinRegistry()
	if err := registry.release(testStableIdentity(1)); !errors.Is(err, ErrUnbalancedResourcePin) {
		t.Fatalf("Release without pin = %v, want ErrUnbalancedResourcePin", err)
	}
}

func TestIdentityPinRegistryRejectsLatePinAfterCommittedDelete(t *testing.T) {
	registry := NewIdentityPinRegistry()
	identity := testStableIdentity(1)
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	lease, err := registry.BeginDelete(identity)
	if err != nil {
		t.Fatal(err)
	}
	lease.Commit()
	if _, err := registry.Pin(testStableIdentity(2)); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("Pin after committed delete = %v, want ErrResourceConflict", err)
	}
	if err := registry.Unobserve(identity); err != nil {
		t.Fatal(err)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("active identities after final observer = %d, want 0", got)
	}
}

func TestIdentityPinRegistryDeletionStressDoesNotRetainState(t *testing.T) {
	registry := NewIdentityPinRegistry()
	for i := 0; i < 10_000; i++ {
		identity := testStableIdentity(1)
		identity.ObjectID[4] = byte(i)
		identity.ObjectID[5] = byte(i >> 8)
		if err := registry.Observe(identity); err != nil {
			t.Fatalf("Observe %d: %v", i, err)
		}
		lease, err := registry.BeginDelete(identity)
		if err != nil {
			t.Fatalf("BeginDelete %d: %v", i, err)
		}
		lease.Commit()
		if err := registry.Unobserve(identity); err != nil {
			t.Fatalf("Unobserve %d: %v", i, err)
		}
	}
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("active pins after stress = %d, want 0", got)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("active identities after stress = %d, want 0", got)
	}
}

func TestStableResourceTokenPinsIdentityRegistryUntilExactRelease(t *testing.T) {
	dir := t.TempDir()
	file, err := os.OpenFile(filepath.Join(dir, "resource.vlog"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.Write([]byte("stable-resource")); err != nil {
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
	var userReleases atomic.Uint64
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "1", Generation: 1,
		DiagnosticPath: "value_vlog/resource.vlog", File: file,
		Frontier: DurableFrontier{Bytes: uint64(len("stable-resource"))},
		Digest:   [32]byte{1}, Reachability: ReachabilityValueLogPointer,
		PinRegistry: registry, OnRelease: func() { userReleases.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := registry.PinCount(token.Identity()); got != 1 {
		t.Fatalf("pin count=%d want 1", got)
	}
	if _, err := registry.BeginDelete(token.Identity()); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("delete while token live error=%v want %v", err, ErrResourcePinned)
	}

	token.Release()
	token.Release()
	if got := registry.PinCount(token.Identity()); got != 0 {
		t.Fatalf("pin count after release=%d want 0", got)
	}
	if got := userReleases.Load(); got != 1 {
		t.Fatalf("user release callbacks=%d want 1", got)
	}
	lease, err := registry.BeginDelete(token.Identity())
	if err != nil {
		t.Fatalf("begin delete after token release: %v", err)
	}
	lease.Abort()
}

func BenchmarkIdentityPinRegistryPinRelease(b *testing.B) {
	registry := NewIdentityPinRegistry()
	identity := testStableIdentity(1)
	if err := registry.Observe(identity); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := registry.Unobserve(identity); err != nil {
			b.Error(err)
		}
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pin, err := registry.Pin(identity)
		if err != nil {
			b.Fatal(err)
		}
		pin.Release()
	}
}

func BenchmarkStableResourceTokenConstructionWithIdentityPinRegistry(b *testing.B) {
	dir := b.TempDir()
	file := writeStableResourceFixture(b, dir, "bench-pinned.vlog", "benchmark-resource")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		b.Fatal(err)
	}
	registry := NewIdentityPinRegistry()
	if err := registry.Observe(identity); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := registry.Unobserve(identity); err != nil {
			b.Error(err)
		}
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "bench", Generation: uint64(i + 1),
			DiagnosticPath: "bench-pinned.vlog", File: file, Frontier: DurableFrontier{Bytes: 4},
			Reachability: ReachabilityValueLogPointer, PinRegistry: registry,
		})
		if err != nil {
			b.Fatal(err)
		}
		token.Release()
	}
}
