package rootpublication

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
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

func TestIdentityPinClonePreservesConsumedObservationCustody(t *testing.T) {
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	identity := testStableIdentity(1)
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	original, err := registry.Pin(identity)
	if err != nil {
		t.Fatal(err)
	}
	defer original.Release()
	if err := registry.Unobserve(identity); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Pin(identity); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("fresh Pin without observation = %v, want ErrResourceConflict", err)
	}
	clone, err := original.clone()
	if err != nil {
		t.Fatalf("clone retained pin: %v", err)
	}
	defer clone.Release()
	original.Release()
	if _, err := original.clone(); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("clone released pin = %v, want ErrResourceOwnership", err)
	}
	if _, err := registry.BeginDelete(identity); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("delete while clone survives = %v, want ErrResourcePinned", err)
	}
	clone.Release()
	if _, err := clone.clone(); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("clone released last pin = %v, want ErrResourceOwnership", err)
	}
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("active pins after last release = %d, want 0", got)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("active identities after last release = %d, want 0", got)
	}
	lease, err := registry.BeginDelete(identity)
	if err != nil {
		t.Fatalf("delete after last pin: %v", err)
	}
	lease.CommitDeleted()
	registry.Close()
	if got := registry.MetadataOwner().Bytes(); got != 0 {
		t.Fatalf("registry bytes after Close = %d, want 0", got)
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

func TestIdentityPinRegistryFrontierPinAndTruncateLease5922(t *testing.T) {
	registry := NewIdentityPinRegistry()
	identity := testStableIdentity(77)
	if err := registry.Observe(identity); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	defer func() {
		if err := registry.Unobserve(identity); err != nil {
			t.Fatalf("Unobserve: %v", err)
		}
		if got := registry.ActiveIdentities(); got != 0 {
			t.Fatalf("active identities after unobserve = %d, want 0", got)
		}
	}()

	// Pin with frontier 500 blocks truncation below 500, allows truncation at >= 500.
	pin1, err := registry.PinWithFrontier(identity, 500)
	if err != nil {
		t.Fatalf("PinWithFrontier(500): %v", err)
	}
	const ns = "column_assets/events/segment-000001.tca"
	if _, err := registry.BeginTruncateAt(identity, ns, 499); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("BeginTruncateAt(499) with pin@500 err=%v, want ErrResourcePinned", err)
	}
	lease, err := registry.BeginTruncateAt(identity, ns, 500)
	if err != nil {
		t.Fatalf("BeginTruncateAt(500) with pin@500: %v", err)
	}
	// While truncating at 500:
	// - concurrent PinWithFrontier > 500 is rejected
	if _, err := registry.PinWithFrontier(identity, 501); !errors.Is(err, ErrResourceDeletionInProgress) {
		t.Fatalf("PinWithFrontier(501) during truncate@500 err=%v, want ErrResourceDeletionInProgress", err)
	}
	// - AdvanceFrontier > 500 is rejected
	if err := pin1.AdvanceFrontier(501); !errors.Is(err, ErrResourceDeletionInProgress) {
		t.Fatalf("AdvanceFrontier(501) during truncate@500 err=%v, want ErrResourceDeletionInProgress", err)
	}
	// - concurrent BeginTruncateAt on same identity/namespace is rejected
	if _, err := registry.BeginTruncateAt(identity, ns, 500); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("nested BeginTruncateAt err=%v, want ErrResourcePinned", err)
	}
	// - concurrent PinWithFrontier <= 500 is allowed
	pinLow, err := registry.PinWithFrontier(identity, 400)
	if err != nil {
		t.Fatalf("PinWithFrontier(400) during truncate@500: %v", err)
	}
	// Commit leaves identity live and un-retired.
	lease.Commit()
	if registry.IsRetired(identity) {
		t.Fatal("BeginTruncateAt Commit unexpectedly retired identity")
	}

	// Advancing pinLow past 500 succeeds after truncate commits and blocks truncation at 600.
	if err := pinLow.AdvanceFrontier(700); err != nil {
		t.Fatalf("AdvanceFrontier(700): %v", err)
	}
	cloned, err := pinLow.clone()
	if err != nil {
		t.Fatalf("clone pin@700: %v", err)
	}
	if _, err := registry.BeginTruncateAt(identity, ns, 650); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("BeginTruncateAt(650) with pin@700 err=%v, want ErrResourcePinned", err)
	}
	pinLow.Release()
	// Cloned pin must still hold frontier 700 until released!
	if _, err := registry.BeginTruncateAt(identity, ns, 650); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("BeginTruncateAt(650) with cloned pin@700 err=%v, want ErrResourcePinned", err)
	}
	cloned.Release()

	// Now only pin1@500 remains; truncating at 500 succeeds and Abort releases cleanly.
	lease2, err := registry.BeginTruncateAt(identity, ns, 500)
	if err != nil {
		t.Fatalf("BeginTruncateAt(500) after releasing 700 pins: %v", err)
	}
	lease2.Abort()
	pin1.Release()
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("active pins after releasing all = %d, want 0", got)
	}
}

func TestIdentityPinRegistryFrontierRetentionAccountingAndBudgetRefusal5922(t *testing.T) {
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := registry.MetadataOwner().Enroll(budget)
	if err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	defer enrollment.Close()

	identity := testStableIdentity(88)
	if err := registry.Observe(identity); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	const ns = "column_assets/events/segment-000088.tca"

	// Single distinct non-zero frontier stays inline with zero overflow allocation.
	afterObserve := registry.MetadataOwner().Bytes()
	pin1, err := registry.PinWithFrontier(identity, 500)
	if err != nil {
		t.Fatalf("PinWithFrontier(500): %v", err)
	}
	if got := registry.MetadataOwner().Bytes(); got != afterObserve {
		t.Fatalf("single frontier pin charged overflow: got=%d want=%d", got, afterObserve)
	}
	// Single-pin AdvanceFrontier updates inline without spilling.
	if err := pin1.AdvanceFrontier(600); err != nil {
		t.Fatalf("AdvanceFrontier(600): %v", err)
	}
	if got := registry.MetadataOwner().Bytes(); got != afterObserve {
		t.Fatalf("single-pin AdvanceFrontier charged overflow: got=%d want=%d", got, afterObserve)
	}
	// Duplicate pin at same frontier stays inline.
	pin1Dup, err := registry.PinWithFrontier(identity, 600)
	if err != nil {
		t.Fatalf("PinWithFrontier(600 dup): %v", err)
	}
	if got := registry.MetadataOwner().Bytes(); got != afterObserve {
		t.Fatalf("duplicate single frontier pin charged overflow: got=%d want=%d", got, afterObserve)
	}

	// Tighten budget below overflow table charge: spilling via PinWithFrontier,
	// cloneWithFrontier, or AdvanceFrontier must fail closed without mutating
	// pin counts or existing frontier protection.
	overflowCap16 := retainedalloc.AllocationCharge(16 * uint64(unsafe.Sizeof(registryCell[uint64, uint64]{})))
	budget.cap = afterObserve + overflowCap16 - 1
	if _, err := registry.PinWithFrontier(identity, 900); !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("PinWithFrontier(900) under tight budget = %v, want ErrCapacity", err)
	}
	if _, err := pin1.cloneWithFrontier(900); !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("cloneWithFrontier(900) under tight budget = %v, want ErrCapacity", err)
	}
	if err := pin1.AdvanceFrontier(900); !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("AdvanceFrontier(900) under tight budget = %v, want ErrCapacity", err)
	}
	if got := registry.ActivePins(); got != 2 {
		t.Fatalf("ActivePins after budget refusals = %d, want 2", got)
	}
	if got := registry.MetadataOwner().Bytes(); got != afterObserve || budget.bytes != afterObserve {
		t.Fatalf("bytes after budget refusals: owner=%d budget=%d want=%d", got, budget.bytes, afterObserve)
	}
	if _, err := registry.BeginTruncateAt(identity, ns, 599); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("BeginTruncateAt(599) after refusal = %v, want ErrResourcePinned", err)
	}

	// Restore budget: existing pins still top out at 600 (refused 900 had no
	// side effect), and spilling to a second distinct frontier accounts for overflow.
	budget.cap = 1 << 20
	leaseAt600, err := registry.BeginTruncateAt(identity, ns, 600)
	if err != nil {
		t.Fatalf("BeginTruncateAt(600) after refusal: %v", err)
	}
	leaseAt600.Abort()
	beforeSpill := registry.MetadataOwner().Bytes()
	pin2, err := registry.PinWithFrontier(identity, 900)
	if err != nil {
		t.Fatalf("PinWithFrontier(900): %v", err)
	}
	afterSpill := registry.MetadataOwner().Bytes()
	if afterSpill != beforeSpill+overflowCap16 {
		t.Fatalf("bytes after spill = %d, want %d", afterSpill, beforeSpill+overflowCap16)
	}
	if _, err := registry.BeginTruncateAt(identity, ns, 899); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("BeginTruncateAt(899) after spill = %v, want ErrResourcePinned", err)
	}

	// Releasing all frontier pins closes overflow table and refunds its backing.
	pin1.Release()
	pin1Dup.Release()
	pin2.Release()
	if got := registry.MetadataOwner().Bytes(); got != beforeSpill {
		t.Fatalf("bytes after releasing overflow pins = %d, want %d", got, beforeSpill)
	}
	if err := registry.Unobserve(identity); err != nil {
		t.Fatalf("Unobserve: %v", err)
	}
	registry.Close()
	enrollment.Close()
	if got := registry.MetadataOwner().Bytes(); got != 0 || budget.bytes != 0 {
		t.Fatalf("bytes after Close: owner=%d budget=%d want 0", got, budget.bytes)
	}
}

func TestIdentityPinRegistryUnionCoalesceRaisesFrontierPin5922(t *testing.T) {
	const ns = "column_assets/events/segment-000001.tca"

	t.Run("shared_union_clone", func(t *testing.T) {
		dir := t.TempDir()
		registry := NewIdentityPinRegistry()
		defer registry.Close()
		filePath := filepath.Join(dir, "segment-000001.tca")
		if err := os.WriteFile(filePath, make([]byte, 2048), 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		fLow, err := os.Open(filePath)
		if err != nil {
			t.Fatalf("Open low: %v", err)
		}
		fHigh, err := os.Open(filePath)
		if err != nil {
			_ = fLow.Close()
			t.Fatalf("Open high: %v", err)
		}
		identity, err := StableIdentityFromFile(fLow)
		if err != nil {
			t.Fatalf("StableIdentityFromFile: %v", err)
		}
		if err := registry.Observe(identity); err != nil {
			t.Fatalf("Observe: %v", err)
		}
		defer func() {
			if err := registry.Unobserve(identity); err != nil {
				t.Fatalf("Unobserve: %v", err)
			}
		}()

		tokLow, err := NewStableResourceToken(StableResourceSpec{
			Kind:           ResourceColumnAsset,
			LogicalLane:    "events",
			ResourceID:     "seg-1",
			Generation:     1,
			DiagnosticPath: ns,
			File:           fLow,
			Frontier:       DurableFrontier{Bytes: 256},
			Reachability:   ReachabilityColumnManifest,
			PinRegistry:    registry,
			ContentSynced:  true,
		})
		if err != nil {
			t.Fatalf("NewStableResourceToken low: %v", err)
		}
		tokHigh, err := NewStableResourceToken(StableResourceSpec{
			Kind:           ResourceColumnAsset,
			LogicalLane:    "events",
			ResourceID:     "seg-1",
			Generation:     1,
			DiagnosticPath: ns,
			File:           fHigh,
			Frontier:       DurableFrontier{Bytes: 1024},
			Reachability:   ReachabilityColumnManifest,
			PinRegistry:    registry,
			ContentSynced:  true,
		})
		if err != nil {
			tokLow.Release()
			t.Fatalf("NewStableResourceToken high: %v", err)
		}

		bLow := NewStableResourceSetBuilder()
		if err := bLow.Add(tokLow); err != nil {
			t.Fatalf("bLow.Add: %v", err)
		}
		setLow, err := bLow.Freeze()
		if err != nil {
			t.Fatalf("bLow.Freeze: %v", err)
		}
		bHigh := NewStableResourceSetBuilder()
		if err := bHigh.Add(tokHigh); err != nil {
			t.Fatalf("bHigh.Add: %v", err)
		}
		setHigh, err := bHigh.Freeze()
		if err != nil {
			t.Fatalf("bHigh.Freeze: %v", err)
		}

		// Union + cloneStableResourceEntryIntoBuilder selects setLow's representative
		// token (frontier 256) while cloning at the merged frontier 1024 via
		// cloneSharedPinnedDirectory -> cloneWithFrontier(1024).
		unionView, err := UnionStableResourceSets(setLow, setHigh)
		if err != nil {
			setLow.Release()
			setHigh.Release()
			t.Fatalf("UnionStableResourceSets: %v", err)
		}
		cloneBuilder := NewStableResourceSetBuilder()
		if err := cloneStableResourceEntryIntoBuilder(cloneBuilder, &unionView.entries[0]); err != nil {
			cloneBuilder.Abandon()
			setLow.Release()
			setHigh.Release()
			t.Fatalf("cloneStableResourceEntryIntoBuilder: %v", err)
		}
		clonedUnion, err := cloneBuilder.Freeze()
		setLow.Release()
		setHigh.Release()
		if err != nil {
			t.Fatalf("cloneBuilder.Freeze: %v", err)
		}

		if _, err := registry.BeginTruncateAt(identity, ns, 512); !errors.Is(err, ErrResourcePinned) {
			clonedUnion.Release()
			t.Fatalf("BeginTruncateAt(512) with clonedUnion@1024 = %v, want ErrResourcePinned", err)
		}
		lease, err := registry.BeginTruncateAt(identity, ns, 1024)
		if err != nil {
			clonedUnion.Release()
			t.Fatalf("BeginTruncateAt(1024) with clonedUnion@1024: %v", err)
		}
		lease.Abort()
		clonedUnion.Release()
		if got := registry.ActivePins(); got != 0 {
			t.Fatalf("ActivePins after clonedUnion release = %d, want 0", got)
		}
	})

	t.Run("owned_entry_union", func(t *testing.T) {
		dir := t.TempDir()
		registry := NewIdentityPinRegistry()
		defer registry.Close()
		owner := new(retainedalloc.Owner)
		owner.Initialize(0)

		filePath := filepath.Join(dir, "segment-000001.tca")
		if err := os.WriteFile(filePath, make([]byte, 2048), 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		fLow, err := os.Open(filePath)
		if err != nil {
			t.Fatalf("Open low: %v", err)
		}
		fHigh, err := os.Open(filePath)
		if err != nil {
			_ = fLow.Close()
			t.Fatalf("Open high: %v", err)
		}
		identity, err := StableIdentityFromFile(fLow)
		if err != nil {
			t.Fatalf("StableIdentityFromFile: %v", err)
		}
		if err := registry.Observe(identity); err != nil {
			t.Fatalf("Observe: %v", err)
		}
		defer func() {
			if err := registry.Unobserve(identity); err != nil {
				t.Fatalf("Unobserve: %v", err)
			}
		}()

		tokLow, err := NewStableResourceToken(StableResourceSpec{
			MetadataOwner:  owner,
			Kind:           ResourceColumnAsset,
			LogicalLane:    "events",
			ResourceID:     "seg-1",
			Generation:     1,
			DiagnosticPath: ns,
			File:           fLow,
			Frontier:       DurableFrontier{Bytes: 256},
			Reachability:   ReachabilityColumnManifest,
			PinRegistry:    registry,
			ContentSynced:  true,
		})
		if err != nil {
			t.Fatalf("NewStableResourceToken owned low: %v", err)
		}
		tokHigh, err := NewStableResourceToken(StableResourceSpec{
			MetadataOwner:  owner,
			Kind:           ResourceColumnAsset,
			LogicalLane:    "events",
			ResourceID:     "seg-1",
			Generation:     1,
			DiagnosticPath: ns,
			File:           fHigh,
			Frontier:       DurableFrontier{Bytes: 1024},
			Reachability:   ReachabilityColumnManifest,
			PinRegistry:    registry,
			ContentSynced:  true,
		})
		if err != nil {
			tokLow.Release()
			t.Fatalf("NewStableResourceToken owned high: %v", err)
		}

		bLow, err := NewStableResourceSetBuilderWithMetadata(owner)
		if err != nil {
			tokLow.Release()
			tokHigh.Release()
			t.Fatalf("NewStableResourceSetBuilderWithMetadata low: %v", err)
		}
		if err := bLow.Add(tokLow); err != nil {
			bLow.Abandon()
			tokHigh.Release()
			t.Fatalf("bLow.Add: %v", err)
		}
		setLow, err := bLow.Freeze()
		if err != nil {
			tokHigh.Release()
			t.Fatalf("bLow.Freeze: %v", err)
		}
		bHigh, err := NewStableResourceSetBuilderWithMetadata(owner)
		if err != nil {
			setLow.Release()
			tokHigh.Release()
			t.Fatalf("NewStableResourceSetBuilderWithMetadata high: %v", err)
		}
		if err := bHigh.Add(tokHigh); err != nil {
			bHigh.Abandon()
			setLow.Release()
			t.Fatalf("bHigh.Add: %v", err)
		}
		setHigh, err := bHigh.Freeze()
		if err != nil {
			setLow.Release()
			t.Fatalf("bHigh.Freeze: %v", err)
		}

		var leftEntry, rightEntry *stableResourceEntry
		rangeStableResourceKindViews(setLow.kindViews, func(entry *stableResourceEntry) bool {
			leftEntry = entry
			return false
		})
		rangeStableResourceKindViews(setHigh.kindViews, func(entry *stableResourceEntry) bool {
			rightEntry = entry
			return false
		})
		if leftEntry == nil || rightEntry == nil {
			setLow.Release()
			setHigh.Release()
			t.Fatal("missing owned kind view entries")
		}

		// cloneOwnedResourceEntryUnion selects leftEntry.token (frontier 256) and
		// clones it at merged frontier 1024 via cloneOwnedPinnedDirectory ->
		// cloneWithFrontier(1024).
		backing, err := cloneOwnedResourceEntryUnion(leftEntry, rightEntry, owner)
		setLow.Release()
		setHigh.Release()
		if err != nil {
			t.Fatalf("cloneOwnedResourceEntryUnion: %v", err)
		}

		if _, err := registry.BeginTruncateAt(identity, ns, 512); !errors.Is(err, ErrResourcePinned) {
			_ = backing.entries[0].token.ReleaseWithError()
			_ = backing.release()
			t.Fatalf("BeginTruncateAt(512) with owned union@1024 = %v, want ErrResourcePinned", err)
		}
		lease, err := registry.BeginTruncateAt(identity, ns, 1024)
		if err != nil {
			_ = backing.entries[0].token.ReleaseWithError()
			_ = backing.release()
			t.Fatalf("BeginTruncateAt(1024) with owned union@1024: %v", err)
		}
		lease.Abort()
		if err := backing.entries[0].token.ReleaseWithError(); err != nil {
			t.Fatalf("release union token: %v", err)
		}
		if err := backing.release(); err != nil {
			t.Fatalf("release union backing: %v", err)
		}
		if got := registry.ActivePins(); got != 0 {
			t.Fatalf("ActivePins after owned union release = %d, want 0", got)
		}
		if got := owner.Bytes(); got != 0 {
			t.Fatalf("owner bytes after owned union release = %d, want 0", got)
		}
	})
}

func TestIdentityPinRegistryImportedTokenRaisesSourceFrontierPin5922(t *testing.T) {
	dir := t.TempDir()
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	filePath := filepath.Join(dir, "segment-000002.tca")
	if err := os.WriteFile(filePath, make([]byte, 4096), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatalf("StableIdentityFromFile: %v", err)
	}
	if err := registry.Observe(identity); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	defer func() {
		if err := registry.Unobserve(identity); err != nil {
			t.Fatalf("Unobserve: %v", err)
		}
	}()

	token, err := NewStableResourceToken(StableResourceSpec{
		Kind:           ResourceColumnAsset,
		LogicalLane:    "events",
		ResourceID:     "seg-2",
		Generation:     1,
		DiagnosticPath: "column_assets/events/segment-000002.tca",
		File:           file,
		Frontier:       DurableFrontier{Bytes: 256},
		Reachability:   ReachabilityColumnManifest,
		PinRegistry:    registry,
		ContentSynced:  true,
	})
	if err != nil {
		t.Fatalf("NewStableResourceToken: %v", err)
	}
	builder := NewStableResourceSetBuilder()
	if err := builder.Add(token); err != nil {
		t.Fatalf("builder.Add: %v", err)
	}
	source, err := builder.Freeze()
	if err != nil {
		t.Fatalf("builder.Freeze: %v", err)
	}
	var root *stableResourceEntryNode
	for _, v := range source.kindViews.all() {
		root = v.root
	}
	owner := new(retainedalloc.Owner)
	owner.Initialize(0)

	const ns = "column_assets/events/segment-000002.tca"
	// While a truncation lease at 512 is active, importing with frontier 1024
	// must fail closed with ErrResourceDeletionInProgress and roll back owner bytes.
	lease, err := registry.BeginTruncateAt(identity, ns, 512)
	if err != nil {
		source.Release()
		t.Fatalf("BeginTruncateAt(512): %v", err)
	}
	if _, err := newImportedResourceToken(owner, token, root, token.logicalLane, token.resourceID, token.diagnosticPath, DurableFrontier{Bytes: 1024}, token.reachability, token.logicalObligations, nil); !errors.Is(err, ErrResourceDeletionInProgress) {
		lease.Abort()
		source.Release()
		t.Fatalf("newImportedResourceToken during truncate@512 = %v, want ErrResourceDeletionInProgress", err)
	}
	if got := owner.Bytes(); got != 0 {
		lease.Abort()
		source.Release()
		t.Fatalf("importer owner bytes after refusal = %d, want 0", got)
	}
	lease.Abort()

	// After abort, importing at 1024 and cloning at 2048 raises the underlying
	// source identityPin frontier so BeginTruncateAt below 2048 is rejected.
	imported, err := newImportedResourceToken(owner, token, root, token.logicalLane, token.resourceID, token.diagnosticPath, DurableFrontier{Bytes: 1024}, token.reachability, token.logicalObligations, nil)
	if err != nil {
		source.Release()
		t.Fatalf("newImportedResourceToken(1024): %v", err)
	}
	cloned, err := imported.cloneOwnedPinnedDirectory(imported.logicalLane, imported.resourceID, imported.diagnosticPath, DurableFrontier{Bytes: 2048}, imported.reachability, imported.logicalObligations, nil, nil)
	if err != nil {
		imported.Release()
		source.Release()
		t.Fatalf("cloneOwnedPinnedDirectory(2048): %v", err)
	}
	source.Release()
	if _, err := registry.BeginTruncateAt(identity, ns, 1500); !errors.Is(err, ErrResourcePinned) {
		cloned.Release()
		imported.Release()
		t.Fatalf("BeginTruncateAt(1500) with imported clone@2048 = %v, want ErrResourcePinned", err)
	}
	if err := advanceTokenIdentityPinFrontier(cloned, 3072); err != nil {
		cloned.Release()
		imported.Release()
		t.Fatalf("advanceTokenIdentityPinFrontier(3072): %v", err)
	}
	if _, err := registry.BeginTruncateAt(identity, ns, 3000); !errors.Is(err, ErrResourcePinned) {
		cloned.Release()
		imported.Release()
		t.Fatalf("BeginTruncateAt(3000) after advanceTokenIdentityPinFrontier(3072) = %v, want ErrResourcePinned", err)
	}
	cloned.Release()
	imported.Release()
	if got := owner.Bytes(); got != 0 {
		t.Fatalf("importer owner bytes after release = %d, want 0", got)
	}
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("ActivePins after imported release = %d, want 0", got)
	}
}

func TestStableResourceSetBuilderMergeRollbackOnTruncateLease5922(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fillers int
	}{
		{name: "linear", fillers: 0},
		{name: "indexed", fillers: stableResourceEntryLinearLookupLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			registry := NewIdentityPinRegistry()
			defer registry.Close()
			filePath := filepath.Join(dir, "segment-000003.tca")
			if err := os.WriteFile(filePath, make([]byte, 2048), 0o600); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			f1, err := os.Open(filePath)
			if err != nil {
				t.Fatalf("Open f1: %v", err)
			}
			f2, err := os.Open(filePath)
			if err != nil {
				_ = f1.Close()
				t.Fatalf("Open f2: %v", err)
			}
			identity, err := StableIdentityFromFile(f1)
			if err != nil {
				t.Fatalf("StableIdentityFromFile: %v", err)
			}
			if err := registry.Observe(identity); err != nil {
				t.Fatalf("Observe: %v", err)
			}
			defer func() {
				if err := registry.Unobserve(identity); err != nil {
					t.Fatalf("Unobserve: %v", err)
				}
			}()

			builder := NewStableResourceSetBuilder()
			defer builder.Abandon()
			if tc.fillers > 0 {
				fillerPath := filepath.Join(dir, "filler.bin")
				if err := os.WriteFile(fillerPath, []byte("x"), 0o600); err != nil {
					t.Fatalf("WriteFile filler: %v", err)
				}
				for i := 0; i < tc.fillers; i++ {
					ff, err := os.Open(fillerPath)
					if err != nil {
						t.Fatalf("Open filler %d: %v", i, err)
					}
					if err := builder.Add(distinctPhysicalTokenFixture(t, ff, uint64(i+1000))); err != nil {
						t.Fatalf("builder.Add filler %d: %v", i, err)
					}
				}
			}

			ob1 := StableLogicalObligation{
				Class:        "column-asset",
				Kind:         "part",
				Namespace:    "events/column-assets",
				Generation:   1,
				PartID:       1,
				FileID:       3,
				Offset:       0,
				Length:       256,
				Reachability: ReachabilityColumnManifest,
				Digest:       [32]byte{1},
			}
			ob2 := StableLogicalObligation{
				Class:        "column-asset",
				Kind:         "part",
				Namespace:    "events/column-assets",
				Generation:   1,
				PartID:       2,
				FileID:       3,
				Offset:       256,
				Length:       768,
				Reachability: ReachabilityColumnManifest,
				Digest:       [32]byte{2},
			}
			tok1, err := NewStableResourceToken(StableResourceSpec{
				Kind:               ResourceColumnAsset,
				LogicalLane:        "events",
				ResourceID:         "seg-3",
				Generation:         1,
				DiagnosticPath:     "column_assets/events/segment-000003.tca",
				File:               f1,
				Frontier:           DurableFrontier{Bytes: 256},
				Reachability:       ReachabilityColumnManifest,
				LogicalObligations: []StableLogicalObligation{ob1},
				PinRegistry:        registry,
				ContentSynced:      true,
			})
			if err != nil {
				t.Fatalf("NewStableResourceToken tok1: %v", err)
			}
			if err := builder.Add(tok1); err != nil {
				t.Fatalf("builder.Add tok1: %v", err)
			}

			// Acquire a truncate lease at 256, then attempt to merge a higher-frontier
			// token (1024) for the same physical identity: advancing tok1's pin must fail
			// with ErrResourceDeletionInProgress BEFORE mutating entry frontier or obligations.
			const ns = "column_assets/events/segment-000003.tca"
			lease, err := registry.BeginTruncateAt(identity, ns, 256)
			if err != nil {
				t.Fatalf("BeginTruncateAt(256): %v", err)
			}
			tok2, err := NewStableResourceToken(StableResourceSpec{
				Kind:               ResourceColumnAsset,
				LogicalLane:        "events",
				ResourceID:         "seg-3",
				Generation:         1,
				DiagnosticPath:     "column_assets/events/segment-000003.tca",
				File:               f2,
				Frontier:           DurableFrontier{Bytes: 1024},
				Reachability:       ReachabilityColumnManifest,
				LogicalObligations: []StableLogicalObligation{ob2},
				ContentSynced:      true,
			})
			if err != nil {
				lease.Abort()
				t.Fatalf("NewStableResourceToken tok2: %v", err)
			}
			if err := builder.Add(tok2); !errors.Is(err, ErrResourceDeletionInProgress) {
				lease.Abort()
				t.Fatalf("builder.Add(tok2) during truncate@256 = %v, want ErrResourceDeletionInProgress", err)
			}
			lease.Abort()

			frozen, err := builder.Freeze()
			if err != nil {
				t.Fatalf("builder.Freeze: %v", err)
			}
			defer frozen.Release()
			descriptors, err := frozen.Descriptors()
			if err != nil {
				t.Fatalf("frozen.Descriptors: %v", err)
			}
			var found bool
			for _, desc := range descriptors {
				if desc.ResourceID() != "seg-3" {
					continue
				}
				found = true
				if desc.Frontier().Bytes != 256 {
					t.Fatalf("entry frontier mutated on failed merge: got=%d want=256", desc.Frontier().Bytes)
				}
				obligations := desc.LogicalObligations()
				if len(obligations) != 1 || obligations[0] != ob1 {
					t.Fatalf("entry logical obligations mutated on failed merge: got=%+v want=[%+v]", obligations, ob1)
				}
			}
			if !found {
				t.Fatal("missing seg-3 descriptor in frozen set")
			}
		})
	}
}
