package rootpublication

import (
	"bytes"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"sync"
	"testing"
)

func TestOwnedManifestKeepsEncodedMetadataAfterEntryRelease(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "manifest.bin", "12345678")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	obligation := appendMutationTestObligation(1)
	obligation.Reachability = ReachabilityIndexFile
	set := admittedCoalescingFixture(t, &owner, registry, file, "lane", DurableFrontier{Bytes: 8}, []StableLogicalObligation{obligation})
	manifest, work, err := set.DependencyManifestV1()
	if err != nil {
		t.Fatal(err)
	}
	if work.EntriesEncoded != 1 {
		t.Fatal(work)
	}
	digest := manifest.digest
	set.Release()
	if owner.Bytes() == 0 {
		t.Fatal("manifest still aliases refunded encoded metadata")
	}
	if manifest.digest != digest || ownedManifestEntryCount(t, manifest) != 1 {
		t.Fatal("manifest lost exact immutable metadata")
	}
	manifest.ReleaseOwnedMetadataV1()
	manifest.ReleaseOwnedMetadataV1()
	if owner.Bytes() != 0 {
		t.Fatalf("manifest last edge retained %d bytes", owner.Bytes())
	}
}

func TestOwnedManifestCacheIndependentAliasesAndRefusal(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "manifest.bin", "12345678")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	v := appendMutationTestObligation(1)
	v.Reachability = ReachabilityIndexFile
	set := admittedCoalescingFixture(t, &owner, registry, file, "lane", NewRIDFrontier([]uint64{2, 7}), []StableLogicalObligation{v})
	before := owner.Bytes()
	budget.cap = before
	if _, _, err = set.DependencyManifestV1(); !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("refusal=%v", err)
	}
	var entry *stableResourceEntry
	rangeOwnedManifestEntriesV1(set.kindViews.get(ResourceIndex).logical, func(e *stableResourceEntry) bool { entry = e; return false })
	if owner.Bytes() != before || entry.dependencyManifestV1.value != nil || set.Owner() != ResourceOwnerBuilder {
		t.Fatal("refusal retained private metadata or changed source")
	}
	budget.cap = 1 << 20
	first, w, err := set.DependencyManifestV1()
	if err != nil || w.EntriesEncoded != 1 {
		t.Fatalf("first=%+v %v", w, err)
	}
	encoded := first.entries[0]
	second, w, err := set.DependencyManifestV1()
	if err != nil || w.EntriesEncoded != 0 || second.entries[0] != encoded {
		t.Fatalf("cache=%+v %v", w, err)
	}
	genericIdentity := identity
	genericIdentity.Generation = 1
	generic, err := NewDependencyManifestV1([]DependencyManifestEntryV1{{Kind: ResourceIndex, LogicalLane: "lane", ResourceID: "index.db", DiagnosticPath: "index.db", Identity: genericIdentity, Generation: 1, Frontier: NewRIDFrontier([]uint64{2, 7}), Reachability: []ReachabilityField{ReachabilityIndexFile}, LogicalObligations: []StableLogicalObligation{v}}})
	if err != nil {
		t.Fatal(err)
	}
	if first.digest != generic.digest || !bytes.Equal(first.entries[0].encoded, encodeDependencyManifestEntryV1(generic.entries[0].entry)) {
		t.Fatal("selected codec changed canonical stream")
	}
	first.ReleaseOwnedMetadataV1()
	set.Release()
	if owner.Bytes() <= baseline || ownedManifestEntryCount(t, second) != 1 || registry.Stats().ActivePins != 0 {
		t.Fatal("independent manifest lost metadata or prolonged physical pins")
	}
	second.ReleaseOwnedMetadataV1()
	if owner.Bytes() != baseline {
		t.Fatalf("manifest alias leak=%d baseline%d", owner.Bytes(), baseline)
	}
	// Unselected manifests retain their public post-release diagnostic semantics.
	generic.ReleaseOwnedMetadataV1()
	if len(generic.Entries()) != 1 {
		t.Fatal("generic diagnostics cleared")
	}
}

// Selected diagnostics cannot pass borrowed strings to a generic constructor
// whose returned diagnostic lifetime is independent of the selected Owner.
func TestOwnedManifestRejectsUnscopedEntries(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "scope.bin", "12345678")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	set := admittedCoalescingFixture(t, &owner, registry, file, "lane", DurableFrontier{Bytes: 8}, nil)
	manifest, _, err := set.DependencyManifestV1()
	if err != nil {
		t.Fatal(err)
	}
	defer manifest.ReleaseOwnedMetadataV1()
	defer set.Release()
	if len(manifest.Entries()) != 0 {
		t.Fatal("selected raw Entries exports borrowed metadata without admitted diagnostic custody")
	}
}

func ownedManifestEntryCount(t *testing.T, manifest *DependencyManifestV1) int {
	t.Helper()
	count := -1
	if err := manifest.WithEntriesV1(func(entries []DependencyManifestEntryV1) error { count = len(entries); return nil }); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestOwnedManifestScopeRetainsActualMetadataAndRefusesScratch(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "scope.bin", "12345678")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	set := admittedCoalescingFixture(t, &owner, registry, file, "lane", DurableFrontier{Bytes: 8}, nil)
	manifest, _, err := set.DependencyManifestV1()
	if err != nil {
		t.Fatal(err)
	}
	set.Release()
	before := owner.Bytes()
	budget.cap = before
	visited := false
	if err = manifest.WithEntriesV1(func([]DependencyManifestEntryV1) error { visited = true; return nil }); !errors.Is(err, retainedalloc.ErrCapacity) || visited || owner.Bytes() != before {
		t.Fatalf("refused scope: %v visited=%v bytes=%d want=%d", err, visited, owner.Bytes(), before)
	}
	budget.cap = 1 << 20
	entered, resume := make(chan struct{}), make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	var scopeErr error
	go func() {
		defer wg.Done()
		scopeErr = manifest.WithEntriesV1(func(entries []DependencyManifestEntryV1) error {
			close(entered)
			<-resume
			if len(entries) != 1 || entries[0].LogicalLane != "lane" || entries[0].DiagnosticPath != "index.db" {
				return ErrResourceOwnership
			}
			return nil
		})
	}()
	<-entered
	manifest.ReleaseOwnedMetadataV1()
	manifest.ReleaseOwnedMetadataV1()
	if owner.Bytes() <= baseline {
		t.Fatal("active scope lost actual charge")
	}
	if err = manifest.WithEntriesV1(func([]DependencyManifestEntryV1) error { t.Error("released owner accepted another scope"); return nil }); !errors.Is(err, ErrResourceOwnership) {
		t.Fatal(err)
	}
	close(resume)
	wg.Wait()
	if scopeErr != nil || owner.Bytes() != baseline {
		t.Fatalf("scope completion: %v metadata%d baseline%d", scopeErr, owner.Bytes(), baseline)
	}
}
