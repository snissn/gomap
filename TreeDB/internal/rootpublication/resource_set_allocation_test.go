package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"testing"
	"time"
)

func TestOwnedSetWrapperFreezeRefusalPreservesSource(t *testing.T) {
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
	file := writeStableResourceFixture(t, t.TempDir(), "wrapper.bin", "12345678")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	source := admittedCoalescingFixture(t, &owner, registry, file, "lane", DurableFrontier{Bytes: 8}, nil)
	builder := NewStableResourceSetBuilder()
	defer builder.Abandon()
	if err = builder.Merge(source); err != nil {
		t.Fatal(err)
	}
	before := owner.Bytes()
	budget.cap = before
	result, err := builder.Freeze()
	if !errors.Is(err, retainedalloc.ErrCapacity) || result != nil {
		if result != nil {
			result.Release()
		}
		t.Fatalf("freeze allocated an unadmitted selected wrapper: result=%v error=%v", result != nil, err)
	}
	if owner.Bytes() != before || builder.State() != ResourceOwnerBuilder {
		t.Fatal("refusal changed source ownership/capacity")
	}
	budget.cap = 1 << 20
	result, err = builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if result.pinHighWater != nil {
		t.Fatal("selected wrapper retained an unadmitted high-water map")
	}
	result.Release()
	if owner.Bytes() != baseline {
		t.Fatalf("last wrapper retained %d bytes, want %d", owner.Bytes(), baseline)
	}
}

func TestOwnedBuilderAddCoalescesAndRefusesBeforeTransfer(t *testing.T) {
	var owner, foreign retainedalloc.Owner
	owner.Initialize(0)
	foreign.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "builder.bin", "12345678")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	capture := func(o *retainedalloc.Owner, bytes uint64) *StableResourceToken {
		token, e := NewStableResourceToken(StableResourceSpec{MetadataOwner: o, PinRegistry: registry, Kind: ResourceIndex, LogicalLane: "lane", ResourceID: "index.db", DiagnosticPath: "index.db", Generation: 1, File: file, Frontier: DurableFrontier{Bytes: bytes}, Reachability: ReachabilityIndexFile})
		if e != nil {
			t.Fatal(e)
		}
		return token
	}
	builder, err := NewStableResourceSetBuilderWithMetadata(&owner, ReachabilityIndexFile)
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Abandon()
	first := capture(&owner, 4)
	before := owner.Bytes()
	budget.cap = before
	if err = builder.Add(first); !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("refused Add=%v", err)
	}
	if ResourceOwnerState(first.owner.Load()) != ResourceOwnerToken || first.released.Load() || owner.Bytes() != before {
		t.Fatal("refused Add consumed input")
	}
	budget.cap = 1 << 20
	if err = builder.Add(first); err != nil {
		t.Fatal(err)
	}
	unsupported := capture(&foreign, 8)
	if err = builder.Add(unsupported); !errors.Is(err, ErrResourceOwnership) || ResourceOwnerState(unsupported.owner.Load()) != ResourceOwnerToken {
		t.Fatalf("foreign Add=%v", err)
	}
	unsupported.Release()
	second := capture(&owner, 8)
	if err = builder.Add(second); err != nil {
		t.Fatal(err)
	}
	result, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if result.Len() != 1 || result.kindViews.get(ResourceIndex).root.entries[0].frontier.Bytes != 8 {
		t.Fatal("actual repeated identity did not coalesce")
	}
	if builder.metadata != nil || builder.requiredFields != nil {
		t.Fatal("closed builder retained admitted constructor storage")
	}
	clone, err := CloneStableResourceSetExcludingKinds(result)
	if err != nil {
		t.Fatal(err)
	}
	result.Release()
	if clone.Len() != 1 {
		t.Fatal("wrapper disposal destroyed independent clone")
	}
	clone.Release()
	if owner.Bytes() != baseline || foreign.Bytes() != 0 || registry.Stats().ActivePins != 0 {
		t.Fatalf("retained owner=%d foreign=%d pins=%d", owner.Bytes(), foreign.Bytes(), registry.Stats().ActivePins)
	}
}

func TestOwnedRopeReleaseUsesAdmittedScratch(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "rope.bin", "12345678")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	source := admittedCoalescingFixture(t, &owner, registry, file, "lane", DurableFrontier{Bytes: 8}, nil)
	baseline := owner.Bytes()
	leaf := source.kindViews.get(ResourceIndex).root
	var roots [101]*stableResourceEntryNode
	for i := range roots {
		for j := 0; j < 64; j++ {
			if !leaf.retain() {
				t.Fatal("leaf retain")
			}
			roots[i], err = concatOwnedAdmittedStableResourceEntryNodes(leaf, roots[i], &owner)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	next := 0
	allocations := testing.AllocsPerRun(100, func() { roots[next].release(); roots[next] = nil; next++ })
	if allocations != 0 {
		t.Fatalf("release allocated %.0f unadmitted scratch objects", allocations)
	}
	if owner.Bytes() != baseline || source.Len() != 1 || registry.Stats().ActivePins == 0 {
		t.Fatal("release lost independent root or retained scratch")
	}
	source.Release()
	if owner.Bytes() != 0 || registry.Stats().ActivePins != 0 {
		t.Fatal("last root cleanup incomplete")
	}
}

func TestOwnedSetRawDiagnosticsCannotEscapeAdmission(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "diagnostics.bin", "12345678")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	source := admittedCoalescingFixture(t, &owner, registry, file, "lane", DurableFrontier{Bytes: 8}, nil)
	defer source.Release()
	if len(source.PhysicalDescriptors()) != 0 || len(source.Tokens()) != 0 || len(source.Stats(time.Now())) != 0 {
		t.Fatal("selected raw diagnostics escaped their actual allocation owner")
	}
	if values, err := source.Descriptors(); values != nil || !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("raw logical diagnostic escape=%v %v", values, err)
	}
}

func TestOwnedSetDiagnosticOwnerRefusalAliasesAndLastCleanup(t *testing.T) {
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
	file := writeStableResourceFixture(t, t.TempDir(), "diagnostic-owner.bin", "12345678")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	source := admittedCoalescingFixture(t, &owner, registry, file, "lane", DurableFrontier{Bytes: 8}, nil)
	before := owner.Bytes()
	budget.cap = before
	if view, e := source.AcquirePhysicalDiagnostics(); view != nil || !errors.Is(e, retainedalloc.ErrCapacity) {
		t.Fatalf("admission refusal=%v %v", view, e)
	}
	if owner.Bytes() != before || registry.Stats().ActivePins != 1 || source.Owner() != ResourceOwnerBuilder {
		t.Fatal("refusal consumed source")
	}
	budget.cap = 1 << 20
	physical, err := source.AcquirePhysicalDiagnostics()
	if err != nil {
		t.Fatal(err)
	}
	stats, err := source.AcquireStats(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(physical.Physical()) != 1 || len(stats.Stats()) != 1 || stats.Stats()[0].ActivePins != 1 || stats.Stats()[0].PinHighWater != 1 {
		t.Fatal("owned view lost exact physical/stat authority")
	}
	source.Release()
	if owner.Bytes() <= baseline || physical.Physical()[0].ResourceID() != "index.db" || registry.Stats().ActivePins != 1 {
		t.Fatal("source disposal stole actual diagnostic custody")
	}
	physical.Close()
	physical.Close()
	if owner.Bytes() <= baseline || registry.Stats().ActivePins != 1 {
		t.Fatal("remaining statistics owner lost physical operation custody")
	}
	stats.Close()
	stats.Close()
	if owner.Bytes() != baseline || registry.Stats().ActivePins != 0 || physical.Physical() != nil || stats.Stats() != nil {
		t.Fatalf("last diagnostic owner retained backing/pins: bytes=%d baseline=%d", owner.Bytes(), baseline)
	}
	if _, err = source.AcquirePhysicalDiagnostics(); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("released diagnostic resurrected=%v", err)
	}
}
