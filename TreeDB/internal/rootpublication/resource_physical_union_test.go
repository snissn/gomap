package rootpublication

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func bindPhysicalUnionTestDirectory(t *testing.T, source *StableResourceSet, release func()) *StableResourceSet {
	t.Helper()
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if err := p.GrowTo(3); err != nil {
		t.Fatal(err)
	}
	records := make(map[string][]byte)
	physical, logical, err := WalkDependencyDirectoryChangesV2(source, nil, func(key, value []byte, deleted bool) error {
		if deleted {
			t.Fatal("initial directory deletion")
		}
		records[string(key)] = bytes.Clone(value)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(records))
	for key := range records {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	image := make([]byte, page.PageSize)
	n := node.NewNode(image)
	n.SetPageID(2)
	n.SetType(page.PageTypeLeaf)
	for _, key := range keys {
		if err := n.AddLeafEntry([]byte(key), records[key], node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatal(err)
		}
	}
	n.UpdateChecksum()
	if err := p.Write(2, image); err != nil {
		t.Fatal(err)
	}
	directory, err := NewDependencyDirectoryV2(p, DependencyDirectoryRefV2{RootPageID: 2, PhysicalCount: physical, LogicalCount: logical}, p.PageCount(), release)
	if err != nil {
		t.Fatal(err)
	}
	defer directory.Release()
	bound, err := BindDependencyDirectoryV2(source, directory)
	if err != nil {
		t.Fatal(err)
	}
	return bound
}

func TestPhysicalDurabilityUnionDirectoryRemovalRetryAndSnapshot(t *testing.T) {
	obligation := StableLogicalObligation{Class: "column", Kind: "chunk", Namespace: "main", Generation: 1, FileID: 1, Length: 4, Reachability: ReachabilityColumnManifest, Digest: [32]byte{1}}
	removed := obligation
	removed.Offset, removed.Digest = 4, [32]byte{2}
	token := stableTokenFixture(t, t.TempDir(), "segment", 1, 20, ReachabilityColumnManifest, "segment", func(spec *StableResourceSpec) {
		spec.Kind = ResourceColumnAsset
		spec.Frontier.Bytes = 16
		spec.LogicalObligations = []StableLogicalObligation{obligation, removed}
	})
	builder := NewStableResourceSetBuilder()
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	firstSource, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	secondToken, err := token.cloneSharedPinned(token.logicalLane, token.resourceID, token.diagnosticPath, DurableFrontier{Bytes: 20}, token.reachability, []StableLogicalObligation{obligation}, nil)
	if err != nil {
		t.Fatal(err)
	}
	builder = NewStableResourceSetBuilder()
	if err := builder.Add(secondToken); err != nil {
		t.Fatal(err)
	}
	secondSource, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	var releases [2]int
	first := bindPhysicalUnionTestDirectory(t, firstSource, func() { releases[0]++ })
	second := bindPhysicalUnionTestDirectory(t, secondSource, func() { releases[1]++ })
	firstSource.Release()
	secondSource.Release()
	if union, err := UnionStableResourceSets(first, second); union != nil || !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("generic directory union=%v error=%v", union, err)
	}
	lineage := durableLineage(1)
	one, two := newDurableRootTransactionFixture(t, lineage, 2), newDurableRootTransactionFixture(t, lineage, 3)
	one.resources.releaseFrom(ResourceOwnerCandidate)
	two.resources.releaseFrom(ResourceOwnerCandidate)
	one.candidate.extensions.resourceSet, two.candidate.extensions.resourceSet = first, second
	if err := first.transfer(ResourceOwnerBuilder, ResourceOwnerCandidate); err != nil {
		t.Fatal(err)
	}
	if err := second.transfer(ResourceOwnerBuilder, ResourceOwnerCandidate); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range [][]*PreparedRootCandidate{{one.candidate, one.candidate}, {two.candidate, one.candidate}, {one.candidate, candidate(t, 3, 1)}} {
		if _, err := testPhysicalDurabilityCount(invalid); !errors.Is(err, ErrDurableRootLineage) {
			t.Fatalf("invalid count group accepted: %v", err)
		}
		if _, err := physicalDurabilityUnion(invalid); !errors.Is(err, ErrDurableRootLineage) {
			t.Fatalf("invalid physical group accepted: %v", err)
		}
	}
	if count, err := testPhysicalDurabilityCount([]*PreparedRootCandidate{one.candidate, two.candidate}); err != nil || count != 1 {
		t.Fatalf("directory-backed count=%d error=%v", count, err)
	}
	var sealed *StableResourceSet
	attempts := 0
	retry := errors.New("retry before meta")
	c, err := New(Options{Clock: NewFakeClock(time.Unix(1, 0)), Publisher: PublisherFunc(func(_ context.Context, candidate *PreparedRootCandidate) PublishResult {
		attempts++
		physical := candidate.Resources().PhysicalDescriptors()
		if len(physical) != 1 || physical[0].Frontier().Bytes != 20 || physical[0].LogicalObligationCountAvailable {
			return PublishResult{Outcome: PublishAmbiguous, Err: errors.New("invalid physical union frontier/count")}
		}
		if descriptors, err := candidate.Resources().Descriptors(); descriptors != nil || !errors.Is(err, ErrResourceOwnership) {
			return PublishResult{Outcome: PublishAmbiguous, Err: errors.New("physical union exported logical metadata")}
		}
		if attempts == 1 {
			return PublishResult{Outcome: PublishRetryableFailure, Err: retry}
		}
		var err error
		sealed, err = CloneStableResourceSetExcludingKinds(second)
		if err != nil {
			return PublishResult{Outcome: PublishAmbiguous, Err: err}
		}
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), one.candidate); err != nil {
		t.Fatal(err)
	}
	if err := c.Enqueue(context.Background(), two.candidate); err != nil {
		t.Fatal(err)
	}
	snapshot, err := c.CaptureReachability()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Release()
	if err := c.WaitThrough(context.Background(), 3); !errors.Is(err, retry) {
		t.Fatalf("retry=%v", err)
	}
	if releases != [2]int{} {
		t.Fatalf("retry released directory leases: %v", releases)
	}
	if err := c.WaitThrough(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	defer sealed.Release()
	if got := mustStableResourceDescriptors(t, sealed); len(got) != 1 || len(got[0].LogicalObligations()) != 1 || got[0].LogicalObligations()[0] != obligation {
		t.Fatal("seal retained removed logical obligation")
	}
	if releases != [2]int{} {
		t.Fatalf("snapshot lost directory leases: %v", releases)
	}
	snapshot.Release()
	if releases != [2]int{1, 0} {
		t.Fatalf("snapshot release=%v", releases)
	}
	sealed.Release()
	if releases != [2]int{1, 1} {
		t.Fatalf("final release=%v", releases)
	}
}

func TestPhysicalReachabilityUnionEmptyDirectoriesOwnsLeasesAndRejectsLogicalAuthority(t *testing.T) {
	var releases [2]int
	sources := make([]*StableResourceSet, 2)
	for i := range sources {
		empty, err := NewStableResourceSetBuilder().Freeze()
		if err != nil {
			t.Fatal(err)
		}
		index := i
		sources[i] = bindPhysicalUnionTestDirectory(t, empty, func() { releases[index]++ })
		empty.Release()
	}
	if union, err := UnionStableResourceSets(sources...); union != nil || !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("generic empty-directory union=%v error=%v", union, err)
	}
	owned, err := ClonePhysicalReachabilityUnion(sources...)
	if err != nil {
		t.Fatal(err)
	}
	defer owned.Release()
	for _, source := range sources {
		source.Release()
	}
	if releases != [2]int{} {
		t.Fatalf("snapshot lost empty-directory leases: %v", releases)
	}
	checks := []func() error{
		func() error { _, err := owned.Descriptors(); return err },
		func() error {
			return owned.WalkLogicalObligations(func(StableResourcePhysicalDescriptor, StableLogicalObligation) error { return nil })
		},
		func() error { _, err := owned.DependencyDirectoryV2(); return err },
		func() error { _, _, err := owned.DependencyManifestV1(); return err },
		func() error { _, err := CloneStableResourceSetExcludingKinds(owned); return err },
		func() error {
			return ValidateStableResourceSetLogicalObligations(owned, StableLogicalObligationRequirements{})
		},
		func() error { _, err := owned.BytesNotCoveredBy(nil); return err },
		func() error { _, err := UnionStableResourceSets(owned); return err },
	}
	for i, check := range checks {
		if err := check(); !errors.Is(err, ErrResourceOwnership) {
			t.Fatalf("physical capability check %d=%v", i, err)
		}
	}
	second, err := ClonePhysicalReachabilityUnion(owned)
	if err != nil {
		t.Fatal(err)
	}
	owned.Release()
	if releases != [2]int{} {
		t.Fatalf("second snapshot lost leases: %v", releases)
	}
	second.Release()
	if releases != [2]int{1, 1} {
		t.Fatalf("directory lease balance=%v", releases)
	}
}

func TestPhysicalDurabilityUnionRejectsPhysicalConflictsBeforeTransfer(t *testing.T) {
	for _, conflict := range []string{"identity", "digest", "frontier"} {
		t.Run(conflict, func(t *testing.T) {
			firstToken := stableTokenFixture(t, t.TempDir(), "first", 1, 4, ReachabilityColumnManifest, "same", func(spec *StableResourceSpec) {
				spec.Kind = ResourceColumnAsset
			})
			secondToken := stableTokenFixture(t, t.TempDir(), "second", 1, 8, ReachabilityColumnManifest, "same", func(spec *StableResourceSpec) {
				spec.Kind = firstToken.Kind()
				spec.ResourceID = firstToken.ResourceID()
				if conflict != "identity" {
					spec.StableIdentityOverride = firstToken.Identity()
				}
				if conflict == "digest" {
					spec.Digest = [32]byte{9}
				}
			})
			one, two := newDurableRootTransactionFixture(t, durableLineage(1), 2), newDurableRootTransactionFixture(t, durableLineage(1), 3)
			for i, fixture := range []*durableRootTransactionFixture{one, two} {
				builder := NewStableResourceSetBuilder()
				if err := builder.Add([]*StableResourceToken{firstToken, secondToken}[i]); err != nil {
					t.Fatal(err)
				}
				resources, err := builder.Freeze()
				if err != nil {
					t.Fatal(err)
				}
				fixture.resources.releaseFrom(ResourceOwnerCandidate)
				fixture.resources, fixture.candidate.extensions.resourceSet = resources, resources
				if err := resources.transfer(ResourceOwnerBuilder, ResourceOwnerCandidate); err != nil {
					t.Fatal(err)
				}
				defer fixture.candidate.Abandon()
			}
			if conflict == "frontier" {
				// A malformed sparse frontier must still fail validation in the
				// physical-only path, before ownership can transfer.
				two.resources.rangeEntries(func(entry *stableResourceEntry) bool {
					entry.frontier.RIDCount = 1
					return true
				})
			}
			if count, err := testPhysicalDurabilityCount([]*PreparedRootCandidate{one.candidate, two.candidate}); count != 0 || !errors.Is(err, ErrResourceConflict) {
				t.Fatalf("physical conflict count=%d error=%v", count, err)
			}
			if union, err := physicalDurabilityUnion([]*PreparedRootCandidate{one.candidate, two.candidate}); union != nil || !errors.Is(err, ErrResourceConflict) {
				t.Fatalf("physical conflict union=%v error=%v", union, err)
			}
			if one.resources.Owner() != ResourceOwnerCandidate || two.resources.Owner() != ResourceOwnerCandidate || one.tx.Owner() != ResourceOwnerCandidate || two.tx.Owner() != ResourceOwnerCandidate {
				t.Fatal("conflict transferred ownership")
			}
		})
	}
}

func TestPhysicalDurabilitySingletonAdmissionRejectsInvalidAuthority(t *testing.T) {
	for _, mode := range []string{"lineage", "sequence", "released"} {
		t.Run(mode, func(t *testing.T) {
			fixture := newDurableRootTransactionFixture(t, durableLineage(1), 2)
			want := ErrDurableRootLineage
			switch mode {
			case "lineage":
				fixture.tx.input.Lineage = DurableRootLineageID{}
			case "sequence":
				fixture.candidate.frontier.commitSeq++
			case "released":
				fixture.resources.releaseFrom(ResourceOwnerCandidate)
				want = ErrResourceOwnership
			}
			coordinator, err := New(Options{Clock: NewFakeClock(time.Unix(1, 0)), Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
				return PublishResult{Outcome: PublishAmbiguous, Err: errors.New("unexpected publish")}
			})})
			if err != nil {
				t.Fatal(err)
			}
			defer stopClean(t, coordinator)
			if err := coordinator.Enqueue(context.Background(), fixture.candidate); !errors.Is(err, want) {
				t.Fatalf("enqueue=%v want %v", err, want)
			}
			if fixture.tx.Owner() != ResourceOwnerCandidate || fixture.activates.Load() != 0 || coordinator.Stats().PendingCommits != 0 {
				t.Fatal("rejected singleton transferred or activated")
			}
			if err := fixture.candidate.Abandon(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func testPhysicalDurabilityCount(candidates []*PreparedRootCandidate) (int, error) {
	sets, err := physicalDurabilityMemberSets(candidates)
	if err != nil {
		return 0, err
	}
	return physicalDurabilityCount(sets)
}

func TestConsumedStableResourceUnionRefusesFlatAndDirectoryInputs(t *testing.T) {
	for _, directory := range []bool{false, true} {
		name := "flat"
		if directory {
			name = "directory"
		}
		t.Run(name, func(t *testing.T) {
			obligation := StableLogicalObligation{Class: "column", Kind: "chunk", Namespace: "main", Generation: 1, FileID: 1, Length: 4, Reachability: ReachabilityColumnManifest, Digest: [32]byte{1}}
			token := stableTokenFixture(t, t.TempDir(), "segment", 1, 20, ReachabilityColumnManifest, "segment", func(spec *StableResourceSpec) {
				spec.Kind = ResourceColumnAsset
				spec.LogicalObligations = []StableLogicalObligation{obligation}
			})
			builder := NewStableResourceSetBuilder()
			if err := builder.Add(token); err != nil {
				t.Fatal(err)
			}
			original, err := builder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			defer original.Release()
			child := original
			releases := 0
			if directory {
				child = bindPhysicalUnionTestDirectory(t, original, func() { releases++ })
				defer child.Release()
			}
			parent := NewStableResourceSetBuilder()
			defer parent.Abandon()
			if err := parent.Merge(child); err != nil {
				t.Fatal(err)
			}
			if union, err := UnionStableResourceSets(child); union != nil || !errors.Is(err, ErrResourceOwnership) {
				t.Fatalf("consumed metadata union: %v", err)
			}
			if union, err := ClonePhysicalReachabilityUnion(child); union != nil || !errors.Is(err, ErrResourceOwnership) {
				t.Fatalf("consumed reachability union: %v", err)
			}
			retained, err := parent.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			descriptors, err := retained.Descriptors()
			if err != nil || len(descriptors) != 1 || len(descriptors[0].LogicalObligations()) != 1 {
				t.Fatalf("transferred metadata lost: %+v %v", descriptors, err)
			}
			retained.Release()
			if directory && releases != 1 {
				t.Fatalf("directory releases=%d", releases)
			}
		})
	}
}

func TestPhysicalReachabilityUnionFailedDirectoryRetainCleansOwnedLeases(t *testing.T) {
	var releases [2]int
	sources := make([]*StableResourceSet, 2)
	for i := range sources {
		empty, err := NewStableResourceSetBuilder().Freeze()
		if err != nil {
			t.Fatal(err)
		}
		index := i
		sources[i] = bindPhysicalUnionTestDirectory(t, empty, func() { releases[index]++ })
		empty.Release()
		defer sources[i].Release()
	}
	good := sources[0].extras.empty
	saturated := sources[1].extras.empty
	prior := saturated.refs.Load()
	// Retain must refuse overflow without consuming either source or leaking
	// any independently retained roots accumulated before the failed retain.
	saturated.refs.Store(math.MaxInt64)
	defer saturated.refs.Store(prior)
	for attempt := 0; attempt < 16; attempt++ {
		if union, err := ClonePhysicalReachabilityUnion(sources...); union != nil || !errors.Is(err, ErrResourceOwnership) {
			t.Fatalf("saturated retain: %v", err)
		}
		if good.refs.Load() != 1 || releases != [2]int{} {
			t.Fatalf("failed clone leaked/released source leases: refs=%d releases=%v", good.refs.Load(), releases)
		}
	}
	if clone, err := CloneStableResourceSetExcludingKinds(sources[1]); clone != nil || !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("saturated empty clone: %v", err)
	}
}

// Counting must fit only its admitted borrowed backing, even when there is no
// room to clone tokens, frontiers or an owned publication union.
func TestOwnedPhysicalDurabilityCountBorrowedAdmissionAndCustody(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 64 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "count.bin", "12345678")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	var sets []*StableResourceSet
	for i := 0; i < 32; i++ {
		frontier := NewRIDFrontier([]uint64{1, uint64(i + 2)})
		frontier.Bytes = 8
		sets = append(sets, admittedCoalescingFixture(t, &owner, registry, file, fmt.Sprint(i), frontier, nil))
	}
	before, pins := owner.Bytes(), registry.Stats().ActivePins
	budget.cap = before
	if count, err := physicalDurabilityCount(sets); count != 0 || !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("unadmitted count=%d error=%v", count, err)
	}
	if owner.Bytes() != before || registry.Stats().ActivePins != pins {
		t.Fatal("refusal changed original metadata or pins")
	}
	budget.cap = before + retainedalloc.AllocationCharge(uint64(len(sets))*uint64(unsafe.Sizeof(stableResourceEntry{})))
	if count, err := physicalDurabilityCount(sets); count != 1 || err != nil {
		t.Fatalf("borrowed count=%d error=%v", count, err)
	}
	if owner.Bytes() != before || registry.Stats().ActivePins != pins {
		t.Fatal("count retained metadata or changed original pins")
	}
	for i, set := range sets {
		set.rangeEntries(func(entry *stableResourceEntry) bool {
			if set.Owner() != ResourceOwnerBuilder || entry.frontier.RIDCount != 2 || entry.frontier.RIDs()[1] != uint64(i+2) {
				t.Fatal("count changed original owner/frontier")
			}
			return true
		})
		if err := set.ReleaseWithError(); err != nil {
			t.Fatal(err)
		}
	}
	enrollment.Close()
	if owner.Bytes() != 0 || budget.bytes != 0 || registry.Stats().ActivePins != 0 {
		t.Fatalf("final custody bytes=%d budget=%d pins=%d", owner.Bytes(), budget.bytes, registry.Stats().ActivePins)
	}
}

func TestOwnedPhysicalDurabilityCountCompatibility(t *testing.T) {
	for _, mode := range []string{"aliases", "immutable-generations", "mutable-kinds", "logical-remap", "mutable-digest", "immutable-digest", "stability", "frontier", "foreign-owner", "released", "transferred"} {
		t.Run(mode, func(t *testing.T) {
			var owner, foreign retainedalloc.Owner
			owner.Initialize(0)
			foreign.Initialize(0)
			registry := NewIdentityPinRegistry()
			defer registry.Close()
			file := writeStableResourceFixture(t, t.TempDir(), "first.bin", "12345678")
			other := file
			if mode == "logical-remap" {
				other = writeStableResourceFixture(t, t.TempDir(), "second.bin", "12345678")
			}
			for _, f := range []*os.File{file, other} {
				identity, err := StableIdentityFromFile(f)
				if err != nil {
					t.Fatal(err)
				}
				if err := registry.Observe(identity); err != nil {
					t.Fatal(err)
				}
				defer registry.Unobserve(identity)
			}
			var sets []*StableResourceSet
			for i, f := range []*os.File{file, other} {
				spec := StableResourceSpec{MetadataOwner: &owner, PinRegistry: registry, Kind: ResourceIndex, LogicalLane: fmt.Sprint(i), ResourceID: "index.db", Generation: 1, File: f, DiagnosticPath: "index.db", Frontier: DurableFrontier{Bytes: uint64(i + 1)}, Reachability: ReachabilityIndexFile}
				if mode == "logical-remap" {
					spec.LogicalLane = "same"
				}
				if mode == "immutable-generations" || mode == "immutable-digest" || (mode == "stability" && i == 1) {
					spec.Kind, spec.Reachability = ResourceOuterLeafPack, ReachabilityOuterLeafPackedPointer
					spec.Generation = uint64(i + 1)
				}
				if mode == "mutable-kinds" && i == 1 {
					spec.Kind, spec.Reachability = ResourceValueLog, ReachabilityValueLogPointer
				}
				if (mode == "mutable-digest" || mode == "immutable-digest") && i == 1 {
					spec.Digest = [32]byte{1}
				}
				if mode == "foreign-owner" && i == 1 {
					spec.MetadataOwner = &foreign
				}
				token, err := NewStableResourceToken(spec)
				if err != nil {
					t.Fatal(err)
				}
				builder, err := NewStableResourceSetBuilderWithMetadata(spec.MetadataOwner)
				if err != nil {
					t.Fatal(err)
				}
				if err := builder.Add(token); err != nil {
					t.Fatal(err)
				}
				set, err := builder.Freeze()
				if err != nil {
					t.Fatal(err)
				}
				sets = append(sets, set)
			}
			defer func() {
				for _, set := range sets {
					if err := set.ReleaseWithError(); err != nil {
						t.Error(err)
					}
				}
				if owner.Bytes() != 0 || foreign.Bytes() != 0 || registry.Stats().ActivePins != 0 {
					t.Errorf("final custody bytes=%d/%d pins=%d", owner.Bytes(), foreign.Bytes(), registry.Stats().ActivePins)
				}
			}()
			want, wantErr := 1, error(nil)
			switch mode {
			case "immutable-generations", "mutable-kinds":
				want = 2
			case "aliases":
			case "foreign-owner":
				want, wantErr = 0, ErrResourceOwnership
			case "released":
				if err := sets[1].ReleaseWithError(); err != nil {
					t.Fatal(err)
				}
				want, wantErr = 0, ErrResourceOwnership
			case "transferred":
				if err := sets[1].transfer(ResourceOwnerBuilder, ResourceOwnerTransferred); err != nil {
					t.Fatal(err)
				}
				defer sets[1].owner.Store(uint32(ResourceOwnerBuilder)) // Restore only test teardown custody.
				want, wantErr = 0, ErrResourceOwnership
			default:
				want, wantErr = 0, ErrResourceConflict
			}
			if mode == "frontier" {
				sets[1].rangeEntries(func(entry *stableResourceEntry) bool { entry.frontier.RIDCount = 1; return true })
			}
			before, pins := owner.Bytes(), registry.Stats().ActivePins
			count, err := physicalDurabilityCount(sets)
			if count != want || !errors.Is(err, wantErr) {
				t.Fatalf("count=%d error=%v want=%d/%v", count, err, want, wantErr)
			}
			if owner.Bytes() != before || registry.Stats().ActivePins != pins {
				t.Fatal("count/refusal changed original custody")
			}
		})
	}
}
