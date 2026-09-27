package rootpublication

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func bindPhysicalUnionTestDirectory(t *testing.T, source *StableResourceSet, release func()) *StableResourceSet {
	t.Helper()
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 4096)
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
		if _, err := physicalDurabilityUnion(invalid); !errors.Is(err, ErrDurableRootLineage) {
			t.Fatalf("invalid physical group accepted: %v", err)
		}
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
			if union, err := physicalDurabilityUnion([]*PreparedRootCandidate{one.candidate, two.candidate}); union != nil || !errors.Is(err, ErrResourceConflict) {
				t.Fatalf("physical conflict union=%v error=%v", union, err)
			}
			if one.resources.Owner() != ResourceOwnerCandidate || two.resources.Owner() != ResourceOwnerCandidate || one.tx.Owner() != ResourceOwnerCandidate || two.tx.Owner() != ResourceOwnerCandidate {
				t.Fatal("conflict transferred ownership")
			}
		})
	}
}
