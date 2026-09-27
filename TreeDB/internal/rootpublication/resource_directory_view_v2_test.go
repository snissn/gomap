package rootpublication

import (
	"bytes"
	"errors"
	"path/filepath"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestDependencyDirectoryV2ResourceClosureAppendAndLease(t *testing.T) {
	dir := t.TempDir()
	obligation := StableLogicalObligation{Class: "column", Kind: "chunk", Namespace: "main", Generation: 1, FileID: 1, Offset: 0, Length: 4, Reachability: ReachabilityColumnManifest, Digest: [32]byte{1}}
	second := obligation
	second.Offset, second.Digest = 4, [32]byte{2}
	token := stableTokenFixture(t, dir, "segment", 1, 20, ReachabilityColumnManifest, "segment", func(spec *StableResourceSpec) {
		spec.Kind = ResourceColumnAsset
		spec.LogicalObligations = []StableLogicalObligation{obligation, second}
	})
	builder := NewStableResourceSetBuilder()
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	source, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()
	records := make(map[string][]byte)
	physical, logical, err := WalkDependencyDirectoryChangesV2(source, nil, func(key, value []byte, deleted bool) error {
		if deleted {
			t.Fatal("new directory deleted a key")
		}
		records[string(key)] = bytes.Clone(value)
		return nil
	})
	if err != nil || physical != 1 || logical != 2 || len(records) != 3 {
		t.Fatalf("initial delta: %d %d %d %v", physical, logical, len(records), err)
	}
	p, err := pager.Open(filepath.Join(dir, "index.db"), 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.GrowTo(3); err != nil {
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
	releases := 0
	directory, err := NewDependencyDirectoryV2(p, DependencyDirectoryRefV2{RootPageID: 2, PhysicalCount: physical, LogicalCount: logical}, func() { releases++ })
	if err != nil {
		t.Fatal(err)
	}
	bound, err := BindDependencyDirectoryV2(source, directory)
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Release()
	directory.Release()
	source.Release()
	if releases != 0 {
		t.Fatal("binding did not retain the directory lease independently")
	}
	bound.rangeEntries(func(entry *stableResourceEntry) bool {
		if entry.logicalObligations.tail != nil || entry.logicalObligations.index != nil || entry.logicalObligations.count != 2 || entry.token == token {
			t.Fatal("binding retained logical history or predecessor token")
		}
		return true
	})
	view := bound.kindViews[ResourceColumnAsset]
	if view.logicalMembership != nil || view.logicalMembershipCount != 2 || view.directory != directory {
		t.Fatal("binding rebuilt retained aggregate membership")
	}

	removed, work, err := CloneStableResourceSetApplyingLogicalObligationMutation(bound, StableLogicalObligationMutation{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, Removed: []StableLogicalObligation{obligation},
	})
	if err != nil {
		t.Fatal(err)
	}
	if work.DroppedObligations != 1 {
		t.Fatalf("removal accounting: %+v", work)
	}
	if _, err := removed.DependencyDirectoryV2(); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("unsealed removal directory accepted: %v", err)
	}
	deletes := 0
	physical, logical, err = WalkDependencyDirectoryChangesV2(removed, directory, func(key, value []byte, deleted bool) error {
		if !deleted || !bytes.Equal(key, DependencyLogicalKeyV2(obligation)) {
			t.Fatalf("unexpected removal delta: %x deleted=%t", key, deleted)
		}
		deletes++
		return nil
	})
	if err != nil || physical != 1 || logical != 1 || deletes != 1 {
		t.Fatalf("removal delta: %d %d %d %v", physical, logical, deletes, err)
	}
	removed.rangeEntries(func(entry *stableResourceEntry) bool {
		if _, found, err := entry.logicalObligations.lookup(obligation, nil); err != nil || found {
			t.Fatalf("removed key visible: %t %v", found, err)
		}
		return true
	})
	removedDescriptors, err := removed.Descriptors()
	if err != nil || len(removedDescriptors) != 1 || len(removedDescriptors[0].LogicalObligations()) != 1 || removedDescriptors[0].LogicalObligations()[0] != second {
		t.Fatalf("materialized removal lost exact contents: %+v %v", removedDescriptors, err)
	}
	removed.Release()
	added := obligation
	added.Offset, added.Digest = 8, [32]byte{3}
	producerToken, err := bound.Tokens()[0].cloneSharedPinned("main", "segment", filepath.Join("maindb", "value_vlog", "segment"), DurableFrontier{Bytes: 20}, ReachabilityColumnManifest, []StableLogicalObligation{added}, nil)
	if err != nil {
		t.Fatal(err)
	}
	producerBuilder := NewStableResourceSetBuilder()
	if err := producerBuilder.Add(producerToken); err != nil {
		t.Fatal(err)
	}
	producer, err := producerBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Release()
	clone, err := CloneStableResourceSetExcludingKinds(bound)
	if err != nil {
		t.Fatal(err)
	}
	appendBuilder := NewStableResourceSetBuilder()
	defer appendBuilder.Abandon()
	if err := appendBuilder.Merge(clone); err != nil {
		t.Fatal(err)
	}
	mutation := StableLogicalObligationMutation{ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, Added: []StableLogicalObligation{added}}
	if _, err := appendBuilder.MergeAppendOnlyLogicalObligations(producer, mutation); err != nil {
		t.Fatal(err)
	}
	candidate, err := appendBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.Release()
	writes := 0
	physical, logical, err = WalkDependencyDirectoryChangesV2(candidate, directory, func(key, value []byte, deleted bool) error {
		writes++
		if deleted || !bytes.Equal(key, DependencyLogicalKeyV2(added)) {
			t.Fatalf("unexpected append write %x deleted=%t", key, deleted)
		}
		return nil
	})
	if err != nil || physical != 1 || logical != 3 || writes != 1 {
		t.Fatalf("append delta: %d %d writes=%d err=%v", physical, logical, writes, err)
	}
	conflicting := obligation
	conflicting.Checksum++
	entry := findStableResourceLogical(view.logical, bound.Tokens()[0].logicalKey())
	if _, err := entry.logicalObligations.appendCertified([]StableLogicalObligation{conflicting}, nil); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("conflicting exact key accepted: %v", err)
	}
	want := []StableLogicalObligation{obligation, second, added}
	descriptors, err := candidate.Descriptors()
	if err != nil || len(descriptors) != 1 || len(descriptors[0].LogicalObligations()) != 3 {
		t.Fatalf("materialized inherited append: %+v %v", descriptors, err)
	}
	if err := ValidateStableResourceSetLogicalObligations(candidate, StableLogicalObligationRequirements{ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, Obligations: want}); err != nil {
		t.Fatal(err)
	}
	stop := errors.New("callback stop")
	if err := candidate.WalkLogicalObligations(func(StableResourcePhysicalDescriptor, StableLogicalObligation) error { return stop }); !errors.Is(err, stop) {
		t.Fatalf("callback error lost: %v", err)
	}
	// A malformed inherited logical value must fail even when the page checksum
	// is valid; physical descriptors never read this page.
	badImage := make([]byte, page.PageSize)
	bad := node.NewNode(badImage)
	bad.SetPageID(2)
	bad.SetType(page.PageTypeLeaf)
	for _, key := range keys {
		value := records[key]
		if key[0] == dependencyLogicalKeyV2 {
			value = []byte{0}
		}
		if err := bad.AddLeafEntry([]byte(key), value, node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatal(err)
		}
	}
	bad.UpdateChecksum()
	if err := p.Write(2, badImage); err != nil {
		t.Fatal(err)
	}
	physicalOnly := candidate.PhysicalDescriptors()
	if len(physicalOnly) != 1 || physicalOnly[0].LogicalObligationCount != 3 {
		t.Fatalf("physical metadata depends on logical page: %+v", physicalOnly)
	}
	if got, err := candidate.Descriptors(); err == nil || got != nil {
		t.Fatalf("corruption produced partial materialization: %+v %v", got, err)
	}
	if err := ValidateStableResourceSetLogicalObligations(candidate, StableLogicalObligationRequirements{ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, Obligations: want}); err == nil {
		t.Fatal("corrupt inherited record validated")
	}
	candidate.Release()
	bound.Release()
	if releases != 1 {
		t.Fatalf("directory lease release count=%d", releases)
	}
}

func TestDependencyDirectoryV2EmptyClosureLease(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.GrowTo(3); err != nil {
		t.Fatal(err)
	}
	image := make([]byte, page.PageSize)
	n := node.NewNode(image)
	n.SetPageID(2)
	n.SetType(page.PageTypeLeaf)
	n.UpdateChecksum()
	if err := p.Write(2, image); err != nil {
		t.Fatal(err)
	}
	releases := 0
	directory, err := NewDependencyDirectoryV2(p, DependencyDirectoryRefV2{RootPageID: 2}, func() { releases++ })
	if err != nil {
		t.Fatal(err)
	}
	builder := NewStableResourceSetBuilder()
	source, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()
	bound, err := BindDependencyDirectoryV2(source, directory)
	if err != nil {
		t.Fatal(err)
	}
	directory.Release()
	clone, err := CloneStableResourceSetExcludingKinds(bound)
	if err != nil {
		t.Fatal(err)
	}
	bound.Release()
	if releases != 0 {
		t.Fatal("empty clone lost root lease")
	}
	got, err := clone.DependencyDirectoryV2()
	if err != nil || got != directory {
		t.Fatalf("empty directory lost: %p %v", got, err)
	}
	builder = NewStableResourceSetBuilder()
	if err := builder.Merge(clone); err != nil {
		t.Fatal(err)
	}
	builder.Abandon()
	clone.Release()
	if releases != 1 {
		t.Fatalf("empty directory releases=%d", releases)
	}
}
