package rootpublication

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func selectorSizedTempFile(t *testing.T, size int64) *os.File {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "selector-*")
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(size); err != nil {
		file.Close()
		t.Fatal(err)
	}
	return file
}

func TestCloneStableResourceForSelector(t *testing.T) {
	file := selectorSizedTempFile(t, 64)
	defer file.Close()
	second := appendMutationTestObligation(2)
	tokens := make([]*StableResourceToken, 0, stableResourceEntryLinearLookupLimit+1)
	for id := uint64(1); id <= stableResourceEntryLinearLookupLimit+1; id++ {
		obligation := appendMutationTestObligation(id + 10)
		if id == 7 {
			obligation = second
		}
		tokens = append(tokens, appendMutationDistinctResourceToken(t, file, id, obligation))
	}
	source := freezeAppendMutationResources(t, tokens...)
	if source.kindViews == nil {
		t.Fatal("fixture did not exercise indexed resource view")
	}
	selector := StableResourceSelector{Kind: ResourceColumnAsset, LogicalLane: "columns", ResourceID: "7", PhysicalGeneration: 1, Obligation: second}

	selected, err := CloneStableResourceForSelector(source, selector)
	if err != nil {
		t.Fatal(err)
	}
	source.Release()
	if err := selected.validateResolved(); err != nil {
		selected.Release()
		t.Fatalf("selected after source release: %v", err)
	}
	descriptors := selected.Descriptors()
	if len(descriptors) != 1 {
		t.Fatalf("descriptors=%d want 1", len(descriptors))
	}
	if got := descriptors[0].LogicalObligations(); len(got) != 1 || got[0] != second {
		t.Fatalf("obligations=%v want [%v]", got, second)
	}
	if descriptors[0].Frontier().Bytes != 1 {
		t.Fatalf("frontier=%d want 1", descriptors[0].Frontier().Bytes)
	}
	selected.Release()

	for _, tc := range []struct {
		name   string
		mutate func(*StableResourceSelector)
		want   error
	}{
		{name: "key mismatch", mutate: func(s *StableResourceSelector) { s.ResourceID = "8" }, want: ErrUnresolvedResource},
		{name: "absent obligation", mutate: func(s *StableResourceSelector) { s.Obligation = appendMutationTestObligation(9) }, want: ErrUnresolvedResource},
		{name: "conflicting obligation", mutate: func(s *StableResourceSelector) { s.Obligation.Checksum++ }, want: ErrResourceConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			live := freezeAppendMutationResources(t, appendMutationDistinctResourceToken(t, file, 7, second))
			defer live.Release()
			candidate := selector
			tc.mutate(&candidate)
			got, err := CloneStableResourceForSelector(live, candidate)
			if got != nil {
				got.Release()
				t.Fatal("unexpected selected resource")
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("error=%v want %v", err, tc.want)
			}
		})
	}
}

func TestCloneStableResourceForSelectorRejectsReleasedSource(t *testing.T) {
	file := selectorSizedTempFile(t, 8)
	defer file.Close()
	obligation := appendMutationTestObligation(1)
	source := freezeAppendMutationResources(t, appendMutationResourceToken(t, file, ResourceColumnAsset, "1", 8, ReachabilityColumnManifest, obligation))
	source.Release()
	_, err := CloneStableResourceForSelector(source, StableResourceSelector{Kind: ResourceColumnAsset, LogicalLane: string(ResourceColumnAsset), ResourceID: "1", PhysicalGeneration: 1, Obligation: obligation})
	if !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("error=%v want ownership", err)
	}
}

func TestCloneStableResourceForSelectorPinsIdentityAndRejectsNamespaceRebound(t *testing.T) {
	dir := t.TempDir()
	parent, err := OpenStableParent(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	path := filepath.Join(dir, "asset.bin")
	if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Stable handles permit namespace rebinding while open, including on Windows.
	file, err := OpenStableChildFile(parent, "asset.bin", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	registry := NewIdentityPinRegistry()
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{Parent: parent, LinkedResource: file, ParentGeneration: 1, Operation: NamespaceCreate, NewName: "asset.bin", DiagnosticPath: "columns/asset.bin"})
	if err != nil {
		t.Fatal(err)
	}
	if err := namespace.Stabilize(); err != nil {
		t.Fatal(err)
	}
	obligation := appendMutationTestObligation(1)
	token, err := NewStableResourceToken(StableResourceSpec{Kind: ResourceColumnAsset, LogicalLane: "columns", ResourceID: "1", Generation: 1, DiagnosticPath: "columns/asset.bin", File: file, Frontier: DurableFrontier{Bytes: 8}, Digest: sha256.Sum256([]byte("asset")), Reachability: ReachabilityColumnManifest, LogicalObligations: []StableLogicalObligation{obligation}, Namespace: namespace, ContentSynced: true, PinRegistry: registry, OnRelease: func() { _ = registry.Unobserve(identity) }})
	if err != nil {
		t.Fatal(err)
	}
	source := freezeAppendMutationResources(t, token)
	selector := StableResourceSelector{Kind: ResourceColumnAsset, LogicalLane: "columns", ResourceID: "1", PhysicalGeneration: 1, Obligation: obligation}
	selected, err := CloneStableResourceForSelector(source, selector)
	if err != nil {
		source.Release()
		t.Fatal(err)
	}
	source.Release()
	if got := registry.Stats().ActivePins; got != 1 {
		selected.Release()
		t.Fatalf("pins=%d want 1", got)
	}
	selected.Release()
	if got := registry.Stats().ActivePins; got != 0 {
		t.Fatalf("pins=%d want 0", got)
	}

	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	namespace2, err := NewStableNamespaceToken(StableNamespaceSpec{Parent: parent, LinkedResource: file, ParentGeneration: 1, Operation: NamespaceCreate, NewName: "asset.bin", DiagnosticPath: "columns/asset.bin"})
	if err != nil {
		t.Fatal(err)
	}
	if err := namespace2.Stabilize(); err != nil {
		t.Fatal(err)
	}
	reboundToken, err := NewStableResourceToken(StableResourceSpec{Kind: ResourceColumnAsset, LogicalLane: "columns", ResourceID: "1", Generation: 1, DiagnosticPath: "columns/asset.bin", File: file, Frontier: DurableFrontier{Bytes: 8}, Digest: sha256.Sum256([]byte("asset")), Reachability: ReachabilityColumnManifest, LogicalObligations: []StableLogicalObligation{obligation}, Namespace: namespace2, ContentSynced: true, PinRegistry: registry, OnRelease: func() { _ = registry.Unobserve(identity) }})
	if err != nil {
		t.Fatal(err)
	}
	rebound := freezeAppendMutationResources(t, reboundToken)
	defer rebound.Release()
	if err := os.Rename(path, filepath.Join(dir, "old.bin")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := CloneStableResourceForSelector(rebound, selector)
	if got != nil {
		got.Release()
		t.Fatal("unexpected rebound selection")
	}
	if !errors.Is(err, ErrResourceConflict) && !errors.Is(err, ErrUnresolvedResource) {
		t.Fatalf("rebound error=%v", err)
	}
}
