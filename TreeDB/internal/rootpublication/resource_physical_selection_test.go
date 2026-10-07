package rootpublication

import (
	"errors"
	"reflect"
	"testing"
)

func TestStablePhysicalKindSelectionRetainsExactAuthorityAndOtherDomains(t *testing.T) {
	dir := t.TempDir()
	builder := NewStableResourceSetBuilder()
	for i, name := range []string{"old", "live", "column"} {
		kind, field := ResourceOuterLeafPack, ReachabilityOuterLeafPackedPointer
		if name == "column" {
			kind, field = ResourceColumnAsset, ReachabilityColumnManifest
		}
		token := stableTokenFixture(t, dir, name, uint64(i+1), 8, field, name, func(spec *StableResourceSpec) { spec.Kind = kind })
		if err := builder.Add(token); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()
	var live StableIdentity
	var want []StableResourcePhysicalDescriptor
	for _, entry := range source.PhysicalDescriptors() {
		if entry.ResourceID() == "live" {
			live = entry.Identity()
		}
		if entry.ResourceID() != "old" {
			want = append(want, entry)
		}
	}
	clone, err := CloneStableResourceSetSelectingPhysicalKind(source, ResourceOuterLeafPack, []StableIdentity{live})
	if err != nil {
		t.Fatal(err)
	}
	defer clone.Release()
	source.Release()
	if got := clone.PhysicalDescriptors(); !reflect.DeepEqual(got, want) {
		t.Fatalf("exact physical authority changed: got=%+v want=%+v", got, want)
	}
	if _, _, err := clone.DependencyManifestV1(); err != nil {
		t.Fatalf("independent clone lost source ownership: %v", err)
	}
}

func TestStablePhysicalKindSelectionFailsClosed(t *testing.T) {
	token := stableTokenFixture(t, t.TempDir(), "pack", 7, 8, ReachabilityOuterLeafPackedPointer, "pack", func(spec *StableResourceSpec) { spec.Kind = ResourceOuterLeafPack })
	builder := NewStableResourceSetBuilder()
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	source, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()
	identity := source.PhysicalDescriptors()[0].Identity()
	missing := identity
	missing.Generation++
	for name, ids := range map[string][]StableIdentity{
		"missing": {missing}, "malformed": {{}}, "duplicate": {identity, identity},
	} {
		t.Run(name, func(t *testing.T) {
			clone, err := CloneStableResourceSetSelectingPhysicalKind(source, ResourceOuterLeafPack, ids)
			if clone != nil {
				clone.Release()
				t.Fatal("returned candidate despite invalid selection")
			}
			if !errors.Is(err, ErrUnresolvedResource) && !errors.Is(err, ErrResourceConflict) {
				t.Fatalf("err=%v", err)
			}
		})
	}
	source.Release()
	if clone, err := CloneStableResourceSetSelectingPhysicalKind(source, ResourceOuterLeafPack, nil); clone != nil || !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("released source accepted: %v", err)
	}
}
