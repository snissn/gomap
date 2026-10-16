package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
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

func TestOwnedPhysicalKindSelectionPreservesAdmissionAndLastBorrow(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	enrollmentBaseline := owner.Bytes()
	genericBuilder := NewStableResourceSetBuilder()
	for i, name := range []string{"obsolete", "live", "column"} {
		kind, field := ResourceOuterLeafPack, ReachabilityOuterLeafPackedPointer
		if name == "column" {
			kind, field = ResourceColumnAsset, ReachabilityColumnManifest
		}
		token := stableTokenFixture(t, t.TempDir(), name, uint64(i+1), 8, field, name, func(spec *StableResourceSpec) { spec.Kind = kind })
		if err := genericBuilder.Add(token); err != nil {
			t.Fatal(err)
		}
	}
	generic, err := genericBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer generic.Release()
	source, err := ImportStableResourceSetMetadata(&owner, generic)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()
	diagnostics, err := source.AcquirePhysicalDiagnostics()
	if err != nil {
		t.Fatal(err)
	}
	var identity StableIdentity
	for _, descriptor := range diagnostics.Physical() {
		if descriptor.ResourceID() == "live" {
			identity = descriptor.Identity()
		}
	}
	diagnostics.Close()
	if !identity.valid() {
		t.Fatal("missing fixture identity")
	}
	baseline := owner.Bytes()
	missing := identity
	missing.Generation++
	for name, selected := range map[string][]StableIdentity{"duplicate": {identity, identity}, "missing": {missing}, "malformed": {{}}} {
		t.Run(name, func(t *testing.T) {
			result, err := CloneStableResourceSetSelectingPhysicalKind(source, ResourceOuterLeafPack, selected)
			if result != nil {
				result.Release()
				t.Fatal("invalid selection returned output")
			}
			if err == nil || owner.Bytes() != baseline {
				t.Fatalf("invalid selection err=%v bytes=%d want=%d", err, owner.Bytes(), baseline)
			}
		})
	}
	budget.cap = baseline
	refused, err := CloneStableResourceSetSelectingPhysicalKind(source, ResourceOuterLeafPack, []StableIdentity{identity})
	if refused != nil {
		refused.Release()
		t.Fatal("capacity refusal returned output")
	}
	if !errors.Is(err, retainedalloc.ErrCapacity) || owner.Bytes() != baseline || source.Owner() != ResourceOwnerBuilder {
		t.Fatalf("refusal changed custody: err=%v bytes=%d/%d owner=%v", err, owner.Bytes(), baseline, source.Owner())
	}
	budget.cap = 1 << 20
	clone, err := CloneStableResourceSetSelectingPhysicalKind(source, ResourceOuterLeafPack, []StableIdentity{identity})
	if err != nil {
		t.Fatal(err)
	}
	defer clone.Release()
	if clone.MetadataOwner() != &owner || clone.Len() != 2 {
		t.Fatalf("selection lost admitted owner: owner=%p expected=%p entries=%d", clone.MetadataOwner(), &owner, clone.Len())
	}
	builder, err := NewStableResourceSetBuilderWithMetadata(&owner)
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Abandon()
	if err := builder.Merge(clone); err != nil {
		t.Fatalf("selected physical clone cannot join publication: %v", err)
	}
	output, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer output.Release()
	borrow, err := output.borrowSelectedKindViews()
	if err != nil {
		t.Fatal(err)
	}
	source.Release()
	generic.Release()
	output.Release()
	for _, cell := range borrow.backing.cells {
		rangeStableResourceLogicalIndex(cell.view.logical, func(entry *stableResourceEntry) bool {
			if err := activeEntryToken(*entry).WithPinnedFile(func(file *os.File) error { _, err := file.Stat(); return err }); err != nil {
				t.Errorf("borrow lost original file: %v", err)
			}
			return true
		})
	}
	if owner.Bytes() == 0 {
		t.Fatal("scoped borrower lost admitted backing")
	}
	borrow.release()
	if owner.Bytes() != enrollmentBaseline {
		t.Fatalf("last borrower retained metadata: bytes=%d enrollment=%d", owner.Bytes(), enrollmentBaseline)
	}
	enrollment.Close()
	if owner.Bytes() != 0 {
		t.Fatalf("completed enrollment retained %d bytes", owner.Bytes())
	}
}
