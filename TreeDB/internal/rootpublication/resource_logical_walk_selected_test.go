package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"testing"
)

func TestSelectedLogicalWalkPreadmitsBorrowedScratch(t *testing.T) {
	obligation := appendMutationTestObligation(1)
	obligation.Reachability = ReachabilityIndexFile
	token := stableTokenFixture(t, t.TempDir(), "segment", 1, 20, ReachabilityIndexFile, "segment", func(spec *StableResourceSpec) {
		spec.Kind = ResourceIndex
		spec.LogicalObligations = []StableLogicalObligation{obligation}
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
	directory := importedDirectoryFixture(t, source)
	defer directory.Release()
	bound, err := BindDependencyDirectoryV2(source, directory)
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Release()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	selected, err := ImportStableResourceSetMetadata(&owner, bound)
	if err != nil {
		t.Fatal(err)
	}
	defer selected.Release()
	budget := &registryBudget{cap: 1 << 24}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	before := owner.Bytes()
	budget.cap = budget.bytes
	seen := 0
	err = selected.WalkLogicalObligations(func(_ StableResourcePhysicalDescriptor, _ StableLogicalObligation) error {
		seen++
		return nil
	})
	if !errors.Is(err, retainedalloc.ErrCapacity) || seen != 0 || owner.Bytes() != before {
		t.Fatalf("unadmitted logical walk err=%v visits=%d bytes=%d/%d", err, seen, owner.Bytes(), before)
	}
	budget.cap = 1 << 24
	err = selected.WalkLogicalObligations(func(_ StableResourcePhysicalDescriptor, got StableLogicalObligation) error {
		seen++
		if got != obligation {
			t.Fatalf("logical value %#v", got)
		}
		if owner.Bytes() <= before {
			t.Fatal("borrowed scratch is not charged")
		}
		return nil
	})
	if err != nil || seen != 1 || owner.Bytes() != before {
		t.Fatalf("walk err=%v visits=%d bytes=%d/%d", err, seen, owner.Bytes(), before)
	}
}
