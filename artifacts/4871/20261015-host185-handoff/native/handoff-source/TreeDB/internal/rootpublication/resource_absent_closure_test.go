package rootpublication

import (
	"errors"
	"testing"
)

func TestGenericEmptyCloneUsesAbsentClosure(t *testing.T) {
	b := NewStableResourceSetBuilder()
	s, err := b.Freeze()
	if err != nil || s == nil || s.Owner() != ResourceOwnerBuilder {
		t.Fatalf("exported Freeze contract: %v %v", s, err)
	}
	clone, err := CloneStableResourceSetExcludingKinds(s)
	if err != nil {
		t.Fatal(err)
	}
	if clone != nil {
		clone.Release()
		t.Error("zero-edge generic clone retained an owner")
	}
	if err = s.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	if _, err = CloneStableResourceSetExcludingKinds(s); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("released source: %v", err)
	}
}

func TestEmptyDirectoryCloneRetainsIndependentCustody(t *testing.T) {
	source, err := NewStableResourceSetBuilder().Freeze()
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
	clone, err := CloneStableResourceSetExcludingKinds(bound)
	if err != nil {
		t.Fatal(err)
	}
	if clone == nil {
		t.Fatal("empty directory is an independent ownership edge")
	}
	bound.Release()
	got, err := clone.DependencyDirectoryV2()
	if err != nil || got != directory {
		t.Fatalf("lost independent directory: %p %v", got, err)
	}
	if err = clone.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
}
