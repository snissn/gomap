package rootpublication

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// Two physical owners share one directory and are extended in the same merge.
// Reusing point scratch must not confuse their borrowed owner keys or retain
// temporary backing after refusal, successful transfer, or final old-root release.
func TestOwnedDirectoryMultiPhysicalAppendScopeAndRefusal(t *testing.T) {
	builder := NewStableResourceSetBuilder()
	defer builder.Abandon()
	var oldValues, added []StableLogicalObligation
	for i, name := range []string{"first", "second"} {
		old := appendMutationTestObligation(uint64(i + 1))
		old.Offset = 0
		oldValues = append(oldValues, old)
		next := appendMutationTestObligation(uint64(i + 3))
		next.Offset = 8
		added = append(added, next)
		token := stableTokenFixture(t, t.TempDir(), name, 1, 20, ReachabilityColumnManifest, name, func(spec *StableResourceSpec) {
			spec.Kind, spec.LogicalObligations = ResourceColumnAsset, []StableLogicalObligation{old}
		})
		if err := builder.Add(token); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 24}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline, governor := owner.Bytes(), budget.bytes
	imported, err := ImportStableResourceSetMetadata(&owner, source)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { imported.Release() }()
	directory, released := importedOwnedDirectoryLifetimeFixture(t, source, &owner)
	defer func() {
		if directory != nil {
			directory.Release()
		}
	}()
	bound, err := BindDependencyDirectoryV2(imported, directory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { bound.Release() }()
	// Joining disjoint entries from one directory must not admit an overlap or
	// substitute another directory, even when its records and owner agree.
	beforeGuards, budgetBeforeGuards := owner.Bytes(), budget.bytes
	otherDirectory, otherReleased := importedOwnedDirectoryLifetimeFixture(t, source, &owner)
	defer func() {
		if otherDirectory != nil {
			otherDirectory.Release()
		}
	}()
	otherBound, err := BindDependencyDirectoryV2(imported, otherDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { otherBound.Release() }()
	for _, tc := range []struct {
		name string
		set  *StableResourceSet
		want error
	}{
		{"overlap", bound, ErrResourceOwnership},
		{"different_directory", otherBound, ErrResourceConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before, beforeBudget := owner.Bytes(), budget.bytes
			left, err := CloneStableResourceSetExcludingKinds(bound)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { left.Release() }()
			right, err := CloneStableResourceSetExcludingKinds(tc.set)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { right.Release() }()
			out, err := NewStableResourceSetBuilderWithMetadata(&owner)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if out != nil {
					out.Abandon()
				}
			}()
			if err = out.Merge(left); err != nil {
				t.Fatal(err)
			}
			if err = out.Merge(right); !errors.Is(err, tc.want) || right.Owner() != ResourceOwnerBuilder {
				t.Fatalf("refused merge err=%v owner=%v", err, right.Owner())
			}
			if err = out.AbandonWithError(); err != nil {
				t.Fatal(err)
			}
			out = nil
			left.Release()
			left = nil
			right.Release()
			right = nil
			if owner.Bytes() != before || budget.bytes != beforeBudget {
				t.Fatal("refused directory merge retained private backing")
			}
			assertDependencyDirectoryRecordStreamV2(t, bound, oldValues)
			assertDependencyDirectoryRecordStreamV2(t, otherBound, oldValues)
		})
	}
	otherBound.Release()
	otherBound = nil
	otherDirectory.Release()
	otherDirectory = nil
	if otherReleased.calls != 1 || owner.Bytes() != beforeGuards || budget.bytes != budgetBeforeGuards {
		t.Fatal("guard controls retained their independent directory")
	}
	producerBuilder := NewStableResourceSetBuilder()
	defer producerBuilder.Abandon()
	for _, original := range source.Tokens() {
		var value StableLogicalObligation
		for i, old := range oldValues {
			if original.logicalObligations[0] == old {
				value = added[i]
			}
		}
		if value.Length == 0 {
			t.Fatal("missing original producer obligation")
		}
		token, err := original.cloneSharedPinned(original.logicalLane, original.resourceID, original.diagnosticPath, original.frontier, original.reachability, []StableLogicalObligation{value}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err = producerBuilder.Add(token); err != nil {
			t.Fatal(err)
		}
	}
	genericIncoming, err := producerBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer genericIncoming.Release()
	incoming, err := ImportStableResourceSetMetadata(&owner, genericIncoming)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { incoming.Release() }()
	mutation := StableLogicalObligationMutation{ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, Added: added}
	merge := func(refuse bool) *StableResourceSet {
		t.Helper()
		before, beforeBudget := owner.Bytes(), budget.bytes
		clone, err := CloneStableResourceSetExcludingKinds(bound)
		if err != nil {
			t.Fatal(err)
		}
		defer clone.Release()
		out, err := NewStableResourceSetBuilderWithMetadata(&owner)
		if err != nil {
			t.Fatal(err)
		}
		if err = out.Merge(clone); err != nil {
			t.Fatal(err)
		}
		if refuse {
			budget.cap = budget.bytes
		}
		_, err = out.MergeAppendOnlyLogicalObligations(incoming, mutation)
		budget.cap = 1 << 24
		if refuse {
			if !errors.Is(err, retainedalloc.ErrCapacity) {
				t.Fatalf("merge refusal: %v", err)
			}
			if err := out.AbandonWithError(); err != nil {
				t.Fatal(err)
			}
			clone.Release()
			if owner.Bytes() != before || budget.bytes != beforeBudget {
				t.Fatal("refused merge retained private backing")
			}
			assertDependencyDirectoryRecordStreamV2(t, bound, oldValues)
			return nil
		}
		if err != nil {
			t.Fatal(err)
		}
		result, err := out.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	merge(true)
	candidate := merge(false)
	defer func() { candidate.Release() }()
	assertDependencyDirectoryRecordStreamV2(t, candidate, append(append([]StableLogicalObligation{}, oldValues...), added...))
	assertDependencyDirectoryRecordStreamV2(t, bound, oldValues)
	if candidate.MetadataOwner() != &owner {
		t.Fatal("merge changed original allocation owner")
	}
	// The candidate survives every opening directory/source edge independently.
	incoming.Release()
	incoming = nil
	imported.Release()
	imported = nil
	bound.Release()
	bound = nil
	directory.Release()
	assertDependencyDirectoryRecordStreamV2(t, candidate, append(append([]StableLogicalObligation{}, oldValues...), added...))
	if released.calls != 0 {
		t.Fatal("candidate lost original directory lease")
	}
	if err := candidate.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	candidate = nil
	directory = nil
	if released.calls != 1 || owner.Bytes() != baseline || budget.bytes != governor {
		t.Fatalf("final directory calls=%d bytes=%d/%d governor=%d/%d", released.calls, owner.Bytes(), baseline, budget.bytes, governor)
	}
}
