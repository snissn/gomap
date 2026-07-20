package rootpublication

import (
	"crypto/sha256"
	"testing"
)

func appendMutationTestObligation(partID uint64) StableLogicalObligation {
	obligation := StableLogicalObligation{
		Class: "column-asset-ref-v1", Kind: "tcs1_part_image", Namespace: "columns",
		Generation: 1, PartID: partID, FileID: 1, Offset: int64(partID * 8), Length: 8,
		Checksum: uint32(partID), Reachability: ReachabilityColumnManifest,
	}
	obligation.Digest = sha256.Sum256([]byte{byte(partID)})
	return obligation
}

func TestStableLogicalObligationAppendDiscardDoesNotPoisonRetryBase(t *testing.T) {
	baseObligation := appendMutationTestObligation(1)
	added := appendMutationTestObligation(2)
	base := newStableLogicalObligationView([]StableLogicalObligation{baseObligation})

	prepared, err := base.appendCertified([]StableLogicalObligation{added}, nil)
	if err != nil {
		t.Fatalf("prepare first append: %v", err)
	}
	if prepared.count != 2 {
		t.Fatalf("prepared count=%d want 2", prepared.count)
	}
	// Simulate an enclosing merge/CAS failure by discarding prepared. Retrying
	// from the exact same immutable base must remain legal and exact.
	retry, err := base.appendCertified([]StableLogicalObligation{added}, nil)
	if err != nil {
		t.Fatalf("retry append from unchanged base: %v", err)
	}
	if got := retry.slice(); len(got) != 2 || got[0] != baseObligation || got[1] != added {
		t.Fatalf("retry obligations=%+v want exact base+addition", got)
	}
	if got := base.slice(); len(got) != 1 || got[0] != baseObligation {
		t.Fatalf("base mutated by discarded candidate: %+v", got)
	}
}

func TestStableLogicalObligationAppendSupportsIndependentCandidateBranches(t *testing.T) {
	baseObligation := appendMutationTestObligation(1)
	leftAdded := appendMutationTestObligation(2)
	rightAdded := appendMutationTestObligation(3)
	base := newStableLogicalObligationView([]StableLogicalObligation{baseObligation})

	left, err := base.appendCertified([]StableLogicalObligation{leftAdded}, nil)
	if err != nil {
		t.Fatalf("left candidate: %v", err)
	}
	right, err := base.appendCertified([]StableLogicalObligation{rightAdded}, nil)
	if err != nil {
		t.Fatalf("right candidate: %v", err)
	}
	if got := left.slice(); len(got) != 2 || got[1] != leftAdded {
		t.Fatalf("left branch=%+v", got)
	}
	if got := right.slice(); len(got) != 2 || got[1] != rightAdded {
		t.Fatalf("right branch=%+v", got)
	}
	if got := base.slice(); len(got) != 1 || got[0] != baseObligation {
		t.Fatalf("base changed by independent candidates: %+v", got)
	}
}

func TestStableLogicalObligationMutationRequiresExactFinalRequirements(t *testing.T) {
	retained := appendMutationTestObligation(1)
	added := appendMutationTestObligation(2)
	removed := appendMutationTestObligation(3)
	requirements, err := NormalizeStableLogicalObligationRequirements(StableLogicalObligationRequirements{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest},
		Obligations:  []StableLogicalObligation{retained, added},
	})
	if err != nil {
		t.Fatal(err)
	}
	mutation := StableLogicalObligationMutation{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest},
		Added:        []StableLogicalObligation{added},
		Removed:      []StableLogicalObligation{removed},
	}
	if err := ValidateStableLogicalObligationMutationFinalRequirements(mutation, requirements); err != nil {
		t.Fatalf("valid exact mutation: %v", err)
	}
	missingAdded := requirements
	missingAdded.Obligations = []StableLogicalObligation{retained}
	if err := ValidateStableLogicalObligationMutationFinalRequirements(mutation, missingAdded); err == nil {
		t.Fatal("missing added obligation was accepted")
	}
	retainsRemoved, err := NormalizeStableLogicalObligationRequirements(StableLogicalObligationRequirements{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest},
		Obligations:  []StableLogicalObligation{retained, added, removed},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateStableLogicalObligationMutationFinalRequirements(mutation, retainsRemoved); err == nil {
		t.Fatal("retained removed obligation was accepted")
	}
}

func TestStableLogicalObligationMutationFinalRequirementsSearchesWithinFieldGroup(t *testing.T) {
	firstField := appendMutationTestObligation(1)
	firstField.Class = "z-class"
	firstField.Reachability = ReachabilityColumnManifest
	secondField := appendMutationTestObligation(2)
	secondField.Class = "a-class"
	secondField.Reachability = ReachabilityTypedColumnValue
	requirements, err := NormalizeStableLogicalObligationRequirements(StableLogicalObligationRequirements{
		ScopedFields: []ReachabilityField{ReachabilityTypedColumnValue, ReachabilityColumnManifest},
		Obligations:  []StableLogicalObligation{secondField, firstField},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(requirements.Obligations) != 2 || requirements.Obligations[0] != firstField || requirements.Obligations[1] != secondField {
		t.Fatalf("requirements not grouped by field as expected: %+v", requirements)
	}
	if err := ValidateStableLogicalObligationMutationFinalRequirements(StableLogicalObligationMutation{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest, ReachabilityTypedColumnValue},
		Added:        []StableLogicalObligation{secondField},
	}, requirements); err != nil {
		t.Fatalf("multi-field mutation-local lookup: %v", err)
	}
}
