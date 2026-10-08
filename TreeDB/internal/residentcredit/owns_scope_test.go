package residentcredit

import "testing"

func TestResidentOwnsScopeUsesActualOriginalCreatorThroughClose(t *testing.T) {
	owner, err := NewOrdinary(128 << 20)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := NewOrdinary(128 << 20)
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Close()
	scope, err := owner.NewOrdinaryScope()
	if err != nil {
		t.Fatal(err)
	}
	before := owner.Stats()
	if !owner.OwnsScope(scope) || foreign.OwnsScope(scope) || owner.OwnsScope(nil) {
		t.Fatal("structural/foreign owner accepted")
	}
	if owner.Stats() != before {
		t.Fatal("ownership check debited or retained")
	}
	owner.Close()
	if !owner.OwnsScope(scope) {
		t.Fatal("creator lost while original scope retained")
	}
	scope.ReleaseStableMetadata()
	if owner.OwnsScope(scope) || owner.Stats().Live != 0 {
		t.Fatal("released scope retained authority")
	}
}
