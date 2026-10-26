package rootpublication

import (
	"errors"
	"testing"
)

func TestIdentityPinRegistryIsRetiredRequiresKnownCompletedDeletion(t *testing.T) {
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	identity := StableIdentity{Platform: "test", VolumeID: 1, ObjectID: [16]byte{1}}
	if registry.IsRetired(StableIdentity{}) || registry.IsRetired(identity) {
		t.Fatal("invalid or unknown identity classified as retired")
	}
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	if registry.IsRetired(identity) {
		t.Fatal("live observation classified as retired")
	}
	lease, err := registry.BeginDelete(identity)
	if err != nil {
		t.Fatal(err)
	}
	if registry.IsRetired(identity) {
		t.Fatal("in-progress deletion classified as retired")
	}
	lease.Abort()
	if registry.IsRetired(identity) {
		t.Fatal("aborted deletion classified as retired")
	}
	lease, err = registry.BeginDelete(identity)
	if err != nil {
		t.Fatal(err)
	}
	lease.CommitDeleted()
	if !registry.IsRetired(identity) {
		t.Fatal("observed completed deletion not classified as retired")
	}
	if pin, err := registry.Pin(identity); err == nil || pin != nil {
		t.Fatal("retirement query revived pin authority")
	}
	if err := registry.Unobserve(identity); err != nil {
		t.Fatal(err)
	}
	if registry.IsRetired(identity) {
		t.Fatal("forgotten identity remained positively known")
	}
}

// The omission witness is produced only by refused observation of the exact
// physical identity. Pin failures remain fail-closed without that witness.
func TestObservationRetirementWitnessPreservesConflictAndIdentity(t *testing.T) {
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	identity := testStableIdentity(17)
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	lease, err := registry.BeginDelete(identity)
	if err != nil {
		t.Fatal(err)
	}
	check := func() {
		t.Helper()
		err := registry.Observe(identity)
		var witness *ResourceObservationRetirementError
		if !errors.Is(err, ErrResourceConflict) || !errors.As(err, &witness) || !SamePhysicalIdentity(witness.Identity(), identity) {
			t.Fatalf("refused observation lost exact physical retirement: %v", err)
		}
		if _, err := registry.Pin(identity); errors.As(err, &witness) || err == nil {
			t.Fatalf("pin failure granted a discovery omission witness: %v", err)
		}
	}
	check()
	lease.Abort()
	if err := registry.Observe(identity); err != nil {
		t.Fatalf("aborted deletion prevented later observation: %v", err)
	}
	if err := registry.Unobserve(identity); err != nil {
		t.Fatal(err)
	}
	lease, err = registry.BeginDelete(identity)
	if err != nil {
		t.Fatal(err)
	}
	lease.CommitDeleted()
	check()
}
