package rootpublication

import "testing"

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
