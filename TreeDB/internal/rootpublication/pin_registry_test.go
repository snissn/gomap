package rootpublication

import (
	"errors"
	"testing"
)

func TestIdentityPinRegistrySerializesPinAndDelete(t *testing.T) {
	registry := NewIdentityPinRegistry()
	identity := StableIdentity{Device: 1, File: 2}
	if err := registry.Observe(identity); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if err := registry.Pin(identity); err != nil {
		t.Fatalf("Pin: %v", err)
	}
	zero := registry.WaitUnpinned(identity)
	if _, err := registry.BeginDelete(identity); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("BeginDelete while pinned = %v, want ErrResourcePinned", err)
	}
	select {
	case <-zero:
		t.Fatal("WaitUnpinned closed before Release")
	default:
	}
	if err := registry.Release(identity); err != nil {
		t.Fatalf("Release: %v", err)
	}
	<-zero
	lease, err := registry.BeginDelete(identity)
	if err != nil {
		t.Fatalf("BeginDelete after release: %v", err)
	}
	if err := registry.Pin(identity); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("Pin during deletion = %v, want ErrResourcePinned", err)
	}
	lease.Abort()
	if err := registry.Pin(identity); err != nil {
		t.Fatalf("Pin after deletion lease: %v", err)
	}
	if err := registry.Release(identity); err != nil {
		t.Fatalf("final Release: %v", err)
	}
	lease, err = registry.BeginDelete(identity)
	if err != nil {
		t.Fatalf("final BeginDelete: %v", err)
	}
	lease.CommitDeleted()
	if err := registry.Pin(identity); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("Pin retired identity = %v, want ErrResourceConflict", err)
	}
	if err := registry.Unobserve(identity); err != nil {
		t.Fatalf("Unobserve: %v", err)
	}
}
