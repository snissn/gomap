package rootpublication

import (
	"errors"
	"os"
	"testing"
)

func TestCheckpointRotationFilePinAdmissionAndRetirement(t *testing.T) {
	f := writeStableResourceFixture(t, t.TempDir(), "sealed", "old bytes")
	id, err := StableIdentityFromFile(f)
	if err != nil {
		t.Fatal(err)
	}
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	if err := registry.Observe(id); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(id)
	var physical int
	spec := StableResourceSpec{
		Kind: ResourceCommandWAL, LogicalLane: "lane-0", ResourceID: "0:1", Generation: 1,
		DiagnosticPath: "wal/sealed", File: f, Frontier: DurableFrontier{Bytes: 9, MaxLSN: 1},
		Reachability: ReachabilityCommandWALRotated, PinRegistry: registry,
		SyncThrough: func(file *os.File, _ DurableFrontier) error {
			physical++
			stats, err := file.Stat()
			if err != nil || stats.Size() != 9 {
				t.Fatalf("retained sync descriptor: %v, %v", stats, err)
			}
			return file.Sync()
		},
	}
	original, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	defer original.Release()

	// Exercise the existing clone admission's exact limit with the original
	// live. Restore its real count before releasing any physical edge.
	registry.mu.Lock()
	held := registry.activePins
	registry.activePins = ^uint64(0)
	registry.mu.Unlock()
	lease, err := original.PinUnsyncedRotatedCommandWAL()
	registry.mu.Lock()
	registry.activePins = held
	registry.mu.Unlock()
	if lease != nil || err != nil {
		t.Fatalf("optional exhausted pin = %p, %v", lease, err)
	}
	if err := original.SyncThrough(); err != nil || physical != 1 {
		t.Fatalf("original fallback after refusal: physical=%d err=%v", physical, err)
	}
	lease, err = original.PinUnsyncedRotatedCommandWAL()
	if err != nil || lease == nil {
		t.Fatalf("eligible pin = %p, %v", lease, err)
	}
	defer lease.Release()
	if err := original.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	if releasedLease, err := original.PinUnsyncedRotatedCommandWAL(); !errors.Is(err, ErrResourceOwnership) || releasedLease != nil {
		t.Fatalf("released original produced a lease: %p %v", releasedLease, err)
	}
	if err := lease.SyncThrough(); err != nil || physical != 2 {
		t.Fatalf("sync after original retirement: physical=%d err=%v", physical, err)
	}
	if err := lease.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	if got := registry.Stats().ActivePins; got != 0 {
		t.Fatalf("pins after releasing original and lease=%d", got)
	}
	for _, setting := range []struct {
		field  ReachabilityField
		synced bool
	}{
		{ReachabilityCommandWALActive, false}, {ReachabilityCommandWALRotated, true},
	} {
		spec.Reachability, spec.ContentSynced = setting.field, setting.synced
		token, err := NewStableResourceToken(spec)
		if err != nil {
			t.Fatal(err)
		}
		pinned, err := token.PinUnsyncedRotatedCommandWAL()
		if err != nil || pinned != nil {
			t.Errorf("%s synced=%t produced extra custody: %p, %v", setting.field, setting.synced, pinned, err)
		}
		if err := token.ReleaseWithError(); err != nil {
			t.Fatal(err)
		}
	}
}
