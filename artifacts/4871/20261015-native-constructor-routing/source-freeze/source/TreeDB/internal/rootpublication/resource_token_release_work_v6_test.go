package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"testing"
)

func TestNativeScopedTokenActualLastPinReleaseV6(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	f := writeStableResourceFixture(t, t.TempDir(), "release.bin", "payload")
	defer f.Close()
	id, err := StableIdentityFromFile(f)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(id); err != nil {
		t.Fatal(err)
	}
	token, err := NewStableResourceToken(StableResourceSpec{MetadataOwner: &owner, PinRegistry: registry, Kind: ResourceIndex, LogicalLane: "index", ResourceID: "release", Generation: 1, DiagnosticPath: "release.bin", File: f, Frontier: DurableFrontier{Bytes: 7}, Reachability: ReachabilityIndexFile})
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Unobserve(id); err != nil {
		t.Fatal(err)
	}
	before := owner.Bytes()
	completion, ready, err := token.ReleaseWithWorkV6(&iterator.OrdinalScanWork{})
	if err != nil || ready || token.released.Load() || owner.Bytes() != before {
		t.Fatalf("pre-effect refusal ready=%v err=%v", ready, err)
	}
	for calls := 0; calls < 32; calls++ {
		completion, ready, err = token.ReleaseWithWorkV6(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
	}
	if !ready || registry.ActivePins() != 0 || registry.ActiveIdentities() != 0 {
		t.Fatal("original last pin did not complete")
	}
	if owner.Bytes() == 0 {
		t.Fatal("containing accounting refunded inside callback")
	}
	token = nil
	completion.Complete()
	completion.Complete()
	if owner.Bytes() != 0 {
		t.Fatalf("accounting retained %d", owner.Bytes())
	}
	if _, err = f.Stat(); err != nil {
		t.Fatal("producer file closed", err)
	}
}
func TestNativeClosingPinRefusesBeforeConsumptionV6(t *testing.T) {
	registry := NewIdentityPinRegistry()
	id := testStableIdentity(1)
	if err := registry.Observe(id); err != nil {
		t.Fatal(err)
	}
	pin, err := registry.Pin(id)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Unobserve(id); err != nil {
		t.Fatal(err)
	}
	registry.Close()
	ready, err := pin.ReleaseWithWorkV6(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if ready || err != retainedalloc.ErrWorkRequired || registry.ActivePins() != 1 {
		t.Fatalf("closing ready=%v err=%v", ready, err)
	}
	pin.Release()
}
