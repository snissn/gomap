package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"testing"
	"time"
)

func TestOwnedTelemetryActualCoordinatorDeduplicatesAndDetaches(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "telemetry.bin", "12345678")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	first := admittedCoalescingFixture(t, &owner, registry, file, "a", DurableFrontier{Bytes: 4}, nil)
	second := admittedCoalescingFixture(t, &owner, registry, file, "b", DurableFrontier{Bytes: 8}, nil)
	defer first.Release()
	defer second.Release()
	c := Coordinator{clock: realClock{}, pending: []pendingEntry{
		{candidate: &PreparedRootCandidate{extensions: extensionSlots{resourceSet: first}}},
		{candidate: &PreparedRootCandidate{extensions: extensionSlots{resourceSet: second}}},
	}, resourceActivePins: map[ResourceKind]uint64{ResourceIndex: 2}, resourcePinHighWater: map[ResourceKind]uint64{ResourceIndex: 5}}
	before := owner.Bytes()
	stats := c.resourceStatsLocked()
	if len(stats) != 1 || stats[0].Kind != ResourceIndex || stats[0].PendingCount != 1 || stats[0].PendingBytes != 8 || stats[0].ActivePins != 2 || stats[0].PinHighWater != 5 || stats[0].LogicalObligationCountAvailable {
		t.Fatalf("actual coordinator telemetry=%+v", stats)
	}
	if owner.Bytes() != before {
		t.Fatal("telemetry scratch retained")
	}
	budget.cap = before
	if result := c.resourceStatsLocked(); result != nil {
		t.Fatalf("refused telemetry=%+v", result)
	}
	if owner.Bytes() != before || first.Owner() != ResourceOwnerBuilder || second.Owner() != ResourceOwnerBuilder {
		t.Fatal("refusal changed source or charge")
	}
	budget.cap = 1 << 20
	first.Release()
	second.Release()
	if owner.Bytes() != baseline || registry.Stats().ActivePins != 0 {
		t.Fatal("source cleanup leak")
	}
	if stats[0].Kind != ResourceIndex || stats[0].PendingBytes != 8 {
		t.Fatal("detached metrics changed after source disposal")
	}
	if _, err = StableResourceTelemetryStats(time.Now(), false, first); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("released input=%v", err)
	}
}
