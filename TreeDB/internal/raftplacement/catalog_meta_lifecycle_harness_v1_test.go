package raftplacement

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func TestCatalogMetaLifecycleHarnessActivatesAndConvergesV1(t *testing.T) {
	catalog := validCatalog()
	catalog.Features = DefaultFeatureSet()
	catalog.Features.Required = append(catalog.Features.Required, raftcluster.RequiredFeature{Name: raftcluster.FeatureVectorPartitionLifecycle, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureVectorPartitionLifecycle]})
	ctx, cancel := context.WithTimeout(context.Background(), realCatalogMetaIntegrationTestTimeoutV1)
	defer cancel()
	harness, err := OpenCatalogMetaLifecycleHarnessV1(ctx, CatalogMetaLifecycleHarnessOptionsV1{Catalog: catalog, Prefix: "m8-meta-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer harness.Close()
	if len(harness.NodeIDs()) != 3 || harness.GroupID() != "catalog-meta" || harness.LeaderID() == "" || harness.LeaderAuthority() == nil || harness.LeaderFence() == nil {
		t.Fatalf("harness identity is incomplete: group=%q nodes=%v leader=%q", harness.GroupID(), harness.NodeIDs(), harness.LeaderID())
	}
	status, ok := harness.LeaderAuthority().Status()
	if !ok {
		t.Fatal("leader catalog authority is unavailable")
	}
	identity := VectorPartitionLifecycleIdentityV1{Index: VectorPartitionLifecycleIndexIdentityV1{Collection: CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "users"}, CollectionIncarnation: 1, IndexName: "embedding", IndexDefinitionDigest: fmt.Sprintf("%064x", 17), IndexEpoch: 1, CatalogEpoch: status.Epoch, CatalogDigest: status.Digest}, Source: VectorPartitionLifecycleSourceIdentityV1{Generation: 11, Checksum: 12, SchemaHash: 13, RowCount: 10_000}, Generation: 7}
	coordinator := harness.LifecycleCoordinator()
	if _, err := coordinator.BeginBuildV1(ctx, identity, []raftcluster.GroupID{"data-group-a", "data-group-b"}, 0, 1); err != nil {
		t.Fatalf("BeginBuildV1: %v", err)
	}
	for i, group := range []raftcluster.GroupID{"data-group-a", "data-group-b"} {
		if _, err := coordinator.RecordGroupReadyV1(ctx, identity, VectorPartitionLifecycleGroupReadyV1{GroupID: group, AppliedIndex: uint64(100 + i), AssetSetDigest: fmt.Sprintf("%064x", 100+i)}); err != nil {
			t.Fatalf("RecordGroupReadyV1 %s: %v", group, err)
		}
	}
	prepared, err := coordinator.PrepareV1(ctx, identity)
	if err != nil {
		t.Fatalf("PrepareV1: %v", err)
	}
	active, err := coordinator.ActivateV1(ctx, identity)
	if err != nil {
		t.Fatalf("ActivateV1: %v", err)
	}
	if active.State != VectorPartitionLifecycleActiveV1 || active.ReadySetDigest == "" || prepared.ReadySetDigest != active.ReadySetDigest {
		t.Fatalf("active lifecycle=%+v prepared=%+v", active, prepared)
	}
	if _, err := harness.LeaderFence().LinearizableCatalogMetaAppliedIndexV1(ctx); err != nil {
		t.Fatalf("fresh linearizable read fence: %v", err)
	}
	if err := harness.WaitForAuthorities(ctx, func(authority *CatalogMetaAuthorityV1) bool {
		record, ok := authority.VectorPartitionLifecycleRecordV1(identity)
		return ok && record.State == VectorPartitionLifecycleActiveV1 && record.Identity == identity && record.ReadySetDigest == active.ReadySetDigest
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogMetaLifecycleHarnessRaftConfigProvidesSchedulingHeadroomV1(t *testing.T) {
	config := catalogMetaLifecycleHarnessRaftConfigV1()
	minimum := 5 * time.Second
	if config.HeartbeatTimeout < minimum || config.ElectionTimeout < minimum || config.LeaderLeaseTimeout < minimum {
		t.Fatalf("coordination timeouts heartbeat=%s election=%s lease=%s want each at least %s", config.HeartbeatTimeout, config.ElectionTimeout, config.LeaderLeaseTimeout, minimum)
	}
	if catalogMetaLifecycleHarnessLeaderDwellV1 < config.LeaderLeaseTimeout {
		t.Fatalf("leader dwell=%s want at least leader lease=%s", catalogMetaLifecycleHarnessLeaderDwellV1, config.LeaderLeaseTimeout)
	}
	if catalogMetaLeaderObservationMaxGapV1 >= config.ElectionTimeout || catalogMetaLeaderObservationMaxGapV1 >= config.LeaderLeaseTimeout {
		t.Fatalf("leader observation max gap=%s want below election=%s and lease=%s", catalogMetaLeaderObservationMaxGapV1, config.ElectionTimeout, config.LeaderLeaseTimeout)
	}
}

func TestCatalogMetaLeaderDwellRestartsAfterObservationGapV1(t *testing.T) {
	const lease = 5 * time.Second
	const maxGap = time.Second
	start := time.Unix(0, 0)
	var dwell catalogMetaLeaderDwellV1
	if dwell.Observe(start, true, "node-a", lease, maxGap) {
		t.Fatal("initial observation unexpectedly satisfies dwell")
	}
	if dwell.Observe(start.Add(500*time.Millisecond), true, "node-a", lease, maxGap) {
		t.Fatal("pre-lease observation unexpectedly satisfies dwell")
	}
	// This same node could have stepped down and been re-elected while the
	// polling goroutine was stalled, so its former wall-clock dwell is invalid.
	afterGap := start.Add(2 * time.Second)
	if dwell.Observe(afterGap, true, "node-a", lease, maxGap) {
		t.Fatal("same node after an observation gap unexpectedly satisfies dwell")
	}
	for elapsed := 500 * time.Millisecond; elapsed < lease; elapsed += 500 * time.Millisecond {
		if dwell.Observe(afterGap.Add(elapsed), true, "node-a", lease, maxGap) {
			t.Fatal("same node inherited dwell from before the observation gap")
		}
	}
	if !dwell.Observe(afterGap.Add(lease), true, "node-a", lease, maxGap) {
		t.Fatal("continuous post-gap observation did not satisfy a full dwell")
	}
}

func TestCatalogMetaLeaderDwellRestartsAfterProbeFailureV1(t *testing.T) {
	const lease = 5 * time.Second
	const maxGap = time.Second
	start := time.Unix(0, 0)
	var dwell catalogMetaLeaderDwellV1
	if dwell.Observe(start, true, "node-a", lease, maxGap) {
		t.Fatal("initial observation unexpectedly satisfies dwell")
	}
	if dwell.Observe(start.Add(500*time.Millisecond), true, "node-a", lease, maxGap) {
		t.Fatal("pre-lease observation unexpectedly satisfies dwell")
	}
	// A probe error makes the cluster sample incomplete: the apparently same
	// leader cannot carry any earlier dwell through that unknown interval.
	failureAt := start.Add(time.Second)
	if dwell.Observe(failureAt, false, "node-a", lease, maxGap) {
		t.Fatal("failed probe unexpectedly satisfies dwell")
	}
	firstComplete := failureAt.Add(500 * time.Millisecond)
	if dwell.Observe(firstComplete, true, "node-a", lease, maxGap) {
		t.Fatal("first complete post-failure observation unexpectedly satisfies dwell")
	}
	for elapsed := time.Second; elapsed < lease; elapsed += 500 * time.Millisecond {
		if dwell.Observe(failureAt.Add(elapsed), true, "node-a", lease, maxGap) {
			t.Fatal("same node inherited dwell from before the probe failure")
		}
	}
	if dwell.Observe(failureAt.Add(lease), true, "node-a", lease, maxGap) {
		t.Fatal("same node inherited the failed-probe timestamp")
	}
	if !dwell.Observe(firstComplete.Add(lease), true, "node-a", lease, maxGap) {
		t.Fatal("continuous post-failure observation did not satisfy a full dwell")
	}
}
