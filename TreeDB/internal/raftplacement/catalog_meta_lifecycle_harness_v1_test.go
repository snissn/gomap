package raftplacement

import (
	"context"
	"fmt"
	"io"
	"strings"
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

func TestCatalogMetaLifecycleImmutableIdentitySurvivesReopenV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), realCatalogMetaIntegrationTestTimeoutV1)
	defer cancel()
	harness, err := OpenCatalogMetaLifecycleHarnessV1(ctx, CatalogMetaLifecycleHarnessOptionsV1{
		Catalog: catalogMetaLifecycleCatalogV1(true), Prefix: "immutable-reopen",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer harness.Close()
	status, ok := harness.LeaderAuthority().Status()
	if !ok {
		t.Fatal("catalog authority unavailable")
	}
	identity := catalogMetaLifecycleTestIdentityV1(CatalogMetaRecordV1{Epoch: status.Epoch, Digest: status.Digest}, 7, 11)
	identity.Immutable = VectorPartitionLifecycleImmutableAuthorityV1{
		ManifestDigest: strings.Repeat("a", 64), PlacementDigest: strings.Repeat("b", 64),
	}
	coordinator := harness.LifecycleCoordinator()
	coordinator.PrepareImmutableV1 = func(context.Context, VectorPartitionLifecycleIdentityV1) (VectorPartitionLifecycleImmutableAuthorityV1, []raftcluster.GroupID, error) {
		return identity.Immutable, []raftcluster.GroupID{"group-a"}, nil
	}
	want, err := coordinator.BeginBuildV1(ctx, identity, []raftcluster.GroupID{"group-a"}, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := harness.WaitForAuthorities(ctx, func(authority *CatalogMetaAuthorityV1) bool {
		got, ok := authority.VectorPartitionLifecycleRecordV1(identity)
		return ok && got.Identity == want.Identity && got.Revision == want.Revision
	}); err != nil {
		t.Fatal(err)
	}
	deadline, _ := ctx.Deadline()
	receipt := &catalogMetaRestartReceiptV1{deadline: deadline, logs: make(map[raftcluster.NodeID]*catalogMetaRaftLogV1)}
	harness.restartObserver = receipt
	defer func() {
		if t.Failed() {
			receipt.report(t, harness.NodeIDs())
		}
	}()
	err = harness.RestartV1(ctx)
	receipt.phase("returned")
	if err != nil {
		t.Fatal(err)
	}
	if err := harness.WaitForAuthorities(ctx, func(authority *CatalogMetaAuthorityV1) bool {
		got, ok := authority.VectorPartitionLifecycleRecordV1(identity)
		return ok && got.Identity == want.Identity && got.Revision == want.Revision && got.State == want.State
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

type catalogMetaRestartReceiptV1 struct {
	deadline                                                time.Time
	phases                                                  []string
	logs                                                    map[raftcluster.NodeID]*catalogMetaRaftLogV1
	beforeCleanup                                           map[raftcluster.NodeID]string
	dwell                                                   catalogMetaLeaderDwellV1
	polls, incomplete, noLeader, multipleLeaders, gapResets int
	maxGap                                                  time.Duration
}

func (r *catalogMetaRestartReceiptV1) phase(name string) {
	now := time.Now()
	r.phases = append(r.phases, fmt.Sprintf("%s at=%s remaining=%s", name, now.Format(time.RFC3339Nano), r.deadline.Sub(now)))
	if name == "failure-cleanup" {
		// Closing the new runtime can generate events that evict the failed
		// election from its bounded rings. Save their contents at failure.
		r.beforeCleanup = make(map[raftcluster.NodeID]string, len(r.logs))
		for id, log := range r.logs {
			r.beforeCleanup[id] = log.String()
		}
	}
}

func (r *catalogMetaRestartReceiptV1) raftLog(id raftcluster.NodeID) io.Writer {
	log := new(catalogMetaRaftLogV1)
	r.logs[id] = log
	return log
}

func (r *catalogMetaRestartReceiptV1) leaderPoll(complete, multiple bool, dwell catalogMetaLeaderDwellV1) {
	r.polls++
	switch {
	case !complete:
		r.incomplete++
	case multiple:
		r.multipleLeaders++
	case dwell.leader == "":
		r.noLeader++
	}
	if !r.dwell.lastObservation.IsZero() {
		gap := dwell.lastObservation.Sub(r.dwell.lastObservation)
		r.maxGap = max(r.maxGap, gap)
		if gap > catalogMetaLeaderObservationMaxGapV1 && dwell.leader != "" && dwell.leader == r.dwell.leader {
			r.gapResets++
		}
	}
	r.dwell = dwell
}

func (r *catalogMetaRestartReceiptV1) report(t *testing.T, peers []raftcluster.NodeID) {
	t.Helper()
	var dwellElapsed time.Duration
	if !r.dwell.since.IsZero() {
		dwellElapsed = r.dwell.lastObservation.Sub(r.dwell.since)
	}
	t.Logf("catalog restart deadline=%s phases=%v polls=%d incomplete=%d no_leader=%d multiple_leaders=%d max_gap=%s same_leader_gap_resets=%d last_dwell_leader=%q last_dwell_elapsed=%s",
		r.deadline.Format(time.RFC3339Nano), r.phases, r.polls, r.incomplete, r.noLeader, r.multipleLeaders, r.maxGap, r.gapResets, r.dwell.leader, dwellElapsed)
	for _, id := range peers {
		if events, ok := r.beforeCleanup[id]; ok {
			t.Logf("reopened peer %s Raft events at failure: %s", id, events)
		} else if log := r.logs[id]; log != nil {
			t.Logf("reopened peer %s recent Raft events: %s", id, log)
		}
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
