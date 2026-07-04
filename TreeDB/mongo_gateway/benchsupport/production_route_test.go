package benchsupport

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/nativewire"
)

func TestProductionRouteProofCatalogVersionClearsStaleKnownState(t *testing.T) {
	provider := &productionRouteProofCatalogVersion{}
	provider.known.Store(true)
	provider.value.Store(42)

	version, ok, err := provider.CurrentCatalogVersion(context.Background())
	if err != nil {
		t.Fatalf("CurrentCatalogVersion: %v", err)
	}
	if ok || version != 0 {
		t.Fatalf("CurrentCatalogVersion version=%d ok=%t want 0/false after missing DB state", version, ok)
	}
}

func TestProductionRouteProofCatalogVersionServerCatalogVersionUsesContext(t *testing.T) {
	provider := &productionRouteProofCatalogVersion{}
	provider.known.Store(true)
	provider.value.Store(42)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	version, err := provider.ServerCatalogVersion(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ServerCatalogVersion version=%d err=%v want context canceled", version, err)
	}
}

func TestProductionRouteProofRecorderClusterRouteTokenBatchRequiresTokens(t *testing.T) {
	recorder := newProductionRouteProofRecorder(1, 1)
	_, err := recorder.ClusterRoute(context.Background(), nativewire.ClusterRouteRequest{
		Shape:  nativewire.ClusterRouteShapeTokenBatch,
		Tokens: nil,
	})
	if err == nil || err.Error() != "missing token(s)" {
		t.Fatalf("ClusterRoute empty token batch err=%v want missing token(s)", err)
	}
}

func TestProductionRouteProofRecorderCountsOnlyConfiguredLocalGroups(t *testing.T) {
	recorder := newProductionRouteProofRecorder(2, 2, productionRouteProofGroupID(0))

	recorder.recordRouteSuccess(recorder.groupTarget(1))
	snapshot := recorder.snapshot()
	if snapshot.RouteAttemptsTotal != 1 || snapshot.RouteRemoteRedirects != 1 || snapshot.RouteLocalOwnerHits != 0 {
		t.Fatalf("snapshot attempts/remote/local=%d/%d/%d want 1/1/0: %+v",
			snapshot.RouteAttemptsTotal, snapshot.RouteRemoteRedirects, snapshot.RouteLocalOwnerHits, snapshot)
	}
	if snapshot.RouteGroupHits[productionRouteProofGroupID(1)] != 1 || snapshot.RouteLeaderHits["node-01-a"] != 1 {
		t.Fatalf("remote route hits not recorded: %+v", snapshot)
	}

	recorder.recordRouteSuccess(recorder.groupTarget(0))
	snapshot = recorder.snapshot()
	if snapshot.RouteAttemptsTotal != 2 || snapshot.RouteRemoteRedirects != 1 || snapshot.RouteLocalOwnerHits != 1 {
		t.Fatalf("snapshot after local route attempts/remote/local=%d/%d/%d want 2/1/1: %+v",
			snapshot.RouteAttemptsTotal, snapshot.RouteRemoteRedirects, snapshot.RouteLocalOwnerHits, snapshot)
	}
}

func TestProductionRouteProofRecorderCountsConfiguredForwardGroups(t *testing.T) {
	recorder := newProductionRouteProofRecorderWithForwardGroups(2, 2, productionRouteProofGroupID(0), productionRouteProofGroupID(1))

	recorder.recordRouteSuccess(recorder.groupTarget(1))
	recorder.recordCommitGroup(productionRouteProofGroupID(1))
	recorder.recordAppliedGroup(productionRouteProofGroupID(1))

	snapshot := recorder.snapshot()
	if snapshot.RouteAttemptsTotal != 1 ||
		snapshot.RouteRemoteForwards != 1 ||
		snapshot.RouteRemoteRedirects != 0 ||
		snapshot.RouteLocalOwnerHits != 0 {
		t.Fatalf("snapshot attempts/forward/redirect/local=%d/%d/%d/%d want 1/1/0/0: %+v",
			snapshot.RouteAttemptsTotal, snapshot.RouteRemoteForwards, snapshot.RouteRemoteRedirects, snapshot.RouteLocalOwnerHits, snapshot)
	}
	if !snapshot.RealRoutedCommits {
		t.Fatalf("snapshot did not treat forwarded owner commit/apply as real routed commit: %+v", snapshot)
	}
	if snapshot.RouteGroupHits[productionRouteProofGroupID(1)] != 1 ||
		snapshot.RouteLeaderHits["node-01-a"] != 1 ||
		snapshot.CommitGroupHits[productionRouteProofGroupID(1)] != 1 ||
		snapshot.AppliedGroupHits[productionRouteProofGroupID(1)] != 1 {
		t.Fatalf("forwarded route/commit/apply hits not recorded on group-01: %+v", snapshot)
	}
}

func TestProductionRouteProofRecorderUnknownOwnerProbeAvoidsConfiguredGroups(t *testing.T) {
	forwardGroups := make([]string, 0, 99)
	for group := 1; group < 100; group++ {
		forwardGroups = append(forwardGroups, productionRouteProofGroupID(group))
	}
	recorder := newProductionRouteProofRecorderWithForwardGroups(100, 100, productionRouteProofGroupID(0), forwardGroups...)

	target := recorder.unknownOwnerProbeTarget()
	if target.GroupID != productionRouteProofGroupID(100) ||
		target.LeaderHint != "node-100-a" ||
		len(target.Members) != 3 {
		t.Fatalf("unknown owner target=%+v want first group beyond configured registry", target)
	}
	if _, ok := recorder.localGroups[target.GroupID]; ok {
		t.Fatalf("unknown owner probe target %q collides with local groups", target.GroupID)
	}
	if _, ok := recorder.forwardGroups[target.GroupID]; ok {
		t.Fatalf("unknown owner probe target %q collides with forward groups", target.GroupID)
	}
}

func TestProductionRouteProofRecorderUnconfiguredGroupStillRedirects(t *testing.T) {
	recorder := newProductionRouteProofRecorderWithForwardGroups(3, 3, productionRouteProofGroupID(0), productionRouteProofGroupID(1))

	recorder.recordRouteSuccess(recorder.groupTarget(2))
	snapshot := recorder.snapshot()
	if snapshot.RouteAttemptsTotal != 1 ||
		snapshot.RouteRemoteRedirects != 1 ||
		snapshot.RouteRemoteForwards != 0 ||
		snapshot.RouteLocalOwnerHits != 0 ||
		snapshot.RealRoutedCommits {
		t.Fatalf("snapshot for unconfigured group=%+v, want redirect-only without real routed commit", snapshot)
	}
}

func TestProductionRouteProofRecorderResetCountersPreservesLocalGroups(t *testing.T) {
	recorder := newProductionRouteProofRecorderWithForwardGroups(2, 2, productionRouteProofGroupID(0), productionRouteProofGroupID(1))
	recorder.recordRouteSuccess(recorder.groupTarget(1))
	recorder.recordCommitGroup(productionRouteProofGroupID(0))
	recorder.recordAppliedGroup(productionRouteProofGroupID(0))
	recorder.resetCounters()

	recorder.recordRouteSuccess(recorder.groupTarget(1))
	snapshot := recorder.snapshot()
	if snapshot.RouteAttemptsTotal != 1 || snapshot.RouteRemoteForwards != 1 || snapshot.RouteRemoteRedirects != 0 || snapshot.RouteLocalOwnerHits != 0 {
		t.Fatalf("snapshot after reset/forward route=%+v want one forwarded route", snapshot)
	}
	if snapshot.CommitGroupHits != nil || snapshot.AppliedGroupHits != nil {
		t.Fatalf("commit/apply hits survived reset: %+v", snapshot)
	}

	recorder.resetCounters()
	recorder.recordRouteSuccess(recorder.groupTarget(0))
	snapshot = recorder.snapshot()
	if snapshot.RouteAttemptsTotal != 1 || snapshot.RouteLocalOwnerHits != 1 || snapshot.RouteRemoteRedirects != 0 {
		t.Fatalf("snapshot after reset/local route=%+v want one local route", snapshot)
	}
	if snapshot.CommitGroupHits != nil || snapshot.AppliedGroupHits != nil {
		t.Fatalf("commit/apply hits survived reset: %+v", snapshot)
	}
}
