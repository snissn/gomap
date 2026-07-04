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

func TestProductionRouteProofRecorderResetCountersPreservesLocalGroups(t *testing.T) {
	recorder := newProductionRouteProofRecorder(2, 2, productionRouteProofGroupID(0))
	recorder.recordRouteSuccess(recorder.groupTarget(1))
	recorder.recordCommitGroup(productionRouteProofGroupID(0))
	recorder.recordAppliedGroup(productionRouteProofGroupID(0))
	recorder.resetCounters()

	recorder.recordRouteSuccess(recorder.groupTarget(0))
	snapshot := recorder.snapshot()
	if snapshot.RouteAttemptsTotal != 1 || snapshot.RouteLocalOwnerHits != 1 || snapshot.RouteRemoteRedirects != 0 {
		t.Fatalf("snapshot after reset/local route=%+v want one local route", snapshot)
	}
	if snapshot.CommitGroupHits != nil || snapshot.AppliedGroupHits != nil {
		t.Fatalf("commit/apply hits survived reset: %+v", snapshot)
	}
}
