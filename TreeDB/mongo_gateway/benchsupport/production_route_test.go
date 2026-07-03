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
