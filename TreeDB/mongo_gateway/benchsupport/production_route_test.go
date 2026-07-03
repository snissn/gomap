package benchsupport

import (
	"context"
	"testing"
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
