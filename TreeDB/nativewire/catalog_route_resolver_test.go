package nativewire

import (
	"context"

	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// staticCatalogRouteProviderForTest is deliberately confined to test code.
// Production code has no adapter from CatalogRouteResolverV1 to
// ClusterRouteProvider; replicated ownership must use
// CatalogMetaClusterRouteProvider.
type staticCatalogRouteProviderForTest struct {
	resolver CatalogRouteResolverV1
}

func newStaticCatalogRouteProviderForTest(catalog raftplacement.ResolvedCatalogV1) staticCatalogRouteProviderForTest {
	return staticCatalogRouteProviderForTest{resolver: NewCatalogRouteResolverV1(catalog)}
}

func (p staticCatalogRouteProviderForTest) ClusterRoute(ctx context.Context, request ClusterRouteRequest) (ClusterRouteTarget, error) {
	return p.resolver.ResolveCatalogRouteV1(ctx, request)
}
