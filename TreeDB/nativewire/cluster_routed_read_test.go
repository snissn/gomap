package nativewire

import (
	"context"
	"strings"
	"testing"
	"time"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func TestClusterRouteSingleIDReadFailsClosedBeforeLocalObservation(t *testing.T) {
	id := []byte("u1")
	token := raftplacement.DocumentIDTokenV1(id)
	for _, mode := range []raftplacement.PlacementModeV1{
		raftplacement.PlacementModeTokenV1,
		raftplacement.PlacementModeRingV1,
	} {
		t.Run(string(mode), func(t *testing.T) {
			routeProvider := &placementRouteClusterSubmitter{
				fakeClusterSubmitter: &fakeClusterSubmitter{},
				provider: newStaticCatalogRouteProviderForTest(
					mustNativewireTokenGroupRouteCatalog(t, mode, token),
				),
			}
			coordinator := &fakeClusterReadCoordinator{result: ClusterReadResult{
				ActualConsistency: ConsistencyLinearizable,
				ServingNode:       "node-c",
				LeaderNode:        "node-c",
				AppliedIndex:      42,
				HasAppliedIndex:   true,
			}}
			client, server, mgr, _ := serveCollectionPipeWithServerAndOptions(t, ServerOptions{
				ClusterSubmitter:       routeProvider,
				ClusterReadCoordinator: coordinator,
			})
			seedReadCollection(t, mgr)

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := client.Hello(ctx); err != nil {
				t.Fatalf("Hello: %v", err)
			}
			_, err := client.GetManyWithOptions(ctx, "users", [][]byte{id}, ReadOptions{
				ConsistencyPolicy: ConsistencyLinearizable,
			})
			if !isRemoteError(err, iwire.ErrReadOnly) ||
				!strings.Contains(err.Error(), "collection-store identity is bound to the owner Raft proof") ||
				!strings.Contains(err.Error(), "route_error_class=owner_store_unbound") {
				t.Fatalf("GetManyWithOptions err=%v want owner-store-unbound read-only rejection", err)
			}
			if len(coordinator.calls) != 0 {
				t.Fatalf("cluster read coordinator calls=%+v want none before local observation", coordinator.calls)
			}
			routes := routeProvider.snapshotRoutes()
			if len(routes) != 1 ||
				routes[0].Shape != ClusterRouteShapeToken ||
				!routes[0].TokenKnown ||
				routes[0].Token != token {
				t.Fatalf("route requests=%+v want one token route for u1", routes)
			}
			stats := server.Stats()
			for key, want := range map[string]string{
				"treedb.native_wire.cluster_read_route.requests_total":            "1",
				"treedb.native_wire.cluster_read_route.errors_total":              "1",
				"treedb.native_wire.cluster_read_route.unsupported_total":         "1",
				"treedb.native_wire.cluster_read_route.owner_store_unbound_total": "1",
			} {
				if got := stats[key]; got != want {
					t.Fatalf("stats[%q]=%q want %q stats=%v", key, got, want, stats)
				}
			}
			for _, key := range []string{
				"treedb.native_wire.cluster_read_route.success_total",
				"treedb.native_wire.cluster_read_route.linearizable_success_total",
				"treedb.native_wire.cluster_read_route.read_index_success_total",
				"treedb.native_wire.cluster_read_route.leader_success_total",
				"treedb.native_wire.cluster_read_route.follower_success_total",
			} {
				if got := stats[key]; got != "" {
					t.Fatalf("disabled routed read stats[%q]=%q want absent", key, got)
				}
			}
		})
	}
}

func TestClusterRouteUnsupportedReadShapesFailClosedBeforeProviderOrLocalRead(t *testing.T) {
	id := []byte("u1")
	token := raftplacement.DocumentIDTokenV1(id)
	routeProvider := &placementRouteClusterSubmitter{
		fakeClusterSubmitter: &fakeClusterSubmitter{},
		provider: newStaticCatalogRouteProviderForTest(
			mustNativewireTokenGroupRouteCatalog(t, raftplacement.PlacementModeRingV1, token),
		),
	}
	client, server, mgr, _ := serveCollectionPipeWithServerAndOptions(t, ServerOptions{
		ClusterSubmitter: routeProvider,
	})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	_, err := client.GetManyWithOptions(ctx, "users", [][]byte{id, []byte("u2")}, ReadOptions{
		ConsistencyPolicy: ConsistencyLinearizable,
	})
	if !isRemoteError(err, iwire.ErrReadOnly) || !strings.Contains(err.Error(), "query route shape is not supported") {
		t.Fatalf("multi-id routed read err=%v want query rejection", err)
	}
	_, _, err = client.IndexLookup(ctx, "users", "email", "ada@example.com", CursorLimits{})
	if !isRemoteError(err, iwire.ErrReadOnly) || !strings.Contains(err.Error(), "query route shape is not supported") {
		t.Fatalf("secondary-index routed read err=%v want query rejection", err)
	}
	if routes := routeProvider.snapshotRoutes(); len(routes) != 0 {
		t.Fatalf("unsupported query route provider calls=%+v want none", routes)
	}
	stats := server.Stats()
	if got := stats["treedb.native_wire.cluster_read_route.errors_total"]; got != "2" {
		t.Fatalf("routed read errors=%q want 2", got)
	}
	if got := stats["treedb.native_wire.cluster_read_route.unsupported_total"]; got != "2" {
		t.Fatalf("unsupported routed reads=%q want 2", got)
	}
}

func TestClusterRouteMetadataReadsFailClosedBeforeLocalCatalogObservation(t *testing.T) {
	routeProvider := &placementRouteClusterSubmitter{
		fakeClusterSubmitter: &fakeClusterSubmitter{},
		provider: newStaticCatalogRouteProviderForTest(
			mustNativewireRouteTestCatalog(t, raftplacement.PlacementModeRingV1),
		),
	}
	client, _, mgr, _ := serveCollectionPipeWithServerAndOptions(t, ServerOptions{
		ClusterSubmitter: routeProvider,
	})
	seedReadCollection(t, mgr)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	operations := []struct {
		name string
		run  func() error
	}{
		{
			name: "list_collections",
			run: func() error {
				_, err := client.ListCollections(ctx)
				return err
			},
		},
		{
			name: "list_indexes",
			run: func() error {
				_, err := client.ListIndexes(ctx, "users")
				return err
			},
		},
		{
			name: "open_collection",
			run: func() error {
				_, err := client.OpenCollection(ctx, "users")
				return err
			},
		},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			err := operation.run()
			if !isRemoteError(err, iwire.ErrReadOnly) ||
				!strings.Contains(err.Error(), "authoritative catalog metadata") {
				t.Fatalf("%s err=%v want routed metadata read-only rejection", operation.name, err)
			}
		})
	}
	if routes := routeProvider.snapshotRoutes(); len(routes) != 0 {
		t.Fatalf("metadata route provider calls=%+v want none", routes)
	}
}
