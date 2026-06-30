package raftplacement

import (
	"errors"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func TestRouteCollectionDecisionIncludesGroupMetadata(t *testing.T) {
	resolved, err := Validate(validCatalog())
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}

	decision, err := resolved.RouteCollection(DefaultDatabase, DefaultCatalog, "orders")
	if err != nil {
		t.Fatalf("RouteCollection: %v", err)
	}

	if decision.Collection.Collection != "orders" {
		t.Fatalf("collection=%q want orders", decision.Collection.Collection)
	}
	if decision.Shape != RouteShapeCollectionV1 {
		t.Fatalf("shape=%q want %q", decision.Shape, RouteShapeCollectionV1)
	}
	if decision.PlacementMode != PlacementModeCollectionV1 {
		t.Fatalf("placement mode=%q want %q", decision.PlacementMode, PlacementModeCollectionV1)
	}
	if decision.GroupID() != "group-b" {
		t.Fatalf("group=%q want group-b", decision.GroupID())
	}
	if decision.LeaderHint() != "node-b" {
		t.Fatalf("leader hint=%q want node-b", decision.LeaderHint())
	}
	wantMembers := []raftcluster.NodeID{"node-b", "node-c"}
	if !reflect.DeepEqual(decision.Group.Members, wantMembers) {
		t.Fatalf("members=%v want %v", decision.Group.Members, wantMembers)
	}
	if decision.Token.Present {
		t.Fatalf("collection decision unexpectedly includes token metadata: %+v", decision.Token)
	}

	decision.Group.Members[0] = "mutated"
	again, err := resolved.RouteCollection(DefaultDatabase, DefaultCatalog, "orders")
	if err != nil {
		t.Fatalf("RouteCollection after mutation: %v", err)
	}
	if again.Group.Members[0] != "node-b" {
		t.Fatalf("route decision exposed mutable catalog group members: %v", again.Group.Members)
	}
}

func TestRouteTokenDecisionIncludesPartitionAndGroupMetadata(t *testing.T) {
	for _, mode := range []PlacementModeV1{PlacementModeTokenV1, PlacementModeRingV1} {
		t.Run(string(mode), func(t *testing.T) {
			ref := CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "events"}
			catalog := validCatalog()
			catalog.Placements = append(catalog.Placements, tokenPlacement(ref, mode))
			resolved, err := Validate(catalog)
			if err != nil {
				t.Fatalf("Validate: %v", err)
			}

			decision, err := resolved.RouteToken(DefaultDatabase, DefaultCatalog, "events", maxTokenV1)
			if err != nil {
				t.Fatalf("RouteToken: %v", err)
			}
			if decision.Shape != RouteShapeTokenV1 {
				t.Fatalf("shape=%q want %q", decision.Shape, RouteShapeTokenV1)
			}
			if decision.PlacementMode != mode {
				t.Fatalf("placement mode=%q want %q", decision.PlacementMode, mode)
			}
			if decision.GroupID() != "group-b" {
				t.Fatalf("group=%q want group-b", decision.GroupID())
			}
			if decision.LeaderHint() != "node-b" {
				t.Fatalf("leader hint=%q want node-b", decision.LeaderHint())
			}
			if !decision.Token.Present {
				t.Fatalf("token decision missing token metadata")
			}
			if decision.Token.Token != maxTokenV1 {
				t.Fatalf("token=%d want %d", decision.Token.Token, uint64(maxTokenV1))
			}
			if decision.Token.Partition.ID != "token-000001" || decision.Token.Partition.GroupID != "group-b" {
				t.Fatalf("partition=(%q,%q) want (token-000001,group-b)", decision.Token.Partition.ID, decision.Token.Partition.GroupID)
			}
		})
	}
}

func TestDocumentIDTokenV1IsStable(t *testing.T) {
	tests := []struct {
		name string
		id   []byte
		want uint64
	}{
		{name: "ascii", id: []byte("u1"), want: 0x0cda0b01b1a4d746},
		{name: "binary", id: []byte("mongo-key\x00"), want: 0xb50bd7be8fa239a1},
		{name: "empty", id: nil, want: 0xddb8fc2412a24a19},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := DocumentIDTokenV1(tc.id); got != tc.want {
				t.Fatalf("DocumentIDTokenV1(%q)=%#x want %#x", tc.id, got, tc.want)
			}
			if got := DocumentIDTokenV1(append([]byte(nil), tc.id...)); got != tc.want {
				t.Fatalf("DocumentIDTokenV1 cloned input=%#x want %#x", got, tc.want)
			}
		})
	}
}

func TestRouteDocumentTokenPreservesCollectionModeAndRoutesTokenModes(t *testing.T) {
	t.Run("collection", func(t *testing.T) {
		resolved, err := Validate(validCatalog())
		if err != nil {
			t.Fatalf("Validate: %v", err)
		}
		decision, err := resolved.RouteDocumentID(DefaultDatabase, DefaultCatalog, "users", []byte("u1"))
		if err != nil {
			t.Fatalf("RouteDocumentID collection: %v", err)
		}
		if decision.Shape != RouteShapeCollectionV1 || decision.PlacementMode != PlacementModeCollectionV1 {
			t.Fatalf("decision shape/mode=%q/%q want collection/collection", decision.Shape, decision.PlacementMode)
		}
		if decision.Token.Present {
			t.Fatalf("collection route should not expose token metadata: %+v", decision.Token)
		}
	})

	for _, mode := range []PlacementModeV1{PlacementModeTokenV1, PlacementModeRingV1} {
		t.Run(string(mode), func(t *testing.T) {
			ref := CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "events"}
			catalog := validCatalog()
			catalog.Placements = append(catalog.Placements, tokenPlacement(ref, mode))
			resolved, err := Validate(catalog)
			if err != nil {
				t.Fatalf("Validate: %v", err)
			}
			token := maxTokenV1
			decision, err := resolved.RouteDocumentToken(DefaultDatabase, DefaultCatalog, "events", token)
			if err != nil {
				t.Fatalf("RouteDocumentToken: %v", err)
			}
			if decision.Shape != RouteShapeTokenV1 || decision.PlacementMode != mode {
				t.Fatalf("decision shape/mode=%q/%q want token/%q", decision.Shape, decision.PlacementMode, mode)
			}
			if !decision.Token.Present || decision.Token.Token != token {
				t.Fatalf("token metadata=%+v want token %d", decision.Token, token)
			}
			if decision.Token.Partition.ID != "token-000001" || decision.Token.Partition.GroupID != "group-b" {
				t.Fatalf("partition=(%q,%q) want (token-000001,group-b)", decision.Token.Partition.ID, decision.Token.Partition.GroupID)
			}
		})
	}
}

func TestRouteFailsClosed(t *testing.T) {
	ref := CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "events"}
	catalog := validCatalog()
	catalog.Placements = append(catalog.Placements, tokenPlacement(ref, PlacementModeRingV1))
	resolved, err := Validate(catalog)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	token := uint64(0)

	tests := []struct {
		name string
		req  RouteRequestV1
		want error
	}{
		{
			name: "unplaced collection",
			req: RouteRequestV1{
				Collection: CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "missing"},
				Shape:      RouteShapeCollectionV1,
			},
			want: ErrUnplacedCollection,
		},
		{
			name: "collection shape against token placement",
			req: RouteRequestV1{
				Collection: ref,
				Shape:      RouteShapeCollectionV1,
			},
			want: ErrUnsupportedPlacementMode,
		},
		{
			name: "token request without token",
			req: RouteRequestV1{
				Collection: ref,
				Shape:      RouteShapeTokenV1,
			},
			want: ErrMissingRouteToken,
		},
		{
			name: "token request against collection placement",
			req: RouteRequestV1{
				Collection: CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "users"},
				Shape:      RouteShapeTokenV1,
				Token:      &token,
			},
			want: ErrUnsupportedPlacementMode,
		},
		{
			name: "query shape unsupported",
			req: RouteRequestV1{
				Collection: CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "users"},
				Shape:      RouteShapeQueryV1,
			},
			want: ErrUnsupportedRouteShape,
		},
		{
			name: "scatter gather unsupported",
			req: RouteRequestV1{
				Collection: CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "users"},
				Shape:      RouteShapeScatterGatherV1,
			},
			want: ErrUnsupportedRouteShape,
		},
		{
			name: "unknown route shape unsupported",
			req: RouteRequestV1{
				Collection: CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "users"},
				Shape:      "range_scan",
			},
			want: ErrUnsupportedRouteShape,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := resolved.Route(tc.req)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Route err=%v want errors.Is(%v)", err, tc.want)
			}
			if !errors.Is(err, ErrInvalidRouteRequest) {
				t.Fatalf("Route err=%v want ErrInvalidRouteRequest", err)
			}
		})
	}
}

func TestRouteInvalidCollectionFailsClosed(t *testing.T) {
	resolved, err := Validate(validCatalog())
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}

	_, err = resolved.Route(RouteRequestV1{
		Collection: CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "bad/name"},
		Shape:      RouteShapeCollectionV1,
	})
	if !errors.Is(err, ErrInvalidRouteRequest) {
		t.Fatalf("Route err=%v want ErrInvalidRouteRequest", err)
	}
	if !errors.Is(err, ErrInvalidCollection) {
		t.Fatalf("Route err=%v want ErrInvalidCollection", err)
	}
}
