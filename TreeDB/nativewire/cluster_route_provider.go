package nativewire

import (
	"context"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// CatalogClusterRouteProvider adapts a validated raftplacement catalog to the
// nativewire route-preflight interface. It only returns catalog-derived
// metadata; it does not prove live leadership or submit routed commands.
type CatalogClusterRouteProvider struct {
	catalog raftplacement.ResolvedCatalogV1
}

// NewCatalogClusterRouteProvider returns a route provider backed by a validated
// raftplacement catalog.
func NewCatalogClusterRouteProvider(catalog raftplacement.ResolvedCatalogV1) CatalogClusterRouteProvider {
	return CatalogClusterRouteProvider{catalog: catalog}
}

// ClusterRoute resolves the request against the provider's catalog. Collection
// placements can satisfy collection, single-token, and token-batch requests with
// a collection target. Token/ring placements require a single token for submit
// targets; multi-token batches return classification metadata so preflight can
// fail closed before submit until split/fanout execution exists.
func (p CatalogClusterRouteProvider) ClusterRoute(_ context.Context, request ClusterRouteRequest) (ClusterRouteTarget, error) {
	switch normalizeClusterRouteShape(request.Shape) {
	case ClusterRouteShapeCollection:
		decision, err := p.catalog.RouteCollection(request.Database, request.Catalog, request.Collection)
		if err != nil {
			return ClusterRouteTarget{}, err
		}
		return clusterRouteTargetFromCatalogDecision(decision), nil
	case ClusterRouteShapeToken:
		if !request.TokenKnown {
			return ClusterRouteTarget{}, errors.Join(
				raftplacement.ErrInvalidRouteRequest,
				raftplacement.ErrMissingRouteToken,
				fmt.Errorf("%s/%s/%s token route request requires explicit token", request.Database, request.Catalog, request.Collection),
			)
		}
		decision, err := p.catalog.RouteDocumentToken(request.Database, request.Catalog, request.Collection, request.Token)
		if err != nil {
			return ClusterRouteTarget{}, err
		}
		return clusterRouteTargetFromCatalogDecision(decision), nil
	case ClusterRouteShapeTokenBatch:
		return p.clusterRouteTokenBatch(request)
	case ClusterRouteShapeQuery:
		return ClusterRouteTarget{}, errors.Join(
			raftplacement.ErrInvalidRouteRequest,
			raftplacement.ErrUnsupportedRouteShape,
			fmt.Errorf("%s/%s/%s query route requires bounded scatter/read-index routing", request.Database, request.Catalog, request.Collection),
		)
	default:
		return ClusterRouteTarget{}, errors.Join(
			raftplacement.ErrInvalidRouteRequest,
			raftplacement.ErrUnsupportedRouteShape,
			fmt.Errorf("%s/%s/%s route shape %q", request.Database, request.Catalog, request.Collection, request.Shape),
		)
	}
}

func (p CatalogClusterRouteProvider) clusterRouteTokenBatch(request ClusterRouteRequest) (ClusterRouteTarget, error) {
	ref := raftplacement.CollectionRefV1{
		Database:   request.Database,
		Catalog:    request.Catalog,
		Collection: request.Collection,
	}
	if placement, ok := p.catalog.Placement(ref); ok && placement.Mode == raftplacement.PlacementModeCollectionV1 {
		decision, err := p.catalog.RouteCollection(request.Database, request.Catalog, request.Collection)
		if err != nil {
			return ClusterRouteTarget{}, err
		}
		return clusterRouteTargetFromCatalogDecision(decision), nil
	}
	decision, err := p.catalog.ClassifyDocumentTokenBatch(request.Database, request.Catalog, request.Collection, request.Tokens)
	if err != nil {
		return ClusterRouteTarget{}, err
	}
	return clusterRouteTargetFromCatalogTokenBatch(decision), nil
}

func clusterRouteTargetFromCatalogDecision(decision raftplacement.RouteDecisionV1) ClusterRouteTarget {
	target := ClusterRouteTarget{
		GroupID:       string(decision.GroupID()),
		Members:       clusterRouteGroupMembers(decision.Group.Members),
		LeaderHint:    string(decision.LeaderHint()),
		PlacementMode: string(decision.PlacementMode),
		RouteKey:      string(decision.RouteKey),
		Shape:         ClusterRouteShape(decision.Shape),
	}
	if decision.Token.Present {
		target.TokenKnown = true
		target.Token = decision.Token.Token
		target.PartitionID = string(decision.Token.Partition.ID)
	}
	return target
}

func clusterRouteTargetFromCatalogTokenBatch(decision raftplacement.RouteTokenBatchDecisionV1) ClusterRouteTarget {
	target := ClusterRouteTarget{
		PlacementMode:   string(decision.PlacementMode),
		RouteKey:        string(decision.RouteKey),
		Shape:           ClusterRouteShapeTokenBatch,
		TokenBatchClass: string(decision.Class),
	}
	if !decision.FanoutRequired() {
		target.GroupID = string(decision.GroupID())
		target.LeaderHint = string(decision.LeaderHint())
		target.Members = clusterRouteGroupMembers(decision.Group.Members)
	}
	return target
}

func clusterRouteGroupMembers[T ~string](members []T) []string {
	out := make([]string, len(members))
	for i, member := range members {
		out[i] = string(member)
	}
	return out
}
