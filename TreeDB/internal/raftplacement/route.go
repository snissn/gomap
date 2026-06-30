package raftplacement

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

var (
	ErrInvalidRouteRequest   = errors.New("raftplacement: invalid route request")
	ErrUnsupportedRouteShape = errors.New("raftplacement: unsupported route shape")
	ErrMissingRouteToken     = errors.New("raftplacement: missing route token")
)

const documentIDTokenDomainV1 = "TreeDB/RaftPlacement/DocumentIDTokenV1\x00"

type RouteShapeV1 string

const (
	RouteShapeCollectionV1    RouteShapeV1 = "collection"
	RouteShapeTokenV1         RouteShapeV1 = "token"
	RouteShapeQueryV1         RouteShapeV1 = "query"
	RouteShapeScatterGatherV1 RouteShapeV1 = "scatter_gather"
)

type RouteRequestV1 struct {
	Collection CollectionRefV1
	Shape      RouteShapeV1
	Token      *uint64
}

type RouteTokenDecisionV1 struct {
	Present   bool
	Token     uint64
	Partition ResolvedTokenPartitionV1
}

type RouteDecisionV1 struct {
	Collection    CollectionRefV1
	Shape         RouteShapeV1
	PlacementMode PlacementModeV1
	Group         ResolvedGroupV1
	Token         RouteTokenDecisionV1
}

func (d RouteDecisionV1) GroupID() raftcluster.GroupID {
	return d.Group.ID
}

func (d RouteDecisionV1) LeaderHint() raftcluster.NodeID {
	return d.Group.LeaderHint
}

func (c ResolvedCatalogV1) Route(req RouteRequestV1) (RouteDecisionV1, error) {
	if err := validateCollectionRef(req.Collection); err != nil {
		return RouteDecisionV1{}, errors.Join(ErrInvalidRouteRequest, err)
	}
	placement, ok := c.placements[req.Collection]
	if !ok {
		return RouteDecisionV1{}, errors.Join(ErrInvalidRouteRequest, ErrUnplacedCollection, fmt.Errorf("%s/%s/%s", req.Collection.Database, req.Collection.Catalog, req.Collection.Collection))
	}

	switch req.Shape {
	case RouteShapeCollectionV1:
		if placement.Mode != PlacementModeCollectionV1 {
			return RouteDecisionV1{}, errors.Join(ErrInvalidRouteRequest, ErrUnsupportedPlacementMode, fmt.Errorf("%s/%s/%s mode %q requires explicit token route request", req.Collection.Database, req.Collection.Catalog, req.Collection.Collection, placement.Mode))
		}
		group, err := c.routeGroup(placement.GroupID)
		if err != nil {
			return RouteDecisionV1{}, err
		}
		return RouteDecisionV1{
			Collection:    req.Collection,
			Shape:         req.Shape,
			PlacementMode: placement.Mode,
			Group:         group,
		}, nil
	case RouteShapeTokenV1:
		if req.Token == nil {
			return RouteDecisionV1{}, errors.Join(ErrInvalidRouteRequest, ErrMissingRouteToken, fmt.Errorf("%s/%s/%s token route request requires explicit token", req.Collection.Database, req.Collection.Catalog, req.Collection.Collection))
		}
		if placement.Mode != PlacementModeTokenV1 && placement.Mode != PlacementModeRingV1 {
			return RouteDecisionV1{}, errors.Join(ErrInvalidRouteRequest, ErrUnsupportedPlacementMode, fmt.Errorf("%s/%s/%s mode %q has no token partitions", req.Collection.Database, req.Collection.Catalog, req.Collection.Collection, placement.Mode))
		}
		plan, ok := c.tokens[req.Collection]
		if !ok {
			return RouteDecisionV1{}, errors.Join(ErrInvalidRouteRequest, ErrInvalidTokenRing, fmt.Errorf("%s/%s/%s has no resolved token plan", req.Collection.Database, req.Collection.Catalog, req.Collection.Collection))
		}
		partition, err := plan.ResolveToken(*req.Token)
		if err != nil {
			return RouteDecisionV1{}, errors.Join(ErrInvalidRouteRequest, err)
		}
		group, err := c.routeGroup(partition.GroupID)
		if err != nil {
			return RouteDecisionV1{}, err
		}
		return RouteDecisionV1{
			Collection:    req.Collection,
			Shape:         req.Shape,
			PlacementMode: placement.Mode,
			Group:         group,
			Token: RouteTokenDecisionV1{
				Present:   true,
				Token:     *req.Token,
				Partition: partition,
			},
		}, nil
	default:
		return RouteDecisionV1{}, errors.Join(ErrInvalidRouteRequest, ErrUnsupportedRouteShape, fmt.Errorf("%s/%s/%s route shape %q", req.Collection.Database, req.Collection.Catalog, req.Collection.Collection, req.Shape))
	}
}

func (c ResolvedCatalogV1) RouteCollection(database, catalog, collection string) (RouteDecisionV1, error) {
	return c.Route(RouteRequestV1{
		Collection: CollectionRefV1{Database: database, Catalog: catalog, Collection: collection},
		Shape:      RouteShapeCollectionV1,
	})
}

func (c ResolvedCatalogV1) RouteToken(database, catalog, collection string, token uint64) (RouteDecisionV1, error) {
	return c.Route(RouteRequestV1{
		Collection: CollectionRefV1{Database: database, Catalog: catalog, Collection: collection},
		Shape:      RouteShapeTokenV1,
		Token:      &token,
	})
}

// DocumentIDTokenV1 maps a deterministic document ID byte identity to the v1
// uint64 token space. The rule is stable across processes and platforms.
func DocumentIDTokenV1(documentID []byte) uint64 {
	h := sha256.New()
	_, _ = h.Write([]byte(documentIDTokenDomainV1))
	_, _ = h.Write(documentID)
	sum := h.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8])
}

func (c ResolvedCatalogV1) RouteDocumentID(database, catalog, collection string, documentID []byte) (RouteDecisionV1, error) {
	return c.RouteDocumentToken(database, catalog, collection, DocumentIDTokenV1(documentID))
}

func (c ResolvedCatalogV1) RouteDocumentToken(database, catalog, collection string, token uint64) (RouteDecisionV1, error) {
	ref := CollectionRefV1{Database: database, Catalog: catalog, Collection: collection}
	if err := validateCollectionRef(ref); err != nil {
		return RouteDecisionV1{}, errors.Join(ErrInvalidRouteRequest, err)
	}
	placement, ok := c.placements[ref]
	if !ok {
		return RouteDecisionV1{}, errors.Join(ErrInvalidRouteRequest, ErrUnplacedCollection, fmt.Errorf("%s/%s/%s", ref.Database, ref.Catalog, ref.Collection))
	}
	switch placement.Mode {
	case PlacementModeCollectionV1:
		return c.RouteCollection(database, catalog, collection)
	case PlacementModeTokenV1, PlacementModeRingV1:
		return c.RouteToken(database, catalog, collection, token)
	default:
		return RouteDecisionV1{}, errors.Join(ErrInvalidRouteRequest, ErrUnsupportedPlacementMode, fmt.Errorf("%s/%s/%s mode %q", ref.Database, ref.Catalog, ref.Collection, placement.Mode))
	}
}

func (c ResolvedCatalogV1) routeGroup(id raftcluster.GroupID) (ResolvedGroupV1, error) {
	group, ok := c.Group(id)
	if !ok {
		return ResolvedGroupV1{}, errors.Join(ErrInvalidRouteRequest, ErrUnknownGroup, fmt.Errorf("route target group %q is not present in resolved catalog", id))
	}
	return group, nil
}
