package raftplacement

import (
	"errors"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func TestValidateResolvesCollectionLevelPlacements(t *testing.T) {
	resolved, err := Validate(validCatalog())
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	groupID, err := resolved.ResolveCollection(DefaultDatabase, DefaultCatalog, "users")
	if err != nil {
		t.Fatalf("Resolve users: %v", err)
	}
	if groupID != "group-a" {
		t.Fatalf("users group=%q want group-a", groupID)
	}
	groupID, err = resolved.Resolve(CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "orders"})
	if err != nil {
		t.Fatalf("Resolve orders: %v", err)
	}
	if groupID != "group-b" {
		t.Fatalf("orders group=%q want group-b", groupID)
	}
	group, ok := resolved.Group("group-b")
	if !ok {
		t.Fatalf("Group group-b missing")
	}
	if group.LeaderHint != "node-b" {
		t.Fatalf("leader hint=%q want node-b", group.LeaderHint)
	}
	wantMembers := []raftcluster.NodeID{"node-b", "node-c"}
	if !reflect.DeepEqual(group.Members, wantMembers) {
		t.Fatalf("members=%v want %v", group.Members, wantMembers)
	}
}

func TestValidateRejectsInvalidCatalog(t *testing.T) {
	base := validCatalog()
	tests := []struct {
		name string
		mut  func(*CatalogV1)
		want error
	}{
		{
			name: "missing groups",
			mut:  func(c *CatalogV1) { c.Groups = nil },
			want: ErrMissingGroup,
		},
		{
			name: "duplicate group",
			mut: func(c *CatalogV1) {
				c.Groups = append(c.Groups, GroupV1{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}})
			},
			want: ErrDuplicateGroup,
		},
		{
			name: "missing group member",
			mut:  func(c *CatalogV1) { c.Groups[0].Members = nil },
			want: ErrMissingMember,
		},
		{
			name: "duplicate group member",
			mut: func(c *CatalogV1) {
				c.Groups[0].Members = append(c.Groups[0].Members, "node-a")
			},
			want: ErrDuplicateMember,
		},
		{
			name: "leader hint outside members",
			mut:  func(c *CatalogV1) { c.Groups[0].LeaderHint = "node-z" },
			want: ErrLeaderHintNotMember,
		},
		{
			name: "unknown placement group",
			mut:  func(c *CatalogV1) { c.Placements[0].GroupID = "group-z" },
			want: ErrUnknownGroup,
		},
		{
			name: "invalid collection ref",
			mut:  func(c *CatalogV1) { c.Placements[0].Collection.Collection = "bad/name" },
			want: ErrInvalidCollection,
		},
		{
			name: "duplicate collection placement",
			mut: func(c *CatalogV1) {
				c.Placements = append(c.Placements, CollectionPlacementV1{
					Collection: CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "users"},
					GroupID:    "group-b",
					Mode:       PlacementModeCollectionV1,
				})
			},
			want: ErrDuplicatePlacement,
		},
		{
			name: "unknown mode unsupported",
			mut:  func(c *CatalogV1) { c.Placements[0].Mode = "hash" },
			want: ErrUnsupportedPlacementMode,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			catalog := cloneCatalog(base)
			tc.mut(&catalog)
			_, err := Validate(catalog)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Validate err=%v want errors.Is(%v)", err, tc.want)
			}
			if !errors.Is(err, ErrInvalidCatalog) {
				t.Fatalf("Validate err=%v want ErrInvalidCatalog", err)
			}
		})
	}
}

func TestValidateFeatureFloorFailsClosed(t *testing.T) {
	base := validCatalog()
	tests := []struct {
		name string
		mut  func(*CatalogV1)
		want error
	}{
		{
			name: "unsupported catalog major",
			mut: func(c *CatalogV1) {
				c.Features = DefaultFeatureSet()
				c.Features.ConfigVersion = raftcluster.Version{Major: 2, Minor: 0}
			},
			want: ErrUnsupportedVersion,
		},
		{
			name: "unsupported catalog minor",
			mut: func(c *CatalogV1) {
				c.Features = DefaultFeatureSet()
				c.Features.ConfigVersion = raftcluster.Version{Major: 1, Minor: 1}
			},
			want: ErrUnsupportedVersion,
		},
		{
			name: "unknown required feature",
			mut: func(c *CatalogV1) {
				c.Features = raftcluster.FeatureSet{
					ConfigVersion: SupportedCatalogVersion,
					Required: []raftcluster.RequiredFeature{
						{Name: "treedb.raftplacement.token_ring", Version: raftcluster.Version{Major: 1, Minor: 0}},
					},
				}
			},
			want: ErrUnsupportedFeature,
		},
		{
			name: "unsupported feature major",
			mut: func(c *CatalogV1) {
				c.Features = raftcluster.FeatureSet{
					ConfigVersion: SupportedCatalogVersion,
					Required: []raftcluster.RequiredFeature{
						{Name: FeatureCollectionGroups, Version: raftcluster.Version{Major: 2, Minor: 0}},
					},
				}
			},
			want: ErrUnsupportedVersion,
		},
		{
			name: "unsupported feature minor",
			mut: func(c *CatalogV1) {
				c.Features = raftcluster.FeatureSet{
					ConfigVersion: SupportedCatalogVersion,
					Required: []raftcluster.RequiredFeature{
						{Name: FeatureCollectionGroups, Version: raftcluster.Version{Major: 1, Minor: 1}},
					},
				}
			},
			want: ErrUnsupportedVersion,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			catalog := cloneCatalog(base)
			tc.mut(&catalog)
			_, err := Validate(catalog)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Validate err=%v want errors.Is(%v)", err, tc.want)
			}
			if !errors.Is(err, ErrInvalidCatalog) {
				t.Fatalf("Validate err=%v want ErrInvalidCatalog", err)
			}
		})
	}
}

func TestResolveFailsClosedForUnplacedCollection(t *testing.T) {
	resolved, err := Validate(validCatalog())
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	_, err = resolved.ResolveCollection(DefaultDatabase, DefaultCatalog, "missing")
	if !errors.Is(err, ErrUnplacedCollection) {
		t.Fatalf("Resolve missing err=%v want ErrUnplacedCollection", err)
	}
	if !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("Resolve missing err=%v want ErrInvalidCatalog", err)
	}
}

func TestValidateAcceptsTokenRingCatalogPlacements(t *testing.T) {
	ref := CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "events"}
	for _, mode := range []PlacementModeV1{PlacementModeTokenV1, PlacementModeRingV1} {
		t.Run(string(mode), func(t *testing.T) {
			catalog := validCatalog()
			catalog.Placements = append(catalog.Placements, tokenPlacement(ref, mode))
			resolved, err := Validate(catalog)
			if err != nil {
				t.Fatalf("Validate: %v", err)
			}

			groupID, err := resolved.ResolveCollection(DefaultDatabase, DefaultCatalog, "users")
			if err != nil {
				t.Fatalf("Resolve users: %v", err)
			}
			if groupID != "group-a" {
				t.Fatalf("users group=%q want group-a", groupID)
			}
			_, err = resolved.ResolveCollection(DefaultDatabase, DefaultCatalog, ref.Collection)
			if !errors.Is(err, ErrUnsupportedPlacementMode) {
				t.Fatalf("Resolve token placement err=%v want ErrUnsupportedPlacementMode", err)
			}
			if !errors.Is(err, ErrInvalidCatalog) {
				t.Fatalf("Resolve token placement err=%v want ErrInvalidCatalog", err)
			}

			first, err := resolved.ResolveCollectionToken(DefaultDatabase, DefaultCatalog, ref.Collection, 0)
			if err != nil {
				t.Fatalf("ResolveCollectionToken first: %v", err)
			}
			if first.ID != "token-000000" || first.GroupID != "group-a" {
				t.Fatalf("first token resolved to (%q,%q), want (token-000000,group-a)", first.ID, first.GroupID)
			}
			last, err := resolved.ResolveToken(ref, maxTokenV1)
			if err != nil {
				t.Fatalf("ResolveToken last: %v", err)
			}
			if last.ID != "token-000001" || last.GroupID != "group-b" {
				t.Fatalf("last token resolved to (%q,%q), want (token-000001,group-b)", last.ID, last.GroupID)
			}

			_, err = resolved.ResolveCollectionToken(DefaultDatabase, DefaultCatalog, "users", 0)
			if !errors.Is(err, ErrUnsupportedPlacementMode) {
				t.Fatalf("ResolveCollectionToken collection placement err=%v want ErrUnsupportedPlacementMode", err)
			}

			placement, ok := resolved.Placement(ref)
			if !ok {
				t.Fatalf("Placement missing")
			}
			if placement.Mode != mode {
				t.Fatalf("placement mode=%q want %q", placement.Mode, mode)
			}
			if placement.RouteKey != RouteKeyDocumentIDV1 {
				t.Fatalf("route key=%q want %q", placement.RouteKey, RouteKeyDocumentIDV1)
			}
			if got := len(placement.TokenPartitions); got != 2 {
				t.Fatalf("token partitions=%d want 2", got)
			}
			placement.TokenPartitions[0].GroupID = "group-z"
			mutateResolvedPlacementTokenPartition(resolved, ref, "group-z")
			plan, ok := resolved.TokenPlacement(ref)
			if !ok {
				t.Fatalf("TokenPlacement missing")
			}
			plan.Partitions[0].GroupID = "group-z"
			first, err = resolved.ResolveCollectionToken(DefaultDatabase, DefaultCatalog, ref.Collection, 0)
			if err != nil {
				t.Fatalf("ResolveCollectionToken after mutation: %v", err)
			}
			if first.GroupID != "group-a" {
				t.Fatalf("catalog token partition was mutated: %+v", first)
			}
		})
	}
}

func TestValidateRejectsInvalidTokenRingCatalogPlacements(t *testing.T) {
	ref := CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "events"}
	tests := []struct {
		name string
		mut  func(*CollectionPlacementV1)
		want error
	}{
		{
			name: "missing token partitions",
			mut:  func(p *CollectionPlacementV1) { p.TokenPartitions = nil },
			want: ErrMissingTokenPartition,
		},
		{
			name: "duplicate token partition id",
			mut:  func(p *CollectionPlacementV1) { p.TokenPartitions[1].ID = p.TokenPartitions[0].ID },
			want: ErrDuplicateTokenPartition,
		},
		{
			name: "unknown token partition group",
			mut:  func(p *CollectionPlacementV1) { p.TokenPartitions[0].GroupID = "group-z" },
			want: ErrUnknownGroup,
		},
		{
			name: "invalid token range",
			mut: func(p *CollectionPlacementV1) {
				p.TokenPartitions[0].Start = 10
				p.TokenPartitions[0].End = 9
			},
			want: ErrInvalidTokenRange,
		},
		{
			name: "gap at start",
			mut:  func(p *CollectionPlacementV1) { p.TokenPartitions[0].Start = 1 },
			want: ErrTokenRangeGap,
		},
		{
			name: "middle gap",
			mut:  func(p *CollectionPlacementV1) { p.TokenPartitions[1].Start = p.TokenPartitions[0].End + 2 },
			want: ErrTokenRangeGap,
		},
		{
			name: "overlap",
			mut:  func(p *CollectionPlacementV1) { p.TokenPartitions[1].Start = p.TokenPartitions[0].End },
			want: ErrTokenRangeOverlap,
		},
		{
			name: "not full",
			mut:  func(p *CollectionPlacementV1) { p.TokenPartitions[1].End = maxTokenV1 - 1 },
			want: ErrTokenRingNotFull,
		},
		{
			name: "collection group on token placement",
			mut:  func(p *CollectionPlacementV1) { p.GroupID = "group-a" },
			want: ErrUnsupportedPlacementMode,
		},
		{
			name: "collection mode with token partitions",
			mut: func(p *CollectionPlacementV1) {
				p.Mode = PlacementModeCollectionV1
				p.GroupID = "group-a"
			},
			want: ErrInvalidTokenRing,
		},
		{
			name: "unsupported token route key",
			mut:  func(p *CollectionPlacementV1) { p.RouteKey = "tenant_id" },
			want: ErrUnsupportedRouteKey,
		},
		{
			name: "collection route key",
			mut: func(p *CollectionPlacementV1) {
				p.Mode = PlacementModeCollectionV1
				p.GroupID = "group-a"
				p.RouteKey = RouteKeyDocumentIDV1
				p.TokenPartitions = nil
			},
			want: ErrUnsupportedRouteKey,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			catalog := validCatalog()
			placement := tokenPlacement(ref, PlacementModeTokenV1)
			tc.mut(&placement)
			catalog.Placements = append(catalog.Placements, placement)
			_, err := Validate(catalog)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Validate err=%v want errors.Is(%v)", err, tc.want)
			}
			if !errors.Is(err, ErrInvalidCatalog) {
				t.Fatalf("Validate err=%v want ErrInvalidCatalog", err)
			}
		})
	}
}

func TestValidateRejectsDuplicateCollectionAcrossTokenAndCollectionPlacement(t *testing.T) {
	catalog := validCatalog()
	ref := catalog.Placements[0].Collection
	catalog.Placements = append(catalog.Placements, tokenPlacement(ref, PlacementModeRingV1))
	_, err := Validate(catalog)
	if !errors.Is(err, ErrDuplicatePlacement) {
		t.Fatalf("Validate err=%v want ErrDuplicatePlacement", err)
	}
	if !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("Validate err=%v want ErrInvalidCatalog", err)
	}
}

func TestResolvedCatalogCopiesGroupMembers(t *testing.T) {
	resolved, err := Validate(validCatalog())
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	resolved.Groups[0].Members[0] = "exported-slice-mutation"
	group, ok := resolved.Group("group-a")
	if !ok {
		t.Fatalf("Group group-a missing")
	}
	group.Members[0] = "mutated"
	group, ok = resolved.Group("group-a")
	if !ok {
		t.Fatalf("Group group-a missing after mutation")
	}
	if group.Members[0] != "node-a" {
		t.Fatalf("stored group was mutated: %v", group.Members)
	}
}

func validCatalog() CatalogV1 {
	return CatalogV1{
		Groups: []GroupV1{
			{ID: "group-a", Members: []raftcluster.NodeID{"node-a", "node-c"}, LeaderHint: "node-a"},
			{ID: "group-b", Members: []raftcluster.NodeID{"node-b", "node-c"}, LeaderHint: "node-b"},
		},
		Placements: []CollectionPlacementV1{
			{
				Collection: CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "users"},
				GroupID:    "group-a",
			},
			{
				Collection: CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "orders"},
				GroupID:    "group-b",
				Mode:       PlacementModeCollectionV1,
			},
		},
	}
}

func cloneCatalog(c CatalogV1) CatalogV1 {
	out := c
	out.Features.Required = append([]raftcluster.RequiredFeature(nil), c.Features.Required...)
	out.Groups = append([]GroupV1(nil), c.Groups...)
	for i := range out.Groups {
		out.Groups[i].Members = append([]raftcluster.NodeID(nil), c.Groups[i].Members...)
	}
	out.Placements = append([]CollectionPlacementV1(nil), c.Placements...)
	for i := range out.Placements {
		out.Placements[i].TokenPartitions = append([]TokenPartitionV1(nil), c.Placements[i].TokenPartitions...)
	}
	return out
}

func tokenPlacement(ref CollectionRefV1, mode PlacementModeV1) CollectionPlacementV1 {
	return CollectionPlacementV1{
		Collection:      ref,
		Mode:            mode,
		TokenPartitions: append([]TokenPartitionV1(nil), validTwoPartitionTokenPlan().Partitions...),
	}
}

func mutateResolvedPlacementTokenPartition(resolved ResolvedCatalogV1, ref CollectionRefV1, groupID raftcluster.GroupID) {
	for i := range resolved.Placements {
		if resolved.Placements[i].Collection == ref && len(resolved.Placements[i].TokenPartitions) > 0 {
			resolved.Placements[i].TokenPartitions[0].GroupID = groupID
			return
		}
	}
}
