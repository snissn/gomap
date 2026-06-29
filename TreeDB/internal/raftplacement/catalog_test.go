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
			name: "token mode unsupported",
			mut:  func(c *CatalogV1) { c.Placements[0].Mode = "token" },
			want: ErrUnsupportedPlacementMode,
		},
		{
			name: "ring mode unsupported",
			mut:  func(c *CatalogV1) { c.Placements[0].Mode = "ring" },
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
	return out
}
