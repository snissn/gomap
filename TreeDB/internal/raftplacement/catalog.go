package raftplacement

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

const (
	FeatureCollectionGroups raftcluster.FeatureName = "treedb.raftplacement.collection_groups"

	DefaultDatabase = "default"
	DefaultCatalog  = "default"
)

var (
	SupportedCatalogVersion = raftcluster.Version{Major: 1, Minor: 0}
	SupportedFeatureFloors  = map[raftcluster.FeatureName]raftcluster.Version{
		FeatureCollectionGroups:                     {Major: 1, Minor: 0},
		raftcluster.FeatureVectorPartitionLifecycle: {Major: 1, Minor: 0},
	}

	ErrInvalidCatalog           = errors.New("raftplacement: invalid catalog")
	ErrMissingGroup             = errors.New("raftplacement: missing group")
	ErrDuplicateGroup           = errors.New("raftplacement: duplicate group")
	ErrMissingMember            = errors.New("raftplacement: missing member")
	ErrDuplicateMember          = errors.New("raftplacement: duplicate member")
	ErrLeaderHintNotMember      = errors.New("raftplacement: leader hint not member")
	ErrUnknownGroup             = errors.New("raftplacement: unknown group")
	ErrInvalidCollection        = errors.New("raftplacement: invalid collection")
	ErrDuplicatePlacement       = errors.New("raftplacement: duplicate placement")
	ErrUnplacedCollection       = errors.New("raftplacement: unplaced collection")
	ErrUnsupportedFeature       = errors.New("raftplacement: unsupported feature")
	ErrUnsupportedVersion       = errors.New("raftplacement: unsupported version")
	ErrUnsupportedPlacementMode = errors.New("raftplacement: unsupported placement mode")
	ErrUnsupportedRouteKey      = errors.New("raftplacement: unsupported route key")
)

type PlacementModeV1 string

const (
	PlacementModeCollectionV1 PlacementModeV1 = "collection"
	PlacementModeTokenV1      PlacementModeV1 = "token"
	PlacementModeRingV1       PlacementModeV1 = "ring"
)

type RouteKeyV1 string

const (
	RouteKeyDocumentIDV1 RouteKeyV1 = "_id"
)

type CollectionRefV1 struct {
	Database   string
	Catalog    string
	Collection string
}

type GroupV1 struct {
	ID         raftcluster.GroupID
	Members    []raftcluster.NodeID
	LeaderHint raftcluster.NodeID
}

type CollectionPlacementV1 struct {
	Collection      CollectionRefV1
	GroupID         raftcluster.GroupID
	Mode            PlacementModeV1
	RouteKey        RouteKeyV1
	TokenPartitions []TokenPartitionV1
}

type CatalogV1 struct {
	Features   raftcluster.FeatureSet
	Groups     []GroupV1
	Placements []CollectionPlacementV1
}

type ResolvedGroupV1 struct {
	ID         raftcluster.GroupID
	Members    []raftcluster.NodeID
	LeaderHint raftcluster.NodeID
}

type ResolvedCollectionPlacementV1 struct {
	Collection      CollectionRefV1
	GroupID         raftcluster.GroupID
	Mode            PlacementModeV1
	RouteKey        RouteKeyV1
	TokenPartitions []ResolvedTokenPartitionV1
}

type ResolvedCatalogV1 struct {
	Features   raftcluster.FeatureSet
	Groups     []ResolvedGroupV1
	Placements []ResolvedCollectionPlacementV1

	groups     map[raftcluster.GroupID]ResolvedGroupV1
	placements map[CollectionRefV1]ResolvedCollectionPlacementV1
	tokens     map[CollectionRefV1]ResolvedTokenRingPlanV1
}

func DefaultFeatureSet() raftcluster.FeatureSet {
	return raftcluster.FeatureSet{
		ConfigVersion: SupportedCatalogVersion,
		Required: []raftcluster.RequiredFeature{
			{Name: FeatureCollectionGroups, Version: SupportedFeatureFloors[FeatureCollectionGroups]},
		},
	}
}

func Validate(c CatalogV1) (ResolvedCatalogV1, error) {
	features, err := validateFeatures(c.Features)
	if err != nil {
		return ResolvedCatalogV1{}, err
	}
	groups, groupIndex, err := validateGroups(c.Groups)
	if err != nil {
		return ResolvedCatalogV1{}, err
	}
	placements, placementIndex, tokenIndex, err := validatePlacements(c.Placements, groupIndex)
	if err != nil {
		return ResolvedCatalogV1{}, err
	}
	return ResolvedCatalogV1{
		Features:   cloneFeatureSet(features),
		Groups:     cloneResolvedGroups(groups),
		Placements: cloneResolvedPlacements(placements),
		groups:     cloneGroupIndex(groupIndex),
		placements: clonePlacementIndex(placementIndex),
		tokens:     cloneTokenPlacementIndex(tokenIndex),
	}, nil
}

func (c ResolvedCatalogV1) Resolve(ref CollectionRefV1) (raftcluster.GroupID, error) {
	if err := validateCollectionRef(ref); err != nil {
		return "", err
	}
	placement, ok := c.placements[ref]
	if !ok {
		return "", errors.Join(ErrInvalidCatalog, ErrUnplacedCollection, fmt.Errorf("%s/%s/%s", ref.Database, ref.Catalog, ref.Collection))
	}
	if placement.Mode != PlacementModeCollectionV1 {
		return "", errors.Join(ErrInvalidCatalog, ErrUnsupportedPlacementMode, fmt.Errorf("%s/%s/%s mode %q requires token-aware resolution", ref.Database, ref.Catalog, ref.Collection, placement.Mode))
	}
	return placement.GroupID, nil
}

func (c ResolvedCatalogV1) ResolveCollection(database, catalog, collection string) (raftcluster.GroupID, error) {
	return c.Resolve(CollectionRefV1{Database: database, Catalog: catalog, Collection: collection})
}

func (c ResolvedCatalogV1) ResolveToken(ref CollectionRefV1, token uint64) (ResolvedTokenPartitionV1, error) {
	if err := validateCollectionRef(ref); err != nil {
		return ResolvedTokenPartitionV1{}, err
	}
	placement, ok := c.placements[ref]
	if !ok {
		return ResolvedTokenPartitionV1{}, errors.Join(ErrInvalidCatalog, ErrUnplacedCollection, fmt.Errorf("%s/%s/%s", ref.Database, ref.Catalog, ref.Collection))
	}
	if placement.Mode != PlacementModeTokenV1 && placement.Mode != PlacementModeRingV1 {
		return ResolvedTokenPartitionV1{}, errors.Join(ErrInvalidCatalog, ErrUnsupportedPlacementMode, fmt.Errorf("%s/%s/%s mode %q has no token partitions", ref.Database, ref.Catalog, ref.Collection, placement.Mode))
	}
	plan, ok := c.tokens[ref]
	if !ok {
		return ResolvedTokenPartitionV1{}, errors.Join(ErrInvalidCatalog, ErrInvalidTokenRing, fmt.Errorf("%s/%s/%s has no resolved token plan", ref.Database, ref.Catalog, ref.Collection))
	}
	return plan.ResolveToken(token)
}

func (c ResolvedCatalogV1) ResolveCollectionToken(database, catalog, collection string, token uint64) (ResolvedTokenPartitionV1, error) {
	return c.ResolveToken(CollectionRefV1{Database: database, Catalog: catalog, Collection: collection}, token)
}

func (c ResolvedCatalogV1) Group(id raftcluster.GroupID) (ResolvedGroupV1, bool) {
	group, ok := c.groups[id]
	if !ok {
		return ResolvedGroupV1{}, false
	}
	group.Members = append([]raftcluster.NodeID(nil), group.Members...)
	return group, true
}

func (c ResolvedCatalogV1) Placement(ref CollectionRefV1) (ResolvedCollectionPlacementV1, bool) {
	placement, ok := c.placements[ref]
	if !ok {
		return ResolvedCollectionPlacementV1{}, false
	}
	return cloneResolvedPlacement(placement), true
}

func (c ResolvedCatalogV1) TokenPlacement(ref CollectionRefV1) (ResolvedTokenRingPlanV1, bool) {
	plan, ok := c.tokens[ref]
	if !ok {
		return ResolvedTokenRingPlanV1{}, false
	}
	return cloneResolvedTokenRingPlan(plan), true
}

func cloneFeatureSet(features raftcluster.FeatureSet) raftcluster.FeatureSet {
	return raftcluster.FeatureSet{
		ConfigVersion: features.ConfigVersion,
		Required:      append([]raftcluster.RequiredFeature(nil), features.Required...),
	}
}

func cloneResolvedGroups(groups []ResolvedGroupV1) []ResolvedGroupV1 {
	out := make([]ResolvedGroupV1, len(groups))
	for i, group := range groups {
		out[i] = cloneResolvedGroup(group)
	}
	return out
}

func cloneResolvedGroup(group ResolvedGroupV1) ResolvedGroupV1 {
	group.Members = append([]raftcluster.NodeID(nil), group.Members...)
	return group
}

func cloneGroupIndex(groups map[raftcluster.GroupID]ResolvedGroupV1) map[raftcluster.GroupID]ResolvedGroupV1 {
	out := make(map[raftcluster.GroupID]ResolvedGroupV1, len(groups))
	for id, group := range groups {
		out[id] = cloneResolvedGroup(group)
	}
	return out
}

func cloneResolvedPlacements(placements []ResolvedCollectionPlacementV1) []ResolvedCollectionPlacementV1 {
	out := make([]ResolvedCollectionPlacementV1, len(placements))
	for i, placement := range placements {
		out[i] = cloneResolvedPlacement(placement)
	}
	return out
}

func cloneResolvedPlacement(placement ResolvedCollectionPlacementV1) ResolvedCollectionPlacementV1 {
	placement.TokenPartitions = cloneResolvedTokenPartitions(placement.TokenPartitions)
	return placement
}

func clonePlacementIndex(placements map[CollectionRefV1]ResolvedCollectionPlacementV1) map[CollectionRefV1]ResolvedCollectionPlacementV1 {
	out := make(map[CollectionRefV1]ResolvedCollectionPlacementV1, len(placements))
	for ref, placement := range placements {
		out[ref] = cloneResolvedPlacement(placement)
	}
	return out
}

func cloneTokenPlacementIndex(tokens map[CollectionRefV1]ResolvedTokenRingPlanV1) map[CollectionRefV1]ResolvedTokenRingPlanV1 {
	out := make(map[CollectionRefV1]ResolvedTokenRingPlanV1, len(tokens))
	for ref, plan := range tokens {
		out[ref] = cloneResolvedTokenRingPlan(plan)
	}
	return out
}

func validateFeatures(features raftcluster.FeatureSet) (raftcluster.FeatureSet, error) {
	if versionIsZero(features.ConfigVersion) && len(features.Required) == 0 {
		return DefaultFeatureSet(), nil
	}
	if versionIsZero(features.ConfigVersion) {
		features.ConfigVersion = SupportedCatalogVersion
	}
	if features.ConfigVersion.Major != SupportedCatalogVersion.Major || features.ConfigVersion.Minor > SupportedCatalogVersion.Minor {
		return raftcluster.FeatureSet{}, errors.Join(ErrInvalidCatalog, ErrUnsupportedVersion, fmt.Errorf("catalog version %d.%d exceeds supported %d.%d", features.ConfigVersion.Major, features.ConfigVersion.Minor, SupportedCatalogVersion.Major, SupportedCatalogVersion.Minor))
	}
	if len(features.Required) == 0 {
		features.Required = DefaultFeatureSet().Required
	}
	out := raftcluster.FeatureSet{ConfigVersion: features.ConfigVersion, Required: make([]raftcluster.RequiredFeature, 0, len(features.Required))}
	seen := make(map[raftcluster.FeatureName]struct{}, len(features.Required))
	for _, required := range features.Required {
		required.Name = raftcluster.FeatureName(strings.TrimSpace(string(required.Name)))
		floor, ok := SupportedFeatureFloors[required.Name]
		if !ok {
			return raftcluster.FeatureSet{}, errors.Join(ErrInvalidCatalog, ErrUnsupportedFeature, fmt.Errorf("required feature %q", required.Name))
		}
		if required.Version.Major != floor.Major || required.Version.Minor > floor.Minor {
			return raftcluster.FeatureSet{}, errors.Join(ErrInvalidCatalog, ErrUnsupportedVersion, fmt.Errorf("required feature %q version %d.%d exceeds supported floor %d.%d", required.Name, required.Version.Major, required.Version.Minor, floor.Major, floor.Minor))
		}
		if _, exists := seen[required.Name]; exists {
			return raftcluster.FeatureSet{}, errors.Join(ErrInvalidCatalog, ErrUnsupportedFeature, fmt.Errorf("duplicate required feature %q", required.Name))
		}
		seen[required.Name] = struct{}{}
		out.Required = append(out.Required, required)
	}
	sort.Slice(out.Required, func(i, j int) bool {
		return out.Required[i].Name < out.Required[j].Name
	})
	return out, nil
}

func versionIsZero(v raftcluster.Version) bool {
	return v.Major == 0 && v.Minor == 0
}

func validateGroups(groups []GroupV1) ([]ResolvedGroupV1, map[raftcluster.GroupID]ResolvedGroupV1, error) {
	if len(groups) == 0 {
		return nil, nil, errors.Join(ErrInvalidCatalog, ErrMissingGroup, fmt.Errorf("at least one group is required"))
	}
	out := make([]ResolvedGroupV1, 0, len(groups))
	index := make(map[raftcluster.GroupID]ResolvedGroupV1, len(groups))
	for i, group := range groups {
		if err := validateID("group id", string(group.ID)); err != nil {
			return nil, nil, errors.Join(ErrInvalidCatalog, ErrMissingGroup, fmt.Errorf("group[%d]: %w", i, err))
		}
		if _, exists := index[group.ID]; exists {
			return nil, nil, errors.Join(ErrInvalidCatalog, ErrDuplicateGroup, fmt.Errorf("group %q appears more than once", group.ID))
		}
		members, err := validateMembers(group.ID, group.Members)
		if err != nil {
			return nil, nil, err
		}
		if group.LeaderHint != "" {
			if err := validateID("leader hint", string(group.LeaderHint)); err != nil {
				return nil, nil, errors.Join(ErrInvalidCatalog, ErrLeaderHintNotMember, fmt.Errorf("group %q: %w", group.ID, err))
			}
			if !hasMember(members, group.LeaderHint) {
				return nil, nil, errors.Join(ErrInvalidCatalog, ErrLeaderHintNotMember, fmt.Errorf("group %q leader hint %q is not a member", group.ID, group.LeaderHint))
			}
		}
		resolved := ResolvedGroupV1{
			ID:         group.ID,
			Members:    members,
			LeaderHint: group.LeaderHint,
		}
		out = append(out, resolved)
		index[group.ID] = resolved
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})
	return out, index, nil
}

func validateMembers(groupID raftcluster.GroupID, members []raftcluster.NodeID) ([]raftcluster.NodeID, error) {
	if len(members) == 0 {
		return nil, errors.Join(ErrInvalidCatalog, ErrMissingMember, fmt.Errorf("group %q has no members", groupID))
	}
	out := make([]raftcluster.NodeID, 0, len(members))
	seen := make(map[raftcluster.NodeID]struct{}, len(members))
	for i, member := range members {
		if err := validateID("member id", string(member)); err != nil {
			return nil, errors.Join(ErrInvalidCatalog, ErrMissingMember, fmt.Errorf("group %q member[%d]: %w", groupID, i, err))
		}
		if _, exists := seen[member]; exists {
			return nil, errors.Join(ErrInvalidCatalog, ErrDuplicateMember, fmt.Errorf("group %q member %q appears more than once", groupID, member))
		}
		seen[member] = struct{}{}
		out = append(out, member)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out, nil
}

func validatePlacements(placements []CollectionPlacementV1, groups map[raftcluster.GroupID]ResolvedGroupV1) ([]ResolvedCollectionPlacementV1, map[CollectionRefV1]ResolvedCollectionPlacementV1, map[CollectionRefV1]ResolvedTokenRingPlanV1, error) {
	out := make([]ResolvedCollectionPlacementV1, 0, len(placements))
	index := make(map[CollectionRefV1]ResolvedCollectionPlacementV1, len(placements))
	tokens := make(map[CollectionRefV1]ResolvedTokenRingPlanV1)
	for i, placement := range placements {
		ref := placement.Collection
		if err := validateCollectionRef(ref); err != nil {
			return nil, nil, nil, errors.Join(err, fmt.Errorf("placement[%d]", i))
		}
		mode := placement.Mode
		if mode == "" {
			mode = PlacementModeCollectionV1
		}
		if _, exists := index[ref]; exists {
			return nil, nil, nil, errors.Join(ErrInvalidCatalog, ErrDuplicatePlacement, fmt.Errorf("placement for %s/%s/%s appears more than once", ref.Database, ref.Catalog, ref.Collection))
		}
		resolved := ResolvedCollectionPlacementV1{
			Collection: ref,
			Mode:       mode,
		}
		switch mode {
		case PlacementModeCollectionV1:
			if placement.RouteKey != "" {
				return nil, nil, nil, errors.Join(ErrInvalidCatalog, ErrUnsupportedRouteKey, fmt.Errorf("placement[%d] %s/%s/%s collection mode must not set route key %q", i, ref.Database, ref.Catalog, ref.Collection, placement.RouteKey))
			}
			if len(placement.TokenPartitions) != 0 {
				return nil, nil, nil, errors.Join(ErrInvalidCatalog, ErrInvalidTokenRing, fmt.Errorf("placement[%d] %s/%s/%s collection mode includes token partitions", i, ref.Database, ref.Catalog, ref.Collection))
			}
			if err := validateID("placement group id", string(placement.GroupID)); err != nil {
				return nil, nil, nil, errors.Join(ErrInvalidCatalog, ErrUnknownGroup, fmt.Errorf("placement[%d]: %w", i, err))
			}
			if _, ok := groups[placement.GroupID]; !ok {
				return nil, nil, nil, errors.Join(ErrInvalidCatalog, ErrUnknownGroup, fmt.Errorf("placement[%d] references group %q", i, placement.GroupID))
			}
			resolved.GroupID = placement.GroupID
		case PlacementModeTokenV1, PlacementModeRingV1:
			routeKey := placement.RouteKey
			if routeKey == "" {
				routeKey = RouteKeyDocumentIDV1
			}
			if routeKey != RouteKeyDocumentIDV1 {
				return nil, nil, nil, errors.Join(ErrInvalidCatalog, ErrUnsupportedRouteKey, fmt.Errorf("placement[%d] %s/%s/%s mode %q route key %q is not supported", i, ref.Database, ref.Catalog, ref.Collection, mode, routeKey))
			}
			if placement.GroupID != "" {
				return nil, nil, nil, errors.Join(ErrInvalidCatalog, ErrUnsupportedPlacementMode, fmt.Errorf("placement[%d] %s/%s/%s mode %q must not set collection group %q", i, ref.Database, ref.Catalog, ref.Collection, mode, placement.GroupID))
			}
			plan, err := validateTokenRingPartitions(placement.TokenPartitions, func(id raftcluster.GroupID) bool {
				_, ok := groups[id]
				return ok
			})
			if err != nil {
				return nil, nil, nil, errors.Join(ErrInvalidCatalog, err, fmt.Errorf("placement[%d] %s/%s/%s", i, ref.Database, ref.Catalog, ref.Collection))
			}
			resolved.RouteKey = routeKey
			resolved.TokenPartitions = cloneResolvedTokenPartitions(plan.Partitions)
			tokens[ref] = cloneResolvedTokenRingPlan(plan)
		default:
			return nil, nil, nil, errors.Join(ErrInvalidCatalog, ErrUnsupportedPlacementMode, fmt.Errorf("placement[%d] %s/%s/%s mode %q", i, ref.Database, ref.Catalog, ref.Collection, mode))
		}
		out = append(out, resolved)
		index[ref] = cloneResolvedPlacement(resolved)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i].Collection, out[j].Collection
		if a.Database != b.Database {
			return a.Database < b.Database
		}
		if a.Catalog != b.Catalog {
			return a.Catalog < b.Catalog
		}
		return a.Collection < b.Collection
	})
	return out, index, tokens, nil
}

func validateCollectionRef(ref CollectionRefV1) error {
	if err := validateScopeSegment("database", ref.Database); err != nil {
		return errors.Join(ErrInvalidCatalog, ErrInvalidCollection, err)
	}
	if err := validateScopeSegment("catalog", ref.Catalog); err != nil {
		return errors.Join(ErrInvalidCatalog, ErrInvalidCollection, err)
	}
	if err := validateCollectionName(ref.Collection); err != nil {
		return errors.Join(ErrInvalidCatalog, ErrInvalidCollection, err)
	}
	return nil
}

func validateScopeSegment(label, value string) error {
	if len(value) == 0 {
		return fmt.Errorf("%s is empty", label)
	}
	if len(value) > 128 {
		return fmt.Errorf("%s is too long", label)
	}
	if strings.ContainsAny(value, "\x00/:") {
		return fmt.Errorf("%s contains reserved punctuation", label)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("%s has leading or trailing spaces", label)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s is invalid utf-8", label)
	}
	return nil
}

func validateCollectionName(name string) error {
	if len(name) == 0 {
		return errors.New("collection name cannot be empty")
	}
	if len(name) > 128 {
		return errors.New("collection name too long")
	}
	if strings.ContainsAny(name, "\x00/:") {
		return errors.New("collection name contains reserved punctuation")
	}
	if strings.TrimSpace(name) != name {
		return errors.New("collection name has leading or trailing spaces")
	}
	if !utf8.ValidString(name) {
		return errors.New("collection name invalid utf-8")
	}
	return nil
}

func validateID(label, id string) error {
	if id == "" {
		return fmt.Errorf("%s is empty", label)
	}
	if strings.TrimSpace(id) != id {
		return fmt.Errorf("%s has leading or trailing whitespace", label)
	}
	if id == "." || id == ".." {
		return fmt.Errorf("%s %q is not a valid path segment", label, id)
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_' || r == '.':
		default:
			return fmt.Errorf("%s %q contains unsupported character %q", label, id, r)
		}
	}
	return nil
}

func hasMember(members []raftcluster.NodeID, id raftcluster.NodeID) bool {
	i := sort.Search(len(members), func(i int) bool {
		return members[i] >= id
	})
	return i < len(members) && members[i] == id
}
