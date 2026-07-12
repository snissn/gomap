package powerlossoracle

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
)

// MaxVariantsPerCut is the hard safety bound for one publication cut. A
// caller may request a smaller bound, but never a larger one.
const MaxVariantsPerCut = 256

// VariantFamily identifies one legal partial-writeback family. The strings are
// persisted in the counterexample ledger and are therefore stable.
type VariantFamily string

const (
	VariantSyncedOnly           VariantFamily = "synced-only"
	VariantTargetMetaOnly       VariantFamily = "target-meta-only"
	VariantOneMissingDependency VariantFamily = "one-missing-dependency"
	VariantDataWithoutNamespace VariantFamily = "data-without-namespace"
	VariantNamespaceWithoutData VariantFamily = "namespace-without-data"
	VariantFullWriteback        VariantFamily = "full-writeback"
	VariantTornFormat           VariantFamily = "torn-format"
	VariantOldPageReuse         VariantFamily = "old-page-reuse"
)

var VariantFamilies = []VariantFamily{
	VariantSyncedOnly,
	VariantTargetMetaOnly,
	VariantOneMissingDependency,
	VariantDataWithoutNamespace,
	VariantNamespaceWithoutData,
	VariantFullWriteback,
	VariantTornFormat,
	VariantOldPageReuse,
}

// FormatKind names an on-disk boundary rather than an arbitrary byte subset.
type FormatKind string

const (
	FormatMeta       FormatKind = "meta"
	FormatRootRecord FormatKind = "root-record"
	FormatFreelist   FormatKind = "freelist"
	FormatIndexPage  FormatKind = "index-page"
)

// TornBoundary describes the prefix of one format-bounded write that may have
// reached stable storage. Persisted must be strictly inside Length.
type TornBoundary struct {
	ID        string
	Format    FormatKind
	Offset    int64
	Length    int64
	Persisted int64
}

// DirtyResource binds a stable logical identity to the path used only for
// materialization. IDs, never paths, participate in cut and variant IDs.
type DirtyResource struct {
	Kind          ResourceKind
	ID            string
	Path          string
	Ranges        []ByteRange
	NewName       bool
	NamespaceDirs []string
	Torn          []TornBoundary
}

func (r DirtyResource) identity() string { return string(r.Kind) + "/" + r.ID }

// CutSpec is the complete bounded input for one publication cut.
type CutSpec struct {
	ID               string
	Point            CutPoint
	Occurrence       int
	Model            *Model
	TargetMeta       *DirtyResource
	Dependencies     []DirtyResource
	OldPageWrites    []DirtyResource
	RequiredFamilies []VariantFamily
	ExpectedByFamily map[VariantFamily]ExpectedResult
	MaxVariants      int
}

// Variant is one deterministic stable-only crash image.
type Variant struct {
	CutID     string
	ID        string
	Family    VariantFamily
	Seed      uint64
	Expected  ExpectedResult
	Qualifier string
	Model     *Model
}

// Coverage records why generation succeeded and makes skipped families
// observable to tests and the checked-in ledger.
type Coverage struct {
	CutID       string
	Generated   int
	ByFamily    map[VariantFamily]int
	ByFormat    map[FormatKind]int
	MaxVariants int
}

// GenerateVariants derives legal partial-writeback images from stable resource
// identities, exact format boundaries, and namespace durability events.
func GenerateVariants(spec CutSpec) ([]Variant, Coverage, error) {
	coverage := Coverage{ByFamily: make(map[VariantFamily]int), ByFormat: make(map[FormatKind]int)}
	if spec.Model == nil {
		return nil, coverage, errorsf("cut %q has no model", spec.ID)
	}
	if spec.ID == "" || spec.Point == "" || spec.Occurrence < 0 {
		return nil, coverage, errorsf("cut requires a stable id, point, and non-negative occurrence")
	}
	limit := spec.MaxVariants
	if limit == 0 {
		limit = MaxVariantsPerCut
	}
	if limit < 1 || limit > MaxVariantsPerCut {
		return nil, coverage, errorsf("cut %q max variants %d is outside [1,%d]", spec.ID, limit, MaxVariantsPerCut)
	}
	coverage.MaxVariants = limit
	cutID := fmt.Sprintf("cut/%s/%s/%03d", stableComponent(spec.ID), stableComponent(string(spec.Point)), spec.Occurrence)
	coverage.CutID = cutID
	if err := validateRequiredFamilies(spec.RequiredFamilies); err != nil {
		return nil, coverage, err
	}

	dependencies := append([]DirtyResource(nil), spec.Dependencies...)
	sortResources(dependencies)
	oldWrites := append([]DirtyResource(nil), spec.OldPageWrites...)
	sortResources(oldWrites)
	all := append([]DirtyResource(nil), dependencies...)
	if spec.TargetMeta != nil {
		all = append(all, *spec.TargetMeta)
	}
	if err := validateResources(spec.Model, all); err != nil {
		return nil, coverage, err
	}
	if err := validateResources(spec.Model, oldWrites); err != nil {
		return nil, coverage, err
	}

	variants := make([]Variant, 0, 2+len(dependencies)+len(oldWrites))
	add := func(family VariantFamily, qualifier string, model *Model, format FormatKind) error {
		if len(variants) == limit {
			return errorsf("cut %s exceeds bounded variant limit %d", cutID, limit)
		}
		expected, exists := spec.ExpectedByFamily[family]
		if !exists {
			return errorsf("cut %s generated family %s without an expected result", cutID, family)
		}
		if !isExpectedResult(expected) {
			return errorsf("cut %s family %s has unknown expected result %q", cutID, family, expected)
		}
		id := cutID + "/variant/" + stableComponent(string(family))
		if qualifier != "" {
			id += "/" + stableComponent(qualifier)
		}
		variants = append(variants, Variant{CutID: cutID, ID: id, Family: family, Seed: stableSeed(id), Expected: expected, Qualifier: qualifier, Model: model})
		coverage.ByFamily[family]++
		if format != "" {
			coverage.ByFormat[format]++
		}
		return nil
	}

	if err := add(VariantSyncedOnly, "", spec.Model.Clone(), ""); err != nil {
		return nil, coverage, err
	}
	if spec.TargetMeta != nil {
		model := spec.Model.Clone()
		if err := promoteFull(model, *spec.TargetMeta); err != nil {
			return nil, coverage, err
		}
		if err := add(VariantTargetMetaOnly, spec.TargetMeta.identity(), model, ""); err != nil {
			return nil, coverage, err
		}
		for omitted := range dependencies {
			model := spec.Model.Clone()
			if err := promoteFull(model, *spec.TargetMeta); err != nil {
				return nil, coverage, err
			}
			for i, resource := range dependencies {
				if i == omitted {
					continue
				}
				if err := promoteComplete(model, resource); err != nil {
					return nil, coverage, err
				}
			}
			if err := add(VariantOneMissingDependency, dependencies[omitted].identity(), model, ""); err != nil {
				return nil, coverage, err
			}
		}
	}

	for _, resource := range dependencies {
		if !resource.NewName {
			continue
		}
		dataOnly := spec.Model.Clone()
		if err := promoteFull(dataOnly, resource); err != nil {
			return nil, coverage, err
		}
		if err := add(VariantDataWithoutNamespace, resource.identity(), dataOnly, ""); err != nil {
			return nil, coverage, err
		}
		nameOnly := spec.Model.Clone()
		if err := promoteNamespace(nameOnly, resource); err != nil {
			return nil, coverage, err
		}
		if err := add(VariantNamespaceWithoutData, resource.identity(), nameOnly, ""); err != nil {
			return nil, coverage, err
		}
	}

	if len(all) > 0 {
		model := spec.Model.Clone()
		for _, resource := range all {
			if err := promoteComplete(model, resource); err != nil {
				return nil, coverage, err
			}
		}
		if err := add(VariantFullWriteback, "", model, ""); err != nil {
			return nil, coverage, err
		}
	}

	for _, tornResource := range all {
		torn := append([]TornBoundary(nil), tornResource.Torn...)
		sort.Slice(torn, func(i, j int) bool { return torn[i].ID < torn[j].ID })
		for _, boundary := range torn {
			model := spec.Model.Clone()
			for _, resource := range all {
				if resource.identity() == tornResource.identity() {
					continue
				}
				if err := promoteComplete(model, resource); err != nil {
					return nil, coverage, err
				}
			}
			if err := model.PromoteRange(tornResource.Path, boundary.Offset, boundary.Persisted); err != nil {
				return nil, coverage, err
			}
			qualifier := tornResource.identity() + "/" + string(boundary.Format) + "/" + boundary.ID
			if err := add(VariantTornFormat, qualifier, model, boundary.Format); err != nil {
				return nil, coverage, err
			}
		}
	}

	for _, resource := range oldWrites {
		model := spec.Model.Clone()
		if err := promoteFull(model, resource); err != nil {
			return nil, coverage, err
		}
		if err := add(VariantOldPageReuse, resource.identity(), model, ""); err != nil {
			return nil, coverage, err
		}
	}

	sort.Slice(variants, func(i, j int) bool { return variants[i].ID < variants[j].ID })
	coverage.Generated = len(variants)
	for _, family := range spec.RequiredFamilies {
		if coverage.ByFamily[family] == 0 {
			return nil, coverage, errorsf("cut %s silently skipped required family %s", cutID, family)
		}
	}
	return variants, coverage, nil
}

func validateRequiredFamilies(families []VariantFamily) error {
	known := make(map[VariantFamily]struct{}, len(VariantFamilies))
	for _, family := range VariantFamilies {
		known[family] = struct{}{}
	}
	for _, family := range families {
		if _, ok := known[family]; !ok {
			return errorsf("unknown required variant family %q", family)
		}
	}
	return nil
}

func validateResources(model *Model, resources []DirtyResource) error {
	seen := make(map[string]struct{}, len(resources))
	for _, resource := range resources {
		if resource.Kind == "" || resource.ID == "" || resource.Path == "" {
			return errorsf("dirty resource requires kind, stable id, and path: %+v", resource)
		}
		identity := resource.identity()
		if _, ok := seen[identity]; ok {
			return errorsf("duplicate dirty resource identity %s", identity)
		}
		seen[identity] = struct{}{}
		if resource.NewName && len(resource.NamespaceDirs) == 0 {
			return errorsf("new resource %s has no namespace directory", identity)
		}
		ranges := append([]ByteRange(nil), resource.Ranges...)
		if len(ranges) == 0 && len(resource.Torn) > 0 {
			var err error
			ranges, err = model.ChangedRanges(resource.Path)
			if err != nil {
				return err
			}
		}
		var actualChanged []ByteRange
		if len(resource.Torn) > 0 {
			var err error
			actualChanged, err = model.ChangedRanges(resource.Path)
			if err != nil {
				return err
			}
		}
		tornIDs := make(map[string]bool, len(resource.Torn))
		selectors := make(map[string]bool, len(resource.Torn))
		for _, boundary := range resource.Torn {
			if boundary.ID == "" || !knownFormat(boundary.Format) || boundary.Offset < 0 || boundary.Length < 2 || boundary.Persisted < 1 || boundary.Persisted >= boundary.Length || boundary.Offset > int64(^uint64(0)>>1)-boundary.Length {
				return errorsf("resource %s has invalid torn boundary %+v", identity, boundary)
			}
			if tornIDs[boundary.ID] {
				return errorsf("resource %s has duplicate torn boundary id %q", identity, boundary.ID)
			}
			tornIDs[boundary.ID] = true
			selector := string(boundary.Format) + "/" + boundary.ID
			if selectors[selector] {
				return errorsf("resource %s has duplicate torn selector %q", identity, selector)
			}
			selectors[selector] = true
			contained := false
			for _, changed := range ranges {
				if changed.Offset <= boundary.Offset && boundary.Offset+boundary.Length <= changed.Offset+changed.Length {
					contained = true
					break
				}
			}
			if !contained {
				return errorsf("resource %s torn boundary %q is outside its changed ranges", identity, boundary.ID)
			}
			actuallyChanged := false
			for _, changed := range actualChanged {
				if changed.Offset <= boundary.Offset && boundary.Offset+boundary.Length <= changed.Offset+changed.Length {
					actuallyChanged = true
					break
				}
			}
			if !actuallyChanged {
				return errorsf("resource %s torn boundary %q includes unchanged bytes", identity, boundary.ID)
			}
		}
	}
	return nil
}

func knownFormat(format FormatKind) bool {
	switch format {
	case FormatMeta, FormatRootRecord, FormatFreelist, FormatIndexPage:
		return true
	default:
		return false
	}
}

func sortResources(resources []DirtyResource) {
	sort.Slice(resources, func(i, j int) bool { return resources[i].identity() < resources[j].identity() })
}

func promoteFull(model *Model, resource DirtyResource) error {
	ranges := append([]ByteRange(nil), resource.Ranges...)
	if len(ranges) == 0 {
		var err error
		ranges, err = model.ChangedRanges(resource.Path)
		if err != nil {
			return err
		}
	}
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].Offset == ranges[j].Offset {
			return ranges[i].Length < ranges[j].Length
		}
		return ranges[i].Offset < ranges[j].Offset
	})
	for _, changed := range ranges {
		if err := model.PromoteRange(resource.Path, changed.Offset, changed.Length); err != nil {
			return err
		}
	}
	return nil
}

func promoteNamespace(model *Model, resource DirtyResource) error {
	dirs := append([]string(nil), resource.NamespaceDirs...)
	sort.Slice(dirs, func(i, j int) bool {
		leftDepth := strings.Count(dirs[i], "/")
		rightDepth := strings.Count(dirs[j], "/")
		if leftDepth == rightDepth {
			return dirs[i] < dirs[j]
		}
		return leftDepth < rightDepth
	})
	for _, dir := range dirs {
		if err := model.SyncDir(dir); err != nil {
			return err
		}
	}
	return nil
}

func promoteComplete(model *Model, resource DirtyResource) error {
	if err := promoteFull(model, resource); err != nil {
		return err
	}
	if resource.NewName {
		return promoteNamespace(model, resource)
	}
	return nil
}

func stableComponent(value string) string { return url.PathEscape(value) }

func stableSeed(value string) uint64 {
	sum := sha256.Sum256([]byte(value))
	return binary.BigEndian.Uint64(sum[:8])
}

func errorsf(format string, args ...any) error {
	return fmt.Errorf("powerlossoracle: "+format, args...)
}

// ShardVariants selects a balanced deterministic shard without changing IDs.
func ShardVariants(variants []Variant, shard, shards int) ([]Variant, error) {
	if shards < 1 || shard < 0 || shard >= shards {
		return nil, errorsf("invalid shard %d/%d", shard, shards)
	}
	ordered := append([]Variant(nil), variants...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	out := make([]Variant, 0, (len(ordered)+shards-1)/shards)
	for i, variant := range ordered {
		if i%shards == shard {
			out = append(out, variant)
		}
	}
	return out, nil
}

// ReplaySelector is the host-path-independent address carried by ledger replay
// commands.
type ReplaySelector struct {
	CutID     string
	VariantID string
	Seed      uint64
}

const (
	EnvReplayCut     = "TREEDB_POWERLOSS_CUT_ID"
	EnvReplayVariant = "TREEDB_POWERLOSS_VARIANT_ID"
	EnvReplaySeed    = "TREEDB_POWERLOSS_SEED"
)

func ReplaySelectorFromEnv() (ReplaySelector, error) {
	selector := ReplaySelector{CutID: os.Getenv(EnvReplayCut), VariantID: os.Getenv(EnvReplayVariant)}
	seedText := os.Getenv(EnvReplaySeed)
	if selector.CutID == "" && selector.VariantID == "" && seedText == "" {
		return selector, nil
	}
	if selector.CutID == "" || selector.VariantID == "" || seedText == "" {
		return ReplaySelector{}, errorsf("replay selection requires %s, %s, and %s", EnvReplayCut, EnvReplayVariant, EnvReplaySeed)
	}
	seed, err := strconv.ParseUint(seedText, 10, 64)
	if err != nil {
		return ReplaySelector{}, errorsf("parse replay seed %q: %v", seedText, err)
	}
	selector.Seed = seed
	return selector, nil
}

func SelectReplayVariant(variants []Variant, selector ReplaySelector) ([]Variant, error) {
	if selector == (ReplaySelector{}) {
		return variants, nil
	}
	for _, variant := range variants {
		if variant.CutID == selector.CutID && variant.ID == selector.VariantID && variant.Seed == selector.Seed {
			return []Variant{variant}, nil
		}
	}
	return nil, errorsf("replay selection cut=%q variant=%q seed=%d matched no generated image", selector.CutID, selector.VariantID, selector.Seed)
}

// StableSizeBytes reports the materialized regular-file bytes for evidence.
func (m *Model) StableSizeBytes() int64 {
	var size int64
	for _, id := range m.stable {
		size += int64(len(m.inodes[id].stable))
	}
	return size
}
