package powerlossoracle

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// allKindProductionDependencies is intentionally literal and independent of
// the rootpublication inventory. The top-level production-witness registry
// checks that both independently maintained sets stay equal.
var allKindProductionDependencies = []struct {
	field string
	kind  ResourceKind
}{
	{"meta.index_file", ResourceIndex},
	{"leaf.value_ptr", ResourceValueLog},
	{"leaf.outer_raw_ref", ResourceOuterLeaf},
	{"leaf.outer_packed_ref", ResourceOuterLeaf},
	{"system.outer_leaf_generation_manifest", ResourceOuterLeaf},
	{"frame.dictionary_generation", ResourceAuxiliary},
	{"frame.template_generation", ResourceAuxiliary},
	{"column.manifest_asset_ref", ResourceAuxiliary},
	{"column.typed_multipart_ref", ResourceAuxiliary},
	{"column.typed_value_ref", ResourceAuxiliary},
	{"column.typed_code_ref", ResourceAuxiliary},
	{"column.hnsw_search_pack_ref", ResourceAuxiliary},
	{"column.vector_graph_pack_ref", ResourceAuxiliary},
	{"command_wal.active_segment", ResourceCommandWAL},
	{"command_wal.rotated_segment", ResourceCommandWAL},
	{"command_wal_v2.external_rid_fence", ResourceValueLog},
}

func TestAllKindAuthorityGeneratesStableTargetAndAllButOneVariants(t *testing.T) {
	// This is deliberately generator-only coverage built from a literal shape.
	// The TreeDB package's TestProductionAuthorityNamespaceAsymmetryVariants
	// separately binds real producer tokens to their exact files and evaluates
	// every generated namespace image through typed recovery.
	required := rootpublication.RequiredReachabilityFields()
	if len(allKindProductionDependencies) != len(required) {
		t.Fatalf("all-kind generator fields=%d canonical registerable fields=%d", len(allKindProductionDependencies), len(required))
	}
	witnessed := make(map[rootpublication.ReachabilityField]struct{}, len(allKindProductionDependencies))
	for _, dependency := range allKindProductionDependencies {
		field := rootpublication.ReachabilityField(dependency.field)
		if _, duplicate := witnessed[field]; duplicate {
			t.Fatalf("duplicate all-kind generator field %q", field)
		}
		witnessed[field] = struct{}{}
	}
	for _, field := range required {
		if _, ok := witnessed[field]; !ok {
			t.Errorf("canonical registerable field %q has no all-kind omission variant", field)
		}
	}

	first, firstCoverage, firstRoot, firstPaths := allKindAuthorityVariantFixture(t)
	second, secondCoverage, _, _ := allKindAuthorityVariantFixture(t)
	if firstCoverage.Generated != 51 || secondCoverage.Generated != 51 {
		t.Fatalf("generated variants first=%d second=%d want 51", firstCoverage.Generated, secondCoverage.Generated)
	}
	if firstCoverage.ByFamily[VariantSyncedOnly] != 1 ||
		firstCoverage.ByFamily[VariantTargetMetaOnly] != 1 ||
		firstCoverage.ByFamily[VariantOneMissingDependency] != len(allKindProductionDependencies) ||
		firstCoverage.ByFamily[VariantDataWithoutNamespace] != len(allKindProductionDependencies) ||
		firstCoverage.ByFamily[VariantNamespaceWithoutData] != len(allKindProductionDependencies) ||
		firstCoverage.ByFamily[VariantFullWriteback] != 1 {
		t.Fatalf("all-kind coverage=%+v", firstCoverage.ByFamily)
	}
	if got, want := variantIdentityList(first), variantIdentityList(second); !reflect.DeepEqual(got, want) {
		t.Fatalf("variant IDs/seeds changed across host roots\nfirst=%v\nsecond=%v", got, want)
	}

	for _, variant := range first {
		targetStable := requireAllKindPathStable(t, variant.Model, firstRoot, firstPaths["target"])
		stableDependencies := 0
		omittedIdentity := ""
		qualifiedPath := ""
		for _, dependency := range allKindProductionDependencies {
			path := firstPaths[dependency.field]
			identity := string(dependency.kind) + "/" + dependency.field
			if variant.Qualifier == identity {
				qualifiedPath = path
			}
			if requireAllKindPathStable(t, variant.Model, firstRoot, path) {
				stableDependencies++
			} else {
				omittedIdentity = identity
			}
		}
		switch variant.Family {
		case VariantSyncedOnly:
			if targetStable || stableDependencies != 0 {
				t.Fatalf("synced-only target=%t stable_dependencies=%d", targetStable, stableDependencies)
			}
		case VariantTargetMetaOnly:
			if !targetStable || stableDependencies != 0 {
				t.Fatalf("target-only target=%t stable_dependencies=%d", targetStable, stableDependencies)
			}
		case VariantOneMissingDependency:
			if !targetStable || stableDependencies != len(allKindProductionDependencies)-1 || variant.Qualifier != omittedIdentity {
				t.Fatalf("one-missing qualifier=%q omitted=%q target=%t stable_dependencies=%d", variant.Qualifier, omittedIdentity, targetStable, stableDependencies)
			}
		case VariantDataWithoutNamespace:
			if targetStable || stableDependencies != 0 || qualifiedPath == "" || containsString(variant.Model.StablePaths(), qualifiedPath) {
				t.Fatalf("data-without-namespace qualifier=%q path=%q target=%t stable_dependencies=%d stable_paths=%v", variant.Qualifier, qualifiedPath, targetStable, stableDependencies, variant.Model.StablePaths())
			}
		case VariantNamespaceWithoutData:
			if targetStable || stableDependencies != 0 || qualifiedPath == "" || !containsString(variant.Model.StablePaths(), qualifiedPath) {
				t.Fatalf("namespace-without-data qualifier=%q path=%q target=%t stable_dependencies=%d stable_paths=%v", variant.Qualifier, qualifiedPath, targetStable, stableDependencies, variant.Model.StablePaths())
			}
		case VariantFullWriteback:
			if !targetStable || stableDependencies != len(allKindProductionDependencies) {
				t.Fatalf("full target=%t stable_dependencies=%d", targetStable, stableDependencies)
			}
		default:
			t.Fatalf("unexpected all-kind family %q", variant.Family)
		}
	}
	t.Logf("all-kind authority generator: fields=%d variants=%d omissions=%d", len(allKindProductionDependencies), firstCoverage.Generated, firstCoverage.ByFamily[VariantOneMissingDependency])
}

func allKindAuthorityVariantFixture(t *testing.T) ([]Variant, Coverage, string, map[string]string) {
	t.Helper()
	root := t.TempDir()
	paths := make(map[string]string, len(allKindProductionDependencies)+1)
	paths["target"] = "target-meta"
	if err := os.WriteFile(filepath.Join(root, paths["target"]), []byte("old-target"), 0o600); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := model.Write(paths["target"], []byte("new-target")); err != nil {
		t.Fatal(err)
	}
	dependencies := make([]DirtyResource, 0, len(allKindProductionDependencies))
	for i, dependency := range allKindProductionDependencies {
		namespace := fmt.Sprintf("namespaces/dependency-%02d", i)
		path := namespace + "/resource"
		paths[dependency.field] = path
		if err := model.Create(path, []byte("new-"+dependency.field)); err != nil {
			t.Fatal(err)
		}
		dependencies = append(dependencies, DirtyResource{
			Kind:          dependency.kind,
			ID:            dependency.field,
			Path:          path,
			NewName:       true,
			NamespaceDirs: []string{".", "namespaces", namespace},
		})
	}
	target := DirtyResource{Kind: ResourceIndex, ID: "target-meta", Path: paths["target"]}
	variants, coverage, err := GenerateVariants(CutSpec{
		ID: "all-kind-production-authority", Point: AfterMetaWrite, Occurrence: 0,
		Model: model, TargetMeta: &target, Dependencies: dependencies,
		RequiredFamilies: []VariantFamily{
			VariantSyncedOnly,
			VariantTargetMetaOnly,
			VariantOneMissingDependency,
			VariantDataWithoutNamespace,
			VariantNamespaceWithoutData,
			VariantFullWriteback,
		},
		ExpectedByFamily: map[VariantFamily]ExpectedResult{
			VariantSyncedOnly:           ExpectedOldRoot,
			VariantTargetMetaOnly:       ExpectedCorruption,
			VariantOneMissingDependency: ExpectedCorruption,
			VariantDataWithoutNamespace: ExpectedOldRoot,
			VariantNamespaceWithoutData: ExpectedOldRoot,
			VariantFullWriteback:        ExpectedNewRoot,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return variants, coverage, root, paths
}

func requireAllKindPathStable(t *testing.T, model *Model, root, path string) bool {
	t.Helper()
	stable, err := model.PathStable(root, filepath.Join(root, path))
	if err != nil {
		t.Fatal(err)
	}
	return stable
}

func variantIdentityList(variants []Variant) []string {
	out := make([]string, len(variants))
	for i, variant := range variants {
		out[i] = fmt.Sprintf("%s=%d", variant.ID, variant.Seed)
	}
	sort.Strings(out)
	return out
}
