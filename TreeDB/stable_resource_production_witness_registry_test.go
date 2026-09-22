package treedb_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type productionWitness struct {
	field          string
	testFile       string
	testSymbol     string
	producerFile   string
	producerSymbol string
	benchmarkFile  string
	benchmark      string
	omissionFile   string
	omissionTest   string
}

// productionWitnesses is deliberately handwritten. It is neither generated
// from RequiredReachabilityFields nor from the authority inventory. Each row
// points at a package-local executable witness because several real producers
// are intentionally unexported.
var productionWitnesses = []productionWitness{
	{"meta.index_file", "db/stable_resource_test.go", "TestCaptureStableIndexFileResourceProductionWitness", "db/stable_resource.go", "CaptureStableIndexFileResource", "db/stable_resource_test.go", "BenchmarkCaptureStableIndexFileResource", "db/stable_resource_test.go", "TestCaptureStableIndexFileResourceRejectsReboundPathBeforeFreeze"},
	{"leaf.value_ptr", "internal/valuelog/stable_resource_supported_test.go", "TestRotateToWithStableResourcesRetainsClosedAndActiveIdentities", "internal/valuelog/stable_resource.go", "RotateToWithStableResources", "internal/valuelog/stable_resource_supported_test.go", "BenchmarkStableValueLogRotation", "internal/valuelog/stable_resource_supported_test.go", "TestStableWriterCreationProofCannotBeOmitted"},
	{"leaf.outer_raw_ref", "db/standalone_leaf_page_stable_resource_test.go", "TestStandaloneLeafPageLogStableBatchCapturesExactRotatedSegments", "db/leaf_page_log_lanes.go", "AppendLeafPagesWithStableResources", "db/standalone_leaf_page_stable_resource_test.go", "BenchmarkStandaloneLeafPageStableBatchAuthority", "db/leaf_page_stable_contract_test.go", "TestLeafPageStableWrapperRejectsMalformedProviderAuthority"},
	{"leaf.outer_packed_ref", "db/leaf_generation_pack_authority_test.go", "TestLeafGenerationPackPromotionAuthorityRetainsExactPackedResourceThroughRegistration", "db/leaf_generation_pack_authority.go", "newLeafGenerationPackPromotionAuthority", "db/leaf_generation_pack_authority_test.go", "BenchmarkLeafGenerationPackPromotionAuthority", "db/leaf_generation_pack_authority_test.go", "TestLeafGenerationPackPromotionAuthorityRejectsMalformedPointers"},
	{"system.outer_leaf_generation_manifest", "db/leaf_generation_manifest_stable_test.go", "TestStableLeafGenerationManifestReplacementReturnsExactSyncedToken", "db/leaf_generation_manifest_store.go", "replaceStable", "db/leaf_generation_manifest_stable_test.go", "BenchmarkStableLeafGenerationManifestReplacement", "db/leaf_generation_manifest_stable_test.go", "TestStableLeafGenerationManifestDestinationRebindFailsClosed"},
	{"frame.dictionary_generation", "internal/dictdb/stable_resource_capture_test.go", "TestCaptureDictionaryResourcesReturnsExactTransitiveClosureForReusedPointerID", "internal/dictdb/resource_capture.go", "CaptureDictionaryResources", "internal/dictdb/stable_resource_capture_test.go", "BenchmarkCaptureDictionaryResources", "internal/dictdb/stable_resource_capture_test.go", "TestCaptureDictionaryResourcesRejectsEachMissingPointerChild"},
	{"frame.template_generation", "internal/templatedb/stable_resource_capture_test.go", "TestCaptureTemplateResourcesPointerDefinitionReturnsExactTransitiveClosure", "internal/templatedb/resource_capture.go", "CaptureTemplateResources", "internal/templatedb/stable_resource_capture_test.go", "BenchmarkCaptureTemplateResources", "internal/templatedb/stable_resource_capture_test.go", "TestCaptureTemplateResourcesRejectsEachMissingPointerChild"},
	{"column.manifest_asset_ref", "collections/stable_resource_supported_test.go", "TestStableColumnAppendSessionReturnsCoalescedPinnedAuthority", "collections/column_asset_manager.go", "closeWithStableResources", "collections/stable_resource_supported_test.go", "BenchmarkStableCentralColumnAppendSessionAuthority", "collections/stable_resource_inventory_test.go", "TestStableColumnPreparedValidationRejectsEachMissingProductionObligation"},
	{"column.typed_multipart_ref", "collections/stable_resource_supported_test.go", "TestStableColumnAppendSessionReturnsCoalescedPinnedAuthority", "collections/column_asset_manager.go", "closeWithStableResources", "collections/stable_resource_supported_test.go", "BenchmarkStableCentralColumnAppendSessionAuthority", "collections/stable_resource_inventory_test.go", "TestStableColumnPreparedValidationRejectsEachMissingProductionObligation"},
	{"column.typed_value_ref", "collections/stable_resource_supported_test.go", "TestStableColumnAppendSessionReturnsCoalescedPinnedAuthority", "collections/column_asset_manager.go", "closeWithStableResources", "collections/stable_resource_supported_test.go", "BenchmarkStableCentralColumnAppendSessionAuthority", "collections/stable_resource_inventory_test.go", "TestStableColumnPreparedValidationRejectsEachMissingProductionObligation"},
	{"column.typed_code_ref", "collections/stable_resource_supported_test.go", "TestStableColumnAppendSessionReturnsCoalescedPinnedAuthority", "collections/column_asset_manager.go", "closeWithStableResources", "collections/stable_resource_supported_test.go", "BenchmarkStableCentralColumnAppendSessionAuthority", "collections/stable_resource_inventory_test.go", "TestStableColumnPreparedValidationRejectsEachMissingProductionObligation"},
	{"column.hnsw_search_pack_ref", "collections/stable_resource_supported_test.go", "TestStableColumnAppendSessionReturnsCoalescedPinnedAuthority", "collections/column_asset_manager.go", "closeWithStableResources", "collections/stable_resource_supported_test.go", "BenchmarkStableCentralColumnAppendSessionAuthority", "collections/stable_resource_inventory_test.go", "TestStableColumnPreparedValidationRejectsEachMissingProductionObligation"},
	{"column.vector_graph_pack_ref", "collections/column_vector_rebuild_stable_authority_test.go", "TestColumnVectorGraphRebuildStableAuthorityMatchesEveryPublishedAsset", "collections/vector_index_rebuild.go", "prepareColumnVectorGraphRebuildManifestWithStableResources", "collections/column_vector_rebuild_stable_authority_test.go", "BenchmarkColumnVectorGraphStableResourceCapture", "collections/column_vector_rebuild_stable_authority_test.go", "TestColumnVectorGraphStableAuthorityRejectsEachMissingTransitiveChild"},
	{"command_wal.active_segment", "internal/commitlog/stable_resource_supported_test.go", "TestCommandJournalStableRotationDoesNotLoseClosedSegment", "internal/commitlog/journal_owner.go", "RotateActiveSegmentWithStableResources", "internal/commitlog/stable_resource_supported_test.go", "BenchmarkStableCommandWALRotation", "internal/commitlog/stable_resource_supported_test.go", "TestCommandJournalStableRotationRejectsEachMissingSegment"},
	{"command_wal.rotated_segment", "internal/commitlog/stable_resource_supported_test.go", "TestCommandJournalStableRotationDoesNotLoseClosedSegment", "internal/commitlog/journal_owner.go", "RotateActiveSegmentWithStableResources", "internal/commitlog/stable_resource_supported_test.go", "BenchmarkStableCommandWALRotation", "internal/commitlog/stable_resource_supported_test.go", "TestCommandJournalStableRotationRejectsEachMissingSegment"},
	{"command_wal_v2.external_rid_fence", "internal/valuelog/stable_resource_test.go", "TestCaptureStableExternalRIDFenceRequiresEveryManagerChild", "internal/valuelog/stable_resource.go", "CaptureStableExternalRIDFence", "internal/valuelog/stable_resource_test.go", "BenchmarkStableValueLogExternalRIDFenceClosure", "internal/valuelog/stable_resource_test.go", "TestCaptureStableExternalRIDFenceRequiresEveryManagerChild"},
}

// negativeProductionWitnesses is the independent fail-closed registry. These
// fields must remain outside producer registration until their adjacent owner
// or separate durability milestone explicitly changes the policy.
var negativeProductionWitnesses = []string{
	"meta.target_page",
	"meta.user_root_page_id",
	"meta.system_root_page_id",
	"meta.freelist_head_id",
	"system.collection_root_descriptor",
	"collection.primary_root",
	"collection.template_root",
	"collection.index_state_root",
	"collection.column_manifest_root",
	"collection.secondary_root",
	"collection.vector_root",
	"collection.text_dictionary_root",
	"collection.text_posting_root",
	"collection.text_position_root",
	"collection.legacy_vector_manifest",
	"column.query_ready_base_v1",
	"column.query_ready_delta_v1",
	"column.query_ready_consolidated_base_v1",
	"meta.legacy_active_slab",
	"raft.snapshot_manifest",
}

func TestProductionWitnessRegistryExactlyMatchesCanonicalPolicy(t *testing.T) {
	positive := make(map[rootpublication.ReachabilityField]struct{}, len(productionWitnesses))
	for _, witness := range productionWitnesses {
		field := rootpublication.ReachabilityField(witness.field)
		if _, duplicate := positive[field]; duplicate {
			t.Fatalf("duplicate production witness for %q", field)
		}
		positive[field] = struct{}{}
		policy, ok := rootpublication.StableResourcePolicyFor(field)
		if !ok || !policy.Registerable {
			t.Errorf("positive witness field %q policy=%+v exists=%t", field, policy, ok)
		}
		requireNamedFunction(t, witness.testFile, witness.testSymbol, true)
		requireNamedFunction(t, witness.producerFile, witness.producerSymbol, false)
		requireNamedFunction(t, witness.benchmarkFile, witness.benchmark, false)
		requireNamedFunction(t, witness.omissionFile, witness.omissionTest, true)
		if !strings.HasPrefix(witness.benchmark, "Benchmark") {
			t.Errorf("benchmark witness %q is not an executable Go benchmark", witness.benchmark)
		}
	}

	canonical := rootpublication.RequiredReachabilityFields()
	if len(positive) != len(canonical) {
		t.Errorf("production witnesses=%d canonical registerable fields=%d", len(positive), len(canonical))
	}
	for _, field := range canonical {
		if _, ok := positive[field]; !ok {
			t.Errorf("canonical registerable field %q has no real production witness", field)
		}
	}

	negative := make(map[rootpublication.ReachabilityField]struct{}, len(negativeProductionWitnesses))
	for _, literal := range negativeProductionWitnesses {
		field := rootpublication.ReachabilityField(literal)
		if _, duplicate := negative[field]; duplicate {
			t.Fatalf("duplicate negative witness for %q", field)
		}
		negative[field] = struct{}{}
		if _, overlap := positive[field]; overlap {
			t.Errorf("field %q has positive and negative witnesses", field)
		}
		policy, ok := rootpublication.StableResourcePolicyFor(field)
		if !ok || policy.Registerable {
			t.Errorf("negative witness field %q policy=%+v exists=%t", field, policy, ok)
		}
	}
	for _, row := range rootpublication.StableResourceInventory() {
		policy, ok := rootpublication.StableResourcePolicyFor(row.Field)
		if !ok {
			t.Errorf("inventory field %q has no policy", row.Field)
			continue
		}
		if !policy.Registerable {
			if _, ok := negative[row.Field]; !ok {
				t.Errorf("non-registerable field %q has no explicit negative witness", row.Field)
			}
		}
	}
	if got, want := len(negative), len(rootpublication.StableResourceInventory())-len(canonical); got != want {
		t.Errorf("negative witnesses=%d canonical non-registerable fields=%d", got, want)
	}
}

func requireNamedFunction(t *testing.T, filename, symbol string, requireTest bool) {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), filename, nil, 0)
	if err != nil {
		t.Fatalf("parse %s for %s: %v", filename, symbol, err)
	}
	found := false
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == symbol {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("%s does not declare production witness symbol %s", filename, symbol)
	}
	if requireTest && (len(symbol) < len("Test") || symbol[:len("Test")] != "Test") {
		t.Errorf("witness symbol %q is not an executable Go test", symbol)
	}
}
