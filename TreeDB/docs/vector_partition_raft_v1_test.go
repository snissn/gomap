package docs_test

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestDocsVectorPartitionRaftM0Contract(t *testing.T) {
	root, _ := repoRoots(t)
	b, err := os.ReadFile(filepath.Join(root, "docs", "spec", "vector-partition-raft-v1.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"_id", "logical vector", "Raft replica", "overlap membership", "vector_index_id", "source_snapshot_generation", "partition_generation", "router_generation", "building", "ready", "cutover", "invalidated", "snapshot-bound", "fail closed", "simulation_only", "SearchRouteHNSWSearchPack=1", "exact_hnsw_search_pack_v1", "response-owned", "sorted partition-ID set", "TestTreeDBHNSWStageUsesExactSearchPackAndMatchesHighEFLocalTruth", "treedb_vector_partition_fixture_v2", "ieee754_binary64_explicit_fma_v1", "full fixture checksum", "canonical selected-stage set", "overrides both environment values", "exact_truth_queries", "ordered exact-truth", "gp-ann", "Parent invariant matrix", "TestTruthOracleTieOrderingAndAllPartitionParity", "M1 owns", "M8", "VPM1", "V1 API/schema contract, VPM1 wire version 3, and bounds", "Lifecycle, publication, and cleanup authority", "AcquireVectorPartitionReaderPinV1", "Durable ingress inventory", "SubmitCommandEntryWithVectorPartitionAdmissionV1", "ConfirmCommittedVectorPartitionMutationV1", "V1 operator boundary (#4018)", "OperationsConfigV1{Enabled:true}", "catalog_mismatch", "topology_unavailable"} {
		if !strings.Contains(string(b), s) {
			t.Fatalf("contract missing %q", s)
		}
	}
}

func TestDocsVectorPartitionServingSnapshotNoLogAuthorityContract(t *testing.T) {
	root, _ := repoRoots(t)
	for path, needles := range map[string][]string{
		filepath.Join(root, "docs", "spec", "vector-partition-raft-v1.md"): {
			"immutable serving snapshot", "appends no `LogBarrier` entry", "short process-local monotonic",
			"existing pins drain", "no quorum call", "#4096", "OperationsV1.SearchFast",
			"OperationsV1.PinSearchSnapshot", "local wait", "current atomic authorization overlay", "#4098",
		},
		filepath.Join(root, "docs", "spec", "raftcluster.md"): {
			"LinearizableCatalogMetaReadProofV1", "committed entry from that term",
			"do not append `LogBarrier` entries", "caller-trusted cross-process capability",
		},
	} {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, needle := range needles {
			if !strings.Contains(string(b), needle) {
				t.Fatalf("%s missing %q", path, needle)
			}
		}
	}
}

func TestDocsVectorPartitionV1CorrectnessAndApproximationContract(t *testing.T) {
	root, _ := repoRoots(t)
	b, err := os.ReadFile(filepath.Join(root, "docs", "spec", "vector-partition-v1-contract.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, needle := range []string{
		"Status: internal, pre-alpha, experimental/off",
		"exact_partition_union_v1",
		"approximate_hnsw_recall_qualified_v1",
		"not an exact or rerank-rescued path",
		"manifest/source identity",
		"(score descending, stable ID bytewise ascending)",
		"all-or-error",
		"generation_mismatch",
		"route_mismatch",
		"assets_unavailable",
		"source_mismatch",
		"placement partition/group drift",
		"mutation invalidates",
		"IDs/scores-only",
		"must not fetch or materialize documents",
		"Issue #3999",
		"historical scoped HNSW evidence",
		"#4013 owns this V1 contract",
		"TestM8ProductionMultiGroupAssetsCheckedIn10kCISmokeV1",
		"TestM8CanonicalFP32ScoreContractTiePrecisionAndDedupeV1",
		"TestDocsVectorPartitionV1CorrectnessAndApproximationContract",
	} {
		if !strings.Contains(string(b), needle) {
			t.Fatalf("V1 contract missing %q", needle)
		}
	}
	index, err := os.ReadFile(filepath.Join(root, "docs", "spec", "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(index), "TreeDB/docs/spec/vector-partition-v1-contract.md") {
		t.Fatal("spec index does not admit the V1 vector-partition contract")
	}
	verification, err := os.ReadFile(filepath.Join(root, "docs", "spec", "verification.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, needle := range []string{
		"Vector partition V1 correctness and approximation verification",
		"TestM8ProductionMultiGroupAssetsCheckedIn10kCISmokeV1",
		"TestPartitionLocalHNSWStageIsRecallQualifiedNotExact",
		"TestDocsVectorPartitionV1CorrectnessAndApproximationContract",
		"GOWORK=off go test -count=1 ./cmd/treedb_vector_partition_bench",
		"GOWORK=off go test -count=1 ./TreeDB/docs",
	} {
		if !strings.Contains(string(verification), needle) {
			t.Fatalf("verification matrix missing %q", needle)
		}
	}
}

func TestDocsVectorPartitionCoordinatorM6Contract(t *testing.T) {
	root, _ := repoRoots(t)
	checks := []struct {
		path    string
		needles []string
	}{
		{
			path: filepath.Join(root, "docs", "spec", "vector-partition-coordinator-v1.md"),
			needles: []string{
				"Status: internal, pre-alpha, experimental",
				"VectorPartitionCoordinatorV1",
				"NewVectorPartitionCoordinatorForTopologyV1",
				"VectorPartitionShardSearchDispatcherV1",
				"linearizable_generation_snapshot",
				"`basic` stats mode; `none` is rejected",
				"There is no partially successful state",
				"MaxConcurrentRequests",
				"MaxWallClock",
				"one retry and one redirect",
				"approximate native HNSW traversal",
				"longest valid group",
				"M5's downstream stable-ID ceiling",
				"max(membership_rows[p] * 64, conservative_search_scratch_bytes(p), ef_search * 64)",
				"only the remaining surplus is divided deterministically by membership weight",
				"applied term may precede the read term",
				"total candidate bytes",
				"bytewise lexicographically smaller stable ID",
				"synthetic read proofs",
				"no production network or",
				"M8",
			},
		},
		{
			path: filepath.Join(root, "docs", "performance", "vector-partition-m6.md"),
			needles: []string{
				"d7f1e0f399b1a563b91177e93281f7e91ee55728",
				"93f48763467aefdf9b45ba0f7d22847f7f0c66ed",
				"result_kind=coordinator_local_service_simulation",
				"production_evidence=false",
				"in_process_no_production_network",
				"synthetic_local_proof_not_measured",
				"1,000,000 / 32 / 16",
				"exact all-partition parity passed",
				"0.067219%",
				"GOMAXPROCS",
				"GOMEMLIMIT=16GiB",
				"804ddf634f8afd05f5ca4d70154de0fc14aa972ba66ce81e9399287177cf9465",
				"3b0e7db5c33668dd68b2b89ab47663ba821c21cca1f930584cde5aa9f9515a81",
				"96f1a915fde33bc5a70e4bfbcb8782374356a12a4886e2fdac01ee7294c7383f",
				"7a2d65bcff053ab60750551915dc4b882c00ecfb256afe9fa90de2e18fb83f5b",
				"7f1685a08b8905a30facf60d288d6ecdeac148cc31a2a9b5e0299bc4226a4ea8",
				"lower-probe recall/latency curve",
			},
		},
		{
			path: filepath.Join(root, "docs", "spec", "README.md"),
			needles: []string{
				"TreeDB/docs/spec/vector-partition-coordinator-v1.md",
				"scoped local-service evidence",
			},
		},
		{
			path: filepath.Join(root, "docs", "spec", "verification.md"),
			needles: []string{
				"Vector partition M6 coordinator verification",
				"all-or-error",
				"not network,",
				"TreeDB/docs/performance/vector-partition-m6.md",
			},
		},
		{
			path: filepath.Join(root, "docs", "spec", "vector-partition-raft-v1.md"),
			needles: []string{
				"M5 now owns the bounded shard-search service contract",
				"M6 owns transport-neutral coordinator fanout and merged top-k",
				"remain M8 work",
			},
		},
	}
	for _, check := range checks {
		b, err := os.ReadFile(check.path)
		if err != nil {
			t.Fatal(err)
		}
		for _, needle := range check.needles {
			if !strings.Contains(string(b), needle) {
				t.Fatalf("%s missing %q", check.path, needle)
			}
		}
	}
}

func TestDocsVectorPartitionM8ProductionContract(t *testing.T) {
	root, _ := repoRoots(t)
	checks := []struct {
		path    string
		needles []string
	}{
		{
			path: filepath.Join(root, "docs", "spec", "vector-partition-m8-production-topology.md"),
			needles: []string{
				"Status: internal, pre-alpha, experimental/off",
				"VectorPartitionM8ProductionMultiGroupV1",
				"real three-node HashiCorp Raft data groups",
				"serialized M5 TCP services",
				"production_multi_group",
				"There is no standalone M8 administration command",
				"-m8-existing-db DIR",
				"-m8-variant-dbs",
				"schema-5 retained descriptor",
				"canonical build-identity digest",
				"partition-builder configuration",
				"one `(probes, ef_search, concurrency)` operating point",
				"multiplies the complete measured",
				"bounded 128 MiB aggregate",
				"actual slowest completed request",
				"Custom shard limits are normalized once",
				"superseded retained p4 calibration",
				"all 256 retained representatives",
				"MUST return no partial neighbors",
				"stable-ID hash assignment",
				"shard-request ceiling",
				"explicitly accepts the narrower result",
			},
		},
		{
			path: filepath.Join(root, "docs", "spec", "vector-partition-v1-qualification.md"),
			needles: []string{
				"p1/p2/p4/p8/p16",
				"budget `256`",
			},
		},
		{
			path: filepath.Join(root, "docs", "performance", "vector-partition-m8.md"),
			needles: []string{
				"9f3cb7c6f8d5aa8283fe2342d9f341cbdbebab48",
				"final local matrix is complete",
				"1,000,000 / 1 / 16",
				"52,974.66 MB",
				"14a3e98ae8d4ab74bb4edb78e6858d8ce7e3d7733eebd3136b1321c0eda236b1",
				"unavailable endpoint returned error, zero neighbors, zero groups",
				"no configured process-resource limit was compared",
				"Enablement stays off",
				"3b52711665297c7396f1f86238840dee1ea2897b",
				"fp32_normalized_cosine_binary64_accum_score_desc_stable_id_asc_best_duplicate_v1",
				"9afad07fb8daf374076fe9fa630106ffdf6241ae746344349eb1885d37cdfbd1",
				"its all-partition owner is partition-local HNSW",
				"8ad06a6e95423c8992638965230862e1ce917d30",
				"d3347736200332cd2a81333a9053899f725eb66309a3b3ca3743376e60d030d2",
				"Each schema-3 descriptor binds",
				"3c7a5665803b2f8f32f0187376b31faa74b7b712d8b7d94b28aea7114db6f556",
				"Coupled graph acceptance",
				"enablement_off_follow_up_required",
				"#3998",
				"#3999",
				"#4001",
			},
		},
		{
			path: filepath.Join(root, "docs", "spec", "vector-partition-raft-v1.md"),
			needles: []string{
				"Canonical V1 partition-result contract",
				"VectorPartitionCanonicalScoreContractV1",
				"candidate is rescored with the canonical contract",
				"Production evidence schema 3",
				"approximate-route local HNSW",
			},
		},
	}
	for _, check := range checks {
		b, err := os.ReadFile(check.path)
		if err != nil {
			t.Fatal(err)
		}
		for _, needle := range check.needles {
			if !strings.Contains(string(b), needle) {
				t.Fatalf("%s missing %q", check.path, needle)
			}
		}
	}
	storage, err := os.ReadFile(filepath.Join(root, "docs", "spec", "storage-format.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, needle := range []string{"m3_bounded_overlap_v1:capacity=<u64>,budget=<u64>,realized=<u64>,unspent=<u64>", "budget = realized + unspent", ",build_identity=<64-lowercase-hex-sha256>", "covered by the VPM1 integrity digest"} {
		if !strings.Contains(string(storage), needle) {
			t.Fatalf("storage format missing %q", needle)
		}
	}
	raw, err := os.ReadFile(filepath.Join(root, "docs", "performance", "vector-partition-m8-evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	var evidence struct {
		Status           string `json:"status"`
		FeatureEnabled   bool   `json:"feature_enabled"`
		MeasuredCodeHead string `json:"measured_code_head"`
		CheckedIn10K     struct {
			Vectors            int     `json:"vectors"`
			Queries            int     `json:"queries"`
			QuarterProbeRecall float64 `json:"quarter_probe_best_recall_at_10"`
		} `json:"checked_in_10k"`
		Retained1M struct {
			Vectors                int `json:"vectors"`
			Groups                 int `json:"raft_groups"`
			MeasuredRows           int `json:"measured_rows"`
			UnsupportedOverlapRows int `json:"unsupported_overlap_rows"`
		} `json:"retained_1m"`
		GateLedger struct {
			ResourceBounds     string `json:"resource_bounds"`
			ResourceBoundsNote string `json:"resource_bounds_note"`
		} `json:"gate_ledger"`
		Continuation struct {
			Status           string `json:"status"`
			SchemaVersion    int    `json:"schema_version"`
			ResultKind       string `json:"result_kind"`
			MeasuredCodeHead string `json:"measured_code_head"`
			Contract         string `json:"canonical_contract"`
			CheckedIn10K     struct {
				ExhaustiveRecall float64 `json:"exhaustive_partition_union_recall_at_10"`
				IDParity         bool    `json:"exhaustive_partition_union_id_parity"`
				ScoreParity      bool    `json:"exhaustive_partition_union_score_parity"`
			} `json:"checked_in_10k"`
			Retained1M struct {
				ExhaustiveRecall  float64 `json:"exhaustive_partition_union_recall_at_10"`
				ExactRouterRecall float64 `json:"all_partition_exact_representative_recall_at_10"`
				LocalHNSWRecall   float64 `json:"all_partition_local_hnsw_recall_at_10"`
				LossOwner         string  `json:"all_partition_loss_owner"`
			} `json:"retained_1m"`
		} `json:"continuation_attribution"`
		FinalLocalGate struct {
			SchemaVersion                  int    `json:"schema_version"`
			ResultKind                     string `json:"result_kind"`
			Status                         string `json:"status"`
			Disposition                    string `json:"disposition"`
			MeasuredCodeHead               string `json:"measured_code_head"`
			ArtifactSHA256                 string `json:"artifact_sha256"`
			MeasuredBinarySHA256           string `json:"measured_binary_sha256"`
			MeasuredBinaryBuild            string `json:"measured_binary_build"`
			VariantDescriptorSchemaVersion int    `json:"variant_descriptor_schema_version"`
			VariantDescriptorResultKind    string `json:"variant_descriptor_result_kind"`
			DirtyChildReportsRejected      bool   `json:"dirty_child_reports_rejected"`
			AllChildReportsClean           bool   `json:"all_child_reports_clean"`
			Fixture                        struct {
				Vectors  int    `json:"vectors"`
				Queries  int    `json:"queries"`
				Checksum string `json:"checksum"`
			} `json:"fixture"`
			GateLedger struct {
				RequiredVariants string `json:"required_variants"`
				Exhaustive       string `json:"exhaustive_correctness"`
				Recall           string `json:"recall"`
				OverlapStorage   string `json:"overlap_storage"`
				CoupledGraph     string `json:"coupled_graph_acceptance"`
				Resources        string `json:"resource_bounds"`
			} `json:"gate_ledger"`
			Variants []struct {
				ID               string  `json:"variant_id"`
				AssignmentBasis  string  `json:"assignment_basis"`
				Overlap          float64 `json:"overlap"`
				ArtifactSHA256   string  `json:"artifact_sha256"`
				GraphSHA256      string  `json:"graph_artifact_sha256"`
				BuildIdentity    string  `json:"build_identity_digest"`
				ManifestIdentity string  `json:"manifest_integrity_digest"`
				IndexDefinition  string  `json:"index_definition_digest"`
				RouterModel      string  `json:"router_model_digest"`
				PartitionHNSWM   int     `json:"partition_hnsw_m"`
				PeakRSSBytes     uint64  `json:"peak_rss_bytes"`
				MaxLoad          uint64  `json:"max_partition_load"`
				BalanceHardCap   uint64  `json:"balance_hard_cap"`
				AllPartitionHNSW float64 `json:"all_partition_local_hnsw_recall_at_10"`
			} `json:"variants"`
			OverlapRequestedMemberships int     `json:"overlap_requested_memberships"`
			OverlapRealizedMemberships  int     `json:"overlap_realized_memberships"`
			OverlapMaterializationRatio float64 `json:"overlap_materialization_ratio"`
			OverlapBudgetUtilization    float64 `json:"overlap_budget_utilization"`
			OverlapStorageRatio         float64 `json:"overlap_storage_ratio"`
			ResourceMeasurementBasis    string  `json:"resource_measurement_basis"`
			BalanceHardCapBasis         string  `json:"balance_hard_cap_basis"`
			TopologySnapshotBoundary    string  `json:"topology_snapshot_boundary"`
			MatrixParentMaterializes    *bool   `json:"matrix_parent_materializes_fixture"`
			RetryRedirectLimitBasis     string  `json:"retry_redirect_limit_basis"`
			AggregateCandidateBytes     struct {
				Configured  uint64 `json:"configured"`
				MaxObserved uint64 `json:"max_observed"`
				Passed      bool   `json:"passed"`
			} `json:"aggregate_candidate_bytes"`
			CoordinatorRouterCandidates struct {
				Configured uint64 `json:"configured"`
				Observed   uint64 `json:"observed"`
				Passed     bool   `json:"passed"`
			} `json:"coordinator_router_candidates"`
			MaximumRequestWallClockNanos        uint64 `json:"maximum_request_wall_clock_nanos"`
			SuccessfulUntimedResourceBoundaries []struct {
				VariantID            string `json:"variant_id"`
				SelectedPartitions   uint64 `json:"selected_partitions"`
				EFSearch             uint64 `json:"ef_search"`
				WallClockNanos       uint64 `json:"wall_clock_nanos"`
				Requests             uint64 `json:"requests"`
				RPCs                 uint64 `json:"rpcs"`
				RequestBytes         uint64 `json:"request_bytes"`
				CandidateBytes       uint64 `json:"candidate_bytes"`
				ResponseBytes        uint64 `json:"response_bytes"`
				MaxShardPartitions   uint64 `json:"max_shard_partitions"`
				MaxShardRequestBytes uint64 `json:"max_shard_request_bytes"`
			} `json:"successful_untimed_resource_boundaries"`
			StoppedGroupFaultResourceBoundary struct {
				SelectedPartitions   uint64   `json:"selected_partitions"`
				EFSearch             uint64   `json:"ef_search"`
				Requests             uint64   `json:"requests"`
				RPCs                 uint64   `json:"rpcs"`
				RequestBytes         uint64   `json:"request_bytes"`
				MaxShardPartitions   uint64   `json:"max_shard_partitions"`
				MaxShardRequestBytes uint64   `json:"max_shard_request_bytes"`
				CandidateBytes       uint64   `json:"candidate_bytes"`
				ResponseBytes        uint64   `json:"response_bytes"`
				WallClockByVariant   []uint64 `json:"wall_clock_nanos_by_variant"`
			} `json:"stopped_group_fault_resource_boundary"`
			FollowUps []int `json:"follow_up_issues"`
		} `json:"continuation_final_local_gate"`
	}
	if err := json.Unmarshal(raw, &evidence); err != nil {
		t.Fatal(err)
	}
	if evidence.Status != "experimental_off_gate_failures" || evidence.FeatureEnabled ||
		evidence.MeasuredCodeHead != "9f3cb7c6f8d5aa8283fe2342d9f341cbdbebab48" ||
		evidence.CheckedIn10K.Vectors != 10_000 || evidence.CheckedIn10K.Queries != 128 ||
		evidence.CheckedIn10K.QuarterProbeRecall != 0.24140625 ||
		evidence.Retained1M.Vectors != 1_000_000 || evidence.Retained1M.Groups != 4 ||
		evidence.Retained1M.MeasuredRows != 60 || evidence.Retained1M.UnsupportedOverlapRows != 1 ||
		evidence.GateLedger.ResourceBounds != "measured_not_bounded" ||
		!strings.Contains(evidence.GateLedger.ResourceBoundsNote, "no configured resource limit was compared") ||
		evidence.Continuation.Status != "diagnosed_experimental_off" || evidence.Continuation.SchemaVersion != 2 ||
		evidence.Continuation.ResultKind != "m8_production_multi_group_evidence_v2" ||
		evidence.Continuation.MeasuredCodeHead != "3b52711665297c7396f1f86238840dee1ea2897b" ||
		evidence.Continuation.Contract != "fp32_normalized_cosine_binary64_accum_score_desc_stable_id_asc_best_duplicate_v1" ||
		evidence.Continuation.CheckedIn10K.ExhaustiveRecall != 1 || !evidence.Continuation.CheckedIn10K.IDParity || !evidence.Continuation.CheckedIn10K.ScoreParity ||
		evidence.Continuation.Retained1M.ExhaustiveRecall != 1 || evidence.Continuation.Retained1M.ExactRouterRecall != 1 ||
		evidence.Continuation.Retained1M.LocalHNSWRecall != 0 || evidence.Continuation.Retained1M.LossOwner != "partition_local_hnsw" ||
		evidence.FinalLocalGate.SchemaVersion != 3 || evidence.FinalLocalGate.ResultKind != "m8_production_multi_variant_matrix_v3" ||
		evidence.FinalLocalGate.Status != "experimental_gate_failures" || evidence.FinalLocalGate.Disposition != "enablement_off_follow_up_required" ||
		evidence.FinalLocalGate.MeasuredCodeHead != "8ad06a6e95423c8992638965230862e1ce917d30" ||
		evidence.FinalLocalGate.ArtifactSHA256 != "d3347736200332cd2a81333a9053899f725eb66309a3b3ca3743376e60d030d2" ||
		evidence.FinalLocalGate.MeasuredBinarySHA256 != "86645edacdfeda86d00160d32de47a6bc44c712948757aae6d110dc5ead8d0d9" ||
		evidence.FinalLocalGate.MeasuredBinaryBuild != "go build -buildvcs=false ./cmd/treedb_vector_partition_bench" ||
		evidence.FinalLocalGate.VariantDescriptorSchemaVersion != 3 || evidence.FinalLocalGate.VariantDescriptorResultKind != "m3_persistent_variant_descriptor_v3" ||
		!evidence.FinalLocalGate.DirtyChildReportsRejected || !evidence.FinalLocalGate.AllChildReportsClean ||
		evidence.FinalLocalGate.Fixture.Vectors != 1_000_000 || evidence.FinalLocalGate.Fixture.Queries != 32 ||
		evidence.FinalLocalGate.Fixture.Checksum != "71239d1335ddd724835d415f57acae7f8bb36a6af52642d1e710392a883b2d6f" ||
		evidence.FinalLocalGate.GateLedger.RequiredVariants != "fail" || evidence.FinalLocalGate.GateLedger.Exhaustive != "fail" || evidence.FinalLocalGate.GateLedger.Recall != "fail" ||
		evidence.FinalLocalGate.GateLedger.OverlapStorage != "fail" || evidence.FinalLocalGate.GateLedger.CoupledGraph != "fail" || evidence.FinalLocalGate.GateLedger.Resources != "pass" ||
		len(evidence.FinalLocalGate.Variants) != 3 || evidence.FinalLocalGate.Variants[0].ID != "graph-disjoint-v1" ||
		evidence.FinalLocalGate.Variants[0].AssignmentBasis != "graph" || evidence.FinalLocalGate.Variants[0].Overlap != 0 ||
		evidence.FinalLocalGate.Variants[0].ArtifactSHA256 != "3c7a5665803b2f8f32f0187376b31faa74b7b712d8b7d94b28aea7114db6f556" ||
		evidence.FinalLocalGate.Variants[0].GraphSHA256 != "3c7a5665803b2f8f32f0187376b31faa74b7b712d8b7d94b28aea7114db6f556" ||
		evidence.FinalLocalGate.Variants[0].BuildIdentity != "5decc334106e0f08aa1382010b9fede73574be6584934d7e1ce9089b1dd85354" ||
		evidence.FinalLocalGate.Variants[0].ManifestIdentity != "d370a035ddb52823452728644020936b495641c79fcf261977e8dbe08c9ff4d6" ||
		evidence.FinalLocalGate.Variants[0].IndexDefinition != "c51c99cdf93b98f5e0d22f7a4464c14c3f51f2563e33d3bd300f3fccab5955fc" ||
		evidence.FinalLocalGate.Variants[0].RouterModel != "5c5492555c8ca7c5ff1b92e1bf07542130d12c5663ca6eb93ac6bb2b4b2074c4" || evidence.FinalLocalGate.Variants[0].PartitionHNSWM != 16 ||
		evidence.FinalLocalGate.Variants[0].PeakRSSBytes != 1_610_182_656 || evidence.FinalLocalGate.Variants[0].BalanceHardCap != 65_625 ||
		evidence.FinalLocalGate.Variants[0].AllPartitionHNSW != 0.7124999999999999 ||
		evidence.FinalLocalGate.Variants[1].ID != "graph-overlap-020-v1" || evidence.FinalLocalGate.Variants[1].AssignmentBasis != "graph" ||
		evidence.FinalLocalGate.Variants[1].Overlap != 0.2 || evidence.FinalLocalGate.Variants[1].GraphSHA256 != evidence.FinalLocalGate.Variants[0].GraphSHA256 ||
		evidence.FinalLocalGate.Variants[1].ArtifactSHA256 != evidence.FinalLocalGate.Variants[0].ArtifactSHA256 || evidence.FinalLocalGate.Variants[1].BuildIdentity != "9f7cf71e9756fadf339b9df346dc6e2949acd702737c64b20a160226dbcb12d7" ||
		evidence.FinalLocalGate.Variants[1].ManifestIdentity != "c39ddb9c5ab55d363ff968e45c60945cd0afcbea3550eb6b5c27e7a2ca5292db" ||
		evidence.FinalLocalGate.Variants[1].IndexDefinition != evidence.FinalLocalGate.Variants[0].IndexDefinition || evidence.FinalLocalGate.Variants[1].RouterModel != evidence.FinalLocalGate.Variants[0].RouterModel ||
		evidence.FinalLocalGate.Variants[1].PartitionHNSWM != 16 || evidence.FinalLocalGate.Variants[1].PeakRSSBytes != 1_759_191_040 || evidence.FinalLocalGate.Variants[1].MaxLoad != 63_918 || evidence.FinalLocalGate.Variants[1].BalanceHardCap != 65_625 ||
		evidence.FinalLocalGate.Variants[2].ID != "stable-id-hash-disjoint-v1" || evidence.FinalLocalGate.Variants[2].AssignmentBasis != "stable_id_hash" ||
		evidence.FinalLocalGate.Variants[2].Overlap != 0 || evidence.FinalLocalGate.Variants[2].GraphSHA256 != evidence.FinalLocalGate.Variants[0].GraphSHA256 ||
		evidence.FinalLocalGate.Variants[2].ArtifactSHA256 != "7a8ec9915de7acc6035024f3fc363c76678e8b27c529a2cba8a9861e764a49ad" || evidence.FinalLocalGate.Variants[2].BuildIdentity != "c5758e525e30bf5cef03a149fe28a9cf6e7206bd88580acde41ea74cc14aec99" ||
		evidence.FinalLocalGate.Variants[2].ManifestIdentity != "05be67319eba00d006af4f28bfcadc7b163d7d3015fd4bb1a42a88cf253322e7" ||
		evidence.FinalLocalGate.Variants[2].IndexDefinition != evidence.FinalLocalGate.Variants[0].IndexDefinition || evidence.FinalLocalGate.Variants[2].RouterModel != "e96bc5f62cefcd88a0e8674255c0a941bed4df9920fcb7b6e537249c67e802fa" || evidence.FinalLocalGate.Variants[2].PeakRSSBytes != 1_642_389_504 || evidence.FinalLocalGate.Variants[2].BalanceHardCap != 65_625 ||
		evidence.FinalLocalGate.Variants[2].PartitionHNSWM != 16 || evidence.FinalLocalGate.OverlapRequestedMemberships != 200_000 ||
		evidence.FinalLocalGate.OverlapRealizedMemberships != 8_096 || evidence.FinalLocalGate.OverlapMaterializationRatio != 0.008096 ||
		evidence.FinalLocalGate.OverlapBudgetUtilization != 0.04048 || evidence.FinalLocalGate.OverlapStorageRatio >= 1.35 ||
		evidence.FinalLocalGate.ResourceMeasurementBasis != "corpus_exclusive_fresh_process_per_variant_actual_per_request_and_per_shard_maxima_including_successful_preflight_warmup_and_stopped_group_fault_requests" ||
		evidence.FinalLocalGate.BalanceHardCapBasis != "manifest_integrity_covered_overlap_policy_capacity" ||
		evidence.FinalLocalGate.TopologySnapshotBoundary != "after_unavailable_group_request" ||
		evidence.FinalLocalGate.MatrixParentMaterializes == nil || *evidence.FinalLocalGate.MatrixParentMaterializes ||
		evidence.FinalLocalGate.RetryRedirectLimitBasis != "per_task_limit_times_max_observed_shard_request_fanout" ||
		evidence.FinalLocalGate.AggregateCandidateBytes.Configured != 134_217_728 || evidence.FinalLocalGate.AggregateCandidateBytes.MaxObserved != 4_207_808 || !evidence.FinalLocalGate.AggregateCandidateBytes.Passed ||
		evidence.FinalLocalGate.CoordinatorRouterCandidates.Configured != 1_000_000 || evidence.FinalLocalGate.CoordinatorRouterCandidates.Observed != 256 || !evidence.FinalLocalGate.CoordinatorRouterCandidates.Passed ||
		evidence.FinalLocalGate.MaximumRequestWallClockNanos != 21_325_331_988 ||
		evidence.FinalLocalGate.StoppedGroupFaultResourceBoundary.SelectedPartitions != 16 ||
		evidence.FinalLocalGate.StoppedGroupFaultResourceBoundary.EFSearch != 4096 ||
		evidence.FinalLocalGate.StoppedGroupFaultResourceBoundary.Requests != 4 ||
		evidence.FinalLocalGate.StoppedGroupFaultResourceBoundary.RPCs != 4 ||
		evidence.FinalLocalGate.StoppedGroupFaultResourceBoundary.RequestBytes != 2344 ||
		evidence.FinalLocalGate.StoppedGroupFaultResourceBoundary.MaxShardPartitions != 4 ||
		evidence.FinalLocalGate.StoppedGroupFaultResourceBoundary.MaxShardRequestBytes != 586 ||
		evidence.FinalLocalGate.StoppedGroupFaultResourceBoundary.CandidateBytes != 0 ||
		evidence.FinalLocalGate.StoppedGroupFaultResourceBoundary.ResponseBytes != 0 ||
		!slices.Equal(evidence.FinalLocalGate.StoppedGroupFaultResourceBoundary.WallClockByVariant, []uint64{15_970_857, 28_464_324, 15_091_465}) ||
		!slices.Equal(evidence.FinalLocalGate.FollowUps, []int{3998, 3999, 4001}) {
		t.Fatalf("M8 evidence boundary changed: %+v", evidence)
	}
	expectedUntimed := []struct {
		variantID      string
		wallClockNanos uint64
		candidateBytes uint64
	}{
		{"graph-disjoint-v1", 21_227_422_286, 4_205_184},
		{"graph-overlap-020-v1", 20_872_708_413, 4_200_832},
		{"stable-id-hash-disjoint-v1", 21_325_331_988, 4_205_376},
	}
	if len(evidence.FinalLocalGate.SuccessfulUntimedResourceBoundaries) != len(expectedUntimed) {
		t.Fatalf("M8 successful untimed resource boundary count changed: %+v", evidence.FinalLocalGate.SuccessfulUntimedResourceBoundaries)
	}
	for i, want := range expectedUntimed {
		got := evidence.FinalLocalGate.SuccessfulUntimedResourceBoundaries[i]
		if got.VariantID != want.variantID || got.SelectedPartitions != 16 || got.EFSearch != 4096 ||
			got.WallClockNanos != want.wallClockNanos || got.Requests != 4 || got.RPCs != 4 ||
			got.RequestBytes != 2352 || got.CandidateBytes != want.candidateBytes || got.ResponseBytes != 7232 ||
			got.MaxShardPartitions != 4 || got.MaxShardRequestBytes != 588 {
			t.Fatalf("M8 successful untimed resource boundary %d changed: %+v", i, got)
		}
	}
}

func TestVectorPartitionM1EvidenceSchema(t *testing.T) {
	root, _ := repoRoots(t)
	raw, err := os.ReadFile(filepath.Join(root, "docs", "performance", "vector-partition-m1-evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	type operation struct {
		NSOp             int64 `json:"ns_op"`
		BOp              int64 `json:"b_op"`
		AllocsOp         int64 `json:"allocs_op"`
		ProcessPeakRSSKB int64 `json:"process_peak_rss_kb"`
	}
	type codecMeasurement struct {
		Memberships          int    `json:"memberships"`
		DecodeValidateNSOp   int64  `json:"decode_validate_ns_op"`
		DecodeValidateBOp    int64  `json:"decode_validate_b_op"`
		DecodeValidateAllocs int64  `json:"decode_validate_allocs_op"`
		EncodeNSOp           int64  `json:"encode_ns_op"`
		EncodeBOp            int64  `json:"encode_b_op"`
		EncodeAllocs         int64  `json:"encode_allocs_op"`
		ProcessPeakRSSKB     int64  `json:"process_peak_rss_kb"`
		RSSContext           string `json:"rss_context"`
	}
	type snapshotMeasurement struct {
		Memberships      int    `json:"memberships"`
		ArchiveNSOp      int64  `json:"archive_ns_op"`
		ArchiveBytes     int64  `json:"archive_bytes"`
		ArchiveBOp       int64  `json:"archive_b_op"`
		ArchiveAllocsOp  int64  `json:"archive_allocs_op"`
		InstallNSOp      int64  `json:"install_ns_op"`
		InstallBOp       int64  `json:"install_b_op"`
		InstallAllocsOp  int64  `json:"install_allocs_op"`
		ProcessPeakRSSKB int64  `json:"process_peak_rss_kb"`
		RSSContext       string `json:"rss_context"`
	}
	var ledger struct {
		SchemaVersion      int    `json:"schema_version"`
		ResultKind         string `json:"result_kind"`
		MeasurementStatus  string `json:"measurement_status"`
		ProductionEvidence bool   `json:"production_evidence"`
		HeadSHA            string `json:"head_sha"`
		BaseSHA            string `json:"base_sha"`
		GoVersion          string `json:"go_version"`
		HardwareContext    string `json:"hardware_context"`
		HardwareDetails    struct {
			OS            string `json:"os"`
			CPU           string `json:"cpu"`
			PhysicalCores int    `json:"physical_cores"`
			LogicalCPUs   int    `json:"logical_cpus"`
		} `json:"hardware_details"`
		Dataset struct {
			Name                  string `json:"name"`
			Scales                []int  `json:"scales"`
			MembershipShape       string `json:"membership_shape"`
			AuthorityConstruction string `json:"authority_construction"`
		} `json:"dataset"`
		FixtureAttribution struct {
			CorrectnessPath string `json:"correctness_path"`
			PerformancePath string `json:"performance_path"`
			MeasuredStorage string `json:"measured_storage"`
			ExcludedClaims  string `json:"excluded_claims"`
		} `json:"fixture_attribution"`
		TimedBoundary string   `json:"timed_boundary"`
		Command       []string `json:"command"`
		RawArtifacts  []struct {
			Path string `json:"path"`
			Hash string `json:"sha256"`
		} `json:"raw_artifacts"`
		StorageMetrics struct {
			ManifestBytes       int64   `json:"manifest_bytes_at_1000000"`
			MetadataBytesPerVec float64 `json:"metadata_bytes_per_vector_at_1000000"`
			GateBytesPerVec     float64 `json:"gate_bytes_per_vector"`
			LargestArchiveBytes int64   `json:"largest_snapshot_archive_bytes"`
			Attribution         string  `json:"attribution"`
		} `json:"storage_metrics"`
		Stages []struct {
			Name              string `json:"name"`
			MeasurementStatus string `json:"measurement_status"`
		} `json:"stages"`
		Metrics struct {
			MeasurementStatus      string  `json:"measurement_status"`
			PeakProcessRSSBytes    int64   `json:"peak_process_rss_bytes"`
			FinalManifestBytes     int64   `json:"final_manifest_bytes"`
			MetadataBytesPerVector float64 `json:"metadata_bytes_per_vector"`
			MetricScope            string  `json:"metric_scope"`
		} `json:"metrics"`
		Codec []codecMeasurement `json:"codec"`
		Warm  struct {
			Open   operation `json:"open"`
			Status operation `json:"status"`
		} `json:"warm"`
		Snapshot []snapshotMeasurement `json:"snapshot"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&ledger); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		t.Fatalf("M1 evidence has trailing JSON: %v", err)
	}
	if ledger.SchemaVersion != 2 || ledger.ResultKind != "local_microbenchmark" || ledger.MeasurementStatus != "measured" || ledger.ProductionEvidence {
		t.Fatalf("invalid M1 evidence identity: schema=%d kind=%q status=%q production=%v", ledger.SchemaVersion, ledger.ResultKind, ledger.MeasurementStatus, ledger.ProductionEvidence)
	}
	for label, value := range map[string]string{"head_sha": ledger.HeadSHA, "base_sha": ledger.BaseSHA} {
		decoded, err := hex.DecodeString(value)
		if err != nil || len(decoded) != sha1.Size {
			t.Fatalf("%s is not an exact SHA-1: %q (%v)", label, value, err)
		}
	}
	if ledger.HeadSHA == ledger.BaseSHA || !strings.HasPrefix(ledger.GoVersion, "go1.") || ledger.HardwareContext == "" || ledger.HardwareDetails.OS == "" || ledger.HardwareDetails.CPU == "" || ledger.HardwareDetails.PhysicalCores <= 0 || ledger.HardwareDetails.LogicalCPUs < ledger.HardwareDetails.PhysicalCores {
		t.Fatalf("incomplete candidate/environment provenance: head=%q base=%q go=%q hardware=%q details=%+v", ledger.HeadSHA, ledger.BaseSHA, ledger.GoVersion, ledger.HardwareContext, ledger.HardwareDetails)
	}
	wantScales := []int{10000, 100000, 1000000}
	if ledger.Dataset.Name != "synthetic_checkpoint_ready_manifest_scale_v1" || fmt.Sprint(ledger.Dataset.Scales) != fmt.Sprint(wantScales) || ledger.Dataset.MembershipShape == "" || !strings.Contains(ledger.Dataset.AuthorityConstruction, "treedb_benchmark") || !strings.Contains(ledger.Dataset.AuthorityConstruction, "excluded") {
		t.Fatalf("invalid dataset attribution: %+v", ledger.Dataset)
	}
	if !strings.Contains(ledger.FixtureAttribution.CorrectnessPath, "genuine") || !strings.Contains(ledger.FixtureAttribution.PerformancePath, "treedb_benchmark-tagged synthetic") || !strings.Contains(ledger.FixtureAttribution.PerformancePath, "VCP1/VLC1") || ledger.FixtureAttribution.MeasuredStorage == "" || !strings.Contains(ledger.FixtureAttribution.ExcludedClaims, "not ANN") || !strings.Contains(ledger.TimedBoundary, "complete go test process") {
		t.Fatalf("incomplete fixture/timing boundary: fixture=%+v boundary=%q", ledger.FixtureAttribution, ledger.TimedBoundary)
	}
	if len(ledger.Command) != 8 {
		t.Fatalf("got %d benchmark commands, want 8", len(ledger.Command))
	}
	seenCommands := make(map[string]struct{}, len(ledger.Command))
	for _, command := range ledger.Command {
		if !strings.Contains(command, "GOWORK=off go test") || !strings.Contains(command, "-benchtime=1x") || !strings.Contains(command, "-benchmem") || !strings.Contains(command, "-count=1") {
			t.Fatalf("incomplete exact benchmark command: %q", command)
		}
		if _, exists := seenCommands[command]; exists {
			t.Fatalf("duplicate benchmark command: %q", command)
		}
		seenCommands[command] = struct{}{}
	}
	if ledger.StorageMetrics.ManifestBytes != 12000642 || ledger.StorageMetrics.MetadataBytesPerVec != 12.000642 || ledger.StorageMetrics.GateBytesPerVec != 64 || ledger.StorageMetrics.LargestArchiveBytes != 12575232 || ledger.StorageMetrics.MetadataBytesPerVec >= ledger.StorageMetrics.GateBytesPerVec || ledger.StorageMetrics.ManifestBytes == ledger.StorageMetrics.LargestArchiveBytes || !strings.Contains(ledger.StorageMetrics.Attribution, "not metadata bytes") {
		t.Fatalf("invalid or conflated storage metrics: %+v", ledger.StorageMetrics)
	}
	if len(ledger.Stages) != 3 {
		t.Fatalf("got %d M0-style evidence stages, want 3", len(ledger.Stages))
	}
	for _, stage := range ledger.Stages {
		if stage.Name == "" || stage.MeasurementStatus != "measured" {
			t.Fatalf("invalid evidence stage: %+v", stage)
		}
	}
	if ledger.Metrics.MeasurementStatus != "measured" || ledger.Metrics.PeakProcessRSSBytes != 632111104 || ledger.Metrics.FinalManifestBytes != ledger.StorageMetrics.ManifestBytes || ledger.Metrics.MetadataBytesPerVector != ledger.StorageMetrics.MetadataBytesPerVec || ledger.Metrics.MetricScope == "" {
		t.Fatalf("invalid M0-style summary metrics: %+v", ledger.Metrics)
	}
	if len(ledger.Codec) != len(wantScales) || len(ledger.Snapshot) != len(wantScales) {
		t.Fatalf("incomplete scale series: codec=%d snapshot=%d", len(ledger.Codec), len(ledger.Snapshot))
	}
	for i, scale := range wantScales {
		codec := ledger.Codec[i]
		if codec.Memberships != scale || codec.DecodeValidateNSOp <= 0 || codec.DecodeValidateBOp <= 0 || codec.DecodeValidateAllocs <= 0 || codec.EncodeNSOp <= 0 || codec.EncodeBOp <= 0 || codec.EncodeAllocs <= 0 || codec.ProcessPeakRSSKB <= 0 || codec.RSSContext == "" {
			t.Fatalf("invalid codec scale %d: %+v", scale, codec)
		}
		snapshot := ledger.Snapshot[i]
		if snapshot.Memberships != scale || snapshot.ArchiveNSOp <= 0 || snapshot.ArchiveBytes <= 0 || snapshot.ArchiveBOp <= 0 || snapshot.ArchiveAllocsOp <= 0 || snapshot.InstallNSOp <= 0 || snapshot.InstallBOp <= 0 || snapshot.InstallAllocsOp <= 0 || snapshot.ProcessPeakRSSKB <= 0 || snapshot.RSSContext == "" {
			t.Fatalf("invalid snapshot scale %d: %+v", scale, snapshot)
		}
	}
	for name, op := range map[string]operation{"open": ledger.Warm.Open, "status": ledger.Warm.Status} {
		if op.NSOp <= 0 || op.BOp <= 0 || op.AllocsOp <= 0 || op.ProcessPeakRSSKB <= 0 {
			t.Fatalf("invalid warm %s measurement: %+v", name, op)
		}
	}
	if len(ledger.RawArtifacts) != 1 || len(ledger.RawArtifacts[0].Hash) != sha256.Size*2 || filepath.IsAbs(ledger.RawArtifacts[0].Path) || strings.HasPrefix(filepath.Clean(ledger.RawArtifacts[0].Path), "..") {
		t.Fatalf("invalid raw artifact descriptor: %+v", ledger.RawArtifacts)
	}
	rawArtifact, err := os.ReadFile(filepath.Join(root, "docs", filepath.FromSlash(ledger.RawArtifacts[0].Path)))
	if err != nil {
		t.Fatal(err)
	}
	wantHash, err := hex.DecodeString(ledger.RawArtifacts[0].Hash)
	if err != nil {
		t.Fatal(err)
	}
	gotHash := sha256.Sum256(rawArtifact)
	if !strings.EqualFold(hex.EncodeToString(gotHash[:]), hex.EncodeToString(wantHash)) {
		t.Fatalf("raw artifact hash mismatch: got %x want %x", gotHash, wantHash)
	}
	rawText := string(rawArtifact)
	for _, needle := range []string{"# candidate: " + ledger.HeadSHA, ledger.BaseSHA, "BenchmarkVectorPartitionManifestV1Scale", "BenchmarkVectorPartitionStoreV1WarmOpen", "BenchmarkVectorPartitionStatusV1Warm", "BenchmarkRaftSnapshotV1VectorPartitionArchiveInstall"} {
		if !strings.Contains(rawText, needle) {
			t.Fatalf("raw artifact missing %q", needle)
		}
	}
	if got := strings.Count(rawText, "EXIT_CODE=0"); got != 9 {
		t.Fatalf("raw artifact has %d successful command sentinels, want 9", got)
	}
	for _, stale := range []string{"vector-partition-m1-02e511fcd.raw.txt", "vector-partition-m1-collections.txt", "vector-partition-m1-raft-snapshot.txt"} {
		if _, err := os.Stat(filepath.Join(root, "docs", "performance", stale)); !os.IsNotExist(err) {
			t.Fatalf("stale M1 evidence artifact %q still exists or stat failed: %v", stale, err)
		}
	}
}
