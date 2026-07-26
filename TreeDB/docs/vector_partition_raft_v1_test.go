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
	"strings"
	"testing"
)

func TestDocsVectorPartitionRaftM0Contract(t *testing.T) {
	root, _ := repoRoots(t)
	b, err := os.ReadFile(filepath.Join(root, "docs", "spec", "vector-partition-raft-v1.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"_id", "logical vector", "Raft replica", "overlap membership", "vector_index_id", "source_snapshot_generation", "partition_generation", "router_generation", "building", "ready", "cutover", "invalidated", "snapshot-bound", "fail closed", "simulation_only", "SearchRouteHNSWSearchPack=1", "exact_hnsw_search_pack_v1", "response-owned", "sorted partition-ID set", "TestTreeDBHNSWStageUsesExactSearchPackAndMatchesHighEFLocalTruth", "treedb_vector_partition_fixture_v2", "ieee754_binary64_explicit_fma_v1", "full fixture checksum", "canonical selected-stage set", "overrides both environment values", "exact_truth_queries", "ordered exact-truth", "gp-ann", "Parent invariant matrix", "TestTruthOracleTieOrderingAndAllPartitionParity", "M1 owns", "M8", "VPM1", "V1 API/schema contract, VPM1 wire version 3, and bounds", "Lifecycle, publication, and cleanup authority", "AcquireVectorPartitionReaderPinV1"} {
		if !strings.Contains(string(b), s) {
			t.Fatalf("contract missing %q", s)
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
				"MUST return no partial neighbors",
				"stable-ID/hash attribution are currently reported `unsupported`",
				"explicitly accepts the narrower result",
			},
		},
		{
			path: filepath.Join(root, "docs", "performance", "vector-partition-m8.md"),
			needles: []string{
				"7733485cc216dee4c0a1acc907cf71e609d11b7b",
				"experimental/off; north-star gates did not pass",
				"1,000,000 / 1 / 16",
				"52,734 MB",
				"e63bf9f8cb2f830504b97be3f5cc9c2ead7525ec9ecf61fd0979390fa9f6ece5",
				"unavailable endpoint returned error, zero neighbors, zero groups",
				"Enablement stays off",
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
			Vectors int `json:"vectors"`
			Groups  int `json:"raft_groups"`
		} `json:"retained_1m"`
	}
	if err := json.Unmarshal(raw, &evidence); err != nil {
		t.Fatal(err)
	}
	if evidence.Status != "experimental_off_gate_failures" || evidence.FeatureEnabled ||
		evidence.MeasuredCodeHead != "7733485cc216dee4c0a1acc907cf71e609d11b7b" ||
		evidence.CheckedIn10K.Vectors != 10_000 || evidence.CheckedIn10K.Queries != 128 ||
		evidence.CheckedIn10K.QuarterProbeRecall != 0.24140625 ||
		evidence.Retained1M.Vectors != 1_000_000 || evidence.Retained1M.Groups != 4 {
		t.Fatalf("M8 evidence boundary changed: %+v", evidence)
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
