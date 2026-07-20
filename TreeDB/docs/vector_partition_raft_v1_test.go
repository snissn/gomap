package docs_test

import (
	"encoding/json"
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
	for _, s := range []string{"_id", "logical vector", "Raft replica", "overlap membership", "vector_index_id", "source_snapshot_generation", "partition_generation", "router_generation", "building", "ready", "cutover", "invalidated", "snapshot-bound", "fail closed", "simulation_only", "SearchRouteHNSWSearchPack=1", "exact_hnsw_search_pack_v1", "response-owned", "sorted partition-ID set", "TestTreeDBHNSWStageUsesExactSearchPackAndMatchesHighEFLocalTruth", "treedb_vector_partition_fixture_v2", "ieee754_binary64_explicit_fma_v1", "full fixture checksum", "canonical selected-stage set", "overrides both environment values", "exact_truth_queries", "ordered exact-truth", "gp-ann", "Parent invariant matrix", "TestTruthOracleTieOrderingAndAllPartitionParity", "M1 owns", "M8", "VPM1", "V1 format and bounds", "Lifecycle, publication, and cleanup authority", "AcquireVectorPartitionReaderPinV1"} {
		if !strings.Contains(string(b), s) {
			t.Fatalf("contract missing %q", s)
		}
	}
}

func TestVectorPartitionM1EvidenceSchema(t *testing.T) {
	root, _ := repoRoots(t)
	raw, err := os.ReadFile(filepath.Join(root, "docs", "performance", "vector-partition-m1-evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	var ledger struct {
		SchemaVersion      int    `json:"schema_version"`
		ResultKind         string `json:"result_kind"`
		ProductionEvidence bool   `json:"production_evidence"`
		Candidate          string `json:"candidate_head_sha"`
		RawArtifacts       []struct {
			Hash string `json:"sha256"`
		} `json:"raw_artifacts"`
		Codec []struct {
			Metadata float64 `json:"metadata_bytes_per_vector"`
		} `json:"codec"`
	}
	if err := json.Unmarshal(raw, &ledger); err != nil {
		t.Fatal(err)
	}
	if ledger.SchemaVersion != 1 || ledger.ResultKind != "local_microbenchmark" || ledger.ProductionEvidence || len(ledger.Candidate) != 40 || len(ledger.RawArtifacts) != 1 || len(ledger.RawArtifacts[0].Hash) != 64 || len(ledger.Codec) != 3 || ledger.Codec[2].Metadata != 12.000642 {
		t.Fatalf("invalid M1 evidence ledger: %+v", ledger)
	}
}
