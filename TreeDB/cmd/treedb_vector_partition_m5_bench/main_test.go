package main

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func TestDeterministicQueryMatchesFixtureV2Contract(t *testing.T) {
	fixture := fixtureManifest{
		Generator:  "treedb_vector_partition_fixture_v2",
		Arithmetic: "ieee754_binary64_explicit_fma_v1",
		Vectors:    m5RequiredVectors,
		Queries:    1,
		Dimensions: 16,
		Metric:     "cosine",
		Seed:       1,
	}
	query, err := deterministicQuery(fixture, 0)
	if err != nil {
		t.Fatalf("deterministicQuery: %v", err)
	}
	if len(query) != fixture.Dimensions || query[0] <= 0 || query[1] != 0 || query[2] != 0 || query[3] != 0 {
		t.Fatalf("query prefix=%v", query[:4])
	}
	var norm float64
	for _, value := range query {
		norm += float64(value) * float64(value)
	}
	if math.Abs(norm-1) > 1e-6 {
		t.Fatalf("query norm squared=%g want 1", norm)
	}
	fixture.Generator = "treedb_vector_partition_fixture_v1"
	if _, err := deterministicQuery(fixture, 0); err == nil {
		t.Fatal("legacy fixture generator unexpectedly accepted")
	}
}

func TestValidateM3InputRequiresExactAcceptanceShape(t *testing.T) {
	report := m3Report{
		SchemaVersion: 3,
		ResultKind:    "m3_native_partition_hnsw_evidence",
		Dataset: fixtureManifest{
			Generator:  "treedb_vector_partition_fixture_v2",
			Arithmetic: "ieee754_binary64_explicit_fma_v1",
			Vectors:    m5RequiredVectors,
			Queries:    1,
			Dimensions: 16,
			Metric:     "cosine",
		},
		Partitions: 16,
		Rows: []m3Row{{
			PartitionHNSWM:   4,
			ManifestDigest:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			SourceGeneration: 1,
			SourceChecksum:   2,
			SourceSchemaHash: 3,
			SourceRows:       m5RequiredVectors,
			SearchRoute:      m5RequiredRoute,
			PersistentDBDir:  "/tmp/example",
		}},
	}
	if _, err := validateM3Input(report, "", 15); err != nil {
		t.Fatalf("validateM3Input: %v", err)
	}
	report.Dataset.Vectors--
	if _, err := validateM3Input(report, "", 15); err == nil {
		t.Fatal("non-million corpus unexpectedly accepted")
	}
}

func TestPercentileNearestRank(t *testing.T) {
	samples := []uint64{1, 2, 3, 4, 5}
	if got := percentile(samples, 0.50); got != 3 {
		t.Fatalf("p50=%d want 3", got)
	}
	if got := percentile(samples, 0.95); got != 5 {
		t.Fatalf("p95=%d want 5", got)
	}
}

func TestEvaluateGateUsesOnlyNonReadNonSearchServiceOverhead(t *testing.T) {
	stages := stageSamples{
		readIndexApply: []uint64{100, 100},
		search:         []uint64{1_000, 1_000},
		total:          []uint64{1_190, 1_210},
	}
	gate := evaluateGate(stages, distribution{MeanNanos: 1_000})
	if gate.WarmMeanServiceOverheadNanos != 100 || gate.ObservedRatio != 0.10 || gate.Status != "PASS" || !gate.MeasurementValid {
		t.Fatalf("gate=%+v", gate)
	}
	stages.total[1] = 1_211
	if got := evaluateGate(stages, distribution{MeanNanos: 1_000}); got.Status != "FAIL" {
		t.Fatalf("gate=%+v want FAIL", got)
	}
}

func TestEvaluateGateWritesSerializableFailureForMissingMeasurements(t *testing.T) {
	tests := []struct {
		name     string
		stages   stageSamples
		baseline distribution
	}{
		{name: "no warm samples", baseline: distribution{MeanNanos: 1}},
		{
			name: "non-positive baseline",
			stages: stageSamples{
				readIndexApply: []uint64{0},
				search:         []uint64{0},
				total:          []uint64{1},
			},
		},
		{name: "incomplete warm stages", stages: stageSamples{total: []uint64{1}}, baseline: distribution{MeanNanos: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gate := evaluateGate(tt.stages, tt.baseline)
			if gate.Status != "FAIL" || gate.MeasurementValid || gate.FailureReason == "" {
				t.Fatalf("gate=%+v want invalid FAIL with a reason", gate)
			}
			if _, err := json.Marshal(gate); err != nil {
				t.Fatalf("marshal gate: %v", err)
			}
		})
	}
}

func TestLocalRaftClusterCommitsAndCoordinatesProductionReadIndex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cluster, err := openLocalRaftCluster(ctx, "benchmark-group-000000")
	if err != nil {
		t.Fatalf("openLocalRaftCluster: %v", err)
	}
	defer cluster.Close()
	commit, err := cluster.commitProofCommand(ctx)
	if err != nil {
		t.Fatalf("commitProofCommand: %v", err)
	}
	if !commit.Evidence.ProvesProductionConsensus() {
		t.Fatalf("commit evidence=%+v", commit.Evidence)
	}
	proof, progress, err := cluster.readCoordinator().CoordinateRoutedReadIndex(ctx, raftcluster.ReadIndexBarrier{
		NodeID:  cluster.leader,
		GroupID: cluster.groupID,
	})
	if err != nil {
		t.Fatalf("CoordinateRoutedReadIndex: %v", err)
	}
	if proof.EvidenceKind != raftcluster.ReadIndexEvidenceProduction || !proof.HasQuorum ||
		proof.Index < commit.Entry.Index || progress.Index < proof.Index {
		t.Fatalf("proof=%+v progress=%+v commit=%+v", proof, progress, commit.Entry)
	}
}
