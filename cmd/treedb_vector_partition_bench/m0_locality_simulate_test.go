package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestM0EvidenceOutputPreservesExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "evidence.json")
	if err := os.WriteFile(path, []byte("retain"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeVectorPartitionSystemJSONExclusiveV1(path, struct{}{}); err == nil {
		t.Fatal("replaced existing evidence")
	}
	if raw, err := os.ReadFile(path); err != nil || string(raw) != "retain" {
		t.Fatalf("existing evidence=%q err=%v", raw, err)
	}
	if err := writeVectorPartitionSystemBytesExclusiveV1(path, []byte("replace")); err == nil {
		t.Fatal("replaced existing assignment artifact")
	}
	if raw, err := os.ReadFile(path); err != nil || string(raw) != "retain" {
		t.Fatalf("existing assignment=%q err=%v", raw, err)
	}
}

func TestM0CaptureSplitPairRejectsLeakage(t *testing.T) {
	var calibration, holdout m0LocalityCaptureV1
	calibration.Split, holdout.Split = "calibration", "holdout"
	calibration.BinarySHA256, holdout.BinarySHA256 = "binary", "binary"
	calibration.SourceRevision, holdout.SourceRevision = "revision", "revision"
	for ordinal := 0; ordinal < 32; ordinal++ {
		capture := &holdout
		if localHNSWCalibrationOrdinalV1(ordinal) {
			capture = &calibration
		}
		capture.Rows = append(capture.Rows, m0LocalityCaptureRowV1{Query: ordinal})
	}
	if len(calibration.Rows) == 0 || len(holdout.Rows) == 0 {
		t.Fatal("test split is degenerate")
	}
	if err := m0ValidateCaptureSplitPairV1(calibration, holdout, 32); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*m0LocalityCaptureV1, *m0LocalityCaptureV1){
		"same identity": func(c, h *m0LocalityCaptureV1) { h.Split = c.Split },
		"overlap":       func(c, h *m0LocalityCaptureV1) { h.Rows[0] = c.Rows[0] },
		"wrong split":   func(c, h *m0LocalityCaptureV1) { c.Rows[0], h.Rows[0] = h.Rows[0], c.Rows[0] },
		"wrong retained input": func(c, h *m0LocalityCaptureV1) {
			h.Descriptor = "different"
		},
		"different binary": func(c, h *m0LocalityCaptureV1) {
			h.BinarySHA256 = "different"
		},
	} {
		t.Run(name, func(t *testing.T) {
			c, h := calibration, holdout
			c.Rows, h.Rows = slices.Clone(c.Rows), slices.Clone(h.Rows)
			mutate(&c, &h)
			if err := m0ValidateCaptureSplitPairV1(c, h, 32); err == nil {
				t.Fatal("accepted contaminated capture split")
			}
		})
	}
	if err := m0ValidateCaptureSplitPairV1(calibration, holdout, 1000); err == nil {
		t.Fatal("accepted incomplete capture coverage")
	}
	calibration.Snapshots = map[uint32]collections.VectorPartitionPackLayoutSnapshotV1{0: {Rows: 1, RowOrdinals: []uint32{7}}}
	holdout.Snapshots = map[uint32]collections.VectorPartitionPackLayoutSnapshotV1{0: {Rows: 1, RowOrdinals: []uint32{7}}}
	if err := m0ValidateCaptureSplitPairV1(calibration, holdout, 32); err != nil {
		t.Fatal(err)
	}
	holdout.Snapshots[0] = collections.VectorPartitionPackLayoutSnapshotV1{Rows: 1, RowOrdinals: []uint32{8}}
	if err := m0ValidateCaptureSplitPairV1(calibration, holdout, 32); err == nil {
		t.Fatal("accepted incompatible capture snapshots")
	}
}

func TestM0ReadCaptureRequiresCleanBuildIdentity(t *testing.T) {
	sha := strings.Repeat("a", 64)
	capture := m0LocalityCaptureV1{
		Schema:         "treedb_vector_partition_m0_exact_pack_trace_v3",
		Artifact:       sha,
		Descriptor:     sha,
		Source:         vectorpartition.Source{SourceID: "fixture", Checksum: sha, Vectors: 1, Dimensions: 1, Metric: "cosine"},
		Manifest:       sha,
		ReadySet:       sha,
		RouterModel:    sha,
		BinarySHA256:   sha,
		SourceRevision: strings.Repeat("b", 40),
		Rows:           []m0LocalityCaptureRowV1{{}},
		Traces:         []m0LocalityTraceRowV1{{}},
		Snapshots:      map[uint32]collections.VectorPartitionPackLayoutSnapshotV1{0: {}},
	}
	write := func(name string, value m0LocalityCaptureV1) string {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}
	if _, _, err := m0ReadCaptureV1(write("clean.json", capture)); err != nil {
		t.Fatal(err)
	}
	capture.VCSModified = true
	if _, _, err := m0ReadCaptureV1(write("modified.json", capture)); err == nil {
		t.Fatal("accepted modified capture binary")
	}
}

func TestM0ObjectiveOrdersAreDeterministicAndDistinct(t *testing.T) {
	snapshot := collections.VectorPartitionPackLayoutSnapshotV1{
		Rows: 5, EntryOrdinal: 0, RowOrdinals: []uint32{0, 1, 2, 3, 4}, VectorStride: 1, VectorOffset: 1,
		LayerOffsets: [][]uint64{{0, 2, 3, 4, 5, 6}}, LayerNeighbors: [][]uint32{{1, 2, 0, 0, 4, 3}},
		LayerOffsetsSectionOffsets: []uint64{2}, LayerNeighborOffsets: []uint64{3},
	}
	pairs := map[[2]int]uint32{{1, 3}: 9, {2, 4}: 7}
	want := map[string][]int{"source": {0, 1, 2, 3, 4}, "bfs": {0, 1, 2, 3, 4}, "edge_window": {0, 1, 2, 3, 4}, "gorder_like": {0, 1, 2, 3, 4}, "co_visitation": {0, 1, 3, 2, 4}, "hybrid": {0, 1, 3, 2, 4}}
	seen := map[string][]int{}
	for _, objective := range []string{"source", "bfs", "edge_window", "gorder_like", "co_visitation", "hybrid"} {
		first, err := m0ObjectiveOrderV1(snapshot, pairs, objective)
		if err != nil {
			t.Fatalf("%s: %v", objective, err)
		}
		second, err := m0ObjectiveOrderV1(snapshot, pairs, objective)
		if err != nil || !reflect.DeepEqual(first, second) {
			t.Fatalf("%s nondeterministic: %v %v", objective, first, second)
		}
		seen[objective] = first
		if !reflect.DeepEqual(first, want[objective]) {
			t.Fatalf("%s got %v want %v", objective, first, want[objective])
		}
	}
	if _, err := m0ObjectiveOrderV1(snapshot, pairs, "unknown"); err == nil {
		t.Fatal("accepted unknown objective")
	}
	if reflect.DeepEqual(seen["edge_window"], seen["co_visitation"]) || reflect.DeepEqual(seen["gorder_like"], seen["hybrid"]) {
		t.Fatalf("objectives collapsed: %+v", seen)
	}
}

func TestM0LayoutPlanBindsCaptureArtifactAndCanonicalOrder(t *testing.T) {
	input := []vectorpartition.Vector{{ID: "doc-0", Values: []float64{1, 0}}, {ID: "doc-1", Values: []float64{0, 1}}, {ID: "doc-2", Values: []float64{1, 1}}, {ID: "doc-3", Values: []float64{-1, 0}}, {ID: "doc-4", Values: []float64{0, -1}}}
	config := vectorpartition.DefaultConfig()
	config.Partitions = 1
	artifact, err := vectorpartition.BuildWithPartitioner(input, config, vectorpartition.Source{SourceID: "m0-plan-test"}, vectorpartition.ReferencePartitioner{})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := vectorpartition.CanonicalJSON(artifact)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := collections.VectorPartitionPackLayoutSnapshotV1{Rows: 5, EntryOrdinal: 0, RowOrdinals: []uint32{0, 1, 2, 3, 4}, VectorStride: 1, VectorOffset: 1, LevelsOffset: 2, LayerOffsets: [][]uint64{{0, 2, 3, 4, 5, 6}}, LayerNeighbors: [][]uint32{{1, 2, 0, 0, 4, 3}}, LayerOffsetsSectionOffsets: []uint64{3}, LayerNeighborOffsets: []uint64{4}}
	capture := m0LocalityCaptureV1{Schema: "treedb_vector_partition_m0_exact_pack_trace_v3", Artifact: m0SHA256V1(raw), Source: artifact.Source, Manifest: strings.Repeat("a", 64), Snapshots: map[uint32]collections.VectorPartitionPackLayoutSnapshotV1{0: snapshot}, Traces: []m0LocalityTraceRowV1{{Partitions: []m0LocalityTracePartitionV1{{Partition: 0, ScoreOrdinals: []uint32{1, 3}}}}}}
	first, err := m0BuildLayoutPlanV1(capture, strings.Repeat("b", 64), raw)
	if err != nil {
		t.Fatal(err)
	}
	second, err := m0BuildLayoutPlanV1(capture, strings.Repeat("b", 64), raw)
	if err != nil || !reflect.DeepEqual(first, second) || first.ArtifactSHA256 == "" || len(first.Partitions) != 1 || len(first.Partitions[0].Order) != 5 {
		t.Fatalf("plan=%+v second=%+v err=%v", first, second, err)
	}
	if first.Partitions[0].Order[2].DocumentID != "doc-3" || first.Partitions[0].Order[2].SourceOrdinal != 3 {
		t.Fatalf("co-visitation order=%+v", first.Partitions[0].Order)
	}
	root := t.TempDir()
	path := filepath.Join(root, "plan.json")
	if err := writeVectorPartitionSystemJSONExclusiveV1(path, first); err != nil {
		t.Fatal(err)
	}
	if _, err := m0ReadLayoutPlanV1(path); err != nil {
		t.Fatal(err)
	}
	first.ArtifactSHA256 = strings.Repeat("c", 64)
	badPath := filepath.Join(root, "bad-plan.json")
	if err := writeVectorPartitionSystemJSONExclusiveV1(badPath, first); err != nil {
		t.Fatal(err)
	}
	if _, err := m0ReadLayoutPlanV1(badPath); err == nil {
		t.Fatal("accepted changed plan digest")
	}
	capture.Artifact = strings.Repeat("d", 64)
	if _, err := m0BuildLayoutPlanV1(capture, strings.Repeat("b", 64), raw); err == nil {
		t.Fatal("accepted mismatched graph artifact")
	}
}

func TestM0SnapshotRejectsOverflowingVectorStride(t *testing.T) {
	snapshot := collections.VectorPartitionPackLayoutSnapshotV1{
		Rows: 1, RowOrdinals: []uint32{0}, VectorStride: int(^uint(0)>>1)/4 + 1, VectorOffset: 1, LevelsOffset: 2,
		LayerOffsets: [][]uint64{{0, 0}}, LayerNeighbors: [][]uint32{{}},
		LayerOffsetsSectionOffsets: []uint64{3}, LayerNeighborOffsets: []uint64{4},
	}
	if err := m0ValidateSnapshotV1(snapshot); err == nil {
		t.Fatal("accepted overflowing vector stride")
	}
}

func TestM0EdgeWindowExpiresOldPlacement(t *testing.T) {
	snapshot := collections.VectorPartitionPackLayoutSnapshotV1{
		Rows: 5, EntryOrdinal: 0, RowOrdinals: []uint32{0, 1, 2, 3, 4}, VectorStride: 512, VectorOffset: 1,
		LayerOffsets: [][]uint64{{0, 2, 5, 8, 10, 12}}, LayerNeighbors: [][]uint32{{1, 3, 0, 2, 4, 1, 3, 4, 0, 2, 1, 2}},
		LayerOffsetsSectionOffsets: []uint64{2}, LayerNeighborOffsets: []uint64{3},
	}
	got, err := m0ObjectiveOrderV1(snapshot, nil, "edge_window")
	if err != nil || !reflect.DeepEqual(got, []int{0, 1, 2, 4, 3}) {
		t.Fatalf("got %v err=%v", got, err)
	}
}

func TestM0GorderAdmitsTwoHopFrontier(t *testing.T) {
	snapshot := collections.VectorPartitionPackLayoutSnapshotV1{
		Rows: 3, EntryOrdinal: 0, RowOrdinals: []uint32{0, 2, 1}, VectorStride: 512, VectorOffset: 1,
		LayerOffsets: [][]uint64{{0, 1, 3, 4}}, LayerNeighbors: [][]uint32{{1, 0, 2, 1}},
		LayerOffsetsSectionOffsets: []uint64{2}, LayerNeighborOffsets: []uint64{3},
	}
	got, err := m0ObjectiveOrderV1(snapshot, nil, "gorder_like")
	if err != nil || !reflect.DeepEqual(got, []int{0, 2, 1}) {
		t.Fatalf("got %v err=%v", got, err)
	}
}
