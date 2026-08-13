package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestM0MembershipModesZeroCutElidesUsefulAndRejectsExactFiller(t *testing.T) {
	vectors := make([]vectorpartition.Vector, 10)
	for i := range vectors {
		vectors[i] = vectorpartition.Vector{ID: fmt.Sprintf("v%02d", i), Values: []float64{1, 0}}
		if i >= 5 {
			vectors[i].Values = []float64{0, 1}
		}
	}
	config := vectorpartition.DefaultConfig()
	config.Partitions = 2
	config.Degree = 1
	artifact, err := vectorpartition.BuildWithPartitioner(vectors, config, vectorpartition.Source{SourceID: "m0-zero-cut"}, m0StaticPartitionerV1{})
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Metrics.EdgeCut != 0 {
		t.Fatalf("edge cut = %d, want 0", artifact.Metrics.EdgeCut)
	}
	capacity, err := m3OverlapCapacityV1(artifact, .2)
	if err != nil {
		t.Fatal(err)
	}
	zero, err := vectorpartition.BuildOverlap(artifact, vectorpartition.OverlapConfig{})
	if err != nil {
		t.Fatal(err)
	}
	useful, err := vectorpartition.BuildOverlap(artifact, vectorpartition.OverlapConfig{Ratio: .2, Capacity: capacity})
	if err != nil {
		t.Fatal(err)
	}
	exact, err := vectorpartition.BuildOverlap(artifact, vectorpartition.OverlapConfig{Ratio: .2, Capacity: capacity, RequireExact: true})
	if err != nil {
		t.Fatal(err)
	}
	modes, err := m0MembershipModesV1(artifact, zero, useful, exact)
	if err != nil {
		t.Fatal(err)
	}
	if got := modes[1]; got.Used != 0 || got.Useful != 0 || got.Filler != 0 || got.EquivalentTo != "zero" || got.Materialize {
		t.Fatalf("useful-only zero-cut mode = %+v, want zero equivalent", got)
	}
	if got := modes[2]; got.Rejected != "exact-20 contains filler" || got.Materialize || got.Used != 2 || got.Filler != 2 {
		t.Fatalf("exact zero-cut mode = %+v, want filler rejection", got)
	}
	raw, err := json.Marshal(modes[1])
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"used":0`, `"useful":0`, `"filler":0`, `"materialize":false`} {
		if !strings.Contains(string(raw), field) {
			t.Fatalf("marshaled zero mode %s lacks %s", raw, field)
		}
	}
}

func TestM0AssignmentArtifactRequiresPinnedKaHIPIdentity(t *testing.T) {
	config := vectorpartition.DefaultConfig()
	config.Partitions = 2
	config.Degree = 1
	graph, err := vectorpartition.BuildWithPartitioner([]vectorpartition.Vector{{ID: "a", Values: []float64{1}}, {ID: "b", Values: []float64{-1}}}, config, vectorpartition.Source{SourceID: "m0-kahip"}, m0StaticPartitionerV1{})
	if err != nil {
		t.Fatal(err)
	}
	request := graph
	candidate := request
	candidate.Backend = fmt.Sprintf("kahip_python_3.25_eco_symmetrized_v1_seed_%d", request.Config.Seed)
	candidate.BackendLicense = m0KaHIPBackendLicenseV1
	if err := m0ValidateAssignmentArtifactV1(graph, request, candidate); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*vectorpartition.Artifact){
		"backend": func(a *vectorpartition.Artifact) { a.Backend = "forged" },
		"license": func(a *vectorpartition.Artifact) { a.BackendLicense = "forged" },
	} {
		t.Run(name, func(t *testing.T) {
			forged := candidate
			mutate(&forged)
			if err := m0ValidateAssignmentArtifactV1(graph, request, forged); err == nil {
				t.Fatal("accepted forged KaHIP identity")
			}
		})
	}
}

type m0StaticPartitionerV1 struct{}

func (m0StaticPartitionerV1) Name() string    { return "m0_static_test" }
func (m0StaticPartitionerV1) License() string { return "test" }
func (m0StaticPartitionerV1) Partition(graph vectorpartition.Graph, partitions, _ int) ([]int, error) {
	if partitions != 2 {
		return nil, fmt.Errorf("partitions = %d, want 2", partitions)
	}
	assignment := make([]int, len(graph.Neighbors))
	for i := range assignment {
		if i >= len(assignment)/2 {
			assignment[i] = 1
		}
	}
	return assignment, nil
}
