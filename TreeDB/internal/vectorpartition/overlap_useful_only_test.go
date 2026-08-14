package vectorpartition

import (
	"reflect"
	"strings"
	"testing"
)

func usefulOnlyArtifact(t *testing.T, neighbors [][]int, assignment []int) Artifact {
	t.Helper()
	ids := []string{"a", "b", "c", "d"}
	c := Config{Metric: "cosine", Seed: 1, Repetitions: 1, Pivots: 2, MaxLeafBucket: 2, Degree: 3, Partitions: 2, Imbalance: 0, MaxVectors: 4, MaxEdges: 12}
	v := []Vector{{"a", []float64{1}}, {"b", []float64{.9}}, {"c", []float64{.8}}, {"d", []float64{.7}}}
	built, err := Build(v, c)
	if err != nil {
		t.Fatal(err)
	}
	a := Artifact{SchemaVersion: SchemaVersion, Backend: "test", BackendLicense: "test", Source: built.Source, Config: c, IDs: ids, Graph: Graph{Neighbors: neighbors}, Assignment: assignment}
	a.Metrics = metrics(a)
	if err := ValidateArtifact(a); err != nil {
		t.Fatal(err)
	}
	return a
}

func TestOverlapUsefulOnlyZeroProposalsHaveZeroFiller(t *testing.T) {
	a := usefulOnlyArtifact(t, [][]int{{}, {}, {}, {}}, []int{0, 0, 1, 1})
	exact, err := BuildOverlap(a, OverlapConfig{Ratio: .5, Capacity: 3, RequireExact: true})
	if err != nil {
		t.Fatal(err)
	}
	if exact.Filler == 0 || exact.Used != exact.Filler {
		t.Fatalf("exact fixture no longer produces filler: %+v", exact)
	}
	cfg := SelectedOverlapConfigV1(3)
	cfg.Ratio = .5
	got, err := BuildOverlap(a, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.Used != 0 || got.Useful != 0 || got.Filler != 0 || got.Unspent != got.Budget || got.Budget == 0 {
		t.Fatalf("useful-only zero proposals=%+v", got)
	}
	if len(got.Replicas) != 0 || len(got.Memberships) != len(a.IDs) {
		t.Fatalf("useful-only zero memberships=%+v", got)
	}
}

func TestOverlapUsefulOnlyRespectsCapsAndTieOrder(t *testing.T) {
	a := usefulOnlyArtifact(t, [][]int{{1, 2}, {0, 2}, {0, 1, 3}, {2}}, []int{0, 0, 1, 1})
	got, err := BuildOverlap(a, OverlapConfig{Ratio: .5, Capacity: 3, UsefulOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	want := []Membership{
		{VectorOrdinal: 0, Partition: 0, Home: true},
		{VectorOrdinal: 1, Partition: 0, Home: true},
		{VectorOrdinal: 2, Partition: 0},
		{VectorOrdinal: 2, Partition: 1, Home: true},
		{VectorOrdinal: 3, Partition: 1, Home: true},
	}
	if got.Used != 1 || got.Useful != 1 || got.Filler != 0 || got.Unspent != 1 || !reflect.DeepEqual(got.Memberships, want) {
		t.Fatalf("useful-only bounded=%+v", got)
	}
	if got.Replicas[0].Class != ReplicaUtilityPositiveGainV1 || got.Replicas[0].Gain <= 0 {
		t.Fatalf("replica=%+v", got.Replicas)
	}
	capped, err := BuildOverlap(a, OverlapConfig{Ratio: .5, Capacity: 2, UsefulOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if capped.Used != 0 || capped.Filler != 0 || !reflect.DeepEqual(capped.Loads, []int{2, 2}) {
		t.Fatalf("capacity-capped useful-only=%+v", capped)
	}
}

func TestOverlapUsefulOnlyRejectsExactFillConflict(t *testing.T) {
	a := usefulOnlyArtifact(t, [][]int{{}, {}, {}, {}}, []int{0, 0, 1, 1})
	got, err := BuildOverlap(a, OverlapConfig{Ratio: .5, Capacity: 3, RequireExact: true, UsefulOnly: true})
	if err == nil || got.Used != 0 || !strings.Contains(err.Error(), "useful-only") {
		t.Fatalf("conflict got=%+v err=%v", got, err)
	}
}
