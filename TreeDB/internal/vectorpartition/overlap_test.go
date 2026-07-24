package vectorpartition

import (
	"reflect"
	"testing"
)

func TestOverlapDeterministicBoundaryAndZeroEquivalent(t *testing.T) {
	a := Artifact{SchemaVersion: SchemaVersion, Backend: "test", BackendLicense: "test", Source: Source{SourceID: "test", Checksum: "6bb9e3f4d3f8baf2f6ec6b0e7b6f5c067495aee4a0bce17a656d9ba4fd4b1c5c", Vectors: 4, Dimensions: 1, Metric: "cosine"}, Config: Config{Metric: "cosine", Seed: 1, Repetitions: 1, Pivots: 2, MaxLeafBucket: 2, Degree: 3, Partitions: 2, Imbalance: 1, MaxVectors: 4, MaxEdges: 12}, IDs: []string{"a", "b", "c", "d"}, Graph: Graph{Neighbors: [][]int{{1, 2}, {0, 2}, {0, 1, 3}, {2}}}, Assignment: []int{0, 0, 1, 1}}
	// Use Build to establish a real source/checksum then retain this hand graph.
	a.Source.Checksum = "" // Build is not needed: construct valid identity from real vectors below.
	v := []Vector{{"a", []float64{1}}, {"b", []float64{.9}}, {"c", []float64{.8}}, {"d", []float64{.7}}}
	c := a.Config
	built, err := Build(v, c)
	if err != nil {
		t.Fatal(err)
	}
	a.Source = built.Source
	a.Metrics = metrics(a)
	if err := ValidateArtifact(a); err != nil {
		t.Fatal(err)
	}
	zero, err := BuildOverlap(a, OverlapConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if zero.Used != 0 || len(zero.Memberships) != len(a.IDs) {
		t.Fatalf("zero=%+v", zero)
	}
	one, err := BuildOverlap(a, OverlapConfig{Ratio: .5})
	if err != nil {
		t.Fatal(err)
	}
	two, err := BuildOverlap(a, OverlapConfig{Ratio: .5})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(one, two) {
		t.Fatalf("nondeterministic\n%+v\n%+v", one, two)
	}
	if one.Used != 2 {
		t.Fatalf("used=%d want 2", one.Used)
	}
}

func TestOverlapSaturatedRecordsUnspent(t *testing.T) {
	c := Config{Metric: "cosine", Seed: 1, Repetitions: 1, Pivots: 2, MaxLeafBucket: 2, Degree: 3, Partitions: 2, Imbalance: 0, MaxVectors: 4, MaxEdges: 12}
	v := []Vector{{"a", []float64{1}}, {"b", []float64{.9}}, {"c", []float64{.8}}, {"d", []float64{.7}}}
	built, err := Build(v, c)
	if err != nil {
		t.Fatal(err)
	}
	a := Artifact{SchemaVersion: SchemaVersion, Backend: "test", BackendLicense: "test", Source: built.Source, Config: c, IDs: []string{"a", "b", "c", "d"}, Graph: Graph{Neighbors: [][]int{{1, 2}, {0, 2}, {0, 1, 3}, {2}}}, Assignment: []int{0, 0, 1, 1}}
	a.Metrics = metrics(a)
	r, err := BuildOverlap(a, OverlapConfig{Ratio: .5})
	if err != nil {
		t.Fatal(err)
	}
	if r.Unspent == 0 {
		t.Fatalf("want unspent at saturated cap: %+v", r)
	}
}
