package vectorpartition

import (
	"errors"
	"reflect"
	"strings"
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
	if one.Used == 0 || one.Used > 2 {
		t.Fatalf("used=%d want bounded positive reduction", one.Used)
	}
	want := OverlapResult{
		Memberships: []Membership{
			{VectorOrdinal: 0, Partition: 0, Home: true},
			{VectorOrdinal: 1, Partition: 0, Home: true},
			{VectorOrdinal: 2, Partition: 0},
			{VectorOrdinal: 2, Partition: 1, Home: true},
			{VectorOrdinal: 3, Partition: 1, Home: true},
		},
		Budget:        2,
		Used:          1,
		Unspent:       1,
		Capacity:      4,
		Loads:         []int{3, 2},
		EdgeCutBefore: 4,
		EdgeCutAfter:  0,
	}
	if !reflect.DeepEqual(one, want) {
		t.Fatalf("boundary overlap=%+v want %+v", one, want)
	}
}

func TestOverlapExactCapacityTreatmentV1(t *testing.T) {
	c := Config{Metric: "cosine", Seed: 1, Repetitions: 1, Pivots: 2, MaxLeafBucket: 2, Degree: 3, Partitions: 2, Imbalance: 0, MaxVectors: 4, MaxEdges: 12}
	v := []Vector{{"a", []float64{1}}, {"b", []float64{.9}}, {"c", []float64{.8}}, {"d", []float64{.7}}}
	built, err := Build(v, c)
	if err != nil {
		t.Fatal(err)
	}
	a := Artifact{SchemaVersion: SchemaVersion, Backend: "test", BackendLicense: "test", Source: built.Source, Config: c, IDs: []string{"a", "b", "c", "d"}, Graph: Graph{Neighbors: [][]int{{1, 2}, {0, 2}, {0, 1, 3}, {2}}}, Assignment: []int{0, 0, 1, 1}}
	a.Metrics = metrics(a)
	short, err := BuildOverlap(a, OverlapConfig{Ratio: .5, Capacity: 2, RequireExact: true})
	if err == nil || short.Budget != 0 {
		t.Fatalf("short=%+v err=%v", short, err)
	}
	var shortfall *OverlapShortfallError
	if !errors.As(err, &shortfall) || shortfall.Requested != 2 || shortfall.Realized != 0 || shortfall.Rejected != 2 || shortfall.Capacity != 2 {
		t.Fatalf("shortfall=%+v err=%v", shortfall, err)
	}
	got, err := BuildOverlap(a, OverlapConfig{Ratio: .25, Capacity: 3, RequireExact: true})
	if err != nil {
		t.Fatal(err)
	}
	if got.Budget != 1 || got.Used != 1 || got.Unspent != 0 || got.Capacity != 3 {
		t.Fatalf("exact=%+v", got)
	}
}

func TestOverlapExactTreatmentFillsLegalNonAffinitySlotsV1(t *testing.T) {
	c := Config{Metric: "cosine", Seed: 1, Repetitions: 1, Pivots: 2, MaxLeafBucket: 2, Degree: 3, Partitions: 2, Imbalance: 0, MaxVectors: 4, MaxEdges: 12}
	v := []Vector{{"a", []float64{1}}, {"b", []float64{.9}}, {"c", []float64{.8}}, {"d", []float64{.7}}}
	built, err := Build(v, c)
	if err != nil {
		t.Fatal(err)
	}
	a := Artifact{SchemaVersion: SchemaVersion, Backend: "test", BackendLicense: "test", Source: built.Source, Config: c, IDs: []string{"a", "b", "c", "d"}, Graph: Graph{Neighbors: [][]int{{}, {}, {}, {}}}, Assignment: []int{0, 0, 1, 1}}
	a.Metrics = metrics(a)
	got, err := BuildOverlap(a, OverlapConfig{Ratio: .5, Capacity: 3, RequireExact: true})
	if err != nil {
		t.Fatal(err)
	}
	if got.Budget != 2 || got.Used != 2 || got.Unspent != 0 || got.EdgeCutBefore != 0 || got.EdgeCutAfter != 0 {
		t.Fatalf("exact fallback=%+v", got)
	}
}

func TestOverlapExactTreatmentFillsUnevenHomeDeficitsDeterministicallyV1(t *testing.T) {
	c := Config{Metric: "cosine", Seed: 1, Repetitions: 1, Pivots: 2, MaxLeafBucket: 2, Degree: 3, Partitions: 3, Imbalance: 1, MaxVectors: 5, MaxEdges: 15}
	v := []Vector{{"a", []float64{1}}, {"b", []float64{.9}}, {"c", []float64{.8}}, {"d", []float64{.7}}, {"e", []float64{.6}}}
	built, err := Build(v, c)
	if err != nil {
		t.Fatal(err)
	}
	a := Artifact{SchemaVersion: SchemaVersion, Backend: "test", BackendLicense: "test", Source: built.Source, Config: c, IDs: []string{"a", "b", "c", "d", "e"}, Graph: Graph{Neighbors: [][]int{{}, {}, {}, {}, {}}}, Assignment: []int{0, 0, 0, 1, 2}}
	a.Metrics = metrics(a)
	first, err := BuildOverlap(a, OverlapConfig{Ratio: .4, Capacity: 4, RequireExact: true})
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildOverlap(a, OverlapConfig{Ratio: .4, Capacity: 4, RequireExact: true})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) || first.Used != 2 || first.Unspent != 0 || !reflect.DeepEqual(first.Loads, []int{3, 2, 2}) {
		t.Fatalf("uneven exact fill first=%+v second=%+v", first, second)
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

func TestValidateOverlapCapacityBelowHomeCapReportsBothValuesV1(t *testing.T) {
	c := Config{Metric: "cosine", Seed: 1, Repetitions: 1, Pivots: 2, MaxLeafBucket: 2, Degree: 3, Partitions: 2, Imbalance: 0, MaxVectors: 4, MaxEdges: 12}
	v := []Vector{{"a", []float64{1}}, {"b", []float64{.9}}, {"c", []float64{.8}}, {"d", []float64{.7}}}
	built, err := Build(v, c)
	if err != nil {
		t.Fatal(err)
	}
	a := Artifact{SchemaVersion: SchemaVersion, Backend: "test", BackendLicense: "test", Source: built.Source, Config: c, IDs: []string{"a", "b", "c", "d"}, Graph: Graph{Neighbors: [][]int{{}, {}, {}, {}}}, Assignment: []int{0, 0, 1, 1}}
	a.Metrics = metrics(a)
	err = ValidateOverlap(a, OverlapConfig{Capacity: a.Metrics.Cap - 1}, OverlapResult{})
	if err == nil || !strings.Contains(err.Error(), "capacity 1 below immutable home load cap 2") {
		t.Fatalf("err=%v", err)
	}
}
