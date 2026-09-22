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
		Budget:               2,
		Used:                 1,
		Unspent:              1,
		Capacity:             4,
		Loads:                []int{3, 2},
		EdgeCutBefore:        4,
		EdgeCutAfter:         0,
		Useful:               1,
		Replicas:             []Replica{{VectorOrdinal: 2, Partition: 0, Policy: overlapReplicaPolicyV1, Gain: 4, Class: ReplicaUtilityPositiveGainV1}},
		CumulativeUtility:    4,
		DestinationDiversity: []int{1, 0},
	}
	if !reflect.DeepEqual(one, want) {
		t.Fatalf("boundary overlap=%+v want %+v", one, want)
	}
	tampered := one
	tampered.Replicas = append([]Replica(nil), one.Replicas...)
	tampered.Replicas[0].Gain++
	tampered.CumulativeUtility++
	if err := ValidateOverlap(a, OverlapConfig{Ratio: .5}, tampered); err == nil {
		t.Fatal("utility detached from edge-cut reduction accepted")
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
	if got.Budget != 2 || got.Used != 2 || got.Useful != 0 || got.Filler != 2 || got.Unspent != 0 || got.EdgeCutBefore != 0 || got.EdgeCutAfter != 0 {
		t.Fatalf("exact fallback=%+v", got)
	}
	for _, replica := range got.Replicas {
		if replica.Policy != overlapReplicaPolicyV1 || replica.Gain != 0 || replica.Class != ReplicaUtilityZeroUtilityV1 {
			t.Fatalf("zero-cut replica was not honestly classified: %+v", replica)
		}
	}
	tampered := got
	tampered.Useful, tampered.Filler = got.Filler, got.Useful
	if err := ValidateOverlap(a, OverlapConfig{Ratio: .5, Capacity: 3, RequireExact: true}, tampered); err == nil {
		t.Fatal("aggregate replica-class swap accepted")
	}
}

func TestOverlapExactTreatmentRanksUtilityBeforeOrdinalFillV1(t *testing.T) {
	c := Config{Metric: "cosine", Seed: 1, Repetitions: 1, Pivots: 2, MaxLeafBucket: 2, Degree: 3, Partitions: 2, Imbalance: 1, MaxVectors: 5, MaxEdges: 15}
	v := []Vector{{"a", []float64{1}}, {"b", []float64{.9}}, {"c", []float64{.8}}, {"d", []float64{.7}}, {"e", []float64{.6}}}
	built, err := Build(v, c)
	if err != nil {
		t.Fatal(err)
	}
	a := Artifact{SchemaVersion: SchemaVersion, Backend: "test", BackendLicense: "test", Source: built.Source, Config: c, IDs: []string{"a", "b", "c", "d", "e"}, Graph: Graph{Neighbors: [][]int{{}, {}, {}, {0, 1}, {}}}, Assignment: []int{0, 0, 1, 1, 1}}
	a.Metrics = metrics(a)
	got, err := BuildOverlap(a, OverlapConfig{Ratio: .4, Capacity: 5, RequireExact: true})
	if err != nil {
		t.Fatal(err)
	}
	want := []Replica{
		{VectorOrdinal: 0, Partition: 1, Policy: overlapReplicaPolicyV1, Gain: 0, Class: ReplicaUtilityZeroUtilityV1},
		{VectorOrdinal: 3, Partition: 0, Policy: overlapReplicaPolicyV1, Gain: 2, Class: ReplicaUtilityPositiveGainV1},
	}
	if got.Used != 2 || got.Useful != 1 || got.Filler != 1 || got.CumulativeUtility != 2 || !reflect.DeepEqual(got.Replicas, want) {
		t.Fatalf("utility order/fill=%+v want replicas=%+v", got, want)
	}
}

func TestExactFillerReclassifiesLaterCutReductionV1(t *testing.T) {
	a := Artifact{IDs: []string{"a", "b", "c", "d"}, Graph: Graph{Neighbors: [][]int{{2}, {}, {0}, {}}}, Config: Config{Partitions: 3}, Assignment: []int{0, 0, 1, 1}}
	members := []map[int]struct{}{{0: {}}, {0: {}}, {1: {}}, {1: {}}}
	loads := []int{2, 2, 0}
	incoming := [][]int{{2}, nil, {0}, nil}
	used, useful, filler := fillExactOverlapSlots(a, members, incoming, loads, 3, 3)
	if used != 3 || useful != 1 || filler != 2 {
		t.Fatalf("filled used=%d useful=%d filler=%d", used, useful, filler)
	}
}

func TestExactFillerCountsIncomingCutReductionAsUsefulV1(t *testing.T) {
	a := Artifact{IDs: []string{"a", "b"}, Graph: Graph{Neighbors: [][]int{{}, {0}}}, Config: Config{Partitions: 2}, Assignment: []int{0, 1}}
	members := []map[int]struct{}{{0: {}}, {1: {}}}
	loads := []int{1, 1}
	used, useful, filler := fillExactOverlapSlots(a, members, [][]int{{1}, nil}, loads, 2, 1)
	if used != 1 || useful != 1 || filler != 0 {
		t.Fatalf("filled used=%d useful=%d filler=%d", used, useful, filler)
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
	var shortfall *OverlapShortfallError
	if err := ValidateOverlap(a, OverlapConfig{Ratio: .5, RequireExact: true}, r); !errors.As(err, &shortfall) || shortfall.Requested != r.Budget || shortfall.Realized != r.Used || shortfall.Rejected != r.Unspent {
		t.Fatalf("exact validation err=%v shortfall=%+v", err, shortfall)
	}
}

func TestOverlapAffinitySkipsFullPreferredPartitionV1(t *testing.T) {
	c := Config{Metric: "cosine", Seed: 1, Repetitions: 1, Pivots: 2, MaxLeafBucket: 2, Degree: 3, Partitions: 3, Imbalance: 0, MaxVectors: 4, MaxEdges: 12}
	v := []Vector{{"a", []float64{1}}, {"b", []float64{.9}}, {"c", []float64{.8}}, {"d", []float64{.7}}}
	built, err := Build(v, c)
	if err != nil {
		t.Fatal(err)
	}
	a := Artifact{SchemaVersion: SchemaVersion, Backend: "test", BackendLicense: "test", Source: built.Source, Config: c, IDs: []string{"a", "b", "c", "d"}, Graph: Graph{Neighbors: [][]int{{}, {}, {0, 1, 3}, {}}}, Assignment: []int{0, 0, 1, 2}}
	a.Metrics = metrics(a)
	got, err := BuildOverlap(a, OverlapConfig{Ratio: .25, Capacity: 2, RequireExact: true})
	if err != nil {
		t.Fatal(err)
	}
	if got.Used != 1 || got.Unspent != 0 || !reflect.DeepEqual(got.Loads, []int{2, 2, 1}) {
		t.Fatalf("overlap=%+v", got)
	}
	if !reflect.DeepEqual(got.Memberships, []Membership{{VectorOrdinal: 0, Partition: 0, Home: true}, {VectorOrdinal: 0, Partition: 1}, {VectorOrdinal: 1, Partition: 0, Home: true}, {VectorOrdinal: 2, Partition: 1, Home: true}, {VectorOrdinal: 3, Partition: 2, Home: true}}) {
		t.Fatalf("memberships=%+v", got.Memberships)
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
