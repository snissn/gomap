package vectorpartition

import (
	"math"
	"reflect"
	"testing"
)

func TestRouterSphericalCenterCancellationReference(t *testing.T) {
	for _, input := range [][][]float32{
		{{1, 0}, {-1, 0}},
		{{1, 1e-7}, {-1, 1e-7}},
		{{1, 0}},
	} {
		vectors := make([]routerBuildVectorV1, len(input))
		members := make([]int, len(input))
		sum := [2]float64{}
		for i, values := range input {
			vectors[i], members[i] = routerBuildVectorV1{ordinal: uint64(i), values: values}, i
			sum[0] += float64(values[0])
			sum[1] += float64(values[1])
		}
		want := input[0]
		if norm := math.Hypot(sum[0], sum[1]); norm != 0 {
			want = []float32{float32(sum[0] / norm), float32(sum[1] / norm)}
		}
		if got := routerSphericalCenterV2(vectors, members); !reflect.DeepEqual(got, want) {
			t.Fatalf("input=%v got=%v reference=%v", input, got, want)
		}
	}
}

func TestRouterGlobalBudgetRetainsInternalCenters(t *testing.T) {
	cfg := DefaultRouterConfigV1()
	cfg.RepresentativeBudget = 7
	cfg.BranchFactor, cfg.LeafSize = 2, 1
	partitions := []RouterPartitionV1{
		{PartitionID: 7, Vectors: []RouterVectorV1{{Ordinal: 1, Values: []float32{1, 0}}, {Ordinal: 2, Values: []float32{0, 1}}, {Ordinal: 3, Values: []float32{-1, 0}}, {Ordinal: 4, Values: []float32{0, -1}}}},
		{PartitionID: 2, Vectors: []RouterVectorV1{{Ordinal: 9, Values: []float32{1, 1}}}},
	}
	model, err := BuildRouterV1(partitions, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(model.Representatives) != len(model.Nodes) || len(model.Nodes) > cfg.RepresentativeBudget {
		t.Fatalf("nodes=%d representatives=%d budget=%d", len(model.Nodes), len(model.Representatives), cfg.RepresentativeBudget)
	}
	internal, sharedAnchor := false, false
	anchors := map[[2]uint64]bool{}
	for _, rep := range model.Representatives {
		node := model.Nodes[rep.NodeID-1]
		internal = internal || !node.Leaf
		key := [2]uint64{uint64(rep.PartitionID), rep.SourceOrdinal}
		sharedAnchor = sharedAnchor || anchors[key]
		anchors[key] = true
	}
	if !internal || !sharedAnchor {
		t.Fatalf("missing internal representative or repeated provenance anchor: %+v", model)
	}
	partitions[0], partitions[1] = partitions[1], partitions[0]
	other, err := BuildRouterV1(partitions, cfg)
	if err != nil || !reflect.DeepEqual(model, other) {
		t.Fatalf("input order changed model: %v", err)
	}
	cfg.RepresentativeBudget = 1
	if _, err := BuildRouterV1(partitions, cfg); err == nil {
		t.Fatal("accepted budget below domain count")
	}
}

func TestRouterGlobalBudgetDegenerateUnderfill(t *testing.T) {
	cfg := DefaultRouterConfigV1()
	cfg.RepresentativeBudget, cfg.LeafSize = 32, 1
	model, err := BuildRouterV1([]RouterPartitionV1{{PartitionID: 0, Vectors: []RouterVectorV1{{Ordinal: 0, Values: []float32{1, 0}}, {Ordinal: 1, Values: []float32{1, 0}}}}}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(model.Nodes) != 1 || model.Metrics.UnusedBudget != 31 {
		t.Fatalf("manufactured centers for identical vectors: %+v", model)
	}
}

func TestRouterGlobalBudgetDoesNotSplitDuplicateDirections(t *testing.T) {
	cfg := DefaultRouterConfigV1()
	cfg.RepresentativeBudget, cfg.LeafSize = 32, 1
	// These non-axis directions have FP32 norm error: dot(x,x) need not be 1.
	// Farthest-first initialization must not mistake that for a new direction.
	model, err := BuildRouterV1([]RouterPartitionV1{{PartitionID: 0, Vectors: []RouterVectorV1{
		{Ordinal: 0, Values: []float32{1, 1}}, {Ordinal: 1, Values: []float32{1, 1}},
		{Ordinal: 2, Values: []float32{-1, 1}}, {Ordinal: 3, Values: []float32{-1, 1}},
	}}}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(model.Nodes) != 3 || model.Metrics.UnusedBudget != 29 {
		t.Fatalf("fabricated duplicate-direction centers: %+v", model)
	}
}

func TestRouterGlobalBudgetApportionment(t *testing.T) {
	for _, tc := range []struct {
		populations []int
		budget      int
		want        []int
	}{
		{[]int{1, 1}, 20, []int{1, 1}},
		{[]int{2, 2, 2}, 5, []int{2, 2, 1}},
		{[]int{1, 4}, 7, []int{1, 6}},
		{[]int{4, 2}, 6, []int{4, 2}},
	} {
		got, err := ApportionRouterBudgetV2(tc.populations, tc.budget)
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("population=%v B=%d got=%v want=%v err=%v", tc.populations, tc.budget, got, tc.want, err)
		}
	}
	for _, populations := range [][]int{{0}, {-1}, {1, 1}} {
		if _, err := ApportionRouterBudgetV2(populations, 1); err == nil {
			t.Fatalf("accepted invalid allocation: %v", populations)
		}
	}
}

func TestRouterGlobalBudgetCanonicalSiblingQuotaIdentity(t *testing.T) {
	// Several seeds produce a different farthest-center order. Identity and
	// quota ties must both use canonical sibling order after clustering.
	for seed := int64(0); seed < 12; seed++ {
		cfg := DefaultRouterConfigV1()
		cfg.Seed, cfg.RepresentativeBudget, cfg.LeafSize, cfg.BranchFactor = seed, 6, 1, 2
		model, err := BuildRouterV1([]RouterPartitionV1{{PartitionID: 0, Vectors: []RouterVectorV1{
			{Ordinal: 0, Values: []float32{1, .1}}, {Ordinal: 1, Values: []float32{1, -.1}},
			{Ordinal: 2, Values: []float32{-1, .1}}, {Ordinal: 3, Values: []float32{-1, -.1}},
		}}}, cfg)
		if err != nil || len(model.Nodes) > 6 {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		if model.Nodes[1].Budget != 3 || model.Nodes[2].Budget != 2 {
			t.Fatalf("noncanonical sibling quotas: %+v", model.Nodes)
		}
	}
}
