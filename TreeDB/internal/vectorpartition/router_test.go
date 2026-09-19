package vectorpartition

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestPartitionRouterDeterministicBytesAndOrder(t *testing.T) {
	cfg := routerTestConfigV1()
	partitions := []RouterPartitionV1{
		{PartitionID: 9, Vectors: []RouterVectorV1{
			{Ordinal: 13, Values: []float32{0, 1}},
			{Ordinal: 11, Values: []float32{1, 0}},
			{Ordinal: 12, Values: []float32{.9, .1}},
		}},
		{PartitionID: 2, Vectors: []RouterVectorV1{
			{Ordinal: 3, Values: []float32{-1, 0}},
			{Ordinal: 1, Values: []float32{0, -1}},
			{Ordinal: 2, Values: []float32{-.9, -.1}},
		}},
	}
	first, err := BuildRouterV1(partitions, cfg)
	if err != nil {
		t.Fatal(err)
	}
	permuted := []RouterPartitionV1{
		{PartitionID: 2, Vectors: []RouterVectorV1{partitions[1].Vectors[2], partitions[1].Vectors[0], partitions[1].Vectors[1]}},
		{PartitionID: 9, Vectors: []RouterVectorV1{partitions[0].Vectors[1], partitions[0].Vectors[2], partitions[0].Vectors[0]}},
	}
	second, err := BuildRouterV1(permuted, cfg)
	if err != nil {
		t.Fatal(err)
	}
	firstBytes, err := CanonicalRouterJSONV1(first)
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := CanonicalRouterJSONV1(second)
	if err != nil {
		t.Fatal(err)
	}
	if string(firstBytes) != string(secondBytes) {
		t.Fatalf("deterministic bytes differ\nfirst:  %s\nsecond: %s", firstBytes, secondBytes)
	}
	if first.Representatives[0].PartitionID != 2 {
		t.Fatalf("representatives are not partition ordered: %+v", first.Representatives)
	}
	wantBytes, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstBytes, wantBytes) {
		t.Fatalf("canonical streaming bytes changed\n got: %s\nwant: %s", firstBytes, wantBytes)
	}
}

func TestPartitionRouterValidationAndDigestObserveContext(t *testing.T) {
	cfg := routerTestConfigV1()
	cfg.MaxVectors = 256
	cfg.MaxRepresentatives = 256
	cfg.RepresentativeBudget = 256
	partitions := make([]RouterPartitionV1, 128)
	for i := range partitions {
		partitions[i] = RouterPartitionV1{
			PartitionID: uint32(i + 1),
			Vectors: []RouterVectorV1{{
				Ordinal: uint64(i + 1),
				Values:  []float32{1, 0},
			}},
		}
	}
	model, err := BuildRouterV1(partitions, cfg)
	if err != nil {
		t.Fatal(err)
	}

	validationCanceled := &routerCancelAfterErrContextV1{cancelAt: 8}
	if err := ValidateRouterModelWithContextV1(validationCanceled, model); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("validation err=%v want deadline exceeded", err)
	}

	validationCounter := &routerCancelAfterErrContextV1{}
	if err := ValidateRouterModelWithContextV1(validationCounter, model); err != nil {
		t.Fatal(err)
	}
	digestCanceled := &routerCancelAfterErrContextV1{cancelAt: validationCounter.calls + 3}
	if _, err := RouterDigestWithContextV1(digestCanceled, model); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("digest err=%v want deadline exceeded", err)
	}
}

func TestKMeansRepresentativeRouterNonConvexFixture(t *testing.T) {
	cfg := routerTestConfigV1()
	cfg.RepresentativeBudget = 6
	partitions := []RouterPartitionV1{
		{PartitionID: 1, Vectors: []RouterVectorV1{
			{Ordinal: 1, Values: []float32{1, .02}},
			{Ordinal: 2, Values: []float32{1, -.02}},
			{Ordinal: 3, Values: []float32{-1, .02}},
			{Ordinal: 4, Values: []float32{-1, -.02}},
		}},
		{PartitionID: 2, Vectors: []RouterVectorV1{
			{Ordinal: 5, Values: []float32{.02, 1}},
			{Ordinal: 6, Values: []float32{-.02, 1}},
			{Ordinal: 7, Values: []float32{.02, -1}},
			{Ordinal: 8, Values: []float32{-.02, -1}},
		}},
	}
	model, err := BuildRouterV1(partitions, cfg)
	if err != nil {
		t.Fatal(err)
	}
	singleConfig := cfg
	singleConfig.RepresentativeBudget = 2
	single, err := BuildRouterV1(partitions, singleConfig)
	if err != nil {
		t.Fatal(err)
	}
	singleResult, err := RouteExactV1(single, []float32{-1, 0}, len(single.Representatives), 1)
	if err != nil {
		t.Fatal(err)
	}
	if singleResult.Partitions[0].PartitionID == 1 {
		t.Fatalf("single-centroid fixture did not expose the non-convex routing loss: %+v", singleResult)
	}
	for _, test := range []struct {
		query []float32
		want  uint32
	}{
		{query: []float32{1, 0}, want: 1},
		{query: []float32{-1, 0}, want: 1},
		{query: []float32{0, 1}, want: 2},
		{query: []float32{0, -1}, want: 2},
	} {
		result, err := RouteExactV1(model, test.query, len(model.Representatives), 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Partitions) != 1 || result.Partitions[0].PartitionID != test.want {
			t.Fatalf("query=%v got=%+v want partition=%d", test.query, result, test.want)
		}
	}
}

func TestKMeansRepresentativeRouterDoesNotFabricateIdenticalCenters(t *testing.T) {
	cfg := routerTestConfigV1()
	cfg.RepresentativeBudget = 3
	model, err := BuildRouterV1([]RouterPartitionV1{{
		PartitionID: 7,
		Vectors: []RouterVectorV1{
			{Ordinal: 1, Values: []float32{1, 0}},
			{Ordinal: 2, Values: []float32{1, 0}},
			{Ordinal: 3, Values: []float32{1, 0}},
			{Ordinal: 4, Values: []float32{1, 0}},
		},
	}}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if model.Metrics.EmptyRepairs != 0 || model.Metrics.UnusedBudget != 2 {
		t.Fatal("identical population must stop without fabricated duplicate centers")
	}
	if got := len(model.Representatives); got != 1 {
		t.Fatalf("representatives=%d want 1", got)
	}
}

func TestPartitionRouterRejectsMalformedAndBoundedInputs(t *testing.T) {
	cfg := routerTestConfigV1()
	valid := []RouterPartitionV1{{PartitionID: 1, Vectors: []RouterVectorV1{
		{Ordinal: 1, Values: []float32{1, 0}},
		{Ordinal: 2, Values: []float32{0, 1}},
	}}}
	tests := []struct {
		name       string
		partitions []RouterPartitionV1
		mutate     func(*RouterConfigV1)
		contains   string
	}{
		{name: "empty partition", partitions: []RouterPartitionV1{{PartitionID: 1}}, contains: "empty"},
		{name: "non finite", partitions: []RouterPartitionV1{{PartitionID: 1, Vectors: []RouterVectorV1{{Ordinal: 1, Values: []float32{float32(math.NaN()), 1}}}}}, contains: "non-finite"},
		{name: "zero norm", partitions: []RouterPartitionV1{{PartitionID: 1, Vectors: []RouterVectorV1{{Ordinal: 1, Values: []float32{0, 0}}}}}, contains: "norm"},
		{name: "dimension mismatch", partitions: []RouterPartitionV1{{PartitionID: 1, Vectors: []RouterVectorV1{{Ordinal: 1, Values: []float32{1, 0}}, {Ordinal: 2, Values: []float32{1}}}}}, contains: "dimensions"},
		{name: "duplicate ordinal", partitions: []RouterPartitionV1{{PartitionID: 1, Vectors: []RouterVectorV1{{Ordinal: 1, Values: []float32{1, 0}}, {Ordinal: 1, Values: []float32{0, 1}}}}}, contains: "duplicate"},
		{name: "branch limit", partitions: valid, mutate: func(c *RouterConfigV1) { c.BranchFactor = routerMaxRepresentatives + 1 }, contains: "branch factor"},
		{name: "work budget", partitions: valid, mutate: func(c *RouterConfigV1) { c.MaxScalarWork = 1 }, contains: "scalar work"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testConfig := cfg
			if test.mutate != nil {
				test.mutate(&testConfig)
			}
			_, err := BuildRouterV1(test.partitions, testConfig)
			if err == nil || !strings.Contains(err.Error(), test.contains) {
				t.Fatalf("err=%v want substring %q", err, test.contains)
			}
		})
	}
}

func TestRepresentativeRouterExactOracleStableTieAndBudgets(t *testing.T) {
	cfg := routerTestConfigV1()
	cfg.RepresentativeBudget = 3
	model, err := BuildRouterV1([]RouterPartitionV1{
		{PartitionID: 8, Vectors: []RouterVectorV1{{Ordinal: 8, Values: []float32{1, 0}}}},
		{PartitionID: 3, Vectors: []RouterVectorV1{{Ordinal: 3, Values: []float32{1, 0}}}},
		{PartitionID: 5, Vectors: []RouterVectorV1{{Ordinal: 5, Values: []float32{0, 1}}}},
	}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	result, err := RouteExactV1(model, []float32{1, 0}, len(model.Representatives), 2)
	if err != nil {
		t.Fatal(err)
	}
	got := []uint32{result.Partitions[0].PartitionID, result.Partitions[1].PartitionID}
	if want := []uint32{3, 8}; !reflect.DeepEqual(got, want) {
		t.Fatalf("stable tie order=%v want %v", got, want)
	}
	if _, err := RouteExactV1(model, []float32{1, 0}, len(model.Representatives)-1, 1); err == nil {
		t.Fatal("expected undersized exact candidate budget to fail")
	}
	if _, err := RouteExactV1(model, []float32{1, 0}, len(model.Representatives), 0); err == nil {
		t.Fatal("expected zero partition probes to fail")
	}
	if _, err := RouteExactV1(model, []float32{1, 0}, len(model.Representatives), model.Metrics.Partitions+1); err == nil {
		t.Fatal("expected oversized partition probes to fail")
	}
	if _, err := RouteExactV1(model, []float32{math.SmallestNonzeroFloat32, 0}, len(model.Representatives), 1); err == nil {
		t.Fatal("expected underflowed router query norm to fail")
	}
}

func TestCheckedRouterScalarWorkBoundsAllLevelDistanceWorkV1(t *testing.T) {
	cfg := DefaultRouterConfigV1()
	work, ok := CheckedRouterScalarWorkV1([]int{75_000, 75_000, 75_000, 75_000}, 128, cfg)
	if !ok || work != 39_667_200_000 {
		t.Fatalf("router work=%d ok=%v want 39667200000", work, ok)
	}
	if work > 50_000_000_000 {
		t.Fatalf("retained D4 envelope exceeds 50B: %d", work)
	}
	cfg.RepresentativeBudget = 1
	work, ok = CheckedRouterScalarWorkV1([]int{300_000}, 128, cfg)
	if !ok || work != 38_400_000 {
		t.Fatalf("root-only router work=%d ok=%v want 38400000", work, ok)
	}
	cfg.RepresentativeBudget = 3
	work, ok = CheckedRouterScalarWorkV1([]int{300_000}, 128, cfg)
	if !ok || work != 1_881_600_000 {
		t.Fatalf("quota-limited router work=%d ok=%v want 1881600000", work, ok)
	}
	cfg.BranchFactor = 256
	cfg.MaxDepth = 64
	cfg.MaxIterations = 1
	cfg.RepresentativeBudget = 256
	work, ok = CheckedRouterScalarWorkV1([]int{1_000}, 128, cfg)
	if !ok || work != 41_088_000 {
		t.Fatalf("wide quota-feasible router work=%d ok=%v want 41088000", work, ok)
	}
	cfg.BranchFactor = 100
	cfg.LeafSize = 900
	cfg.MaxDepth = 8
	cfg.MaxIterations = 16
	cfg.RepresentativeBudget = 1_999
	work, ok = CheckedRouterScalarWorkV1([]int{1_000}, 1, cfg)
	if !ok || work != 14_409_000 {
		t.Fatalf("member-limited router work=%d ok=%v want 14409000", work, ok)
	}
	cfg.BranchFactor = 1_000
	cfg.LeafSize = 1
	cfg.MaxDepth = 2
	cfg.MaxIterations = 1
	work, ok = CheckedRouterScalarWorkV1([]int{1_000}, 64, cfg)
	if !ok || work != 128_128_000 {
		t.Fatalf("total-member-limited router work=%d ok=%v want 128128000", work, ok)
	}
	if _, ok := CheckedRouterScalarWorkV1([]int{routerMaxVectors}, math.MaxInt, cfg); ok {
		t.Fatal("overflowing router work was accepted")
	}
}

func TestCheckedRouterScalarWorkDoesNotCombineExclusiveCenterSelectionPathsV1(t *testing.T) {
	cfg := DefaultRouterConfigV1()
	cfg.BranchFactor = 2
	cfg.LeafSize = 1
	cfg.MaxDepth = 1
	cfg.MaxIterations = 42
	cfg.RepresentativeBudget = 3
	work, ok := CheckedRouterScalarWorkV1([]int{1_200_000}, 128, cfg)
	if !ok || work != 13_056_000_000 {
		t.Fatalf("router work=%d ok=%v want 13056000000", work, ok)
	}
}

func TestCheckedRouterScalarWorkPreservesTerminalFullWidthV1(t *testing.T) {
	cfg := DefaultRouterConfigV1()
	cfg.BranchFactor = 3
	cfg.LeafSize = 1
	cfg.MaxDepth = 2
	cfg.MaxIterations = 1
	cfg.RepresentativeBudget = 5
	work, ok := CheckedRouterScalarWorkV1([]int{300_000}, 4_096, cfg)
	if !ok || work != 8_601_600_000 {
		t.Fatalf("router work=%d ok=%v want 8601600000", work, ok)
	}
}

func TestCheckedRouterScalarWorkCoversExhaustiveForestOracleV3(t *testing.T) {
	type state struct{ members, budget, depth int }
	compositions := func(total, parts int, visit func([]int)) {
		values := make([]int, parts)
		var walk func(int, int)
		walk = func(at, remaining int) {
			if at == parts-1 {
				values[at] = remaining
				visit(append([]int(nil), values...))
				return
			}
			for value := 1; value <= remaining-(parts-at-1); value++ {
				values[at] = value
				walk(at+1, remaining-value)
			}
		}
		walk(0, total)
	}
	for population := 1; population <= 8; population++ {
		for budget := 1; budget <= 10; budget++ {
			for branch := 2; branch <= 4; branch++ {
				for maxDepth := 1; maxDepth <= 3; maxDepth++ {
					for leafSize := 1; leafSize <= population; leafSize++ {
						for iterations := 1; iterations <= 3; iterations++ {
							cfg := DefaultRouterConfigV1()
							cfg.BranchFactor, cfg.LeafSize = branch, leafSize
							cfg.MaxDepth, cfg.MaxIterations = maxDepth, iterations
							cfg.RepresentativeBudget = budget
							quotas, err := ApportionRouterBudgetV2([]int{population}, budget)
							if err != nil {
								t.Fatal(err)
							}
							memo := make(map[state]int)
							var subtree func(state) int
							subtree = func(current state) int {
								if cached, ok := memo[current]; ok {
									return cached
								}
								best := current.members // this centroid's medoid pass.
								if current.members > leafSize && current.depth < maxDepth && current.budget >= 3 {
									width := min(branch, min(current.members, current.budget-1))
									compositions(current.members, width, func(counts []int) {
										eligible := make([]bool, width)
										for i, count := range counts {
											eligible[i] = count > leafSize && current.depth+1 < maxDepth
										}
										childBudgets, err := apportionRouterSubtreeBudgetsV3(counts, eligible, current.budget-1)
										if err != nil {
											t.Fatal(err)
										}
										candidate := current.members + current.members*width*iterations
										for i, count := range counts {
											candidate += subtree(state{count, childBudgets[i], current.depth + 1})
										}
										best = max(best, candidate)
									})
								}
								memo[current] = best
								return best
							}
							quota := quotas[0]
							rootWidth := min(branch, min(population, quota))
							oracle := population
							if rootWidth >= 2 {
								oracle = 0
								compositions(population, rootWidth, func(counts []int) {
									eligible := make([]bool, rootWidth)
									for i, count := range counts {
										eligible[i] = count > leafSize
									}
									rootBudgets, err := apportionRouterSubtreeBudgetsV3(counts, eligible, quota)
									if err != nil {
										t.Fatal(err)
									}
									candidate := population * rootWidth * iterations
									for i, count := range counts {
										candidate += subtree(state{count, rootBudgets[i], 0})
									}
									oracle = max(oracle, candidate)
								})
							}
							got, ok := CheckedRouterScalarWorkV1([]int{population}, 1, cfg)
							if !ok || got < int64(oracle) {
								t.Fatalf("population=%d budget=%d branch=%d depth=%d leaf=%d iterations=%d: work=%d ok=%v oracle=%d", population, budget, branch, maxDepth, leafSize, iterations, got, ok, oracle)
							}
						}
					}
				}
			}
		}
	}
}

func TestRouterSampledInitializationKeepsDistinctRoundedDirectionsV3(t *testing.T) {
	vectors := []routerBuildVectorV1{
		{ordinal: 1, values: []float32{0.5718129277229309, -0.8203840851783752}},
		{ordinal: 2, values: []float32{0.5718127489089966, -0.8203842043876648}},
	}
	if reflect.DeepEqual(vectors[0].values, vectors[1].values) {
		t.Fatal("test vectors must be distinct")
	}
	if distance := routerCosineDistanceNormalizedV1(vectors[0].values, vectors[1].values); distance != 0 {
		t.Fatalf("rounded cosine distance=%g want 0", distance)
	}
	parent := &routerBuildNodeV1{
		record:  RouterHierarchyNodeV1{NodeID: 1, PartitionID: 1},
		members: []int{0, 1},
	}
	if centers := routerInitialCentersV1(vectors, parent, 2, 0); len(centers) != 2 {
		t.Fatalf("initial centers=%d want 2 distinct bitwise directions", len(centers))
	}
	cfg := DefaultRouterConfigV1()
	cfg.BranchFactor = 2
	cfg.LeafSize = 1
	cfg.MaxDepth = 1
	cfg.MaxIterations = 1
	cfg.RepresentativeBudget = 3
	work, ok := CheckedRouterScalarWorkV1([]int{2}, 2, cfg)
	if !ok || work != 12 {
		t.Fatalf("sampled-initialization router work=%d ok=%v want 12", work, ok)
	}
}

func TestPartitionRouterModelValidationRejectsForgedMetadata(t *testing.T) {
	cfg := routerTestConfigV1()
	cfg.RepresentativeBudget = 3
	model, err := BuildRouterV1([]RouterPartitionV1{{
		PartitionID: 4,
		Vectors: []RouterVectorV1{
			{Ordinal: 1, Values: []float32{1, 0}},
			{Ordinal: 2, Values: []float32{.9, .1}},
			{Ordinal: 3, Values: []float32{0, 1}},
		},
	}}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*RouterModelV1){
		func(candidate *RouterModelV1) { candidate.Metrics.Vectors++ },
		func(candidate *RouterModelV1) { candidate.Metrics.StoppedNoSplit++ },
		func(candidate *RouterModelV1) {
			candidate.Metrics.LloydIterations += candidate.Config.MaxIterations + 1
		},
		func(candidate *RouterModelV1) { candidate.Nodes[0].MemberCount-- },
		func(candidate *RouterModelV1) { candidate.Nodes[1].Depth++ },
		func(candidate *RouterModelV1) { candidate.Representatives[0].MemberCount++ },
		func(candidate *RouterModelV1) {
			leafID := candidate.Representatives[0].NodeID
			for i := range candidate.Nodes {
				if candidate.Nodes[i].NodeID == leafID {
					candidate.Nodes[i].MemberCount++
				}
			}
			candidate.Representatives[0].MemberCount++
		},
		func(candidate *RouterModelV1) {
			candidate.Representatives[1].NodeID = candidate.Representatives[0].NodeID
		},
		func(candidate *RouterModelV1) { candidate.Representatives[0].Values[0] *= .5 },
	} {
		candidate := model
		candidate.Nodes = append([]RouterHierarchyNodeV1(nil), model.Nodes...)
		candidate.Representatives = append([]RouterRepresentativeV1(nil), model.Representatives...)
		for i := range candidate.Representatives {
			candidate.Representatives[i].Path = append([]uint32(nil), model.Representatives[i].Path...)
			candidate.Representatives[i].Values = append([]float32(nil), model.Representatives[i].Values...)
		}
		mutate(&candidate)
		if err := ValidateRouterModelV1(candidate); err == nil {
			t.Fatalf("accepted forged router model: %+v", candidate)
		}
	}
}

func routerTestConfigV1() RouterConfigV1 {
	cfg := DefaultRouterConfigV1()
	cfg.BranchFactor = 3
	cfg.LeafSize = 1
	cfg.RepresentativeBudget = 3
	cfg.MaxDepth = 4
	cfg.MaxIterations = 8
	cfg.MaxVectors = 100
	cfg.MaxDimensions = 16
	cfg.MaxRepresentatives = 100
	cfg.MaxScalarWork = 1_000_000
	return cfg
}

func TestDefaultRouterConfigV1(t *testing.T) {
	cfg := DefaultRouterConfigV1()
	if cfg.RepresentativeBudget != 256 {
		t.Fatalf("global representative budget=%d want 256", cfg.RepresentativeBudget)
	}
	if cfg.BranchFactor != 64 || cfg.LeafSize != 250 {
		t.Fatalf("reference geometry fanout=%d leaf=%d want 64,250", cfg.BranchFactor, cfg.LeafSize)
	}
	if cfg.MaxIterations != 16 {
		t.Fatalf("max iterations=%d want 16", cfg.MaxIterations)
	}
	if cfg.MaxScalarWork != routerDefaultScalarWork {
		t.Fatalf("max scalar work=%d want default %d", cfg.MaxScalarWork, routerDefaultScalarWork)
	}
	for _, work := range []int64{50_000_000_000, 0, 50_000_000_001} {
		cfg := cfg
		cfg.MaxScalarWork = work
		err := ValidateRouterConfigV1(cfg)
		if work == 50_000_000_000 && err != nil {
			t.Fatalf("explicit 50B scalar-work cap rejected: %v", err)
		}
		if work != 50_000_000_000 && err == nil {
			t.Fatalf("invalid scalar-work cap %d accepted", work)
		}
	}
}

type routerCancelAfterErrContextV1 struct {
	calls    int
	cancelAt int
}

func (c *routerCancelAfterErrContextV1) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *routerCancelAfterErrContextV1) Done() <-chan struct{}       { return nil }
func (c *routerCancelAfterErrContextV1) Value(any) any               { return nil }
func (c *routerCancelAfterErrContextV1) Err() error {
	c.calls++
	if c.cancelAt > 0 && c.calls >= c.cancelAt {
		return context.DeadlineExceeded
	}
	return nil
}
