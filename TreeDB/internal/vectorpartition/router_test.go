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
	if !ok || work != 37_862_400_000 {
		t.Fatalf("router work=%d ok=%v want 37862400000", work, ok)
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
	if !ok || work != 2_073_600_000 {
		t.Fatalf("quota-limited router work=%d ok=%v want 2073600000", work, ok)
	}
	cfg.BranchFactor = 256
	cfg.MaxDepth = 64
	cfg.MaxIterations = 1
	cfg.RepresentativeBudget = 256
	work, ok = CheckedRouterScalarWorkV1([]int{1_000}, 128, cfg)
	if !ok || work != 4_243_456_000 {
		t.Fatalf("wide quota-feasible router work=%d ok=%v want 4243456000", work, ok)
	}
	cfg.BranchFactor = 100
	cfg.LeafSize = 900
	cfg.RepresentativeBudget = 1_999
	work, ok = CheckedRouterScalarWorkV1([]int{1_000}, 1, cfg)
	if !ok || work != 10_502_000 {
		t.Fatalf("member-limited router work=%d ok=%v want 10502000", work, ok)
	}
	cfg.BranchFactor = 1_000
	cfg.LeafSize = 1
	cfg.MaxDepth = 2
	work, ok = CheckedRouterScalarWorkV1([]int{1_000}, 64, cfg)
	if !ok || work != 32_160_128_000 {
		t.Fatalf("total-member-limited router work=%d ok=%v want 32160128000", work, ok)
	}
	if _, ok := CheckedRouterScalarWorkV1([]int{routerMaxVectors}, math.MaxInt, cfg); ok {
		t.Fatal("overflowing router work was accepted")
	}
}

func TestCheckedRouterScalarWorkCoversFailedCenterSelectionV1(t *testing.T) {
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
	if centers := routerInitialCentersV1(vectors, parent, 2, 0); len(centers) != 1 {
		t.Fatalf("initial centers=%d want 1 after the failed selection scan", len(centers))
	}
	cfg := DefaultRouterConfigV1()
	cfg.BranchFactor = 2
	cfg.LeafSize = 1
	cfg.MaxDepth = 1
	cfg.MaxIterations = 1
	cfg.RepresentativeBudget = 3
	work, ok := CheckedRouterScalarWorkV1([]int{2}, 2, cfg)
	if !ok || work != 36 {
		t.Fatalf("failed-selection router work=%d ok=%v want 36", work, ok)
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
		func(candidate *RouterModelV1) { candidate.Nodes[1].ParentNodeID = 0 },
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
		func(candidate *RouterModelV1) { candidate.Representatives[0].Values[1] *= .5 },
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
