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
	cfg.RepresentativesPerPartition = 2
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
	singleConfig.RepresentativesPerPartition = 1
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

func TestKMeansRepresentativeRouterRepairsEmptyClusters(t *testing.T) {
	cfg := routerTestConfigV1()
	cfg.RepresentativesPerPartition = 3
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
	if model.Metrics.EmptyRepairs == 0 {
		t.Fatal("expected deterministic empty-cluster repair")
	}
	if got := len(model.Representatives); got != 3 {
		t.Fatalf("representatives=%d want 3", got)
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
	cfg.RepresentativesPerPartition = 1
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

func TestCheckedRouterWorkUsesWidePairMultiplication(t *testing.T) {
	const vectors = 50_000
	work, ok := checkedRouterWorkV1(
		[][]routerBuildVectorV1{make([]routerBuildVectorV1, vectors)},
		1,
		RouterConfigV1{
			RepresentativesPerPartition: vectors,
			BranchFactor:                1,
			MaxIterations:               1,
		},
	)
	if !ok {
		t.Fatal("expected representable router work")
	}
	if want := int64(vectors) * int64(vectors); work != want {
		t.Fatalf("router work=%d want %d", work, want)
	}
}

func TestPartitionRouterModelValidationRejectsForgedMetadata(t *testing.T) {
	cfg := routerTestConfigV1()
	cfg.RepresentativesPerPartition = 2
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
			leafID := candidate.Representatives[0].LeafNodeID
			for i := range candidate.Nodes {
				if candidate.Nodes[i].NodeID == leafID {
					candidate.Nodes[i].MemberCount++
				}
			}
			candidate.Representatives[0].MemberCount++
		},
		func(candidate *RouterModelV1) {
			candidate.Representatives[1].SourceOrdinal = candidate.Representatives[0].SourceOrdinal
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
	cfg.RepresentativesPerPartition = 3
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
	if cfg.RepresentativesPerPartition != 16 {
		t.Fatalf("representatives per partition=%d want 16", cfg.RepresentativesPerPartition)
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
