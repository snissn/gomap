package collections

import (
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"testing"
)

type q2DenseGlobalRankPrepVariant struct {
	name     string
	prepare  func([]columnTypedColumnPhysicalQueryPart, *columnTypedColumnPhysicalQueryPrepareDiagnostics) error
	strategy func([]columnTypedColumnPhysicalQueryPart) columnTypedColumnDenseGroupCountDistinctRankStrategy
}

var q2DenseGlobalRankPrepVariants = []q2DenseGlobalRankPrepVariant{
	{
		name:    "current_map",
		prepare: prepareColumnTypedColumnDenseGroupCountDistinctGlobalRankMapsWithDiagnostics,
		strategy: func([]columnTypedColumnPhysicalQueryPart) columnTypedColumnDenseGroupCountDistinctRankStrategy {
			return columnTypedColumnDenseGroupCountDistinctRankStrategyCurrentMap
		},
	},
	{
		name:    "sharded_hash_rank",
		prepare: prepareColumnTypedColumnDenseGroupCountDistinctGlobalRankMapsShardedWithDiagnostics,
		strategy: func([]columnTypedColumnPhysicalQueryPart) columnTypedColumnDenseGroupCountDistinctRankStrategy {
			return columnTypedColumnDenseGroupCountDistinctRankStrategyShardedHash
		},
	},
	{
		name:     "adaptive",
		prepare:  prepareColumnTypedColumnDenseGroupCountDistinctGlobalRankMapsAdaptiveWithDiagnostics,
		strategy: q2DenseGlobalRankPrepAdaptiveStrategy,
	},
}

var q2DenseGlobalRankPrepBenchmarkVariants = []q2DenseGlobalRankPrepVariant{
	{
		name:    "current_map",
		prepare: prepareColumnTypedColumnDenseGroupCountDistinctGlobalRankMapsWithDiagnostics,
		strategy: func([]columnTypedColumnPhysicalQueryPart) columnTypedColumnDenseGroupCountDistinctRankStrategy {
			return columnTypedColumnDenseGroupCountDistinctRankStrategyCurrentMap
		},
	},
	{
		name:    "sharded_hash_rank",
		prepare: prepareColumnTypedColumnDenseGroupCountDistinctGlobalRankMapsShardedWithDiagnostics,
		strategy: func([]columnTypedColumnPhysicalQueryPart) columnTypedColumnDenseGroupCountDistinctRankStrategy {
			return columnTypedColumnDenseGroupCountDistinctRankStrategyShardedHash
		},
	},
	{
		name:     "adaptive_baseline",
		prepare:  prepareColumnTypedColumnDenseGroupCountDistinctGlobalRankMapsAdaptiveBaselineWithDiagnostics,
		strategy: q2DenseGlobalRankPrepAdaptiveStrategy,
	},
	{
		name:     "adaptive",
		prepare:  prepareColumnTypedColumnDenseGroupCountDistinctGlobalRankMapsAdaptiveWithDiagnostics,
		strategy: q2DenseGlobalRankPrepAdaptiveStrategy,
	},
}

func q2DenseGlobalRankPrepAdaptiveStrategy(parts []columnTypedColumnPhysicalQueryPart) columnTypedColumnDenseGroupCountDistinctRankStrategy {
	return columnTypedColumnDenseGroupCountDistinctSelectAdaptiveGlobalRankStrategy(parts, func(part *columnTypedColumnDenseGroupCountDistinctPart) *columnTypedColumnDenseStringCodeColumn {
		return &part.Distinct
	})
}

type q2DenseGlobalRankPrepDictionaryShape struct {
	name string
}

var q2DenseGlobalRankPrepDictionaryShapes = []q2DenseGlobalRankPrepDictionaryShape{
	{name: "mostly_disjoint"},
	{name: "shared_heavy"},
	{name: "mixed"},
}

var q2DenseGlobalRankPrepPartCounts = []int{40, 80, 160}

const (
	q2DenseGlobalRankPrepGroupValuesPerPart    = 128
	q2DenseGlobalRankPrepDistinctValuesPerPart = 2048
)

type q2DenseGlobalRankPrepFixtureStats struct {
	groupGlobalValues       int
	distinctGlobalValues    int
	groupLocalValues        int
	distinctLocalValues     int
	groupIncludesEmpty      bool
	distinctIncludesEmpty   bool
	totalLocalDictionaryLen int
}

func TestTypedColumnQ2DenseGroupCountDistinctGlobalRankPrepHarness(t *testing.T) {
	for _, partCount := range q2DenseGlobalRankPrepPartCounts {
		if partCount <= columnTypedColumnDenseGroupCountDistinctSortedMergeMaxParts {
			t.Fatalf("part count %d must force current map fallback above sorted merge max %d", partCount, columnTypedColumnDenseGroupCountDistinctSortedMergeMaxParts)
		}
	}

	for _, variant := range q2DenseGlobalRankPrepVariants {
		for _, shape := range q2DenseGlobalRankPrepDictionaryShapes {
			t.Run(variant.name+"/"+shape.name, func(t *testing.T) {
				parts := newQ2DenseGlobalRankPrepParts(40, shape)
				stats := q2DenseGlobalRankPrepStats(parts)
				var diagnostics columnTypedColumnPhysicalQueryPrepareDiagnostics
				if err := variant.prepare(parts, &diagnostics); err != nil {
					t.Fatalf("prepare q2 dense global rank maps: %v", err)
				}
				assertQ2DenseGlobalRankPrepParts(t, parts, stats)
			})
		}
	}
}

func TestTypedColumnQ2DenseGroupCountDistinctShardedHashRankPrepEquivalence(t *testing.T) {
	for _, shape := range q2DenseGlobalRankPrepDictionaryShapes {
		for _, unsorted := range []bool{false, true} {
			name := shape.name + "/sorted"
			if unsorted {
				name = shape.name + "/unsorted"
			}
			t.Run(name, func(t *testing.T) {
				baseFixture := newQ2DenseGlobalRankPrepParts(40, shape)
				if unsorted {
					unsortQ2DenseGlobalRankPrepDictionaries(baseFixture)
				}
				stats := q2DenseGlobalRankPrepStats(baseFixture)
				baseline := cloneQ2DenseGlobalRankPrepParts(baseFixture)
				prototype := cloneQ2DenseGlobalRankPrepParts(baseFixture)
				var baselineDiagnostics, prototypeDiagnostics columnTypedColumnPhysicalQueryPrepareDiagnostics
				if err := prepareColumnTypedColumnDenseGroupCountDistinctGlobalRankMapsWithDiagnostics(baseline, &baselineDiagnostics); err != nil {
					t.Fatalf("prepare current map fallback: %v", err)
				}
				if err := prepareColumnTypedColumnDenseGroupCountDistinctGlobalRankMapsShardedWithDiagnostics(prototype, &prototypeDiagnostics); err != nil {
					t.Fatalf("prepare sharded hash rank: %v", err)
				}
				assertQ2DenseGlobalRankPrepParts(t, baseline, stats)
				assertQ2DenseGlobalRankPrepParts(t, prototype, stats)
				assertQ2DenseGlobalRankPrepEquivalent(t, baseline, prototype, stats)
			})
		}
	}
}

func TestTypedColumnQ2DenseGroupCountDistinctAdaptiveRankPrepPolicy(t *testing.T) {
	for _, shape := range q2DenseGlobalRankPrepDictionaryShapes {
		t.Run(shape.name, func(t *testing.T) {
			parts := newQ2DenseGlobalRankPrepParts(40, shape)
			strategy := columnTypedColumnDenseGroupCountDistinctSelectAdaptiveGlobalRankStrategy(parts, func(part *columnTypedColumnDenseGroupCountDistinctPart) *columnTypedColumnDenseStringCodeColumn {
				return &part.Distinct
			})
			want := columnTypedColumnDenseGroupCountDistinctRankStrategyShardedHash
			if strategy != want {
				t.Fatalf("adaptive rank strategy=%v want %v", strategy, want)
			}

			baseline := cloneQ2DenseGlobalRankPrepParts(parts)
			adaptive := cloneQ2DenseGlobalRankPrepParts(parts)
			stats := q2DenseGlobalRankPrepStats(parts)
			if err := prepareColumnTypedColumnDenseGroupCountDistinctGlobalRankMapsWithDiagnostics(baseline, nil); err != nil {
				t.Fatalf("prepare current map fallback: %v", err)
			}
			if err := prepareColumnTypedColumnDenseGroupCountDistinctGlobalRankMapsAdaptiveWithDiagnostics(adaptive, nil); err != nil {
				t.Fatalf("prepare adaptive rank: %v", err)
			}
			assertQ2DenseGlobalRankPrepParts(t, adaptive, stats)
			assertQ2DenseGlobalRankPrepEquivalent(t, baseline, adaptive, stats)
		})
	}
}

func TestTypedColumnQ2DenseGroupCountDistinctAdaptiveRankPrepPolicySerialSharedFallback(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldProcs)

	parts := newQ2DenseGlobalRankPrepParts(40, q2DenseGlobalRankPrepDictionaryShape{name: "shared_heavy"})
	strategy := columnTypedColumnDenseGroupCountDistinctSelectAdaptiveGlobalRankStrategy(parts, func(part *columnTypedColumnDenseGroupCountDistinctPart) *columnTypedColumnDenseStringCodeColumn {
		return &part.Distinct
	})
	if strategy != columnTypedColumnDenseGroupCountDistinctRankStrategyCurrentMap {
		t.Fatalf("serial shared adaptive rank strategy=%v want %v", strategy, columnTypedColumnDenseGroupCountDistinctRankStrategyCurrentMap)
	}
}

func TestTypedColumnQ2DenseGroupCountDistinctOneShotRankPrepMatchesAdaptivePolicy(t *testing.T) {
	for _, shape := range q2DenseGlobalRankPrepDictionaryShapes {
		t.Run(shape.name, func(t *testing.T) {
			fixture := newQ2DenseGlobalRankPrepParts(40, shape)
			unsortQ2DenseGlobalRankPrepDictionaries(fixture)
			stats := q2DenseGlobalRankPrepStats(fixture)
			oneShot := cloneQ2DenseGlobalRankPrepParts(fixture)
			adaptive := cloneQ2DenseGlobalRankPrepParts(fixture)
			current := cloneQ2DenseGlobalRankPrepParts(fixture)

			if err := prepareColumnTypedColumnDenseGroupCountDistinctGlobalRankMapsOneShotWithDiagnostics(oneShot, nil); err != nil {
				t.Fatalf("prepare one-shot adaptive rank: %v", err)
			}
			if err := prepareColumnTypedColumnDenseGroupCountDistinctGlobalRankMapsAdaptiveWithDiagnostics(adaptive, nil); err != nil {
				t.Fatalf("prepare explicit adaptive rank: %v", err)
			}
			if err := prepareColumnTypedColumnDenseGroupCountDistinctGlobalRankMapsWithDiagnostics(current, nil); err != nil {
				t.Fatalf("prepare current map fallback: %v", err)
			}

			assertQ2DenseGlobalRankPrepParts(t, oneShot, stats)
			assertQ2DenseGlobalRankPrepEquivalent(t, adaptive, oneShot, stats)
			if !q2DenseGlobalRankPrepExactDistinctRanksEqual(oneShot, adaptive) {
				t.Fatalf("one-shot rank prep differs from adaptive policy")
			}
			strategy := columnTypedColumnDenseGroupCountDistinctSelectAdaptiveGlobalRankStrategy(fixture, func(part *columnTypedColumnDenseGroupCountDistinctPart) *columnTypedColumnDenseStringCodeColumn {
				return &part.Distinct
			})
			if strategy == columnTypedColumnDenseGroupCountDistinctRankStrategyShardedHash && q2DenseGlobalRankPrepExactDistinctRanksEqual(oneShot, current) {
				t.Fatalf("one-shot rank prep used current-map ranks for adaptive sharded shape %s", shape.name)
			}
		})
	}
}

func TestTypedColumnQ2DenseGroupCountDistinctShardedReferenceFillNullableEmpty(t *testing.T) {
	parts := []columnTypedColumnPhysicalQueryPart{
		{
			DenseGroupCountDistinct: &columnTypedColumnDenseGroupCountDistinctPart{
				Group: columnTypedColumnDenseStringCodeColumn{
					Dictionary: []string{"app.a"},
				},
				Distinct: columnTypedColumnDenseStringCodeColumn{
					Dictionary: []string{"did:a", "did:shared"},
					Valid:      []bool{true, false},
				},
			},
		},
		{
			DenseGroupCountDistinct: &columnTypedColumnDenseGroupCountDistinctPart{
				Group: columnTypedColumnDenseStringCodeColumn{
					Dictionary: []string{"app.b"},
				},
				Distinct: columnTypedColumnDenseStringCodeColumn{
					Dictionary: []string{"did:b", "did:shared"},
				},
			},
		},
	}
	expectedDistinctRankRefs := 0
	expectedDistinctValues := make(map[string]struct{})
	expectedDistinctEmptyRank := false
	for _, part := range parts {
		distinct := part.DenseGroupCountDistinct.Distinct
		expectedDistinctRankRefs += len(distinct.Dictionary)
		for localCode, value := range distinct.Dictionary {
			if distinct.Valid != nil && localCode < len(distinct.Valid) && !distinct.Valid[localCode] {
				expectedDistinctEmptyRank = true
				continue
			}
			expectedDistinctValues[value] = struct{}{}
		}
	}
	expectedDistinctGlobalRanks := len(expectedDistinctValues)
	if expectedDistinctEmptyRank {
		expectedDistinctGlobalRanks++
	}

	var diagnostics columnTypedColumnPhysicalQueryPrepareDiagnostics
	if err := prepareColumnTypedColumnDenseGroupCountDistinctGlobalRankMapsShardedWithDiagnostics(parts, &diagnostics); err != nil {
		t.Fatalf("prepare sharded reference-fill rank maps: %v", err)
	}
	if diagnostics.Q2DenseDistinctGlobalRankNanos != diagnostics.Q2DistinctRankNanos ||
		diagnostics.Q2DensePartLocalRankNanos != diagnostics.Q2LocalRankNanos {
		t.Fatalf("reference-fill diagnostics=%+v want dense q2 distinct/local split", diagnostics)
	}
	distinctSubphaseTotal := diagnostics.Q2DenseDistinctRankPlanNanos +
		diagnostics.Q2DenseDistinctRankCollectRefsNanos +
		diagnostics.Q2DenseDistinctRankBuildShardsNanos
	if distinctSubphaseTotal != diagnostics.Q2DenseDistinctGlobalRankNanos {
		t.Fatalf("reference-fill distinct rank subphases=%d want aggregate=%d diagnostics=%+v", distinctSubphaseTotal, diagnostics.Q2DenseDistinctGlobalRankNanos, diagnostics)
	}
	if diagnostics.Q2DenseDistinctRankShardCount == 0 ||
		diagnostics.Q2DenseDistinctRankRefs != expectedDistinctRankRefs ||
		diagnostics.Q2DenseDistinctRankMaxShardRefs == 0 ||
		diagnostics.Q2DenseDistinctGlobalRanks != expectedDistinctGlobalRanks {
		t.Fatalf("reference-fill count diagnostics=%+v want shard count, %d refs, max shard refs, %d global ranks", diagnostics, expectedDistinctRankRefs, expectedDistinctGlobalRanks)
	}
	distinct0 := parts[0].DenseGroupCountDistinct.Distinct
	distinct1 := parts[1].DenseGroupCountDistinct.Distinct
	if distinct0.GlobalDictionary != nil || distinct1.GlobalDictionary != nil {
		t.Fatalf("distinct global dictionary allocated part0=%v part1=%v", distinct0.GlobalDictionary, distinct1.GlobalDictionary)
	}
	if !distinct0.GlobalCardinalityOK || distinct0.GlobalCardinality != expectedDistinctGlobalRanks || !distinct1.GlobalCardinalityOK || distinct1.GlobalCardinality != expectedDistinctGlobalRanks {
		t.Fatalf("distinct cardinality part0=(%d,%t) part1=(%d,%t) want (%d,true)", distinct0.GlobalCardinality, distinct0.GlobalCardinalityOK, distinct1.GlobalCardinality, distinct1.GlobalCardinalityOK, expectedDistinctGlobalRanks)
	}
	if !distinct0.GlobalEmptyRankOK || !distinct1.GlobalEmptyRankOK || distinct0.GlobalEmptyRank != distinct1.GlobalEmptyRank || distinct0.GlobalEmptyRank >= uint32(expectedDistinctGlobalRanks) {
		t.Fatalf("distinct empty ranks part0=(%d,%t) part1=(%d,%t) want shared rank below cardinality", distinct0.GlobalEmptyRank, distinct0.GlobalEmptyRankOK, distinct1.GlobalEmptyRank, distinct1.GlobalEmptyRankOK)
	}
	rankByValue := func(column columnTypedColumnDenseStringCodeColumn, value string) (uint32, bool) {
		for localCode, localValue := range column.Dictionary {
			if localValue == value {
				return column.GlobalLocalRanks[localCode], true
			}
		}
		return 0, false
	}
	shared0, ok := rankByValue(distinct0, "did:shared")
	if !ok || shared0 >= uint32(expectedDistinctGlobalRanks) {
		t.Fatalf("part 0 shared rank=(%d,%t) cardinality=%d ranks=%v", shared0, ok, expectedDistinctGlobalRanks, distinct0.GlobalLocalRanks)
	}
	shared1, ok := rankByValue(distinct1, "did:shared")
	if !ok || shared1 >= uint32(expectedDistinctGlobalRanks) {
		t.Fatalf("part 1 shared rank=(%d,%t) cardinality=%d ranks=%v", shared1, ok, expectedDistinctGlobalRanks, distinct1.GlobalLocalRanks)
	}
	if shared0 != shared1 {
		t.Fatalf("shared did ranks part0=%d part1=%d want equal", shared0, shared1)
	}
	for label, rank := range map[string]uint32{"did:a": distinct0.GlobalLocalRanks[0], "did:b": distinct1.GlobalLocalRanks[0], "did:shared": shared0, "empty": distinct0.GlobalEmptyRank} {
		if rank >= uint32(expectedDistinctGlobalRanks) {
			t.Fatalf("%s rank=%d outside cardinality %d", label, rank, expectedDistinctGlobalRanks)
		}
	}
}

func TestTypedColumnQ2DenseGroupCountDistinctHashRankShardCollision(t *testing.T) {
	shard := newColumnTypedColumnDenseGroupCountDistinctHashRankShard(3)
	const hash = uint32(42)
	first, err := shard.addHash(hash, "did:first")
	if err != nil {
		t.Fatalf("add first: %v", err)
	}
	second, err := shard.addHash(hash, "did:second")
	if err != nil {
		t.Fatalf("add second: %v", err)
	}
	firstAgain, err := shard.addHash(hash, "did:first")
	if err != nil {
		t.Fatalf("add first again: %v", err)
	}
	secondAgain, ok := shard.lookupHash(hash, "did:second")
	if !ok {
		t.Fatalf("lookup second collision failed")
	}
	if first != 0 || second != 1 || firstAgain != first || secondAgain != second {
		t.Fatalf("collision ranks first=%d second=%d firstAgain=%d secondAgain=%d", first, second, firstAgain, secondAgain)
	}
	if _, ok := shard.lookupHash(hash, "did:third"); ok {
		t.Fatalf("lookup returned rank for missing colliding value")
	}
	if got := shard.cardinality(); got != 2 {
		t.Fatalf("cardinality=%d want 2", got)
	}
}

func TestTypedColumnQ2DenseGroupCountDistinctShardRankRefsParallelMatchesSerial(t *testing.T) {
	newPart := func(dictionary []string, valid []bool) columnTypedColumnPhysicalQueryPart {
		return columnTypedColumnPhysicalQueryPart{
			DenseGroupCountDistinct: &columnTypedColumnDenseGroupCountDistinctPart{
				Distinct: columnTypedColumnDenseStringCodeColumn{
					Dictionary: dictionary,
					Valid:      valid,
				},
			},
		}
	}
	parts := []columnTypedColumnPhysicalQueryPart{
		newPart([]string{"did:shared", "", "did:a"}, []bool{true, false, true}),
		newPart([]string{"did:b", "did:shared"}, nil),
		newPart([]string{"did:c", "did:b", "did:c2"}, []bool{true, true, false}),
		newPart([]string{"", "did:d"}, nil),
		newPart([]string{"did:e", "did:shared", "did:a"}, []bool{false, true, true}),
	}
	capacity := 0
	for partIdx := range parts {
		capacity += len(parts[partIdx].DenseGroupCountDistinct.Distinct.Dictionary)
	}
	shardCount := 8
	serialRefs, err := columnTypedColumnDenseGroupCountDistinctCollectShardRankRefsSerial(parts, shardCount, capacity)
	if err != nil {
		t.Fatalf("collect serial refs: %v", err)
	}
	parallelRefs, err := columnTypedColumnDenseGroupCountDistinctCollectShardRankRefs(parts, shardCount, capacity, 3)
	if err != nil {
		t.Fatalf("collect parallel refs: %v", err)
	}
	if !reflect.DeepEqual(parallelRefs, serialRefs) {
		t.Fatalf("parallel refs mismatch\nserial=%v\nparallel=%v", serialRefs, parallelRefs)
	}
	for shardIdx, refs := range parallelRefs {
		for refIdx := 1; refIdx < len(refs); refIdx++ {
			if refs[refIdx].packed() < refs[refIdx-1].packed() {
				t.Fatalf("shard %d refs not monotonic at %d: %d < %d", shardIdx, refIdx, refs[refIdx].packed(), refs[refIdx-1].packed())
			}
		}
	}
	wantRanges := []columnTypedColumnDenseGroupCountDistinctShardRankRefWorkerRange{
		{start: 0, end: 2},
		{start: 2, end: 4},
		{start: 4, end: 5},
	}
	if got := columnTypedColumnDenseGroupCountDistinctShardRankRefWorkerRanges(len(parts), 3); !reflect.DeepEqual(got, wantRanges) {
		t.Fatalf("worker ranges=%+v want %+v", got, wantRanges)
	}
}

// Dense q2 distinct ranks are reducer-local equality IDs, not externally visible
// lexical order. This verifies the q2-visible result invariant after rank renumbering.
func TestTypedColumnQ2DenseGroupCountDistinctShardedHashRankRunEquivalence(t *testing.T) {
	req := ColumnPhysicalQueryRequest{
		Kind:           ColumnPhysicalQueryGroupCountAndDistinct,
		GroupColumn:    "collection",
		DistinctColumn: "did",
	}
	fixture := newQ2DenseGlobalRankPrepParts(40, q2DenseGlobalRankPrepDictionaryShape{name: "mixed"})
	unsortQ2DenseGlobalRankPrepDictionaries(fixture)
	populateQ2DenseGlobalRankPrepCodes(fixture, 512)
	baseline := cloneQ2DenseGlobalRankPrepParts(fixture)
	prototype := cloneQ2DenseGlobalRankPrepParts(fixture)
	if err := prepareColumnTypedColumnDenseGroupCountDistinctGlobalRankMapsWithDiagnostics(baseline, nil); err != nil {
		t.Fatalf("prepare current map fallback: %v", err)
	}
	if err := prepareColumnTypedColumnDenseGroupCountDistinctGlobalRankMapsShardedWithDiagnostics(prototype, nil); err != nil {
		t.Fatalf("prepare sharded hash rank: %v", err)
	}
	baselineRunner := &columnTypedColumnPhysicalQueryRunner{
		plan: columnTypedColumnPhysicalQueryPlan{
			ProjectedColumns:        []string{"collection", "did"},
			PredicateDiagnostics:    newColumnPhysicalQueryPredicateDiagnosticPlan(req),
			DenseGroupCountDistinct: true,
		},
		parts: baseline,
	}
	prototypeRunner := &columnTypedColumnPhysicalQueryRunner{
		plan: columnTypedColumnPhysicalQueryPlan{
			ProjectedColumns:        []string{"collection", "did"},
			PredicateDiagnostics:    newColumnPhysicalQueryPredicateDiagnosticPlan(req),
			DenseGroupCountDistinct: true,
		},
		parts: prototype,
	}
	baselineResult, err := baselineRunner.runDenseGroupCountDistinct(columnPhysicalScanSnapshotView{}, req)
	if err != nil {
		t.Fatalf("run current map fallback: %v", err)
	}
	prototypeResult, err := prototypeRunner.runDenseGroupCountDistinct(columnPhysicalScanSnapshotView{}, req)
	if err != nil {
		t.Fatalf("run sharded hash rank: %v", err)
	}
	baselineCounts := columnPhysicalJSONBenchQ2CountsP0(baselineResult.Groups)
	prototypeCounts := columnPhysicalJSONBenchQ2CountsP0(prototypeResult.Groups)
	if !reflect.DeepEqual(prototypeCounts, baselineCounts) {
		t.Fatalf("prototype counts=%v want current map fallback %v", prototypeCounts, baselineCounts)
	}
}

func TestTypedColumnQ2DenseGroupCountDistinctAdaptiveRankRunEquivalence(t *testing.T) {
	req := ColumnPhysicalQueryRequest{
		Kind:           ColumnPhysicalQueryGroupCountAndDistinct,
		GroupColumn:    "collection",
		DistinctColumn: "did",
	}
	for _, shape := range q2DenseGlobalRankPrepDictionaryShapes {
		t.Run(shape.name, func(t *testing.T) {
			fixture := newQ2DenseGlobalRankPrepParts(40, shape)
			unsortQ2DenseGlobalRankPrepDictionaries(fixture)
			populateQ2DenseGlobalRankPrepCodes(fixture, 512)
			baseline := cloneQ2DenseGlobalRankPrepParts(fixture)
			adaptive := cloneQ2DenseGlobalRankPrepParts(fixture)
			if err := prepareColumnTypedColumnDenseGroupCountDistinctGlobalRankMapsWithDiagnostics(baseline, nil); err != nil {
				t.Fatalf("prepare current map fallback: %v", err)
			}
			if err := prepareColumnTypedColumnDenseGroupCountDistinctGlobalRankMapsAdaptiveWithDiagnostics(adaptive, nil); err != nil {
				t.Fatalf("prepare adaptive rank: %v", err)
			}
			baselineRunner := &columnTypedColumnPhysicalQueryRunner{
				plan: columnTypedColumnPhysicalQueryPlan{
					ProjectedColumns:        []string{"collection", "did"},
					PredicateDiagnostics:    newColumnPhysicalQueryPredicateDiagnosticPlan(req),
					DenseGroupCountDistinct: true,
				},
				parts: baseline,
			}
			adaptiveRunner := &columnTypedColumnPhysicalQueryRunner{
				plan: columnTypedColumnPhysicalQueryPlan{
					ProjectedColumns:        []string{"collection", "did"},
					PredicateDiagnostics:    newColumnPhysicalQueryPredicateDiagnosticPlan(req),
					DenseGroupCountDistinct: true,
				},
				parts: adaptive,
			}
			baselineResult, err := baselineRunner.runDenseGroupCountDistinct(columnPhysicalScanSnapshotView{}, req)
			if err != nil {
				t.Fatalf("run current map fallback: %v", err)
			}
			adaptiveResult, err := adaptiveRunner.runDenseGroupCountDistinct(columnPhysicalScanSnapshotView{}, req)
			if err != nil {
				t.Fatalf("run adaptive rank: %v", err)
			}
			baselineCounts := columnPhysicalJSONBenchQ2CountsP0(baselineResult.Groups)
			adaptiveCounts := columnPhysicalJSONBenchQ2CountsP0(adaptiveResult.Groups)
			if !reflect.DeepEqual(adaptiveCounts, baselineCounts) {
				t.Fatalf("adaptive counts=%v want current map fallback %v", adaptiveCounts, baselineCounts)
			}
		})
	}
}

func BenchmarkTypedColumnQ2DenseGroupCountDistinctGlobalRankPrep(b *testing.B) {
	for _, variant := range q2DenseGlobalRankPrepBenchmarkVariants {
		for _, shape := range q2DenseGlobalRankPrepDictionaryShapes {
			for _, partCount := range q2DenseGlobalRankPrepPartCounts {
				b.Run(fmt.Sprintf("%s/%s/parts=%d", variant.name, shape.name, partCount), func(b *testing.B) {
					fixture := newQ2DenseGlobalRankPrepParts(partCount, shape)
					stats := q2DenseGlobalRankPrepStats(fixture)
					currentStrategy, shardedStrategy := q2DenseGlobalRankPrepStrategyMetrics(variant.strategy(fixture))
					var diagnostics columnTypedColumnPhysicalQueryPrepareDiagnostics
					b.ReportAllocs()

					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						b.StopTimer()
						parts := cloneQ2DenseGlobalRankPrepParts(fixture)
						b.StartTimer()
						if err := variant.prepare(parts, &diagnostics); err != nil {
							b.Fatalf("prepare q2 dense global rank maps: %v", err)
						}
					}
					b.StopTimer()

					if b.N > 0 {
						b.ReportMetric(float64(partCount), "parts/op")
						b.ReportMetric(float64(stats.groupGlobalValues), "group_global_values/op")
						b.ReportMetric(float64(stats.distinctGlobalValues), "distinct_global_values/op")
						b.ReportMetric(float64(stats.totalLocalDictionaryLen), "local_dictionary_values/op")
						b.ReportMetric(float64(diagnostics.Q2GroupRankNanos)/float64(b.N), "diag_group_rank_ns/op")
						b.ReportMetric(float64(diagnostics.Q2DistinctRankNanos)/float64(b.N), "diag_distinct_rank_ns/op")
						b.ReportMetric(float64(diagnostics.Q2LocalRankNanos)/float64(b.N), "diag_local_rank_ns/op")
						b.ReportMetric(float64(diagnostics.Q2DenseDistinctRankPlanNanos)/float64(b.N), "diag_dense_distinct_rank_plan_ns/op")
						b.ReportMetric(float64(diagnostics.Q2DenseDistinctRankCollectRefsNanos)/float64(b.N), "diag_dense_distinct_rank_collect_refs_ns/op")
						b.ReportMetric(float64(diagnostics.Q2DenseDistinctRankBuildShardsNanos)/float64(b.N), "diag_dense_distinct_rank_build_shards_ns/op")
						b.ReportMetric(float64(diagnostics.Q2DenseDistinctRankShardCount), "diag_dense_distinct_rank_shards/op")
						b.ReportMetric(float64(diagnostics.Q2DenseDistinctRankRefs)/float64(b.N), "diag_dense_distinct_rank_refs/op")
						b.ReportMetric(float64(diagnostics.Q2DenseDistinctRankMaxShardRefs), "diag_dense_distinct_rank_max_shard_refs/op")
						b.ReportMetric(float64(diagnostics.Q2DenseDistinctGlobalRanks), "diag_dense_distinct_global_ranks/op")
						b.ReportMetric(currentStrategy, "current_strategy/op")
						b.ReportMetric(shardedStrategy, "sharded_strategy/op")
					}
				})
			}
		}
	}
}

func q2DenseGlobalRankPrepStrategyMetrics(strategy columnTypedColumnDenseGroupCountDistinctRankStrategy) (float64, float64) {
	switch strategy {
	case columnTypedColumnDenseGroupCountDistinctRankStrategyCurrentMap:
		return 1, 0
	case columnTypedColumnDenseGroupCountDistinctRankStrategyShardedHash:
		return 0, 1
	default:
		panic(fmt.Sprintf("unknown q2 dense global rank prep strategy %d", strategy))
	}
}

func newQ2DenseGlobalRankPrepParts(partCount int, shape q2DenseGlobalRankPrepDictionaryShape) []columnTypedColumnPhysicalQueryPart {
	parts := make([]columnTypedColumnPhysicalQueryPart, partCount)
	for partIdx := range parts {
		groupDict := q2DenseGlobalRankPrepDictionary(shape, "group", partIdx, q2DenseGlobalRankPrepGroupValuesPerPart)
		distinctDict := q2DenseGlobalRankPrepDictionary(shape, "distinct", partIdx, q2DenseGlobalRankPrepDistinctValuesPerPart)
		parts[partIdx] = columnTypedColumnPhysicalQueryPart{
			Rows: q2DenseGlobalRankPrepDistinctValuesPerPart,
			DenseGroupCountDistinct: &columnTypedColumnDenseGroupCountDistinctPart{
				Rows:     q2DenseGlobalRankPrepDistinctValuesPerPart,
				Group:    q2DenseGlobalRankPrepColumn(groupDict, q2DenseGlobalRankPrepValidity(partIdx, 17)),
				Distinct: q2DenseGlobalRankPrepColumn(distinctDict, q2DenseGlobalRankPrepValidity(partIdx, 19)),
			},
		}
	}
	return parts
}

func cloneQ2DenseGlobalRankPrepParts(parts []columnTypedColumnPhysicalQueryPart) []columnTypedColumnPhysicalQueryPart {
	clone := make([]columnTypedColumnPhysicalQueryPart, len(parts))
	for partIdx := range parts {
		clone[partIdx].Rows = parts[partIdx].Rows
		src := parts[partIdx].DenseGroupCountDistinct
		if src == nil {
			continue
		}
		dst := *src
		dst.Group = cloneQ2DenseGlobalRankPrepColumn(src.Group)
		dst.Distinct = cloneQ2DenseGlobalRankPrepColumn(src.Distinct)
		clone[partIdx].DenseGroupCountDistinct = &dst
	}
	return clone
}

func cloneQ2DenseGlobalRankPrepColumn(column columnTypedColumnDenseStringCodeColumn) columnTypedColumnDenseStringCodeColumn {
	return columnTypedColumnDenseStringCodeColumn{
		Dictionary:      append([]string(nil), column.Dictionary...),
		Valid:           append([]bool(nil), column.Valid...),
		HasMissing:      column.HasMissing,
		HasMissingKnown: column.HasMissingKnown,
		Codes:           append([]uint32(nil), column.Codes...),
	}
}

func unsortQ2DenseGlobalRankPrepDictionaries(parts []columnTypedColumnPhysicalQueryPart) {
	for partIdx := range parts {
		part := parts[partIdx].DenseGroupCountDistinct
		if part == nil {
			continue
		}
		reverseQ2DenseGlobalRankPrepStrings(part.Group.Dictionary)
		if partIdx%2 == 0 && len(part.Group.Dictionary) > 1 {
			first := part.Group.Dictionary[0]
			copy(part.Group.Dictionary, part.Group.Dictionary[1:])
			part.Group.Dictionary[len(part.Group.Dictionary)-1] = first
		}
		reverseQ2DenseGlobalRankPrepStrings(part.Distinct.Dictionary)
		if partIdx%3 == 0 && len(part.Distinct.Dictionary) > 1 {
			last := part.Distinct.Dictionary[len(part.Distinct.Dictionary)-1]
			copy(part.Distinct.Dictionary[1:], part.Distinct.Dictionary)
			part.Distinct.Dictionary[0] = last
		}
	}
}

func reverseQ2DenseGlobalRankPrepStrings(values []string) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

func populateQ2DenseGlobalRankPrepCodes(parts []columnTypedColumnPhysicalQueryPart, rows int) {
	for partIdx := range parts {
		part := parts[partIdx].DenseGroupCountDistinct
		if part == nil {
			continue
		}
		part.Rows = rows
		parts[partIdx].Rows = rows
		part.Group.Codes = make([]uint32, rows)
		part.Distinct.Codes = make([]uint32, rows)
		for row := 0; row < rows; row++ {
			part.Group.Codes[row] = uint32((row + partIdx) % len(part.Group.Dictionary))
			part.Distinct.Codes[row] = uint32((row*31 + partIdx) % len(part.Distinct.Dictionary))
		}
		if partIdx%17 == 0 {
			part.Group.Valid = q2DenseGlobalRankPrepFullValidity(rows)
			part.Group.Valid[(partIdx*7)%rows] = false
		} else {
			part.Group.Valid = nil
		}
		if partIdx%19 == 0 {
			part.Distinct.Valid = q2DenseGlobalRankPrepFullValidity(rows)
			part.Distinct.Valid[(partIdx*11+3)%rows] = false
		} else {
			part.Distinct.Valid = nil
		}
	}
}

func q2DenseGlobalRankPrepFullValidity(rows int) []bool {
	valid := make([]bool, rows)
	for idx := range valid {
		valid[idx] = true
	}
	return valid
}

func q2DenseGlobalRankPrepDictionary(shape q2DenseGlobalRankPrepDictionaryShape, role string, partIdx, valuesPerPart int) []string {
	sharedValues := q2DenseGlobalRankPrepSharedValues(shape, valuesPerPart)
	uniqueValues := valuesPerPart - sharedValues
	sharedPool := q2DenseGlobalRankPrepSharedPool(shape, valuesPerPart)
	values := make([]string, 0, valuesPerPart)
	for valueIdx := 0; valueIdx < sharedValues; valueIdx++ {
		sharedIdx := valueIdx
		if shape.name == "mixed" {
			sharedIdx = (partIdx*(sharedValues/2+1) + valueIdx) % sharedPool
		}
		values = append(values, fmt.Sprintf("q2_%s_shared_%06d", role, sharedIdx))
	}
	for valueIdx := 0; valueIdx < uniqueValues; valueIdx++ {
		values = append(values, fmt.Sprintf("q2_%s_part_%03d_unique_%06d", role, partIdx, valueIdx))
	}
	sort.Strings(values)
	return values
}

func q2DenseGlobalRankPrepSharedValues(shape q2DenseGlobalRankPrepDictionaryShape, valuesPerPart int) int {
	switch shape.name {
	case "mostly_disjoint":
		return max(1, valuesPerPart/32)
	case "shared_heavy":
		return valuesPerPart - max(1, valuesPerPart/16)
	case "mixed":
		return valuesPerPart / 2
	default:
		panic(fmt.Sprintf("unknown q2 dense global rank prep shape %q", shape.name))
	}
}

func q2DenseGlobalRankPrepSharedPool(shape q2DenseGlobalRankPrepDictionaryShape, valuesPerPart int) int {
	switch shape.name {
	case "mostly_disjoint", "shared_heavy":
		return max(1, q2DenseGlobalRankPrepSharedValues(shape, valuesPerPart))
	case "mixed":
		return max(1, valuesPerPart*2)
	default:
		panic(fmt.Sprintf("unknown q2 dense global rank prep shape %q", shape.name))
	}
}

func q2DenseGlobalRankPrepValidity(partIdx, period int) []bool {
	if partIdx%period != 0 {
		return nil
	}
	valid := make([]bool, 8)
	for idx := range valid {
		valid[idx] = true
	}
	valid[partIdx%len(valid)] = false
	return valid
}

func q2DenseGlobalRankPrepColumn(dictionary []string, valid []bool) columnTypedColumnDenseStringCodeColumn {
	return columnTypedColumnDenseStringCodeColumn{
		Dictionary:      dictionary,
		Valid:           valid,
		HasMissing:      columnTypedColumnDenseValidityHasMissing(valid),
		HasMissingKnown: true,
	}
}

func q2DenseGlobalRankPrepStats(parts []columnTypedColumnPhysicalQueryPart) q2DenseGlobalRankPrepFixtureStats {
	groupValues := make(map[string]struct{})
	distinctValues := make(map[string]struct{})
	var stats q2DenseGlobalRankPrepFixtureStats
	for partIdx := range parts {
		part := parts[partIdx].DenseGroupCountDistinct
		if part == nil {
			panic(fmt.Sprintf("missing q2 dense global rank prep part %d", partIdx))
		}
		q2DenseGlobalRankPrepColumnStats(&part.Group, groupValues, &stats.groupLocalValues, &stats.groupIncludesEmpty)
		q2DenseGlobalRankPrepColumnStats(&part.Distinct, distinctValues, &stats.distinctLocalValues, &stats.distinctIncludesEmpty)
	}
	stats.groupGlobalValues = len(groupValues)
	stats.distinctGlobalValues = len(distinctValues)
	stats.totalLocalDictionaryLen = stats.groupLocalValues + stats.distinctLocalValues
	return stats
}

func q2DenseGlobalRankPrepColumnStats(column *columnTypedColumnDenseStringCodeColumn, values map[string]struct{}, localValues *int, includesEmpty *bool) {
	for _, value := range column.Dictionary {
		values[value] = struct{}{}
	}
	*localValues += len(column.Dictionary)
	for _, valid := range column.Valid {
		if !valid {
			values[""] = struct{}{}
			*includesEmpty = true
			break
		}
	}
}

func assertQ2DenseGlobalRankPrepParts(t *testing.T, parts []columnTypedColumnPhysicalQueryPart, stats q2DenseGlobalRankPrepFixtureStats) {
	t.Helper()
	for partIdx := range parts {
		part := parts[partIdx].DenseGroupCountDistinct
		if part == nil {
			t.Fatalf("part %d missing dense q2 prep", partIdx)
		}
		if got := len(part.Group.GlobalDictionary); got != stats.groupGlobalValues {
			t.Fatalf("part %d group global dictionary len=%d want %d", partIdx, got, stats.groupGlobalValues)
		}
		assertQ2DenseGlobalRankPrepDictionaryOrder(t, partIdx, "group", part.Group.GlobalDictionary)
		if !part.Group.GlobalCardinalityOK || part.Group.GlobalCardinality != stats.groupGlobalValues {
			t.Fatalf("part %d group global cardinality=(%d,%t) want (%d,true)", partIdx, part.Group.GlobalCardinality, part.Group.GlobalCardinalityOK, stats.groupGlobalValues)
		}
		if got := len(part.Group.GlobalLocalRanks); got != len(part.Group.Dictionary) {
			t.Fatalf("part %d group local ranks=%d want dictionary len %d", partIdx, got, len(part.Group.Dictionary))
		}
		assertQ2DenseGlobalRankPrepLocalRanks(t, partIdx, "group", part.Group.Dictionary, part.Group.GlobalLocalRanks, part.Group.GlobalDictionary)
		if part.Group.GlobalEmptyRankOK != stats.groupIncludesEmpty {
			t.Fatalf("part %d group empty rank ok=%t want %t", partIdx, part.Group.GlobalEmptyRankOK, stats.groupIncludesEmpty)
		}
		if part.Group.GlobalEmptyRankOK && (uint64(part.Group.GlobalEmptyRank) >= uint64(len(part.Group.GlobalDictionary)) || part.Group.GlobalDictionary[part.Group.GlobalEmptyRank] != "") {
			t.Fatalf("part %d group empty rank=%d dictionary prefix=%v", partIdx, part.Group.GlobalEmptyRank, part.Group.GlobalDictionary[:min(len(part.Group.GlobalDictionary), 4)])
		}
		if part.Distinct.GlobalDictionary != nil {
			t.Fatalf("part %d distinct global dictionary allocated len=%d", partIdx, len(part.Distinct.GlobalDictionary))
		}
		if !part.Distinct.GlobalCardinalityOK || part.Distinct.GlobalCardinality != stats.distinctGlobalValues {
			t.Fatalf("part %d distinct global cardinality=(%d,%t) want (%d,true)", partIdx, part.Distinct.GlobalCardinality, part.Distinct.GlobalCardinalityOK, stats.distinctGlobalValues)
		}
		if got := len(part.Distinct.GlobalLocalRanks); got != len(part.Distinct.Dictionary) {
			t.Fatalf("part %d distinct local ranks=%d want dictionary len %d", partIdx, got, len(part.Distinct.Dictionary))
		}
		assertQ2DenseGlobalRankPrepDistinctRanks(t, partIdx, part.Distinct.Dictionary, part.Distinct.GlobalLocalRanks, part.Distinct.GlobalCardinality)
		if part.Distinct.GlobalEmptyRankOK != stats.distinctIncludesEmpty {
			t.Fatalf("part %d distinct empty rank ok=%t want %t", partIdx, part.Distinct.GlobalEmptyRankOK, stats.distinctIncludesEmpty)
		}
		if part.Distinct.GlobalEmptyRankOK && uint64(part.Distinct.GlobalEmptyRank) >= uint64(part.Distinct.GlobalCardinality) {
			t.Fatalf("part %d distinct empty rank=%d cardinality=%d", partIdx, part.Distinct.GlobalEmptyRank, part.Distinct.GlobalCardinality)
		}
	}
}

func assertQ2DenseGlobalRankPrepDictionaryOrder(t *testing.T, partIdx int, role string, dictionary []string) {
	t.Helper()
	for idx := 1; idx < len(dictionary); idx++ {
		if dictionary[idx-1] >= dictionary[idx] {
			t.Fatalf("part %d %s global dictionary not sorted at %d: %q >= %q", partIdx, role, idx, dictionary[idx-1], dictionary[idx])
		}
	}
}

func assertQ2DenseGlobalRankPrepLocalRanks(t *testing.T, partIdx int, role string, localDictionary []string, localRanks []uint32, globalDictionary []string) {
	t.Helper()
	for localCode, value := range localDictionary {
		rank := localRanks[localCode]
		if uint64(rank) >= uint64(len(globalDictionary)) {
			t.Fatalf("part %d %s local code %d rank=%d outside global cardinality=%d", partIdx, role, localCode, rank, len(globalDictionary))
		}
		if got := globalDictionary[rank]; got != value {
			t.Fatalf("part %d %s local code %d rank=%d maps to %q want %q", partIdx, role, localCode, rank, got, value)
		}
	}
}

func assertQ2DenseGlobalRankPrepDistinctRanks(t *testing.T, partIdx int, localDictionary []string, localRanks []uint32, globalCardinality int) {
	t.Helper()
	seen := make(map[uint32]string, len(localRanks))
	for localCode, value := range localDictionary {
		rank := localRanks[localCode]
		if uint64(rank) >= uint64(globalCardinality) {
			t.Fatalf("part %d distinct local code %d rank=%d outside global cardinality=%d", partIdx, localCode, rank, globalCardinality)
		}
		if prior, ok := seen[rank]; ok && prior != value {
			t.Fatalf("part %d distinct rank collision rank=%d values=%q/%q", partIdx, rank, prior, value)
		}
		seen[rank] = value
	}
}

func assertQ2DenseGlobalRankPrepEquivalent(t *testing.T, baseline, prototype []columnTypedColumnPhysicalQueryPart, stats q2DenseGlobalRankPrepFixtureStats) {
	t.Helper()
	if len(prototype) != len(baseline) {
		t.Fatalf("prototype parts=%d want baseline parts=%d", len(prototype), len(baseline))
	}
	baselineDistinctRanks := newQ2DenseGlobalRankPrepRankInvariant("baseline", stats.distinctGlobalValues)
	prototypeDistinctRanks := newQ2DenseGlobalRankPrepRankInvariant("prototype", stats.distinctGlobalValues)
	for partIdx := range baseline {
		basePart := baseline[partIdx].DenseGroupCountDistinct
		protoPart := prototype[partIdx].DenseGroupCountDistinct
		if basePart == nil || protoPart == nil {
			t.Fatalf("part %d missing baseline/prototype dense prep baseline=%t prototype=%t", partIdx, basePart != nil, protoPart != nil)
		}
		if !reflect.DeepEqual(protoPart.Group.GlobalDictionary, basePart.Group.GlobalDictionary) {
			t.Fatalf("part %d group global dictionary changed prototype=%v baseline=%v", partIdx, protoPart.Group.GlobalDictionary, basePart.Group.GlobalDictionary)
		}
		if !reflect.DeepEqual(protoPart.Group.GlobalLocalRanks, basePart.Group.GlobalLocalRanks) {
			t.Fatalf("part %d group local ranks changed prototype=%v baseline=%v", partIdx, protoPart.Group.GlobalLocalRanks, basePart.Group.GlobalLocalRanks)
		}
		assertQ2DenseGlobalRankPrepColumnRankInvariant(t, baselineDistinctRanks, partIdx, "distinct", basePart.Distinct.Dictionary, basePart.Distinct.GlobalLocalRanks, basePart.Distinct.GlobalCardinality)
		assertQ2DenseGlobalRankPrepColumnRankInvariant(t, prototypeDistinctRanks, partIdx, "distinct", protoPart.Distinct.Dictionary, protoPart.Distinct.GlobalLocalRanks, protoPart.Distinct.GlobalCardinality)
		if basePart.Distinct.GlobalEmptyRankOK {
			baselineDistinctRanks.record(t, partIdx, "distinct", "", basePart.Distinct.GlobalEmptyRank, basePart.Distinct.GlobalCardinality)
		}
		if protoPart.Distinct.GlobalEmptyRankOK {
			prototypeDistinctRanks.record(t, partIdx, "distinct", "", protoPart.Distinct.GlobalEmptyRank, protoPart.Distinct.GlobalCardinality)
		}
		if protoPart.Distinct.GlobalCardinality != basePart.Distinct.GlobalCardinality ||
			protoPart.Distinct.GlobalCardinalityOK != basePart.Distinct.GlobalCardinalityOK ||
			protoPart.Distinct.GlobalEmptyRankOK != basePart.Distinct.GlobalEmptyRankOK {
			t.Fatalf("part %d distinct metadata prototype=(card=%d ok=%t empty=%t) baseline=(card=%d ok=%t empty=%t)",
				partIdx,
				protoPart.Distinct.GlobalCardinality,
				protoPart.Distinct.GlobalCardinalityOK,
				protoPart.Distinct.GlobalEmptyRankOK,
				basePart.Distinct.GlobalCardinality,
				basePart.Distinct.GlobalCardinalityOK,
				basePart.Distinct.GlobalEmptyRankOK)
		}
		if protoPart.Distinct.GlobalDictionary != nil {
			t.Fatalf("part %d prototype distinct global dictionary allocated len=%d", partIdx, len(protoPart.Distinct.GlobalDictionary))
		}
	}
	baselineDistinctRanks.finish(t)
	prototypeDistinctRanks.finish(t)
	if !reflect.DeepEqual(q2DenseGlobalRankPrepRankKeys(prototypeDistinctRanks.valueToRank), q2DenseGlobalRankPrepRankKeys(baselineDistinctRanks.valueToRank)) {
		t.Fatalf("prototype distinct value set differs from baseline")
	}
}

func q2DenseGlobalRankPrepExactDistinctRanksEqual(left, right []columnTypedColumnPhysicalQueryPart) bool {
	if len(left) != len(right) {
		return false
	}
	for partIdx := range left {
		leftPart := left[partIdx].DenseGroupCountDistinct
		rightPart := right[partIdx].DenseGroupCountDistinct
		if leftPart == nil || rightPart == nil {
			return leftPart == rightPart
		}
		if leftPart.Distinct.GlobalCardinality != rightPart.Distinct.GlobalCardinality ||
			leftPart.Distinct.GlobalCardinalityOK != rightPart.Distinct.GlobalCardinalityOK ||
			leftPart.Distinct.GlobalEmptyRank != rightPart.Distinct.GlobalEmptyRank ||
			leftPart.Distinct.GlobalEmptyRankOK != rightPart.Distinct.GlobalEmptyRankOK ||
			!reflect.DeepEqual(leftPart.Distinct.GlobalLocalRanks, rightPart.Distinct.GlobalLocalRanks) {
			return false
		}
	}
	return true
}

type q2DenseGlobalRankPrepRankInvariant struct {
	label       string
	cardinality int
	valueToRank map[string]uint32
	rankToValue map[uint32]string
}

func newQ2DenseGlobalRankPrepRankInvariant(label string, cardinality int) *q2DenseGlobalRankPrepRankInvariant {
	return &q2DenseGlobalRankPrepRankInvariant{
		label:       label,
		cardinality: cardinality,
		valueToRank: make(map[string]uint32, cardinality),
		rankToValue: make(map[uint32]string, cardinality),
	}
}

func assertQ2DenseGlobalRankPrepColumnRankInvariant(t *testing.T, invariant *q2DenseGlobalRankPrepRankInvariant, partIdx int, role string, localDictionary []string, localRanks []uint32, globalCardinality int) {
	t.Helper()
	if globalCardinality != invariant.cardinality {
		t.Fatalf("%s part %d %s cardinality=%d want %d", invariant.label, partIdx, role, globalCardinality, invariant.cardinality)
	}
	for localCode, value := range localDictionary {
		invariant.record(t, partIdx, role, value, localRanks[localCode], globalCardinality)
	}
}

func (invariant *q2DenseGlobalRankPrepRankInvariant) record(t *testing.T, partIdx int, role, value string, rank uint32, globalCardinality int) {
	t.Helper()
	if uint64(rank) >= uint64(globalCardinality) {
		t.Fatalf("%s part %d %s value %q rank=%d outside cardinality=%d", invariant.label, partIdx, role, value, rank, globalCardinality)
	}
	if priorRank, ok := invariant.valueToRank[value]; ok && priorRank != rank {
		t.Fatalf("%s part %d %s value %q rank changed %d -> %d", invariant.label, partIdx, role, value, priorRank, rank)
	}
	if priorValue, ok := invariant.rankToValue[rank]; ok && priorValue != value {
		t.Fatalf("%s part %d %s rank collision rank=%d values=%q/%q", invariant.label, partIdx, role, rank, priorValue, value)
	}
	invariant.valueToRank[value] = rank
	invariant.rankToValue[rank] = value
}

func (invariant *q2DenseGlobalRankPrepRankInvariant) finish(t *testing.T) {
	t.Helper()
	if len(invariant.valueToRank) != invariant.cardinality || len(invariant.rankToValue) != invariant.cardinality {
		t.Fatalf("%s rank coverage values=%d ranks=%d want cardinality=%d", invariant.label, len(invariant.valueToRank), len(invariant.rankToValue), invariant.cardinality)
	}
}

func q2DenseGlobalRankPrepRankKeys(values map[string]uint32) []string {
	keys := make([]string, 0, len(values))
	for value := range values {
		keys = append(keys, value)
	}
	sort.Strings(keys)
	return keys
}
