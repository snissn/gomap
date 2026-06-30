package collections

import (
	"fmt"
	"sort"
	"testing"
)

type q2DenseGlobalRankPrepVariant struct {
	name    string
	prepare func([]columnTypedColumnPhysicalQueryPart, *columnTypedColumnPhysicalQueryPrepareDiagnostics) error
}

var q2DenseGlobalRankPrepVariants = []q2DenseGlobalRankPrepVariant{
	{
		name:    "current_map_fallback",
		prepare: prepareColumnTypedColumnDenseGroupCountDistinctGlobalRankMapsWithDiagnostics,
	},
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

func BenchmarkTypedColumnQ2DenseGroupCountDistinctGlobalRankPrep(b *testing.B) {
	for _, variant := range q2DenseGlobalRankPrepVariants {
		for _, shape := range q2DenseGlobalRankPrepDictionaryShapes {
			for _, partCount := range q2DenseGlobalRankPrepPartCounts {
				b.Run(fmt.Sprintf("%s/%s/parts=%d", variant.name, shape.name, partCount), func(b *testing.B) {
					fixture := newQ2DenseGlobalRankPrepParts(partCount, shape)
					stats := q2DenseGlobalRankPrepStats(fixture)
					var diagnostics columnTypedColumnPhysicalQueryPrepareDiagnostics
					b.ReportAllocs()
					b.ReportMetric(float64(partCount), "parts/op")
					b.ReportMetric(float64(stats.groupGlobalValues), "group_global_values/op")
					b.ReportMetric(float64(stats.distinctGlobalValues), "distinct_global_values/op")
					b.ReportMetric(float64(stats.totalLocalDictionaryLen), "local_dictionary_values/op")

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
						b.ReportMetric(float64(diagnostics.Q2GroupRankNanos)/float64(b.N), "diag_group_rank_ns/op")
						b.ReportMetric(float64(diagnostics.Q2DistinctRankNanos)/float64(b.N), "diag_distinct_rank_ns/op")
						b.ReportMetric(float64(diagnostics.Q2LocalRankNanos)/float64(b.N), "diag_local_rank_ns/op")
					}
				})
			}
		}
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
				Rows: q2DenseGlobalRankPrepDistinctValuesPerPart,
				Group: columnTypedColumnDenseStringCodeColumn{
					Dictionary: groupDict,
					Valid:      q2DenseGlobalRankPrepValidity(partIdx, 17),
				},
				Distinct: columnTypedColumnDenseStringCodeColumn{
					Dictionary: distinctDict,
					Valid:      q2DenseGlobalRankPrepValidity(partIdx, 19),
				},
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
		Dictionary: append([]string(nil), column.Dictionary...),
		Valid:      append([]bool(nil), column.Valid...),
	}
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
