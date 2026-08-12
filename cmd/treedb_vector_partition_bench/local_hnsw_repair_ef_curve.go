package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

const localHNSWRepairEFCurveSchemaV1 = "treedb_local_hnsw_repair_ef_curve_v1"
const localHNSWRepairEFCurveTimingVariantV1 = "source_order_auxiliary_navigation_v3"

var localHNSWRepairEFCurvePointsV1 = []int{64, 128, 512, 4096}

type localHNSWRepairEFCurveCellV1 struct {
	EFSearch         int                                   `json:"ef_search"`
	QueryCount       int                                   `json:"query_count"`
	RoutesSHA256     string                                `json:"routes_sha256"`
	RoutingRecall    localHNSWAttributionRecallAggregateV1 `json:"routing_recall"`
	P2Recall         localHNSWAttributionRecallAggregateV1 `json:"p2_recall"`
	P16Recall        localHNSWAttributionRecallAggregateV1 `json:"p16_recall"`
	RoutingMissSlots uint64                                `json:"routing_miss_slots"`
	P2HitSlots       uint64                                `json:"p2_hit_slots"`
	P16HitSlots      uint64                                `json:"p16_hit_slots"`
	P2ResultsSHA256  string                                `json:"p2_results_sha256"`
	P16ResultsSHA256 string                                `json:"p16_results_sha256"`
	P2Work           localHNSWRepairCalibrationWorkV1      `json:"p2_work"`
	P16Work          localHNSWRepairCalibrationWorkV1      `json:"p16_work"`
	TerminationCount map[string]uint64                     `json:"termination_counts"`
}

type localHNSWRepairEFCurveTimingCellV1 struct {
	Repetition          int      `json:"repetition"`
	EFSearch            int      `json:"ef_search"`
	Probes              int      `json:"probes"`
	QueryCount          int      `json:"query_count"`
	ElapsedNanos        uint64   `json:"elapsed_nanos"`
	QPS                 float64  `json:"qps"`
	P50Nanos            uint64   `json:"p50_nanos"`
	P95Nanos            uint64   `json:"p95_nanos"`
	P99Nanos            uint64   `json:"p99_nanos"`
	Candidates          uint64   `json:"candidates"`
	NativeEdges         uint64   `json:"native_edges"`
	AuxiliaryEdges      uint64   `json:"auxiliary_edges"`
	AuxiliaryCandidates uint64   `json:"auxiliary_candidates"`
	AuxiliaryAdmissions uint64   `json:"auxiliary_admissions"`
	CPUAvailable        bool     `json:"cpu_available"`
	CPUDeltaNanos       int64    `json:"cpu_delta_nanos"`
	AllocBytesDelta     uint64   `json:"alloc_bytes_delta"`
	ResultSHA256        []string `json:"result_sha256"`
}

type localHNSWRepairEFCurveTimingGateV1 struct {
	P2QPS148Over128  float64 `json:"p2_qps_148_over_128"`
	P16QPS148Over128 float64 `json:"p16_qps_148_over_128"`
	P2P95148Over128  float64 `json:"p2_p95_148_over_128"`
	P16P95148Over128 float64 `json:"p16_p95_148_over_128"`
	Disposition      string  `json:"disposition"`
}

type localHNSWRepairEFCurveTimingV1 struct {
	Variant      string                               `json:"variant"`
	RoutesSHA256 string                               `json:"routes_sha256"`
	Cells        []localHNSWRepairEFCurveTimingCellV1 `json:"cells"`
	Gate         localHNSWRepairEFCurveTimingGateV1   `json:"gate"`
}

type localHNSWRepairEFCurveTimingQueryV1 struct {
	Ordinal         int
	Query           []float32
	QueryFP32SHA256 string
	P2Route         []uint32
	P16Route        []uint32
	P2EF128SHA256   string
	P16EF128SHA256  string
	P2EF148SHA256   string
	P16EF148SHA256  string
}

type localHNSWRepairEFCurveReportV1 struct {
	Schema      string                               `json:"schema"`
	ResultKind  string                               `json:"result_kind"`
	Status      string                               `json:"status"`
	GeneratedAt string                               `json:"generated_at"`
	Provenance  localHNSWAttributionProvenanceV1     `json:"provenance"`
	Host        m8ProductionHostEvidenceV1           `json:"host"`
	Inputs      localHNSWAttributionInputsEvidenceV1 `json:"inputs"`
	Source      localHNSWAttributionSourceEvidenceV1 `json:"source"`
	TopK        int                                  `json:"top_k"`
	EFSearch    []int                                `json:"ef_search"`
	ProbeCounts []int                                `json:"probe_counts"`
	RepairBuild localHNSWAttributionBuildEvidenceV1  `json:"repair_build"`
	Graph       localHNSWRepairCalibrationGraphV1    `json:"graph"`
	Cells       []localHNSWRepairEFCurveCellV1       `json:"cells"`
	Timing      *localHNSWRepairEFCurveTimingV1      `json:"timing,omitempty"`
	Disposition string                               `json:"disposition"`
	Limitations []string                             `json:"limitations"`
}

func localHNSWRepairEFCurvePointsValidV1(points []int) bool {
	if len(points) == 0 || len(points) > 8 {
		return false
	}
	for i, point := range points {
		if point < 10 || point > 4096 || i > 0 && points[i-1] >= point {
			return false
		}
	}
	return true
}

func localHNSWRepairEFCurveV1Build(ctx context.Context, source *m8ProductionMultiGroupAssetsV1, repair *localHNSWVariantHarnessV1, points, ordinals []int, queries [][]float32, truth [][]m8CanonicalResultV1) ([]localHNSWRepairEFCurveCellV1, error) {
	if source == nil || repair == nil || !localHNSWRepairEFCurvePointsValidV1(points) || len(ordinals) == 0 || len(ordinals) != len(queries) || len(ordinals) != len(truth) || localHNSWAttributionGraphHarnessV1(source, repair) != nil {
		return nil, errors.New("invalid local HNSW repair EF curve inputs")
	}
	partitions := int(source.manifest.PartitionCount)
	candidates := min(256, int(source.status.Representatives))
	if partitions < 1 || candidates < 1 {
		return nil, errors.New("invalid local HNSW repair EF curve router")
	}
	out := make([]localHNSWRepairEFCurveCellV1, len(points))
	resultHashes := make([][2]hash.Hash, len(points))
	routesHash := sha256.New()
	routesHash.Write([]byte("treedb-4106-local-hnsw-repair-ef-curve-routes-v1/"))
	var raw [4]byte
	for i, efSearch := range points {
		out[i].EFSearch, out[i].TerminationCount = efSearch, map[string]uint64{}
		resultHashes[i][0], resultHashes[i][1] = sha256.New(), sha256.New()
		resultHashes[i][0].Write([]byte("treedb-4106-local-hnsw-repair-ef-curve-p2-results-v1/"))
		resultHashes[i][1].Write([]byte("treedb-4106-local-hnsw-repair-ef-curve-p16-results-v1/"))
	}
	for i, ordinal := range ordinals {
		if ordinal < 0 || !localHNSWCalibrationOrdinalV1(ordinal) || i > 0 && ordinals[i-1] >= ordinal || len(queries[i]) == 0 {
			return nil, errors.New("invalid local HNSW repair EF curve ordinal")
		}
		canonicalTruth, err := localHNSWAttributionCanonicalResultsV1(truth[i], true)
		ids, scores := m8CanonicalParityV1(truth[i], canonicalTruth)
		if err != nil || !ids || !scores {
			return nil, errors.New("invalid local HNSW repair EF curve truth")
		}
		p2, err := localHNSWAttributionQueryRouteV1(ctx, source, queries[i], candidates, min(2, partitions))
		if err != nil {
			return nil, err
		}
		p16, err := localHNSWAttributionQueryRouteV1(ctx, source, queries[i], candidates, partitions)
		if err != nil || !localHNSWAttributionRoutePrefixV1(p2, p16) || !localHNSWAttributionRoutePermutationV1(p16, partitions) {
			return nil, errors.New("invalid local HNSW repair EF curve route")
		}
		binary.LittleEndian.PutUint32(raw[:], uint32(ordinal))
		routesHash.Write(raw[:])
		routesHash.Write([]byte(localHNSWAttributionQueryFP32SHA256V1(queries[i])))
		for _, route := range [][]uint32{p2, p16} {
			for _, partition := range route {
				binary.LittleEndian.PutUint32(raw[:], partition)
				routesHash.Write(raw[:])
			}
		}
		exactLocal, err := localHNSWAttributionExactLocalV1(ctx, repair, queries[i], p2)
		if err != nil {
			return nil, err
		}
		for cellIndex, efSearch := range points {
			searches, results, err := localHNSWRepairCalibrationSearchesAtEFV1(ctx, repair, queries[i], efSearch)
			if err != nil {
				return nil, err
			}
			p2Results, err := localHNSWRepairCalibrationMergeV1(results, p2)
			if err != nil {
				return nil, err
			}
			p16Results, err := localHNSWRepairCalibrationMergeV1(results, p16)
			if err != nil {
				return nil, err
			}
			cell := &out[cellIndex]
			binary.LittleEndian.PutUint32(raw[:], uint32(ordinal))
			resultHashes[cellIndex][0].Write(raw[:])
			resultHashes[cellIndex][0].Write([]byte(localHNSWRepairCalibrationResultsSHA256V1(p2Results)))
			resultHashes[cellIndex][1].Write(raw[:])
			resultHashes[cellIndex][1].Write([]byte(localHNSWRepairCalibrationResultsSHA256V1(p16Results)))
			routingHits := localHNSWRepairEFCurveHitSlotsV1(canonicalTruth, exactLocal)
			p2Hits := localHNSWRepairEFCurveHitSlotsV1(canonicalTruth, p2Results)
			p16Hits := localHNSWRepairEFCurveHitSlotsV1(canonicalTruth, p16Results)
			if routingHits > uint64(len(canonicalTruth)) || math.MaxUint64-cell.RoutingMissSlots < uint64(len(canonicalTruth))-routingHits || math.MaxUint64-cell.P2HitSlots < p2Hits || math.MaxUint64-cell.P16HitSlots < p16Hits {
				return nil, errors.New("invalid local HNSW repair EF curve slots")
			}
			cell.RoutingMissSlots += uint64(len(canonicalTruth)) - routingHits
			cell.P2HitSlots += p2Hits
			cell.P16HitSlots += p16Hits
			count := cell.QueryCount
			if err := localHNSWAttributionRecallAddV1(&cell.RoutingRecall, m8CanonicalRecallV1(canonicalTruth, exactLocal), count); err != nil {
				return nil, err
			}
			if err := localHNSWAttributionRecallAddV1(&cell.P2Recall, m8CanonicalRecallV1(canonicalTruth, p2Results), count); err != nil {
				return nil, err
			}
			if err := localHNSWAttributionRecallAddV1(&cell.P16Recall, m8CanonicalRecallV1(canonicalTruth, p16Results), count); err != nil || localHNSWRepairCalibrationWorkAddV1(&cell.P2Work, p2, searches) != nil || localHNSWRepairCalibrationWorkAddV1(&cell.P16Work, p16, searches) != nil {
				return nil, errors.New("invalid local HNSW repair EF curve work")
			}
			for _, search := range searches {
				if !localHNSWAttributionTimingTerminationV1(search.TerminationReason) || cell.TerminationCount[search.TerminationReason] == math.MaxUint64 {
					return nil, errors.New("invalid local HNSW repair EF curve termination")
				}
				cell.TerminationCount[search.TerminationReason]++
			}
			cell.QueryCount++
		}
	}
	routesSHA256 := fmt.Sprintf("%x", routesHash.Sum(nil))
	for i := range out {
		cell := &out[i]
		cell.RoutesSHA256 = routesSHA256
		cell.P2ResultsSHA256 = fmt.Sprintf("%x", resultHashes[i][0].Sum(nil))
		cell.P16ResultsSHA256 = fmt.Sprintf("%x", resultHashes[i][1].Sum(nil))
		if cell.QueryCount != len(ordinals) || !localHNSWAttributionSHA256V1(cell.RoutesSHA256) || !localHNSWAttributionSHA256V1(cell.P2ResultsSHA256) || !localHNSWAttributionSHA256V1(cell.P16ResultsSHA256) {
			return nil, errors.New("invalid local HNSW repair EF curve aggregate")
		}
		for _, recall := range []*localHNSWAttributionRecallAggregateV1{&cell.RoutingRecall, &cell.P2Recall, &cell.P16Recall} {
			recall.Mean /= float64(cell.QueryCount)
			if !localHNSWAttributionFiniteRecallV1(recall.Mean) || !localHNSWAttributionFiniteRecallV1(recall.Min) {
				return nil, errors.New("invalid local HNSW repair EF curve recall")
			}
		}
	}
	return out, nil
}

func localHNSWRepairEFCurveHitSlotsV1(want, got []m8CanonicalResultV1) uint64 {
	wantIDs := make(map[string]struct{}, len(want))
	for _, result := range want {
		wantIDs[result.ID] = struct{}{}
	}
	var hits uint64
	for _, result := range got {
		if _, ok := wantIDs[result.ID]; ok {
			hits++
		}
	}
	return hits
}

func localHNSWRepairEFCurveTimingV1Build(ctx context.Context, source *m8ProductionMultiGroupAssetsV1, repair *localHNSWVariantHarnessV1, ordinals []int, queries [][]float32) (localHNSWRepairEFCurveTimingV1, error) {
	out := localHNSWRepairEFCurveTimingV1{Variant: localHNSWRepairEFCurveTimingVariantV1}
	if source == nil || repair == nil || len(ordinals) == 0 || len(ordinals) != len(queries) || localHNSWAttributionGraphHarnessV1(source, repair) != nil {
		return out, errors.New("invalid local HNSW repair EF timing inputs")
	}
	partitions := int(source.manifest.PartitionCount)
	candidates := min(256, int(source.status.Representatives))
	if partitions < 1 || candidates < 1 || len(repair.searchers) != partitions {
		return out, errors.New("invalid local HNSW repair EF timing harness")
	}
	timingQueries := make([]localHNSWRepairEFCurveTimingQueryV1, 0, len(ordinals))
	routesHash := sha256.New()
	routesHash.Write([]byte("treedb-4106-local-hnsw-repair-ef-curve-routes-v1/"))
	var raw [4]byte
	for i, ordinal := range ordinals {
		if ordinal < 0 || !localHNSWCalibrationOrdinalV1(ordinal) || i > 0 && ordinals[i-1] >= ordinal || len(queries[i]) == 0 {
			return out, errors.New("invalid local HNSW repair EF timing query")
		}
		p2, err := localHNSWAttributionQueryRouteV1(ctx, source, queries[i], candidates, min(2, partitions))
		if err != nil {
			return out, err
		}
		p16, err := localHNSWAttributionQueryRouteV1(ctx, source, queries[i], candidates, partitions)
		if err != nil || !localHNSWAttributionRoutePrefixV1(p2, p16) || !localHNSWAttributionRoutePermutationV1(p16, partitions) {
			return out, errors.New("invalid local HNSW repair EF timing route")
		}
		binary.LittleEndian.PutUint32(raw[:], uint32(ordinal))
		routesHash.Write(raw[:])
		routesHash.Write([]byte(localHNSWAttributionQueryFP32SHA256V1(queries[i])))
		for _, route := range [][]uint32{p2, p16} {
			for _, partition := range route {
				binary.LittleEndian.PutUint32(raw[:], partition)
				routesHash.Write(raw[:])
			}
		}
		row := localHNSWRepairEFCurveTimingQueryV1{Ordinal: ordinal, Query: append([]float32(nil), queries[i]...), QueryFP32SHA256: localHNSWAttributionQueryFP32SHA256V1(queries[i]), P2Route: append([]uint32(nil), p2...), P16Route: append([]uint32(nil), p16...)}
		for _, efSearch := range []int{128, 148} {
			_, results, err := localHNSWRepairCalibrationSearchesAtEFV1(ctx, repair, row.Query, efSearch)
			if err != nil {
				return out, err
			}
			p2Results, err := localHNSWRepairCalibrationMergeV1(results, p2)
			if err != nil {
				return out, err
			}
			p16Results, err := localHNSWRepairCalibrationMergeV1(results, p16)
			if err != nil {
				return out, err
			}
			if efSearch == 128 {
				row.P2EF128SHA256, row.P16EF128SHA256 = localHNSWRepairCalibrationResultsSHA256V1(p2Results), localHNSWRepairCalibrationResultsSHA256V1(p16Results)
			} else {
				row.P2EF148SHA256, row.P16EF148SHA256 = localHNSWRepairCalibrationResultsSHA256V1(p2Results), localHNSWRepairCalibrationResultsSHA256V1(p16Results)
			}
		}
		if !localHNSWRepairEFCurveTimingQueryV1Valid(row, partitions) {
			return out, errors.New("invalid local HNSW repair EF timing attribution")
		}
		timingQueries = append(timingQueries, row)
	}
	out.RoutesSHA256 = fmt.Sprintf("%x", routesHash.Sum(nil))
	if !localHNSWAttributionSHA256V1(out.RoutesSHA256) {
		return localHNSWRepairEFCurveTimingV1{}, errors.New("invalid local HNSW repair EF timing routes")
	}
	orders := [4][4]struct{ efSearch, probes int }{
		{{128, 2}, {148, 2}, {128, partitions}, {148, partitions}},
		{{148, partitions}, {128, partitions}, {148, 2}, {128, 2}},
		{{128, partitions}, {148, partitions}, {128, 2}, {148, 2}},
		{{148, 2}, {128, 2}, {148, partitions}, {128, partitions}},
	}
	for repetition, order := range orders {
		for _, item := range order {
			cell, err := localHNSWRepairEFCurveTimingCellV1Run(ctx, repair, timingQueries, repetition, item.efSearch, item.probes)
			if err != nil {
				return localHNSWRepairEFCurveTimingV1{}, err
			}
			out.Cells = append(out.Cells, cell)
		}
	}
	gate, err := localHNSWRepairEFCurveTimingGateV1Build(out.Cells, partitions)
	if err != nil {
		return localHNSWRepairEFCurveTimingV1{}, err
	}
	out.Gate = gate
	return out, nil
}

func localHNSWRepairEFCurveTimingQueryV1Valid(query localHNSWRepairEFCurveTimingQueryV1, partitions int) bool {
	return query.Ordinal >= 0 && localHNSWCalibrationOrdinalV1(query.Ordinal) && len(query.Query) > 0 && query.QueryFP32SHA256 == localHNSWAttributionQueryFP32SHA256V1(query.Query) && len(query.P2Route) == min(2, partitions) && localHNSWAttributionRoutePrefixV1(query.P2Route, query.P16Route) && localHNSWAttributionRoutePermutationV1(query.P16Route, partitions) && localHNSWAttributionSHA256V1(query.P2EF128SHA256) && localHNSWAttributionSHA256V1(query.P16EF128SHA256) && localHNSWAttributionSHA256V1(query.P2EF148SHA256) && localHNSWAttributionSHA256V1(query.P16EF148SHA256)
}

func localHNSWRepairEFCurveTimingCellV1Run(ctx context.Context, harness *localHNSWVariantHarnessV1, queries []localHNSWRepairEFCurveTimingQueryV1, repetition, efSearch, probes int) (localHNSWRepairEFCurveTimingCellV1, error) {
	var cell localHNSWRepairEFCurveTimingCellV1
	if harness == nil || len(queries) == 0 || (efSearch != 128 && efSearch != 148) || (probes != 2 && probes != len(harness.searchers)) {
		return cell, errors.New("invalid local HNSW repair EF timing cell")
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	cpuBefore, cpuOK := vectorPartitionBenchmarkCPUNanos()
	started := time.Now()
	durations := make([]uint64, 0, len(queries))
	digests := make([]string, 0, len(queries))
	for _, query := range queries {
		if !localHNSWRepairEFCurveTimingQueryV1Valid(query, len(harness.searchers)) {
			return cell, errors.New("invalid local HNSW repair EF timing query")
		}
		route, wantDigest := query.P2Route, query.P2EF128SHA256
		if probes == len(harness.searchers) {
			route, wantDigest = query.P16Route, query.P16EF128SHA256
		}
		if efSearch == 148 {
			if probes == len(harness.searchers) {
				wantDigest = query.P16EF148SHA256
			} else {
				wantDigest = query.P2EF148SHA256
			}
		}
		queryStarted := time.Now()
		results, work, err := localHNSWRepairCalibrationOrdinarySearchAtEFV1(ctx, harness, route, query.Query, efSearch, "auxiliary_navigation", query.QueryFP32SHA256)
		if err != nil || localHNSWRepairCalibrationResultsSHA256V1(results) != wantDigest {
			return cell, errors.New("local HNSW repair EF ordinary result drift")
		}
		elapsed := uint64(time.Since(queryStarted))
		if elapsed == 0 {
			elapsed = 1
		}
		if math.MaxUint64-cell.Candidates < work.Candidates || math.MaxUint64-cell.NativeEdges < work.NativeEdges || math.MaxUint64-cell.AuxiliaryEdges < work.AuxiliaryEdges || math.MaxUint64-cell.AuxiliaryCandidates < work.AuxiliaryCandidates || math.MaxUint64-cell.AuxiliaryAdmissions < work.AuxiliaryAdmissions {
			return cell, errors.New("local HNSW repair EF timing overflow")
		}
		durations, digests = append(durations, elapsed), append(digests, wantDigest)
		cell.Candidates += work.Candidates
		cell.NativeEdges += work.NativeEdges
		cell.AuxiliaryEdges += work.AuxiliaryEdges
		cell.AuxiliaryCandidates += work.AuxiliaryCandidates
		cell.AuxiliaryAdmissions += work.AuxiliaryAdmissions
	}
	elapsed := uint64(time.Since(started))
	if elapsed == 0 {
		elapsed = 1
	}
	cpuAfter, cpuAfterOK := vectorPartitionBenchmarkCPUNanos()
	runtime.ReadMemStats(&after)
	if cpuOK != cpuAfterOK || cpuOK && cpuAfter < cpuBefore || after.TotalAlloc < before.TotalAlloc {
		return cell, errors.New("invalid local HNSW repair EF timing resources")
	}
	cell.Repetition, cell.EFSearch, cell.Probes, cell.QueryCount = repetition, efSearch, probes, len(queries)
	cell.ElapsedNanos, cell.QPS = elapsed, float64(len(queries))*float64(time.Second)/float64(elapsed)
	cell.P50Nanos, cell.P95Nanos, cell.P99Nanos = m8PercentileV1(durations, 50), m8PercentileV1(durations, 95), m8PercentileV1(durations, 99)
	cell.CPUAvailable, cell.AllocBytesDelta, cell.ResultSHA256 = cpuOK, after.TotalAlloc-before.TotalAlloc, digests
	if cpuOK {
		cell.CPUDeltaNanos = cpuAfter - cpuBefore
	}
	return cell, nil
}

func localHNSWRepairEFCurveTimingGateV1Build(cells []localHNSWRepairEFCurveTimingCellV1, partitions int) (localHNSWRepairEFCurveTimingGateV1, error) {
	var out localHNSWRepairEFCurveTimingGateV1
	if partitions < 2 || len(cells) != 16 || cells[0].QueryCount < 1 {
		return out, errors.New("invalid local HNSW repair EF timing cells")
	}
	queryCount := cells[0].QueryCount
	orders := [4][4]struct{ efSearch, probes int }{
		{{128, 2}, {148, 2}, {128, partitions}, {148, partitions}},
		{{148, partitions}, {128, partitions}, {148, 2}, {128, 2}},
		{{128, partitions}, {148, partitions}, {128, 2}, {148, 2}},
		{{148, 2}, {128, 2}, {148, partitions}, {128, partitions}},
	}
	for repetition, order := range orders {
		for position, want := range order {
			got := cells[repetition*len(order)+position]
			if got.Repetition != repetition || got.EFSearch != want.efSearch || got.Probes != want.probes {
				return out, errors.New("invalid local HNSW repair EF timing order")
			}
		}
	}
	qps := [2][2][]float64{}
	p95 := [2][2][]uint64{}
	digests := [2][2][]string{}
	seen := [4][2][2]bool{}
	for _, cell := range cells {
		efIndex, probeIndex := -1, -1
		if cell.EFSearch == 128 {
			efIndex = 0
		} else if cell.EFSearch == 148 {
			efIndex = 1
		}
		if cell.Probes == 2 {
			probeIndex = 0
		} else if cell.Probes == partitions {
			probeIndex = 1
		}
		if cell.Repetition < 0 || cell.Repetition >= 4 || efIndex < 0 || probeIndex < 0 || seen[cell.Repetition][efIndex][probeIndex] || cell.QueryCount != queryCount || cell.ElapsedNanos == 0 || !localHNSWRepairEFCurveFinitePositiveV1(cell.QPS) || cell.P50Nanos == 0 || cell.P95Nanos == 0 || cell.P99Nanos == 0 || cell.P50Nanos > cell.P95Nanos || cell.P95Nanos > cell.P99Nanos || cell.Candidates == 0 || cell.NativeEdges == 0 || len(cell.ResultSHA256) != cell.QueryCount {
			return out, errors.New("invalid local HNSW repair EF timing cell")
		}
		for _, digest := range cell.ResultSHA256 {
			if !localHNSWAttributionSHA256V1(digest) {
				return out, errors.New("invalid local HNSW repair EF timing digest")
			}
		}
		if prior := digests[efIndex][probeIndex]; prior != nil && !slices.Equal(prior, cell.ResultSHA256) {
			return out, errors.New("local HNSW repair EF timing result drift")
		} else if prior == nil {
			digests[efIndex][probeIndex] = append([]string(nil), cell.ResultSHA256...)
		}
		seen[cell.Repetition][efIndex][probeIndex] = true
		qps[efIndex][probeIndex] = append(qps[efIndex][probeIndex], cell.QPS)
		p95[efIndex][probeIndex] = append(p95[efIndex][probeIndex], cell.P95Nanos)
	}
	for repetition := range seen {
		for efIndex := range seen[repetition] {
			for probeIndex := range seen[repetition][efIndex] {
				if !seen[repetition][efIndex][probeIndex] {
					return out, errors.New("incomplete local HNSW repair EF timing")
				}
			}
		}
	}
	for probeIndex := range qps[0] {
		if len(qps[0][probeIndex]) != 4 || len(qps[1][probeIndex]) != 4 || len(p95[0][probeIndex]) != 4 || len(p95[1][probeIndex]) != 4 {
			return out, errors.New("invalid local HNSW repair EF timing repetitions")
		}
	}
	var err error
	if out.P2QPS148Over128, err = localHNSWRepairEFCurveMedianRatioV1(qps[1][0], qps[0][0]); err != nil {
		return out, err
	}
	if out.P16QPS148Over128, err = localHNSWRepairEFCurveMedianRatioV1(qps[1][1], qps[0][1]); err != nil {
		return out, err
	}
	if out.P2P95148Over128, err = localHNSWRepairEFCurveMedianUintRatioV1(p95[1][0], p95[0][0]); err != nil {
		return out, err
	}
	if out.P16P95148Over128, err = localHNSWRepairEFCurveMedianUintRatioV1(p95[1][1], p95[0][1]); err != nil {
		return out, err
	}
	if !localHNSWRepairEFCurveFinitePositiveV1(out.P2QPS148Over128) || !localHNSWRepairEFCurveFinitePositiveV1(out.P16QPS148Over128) || !localHNSWRepairEFCurveFinitePositiveV1(out.P2P95148Over128) || !localHNSWRepairEFCurveFinitePositiveV1(out.P16P95148Over128) {
		return out, errors.New("invalid local HNSW repair EF timing ratio")
	}
	if out.P2QPS148Over128 >= .90 && out.P16QPS148Over128 >= .90 && out.P2P95148Over128 <= 1.10 && out.P16P95148Over128 <= 1.10 {
		out.Disposition = "calibration_timing_gate_pass"
	} else {
		out.Disposition = "calibration_timing_gate_fail"
	}
	return out, nil
}

func localHNSWRepairEFCurveMedianRatioV1(numerator, denominator []float64) (float64, error) {
	if len(numerator) != 4 || len(denominator) != 4 {
		return 0, errors.New("invalid local HNSW repair EF timing median")
	}
	numerator, denominator = append([]float64(nil), numerator...), append([]float64(nil), denominator...)
	sort.Float64s(numerator)
	sort.Float64s(denominator)
	medianNumerator, medianDenominator := (numerator[1]+numerator[2])/2, (denominator[1]+denominator[2])/2
	if !localHNSWRepairEFCurveFinitePositiveV1(medianNumerator) || !localHNSWRepairEFCurveFinitePositiveV1(medianDenominator) {
		return 0, errors.New("invalid local HNSW repair EF timing median")
	}
	return medianNumerator / medianDenominator, nil
}

func localHNSWRepairEFCurveMedianUintRatioV1(numerator, denominator []uint64) (float64, error) {
	if len(numerator) != 4 || len(denominator) != 4 {
		return 0, errors.New("invalid local HNSW repair EF timing median")
	}
	numerator, denominator = append([]uint64(nil), numerator...), append([]uint64(nil), denominator...)
	slices.Sort(numerator)
	slices.Sort(denominator)
	medianNumerator, medianDenominator := float64(numerator[1])/2+float64(numerator[2])/2, float64(denominator[1])/2+float64(denominator[2])/2
	if !localHNSWRepairEFCurveFinitePositiveV1(medianNumerator) || !localHNSWRepairEFCurveFinitePositiveV1(medianDenominator) {
		return 0, errors.New("invalid local HNSW repair EF timing median")
	}
	return medianNumerator / medianDenominator, nil
}

func localHNSWRepairEFCurveFinitePositiveV1(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func localHNSWRepairEFCurveDispositionV1(cells []localHNSWRepairEFCurveCellV1) (string, error) {
	points := make([]int, len(cells))
	for i, cell := range cells {
		points[i] = cell.EFSearch
	}
	if !localHNSWRepairEFCurvePointsValidV1(points) {
		return "", errors.New("invalid local HNSW repair EF curve")
	}
	for _, cell := range cells {
		if cell.P2Recall.Mean >= .95 {
			return fmt.Sprintf("p2_target_crossed_smallest_passing_ef_%d", cell.EFSearch), nil
		}
	}
	return "no_point_reaches_0_9500", nil
}

func runLocalHNSWRepairEFCurveV1(args []string, stdout io.Writer) (runErr error) {
	fs := flag.NewFlagSet("treedb_vector_partition_bench local-hnsw-repair-ef-curve", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var dataset, retainedDB, calibrationSplit, holdoutSplit, truthArtifact, historicalCSV, tempRoot, out, efSearch, baseSHA, headSHA, sourceCheckout string
	var timingGate bool
	fs.StringVar(&dataset, "dataset", "", "frozen fixture directory")
	fs.StringVar(&retainedDB, "retained-db", "", "literal retained 250k database")
	fs.StringVar(&calibrationSplit, "calibration-split", "", "frozen calibration manifest")
	fs.StringVar(&holdoutSplit, "holdout-split", "", "sealed holdout manifest")
	fs.StringVar(&truthArtifact, "truth-artifact", "", "sealed trusted truth artifact")
	fs.StringVar(&historicalCSV, "historical-search", "", "three comma-separated retained search reports")
	fs.StringVar(&tempRoot, "temp-root", "", "existing fast temporary root")
	fs.StringVar(&out, "out", "", "fresh report path")
	fs.StringVar(&efSearch, "ef-search", "64,128,512,4096", "comma-separated calibration EF points")
	fs.BoolVar(&timingGate, "timing-gate", false, "run the frozen EF128/EF148 ordinary-path calibration timing gate")
	fs.StringVar(&baseSHA, "base-sha", "", "source-lock base SHA")
	fs.StringVar(&headSHA, "head-sha", "", "exact implementation head SHA")
	fs.StringVar(&sourceCheckout, "source-checkout", "", "clean exact-head checkout")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || dataset == "" || retainedDB == "" || calibrationSplit == "" || holdoutSplit == "" || truthArtifact == "" || historicalCSV == "" || tempRoot == "" || out == "" || baseSHA == "" || headSHA == "" || sourceCheckout == "" {
		return errors.New("local-hnsw-repair-ef-curve requires all frozen inputs, paths, provenance, and no positional arguments")
	}
	var err error
	points, err := parseInts(efSearch)
	if err != nil || !localHNSWRepairEFCurvePointsValidV1(points) {
		return errors.New("invalid local HNSW repair EF curve points")
	}
	if timingGate && !slices.Equal(points, []int{128, 148}) {
		return errors.New("local HNSW repair EF timing gate requires ef-search=128,148")
	}
	for destination, value := range map[*string]string{&dataset: dataset, &retainedDB: retainedDB, &calibrationSplit: calibrationSplit, &holdoutSplit: holdoutSplit, &truthArtifact: truthArtifact, &tempRoot: tempRoot, &out: out, &sourceCheckout: sourceCheckout} {
		*destination, err = m8CanonicalPathV1(value)
		if err != nil {
			return err
		}
	}
	baseSHA, headSHA, err = provenanceWithExplicitV1(baseSHA, headSHA)
	if err != nil || baseSHA != localHNSWAttributionSourceLockV1 {
		return errors.New("local HNSW repair EF curve source lock")
	}
	sourceCheckout, err = localHNSWAttributionSourceCheckoutV1(sourceCheckout, baseSHA, headSHA)
	if err != nil || m8GitDirtyInV1(sourceCheckout) || filepath.Ext(out) != ".json" {
		return errors.New("invalid local HNSW repair EF curve provenance or output")
	}
	info, err := os.Lstat(tempRoot)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("invalid local HNSW repair EF curve temporary root")
	}
	if _, err := os.Lstat(out); !errors.Is(err, os.ErrNotExist) {
		return errors.New("local HNSW repair EF curve output exists")
	}
	parts := strings.Split(historicalCSV, ",")
	if len(parts) != 3 {
		return errors.New("local HNSW repair EF curve requires three historical reports")
	}
	var historicalPaths [3]string
	for i, path := range parts {
		historicalPaths[i], err = m8CanonicalPathV1(strings.TrimSpace(path))
		if err != nil {
			return err
		}
	}
	datasetManifest := filepath.Join(dataset, "fixture_manifest.json")
	if digest, hashErr := localHNSWAttributionRegularFileSHA256V1(datasetManifest, maxManifestBytes); hashErr != nil || digest != localHNSWAttributionFixtureManifestSHA256V1 {
		return errors.New("local HNSW repair EF curve dataset identity")
	}
	fixture, err := loadFixture(dataset)
	if err != nil || !localHNSWAttributionFixtureV1(fixture) {
		return errors.New("local HNSW repair EF curve fixture identity")
	}
	inputConfig := localHNSWAttributionInputConfigV1{Fixture: fixture, RetainedDB: retainedDB, Descriptor: filepath.Join(retainedDB, m3VariantDescriptorFileV1), CalibrationSplit: calibrationSplit, HoldoutSplit: holdoutSplit, TruthArtifact: truthArtifact, HistoricalSearchReports: historicalPaths, DescriptorSHA256: localHNSWAttributionDescriptorSHA256V1, CalibrationSplitSHA256: localHNSWAttributionCalibrationSHA256V1, HoldoutSplitSHA256: localHNSWAttributionHoldoutSHA256V1, TruthArtifactSHA256: localHNSWAttributionTruthSHA256V1, HistoricalReportSHA256: localHNSWAttributionHistoricalSHA256V1}
	inputs, err := localHNSWAttributionInputsV1(inputConfig)
	if err != nil {
		return err
	}
	historical, err := localHNSWAttributionHistoricalBaselineV1(inputConfig)
	if err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	executable, err = m8CanonicalPathV1(executable)
	if err != nil {
		return err
	}
	executableSHA, err := m8BenchmarkExecutableSHA256V1(executable)
	if err != nil {
		return err
	}
	source, err := openM8ProductionExistingAssetSetV1(retainedDB)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, source.Close()) }()
	if err := localHNSWRepairCalibrationBindDescriptorV1(source, fixture); err != nil {
		return err
	}
	calibration, err := localHNSWAttributionCalibrationV1Build(source, fixture, inputs.Calibration.Ordinals)
	if err != nil || calibration.Schema != localHNSWAttributionCalibrationSchemaV1 || len(calibration.Ordinals) != 806 {
		return errors.New("local HNSW repair EF curve query source")
	}
	repair, build, err := localHNSWAttributionBuildVariantV1(source, tempRoot, collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1, 4106003)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, repair.Close()) }()
	graph, err := localHNSWRepairCalibrationGraphV1Build(repair)
	if err != nil {
		return err
	}
	cells, err := localHNSWRepairEFCurveV1Build(context.Background(), source, repair, points, calibration.Ordinals, calibration.Queries, calibration.Truth)
	if err != nil {
		return err
	}
	var timing *localHNSWRepairEFCurveTimingV1
	if timingGate {
		value, timingErr := localHNSWRepairEFCurveTimingV1Build(context.Background(), source, repair, calibration.Ordinals, calibration.Queries)
		if timingErr != nil {
			return timingErr
		}
		timing = &value
	}
	if _, err := localHNSWAttributionInputsV1(inputConfig); err != nil {
		return fmt.Errorf("local HNSW repair EF curve inputs changed: %w", err)
	}
	if _, err := localHNSWAttributionSourceCheckoutV1(sourceCheckout, baseSHA, headSHA); err != nil || m8GitDirtyInV1(sourceCheckout) {
		return errors.New("local HNSW repair EF curve source changed")
	}
	if digest, err := m8BenchmarkExecutableSHA256V1(executable); err != nil || digest != executableSHA {
		return errors.New("local HNSW repair EF curve executable changed")
	}
	loads, err := m8PartitionLoadsV1(source.manifest)
	if err != nil || source.descriptor == nil {
		return errors.New("local HNSW repair EF curve source evidence")
	}
	disposition, err := localHNSWRepairEFCurveDispositionV1(cells)
	if err != nil {
		return err
	}
	limitations := []string{"offline calibration-only fixed-asset EF quality/work pre-gate; not product qualification", "holdout query outcomes and trusted truth contents remained unopened", "profiles are deferred; timing is ordinary-path calibration evidence only"}
	if timing == nil {
		limitations[2] = "profiles and repeated timing are deferred until a curve point clears quality"
	}
	report := localHNSWRepairEFCurveReportV1{Schema: localHNSWRepairEFCurveSchemaV1, ResultKind: "local_hnsw_repair_ef_curve_v1", Status: "valid", GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), Provenance: localHNSWAttributionProvenanceV1{Command: commandWithProvenanceAndSourceCheckoutV1("local-hnsw-repair-ef-curve", args, baseSHA, headSHA, sourceCheckout), BaseSHA: baseSHA, HeadSHA: headSHA, SourceCheckout: sourceCheckout, Executable: executable, ExecutableSHA256: executableSHA}, Host: m8ProductionHostV1(config{out: out, dataset: dataset}, retainedDB), Inputs: localHNSWAttributionInputsEvidenceV1{DatasetManifest: localHNSWAttributionFileInputV1{Path: datasetManifest, SHA256: localHNSWAttributionFixtureManifestSHA256V1}, Fixture: fixture, RetainedDB: retainedDB, Descriptor: localHNSWAttributionFileInputV1{Path: inputConfig.Descriptor, SHA256: inputConfig.DescriptorSHA256}, Calibration: localHNSWAttributionFileInputV1{Path: calibrationSplit, SHA256: inputConfig.CalibrationSplitSHA256}, CalibrationRows: len(inputs.Calibration.Ordinals), Holdout: localHNSWAttributionFileInputV1{Path: holdoutSplit, SHA256: inputConfig.HoldoutSplitSHA256}, HoldoutRows: len(inputs.Holdout.Ordinals), HoldoutStatus: "manifest_validated_query_outcomes_unopened", Truth: localHNSWAttributionFileInputV1{Path: truthArtifact, SHA256: inputConfig.TruthArtifactSHA256}, TruthStatus: "sha256_only_not_decoded", Historical: historical}, Source: localHNSWAttributionSourceEvidenceV1{IndexName: source.manifest.IndexName, PartitionGeneration: source.manifest.Generation, Partitions: source.manifest.PartitionCount, ManifestIntegrity: source.manifest.IntegrityDigest, ReadySetDigest: source.manifest.ReadySetDigest, SourceGeneration: source.manifest.SourceGeneration, SourceChecksum: source.manifest.SourceChecksum, SourceSchemaHash: source.manifest.SourceSchemaHash, SourceRows: source.manifest.SourceRowCount, RouterGeneration: source.manifest.RouterGeneration, RouterModelDigest: source.status.ModelDigest, RouterRepresentatives: source.status.Representatives, PartitionLoads: loads, Descriptor: *source.descriptor}, TopK: 10, EFSearch: append([]int(nil), points...), ProbeCounts: []int{2, int(source.manifest.PartitionCount)}, RepairBuild: build, Graph: graph, Cells: cells, Timing: timing, Disposition: disposition, Limitations: limitations}
	if err := validateLocalHNSWRepairEFCurveReportV1(report); err != nil {
		return err
	}
	if err := writeVectorPartitionSystemJSONExclusiveV1(out, report); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "report=%s ef_points=%v disposition=%s\n", out, points, disposition)
	return err
}

func validateLocalHNSWRepairEFCurveReportV1(report localHNSWRepairEFCurveReportV1) error {
	if report.Schema != localHNSWRepairEFCurveSchemaV1 || report.ResultKind != "local_hnsw_repair_ef_curve_v1" || report.Status != "valid" || report.Provenance.BaseSHA != localHNSWAttributionSourceLockV1 || report.Provenance.SourceDirty || !validLowerSHA(report.Provenance.HeadSHA) || !localHNSWAttributionSHA256V1(report.Provenance.ExecutableSHA256) || report.TopK != 10 || !localHNSWRepairEFCurvePointsValidV1(report.EFSearch) || !slices.Equal(report.ProbeCounts, []int{2, 16}) {
		return errors.New("invalid local HNSW repair EF curve identity")
	}
	if _, err := time.Parse(time.RFC3339Nano, report.GeneratedAt); err != nil || !localHNSWAttributionFixtureV1(report.Inputs.Fixture) || report.Inputs.DatasetManifest.SHA256 != localHNSWAttributionFixtureManifestSHA256V1 || report.Inputs.Descriptor.SHA256 != localHNSWAttributionDescriptorSHA256V1 || report.Inputs.Calibration.SHA256 != localHNSWAttributionCalibrationSHA256V1 || report.Inputs.Holdout.SHA256 != localHNSWAttributionHoldoutSHA256V1 || report.Inputs.Truth.SHA256 != localHNSWAttributionTruthSHA256V1 || report.Inputs.CalibrationRows != 806 || report.Inputs.HoldoutRows != 194 || report.Inputs.HoldoutStatus != "manifest_validated_query_outcomes_unopened" || report.Inputs.TruthStatus != "sha256_only_not_decoded" || report.Source.Partitions != 16 || report.Source.SourceRows != 250000 || len(report.Source.PartitionLoads) != 16 {
		return errors.New("invalid local HNSW repair EF curve inputs")
	}
	for i, historical := range report.Inputs.Historical {
		if historical.SHA256 != localHNSWAttributionHistoricalSHA256V1[i] || historical.TopologyIdentitySHA256 != localHNSWAttributionHistoricalTopologySHA256V1[i] || historical.Probe2.Probes != 2 || historical.Probe16.Probes != 16 {
			return errors.New("invalid local HNSW repair EF curve historical context")
		}
	}
	if report.RepairBuild.Schema != localHNSWAttributionBuildSchemaV1 || report.RepairBuild.Variant != string(collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1) || report.RepairBuild.Partitions != 16 || report.RepairBuild.PackBytes == 0 || report.Graph.Rows != 300000 || report.Graph.NativeReachableRows != 299968 || report.Graph.CombinedReachableRows != 300000 || report.Graph.NativeTraversalRoots != 48 || report.Graph.NativeTraversalRoots < 16 || report.Graph.AuxiliaryEdges < 2*(report.Graph.NativeTraversalRoots-16) || report.Graph.AuxiliaryCSRBytes != 8*(report.Graph.Rows+16)+4*report.Graph.AuxiliaryEdges || report.Graph.AuxiliaryMaxDegree > 9 {
		return errors.New("invalid local HNSW repair EF curve graph")
	}
	if len(report.Cells) != len(report.EFSearch) {
		return errors.New("invalid local HNSW repair EF curve cells")
	}
	routes := ""
	for i, cell := range report.Cells {
		if cell.EFSearch != report.EFSearch[i] || cell.QueryCount != 806 || !localHNSWAttributionSHA256V1(cell.RoutesSHA256) || cell.P2Work.Candidates == 0 || cell.P2Work.NativeEdges == 0 || cell.P16Work.Candidates == 0 || cell.P16Work.NativeEdges == 0 {
			return errors.New("invalid local HNSW repair EF curve cell")
		}
		if routes != "" && routes != cell.RoutesSHA256 {
			return errors.New("local HNSW repair EF curve route drift")
		}
		routes = cell.RoutesSHA256
		for _, recall := range []localHNSWAttributionRecallAggregateV1{cell.RoutingRecall, cell.P2Recall, cell.P16Recall} {
			if !localHNSWAttributionFiniteRecallV1(recall.Mean) || !localHNSWAttributionFiniteRecallV1(recall.Min) {
				return errors.New("invalid local HNSW repair EF curve recall")
			}
		}
		var terminations uint64
		for reason, count := range cell.TerminationCount {
			if !localHNSWAttributionTimingTerminationV1(reason) || math.MaxUint64-terminations < count {
				return errors.New("invalid local HNSW repair EF curve termination")
			}
			terminations += count
		}
		if terminations != 806*16 {
			return errors.New("incomplete local HNSW repair EF curve termination")
		}
	}
	if report.Timing != nil {
		if !slices.Equal(report.EFSearch, []int{128, 148}) || report.Disposition != "p2_target_crossed_smallest_passing_ef_148" {
			return errors.New("invalid local HNSW repair EF timing points")
		}
		if report.Timing.Variant != localHNSWRepairEFCurveTimingVariantV1 || !localHNSWAttributionSHA256V1(report.Timing.RoutesSHA256) || report.Timing.RoutesSHA256 != routes {
			return errors.New("invalid local HNSW repair EF timing routes")
		}
		for _, cell := range report.Timing.Cells {
			if cell.QueryCount != 806 {
				return errors.New("invalid local HNSW repair EF timing query count")
			}
		}
		want, err := localHNSWRepairEFCurveTimingGateV1Build(report.Timing.Cells, 16)
		if err != nil || want.P2QPS148Over128 != report.Timing.Gate.P2QPS148Over128 || want.P16QPS148Over128 != report.Timing.Gate.P16QPS148Over128 || want.P2P95148Over128 != report.Timing.Gate.P2P95148Over128 || want.P16P95148Over128 != report.Timing.Gate.P16P95148Over128 || want.Disposition != report.Timing.Gate.Disposition {
			return errors.New("invalid local HNSW repair EF timing")
		}
	}
	want, err := localHNSWRepairEFCurveDispositionV1(report.Cells)
	if err != nil || report.Disposition != want {
		return errors.New("invalid local HNSW repair EF curve disposition")
	}
	return nil
}
