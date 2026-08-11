package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"runtime"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

const localHNSWRepairCalibrationSchemaV1 = "treedb_local_hnsw_repair_calibration_v1"

type localHNSWRepairCalibrationSearchV1 struct {
	Results               []localHNSWAttributionQueryResultV1 `json:"results"`
	Candidates            uint64                              `json:"candidates"`
	NativeEdges           uint64                              `json:"native_edges"`
	AuxiliaryEdges        uint64                              `json:"auxiliary_edges"`
	AuxiliaryCandidates   uint64                              `json:"auxiliary_candidates"`
	AuxiliaryAdmissions   uint64                              `json:"auxiliary_admissions"`
	FrontierAdmissions    uint64                              `json:"frontier_admissions"`
	TerminationReason     string                              `json:"termination_reason"`
	VisitedOrdinalsSHA256 string                              `json:"visited_ordinals_sha256"`
}

type localHNSWRepairCalibrationVariantV1 struct {
	P2Results  []localHNSWAttributionQueryResultV1 `json:"p2_results"`
	P16Results []localHNSWAttributionQueryResultV1 `json:"p16_results"`
	P2Recall   float64                             `json:"p2_recall"`
	P16Recall  float64                             `json:"p16_recall"`
}

type localHNSWRepairCalibrationQueryV1 struct {
	Schema          string                               `json:"schema"`
	Ordinal         int                                  `json:"ordinal"`
	QueryFP32SHA256 string                               `json:"query_fp32_sha256"`
	P2Route         []uint32                             `json:"p2_route"`
	P16Route        []uint32                             `json:"p16_route"`
	Truth           []localHNSWAttributionQueryResultV1  `json:"truth"`
	RoutingRecall   float64                              `json:"routing_recall"`
	Overlay         localHNSWRepairCalibrationVariantV1  `json:"overlay_current"`
	Repair          localHNSWRepairCalibrationVariantV1  `json:"auxiliary_navigation"`
	OverlaySearches []localHNSWRepairCalibrationSearchV1 `json:"overlay_searches"`
	RepairSearches  []localHNSWRepairCalibrationSearchV1 `json:"repair_searches"`
	Query           []float32                            `json:"-"`
}

type localHNSWRepairCalibrationWorkV1 struct {
	Candidates          uint64 `json:"candidates"`
	NativeEdges         uint64 `json:"native_edges"`
	AuxiliaryEdges      uint64 `json:"auxiliary_edges"`
	AuxiliaryCandidates uint64 `json:"auxiliary_candidates"`
	AuxiliaryAdmissions uint64 `json:"auxiliary_admissions"`
}

type localHNSWRepairCalibrationAggregateV1 struct {
	QueryCount       int                                   `json:"query_count"`
	RoutingRecall    localHNSWAttributionRecallAggregateV1 `json:"routing_recall"`
	P2Recall         localHNSWAttributionRecallAggregateV1 `json:"p2_recall"`
	P16Recall        localHNSWAttributionRecallAggregateV1 `json:"p16_recall"`
	P2Work           localHNSWRepairCalibrationWorkV1      `json:"p2_work"`
	P16Work          localHNSWRepairCalibrationWorkV1      `json:"p16_work"`
	TerminationCount map[string]uint64                     `json:"termination_counts"`
}

type localHNSWRepairCalibrationSummaryV1 struct {
	Schema  string                                `json:"schema"`
	Overlay localHNSWRepairCalibrationAggregateV1 `json:"overlay_current"`
	Repair  localHNSWRepairCalibrationAggregateV1 `json:"auxiliary_navigation"`
}

type localHNSWRepairCalibrationTimingCellV1 struct {
	Repetition          int      `json:"repetition"`
	Variant             string   `json:"variant"`
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

type localHNSWRepairCalibrationTimingV1 struct {
	Cells []localHNSWRepairCalibrationTimingCellV1 `json:"cells"`
}

type localHNSWRepairCalibrationGraphV1 struct {
	Rows                  uint64 `json:"rows"`
	NativeReachableRows   uint64 `json:"native_reachable_rows"`
	CombinedReachableRows uint64 `json:"combined_reachable_rows"`
	NativeTraversalRoots  uint64 `json:"native_traversal_roots"`
	AuxiliaryEdges        uint64 `json:"auxiliary_edges"`
	AuxiliaryCSRBytes     uint64 `json:"auxiliary_csr_bytes"`
	AuxiliaryMaxDegree    uint64 `json:"auxiliary_max_degree"`
}

func localHNSWRepairCalibrationQueryV1Build(ctx context.Context, source *m8ProductionMultiGroupAssetsV1, overlay, repair *localHNSWVariantHarnessV1, ordinal int, query []float32, truth []m8CanonicalResultV1) (localHNSWRepairCalibrationQueryV1, error) {
	var out localHNSWRepairCalibrationQueryV1
	if source == nil || ordinal < 0 || !localHNSWCalibrationOrdinalV1(ordinal) || len(query) == 0 || localHNSWAttributionGraphHarnessV1(source, overlay) != nil || localHNSWAttributionGraphHarnessV1(source, repair) != nil {
		return out, errors.New("invalid local HNSW repair calibration query")
	}
	canonicalTruth, err := localHNSWAttributionCanonicalResultsV1(truth, true)
	if err != nil {
		return out, err
	}
	truthIDs, truthScores := m8CanonicalParityV1(truth, canonicalTruth)
	if !truthIDs || !truthScores {
		return out, errors.New("noncanonical local HNSW repair calibration truth")
	}
	partitions := int(source.manifest.PartitionCount)
	candidates := min(256, int(source.status.Representatives))
	if candidates < 1 {
		return out, errors.New("invalid local HNSW repair router")
	}
	p2, err := localHNSWAttributionQueryRouteV1(ctx, source, query, candidates, min(2, partitions))
	if err != nil {
		return out, err
	}
	p16, err := localHNSWAttributionQueryRouteV1(ctx, source, query, candidates, partitions)
	if err != nil || !localHNSWAttributionRoutePrefixV1(p2, p16) || !localHNSWAttributionRoutePermutationV1(p16, partitions) {
		return out, errors.New("invalid local HNSW repair calibration route")
	}
	exactLocal, err := localHNSWAttributionExactLocalV1(ctx, overlay, query, p2)
	if err != nil {
		return out, err
	}
	overlaySearches, overlayResults, err := localHNSWRepairCalibrationSearchesV1(ctx, overlay, query)
	if err != nil {
		return out, err
	}
	repairSearches, repairResults, err := localHNSWRepairCalibrationSearchesV1(ctx, repair, query)
	if err != nil {
		return out, err
	}
	overlayP2, err := localHNSWRepairCalibrationMergeV1(overlayResults, p2)
	if err != nil {
		return out, err
	}
	overlayP16, err := localHNSWRepairCalibrationMergeV1(overlayResults, p16)
	if err != nil {
		return out, err
	}
	repairP2, err := localHNSWRepairCalibrationMergeV1(repairResults, p2)
	if err != nil {
		return out, err
	}
	repairP16, err := localHNSWRepairCalibrationMergeV1(repairResults, p16)
	if err != nil {
		return out, err
	}
	out = localHNSWRepairCalibrationQueryV1{
		Schema: localHNSWRepairCalibrationSchemaV1, Ordinal: ordinal, QueryFP32SHA256: localHNSWAttributionQueryFP32SHA256V1(query), P2Route: append([]uint32(nil), p2...), P16Route: append([]uint32(nil), p16...), Truth: localHNSWAttributionQueryResultBitsV1(canonicalTruth), RoutingRecall: m8CanonicalRecallV1(canonicalTruth, exactLocal),
		Overlay:         localHNSWRepairCalibrationVariantV1{P2Results: localHNSWAttributionQueryResultBitsV1(overlayP2), P16Results: localHNSWAttributionQueryResultBitsV1(overlayP16), P2Recall: m8CanonicalRecallV1(canonicalTruth, overlayP2), P16Recall: m8CanonicalRecallV1(canonicalTruth, overlayP16)},
		Repair:          localHNSWRepairCalibrationVariantV1{P2Results: localHNSWAttributionQueryResultBitsV1(repairP2), P16Results: localHNSWAttributionQueryResultBitsV1(repairP16), P2Recall: m8CanonicalRecallV1(canonicalTruth, repairP2), P16Recall: m8CanonicalRecallV1(canonicalTruth, repairP16)},
		OverlaySearches: overlaySearches, RepairSearches: repairSearches, Query: append([]float32(nil), query...),
	}
	if !localHNSWRepairCalibrationQueryV1Valid(out, partitions) {
		return localHNSWRepairCalibrationQueryV1{}, errors.New("invalid local HNSW repair calibration evidence")
	}
	return out, nil
}

func localHNSWRepairCalibrationSearchesV1(ctx context.Context, harness *localHNSWVariantHarnessV1, query []float32) ([]localHNSWRepairCalibrationSearchV1, [][]m8CanonicalResultV1, error) {
	return localHNSWRepairCalibrationSearchesAtEFV1(ctx, harness, query, 128)
}

func localHNSWRepairCalibrationSearchesAtEFV1(ctx context.Context, harness *localHNSWVariantHarnessV1, query []float32, efSearch int) ([]localHNSWRepairCalibrationSearchV1, [][]m8CanonicalResultV1, error) {
	if harness == nil || len(query) == 0 || efSearch < 10 {
		return nil, nil, errors.New("invalid local HNSW repair search")
	}
	searches := make([]localHNSWRepairCalibrationSearchV1, len(harness.searchers))
	results := make([][]m8CanonicalResultV1, len(harness.searchers))
	for partition, searcher := range harness.searchers {
		if searcher == nil || searcher.Status().SearchRoute != collections.VectorPartitionSearchRouteHNSWSearchPackV1 {
			return nil, nil, errors.New("invalid local HNSW repair search route")
		}
		found, metrics, attribution, err := searcher.SearchWithAttributionV1(ctx, query, collections.VectorPartitionSearchOptionsV1{TopK: 10, EfSearch: efSearch})
		if err != nil || metrics.Route != collections.VectorPartitionSearchRouteHNSWSearchPackV1 || !localHNSWAttributionSearchValidV1(attribution) || metrics.AuxiliaryAdmissions > metrics.AuxiliaryCandidates {
			return nil, nil, errors.New("invalid local HNSW repair attributed search")
		}
		canonical := make([]m8CanonicalResultV1, len(found))
		for i, result := range found {
			canonical[i] = m8CanonicalResultV1{ID: result.ID, Score: result.Score}
		}
		canonical, err = localHNSWAttributionCanonicalResultsV1(canonical, false)
		if err != nil {
			return nil, nil, err
		}
		searches[partition] = localHNSWRepairCalibrationSearchV1{Results: localHNSWAttributionQueryResultBitsV1(canonical), Candidates: metrics.Candidates, NativeEdges: metrics.Edges, AuxiliaryEdges: metrics.AuxiliaryEdges, AuxiliaryCandidates: metrics.AuxiliaryCandidates, AuxiliaryAdmissions: metrics.AuxiliaryAdmissions, FrontierAdmissions: attribution.FrontierAdmissions, TerminationReason: attribution.TerminationReason, VisitedOrdinalsSHA256: attribution.VisitedOrdinalsSHA256}
		results[partition] = canonical
	}
	return searches, results, nil
}

func localHNSWRepairCalibrationMergeV1(results [][]m8CanonicalResultV1, route []uint32) ([]m8CanonicalResultV1, error) {
	if len(route) == 0 {
		return nil, errors.New("empty local HNSW repair route")
	}
	seen := make([]bool, len(results))
	var merged []m8CanonicalResultV1
	for _, partition := range route {
		if int(partition) >= len(results) || seen[partition] || len(results[partition]) == 0 {
			return nil, errors.New("invalid local HNSW repair result route")
		}
		seen[partition] = true
		merged = append(merged, results[partition]...)
	}
	return localHNSWAttributionCanonicalResultsV1(merged, false)
}

func localHNSWRepairCalibrationQueryV1Valid(query localHNSWRepairCalibrationQueryV1, partitions int) bool {
	if query.Schema != localHNSWRepairCalibrationSchemaV1 || query.Ordinal < 0 || !localHNSWCalibrationOrdinalV1(query.Ordinal) || !localHNSWAttributionSHA256V1(query.QueryFP32SHA256) || !localHNSWAttributionRoutePermutationV1(query.P16Route, partitions) || len(query.P2Route) != min(2, partitions) || !localHNSWAttributionRoutePrefixV1(query.P2Route, query.P16Route) || len(query.Truth) != 10 || !localHNSWAttributionFiniteRecallV1(query.RoutingRecall) || len(query.OverlaySearches) != partitions || len(query.RepairSearches) != partitions {
		return false
	}
	for _, value := range []float64{query.Overlay.P2Recall, query.Overlay.P16Recall, query.Repair.P2Recall, query.Repair.P16Recall} {
		if !localHNSWAttributionFiniteRecallV1(value) {
			return false
		}
	}
	for _, searches := range [][]localHNSWRepairCalibrationSearchV1{query.OverlaySearches, query.RepairSearches} {
		for _, search := range searches {
			if len(search.Results) == 0 || !localHNSWAttributionSHA256V1(search.VisitedOrdinalsSHA256) || !localHNSWAttributionTimingTerminationV1(search.TerminationReason) || search.AuxiliaryAdmissions > search.AuxiliaryCandidates {
				return false
			}
		}
	}
	return true
}

func localHNSWRepairCalibrationSummaryAddV1(summary *localHNSWRepairCalibrationSummaryV1, query localHNSWRepairCalibrationQueryV1) error {
	if summary == nil || !localHNSWRepairCalibrationQueryV1Valid(query, len(query.P16Route)) {
		return errors.New("invalid local HNSW repair summary query")
	}
	if err := localHNSWRepairCalibrationAggregateAddV1(&summary.Overlay, query.Overlay, query.RoutingRecall, query.P2Route, query.P16Route, query.OverlaySearches); err != nil {
		return err
	}
	if err := localHNSWRepairCalibrationAggregateAddV1(&summary.Repair, query.Repair, query.RoutingRecall, query.P2Route, query.P16Route, query.RepairSearches); err != nil {
		return err
	}
	return nil
}

func localHNSWRepairCalibrationAggregateAddV1(aggregate *localHNSWRepairCalibrationAggregateV1, value localHNSWRepairCalibrationVariantV1, routing float64, p2, p16 []uint32, searches []localHNSWRepairCalibrationSearchV1) error {
	if aggregate == nil || !localHNSWAttributionFiniteRecallV1(routing) || !localHNSWAttributionFiniteRecallV1(value.P2Recall) || !localHNSWAttributionFiniteRecallV1(value.P16Recall) || aggregate.QueryCount == math.MaxInt {
		return errors.New("invalid local HNSW repair aggregate")
	}
	count := aggregate.QueryCount
	if err := localHNSWAttributionRecallAddV1(&aggregate.RoutingRecall, routing, count); err != nil {
		return err
	}
	if err := localHNSWAttributionRecallAddV1(&aggregate.P2Recall, value.P2Recall, count); err != nil {
		return err
	}
	if err := localHNSWAttributionRecallAddV1(&aggregate.P16Recall, value.P16Recall, count); err != nil {
		return err
	}
	if err := localHNSWRepairCalibrationWorkAddV1(&aggregate.P2Work, p2, searches); err != nil {
		return err
	}
	if err := localHNSWRepairCalibrationWorkAddV1(&aggregate.P16Work, p16, searches); err != nil {
		return err
	}
	if aggregate.TerminationCount == nil {
		aggregate.TerminationCount = map[string]uint64{}
	}
	for _, search := range searches {
		if !localHNSWAttributionTimingTerminationV1(search.TerminationReason) || aggregate.TerminationCount[search.TerminationReason] == math.MaxUint64 {
			return errors.New("invalid local HNSW repair termination")
		}
		aggregate.TerminationCount[search.TerminationReason]++
	}
	aggregate.QueryCount++
	return nil
}

func localHNSWRepairCalibrationWorkAddV1(work *localHNSWRepairCalibrationWorkV1, route []uint32, searches []localHNSWRepairCalibrationSearchV1) error {
	if work == nil || len(route) == 0 {
		return errors.New("invalid local HNSW repair work")
	}
	for _, partition := range route {
		if int(partition) >= len(searches) {
			return errors.New("invalid local HNSW repair work route")
		}
		search := searches[partition]
		if math.MaxUint64-work.Candidates < search.Candidates || math.MaxUint64-work.NativeEdges < search.NativeEdges || math.MaxUint64-work.AuxiliaryEdges < search.AuxiliaryEdges || math.MaxUint64-work.AuxiliaryCandidates < search.AuxiliaryCandidates || math.MaxUint64-work.AuxiliaryAdmissions < search.AuxiliaryAdmissions {
			return errors.New("local HNSW repair work overflow")
		}
		work.Candidates += search.Candidates
		work.NativeEdges += search.NativeEdges
		work.AuxiliaryEdges += search.AuxiliaryEdges
		work.AuxiliaryCandidates += search.AuxiliaryCandidates
		work.AuxiliaryAdmissions += search.AuxiliaryAdmissions
	}
	return nil
}

func localHNSWRepairCalibrationSummaryFinishV1(summary *localHNSWRepairCalibrationSummaryV1) error {
	if summary == nil || summary.Schema != localHNSWRepairCalibrationSchemaV1 || summary.Overlay.QueryCount != 806 || summary.Repair.QueryCount != 806 {
		return errors.New("invalid local HNSW repair summary")
	}
	for _, aggregate := range []*localHNSWRepairCalibrationAggregateV1{&summary.Overlay, &summary.Repair} {
		count := float64(aggregate.QueryCount)
		for _, recall := range []*localHNSWAttributionRecallAggregateV1{&aggregate.RoutingRecall, &aggregate.P2Recall, &aggregate.P16Recall} {
			recall.Mean /= count
			if !localHNSWAttributionFiniteRecallV1(recall.Mean) || !localHNSWAttributionFiniteRecallV1(recall.Min) {
				return errors.New("invalid local HNSW repair recall")
			}
		}
	}
	return nil
}

func localHNSWRepairCalibrationSummaryV1Build(ctx context.Context, sidecar string, source *m8ProductionMultiGroupAssetsV1, overlay, repair *localHNSWVariantHarnessV1, ordinals []int, queries [][]float32, truth [][]m8CanonicalResultV1) (localHNSWAttributionArtifactV1, []localHNSWRepairCalibrationQueryV1, localHNSWRepairCalibrationSummaryV1, error) {
	var summary localHNSWRepairCalibrationSummaryV1
	if sidecar == "" || len(ordinals) != 806 || len(ordinals) != len(queries) || len(ordinals) != len(truth) {
		return localHNSWAttributionArtifactV1{}, nil, summary, errors.New("invalid local HNSW repair calibration alignment")
	}
	summary.Schema = localHNSWRepairCalibrationSchemaV1
	rows := make([]localHNSWRepairCalibrationQueryV1, 0, len(ordinals))
	artifact, err := localHNSWAttributionWriteGzipJSONLV1(sidecar, func(encoder *json.Encoder) (int, error) {
		for i, ordinal := range ordinals {
			if i > 0 && ordinals[i-1] >= ordinal {
				return 0, errors.New("invalid local HNSW repair calibration ordinals")
			}
			query, err := localHNSWRepairCalibrationQueryV1Build(ctx, source, overlay, repair, ordinal, queries[i], truth[i])
			if err != nil {
				return 0, err
			}
			if err := localHNSWRepairCalibrationSummaryAddV1(&summary, query); err != nil {
				return 0, err
			}
			rows = append(rows, query)
			if err := encoder.Encode(query); err != nil {
				return 0, err
			}
		}
		return len(ordinals), nil
	})
	if err != nil || artifact.Records != len(ordinals) {
		return localHNSWAttributionArtifactV1{}, nil, localHNSWRepairCalibrationSummaryV1{}, errors.Join(errors.New("write local HNSW repair calibration"), err)
	}
	if err := localHNSWRepairCalibrationSummaryFinishV1(&summary); err != nil {
		return localHNSWAttributionArtifactV1{}, nil, localHNSWRepairCalibrationSummaryV1{}, err
	}
	return artifact, rows, summary, nil
}

func localHNSWRepairCalibrationTimingV1Build(ctx context.Context, overlay, repair *localHNSWVariantHarnessV1, queries []localHNSWRepairCalibrationQueryV1) (localHNSWRepairCalibrationTimingV1, error) {
	var out localHNSWRepairCalibrationTimingV1
	if len(queries) != 806 || overlay == nil || repair == nil || len(overlay.searchers) == 0 || len(overlay.searchers) != len(repair.searchers) {
		return out, errors.New("invalid local HNSW repair timing")
	}
	for i, query := range queries {
		if (i > 0 && queries[i-1].Ordinal >= query.Ordinal) || len(query.Query) == 0 || query.QueryFP32SHA256 != localHNSWAttributionQueryFP32SHA256V1(query.Query) || !localHNSWRepairCalibrationQueryV1Valid(query, len(overlay.searchers)) {
			return out, errors.New("invalid local HNSW repair timing query")
		}
	}
	orders := [4][4]struct {
		variant string
		all     bool
	}{
		{{"overlay_current", false}, {"auxiliary_navigation", false}, {"overlay_current", true}, {"auxiliary_navigation", true}},
		{{"auxiliary_navigation", true}, {"overlay_current", true}, {"auxiliary_navigation", false}, {"overlay_current", false}},
		{{"overlay_current", true}, {"auxiliary_navigation", true}, {"overlay_current", false}, {"auxiliary_navigation", false}},
		{{"auxiliary_navigation", false}, {"overlay_current", false}, {"auxiliary_navigation", true}, {"overlay_current", true}},
	}
	for repetition, order := range orders {
		for _, item := range order {
			harness := overlay
			if item.variant == "auxiliary_navigation" {
				harness = repair
			}
			cell, err := localHNSWRepairCalibrationTimingCellV1Run(ctx, harness, queries, repetition, item.variant, item.all)
			if err != nil {
				return localHNSWRepairCalibrationTimingV1{}, err
			}
			out.Cells = append(out.Cells, cell)
		}
	}
	return out, nil
}

func localHNSWRepairCalibrationTimingCellV1Run(ctx context.Context, harness *localHNSWVariantHarnessV1, queries []localHNSWRepairCalibrationQueryV1, repetition int, variant string, all bool) (localHNSWRepairCalibrationTimingCellV1, error) {
	var cell localHNSWRepairCalibrationTimingCellV1
	if harness == nil || len(queries) != 806 || (variant != "overlay_current" && variant != "auxiliary_navigation") {
		return cell, errors.New("invalid local HNSW repair timing cell")
	}
	probes := 2
	if all {
		probes = len(harness.searchers)
	}
	if probes < 1 {
		return cell, errors.New("invalid local HNSW repair timing probes")
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	cpuBefore, cpuOK := vectorPartitionBenchmarkCPUNanos()
	started := time.Now()
	durations := make([]uint64, 0, len(queries))
	digests := make([]string, 0, len(queries))
	for _, query := range queries {
		route := query.P2Route
		if all {
			route = query.P16Route
		}
		queryStarted := time.Now()
		results, work, err := localHNSWRepairCalibrationOrdinarySearchV1(ctx, harness, route, query.Query, variant, query.QueryFP32SHA256)
		if err != nil {
			return cell, err
		}
		wantDigest, err := localHNSWRepairCalibrationFrozenResultSHA256V1(query, variant, all)
		if err != nil || localHNSWRepairCalibrationResultsSHA256V1(results) != wantDigest {
			return cell, errors.New("local HNSW repair ordinary result drift")
		}
		elapsed := uint64(time.Since(queryStarted))
		if elapsed == 0 {
			elapsed = 1
		}
		if math.MaxUint64-cell.Candidates < work.Candidates || math.MaxUint64-cell.NativeEdges < work.NativeEdges || math.MaxUint64-cell.AuxiliaryEdges < work.AuxiliaryEdges || math.MaxUint64-cell.AuxiliaryCandidates < work.AuxiliaryCandidates || math.MaxUint64-cell.AuxiliaryAdmissions < work.AuxiliaryAdmissions {
			return cell, errors.New("local HNSW repair timing overflow")
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
		return cell, errors.New("invalid local HNSW repair timing resources")
	}
	cell.Repetition, cell.Variant, cell.Probes, cell.QueryCount = repetition, variant, probes, len(queries)
	cell.ElapsedNanos, cell.QPS = elapsed, float64(len(queries))*float64(time.Second)/float64(elapsed)
	cell.P50Nanos, cell.P95Nanos, cell.P99Nanos = m8PercentileV1(durations, 50), m8PercentileV1(durations, 95), m8PercentileV1(durations, 99)
	cell.CPUAvailable, cell.AllocBytesDelta, cell.ResultSHA256 = cpuOK, after.TotalAlloc-before.TotalAlloc, digests
	if cpuOK {
		cell.CPUDeltaNanos = cpuAfter - cpuBefore
	}
	return cell, nil
}

func localHNSWRepairCalibrationFrozenResultSHA256V1(query localHNSWRepairCalibrationQueryV1, variant string, all bool) (string, error) {
	var bits []localHNSWAttributionQueryResultV1
	switch variant {
	case "overlay_current":
		bits = query.Overlay.P2Results
		if all {
			bits = query.Overlay.P16Results
		}
	case "auxiliary_navigation":
		bits = query.Repair.P2Results
		if all {
			bits = query.Repair.P16Results
		}
	default:
		return "", errors.New("invalid local HNSW repair result variant")
	}
	if len(bits) == 0 || len(bits) > 10 {
		return "", errors.New("invalid local HNSW repair frozen results")
	}
	results := make([]m8CanonicalResultV1, len(bits))
	for i, result := range bits {
		if result.ID == "" {
			return "", errors.New("invalid local HNSW repair frozen result")
		}
		results[i] = m8CanonicalResultV1{ID: result.ID, Score: math.Float32frombits(result.ScoreBits)}
	}
	canonical, err := localHNSWAttributionCanonicalResultsV1(results, false)
	if err != nil {
		return "", err
	}
	return localHNSWRepairCalibrationResultsSHA256V1(canonical), nil
}

func localHNSWRepairCalibrationOrdinarySearchV1(ctx context.Context, harness *localHNSWVariantHarnessV1, route []uint32, query []float32, variant, queryDigest string) ([]m8CanonicalResultV1, localHNSWRepairCalibrationWorkV1, error) {
	if harness == nil || len(query) == 0 || !localHNSWAttributionSHA256V1(queryDigest) || queryDigest != localHNSWAttributionQueryFP32SHA256V1(query) || (variant != "overlay_current" && variant != "auxiliary_navigation") {
		return nil, localHNSWRepairCalibrationWorkV1{}, errors.New("invalid local HNSW repair ordinary query")
	}
	seen := make([]bool, len(harness.searchers))
	var merged []m8CanonicalResultV1
	var work localHNSWRepairCalibrationWorkV1
	for _, partition := range route {
		if int(partition) >= len(harness.searchers) || seen[partition] || harness.searchers[partition] == nil {
			return nil, localHNSWRepairCalibrationWorkV1{}, errors.New("invalid local HNSW repair ordinary route")
		}
		seen[partition] = true
		found, metrics, err := harness.searchers[partition].SearchWithOptionsV1(ctx, query, collections.VectorPartitionSearchOptionsV1{TopK: 10, EfSearch: 128})
		if err != nil || metrics.Route != collections.VectorPartitionSearchRouteHNSWSearchPackV1 || metrics.AuxiliaryAdmissions > metrics.AuxiliaryCandidates || math.MaxUint64-work.Candidates < metrics.Candidates || math.MaxUint64-work.NativeEdges < metrics.Edges || math.MaxUint64-work.AuxiliaryEdges < metrics.AuxiliaryEdges || math.MaxUint64-work.AuxiliaryCandidates < metrics.AuxiliaryCandidates || math.MaxUint64-work.AuxiliaryAdmissions < metrics.AuxiliaryAdmissions {
			return nil, localHNSWRepairCalibrationWorkV1{}, errors.New("invalid local HNSW repair ordinary search")
		}
		work.Candidates += metrics.Candidates
		work.NativeEdges += metrics.Edges
		work.AuxiliaryEdges += metrics.AuxiliaryEdges
		work.AuxiliaryCandidates += metrics.AuxiliaryCandidates
		work.AuxiliaryAdmissions += metrics.AuxiliaryAdmissions
		for _, result := range found {
			merged = append(merged, m8CanonicalResultV1{ID: result.ID, Score: result.Score})
		}
	}
	canonical, err := localHNSWAttributionCanonicalResultsV1(merged, false)
	return canonical, work, err
}

func localHNSWRepairCalibrationResultsSHA256V1(results []m8CanonicalResultV1) string {
	h := sha256.New()
	h.Write([]byte("treedb-4106-local-hnsw-repair-results-v1/"))
	var raw [4]byte
	for _, result := range results {
		h.Write([]byte(result.ID))
		h.Write([]byte{0})
		binary.LittleEndian.PutUint32(raw[:], math.Float32bits(result.Score))
		h.Write(raw[:])
	}
	return hex.EncodeToString(h.Sum(nil))
}
