package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"runtime"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

const localHNSWAttributionTimingSchemaV1 = "treedb_local_hnsw_attribution_timing_v1"

type localHNSWAttributionTimingCaseV1 struct {
	Ordinal         int
	Query           []float32
	QueryFP32SHA256 string
	LowRoute        []uint32
	HighRoute       []uint32
}

type localHNSWAttributionTimingCellV1 struct {
	Repetition      int      `json:"repetition"`
	Variant         string   `json:"variant"`
	Probes          int      `json:"probes"`
	QueryCount      int      `json:"query_count"`
	ElapsedNanos    uint64   `json:"elapsed_nanos"`
	QPS             float64  `json:"qps"`
	P50Nanos        uint64   `json:"p50_nanos"`
	P95Nanos        uint64   `json:"p95_nanos"`
	P99Nanos        uint64   `json:"p99_nanos"`
	Candidates      uint64   `json:"candidates"`
	Edges           uint64   `json:"edges"`
	CPUAvailable    bool     `json:"cpu_available"`
	CPUDeltaNanos   int64    `json:"cpu_delta_nanos"`
	AllocBytesDelta uint64   `json:"alloc_bytes_delta"`
	ResultSHA256    []string `json:"result_sha256"`
}

type localHNSWAttributionTimingEvidenceV1 struct {
	Schema string                             `json:"schema"`
	Cells  []localHNSWAttributionTimingCellV1 `json:"cells"`
}

// localHNSWAttributionTimingV1 runs the fixed, ordinary-search timing order.
// It is offline evidence plumbing, not a production benchmark entrypoint.
func localHNSWAttributionTimingV1(ctx context.Context, source *m8ProductionMultiGroupAssetsV1, native, overlay *localHNSWVariantHarnessV1, cases []localHNSWAttributionTimingCaseV1) (localHNSWAttributionTimingEvidenceV1, error) {
	var out localHNSWAttributionTimingEvidenceV1
	if source == nil || len(cases) == 0 || source.manifest.PartitionCount == 0 || localHNSWAttributionGraphHarnessV1(source, native) != nil || localHNSWAttributionGraphHarnessV1(source, overlay) != nil {
		return out, errors.New("invalid local HNSW timing harness")
	}
	if err := localHNSWAttributionTimingCasesV1(cases, int(source.manifest.PartitionCount)); err != nil {
		return out, err
	}
	orders := [4][4]localHNSWAttributionTimingOrderV1{
		{{"native", false}, {"overlay", false}, {"native", true}, {"overlay", true}},
		{{"overlay", true}, {"native", true}, {"overlay", false}, {"native", false}},
		{{"native", true}, {"overlay", true}, {"native", false}, {"overlay", false}},
		{{"overlay", false}, {"native", false}, {"overlay", true}, {"native", true}},
	}
	out.Schema, out.Cells = localHNSWAttributionTimingSchemaV1, make([]localHNSWAttributionTimingCellV1, 0, 16)
	for repetition, order := range orders {
		for _, item := range order {
			harness := native
			if item.variant == "overlay" {
				harness = overlay
			}
			cell, err := localHNSWAttributionTimingCellV1Run(ctx, harness, cases, repetition, item.variant, item.all)
			if err != nil {
				return localHNSWAttributionTimingEvidenceV1{}, err
			}
			out.Cells = append(out.Cells, cell)
		}
	}
	return out, nil
}

type localHNSWAttributionTimingOrderV1 struct {
	variant string
	all     bool
}

func localHNSWAttributionTimingCasesV1(cases []localHNSWAttributionTimingCaseV1, partitions int) error {
	if len(cases) == 0 || partitions < 1 {
		return errors.New("invalid local HNSW timing query cases")
	}
	seen := make(map[int]bool, len(cases))
	for _, c := range cases {
		if c.Ordinal < 0 || !localHNSWCalibrationOrdinalV1(c.Ordinal) || seen[c.Ordinal] || len(c.Query) == 0 || c.QueryFP32SHA256 != localHNSWAttributionQueryFP32SHA256V1(c.Query) || !localHNSWAttributionRoutePermutationV1(c.HighRoute, partitions) || len(c.LowRoute) != min(2, partitions) || !localHNSWAttributionRoutePrefixV1(c.LowRoute, c.HighRoute) {
			return errors.New("invalid local HNSW timing query case")
		}
		seen[c.Ordinal] = true
	}
	return nil
}

func localHNSWAttributionTimingCellV1Run(ctx context.Context, harness *localHNSWVariantHarnessV1, cases []localHNSWAttributionTimingCaseV1, repetition int, variant string, all bool) (localHNSWAttributionTimingCellV1, error) {
	var cell localHNSWAttributionTimingCellV1
	if harness == nil || (variant != "native" && variant != "overlay") {
		return cell, errors.New("invalid local HNSW timing cell")
	}
	probes := 2
	if all {
		probes = len(harness.searchers)
	}
	if probes < 1 || (probes == 2 && len(harness.searchers) < 2) {
		return cell, errors.New("invalid local HNSW timing probes")
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	cpuBefore, cpuOK := vectorPartitionBenchmarkCPUNanos()
	started := time.Now()
	durations := make([]uint64, 0, len(cases))
	digests := make([]string, 0, len(cases))
	for _, c := range cases {
		route := c.LowRoute
		if all {
			route = c.HighRoute
		}
		queryStarted := time.Now()
		results, candidates, edges, err := localHNSWAttributionTimingSearchV1(ctx, harness, c.Query, route)
		if err != nil {
			return cell, err
		}
		elapsed := uint64(time.Since(queryStarted))
		if elapsed == 0 {
			elapsed = 1
		}
		if math.MaxUint64-cell.Candidates < candidates || math.MaxUint64-cell.Edges < edges {
			return cell, errors.New("invalid local HNSW timing measurement")
		}
		durations, digests = append(durations, elapsed), append(digests, localHNSWAttributionTimingResultsSHA256V1(results))
		cell.Candidates += candidates
		cell.Edges += edges
	}
	elapsed := uint64(time.Since(started))
	if elapsed == 0 {
		elapsed = 1
	}
	cpuAfter, cpuAfterOK := vectorPartitionBenchmarkCPUNanos()
	runtime.ReadMemStats(&after)
	if cpuOK != cpuAfterOK || cpuOK && cpuAfter < cpuBefore || after.TotalAlloc < before.TotalAlloc {
		return cell, errors.New("invalid local HNSW timing resources")
	}
	cell.Repetition, cell.Variant, cell.Probes, cell.QueryCount = repetition, variant, probes, len(cases)
	cell.ElapsedNanos, cell.QPS = elapsed, float64(len(cases))*float64(time.Second)/float64(elapsed)
	cell.P50Nanos, cell.P95Nanos, cell.P99Nanos = m8PercentileV1(durations, 50), m8PercentileV1(durations, 95), m8PercentileV1(durations, 99)
	cell.CPUAvailable, cell.AllocBytesDelta, cell.ResultSHA256 = cpuOK, after.TotalAlloc-before.TotalAlloc, digests
	if cpuOK {
		cell.CPUDeltaNanos = cpuAfter - cpuBefore
	}
	return cell, nil
}

func localHNSWAttributionTimingSearchV1(ctx context.Context, harness *localHNSWVariantHarnessV1, query []float32, route []uint32) ([]m8CanonicalResultV1, uint64, uint64, error) {
	var merged []m8CanonicalResultV1
	var candidates, edges uint64
	seen := make([]bool, len(harness.searchers))
	for _, partition := range route {
		if int(partition) >= len(harness.searchers) || seen[partition] || harness.searchers[partition] == nil {
			return nil, 0, 0, errors.New("invalid local HNSW timing route")
		}
		seen[partition] = true
		found, metrics, err := harness.searchers[partition].SearchWithOptionsV1(ctx, query, collections.VectorPartitionSearchOptionsV1{TopK: 10, EfSearch: 128})
		if err != nil || metrics.Route != collections.VectorPartitionSearchRouteHNSWSearchPackV1 || math.MaxUint64-candidates < metrics.Candidates || math.MaxUint64-edges < metrics.Edges {
			return nil, 0, 0, errors.New("invalid local HNSW timing search")
		}
		candidates, edges = candidates+metrics.Candidates, edges+metrics.Edges
		for _, result := range found {
			merged = append(merged, m8CanonicalResultV1{ID: result.ID, Score: result.Score})
		}
	}
	canonical := m8CanonicalResultsV1(merged, 10)
	if canonical == nil || len(canonical) == 0 {
		return nil, 0, 0, errors.New("invalid local HNSW timing results")
	}
	return canonical, candidates, edges, nil
}

func localHNSWAttributionTimingResultsSHA256V1(results []m8CanonicalResultV1) string {
	h := sha256.New()
	h.Write([]byte("treedb-4105-local-hnsw-timing-results-v1/"))
	var raw [4]byte
	for _, result := range results {
		h.Write([]byte(result.ID))
		h.Write([]byte{0})
		binary.LittleEndian.PutUint32(raw[:], math.Float32bits(result.Score))
		h.Write(raw[:])
	}
	return hex.EncodeToString(h.Sum(nil))
}
