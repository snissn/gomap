package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

type m0FrontierCellV1 struct {
	Repetition               int     `json:"repetition"`
	Probes                   int     `json:"probes"`
	SelectedPartitions       int     `json:"selected_partitions"`
	RouterSelectedPartitions uint64  `json:"router_selected_partitions"`
	EFSearch                 int     `json:"ef_search"`
	Queries                  int     `json:"queries"`
	Recall                   float64 `json:"recall"`
	RoutingMissSlots         uint64  `json:"routing_miss_slots"`
	Candidates               uint64  `json:"candidates"`
	Edges                    uint64  `json:"edges"`
	ElapsedNanos             uint64  `json:"elapsed_nanos"`
	QPS                      float64 `json:"qps"`
	P50Nanos                 uint64  `json:"p50_nanos"`
	P95Nanos                 uint64  `json:"p95_nanos"`
	ResultSHA256             string  `json:"result_sha256"`
	WorkSHA256               string  `json:"work_sha256"`
}
type m0FrontierReportV1 struct {
	Schema                   string             `json:"schema"`
	DB                       string             `json:"db"`
	ManifestIntegrity        string             `json:"manifest_integrity_digest"`
	ReadySet                 string             `json:"ready_set_digest"`
	PackBytes                uint64             `json:"pack_bytes"`
	AssetChecksumsSHA256     string             `json:"asset_checksums_sha256"`
	SourceGeneration         uint64             `json:"source_generation"`
	SourceChecksum           uint64             `json:"source_checksum"`
	SourceSchemaHash         uint64             `json:"source_schema_hash"`
	SourceRows               uint64             `json:"source_rows"`
	PartitionGeneration      uint64             `json:"partition_generation"`
	PartitionCount           uint32             `json:"partition_count"`
	RouterGeneration         uint64             `json:"router_generation"`
	RouterModelDigest        string             `json:"router_model_digest"`
	BalancePolicy            string             `json:"balance_policy"`
	OverlapCount             int                `json:"overlap_count"`
	Mode                     string             `json:"mode"`
	MembershipSHA256         string             `json:"membership_sha256"`
	MembershipReportSHA256   string             `json:"membership_report_sha256"`
	GraphArtifactSHA256      string             `json:"graph_artifact_sha256"`
	AssignmentArtifactSHA256 string             `json:"assignment_artifact_sha256"`
	DatasetManifestSHA256    string             `json:"dataset_manifest_sha256"`
	BinarySHA256             string             `json:"binary_sha256"`
	CalibrationSHA256        string             `json:"calibration_sha256"`
	TruthSHA256              string             `json:"truth_sha256"`
	RouterCandidates         int                `json:"router_candidates"`
	TopK                     int                `json:"top_k"`
	Measurements             []m0FrontierCellV1 `json:"measurements"`
	Cells                    []m0FrontierCellV1 `json:"cells"`
}
type m0FrontierQueryRouteV1 struct {
	Ordinal          int
	Route            []uint32
	RoutingMissSlots uint64
}

func runM0CalibrationFrontierV1(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("treedb_vector_partition_bench m0-calibration-frontier", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var db, dataset, calibration, truthCache, membershipReport, assignmentArtifact, out, probesRaw, efRaw, mode string
	candidates, topK := 0, 0
	fs.StringVar(&db, "db", "", "materialized clone")
	fs.StringVar(&dataset, "dataset", "", "frozen dataset directory")
	fs.StringVar(&calibration, "calibration", "", "frozen calibration split")
	fs.StringVar(&truthCache, "truth-cache", "", "frozen truth cache directory")
	fs.StringVar(&membershipReport, "membership-report", "", "strict M0 membership report")
	fs.StringVar(&assignmentArtifact, "assignment-artifact", "", "strict canonical assignment artifact")
	fs.StringVar(&out, "out", "", "fresh report")
	fs.StringVar(&mode, "mode", "zero", "materialized membership mode")
	fs.StringVar(&probesRaw, "probes", "1,2,4", "ordered probes")
	fs.StringVar(&efRaw, "ef", "64,80,96,128", "ordered EFs")
	fs.IntVar(&candidates, "router-candidates", 64, "router candidates")
	fs.IntVar(&topK, "top-k", 10, "top K")
	if fs.Parse(args) != nil || fs.NArg() != 0 || db == "" || dataset == "" || calibration == "" || truthCache == "" || membershipReport == "" || assignmentArtifact == "" || out == "" || (mode != "zero" && mode != "useful_only_20") || candidates < 1 || topK != 10 {
		return errors.New("M0 calibration frontier arguments")
	}
	if _, e := os.Stat(out); e == nil || !errors.Is(e, os.ErrNotExist) {
		return errors.New("M0 frontier output exists")
	}
	probes, e := m0FrontierIntsV1(probesRaw)
	if e != nil {
		return e
	}
	efs, e := m0FrontierIntsV1(efRaw)
	if e != nil {
		return e
	}
	if len(probes) != 3 || len(efs) != 4 {
		return errors.New("M0 frontier requires 3 probes and 4 EFs")
	}
	fixture, e := loadFixture(dataset)
	if e != nil {
		return e
	}
	split, splitSHA, e := loadLocalHNSWQuerySplitV1(calibration)
	if e != nil {
		return e
	}
	if len(split.Ordinals) != 806 || split.DatasetChecksum != fixture.Checksum || split.Selection != localHNSWQuerySplitSelectionV1 {
		return errors.New("M0 calibration split identity")
	}
	queries := qualificationQueriesV1(fixture)
	truthPath := m8TruthCacheArtifactPathV1(truthCache, m8TruthCacheIdentityV1(fixture, topK))
	truth, truthSHA, e := m8ReadTruthCacheV1(truthPath, fixture, len(queries), topK, uint64(fixture.Vectors), split.TruthArtifactSHA256)
	if e != nil {
		return e
	}
	h, e := openM8ProductionExistingAssetSetModeV1(db, true)
	if e != nil {
		return e
	}
	defer h.Close()
	if h.manifest.PartitionCount < 4 || h.status.Manifest.State != "ready" {
		return errors.New("M0 frontier DB status")
	}
	if h.status.Representatives == 0 || uint64(candidates) > h.status.Representatives {
		return errors.New("M0 frontier router candidate budget")
	}
	account, selected, accountSHA, e := m0FrontierAccountV1(membershipReport, h.manifest, mode)
	if e != nil {
		return e
	}
	if e = m0FrontierMembershipTopologyV1(assignmentArtifact, account, selected, h); e != nil {
		return e
	}
	searchers := make([]*collections.VectorPartitionLocalSearcherV1, len(h.manifest.Assets))
	defer closeM3PartitionSearchers(searchers)
	for i, a := range h.manifest.Assets {
		searchers[i], e = h.collection.OpenVectorPartitionLocalSearcherForOfflineAssetWithContextV1(context.Background(), h.manifest.IndexName, h.manifest, a)
		if e != nil {
			return e
		}
	}
	idMemberships, e := m0FrontierMembershipOracleV1(h)
	if e != nil {
		return e
	}
	datasetSHA, e := localHNSWAttributionRegularFileSHA256V1(filepath.Join(dataset, "fixture_manifest.json"), localHNSWQuerySplitMaxBytesV1)
	if e != nil {
		return e
	}
	executable, e := os.Executable()
	if e != nil {
		return e
	}
	binarySHA, e := m8BenchmarkExecutableSHA256V1(executable)
	if e != nil {
		return e
	}
	report := m0FrontierReportV1{Schema: "treedb_vector_partition_m0_calibration_frontier_v1", DB: db, ManifestIntegrity: h.manifest.IntegrityDigest, ReadySet: h.manifest.ReadySetDigest, AssetChecksumsSHA256: m0FrontierAssetDigestV1(h.manifest), SourceGeneration: h.manifest.SourceGeneration, SourceChecksum: h.manifest.SourceChecksum, SourceSchemaHash: h.manifest.SourceSchemaHash, SourceRows: h.manifest.SourceRowCount, PartitionGeneration: h.manifest.Generation, PartitionCount: h.manifest.PartitionCount, RouterGeneration: h.manifest.RouterGeneration, RouterModelDigest: h.status.ModelDigest, BalancePolicy: h.manifest.BalancePolicy, OverlapCount: len(h.manifest.OverlapMemberships), Mode: mode, MembershipSHA256: selected.MembershipSHA256, MembershipReportSHA256: accountSHA, GraphArtifactSHA256: account.GraphArtifactSHA256, AssignmentArtifactSHA256: account.AssignmentArtifactSHA256, DatasetManifestSHA256: datasetSHA, BinarySHA256: binarySHA, CalibrationSHA256: splitSHA, TruthSHA256: truthSHA, RouterCandidates: candidates, TopK: topK}
	for _, a := range h.manifest.Assets {
		report.PackBytes += a.Bytes
	}
	routes := make(map[int][]m0FrontierQueryRouteV1, len(probes))
	for _, p := range probes {
		routes[p], e = m0FrontierRoutesV1(h, split.Ordinals, queries, truth, idMemberships, p, candidates)
		if e != nil {
			return e
		}
	}
	canonical := m0FrontierPlanV1(probes, efs)
	for _, point := range canonical {
		if _, e = m0FrontierCellBuildV1(h, searchers, queries, truth, routes[point.Probes], point.Probes, point.EFSearch, candidates, -1); e != nil {
			return e
		}
	}
	for repetition := 0; repetition < 3; repetition++ {
		for _, point := range m0FrontierExecutionOrderV1(canonical, repetition) {
			cell, e := m0FrontierCellBuildV1(h, searchers, queries, truth, routes[point.Probes], point.Probes, point.EFSearch, candidates, repetition)
			if e != nil {
				return e
			}
			report.Measurements = append(report.Measurements, cell)
		}
	}
	report.Cells, e = m0FrontierAggregateV1(report.Measurements, canonical, len(split.Ordinals))
	if e != nil {
		return e
	}
	if !m0FrontierCellsCompleteV1(report.Cells, probes, efs, len(split.Ordinals)) {
		return errors.New("M0 frontier incomplete or duplicate cells")
	}
	if !validateM0FrontierReportV1(report, probes, efs, candidates) {
		return errors.New("M0 frontier report validation")
	}
	raw, e := json.MarshalIndent(report, "", "  ")
	if e != nil {
		return e
	}
	if e = os.MkdirAll(filepath.Dir(out), 0755); e != nil {
		return e
	}
	if e = os.WriteFile(out, append(raw, '\n'), 0644); e != nil {
		return e
	}
	_, e = fmt.Fprintf(stdout, "m0_calibration_frontier=%s cells=%d\n", out, len(report.Cells))
	return e
}

func m0FrontierAccountV1(path string, manifest collections.VectorPartitionManifestV1, mode string) (m0MembershipAccountV1, m0MembershipModeV1, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return m0MembershipAccountV1{}, m0MembershipModeV1{}, "", err
	}
	var account m0MembershipAccountV1
	if err = json.Unmarshal(raw, &account); err != nil {
		return account, m0MembershipModeV1{}, "", err
	}
	policy, ok := collections.ParseVectorPartitionOverlapPolicyV1(manifest.BalancePolicy)
	if !ok || account.Schema != "treedb_vector_partition_m0_membership_account_v1" || account.Partitions < 4 || account.Partitions > math.MaxUint32 || manifest.PartitionCount != uint32(account.Partitions) || policy.BuildIdentityDigest != account.AssignmentArtifactSHA256 {
		return account, m0MembershipModeV1{}, "", errors.New("M0 frontier membership binding")
	}
	var zero, useful, exact *m0MembershipModeV1
	for i := range account.Modes {
		switch account.Modes[i].Name {
		case "zero":
			zero = &account.Modes[i]
		case "useful_only_20":
			useful = &account.Modes[i]
		case "exact_20":
			exact = &account.Modes[i]
		}
	}
	if zero == nil || useful == nil || exact == nil || !localHNSWAttributionSHA256V1(account.GraphArtifactSHA256) || !localHNSWAttributionSHA256V1(account.AssignmentArtifactSHA256) {
		return account, m0MembershipModeV1{}, "", errors.New("M0 frontier membership dispositions")
	}
	selected, err := m0FrontierModeV1(mode, *zero, *useful, *exact, len(manifest.OverlapMemberships))
	if err != nil {
		return account, m0MembershipModeV1{}, "", err
	}
	return account, selected, m0SHA256V1(raw), nil
}

func m0FrontierModeV1(mode string, zero, useful, exact m0MembershipModeV1, overlapCount int) (m0MembershipModeV1, error) {
	if !zero.Materialize || zero.Used != 0 || zero.Useful != 0 || zero.Filler != 0 || zero.MembershipSHA256 == "" {
		return m0MembershipModeV1{}, errors.New("M0 zero disposition")
	}
	if mode == "zero" {
		if overlapCount != 0 {
			return m0MembershipModeV1{}, errors.New("M0 zero manifest overlap")
		}
		return zero, nil
	}
	if mode != "useful_only_20" || !useful.Materialize || useful.EquivalentTo != "" || useful.Rejected != "" || useful.Used == 0 || useful.Useful != useful.Used || useful.Filler != 0 || useful.MembershipSHA256 == "" || overlapCount != useful.Used || exact.MembershipSHA256 != useful.MembershipSHA256 || !exact.Materialize || exact.Used != useful.Used || exact.Useful != useful.Useful || exact.Filler != 0 {
		return m0MembershipModeV1{}, errors.New("M0 useful membership disposition")
	}
	return useful, nil
}

func validateM0FrontierReportV1(report m0FrontierReportV1, probes, efs []int, candidates int) bool {
	if report.Schema != "treedb_vector_partition_m0_calibration_frontier_v1" || report.PartitionCount < 4 || report.PartitionGeneration != 2 || report.SourceGeneration == 0 || report.SourceChecksum == 0 || report.SourceSchemaHash == 0 || report.SourceRows != 250000 || report.PackBytes == 0 || report.RouterCandidates != candidates || candidates < 1 || report.TopK != 10 || (report.Mode != "zero" && report.Mode != "useful_only_20") || (report.Mode == "zero" && report.OverlapCount != 0) || (report.Mode == "useful_only_20" && report.OverlapCount == 0) || !m0FrontierCellsCompleteV1(report.Cells, probes, efs, 806) || len(report.Measurements) != 36 {
		return false
	}
	for _, id := range []string{report.ManifestIntegrity, report.ReadySet, report.AssetChecksumsSHA256, report.RouterModelDigest, report.MembershipSHA256, report.MembershipReportSHA256, report.GraphArtifactSHA256, report.AssignmentArtifactSHA256, report.DatasetManifestSHA256, report.BinarySHA256, report.CalibrationSHA256, report.TruthSHA256} {
		if !localHNSWAttributionSHA256V1(id) {
			return false
		}
	}
	aggregates := map[[2]int]m0FrontierCellV1{}
	for _, c := range report.Cells {
		aggregates[[2]int{c.Probes, c.EFSearch}] = c
	}
	seen := map[[3]int]bool{}
	for _, m := range report.Measurements {
		k := [3]int{m.Repetition, m.Probes, m.EFSearch}
		if m.Repetition < 0 || m.Repetition > 2 || seen[k] || m.SelectedPartitions != m.Probes || m.RouterSelectedPartitions != uint64(m.Probes*806) || m.Queries != 806 || !localHNSWAttributionSHA256V1(m.ResultSHA256) || !localHNSWAttributionSHA256V1(m.WorkSHA256) {
			return false
		}
		seen[k] = true
	}
	for _, m := range report.Measurements {
		c, ok := aggregates[[2]int{m.Probes, m.EFSearch}]
		if !ok || m.Recall != c.Recall || m.Candidates != c.Candidates || m.Edges != c.Edges || m.RoutingMissSlots != c.RoutingMissSlots || m.ResultSHA256 != c.ResultSHA256 || m.WorkSHA256 != c.WorkSHA256 {
			return false
		}
	}
	return len(seen) == 36
}

func m0FrontierPlanV1(probes, efs []int) []m0FrontierCellV1 {
	points := make([]m0FrontierCellV1, 0, len(probes)*len(efs))
	for _, p := range probes {
		for _, ef := range efs {
			points = append(points, m0FrontierCellV1{Probes: p, EFSearch: ef})
		}
	}
	return points
}

func m0FrontierExecutionOrderV1(points []m0FrontierCellV1, repetition int) []m0FrontierCellV1 {
	out := append([]m0FrontierCellV1(nil), points...)
	if repetition == 1 {
		for i := range out[:len(out)/2] {
			j := len(out) - 1 - i
			out[i], out[j] = out[j], out[i]
		}
		return out
	}
	if repetition == 2 {
		alt := make([]m0FrontierCellV1, 0, len(out))
		for lo, hi := 0, len(out)-1; lo <= hi; lo, hi = lo+1, hi-1 {
			alt = append(alt, out[lo])
			if lo != hi {
				alt = append(alt, out[hi])
			}
		}
		return alt
	}
	return out
}

func m0FrontierAssetDigestV1(manifest collections.VectorPartitionManifestV1) string {
	parts := make([]string, 0, len(manifest.Assets)+1)
	for _, a := range manifest.Assets {
		parts = append(parts, fmt.Sprintf("%d/%s/%d/%s", a.PartitionID, a.ID, a.Bytes, a.Checksum))
	}
	sort.Strings(parts)
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(sum[:])
}

func m0FrontierCellsCompleteV1(cells []m0FrontierCellV1, probes, efs []int, queries int) bool {
	if len(cells) != len(probes)*len(efs) || queries != 806 {
		return false
	}
	for i, c := range cells {
		if c.Probes != probes[i/len(efs)] || c.EFSearch != efs[i%len(efs)] || c.SelectedPartitions != c.Probes || c.RouterSelectedPartitions != uint64(c.Probes*c.Queries) || c.Queries != queries || c.QPS <= 0 || c.P50Nanos == 0 || c.P95Nanos < c.P50Nanos || !localHNSWAttributionSHA256V1(c.ResultSHA256) || !localHNSWAttributionSHA256V1(c.WorkSHA256) {
			return false
		}
	}
	return true
}
func m0FrontierIntsV1(raw string) ([]int, error) {
	var out []int
	for _, s := range strings.Split(raw, ",") {
		v, e := strconv.Atoi(s)
		if e != nil || v < 1 || len(out) > 0 && out[len(out)-1] >= v {
			return nil, errors.New("invalid M0 frontier points")
		}
		out = append(out, v)
	}
	return out, nil
}
func m0FrontierCellBuildV1(h *m8ProductionMultiGroupAssetsV1, searchers []*collections.VectorPartitionLocalSearcherV1, queries [][]float64, truth [][]m8CanonicalResultV1, routes []m0FrontierQueryRouteV1, probes, ef, candidates, repetition int) (m0FrontierCellV1, error) {
	var c m0FrontierCellV1
	if probes > len(searchers) || ef < 10 {
		return c, errors.New("M0 frontier cell")
	}
	c.Repetition = repetition
	c.Probes = probes
	c.EFSearch = ef
	c.SelectedPartitions = probes
	lat := make([]uint64, 0, len(routes))
	resultHash, workHash := sha256.New(), sha256.New()
	for _, routeInput := range routes {
		ordinal := routeInput.Ordinal
		q := m8Query32V1(queries[ordinal])
		one := time.Now()
		routed, err := h.router.SearchWithContextV1(context.Background(), q, collections.VectorPartitionRouterSearchOptionsV1{Mode: collections.VectorPartitionRouterModeApproxV1, CandidateBudget: candidates, PartitionProbes: probes})
		if err != nil || len(routed.Partitions) != len(routeInput.Route) {
			return c, errors.New("M0 timed route")
		}
		for i, r := range routed.Partitions {
			if r.PartitionID != routeInput.Route[i] {
				return c, errors.New("M0 timed route drift")
			}
		}
		c.RouterSelectedPartitions += uint64(len(routed.Partitions))
		var found []m8CanonicalResultV1
		for _, partition := range routeInput.Route {
			s := searchers[partition]
			got, m, _, e := s.SearchWithAttributionV1(context.Background(), q, collections.VectorPartitionSearchOptionsV1{TopK: 10, EfSearch: ef})
			if e != nil {
				return c, e
			}
			c.Candidates += m.Candidates
			c.Edges += m.Edges
			for _, x := range got {
				found = append(found, m8CanonicalResultV1{ID: x.ID, Score: x.Score})
			}
		}
		found = m8CanonicalResultsV1(found, 10)
		if len(found) != 10 {
			return c, errors.New("M0 incomplete result")
		}
		c.Recall += m8CanonicalRecallV1(truth[ordinal], found)
		c.RoutingMissSlots += routeInput.RoutingMissSlots
		elapsed := uint64(time.Since(one).Nanoseconds())
		lat = append(lat, elapsed)
		c.ElapsedNanos += elapsed
		if _, err := fmt.Fprintf(resultHash, "%d/%v\n", ordinal, found); err != nil {
			return c, err
		}
		if _, err := fmt.Fprintf(workHash, "%d/%d/%d/%d\n", ordinal, routeInput.RoutingMissSlots, c.Candidates, c.Edges); err != nil {
			return c, err
		}
	}
	c.Queries = len(routes)
	c.Recall /= float64(c.Queries)
	if c.ElapsedNanos == 0 {
		return c, errors.New("M0 zero elapsed frontier cell")
	}
	c.QPS = float64(c.Queries) * 1e9 / float64(c.ElapsedNanos)
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	c.P50Nanos = lat[len(lat)/2]
	c.P95Nanos = lat[(len(lat)*95+99)/100-1]
	c.ResultSHA256 = hex.EncodeToString(resultHash.Sum(nil))
	c.WorkSHA256 = hex.EncodeToString(workHash.Sum(nil))
	return c, nil
}

func m0FrontierAggregateV1(measurements, canonical []m0FrontierCellV1, queries int) ([]m0FrontierCellV1, error) {
	if len(measurements) != 3*len(canonical) {
		return nil, errors.New("M0 frontier repetition count")
	}
	byPoint := make(map[[2]int][]m0FrontierCellV1, len(canonical))
	for _, m := range measurements {
		k := [2]int{m.Probes, m.EFSearch}
		byPoint[k] = append(byPoint[k], m)
	}
	out := make([]m0FrontierCellV1, 0, len(canonical))
	for _, point := range canonical {
		rows := byPoint[[2]int{point.Probes, point.EFSearch}]
		if len(rows) != 3 {
			return nil, errors.New("M0 frontier duplicate or missing measurement")
		}
		base := rows[0]
		qps, p50, p95 := make([]float64, 3), make([]uint64, 3), make([]uint64, 3)
		for i, row := range rows {
			if row.Queries != queries || row.SelectedPartitions != point.Probes || row.Recall != base.Recall || row.Candidates != base.Candidates || row.Edges != base.Edges || row.RoutingMissSlots != base.RoutingMissSlots || row.RouterSelectedPartitions != base.RouterSelectedPartitions || row.ResultSHA256 != base.ResultSHA256 || row.WorkSHA256 != base.WorkSHA256 {
				return nil, errors.New("M0 frontier invariant repetition drift")
			}
			qps[i], p50[i], p95[i] = row.QPS, row.P50Nanos, row.P95Nanos
		}
		sort.Float64s(qps)
		sort.Slice(p50, func(i, j int) bool { return p50[i] < p50[j] })
		sort.Slice(p95, func(i, j int) bool { return p95[i] < p95[j] })
		base.Repetition = 0
		base.QPS = qps[1]
		base.P50Nanos = p50[1]
		base.P95Nanos = p95[1]
		base.ElapsedNanos = 0
		out = append(out, base)
	}
	return out, nil
}

func m0FrontierRoutesV1(h *m8ProductionMultiGroupAssetsV1, ordinals []int, queries [][]float64, truth [][]m8CanonicalResultV1, idMemberships map[string][]uint32, probes, candidates int) ([]m0FrontierQueryRouteV1, error) {
	out := make([]m0FrontierQueryRouteV1, 0, len(ordinals))
	for _, ordinal := range ordinals {
		if ordinal < 0 || ordinal >= len(queries) || ordinal >= len(truth) {
			return nil, errors.New("M0 route query ordinal")
		}
		q := m8Query32V1(queries[ordinal])
		r, e := h.router.SearchWithContextV1(context.Background(), q, collections.VectorPartitionRouterSearchOptionsV1{Mode: collections.VectorPartitionRouterModeApproxV1, CandidateBudget: candidates, PartitionProbes: probes})
		if e != nil || len(r.Partitions) != probes {
			return nil, errors.New("M0 route")
		}
		selected := map[uint32]bool{}
		route := make([]uint32, len(r.Partitions))
		for i, x := range r.Partitions {
			route[i] = x.PartitionID
			selected[x.PartitionID] = true
		}
		var miss uint64
		for _, want := range truth[ordinal] {
			memberships, ok := idMemberships[want.ID]
			if !ok || len(memberships) == 0 {
				return nil, errors.New("M0 truth membership missing")
			}
			hit := false
			for _, part := range memberships {
				hit = hit || selected[part]
			}
			if !hit {
				miss++
			}
		}
		out = append(out, m0FrontierQueryRouteV1{Ordinal: ordinal, Route: route, RoutingMissSlots: miss})
	}
	return out, nil
}

func m0FrontierMembershipOracleV1(h *m8ProductionMultiGroupAssetsV1) (map[string][]uint32, error) {
	if h == nil || h.collection == nil || h.manifest.PartitionCount == 0 {
		return nil, errors.New("M0 frontier membership oracle")
	}
	_, rows, err := h.collection.ReadVectorPartitionRouterSourceRowsV1(partitionHNSWIndex)
	if err != nil {
		return nil, err
	}
	ids := make(map[uint64]string, len(rows))
	for _, row := range rows {
		id := string(row.DocumentID)
		if id == "" || row.VectorOrdinal >= uint64(len(rows)) {
			return nil, errors.New("M0 frontier source row")
		}
		if _, ok := ids[row.VectorOrdinal]; ok {
			return nil, errors.New("M0 frontier duplicate source ID")
		}
		ids[row.VectorOrdinal] = id
	}
	members := make(map[string][]uint32, len(rows))
	add := func(m collections.VectorPartitionMembershipV1) error {
		id, ok := ids[m.VectorOrdinal]
		if !ok || m.PartitionID >= h.manifest.PartitionCount {
			return errors.New("M0 frontier membership")
		}
		for _, partition := range members[id] {
			if partition == m.PartitionID {
				return nil
			}
		}
		members[id] = append(members[id], m.PartitionID)
		return nil
	}
	for _, m := range h.manifest.Memberships {
		if err := add(m); err != nil {
			return nil, err
		}
	}
	for _, m := range h.manifest.OverlapMemberships {
		if err := add(m); err != nil {
			return nil, err
		}
	}
	return members, nil
}

func m0FrontierMembershipTopologyV1(path string, account m0MembershipAccountV1, selected m0MembershipModeV1, h *m8ProductionMultiGroupAssetsV1) error {
	raw, err := os.ReadFile(path)
	if err != nil || m0SHA256V1(raw) != account.AssignmentArtifactSHA256 {
		return errors.New("M0 frontier assignment artifact binding")
	}
	artifact, err := vectorpartition.DecodeArtifact(raw, len(raw))
	if err != nil || artifact.Config.Partitions != account.Partitions || uint32(account.Partitions) != h.manifest.PartitionCount {
		return errors.New("M0 frontier assignment artifact")
	}
	capacity, err := m3OverlapCapacityV1(artifact, m0OverlapRatioV1)
	if err != nil {
		return err
	}
	config := vectorpartition.OverlapConfig{}
	if selected.Name == "useful_only_20" {
		config = vectorpartition.OverlapConfig{Ratio: m0OverlapRatioV1, Capacity: capacity}
	}
	overlap, err := vectorpartition.BuildOverlap(artifact, config)
	if err != nil {
		return err
	}
	digest, err := m0MembershipDigestV1(overlap.Memberships)
	if err != nil || digest != selected.MembershipSHA256 {
		return errors.New("M0 frontier selected membership")
	}
	_, rows, err := h.collection.VectorPartitionSourceOrdinalsV1(partitionHNSWIndex)
	if err != nil {
		return err
	}
	sourceOrdinals, err := m3SourceOrdinalsByArtifactID(artifact, rows)
	if err != nil {
		return err
	}
	expected := make([]collections.VectorPartitionMembershipV1, 0, overlap.Used)
	for _, membership := range overlap.Memberships {
		if !membership.Home {
			expected = append(expected, collections.VectorPartitionMembershipV1{VectorOrdinal: uint64(sourceOrdinals[membership.VectorOrdinal]), PartitionID: uint32(membership.Partition)})
		}
	}
	return m0FrontierManifestMembershipsEqualV1(expected, h.manifest.OverlapMemberships)
}

func m0FrontierManifestMembershipsEqualV1(left, right []collections.VectorPartitionMembershipV1) error {
	left = append([]collections.VectorPartitionMembershipV1(nil), left...)
	right = append([]collections.VectorPartitionMembershipV1(nil), right...)
	less := func(values []collections.VectorPartitionMembershipV1) {
		sort.Slice(values, func(i, j int) bool {
			return values[i].VectorOrdinal < values[j].VectorOrdinal || values[i].VectorOrdinal == values[j].VectorOrdinal && values[i].PartitionID < values[j].PartitionID
		})
	}
	less(left)
	less(right)
	if len(left) != len(right) {
		return errors.New("M0 frontier materialized overlap count")
	}
	for i := range left {
		if left[i] != right[i] {
			return errors.New("M0 frontier materialized overlap membership")
		}
	}
	return nil
}
