package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

// m0-frontier-diagnose is a bounded, read-only calibration diagnostic for an
// unexpected frontier result. It never opens the DB read-write or queries the
// protected holdout split.
type m0FrontierDiagnosticQueryV1 struct {
	Query                 int      `json:"query_ordinal"`
	Route                 []uint32 `json:"route"`
	TruthAtRouteRank      [5]uint8 `json:"truth_slots_at_route_rank"`
	Top10ChangedAtRank    [5]bool  `json:"top10_changed_at_route_rank"`
	NewTop10IDsAtRank     [5]uint8 `json:"new_top10_ids_at_route_rank"`
	NewTruthIDsAtRank     [5]uint8 `json:"new_truth_ids_at_route_rank"`
	ExactTruthAtRouteRank [5]uint8 `json:"exact_truth_slots_at_route_rank"`
}
type m0FrontierDiagnosticBindingV1 struct {
	ArtifactOrdinal int    `json:"artifact_ordinal"`
	StableID        string `json:"stable_id"`
	DocumentID      string `json:"document_id"`
	SourceOrdinal   int    `json:"source_ordinal"`
	AssignedPack    int    `json:"assigned_pack"`
	ManifestPack    uint32 `json:"manifest_pack"`
}
type m0FrontierRouterSweepCellV1 struct {
	Mode            string    `json:"mode"`
	CandidateBudget int       `json:"candidate_budget"`
	Probes          int       `json:"probes"`
	Queries         int       `json:"queries"`
	TruthRankSlots  [5]uint64 `json:"truth_slots_at_route_rank"`
	Candidates      uint64    `json:"router_candidates"`
	Edges           uint64    `json:"router_edges"`
	ElapsedNanos    uint64    `json:"router_elapsed_nanos"`
	P50Nanos        uint64    `json:"router_p50_nanos"`
	P95Nanos        uint64    `json:"router_p95_nanos"`
}
type m0FrontierDiagnosticV1 struct {
	Schema                    string                          `json:"schema"`
	CalibrationSHA256         string                          `json:"calibration_sha256"`
	GraphArtifactSHA256       string                          `json:"graph_artifact_sha256"`
	AssignmentSHA256          string                          `json:"assignment_artifact_sha256"`
	ManifestIntegrity         string                          `json:"manifest_integrity_digest"`
	EFSearch                  int                             `json:"ef_search"`
	TruthRankSlots            [5]uint64                       `json:"truth_slots_at_route_rank"`
	ExactTruthRankSlots       [5]uint64                       `json:"exact_truth_slots_at_route_rank"`
	ApproxMissSiblingSelected uint64                          `json:"approx_miss_slots_with_selected_original_component_sibling"`
	Top10ChangedQueries       [5]uint64                       `json:"top10_changed_queries_at_route_rank"`
	NewTop10IDs               [5]uint64                       `json:"new_top10_ids_at_route_rank"`
	NewTruthIDs               [5]uint64                       `json:"new_truth_ids_at_route_rank"`
	RouterSweep               []m0FrontierRouterSweepCellV1   `json:"router_sweep"`
	Bindings                  []m0FrontierDiagnosticBindingV1 `json:"binding_spot_checks"`
	Queries                   []m0FrontierDiagnosticQueryV1   `json:"queries"`
}

func runM0FrontierDiagnoseV1(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("treedb_vector_partition_bench m0-frontier-diagnose", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var db, dataset, calibration, truthCache, artifactPath, graphArtifactPath, membershipReport, out string
	ef := 0
	fs.StringVar(&db, "db", "", "materialized clone")
	fs.StringVar(&dataset, "dataset", "", "frozen dataset")
	fs.StringVar(&calibration, "calibration", "", "frozen calibration split")
	fs.StringVar(&truthCache, "truth-cache", "", "frozen truth cache")
	fs.StringVar(&artifactPath, "artifact", "", "frozen graph assignment artifact")
	fs.StringVar(&graphArtifactPath, "graph-artifact", "", "frozen source graph artifact")
	fs.StringVar(&membershipReport, "membership-report", "", "strict membership report")
	fs.StringVar(&out, "out", "", "fresh diagnostic JSON")
	fs.IntVar(&ef, "ef-search", 128, "EF search")
	if fs.Parse(args) != nil || fs.NArg() != 0 || db == "" || dataset == "" || calibration == "" || truthCache == "" || artifactPath == "" || graphArtifactPath == "" || membershipReport == "" || out == "" || ef < 1 {
		return errors.New("M0 frontier diagnostic arguments")
	}
	if _, err := os.Stat(out); err == nil || !errors.Is(err, os.ErrNotExist) {
		return errors.New("M0 frontier diagnostic output exists")
	}
	fixture, err := loadFixture(dataset)
	if err != nil {
		return err
	}
	if err := validateFixture(fixture); err != nil {
		return err
	}
	split, splitSHA, err := loadLocalHNSWQuerySplitV1(calibration)
	if err != nil {
		return err
	}
	if len(split.Ordinals) != 806 || split.DatasetChecksum != fixture.Checksum || split.Selection != localHNSWQuerySplitSelectionV1 || m0FrontierCalibrationOrdinalsV1(split.Ordinals, fixture.Queries) != nil {
		return errors.New("M0 diagnostic calibration identity")
	}
	queries := qualificationQueriesV1(fixture)
	truthPath := m8TruthCacheArtifactPathV1(truthCache, m8TruthCacheIdentityV1(fixture, 10))
	truth, _, err := m8ReadTruthCacheV1(truthPath, fixture, len(queries), 10, uint64(fixture.Vectors), split.TruthArtifactSHA256)
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(artifactPath)
	if err != nil {
		return err
	}
	artifact, err := vectorpartition.DecodeArtifact(raw, len(raw))
	if err != nil {
		return err
	}
	h, err := openM8ProductionExistingAssetSetModeV1(db, true)
	if err != nil {
		return err
	}
	defer h.Close()
	account, selected, _, err := m0FrontierAccountV1(membershipReport, h.manifest, "zero")
	if err != nil {
		return err
	}
	if account.AssignmentArtifactSHA256 != m0SHA256V1(raw) || artifact.Config.Partitions != int(h.manifest.PartitionCount) {
		return errors.New("M0 diagnostic artifact binding")
	}
	if err := m0FrontierMembershipTopologyV1(artifactPath, account, selected, fixture, h); err != nil {
		return fmt.Errorf("M0 diagnostic topology: %w", err)
	}
	graphRaw, err := os.ReadFile(graphArtifactPath)
	if err != nil {
		return err
	}
	graph, err := vectorpartition.DecodeArtifact(graphRaw, len(graphRaw))
	if err != nil || m0SHA256V1(graphRaw) != account.GraphArtifactSHA256 || len(graph.IDs) != len(artifact.IDs) {
		return errors.New("M0 diagnostic graph artifact binding")
	}
	originalByID := make(map[string]int, len(graph.IDs))
	for i, id := range graph.IDs {
		originalByID[id] = graph.Assignment[i]
	}
	childParent := make(map[int]int, artifact.Config.Partitions)
	for i, id := range artifact.IDs {
		parent, ok := originalByID[id]
		if !ok {
			return errors.New("M0 diagnostic graph ID")
		}
		child := artifact.Assignment[i]
		if prior, ok := childParent[child]; ok && prior != parent {
			return errors.New("M0 diagnostic p32 crosses original component")
		}
		childParent[child] = parent
	}
	searchers := make([]*collections.VectorPartitionLocalSearcherV1, len(h.manifest.Assets))
	defer closeM3PartitionSearchers(searchers)
	for i, a := range h.manifest.Assets {
		searchers[i], err = h.collection.OpenVectorPartitionLocalSearcherForOfflineAssetWithContextV1(context.Background(), h.manifest.IndexName, h.manifest, a)
		if err != nil {
			return err
		}
	}
	members, err := m0FrontierMembershipOracleV1(h)
	if err != nil {
		return err
	}
	report := m0FrontierDiagnosticV1{Schema: "treedb_vector_partition_m0_frontier_diagnostic_v1", CalibrationSHA256: splitSHA, GraphArtifactSHA256: account.GraphArtifactSHA256, AssignmentSHA256: account.AssignmentArtifactSHA256, ManifestIntegrity: h.manifest.IntegrityDigest, EFSearch: ef}
	report.RouterSweep, err = m0FrontierRouterSweepV1(h, split.Ordinals, queries, truth, members)
	if err != nil {
		return fmt.Errorf("M0 diagnostic router sweep: %w", err)
	}
	if !m0FrontierRouterSweepCompleteV1(report.RouterSweep) {
		return errors.New("M0 diagnostic router sweep")
	}
	for _, ordinal := range split.Ordinals {
		q := m8Query32V1(queries[ordinal])
		routed, err := h.router.SearchWithContextV1(context.Background(), q, collections.VectorPartitionRouterSearchOptionsV1{Mode: collections.VectorPartitionRouterModeApproxV1, CandidateBudget: 64, PartitionProbes: 4})
		if err != nil || len(routed.Partitions) != 4 {
			return errors.New("M0 diagnostic route")
		}
		row := m0FrontierDiagnosticQueryV1{Query: ordinal, Route: make([]uint32, 4)}
		for i, r := range routed.Partitions {
			row.Route[i] = r.PartitionID
		}
		for _, want := range truth[ordinal] {
			rank := 0
			for i, p := range row.Route {
				for _, member := range members[want.ID] {
					if member == p && rank == 0 {
						rank = i + 1
					}
				}
			}
			row.TruthAtRouteRank[rank]++
			report.TruthRankSlots[rank]++
			if rank == 0 {
				parent, ok := originalByID[want.ID]
				if !ok {
					return errors.New("M0 diagnostic truth ID absent from graph artifact")
				}
				for _, p := range row.Route {
					if childParent[int(p)] == parent {
						report.ApproxMissSiblingSelected++
						break
					}
				}
			}
		}
		if h.status.Representatives == 0 || h.status.Representatives > uint64(^uint(0)>>1) {
			return errors.New("M0 diagnostic exact router representatives")
		}
		exact, err := h.router.SearchWithContextV1(context.Background(), q, collections.VectorPartitionRouterSearchOptionsV1{Mode: collections.VectorPartitionRouterModeExactV1, CandidateBudget: int(h.status.Representatives), PartitionProbes: 4})
		if err != nil || len(exact.Partitions) != 4 {
			return errors.New("M0 diagnostic exact route")
		}
		for _, want := range truth[ordinal] {
			rank := 0
			for i, p := range exact.Partitions {
				for _, member := range members[want.ID] {
					if member == p.PartitionID && rank == 0 {
						rank = i + 1
					}
				}
			}
			row.ExactTruthAtRouteRank[rank]++
			report.ExactTruthRankSlots[rank]++
		}
		var prior []m8CanonicalResultV1
		for rank, p := range row.Route {
			got, _, _, e := searchers[p].SearchWithAttributionV1(context.Background(), q, collections.VectorPartitionSearchOptionsV1{TopK: 10, EfSearch: ef})
			if e != nil {
				return e
			}
			combined := append(append([]m8CanonicalResultV1(nil), prior...), func() []m8CanonicalResultV1 {
				out := make([]m8CanonicalResultV1, len(got))
				for i, x := range got {
					out[i] = m8CanonicalResultV1{ID: x.ID, Score: x.Score}
				}
				return out
			}()...)
			now := m8CanonicalResultsV1(combined, 10)
			if len(now) != 10 {
				return errors.New("M0 diagnostic result")
			}
			if rank > 0 && !m0FrontierSameIDsV1(prior, now) {
				row.Top10ChangedAtRank[rank+1] = true
				report.Top10ChangedQueries[rank+1]++
			}
			seen := map[string]bool{}
			for _, x := range prior {
				seen[x.ID] = true
			}
			truthIDs := map[string]bool{}
			for _, x := range truth[ordinal] {
				truthIDs[x.ID] = true
			}
			for _, x := range now {
				if !seen[x.ID] {
					row.NewTop10IDsAtRank[rank+1]++
					report.NewTop10IDs[rank+1]++
					if truthIDs[x.ID] {
						row.NewTruthIDsAtRank[rank+1]++
						report.NewTruthIDs[rank+1]++
					}
				}
			}
			prior = now
		}
		report.Queries = append(report.Queries, row)
	}
	report.Bindings, err = m0FrontierBindingChecksV1(h, artifact)
	if err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(out), 0755); err != nil {
		return err
	}
	if err = os.WriteFile(out, append(encoded, '\n'), 0644); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "m0_frontier_diagnostic=%s queries=%d\n", out, len(report.Queries))
	return err
}

func m0FrontierRouterSweepV1(h *m8ProductionMultiGroupAssetsV1, ordinals []int, queries [][]float64, truth [][]m8CanonicalResultV1, members map[string][]uint32) ([]m0FrontierRouterSweepCellV1, error) {
	if h.status.Representatives == 0 || h.status.Representatives > uint64(^uint(0)>>1) {
		return nil, errors.New("M0 diagnostic router sweep status")
	}
	cells := make([]m0FrontierRouterSweepCellV1, 0, 12)
	for _, budget := range []int{64, 128, 256} {
		for _, probes := range []int{1, 2, 4} {
			cells = append(cells, m0FrontierRouterSweepCellV1{Mode: collections.VectorPartitionRouterModeApproxV1, CandidateBudget: budget, Probes: probes})
		}
	}
	for _, probes := range []int{1, 2, 4} {
		cells = append(cells, m0FrontierRouterSweepCellV1{Mode: collections.VectorPartitionRouterModeExactV1, CandidateBudget: int(h.status.Representatives), Probes: probes})
	}
	for i := range cells {
		lat := make([]uint64, 0, len(ordinals))
		for _, ordinal := range ordinals {
			started := time.Now()
			route, err := h.router.SearchWithContextV1(context.Background(), m8Query32V1(queries[ordinal]), collections.VectorPartitionRouterSearchOptionsV1{Mode: cells[i].Mode, CandidateBudget: cells[i].CandidateBudget, PartitionProbes: cells[i].Probes})
			elapsed := uint64(time.Since(started).Nanoseconds())
			if err != nil || len(route.Partitions) != cells[i].Probes {
				return nil, errors.New("M0 diagnostic sweep route")
			}
			lat = append(lat, elapsed)
			cells[i].ElapsedNanos += elapsed
			cells[i].Candidates += route.Status.Candidates
			cells[i].Edges += route.Status.Edges
			for _, want := range truth[ordinal] {
				rank := 0
				for pos, p := range route.Partitions {
					for _, member := range members[want.ID] {
						if member == p.PartitionID && rank == 0 {
							rank = pos + 1
						}
					}
				}
				cells[i].TruthRankSlots[rank]++
			}
		}
		sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
		cells[i].Queries = len(ordinals)
		cells[i].P50Nanos = lat[len(lat)/2]
		cells[i].P95Nanos = lat[(len(lat)*95+99)/100-1]
	}
	return cells, nil
}

func m0FrontierRouterSweepCompleteV1(cells []m0FrontierRouterSweepCellV1) bool {
	if len(cells) != 12 {
		return false
	}
	seen := map[string]bool{}
	for _, c := range cells {
		key := fmt.Sprintf("%s/%d/%d", c.Mode, c.CandidateBudget, c.Probes)
		if seen[key] || c.Queries != 806 || c.Candidates == 0 || c.ElapsedNanos == 0 || c.P50Nanos == 0 || c.P95Nanos < c.P50Nanos {
			return false
		}
		seen[key] = true
	}
	return true
}

func m0FrontierSameIDsV1(a, b []m8CanonicalResultV1) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ID != b[i].ID {
			return false
		}
	}
	return true
}

func m0FrontierBindingChecksV1(h *m8ProductionMultiGroupAssetsV1, artifact vectorpartition.Artifact) ([]m0FrontierDiagnosticBindingV1, error) {
	_, sourceRows, err := h.collection.VectorPartitionSourceOrdinalsV1(partitionHNSWIndex)
	if err != nil {
		return nil, err
	}
	mapping, err := m3SourceOrdinalsByArtifactID(artifact, sourceRows)
	if err != nil {
		return nil, err
	}
	_, routerRows, err := h.collection.ReadVectorPartitionRouterSourceRowsV1(partitionHNSWIndex)
	if err != nil {
		return nil, err
	}
	docs := map[uint64]string{}
	for _, row := range routerRows {
		if _, ok := docs[row.VectorOrdinal]; ok {
			return nil, errors.New("M0 diagnostic duplicate router ordinal")
		}
		docs[row.VectorOrdinal] = string(row.DocumentID)
	}
	home := map[uint64]uint32{}
	for _, m := range h.manifest.Memberships {
		if _, ok := home[m.VectorOrdinal]; ok {
			return nil, errors.New("M0 diagnostic duplicate manifest membership")
		}
		home[m.VectorOrdinal] = m.PartitionID
	}
	samples := []int{0, len(artifact.IDs) / 7, len(artifact.IDs) / 3, len(artifact.IDs) / 2, 2 * len(artifact.IDs) / 3, 6 * len(artifact.IDs) / 7, len(artifact.IDs) - 1}
	out := make([]m0FrontierDiagnosticBindingV1, 0, len(samples))
	for _, ordinal := range samples {
		source := mapping[ordinal]
		pack, ok := home[uint64(source)]
		doc := docs[uint64(source)]
		if !ok || doc == "" || pack != uint32(artifact.Assignment[ordinal]) {
			return nil, errors.New("M0 diagnostic source ID pack binding")
		}
		out = append(out, m0FrontierDiagnosticBindingV1{ArtifactOrdinal: ordinal, StableID: artifact.IDs[ordinal], DocumentID: doc, SourceOrdinal: source, AssignedPack: artifact.Assignment[ordinal], ManifestPack: pack})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ArtifactOrdinal < out[j].ArtifactOrdinal })
	return out, nil
}
