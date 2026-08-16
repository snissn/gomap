package main

// This command is deliberately narrower than local-hnsw-attribution.  It is
// an offline, calibration-only diagnostic for the frozen M18/eFC256 assets;
// it neither publishes an asset nor changes a serving default.

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
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

const localHNSWM18EdgeDiagnosisSchemaV1 = "treedb_local_hnsw_m18_edge_diagnosis_v1"

const (
	localHNSWM18DescriptorSHA256V1      = "d1bca744bbdb20c59f18397bd20f043955c2c71bce16bc34eaf1a9ad006923f2"
	localHNSWM18AssignmentSHA256V1      = "b07ab6272598447ee517d41665305af776ba806bb94033046b687e283a786040"
	localHNSWM18GraphSHA256V1           = "57ad36d923c5fdb701a082727fd24efdcf0c6ac0e24efeda28ca11f232a65f1d"
	localHNSWM18ShardGenerationSHA256V1 = "5aa286730362968994cde3c52daff6dcbe562e4377e08cc48ed1721abc4955e5"
)

var localHNSWM18EdgeDiagnosisEFV1 = []int{80, 81, 88, 96}
var localHNSWM18EdgeDiagnosisPacksV1 = []uint32{0, 1, 3, 16, 36}

type localHNSWM18EdgeDiagnosisCellV1 struct {
	EFSearch       int                                   `json:"ef_search"`
	Queries        int                                   `json:"queries"`
	EndToEndRecall localHNSWAttributionRecallAggregateV1 `json:"end_to_end_recall"`
	Work           localHNSWAttributionQueryWorkV1       `json:"work"`
	HardMisses     []localHNSWM18EdgeDiagnosisHardMissV1 `json:"hard_misses"`
	Traces         []localHNSWM18EdgeTraceV1             `json:"-"`
	SelectedWork   []localHNSWM18EdgeDiagnosisPackWorkV1 `json:"selected_pack_work"`
}
type localHNSWM18EdgeDiagnosisPackWorkV1 struct {
	Partition uint32                          `json:"partition"`
	Work      localHNSWAttributionQueryWorkV1 `json:"work"`
}

type localHNSWM18EdgeDiagnosisPackConstructionV1 struct {
	Partition    uint32                                   `json:"partition"`
	Construction localHNSWAttributionConstructionTotalsV1 `json:"construction"`
}

type localHNSWM18EdgeDiagnosisPackNeighborhoodV1 struct {
	Partition    uint32                                   `json:"partition"`
	Neighborhood localHNSWAttributionNeighborhoodOracleV1 `json:"neighborhood"`
}

type localHNSWM18EdgeDiagnosisHardMissV1 struct {
	QueryOrdinal int    `json:"query_ordinal"`
	QuerySHA256  string `json:"query_fp32_sha256"`
	RecallBits   uint32 `json:"recall_bits"`
	Rank         string `json:"rank"`
}

type localHNSWM18EdgeDiagnosisReportV1 struct {
	Schema               string                                         `json:"schema"`
	ResultKind           string                                         `json:"result_kind"`
	Status               string                                         `json:"status"`
	GeneratedAt          string                                         `json:"generated_at"`
	Provenance           localHNSWAttributionProvenanceV1               `json:"provenance"`
	Variant              string                                         `json:"variant"`
	Probes               int                                            `json:"probes"`
	EFSearch             []int                                          `json:"ef_search"`
	Calibration          localHNSWAttributionFileInputV1                `json:"calibration_split"`
	Truth                localHNSWAttributionFileInputV1                `json:"truth_artifact"`
	Descriptor           localHNSWAttributionFileInputV1                `json:"retained_descriptor"`
	RawSidecar           localHNSWAttributionFileInputV1                `json:"raw_sidecar"`
	Manifest             string                                         `json:"manifest_integrity_digest"`
	Source               localHNSWAttributionSourceEvidenceV1           `json:"source"`
	Build                localHNSWAttributionBuildEvidenceV1            `json:"build"`
	Construction         localHNSWAttributionConstructionTotalsV1       `json:"construction"`
	Neighborhood         localHNSWAttributionNeighborhoodOracleV1       `json:"neighborhood"`
	SelectedPacks        []uint32                                       `json:"selected_packs"`
	SelectedDiagnostics  []collections.VectorPartitionPackDiagnosticsV1 `json:"selected_pack_diagnostics"`
	SelectedConstruction []localHNSWM18EdgeDiagnosisPackConstructionV1  `json:"selected_pack_construction"`
	SelectedNeighborhood []localHNSWM18EdgeDiagnosisPackNeighborhoodV1  `json:"selected_pack_neighborhood"`
	Cells                []localHNSWM18EdgeDiagnosisCellV1              `json:"cells"`
	Limitations          []string                                       `json:"limitations"`
}

func localHNSWM18EdgeDiagnosisReportValidateV1(r localHNSWM18EdgeDiagnosisReportV1) error {
	if r.Schema != localHNSWM18EdgeDiagnosisSchemaV1 || r.ResultKind != "local_hnsw_m18_edge_diagnosis_v1" || r.Status != "valid" || r.Variant != string(collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1) || r.Probes != 2 || !localHNSWM18EdgeDiagnosisPointsV1(r.EFSearch) || !slices.Equal(r.SelectedPacks, localHNSWM18EdgeDiagnosisPacksV1) || len(r.SelectedDiagnostics) != len(r.SelectedPacks) || len(r.SelectedConstruction) != len(r.SelectedPacks) || len(r.SelectedNeighborhood) != len(r.SelectedPacks) || r.Calibration.SHA256 != localHNSWAttributionCalibrationSHA256V1 || r.Truth.SHA256 != localHNSWAttributionTruthSHA256V1 || r.Descriptor.SHA256 != localHNSWM18DescriptorSHA256V1 || !localHNSWAttributionSHA256V1(r.RawSidecar.SHA256) || r.Source.Partitions != 40 || r.Source.SourceRows != 250000 || r.Source.ManifestIntegrity != r.Manifest || r.Source.Descriptor.ArtifactSHA256 != localHNSWM18AssignmentSHA256V1 || r.Source.Descriptor.GraphArtifactSHA256 != localHNSWM18GraphSHA256V1 || r.Source.Descriptor.ShardGenerationDigest != localHNSWM18ShardGenerationSHA256V1 || r.Source.Descriptor.PartitionHNSWM != 18 || r.Source.Descriptor.PartitionHNSWEfC != 256 || len(r.Source.PartitionLoads) != 40 || r.Build.Variant != r.Variant || r.Build.Partitions != 40 || len(r.Cells) != len(r.EFSearch) {
		return errors.New("invalid M18 edge diagnosis report")
	}
	for i, p := range r.SelectedPacks {
		if r.SelectedConstruction[i].Partition != p || r.SelectedConstruction[i].Construction.FinalSurvivors == 0 || r.SelectedConstruction[i].Construction.OriginOrder != localHNSWAttributionConstructionOriginOrderV1 || r.SelectedNeighborhood[i].Partition != p || r.SelectedNeighborhood[i].Neighborhood.Schema != localHNSWAttributionNeighborhoodOracleSchemaV1 || r.SelectedNeighborhood[i].Neighborhood.CandidateSamples == 0 || len(r.SelectedNeighborhood[i].Neighborhood.PackDiagnostics) != 1 {
			return errors.New("invalid M18 selected construction")
		}
	}
	for i, cell := range r.Cells {
		if cell.EFSearch != r.EFSearch[i] || cell.Queries != 806 || cell.Work.Candidates == 0 || !localHNSWAttributionQueryUtilityConservedV1(cell.Work.Utility, cell.Work.Edges) || !localHNSWAttributionFiniteRecallV1(cell.EndToEndRecall.Mean) || len(cell.HardMisses) > 32 || len(cell.SelectedWork) != len(r.SelectedPacks) {
			return errors.New("invalid M18 edge diagnosis cell")
		}
		for j, p := range r.SelectedPacks {
			work := cell.SelectedWork[j]
			if work.Partition != p || work.Work.Candidates == 0 || !localHNSWAttributionQueryUtilityConservedV1(work.Work.Utility, work.Work.Edges) {
				return errors.New("invalid M18 selected pack work")
			}
		}
	}
	return nil
}

func localHNSWM18EdgeDiagnosisSelectedNeighborhoodV1(h *localHNSWVariantHarnessV1, diagnostics []collections.VectorPartitionPackDiagnosticsV1) ([]localHNSWM18EdgeDiagnosisPackNeighborhoodV1, error) {
	if h == nil || len(h.searchers) != 40 || len(diagnostics) != 40 || len(h.constructionEvidence.Partitions) != 40 {
		return nil, errors.New("invalid M18 neighborhood partition count")
	}
	out := make([]localHNSWM18EdgeDiagnosisPackNeighborhoodV1, len(localHNSWM18EdgeDiagnosisPacksV1))
	for i, p := range localHNSWM18EdgeDiagnosisPacksV1 {
		one := *h
		one.searchers = []*collections.VectorPartitionLocalSearcherV1{h.searchers[p]}
		one.documentIDs = [][]string{h.documentIDs[p]}
		one.constructionEvidence = h.constructionEvidence
		one.constructionEvidence.Partitions = []collections.VectorPartitionConstructionPartitionEvidenceV1{h.constructionEvidence.Partitions[p]}
		neighborhood, err := localHNSWAttributionNeighborhoodOracleV1Build(&one, []collections.VectorPartitionPackDiagnosticsV1{diagnostics[p]})
		if err != nil {
			return nil, err
		}
		out[i] = localHNSWM18EdgeDiagnosisPackNeighborhoodV1{Partition: p, Neighborhood: neighborhood}
	}
	return out, nil
}

type localHNSWM18EdgeSidecarV1 struct {
	Schema string                          `json:"schema"`
	Cells  []localHNSWM18EdgeSidecarCellV1 `json:"cells"`
}

type localHNSWM18EdgeSidecarCellV1 struct {
	EFSearch int                       `json:"ef_search"`
	Traces   []localHNSWM18EdgeTraceV1 `json:"traces"`
}

func localHNSWM18EdgeDiagnosisPointsV1(points []int) bool {
	return slices.Equal(points, localHNSWM18EdgeDiagnosisEFV1)
}

func localHNSWM18EdgeDiagnosisWorkAddV1(dst *localHNSWAttributionQueryWorkV1, candidates, edges, frontier uint64, utility localHNSWAttributionQueryUtilityV1) error {
	if dst == nil || math.MaxUint64-dst.Candidates < candidates || math.MaxUint64-dst.Edges < edges || math.MaxUint64-dst.FrontierAdmissions < frontier {
		return errors.New("M18 diagnosis work overflow")
	}
	if err := localHNSWAttributionQueryUtilityAddV1(&dst.Utility, utility); err != nil {
		return err
	}
	dst.Candidates += candidates
	dst.Edges += edges
	dst.FrontierAdmissions += frontier
	return nil
}

func localHNSWM18EdgeDiagnosisSelectedConstructionV1(evidence collections.VectorPartitionConstructionEvidenceV1) ([]localHNSWM18EdgeDiagnosisPackConstructionV1, error) {
	if len(evidence.Partitions) != 40 {
		return nil, errors.New("invalid M18 construction partition count")
	}
	out := make([]localHNSWM18EdgeDiagnosisPackConstructionV1, len(localHNSWM18EdgeDiagnosisPacksV1))
	for i, p := range localHNSWM18EdgeDiagnosisPacksV1 {
		if int(p) >= len(evidence.Partitions) || evidence.Partitions[p].PartitionID != p {
			return nil, errors.New("invalid M18 selected construction partition")
		}
		one := evidence
		one.Partitions = []collections.VectorPartitionConstructionPartitionEvidenceV1{evidence.Partitions[p]}
		totals, err := localHNSWAttributionConstructionReduceV1(one)
		if err != nil {
			return nil, err
		}
		out[i] = localHNSWM18EdgeDiagnosisPackConstructionV1{Partition: p, Construction: totals}
	}
	return out, nil
}

func localHNSWM18EdgeDiagnosisMissesV1(in []localHNSWM18EdgeDiagnosisHardMissV1) []localHNSWM18EdgeDiagnosisHardMissV1 {
	slices.SortFunc(in, func(a, b localHNSWM18EdgeDiagnosisHardMissV1) int {
		if a.Rank != b.Rank {
			if a.Rank < b.Rank {
				return -1
			}
			return 1
		}
		if a.QueryOrdinal != b.QueryOrdinal {
			return a.QueryOrdinal - b.QueryOrdinal
		}
		if a.QuerySHA256 < b.QuerySHA256 {
			return -1
		}
		if a.QuerySHA256 > b.QuerySHA256 {
			return 1
		}
		return 0
	})
	if len(in) > 32 {
		in = in[:32]
	}
	return in
}

func localHNSWM18EdgeSidecarValidateV1(sidecar localHNSWM18EdgeSidecarV1, cells []localHNSWM18EdgeDiagnosisCellV1, h *localHNSWVariantHarnessV1, truthByQuery map[string]map[string]struct{}) error {
	if sidecar.Schema != localHNSWM18EdgeTraceSchemaV1 || len(sidecar.Cells) != len(cells) || h == nil || len(h.documentIDs) != 40 || len(h.finalOrigins) != 40 {
		return errors.New("invalid M18 edge sidecar identity")
	}
	for i, sidecarCell := range sidecar.Cells {
		if sidecarCell.EFSearch != cells[i].EFSearch {
			return errors.New("invalid M18 edge sidecar EF")
		}
		expected := make(map[string]struct{}, len(cells[i].HardMisses))
		for _, miss := range cells[i].HardMisses {
			expected[miss.QuerySHA256] = struct{}{}
		}
		counts := make(map[string]int, len(expected))
		partitions := make(map[string]map[uint32]struct{}, len(expected))
		for _, trace := range sidecarCell.Traces {
			if _, ok := expected[trace.QuerySHA]; !ok || int(trace.Partition) >= len(h.documentIDs) {
				return errors.New("unexpected M18 edge sidecar trace")
			}
			truth, ok := truthByQuery[trace.QuerySHA]
			if !ok || localHNSWM18EdgeTraceValidateV1(trace, h.documentIDs[trace.Partition], h.finalOrigins[trace.Partition], truth) != nil {
				return errors.New("M18 edge sidecar replay")
			}
			if partitions[trace.QuerySHA] == nil {
				partitions[trace.QuerySHA] = map[uint32]struct{}{}
			}
			if _, duplicate := partitions[trace.QuerySHA][trace.Partition]; duplicate {
				return errors.New("duplicate M18 edge sidecar partition")
			}
			partitions[trace.QuerySHA][trace.Partition] = struct{}{}
			counts[trace.QuerySHA]++
		}
		for querySHA := range expected {
			if counts[querySHA] != 2 {
				return errors.New("incomplete M18 edge sidecar query")
			}
		}
	}
	return nil
}

func localHNSWM18EdgeDiagnosisBuildV1(ctx context.Context, source *m8ProductionMultiGroupAssetsV1, h *localHNSWVariantHarnessV1, calibration localHNSWAttributionCalibrationV1) ([]localHNSWM18EdgeDiagnosisCellV1, error) {
	if source == nil || h == nil || !localHNSWM18EdgeDiagnosisPointsV1(localHNSWM18EdgeDiagnosisEFV1) || len(calibration.Ordinals) != 806 || len(calibration.Queries) != 806 || len(calibration.Truth) != 806 || localHNSWAttributionGraphHarnessV1(source, h) != nil {
		return nil, errors.New("invalid M18 edge diagnosis inputs")
	}
	candidates := min(256, int(source.status.Representatives))
	if candidates < 1 {
		return nil, errors.New("invalid M18 edge diagnosis router")
	}
	out := make([]localHNSWM18EdgeDiagnosisCellV1, len(localHNSWM18EdgeDiagnosisEFV1))
	for i, ef := range localHNSWM18EdgeDiagnosisEFV1 {
		out[i].EFSearch = ef
		out[i].SelectedWork = make([]localHNSWM18EdgeDiagnosisPackWorkV1, len(localHNSWM18EdgeDiagnosisPacksV1))
		for j, p := range localHNSWM18EdgeDiagnosisPacksV1 {
			out[i].SelectedWork[j].Partition = p
		}
	}
	for i, query := range calibration.Queries {
		route, err := localHNSWAttributionQueryRouteV1(ctx, source, query, candidates, 2)
		if err != nil {
			return nil, err
		}
		truth := calibration.Truth[i]
		truthSet := localHNSWAttributionResultIDSetV1(truth)
		for ci, cell := range out {
			records := make([]localHNSWAttributionQuerySearchV1, len(h.searchers))
			results := make([][]m8CanonicalResultV1, len(h.searchers))
			queryTraces := make([]localHNSWM18EdgeTraceV1, 0, len(route))
			for _, p := range route {
				search, metrics, trace, err := h.searchers[p].SearchWithAttributionV1(ctx, query, collections.VectorPartitionSearchOptionsV1{TopK: 10, EfSearch: cell.EFSearch})
				if err != nil || !localHNSWAttributionSearchValidV1(trace) {
					return nil, errors.New("invalid M18 attributed search")
				}
				canonical := make([]m8CanonicalResultV1, len(search))
				for j := range search {
					canonical[j] = m8CanonicalResultV1{ID: search[j].ID, Score: search[j].Score}
				}
				canonical, err = localHNSWAttributionCanonicalResultsV1(canonical, false)
				if err != nil {
					return nil, err
				}
				utility, err := localHNSWAttributionQueryUtilityReduceV1(metrics, trace, h.finalOrigins[p], h.documentIDs[p], truthSet)
				if err != nil {
					return nil, err
				}
				recoveries := localHNSWAttributionTruthRecoveriesV1(trace, h.finalOrigins[p], h.documentIDs[p], truthSet)
				records[p] = localHNSWAttributionQuerySearchV1{Results: localHNSWAttributionQueryResultBitsV1(canonical), Candidates: metrics.Candidates, Edges: metrics.Edges, FrontierAdmissions: trace.FrontierAdmissions, SeedCandidates: trace.SeedCandidates, SeedAdmissions: trace.SeedAdmissions, TerminationReason: trace.TerminationReason, VisitedOrdinalsSHA256: trace.VisitedOrdinalsSHA256, VisitedOrdinals: append([]uint32(nil), trace.VisitedOrdinals...), Utility: utility, TruthRecoveries: localHNSWAttributionTruthRecoveryRecordsV1(recoveries)}
				queryTraces = append(queryTraces, localHNSWM18EdgeTraceV1{Schema: localHNSWM18EdgeTraceSchemaV1, QuerySHA: localHNSWAttributionQueryFP32SHA256V1(query), Partition: p, Record: records[p], Edges: append([]collections.VectorPartitionSearchEdgeEventV1(nil), trace.EdgeEvents...), Seeds: append([]collections.VectorPartitionSearchSeedEventV1(nil), trace.SeedEvents...)})
				results[p] = canonical
				for j, selected := range localHNSWM18EdgeDiagnosisPacksV1 {
					if p == selected {
						w := &out[ci].SelectedWork[j].Work
						if localHNSWM18EdgeDiagnosisWorkAddV1(w, metrics.Candidates, metrics.Edges, trace.FrontierAdmissions, utility) != nil {
							return nil, errors.New("M18 selected utility")
						}
					}
				}
			}
			merged, work, err := localHNSWAttributionQueryMergeV1(records, results, h.documentIDs, route, truthSet)
			if err != nil {
				return nil, err
			}
			recall := m8CanonicalRecallV1(truth, merged)
			if err := localHNSWAttributionRecallAddV1(&out[ci].EndToEndRecall, recall, out[ci].Queries); err != nil || localHNSWM18EdgeDiagnosisWorkAddV1(&out[ci].Work, work.Candidates, work.Edges, work.FrontierAdmissions, work.Utility) != nil {
				return nil, errors.New("invalid M18 edge diagnosis aggregate")
			}
			out[ci].Queries++
			if recall < 1 {
				sum := sha256.Sum256([]byte("treedb-4171-m18-hard-miss-v1/" + localHNSWAttributionQueryFP32SHA256V1(query)))
				out[ci].HardMisses = append(out[ci].HardMisses, localHNSWM18EdgeDiagnosisHardMissV1{QueryOrdinal: calibration.Ordinals[i], QuerySHA256: localHNSWAttributionQueryFP32SHA256V1(query), RecallBits: math.Float32bits(float32(recall)), Rank: hex.EncodeToString(sum[:])})
				out[ci].Traces = append(out[ci].Traces, queryTraces...)
			}
		}
	}
	for i := range out {
		if out[i].Queries != 806 {
			return nil, errors.New("incomplete M18 edge diagnosis")
		}
		out[i].EndToEndRecall.Mean /= float64(out[i].Queries)
		out[i].HardMisses = localHNSWM18EdgeDiagnosisMissesV1(out[i].HardMisses)
		allowed := make(map[string]struct{}, len(out[i].HardMisses))
		for _, miss := range out[i].HardMisses {
			allowed[miss.QuerySHA256] = struct{}{}
		}
		kept := out[i].Traces[:0]
		for _, trace := range out[i].Traces {
			if _, ok := allowed[trace.QuerySHA]; ok {
				kept = append(kept, trace)
			}
		}
		out[i].Traces = kept
	}
	return out, nil
}

func runLocalHNSWM18EdgeDiagnosisV1(args []string, stdout io.Writer) (runErr error) {
	fs := flag.NewFlagSet("treedb_vector_partition_bench local-hnsw-m18-edge-diagnosis", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var dataset, retainedDB, calibrationPath, truthPath, tempRoot, out, rawSidecar, baseSHA, headSHA, sourceCheckout string
	fs.StringVar(&dataset, "dataset", "", "frozen fixture directory")
	fs.StringVar(&retainedDB, "retained-db", "", "literal M18 retained database")
	fs.StringVar(&calibrationPath, "calibration-split", "", "frozen calibration manifest")
	fs.StringVar(&truthPath, "truth-artifact", "", "frozen truth artifact")
	fs.StringVar(&tempRoot, "temp-root", "", "existing fast temporary root")
	fs.StringVar(&out, "out", "", "new report path")
	fs.StringVar(&rawSidecar, "raw-sidecar", "", "fresh local bounded trace sidecar")
	fs.StringVar(&baseSHA, "base-sha", "", "exact main base SHA")
	fs.StringVar(&headSHA, "head-sha", "", "exact diagnosis implementation SHA")
	fs.StringVar(&sourceCheckout, "source-checkout", "", "clean exact-head checkout")
	if fs.Parse(args) != nil || fs.NArg() != 0 || dataset == "" || retainedDB == "" || calibrationPath == "" || truthPath == "" || tempRoot == "" || out == "" || rawSidecar == "" || baseSHA == "" || headSHA == "" || sourceCheckout == "" {
		return errors.New("local-hnsw-m18-edge-diagnosis requires frozen inputs, temp root, and fresh output")
	}
	var err error
	for ptr, value := range map[*string]string{&dataset: dataset, &retainedDB: retainedDB, &calibrationPath: calibrationPath, &truthPath: truthPath, &tempRoot: tempRoot, &out: out, &rawSidecar: rawSidecar, &sourceCheckout: sourceCheckout} {
		if *ptr, err = m8CanonicalPathV1(value); err != nil {
			return err
		}
	}
	baseSHA, headSHA, err = provenanceWithExplicitV1(baseSHA, headSHA)
	if err != nil || baseSHA != "2a7d01443d3c842990c259b08bd442a4d0109511" || m8GitDirtyInV1(sourceCheckout) {
		return errors.New("M18 edge diagnosis source provenance")
	}
	if _, err := localHNSWAttributionSourceCheckoutV1(sourceCheckout, baseSHA, headSHA); err != nil {
		return errors.New("M18 edge diagnosis source checkout")
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
	if err != nil || !m8QualificationBenchmarkExecutableV1(sourceCheckout, executable, headSHA, executableSHA) {
		return errors.New("M18 edge diagnosis executable provenance")
	}
	if filepath.Ext(out) != ".json" {
		return errors.New("M18 edge diagnosis output must be JSON")
	}
	if _, err := os.Lstat(out); !errors.Is(err, os.ErrNotExist) {
		return errors.New("M18 edge diagnosis output exists")
	}
	if _, err := os.Lstat(rawSidecar); !errors.Is(err, os.ErrNotExist) {
		return errors.New("M18 edge diagnosis sidecar exists")
	}
	fixture, err := loadFixture(dataset)
	if err != nil || !localHNSWAttributionFixtureV1(fixture) {
		return errors.New("M18 edge diagnosis fixture identity")
	}
	calibration, calibrationSHA, err := loadLocalHNSWQuerySplitV1(calibrationPath)
	if err != nil || calibrationSHA != localHNSWAttributionCalibrationSHA256V1 || len(calibration.Ordinals) != 806 {
		return errors.New("M18 edge diagnosis calibration identity")
	}
	truthSHA, err := localHNSWAttributionRegularFileSHA256V1(truthPath, m8ProfileArtifactMaxBytesV1)
	if err != nil || truthSHA != localHNSWAttributionTruthSHA256V1 {
		return errors.New("M18 edge diagnosis truth identity")
	}
	descriptorPath := filepath.Join(retainedDB, m3VariantDescriptorFileV1)
	descriptorSHA, err := localHNSWAttributionRegularFileSHA256V1(descriptorPath, m3VariantDescriptorMaxBytesV1)
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
	queries, err := localHNSWAttributionCalibrationV1Build(source, fixture, calibration.Ordinals)
	if err != nil {
		return err
	}
	h, build, err := localHNSWAttributionBuildVariantV1(source, tempRoot, collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1, 4171001)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, h.Close()) }()
	construction, err := localHNSWAttributionConstructionReduceV1(h.constructionEvidence)
	if err != nil {
		return err
	}
	selectedConstruction, err := localHNSWM18EdgeDiagnosisSelectedConstructionV1(h.constructionEvidence)
	if err != nil {
		return err
	}
	diagnostics, err := localHNSWAttributionPackDiagnosticsV1(h.searchers)
	if err != nil {
		return err
	}
	neighborhood, err := localHNSWAttributionNeighborhoodOracleV1Build(h, diagnostics)
	if err != nil {
		return err
	}
	selectedNeighborhood, err := localHNSWM18EdgeDiagnosisSelectedNeighborhoodV1(h, diagnostics)
	if err != nil {
		return err
	}
	cells, err := localHNSWM18EdgeDiagnosisBuildV1(context.Background(), source, h, queries)
	if err != nil {
		return err
	}
	sidecar := localHNSWM18EdgeSidecarV1{Schema: localHNSWM18EdgeTraceSchemaV1, Cells: make([]localHNSWM18EdgeSidecarCellV1, len(cells))}
	for i := range cells {
		sidecar.Cells[i] = localHNSWM18EdgeSidecarCellV1{EFSearch: cells[i].EFSearch, Traces: append([]localHNSWM18EdgeTraceV1(nil), cells[i].Traces...)}
	}
	if err := writeVectorPartitionSystemJSONExclusiveV1(rawSidecar, sidecar); err != nil {
		return err
	}
	truthByQuery := make(map[string]map[string]struct{}, len(queries.Queries))
	for i, query := range queries.Queries {
		truthByQuery[localHNSWAttributionQueryFP32SHA256V1(query)] = localHNSWAttributionResultIDSetV1(queries.Truth[i])
	}
	raw, err := os.ReadFile(rawSidecar)
	if err != nil {
		return err
	}
	var reread localHNSWM18EdgeSidecarV1
	if json.Unmarshal(raw, &reread) != nil {
		return errors.New("M18 edge sidecar reread")
	}
	if err := localHNSWM18EdgeSidecarValidateV1(reread, cells, h, truthByQuery); err != nil {
		return err
	}
	sidecarSHA, err := localHNSWAttributionRegularFileSHA256V1(rawSidecar, m8ProfileArtifactMaxBytesV1)
	if err != nil {
		return err
	}
	for i := range cells {
		cells[i].Traces = nil
	}
	if m8GitDirtyInV1(sourceCheckout) || strings.TrimSpace(headSHA) == "" {
		return errors.New("M18 edge diagnosis provenance changed")
	}
	if digest, err := m8BenchmarkExecutableSHA256V1(executable); err != nil || digest != executableSHA || !m8QualificationBenchmarkExecutableV1(sourceCheckout, executable, headSHA, executableSHA) {
		return errors.New("M18 edge diagnosis executable changed")
	}
	loads, err := m8PartitionLoadsV1(source.manifest)
	if err != nil || source.descriptor == nil {
		return errors.New("M18 edge diagnosis source binding")
	}
	selectedDiagnostics := make([]collections.VectorPartitionPackDiagnosticsV1, len(localHNSWM18EdgeDiagnosisPacksV1))
	for i, p := range localHNSWM18EdgeDiagnosisPacksV1 {
		if int(p) >= len(diagnostics) {
			return errors.New("M18 selected pack")
		}
		selectedDiagnostics[i] = diagnostics[p]
	}
	report := localHNSWM18EdgeDiagnosisReportV1{
		Schema: localHNSWM18EdgeDiagnosisSchemaV1, ResultKind: "local_hnsw_m18_edge_diagnosis_v1", Status: "valid", GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Provenance: localHNSWAttributionProvenanceV1{Command: commandWithProvenanceAndSourceCheckoutV1("local-hnsw-m18-edge-diagnosis", args, baseSHA, headSHA, sourceCheckout), BaseSHA: baseSHA, HeadSHA: headSHA, SourceCheckout: sourceCheckout, Executable: executable, ExecutableSHA256: executableSHA},
		Variant:    string(collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1), Probes: 2, EFSearch: append([]int(nil), localHNSWM18EdgeDiagnosisEFV1...),
		Calibration: localHNSWAttributionFileInputV1{Path: calibrationPath, SHA256: calibrationSHA}, Truth: localHNSWAttributionFileInputV1{Path: truthPath, SHA256: truthSHA}, Descriptor: localHNSWAttributionFileInputV1{Path: descriptorPath, SHA256: descriptorSHA}, RawSidecar: localHNSWAttributionFileInputV1{Path: rawSidecar, SHA256: sidecarSHA},
		Manifest: source.manifest.IntegrityDigest,
		Source:   localHNSWAttributionSourceEvidenceV1{IndexName: source.manifest.IndexName, PartitionGeneration: source.manifest.Generation, Partitions: source.manifest.PartitionCount, ManifestIntegrity: source.manifest.IntegrityDigest, ReadySetDigest: source.manifest.ReadySetDigest, SourceGeneration: source.manifest.SourceGeneration, SourceChecksum: source.manifest.SourceChecksum, SourceSchemaHash: source.manifest.SourceSchemaHash, SourceRows: source.manifest.SourceRowCount, RouterGeneration: source.manifest.RouterGeneration, RouterModelDigest: source.status.ModelDigest, RouterRepresentatives: source.status.Representatives, PartitionLoads: loads, Descriptor: *source.descriptor},
		Build:    build, Construction: construction, Neighborhood: neighborhood,
		SelectedPacks: append([]uint32(nil), localHNSWM18EdgeDiagnosisPacksV1...), SelectedDiagnostics: selectedDiagnostics, SelectedConstruction: selectedConstruction, SelectedNeighborhood: selectedNeighborhood,
		Cells:       cells,
		Limitations: []string{"offline calibration-only M18 diagnosis; no holdout outcomes opened", "hard misses are bounded deterministic summaries; raw attributed events are retained only in the hash-bound local sidecar"},
	}
	if err := localHNSWM18EdgeDiagnosisReportValidateV1(report); err != nil {
		return err
	}
	if err := writeVectorPartitionSystemJSONExclusiveV1(out, report); err != nil {
		return err
	}
	reportRaw, err := os.ReadFile(out)
	if err != nil {
		return err
	}
	var reportReread localHNSWM18EdgeDiagnosisReportV1
	if json.Unmarshal(reportRaw, &reportReread) != nil || localHNSWM18EdgeDiagnosisReportValidateV1(reportReread) != nil || !reflect.DeepEqual(reportReread, report) {
		return errors.New("M18 edge diagnosis report reread")
	}
	_, err = fmt.Fprintf(stdout, "report=%s queries=806 probes=2 ef=%v\n", out, localHNSWM18EdgeDiagnosisEFV1)
	return err
}
