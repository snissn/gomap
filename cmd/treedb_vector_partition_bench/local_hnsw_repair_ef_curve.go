package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

const localHNSWRepairEFCurveSchemaV1 = "treedb_local_hnsw_repair_ef_curve_v1"

var localHNSWRepairEFCurvePointsV1 = []int{64, 128, 512, 4096}

type localHNSWRepairEFCurveCellV1 struct {
	EFSearch         int                                   `json:"ef_search"`
	QueryCount       int                                   `json:"query_count"`
	RoutesSHA256     string                                `json:"routes_sha256"`
	RoutingRecall    localHNSWAttributionRecallAggregateV1 `json:"routing_recall"`
	P2Recall         localHNSWAttributionRecallAggregateV1 `json:"p2_recall"`
	P16Recall        localHNSWAttributionRecallAggregateV1 `json:"p16_recall"`
	P2Work           localHNSWRepairCalibrationWorkV1      `json:"p2_work"`
	P16Work          localHNSWRepairCalibrationWorkV1      `json:"p16_work"`
	TerminationCount map[string]uint64                     `json:"termination_counts"`
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
	Disposition string                               `json:"disposition"`
	Limitations []string                             `json:"limitations"`
}

func localHNSWRepairEFCurvePointsValidV1(points []int) bool {
	return slices.Equal(points, localHNSWRepairEFCurvePointsV1)
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
	routesHash := sha256.New()
	routesHash.Write([]byte("treedb-4106-local-hnsw-repair-ef-curve-routes-v1/"))
	var raw [4]byte
	for i, efSearch := range points {
		out[i].EFSearch, out[i].TerminationCount = efSearch, map[string]uint64{}
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
		if cell.QueryCount != len(ordinals) || !localHNSWAttributionSHA256V1(cell.RoutesSHA256) {
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

func localHNSWRepairEFCurveDispositionV1(cells []localHNSWRepairEFCurveCellV1) (string, error) {
	if len(cells) != len(localHNSWRepairEFCurvePointsV1) {
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
	var dataset, retainedDB, calibrationSplit, holdoutSplit, truthArtifact, historicalCSV, tempRoot, out, baseSHA, headSHA, sourceCheckout string
	fs.StringVar(&dataset, "dataset", "", "frozen fixture directory")
	fs.StringVar(&retainedDB, "retained-db", "", "literal retained 250k database")
	fs.StringVar(&calibrationSplit, "calibration-split", "", "frozen calibration manifest")
	fs.StringVar(&holdoutSplit, "holdout-split", "", "sealed holdout manifest")
	fs.StringVar(&truthArtifact, "truth-artifact", "", "sealed trusted truth artifact")
	fs.StringVar(&historicalCSV, "historical-search", "", "three comma-separated retained search reports")
	fs.StringVar(&tempRoot, "temp-root", "", "existing fast temporary root")
	fs.StringVar(&out, "out", "", "fresh report path")
	fs.StringVar(&baseSHA, "base-sha", "", "source-lock base SHA")
	fs.StringVar(&headSHA, "head-sha", "", "exact implementation head SHA")
	fs.StringVar(&sourceCheckout, "source-checkout", "", "clean exact-head checkout")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || dataset == "" || retainedDB == "" || calibrationSplit == "" || holdoutSplit == "" || truthArtifact == "" || historicalCSV == "" || tempRoot == "" || out == "" || baseSHA == "" || headSHA == "" || sourceCheckout == "" {
		return errors.New("local-hnsw-repair-ef-curve requires all frozen inputs, paths, provenance, and no positional arguments")
	}
	var err error
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
	cells, err := localHNSWRepairEFCurveV1Build(context.Background(), source, repair, localHNSWRepairEFCurvePointsV1, calibration.Ordinals, calibration.Queries, calibration.Truth)
	if err != nil {
		return err
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
	report := localHNSWRepairEFCurveReportV1{Schema: localHNSWRepairEFCurveSchemaV1, ResultKind: "local_hnsw_repair_ef_curve_v1", Status: "valid", GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), Provenance: localHNSWAttributionProvenanceV1{Command: commandWithProvenanceAndSourceCheckoutV1("local-hnsw-repair-ef-curve", args, baseSHA, headSHA, sourceCheckout), BaseSHA: baseSHA, HeadSHA: headSHA, SourceCheckout: sourceCheckout, Executable: executable, ExecutableSHA256: executableSHA}, Host: m8ProductionHostV1(config{out: out, dataset: dataset}, retainedDB), Inputs: localHNSWAttributionInputsEvidenceV1{DatasetManifest: localHNSWAttributionFileInputV1{Path: datasetManifest, SHA256: localHNSWAttributionFixtureManifestSHA256V1}, Fixture: fixture, RetainedDB: retainedDB, Descriptor: localHNSWAttributionFileInputV1{Path: inputConfig.Descriptor, SHA256: inputConfig.DescriptorSHA256}, Calibration: localHNSWAttributionFileInputV1{Path: calibrationSplit, SHA256: inputConfig.CalibrationSplitSHA256}, CalibrationRows: len(inputs.Calibration.Ordinals), Holdout: localHNSWAttributionFileInputV1{Path: holdoutSplit, SHA256: inputConfig.HoldoutSplitSHA256}, HoldoutRows: len(inputs.Holdout.Ordinals), HoldoutStatus: "manifest_validated_query_outcomes_unopened", Truth: localHNSWAttributionFileInputV1{Path: truthArtifact, SHA256: inputConfig.TruthArtifactSHA256}, TruthStatus: "sha256_only_not_decoded", Historical: historical}, Source: localHNSWAttributionSourceEvidenceV1{IndexName: source.manifest.IndexName, PartitionGeneration: source.manifest.Generation, Partitions: source.manifest.PartitionCount, ManifestIntegrity: source.manifest.IntegrityDigest, ReadySetDigest: source.manifest.ReadySetDigest, SourceGeneration: source.manifest.SourceGeneration, SourceChecksum: source.manifest.SourceChecksum, SourceSchemaHash: source.manifest.SourceSchemaHash, SourceRows: source.manifest.SourceRowCount, RouterGeneration: source.manifest.RouterGeneration, RouterModelDigest: source.status.ModelDigest, RouterRepresentatives: source.status.Representatives, PartitionLoads: loads, Descriptor: *source.descriptor}, TopK: 10, EFSearch: append([]int(nil), localHNSWRepairEFCurvePointsV1...), ProbeCounts: []int{2, int(source.manifest.PartitionCount)}, RepairBuild: build, Graph: graph, Cells: cells, Disposition: disposition, Limitations: []string{"offline calibration-only fixed-asset EF quality/work pre-gate; not product qualification", "holdout query outcomes and trusted truth contents remained unopened", "profiles and repeated timing are deferred until a curve point clears quality"}}
	if err := validateLocalHNSWRepairEFCurveReportV1(report); err != nil {
		return err
	}
	if err := writeVectorPartitionSystemJSONExclusiveV1(out, report); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "report=%s p2_64=%.6f p2_128=%.6f disposition=%s\n", out, cells[0].P2Recall.Mean, cells[1].P2Recall.Mean, disposition)
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
	if report.RepairBuild.Schema != localHNSWAttributionBuildSchemaV1 || report.RepairBuild.Variant != string(collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1) || report.RepairBuild.Partitions != 16 || report.RepairBuild.PackBytes == 0 || report.Graph.Rows != 300000 || report.Graph.NativeReachableRows != 299968 || report.Graph.CombinedReachableRows != 300000 || report.Graph.NativeTraversalRoots != 48 || report.Graph.AuxiliaryEdges != 64 || report.Graph.AuxiliaryCSRBytes != 2400384 || report.Graph.AuxiliaryMaxDegree != 4 {
		return errors.New("invalid local HNSW repair EF curve graph")
	}
	if len(report.Cells) != len(localHNSWRepairEFCurvePointsV1) {
		return errors.New("invalid local HNSW repair EF curve cells")
	}
	routes := ""
	for i, cell := range report.Cells {
		if cell.EFSearch != localHNSWRepairEFCurvePointsV1[i] || cell.QueryCount != 806 || !localHNSWAttributionSHA256V1(cell.RoutesSHA256) || cell.P2Work.Candidates == 0 || cell.P2Work.NativeEdges == 0 || cell.P16Work.Candidates == 0 || cell.P16Work.NativeEdges == 0 {
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
	want, err := localHNSWRepairEFCurveDispositionV1(report.Cells)
	if err != nil || report.Disposition != want {
		return errors.New("invalid local HNSW repair EF curve disposition")
	}
	return nil
}
