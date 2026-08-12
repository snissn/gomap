package main

import (
	"context"
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

const localHNSWRepairM18EFCurveSchemaV1 = "treedb_local_hnsw_repair_m18_ef_curve_v1"
const localHNSWRepairM18EFCurveDescriptorSHA256V1 = "057287712e84e219a2ecb1d36aebab53e2d78c044b06cbe56fe288cb854ac43b"

var localHNSWRepairM18EFCurvePointsV1 = []int{96, 112, 120, 128}

func localHNSWRepairM18EFCurveDescriptorV1(digest string) bool {
	return digest == localHNSWRepairM18EFCurveDescriptorSHA256V1
}

type localHNSWRepairM18EFCurveReportV1 struct {
	Schema         string                               `json:"schema"`
	ResultKind     string                               `json:"result_kind"`
	Status         string                               `json:"status"`
	GeneratedAt    string                               `json:"generated_at"`
	Provenance     localHNSWAttributionProvenanceV1     `json:"provenance"`
	Host           m8ProductionHostEvidenceV1           `json:"host"`
	Inputs         localHNSWAttributionInputsEvidenceV1 `json:"inputs"`
	Source         localHNSWAttributionSourceEvidenceV1 `json:"source"`
	TopK           int                                  `json:"top_k"`
	M              int                                  `json:"m"`
	EfConstruction int                                  `json:"ef_construction"`
	Variant        string                               `json:"variant"`
	ProbeCounts    []int                                `json:"probe_counts"`
	EFSearch       []int                                `json:"ef_search"`
	Build          localHNSWAttributionBuildEvidenceV1  `json:"build"`
	Graph          localHNSWRepairCalibrationGraphV1    `json:"graph"`
	Cells          []localHNSWRepairEFCurveCellV1       `json:"cells"`
	Disposition    string                               `json:"disposition"`
	Limitations    []string                             `json:"limitations"`
}

func localHNSWRepairM18EFCurveDispositionV1(cells []localHNSWRepairEFCurveCellV1) (string, error) {
	if len(cells) != len(localHNSWRepairM18EFCurvePointsV1) {
		return "", errors.New("invalid local HNSW repair M18 EF cells")
	}
	for i, cell := range cells {
		if cell.EFSearch != localHNSWRepairM18EFCurvePointsV1[i] || !localHNSWRepairMCurveSlotMeansV1(cell) {
			return "", errors.New("invalid local HNSW repair M18 EF cell")
		}
		if cell.P2HitSlots >= 7657 && localHNSWRepairMCurveHitSlotGapV1(cell.P2HitSlots, cell.P16HitSlots) <= 20 && cell.RoutingMissSlots <= 20 {
			return fmt.Sprintf("smallest_point_passes_ef_%d", cell.EFSearch), nil
		}
	}
	return "no_point_passes", nil
}

func validateLocalHNSWRepairM18EFCurveReportV1(report localHNSWRepairM18EFCurveReportV1) error {
	if report.Schema != localHNSWRepairM18EFCurveSchemaV1 || report.ResultKind != "local_hnsw_repair_m18_ef_curve_v1" || report.Status != "valid" || report.Provenance.BaseSHA != localHNSWAttributionSourceLockV1 || report.Provenance.SourceDirty || !validLowerSHA(report.Provenance.HeadSHA) || !localHNSWAttributionSHA256V1(report.Provenance.ExecutableSHA256) || report.TopK != 10 || report.M != 18 || report.EfConstruction != 256 || report.Variant != string(collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1) || !slices.Equal(report.EFSearch, localHNSWRepairM18EFCurvePointsV1) || !slices.Equal(report.ProbeCounts, []int{2, 16}) {
		return errors.New("invalid local HNSW repair M18 EF identity")
	}
	if _, err := time.Parse(time.RFC3339Nano, report.GeneratedAt); err != nil || !localHNSWAttributionFixtureV1(report.Inputs.Fixture) || report.Inputs.DatasetManifest.SHA256 != localHNSWAttributionFixtureManifestSHA256V1 || !localHNSWRepairM18EFCurveDescriptorV1(report.Inputs.Descriptor.SHA256) || report.Inputs.Calibration.SHA256 != localHNSWAttributionCalibrationSHA256V1 || report.Inputs.Holdout.SHA256 != localHNSWAttributionHoldoutSHA256V1 || report.Inputs.Truth.SHA256 != localHNSWAttributionTruthSHA256V1 || report.Inputs.CalibrationRows != 806 || report.Inputs.HoldoutRows != 194 || report.Inputs.HoldoutStatus != "manifest_validated_lower_ef_query_outcomes_unopened" || report.Inputs.TruthStatus != "sha256_only_not_decoded" || report.Source.Partitions != 16 || report.Source.SourceRows != 250000 || len(report.Source.PartitionLoads) != 16 {
		return errors.New("invalid local HNSW repair M18 EF inputs")
	}
	for i, historical := range report.Inputs.Historical {
		if historical.SHA256 != localHNSWAttributionHistoricalSHA256V1[i] || historical.TopologyIdentitySHA256 != localHNSWAttributionHistoricalTopologySHA256V1[i] || historical.Probe2.Probes != 2 || historical.Probe16.Probes != 16 {
			return errors.New("invalid local HNSW repair M18 EF historical context")
		}
	}
	if report.Build.Schema != localHNSWAttributionBuildSchemaV1 || report.Build.Variant != report.Variant || report.Build.VariantIdentity == "" || report.Build.Partitions != 16 || report.Build.PackBytes == 0 || report.Graph.Rows != 300000 || report.Graph.CombinedReachableRows != 300000 || report.Graph.NativeTraversalRoots < 16 || report.Graph.AuxiliaryEdges < 2*(report.Graph.NativeTraversalRoots-16) || report.Graph.AuxiliaryCSRBytes != 8*(report.Graph.Rows+16)+4*report.Graph.AuxiliaryEdges || report.Graph.AuxiliaryMaxDegree > 9 || len(report.Cells) != len(report.EFSearch) {
		return errors.New("invalid local HNSW repair M18 EF graph")
	}
	routes := ""
	for i, cell := range report.Cells {
		if cell.EFSearch != report.EFSearch[i] || cell.QueryCount != 806 || !localHNSWAttributionSHA256V1(cell.RoutesSHA256) || !localHNSWAttributionSHA256V1(cell.P2ResultsSHA256) || !localHNSWAttributionSHA256V1(cell.P16ResultsSHA256) || cell.P2Work.Candidates == 0 || cell.P2Work.NativeEdges == 0 || cell.P16Work.Candidates == 0 || cell.P16Work.NativeEdges == 0 || !localHNSWRepairMCurveSlotMeansV1(cell) {
			return errors.New("invalid local HNSW repair M18 EF cell")
		}
		if routes != "" && routes != cell.RoutesSHA256 {
			return errors.New("local HNSW repair M18 EF route drift")
		}
		routes = cell.RoutesSHA256
		var terminations uint64
		for reason, count := range cell.TerminationCount {
			if !localHNSWAttributionTimingTerminationV1(reason) || math.MaxUint64-terminations < count {
				return errors.New("invalid local HNSW repair M18 EF termination")
			}
			terminations += count
		}
		if terminations != 806*16 {
			return errors.New("incomplete local HNSW repair M18 EF termination")
		}
	}
	want, err := localHNSWRepairM18EFCurveDispositionV1(report.Cells)
	if err != nil || report.Disposition != want {
		return errors.New("invalid local HNSW repair M18 EF disposition")
	}
	return nil
}

func runLocalHNSWRepairM18EFCurveV1(args []string, stdout io.Writer) (runErr error) {
	fs := flag.NewFlagSet("treedb_vector_partition_bench local-hnsw-repair-m18-ef-curve", flag.ContinueOnError)
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
		return errors.New("local-hnsw-repair-m18-ef-curve requires all frozen inputs, paths, provenance, and no positional arguments")
	}
	var err error
	for destination, value := range map[*string]string{&dataset: dataset, &retainedDB: retainedDB, &calibrationSplit: calibrationSplit, &holdoutSplit: holdoutSplit, &truthArtifact: truthArtifact, &tempRoot: tempRoot, &out: out, &sourceCheckout: sourceCheckout} {
		if *destination, err = m8CanonicalPathV1(value); err != nil {
			return err
		}
	}
	baseSHA, headSHA, err = provenanceWithExplicitV1(baseSHA, headSHA)
	if err != nil || baseSHA != localHNSWAttributionSourceLockV1 {
		return errors.New("local HNSW repair M18 EF source lock")
	}
	sourceCheckout, err = localHNSWAttributionSourceCheckoutV1(sourceCheckout, baseSHA, headSHA)
	if err != nil || m8GitDirtyInV1(sourceCheckout) || filepath.Ext(out) != ".json" {
		return errors.New("invalid local HNSW repair M18 EF provenance or output")
	}
	if info, err := os.Lstat(tempRoot); err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("invalid local HNSW repair M18 EF temporary root")
	}
	if _, err := os.Lstat(out); !errors.Is(err, os.ErrNotExist) {
		return errors.New("local HNSW repair M18 EF output exists")
	}
	parts := strings.Split(historicalCSV, ",")
	if len(parts) != 3 {
		return errors.New("local HNSW repair M18 EF requires three historical reports")
	}
	var historicalPaths [3]string
	for i, path := range parts {
		if historicalPaths[i], err = m8CanonicalPathV1(strings.TrimSpace(path)); err != nil {
			return err
		}
	}
	datasetManifest := filepath.Join(dataset, "fixture_manifest.json")
	if digest, hashErr := localHNSWAttributionRegularFileSHA256V1(datasetManifest, maxManifestBytes); hashErr != nil || digest != localHNSWAttributionFixtureManifestSHA256V1 {
		return errors.New("local HNSW repair M18 EF dataset identity")
	}
	fixture, err := loadFixture(dataset)
	if err != nil || !localHNSWAttributionFixtureV1(fixture) {
		return errors.New("local HNSW repair M18 EF fixture identity")
	}
	inputConfig := localHNSWAttributionInputConfigV1{Fixture: fixture, RetainedDB: retainedDB, Descriptor: filepath.Join(retainedDB, m3VariantDescriptorFileV1), CalibrationSplit: calibrationSplit, HoldoutSplit: holdoutSplit, TruthArtifact: truthArtifact, HistoricalSearchReports: historicalPaths, DescriptorSHA256: localHNSWRepairM18EFCurveDescriptorSHA256V1, CalibrationSplitSHA256: localHNSWAttributionCalibrationSHA256V1, HoldoutSplitSHA256: localHNSWAttributionHoldoutSHA256V1, TruthArtifactSHA256: localHNSWAttributionTruthSHA256V1, HistoricalReportSHA256: localHNSWAttributionHistoricalSHA256V1}
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
		return errors.New("local HNSW repair M18 EF query source")
	}
	repair, build, err := localHNSWAttributionBuildVariantV1(source, tempRoot, collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1, 4132001)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, repair.Close()) }()
	graph, err := localHNSWRepairCalibrationGraphV1Build(repair)
	if err != nil {
		return err
	}
	cells, err := localHNSWRepairEFCurveV1Build(context.Background(), source, repair, localHNSWRepairM18EFCurvePointsV1, calibration.Ordinals, calibration.Queries, calibration.Truth)
	if err != nil {
		return err
	}
	if _, err := localHNSWAttributionInputsV1(inputConfig); err != nil {
		return fmt.Errorf("local HNSW repair M18 EF inputs changed: %w", err)
	}
	if _, err := localHNSWAttributionSourceCheckoutV1(sourceCheckout, baseSHA, headSHA); err != nil || m8GitDirtyInV1(sourceCheckout) {
		return errors.New("local HNSW repair M18 EF source changed")
	}
	if digest, err := m8BenchmarkExecutableSHA256V1(executable); err != nil || digest != executableSHA {
		return errors.New("local HNSW repair M18 EF executable changed")
	}
	loads, err := m8PartitionLoadsV1(source.manifest)
	if err != nil || source.descriptor == nil {
		return errors.New("local HNSW repair M18 EF source evidence")
	}
	disposition, err := localHNSWRepairM18EFCurveDispositionV1(cells)
	if err != nil {
		return err
	}
	report := localHNSWRepairM18EFCurveReportV1{Schema: localHNSWRepairM18EFCurveSchemaV1, ResultKind: "local_hnsw_repair_m18_ef_curve_v1", Status: "valid", GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), Provenance: localHNSWAttributionProvenanceV1{Command: commandWithProvenanceAndSourceCheckoutV1("local-hnsw-repair-m18-ef-curve", args, baseSHA, headSHA, sourceCheckout), BaseSHA: baseSHA, HeadSHA: headSHA, SourceCheckout: sourceCheckout, Executable: executable, ExecutableSHA256: executableSHA}, Host: m8ProductionHostV1(config{out: out, dataset: dataset}, retainedDB), Inputs: localHNSWAttributionInputsEvidenceV1{DatasetManifest: localHNSWAttributionFileInputV1{Path: datasetManifest, SHA256: localHNSWAttributionFixtureManifestSHA256V1}, Fixture: fixture, RetainedDB: retainedDB, Descriptor: localHNSWAttributionFileInputV1{Path: inputConfig.Descriptor, SHA256: inputConfig.DescriptorSHA256}, Calibration: localHNSWAttributionFileInputV1{Path: calibrationSplit, SHA256: inputConfig.CalibrationSplitSHA256}, CalibrationRows: len(inputs.Calibration.Ordinals), Holdout: localHNSWAttributionFileInputV1{Path: holdoutSplit, SHA256: inputConfig.HoldoutSplitSHA256}, HoldoutRows: len(inputs.Holdout.Ordinals), HoldoutStatus: "manifest_validated_lower_ef_query_outcomes_unopened", Truth: localHNSWAttributionFileInputV1{Path: truthArtifact, SHA256: inputConfig.TruthArtifactSHA256}, TruthStatus: "sha256_only_not_decoded", Historical: historical}, Source: localHNSWAttributionSourceEvidenceV1{IndexName: source.manifest.IndexName, PartitionGeneration: source.manifest.Generation, Partitions: source.manifest.PartitionCount, ManifestIntegrity: source.manifest.IntegrityDigest, ReadySetDigest: source.manifest.ReadySetDigest, SourceGeneration: source.manifest.SourceGeneration, SourceChecksum: source.manifest.SourceChecksum, SourceSchemaHash: source.manifest.SourceSchemaHash, SourceRows: source.manifest.SourceRowCount, RouterGeneration: source.manifest.RouterGeneration, RouterModelDigest: source.status.ModelDigest, RouterRepresentatives: source.status.Representatives, PartitionLoads: loads, Descriptor: *source.descriptor}, TopK: 10, M: 18, EfConstruction: 256, Variant: string(collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1), ProbeCounts: []int{2, int(source.manifest.PartitionCount)}, EFSearch: append([]int(nil), localHNSWRepairM18EFCurvePointsV1...), Build: build, Graph: graph, Cells: cells, Disposition: disposition, Limitations: []string{"offline M18/eFC256 lower request-EF calibration quality/work curve; not product qualification", "holdout manifest was validated; lower-EF holdout query outcomes remained unopened", "timing, profiles, 100k, and distributed qualification are deferred"}}
	if err := validateLocalHNSWRepairM18EFCurveReportV1(report); err != nil {
		return err
	}
	if err := writeVectorPartitionSystemJSONExclusiveV1(out, report); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "report=%s ef_points=%v disposition=%s\n", out, localHNSWRepairM18EFCurvePointsV1, disposition)
	return err
}
