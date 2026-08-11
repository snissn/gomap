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
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

const localHNSWRepairCalibrationReportSchemaV1 = "treedb_local_hnsw_repair_calibration_report_v1"

type localHNSWRepairCalibrationReportV1 struct {
	Schema       string                                     `json:"schema"`
	ResultKind   string                                     `json:"result_kind"`
	Status       string                                     `json:"status"`
	GeneratedAt  string                                     `json:"generated_at"`
	Provenance   localHNSWAttributionProvenanceV1           `json:"provenance"`
	Host         m8ProductionHostEvidenceV1                 `json:"host"`
	Inputs       localHNSWAttributionInputsEvidenceV1       `json:"inputs"`
	Source       localHNSWAttributionSourceEvidenceV1       `json:"source"`
	TopK         int                                        `json:"top_k"`
	EFSearch     int                                        `json:"ef_search"`
	ProbeCounts  []int                                      `json:"probe_counts"`
	OverlayBuild localHNSWAttributionBuildEvidenceV1        `json:"overlay_build"`
	RepairBuild  localHNSWAttributionBuildEvidenceV1        `json:"repair_build"`
	Graph        localHNSWRepairCalibrationGraphV1          `json:"graph"`
	Calibration  localHNSWRepairCalibrationReportEvidenceV1 `json:"calibration"`
	Timing       localHNSWRepairCalibrationTimingV1         `json:"timing"`
	Profiles     m8ProductionProfileEvidenceV1              `json:"profiles"`
	Disposition  string                                     `json:"disposition"`
	Limitations  []string                                   `json:"limitations"`
}

type localHNSWRepairCalibrationReportEvidenceV1 struct {
	Artifact localHNSWAttributionArtifactV1      `json:"artifact"`
	Summary  localHNSWRepairCalibrationSummaryV1 `json:"summary"`
}

func runLocalHNSWRepairCalibrationV1(args []string, stdout io.Writer) (runErr error) {
	fs := flag.NewFlagSet("treedb_vector_partition_bench local-hnsw-repair-calibration", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var dataset, retainedDB, calibrationSplit, holdoutSplit, truthArtifact, historicalCSV, tempRoot, out, profiles, baseSHA, headSHA, sourceCheckout string
	fs.StringVar(&dataset, "dataset", "", "frozen fixture directory")
	fs.StringVar(&retainedDB, "retained-db", "", "literal retained 250k database")
	fs.StringVar(&calibrationSplit, "calibration-split", "", "frozen calibration manifest")
	fs.StringVar(&holdoutSplit, "holdout-split", "", "sealed holdout manifest")
	fs.StringVar(&truthArtifact, "truth-artifact", "", "sealed trusted truth artifact")
	fs.StringVar(&historicalCSV, "historical-search", "", "three comma-separated retained search reports")
	fs.StringVar(&tempRoot, "temp-root", "", "existing fast temporary root")
	fs.StringVar(&out, "out", "", "fresh report path")
	fs.StringVar(&profiles, "profiles", "", "fresh existing profile directory")
	fs.StringVar(&baseSHA, "base-sha", "", "source-lock base SHA")
	fs.StringVar(&headSHA, "head-sha", "", "exact implementation head SHA")
	fs.StringVar(&sourceCheckout, "source-checkout", "", "clean exact-head checkout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || dataset == "" || retainedDB == "" || calibrationSplit == "" || holdoutSplit == "" || truthArtifact == "" || historicalCSV == "" || tempRoot == "" || out == "" || profiles == "" || baseSHA == "" || headSHA == "" || sourceCheckout == "" {
		return errors.New("local-hnsw-repair-calibration requires all frozen inputs, paths, provenance, and no positional arguments")
	}
	var err error
	for destination, value := range map[*string]string{&dataset: dataset, &retainedDB: retainedDB, &calibrationSplit: calibrationSplit, &holdoutSplit: holdoutSplit, &truthArtifact: truthArtifact, &tempRoot: tempRoot, &out: out, &profiles: profiles, &sourceCheckout: sourceCheckout} {
		*destination, err = m8CanonicalPathV1(value)
		if err != nil {
			return err
		}
	}
	baseSHA, headSHA, err = provenanceWithExplicitV1(baseSHA, headSHA)
	if err != nil || baseSHA != localHNSWAttributionSourceLockV1 {
		return errors.New("local HNSW repair calibration source lock")
	}
	sourceCheckout, err = localHNSWAttributionSourceCheckoutV1(sourceCheckout, baseSHA, headSHA)
	if err != nil || m8GitDirtyInV1(sourceCheckout) {
		return errors.New("local HNSW repair calibration requires clean exact-head checkout")
	}
	if filepath.Ext(out) != ".json" {
		return errors.New("local HNSW repair calibration report must use .json")
	}
	for _, directory := range []string{tempRoot, profiles} {
		info, statErr := os.Lstat(directory)
		if statErr != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return errors.New("invalid local HNSW repair calibration directory")
		}
	}
	entries, err := os.ReadDir(profiles)
	if err != nil || len(entries) != 0 {
		return errors.New("local HNSW repair calibration profile directory must be empty")
	}
	prefix := strings.TrimSuffix(out, filepath.Ext(out))
	queryPath := prefix + ".queries.jsonl.gz"
	for _, path := range []string{out, queryPath} {
		if _, statErr := os.Lstat(path); !errors.Is(statErr, os.ErrNotExist) {
			return errors.New("local HNSW repair calibration output exists")
		}
	}
	historicalParts := strings.Split(historicalCSV, ",")
	if len(historicalParts) != 3 {
		return errors.New("local HNSW repair calibration requires three historical reports")
	}
	var historicalPaths [3]string
	for i, path := range historicalParts {
		historicalPaths[i], err = m8CanonicalPathV1(strings.TrimSpace(path))
		if err != nil {
			return err
		}
	}
	datasetManifest := filepath.Join(dataset, "fixture_manifest.json")
	if digest, hashErr := localHNSWAttributionRegularFileSHA256V1(datasetManifest, maxManifestBytes); hashErr != nil || digest != localHNSWAttributionFixtureManifestSHA256V1 {
		return errors.New("local HNSW repair calibration dataset identity")
	}
	fixture, err := loadFixture(dataset)
	if err != nil || !localHNSWAttributionFixtureV1(fixture) {
		return errors.New("local HNSW repair calibration fixture identity")
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
		return errors.New("local HNSW repair calibration query source")
	}
	overlay, overlayBuild, err := localHNSWAttributionBuildVariantV1(source, tempRoot, collections.VectorPartitionLocalGraphVariantOverlayCurrentV1, 4106001)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, overlay.Close()) }()
	if err := localHNSWRepairCalibrationOverlayRetainedV1(source, overlay); err != nil {
		return err
	}
	repair, repairBuild, err := localHNSWAttributionBuildVariantV1(source, tempRoot, collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1, 4106002)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, repair.Close()) }()
	graph, err := localHNSWRepairCalibrationGraphV1Build(repair)
	if err != nil {
		return err
	}
	queryArtifact, queryRows, summary, err := localHNSWRepairCalibrationSummaryV1Build(context.Background(), queryPath, source, overlay, repair, calibration.Ordinals, calibration.Queries, calibration.Truth)
	if err != nil {
		return err
	}
	capture, err := startM8ProfileCaptureV1(profiles)
	if err != nil {
		return err
	}
	timing, timingErr := localHNSWRepairCalibrationTimingV1Build(context.Background(), overlay, repair, queryRows)
	profilePaths, stopErr := capture.Stop()
	if timingErr != nil || stopErr != nil {
		return errors.Join(timingErr, stopErr)
	}
	profileArtifacts, err := m8ProfileArtifactsV1(profilePaths)
	if err != nil {
		return err
	}
	if _, err := localHNSWAttributionInputsV1(inputConfig); err != nil {
		return fmt.Errorf("local HNSW repair calibration inputs changed: %w", err)
	}
	if digest, hashErr := localHNSWAttributionRegularFileSHA256V1(datasetManifest, maxManifestBytes); hashErr != nil || digest != localHNSWAttributionFixtureManifestSHA256V1 {
		return errors.New("local HNSW repair calibration dataset changed")
	}
	if _, err := localHNSWAttributionSourceCheckoutV1(sourceCheckout, baseSHA, headSHA); err != nil || m8GitDirtyInV1(sourceCheckout) {
		return errors.New("local HNSW repair calibration source changed")
	}
	if digest, hashErr := m8BenchmarkExecutableSHA256V1(executable); hashErr != nil || digest != executableSHA {
		return errors.New("local HNSW repair calibration executable changed")
	}
	loads, err := m8PartitionLoadsV1(source.manifest)
	if err != nil || source.descriptor == nil {
		return errors.New("local HNSW repair calibration source evidence")
	}
	disposition, err := localHNSWRepairCalibrationDispositionV1(summary.Repair.P2Recall.Mean, summary.Repair.P16Recall.Mean, summary.Repair.RoutingRecall.Mean)
	if err != nil {
		return err
	}
	report := localHNSWRepairCalibrationReportV1{
		Schema: localHNSWRepairCalibrationReportSchemaV1, ResultKind: "local_hnsw_repair_calibration_v1", Status: "valid", GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Provenance: localHNSWAttributionProvenanceV1{Command: commandWithProvenanceAndSourceCheckoutV1("local-hnsw-repair-calibration", args, baseSHA, headSHA, sourceCheckout), BaseSHA: baseSHA, HeadSHA: headSHA, SourceCheckout: sourceCheckout, Executable: executable, ExecutableSHA256: executableSHA},
		Host:       m8ProductionHostV1(config{out: out, dataset: dataset}, retainedDB), Inputs: localHNSWAttributionInputsEvidenceV1{DatasetManifest: localHNSWAttributionFileInputV1{Path: datasetManifest, SHA256: localHNSWAttributionFixtureManifestSHA256V1}, Fixture: fixture, RetainedDB: retainedDB, Descriptor: localHNSWAttributionFileInputV1{Path: inputConfig.Descriptor, SHA256: inputConfig.DescriptorSHA256}, Calibration: localHNSWAttributionFileInputV1{Path: calibrationSplit, SHA256: inputConfig.CalibrationSplitSHA256}, CalibrationRows: len(inputs.Calibration.Ordinals), Holdout: localHNSWAttributionFileInputV1{Path: holdoutSplit, SHA256: inputConfig.HoldoutSplitSHA256}, HoldoutRows: len(inputs.Holdout.Ordinals), HoldoutStatus: "manifest_validated_query_outcomes_unopened", Truth: localHNSWAttributionFileInputV1{Path: truthArtifact, SHA256: inputConfig.TruthArtifactSHA256}, TruthStatus: "sha256_only_not_decoded", Historical: historical},
		Source: localHNSWAttributionSourceEvidenceV1{IndexName: source.manifest.IndexName, PartitionGeneration: source.manifest.Generation, Partitions: source.manifest.PartitionCount, ManifestIntegrity: source.manifest.IntegrityDigest, ReadySetDigest: source.manifest.ReadySetDigest, SourceGeneration: source.manifest.SourceGeneration, SourceChecksum: source.manifest.SourceChecksum, SourceSchemaHash: source.manifest.SourceSchemaHash, SourceRows: source.manifest.SourceRowCount, RouterGeneration: source.manifest.RouterGeneration, RouterModelDigest: source.status.ModelDigest, RouterRepresentatives: source.status.Representatives, PartitionLoads: loads, Descriptor: *source.descriptor},
		TopK:   10, EFSearch: 128, ProbeCounts: []int{2, int(source.manifest.PartitionCount)}, OverlayBuild: overlayBuild, RepairBuild: repairBuild, Graph: graph, Calibration: localHNSWRepairCalibrationReportEvidenceV1{Artifact: queryArtifact, Summary: summary}, Timing: timing, Profiles: m8ProductionProfileEvidenceV1{Directory: profiles, Captured: profilePaths, Artifacts: profileArtifacts, Status: "complete", Scope: "ordinary overlay-current versus auxiliary-navigation local search; top_k=10 ef_search=128 probes=2,all concurrency=1 four order-balanced repetitions"}, Disposition: disposition,
		Limitations: []string{"offline calibration-only repair pre-gate; not production qualification", "holdout query outcomes and trusted truth contents remained unopened", "no distributed or 100k guardrail execution occurs unless the repaired p2 recall is eligible"},
	}
	if err := localHNSWAttributionMatchFileSHA256V1(queryArtifact.Path, m8ProfileArtifactMaxBytesV1, queryArtifact.SHA256); err != nil {
		return fmt.Errorf("local HNSW repair calibration sidecar changed: %w", err)
	}
	if err := validateLocalHNSWRepairCalibrationReportV1(report); err != nil {
		return err
	}
	if err := writeVectorPartitionSystemJSONExclusiveV1(out, report); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "report=%s queries=%s queries_sha256=%s overlay_p2=%.6f repair_p2=%.6f disposition=%s\n", out, queryPath, queryArtifact.SHA256, summary.Overlay.P2Recall.Mean, summary.Repair.P2Recall.Mean, disposition)
	return err
}

func localHNSWRepairCalibrationGraphV1Build(repair *localHNSWVariantHarnessV1) (localHNSWRepairCalibrationGraphV1, error) {
	var out localHNSWRepairCalibrationGraphV1
	if repair == nil || len(repair.searchers) == 0 {
		return out, errors.New("invalid local HNSW repair graph")
	}
	for _, searcher := range repair.searchers {
		diagnostics, err := searcher.PackDiagnosticsV1()
		if err != nil || diagnostics.Rows == 0 || diagnostics.CombinedReachableRows != diagnostics.Rows || diagnostics.AuxiliaryMaxDegree > 9 || math.MaxUint64-out.Rows < diagnostics.Rows || math.MaxUint64-out.NativeReachableRows < diagnostics.ReachableRows || math.MaxUint64-out.CombinedReachableRows < diagnostics.CombinedReachableRows || math.MaxUint64-out.NativeTraversalRoots < diagnostics.TraversalRoots || math.MaxUint64-out.AuxiliaryEdges < diagnostics.AuxiliaryEdges || math.MaxUint64-out.AuxiliaryCSRBytes < diagnostics.AuxiliaryCSRBytes {
			return localHNSWRepairCalibrationGraphV1{}, errors.New("invalid local HNSW repair graph diagnostics")
		}
		out.Rows += diagnostics.Rows
		out.NativeReachableRows += diagnostics.ReachableRows
		out.CombinedReachableRows += diagnostics.CombinedReachableRows
		out.NativeTraversalRoots += diagnostics.TraversalRoots
		out.AuxiliaryEdges += diagnostics.AuxiliaryEdges
		out.AuxiliaryCSRBytes += diagnostics.AuxiliaryCSRBytes
		out.AuxiliaryMaxDegree = max(out.AuxiliaryMaxDegree, diagnostics.AuxiliaryMaxDegree)
	}
	return out, nil
}

func localHNSWRepairCalibrationOverlayRetainedV1(source *m8ProductionMultiGroupAssetsV1, overlay *localHNSWVariantHarnessV1) error {
	if source == nil || source.manifest.PartitionCount == 0 || localHNSWAttributionGraphHarnessV1(source, overlay) != nil {
		return errors.New("invalid local HNSW repair retained overlay")
	}
	for partition, retained := range source.manifest.Assets {
		asset := overlay.packAssets[partition]
		if asset.PartitionID != retained.PartitionID || asset.Checksum != retained.Checksum || asset.Bytes != retained.Bytes || asset.MembershipDigest != retained.MembershipDigest {
			return errors.New("local HNSW repair retained overlay drift")
		}
	}
	return nil
}

func validateLocalHNSWRepairCalibrationReportV1(report localHNSWRepairCalibrationReportV1) error {
	if report.Schema != localHNSWRepairCalibrationReportSchemaV1 || report.ResultKind != "local_hnsw_repair_calibration_v1" || report.Status != "valid" || report.Provenance.BaseSHA != localHNSWAttributionSourceLockV1 || report.Provenance.SourceDirty || !validLowerSHA(report.Provenance.HeadSHA) || !localHNSWAttributionSHA256V1(report.Provenance.ExecutableSHA256) || report.TopK != 10 || report.EFSearch != 128 || !slices.Equal(report.ProbeCounts, []int{2, 16}) {
		return errors.New("invalid local HNSW repair report identity")
	}
	if _, err := time.Parse(time.RFC3339Nano, report.GeneratedAt); err != nil || !localHNSWAttributionFixtureV1(report.Inputs.Fixture) || report.Inputs.DatasetManifest.SHA256 != localHNSWAttributionFixtureManifestSHA256V1 || report.Inputs.Descriptor.SHA256 != localHNSWAttributionDescriptorSHA256V1 || report.Inputs.Calibration.SHA256 != localHNSWAttributionCalibrationSHA256V1 || report.Inputs.Holdout.SHA256 != localHNSWAttributionHoldoutSHA256V1 || report.Inputs.Truth.SHA256 != localHNSWAttributionTruthSHA256V1 || report.Inputs.CalibrationRows != 806 || report.Inputs.HoldoutRows != 194 || report.Inputs.HoldoutStatus != "manifest_validated_query_outcomes_unopened" || report.Inputs.TruthStatus != "sha256_only_not_decoded" || report.Source.Partitions != 16 || report.Source.SourceRows != 250000 || len(report.Source.PartitionLoads) != 16 {
		return errors.New("invalid local HNSW repair report inputs")
	}
	for i, historical := range report.Inputs.Historical {
		if historical.SHA256 != localHNSWAttributionHistoricalSHA256V1[i] || historical.TopologyIdentitySHA256 != localHNSWAttributionHistoricalTopologySHA256V1[i] || historical.Probe2.Probes != 2 || historical.Probe16.Probes != 16 {
			return errors.New("invalid local HNSW repair historical context")
		}
	}
	if report.OverlayBuild.Schema != localHNSWAttributionBuildSchemaV1 || report.OverlayBuild.Variant != string(collections.VectorPartitionLocalGraphVariantOverlayCurrentV1) || report.RepairBuild.Schema != localHNSWAttributionBuildSchemaV1 || report.RepairBuild.Variant != string(collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1) || report.OverlayBuild.Partitions != 16 || report.RepairBuild.Partitions != 16 || report.OverlayBuild.PackBytes == 0 || report.RepairBuild.PackBytes == 0 {
		return errors.New("invalid local HNSW repair build report")
	}
	if report.Graph.Rows != 300000 || report.Graph.NativeReachableRows != 299968 || report.Graph.CombinedReachableRows != 300000 || report.Graph.NativeTraversalRoots != 48 || report.Graph.NativeTraversalRoots < 16 || report.Graph.AuxiliaryEdges < 2*(report.Graph.NativeTraversalRoots-16) || report.Graph.AuxiliaryCSRBytes != 8*(report.Graph.Rows+16)+4*report.Graph.AuxiliaryEdges || report.Graph.AuxiliaryMaxDegree > 9 {
		return errors.New("invalid local HNSW repair graph report")
	}
	if report.Calibration.Artifact.Schema != localHNSWAttributionSidecarSchemaV1 || report.Calibration.Artifact.Records != 806 || !localHNSWAttributionSHA256V1(report.Calibration.Artifact.SHA256) || report.Calibration.Summary.Schema != localHNSWRepairCalibrationSchemaV1 || report.Calibration.Summary.Overlay.QueryCount != 806 || report.Calibration.Summary.Repair.QueryCount != 806 {
		return errors.New("invalid local HNSW repair calibration report")
	}
	for _, aggregate := range []localHNSWRepairCalibrationAggregateV1{report.Calibration.Summary.Overlay, report.Calibration.Summary.Repair} {
		for _, value := range []float64{aggregate.RoutingRecall.Mean, aggregate.RoutingRecall.Min, aggregate.P2Recall.Mean, aggregate.P2Recall.Min, aggregate.P16Recall.Mean, aggregate.P16Recall.Min} {
			if !localHNSWAttributionFiniteRecallV1(value) {
				return errors.New("invalid local HNSW repair recall report")
			}
		}
		var terminations uint64
		for reason, count := range aggregate.TerminationCount {
			if !localHNSWAttributionTimingTerminationV1(reason) || math.MaxUint64-terminations < count {
				return errors.New("invalid local HNSW repair termination report")
			}
			terminations += count
		}
		if terminations != 806*16 || aggregate.P2Work.Candidates == 0 || aggregate.P2Work.NativeEdges == 0 || aggregate.P16Work.Candidates == 0 || aggregate.P16Work.NativeEdges == 0 {
			return errors.New("incomplete local HNSW repair aggregate work")
		}
	}
	overlayWork := report.Calibration.Summary.Overlay
	if overlayWork.P2Work.AuxiliaryEdges != 0 || overlayWork.P2Work.AuxiliaryCandidates != 0 || overlayWork.P2Work.AuxiliaryAdmissions != 0 || overlayWork.P16Work.AuxiliaryEdges != 0 || overlayWork.P16Work.AuxiliaryCandidates != 0 || overlayWork.P16Work.AuxiliaryAdmissions != 0 {
		return errors.New("invalid local HNSW overlay auxiliary work")
	}
	repairWork := report.Calibration.Summary.Repair
	if repairWork.P16Work.AuxiliaryEdges == 0 || repairWork.P16Work.AuxiliaryCandidates == 0 || repairWork.P16Work.AuxiliaryAdmissions == 0 {
		return errors.New("missing local HNSW repair auxiliary work")
	}
	if len(report.Timing.Cells) != 16 || report.Profiles.Status != "complete" || report.Profiles.Scope == "" || report.Disposition == "" {
		return errors.New("invalid local HNSW repair timing report")
	}
	profileArtifacts, err := m8ProfileArtifactsV1(report.Profiles.Captured)
	if err != nil || !reflect.DeepEqual(profileArtifacts, report.Profiles.Artifacts) {
		return errors.New("invalid local HNSW repair profile artifacts")
	}
	wantTiming := [4][4]struct {
		variant string
		probes  int
	}{{{"overlay_current", 2}, {"auxiliary_navigation", 2}, {"overlay_current", 16}, {"auxiliary_navigation", 16}}, {{"auxiliary_navigation", 16}, {"overlay_current", 16}, {"auxiliary_navigation", 2}, {"overlay_current", 2}}, {{"overlay_current", 16}, {"auxiliary_navigation", 16}, {"overlay_current", 2}, {"auxiliary_navigation", 2}}, {{"auxiliary_navigation", 2}, {"overlay_current", 2}, {"auxiliary_navigation", 16}, {"overlay_current", 16}}}
	stable := map[string][]string{}
	for i, cell := range report.Timing.Cells {
		want := wantTiming[i/4][i%4]
		if cell.Repetition != i/4 || cell.Variant != want.variant || cell.Probes != want.probes || cell.QueryCount != 806 || cell.ElapsedNanos == 0 || cell.QPS <= 0 || cell.P50Nanos == 0 || cell.P50Nanos > cell.P95Nanos || cell.P95Nanos > cell.P99Nanos || cell.Candidates == 0 || cell.NativeEdges == 0 || len(cell.ResultSHA256) != 806 {
			return errors.New("invalid local HNSW repair timing cell")
		}
		for _, digest := range cell.ResultSHA256 {
			if !localHNSWAttributionSHA256V1(digest) {
				return errors.New("invalid local HNSW repair timing digest")
			}
		}
		key := fmt.Sprintf("%s/%d", cell.Variant, cell.Probes)
		if previous, ok := stable[key]; ok && !slices.Equal(previous, cell.ResultSHA256) {
			return errors.New("unstable local HNSW repair timing results")
		}
		stable[key] = cell.ResultSHA256
	}
	wantDisposition, err := localHNSWRepairCalibrationDispositionV1(report.Calibration.Summary.Repair.P2Recall.Mean, report.Calibration.Summary.Repair.P16Recall.Mean, report.Calibration.Summary.Repair.RoutingRecall.Mean)
	if err != nil || report.Disposition != wantDisposition {
		return errors.New("invalid local HNSW repair disposition")
	}
	return nil
}

func localHNSWRepairCalibrationDispositionV1(p2, p16, routing float64) (string, error) {
	if !localHNSWAttributionFiniteRecallV1(p2) || !localHNSWAttributionFiniteRecallV1(p16) || !localHNSWAttributionFiniteRecallV1(routing) {
		return "", errors.New("invalid local HNSW repair disposition recall")
	}
	if p2 < .95 {
		return "blocker_activate_4107_recall_below_0_9500", nil
	}
	if math.Abs(p16-p2) > .002 || routing < .998 {
		return "blocker_calibration_p2_p16_or_routing_guardrail", nil
	}
	return "recall_eligible_stop_before_distributed_qualification", nil
}
