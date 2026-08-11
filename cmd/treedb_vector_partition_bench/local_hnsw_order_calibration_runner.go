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

const localHNSWOrderCalibrationReportSchemaV1 = "treedb_local_hnsw_order_calibration_report_v1"

type localHNSWOrderCalibrationReportV1 struct {
	Schema            string                                    `json:"schema"`
	ResultKind        string                                    `json:"result_kind"`
	Status            string                                    `json:"status"`
	GeneratedAt       string                                    `json:"generated_at"`
	Provenance        localHNSWAttributionProvenanceV1          `json:"provenance"`
	Host              m8ProductionHostEvidenceV1                `json:"host"`
	Inputs            localHNSWAttributionInputsEvidenceV1      `json:"inputs"`
	Source            localHNSWAttributionSourceEvidenceV1      `json:"source"`
	TopK              int                                       `json:"top_k"`
	EFSearch          int                                       `json:"ef_search"`
	ProbeCounts       []int                                     `json:"probe_counts"`
	SourceOrderBuild  localHNSWAttributionBuildEvidenceV1       `json:"source_order_build"`
	StableIDHashBuild localHNSWAttributionBuildEvidenceV1       `json:"stable_id_hash_order_build"`
	SourceOrderGraph  localHNSWRepairCalibrationGraphV1         `json:"source_order_graph"`
	StableIDHashGraph localHNSWRepairCalibrationGraphV1         `json:"stable_id_hash_order_graph"`
	Calibration       localHNSWOrderCalibrationReportEvidenceV1 `json:"calibration"`
	Timing            localHNSWRepairCalibrationTimingV1        `json:"timing"`
	Profiles          m8ProductionProfileEvidenceV1             `json:"profiles"`
	Disposition       string                                    `json:"disposition"`
	Limitations       []string                                  `json:"limitations"`
}

type localHNSWOrderCalibrationReportEvidenceV1 struct {
	Artifact localHNSWAttributionArtifactV1     `json:"artifact"`
	Summary  localHNSWOrderCalibrationSummaryV1 `json:"summary"`
}

func runLocalHNSWOrderCalibrationV1(args []string, stdout io.Writer) (runErr error) {
	fs := flag.NewFlagSet("treedb_vector_partition_bench local-hnsw-order-calibration", flag.ContinueOnError)
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
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || dataset == "" || retainedDB == "" || calibrationSplit == "" || holdoutSplit == "" || truthArtifact == "" || historicalCSV == "" || tempRoot == "" || out == "" || profiles == "" || baseSHA == "" || headSHA == "" || sourceCheckout == "" {
		return errors.New("local-hnsw-order-calibration requires all frozen inputs, paths, provenance, and no positional arguments")
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
		return errors.New("local HNSW order calibration source lock")
	}
	sourceCheckout, err = localHNSWAttributionSourceCheckoutV1(sourceCheckout, baseSHA, headSHA)
	if err != nil || m8GitDirtyInV1(sourceCheckout) {
		return errors.New("local HNSW order calibration requires clean exact-head checkout")
	}
	if filepath.Ext(out) != ".json" {
		return errors.New("local HNSW order calibration report must use .json")
	}
	for _, directory := range []string{tempRoot, profiles} {
		info, statErr := os.Lstat(directory)
		if statErr != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return errors.New("invalid local HNSW order calibration directory")
		}
	}
	if entries, readErr := os.ReadDir(profiles); readErr != nil || len(entries) != 0 {
		return errors.New("local HNSW order calibration profile directory must be empty")
	}
	queryPath := strings.TrimSuffix(out, filepath.Ext(out)) + ".queries.jsonl.gz"
	for _, path := range []string{out, queryPath} {
		if _, statErr := os.Lstat(path); !errors.Is(statErr, os.ErrNotExist) {
			return errors.New("local HNSW order calibration output exists")
		}
	}
	parts := strings.Split(historicalCSV, ",")
	if len(parts) != 3 {
		return errors.New("local HNSW order calibration requires three historical reports")
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
		return errors.New("local HNSW order calibration dataset identity")
	}
	fixture, err := loadFixture(dataset)
	if err != nil || !localHNSWAttributionFixtureV1(fixture) {
		return errors.New("local HNSW order calibration fixture identity")
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
		return errors.New("local HNSW order calibration query source")
	}
	sourceOrder, sourceBuild, err := localHNSWAttributionBuildVariantV1(source, tempRoot, collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1, 4107001)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, sourceOrder.Close()) }()
	stableIDHash, hashBuild, err := localHNSWAttributionBuildVariantV1(source, tempRoot, collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationStableIDHashV1, 4107002)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, stableIDHash.Close()) }()
	sourceGraph, err := localHNSWRepairCalibrationGraphV1Build(sourceOrder)
	if err != nil {
		return err
	}
	hashGraph, err := localHNSWRepairCalibrationGraphV1Build(stableIDHash)
	if err != nil {
		return err
	}
	artifact, rows, summary, err := localHNSWOrderCalibrationSummaryV1Build(context.Background(), queryPath, source, sourceOrder, stableIDHash, calibration.Ordinals, calibration.Queries, calibration.Truth)
	if err != nil {
		return err
	}
	capture, err := startM8ProfileCaptureV1(profiles)
	if err != nil {
		return err
	}
	timing, timingErr := localHNSWOrderCalibrationTimingV1Build(context.Background(), sourceOrder, stableIDHash, rows)
	profilePaths, stopErr := capture.Stop()
	if timingErr != nil || stopErr != nil {
		return errors.Join(timingErr, stopErr)
	}
	profileArtifacts, err := m8ProfileArtifactsV1(profilePaths)
	if err != nil {
		return err
	}
	if _, err := localHNSWAttributionInputsV1(inputConfig); err != nil {
		return fmt.Errorf("local HNSW order calibration inputs changed: %w", err)
	}
	if digest, hashErr := localHNSWAttributionRegularFileSHA256V1(datasetManifest, maxManifestBytes); hashErr != nil || digest != localHNSWAttributionFixtureManifestSHA256V1 {
		return errors.New("local HNSW order calibration dataset changed")
	}
	if _, err := localHNSWAttributionSourceCheckoutV1(sourceCheckout, baseSHA, headSHA); err != nil || m8GitDirtyInV1(sourceCheckout) {
		return errors.New("local HNSW order calibration source changed")
	}
	if digest, hashErr := m8BenchmarkExecutableSHA256V1(executable); hashErr != nil || digest != executableSHA {
		return errors.New("local HNSW order calibration executable changed")
	}
	loads, err := m8PartitionLoadsV1(source.manifest)
	if err != nil || source.descriptor == nil {
		return errors.New("local HNSW order calibration source evidence")
	}
	disposition, err := localHNSWOrderCalibrationDispositionV1(summary.StableIDHash.P2Recall.Mean, summary.StableIDHash.P16Recall.Mean, summary.StableIDHash.RoutingRecall.Mean)
	if err != nil {
		return err
	}
	report := localHNSWOrderCalibrationReportV1{Schema: localHNSWOrderCalibrationReportSchemaV1, ResultKind: "local_hnsw_order_calibration_v1", Status: "valid", GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), Provenance: localHNSWAttributionProvenanceV1{Command: commandWithProvenanceAndSourceCheckoutV1("local-hnsw-order-calibration", args, baseSHA, headSHA, sourceCheckout), BaseSHA: baseSHA, HeadSHA: headSHA, SourceCheckout: sourceCheckout, Executable: executable, ExecutableSHA256: executableSHA}, Host: m8ProductionHostV1(config{out: out, dataset: dataset}, retainedDB), Inputs: localHNSWAttributionInputsEvidenceV1{DatasetManifest: localHNSWAttributionFileInputV1{Path: datasetManifest, SHA256: localHNSWAttributionFixtureManifestSHA256V1}, Fixture: fixture, RetainedDB: retainedDB, Descriptor: localHNSWAttributionFileInputV1{Path: inputConfig.Descriptor, SHA256: inputConfig.DescriptorSHA256}, Calibration: localHNSWAttributionFileInputV1{Path: calibrationSplit, SHA256: inputConfig.CalibrationSplitSHA256}, CalibrationRows: len(inputs.Calibration.Ordinals), Holdout: localHNSWAttributionFileInputV1{Path: holdoutSplit, SHA256: inputConfig.HoldoutSplitSHA256}, HoldoutRows: len(inputs.Holdout.Ordinals), HoldoutStatus: "manifest_validated_query_outcomes_unopened", Truth: localHNSWAttributionFileInputV1{Path: truthArtifact, SHA256: inputConfig.TruthArtifactSHA256}, TruthStatus: "sha256_only_not_decoded", Historical: historical}, Source: localHNSWAttributionSourceEvidenceV1{IndexName: source.manifest.IndexName, PartitionGeneration: source.manifest.Generation, Partitions: source.manifest.PartitionCount, ManifestIntegrity: source.manifest.IntegrityDigest, ReadySetDigest: source.manifest.ReadySetDigest, SourceGeneration: source.manifest.SourceGeneration, SourceChecksum: source.manifest.SourceChecksum, SourceSchemaHash: source.manifest.SourceSchemaHash, SourceRows: source.manifest.SourceRowCount, RouterGeneration: source.manifest.RouterGeneration, RouterModelDigest: source.status.ModelDigest, RouterRepresentatives: source.status.Representatives, PartitionLoads: loads, Descriptor: *source.descriptor}, TopK: 10, EFSearch: 128, ProbeCounts: []int{2, int(source.manifest.PartitionCount)}, SourceOrderBuild: sourceBuild, StableIDHashBuild: hashBuild, SourceOrderGraph: sourceGraph, StableIDHashGraph: hashGraph, Calibration: localHNSWOrderCalibrationReportEvidenceV1{Artifact: artifact, Summary: summary}, Timing: timing, Profiles: m8ProductionProfileEvidenceV1{Directory: profiles, Captured: profilePaths, Artifacts: profileArtifacts, Status: "complete", Scope: "ordinary source-order versus stable-ID-hash-order auxiliary-navigation local search; top_k=10 ef_search=128 probes=2,all concurrency=1 four order-balanced repetitions"}, Disposition: disposition, Limitations: []string{"offline calibration-only insertion-order pre-gate; not production qualification", "holdout query outcomes and trusted truth contents remained unopened", "no distributed or 100k guardrail execution occurs unless stable-ID-hash p2 recall is eligible"}}
	if err := localHNSWAttributionMatchFileSHA256V1(artifact.Path, m8ProfileArtifactMaxBytesV1, artifact.SHA256); err != nil {
		return fmt.Errorf("local HNSW order calibration sidecar changed: %w", err)
	}
	if err := validateLocalHNSWOrderCalibrationReportV1(report); err != nil {
		return err
	}
	if err := writeVectorPartitionSystemJSONExclusiveV1(out, report); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "report=%s queries=%s queries_sha256=%s source_order_p2=%.6f stable_id_hash_order_p2=%.6f disposition=%s\n", out, queryPath, artifact.SHA256, summary.SourceOrder.P2Recall.Mean, summary.StableIDHash.P2Recall.Mean, disposition)
	return err
}

func localHNSWOrderCalibrationDispositionV1(p2, p16, routing float64) (string, error) {
	if !localHNSWAttributionFiniteRecallV1(p2) || !localHNSWAttributionFiniteRecallV1(p16) || !localHNSWAttributionFiniteRecallV1(routing) {
		return "", errors.New("invalid local HNSW order calibration recall")
	}
	if p2 < .95 {
		return "blocker_hash_order_p2_below_0_9500", nil
	}
	if math.Abs(p16-p2) > .002 || routing < .998 {
		return "blocker_hash_order_p2_p16_or_routing_guardrail", nil
	}
	return "recall_eligible_stop_before_distributed_qualification", nil
}

func validateLocalHNSWOrderCalibrationReportV1(report localHNSWOrderCalibrationReportV1) error {
	if report.Schema != localHNSWOrderCalibrationReportSchemaV1 || report.ResultKind != "local_hnsw_order_calibration_v1" || report.Status != "valid" || report.Provenance.BaseSHA != localHNSWAttributionSourceLockV1 || report.Provenance.SourceDirty || !validLowerSHA(report.Provenance.HeadSHA) || !localHNSWAttributionSHA256V1(report.Provenance.ExecutableSHA256) || report.TopK != 10 || report.EFSearch != 128 || !slices.Equal(report.ProbeCounts, []int{2, 16}) {
		return errors.New("invalid local HNSW order report identity")
	}
	if _, err := time.Parse(time.RFC3339Nano, report.GeneratedAt); err != nil || !localHNSWAttributionFixtureV1(report.Inputs.Fixture) || report.Inputs.DatasetManifest.SHA256 != localHNSWAttributionFixtureManifestSHA256V1 || report.Inputs.Descriptor.SHA256 != localHNSWAttributionDescriptorSHA256V1 || report.Inputs.Calibration.SHA256 != localHNSWAttributionCalibrationSHA256V1 || report.Inputs.Holdout.SHA256 != localHNSWAttributionHoldoutSHA256V1 || report.Inputs.Truth.SHA256 != localHNSWAttributionTruthSHA256V1 || report.Inputs.CalibrationRows != 806 || report.Inputs.HoldoutRows != 194 || report.Inputs.HoldoutStatus != "manifest_validated_query_outcomes_unopened" || report.Inputs.TruthStatus != "sha256_only_not_decoded" || report.Source.Partitions != 16 || report.Source.SourceRows != 250000 {
		return errors.New("invalid local HNSW order report inputs")
	}
	for i, historical := range report.Inputs.Historical {
		if historical.SHA256 != localHNSWAttributionHistoricalSHA256V1[i] || historical.TopologyIdentitySHA256 != localHNSWAttributionHistoricalTopologySHA256V1[i] {
			return errors.New("invalid local HNSW order historical context")
		}
	}
	if report.SourceOrderBuild.Schema != localHNSWAttributionBuildSchemaV1 || report.StableIDHashBuild.Schema != localHNSWAttributionBuildSchemaV1 || report.SourceOrderBuild.Variant != string(collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1) || report.StableIDHashBuild.Variant != string(collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationStableIDHashV1) || report.SourceOrderBuild.VariantIdentity == "" || report.StableIDHashBuild.VariantIdentity == "" || report.SourceOrderBuild.Partitions != 16 || report.StableIDHashBuild.Partitions != 16 || report.SourceOrderBuild.PackBytes == 0 || report.StableIDHashBuild.PackBytes == 0 {
		return errors.New("invalid local HNSW order build report")
	}
	if !localHNSWOrderCalibrationGraphV1Valid(report.SourceOrderGraph, true) || !localHNSWOrderCalibrationGraphV1Valid(report.StableIDHashGraph, false) {
		return errors.New("invalid local HNSW order graph report")
	}
	if report.Calibration.Artifact.Schema != localHNSWAttributionSidecarSchemaV1 || report.Calibration.Artifact.Records != 806 || !localHNSWAttributionSHA256V1(report.Calibration.Artifact.SHA256) || report.Calibration.Summary.Schema != localHNSWOrderCalibrationSchemaV1 || report.Calibration.Summary.SourceOrder.QueryCount != 806 || report.Calibration.Summary.StableIDHash.QueryCount != 806 {
		return errors.New("invalid local HNSW order calibration report")
	}
	for _, aggregate := range []localHNSWRepairCalibrationAggregateV1{report.Calibration.Summary.SourceOrder, report.Calibration.Summary.StableIDHash} {
		var terminations uint64
		for reason, count := range aggregate.TerminationCount {
			if !localHNSWAttributionTimingTerminationV1(reason) || math.MaxUint64-terminations < count {
				return errors.New("invalid local HNSW order termination")
			}
			terminations += count
		}
		if terminations != 806*16 || !localHNSWAttributionFiniteRecallV1(aggregate.RoutingRecall.Mean) || !localHNSWAttributionFiniteRecallV1(aggregate.P2Recall.Mean) || !localHNSWAttributionFiniteRecallV1(aggregate.P16Recall.Mean) || aggregate.P2Work.Candidates == 0 || aggregate.P2Work.NativeEdges == 0 || aggregate.P16Work.Candidates == 0 || aggregate.P16Work.NativeEdges == 0 {
			return errors.New("incomplete local HNSW order work")
		}
	}
	if report.Calibration.Summary.SourceOrder.P16Work.AuxiliaryEdges == 0 || report.Calibration.Summary.SourceOrder.P16Work.AuxiliaryCandidates == 0 || report.Calibration.Summary.SourceOrder.P16Work.AuxiliaryAdmissions == 0 {
		return errors.New("missing source-order auxiliary work")
	}
	if len(report.Timing.Cells) != 16 || report.Profiles.Status != "complete" || report.Profiles.Scope == "" {
		return errors.New("invalid local HNSW order timing")
	}
	profiles, err := m8ProfileArtifactsV1(report.Profiles.Captured)
	if err != nil || !reflect.DeepEqual(profiles, report.Profiles.Artifacts) {
		return errors.New("invalid local HNSW order profiles")
	}
	want := [4][4]struct {
		variant string
		probes  int
	}{{{"source_order", 2}, {"stable_id_hash_order", 2}, {"source_order", 16}, {"stable_id_hash_order", 16}}, {{"stable_id_hash_order", 16}, {"source_order", 16}, {"stable_id_hash_order", 2}, {"source_order", 2}}, {{"source_order", 16}, {"stable_id_hash_order", 16}, {"source_order", 2}, {"stable_id_hash_order", 2}}, {{"stable_id_hash_order", 2}, {"source_order", 2}, {"stable_id_hash_order", 16}, {"source_order", 16}}}
	stable := map[string][]string{}
	for i, cell := range report.Timing.Cells {
		expected := want[i/4][i%4]
		if cell.Repetition != i/4 || cell.Variant != expected.variant || cell.Probes != expected.probes || cell.QueryCount != 806 || cell.ElapsedNanos == 0 || cell.QPS <= 0 || cell.P50Nanos == 0 || cell.P50Nanos > cell.P95Nanos || cell.P95Nanos > cell.P99Nanos || cell.Candidates == 0 || cell.NativeEdges == 0 || len(cell.ResultSHA256) != 806 {
			return errors.New("invalid local HNSW order timing cell")
		}
		for _, digest := range cell.ResultSHA256 {
			if !localHNSWAttributionSHA256V1(digest) {
				return errors.New("invalid local HNSW order timing digest")
			}
		}
		key := fmt.Sprintf("%s/%d", cell.Variant, cell.Probes)
		if prior, ok := stable[key]; ok && !slices.Equal(prior, cell.ResultSHA256) {
			return errors.New("unstable local HNSW order timing results")
		}
		stable[key] = cell.ResultSHA256
	}
	wantDisposition, err := localHNSWOrderCalibrationDispositionV1(report.Calibration.Summary.StableIDHash.P2Recall.Mean, report.Calibration.Summary.StableIDHash.P16Recall.Mean, report.Calibration.Summary.StableIDHash.RoutingRecall.Mean)
	if err != nil || report.Disposition != wantDisposition {
		return errors.New("invalid local HNSW order disposition")
	}
	return nil
}

func localHNSWOrderCalibrationGraphV1Valid(graph localHNSWRepairCalibrationGraphV1, sourceOrder bool) bool {
	if sourceOrder {
		return graph.Rows == 300000 && graph.NativeReachableRows == 299968 && graph.CombinedReachableRows == 300000 && graph.NativeTraversalRoots == 48 && graph.AuxiliaryEdges == 64 && graph.AuxiliaryMaxDegree <= 9
	}
	if graph.Rows != 300000 || graph.NativeReachableRows == 0 || graph.NativeReachableRows > graph.Rows || graph.CombinedReachableRows != graph.Rows || graph.NativeTraversalRoots < 16 || graph.NativeTraversalRoots > graph.Rows || graph.AuxiliaryMaxDegree > 9 {
		return false
	}
	return graph.AuxiliaryEdges == 2*(graph.NativeTraversalRoots-16)
}
