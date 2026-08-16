package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

const (
	localHNSWAttributionReportSchemaV1          = "treedb_local_hnsw_attribution_report_v1"
	localHNSWAttributionSourceLockV1            = "fac59220f5ca5e7d9bbbce3d31c95fad708c6070"
	localHNSWAttributionFixtureManifestSHA256V1 = "14194cca83e94d776baf78897e423ba505d51b342cc189845e6b271945502025"
	localHNSWAttributionDescriptorSHA256V1      = "c0fc10a797e6c8ceb2e8f545451ce45653f701535cff1e70d010256e57881b24"
	localHNSWAttributionCalibrationSHA256V1     = "077ec68492638dfe4f3cd589e125a769149130666533491e50143767f28ea46f"
	localHNSWAttributionHoldoutSHA256V1         = "b25cc80df7d03294949f3ce3ef70f14e10692d1127d14e45b9081e07e8196e28"
	localHNSWAttributionTruthSHA256V1           = "5a518c1cb8182edc685ab692dc17a6974655572f426a4b97c10482fd1643f04e"
)

var localHNSWAttributionHistoricalSHA256V1 = [3]string{
	"34ac101b1da9e3e3e7e65e0ecb9092f99e7ceec61983b898c2ace14ecb64f512",
	"1b4d2c1c42a3da876e9f6e1ae23204467744278ef01dbd472dc6a5b0c09aefb6",
	"ca1e93367fc050ae17db95b08a75f892bdb672d09f6ffbfd3f091410ecf507c2",
}

type localHNSWAttributionProvenanceV1 struct {
	Command          []string `json:"command"`
	BaseSHA          string   `json:"base_sha"`
	HeadSHA          string   `json:"head_sha"`
	SourceCheckout   string   `json:"source_checkout"`
	SourceDirty      bool     `json:"source_dirty"`
	Executable       string   `json:"executable"`
	ExecutableSHA256 string   `json:"executable_sha256"`
}

type localHNSWAttributionFileInputV1 struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type localHNSWAttributionInputsEvidenceV1 struct {
	DatasetManifest localHNSWAttributionFileInputV1           `json:"dataset_manifest"`
	Fixture         fixtureManifest                           `json:"fixture"`
	RetainedDB      string                                    `json:"retained_db"`
	Descriptor      localHNSWAttributionFileInputV1           `json:"descriptor"`
	Calibration     localHNSWAttributionFileInputV1           `json:"calibration_split"`
	CalibrationRows int                                       `json:"calibration_rows"`
	Holdout         localHNSWAttributionFileInputV1           `json:"holdout_split"`
	HoldoutRows     int                                       `json:"holdout_rows"`
	HoldoutStatus   string                                    `json:"holdout_status"`
	Truth           localHNSWAttributionFileInputV1           `json:"truth_artifact"`
	TruthStatus     string                                    `json:"truth_status"`
	Historical      [3]localHNSWAttributionHistoricalReportV1 `json:"historical_baseline"`
}

type localHNSWAttributionSourceEvidenceV1 struct {
	IndexName             string                `json:"index_name"`
	PartitionGeneration   uint64                `json:"partition_generation"`
	Partitions            uint32                `json:"partitions"`
	ManifestIntegrity     string                `json:"manifest_integrity_digest"`
	ReadySetDigest        string                `json:"ready_set_digest"`
	SourceGeneration      uint64                `json:"source_generation"`
	SourceChecksum        uint64                `json:"source_checksum"`
	SourceSchemaHash      uint64                `json:"source_schema_hash"`
	SourceRows            uint64                `json:"source_rows"`
	RouterGeneration      uint64                `json:"router_generation"`
	RouterModelDigest     string                `json:"router_model_digest"`
	RouterRepresentatives uint64                `json:"router_representatives"`
	PartitionLoads        []uint64              `json:"partition_loads"`
	Descriptor            m3VariantDescriptorV1 `json:"descriptor"`
}

type localHNSWAttributionGraphEvidenceReportV1 struct {
	Artifact  localHNSWAttributionArtifactV1       `json:"artifact"`
	Aggregate localHNSWAttributionGraphAggregateV1 `json:"aggregate"`
}

type localHNSWAttributionCalibrationReportV1 struct {
	Artifact localHNSWAttributionArtifactV1           `json:"artifact"`
	Summary  localHNSWAttributionCalibrationSummaryV1 `json:"summary"`
}

type localHNSWAttributionInstrumentationReportV1 struct {
	Artifact localHNSWAttributionArtifactV1               `json:"artifact"`
	Summary  localHNSWAttributionInstrumentationSummaryV1 `json:"summary"`
}

type localHNSWAttributionDecisionFactsV1 struct {
	NativeDisconnected          bool    `json:"native_disconnected"`
	OverlayConnected            bool    `json:"overlay_connected"`
	MutationChangedTraversal    bool    `json:"mutation_changed_traversal"`
	MutationChangedP2TopK       bool    `json:"mutation_changed_p2_top_k"`
	MutationChangedAllTopK      bool    `json:"mutation_changed_all_top_k"`
	NativeMinusOverlayP2Recall  float64 `json:"native_minus_overlay_p2_recall"`
	NativeMinusOverlayAllRecall float64 `json:"native_minus_overlay_all_recall"`
	NativeP2TargetMet           bool    `json:"native_p2_target_met"`
	NativeAllTargetMet          bool    `json:"native_all_target_met"`
	RoutingTargetMet            bool    `json:"routing_target_met"`
}

type localHNSWAttributionReportV1 struct {
	Schema          string                                      `json:"schema"`
	ResultKind      string                                      `json:"result_kind"`
	Status          string                                      `json:"status"`
	GeneratedAt     string                                      `json:"generated_at"`
	Provenance      localHNSWAttributionProvenanceV1            `json:"provenance"`
	Host            m8ProductionHostEvidenceV1                  `json:"host"`
	Inputs          localHNSWAttributionInputsEvidenceV1        `json:"inputs"`
	Source          localHNSWAttributionSourceEvidenceV1        `json:"source"`
	TopK            int                                         `json:"top_k"`
	EFSearch        int                                         `json:"ef_search"`
	ProbeCounts     []int                                       `json:"probe_counts"`
	NativeBuild     localHNSWAttributionBuildEvidenceV1         `json:"native_build"`
	OverlayBuild    localHNSWAttributionBuildEvidenceV1         `json:"overlay_build"`
	Graph           localHNSWAttributionGraphEvidenceReportV1   `json:"graph"`
	Calibration     localHNSWAttributionCalibrationReportV1     `json:"calibration"`
	Instrumentation localHNSWAttributionInstrumentationReportV1 `json:"instrumentation"`
	Timing          localHNSWAttributionTimingEvidenceV1        `json:"timing"`
	Profiles        m8ProductionProfileEvidenceV1               `json:"profiles"`
	Decision        localHNSWAttributionDecisionFactsV1         `json:"decision_facts"`
	Limitations     []string                                    `json:"limitations"`
}

func runLocalHNSWAttributionV1(args []string, stdout io.Writer) (runErr error) {
	fs := flag.NewFlagSet("treedb_vector_partition_bench local-hnsw-attribution", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var dataset, retainedDB, calibrationSplit, holdoutSplit, truthArtifact, historicalCSV, tempRoot, out, profiles, baseSHA, headSHA, sourceCheckout string
	fs.StringVar(&dataset, "dataset", "", "frozen fixture directory")
	fs.StringVar(&retainedDB, "retained-db", "", "literal retained 250k database")
	fs.StringVar(&calibrationSplit, "calibration-split", "", "frozen calibration manifest")
	fs.StringVar(&holdoutSplit, "holdout-split", "", "sealed holdout manifest")
	fs.StringVar(&truthArtifact, "truth-artifact", "", "sealed trusted truth artifact")
	fs.StringVar(&historicalCSV, "historical-search", "", "three comma-separated retained search reports")
	fs.StringVar(&tempRoot, "temp-root", "", "existing fast temporary root")
	fs.StringVar(&out, "out", "", "fresh main report path")
	fs.StringVar(&profiles, "profiles", "", "fresh existing profile directory")
	fs.StringVar(&baseSHA, "base-sha", "", "source-lock base SHA")
	fs.StringVar(&headSHA, "head-sha", "", "exact implementation head SHA")
	fs.StringVar(&sourceCheckout, "source-checkout", "", "clean exact-head checkout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || dataset == "" || retainedDB == "" || calibrationSplit == "" || holdoutSplit == "" || truthArtifact == "" || historicalCSV == "" || tempRoot == "" || out == "" || profiles == "" || baseSHA == "" || headSHA == "" || sourceCheckout == "" {
		return errors.New("local-hnsw-attribution requires all frozen inputs, temp/output/profile paths, provenance, and no positional arguments")
	}
	var err error
	for destination, value := range map[*string]string{&dataset: dataset, &retainedDB: retainedDB, &calibrationSplit: calibrationSplit, &holdoutSplit: holdoutSplit, &truthArtifact: truthArtifact, &tempRoot: tempRoot, &out: out, &profiles: profiles, &sourceCheckout: sourceCheckout} {
		*destination, err = m8CanonicalPathV1(value)
		if err != nil {
			return err
		}
	}
	baseSHA, headSHA, err = provenanceWithExplicitV1(baseSHA, headSHA)
	if err != nil {
		return fmt.Errorf("local HNSW attribution provenance: %w", err)
	}
	if baseSHA != localHNSWAttributionSourceLockV1 {
		return errors.New("local HNSW attribution source lock")
	}
	sourceCheckout, err = localHNSWAttributionSourceCheckoutV1(sourceCheckout, baseSHA, headSHA)
	if err != nil {
		return err
	}
	if m8GitDirtyInV1(sourceCheckout) {
		return errors.New("local HNSW attribution requires clean exact-head checkout")
	}
	if filepath.Ext(out) != ".json" {
		return errors.New("local HNSW attribution report must use a .json path")
	}
	for _, directory := range []string{tempRoot, profiles} {
		info, statErr := os.Lstat(directory)
		if statErr != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return errors.New("invalid local HNSW attribution output directory")
		}
	}
	profileEntries, err := os.ReadDir(profiles)
	if err != nil || len(profileEntries) != 0 {
		return errors.New("local HNSW attribution profile directory must be empty")
	}
	prefix := strings.TrimSuffix(out, filepath.Ext(out))
	graphPath, queryPath := prefix+".graph.jsonl.gz", prefix+".queries.jsonl.gz"
	outputParent := filepath.Dir(out)
	if info, statErr := os.Lstat(outputParent); statErr != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("invalid local HNSW attribution output parent")
	}
	for _, path := range []string{out, graphPath, queryPath} {
		if _, statErr := os.Lstat(path); !errors.Is(statErr, os.ErrNotExist) {
			return errors.New("local HNSW attribution output already exists")
		}
	}
	historicalParts := strings.Split(historicalCSV, ",")
	if len(historicalParts) != 3 {
		return errors.New("local HNSW attribution requires three historical reports")
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
		return errors.New("local HNSW attribution dataset manifest identity")
	}
	fixture, err := loadFixture(dataset)
	if err != nil || !localHNSWAttributionFixtureV1(fixture) {
		return errors.New("local HNSW attribution fixture identity")
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
	if err := m8BindRetainedM3DescriptorV1(source, fixture); err != nil {
		return err
	}
	calibration, err := localHNSWAttributionCalibrationV1Build(source, fixture, inputs.Calibration.Ordinals)
	if err != nil {
		return err
	}
	native, nativeBuild, err := localHNSWAttributionBuildVariantV1(source, tempRoot, collections.VectorPartitionLocalGraphVariantNativeV1, 4105001)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, native.Close()) }()
	overlay, overlayBuild, err := localHNSWAttributionBuildVariantV1(source, tempRoot, collections.VectorPartitionLocalGraphVariantOverlayCurrentV1, 4105002)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, overlay.Close()) }()
	graphRows, graphAggregate, err := localHNSWAttributionGraphEvidenceV1(source, native, overlay)
	if err != nil {
		return err
	}
	graphArtifact, err := localHNSWAttributionWriteGzipJSONLV1(graphPath, func(encoder *json.Encoder) (int, error) {
		for i := range graphRows {
			if err := encoder.Encode(graphRows[i]); err != nil {
				return i, err
			}
		}
		return len(graphRows), nil
	})
	if err != nil {
		return err
	}
	graphRows = nil
	runtime.GC()
	queryArtifact, timingCases, summary, err := localHNSWAttributionCalibrationSummaryV1Build(context.Background(), queryPath, source, native, overlay, calibration.Ordinals, calibration.Queries, calibration.Truth)
	if err != nil {
		return err
	}
	instrumentation, err := localHNSWAttributionInstrumentationSummaryV1Build(source, native, overlay, graphAggregate, summary)
	if err != nil {
		return err
	}
	instrumentationPath := filepath.Join(filepath.Dir(out), "local_hnsw_attribution_instrumentation.jsonl.gz")
	instrumentationArtifact, err := localHNSWAttributionWriteGzipJSONLV1(instrumentationPath, func(encoder *json.Encoder) (int, error) {
		if err := encoder.Encode(instrumentation); err != nil {
			return 0, err
		}
		return 1, nil
	})
	if err != nil {
		return err
	}
	capture, err := startM8ProfileCaptureV1(profiles)
	if err != nil {
		return err
	}
	defer func() { _, stopErr := capture.Stop(); runErr = errors.Join(runErr, stopErr) }()
	timing, timingErr := localHNSWAttributionTimingV1(context.Background(), source, native, overlay, timingCases)
	profilePaths, stopErr := capture.Stop()
	if timingErr != nil || stopErr != nil {
		return errors.Join(timingErr, stopErr)
	}
	profileArtifacts, err := m8ProfileArtifactsV1(profilePaths)
	if err != nil {
		return err
	}
	if _, err := localHNSWAttributionInputsV1(inputConfig); err != nil {
		return fmt.Errorf("local HNSW attribution inputs changed during execution: %w", err)
	}
	if digest, hashErr := localHNSWAttributionRegularFileSHA256V1(datasetManifest, maxManifestBytes); hashErr != nil || digest != localHNSWAttributionFixtureManifestSHA256V1 {
		return errors.New("local HNSW attribution dataset changed during execution")
	}
	if _, err := localHNSWAttributionSourceCheckoutV1(sourceCheckout, baseSHA, headSHA); err != nil {
		return fmt.Errorf("local HNSW attribution source changed during execution: %w", err)
	}
	if m8GitDirtyInV1(sourceCheckout) {
		return errors.New("local HNSW attribution source changed during execution")
	}
	if digest, hashErr := m8BenchmarkExecutableSHA256V1(executable); hashErr != nil || digest != executableSHA {
		return errors.New("local HNSW attribution executable changed during execution")
	}
	loads, err := m8PartitionLoadsV1(source.manifest)
	if err != nil || source.descriptor == nil {
		return errors.New("local HNSW attribution source evidence")
	}
	report := localHNSWAttributionReportV1{
		Schema: localHNSWAttributionReportSchemaV1, ResultKind: "local_hnsw_attribution_v1", Status: "valid", GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Provenance: localHNSWAttributionProvenanceV1{Command: commandWithProvenanceAndSourceCheckoutV1("local-hnsw-attribution", args, baseSHA, headSHA, sourceCheckout), BaseSHA: baseSHA, HeadSHA: headSHA, SourceCheckout: sourceCheckout, SourceDirty: false, Executable: executable, ExecutableSHA256: executableSHA},
		Host:       m8ProductionHostV1(config{out: out, dataset: dataset}, retainedDB),
		Inputs:     localHNSWAttributionInputsEvidenceV1{DatasetManifest: localHNSWAttributionFileInputV1{Path: datasetManifest, SHA256: localHNSWAttributionFixtureManifestSHA256V1}, Fixture: fixture, RetainedDB: retainedDB, Descriptor: localHNSWAttributionFileInputV1{Path: inputConfig.Descriptor, SHA256: inputConfig.DescriptorSHA256}, Calibration: localHNSWAttributionFileInputV1{Path: calibrationSplit, SHA256: inputConfig.CalibrationSplitSHA256}, CalibrationRows: len(inputs.Calibration.Ordinals), Holdout: localHNSWAttributionFileInputV1{Path: holdoutSplit, SHA256: inputConfig.HoldoutSplitSHA256}, HoldoutRows: len(inputs.Holdout.Ordinals), HoldoutStatus: "manifest_validated_query_outcomes_unopened", Truth: localHNSWAttributionFileInputV1{Path: truthArtifact, SHA256: inputConfig.TruthArtifactSHA256}, TruthStatus: "sha256_only_not_decoded", Historical: historical},
		Source:     localHNSWAttributionSourceEvidenceV1{IndexName: source.manifest.IndexName, PartitionGeneration: source.manifest.Generation, Partitions: source.manifest.PartitionCount, ManifestIntegrity: source.manifest.IntegrityDigest, ReadySetDigest: source.manifest.ReadySetDigest, SourceGeneration: source.manifest.SourceGeneration, SourceChecksum: source.manifest.SourceChecksum, SourceSchemaHash: source.manifest.SourceSchemaHash, SourceRows: source.manifest.SourceRowCount, RouterGeneration: source.manifest.RouterGeneration, RouterModelDigest: source.status.ModelDigest, RouterRepresentatives: source.status.Representatives, PartitionLoads: loads, Descriptor: *source.descriptor},
		TopK:       10, EFSearch: 128, ProbeCounts: []int{2, int(source.manifest.PartitionCount)}, NativeBuild: nativeBuild, OverlayBuild: overlayBuild,
		Graph: localHNSWAttributionGraphEvidenceReportV1{Artifact: graphArtifact, Aggregate: graphAggregate}, Calibration: localHNSWAttributionCalibrationReportV1{Artifact: queryArtifact, Summary: summary}, Instrumentation: localHNSWAttributionInstrumentationReportV1{Artifact: instrumentationArtifact, Summary: instrumentation}, Timing: timing,
		Profiles:    m8ProductionProfileEvidenceV1{Directory: profiles, Captured: profilePaths, Artifacts: profileArtifacts, Status: "complete", Scope: "ordinary native-versus-overlay local search; top_k=10 ef_search=128 probes=2,all concurrency=1 four order-balanced repetitions"},
		Decision:    localHNSWAttributionDecisionFactsV1Build(graphAggregate, summary),
		Limitations: []string{"offline calibration-only causal evidence; not production qualification", "holdout query outcomes and trusted truth contents remained unopened", "native is diagnostic only when disconnected; no production default changed", "historical distributed p2/p16 cells are source-locked context, while timing here measures local retained-pack search only"},
	}
	if err := validateLocalHNSWAttributionReportV1(report); err != nil {
		return err
	}
	if err := writeVectorPartitionSystemJSONExclusiveV1(out, report); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "report=%s graph=%s graph_sha256=%s queries=%s queries_sha256=%s native_p2=%.6f overlay_p2=%.6f native_all=%.6f overlay_all=%.6f\n", out, graphPath, graphArtifact.SHA256, queryPath, queryArtifact.SHA256, summary.Native.P2EndToEnd.Mean, summary.Overlay.P2EndToEnd.Mean, summary.Native.AllGlobal.Mean, summary.Overlay.AllGlobal.Mean)
	return err
}

func localHNSWAttributionSourceCheckoutV1(path, base, head string) (string, error) {
	checkout, err := m8SourceCheckoutV1(path, head)
	if err != nil {
		return "", fmt.Errorf("local HNSW attribution source checkout: %w", err)
	}
	if !validLowerSHA(base) {
		return "", errors.New("invalid local HNSW attribution source checkout")
	}
	if err := exec.Command("git", "-C", checkout, "merge-base", "--is-ancestor", base, head).Run(); err != nil {
		return "", fmt.Errorf("local HNSW attribution base is not an ancestor of head: %w", err)
	}
	return checkout, nil
}

func localHNSWAttributionFixtureV1(fixture fixtureManifest) bool {
	return validateM3FixtureWithCaps(fixture, maxVectors, maxFixtureBytes) == nil && fixture.SchemaVersion == 1 && fixture.Fixture == "qualification_embedding_mixture_250000" && fixture.Generator == qualificationEmbeddingGeneratorV1 && fixture.Arithmetic == fixtureArithmetic && fixture.Vectors == 250000 && fixture.Queries == 1000 && fixture.Dimensions == 128 && fixture.Metric == "cosine" && fixture.Seed == 4016 && fixture.Checksum == "d0c7c82ba868853aae9a4280161003d72714ad1701d41ed3169c2fa94d470d69"
}

func localHNSWAttributionDecisionFactsV1Build(graph localHNSWAttributionGraphAggregateV1, calibration localHNSWAttributionCalibrationSummaryV1) localHNSWAttributionDecisionFactsV1 {
	return localHNSWAttributionDecisionFactsV1{NativeDisconnected: graph.NativeUnreachableRows != 0 || graph.NativeDisconnectedPacks != 0, OverlayConnected: graph.FinalUnreachableRows == 0 && graph.FinalDisconnectedPacks == 0, MutationChangedTraversal: calibration.ChangedPackVisitedDigest != 0 || calibration.ChangedPackTermination != 0, MutationChangedP2TopK: calibration.ChangedP2TopK != 0, MutationChangedAllTopK: calibration.ChangedAllTopK != 0, NativeMinusOverlayP2Recall: calibration.Native.P2EndToEnd.Mean - calibration.Overlay.P2EndToEnd.Mean, NativeMinusOverlayAllRecall: calibration.Native.AllGlobal.Mean - calibration.Overlay.AllGlobal.Mean, NativeP2TargetMet: calibration.Native.P2EndToEnd.Mean >= .95, NativeAllTargetMet: calibration.Native.AllGlobal.Mean >= .95, RoutingTargetMet: calibration.RoutingRecall.Mean >= .998}
}

func validateLocalHNSWAttributionReportV1(report localHNSWAttributionReportV1) error {
	if report.Schema != localHNSWAttributionReportSchemaV1 || report.ResultKind != "local_hnsw_attribution_v1" || report.Status != "valid" || report.Provenance.BaseSHA != localHNSWAttributionSourceLockV1 || !validLowerSHA(report.Provenance.HeadSHA) || report.Provenance.SourceDirty || !localHNSWAttributionSHA256V1(report.Provenance.ExecutableSHA256) || report.TopK != 10 || report.EFSearch != 128 || len(report.ProbeCounts) != 2 || report.ProbeCounts[0] != 2 || report.ProbeCounts[1] != 16 {
		return errors.New("invalid local HNSW attribution report identity")
	}
	if _, err := time.Parse(time.RFC3339Nano, report.GeneratedAt); err != nil || !localHNSWAttributionFixtureV1(report.Inputs.Fixture) || report.Inputs.CalibrationRows != 806 || report.Inputs.HoldoutRows != 194 || report.Inputs.HoldoutStatus != "manifest_validated_query_outcomes_unopened" || report.Inputs.TruthStatus != "sha256_only_not_decoded" {
		return errors.New("invalid local HNSW attribution report inputs")
	}
	if report.Inputs.DatasetManifest.SHA256 != localHNSWAttributionFixtureManifestSHA256V1 || report.Inputs.Descriptor.SHA256 != localHNSWAttributionDescriptorSHA256V1 || report.Inputs.Calibration.SHA256 != localHNSWAttributionCalibrationSHA256V1 || report.Inputs.Holdout.SHA256 != localHNSWAttributionHoldoutSHA256V1 || report.Inputs.Truth.SHA256 != localHNSWAttributionTruthSHA256V1 {
		return errors.New("invalid local HNSW attribution report input digests")
	}
	for i, historical := range report.Inputs.Historical {
		if historical.SHA256 != localHNSWAttributionHistoricalSHA256V1[i] || historical.TopologyIdentitySHA256 != localHNSWAttributionHistoricalTopologySHA256V1[i] || historical.Probe2.Probes != 2 || historical.Probe16.Probes != 16 {
			return errors.New("invalid local HNSW attribution historical report")
		}
	}
	if report.Source.Partitions != 16 || report.Source.SourceRows != 250000 || report.Source.Descriptor.Partitions != report.Source.Partitions || report.Source.Descriptor.ManifestIntegrity != report.Source.ManifestIntegrity || report.Source.Descriptor.ReadySetDigest != report.Source.ReadySetDigest || len(report.Source.PartitionLoads) != 16 {
		return errors.New("invalid local HNSW attribution report source")
	}
	var rows uint64
	for _, load := range report.Source.PartitionLoads {
		if math.MaxUint64-rows < load {
			return errors.New("local HNSW attribution source rows overflow")
		}
		rows += load
	}
	if rows != report.Graph.Aggregate.TotalRows || report.Graph.Aggregate.Schema != localHNSWAttributionGraphSchemaV1 || report.Graph.Aggregate.FinalReachableRows != rows || report.Graph.Aggregate.FinalUnreachableRows != 0 || report.Graph.Aggregate.FinalTraversalRoots != uint64(report.Source.Partitions) || report.Graph.Aggregate.FinalDisconnectedPacks != 0 || report.Graph.Artifact.Records != int(report.Source.Partitions) {
		return errors.New("invalid local HNSW attribution graph report")
	}
	nativeIdentity, _ := collections.VectorPartitionLocalGraphVariantIdentityV1(collections.VectorPartitionLocalGraphVariantNativeV1)
	overlayIdentity, _ := collections.VectorPartitionLocalGraphVariantIdentityV1(collections.VectorPartitionLocalGraphVariantOverlayCurrentV1)
	if report.NativeBuild.Schema != localHNSWAttributionBuildSchemaV1 || report.NativeBuild.Variant != "native" || report.NativeBuild.VariantIdentity != nativeIdentity || report.OverlayBuild.Schema != localHNSWAttributionBuildSchemaV1 || report.OverlayBuild.Variant != "overlay_current" || report.OverlayBuild.VariantIdentity != overlayIdentity || report.NativeBuild.Partitions != 16 || report.OverlayBuild.Partitions != 16 || report.NativeBuild.PackBytes == 0 || report.OverlayBuild.PackBytes == 0 {
		return errors.New("invalid local HNSW attribution build report")
	}
	if report.Calibration.Summary.Schema != localHNSWAttributionCalibrationSummarySchemaV1 || report.Calibration.Summary.QueryCount != 806 || report.Calibration.Artifact.Records != 806 || report.Timing.Schema != localHNSWAttributionTimingSchemaV1 || len(report.Timing.Cells) != 16 {
		return errors.New("invalid local HNSW attribution calibration report")
	}
	wantTiming := [4][4]localHNSWAttributionTimingOrderV1{{{"native", false}, {"overlay", false}, {"native", true}, {"overlay", true}}, {{"overlay", true}, {"native", true}, {"overlay", false}, {"native", false}}, {{"native", true}, {"overlay", true}, {"native", false}, {"overlay", false}}, {{"overlay", false}, {"native", false}, {"overlay", true}, {"native", true}}}
	stableResults := map[string][]string{}
	for i, cell := range report.Timing.Cells {
		want := wantTiming[i/4][i%4]
		wantProbes := 2
		if want.all {
			wantProbes = 16
		}
		if cell.Repetition != i/4 || cell.Variant != want.variant || cell.Probes != wantProbes || cell.QueryCount != 806 || len(cell.ResultSHA256) != 806 || cell.ElapsedNanos == 0 || cell.QPS <= 0 || cell.P50Nanos == 0 || cell.P50Nanos > cell.P95Nanos || cell.P95Nanos > cell.P99Nanos {
			return errors.New("invalid local HNSW attribution timing report")
		}
		key := fmt.Sprintf("%s/%d", cell.Variant, cell.Probes)
		if previous, ok := stableResults[key]; ok && !slices.Equal(previous, cell.ResultSHA256) {
			return errors.New("unstable local HNSW attribution timing results")
		}
		stableResults[key] = cell.ResultSHA256
	}
	for _, variant := range []localHNSWAttributionCalibrationVariantV1{report.Calibration.Summary.Native, report.Calibration.Summary.Overlay} {
		var terminations uint64
		for reason, count := range variant.TerminationCounts {
			if !localHNSWAttributionTimingTerminationV1(reason) || math.MaxUint64-terminations < count {
				return errors.New("invalid local HNSW attribution termination report")
			}
			terminations += count
		}
		if terminations != uint64(report.Calibration.Summary.QueryCount)*uint64(report.Source.Partitions) {
			return errors.New("incomplete local HNSW attribution termination report")
		}
	}
	if report.Profiles.Status != "complete" || len(report.Profiles.Artifacts) != len(m8ProfileArtifactNamesV1) || report.Decision != localHNSWAttributionDecisionFactsV1Build(report.Graph.Aggregate, report.Calibration.Summary) || len(report.Limitations) == 0 {
		return errors.New("invalid local HNSW attribution report disposition")
	}
	if report.Instrumentation.Summary.Schema != localHNSWAttributionInstrumentationSchemaV1 || report.Instrumentation.Summary.ManifestIntegrity != report.Source.ManifestIntegrity || report.Instrumentation.Summary.NativeVariant != "native" || report.Instrumentation.Summary.OverlayVariant != "overlay_current" || report.Instrumentation.Summary.NativeConstruction.FinalSurvivors == 0 || report.Instrumentation.Summary.OverlayConstruction.FinalSurvivors == 0 || report.Instrumentation.Summary.NativeQuery.ExaminedNative+report.Instrumentation.Summary.NativeQuery.ExaminedAuxiliary == 0 || report.Instrumentation.Summary.OverlayQuery.ExaminedNative+report.Instrumentation.Summary.OverlayQuery.ExaminedAuxiliary == 0 {
		return errors.New("invalid local HNSW attribution instrumentation report")
	}
	for _, artifact := range []localHNSWAttributionArtifactV1{report.Graph.Artifact, report.Calibration.Artifact, report.Instrumentation.Artifact} {
		if artifact.Schema != localHNSWAttributionSidecarSchemaV1 || artifact.Bytes < 1 || !localHNSWAttributionSHA256V1(artifact.SHA256) {
			return errors.New("invalid local HNSW attribution report artifact")
		}
		if digest, err := localHNSWAttributionRegularFileSHA256V1(artifact.Path, m8ProfileArtifactMaxBytesV1); err != nil || digest != artifact.SHA256 {
			return errors.New("local HNSW attribution report artifact drift")
		}
	}
	return nil
}
