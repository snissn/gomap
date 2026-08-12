package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

const localHNSWRepairMTimingSchemaV1 = "treedb_local_hnsw_repair_m18_timing_v2"
const localHNSWRepairMTimingSelectedCurveSHA256V1 = "e5481418f41c7af33448bb35a9077a53e3fbb05d358023c0cce4a3f8fddda19d"
const localHNSWRepairMTimingSelectedCurveHeadV1 = "28319a231a9666956c37500acd00cc871eb067bd"

const (
	localHNSWRepairMTimingGateReadyV1 = "treedb-local-hnsw-repair-m-timing-ready-v1\n"
	localHNSWRepairMTimingGateStartV1 = "treedb-local-hnsw-repair-m-timing-start-v1\n"
)

type localHNSWRepairMTimingCurveV1 struct {
	Path        string `json:"path"`
	SHA256      string `json:"sha256"`
	Disposition string `json:"disposition"`
}

func localHNSWRepairMTimingEFsV1(baseline, candidate int) bool {
	return baseline == 128 && candidate == 120
}

func localHNSWRepairMTimingGateDirectoryV1(dir string) error {
	info, err := os.Lstat(dir)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("invalid local HNSW repair M timing gate directory")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		return errors.New("local HNSW repair M timing gate directory must be empty")
	}
	return nil
}

func localHNSWRepairMTimingGateMarkerV1(path, want string) error {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return errors.New("invalid local HNSW repair M timing gate marker")
	}
	raw, err := readBoundedRegularFileV1(path, int64(len(want)))
	if err != nil {
		return fmt.Errorf("invalid local HNSW repair M timing gate marker: %w", err)
	}
	if string(raw) != want {
		return errors.New("invalid local HNSW repair M timing gate marker")
	}
	return nil
}

func localHNSWRepairMTimingStartMarkerV1(path string) (bool, error) {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return false, errors.New("invalid local HNSW repair M timing gate marker")
	}
	if info.Size() > int64(len(localHNSWRepairMTimingGateStartV1)) {
		return false, errors.New("invalid local HNSW repair M timing gate marker")
	}
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, int64(len(localHNSWRepairMTimingGateStartV1)+1)))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || len(raw) > len(localHNSWRepairMTimingGateStartV1) {
		return false, errors.New("invalid local HNSW repair M timing gate marker")
	}
	if string(raw) == localHNSWRepairMTimingGateStartV1 {
		return true, nil
	}
	if strings.HasPrefix(localHNSWRepairMTimingGateStartV1, string(raw)) {
		return false, nil
	}
	return false, errors.New("invalid local HNSW repair M timing gate marker")
}

func localHNSWRepairMTimingWaitForStartV1(ctx context.Context, dir string) error {
	ready := filepath.Join(dir, "ready")
	file, err := os.OpenFile(ready, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	_, writeErr := file.WriteString(localHNSWRepairMTimingGateReadyV1)
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		return errors.Join(writeErr, closeErr)
	}
	if err := localHNSWRepairMTimingGateMarkerV1(ready, localHNSWRepairMTimingGateReadyV1); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.Name() != "ready" && entry.Name() != "start" {
				return errors.New("invalid local HNSW repair M timing gate entry")
			}
		}
		if err := localHNSWRepairMTimingGateMarkerV1(ready, localHNSWRepairMTimingGateReadyV1); err != nil {
			return err
		}
		start := filepath.Join(dir, "start")
		if _, err := os.Lstat(start); err == nil {
			complete, markerErr := localHNSWRepairMTimingStartMarkerV1(start)
			if markerErr != nil {
				return markerErr
			}
			if complete {
				return nil
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

type localHNSWRepairMTimingGateV1 struct {
	P2QPSCandidateOverBaseline  float64 `json:"p2_qps_candidate_over_baseline"`
	P16QPSCandidateOverBaseline float64 `json:"p16_qps_candidate_over_baseline"`
	P2P95CandidateOverBaseline  float64 `json:"p2_p95_candidate_over_baseline"`
	P16P95CandidateOverBaseline float64 `json:"p16_p95_candidate_over_baseline"`
	Disposition                 string  `json:"disposition"`
}

type localHNSWRepairMTimingReportV1 struct {
	Schema             string                               `json:"schema"`
	ResultKind         string                               `json:"result_kind"`
	Status             string                               `json:"status"`
	GeneratedAt        string                               `json:"generated_at"`
	Provenance         localHNSWAttributionProvenanceV1     `json:"provenance"`
	Host               m8ProductionHostEvidenceV1           `json:"host"`
	Inputs             localHNSWAttributionInputsEvidenceV1 `json:"inputs"`
	Source             localHNSWAttributionSourceEvidenceV1 `json:"source"`
	TopK               int                                  `json:"top_k"`
	BaselineEFSearch   int                                  `json:"baseline_ef_search"`
	CandidateEFSearch  int                                  `json:"candidate_ef_search"`
	ProbeCounts        []int                                `json:"probe_counts"`
	BaselineBuild      localHNSWAttributionBuildEvidenceV1  `json:"m16_efc128_build"`
	Candidate          localHNSWRepairMCurveCellV1          `json:"m18_efc256_candidate"`
	SelectedCurve      localHNSWRepairMTimingCurveV1        `json:"selected_curve"`
	Quality            localHNSWRepairEFCurveCellV1         `json:"quality"`
	Calibration        localHNSWRepairMTimingSummaryV1      `json:"calibration"`
	Timing             localHNSWRepairCalibrationTimingV1   `json:"timing"`
	TimingRoutesSHA256 string                               `json:"timing_routes_sha256"`
	Gate               localHNSWRepairMTimingGateV1         `json:"gate"`
	Profiles           m8ProductionProfileEvidenceV1        `json:"profiles"`
	Limitations        []string                             `json:"limitations"`
}

type localHNSWRepairMTimingSummaryV1 struct {
	BaselineRoutesSHA256  string                                `json:"m16_efc128_routes_sha256"`
	CandidateRoutesSHA256 string                                `json:"m18_efc256_routes_sha256"`
	Baseline              localHNSWRepairCalibrationAggregateV1 `json:"m16_efc128"`
	Candidate             localHNSWRepairCalibrationAggregateV1 `json:"m18_efc256"`
}

func loadLocalHNSWRepairM18EFCurveV1(path string) (localHNSWRepairM18EFCurveReportV1, string, error) {
	raw, err := readBoundedRegularFileV1(path, m8QualificationMatrixMaxBytesV1)
	if err != nil {
		return localHNSWRepairM18EFCurveReportV1{}, "", err
	}
	var report localHNSWRepairM18EFCurveReportV1
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&report); err != nil {
		return report, "", err
	}
	if decoder.Decode(&struct{}{}) != io.EOF || validateLocalHNSWRepairM18EFCurveReportV1(report) != nil {
		return report, "", errors.New("invalid local HNSW repair M timing curve")
	}
	digest := sha256.Sum256(raw)
	return report, fmt.Sprintf("%x", digest[:]), nil
}

func localHNSWRepairMTimingSelectedCurveUnchangedV1(path, wantSHA string) error {
	if err := localHNSWAttributionMatchFileSHA256V1(path, m8QualificationMatrixMaxBytesV1, wantSHA); err != nil {
		return fmt.Errorf("local HNSW repair M timing selected curve changed: %w", err)
	}
	return nil
}

func localHNSWRepairMTimingGateV1Build(cells []localHNSWRepairCalibrationTimingCellV1) (localHNSWRepairMTimingGateV1, error) {
	var out localHNSWRepairMTimingGateV1
	if len(cells) != 16 {
		return out, errors.New("invalid local HNSW repair M timing cells")
	}
	qps := [2][2][]float64{}
	p95 := [2][2][]uint64{}
	digests := [2][2][]string{}
	for _, cell := range cells {
		variant := 0
		if cell.Variant == "m18_efc256" {
			variant = 1
		} else if cell.Variant != "m16_efc128" {
			return out, errors.New("invalid local HNSW repair M timing variant")
		}
		probe := 0
		if cell.Probes == 16 {
			probe = 1
		} else if cell.Probes != 2 {
			return out, errors.New("invalid local HNSW repair M timing probes")
		}
		if cell.QueryCount != 806 || !localHNSWRepairEFCurveFinitePositiveV1(cell.QPS) || cell.P50Nanos == 0 || cell.P50Nanos > cell.P95Nanos || cell.P95Nanos > cell.P99Nanos || cell.Candidates == 0 || cell.NativeEdges == 0 || len(cell.ResultSHA256) != 806 {
			return out, errors.New("invalid local HNSW repair M timing cell")
		}
		for _, digest := range cell.ResultSHA256 {
			if !localHNSWAttributionSHA256V1(digest) {
				return out, errors.New("invalid local HNSW repair M timing digest")
			}
		}
		if prior := digests[variant][probe]; prior != nil && !slices.Equal(prior, cell.ResultSHA256) {
			return out, errors.New("unstable local HNSW repair M timing results")
		} else if prior == nil {
			digests[variant][probe] = append([]string(nil), cell.ResultSHA256...)
		}
		qps[variant][probe] = append(qps[variant][probe], cell.QPS)
		p95[variant][probe] = append(p95[variant][probe], cell.P95Nanos)
	}
	var err error
	if out.P2QPSCandidateOverBaseline, err = localHNSWRepairEFCurveMedianRatioV1(qps[1][0], qps[0][0]); err != nil {
		return out, err
	}
	if out.P16QPSCandidateOverBaseline, err = localHNSWRepairEFCurveMedianRatioV1(qps[1][1], qps[0][1]); err != nil {
		return out, err
	}
	if out.P2P95CandidateOverBaseline, err = localHNSWRepairEFCurveMedianUintRatioV1(p95[1][0], p95[0][0]); err != nil {
		return out, err
	}
	if out.P16P95CandidateOverBaseline, err = localHNSWRepairEFCurveMedianUintRatioV1(p95[1][1], p95[0][1]); err != nil {
		return out, err
	}
	if out.P2QPSCandidateOverBaseline >= .90 && out.P16QPSCandidateOverBaseline >= .90 && out.P2P95CandidateOverBaseline <= 1.10 && out.P16P95CandidateOverBaseline <= 1.10 {
		out.Disposition = "calibration_timing_gate_pass"
	} else {
		out.Disposition = "calibration_timing_gate_fail"
	}
	return out, nil
}

func validateLocalHNSWRepairMTimingReportV1(report localHNSWRepairMTimingReportV1) error {
	if report.Schema != localHNSWRepairMTimingSchemaV1 || report.ResultKind != "local_hnsw_repair_m18_timing_v2" || report.Status != "valid" || report.Provenance.BaseSHA != localHNSWAttributionSourceLockV1 || report.Provenance.SourceDirty || !validLowerSHA(report.Provenance.HeadSHA) || !localHNSWAttributionSHA256V1(report.Provenance.ExecutableSHA256) || report.TopK != 10 || !localHNSWRepairMTimingEFsV1(report.BaselineEFSearch, report.CandidateEFSearch) || !slices.Equal(report.ProbeCounts, []int{2, 16}) || !localHNSWAttributionFixtureV1(report.Inputs.Fixture) || report.Inputs.DatasetManifest.SHA256 != localHNSWAttributionFixtureManifestSHA256V1 || !localHNSWRepairM18EFCurveDescriptorV1(report.Inputs.Descriptor.SHA256) || report.Inputs.Calibration.SHA256 != localHNSWAttributionCalibrationSHA256V1 || report.Inputs.Holdout.SHA256 != localHNSWAttributionHoldoutSHA256V1 || report.Inputs.Truth.SHA256 != localHNSWAttributionTruthSHA256V1 || report.Inputs.CalibrationRows != 806 || report.Inputs.HoldoutRows != 194 || report.Inputs.HoldoutStatus != "manifest_validated_lower_ef_query_outcomes_unopened" || report.Inputs.TruthStatus != "sha256_only_not_decoded" || report.Source.Partitions != 16 || report.Source.SourceRows != 250000 || len(report.Source.PartitionLoads) != 16 || report.Source.ManifestIntegrity == "" || report.Source.ReadySetDigest == "" || report.Source.RouterModelDigest == "" || report.BaselineBuild.Variant != string(collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1) || report.Candidate.M != 18 || report.Candidate.EfConstruction != 256 || report.Candidate.Build.Variant != string(collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1) || report.Candidate.Graph.Rows != 300000 || report.Candidate.Graph.CombinedReachableRows != 300000 || report.Calibration.Baseline.QueryCount != 806 || report.Calibration.Candidate.QueryCount != 806 || report.Profiles.Status != "complete" || report.SelectedCurve.Path == "" || report.SelectedCurve.SHA256 != localHNSWRepairMTimingSelectedCurveSHA256V1 || report.SelectedCurve.Disposition != "smallest_point_passes_ef_120" {
		return errors.New("invalid local HNSW repair M timing report")
	}
	if report.Quality.QueryCount != 806 || report.Quality.EFSearch != report.CandidateEFSearch || report.Quality.P2Recall.Mean < .95 || report.Quality.RoutingMissSlots > 20 || !localHNSWAttributionSHA256V1(report.Quality.RoutesSHA256) || !localHNSWAttributionSHA256V1(report.Quality.P2ResultsSHA256) || !localHNSWAttributionSHA256V1(report.Quality.P16ResultsSHA256) || report.Quality.RoutesSHA256 != report.Calibration.BaselineRoutesSHA256 || report.Quality.RoutesSHA256 != report.Calibration.CandidateRoutesSHA256 || report.Quality.RoutesSHA256 != report.TimingRoutesSHA256 || !localHNSWRepairMCurveSlotMeansV1(report.Quality) || localHNSWRepairMCurveHitSlotGapV1(report.Quality.P2HitSlots, report.Quality.P16HitSlots) > 20 || !reflect.DeepEqual(report.Candidate.Quality, report.Quality) {
		return errors.New("invalid local HNSW repair M timing quality")
	}
	want, err := localHNSWRepairMTimingGateV1Build(report.Timing.Cells)
	if err != nil || want != report.Gate {
		return errors.New("invalid local HNSW repair M timing gate")
	}
	return nil
}

func runLocalHNSWRepairMTimingV1(args []string, stdout io.Writer) (runErr error) {
	fs := flag.NewFlagSet("treedb_vector_partition_bench local-hnsw-repair-m-timing", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var dataset, retainedDB, calibrationSplit, holdoutSplit, truthArtifact, historicalCSV, tempRoot, out, profiles, timingGateDir, selectedCurve, baseSHA, headSHA, sourceCheckout string
	fs.StringVar(&dataset, "dataset", "", "frozen fixture directory")
	fs.StringVar(&retainedDB, "retained-db", "", "literal retained 250k database")
	fs.StringVar(&calibrationSplit, "calibration-split", "", "frozen calibration manifest")
	fs.StringVar(&holdoutSplit, "holdout-split", "", "sealed holdout manifest")
	fs.StringVar(&truthArtifact, "truth-artifact", "", "sealed trusted truth artifact")
	fs.StringVar(&historicalCSV, "historical-search", "", "three comma-separated retained search reports")
	fs.StringVar(&tempRoot, "temp-root", "", "existing fast temporary root")
	fs.StringVar(&out, "out", "", "fresh report path")
	fs.StringVar(&profiles, "profiles", "", "fresh existing profile directory")
	fs.StringVar(&timingGateDir, "timing-gate-dir", "", "empty directory requiring ready/start timing release markers")
	fs.StringVar(&selectedCurve, "selected-curve", "", "frozen M18 lower-EF curve report")
	fs.StringVar(&baseSHA, "base-sha", "", "source-lock base SHA")
	fs.StringVar(&headSHA, "head-sha", "", "exact implementation head SHA")
	fs.StringVar(&sourceCheckout, "source-checkout", "", "clean exact-head checkout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || dataset == "" || retainedDB == "" || calibrationSplit == "" || holdoutSplit == "" || truthArtifact == "" || historicalCSV == "" || tempRoot == "" || out == "" || profiles == "" || timingGateDir == "" || selectedCurve == "" || baseSHA == "" || headSHA == "" || sourceCheckout == "" {
		return errors.New("local-hnsw-repair-m-timing requires all frozen inputs")
	}
	var err error
	for destination, value := range map[*string]string{&dataset: dataset, &retainedDB: retainedDB, &calibrationSplit: calibrationSplit, &holdoutSplit: holdoutSplit, &truthArtifact: truthArtifact, &tempRoot: tempRoot, &out: out, &profiles: profiles, &timingGateDir: timingGateDir, &selectedCurve: selectedCurve, &sourceCheckout: sourceCheckout} {
		if *destination, err = m8CanonicalPathV1(value); err != nil {
			return err
		}
	}
	baseSHA, headSHA, err = provenanceWithExplicitV1(baseSHA, headSHA)
	if err != nil || baseSHA != localHNSWAttributionSourceLockV1 {
		return errors.New("local HNSW repair M timing source lock")
	}
	if sourceCheckout, err = localHNSWAttributionSourceCheckoutV1(sourceCheckout, baseSHA, headSHA); err != nil || m8GitDirtyInV1(sourceCheckout) {
		return errors.New("local HNSW repair M timing requires clean exact-head checkout")
	}
	if filepath.Ext(out) != ".json" {
		return errors.New("local HNSW repair M timing report must use .json")
	}
	curve, curveSHA, err := loadLocalHNSWRepairM18EFCurveV1(selectedCurve)
	if err != nil || curve.Disposition != "smallest_point_passes_ef_120" || curve.Provenance.HeadSHA != localHNSWRepairMTimingSelectedCurveHeadV1 || curveSHA != localHNSWRepairMTimingSelectedCurveSHA256V1 {
		return errors.New("invalid local HNSW repair M timing selected curve")
	}
	for _, dir := range []string{tempRoot, profiles} {
		info, statErr := os.Lstat(dir)
		if statErr != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return errors.New("invalid local HNSW repair M timing directory")
		}
	}
	if entries, readErr := os.ReadDir(profiles); readErr != nil || len(entries) != 0 {
		return errors.New("local HNSW repair M timing profile directory must be empty")
	}
	if err := localHNSWRepairMTimingGateDirectoryV1(timingGateDir); err != nil {
		return err
	}
	for _, path := range []string{out} {
		if _, statErr := os.Lstat(path); !errors.Is(statErr, os.ErrNotExist) {
			return errors.New("local HNSW repair M timing output exists")
		}
	}
	parts := strings.Split(historicalCSV, ",")
	if len(parts) != 3 {
		return errors.New("local HNSW repair M timing requires three historical reports")
	}
	var historicalPaths [3]string
	for i, path := range parts {
		if historicalPaths[i], err = m8CanonicalPathV1(strings.TrimSpace(path)); err != nil {
			return err
		}
	}
	fixture, err := loadFixture(dataset)
	if err != nil || !localHNSWAttributionFixtureV1(fixture) {
		return errors.New("local HNSW repair M timing fixture identity")
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
	if err = localHNSWRepairCalibrationBindDescriptorV1(source, fixture); err != nil {
		return err
	}
	calibration, err := localHNSWAttributionCalibrationV1Build(source, fixture, inputs.Calibration.Ordinals)
	if err != nil || len(calibration.Ordinals) != 806 {
		return errors.New("local HNSW repair M timing calibration")
	}
	overlay, overlayBuild, err := localHNSWAttributionBuildVariantV1(source, tempRoot, collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1, 4106181)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, overlay.Close()) }()
	candidate, candidateBuild, err := localHNSWAttributionBuildVariantV1(source, tempRoot, collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1, 4106182)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, candidate.Close()) }()
	graph, err := localHNSWRepairCalibrationGraphV1Build(candidate)
	if err != nil {
		return err
	}
	rows, summary, err := localHNSWRepairMTimingRowsAtEFV1Build(context.Background(), source, overlay, candidate, calibration.Ordinals, calibration.Queries, calibration.Truth, 128, 120)
	if err != nil {
		return err
	}
	gateContext, stopGate := signal.NotifyContext(context.Background(), os.Interrupt)
	err = localHNSWRepairMTimingWaitForStartV1(gateContext, timingGateDir)
	stopGate()
	if err != nil {
		return err
	}
	if _, err := localHNSWAttributionInputsV1(inputConfig); err != nil {
		return fmt.Errorf("local HNSW repair M timing inputs changed: %w", err)
	}
	if _, err := localHNSWAttributionSourceCheckoutV1(sourceCheckout, baseSHA, headSHA); err != nil || m8GitDirtyInV1(sourceCheckout) {
		return errors.New("local HNSW repair M timing source changed")
	}
	if digest, err := m8BenchmarkExecutableSHA256V1(executable); err != nil || digest != executableSHA {
		return errors.New("local HNSW repair M timing executable changed")
	}
	if err := localHNSWRepairMTimingSelectedCurveUnchangedV1(selectedCurve, curveSHA); err != nil {
		return err
	}
	capture, err := startM8ProfileCaptureV1(profiles)
	if err != nil {
		return err
	}
	timing, timingErr := localHNSWRepairCalibrationTimingAtEFV1Build(context.Background(), overlay, candidate, rows, 128, 120)
	profilePaths, stopErr := capture.Stop()
	if timingErr != nil || stopErr != nil {
		return errors.Join(timingErr, stopErr)
	}
	profileArtifacts, err := m8ProfileArtifactsV1(profilePaths)
	if err != nil {
		return err
	}
	for i := range timing.Cells {
		if timing.Cells[i].Variant == "overlay_current" {
			timing.Cells[i].Variant = "m16_efc128"
		} else {
			timing.Cells[i].Variant = "m18_efc256"
		}
	}
	gate, err := localHNSWRepairMTimingGateV1Build(timing.Cells)
	if err != nil {
		return err
	}
	loads, err := m8PartitionLoadsV1(source.manifest)
	if err != nil || source.descriptor == nil {
		return errors.New("local HNSW repair M timing source evidence")
	}
	membershipSHA, checksumSHA, err := localHNSWRepairConstructionCurvePackIdentityV1(candidate)
	if err != nil {
		return err
	}
	var definition collections.VectorIndexDefinition
	for _, candidateDefinition := range source.collection.Meta().VectorIndexes {
		if candidateDefinition.Name == source.manifest.IndexName {
			definition = candidateDefinition
			break
		}
	}
	if definition.Name == "" {
		return errors.New("local HNSW repair M timing source definition")
	}
	definition.M, definition.EfConstruction = 18, 256
	candidateInfo := localHNSWRepairMCurveCellV1{M: 18, EfConstruction: 256, DefinitionDigest: collections.VectorIndexDefinitionDigestV1(definition), PackMembershipSHA256: membershipSHA, PackChecksumsSHA256: checksumSHA, Build: candidateBuild, Graph: graph}
	quality := localHNSWRepairEFCurveCellV1{EFSearch: 120, QueryCount: len(rows), RoutingRecall: summary.Repair.RoutingRecall, P2Recall: summary.Repair.P2Recall, P16Recall: summary.Repair.P16Recall, P2Work: summary.Repair.P2Work, P16Work: summary.Repair.P16Work, TerminationCount: summary.Repair.TerminationCount}
	quality.RoutingMissSlots, quality.P2HitSlots, quality.P16HitSlots = localHNSWRepairMTimingHitSlotsV1(rows)
	quality.RoutesSHA256, err = localHNSWRepairMTimingRoutesSHA256V1(rows)
	if err != nil {
		return err
	}
	quality.P2ResultsSHA256, quality.P16ResultsSHA256, err = localHNSWRepairMTimingResultDigestsV1(rows)
	if err != nil {
		return err
	}
	if !localHNSWRepairMTimingQualityMatchesCurveV1(quality, curve.Cells) {
		return errors.New("local HNSW repair M timing selected curve drift")
	}
	candidateInfo.Quality = quality
	if _, err := localHNSWAttributionInputsV1(inputConfig); err != nil {
		return fmt.Errorf("local HNSW repair M timing inputs changed: %w", err)
	}
	if _, err := localHNSWAttributionSourceCheckoutV1(sourceCheckout, baseSHA, headSHA); err != nil || m8GitDirtyInV1(sourceCheckout) {
		return errors.New("local HNSW repair M timing source changed")
	}
	if digest, err := m8BenchmarkExecutableSHA256V1(executable); err != nil || digest != executableSHA {
		return errors.New("local HNSW repair M timing executable changed")
	}
	inputsEvidence := localHNSWAttributionInputsEvidenceV1{DatasetManifest: localHNSWAttributionFileInputV1{Path: filepath.Join(dataset, "fixture_manifest.json"), SHA256: localHNSWAttributionFixtureManifestSHA256V1}, Fixture: fixture, RetainedDB: retainedDB, Descriptor: localHNSWAttributionFileInputV1{Path: inputConfig.Descriptor, SHA256: inputConfig.DescriptorSHA256}, Calibration: localHNSWAttributionFileInputV1{Path: calibrationSplit, SHA256: inputConfig.CalibrationSplitSHA256}, CalibrationRows: len(inputs.Calibration.Ordinals), Holdout: localHNSWAttributionFileInputV1{Path: holdoutSplit, SHA256: inputConfig.HoldoutSplitSHA256}, HoldoutRows: len(inputs.Holdout.Ordinals), HoldoutStatus: "manifest_validated_lower_ef_query_outcomes_unopened", Truth: localHNSWAttributionFileInputV1{Path: truthArtifact, SHA256: inputConfig.TruthArtifactSHA256}, TruthStatus: "sha256_only_not_decoded", Historical: historical}
	report := localHNSWRepairMTimingReportV1{Schema: localHNSWRepairMTimingSchemaV1, ResultKind: "local_hnsw_repair_m18_timing_v2", Status: "valid", GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), Provenance: localHNSWAttributionProvenanceV1{Command: commandWithProvenanceAndSourceCheckoutV1("local-hnsw-repair-m-timing", args, baseSHA, headSHA, sourceCheckout), BaseSHA: baseSHA, HeadSHA: headSHA, SourceCheckout: sourceCheckout, Executable: executable, ExecutableSHA256: executableSHA}, Host: m8ProductionHostV1(config{out: out, dataset: dataset}, retainedDB), Inputs: inputsEvidence, Source: localHNSWAttributionSourceEvidenceV1{IndexName: source.manifest.IndexName, PartitionGeneration: source.manifest.Generation, Partitions: source.manifest.PartitionCount, ManifestIntegrity: source.manifest.IntegrityDigest, ReadySetDigest: source.manifest.ReadySetDigest, SourceGeneration: source.manifest.SourceGeneration, SourceChecksum: source.manifest.SourceChecksum, SourceSchemaHash: source.manifest.SourceSchemaHash, SourceRows: source.manifest.SourceRowCount, RouterGeneration: source.manifest.RouterGeneration, RouterModelDigest: source.status.ModelDigest, RouterRepresentatives: source.status.Representatives, PartitionLoads: loads, Descriptor: *source.descriptor}, TopK: 10, BaselineEFSearch: 128, CandidateEFSearch: 120, ProbeCounts: []int{2, 16}, BaselineBuild: overlayBuild, Candidate: candidateInfo, SelectedCurve: localHNSWRepairMTimingCurveV1{Path: selectedCurve, SHA256: curveSHA, Disposition: curve.Disposition}, Quality: quality, Calibration: localHNSWRepairMTimingSummaryV1{BaselineRoutesSHA256: quality.RoutesSHA256, CandidateRoutesSHA256: quality.RoutesSHA256, Baseline: summary.Overlay, Candidate: summary.Repair}, Timing: timing, TimingRoutesSHA256: quality.RoutesSHA256, Gate: gate, Profiles: m8ProductionProfileEvidenceV1{Directory: profiles, Captured: profilePaths, Artifacts: profileArtifacts, Status: "complete", Scope: "ordinary m16_efc128 ef_search=128 versus m18_efc256 ef_search=120 auxiliary-navigation local search; top_k=10 probes=2,all concurrency=1 four order-balanced repetitions"}, Limitations: []string{"offline calibration-only timing pre-gate; not product qualification", "holdout manifest was validated; lower-EF holdout query outcomes remained unopened"}}
	if err := validateLocalHNSWRepairMTimingReportV1(report); err != nil {
		return err
	}
	if err := writeVectorPartitionSystemJSONExclusiveV1(out, report); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "report=%s timing=%s\n", out, gate.Disposition)
	return err
}

func localHNSWRepairMTimingRowsV1Build(ctx context.Context, source *m8ProductionMultiGroupAssetsV1, baseline, candidate *localHNSWVariantHarnessV1, ordinals []int, queries [][]float32, truth [][]m8CanonicalResultV1) ([]localHNSWRepairCalibrationQueryV1, localHNSWRepairCalibrationSummaryV1, error) {
	return localHNSWRepairMTimingRowsAtEFV1Build(ctx, source, baseline, candidate, ordinals, queries, truth, 128, 128)
}

func localHNSWRepairMTimingRowsAtEFV1Build(ctx context.Context, source *m8ProductionMultiGroupAssetsV1, baseline, candidate *localHNSWVariantHarnessV1, ordinals []int, queries [][]float32, truth [][]m8CanonicalResultV1, baselineEF, candidateEF int) ([]localHNSWRepairCalibrationQueryV1, localHNSWRepairCalibrationSummaryV1, error) {
	if len(ordinals) != 806 || len(ordinals) != len(queries) || len(ordinals) != len(truth) {
		return nil, localHNSWRepairCalibrationSummaryV1{}, errors.New("invalid local HNSW repair M timing alignment")
	}
	summary := localHNSWRepairCalibrationSummaryV1{Schema: localHNSWRepairCalibrationSchemaV1}
	rows := make([]localHNSWRepairCalibrationQueryV1, 0, len(ordinals))
	for i, ordinal := range ordinals {
		if i > 0 && ordinals[i-1] >= ordinal {
			return nil, localHNSWRepairCalibrationSummaryV1{}, errors.New("invalid local HNSW repair M timing ordinals")
		}
		row, err := localHNSWRepairCalibrationQueryAtEFV1Build(ctx, source, baseline, candidate, ordinal, queries[i], truth[i], baselineEF, candidateEF)
		if err != nil || localHNSWRepairCalibrationSummaryAddV1(&summary, row) != nil {
			return nil, localHNSWRepairCalibrationSummaryV1{}, errors.New("invalid local HNSW repair M timing query")
		}
		rows = append(rows, row)
	}
	if err := localHNSWRepairCalibrationSummaryFinishV1(&summary); err != nil {
		return nil, localHNSWRepairCalibrationSummaryV1{}, err
	}
	return rows, summary, nil
}

func localHNSWRepairMTimingHitSlotsV1(rows []localHNSWRepairCalibrationQueryV1) (routingMisses, p2Hits, p16Hits uint64) {
	for _, row := range rows {
		routingMisses += uint64(math.Round(10 * (1 - row.RoutingRecall)))
		truth := make(map[string]struct{}, len(row.Truth))
		for _, want := range row.Truth {
			truth[want.ID] = struct{}{}
		}
		for _, result := range row.Repair.P2Results {
			if _, ok := truth[result.ID]; ok {
				p2Hits++
			}
		}
		for _, result := range row.Repair.P16Results {
			if _, ok := truth[result.ID]; ok {
				p16Hits++
			}
		}
	}
	return
}

func localHNSWRepairMTimingResultDigestsV1(rows []localHNSWRepairCalibrationQueryV1) (string, string, error) {
	if len(rows) != 806 {
		return "", "", errors.New("invalid local HNSW repair M timing result row count")
	}
	p2, p16 := sha256.New(), sha256.New()
	p2.Write([]byte("treedb-4106-local-hnsw-repair-ef-curve-p2-results-v1/"))
	p16.Write([]byte("treedb-4106-local-hnsw-repair-ef-curve-p16-results-v1/"))
	var raw [4]byte
	for i, row := range rows {
		if row.Ordinal < 0 || !localHNSWCalibrationOrdinalV1(row.Ordinal) || i > 0 && rows[i-1].Ordinal >= row.Ordinal {
			return "", "", errors.New("invalid local HNSW repair M timing result rows")
		}
		p2Digest, err := localHNSWRepairCalibrationFrozenResultSHA256V1(row, "auxiliary_navigation", false)
		if err != nil {
			return "", "", err
		}
		p16Digest, err := localHNSWRepairCalibrationFrozenResultSHA256V1(row, "auxiliary_navigation", true)
		if err != nil {
			return "", "", err
		}
		binary.LittleEndian.PutUint32(raw[:], uint32(row.Ordinal))
		p2.Write(raw[:])
		p2.Write([]byte(p2Digest))
		p16.Write(raw[:])
		p16.Write([]byte(p16Digest))
	}
	return fmt.Sprintf("%x", p2.Sum(nil)), fmt.Sprintf("%x", p16.Sum(nil)), nil
}

func localHNSWRepairMTimingQualityMatchesCurveV1(quality localHNSWRepairEFCurveCellV1, cells []localHNSWRepairEFCurveCellV1) bool {
	for _, cell := range cells {
		if cell.EFSearch == quality.EFSearch {
			return quality.QueryCount == cell.QueryCount &&
				quality.RoutesSHA256 == cell.RoutesSHA256 &&
				quality.RoutingMissSlots == cell.RoutingMissSlots &&
				quality.P2HitSlots == cell.P2HitSlots &&
				quality.P16HitSlots == cell.P16HitSlots &&
				quality.P2ResultsSHA256 == cell.P2ResultsSHA256 &&
				quality.P16ResultsSHA256 == cell.P16ResultsSHA256 &&
				quality.P2Recall.Mean == cell.P2Recall.Mean &&
				quality.P16Recall.Mean == cell.P16Recall.Mean
		}
	}
	return false
}

func localHNSWRepairMTimingRoutesSHA256V1(rows []localHNSWRepairCalibrationQueryV1) (string, error) {
	h := sha256.New()
	h.Write([]byte("treedb-4106-local-hnsw-repair-ef-curve-routes-v1/"))
	var raw [4]byte
	for i, row := range rows {
		if row.Ordinal < 0 || !localHNSWCalibrationOrdinalV1(row.Ordinal) || i > 0 && rows[i-1].Ordinal >= row.Ordinal || !localHNSWAttributionSHA256V1(row.QueryFP32SHA256) || len(row.P2Route) != 2 || len(row.P16Route) != 16 || !localHNSWAttributionRoutePrefixV1(row.P2Route, row.P16Route) || !localHNSWAttributionRoutePermutationV1(row.P16Route, 16) {
			return "", errors.New("invalid local HNSW repair M timing routes")
		}
		binary.LittleEndian.PutUint32(raw[:], uint32(row.Ordinal))
		h.Write(raw[:])
		h.Write([]byte(row.QueryFP32SHA256))
		for _, route := range [][]uint32{row.P2Route, row.P16Route} {
			for _, partition := range route {
				binary.LittleEndian.PutUint32(raw[:], partition)
				h.Write(raw[:])
			}
		}
	}
	digest := fmt.Sprintf("%x", h.Sum(nil))
	if !localHNSWAttributionSHA256V1(digest) {
		return "", errors.New("invalid local HNSW repair M timing route digest")
	}
	return digest, nil
}
