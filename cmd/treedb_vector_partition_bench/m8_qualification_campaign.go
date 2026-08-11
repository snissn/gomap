package main

import (
	"context"
	"crypto/sha256"
	"debug/buildinfo"
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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/nativewire"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

// m8QualificationCampaignV1 is deliberately an evidence-only reader.  M8
// emits one strict matrix per run; qualification needs three independently
// hashed matrices before it can make a repeat/variance claim.
type m8QualificationCampaignV1 struct {
	FixtureChecksum string                         `json:"fixture_checksum"`
	BaseSHA         string                         `json:"base_sha"`
	HeadSHA         string                         `json:"head_sha"`
	Runs            []m8QualificationCampaignRunV1 `json:"runs"`
}

type m8QualificationCampaignRunV1 struct {
	Path                   string    `json:"path"`
	SHA256                 string    `json:"sha256"`
	PublicationCompletedAt time.Time `json:"publication_completed_at"`
}

type m8QualificationIndexV1 struct {
	SchemaVersion int                         `json:"schema_version"`
	ResultKind    string                      `json:"result_kind"`
	BaseSHA       string                      `json:"base_sha"`
	HeadSHA       string                      `json:"head_sha"`
	Campaigns     []m8QualificationCampaignV1 `json:"campaigns"`
}

type m8QualificationIndexSummaryV1 struct {
	SchemaVersion int                                         `json:"schema_version"`
	ResultKind    string                                      `json:"result_kind"`
	Status        string                                      `json:"status"`
	BaseSHA       string                                      `json:"base_sha"`
	HeadSHA       string                                      `json:"head_sha"`
	Campaigns     map[string]m8QualificationCampaignSummaryV1 `json:"campaigns"`
}

const m8QualificationFrozenBaseSHAV1 = "03e7a26e56100964f14f603f0248a1a6ccc50a68"
const m8QualificationRouterCandidatesV1 = 256

type m8QualificationCampaignSummaryV1 struct {
	ExecutableSHA256 string  `json:"executable_sha256"`
	P2QPSMin         float64 `json:"p2_qps_min"`
	P2QPSMedian      float64 `json:"p2_qps_median"`
	P2QPSMax         float64 `json:"p2_qps_max"`
	P16QPSMin        float64 `json:"p16_qps_min"`
	P16QPSMedian     float64 `json:"p16_qps_median"`
	P16QPSMax        float64 `json:"p16_qps_max"`
	P2P95Min         uint64  `json:"p2_p95_min"`
	P2P95Median      uint64  `json:"p2_p95_median"`
	P2P95Max         uint64  `json:"p2_p95_max"`
	P16P95Min        uint64  `json:"p16_p95_min"`
	P16P95Median     uint64  `json:"p16_p95_median"`
	P16P95Max        uint64  `json:"p16_p95_max"`
	intervals        []m8QualificationRunIntervalV1
}

type m8QualificationRunIntervalV1 struct {
	Path      string
	StartedAt time.Time
	EndedAt   time.Time
}

func runValidateQualification(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("treedb_vector_partition_bench validate-qualification", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var index string
	fs.StringVar(&index, "index", "", "qualification campaign index JSON; retained artifacts must be below its directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || index == "" {
		return errors.New("validate-qualification requires -index")
	}
	index, err := filepath.Abs(index)
	if err != nil {
		return fmt.Errorf("qualification index path: %w", err)
	}
	index, err = filepath.EvalSymlinks(index)
	if err != nil {
		return fmt.Errorf("resolve qualification index: %w", err)
	}
	raw, err := readBoundedRegularFileV1(index, m8QualificationIndexMaxBytesV1)
	if err != nil {
		return fmt.Errorf("read qualification index: %w", err)
	}
	var qualificationIndex m8QualificationIndexV1
	if err := json.Unmarshal(raw, &qualificationIndex); err != nil {
		return fmt.Errorf("decode qualification index: %w", err)
	}
	summary, err := m8ValidateQualificationIndexV1(filepath.Dir(index), qualificationIndex)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(summary)
}

func m8ValidateQualificationIndexV1(root string, index m8QualificationIndexV1) (m8QualificationIndexSummaryV1, error) {
	return m8ValidateQualificationIndexWithVerifiersV1(root, index, m8QualificationRetainedVariantV1, m8QualificationBenchmarkExecutableV1, m8QualificationTrustedTruthCacheV1, validM8ProductionProfilesV1, m8QualificationRetainedAttributionV1)
}

func m8ValidateQualificationIndexWithRetainedVariantV1(root string, index m8QualificationIndexV1, retainedVariant m8QualificationRetainedVariantVerifierV1) (m8QualificationIndexSummaryV1, error) {
	return m8ValidateQualificationIndexWithVerifiersV1(root, index, retainedVariant, m8QualificationBenchmarkExecutableV1, m8QualificationTrustedTruthCacheV1, validM8ProductionProfilesV1, func(string, m8ProductionReportV1, [][]m8CanonicalResultV1, m8ProductionMeasurementTranscriptV1) error {
		return nil
	})
}

func m8ValidateQualificationIndexWithVerifiersV1(root string, index m8QualificationIndexV1, retainedVariant m8QualificationRetainedVariantVerifierV1, commandExecutable m8QualificationCommandExecutableVerifierV1, truthCache m8QualificationTruthCacheVerifierV1, profileVerifier m8ProductionProfileVerifierV1, retainedAttribution m8QualificationRetainedAttributionVerifierV1) (m8QualificationIndexSummaryV1, error) {
	summary := m8QualificationIndexSummaryV1{SchemaVersion: 1, ResultKind: "vector_partition_structured_qualification_summary_v1", Status: "qualified", BaseSHA: index.BaseSHA, HeadSHA: index.HeadSHA, Campaigns: make(map[string]m8QualificationCampaignSummaryV1, len(m8QualificationFixturesV1))}
	if index.BaseSHA != m8QualificationFrozenBaseSHAV1 {
		return m8QualificationIndexSummaryV1{}, errors.New("qualification index does not use the frozen base revision")
	}
	if index.SchemaVersion != 2 || index.ResultKind != "vector_partition_structured_qualification_index_v2" || !m8QualificationGitSHAV1(index.BaseSHA) || !m8QualificationGitSHAV1(index.HeadSHA) || len(index.Campaigns) != len(m8QualificationFixturesV1) {
		return m8QualificationIndexSummaryV1{}, errors.New("qualification index requires exactly the two authoritative corpus campaigns")
	}
	for _, campaign := range index.Campaigns {
		if _, duplicate := summary.Campaigns[campaign.FixtureChecksum]; !m8QualificationFixtureChecksumV1(campaign.FixtureChecksum) || duplicate {
			return m8QualificationIndexSummaryV1{}, errors.New("qualification index has duplicate or unknown corpus")
		}
		if campaign.BaseSHA != index.BaseSHA || campaign.HeadSHA != index.HeadSHA {
			return m8QualificationIndexSummaryV1{}, errors.New("qualification index campaigns do not share the index revision")
		}
		campaignSummary, err := m8ValidateQualificationCampaignWithVerifiersV1(root, campaign, retainedVariant, commandExecutable, truthCache, profileVerifier, retainedAttribution)
		if err != nil {
			return m8QualificationIndexSummaryV1{}, err
		}
		if summary.Campaigns != nil {
			for _, prior := range summary.Campaigns {
				if prior.ExecutableSHA256 != campaignSummary.ExecutableSHA256 {
					return m8QualificationIndexSummaryV1{}, errors.New("qualification index campaigns use different benchmark executables")
				}
			}
		}
		summary.Campaigns[campaign.FixtureChecksum] = campaignSummary
	}
	intervals := make([]m8QualificationRunIntervalV1, 0, len(index.Campaigns)*3)
	for _, campaign := range summary.Campaigns {
		intervals = append(intervals, campaign.intervals...)
	}
	if err := m8ValidateQualificationSerialIntervalsV1(intervals); err != nil {
		return m8QualificationIndexSummaryV1{}, err
	}
	for _, fixture := range m8QualificationFixturesV1 {
		if _, ok := summary.Campaigns[fixture.Checksum]; !ok {
			return m8QualificationIndexSummaryV1{}, errors.New("qualification index is missing an authoritative corpus")
		}
	}
	return summary, nil
}

type m8QualificationRetainedVariantVerifierV1 func(string, m8ProductionReportV1) error
type m8QualificationCommandExecutableVerifierV1 func(string, string, string, string) bool
type m8QualificationTruthCacheVerifierV1 func(string, m8ProductionReportV1) ([][]m8CanonicalResultV1, error)
type m8QualificationRetainedAttributionVerifierV1 func(string, m8ProductionReportV1, [][]m8CanonicalResultV1, m8ProductionMeasurementTranscriptV1) error

func m8ValidateQualificationCampaignV1(root string, campaign m8QualificationCampaignV1) (m8QualificationCampaignSummaryV1, error) {
	return m8ValidateQualificationCampaignWithVerifiersV1(root, campaign, m8QualificationRetainedVariantV1, m8QualificationBenchmarkExecutableV1, m8QualificationTrustedTruthCacheV1, validM8ProductionProfilesV1, m8QualificationRetainedAttributionV1)
}

// m8ValidateQualificationCampaignWithRetainedVariantV1 keeps the retained
// asset boundary explicit for focused evidence tests. Production callers use
// m8ValidateQualificationCampaignV1 above and cannot select a verifier.
func m8ValidateQualificationCampaignWithRetainedVariantV1(root string, campaign m8QualificationCampaignV1, retainedVariant m8QualificationRetainedVariantVerifierV1) (m8QualificationCampaignSummaryV1, error) {
	return m8ValidateQualificationCampaignWithVerifiersV1(root, campaign, retainedVariant, m8QualificationBenchmarkExecutableV1, m8QualificationTrustedTruthCacheV1, validM8ProductionProfilesV1, func(string, m8ProductionReportV1, [][]m8CanonicalResultV1, m8ProductionMeasurementTranscriptV1) error {
		return nil
	})
}

func m8ValidateQualificationCampaignWithVerifiersV1(root string, campaign m8QualificationCampaignV1, retainedVariant m8QualificationRetainedVariantVerifierV1, commandExecutable m8QualificationCommandExecutableVerifierV1, truthCache m8QualificationTruthCacheVerifierV1, profileVerifier m8ProductionProfileVerifierV1, retainedAttribution m8QualificationRetainedAttributionVerifierV1) (m8QualificationCampaignSummaryV1, error) {
	if retainedVariant == nil {
		return m8QualificationCampaignSummaryV1{}, errors.New("qualification retained-asset verifier is required")
	}
	if commandExecutable == nil {
		return m8QualificationCampaignSummaryV1{}, errors.New("qualification command executable verifier is required")
	}
	if truthCache == nil {
		return m8QualificationCampaignSummaryV1{}, errors.New("qualification trusted truth-cache verifier is required")
	}
	if profileVerifier == nil {
		return m8QualificationCampaignSummaryV1{}, errors.New("qualification profile verifier is required")
	}
	if retainedAttribution == nil {
		return m8QualificationCampaignSummaryV1{}, errors.New("qualification retained-attribution verifier is required")
	}
	if !m8QualificationSHA256V1(campaign.FixtureChecksum) || !m8QualificationGitSHAV1(campaign.BaseSHA) || !m8QualificationGitSHAV1(campaign.HeadSHA) || len(campaign.Runs) != 3 {
		return m8QualificationCampaignSummaryV1{}, errors.New("qualification campaign requires one fixture/head and exactly three runs")
	}
	if !m8QualificationFixtureChecksumV1(campaign.FixtureChecksum) {
		return m8QualificationCampaignSummaryV1{}, errors.New("qualification campaign fixture is not an authoritative corpus")
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return m8QualificationCampaignSummaryV1{}, fmt.Errorf("resolve qualification root: %w", err)
	}
	rootInfo, err := os.Stat(resolvedRoot)
	if err != nil || !rootInfo.IsDir() {
		return m8QualificationCampaignSummaryV1{}, errors.New("qualification root is not a directory")
	}
	var p2QPS, p16QPS []float64
	var p2P95, p16P95 []uint64
	var summary m8QualificationCampaignSummaryV1
	variantDescriptors := make(map[string]m3VariantDescriptorV1, len(m8RequiredVariantIDsV1))
	var topology *nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1
	truthArtifact := ""
	var dataset fixtureManifest
	executableSHA256 := ""
	var environment *m8QualificationEnvironmentV1
	configs := make(map[string]m8ProductionConfigEvidenceV1, len(m8RequiredVariantIDsV1))
	routerSessionIdentities := make(map[string]nativewire.VectorPartitionCoordinatorRouterSessionIdentityV1, len(m8RequiredVariantIDsV1))
	profileModes := make(map[string]m8QualificationProfileModeV1, len(m8RequiredVariantIDsV1))
	executionIDs := make(map[string]bool, len(campaign.Runs)*len(m8RequiredVariantIDsV1))
	profileSets := make(map[string]bool, len(campaign.Runs)*len(m8RequiredVariantIDsV1))
	transcripts := make(map[string]bool, len(campaign.Runs)*len(m8RequiredVariantIDsV1))
	transcriptPaths := make(map[string]bool, len(campaign.Runs)*len(m8RequiredVariantIDsV1))
	paths, digests := make(map[string]bool, len(campaign.Runs)), make(map[string]bool, len(campaign.Runs))
	for runIndex, run := range campaign.Runs {
		if run.Path == "" || filepath.IsAbs(run.Path) || !m8QualificationSHA256V1(run.SHA256) {
			return summary, errors.New("qualification campaign has malformed retained artifact identity")
		}
		cleanPath := filepath.Clean(run.Path)
		if cleanPath == "." || cleanPath == ".." || strings.HasPrefix(cleanPath, ".."+string(filepath.Separator)) || paths[cleanPath] || digests[run.SHA256] {
			return summary, errors.New("qualification campaign has duplicate or escaping retained artifact identity")
		}
		paths[cleanPath], digests[run.SHA256] = true, true
		resolvedPath, err := filepath.EvalSymlinks(filepath.Join(resolvedRoot, cleanPath))
		if err != nil {
			return summary, fmt.Errorf("resolve qualification artifact %s: %w", cleanPath, err)
		}
		rel, err := filepath.Rel(resolvedRoot, resolvedPath)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return summary, errors.New("qualification campaign artifact escapes root")
		}
		raw, err := readBoundedRegularFileV1(resolvedPath, m8QualificationMatrixMaxBytesV1)
		if err != nil {
			return summary, err
		}
		digest := sha256.Sum256(raw)
		if hex.EncodeToString(digest[:]) != run.SHA256 {
			return summary, fmt.Errorf("qualification campaign artifact digest mismatch: %s", run.Path)
		}
		var matrix m8ProductionMatrixV1
		if err := json.Unmarshal(raw, &matrix); err != nil {
			return summary, fmt.Errorf("decode qualification matrix %s: %w", run.Path, err)
		}
		if matrix.SchemaVersion != 5 || matrix.ResultKind != "m8_production_multi_variant_matrix_v5" || matrix.BaseSHA != campaign.BaseSHA || matrix.HeadSHA != campaign.HeadSHA || !m8QualificationSHA256V1(matrix.ExecutableSHA256) || !m8QualificationGitSHAV1(matrix.BaseSHA) || !m8QualificationGitSHAV1(matrix.HeadSHA) {
			return summary, fmt.Errorf("qualification matrix %s has invalid schema/provenance/status", cleanPath)
		}
		if err := validateM8ProductionMatrixV1(matrix); err != nil {
			return summary, fmt.Errorf("validate qualification matrix %s: %w", run.Path, err)
		}
		if run.PublicationCompletedAt.IsZero() || !run.PublicationCompletedAt.After(matrix.ExecutionCompletedAt) {
			return summary, fmt.Errorf("qualification matrix %s lacks a post-publication completion timestamp", cleanPath)
		}
		summary.intervals = append(summary.intervals, m8QualificationRunIntervalV1{Path: cleanPath, StartedAt: matrix.ExecutionStartedAt, EndedAt: run.PublicationCompletedAt})
		if matrix.Dataset.Checksum != campaign.FixtureChecksum || !m8QualificationFixtureV1(matrix.Dataset) || len(matrix.Variants) != len(m8RequiredVariantIDsV1) || !slices.Equal(matrix.RequiredVariants, m8RequiredVariantIDsV1) || matrix.OverlapStorageRatio >= 1.35 {
			return summary, fmt.Errorf("qualification matrix %s does not bind the required identity/storage", run.Path)
		}
		if dataset.Checksum != "" && dataset != matrix.Dataset {
			return summary, fmt.Errorf("qualification matrix %s changes dataset manifest", cleanPath)
		}
		dataset = matrix.Dataset
		var selected *m8ProductionReportV1
		seenVariants := make(map[string]bool, len(m8RequiredVariantIDsV1))
		for i := range matrix.Variants {
			report := &matrix.Variants[i]
			if err := validateM8ProductionReportWithProfilesV1(*report, m8QualificationResourceCapsV1(), profileVerifier); err != nil {
				return summary, fmt.Errorf("validate qualification child %s: %w", cleanPath, err)
			}
			truth, err := truthCache(resolvedRoot, *report)
			if err != nil {
				return summary, fmt.Errorf("qualification matrix %s does not bind the frozen truth cache: %w", cleanPath, err)
			}
			// The production verifier returns independently anchored truth. Test-only
			// verifier seams may return nil to keep synthetic campaign mutations cheap.
			var transcript m8ProductionMeasurementTranscriptV1
			if truth != nil {
				transcript, err = m8QualificationMeasurementTranscriptOutcomesV1(resolvedRoot, *report, truth)
				if err != nil {
					return summary, fmt.Errorf("qualification matrix %s has unbound query outcomes: %w", cleanPath, err)
				}
			} else {
				if !m8QualificationMeasurementTranscriptV1(resolvedRoot, *report) {
					return summary, fmt.Errorf("qualification matrix %s has unbound measurement transcript", cleanPath)
				}
				transcript, err = m8ReadProductionMeasurementTranscriptV1(*report)
				if err != nil {
					return summary, fmt.Errorf("qualification matrix %s has unreadable measurement transcript: %w", cleanPath, err)
				}
			}
			if !m8QualificationCommandWithExecutableV1(resolvedRoot, filepath.Dir(resolvedPath), *report, commandExecutable) || report.ExecutableSHA256 != matrix.ExecutableSHA256 {
				return summary, fmt.Errorf("qualification matrix %s has command/config mismatch", cleanPath)
			}
			if executableSHA256 != "" && executableSHA256 != report.ExecutableSHA256 {
				return summary, fmt.Errorf("qualification matrix %s changes benchmark executable", cleanPath)
			}
			executableSHA256 = report.ExecutableSHA256
			derivedLedger := m8ProductionGateLedgerForReportV1(*report)
			derivedLedger.OverlapStorage = report.GateLedger.OverlapStorage
			if report.GateLedger != derivedLedger {
				return summary, fmt.Errorf("qualification matrix %s has stale child gate ledger", cleanPath)
			}
			if runIndex == 0 && !m8QualificationHasFullLadderV1(*report) {
				return summary, fmt.Errorf("qualification matrix %s omits the required p1/2/4/8/16 ladder", cleanPath)
			}
			if report.Variant != nil && (report.Variant.BaseSHA != campaign.BaseSHA || report.Variant.HeadSHA != campaign.HeadSHA) {
				return summary, fmt.Errorf("qualification matrix %s retained M3 revision does not match campaign", cleanPath)
			}
			if report.Variant != nil && report.Variant.ExecutableSHA256 != report.ExecutableSHA256 {
				return summary, fmt.Errorf("qualification matrix %s retained M3 executable does not match M8 evidence", cleanPath)
			}
			if report.Variant != nil && !m8QualificationM3BuildCapsV1(*report.Variant, report.Dataset) {
				return summary, fmt.Errorf("qualification matrix %s has off-plan M3 construction caps/router configuration", run.Path)
			}
			if report.BaseSHA != campaign.BaseSHA || report.HeadSHA != campaign.HeadSHA || report.Dataset != matrix.Dataset || report.Dirty || !m8QualificationSHA256V1(report.TruthCache.ArtifactSHA256) || report.Variant == nil || report.Variant.BuildDirty || seenVariants[report.Variant.VariantID] || !slices.Contains(m8RequiredVariantIDsV1, report.Variant.VariantID) || !m8QualificationSHA256V1(report.Variant.ArtifactSHA256) || !m8QualificationConfigV1(report.Config, report.Dataset, report.Variant.OverlapRatio, runIndex) || !m8QualificationVariantBackendV1(*report.Variant, report.Dataset) {
				return summary, fmt.Errorf("qualification matrix %s has unbound child identity", run.Path)
			}
			if err := retainedVariant(resolvedRoot, *report); err != nil {
				return summary, fmt.Errorf("qualification matrix %s has unavailable or mismatched retained M3 assets: %w", cleanPath, err)
			}
			if err := retainedAttribution(resolvedRoot, *report, truth, transcript); err != nil {
				return summary, fmt.Errorf("qualification matrix %s has unbound retained attribution: %w", cleanPath, err)
			}
			if executionIDs[report.ExecutionID] {
				return summary, fmt.Errorf("qualification matrix %s reuses execution identity", cleanPath)
			}
			executionIDs[report.ExecutionID] = true
			if !m8QualificationResourcesV1(*report, report.Dataset, transcript) {
				return summary, fmt.Errorf("qualification matrix %s has unbound environment or resources", run.Path)
			}
			if !m8QualificationMeasurementTranscriptV1(resolvedRoot, *report) || transcripts[report.MeasurementTranscript.SHA256] || transcriptPaths[report.MeasurementTranscript.Path] {
				return summary, fmt.Errorf("qualification matrix %s has unbound or reused measurement transcript", cleanPath)
			}
			transcripts[report.MeasurementTranscript.SHA256] = true
			transcriptPaths[report.MeasurementTranscript.Path] = true
			profileMode, ok := m8QualificationProfilesV1(resolvedRoot, report.Profiles)
			if !ok || report.Profiles.Status != "captured_production_query_and_fault_boundary" {
				return summary, fmt.Errorf("qualification matrix %s has unbound profile capture", cleanPath)
			}
			profileSet, err := m8ProductionProfileSetDigestV1(report.Profiles.Artifacts)
			if err != nil || profileSets[profileSet] {
				return summary, fmt.Errorf("qualification matrix %s reuses profile artifact set", cleanPath)
			}
			profileSets[profileSet] = true
			if prior, ok := profileModes[report.Variant.VariantID]; ok && prior != profileMode {
				return summary, fmt.Errorf("qualification matrix %s changes profile capture mode", cleanPath)
			}
			profileModes[report.Variant.VariantID] = profileMode
			currentEnvironment := m8QualificationEnvironmentV1{GoVersion: report.GoVersion, GOOS: report.GOOS, GOARCH: report.GOARCH, LogicalCPUs: report.LogicalCPUs, GOMAXPROCS: report.GOMAXPROCS, GoMemoryLimitBytes: report.GoMemoryLimitBytes, Host: report.Host, PeakRSSCapBytes: report.Resources.PeakRSSCapBytes, PersistentAssetCap: report.Resources.PersistentAssetCap}
			if environment != nil && *environment != currentEnvironment {
				return summary, fmt.Errorf("qualification matrix %s changes environment or resources", cleanPath)
			}
			environment = &currentEnvironment
			seenVariants[report.Variant.VariantID] = true
			config := report.Config
			config.Probes = nil // all repeats record the same p1/2/4/8/16 ladder.
			if prior, ok := configs[report.Variant.VariantID]; ok && !reflect.DeepEqual(prior, config) {
				return summary, fmt.Errorf("qualification matrix %s changes %s topology/configuration", cleanPath, report.Variant.VariantID)
			}
			configs[report.Variant.VariantID] = config
			routerSessionIdentity, ok := m8CanonicalRouterSessionIdentityV1(report.RouterSessions)
			if !ok {
				return summary, fmt.Errorf("qualification matrix %s has inconsistent router-session identity", cleanPath)
			}
			if prior, ok := routerSessionIdentities[report.Variant.VariantID]; ok && !reflect.DeepEqual(prior, routerSessionIdentity) {
				return summary, fmt.Errorf("qualification matrix %s changes router-session identity", cleanPath)
			}
			routerSessionIdentities[report.Variant.VariantID] = routerSessionIdentity
			if prior, ok := variantDescriptors[report.Variant.VariantID]; ok && !reflect.DeepEqual(prior, *report.Variant) {
				return summary, fmt.Errorf("qualification matrix %s changes retained M3 descriptor", run.Path)
			}
			variantDescriptors[report.Variant.VariantID] = *report.Variant
			currentTopology := m8QualificationImmutableTopologyV1(report.Topology)
			if topology != nil && !reflect.DeepEqual(*topology, currentTopology) {
				return summary, fmt.Errorf("qualification matrix %s changes retained topology", cleanPath)
			}
			topology = &currentTopology
			if truthArtifact != "" && truthArtifact != report.TruthCache.ArtifactSHA256 {
				return summary, fmt.Errorf("qualification matrix %s changes truth identity", run.Path)
			}
			truthArtifact = report.TruthCache.ArtifactSHA256
			if report.Variant.VariantID == "graph-overlap-020-v1" {
				selected = report
			}
		}
		if !m8QualificationMatrixCommandWithExecutableV1(resolvedRoot, filepath.Dir(resolvedPath), matrix, commandExecutable) {
			return summary, fmt.Errorf("qualification matrix %s has command/config mismatch", cleanPath)
		}
		if err := m8ValidateQualificationMatrixDerivationV1(matrix); err != nil {
			return summary, fmt.Errorf("derive qualification matrix %s: %w", cleanPath, err)
		}
		if len(seenVariants) != len(m8RequiredVariantIDsV1) || selected == nil {
			return summary, fmt.Errorf("qualification matrix %s has no graph-overlap candidate", run.Path)
		}
		p2, p16 := m8QualificationRowsV1(*selected)
		if p2 == nil || p16 == nil || p2.RecallAtK < .90 || p16.RecallAtK < .90 || p2.Attribution.FinalMembershipOracleRecallAtK < .90 || math.Abs(p2.Attribution.ExactToApproximateLossAtK) > .01 || p2.QPS < p16.QPS*1.15 || p2.P95Nanos > p16.P95Nanos {
			return summary, fmt.Errorf("qualification matrix %s misses the selected p2/p16 gate", run.Path)
		}
		if matrix.Status != "local_gate_pass" || matrix.Gates.RequiredVariants != "pass" || matrix.Gates.ExhaustiveParity != "pass" || matrix.Gates.FailureHonesty != "pass" || matrix.Gates.PartitionPackReachability != "pass" || matrix.Gates.Balance != "pass" || matrix.Gates.ResourceBounds != "pass" || matrix.Gates.OverlapStorage != "pass" {
			return summary, fmt.Errorf("qualification matrix %s does not bind the required gates", run.Path)
		}
		p2QPS, p16QPS = append(p2QPS, p2.QPS), append(p16QPS, p16.QPS)
		p2P95, p16P95 = append(p2P95, p2.P95Nanos), append(p16P95, p16.P95Nanos)
	}
	summary.ExecutableSHA256 = executableSHA256
	if len(executionIDs) != len(campaign.Runs)*len(m8RequiredVariantIDsV1) {
		return summary, errors.New("qualification campaign requires distinct execution identities")
	}
	minMedianMax := func(values []float64) (float64, float64, float64) {
		sort.Float64s(values)
		return values[0], values[len(values)/2], values[len(values)-1]
	}
	summary.P2QPSMin, summary.P2QPSMedian, summary.P2QPSMax = minMedianMax(p2QPS)
	summary.P16QPSMin, summary.P16QPSMedian, summary.P16QPSMax = minMedianMax(p16QPS)
	minMedianMaxU64 := func(values []uint64) (uint64, uint64, uint64) {
		slices.Sort(values)
		return values[0], values[len(values)/2], values[len(values)-1]
	}
	summary.P2P95Min, summary.P2P95Median, summary.P2P95Max = minMedianMaxU64(p2P95)
	summary.P16P95Min, summary.P16P95Median, summary.P16P95Max = minMedianMaxU64(p16P95)
	return summary, nil
}

var m8QualificationFixturesV1 = [...]fixtureManifest{
	{SchemaVersion: 1, Fixture: "deterministic_100000", Generator: "treedb_vector_partition_embedding_mixture_v1", Arithmetic: "ieee754_binary64_explicit_fma_v1", Vectors: 100000, Queries: 1000, Dimensions: 128, Metric: "cosine", Seed: 4017, Checksum: "ecc2224f386932e580e4956f2cfa852140d3134625971c3511bc0d5feddf9b95"},
	{SchemaVersion: 1, Fixture: "qualification_embedding_mixture_250000", Generator: "treedb_vector_partition_embedding_mixture_v1", Arithmetic: "ieee754_binary64_explicit_fma_v1", Vectors: 250000, Queries: 1000, Dimensions: 128, Metric: "cosine", Seed: 4016, Checksum: "d0c7c82ba868853aae9a4280161003d72714ad1701d41ed3169c2fa94d470d69"},
}

type m8QualificationTruthAnchorV1 struct {
	Identity, ArtifactSHA256, TruthSHA256 string
}

func m8QualificationTruthCacheAnchorV1(fixture fixtureManifest) (m8QualificationTruthAnchorV1, bool) {
	if !m8QualificationFixtureV1(fixture) {
		return m8QualificationTruthAnchorV1{}, false
	}
	switch fixture.Checksum {
	case "ecc2224f386932e580e4956f2cfa852140d3134625971c3511bc0d5feddf9b95":
		return m8QualificationTruthAnchorV1{Identity: "accdb76c693e2da99333b9327efc0e3d83ba630b25a8ba2b6820f5a6f6e38937", ArtifactSHA256: "0e9bce9465c9e1fa70c7833364e88c332bc831cfc52c628c90085e1c3068763c", TruthSHA256: "6e17b00a04ad86ad4b13507e6afc1ae38d323280b3a0aa8405bce88b222fa1bc"}, true
	case "d0c7c82ba868853aae9a4280161003d72714ad1701d41ed3169c2fa94d470d69":
		return m8QualificationTruthAnchorV1{Identity: "f1fab20b88cd3dcdd6e95a284400983230b1432b36bd4d73e321e251159795ab", ArtifactSHA256: "5a518c1cb8182edc685ab692dc17a6974655572f426a4b97c10482fd1643f04e", TruthSHA256: "89b84125e518f33cc30bc1e4e9defcc0639378d7108fb180f56ec2dc91d6f254"}, true
	default:
		return m8QualificationTruthAnchorV1{}, false
	}
}

func m8QualificationFixtureChecksumV1(checksum string) bool {
	for _, fixture := range m8QualificationFixturesV1 {
		if checksum == fixture.Checksum {
			return true
		}
	}
	return false
}

func m8QualificationFixtureV1(candidate fixtureManifest) bool {
	for _, fixture := range m8QualificationFixturesV1 {
		if candidate == fixture {
			return true
		}
	}
	return false
}

func m8QualificationConfigV1(cfg m8ProductionConfigEvidenceV1, fixture fixtureManifest, overlap float64, _ int) bool {
	return cfg.RaftGroups == 4 && cfg.RaftNodesPerGroup == 3 && cfg.Partitions == 16 && cfg.TopK == 10 && cfg.RecallTarget == .90 && cfg.Warmup == 0 && cfg.EffectiveWarmup == 0 && cfg.RouterCandidates == m8QualificationRouterCandidatesV1 && cfg.MaxExactTruthVisits == m8QualificationExactTruthCapV1(fixture) && cfg.Seed == fixture.Seed && slices.Equal(cfg.Probes, []int{1, 2, 4, 8, 16}) && slices.Equal(cfg.Concurrency, []int{1}) && slices.Equal(cfg.EfSearch, []int{128}) && slices.Equal(cfg.Overlap, []float64{overlap})
}

func m8QualificationTrustedTruthCacheV1(root string, report m8ProductionReportV1) ([][]m8CanonicalResultV1, error) {
	anchor, ok := m8QualificationTruthCacheAnchorV1(report.Dataset)
	if !ok {
		return nil, errors.New("truth cache has no frozen corpus anchor")
	}
	return m8QualificationReadTruthCacheWithAnchorV1(root, report, anchor)
}

func m8QualificationTruthCacheWithAnchorV1(root string, report m8ProductionReportV1, anchor m8QualificationTruthAnchorV1) error {
	_, err := m8QualificationReadTruthCacheWithAnchorV1(root, report, anchor)
	return err
}

func m8QualificationReadTruthCacheWithAnchorV1(root string, report m8ProductionReportV1, anchor m8QualificationTruthAnchorV1) ([][]m8CanonicalResultV1, error) {
	if report.Variant == nil {
		return nil, errors.New("truth cache has no retained variant")
	}
	dir, err := m8QualificationContainedPathV1(root, report.TruthCacheDirectory, "canonical truth cache")
	if err != nil {
		return nil, err
	}
	if dir != report.TruthCacheDirectory || report.TruthCache.Identity != anchor.Identity || report.TruthCache.ArtifactSHA256 != anchor.ArtifactSHA256 {
		return nil, errors.New("truth cache evidence differs from the frozen corpus anchor")
	}
	path := m8TruthCacheArtifactPathV1(dir, anchor.Identity)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("truth cache anchor artifact is not a regular file")
	}
	truth, artifactSHA256, err := m8ReadTruthCacheV1(path, report.Dataset, report.Dataset.Queries, report.Config.TopK, report.Variant.SourceRows, anchor.ArtifactSHA256)
	if err != nil {
		return nil, fmt.Errorf("decode frozen truth cache: %w", err)
	}
	truthSHA256, err := m8TruthContentSHA256V1(truth)
	if err != nil || artifactSHA256 != anchor.ArtifactSHA256 || truthSHA256 != anchor.TruthSHA256 {
		return nil, errors.New("truth cache artifact or semantic digest differs from the frozen corpus anchor")
	}
	return truth, nil
}

func m8QualificationRetainedVariantV1(root string, report m8ProductionReportV1) (err error) {
	if report.Variant == nil {
		return errors.New("missing retained M3 descriptor")
	}
	root, err = m8CanonicalPathV1(root)
	if err != nil {
		return err
	}
	rootInfo, err := os.Stat(root)
	if err != nil || !rootInfo.IsDir() {
		return errors.New("qualification root is not a directory")
	}
	datasetDirectory, err := m8QualificationContainedPathV1(root, report.DatasetDirectory, "qualification dataset")
	if err != nil {
		return err
	}
	if datasetDirectory != report.DatasetDirectory {
		return errors.New("qualification dataset directory is not canonical")
	}
	manifestInfo, err := os.Lstat(filepath.Join(datasetDirectory, "fixture_manifest.json"))
	if err != nil || !manifestInfo.Mode().IsRegular() {
		return errors.New("qualification dataset manifest is not a regular file")
	}
	dataset, err := loadFixture(datasetDirectory)
	if err != nil {
		return fmt.Errorf("load qualification dataset manifest: %w", err)
	}
	if dataset != report.Dataset {
		return errors.New("qualification dataset manifest does not match report")
	}
	truthCacheDirectory, err := m8QualificationContainedPathV1(root, report.TruthCacheDirectory, "canonical truth cache")
	if err != nil {
		return err
	}
	if truthCacheDirectory != report.TruthCacheDirectory {
		return errors.New("canonical truth-cache directory is not canonical")
	}
	truthPath := m8TruthCacheArtifactPathV1(truthCacheDirectory, report.TruthCache.Identity)
	truthInfo, err := os.Lstat(truthPath)
	if err != nil || !truthInfo.Mode().IsRegular() {
		return errors.New("canonical truth-cache artifact is not a regular file")
	}
	if _, _, err := m8ReadTruthCacheV1(truthPath, report.Dataset, report.Dataset.Queries, report.Config.TopK, report.Variant.SourceRows, report.TruthCache.ArtifactSHA256); err != nil {
		return fmt.Errorf("validate canonical truth-cache artifact: %w", err)
	}
	dir, err := m8QualificationContainedPathV1(root, report.Variant.DatabaseDirectory, "retained M3 database")
	if err != nil {
		return err
	}
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return errors.New("retained M3 database is not a directory")
	}
	if err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("retained M3 database contains symlink: %s", path)
		}
		if !entry.IsDir() && !entry.Type().IsRegular() {
			return fmt.Errorf("retained M3 database contains non-regular entry: %s", path)
		}
		return nil
	}); err != nil {
		return err
	}
	assets, err := openM8ProductionExistingAssetSetV1(dir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, assets.Close()) }()
	if err := m8ValidateExistingAssetsFixtureV1(assets.collection, assets.status.Manifest, report.Dataset, fixtureVectors(report.Dataset)); err != nil {
		return fmt.Errorf("verify retained M3 source rows: %w", err)
	}
	if err := m8BindRetainedM3DescriptorV1(assets, report.Dataset); err != nil {
		return err
	}
	actual := *assets.descriptor
	actualDir, err := m8CanonicalPathV1(actual.DatabaseDirectory)
	if err != nil || actualDir != dir {
		return errors.New("retained M3 descriptor has a different database directory")
	}
	want := *report.Variant
	want.DatabaseDirectory, actual.DatabaseDirectory = dir, actualDir
	if !reflect.DeepEqual(actual, want) {
		return errors.New("retained M3 descriptor does not match report variant")
	}
	return nil
}

// m8QualificationRetainedAttributionV1 independently replays the deterministic
// offline attribution stages from the retained M3 assets and anchored truth.
// Coordinator-only fields remain bound by the retained query outcomes.
func m8QualificationRetainedAttributionV1(root string, report m8ProductionReportV1, truth [][]m8CanonicalResultV1, transcript m8ProductionMeasurementTranscriptV1) (err error) {
	if report.Variant == nil || len(truth) != report.Dataset.Queries {
		return errors.New("missing retained attribution identity")
	}
	if len(transcript.Outcomes) != len(report.Rows) {
		return errors.New("missing retained coordinator outcomes")
	}
	dir, err := m8QualificationContainedPathV1(root, report.Variant.DatabaseDirectory, "retained M3 database")
	if err != nil {
		return err
	}
	assets, err := openM8ProductionExistingAssetSetV1(dir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, assets.Close()) }()
	primaryHomes, finalMemberships, err := m8TruthPartitionMembershipsByDocumentIDV1(assets, truth)
	if err != nil {
		return err
	}
	harness, err := newM8AttributionHarnessV1(assets)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, harness.Close()) }()
	_, queries := fixtureData(report.Dataset)
	if len(queries) != len(truth) {
		return errors.New("retained attribution query shape mismatch")
	}
	approximateCandidates := min(report.Config.RouterCandidates, int(assets.status.Representatives))
	if approximateCandidates < 1 {
		return errors.New("retained attribution has no router candidates")
	}
	exhaustive := make([][]m8CanonicalResultV1, len(queries))
	for rowIndex, row := range report.Rows {
		if row.Status != "pass" && row.Status != "fail" {
			continue
		}
		membershipOracles, err := m8MembershipOracleRecallCacheV1(truth, primaryHomes, finalMemberships, len(harness.searchers), row.Probes)
		if err != nil {
			return err
		}
		cell, err := m8BuildAttributionV1(context.Background(), assets, primaryHomes, finalMemberships, queries, truth, membershipOracles, row.Probes, row.EfSearch, report.Config.TopK, approximateCandidates, exhaustive, harness)
		if err != nil {
			return err
		}
		idParity, scoreParity := true, true
		for query, local := range cell.Local {
			outcome := transcript.Outcomes[rowIndex]
			if query >= len(outcome.TopKIDs) || query >= len(outcome.TopKScoreBits) || len(local) != len(outcome.TopKIDs[query]) || len(local) != len(outcome.TopKScoreBits[query]) {
				return errors.New("retained coordinator outcomes have invalid shape")
			}
			for result := range local {
				if local[result].ID != outcome.TopKIDs[query][result] {
					idParity = false
				}
				if math.Float32bits(local[result].Score) != outcome.TopKScoreBits[query][result] {
					scoreParity = false
				}
			}
		}
		want := cell.Evidence
		want.EndToEndRecallAtK = row.Attribution.EndToEndRecallAtK
		want.CoordinatorMergeIDParity = idParity
		want.CoordinatorMergeScoreParity = scoreParity
		want.ApproximateLocalToEndToEndLossAtK = want.ApproximateLocalHNSWRecallAtK - want.EndToEndRecallAtK
		want.ResidualLossOwners = m8AttributionLossOwnersV1(want)
		want.StageOwners = m8AttributionStageOwnersV1(want)
		if !reflect.DeepEqual(want, row.Attribution) {
			return errors.New("retained attribution does not reproduce report")
		}
	}
	return nil
}

func m8QualificationContainedPathV1(root, path, label string) (string, error) {
	resolved, err := m8CanonicalPathV1(path)
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", label, err)
	}
	rel, err := filepath.Rel(root, resolved)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%s is outside qualification root", label)
	}
	return resolved, nil
}

func m8QualificationCommandWithExecutableV1(root, matrixDirectory string, report m8ProductionReportV1, commandExecutable m8QualificationCommandExecutableVerifierV1) bool {
	if len(report.Command) < 2 {
		return false
	}
	if !m8QualificationCommandExecutableV1(root, report.Command[0], report.HeadSHA, report.ExecutableSHA256, commandExecutable) {
		return false
	}
	cfg, err := parseConfig(report.Command[1:])
	if err != nil || cfg.stage != m8ProductionMultiGroupModeV1 || cfg.baseSHA != report.BaseSHA || cfg.headSHA != report.HeadSHA ||
		!m8QualificationExactFlagV1(report.Command[1:], "-base-sha", report.BaseSHA) || !m8QualificationExactFlagV1(report.Command[1:], "-head-sha", report.HeadSHA) ||
		!m8QualificationSourceCheckoutV1(root, report.Command[1:], cfg) {
		return false
	}
	out, err := m8CanonicalPathV1(cfg.out)
	if err != nil || out != matrixDirectory {
		return false
	}
	matrixOut, err := m8CanonicalPathV1(cfg.m8MatrixOut)
	if err != nil || matrixOut != matrixDirectory {
		return false
	}
	datasetDirectory, err := m8CanonicalPathV1(cfg.dataset)
	if err != nil || report.DatasetDirectory == "" || datasetDirectory != report.DatasetDirectory {
		return false
	}
	truthCacheDirectory, err := m8CanonicalPathV1(cfg.m8TruthCache)
	if err != nil || report.TruthCacheDirectory == "" || truthCacheDirectory != report.TruthCacheDirectory {
		return false
	}
	if report.Variant == nil || cfg.m8ExistingDB == "" {
		return false
	}
	existingDB, err := m8CanonicalPathV1(cfg.m8ExistingDB)
	if err != nil {
		return false
	}
	variantDB, err := m8CanonicalPathV1(report.Variant.DatabaseDirectory)
	if err != nil || existingDB != variantDB {
		return false
	}
	profiles, err := m8CanonicalPathV1(cfg.profiles)
	if err != nil || profiles != report.Profiles.Directory {
		return false
	}
	matrixProfiles, err := m8CanonicalPathV1(cfg.m8MatrixProfiles)
	if err != nil || matrixProfiles != filepath.Dir(report.Profiles.Directory) {
		return false
	}
	switch report.TruthCache.Status {
	case "computed", "reused":
		if cfg.m8TruthCacheSHA256 != report.TruthCache.ArtifactSHA256 {
			return false
		}
	default:
		return false
	}
	return reflect.DeepEqual(m8QualificationCommandConfigV1(cfg), report.Config) &&
		cfg.m8MaxRSSBytes == report.Resources.PeakRSSCapBytes &&
		cfg.m8MaxAssetBytes == report.Resources.PersistentAssetCap &&
		m8QualificationCommandAdmissionV1(report.Command[1:], cfg, report.Dataset)
}

func m8QualificationCommandConfigV1(cfg config) m8ProductionConfigEvidenceV1 {
	warmup, _ := m8WarmupCountAndConcurrencyV1(cfg)
	return m8ProductionConfigEvidenceV1{
		RaftGroups: cfg.raftGroups, RaftNodesPerGroup: cfg.raftNodes, Partitions: cfg.partitions,
		Probes: cfg.probes, Overlap: cfg.overlaps, TopK: cfg.topK, RecallTarget: cfg.recallTarget,
		Concurrency: cfg.concurrency, Warmup: cfg.warmup, EffectiveWarmup: warmup,
		EfSearch: cfg.efSearch, RouterCandidates: cfg.routerCandidates,
		MaxExactTruthVisits: cfg.m8MaxExactTruthVisits, Seed: cfg.seed,
	}
}

func m8QualificationMatrixCommandWithExecutableV1(root, matrixDirectory string, matrix m8ProductionMatrixV1, commandExecutable m8QualificationCommandExecutableVerifierV1) bool {
	if len(matrix.Command) < 2 || len(matrix.Variants) != len(m8RequiredVariantIDsV1) {
		return false
	}
	if !m8QualificationCommandExecutableV1(root, matrix.Command[0], matrix.HeadSHA, matrix.ExecutableSHA256, commandExecutable) {
		return false
	}
	cfg, err := parseConfig(matrix.Command[1:])
	if err != nil || cfg.stage != m8ProductionMultiGroupModeV1 || len(cfg.m8VariantDBs) != len(m8RequiredVariantIDsV1) || cfg.baseSHA != matrix.BaseSHA || cfg.headSHA != matrix.HeadSHA ||
		!m8QualificationExactFlagV1(matrix.Command[1:], "-base-sha", matrix.BaseSHA) || !m8QualificationExactFlagV1(matrix.Command[1:], "-head-sha", matrix.HeadSHA) ||
		!m8QualificationSourceCheckoutV1(root, matrix.Command[1:], cfg) {
		return false
	}
	out, err := m8CanonicalPathV1(cfg.out)
	if err != nil || out != matrixDirectory {
		return false
	}
	byID := make(map[string]*m8ProductionReportV1, len(matrix.Variants))
	for i := range matrix.Variants {
		report := &matrix.Variants[i]
		if report.Variant == nil || byID[report.Variant.VariantID] != nil {
			return false
		}
		byID[report.Variant.VariantID] = report
	}
	base := byID[m8RequiredVariantIDsV1[0]]
	if base == nil || base.DatasetDirectory == "" {
		return false
	}
	dataset, err := m8CanonicalPathV1(cfg.dataset)
	if err != nil || dataset != base.DatasetDirectory {
		return false
	}
	truthCacheDirectory, err := m8CanonicalPathV1(cfg.m8TruthCache)
	if err != nil || truthCacheDirectory == "" || truthCacheDirectory != base.TruthCacheDirectory {
		return false
	}
	commandConfig := m8QualificationCommandConfigV1(cfg)
	commandConfig.Overlap = nil
	profileRoot := ""
	variantDBs := make(map[string]bool, len(cfg.m8VariantDBs))
	for _, dir := range cfg.m8VariantDBs {
		canonical, err := m8CanonicalPathV1(dir)
		if err != nil || variantDBs[canonical] {
			return false
		}
		variantDBs[canonical] = true
	}
	for _, variantID := range m8RequiredVariantIDsV1 {
		report := byID[variantID]
		if report == nil || report.Profiles.Status == "not_captured" || report.DatasetDirectory != dataset || report.TruthCacheDirectory != truthCacheDirectory {
			return false
		}
		expectedConfig := report.Config
		expectedConfig.Overlap = nil
		if !reflect.DeepEqual(commandConfig, expectedConfig) || cfg.m8MaxRSSBytes != report.Resources.PeakRSSCapBytes || cfg.m8MaxAssetBytes != report.Resources.PersistentAssetCap {
			return false
		}
		dir, err := m8CanonicalPathV1(report.Variant.DatabaseDirectory)
		if err != nil || !variantDBs[dir] {
			return false
		}
		root, err := m8CanonicalPathV1(filepath.Dir(report.Profiles.Directory))
		if err != nil || (profileRoot != "" && root != profileRoot) {
			return false
		}
		profileRoot = root
		if cfg.m8TruthCacheSHA256 != report.TruthCache.ArtifactSHA256 {
			return false
		}
	}
	profiles, err := m8CanonicalPathV1(cfg.profiles)
	return err == nil && profiles == profileRoot &&
		m8QualificationCommandAdmissionV1(matrix.Command[1:], cfg, base.Dataset)
}

func m8QualificationSourceCheckoutV1(root string, args []string, cfg config) bool {
	want, err := m8QualificationContainedPathV1(root, filepath.Join(root, "source"), "source checkout")
	if err != nil {
		return false
	}
	got, err := m8CanonicalPathV1(cfg.sourceCheckout)
	if err != nil || got != want || !m8QualificationExactFlagV1(args, "-source-checkout", want) {
		return false
	}
	checkout, err := m8SourceCheckoutV1(want, cfg.headSHA)
	return err == nil && checkout == want && !m8GitDirtyInV1(want)
}

// m8QualificationCommandAdmissionV1 binds retained argv to the same fixture
// and bounded-work gates that production applies before measurement.  The
// qualification plan deliberately chooses the exact fixture vector count and
// the runner's fixture-byte ceiling for each corpus, so defaults and looser
// replay limits cannot silently qualify.
func m8QualificationCommandAdmissionV1(args []string, cfg config, fixture fixtureManifest) bool {
	expected, ok := m8QualificationAdmissionConfigV1(fixture)
	if !ok || cfg.maxVectors != expected.maxVectors || cfg.maxBytes != expected.maxBytes ||
		m8QualificationExactFlagV1(args, "-max-vectors", strconv.Itoa(expected.maxVectors)) == false ||
		m8QualificationExactFlagV1(args, "-max-fixture-bytes", strconv.FormatInt(expected.maxBytes, 10)) == false {
		return false
	}
	if err := validateM3FixtureWithCaps(fixture, cfg.maxVectors, cfg.maxBytes); err != nil {
		return false
	}
	_, err := validateM8BenchmarkWork(cfg, fixture, maxBenchmarkWorkUnits, cfg.maxBytes)
	return err == nil
}

func m8QualificationAdmissionConfigV1(fixture fixtureManifest) (config, bool) {
	cfg, err := parseConfig([]string{
		"-stage", "overlap,partition_index", "-dataset", ".", "-out", ".", "-partitions", "16", "-probes", "1",
		"-max-vectors", strconv.Itoa(fixture.Vectors), "-max-fixture-bytes", strconv.FormatInt(maxFixtureBytes, 10),
	})
	return cfg, err == nil
}

func m8QualificationExactFlagV1(args []string, name, want string) bool {
	found := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg != name && arg != "-"+name {
			value := ""
			switch {
			case strings.HasPrefix(arg, name+"="):
				value = strings.TrimPrefix(arg, name+"=")
			case strings.HasPrefix(arg, "-"+name+"="):
				value = strings.TrimPrefix(arg, "-"+name+"=")
			default:
				continue
			}
			if found || value != want {
				return false
			}
			found = true
			continue
		}
		if found || i+1 == len(args) || args[i+1] != want {
			return false
		}
		found = true
		i++
	}
	return found
}

func m8QualificationCommandExecutableV1(root, command, headSHA, executableSHA256 string, verify m8QualificationCommandExecutableVerifierV1) bool {
	if verify == nil || command == "" || !filepath.IsAbs(command) || !m8QualificationSHA256V1(executableSHA256) {
		return false
	}
	canonical, err := m8CanonicalPathV1(command)
	if err != nil || canonical != command {
		return false
	}
	return verify(root, canonical, headSHA, executableSHA256)
}

func m8QualificationBenchmarkExecutableV1(root, command, headSHA, executableSHA256 string) bool {
	if _, err := m8QualificationContainedPathV1(root, command, "benchmark executable"); err != nil {
		return false
	}
	digest, err := m8BenchmarkExecutableSHA256V1(command)
	if err != nil || digest != executableSHA256 {
		return false
	}
	build, err := buildinfo.ReadFile(command)
	if err != nil || build.Path != "github.com/snissn/gomap/cmd/treedb_vector_partition_bench" {
		return false
	}
	settings := make(map[string]string, len(build.Settings))
	for _, setting := range build.Settings {
		settings[setting.Key] = setting.Value
	}
	return settings["vcs.revision"] == headSHA && settings["vcs.modified"] == "false"
}

func m8QualificationExactTruthCapV1(fixture fixtureManifest) int64 {
	if fixture.Vectors == 250000 {
		return 1_500_000_000
	}
	return 600_000_000
}

func m8QualificationM3BuildConfigV1(fixture fixtureManifest) (vectorpartition.Config, vectorpartition.RouterConfigV1, int64, bool) {
	cap, visits := int64(20_000_000_000), int64(400_000_000)
	if fixture.Vectors == 250000 {
		cap, visits = 50_000_000_000, 900_000_000
	}
	routerMaxVectors, ok := m8QualificationRouterMaxVectorsV1(fixture.Vectors)
	if !ok {
		return vectorpartition.Config{}, vectorpartition.RouterConfigV1{}, 0, false
	}
	cfg, err := parseConfig([]string{
		"-stage", "overlap,partition_index", "-dataset", ".", "-out", ".", "-probes", "1", "-partitions", "16", "-seed", strconv.FormatInt(fixture.Seed, 10),
		"-partition-max-distance-work", strconv.FormatInt(cap, 10), "-router-max-vectors", strconv.Itoa(routerMaxVectors), "-router-max-scalar-work", strconv.FormatInt(cap, 10),
		"-m3-max-benchmark-visits", strconv.FormatInt(visits, 10),
		"-max-vectors", strconv.Itoa(fixture.Vectors), "-max-fixture-bytes", strconv.FormatInt(maxFixtureBytes, 10),
	})
	if err != nil {
		return vectorpartition.Config{}, vectorpartition.RouterConfigV1{}, 0, false
	}
	return cfg.partition, cfg.routerConfig, cfg.m3MaxBenchmarkVisits, true
}

func m8QualificationRouterMaxVectorsV1(source int) (int, bool) {
	if source < 1 {
		return 0, false
	}
	overlap := source / 5
	if source%5 != 0 {
		overlap++
	}
	if source > int(^uint(0)>>1)-overlap {
		return 0, false
	}
	return source + overlap, true
}

func m8QualificationM3BuildCapsV1(variant m3VariantDescriptorV1, fixture fixtureManifest) bool {
	partitionConfig, routerConfig, visits, ok := m8QualificationM3BuildConfigV1(fixture)
	if !ok || variant.PartitionMaxDistanceWork != partitionConfig.MaxDistanceWork || variant.RouterMaxScalarWork != routerConfig.MaxScalarWork || variant.M3MaxBenchmarkVisits != visits || variant.PartitionConfig != partitionConfig || variant.RouterConfig != routerConfig {
		return false
	}
	switch variant.PartitionHNSWM {
	case partitionConfig.Degree:
	case 18:
	default:
		return false
	}
	definition := partitionCollectionMetaWithDegree(m3BenchmarkCollection, fixture.Dimensions, variant.PartitionHNSWM).VectorIndexes[0]
	if variant.PartitionHNSWM == 18 {
		definition.EfConstruction = 256
	}
	return variant.IndexDefinitionDigest == collections.VectorIndexDefinitionDigestV1(definition)
}

func m8QualificationVariantBackendV1(variant m3VariantDescriptorV1, fixture fixtureManifest) bool {
	switch variant.VariantID {
	case "graph-disjoint-v1", "graph-overlap-020-v1":
		return variant.AssignmentBasis == partitionAssignmentGraphV1 && variant.ArtifactSHA256 == variant.GraphArtifactSHA256 && variant.ArtifactBackend == fmt.Sprintf("kahip_python_3.25_eco_symmetrized_v1_seed_%d", fixture.Seed) && variant.KaHIPPythonSHA256 == m8QualificationKaHIPPythonSHA256V1 && variant.KaHIPAdapterSHA256 == kahipAdapterSHA256
	case "stable-id-hash-disjoint-v1":
		return variant.AssignmentBasis == partitionAssignmentStableIDHashV1 && variant.ArtifactBackend == "stable_id_hash_baseline_v1" && variant.KaHIPPythonSHA256 == "" && variant.KaHIPAdapterSHA256 == ""
	default:
		return false
	}
}

const m8QualificationKaHIPPythonSHA256V1 = "7d51cd6b48b521277f5caa4610a82126e315fa2be4df069823a8b1eeb5bd4a86"

type m8QualificationEnvironmentV1 struct {
	GoVersion, GOOS, GOARCH string
	LogicalCPUs             int
	GOMAXPROCS              int
	GoMemoryLimitBytes      int64
	Host                    m8ProductionHostEvidenceV1
	PeakRSSCapBytes         uint64
	PersistentAssetCap      uint64
}

type m8QualificationProfileModeV1 struct {
	Status string
	Scope  string
}

// m8QualificationImmutableTopologyV1 retains serving-layout identity while
// omitting per-run listener addresses, request-progress counters, the
// asset-specific ready-set digest, and the volatile elected meta leader.
// Evidence() already emits groups in canonical group-ID order.
func m8QualificationImmutableTopologyV1(topology nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1) nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1 {
	topology.ReadySetDigest = ""
	topology.MetaLeader = ""
	topology.Groups = append([]nativewire.VectorPartitionM8ProductionGroupEvidenceV1(nil), topology.Groups...)
	for i := range topology.Groups {
		topology.Groups[i].Endpoint = ""
		topology.Groups[i].CommitIndex = 0
		topology.Groups[i].ReadIndex = 0
		topology.Groups[i].AppliedIndex = 0
		topology.Groups[i].EndpointHits = 0
	}
	return topology
}

const (
	m8QualificationPersistentAssetCapBytesV1 uint64 = 2 << 30
	m8QualificationPeakRSSCapBytesV1         uint64 = 4 << 30
	m8QualificationIndexMaxBytesV1                  = 1 << 20
	m8QualificationMatrixMaxBytesV1                 = 16 << 20
	m8QualificationTranscriptMaxBytesV1             = 2 << 20
)

// readBoundedRegularFileV1 reads a regular file without trusting a
// pre-allocation read of a caller-controlled path.
func readBoundedRegularFileV1(path string, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, errors.New("invalid bounded file byte cap")
	}
	before, err := os.Stat(path)
	if err != nil || !before.Mode().IsRegular() {
		return nil, errors.New("bounded input is not a regular file")
	}
	if before.Size() <= 0 || before.Size() > maxBytes {
		return nil, fmt.Errorf("bounded input has invalid byte length (cap %d)", maxBytes)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, maxBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(raw) == 0 || int64(len(raw)) > maxBytes || int64(len(raw)) != before.Size() {
		return nil, fmt.Errorf("bounded input changed or exceeds %d-byte cap", maxBytes)
	}
	after, err := os.Stat(path)
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
		return nil, errors.New("bounded input changed while reading")
	}
	return raw, nil
}

func m8QualificationResourceCapsV1() m8ProductionResourceCapsV1 {
	return m8ProductionResourceCapsV1{PersistentAssetBytes: m8QualificationPersistentAssetCapBytesV1, PeakRSSBytes: m8QualificationPeakRSSCapBytesV1}
}

func m8QualificationResourcesV1(report m8ProductionReportV1, fixture fixtureManifest, transcript m8ProductionMeasurementTranscriptV1) bool {
	resources := report.Resources
	peakRSS, peakOK := m8ProductionMeasurementTranscriptPeakRSSV1(transcript)
	return report.GoVersion != "" && report.GOOS != "" && report.GOARCH != "" && report.LogicalCPUs > 0 && report.GOMAXPROCS > 0 && report.GoMemoryLimitBytes > 0 && report.Host.CPUModel != "" &&
		peakOK && resources.PeakRSSMeasured && resources.PeakRSSBytes > 0 && uint64(resources.PeakRSSBytes) == peakRSS && resources.PeakRSSCapBytes == m8QualificationPeakRSSCapBytesV1 && peakRSS <= resources.PeakRSSCapBytes &&
		resources.PersistentAssetBytes > 0 && resources.PersistentAssetCap == m8QualificationPersistentAssetCapBytesV1 && resources.PersistentAssetBytes <= resources.PersistentAssetCap &&
		m8QualificationExactTruthCapV1(fixture) == report.Config.MaxExactTruthVisits
}

func m8QualificationProfilesV1(root string, profiles m8ProductionProfileEvidenceV1) (m8QualificationProfileModeV1, bool) {
	root, err := m8CanonicalPathV1(root)
	if err != nil {
		return m8QualificationProfileModeV1{}, false
	}
	directory, err := m8CanonicalPathV1(profiles.Directory)
	if err != nil || directory != profiles.Directory {
		return m8QualificationProfileModeV1{}, false
	}
	rel, err := filepath.Rel(root, directory)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return m8QualificationProfileModeV1{}, false
	}
	for _, artifact := range profiles.Artifacts {
		path, err := m8CanonicalPathV1(artifact.Path)
		if err != nil || path != artifact.Path {
			return m8QualificationProfileModeV1{}, false
		}
		rel, err := filepath.Rel(root, path)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return m8QualificationProfileModeV1{}, false
		}
	}
	return m8QualificationProfileModeV1{Status: profiles.Status, Scope: profiles.Scope}, true
}

func m8QualificationMeasurementTranscriptV1(root string, report m8ProductionReportV1) bool {
	if !validM8ProductionMeasurementTranscriptV1(report) {
		return false
	}
	rel, err := filepath.Rel(root, report.MeasurementTranscript.Path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func m8QualificationMeasurementTranscriptOutcomesV1(root string, report m8ProductionReportV1, truth [][]m8CanonicalResultV1) (m8ProductionMeasurementTranscriptV1, error) {
	if !m8QualificationMeasurementTranscriptV1(root, report) {
		return m8ProductionMeasurementTranscriptV1{}, errors.New("measurement transcript is not a canonical in-root artifact")
	}
	transcript, err := m8ReadProductionMeasurementTranscriptV1(report)
	if err != nil {
		return m8ProductionMeasurementTranscriptV1{}, err
	}
	if len(truth) != report.Dataset.Queries || len(transcript.Outcomes) != len(report.Rows) {
		return m8ProductionMeasurementTranscriptV1{}, errors.New("measurement transcript outcome/truth shape mismatch")
	}
	for rowIndex, row := range report.Rows {
		if row.Status != "pass" && row.Status != "fail" {
			continue
		}
		var recallSum float64
		for query, ids := range transcript.Outcomes[rowIndex].TopKIDs {
			recallSum += m8IDRecallV1(m8CanonicalIDsV1(truth[query]), ids)
		}
		recall := recallSum / float64(row.Samples)
		if math.Float64bits(recall) != math.Float64bits(row.RecallAtK) || math.Float64bits(recall) != math.Float64bits(row.Attribution.EndToEndRecallAtK) {
			return m8ProductionMeasurementTranscriptV1{}, errors.New("measurement transcript query outcomes do not reproduce retained recall")
		}
	}
	return transcript, nil
}

func m8ValidateQualificationMatrixDerivationV1(matrix m8ProductionMatrixV1) error {
	if len(matrix.Variants) == 0 {
		return errors.New("qualification matrix has no child reports")
	}
	cfg := config{baseSHA: matrix.BaseSHA, headSHA: matrix.HeadSHA, partitions: matrix.Variants[0].Config.Partitions, command: append([]string(nil), matrix.Command...)}
	expected, err := m8BuildProductionMatrixWithExecutionIntervalV1(cfg, matrix.Dataset, append([]m8ProductionReportV1(nil), matrix.Variants...), matrix.ExecutionStartedAt, matrix.ExecutionCompletedAt)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(matrix.RequiredVariants, expected.RequiredVariants) || !reflect.DeepEqual(matrix.Gates, expected.Gates) || matrix.Status != expected.Status || matrix.Disposition != expected.Disposition || matrix.ExecutionStartedAt != expected.ExecutionStartedAt || matrix.ExecutionCompletedAt != expected.ExecutionCompletedAt || matrix.OverlapMaterializationRatio != expected.OverlapMaterializationRatio || matrix.OverlapStorageRatio != expected.OverlapStorageRatio || !reflect.DeepEqual(matrix.OverlapDiagnostics, expected.OverlapDiagnostics) || !reflect.DeepEqual(matrix.Decision, expected.Decision) || !reflect.DeepEqual(matrix.Comparison, expected.Comparison) {
		return errors.New("qualification matrix derived evidence does not match child reports")
	}
	return nil
}

func m8ValidateQualificationSerialIntervalsV1(intervals []m8QualificationRunIntervalV1) error {
	if len(intervals) != len(m8QualificationFixturesV1)*3 {
		return errors.New("qualification index does not retain every matrix execution interval")
	}
	sort.Slice(intervals, func(i, j int) bool {
		if intervals[i].StartedAt.Equal(intervals[j].StartedAt) {
			return intervals[i].Path < intervals[j].Path
		}
		return intervals[i].StartedAt.Before(intervals[j].StartedAt)
	})
	for i, interval := range intervals {
		if interval.StartedAt.IsZero() || interval.EndedAt.IsZero() || !interval.EndedAt.After(interval.StartedAt) {
			return errors.New("qualification index has an invalid matrix execution interval")
		}
		if i > 0 && interval.StartedAt.Before(intervals[i-1].EndedAt) {
			return errors.New("qualification index retains overlapping matrix executions")
		}
	}
	return nil
}

func m8QualificationHasFullLadderV1(report m8ProductionReportV1) bool {
	seen := make(map[int]bool, 5)
	for _, row := range report.Rows {
		seen[row.Probes] = true
	}
	return seen[1] && seen[2] && seen[4] && seen[8] && seen[16]
}

func m8QualificationQualifiedRowV1(row m8ProductionRowV1) bool {
	return row.Status == "pass" && row.EfSearch == 128 && row.Concurrency == 1 && row.RouterMode == collections.VectorPartitionRouterModeApproxV1 && row.RouterCandidates == m8QualificationRouterCandidatesV1 && row.Attribution.OracleStagesComplete
}

func m8QualificationSHA256V1(value string) bool {
	return len(value) == sha256.Size*2 && value == strings.ToLower(value) && m8SHA256V1(value)
}

func m8QualificationGitSHAV1(value string) bool {
	return len(value) == 40 && value == strings.ToLower(value) && validSHA(value)
}

func m8QualificationRowsV1(report m8ProductionReportV1) (*m8ProductionRowV1, *m8ProductionRowV1) {
	var p2, p16 *m8ProductionRowV1
	for i := range report.Rows {
		row := &report.Rows[i]
		if !m8QualificationQualifiedRowV1(*row) {
			continue
		}
		switch row.Probes {
		case 2:
			p2 = row
		case 16:
			p16 = row
		}
	}
	return p2, p16
}
