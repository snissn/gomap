package main

import (
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
	"sort"
	"strings"

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
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
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

type m8QualificationCampaignSummaryV1 struct {
	P4QPSMin     float64 `json:"p4_qps_min"`
	P4QPSMedian  float64 `json:"p4_qps_median"`
	P4QPSMax     float64 `json:"p4_qps_max"`
	P16QPSMin    float64 `json:"p16_qps_min"`
	P16QPSMedian float64 `json:"p16_qps_median"`
	P16QPSMax    float64 `json:"p16_qps_max"`
	P4P95Min     uint64  `json:"p4_p95_min"`
	P4P95Median  uint64  `json:"p4_p95_median"`
	P4P95Max     uint64  `json:"p4_p95_max"`
	P16P95Min    uint64  `json:"p16_p95_min"`
	P16P95Median uint64  `json:"p16_p95_median"`
	P16P95Max    uint64  `json:"p16_p95_max"`
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
	info, err := os.Stat(index)
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("qualification index is not a regular file")
	}
	raw, err := os.ReadFile(index)
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
	summary := m8QualificationIndexSummaryV1{SchemaVersion: 1, ResultKind: "vector_partition_structured_qualification_summary_v1", Status: "qualified", BaseSHA: index.BaseSHA, HeadSHA: index.HeadSHA, Campaigns: make(map[string]m8QualificationCampaignSummaryV1, len(m8QualificationFixturesV1))}
	if index.SchemaVersion != 1 || index.ResultKind != "vector_partition_structured_qualification_index_v1" || !m8QualificationGitSHAV1(index.BaseSHA) || !m8QualificationGitSHAV1(index.HeadSHA) || len(index.Campaigns) != len(m8QualificationFixturesV1) {
		return m8QualificationIndexSummaryV1{}, errors.New("qualification index requires exactly the two authoritative corpus campaigns")
	}
	for _, campaign := range index.Campaigns {
		if _, duplicate := summary.Campaigns[campaign.FixtureChecksum]; !m8QualificationFixtureChecksumV1(campaign.FixtureChecksum) || duplicate {
			return m8QualificationIndexSummaryV1{}, errors.New("qualification index has duplicate or unknown corpus")
		}
		if campaign.BaseSHA != index.BaseSHA || campaign.HeadSHA != index.HeadSHA {
			return m8QualificationIndexSummaryV1{}, errors.New("qualification index campaigns do not share the index revision")
		}
		campaignSummary, err := m8ValidateQualificationCampaignV1(root, campaign)
		if err != nil {
			return m8QualificationIndexSummaryV1{}, err
		}
		summary.Campaigns[campaign.FixtureChecksum] = campaignSummary
	}
	for _, fixture := range m8QualificationFixturesV1 {
		if _, ok := summary.Campaigns[fixture.Checksum]; !ok {
			return m8QualificationIndexSummaryV1{}, errors.New("qualification index is missing an authoritative corpus")
		}
	}
	return summary, nil
}

func m8ValidateQualificationCampaignV1(root string, campaign m8QualificationCampaignV1) (m8QualificationCampaignSummaryV1, error) {
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
	var p4QPS, p16QPS []float64
	var p4P95, p16P95 []uint64
	var summary m8QualificationCampaignSummaryV1
	variantDescriptors := make(map[string]m3VariantDescriptorV1, len(m8RequiredVariantIDsV1))
	var topology *nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1
	truthArtifact := ""
	var dataset fixtureManifest
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
		info, err := os.Stat(resolvedPath)
		if err != nil || !info.Mode().IsRegular() {
			return summary, errors.New("qualification campaign artifact is not a regular file")
		}
		raw, err := os.ReadFile(resolvedPath)
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
		if matrix.SchemaVersion != 4 || matrix.ResultKind != "m8_production_multi_variant_matrix_v4" || matrix.BaseSHA != campaign.BaseSHA || matrix.HeadSHA != campaign.HeadSHA || !m8QualificationGitSHAV1(matrix.BaseSHA) || !m8QualificationGitSHAV1(matrix.HeadSHA) {
			return summary, fmt.Errorf("qualification matrix %s has invalid schema/provenance/status", cleanPath)
		}
		if err := validateM8ProductionMatrixV1(matrix); err != nil {
			return summary, fmt.Errorf("validate qualification matrix %s: %w", run.Path, err)
		}
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
			if err := validateM8ProductionReportV1(*report, m8QualificationResourceCapsV1()); err != nil {
				return summary, fmt.Errorf("validate qualification child %s: %w", cleanPath, err)
			}
			if !m8QualificationCommandV1(*report) {
				return summary, fmt.Errorf("qualification matrix %s has command/config mismatch", cleanPath)
			}
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
			if report.Variant != nil && !m8QualificationM3BuildCapsV1(*report.Variant, report.Dataset) {
				return summary, fmt.Errorf("qualification matrix %s has off-plan M3 construction caps/router configuration", run.Path)
			}
			if report.BaseSHA != campaign.BaseSHA || report.HeadSHA != campaign.HeadSHA || report.Dataset != matrix.Dataset || report.Dirty || !m8QualificationSHA256V1(report.TruthCache.ArtifactSHA256) || report.Variant == nil || report.Variant.BuildDirty || seenVariants[report.Variant.VariantID] || !slices.Contains(m8RequiredVariantIDsV1, report.Variant.VariantID) || !m8QualificationSHA256V1(report.Variant.ArtifactSHA256) || !m8QualificationConfigV1(report.Config, report.Dataset, report.Variant.OverlapRatio, runIndex) || !m8QualificationVariantBackendV1(*report.Variant, report.Dataset) {
				return summary, fmt.Errorf("qualification matrix %s has unbound child identity", run.Path)
			}
			if executionIDs[report.ExecutionID] {
				return summary, fmt.Errorf("qualification matrix %s reuses execution identity", cleanPath)
			}
			executionIDs[report.ExecutionID] = true
			if !m8QualificationResourcesV1(*report, report.Dataset) {
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
		if err := m8ValidateQualificationMatrixDerivationV1(matrix); err != nil {
			return summary, fmt.Errorf("derive qualification matrix %s: %w", cleanPath, err)
		}
		if len(seenVariants) != len(m8RequiredVariantIDsV1) || selected == nil {
			return summary, fmt.Errorf("qualification matrix %s has no graph-overlap candidate", run.Path)
		}
		p4, p16 := m8QualificationRowsV1(*selected)
		if p4 == nil || p16 == nil || p4.RecallAtK < .90 || p16.RecallAtK < .90 || p4.Attribution.FinalMembershipOracleRecallAtK < .90 || math.Abs(p4.Attribution.ExactToApproximateLossAtK) > .01 || p4.QPS < p16.QPS*1.15 || p4.P95Nanos > p16.P95Nanos {
			return summary, fmt.Errorf("qualification matrix %s misses the selected p4/p16 gate", run.Path)
		}
		if matrix.Status != "local_gate_pass" || matrix.Gates.RequiredVariants != "pass" || matrix.Gates.ExhaustiveParity != "pass" || matrix.Gates.FailureHonesty != "pass" || matrix.Gates.PartitionPackReachability != "pass" || matrix.Gates.Balance != "pass" || matrix.Gates.ResourceBounds != "pass" || matrix.Gates.OverlapStorage != "pass" {
			return summary, fmt.Errorf("qualification matrix %s does not bind the required gates", run.Path)
		}
		p4QPS, p16QPS = append(p4QPS, p4.QPS), append(p16QPS, p16.QPS)
		p4P95, p16P95 = append(p4P95, p4.P95Nanos), append(p16P95, p16.P95Nanos)
	}
	if len(executionIDs) != len(campaign.Runs)*len(m8RequiredVariantIDsV1) {
		return summary, errors.New("qualification campaign requires distinct execution identities")
	}
	minMedianMax := func(values []float64) (float64, float64, float64) {
		sort.Float64s(values)
		return values[0], values[len(values)/2], values[len(values)-1]
	}
	summary.P4QPSMin, summary.P4QPSMedian, summary.P4QPSMax = minMedianMax(p4QPS)
	summary.P16QPSMin, summary.P16QPSMedian, summary.P16QPSMax = minMedianMax(p16QPS)
	minMedianMaxU64 := func(values []uint64) (uint64, uint64, uint64) {
		slices.Sort(values)
		return values[0], values[len(values)/2], values[len(values)-1]
	}
	summary.P4P95Min, summary.P4P95Median, summary.P4P95Max = minMedianMaxU64(p4P95)
	summary.P16P95Min, summary.P16P95Median, summary.P16P95Max = minMedianMaxU64(p16P95)
	return summary, nil
}

var m8QualificationFixturesV1 = [...]fixtureManifest{
	{SchemaVersion: 1, Fixture: "deterministic_100000", Generator: "treedb_vector_partition_embedding_mixture_v1", Arithmetic: "ieee754_binary64_explicit_fma_v1", Vectors: 100000, Queries: 1000, Dimensions: 128, Metric: "cosine", Seed: 4017, Checksum: "ecc2224f386932e580e4956f2cfa852140d3134625971c3511bc0d5feddf9b95"},
	{SchemaVersion: 1, Fixture: "qualification_embedding_mixture_250000", Generator: "treedb_vector_partition_embedding_mixture_v1", Arithmetic: "ieee754_binary64_explicit_fma_v1", Vectors: 250000, Queries: 1000, Dimensions: 128, Metric: "cosine", Seed: 4016, Checksum: "d0c7c82ba868853aae9a4280161003d72714ad1701d41ed3169c2fa94d470d69"},
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
	return cfg.RaftGroups == 4 && cfg.RaftNodesPerGroup == 3 && cfg.Partitions == 16 && cfg.TopK == 10 && cfg.RecallTarget == .90 && cfg.Warmup == 0 && cfg.EffectiveWarmup == 0 && cfg.RouterCandidates == 64 && cfg.MaxExactTruthVisits == m8QualificationExactTruthCapV1(fixture) && cfg.Seed == fixture.Seed && slices.Equal(cfg.Probes, []int{1, 2, 4, 8, 16}) && slices.Equal(cfg.Concurrency, []int{1}) && slices.Equal(cfg.EfSearch, []int{64}) && slices.Equal(cfg.Overlap, []float64{overlap})
}

// m8QualificationCommandV1 re-parses runner argv so the retained replay
// command cannot disagree with the already-validated measured configuration.
func m8QualificationCommandV1(report m8ProductionReportV1) bool {
	if len(report.Command) < 2 {
		return false
	}
	cfg, err := parseConfig(report.Command[1:])
	if err != nil || cfg.stage != m8ProductionMultiGroupModeV1 {
		return false
	}
	datasetDirectory, err := m8CanonicalPathV1(cfg.dataset)
	if err != nil || report.DatasetDirectory == "" || datasetDirectory != report.DatasetDirectory {
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
	if report.Profiles.Status == "not_captured" {
		if cfg.profiles != "" {
			return false
		}
	} else {
		profiles, err := m8CanonicalPathV1(cfg.profiles)
		if err != nil || profiles != report.Profiles.Directory {
			return false
		}
	}
	switch report.TruthCache.Status {
	case "computed":
		if cfg.m8TruthCacheSHA256 != "" && cfg.m8TruthCacheSHA256 != report.TruthCache.ArtifactSHA256 {
			return false
		}
	case "reused":
		if cfg.m8TruthCacheSHA256 != report.TruthCache.ArtifactSHA256 {
			return false
		}
	default:
		return false
	}
	warmup, _ := m8WarmupCountAndConcurrencyV1(cfg)
	commandConfig := m8ProductionConfigEvidenceV1{
		RaftGroups: cfg.raftGroups, RaftNodesPerGroup: cfg.raftNodes, Partitions: cfg.partitions,
		Probes: cfg.probes, Overlap: cfg.overlaps, TopK: cfg.topK, RecallTarget: cfg.recallTarget,
		Concurrency: cfg.concurrency, Warmup: cfg.warmup, EffectiveWarmup: warmup,
		EfSearch: cfg.efSearch, RouterCandidates: cfg.routerCandidates,
		MaxExactTruthVisits: cfg.m8MaxExactTruthVisits, Seed: cfg.seed,
	}
	return reflect.DeepEqual(commandConfig, report.Config) &&
		cfg.m8MaxRSSBytes == report.Resources.PeakRSSCapBytes &&
		cfg.m8MaxAssetBytes == report.Resources.PersistentAssetCap
}

func m8QualificationExactTruthCapV1(fixture fixtureManifest) int64 {
	if fixture.Vectors == 250000 {
		return 1_500_000_000
	}
	return 600_000_000
}

func m8QualificationM3BuildCapsV1(variant m3VariantDescriptorV1, fixture fixtureManifest) bool {
	cap, visits := int64(20_000_000_000), int64(400_000_000)
	if fixture.Vectors == 250000 {
		cap, visits = 50_000_000_000, 900_000_000
	}
	routerConfig := vectorpartition.DefaultRouterConfigV1()
	routerConfig.MaxScalarWork = cap
	return variant.PartitionMaxDistanceWork == cap && variant.RouterMaxScalarWork == cap && variant.M3MaxBenchmarkVisits == visits && variant.RouterConfig == routerConfig
}

func m8QualificationVariantBackendV1(variant m3VariantDescriptorV1, fixture fixtureManifest) bool {
	switch variant.VariantID {
	case "graph-disjoint-v1", "graph-overlap-020-v1":
		return variant.AssignmentBasis == partitionAssignmentGraphV1 && variant.ArtifactSHA256 == variant.GraphArtifactSHA256 && variant.ArtifactBackend == fmt.Sprintf("kahip_python_3.25_eco_symmetrized_v1_seed_%d", fixture.Seed)
	case "stable-id-hash-disjoint-v1":
		return variant.AssignmentBasis == partitionAssignmentStableIDHashV1 && variant.ArtifactBackend == "stable_id_hash_baseline_v1"
	default:
		return false
	}
}

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
// omitting per-run listener addresses, request-progress counters, and the
// asset-specific ready-set digest.
// Evidence() already emits groups in canonical group-ID order.
func m8QualificationImmutableTopologyV1(topology nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1) nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1 {
	topology.ReadySetDigest = ""
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
)

func m8QualificationResourceCapsV1() m8ProductionResourceCapsV1 {
	return m8ProductionResourceCapsV1{PersistentAssetBytes: m8QualificationPersistentAssetCapBytesV1, PeakRSSBytes: m8QualificationPeakRSSCapBytesV1}
}

func m8QualificationResourcesV1(report m8ProductionReportV1, fixture fixtureManifest) bool {
	resources := report.Resources
	return report.GoVersion != "" && report.GOOS != "" && report.GOARCH != "" && report.LogicalCPUs > 0 && report.GOMAXPROCS > 0 && report.GoMemoryLimitBytes > 0 && report.Host.CPUModel != "" &&
		resources.PeakRSSMeasured && resources.PeakRSSBytes > 0 && resources.PeakRSSCapBytes == m8QualificationPeakRSSCapBytesV1 && uint64(resources.PeakRSSBytes) <= resources.PeakRSSCapBytes &&
		resources.PersistentAssetBytes > 0 && resources.PersistentAssetCap == m8QualificationPersistentAssetCapBytesV1 && resources.PersistentAssetBytes <= resources.PersistentAssetCap &&
		m8QualificationExactTruthCapV1(fixture) == report.Config.MaxExactTruthVisits
}

func m8QualificationProfilesV1(root string, profiles m8ProductionProfileEvidenceV1) (m8QualificationProfileModeV1, bool) {
	if !validM8ProductionProfilesV1(profiles) {
		return m8QualificationProfileModeV1{}, false
	}
	for _, path := range append(append([]string(nil), profiles.Captured...), profiles.Directory) {
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

func m8ValidateQualificationMatrixDerivationV1(matrix m8ProductionMatrixV1) error {
	if len(matrix.Variants) == 0 {
		return errors.New("qualification matrix has no child reports")
	}
	cfg := config{baseSHA: matrix.BaseSHA, headSHA: matrix.HeadSHA, partitions: matrix.Variants[0].Config.Partitions, command: append([]string(nil), matrix.Command...)}
	expected, err := m8BuildProductionMatrixV1(cfg, matrix.Dataset, append([]m8ProductionReportV1(nil), matrix.Variants...))
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(matrix.RequiredVariants, expected.RequiredVariants) || !reflect.DeepEqual(matrix.Gates, expected.Gates) || matrix.Status != expected.Status || matrix.Disposition != expected.Disposition || matrix.OverlapMaterializationRatio != expected.OverlapMaterializationRatio || matrix.OverlapStorageRatio != expected.OverlapStorageRatio || !reflect.DeepEqual(matrix.OverlapDiagnostics, expected.OverlapDiagnostics) || !reflect.DeepEqual(matrix.Decision, expected.Decision) || !reflect.DeepEqual(matrix.Comparison, expected.Comparison) {
		return errors.New("qualification matrix derived evidence does not match child reports")
	}
	return nil
}

func m8QualificationHasFullLadderV1(report m8ProductionReportV1) bool {
	seen := make(map[int]bool, 5)
	for _, row := range report.Rows {
		if m8QualificationQualifiedRowV1(row) {
			seen[row.Probes] = true
		}
	}
	return seen[1] && seen[2] && seen[4] && seen[8] && seen[16]
}

func m8QualificationQualifiedRowV1(row m8ProductionRowV1) bool {
	return row.Status == "pass" && row.EfSearch == 64 && row.Concurrency == 1 && row.RouterMode == collections.VectorPartitionRouterModeApproxV1 && row.RouterCandidates == 64 && row.Attribution.OracleStagesComplete
}

func m8QualificationSHA256V1(value string) bool {
	return len(value) == sha256.Size*2 && value == strings.ToLower(value) && m8SHA256V1(value)
}

func m8QualificationGitSHAV1(value string) bool {
	return len(value) == 40 && value == strings.ToLower(value) && validSHA(value)
}

func m8QualificationRowsV1(report m8ProductionReportV1) (*m8ProductionRowV1, *m8ProductionRowV1) {
	var p4, p16 *m8ProductionRowV1
	for i := range report.Rows {
		row := &report.Rows[i]
		if !m8QualificationQualifiedRowV1(*row) {
			continue
		}
		switch row.Probes {
		case 4:
			p4 = row
		case 16:
			p16 = row
		}
	}
	return p4, p16
}
