package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	localHNSWFinalQualificationSchemaV1 = "treedb_local_hnsw_final_qualification_v1"
	localHNSWFinalQueryCountV1          = 1000
	localHNSWFinalTopKV1                = 10
)

type localHNSWFinalQualificationCellV1 struct {
	Repetition  int    `json:"repetition"`
	Variant     string `json:"variant"`
	Probes      int    `json:"probes"`
	Concurrency int    `json:"concurrency"`
	EFSearch    int    `json:"ef_search"`
}

const (
	localHNSWFinalQualificationCorpus250KV1  = "250k"
	localHNSWFinalQualificationCorpus100KV1  = "100k"
	localHNSWFinalQualificationBaselineV1    = "m16_efc128"
	localHNSWFinalQualificationCandidateV1   = "m18_efc256"
	localHNSWFinalQualificationBaselineEFV1  = 128
	localHNSWFinalQualificationCandidateEFV1 = 120
)

// localHNSWFinalQualificationScheduleV1 returns the three serialized,
// pair-order-balanced executions for both required probe/concurrency cells.
func localHNSWFinalQualificationScheduleV1() []localHNSWFinalQualificationCellV1 {
	cells := make([]localHNSWFinalQualificationCellV1, 0, 24)
	for repetition := range 3 {
		pair := 0
		for _, probes := range []int{2, 16} {
			for _, concurrency := range []int{1, 32} {
				variants := []string{localHNSWFinalQualificationBaselineV1, localHNSWFinalQualificationCandidateV1}
				if (repetition+pair)%2 == 1 {
					variants[0], variants[1] = variants[1], variants[0]
				}
				for _, variant := range variants {
					ef := localHNSWFinalQualificationBaselineEFV1
					if variant == localHNSWFinalQualificationCandidateV1 {
						ef = localHNSWFinalQualificationCandidateEFV1
					}
					cells = append(cells, localHNSWFinalQualificationCellV1{Repetition: repetition, Variant: variant, Probes: probes, Concurrency: concurrency, EFSearch: ef})
				}
				pair++
			}
		}
	}
	return cells
}

type localHNSWFinalQualificationRunV1 struct {
	Corpus string `json:"corpus"`
	localHNSWFinalQualificationCellV1
}

// localHNSWFinalQualificationRunsV1 returns all 48 child runs: 24 for the
// frozen 250k union and 24 for the independent 100k control.
func localHNSWFinalQualificationRunsV1() []localHNSWFinalQualificationRunV1 {
	schedule := localHNSWFinalQualificationScheduleV1()
	runs := make([]localHNSWFinalQualificationRunV1, 0, len(schedule)*2)
	for _, corpus := range []string{localHNSWFinalQualificationCorpus250KV1, localHNSWFinalQualificationCorpus100KV1} {
		for _, cell := range schedule {
			runs = append(runs, localHNSWFinalQualificationRunV1{Corpus: corpus, localHNSWFinalQualificationCellV1: cell})
		}
	}
	return runs
}

// localHNSWFinalQualificationUnionV1 binds the sealed split manifests before
// the final authority is permitted to use the full fixture query order.
func localHNSWFinalQualificationUnionV1(calibration, holdout localHNSWQuerySplitV1, fixture fixtureManifest, truthSHA256 string) ([]int, error) {
	if fixture.Queries != localHNSWFinalQueryCountV1 || validateLocalHNSWQuerySplitPairV1(calibration, holdout, fixture, truthSHA256) != nil {
		return nil, errors.New("invalid local HNSW final qualification query union")
	}
	ordinals := append(append([]int(nil), calibration.Ordinals...), holdout.Ordinals...)
	slices.Sort(ordinals)
	for ordinal := range ordinals {
		if ordinals[ordinal] != ordinal {
			return nil, errors.New("noncanonical local HNSW final qualification query union")
		}
	}
	return ordinals, nil
}

type localHNSWFinalQualificationCountsV1 struct {
	QueryCount       int    `json:"query_count"`
	TopK             int    `json:"top_k"`
	P2HitSlots       uint64 `json:"p2_hit_slots"`
	P16HitSlots      uint64 `json:"p16_hit_slots"`
	RoutingMissSlots uint64 `json:"routing_miss_slots"`
}

func localHNSWFinalQualificationQualityKeyV1(variant string, probes int) string {
	return variant + ":" + strconv.Itoa(probes)
}

func localHNSWFinalQualificationCountsValidV1(counts localHNSWFinalQualificationCountsV1) bool {
	slots := uint64(localHNSWFinalQueryCountV1 * localHNSWFinalTopKV1)
	return counts.QueryCount == localHNSWFinalQueryCountV1 && counts.TopK == localHNSWFinalTopKV1 && counts.P2HitSlots <= slots && counts.P16HitSlots <= slots && counts.RoutingMissSlots <= slots
}

func localHNSWFinalQualificationCountsPassV1(counts localHNSWFinalQualificationCountsV1) bool {
	return localHNSWFinalQualificationCountsValidV1(counts) && counts.P2HitSlots >= 9500 && counts.RoutingMissSlots <= 20 && localHNSWRepairMCurveHitSlotGapV1(counts.P2HitSlots, counts.P16HitSlots) <= 20
}

func localHNSWFinalQualificationControlPassV1(baseline, candidate localHNSWFinalQualificationCountsV1) bool {
	return localHNSWFinalQualificationCountsValidV1(baseline) && localHNSWFinalQualificationCountsValidV1(candidate) &&
		candidate.P2HitSlots+20 >= baseline.P2HitSlots && candidate.P16HitSlots+20 >= baseline.P16HitSlots && candidate.RoutingMissSlots <= baseline.RoutingMissSlots+20
}

type localHNSWFinalQualificationTimingCellV1 struct {
	localHNSWFinalQualificationCellV1
	QPS          float64 `json:"qps"`
	P95Nanos     uint64  `json:"p95_nanos"`
	ResultSHA256 string  `json:"result_sha256"`
}

type localHNSWFinalQualificationChildV1 struct {
	localHNSWFinalQualificationRunV1
	ReportSHA256          string                                  `json:"report_sha256"`
	ReportPath            string                                  `json:"report_path"`
	TranscriptSHA256      string                                  `json:"transcript_sha256"`
	TranscriptPath        string                                  `json:"transcript_path"`
	SourceIdentitySHA256  string                                  `json:"source_identity_sha256"`
	VariantIdentitySHA256 string                                  `json:"variant_identity_sha256"`
	M                     int                                     `json:"m"`
	EfConstruction        int                                     `json:"ef_construction"`
	StartedAt             time.Time                               `json:"started_at"`
	EndedAt               time.Time                               `json:"ended_at"`
	Counts                localHNSWFinalQualificationCountsV1     `json:"counts"`
	Timing                localHNSWFinalQualificationTimingCellV1 `json:"timing"`
}

type localHNSWFinalQualificationReportV1 struct {
	Schema      string                                      `json:"schema"`
	ResultKind  string                                      `json:"result_kind"`
	Status      string                                      `json:"status"`
	GeneratedAt string                                      `json:"generated_at"`
	Provenance  localHNSWAttributionProvenanceV1            `json:"provenance"`
	Inputs      localHNSWFinalQualificationInputsEvidenceV1 `json:"inputs"`
	Children    []localHNSWFinalQualificationChildV1        `json:"children"`
	Disposition string                                      `json:"disposition"`
	Limitations []string                                    `json:"limitations"`
}

type localHNSWFinalQualificationCorpusEvidenceV1 struct {
	Corpus              string                          `json:"corpus"`
	Fixture             fixtureManifest                 `json:"fixture"`
	DatasetManifest     localHNSWAttributionFileInputV1 `json:"dataset_manifest"`
	TruthCache          localHNSWAttributionFileInputV1 `json:"truth_cache"`
	TruthSHA256         string                          `json:"truth_sha256"`
	BaselineDB          string                          `json:"baseline_db"`
	CandidateDB         string                          `json:"candidate_db"`
	BaselineDescriptor  localHNSWAttributionFileInputV1 `json:"baseline_descriptor"`
	CandidateDescriptor localHNSWAttributionFileInputV1 `json:"candidate_descriptor"`
}

type localHNSWFinalQualificationInputsEvidenceV1 struct {
	Corpora         []localHNSWFinalQualificationCorpusEvidenceV1 `json:"corpora"`
	Calibration     localHNSWAttributionFileInputV1               `json:"calibration_split"`
	CalibrationRows int                                           `json:"calibration_rows"`
	Holdout         localHNSWAttributionFileInputV1               `json:"holdout_split"`
	HoldoutRows     int                                           `json:"holdout_rows"`
	QueryUnionRows  int                                           `json:"query_union_rows"`
	ApprovalSHA     string                                        `json:"approval_sha"`
	Artifacts       string                                        `json:"child_artifacts"`
	M18Curve        localHNSWAttributionFileInputV1               `json:"m18_ef_curve"`
	M18Timing       localHNSWAttributionFileInputV1               `json:"m18_timing"`
}

type localHNSWFinalQualificationRootsV1 struct {
	Baseline250K, Candidate250K string
	Baseline100K, Candidate100K string
}

type localHNSWFinalQualificationCorpusInputV1 struct {
	Fixture          fixtureManifest
	Dataset          string
	TruthCache       string
	TruthCacheSHA256 string
	Truth            [][]m8CanonicalResultV1
	Vectors          [][]float64
	Queries          [][]float64
}

func (r localHNSWFinalQualificationRootsV1) database(corpus, variant string) string {
	if corpus == localHNSWFinalQualificationCorpus250KV1 && variant == localHNSWFinalQualificationBaselineV1 {
		return r.Baseline250K
	}
	if corpus == localHNSWFinalQualificationCorpus250KV1 && variant == localHNSWFinalQualificationCandidateV1 {
		return r.Candidate250K
	}
	if corpus == localHNSWFinalQualificationCorpus100KV1 && variant == localHNSWFinalQualificationBaselineV1 {
		return r.Baseline100K
	}
	if corpus == localHNSWFinalQualificationCorpus100KV1 && variant == localHNSWFinalQualificationCandidateV1 {
		return r.Candidate100K
	}
	return ""
}

type localHNSWFinalQualificationRunnerV1 func(config, fixtureManifest, [][]float64, [][]float64, io.Writer) error
type localHNSWFinalQualificationDiscoveryV1 func(string) (m8ProductionReportV1, m8ProductionMeasurementTranscriptV1, string, string, error)

func localHNSWFinalQualificationInvokeV1(cfg config, inputs map[string]localHNSWFinalQualificationCorpusInputV1, roots localHNSWFinalQualificationRootsV1, runner localHNSWFinalQualificationRunnerV1, stdout io.Writer) ([]localHNSWFinalQualificationChildV1, error) {
	return localHNSWFinalQualificationInvokeWithDiscoveryV1(cfg, inputs, roots, runner, localHNSWFinalQualificationChildReportV1, stdout)
}

func localHNSWFinalQualificationInvokeWithDiscoveryV1(cfg config, inputs map[string]localHNSWFinalQualificationCorpusInputV1, roots localHNSWFinalQualificationRootsV1, runner localHNSWFinalQualificationRunnerV1, discover localHNSWFinalQualificationDiscoveryV1, stdout io.Writer) ([]localHNSWFinalQualificationChildV1, error) {
	if runner == nil || discover == nil {
		return nil, errors.New("missing local HNSW final qualification runner")
	}
	children := make([]localHNSWFinalQualificationChildV1, 0, len(localHNSWFinalQualificationRunsV1()))
	for i, run := range localHNSWFinalQualificationRunsV1() {
		input, ok := inputs[run.Corpus]
		db := roots.database(run.Corpus, run.Variant)
		if !ok || db == "" {
			return nil, errors.New("missing local HNSW final qualification child input")
		}
		child, err := localHNSWFinalQualificationChildConfigV1(cfg, input, db, run, i)
		if err != nil {
			return nil, err
		}
		if err := os.MkdirAll(child.out, 0o755); err != nil {
			return nil, err
		}
		startedAt := time.Now().UTC()
		if err := runner(child, input.Fixture, input.Vectors, input.Queries, stdout); err != nil {
			return nil, err
		}
		endedAt := time.Now().UTC()
		report, transcript, reportPath, reportSHA256, err := discover(child.out)
		if err != nil {
			return nil, err
		}
		evidence, err := localHNSWFinalQualificationChildFromTranscriptV1(run, report, transcript, reportPath, reportSHA256, input.Truth, startedAt, endedAt)
		if err != nil {
			return nil, err
		}
		children = append(children, evidence)
	}
	return children, nil
}

func localHNSWFinalQualificationChildConfigV1(base config, input localHNSWFinalQualificationCorpusInputV1, db string, run localHNSWFinalQualificationRunV1, ordinal int) (config, error) {
	if len(base.command) == 0 || input.Dataset == "" || input.TruthCache == "" || !localHNSWAttributionSHA256V1(input.TruthCacheSHA256) || db == "" || len(input.Truth) != input.Fixture.Queries || len(input.Vectors) != input.Fixture.Vectors || len(input.Queries) != input.Fixture.Queries {
		return config{}, errors.New("invalid local HNSW final qualification child config")
	}
	out := filepath.Join(base.out, "child-"+strconv.Itoa(ordinal))
	args := []string{
		"-mode", m8ProductionMultiGroupModeV1,
		"-dataset", input.Dataset,
		"-partitions", "16",
		"-probes", strconv.Itoa(run.Probes),
		"-overlap", "0.2",
		"-top-k", "10",
		"-recall-target", "0.9",
		"-seed", strconv.FormatInt(input.Fixture.Seed, 10),
		"-base-sha", base.baseSHA,
		"-head-sha", base.headSHA,
		"-source-checkout", base.sourceCheckout,
		"-format", "json",
		"-out", out,
		"-raft-groups", "4",
		"-raft-nodes-per-group", "3",
		"-concurrency", strconv.Itoa(run.Concurrency),
		"-warmup", "0",
		"-profiles", filepath.Join(out, "profiles"),
		"-m8-existing-db", db,
		"-m8-max-rss-bytes", strconv.FormatUint(m8QualificationPeakRSSCapBytesV1, 10),
		"-m8-max-persistent-asset-bytes", strconv.FormatUint(m8QualificationPersistentAssetCapBytesV1, 10),
		"-m8-max-exact-truth-visits", strconv.FormatInt(m8QualificationExactTruthCapV1(input.Fixture), 10),
		"-m8-truth-cache", input.TruthCache,
		"-m8-truth-cache-sha256", input.TruthCacheSHA256,
		"-router-candidates", strconv.Itoa(m8QualificationRouterCandidatesV1),
		"-ef-search", strconv.Itoa(run.EFSearch),
	}
	child, err := parseConfig(args)
	if err != nil {
		return config{}, err
	}
	child.command = append([]string{base.command[0]}, args...)
	return child, nil
}

func localHNSWFinalQualificationChildValidV1(child localHNSWFinalQualificationChildV1, expected localHNSWFinalQualificationRunV1) bool {
	if child.localHNSWFinalQualificationRunV1 != expected || child.ReportPath == "" || child.TranscriptPath == "" || !localHNSWAttributionSHA256V1(child.ReportSHA256) || !localHNSWAttributionSHA256V1(child.TranscriptSHA256) || !localHNSWAttributionSHA256V1(child.SourceIdentitySHA256) || !localHNSWAttributionSHA256V1(child.VariantIdentitySHA256) || child.StartedAt.IsZero() || child.EndedAt.IsZero() || child.EndedAt.Before(child.StartedAt) {
		return false
	}
	if child.Variant == localHNSWFinalQualificationBaselineV1 {
		return child.M == 16 && child.EfConstruction == 128 && child.EFSearch == localHNSWFinalQualificationBaselineEFV1
	}
	return child.Variant == localHNSWFinalQualificationCandidateV1 && child.M == 18 && child.EfConstruction == 256 && child.EFSearch == localHNSWFinalQualificationCandidateEFV1
}

// localHNSWFinalQualificationChildReportV1 reads exactly the one report
// emitted into a dedicated child directory by the existing M8 runner.
func localHNSWFinalQualificationChildReportV1(dir string) (m8ProductionReportV1, m8ProductionMeasurementTranscriptV1, string, string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return m8ProductionReportV1{}, m8ProductionMeasurementTranscriptV1{}, "", "", err
	}
	var path string
	for _, entry := range entries {
		name := entry.Name()
		if entry.Type().IsRegular() && strings.HasPrefix(name, "vector_partition_m8_") && strings.HasSuffix(name, ".json") && !strings.HasPrefix(name, "vector_partition_m8_measurements_") {
			if path != "" {
				return m8ProductionReportV1{}, m8ProductionMeasurementTranscriptV1{}, "", "", errors.New("multiple local HNSW final qualification child reports")
			}
			path = filepath.Join(dir, name)
		}
	}
	if path == "" {
		return m8ProductionReportV1{}, m8ProductionMeasurementTranscriptV1{}, "", "", errors.New("missing local HNSW final qualification child report")
	}
	raw, err := readBoundedRegularFileV1(path, m8QualificationMatrixMaxBytesV1)
	if err != nil {
		return m8ProductionReportV1{}, m8ProductionMeasurementTranscriptV1{}, "", "", err
	}
	var report m8ProductionReportV1
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&report); err != nil {
		return m8ProductionReportV1{}, m8ProductionMeasurementTranscriptV1{}, "", "", err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return m8ProductionReportV1{}, m8ProductionMeasurementTranscriptV1{}, "", "", errors.New("local HNSW final qualification child report has trailing JSON")
	}
	if err := validateM8ProductionReportV1(report, m8ProductionResourceCapsV1{PersistentAssetBytes: m8QualificationPersistentAssetCapBytesV1, PeakRSSBytes: m8QualificationPeakRSSCapBytesV1}); err != nil {
		return m8ProductionReportV1{}, m8ProductionMeasurementTranscriptV1{}, "", "", err
	}
	transcript, err := m8ReadProductionMeasurementTranscriptV1(report)
	if err != nil || transcript.ExecutionID != report.ExecutionID {
		return m8ProductionReportV1{}, m8ProductionMeasurementTranscriptV1{}, "", "", errors.New("invalid local HNSW final qualification child transcript")
	}
	digest := sha256.Sum256(raw)
	return report, transcript, path, hex.EncodeToString(digest[:]), nil
}

func localHNSWFinalQualificationChildFromTranscriptV1(expected localHNSWFinalQualificationRunV1, report m8ProductionReportV1, transcript m8ProductionMeasurementTranscriptV1, reportPath, reportSHA256 string, truth [][]m8CanonicalResultV1, startedAt, endedAt time.Time) (localHNSWFinalQualificationChildV1, error) {
	var out localHNSWFinalQualificationChildV1
	if len(report.Rows) != 1 || len(transcript.Outcomes) != 1 || len(truth) != localHNSWFinalQueryCountV1 || reportPath == "" || report.MeasurementTranscript.Path == "" || !localHNSWAttributionSHA256V1(reportSHA256) || report.Variant == nil || startedAt.IsZero() || endedAt.Before(startedAt) {
		return out, errors.New("invalid local HNSW final qualification child evidence")
	}
	row, outcome := report.Rows[0], transcript.Outcomes[0]
	if !localHNSWFinalQualificationChildGateLedgerValidV1(expected.Probes, report) || row.Status != "pass" || outcome.Status != "pass" || row.Probes != expected.Probes || row.Concurrency != expected.Concurrency || row.EfSearch != expected.EFSearch || row.Samples != localHNSWFinalQueryCountV1 || len(outcome.TopKIDs) != localHNSWFinalQueryCountV1 || len(outcome.TopKScoreBits) != localHNSWFinalQueryCountV1 || len(outcome.ExactRepresentativeTruthHits) != localHNSWFinalQueryCountV1 || transcript.ExecutionID != report.ExecutionID || !localHNSWAttributionSHA256V1(report.MeasurementTranscript.SHA256) || !row.Attribution.ApproximateRouterPartitionCoverageComplete || !row.Attribution.CoordinatorMergeIDParity || !row.Attribution.CoordinatorMergeScoreParity || expected.Probes == 16 && (!row.Attribution.ExhaustivePartitionIDParity || !row.Attribution.ExhaustivePartitionScoreParity || row.Attribution.ExhaustivePartitionRecallAtK != 1) {
		return out, errors.New("invalid local HNSW final qualification child row")
	}
	var finalHits, routingHits uint64
	resultHash := sha256.New()
	_, _ = resultHash.Write([]byte("treedb_local_hnsw_final_qualification_results_v1\x00"))
	for i, ids := range outcome.TopKIDs {
		if len(ids) != localHNSWFinalTopKV1 || int(outcome.ExactRepresentativeTruthHits[i]) > localHNSWFinalTopKV1 {
			return out, errors.New("invalid local HNSW final qualification child outcome")
		}
		if len(outcome.TopKScoreBits[i]) != len(ids) {
			return out, errors.New("invalid local HNSW final qualification child score outcome")
		}
		finalHits += uint64(m8IDHitCountV1(m8CanonicalIDsV1(truth[i]), ids))
		routingHits += uint64(outcome.ExactRepresentativeTruthHits[i])
		raw, _ := json.Marshal(struct {
			IDs    []string
			Scores []uint32
		}{ids, outcome.TopKScoreBits[i]})
		sum := sha256.Sum256(raw)
		_, _ = resultHash.Write(sum[:])
	}
	if math.Abs(row.Attribution.ExactRepresentativeRecallAtK-float64(routingHits)/float64(localHNSWFinalQueryCountV1*localHNSWFinalTopKV1)) > 1e-12 {
		return out, errors.New("local HNSW final qualification routing-hit mismatch")
	}
	variant := expected.Variant
	if (variant == localHNSWFinalQualificationBaselineV1 && (report.Variant.PartitionHNSWM != 16 || m3DescriptorPartitionHNSWEfCV1(*report.Variant) != 128)) || (variant == localHNSWFinalQualificationCandidateV1 && (report.Variant.PartitionHNSWM != 18 || m3DescriptorPartitionHNSWEfCV1(*report.Variant) != 256)) {
		return out, errors.New("local HNSW final qualification variant mismatch")
	}
	out = localHNSWFinalQualificationChildV1{localHNSWFinalQualificationRunV1: expected, ReportSHA256: reportSHA256, ReportPath: reportPath, TranscriptSHA256: report.MeasurementTranscript.SHA256, TranscriptPath: report.MeasurementTranscript.Path, SourceIdentitySHA256: report.Variant.Source.Checksum, VariantIdentitySHA256: report.Variant.BuildIdentityDigest, M: report.Variant.PartitionHNSWM, EfConstruction: m3DescriptorPartitionHNSWEfCV1(*report.Variant), StartedAt: startedAt, EndedAt: endedAt, Counts: localHNSWFinalQualificationCountsV1{QueryCount: 1000, TopK: 10, RoutingMissSlots: uint64(10000) - routingHits}, Timing: localHNSWFinalQualificationTimingCellV1{localHNSWFinalQualificationCellV1: expected.localHNSWFinalQualificationCellV1, QPS: row.QPS, P95Nanos: row.P95Nanos, ResultSHA256: hex.EncodeToString(resultHash.Sum(nil))}}
	if expected.Probes == 2 {
		out.Counts.P2HitSlots = finalHits
	} else {
		out.Counts.P16HitSlots = finalHits
	}
	return out, nil
}

func localHNSWFinalQualificationChildGateLedgerValidV1(probes int, report m8ProductionReportV1) bool {
	ledger := report.GateLedger
	if report.Status != "experimental_gate_failures" || ledger != m8ProductionGateLedgerForReportV1(report) {
		return false
	}
	exhaustive, reduction := "not_run", "pass"
	if probes == 16 {
		exhaustive, reduction = "pass", "fail"
	}
	return ledger.ExhaustiveParity == exhaustive &&
		ledger.FailureHonesty == "pass" && ledger.PartitionPackReachability == "pass" &&
		ledger.Recall == "pass" && ledger.ProbeReduction == reduction &&
		ledger.EndToEndQPS == "fail" && ledger.TailLatency == "fail" && ledger.Balance == "pass" &&
		ledger.OverlapStorage == "fail" && ledger.ResourceBounds == "pass" &&
		ledger.ExistingBehavior == "pending_full_required_suites"
}

func localHNSWFinalQualificationReportValidV1(report localHNSWFinalQualificationReportV1) error {
	expected := localHNSWFinalQualificationRunsV1()
	if report.Schema != localHNSWFinalQualificationSchemaV1 || report.ResultKind != "local_hnsw_final_qualification_v1" || report.Status != "valid" || report.Disposition != "pass" || len(report.Limitations) == 0 || len(report.Children) != len(expected) || len(report.Provenance.Command) == 0 || report.Provenance.BaseSHA != localHNSWAttributionSourceLockV1 || !validLowerSHA(report.Provenance.HeadSHA) || report.Provenance.SourceDirty || !filepath.IsAbs(report.Provenance.SourceCheckout) || !filepath.IsAbs(report.Provenance.Executable) || !localHNSWAttributionSHA256V1(report.Provenance.ExecutableSHA256) || !validLowerSHA(report.Inputs.ApprovalSHA) || report.Inputs.Calibration.SHA256 != localHNSWAttributionCalibrationSHA256V1 || report.Inputs.Holdout.SHA256 != localHNSWAttributionHoldoutSHA256V1 || report.Inputs.CalibrationRows != 806 || report.Inputs.HoldoutRows != 194 || report.Inputs.QueryUnionRows != localHNSWFinalQueryCountV1 || !filepath.IsAbs(report.Inputs.Artifacts) || report.Inputs.M18Curve.Path == "" || report.Inputs.M18Curve.SHA256 != localHNSWRepairMTimingSelectedCurveSHA256V1 || report.Inputs.M18Timing.Path == "" || report.Inputs.M18Timing.SHA256 != localHNSWFinalQualificationM18TimingSHA256V1 || len(report.Inputs.Corpora) != 2 {
		return errors.New("invalid local HNSW final qualification report")
	}
	if _, err := time.Parse(time.RFC3339Nano, report.GeneratedAt); err != nil {
		return errors.New("invalid local HNSW final qualification generation time")
	}
	for i, corpus := range report.Inputs.Corpora {
		fixture := m8QualificationFixturesV1[1-i]
		anchor, ok := m8QualificationTruthCacheAnchorV1(fixture)
		if !ok || corpus.Corpus != []string{localHNSWFinalQualificationCorpus250KV1, localHNSWFinalQualificationCorpus100KV1}[i] || corpus.Fixture != fixture || corpus.DatasetManifest.Path == "" || !localHNSWAttributionSHA256V1(corpus.DatasetManifest.SHA256) || corpus.TruthCache.Path == "" || corpus.TruthCache.SHA256 != anchor.ArtifactSHA256 || corpus.TruthSHA256 != anchor.TruthSHA256 || corpus.BaselineDB == "" || corpus.CandidateDB == "" || corpus.BaselineDB == corpus.CandidateDB || corpus.BaselineDescriptor.Path != filepath.Join(corpus.BaselineDB, m3VariantDescriptorFileV1) || corpus.CandidateDescriptor.Path != filepath.Join(corpus.CandidateDB, m3VariantDescriptorFileV1) || !localHNSWAttributionSHA256V1(corpus.BaselineDescriptor.SHA256) || !localHNSWAttributionSHA256V1(corpus.CandidateDescriptor.SHA256) {
			return errors.New("invalid local HNSW final qualification corpus input")
		}
	}
	var previous time.Time
	byCorpusSource := map[string]string{}
	byCorpusVariant := map[string]string{}
	quality := map[string]localHNSWFinalQualificationCountsV1{}
	timing := map[string][]localHNSWFinalQualificationTimingCellV1{}
	for i, child := range report.Children {
		childDir := filepath.Join(report.Inputs.Artifacts, "child-"+strconv.Itoa(i))
		if !localHNSWFinalQualificationChildValidV1(child, expected[i]) || filepath.Dir(child.ReportPath) != childDir || filepath.Dir(child.TranscriptPath) != childDir || (!previous.IsZero() && child.StartedAt.Before(previous)) {
			return errors.New("invalid local HNSW final qualification child")
		}
		previous = child.EndedAt
		if prior, ok := byCorpusSource[child.Corpus]; ok && prior != child.SourceIdentitySHA256 {
			return errors.New("local HNSW final qualification source identity drift")
		}
		byCorpusSource[child.Corpus] = child.SourceIdentitySHA256
		key := child.Corpus + ":" + child.Variant
		if prior, ok := byCorpusVariant[key]; ok && prior != child.VariantIdentitySHA256 {
			return errors.New("local HNSW final qualification variant identity drift")
		}
		byCorpusVariant[key] = child.VariantIdentitySHA256
		timing[child.Corpus] = append(timing[child.Corpus], child.Timing)
		if child.Counts.QueryCount != localHNSWFinalQueryCountV1 || child.Counts.TopK != localHNSWFinalTopKV1 || child.Counts.RoutingMissSlots > uint64(localHNSWFinalQueryCountV1*localHNSWFinalTopKV1) || (child.Probes == 2 && child.Counts.P16HitSlots != 0) || (child.Probes == 16 && child.Counts.P2HitSlots != 0) {
			return errors.New("invalid local HNSW final qualification counts")
		}
		qualityKey := child.Corpus + ":" + localHNSWFinalQualificationQualityKeyV1(child.Variant, child.Probes)
		if prior, ok := quality[qualityKey]; ok && prior != child.Counts {
			return errors.New("unstable local HNSW final qualification counts")
		}
		quality[qualityKey] = child.Counts
	}
	qualityCell := func(corpus, variant string, probes int) (localHNSWFinalQualificationCountsV1, bool) {
		counts, ok := quality[corpus+":"+localHNSWFinalQualificationQualityKeyV1(variant, probes)]
		return counts, ok
	}
	candidateP2, p2OK := qualityCell(localHNSWFinalQualificationCorpus250KV1, localHNSWFinalQualificationCandidateV1, 2)
	candidateP16, p16OK := qualityCell(localHNSWFinalQualificationCorpus250KV1, localHNSWFinalQualificationCandidateV1, 16)
	baselineP2, baselineP2OK := qualityCell(localHNSWFinalQualificationCorpus250KV1, localHNSWFinalQualificationBaselineV1, 2)
	baselineP16, baselineP16OK := qualityCell(localHNSWFinalQualificationCorpus250KV1, localHNSWFinalQualificationBaselineV1, 16)
	counts := localHNSWFinalQualificationCountsV1{QueryCount: localHNSWFinalQueryCountV1, TopK: localHNSWFinalTopKV1, P2HitSlots: candidateP2.P2HitSlots, P16HitSlots: candidateP16.P16HitSlots, RoutingMissSlots: candidateP2.RoutingMissSlots}
	if !p2OK || !p16OK || !baselineP2OK || !baselineP16OK || candidateP2.RoutingMissSlots != baselineP2.RoutingMissSlots || candidateP16.RoutingMissSlots != baselineP16.RoutingMissSlots || byCorpusSource[localHNSWFinalQualificationCorpus250KV1] == "" || byCorpusSource[localHNSWFinalQualificationCorpus100KV1] == "" || byCorpusVariant[localHNSWFinalQualificationCorpus250KV1+":"+localHNSWFinalQualificationBaselineV1] == byCorpusVariant[localHNSWFinalQualificationCorpus250KV1+":"+localHNSWFinalQualificationCandidateV1] || byCorpusVariant[localHNSWFinalQualificationCorpus100KV1+":"+localHNSWFinalQualificationBaselineV1] == byCorpusVariant[localHNSWFinalQualificationCorpus100KV1+":"+localHNSWFinalQualificationCandidateV1] || !localHNSWFinalQualificationCountsPassV1(counts) {
		return errors.New("local HNSW final qualification quality gate failed")
	}
	baseline100P2, baseline100P2OK := qualityCell(localHNSWFinalQualificationCorpus100KV1, localHNSWFinalQualificationBaselineV1, 2)
	baseline100P16, baseline100P16OK := qualityCell(localHNSWFinalQualificationCorpus100KV1, localHNSWFinalQualificationBaselineV1, 16)
	candidate100P2, candidate100P2OK := qualityCell(localHNSWFinalQualificationCorpus100KV1, localHNSWFinalQualificationCandidateV1, 2)
	candidate100P16, candidate100P16OK := qualityCell(localHNSWFinalQualificationCorpus100KV1, localHNSWFinalQualificationCandidateV1, 16)
	baseline100 := localHNSWFinalQualificationCountsV1{QueryCount: localHNSWFinalQueryCountV1, TopK: localHNSWFinalTopKV1, P2HitSlots: baseline100P2.P2HitSlots, P16HitSlots: baseline100P16.P16HitSlots, RoutingMissSlots: baseline100P2.RoutingMissSlots}
	candidate100 := localHNSWFinalQualificationCountsV1{QueryCount: localHNSWFinalQueryCountV1, TopK: localHNSWFinalTopKV1, P2HitSlots: candidate100P2.P2HitSlots, P16HitSlots: candidate100P16.P16HitSlots, RoutingMissSlots: candidate100P2.RoutingMissSlots}
	if !baseline100P2OK || !baseline100P16OK || !candidate100P2OK || !candidate100P16OK || baseline100P2.RoutingMissSlots != candidate100P2.RoutingMissSlots || baseline100P16.RoutingMissSlots != candidate100P16.RoutingMissSlots || !localHNSWFinalQualificationControlPassV1(baseline100, candidate100) {
		return errors.New("local HNSW final qualification 100k control gate failed")
	}
	for _, corpus := range []string{localHNSWFinalQualificationCorpus250KV1, localHNSWFinalQualificationCorpus100KV1} {
		if err := localHNSWFinalQualificationTimingPassV1(timing[corpus]); err != nil {
			return err
		}
	}
	return nil
}

func localHNSWFinalQualificationTimingPassV1(cells []localHNSWFinalQualificationTimingCellV1) error {
	schedule := localHNSWFinalQualificationScheduleV1()
	if len(cells) != len(schedule) {
		return errors.New("invalid local HNSW final qualification timing cell count")
	}
	qps := [2][4][]float64{}
	p95 := [2][4][]uint64{}
	digests := [2][2]string{}
	for i, cell := range cells {
		if cell.localHNSWFinalQualificationCellV1 != schedule[i] || !finitePositive(cell.QPS) || cell.P95Nanos == 0 || !localHNSWAttributionSHA256V1(cell.ResultSHA256) {
			return errors.New("invalid local HNSW final qualification timing cell")
		}
		variant := 0
		if cell.Variant == localHNSWFinalQualificationCandidateV1 {
			variant = 1
		} else if cell.Variant != localHNSWFinalQualificationBaselineV1 {
			return errors.New("invalid local HNSW final qualification variant")
		}
		probe := 0
		if cell.Probes == 16 {
			probe = 1
		} else if cell.Probes != 2 {
			return errors.New("invalid local HNSW final qualification probes")
		}
		concurrency := 0
		if cell.Concurrency == 32 {
			concurrency = 1
		} else if cell.Concurrency != 1 {
			return errors.New("invalid local HNSW final qualification concurrency")
		}
		if prior := digests[variant][probe]; prior != "" && prior != cell.ResultSHA256 {
			return errors.New("unstable local HNSW final qualification results")
		} else if prior == "" {
			digests[variant][probe] = cell.ResultSHA256
		}
		qps[variant][probe*2+concurrency] = append(qps[variant][probe*2+concurrency], cell.QPS)
		p95[variant][probe*2+concurrency] = append(p95[variant][probe*2+concurrency], cell.P95Nanos)
	}
	for i := range qps[0] {
		slices.Sort(qps[0][i])
		slices.Sort(qps[1][i])
		slices.Sort(p95[0][i])
		slices.Sort(p95[1][i])
		if qps[1][i][1]/qps[0][i][1] < .90 || float64(p95[1][i][1])/float64(p95[0][i][1]) > 1.10 || math.IsNaN(qps[1][i][1]) || math.IsInf(qps[1][i][1], 0) {
			return errors.New("local HNSW final qualification timing gate failed")
		}
	}
	return nil
}

func finitePositive(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}
