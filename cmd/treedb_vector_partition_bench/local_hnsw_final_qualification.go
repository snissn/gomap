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
}

const (
	localHNSWFinalQualificationCorpus250KV1 = "250k"
	localHNSWFinalQualificationCorpus100KV1 = "100k"
	localHNSWFinalQualificationBaselineV1   = "m16_efc128"
	localHNSWFinalQualificationCandidateV1  = "m18_efc256"
)

// localHNSWFinalQualificationScheduleV1 returns the three serialized,
// pair-order-balanced executions for both required probe/concurrency cells.
func localHNSWFinalQualificationScheduleV1() []localHNSWFinalQualificationCellV1 {
	cells := make([]localHNSWFinalQualificationCellV1, 0, 24)
	for repetition := range 3 {
		variants := []string{localHNSWFinalQualificationBaselineV1, localHNSWFinalQualificationCandidateV1}
		if repetition%2 == 1 {
			variants[0], variants[1] = variants[1], variants[0]
		}
		for _, probes := range []int{2, 16} {
			for _, concurrency := range []int{1, 32} {
				for _, variant := range variants {
					cells = append(cells, localHNSWFinalQualificationCellV1{Repetition: repetition, Variant: variant, Probes: probes, Concurrency: concurrency})
				}
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

type localHNSWFinalQualificationTimingCellV1 struct {
	localHNSWFinalQualificationCellV1
	QPS          float64  `json:"qps"`
	P95Nanos     uint64   `json:"p95_nanos"`
	ResultSHA256 []string `json:"result_sha256"`
}

type localHNSWFinalQualificationChildV1 struct {
	localHNSWFinalQualificationRunV1
	ReportSHA256          string                                  `json:"report_sha256"`
	TranscriptSHA256      string                                  `json:"transcript_sha256"`
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
	Schema     string                               `json:"schema"`
	ResultKind string                               `json:"result_kind"`
	Status     string                               `json:"status"`
	Children   []localHNSWFinalQualificationChildV1 `json:"children"`
}

type localHNSWFinalQualificationRootsV1 struct {
	Baseline250K, Candidate250K string
	Baseline100K, Candidate100K string
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

func localHNSWFinalQualificationInvokeV1(cfg config, fixtures map[string]fixtureManifest, roots localHNSWFinalQualificationRootsV1, runner localHNSWFinalQualificationRunnerV1, stdout io.Writer) error {
	return localHNSWFinalQualificationInvokeWithDiscoveryV1(cfg, fixtures, roots, runner, func(dir string) error { _, _, err := localHNSWFinalQualificationChildReportV1(dir); return err }, stdout)
}

func localHNSWFinalQualificationInvokeWithDiscoveryV1(cfg config, fixtures map[string]fixtureManifest, roots localHNSWFinalQualificationRootsV1, runner localHNSWFinalQualificationRunnerV1, discover func(string) error, stdout io.Writer) error {
	if runner == nil || discover == nil {
		return errors.New("missing local HNSW final qualification runner")
	}
	for i, run := range localHNSWFinalQualificationRunsV1() {
		fixture, ok := fixtures[run.Corpus]
		db := roots.database(run.Corpus, run.Variant)
		if !ok || db == "" {
			return errors.New("missing local HNSW final qualification child input")
		}
		child := cfg
		child.m8ExistingDB, child.probes, child.concurrency, child.efSearch = db, []int{run.Probes}, []int{run.Concurrency}, []int{128}
		child.out, child.profiles = filepath.Join(cfg.out, "child-"+strconv.Itoa(i)), filepath.Join(cfg.out, "child-"+strconv.Itoa(i), "profiles")
		if err := os.MkdirAll(child.out, 0o755); err != nil {
			return err
		}
		vectors, queries := fixtureData(fixture)
		if err := runner(child, fixture, vectors, queries, stdout); err != nil {
			return err
		}
		if err := discover(child.out); err != nil {
			return err
		}
	}
	return nil
}

func localHNSWFinalQualificationChildValidV1(child localHNSWFinalQualificationChildV1, expected localHNSWFinalQualificationRunV1) bool {
	if child.localHNSWFinalQualificationRunV1 != expected || !localHNSWAttributionSHA256V1(child.ReportSHA256) || !localHNSWAttributionSHA256V1(child.TranscriptSHA256) || !localHNSWAttributionSHA256V1(child.SourceIdentitySHA256) || !localHNSWAttributionSHA256V1(child.VariantIdentitySHA256) || child.StartedAt.IsZero() || child.EndedAt.IsZero() || !child.EndedAt.After(child.StartedAt) {
		return false
	}
	if child.Variant == localHNSWFinalQualificationBaselineV1 {
		return child.M == 16 && child.EfConstruction == 128
	}
	return child.Variant == localHNSWFinalQualificationCandidateV1 && child.M == 18 && child.EfConstruction == 256
}

// localHNSWFinalQualificationChildReportV1 reads exactly the one report
// emitted into a dedicated child directory by the existing M8 runner.
func localHNSWFinalQualificationChildReportV1(dir string) (m8ProductionReportV1, string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return m8ProductionReportV1{}, "", err
	}
	var path string
	for _, entry := range entries {
		name := entry.Name()
		if entry.Type().IsRegular() && strings.HasPrefix(name, "vector_partition_m8_") && strings.HasSuffix(name, ".json") && !strings.HasPrefix(name, "vector_partition_m8_measurements_") {
			if path != "" {
				return m8ProductionReportV1{}, "", errors.New("multiple local HNSW final qualification child reports")
			}
			path = filepath.Join(dir, name)
		}
	}
	if path == "" {
		return m8ProductionReportV1{}, "", errors.New("missing local HNSW final qualification child report")
	}
	raw, err := readBoundedRegularFileV1(path, m8QualificationMatrixMaxBytesV1)
	if err != nil {
		return m8ProductionReportV1{}, "", err
	}
	var report m8ProductionReportV1
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&report); err != nil {
		return m8ProductionReportV1{}, "", err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return m8ProductionReportV1{}, "", errors.New("local HNSW final qualification child report has trailing JSON")
	}
	transcript, err := m8ReadProductionMeasurementTranscriptV1(report)
	if err != nil || transcript.ExecutionID != report.ExecutionID {
		return m8ProductionReportV1{}, "", errors.New("invalid local HNSW final qualification child transcript")
	}
	digest := sha256.Sum256(raw)
	return report, hex.EncodeToString(digest[:]), nil
}

func localHNSWFinalQualificationChildFromTranscriptV1(expected localHNSWFinalQualificationRunV1, report m8ProductionReportV1, transcript m8ProductionMeasurementTranscriptV1, reportSHA256 string, truth [][]m8CanonicalResultV1, startedAt, endedAt time.Time) (localHNSWFinalQualificationChildV1, error) {
	var out localHNSWFinalQualificationChildV1
	if len(report.Rows) != 1 || len(transcript.Outcomes) != 1 || len(truth) != localHNSWFinalQueryCountV1 || !localHNSWAttributionSHA256V1(reportSHA256) || report.Variant == nil || startedAt.IsZero() || !endedAt.After(startedAt) {
		return out, errors.New("invalid local HNSW final qualification child evidence")
	}
	row, outcome := report.Rows[0], transcript.Outcomes[0]
	if row.Probes != expected.Probes || row.Concurrency != expected.Concurrency || row.EfSearch != 128 || row.Samples != localHNSWFinalQueryCountV1 || len(outcome.TopKIDs) != localHNSWFinalQueryCountV1 || len(outcome.TopKScoreBits) != localHNSWFinalQueryCountV1 || len(outcome.ExactRepresentativeTruthHits) != localHNSWFinalQueryCountV1 || transcript.ExecutionID != report.ExecutionID || !localHNSWAttributionSHA256V1(report.MeasurementTranscript.SHA256) {
		return out, errors.New("invalid local HNSW final qualification child row")
	}
	var finalHits, routingHits uint64
	digests := make([]string, len(outcome.TopKIDs))
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
		digests[i] = hex.EncodeToString(sum[:])
	}
	if math.Abs(row.Attribution.ExactRepresentativeRecallAtK-float64(routingHits)/float64(localHNSWFinalQueryCountV1*localHNSWFinalTopKV1)) > 1e-12 {
		return out, errors.New("local HNSW final qualification routing-hit mismatch")
	}
	variant := expected.Variant
	if (variant == localHNSWFinalQualificationBaselineV1 && (report.Variant.PartitionHNSWM != 16)) || (variant == localHNSWFinalQualificationCandidateV1 && report.Variant.PartitionHNSWM != 18) {
		return out, errors.New("local HNSW final qualification variant mismatch")
	}
	out = localHNSWFinalQualificationChildV1{localHNSWFinalQualificationRunV1: expected, ReportSHA256: reportSHA256, TranscriptSHA256: report.MeasurementTranscript.SHA256, SourceIdentitySHA256: report.Variant.Source.Checksum, VariantIdentitySHA256: report.Variant.IndexDefinitionDigest, M: report.Variant.PartitionHNSWM, EfConstruction: map[string]int{localHNSWFinalQualificationBaselineV1: 128, localHNSWFinalQualificationCandidateV1: 256}[variant], StartedAt: startedAt, EndedAt: endedAt, Counts: localHNSWFinalQualificationCountsV1{QueryCount: 1000, TopK: 10, RoutingMissSlots: uint64(10000) - routingHits}, Timing: localHNSWFinalQualificationTimingCellV1{localHNSWFinalQualificationCellV1: expected.localHNSWFinalQualificationCellV1, QPS: row.QPS, P95Nanos: row.P95Nanos, ResultSHA256: digests}}
	if expected.Probes == 2 {
		out.Counts.P2HitSlots = finalHits
	} else {
		out.Counts.P16HitSlots = finalHits
	}
	return out, nil
}

func localHNSWFinalQualificationReportValidV1(report localHNSWFinalQualificationReportV1) error {
	expected := localHNSWFinalQualificationRunsV1()
	if report.Schema != localHNSWFinalQualificationSchemaV1 || report.ResultKind != "local_hnsw_final_qualification_v1" || report.Status != "valid" || len(report.Children) != len(expected) {
		return errors.New("invalid local HNSW final qualification report")
	}
	var previous time.Time
	byCorpusSource := map[string]string{}
	byCorpusVariant := map[string]string{}
	quality := map[string]localHNSWFinalQualificationCountsV1{}
	timing := map[string][]localHNSWFinalQualificationTimingCellV1{}
	for i, child := range report.Children {
		if !localHNSWFinalQualificationChildValidV1(child, expected[i]) || (!previous.IsZero() && child.StartedAt.Before(previous)) {
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
		if child.Corpus == localHNSWFinalQualificationCorpus250KV1 {
			if child.Counts.QueryCount != localHNSWFinalQueryCountV1 || child.Counts.TopK != localHNSWFinalTopKV1 || child.Counts.RoutingMissSlots > uint64(localHNSWFinalQueryCountV1*localHNSWFinalTopKV1) || (child.Probes == 2 && child.Counts.P16HitSlots != 0) || (child.Probes == 16 && child.Counts.P2HitSlots != 0) {
				return errors.New("invalid local HNSW final qualification counts")
			}
			key := localHNSWFinalQualificationQualityKeyV1(child.Variant, child.Probes)
			if prior, ok := quality[key]; ok && prior != child.Counts {
				return errors.New("unstable local HNSW final qualification counts")
			}
			quality[key] = child.Counts
		}
	}
	candidateP2, p2OK := quality[localHNSWFinalQualificationQualityKeyV1(localHNSWFinalQualificationCandidateV1, 2)]
	candidateP16, p16OK := quality[localHNSWFinalQualificationQualityKeyV1(localHNSWFinalQualificationCandidateV1, 16)]
	counts := localHNSWFinalQualificationCountsV1{QueryCount: localHNSWFinalQueryCountV1, TopK: localHNSWFinalTopKV1, P2HitSlots: candidateP2.P2HitSlots, P16HitSlots: candidateP16.P16HitSlots, RoutingMissSlots: candidateP2.RoutingMissSlots}
	if !p2OK || !p16OK || candidateP2.RoutingMissSlots != candidateP16.RoutingMissSlots || byCorpusSource[localHNSWFinalQualificationCorpus250KV1] == "" || byCorpusSource[localHNSWFinalQualificationCorpus100KV1] == "" || byCorpusVariant[localHNSWFinalQualificationCorpus250KV1+":"+localHNSWFinalQualificationBaselineV1] == byCorpusVariant[localHNSWFinalQualificationCorpus250KV1+":"+localHNSWFinalQualificationCandidateV1] || byCorpusVariant[localHNSWFinalQualificationCorpus100KV1+":"+localHNSWFinalQualificationBaselineV1] == byCorpusVariant[localHNSWFinalQualificationCorpus100KV1+":"+localHNSWFinalQualificationCandidateV1] || !localHNSWFinalQualificationCountsPassV1(counts) {
		return errors.New("local HNSW final qualification quality gate failed")
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
	digests := [2][4][]string{}
	for i, cell := range cells {
		if cell.localHNSWFinalQualificationCellV1 != schedule[i] || !finitePositive(cell.QPS) || cell.P95Nanos == 0 || len(cell.ResultSHA256) != localHNSWFinalQueryCountV1 {
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
		for _, digest := range cell.ResultSHA256 {
			if !localHNSWAttributionSHA256V1(digest) {
				return errors.New("invalid local HNSW final qualification result digest")
			}
		}
		if prior := digests[variant][probe*2+concurrency]; prior != nil && !slices.Equal(prior, cell.ResultSHA256) {
			return errors.New("unstable local HNSW final qualification results")
		} else if prior == nil {
			digests[variant][probe*2+concurrency] = append([]string(nil), cell.ResultSHA256...)
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
