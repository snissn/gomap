package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/nativewire"
)

func TestM8ProductionCanceledCellRetainsDeclaredPopulation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	queries := [][]float64{{1}, {2}, {3}}
	row, results, _, err := m8RunProductionCellV1(ctx, nil, &m8ProductionMultiGroupAssetsV1{}, queries, make([][]m8CanonicalResultV1, len(queries)), 1, 10, 2, 1, 256, 1<<20)
	if err != nil {
		t.Fatalf("canceled attempts must remain evidence, not erase the cell: %v", err)
	}
	if row.Samples != len(queries) || len(results) != len(queries) || row.Status == "pass" || row.QPS != 0 {
		t.Fatalf("canceled population lost or reported successful: row=%+v results=%v", row, results)
	}
	if row.Accounting.Summary.NotDispatched != 3 || row.Accounting.Summary.Canceled != 3 || row.Accounting.Summary.Dispatched != 0 || row.Accounting.Summary.TruthSlots != 3 {
		t.Fatalf("nondispatched queries were not retained: %+v", row.Accounting)
	}
}

func m8CompleteMeasurementGoldenV1(t testing.TB) m8ProductionRowV1 {
	t.Helper()
	row := m8ProductionRowV1{Samples: 100, Concurrency: 1, ElapsedNanos: uint64(time.Second), Accounting: &m8MeasurementAccountingV1{Contract: m8CompleteAttemptsV1, Attempts: make([]m8MeasuredAttemptV1, 100)}}
	for i := range row.Accounting.Attempts {
		a := m8MeasuredAttemptV1{Class: "success", Dispatched: true, TerminalNanos: 20, CoordinatorNanos: 10, WorkObserved: true, TruthHits: 9, Counters: nativewire.VectorPartitionCoordinatorCountersV1{RouterScoreCalls: 3}}
		if i >= 90 {
			a.Class, a.TruthHits = "timeout", 0
			a.TerminalNanos, a.CoordinatorNanos, a.Counters.RouterScoreCalls = 200, 100, 7
		}
		row.Accounting.Attempts[i] = a
	}
	if err := m8SummarizeAttemptsV1(&row, 10); err != nil {
		t.Fatal(err)
	}
	return row
}

func assertM8MixedMeasurementV1(t *testing.T, row m8ProductionRowV1, results [][]m8CanonicalResultV1, durations []uint64, successes, refusals int) {
	t.Helper()
	if row.Accounting == nil || len(results) != row.Samples || len(durations) != row.Samples || len(row.Accounting.Attempts) != row.Samples || row.Accounting.Summary.Succeeded != successes || row.Accounting.Summary.Refused != refusals || !row.NoPartialResults {
		t.Fatalf("lost mixed population: %+v", row)
	}
	for i, a := range row.Accounting.Attempts {
		if a.Class == "success" {
			if len(results[i]) != 10 || durations[i] == 0 {
				t.Fatalf("successful query %d was erased", i)
			}
		} else if len(results[i]) != 0 || durations[i] != 0 {
			t.Fatalf("failed query %d retained a partial result or successful latency", i)
		}
	}
}

func TestM8CompleteMeasurementDenominatorsAndFailureWork(t *testing.T) {
	row := m8CompleteMeasurementGoldenV1(t)
	s := row.Accounting.Summary
	if s.Declared != 100 || s.Dispatched != 100 || s.Succeeded != 90 || s.TimedOut != 10 || s.TruthHits != 810 || s.TruthSlots != 1000 || s.SuccessRecall != .9 || s.ServiceRecall != .81 || s.CompletionRate != .9 || s.AttemptRate != 100 || s.Goodput != 90 {
		t.Fatalf("incorrect 100/90/10 denominators: %+v", s)
	}
	if row.Status != "measurement_failure" || row.RecallAtK != .81 || row.QPS != 90 || row.RouterScoreCalls != 340 || row.MaxTotalNanos != 100 || row.P99Nanos != 10 {
		t.Fatalf("failure work or conditional latency lost: %+v", row)
	}
	if err := m8ValidateAttemptAccountingV1(row, 10); err != nil {
		t.Fatal(err)
	}
	// Even perfect successful results cannot hide the ten failed requests.
	perfect := m8CompleteMeasurementGoldenV1(t)
	for i := range 90 {
		perfect.Accounting.Attempts[i].TruthHits = 10
	}
	if err := m8SummarizeAttemptsV1(&perfect, 10); err != nil || perfect.Accounting.Summary.SuccessRecall != 1 || perfect.RecallAtK != .9 || perfect.Status != "measurement_failure" {
		t.Fatalf("perfect successes hid failures: %+v err=%v", perfect, err)
	}
	for name, mutate := range map[string]func(*m8ProductionRowV1){
		"unknown terminal":       func(r *m8ProductionRowV1) { r.Accounting.Attempts[0].Class = "" },
		"missing query":          func(r *m8ProductionRowV1) { r.Accounting.Attempts = r.Accounting.Attempts[:99] },
		"failed hits":            func(r *m8ProductionRowV1) { r.Accounting.Attempts[99].TruthHits = 1 },
		"unobserved work":        func(r *m8ProductionRowV1) { r.Accounting.Attempts[0].WorkObserved = false },
		"invented completion":    func(r *m8ProductionRowV1) { r.Accounting.Summary.Succeeded++ },
		"filtered denominator":   func(r *m8ProductionRowV1) { r.RecallAtK = .9 },
		"unbound failure work":   func(r *m8ProductionRowV1) { r.RouterScoreCalls-- },
		"failure latency in p99": func(r *m8ProductionRowV1) { r.P99Nanos = 100 },
		"terminal beyond window": func(r *m8ProductionRowV1) {
			r.Concurrency = 100
			r.Accounting.Attempts[99].TerminalNanos = r.ElapsedNanos + 1
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := m8CompleteMeasurementGoldenV1(t)
			mutate(&r)
			if m8ValidateAttemptAccountingV1(r, 10) == nil {
				t.Fatal("accepted corrupted complete measurement")
			}
		})
	}
}

func TestM8RepeatedWindowsKeepFailuresAndOrder(t *testing.T) {
	cfg := m8ProductionConfigEvidenceV1{MeasurementAccounting: m8CompleteAttemptsV1, MeasuredRepetitions: 3, Partitions: 4, Probes: []int{1, 4}, EfSearch: []int{10}, Concurrency: []int{1, 32}, Overlap: []float64{0}, TopK: 10, RecallTarget: .95}
	order := m8MeasurementOrderV1(cfg)
	if len(order) != 12 || order[0].probes != 1 || order[3].probes != 4 || order[4].probes != 4 || order[4].concurrency != 32 || order[7].probes != 1 || order[8].probes != 1 {
		t.Fatalf("not alternating complete blocks: %+v", order)
	}
	report := m8ProductionReportV1{Config: cfg}
	for _, c := range order {
		report.Rows = append(report.Rows, m8ProductionRowV1{Status: "pass", Probes: c.probes, EfSearch: c.efSearch, Concurrency: c.concurrency, Repetition: c.repetition, Samples: 1, RecallAtK: 1, QPS: 100 / float64(c.probes), P95Nanos: uint64(c.probes), Accounting: &m8MeasurementAccountingV1{Contract: m8CompleteAttemptsV1, Summary: m8MeasurementSummaryV1{Succeeded: 1}}})
	}
	if err := validateM8ProductionMeasurementCellsV1(cfg, 4, report.Rows); err != nil {
		t.Fatal(err)
	}
	if !m8RepeatedPairGateV1(report, report.Rows[0], report.Rows[2], true, true) {
		t.Fatal("complete repeated pair was rejected")
	}
	report.Rows[7].Status = "measurement_failure"
	if m8MeasuredCoordinateCompleteV1(report, report.Rows[0]) || m8RepeatedPairGateV1(report, report.Rows[0], report.Rows[2], true, true) {
		t.Fatal("failed required repetition hidden by successful windows")
	}
	report.Rows[7].Status = "pass"
	report.Rows[7].QPS = 1
	if m8RepeatedPairGateV1(report, report.Rows[0], report.Rows[2], true, true) {
		t.Fatal("slow repeated window hidden by faster windows")
	}
	for name, rows := range map[string][]m8ProductionRowV1{
		"missing":     report.Rows[:11],
		"duplicate":   append(slices.Clone(report.Rows[:11]), report.Rows[0]),
		"wrong order": append([]m8ProductionRowV1{report.Rows[1], report.Rows[0]}, report.Rows[2:]...),
	} {
		t.Run(name, func(t *testing.T) {
			if validateM8ProductionMeasurementCellsV1(cfg, 4, rows) == nil {
				t.Fatal("accepted incomplete or reordered repeated windows")
			}
		})
	}
}

func TestM8CompleteMeasurementTerminalClassesAndBounds(t *testing.T) {
	row := m8CompleteMeasurementGoldenV1(t)
	// Keep declared, dispatched and successful populations distinct.
	for i := 95; i < 100; i++ {
		row.Accounting.Attempts[i] = m8MeasuredAttemptV1{Class: "canceled"}
	}
	if err := m8SummarizeAttemptsV1(&row, 10); err != nil {
		t.Fatal(err)
	}
	if row.Accounting.Summary.CompletionRate != float64(90)/95 || row.Accounting.Summary.AttemptRate != 95 || row.Accounting.Summary.ServiceRecall != .81 || row.Accounting.Summary.NotDispatched != 5 {
		t.Fatalf("mixed dispatch denominators: %+v", row.Accounting.Summary)
	}
	for _, class := range []string{m8ProductionCandidateCoverageShortfallV1, m8ProductionRouterScoreBudgetExhaustedV1, "timeout", "canceled", "error", "invalid_response"} {
		t.Run(class, func(t *testing.T) {
			r := m8ProductionRowV1{Samples: 1, Concurrency: 1, ElapsedNanos: 100, Accounting: &m8MeasurementAccountingV1{Contract: m8CompleteAttemptsV1, Attempts: []m8MeasuredAttemptV1{{Class: class, Dispatched: true, TerminalNanos: 20, PartialResponse: true}}}}
			if err := m8SummarizeAttemptsV1(&r, 10); err != nil {
				t.Fatal(err)
			}
			if r.Status != "measurement_failure" || r.QPS != 0 || r.RecallAtK != 0 || r.P99Nanos != 0 || r.NoPartialResults || r.Accounting.Summary.Dispatched != 1 {
				t.Fatalf("all-failure or partial-response evidence erased: %+v", r)
			}
		})
	}
	row = m8CompleteMeasurementGoldenV1(t)
	row.Accounting.Attempts[0].Counters.RouterScoreCalls = math.MaxUint64
	if m8SummarizeAttemptsV1(&row, 10) == nil {
		t.Fatal("counter overflow accepted")
	}
	// Bind the compact JSON admission allowance to maximum-width native fields.
	attempt := m8MeasuredAttemptV1{Class: m8ProductionCandidateCoverageShortfallV1, Dispatched: true, WorkObserved: true, PartialResponse: true, TerminalNanos: math.MaxUint64, CoordinatorNanos: math.MaxUint64, TruthHits: math.MaxInt}
	counters := reflect.ValueOf(&attempt.Counters).Elem()
	for i := range counters.NumField() {
		if counters.Field(i).Kind() != reflect.Uint64 {
			t.Fatal("audit new counter type before changing receipt admission")
		}
		counters.Field(i).SetUint(math.MaxUint64)
	}
	raw, err := json.Marshal(attempt)
	if err != nil || len(raw) > m8AttemptJSONMaxBytesV1 {
		t.Fatalf("terminal JSON exceeds preflight bound: bytes=%d err=%v", len(raw), err)
	}
}

func TestM8CompleteOutcomeClassificationAndPartialResponse(t *testing.T) {
	for _, tc := range []struct {
		err   error
		class string
	}{{context.DeadlineExceeded, "timeout"}, {context.Canceled, "canceled"}, {collections.ErrVectorPartitionRouterCandidateCoverageV1, m8ProductionCandidateCoverageShortfallV1}, {collections.ErrVectorPartitionRouterScoreBudget, m8ProductionRouterScoreBudgetExhaustedV1}, {errors.New("transport failed"), "error"}} {
		t.Run(tc.class, func(t *testing.T) {
			outcome := m8ProductionCellOutcomeV1{dispatched: true, terminalNanos: 100, err: &nativewire.VectorPartitionCoordinatorErrorV1{Err: tc.err, Timing: nativewire.VectorPartitionCoordinatorTimingV1{TotalNanos: 90}, Counters: nativewire.VectorPartitionCoordinatorCountersV1{LocalScoreCalls: 17}}}
			attempt, results := m8ReduceProductionOutcomeV1(outcome, collections.VectorPartitionManifestV1{}, nil, 1, 10)
			if attempt.Class != tc.class || !attempt.WorkObserved || attempt.Counters.LocalScoreCalls != 17 || attempt.CoordinatorNanos != 90 || attempt.PartialResponse || len(results) != 0 {
				t.Fatalf("typed failure was lost: %+v %v", attempt, results)
			}
			outcome.response.Neighbors = []nativewire.VectorPartitionCoordinatorNeighborV1{{ID: "partial"}}
			attempt, results = m8ReduceProductionOutcomeV1(outcome, collections.VectorPartitionManifestV1{}, nil, 1, 10)
			if !attempt.PartialResponse || len(results) != 0 || attempt.TruthHits != 0 {
				t.Fatal("partial payload escaped failure accounting")
			}
			outcome.err = nil
			attempt, results = m8ReduceProductionOutcomeV1(outcome, collections.VectorPartitionManifestV1{}, nil, 1, 10)
			if attempt.Class != "invalid_response" || !attempt.PartialResponse || len(results) != 0 {
				t.Fatal("malformed successful payload was accepted")
			}
		})
	}
}

func TestM8CompleteMeasuredQualityDoesNotInventFailureTraversal(t *testing.T) {
	h, queries, truth, homes, members := qualityFixtureV1(t, 0)
	oracles, err := h.membershipOraclesV1(truth, homes, members, 4)
	if err != nil {
		t.Fatal(err)
	}
	cell, err := m8BuildAttributionV1(t.Context(), h.assets, homes, members, queries, truth, oracles, 4, 32, 10, defaultRouterScoreBudgetV3, make([][]m8CanonicalResultV1, len(queries)), h)
	if err != nil {
		t.Fatal(err)
	}
	row := m8ProductionRowV1{Samples: len(queries), Probes: 4, Concurrency: 1, ElapsedNanos: 100, Accounting: &m8MeasurementAccountingV1{Contract: m8CompleteAttemptsV1, Attempts: []m8MeasuredAttemptV1{
		{Class: "success", Dispatched: true, WorkObserved: true, TerminalNanos: 20, CoordinatorNanos: 10, TruthHits: m8IDHitCountV1(m8CanonicalIDsV1(truth[0]), m8CanonicalIDsV1(cell.Local[0]))},
		{Class: "timeout", Dispatched: true, TerminalNanos: 30},
	}}}
	if err := m8SummarizeAttemptsV1(&row, 10); err != nil {
		t.Fatal(err)
	}
	if err := m8AttachAttributionV1(&row, cell, [][]m8CanonicalResultV1{cell.Local[0], nil}); err != nil {
		t.Fatal(err)
	}
	if !row.Attribution.CoordinatorMergeIDParity || row.Attribution.MeasuredSuccesses != 1 || row.Attribution.MeasuredFailures != 1 || !slices.Contains(row.Attribution.ResidualLossOwners, "measurement_failure_unattributed") || slices.Contains(row.Attribution.ResidualLossOwners, "coordinator_merge_or_transport") || row.Attribution.Quality.Queries[1].CoordinatorReturned != nil || row.Attribution.Quality.Queries[1].Actual == nil || cell.Evidence.Quality.Queries[0].CoordinatorReturned != nil {
		t.Fatalf("measured failure conflated with offline traversal: %+v", row.Attribution)
	}
	if err := m8QualityEvidenceSelectionV1(m8ProductionConfigEvidenceV1{QualityDiagnostics: true, TopK: 10, Partitions: 4}, row); err != nil {
		t.Fatal(err)
	}
}

func TestM8RepeatedWindowWorkAndReceiptAdmission(t *testing.T) {
	cfg := config{partitions: 4, overlaps: []float64{0}, probes: []int{1, 4}, efSearch: []int{64}, concurrency: []int{1, 32}, topK: 10, m8MeasuredRepetitions: 1, m8MaxExactTruthVisits: math.MaxInt64}
	fixture := fixtureManifest{Vectors: 100, Queries: 10, Dimensions: 8}
	one, err := validateM8BenchmarkWork(cfg, fixture, math.MaxInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	cfg.m8MeasuredRepetitions = 5
	five, err := validateM8BenchmarkWork(cfg, fixture, math.MaxInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if five.MeasuredQueryRequests != 5*one.MeasuredQueryRequests || five.RetainedCoordinatorBytes != 5*one.RetainedCoordinatorBytes || five.MeasurementReceiptBytes != 5*one.MeasurementReceiptBytes || five.AttributionQueryPasses != one.AttributionQueryPasses || five.AttributionDiagnosticWorkUnits <= one.AttributionDiagnosticWorkUnits || five.ModeledPeakBytes <= one.ModeledPeakBytes {
		t.Fatalf("repeated requests/retention not charged or attribution repeated: one=%+v five=%+v", one, five)
	}
	if _, err := validateM8BenchmarkWork(cfg, fixture, math.MaxInt64, one.ModeledPeakBytes); err == nil {
		t.Fatal("repeated-window memory cap ignored")
	}
	fixture.Queries = 100_000
	if _, err := validateM8BenchmarkWork(cfg, fixture, math.MaxInt64, math.MaxInt64); err == nil || !strings.Contains(err.Error(), "receipt bound") {
		t.Fatalf("oversized receipt not rejected before collection: %v", err)
	}
}

func TestM8RepeatedWindowsCannotEnterHistoricalCampaign(t *testing.T) {
	fixture := m8QualificationFixturesV1[0]
	cfg := m8ProductionConfigEvidenceV1{RaftGroups: 4, RaftNodesPerGroup: 3, Partitions: 16, TopK: 10, RecallTarget: .90, RouterScoreBudget: m8QualificationRouterCandidatesV1, LocalScoreBudget: nativewire.DefaultVectorPartitionCoordinatorLimitsV1().MaxLocalScoreCalls, MaxExactTruthVisits: m8QualificationExactTruthCapV1(fixture), Seed: fixture.Seed, Probes: []int{1, 2, 4, 8, 16}, Concurrency: []int{1}, EfSearch: []int{128}, Overlap: []float64{.2}}
	for _, repetitions := range []int{0, 1, 2, 5} {
		cfg.MeasuredRepetitions = repetitions
		if got := m8QualificationConfigV1(cfg, fixture, .2, 0); got != (repetitions <= 1) {
			t.Fatalf("historical campaign admitted repetitions=%d: %t", repetitions, got)
		}
	}
}

func TestM8IncompleteMeasurementsSurviveDiagnosticFailure(t *testing.T) {
	row := m8CompleteMeasurementGoldenV1(t)
	results := make([][]m8CanonicalResultV1, row.Samples)
	durations := make([]uint64, row.Samples)
	for i, a := range row.Accounting.Attempts {
		if a.Class == "success" {
			for j := range 10 {
				results[i] = append(results[i], m8CanonicalResultV1{ID: fmt.Sprintf("doc-%06d", j)})
			}
			durations[i] = a.CoordinatorNanos
		}
	}
	report := m8ProductionReportV1{ExecutionID: strings.Repeat("a", 32), Dataset: fixtureManifest{Vectors: 100, Queries: 100}, Config: m8ProductionConfigEvidenceV1{TopK: 10, MeasurementAccounting: m8CompleteAttemptsV1}, Rows: []m8ProductionRowV1{row}}
	report.Resources.PeakRSSMeasured, report.Resources.PeakRSSBytes = true, 1 // platform-independent artifact fixture
	dir := t.TempDir()
	if err := m8WriteIncompleteMeasurementsV1(dir, report, []m8MeasuredCellV1{{rowIndex: 0, results: results, durations: durations}}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "incomplete_"+report.ExecutionID, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatal(err)
	}
	if report.Status != "incomplete_after_measurement" || report.GateLedger.Recall != "" || report.Rows[0].Accounting.Summary.Succeeded != 90 {
		t.Fatal("incomplete evidence lost or mislabelled")
	}
	if _, err := m8ReadProductionMeasurementTranscriptV1(report); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkM8CompleteAttemptReduction(b *testing.B) {
	row := m8CompleteMeasurementGoldenV1(b)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := m8SummarizeAttemptsV1(&row, 10); err != nil {
			b.Fatal(err)
		}
	}
}
