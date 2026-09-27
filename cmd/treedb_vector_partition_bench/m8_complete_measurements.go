package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"time"

	"github.com/snissn/gomap/TreeDB/nativewire"
)

const m8CompleteAttemptsV1 = "complete_attempts_v1"

// Includes all counter keys and maximum-width uint64 values. A shape regression
// test binds this conservative admission allowance to the native counter type.
const m8AttemptJSONMaxBytesV1 = 4096

// A diagnostic/report failure must not erase completed timing windows. This
// separate artifact is explicitly incomplete and cannot satisfy strict replay.
func m8WriteIncompleteMeasurementsV1(dir string, report m8ProductionReportV1, measured []m8MeasuredCellV1) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	dir = filepath.Join(dir, "incomplete_"+report.ExecutionID)
	if err := os.Mkdir(dir, 0o755); err != nil {
		return fmt.Errorf("retain incomplete M8 measurements: %w", err)
	}
	report.Status = "incomplete_after_measurement"
	report.GateLedger = m8ProductionGateLedgerV1{}
	if peak, ok := vectorPartitionBenchmarkPeakRSS(); ok {
		report.Resources.PeakRSSBytes, report.Resources.PeakRSSMeasured = peak, true
	}
	transcript, err := m8WriteProductionMeasurementTranscriptV1(dir, report, measured)
	if err != nil {
		return err
	}
	report.MeasurementTranscript = transcript
	raw, err := json.Marshal(report)
	if err != nil {
		return err
	}
	if len(raw) > m8CompleteMeasurementMaxBytesV1 {
		return errors.New("incomplete M8 report exceeds retained byte cap (transcript preserved)")
	}
	return os.WriteFile(filepath.Join(dir, "report.json"), raw, 0o644)
}

// Alternate the complete coordinate order, not query identity or the corpus.
// Repetition is part of a cell's identity even when a required window fails.
func m8MeasurementOrderV1(cfg m8ProductionConfigEvidenceV1) []m8ProductionMeasurementCellKeyV1 {
	var base []m8ProductionMeasurementCellKeyV1
	for _, overlap := range cfg.Overlap {
		for _, probes := range cfg.Probes {
			for _, ef := range cfg.EfSearch {
				for _, concurrency := range cfg.Concurrency {
					base = append(base, m8ProductionMeasurementCellKeyV1{math.Float64bits(overlap), probes, ef, concurrency, 0})
				}
			}
		}
	}
	out := make([]m8ProductionMeasurementCellKeyV1, 0, len(base)*max(1, cfg.MeasuredRepetitions))
	for repetition := range max(1, cfg.MeasuredRepetitions) {
		for i := range base {
			index := i
			if repetition%2 != 0 {
				index = len(base) - 1 - i
			}
			cell := base[index]
			cell.repetition = repetition
			out = append(out, cell)
		}
	}
	return out
}

func m8SameMeasurementCoordinateV1(a, b m8ProductionRowV1) bool {
	return a.Overlap == b.Overlap && a.Probes == b.Probes && a.EfSearch == b.EfSearch && a.Concurrency == b.Concurrency
}

func m8MeasuredCoordinateCompleteV1(report m8ProductionReportV1, coordinate m8ProductionRowV1) bool {
	if report.Config.MeasurementAccounting == "" {
		return coordinate.Status == "pass"
	}
	count := 0
	for _, row := range report.Rows {
		if !m8SameMeasurementCoordinateV1(row, coordinate) {
			continue
		}
		if row.Status != "pass" || row.Accounting == nil || row.Accounting.Summary.Succeeded != row.Samples || row.RecallAtK < report.Config.RecallTarget {
			return false
		}
		count++
	}
	return count == max(1, report.Config.MeasuredRepetitions)
}

func m8RepeatedPairGateV1(report m8ProductionReportV1, candidate, base m8ProductionRowV1, qps, tail bool) bool {
	passes := func(a, b m8ProductionRowV1) bool {
		return (!qps || a.QPS >= b.QPS*1.15) && (!tail || a.P95Nanos <= b.P95Nanos)
	}
	if report.Config.MeasurementAccounting == "" {
		return passes(candidate, base)
	}
	if !m8MeasuredCoordinateCompleteV1(report, candidate) || !m8MeasuredCoordinateCompleteV1(report, base) {
		return false
	}
	for _, row := range report.Rows {
		if !m8SameMeasurementCoordinateV1(row, candidate) {
			continue
		}
		found := false
		for _, reference := range report.Rows {
			if m8SameMeasurementCoordinateV1(reference, base) && reference.Repetition == row.Repetition {
				if !passes(row, reference) {
					return false
				}
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// Query identity is the slice index, including requests never dispatched. These
// are observations, not promises that an intermittent failure can be replayed.
type m8MeasuredAttemptV1 struct {
	Class            string                                          `json:"class"`
	Dispatched       bool                                            `json:"dispatched"`
	TerminalNanos    uint64                                          `json:"terminal_nanos"`
	CoordinatorNanos uint64                                          `json:"coordinator_nanos"`
	WorkObserved     bool                                            `json:"work_observed"`
	Counters         nativewire.VectorPartitionCoordinatorCountersV1 `json:"counters"`
	PartialResponse  bool                                            `json:"partial_response"`
	ReturnedResults  int                                             `json:"returned_results"`
	TruthHits        int                                             `json:"truth_hits"`
}

// All rates share the cell's elapsed window; successful-request latency and
// recall are explicitly conditional. ServiceRecall charges failures zero hits.
type m8MeasurementSummaryV1 struct {
	Declared       int     `json:"declared"`
	Dispatched     int     `json:"dispatched"`
	Succeeded      int     `json:"succeeded"`
	Refused        int     `json:"refused"`
	TimedOut       int     `json:"timed_out"`
	Canceled       int     `json:"canceled"`
	Errors         int     `json:"errors"`
	Invalid        int     `json:"invalid_responses"`
	NotDispatched  int     `json:"not_dispatched"`
	TruthHits      int     `json:"truth_hits"`
	TruthSlots     int     `json:"truth_slots"`
	SuccessRecall  float64 `json:"successful_recall_at_k"`
	ServiceRecall  float64 `json:"service_recall_at_k"`
	CompletionRate float64 `json:"completion_rate"`
	AttemptRate    float64 `json:"dispatched_per_second"`
	Goodput        float64 `json:"successful_per_second"`
}

type m8MeasurementAccountingV1 struct {
	Contract string                 `json:"contract"`
	Summary  m8MeasurementSummaryV1 `json:"summary"`
	Attempts []m8MeasuredAttemptV1  `json:"attempts"`
}

// Reduce the complete population once. The same pure reducer validates retained
// counts, failure work, rates and timings; truth hits are checked against IDs by
// retained replay, not trusted simply because this arithmetic agrees.
func m8SummarizeAttemptsV1(row *m8ProductionRowV1, topK int) error {
	if row == nil || row.Accounting == nil || row.Accounting.Contract != m8CompleteAttemptsV1 || row.Samples < 1 || row.Concurrency < 1 || topK < 1 || row.ElapsedNanos == 0 || len(row.Accounting.Attempts) != row.Samples || row.Samples > math.MaxInt/topK {
		return errors.New("invalid complete M8 measurement population")
	}
	summary := m8MeasurementSummaryV1{Declared: row.Samples, TruthSlots: row.Samples * topK}
	var work m8ProductionRowV1
	durations := make([]uint64, 0, row.Samples)
	terminal := make([]uint64, 0, row.Samples)
	noPartial := true
	for _, attempt := range row.Accounting.Attempts {
		if attempt.ReturnedResults < 0 || attempt.ReturnedResults > topK || attempt.TruthHits < 0 || attempt.TruthHits > attempt.ReturnedResults || attempt.CoordinatorNanos > attempt.TerminalNanos {
			return errors.New("invalid M8 attempt truth or timing")
		}
		if !attempt.Dispatched {
			if attempt.Class != "canceled" && attempt.Class != "timeout" || attempt.TerminalNanos != 0 || attempt.WorkObserved || attempt.PartialResponse {
				return errors.New("nondispatched M8 attempt claims execution")
			}
			summary.NotDispatched++
		} else {
			if attempt.TerminalNanos == 0 || attempt.TerminalNanos > row.ElapsedNanos {
				return errors.New("dispatched M8 terminal time is outside its cell window")
			}
			summary.Dispatched++
			terminal = append(terminal, attempt.TerminalNanos)
		}
		if !attempt.WorkObserved && (attempt.Counters != (nativewire.VectorPartitionCoordinatorCountersV1{}) || attempt.CoordinatorNanos != 0) {
			return errors.New("unobserved M8 attempt claims coordinator work")
		}
		switch attempt.Class {
		case "success":
			if !attempt.Dispatched || !attempt.WorkObserved || attempt.CoordinatorNanos == 0 || attempt.PartialResponse {
				return errors.New("invalid successful M8 attempt")
			}
			summary.Succeeded++
			summary.TruthHits += attempt.TruthHits
			durations = append(durations, attempt.CoordinatorNanos)
		case m8ProductionCandidateCoverageShortfallV1, m8ProductionRouterScoreBudgetExhaustedV1:
			summary.Refused++
		case "timeout":
			summary.TimedOut++
		case "canceled":
			summary.Canceled++
		case "error":
			summary.Errors++
		case "invalid_response":
			summary.Invalid++
		default:
			return errors.New("missing or unknown M8 attempt terminal class")
		}
		if attempt.Class != "success" && (attempt.ReturnedResults != 0 || attempt.TruthHits != 0) {
			return errors.New("failed M8 attempt claims successful results")
		}
		for _, pair := range [][2]uint64{
			{work.RequestBytes, attempt.Counters.RequestBytes}, {work.ResponseBytes, attempt.Counters.ResponseBytes},
			{work.CandidateBytes, attempt.Counters.CandidateBytes}, {work.RPCs, attempt.Counters.RPCs},
			{work.RouterScoreCalls, attempt.Counters.RouterScoreCalls}, {work.LocalScoreCalls, attempt.Counters.LocalScoreCalls},
			{work.RouterVisited, attempt.Counters.RouterCandidates}, {work.RouterEdges, attempt.Counters.RouterEdges},
		} {
			if pair[0] > math.MaxUint64-pair[1] {
				return errors.New("M8 attempt counter aggregate overflows")
			}
		}
		m8AccumulateProductionRowCountersV1(&work, attempt.Counters)
		work.MaxTotalNanos = max(work.MaxTotalNanos, attempt.CoordinatorNanos)
		noPartial = noPartial && !attempt.PartialResponse
	}
	if len(terminal) > 0 {
		minimum, ok := m8TotalNanosElapsedLowerBoundV1(terminal, row.Concurrency)
		if !ok || row.ElapsedNanos < minimum {
			return errors.New("M8 terminal timings exceed measured cell window")
		}
	}
	if summary.Succeeded > 0 {
		summary.SuccessRecall = float64(summary.TruthHits) / float64(summary.Succeeded*topK)
	}
	summary.ServiceRecall = float64(summary.TruthHits) / float64(summary.TruthSlots)
	if summary.Dispatched > 0 {
		summary.CompletionRate = float64(summary.Succeeded) / float64(summary.Dispatched)
	}
	summary.AttemptRate = float64(summary.Dispatched) * float64(time.Second) / float64(row.ElapsedNanos)
	summary.Goodput = float64(summary.Succeeded) * float64(time.Second) / float64(row.ElapsedNanos)
	row.Accounting.Summary = summary
	// Reset aggregate work before copying the independently reduced counters.
	row.RequestBytes, row.RouterScoreCalls, row.LocalScoreCalls = work.RequestBytes, work.RouterScoreCalls, work.LocalScoreCalls
	row.RouterVisited, row.RouterEdges, row.ResponseBytes, row.CandidateBytes, row.RPCs = work.RouterVisited, work.RouterEdges, work.ResponseBytes, work.CandidateBytes, work.RPCs
	row.MaxRequests, row.MaxRPCs, row.MaxRetries, row.MaxRedirects = work.MaxRequests, work.MaxRPCs, work.MaxRetries, work.MaxRedirects
	row.MaxRequestBytes, row.MaxResponseBytes, row.MaxCandidateBytes, row.MaxMergeEntries = work.MaxRequestBytes, work.MaxResponseBytes, work.MaxCandidateBytes, work.MaxMergeEntries
	row.MaxShardPartitions, row.MaxShardRequestBytes, row.MaxShardResponseBytes, row.MaxShardCandidateBytes = work.MaxShardPartitions, work.MaxShardRequestBytes, work.MaxShardResponseBytes, work.MaxShardCandidateBytes
	row.MaxLocalScoreCalls, row.MaxTotalNanos = work.MaxLocalScoreCalls, work.MaxTotalNanos
	row.NoPartialResults = noPartial
	row.RecallAtK, row.QPS = summary.ServiceRecall, summary.Goodput
	// This buffer is owned by the reducer: sort once without percentile copies.
	slices.Sort(durations)
	percentile := func(p uint64) uint64 {
		if index, ok := m8NearestRankPercentileIndexV1(uint64(len(durations)), p); ok {
			return durations[index]
		}
		return 0
	}
	row.P50Nanos, row.P95Nanos, row.P99Nanos = percentile(50), percentile(95), percentile(99)
	row.Status = "pass"
	if summary.Succeeded != summary.Declared {
		row.Status = "measurement_failure"
	}
	return nil
}

func m8ValidateAttemptAccountingV1(row m8ProductionRowV1, topK int) error {
	if row.Accounting == nil {
		return errors.New("missing complete M8 attempt accounting")
	}
	want := row
	accounting := *row.Accounting
	want.Accounting = &accounting
	if err := m8SummarizeAttemptsV1(&want, topK); err != nil {
		return err
	}
	if !reflect.DeepEqual(want, row) {
		return errors.New("M8 attempt records do not reproduce measurement row")
	}
	return nil
}

func m8ValidateCompleteOutcomesV1(report m8ProductionReportV1, row m8ProductionRowV1, outcome m8ProductionRowOutcomesV1) error {
	if err := m8ValidateAttemptAccountingV1(row, report.Config.TopK); err != nil {
		return err
	}
	if len(outcome.TopKIDs) != row.Samples || len(outcome.TopKScoreBits) != row.Samples || len(outcome.TotalNanos) != row.Samples {
		return errors.New("incomplete M8 terminal outcomes")
	}
	// The result count is a retained claim from the live manifest-aware response
	// validation. Replay binds that count to the retained IDs and score bits; it
	// does not attempt to reconstruct selected-partition membership from a scalar.
	successDurations := make([]uint64, 0, row.Accounting.Summary.Succeeded)
	for i, attempt := range row.Accounting.Attempts {
		if attempt.Class != "success" {
			if len(outcome.TopKIDs[i]) != 0 || len(outcome.TopKScoreBits[i]) != 0 || outcome.TotalNanos[i] != 0 {
				return errors.New("failed M8 request claims completed results or latency")
			}
			continue
		}
		if outcome.TotalNanos[i] != attempt.CoordinatorNanos {
			return errors.New("M8 successful latency differs from terminal record")
		}
		if err := m8ValidateProductionOutcomeSampleV1(outcome.TopKIDs[i], outcome.TopKScoreBits[i], attempt.ReturnedResults, report.Dataset.Vectors); err != nil {
			return err
		}
		successDurations = append(successDurations, outcome.TotalNanos[i])
	}
	if len(outcome.ExactRepresentativeTruthHits) != 0 {
		if len(outcome.ExactRepresentativeTruthHits) != row.Samples {
			return errors.New("M8 routing population differs from declared queries")
		}
		var hits uint64
		for _, hit := range outcome.ExactRepresentativeTruthHits {
			if int(hit) > report.Config.TopK {
				return errors.New("M8 routing hits exceed top-k")
			}
			hits += uint64(hit)
		}
		if math.Abs(row.Attribution.ExactRepresentativeRecallAtK-float64(hits)/float64(row.Samples*report.Config.TopK)) > 1e-12 {
			return errors.New("M8 routing hits disagree with attribution")
		}
	}
	if len(successDurations) == 0 {
		return nil
	}
	p50, p95, p99 := m8PercentileV1(successDurations, 50), m8PercentileV1(successDurations, 95), m8PercentileV1(successDurations, 99)
	if row.P50Nanos != p50 || row.P95Nanos != p95 || row.P99Nanos != p99 {
		return errors.New("M8 measurement transcript timings do not reproduce retained percentiles")
	}
	minimumElapsed, ok := m8TotalNanosElapsedLowerBoundV1(successDurations, row.Concurrency)
	if !ok {
		return errors.New("M8 measurement transcript timing aggregate overflows")
	}
	if row.ElapsedNanos < minimumElapsed {
		return errors.New("M8 measurement transcript timings exceed retained elapsed time")
	}
	return nil
}

func m8AttachCompleteAttributionV1(row *m8ProductionRowV1, attribution m8AttributionCellV1, results [][]m8CanonicalResultV1) error {
	if len(row.Accounting.Attempts) != len(results) {
		return errors.New("M8 attribution is missing measured attempt identities")
	}
	a := &row.Attribution
	a.MeasuredSuccesses = row.Accounting.Summary.Succeeded
	a.MeasuredFailures = row.Samples - a.MeasuredSuccesses
	a.CoordinatorMergeIDParity = a.MeasuredSuccesses > 0 && a.ApproximateRouterPartitionCoverageComplete
	a.CoordinatorMergeScoreParity = a.CoordinatorMergeIDParity
	if a.Quality != nil {
		quality, err := m8QualityAttachCoordinatorV1(a.Quality, attribution.qualityTruth, results)
		if err != nil {
			return err
		}
		a.Quality = quality
	}
	for i, attempt := range row.Accounting.Attempts {
		if attempt.Class != "success" {
			if len(results[i]) != 0 {
				return errors.New("failed M8 request has coordinator attribution results")
			}
			if a.Quality != nil {
				a.Quality.Queries[i].CoordinatorReturned = nil
			}
			continue
		}
		ids, scores := m8CanonicalParityV1(attribution.Local[i], results[i])
		a.CoordinatorMergeIDParity = a.CoordinatorMergeIDParity && ids
		a.CoordinatorMergeScoreParity = a.CoordinatorMergeScoreParity && scores
	}
	// Offline routing/local searches remain labelled as offline evidence. A
	// failed measured request supplies no actual traversal or merge observation.
	a.EndToEndRecallAtK = row.RecallAtK
	a.ApproximateLocalToEndToEndLossAtK = a.ApproximateLocalHNSWRecallAtK - a.EndToEndRecallAtK
	if !a.ApproximateRouterPartitionCoverageComplete {
		// The offline population was not executed, so it cannot establish a
		// loss (or gain) against the independently measured successes.
		a.ApproximateLocalToEndToEndLossAtK = 0
	}
	a.ResidualLossOwners = m8AttributionLossOwnersV1(*a)
	a.StageOwners = m8AttributionStageOwnersV1(*a)
	return nil
}
