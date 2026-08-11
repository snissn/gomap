package main

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestLocalHNSWFinalQualificationScheduleAndGatesV1(t *testing.T) {
	schedule := localHNSWFinalQualificationScheduleV1()
	if len(schedule) != 24 || schedule[0].Variant != "m16_efc128" || schedule[2].Variant != "m18_efc256" || schedule[8].Variant != "m18_efc256" || schedule[16].Variant != "m16_efc128" {
		t.Fatalf("schedule=%+v", schedule)
	}
	if runs := localHNSWFinalQualificationRunsV1(); len(runs) != 48 || runs[0].Corpus != localHNSWFinalQualificationCorpus250KV1 || runs[24].Corpus != localHNSWFinalQualificationCorpus100KV1 {
		t.Fatalf("runs=%+v", runs)
	}
	counts := localHNSWFinalQualificationCountsV1{QueryCount: 1000, TopK: 10, P2HitSlots: 9500, P16HitSlots: 9520, RoutingMissSlots: 20}
	if !localHNSWFinalQualificationCountsPassV1(counts) {
		t.Fatalf("counts=%+v", counts)
	}
	counts.P16HitSlots++
	if localHNSWFinalQualificationCountsPassV1(counts) {
		t.Fatal("accepted excessive p2/p16 hit-slot gap")
	}
	counts.P16HitSlots--

	digest := strings.Repeat("a", 64)
	cells := make([]localHNSWFinalQualificationTimingCellV1, len(schedule))
	for i, cell := range schedule {
		cells[i] = localHNSWFinalQualificationTimingCellV1{localHNSWFinalQualificationCellV1: cell, QPS: 100, P95Nanos: 100, ResultSHA256: make([]string, 1000)}
		for j := range cells[i].ResultSHA256 {
			cells[i].ResultSHA256[j] = digest
		}
	}
	if err := localHNSWFinalQualificationTimingPassV1(cells); err != nil {
		t.Fatal(err)
	}
	cells[1].ResultSHA256[0] = strings.Repeat("b", 64)
	if err := localHNSWFinalQualificationTimingPassV1(cells); err == nil {
		t.Fatal("accepted unstable timing result")
	}
}

func TestLocalHNSWFinalQualificationChildReportDiscoveryV1(t *testing.T) {
	dir := t.TempDir()
	if _, _, _, err := localHNSWFinalQualificationChildReportV1(dir); err == nil {
		t.Fatal("accepted missing child report")
	}
	for _, name := range []string{"vector_partition_m8_a.json", "vector_partition_m8_b.json"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, _, err := localHNSWFinalQualificationChildReportV1(dir); err == nil {
		t.Fatal("accepted multiple child reports")
	}
}

func TestLocalHNSWFinalQualificationInvokeV1(t *testing.T) {
	fixture := fixtureManifest{Vectors: 10, Queries: 1000, Dimensions: 1, Seed: 1}
	truth := make([][]m8CanonicalResultV1, fixture.Queries)
	outcome := m8ProductionRowOutcomesV1{Status: "pass", Samples: fixture.Queries, TopKIDs: make([][]string, fixture.Queries), TopKScoreBits: make([][]uint32, fixture.Queries), TotalNanos: make([]uint64, fixture.Queries), ExactRepresentativeTruthHits: make([]uint8, fixture.Queries)}
	for query := range truth {
		truth[query], outcome.TopKIDs[query], outcome.TopKScoreBits[query] = make([]m8CanonicalResultV1, 10), make([]string, 10), make([]uint32, 10)
		outcome.TotalNanos[query], outcome.ExactRepresentativeTruthHits[query] = 1, 10
		for rank := range truth[query] {
			id := "doc-" + strconv.Itoa(rank)
			truth[query][rank].ID, outcome.TopKIDs[query][rank] = id, id
		}
	}
	digest := strings.Repeat("a", 64)
	inputs := map[string]localHNSWFinalQualificationCorpusInputV1{
		localHNSWFinalQualificationCorpus250KV1: {Fixture: fixture, Dataset: "/dataset-250", TruthCache: "/truth-250", TruthCacheSHA256: digest, Truth: truth},
		localHNSWFinalQualificationCorpus100KV1: {Fixture: fixture, Dataset: "/dataset-100", TruthCache: "/truth-100", TruthCacheSHA256: digest, Truth: truth},
	}
	roots := localHNSWFinalQualificationRootsV1{"a", "b", "c", "d"}
	var got []config
	call := 0
	children, err := localHNSWFinalQualificationInvokeWithDiscoveryV1(config{out: t.TempDir(), baseSHA: strings.Repeat("a", 40), headSHA: strings.Repeat("b", 40), sourceCheckout: "/source", command: []string{"/bench"}}, inputs, roots, func(cfg config, _ fixtureManifest, _, _ [][]float64, _ io.Writer) error {
		got = append(got, cfg)
		return nil
	}, func(string) (m8ProductionReportV1, m8ProductionMeasurementTranscriptV1, string, error) {
		run := localHNSWFinalQualificationRunsV1()[call]
		call++
		rowOutcome := outcome
		rowOutcome.Probes, rowOutcome.EfSearch, rowOutcome.Concurrency = run.Probes, 128, run.Concurrency
		report := m8ProductionReportV1{ExecutionID: "m8-production-test", MeasurementTranscript: m8ProductionMeasurementTranscriptEvidenceV1{SHA256: digest}, Variant: &m3VariantDescriptorV1{PartitionHNSWM: map[string]int{localHNSWFinalQualificationBaselineV1: 16, localHNSWFinalQualificationCandidateV1: 18}[run.Variant], IndexDefinitionDigest: digest, Source: vectorpartition.Source{Checksum: digest}}, Rows: []m8ProductionRowV1{{Probes: run.Probes, EfSearch: 128, Concurrency: run.Concurrency, Samples: fixture.Queries, QPS: 100, P95Nanos: 10, Attribution: m8ProductionAttributionV1{ExactRepresentativeRecallAtK: 1}}}}
		return report, m8ProductionMeasurementTranscriptV1{ExecutionID: report.ExecutionID, Outcomes: []m8ProductionRowOutcomesV1{rowOutcome}}, digest, nil
	}, io.Discard)
	if err != nil || len(children) != 48 || len(got) != 48 || got[0].m8ExistingDB != "a" || got[1].m8ExistingDB != "b" || got[24].m8ExistingDB != "c" || got[25].m8ExistingDB != "d" || got[0].probes[0] != 2 || got[0].efSearch[0] != 128 {
		t.Fatalf("err=%v calls=%d", err, len(got))
	}
}

func TestLocalHNSWFinalQualificationChildFromTranscriptV1(t *testing.T) {
	digest := strings.Repeat("a", 64)
	truth := make([][]m8CanonicalResultV1, localHNSWFinalQueryCountV1)
	outcome := m8ProductionRowOutcomesV1{
		Probes:                       2,
		EfSearch:                     128,
		Concurrency:                  1,
		Status:                       "pass",
		Samples:                      localHNSWFinalQueryCountV1,
		TopKIDs:                      make([][]string, localHNSWFinalQueryCountV1),
		TopKScoreBits:                make([][]uint32, localHNSWFinalQueryCountV1),
		TotalNanos:                   make([]uint64, localHNSWFinalQueryCountV1),
		ExactRepresentativeTruthHits: make([]uint8, localHNSWFinalQueryCountV1),
	}
	for query := range truth {
		truth[query] = make([]m8CanonicalResultV1, localHNSWFinalTopKV1)
		outcome.TopKIDs[query] = make([]string, localHNSWFinalTopKV1)
		outcome.TopKScoreBits[query] = make([]uint32, localHNSWFinalTopKV1)
		outcome.TotalNanos[query] = 1
		outcome.ExactRepresentativeTruthHits[query] = localHNSWFinalTopKV1
		for rank := range truth[query] {
			id := "doc-" + strconv.Itoa(query) + "-" + strconv.Itoa(rank)
			truth[query][rank].ID = id
			outcome.TopKIDs[query][rank] = id
		}
	}
	expected := localHNSWFinalQualificationRunV1{Corpus: localHNSWFinalQualificationCorpus250KV1, localHNSWFinalQualificationCellV1: localHNSWFinalQualificationCellV1{Variant: localHNSWFinalQualificationBaselineV1, Probes: 2, Concurrency: 1}}
	report := m8ProductionReportV1{
		ExecutionID:           "m8-production-test",
		MeasurementTranscript: m8ProductionMeasurementTranscriptEvidenceV1{SHA256: digest},
		Variant:               &m3VariantDescriptorV1{PartitionHNSWM: 16, IndexDefinitionDigest: digest, Source: vectorpartition.Source{Checksum: digest}},
		Rows:                  []m8ProductionRowV1{{Probes: 2, EfSearch: 128, Concurrency: 1, Samples: localHNSWFinalQueryCountV1, QPS: 100, P95Nanos: 10, Attribution: m8ProductionAttributionV1{ExactRepresentativeRecallAtK: 1}}},
	}
	transcript := m8ProductionMeasurementTranscriptV1{ExecutionID: report.ExecutionID, Outcomes: []m8ProductionRowOutcomesV1{outcome}}
	started := time.Unix(1, 0).UTC()
	child, err := localHNSWFinalQualificationChildFromTranscriptV1(expected, report, transcript, digest, truth, started, started.Add(time.Second))
	if err != nil || child.Counts.P2HitSlots != 10000 || child.Counts.RoutingMissSlots != 0 || child.SourceIdentitySHA256 != digest || len(child.Timing.ResultSHA256) != localHNSWFinalQueryCountV1 {
		t.Fatalf("child=%+v err=%v", child, err)
	}

	transcript.Outcomes[0].ExactRepresentativeTruthHits = nil
	if _, err := localHNSWFinalQualificationChildFromTranscriptV1(expected, report, transcript, digest, truth, started, started.Add(time.Second)); err == nil {
		t.Fatal("accepted missing routing outcomes")
	}
	transcript.Outcomes[0].ExactRepresentativeTruthHits = outcome.ExactRepresentativeTruthHits
	transcript.Outcomes[0].ExactRepresentativeTruthHits[0] = localHNSWFinalTopKV1 + 1
	if _, err := localHNSWFinalQualificationChildFromTranscriptV1(expected, report, transcript, digest, truth, started, started.Add(time.Second)); err == nil {
		t.Fatal("accepted routing hit above top-k")
	}
	transcript.Outcomes[0].ExactRepresentativeTruthHits[0] = localHNSWFinalTopKV1 - 1
	if _, err := localHNSWFinalQualificationChildFromTranscriptV1(expected, report, transcript, digest, truth, started, started.Add(time.Second)); err == nil {
		t.Fatal("accepted routing aggregate mismatch")
	}
}

func TestLocalHNSWFinalQualificationReportV1(t *testing.T) {
	digest := strings.Repeat("a", 64)
	children := make([]localHNSWFinalQualificationChildV1, 0, 48)
	start := time.Unix(1, 0).UTC()
	counts := localHNSWFinalQualificationCountsV1{QueryCount: 1000, TopK: 10, P2HitSlots: 9500, P16HitSlots: 9520, RoutingMissSlots: 20}
	for i, run := range localHNSWFinalQualificationRunsV1() {
		variantDigest := strings.Repeat("b", 64)
		if run.Variant == localHNSWFinalQualificationCandidateV1 {
			variantDigest = strings.Repeat("c", 64)
		}
		result := make([]string, 1000)
		for j := range result {
			result[j] = digest
		}
		childStart := start.Add(time.Duration(i) * time.Second)
		childCounts := counts
		if run.Corpus == localHNSWFinalQualificationCorpus250KV1 {
			if run.Probes == 2 {
				childCounts.P2HitSlots = 9500
				childCounts.P16HitSlots = 0
			} else {
				childCounts.P2HitSlots = 0
				childCounts.P16HitSlots = 9520
			}
		}
		children = append(children, localHNSWFinalQualificationChildV1{
			localHNSWFinalQualificationRunV1: run,
			ReportSHA256:                     digest,
			TranscriptSHA256:                 digest,
			SourceIdentitySHA256:             digest,
			VariantIdentitySHA256:            variantDigest,
			M:                                map[string]int{localHNSWFinalQualificationBaselineV1: 16, localHNSWFinalQualificationCandidateV1: 18}[run.Variant],
			EfConstruction:                   map[string]int{localHNSWFinalQualificationBaselineV1: 128, localHNSWFinalQualificationCandidateV1: 256}[run.Variant],
			StartedAt:                        childStart,
			EndedAt:                          childStart.Add(time.Millisecond),
			Counts:                           childCounts,
			Timing:                           localHNSWFinalQualificationTimingCellV1{localHNSWFinalQualificationCellV1: run.localHNSWFinalQualificationCellV1, QPS: 100, P95Nanos: 100, ResultSHA256: result},
		})
	}
	report := localHNSWFinalQualificationReportV1{Schema: localHNSWFinalQualificationSchemaV1, ResultKind: "local_hnsw_final_qualification_v1", Status: "valid", Children: children}
	if err := localHNSWFinalQualificationReportValidV1(report); err != nil {
		t.Fatal(err)
	}
	report.Children[1].SourceIdentitySHA256 = strings.Repeat("c", 64)
	if err := localHNSWFinalQualificationReportValidV1(report); err == nil {
		t.Fatal("accepted source identity drift")
	}
	report.Children[1].SourceIdentitySHA256 = digest
	report.Children = report.Children[:47]
	if err := localHNSWFinalQualificationReportValidV1(report); err == nil {
		t.Fatal("accepted missing child run")
	}
}

func TestLocalHNSWFinalQualificationUnionV1(t *testing.T) {
	fixture := fixtureManifest{Queries: 1000, Checksum: strings.Repeat("a", 64)}
	truth := strings.Repeat("b", 64)
	calibration := localHNSWQuerySplitV1{Schema: "vector_partition_4105_query_split_v1", DatasetChecksum: fixture.Checksum, TruthArtifactSHA256: truth, Selection: localHNSWQuerySplitSelectionV1}
	holdout := calibration
	for ordinal := range fixture.Queries {
		if localHNSWCalibrationOrdinalV1(ordinal) {
			calibration.Ordinals = append(calibration.Ordinals, ordinal)
		} else {
			holdout.Ordinals = append(holdout.Ordinals, ordinal)
		}
	}
	union, err := localHNSWFinalQualificationUnionV1(calibration, holdout, fixture, truth)
	if err != nil || len(union) != 1000 || union[0] != 0 || union[999] != 999 {
		t.Fatalf("union=%v err=%v", len(union), err)
	}
	holdout.Ordinals = holdout.Ordinals[:len(holdout.Ordinals)-1]
	if _, err := localHNSWFinalQualificationUnionV1(calibration, holdout, fixture, truth); err == nil {
		t.Fatal("accepted incomplete frozen union")
	}
}
