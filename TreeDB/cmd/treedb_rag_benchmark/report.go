package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

const ragReportSchemaVersion = "treedb_rag_benchmark/v1"

type reportContext struct {
	GoVersion string `json:"go_version"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	CPUs      int    `json:"cpus"`
	Host      string `json:"host,omitempty"`
	Note      string `json:"note,omitempty"`
}

type reportFixture struct {
	CorpusVersion  string `json:"corpus_version"`
	Fingerprint    string `json:"fingerprint_sha256"`
	Docs           int    `json:"docs"`
	Chunks         int    `json:"chunks"`
	ChunksPerDoc   int    `json:"chunks_per_doc"`
	Dims           int    `json:"dims"`
	VectorM        int    `json:"vector_m"`
	VectorEfSearch int    `json:"vector_ef_search"`
	VectorMode     string `json:"vector_query_mode"`
	Queries        int    `json:"queries"`
	TopK           int    `json:"top_k"`
	CandidateLimit int    `json:"candidate_limit"`
	Reps           int    `json:"reps"`
}

type reportIngest struct {
	EmbedSeconds             float64 `json:"embed_seconds"`
	EmbeddedChunksPerSec     float64 `json:"embedded_chunks_per_sec"`
	ChunkRowInsertSeconds    float64 `json:"chunk_row_insert_seconds"`
	GeneratedChunkRowsPerSec float64 `json:"generated_chunk_rows_per_sec"`
	IndexBuildSeconds        float64 `json:"index_build_seconds"`
	StorageBytes             int64   `json:"storage_bytes"`
	StorageBytesPerD         float64 `json:"storage_bytes_per_doc"`
	SetupSeconds             float64 `json:"setup_seconds_excluded_from_query_timing"`
	SearchSeconds            float64 `json:"search_phase_seconds"`
}

type counterValidation struct {
	Row      string   `json:"row"`
	Check    string   `json:"check"`
	OK       bool     `json:"ok"`
	Detail   string   `json:"detail,omitempty"`
	Failures []string `json:"failures,omitempty"`
}

// ragBenchmarkReport is the versioned artifact schema. JSON is the machine
// artifact; markdown is the human-readable rendering of the same rows.
type ragBenchmarkReport struct {
	SchemaVersion      string              `json:"schema_version"`
	GeneratedAt        string              `json:"generated_at"`
	Issue              string              `json:"issue,omitempty"`
	Context            reportContext       `json:"context"`
	Fixture            reportFixture       `json:"fixture"`
	Ingest             reportIngest        `json:"ingest"`
	Rows               []reportRow         `json:"rows"`
	CounterValidations []counterValidation `json:"counter_validations"`
	Measurement        measurementContract `json:"measurement_contract"`
}

type reportRow struct {
	Route               string             `json:"route"`
	ResultMode          string             `json:"result_mode"`
	Filter              string             `json:"filter"`
	FilterSelectivityPc float64            `json:"filter_selectivity_pct"`
	TopK                int                `json:"top_k"`
	Queries             int                `json:"queries"`
	Reps                int                `json:"reps"`
	RecallAt5           float64            `json:"recall_at_5"`
	RecallAt10          float64            `json:"recall_at_10"`
	MRRAt10             float64            `json:"mrr_at_10"`
	LatencyMSMean       float64            `json:"latency_ms_mean"`
	LatencyMSP50        float64            `json:"latency_ms_p50"`
	LatencyMSP99        float64            `json:"latency_ms_p99"`
	Counters            map[string]float64 `json:"counters"`
}

type measurementContract struct {
	QueryTimingBoundary string `json:"query_timing_boundary"`
	EmbedReporting      string `json:"embed_reporting"`
	IngestBoundary      string `json:"ingest_boundary"`
}

func defaultMeasurementContract() measurementContract {
	return measurementContract{
		QueryTimingBoundary: "per-query wall time around SearchHybrid only; fixture build, chunk-row projection, index build, checkpoint, and warmup queries are excluded",
		EmbedReporting:      "embedding runs at fixture build; judgment derivation starts only after the embedding timer stops",
		IngestBoundary:      "generated_chunk_rows_per_sec covers InsertBatch + Flush of pre-generated rows and is not source ingestion",
	}
}

// validateCounterContract enforces the hybrid-search fail-closed contract on
// the measured rows before any report can be written:
//
//   - score_only rows must fetch zero documents (documents_fetched == 0) and
//     must show zero full-document-scan fallbacks;
//   - fetch_topk rows must stay bounded by TopK per query on average;
//   - no row may report fail_closed or full_document_scan_fallbacks.
//
// Any violation fails report generation.
func validateCounterContract(rows []rowResult, topK int) []counterValidation {
	validations := make([]counterValidation, 0, len(rows)*2)
	for _, row := range rows {
		label := fmt.Sprintf("%s/%s/%s", row.Route, row.ResultMode, row.Filter)
		fetches := row.Counters["documents_fetched"]
		if row.ResultMode == "score_only" {
			ok := fetches == 0
			detail := fmt.Sprintf("documents_fetched/search=%.4f want 0", fetches)
			v := counterValidation{Row: label, Check: "score_only_zero_doc_fetch", OK: ok, Detail: detail}
			if !ok {
				v.Failures = []string{detail}
			}
			validations = append(validations, v)
		} else {
			bounded := fetches <= float64(topK)+1e-9
			detail := fmt.Sprintf("documents_fetched/search=%.4f want <=%d", fetches, topK)
			v := counterValidation{Row: label, Check: "fetch_topk_bounded_by_topk", OK: bounded, Detail: detail}
			if !bounded {
				v.Failures = []string{detail}
			}
			validations = append(validations, v)
		}
		fallbacks := row.Counters["full_document_scan_fallbacks"]
		okFallbacks := fallbacks == 0
		vf := counterValidation{Row: label, Check: "zero_full_scan_fallbacks", OK: okFallbacks,
			Detail: fmt.Sprintf("full_document_scan_fallbacks/search=%.4f want 0", fallbacks)}
		if !okFallbacks {
			vf.Failures = []string{vf.Detail}
		}
		validations = append(validations, vf)
		failClosed := row.Counters["fail_closed"]
		okFC := failClosed == 0
		vfc := counterValidation{Row: label, Check: "zero_fail_closed", OK: okFC,
			Detail: fmt.Sprintf("fail_closed/search=%.4f want 0", failClosed)}
		if !okFC {
			vfc.Failures = []string{vfc.Detail}
		}
		validations = append(validations, vfc)
	}
	return validations
}

func validationsAllOK(vs []counterValidation) bool {
	for _, v := range vs {
		if !v.OK {
			return false
		}
	}
	return true
}

func validationFailures(vs []counterValidation) []string {
	var failures []string
	for _, v := range vs {
		for _, f := range v.Failures {
			failures = append(failures, fmt.Sprintf("%s: %s (%s)", v.Row, f, v.Check))
		}
	}
	return failures
}

// buildReport assembles the versioned report payload from a run.
func buildReport(out *runOutput, cfg benchConfig, issue, hostNote string) (*ragBenchmarkReport, error) {
	// Recompute the counter contract from the measured rows so a report can
	// never be emitted from stale or cached validation state.
	validations := validateCounterContract(out.Rows, cfg.TopK)
	if !validationsAllOK(validations) {
		return nil, fmt.Errorf("fail-closed counter-contract violations:\n%s", strings.Join(validationFailures(validations), "\n"))
	}
	host, _ := os.Hostname()
	report := &ragBenchmarkReport{
		SchemaVersion: ragReportSchemaVersion,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		Issue:         issue,
		Context: reportContext{
			GoVersion: runtime.Version(),
			OS:        runtime.GOOS,
			Arch:      runtime.GOARCH,
			CPUs:      runtime.NumCPU(),
			Host:      host,
			Note:      hostNote,
		},
		Fixture: reportFixture{
			CorpusVersion:  out.Corpus.CorpusVersion,
			Fingerprint:    out.Fingerprint,
			Docs:           out.Ingest.Docs,
			Chunks:         out.Ingest.Chunks,
			ChunksPerDoc:   chunksPerDoc,
			Dims:           out.Ingest.Dims,
			VectorM:        out.Ingest.VectorM,
			VectorEfSearch: out.Ingest.VectorEfSearch,
			VectorMode:     "exact (column_graph exact score plane)",
			Queries:        len(out.Corpus.Queries),
			TopK:           cfg.TopK,
			CandidateLimit: cfg.CandidateLimit,
			Reps:           cfg.Reps,
		},
		Ingest: reportIngest{
			EmbedSeconds:             out.Ingest.EmbedSeconds,
			EmbeddedChunksPerSec:     out.Ingest.EmbeddedChunksPerSec,
			ChunkRowInsertSeconds:    out.Ingest.ChunkRowInsertSeconds,
			GeneratedChunkRowsPerSec: out.Ingest.GeneratedChunkRowsPerSec,
			IndexBuildSeconds:        out.Ingest.IndexBuildSeconds,
			StorageBytes:             out.Ingest.StorageBytes,
			StorageBytesPerD:         out.Ingest.StorageBytesPerD,
			SetupSeconds:             out.SetupSeconds,
			SearchSeconds:            out.SearchSeconds,
		},
		Measurement: defaultMeasurementContract(),
	}
	for _, row := range out.Rows {
		counters := make(map[string]float64, len(row.Counters))
		for k, v := range row.Counters {
			counters[k] = v
		}
		report.Rows = append(report.Rows, reportRow{
			Route:               row.Route,
			ResultMode:          row.ResultMode,
			Filter:              row.Filter,
			FilterSelectivityPc: row.FilterSelectivityPc,
			TopK:                row.TopK,
			Queries:             row.Queries,
			Reps:                row.Reps,
			RecallAt5:           row.RecallAt5,
			RecallAt10:          row.RecallAt10,
			MRRAt10:             row.MRRAt10,
			LatencyMSMean:       row.LatencyMSMean,
			LatencyMSP50:        row.LatencyMSP50,
			LatencyMSP99:        row.LatencyMSP99,
			Counters:            counters,
		})
	}
	report.CounterValidations = validations
	return report, nil
}

// writeReport emits the JSON and markdown artifacts into outDir.
func writeReport(report *ragBenchmarkReport, outDir string) (jsonPath, mdPath string, err error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", "", err
	}
	jsonBytes, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return "", "", err
	}
	jsonBytes = append(jsonBytes, '\n')
	mdBytes := renderMarkdown(report)
	jsonPath = filepath.Join(outDir, "treedb_rag_benchmark.json")
	mdPath = filepath.Join(outDir, "treedb_rag_benchmark.md")
	if err := os.WriteFile(jsonPath, jsonBytes, 0o644); err != nil {
		return "", "", err
	}
	if err := os.WriteFile(mdPath, mdBytes, 0o644); err != nil {
		return "", "", err
	}
	return jsonPath, mdPath, nil
}

func renderMarkdown(report *ragBenchmarkReport) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "# TreeDB RAG Retrieval Benchmark (%s)\n\n", ragReportSchemaVersion)
	fmt.Fprintf(&b, "Generated at %s.\n\n", report.GeneratedAt)
	if report.Issue != "" {
		fmt.Fprintf(&b, "Issue: #%s. ", report.Issue)
	}
	fmt.Fprintf(&b, "Host: %s (%s/%s, %d CPUs, %s).\n\n", report.Context.Host, report.Context.OS, report.Context.Arch, report.Context.CPUs, report.Context.GoVersion)

	fmt.Fprintf(&b, "## Fixture\n\n")
	fmt.Fprintf(&b, "- corpus `%s` fingerprint `%s`\n", report.Fixture.CorpusVersion, report.Fixture.Fingerprint[:16]+"…")
	fmt.Fprintf(&b, "- docs=%d chunks=%d (%d/doc) dims=%d queries=%d top_k=%d candidate_limit=%d reps=%d vector_m=%d ef_search=%d vector_mode=%s\n\n",
		report.Fixture.Docs, report.Fixture.Chunks, report.Fixture.ChunksPerDoc, report.Fixture.Dims, report.Fixture.Queries,
		report.Fixture.TopK, report.Fixture.CandidateLimit, report.Fixture.Reps, report.Fixture.VectorM, report.Fixture.VectorEfSearch, report.Fixture.VectorMode)

	fmt.Fprintf(&b, "## Hashing regression setup / storage\n\n")
	fmt.Fprintf(&b, "| embed s | generated chunk rows/s | index build s | storage B | B/chunk |\n|---:|---:|---:|---:|---:|\n")
	fmt.Fprintf(&b, "| %.3f | %.0f | %.3f | %d | %.0f |\n\n",
		report.Ingest.EmbedSeconds, report.Ingest.GeneratedChunkRowsPerSec, report.Ingest.IndexBuildSeconds, report.Ingest.StorageBytes, report.Ingest.StorageBytesPerD)

	fmt.Fprintf(&b, "## Rows\n\n")
	fmt.Fprintf(&b, "| route | mode | filter | sel%% | recall@5 | recall@10 | mrr@10 | p50 ms | p99 ms | mean ms | docs_fetched | postings_scanned | candidates_fused |\n")
	b.WriteString("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	rows := append([]reportRow(nil), report.Rows...)
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Route != rows[j].Route {
			return rows[i].Route < rows[j].Route
		}
		if rows[i].ResultMode != rows[j].ResultMode {
			return rows[i].ResultMode < rows[j].ResultMode
		}
		return rows[i].Filter < rows[j].Filter
	})
	for _, r := range rows {
		fmt.Fprintf(&b, "| %s | %s | %s | %.2f | %.4f | %.4f | %.4f | %.3f | %.3f | %.3f | %.2f | %.1f | %.1f |\n",
			r.Route, r.ResultMode, r.Filter, r.FilterSelectivityPc,
			r.RecallAt5, r.RecallAt10, r.MRRAt10,
			r.LatencyMSP50, r.LatencyMSP99, r.LatencyMSMean,
			r.Counters["documents_fetched"], r.Counters["text_postings_scanned"], r.Counters["candidates_fused"])
	}

	fmt.Fprintf(&b, "\n## Measurement boundary\n\n")
	fmt.Fprintf(&b, "- Query timing: %s\n- Embedding: %s\n- Ingest: %s\n", report.Measurement.QueryTimingBoundary, report.Measurement.EmbedReporting, report.Measurement.IngestBoundary)

	fmt.Fprintf(&b, "\n## Counter validations\n\n")
	allOK := validationsAllOK(report.CounterValidations)
	status := "PASS"
	if !allOK {
		status = "FAIL"
	}
	fmt.Fprintf(&b, "%d checks: **%s**\n\n", len(report.CounterValidations), status)
	for _, v := range report.CounterValidations {
		mark := "ok"
		if !v.OK {
			mark = "FAIL"
		}
		fmt.Fprintf(&b, "- %s %s — %s: %s\n", mark, v.Check, v.Row, v.Detail)
	}
	return b.Bytes()
}
