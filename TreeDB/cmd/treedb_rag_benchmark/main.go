// Command treedb_rag_benchmark measures TreeDB RAG retrieval quality and cost
// on a committed, deterministic fixture: recall@k / MRR for text-only,
// vector-only, and hybrid rows; query p50/p99; ingest docs/sec; storage
// bytes/doc. It publishes a versioned report (treedb_rag_benchmark/v1) as JSON
// plus markdown and fails closed on counter-contract violations.
//
// See TreeDB/docs/benchmarks/treedb_rag_benchmark_runbook.md for the baseline
// commands, host context, and measurement boundary.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	var (
		docs           = flag.Int("docs", 512, "documents in the corpus (each document contributes 2 indexed chunks)")
		dims           = flag.Int("dims", ragFixtureDims, "embedding dimensions of the deterministic feature-hashing embedder")
		m              = flag.Int("m", 8, "vector graph M")
		efSearch       = flag.Int("ef-search", 128, "vector search ef parameter")
		topK           = flag.Int("top-k", 10, "final fused result count per query")
		candidateLimit = flag.Int("candidate-limit", 64, "per-source candidate budget")
		reps           = flag.Int("reps", 3, "timed repetitions over the committed query set per row")
		warmup         = flag.Int("warmup", 8, "untimed warmup queries per row before the timing loop")
		batchSize      = flag.Int("batch-size", ragDefaultBatchSize, "InsertBatch size during ingest")
		dir            = flag.String("dir", "", "persistent DB directory to build the fixture in (default: temp dir removed at exit)")
		keepDir        = flag.Bool("keep-dir", false, "keep the generated temp DB directory")
		outDir         = flag.String("out-dir", ".", "directory receiving treedb_rag_benchmark.{json,md}")
		issue          = flag.String("issue", "4267", "issue number recorded in the report")
		hostNote       = flag.String("host-note", "", "free-form host context note recorded in the report")
	)
	flag.Parse()

	cfg := benchConfig{
		Docs:           *docs,
		Dims:           *dims,
		M:              *m,
		EfSearch:       *efSearch,
		TopK:           *topK,
		CandidateLimit: *candidateLimit,
		Reps:           *reps,
		Warmup:         *warmup,
		BatchSize:      *batchSize,
		Dir:            *dir,
		KeepDir:        *keepDir,
	}

	out, err := runBenchmark(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "treedb_rag_benchmark: %v\n", err)
		os.Exit(1)
	}
	report, err := buildReport(out, cfg, strings.TrimSpace(*issue), strings.TrimSpace(*hostNote))
	if err != nil {
		fmt.Fprintf(os.Stderr, "treedb_rag_benchmark: %v\n", err)
		os.Exit(1)
	}
	jsonPath, mdPath, err := writeReport(report, *outDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "treedb_rag_benchmark: write report: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("corpus fingerprint: %s\n", out.Fingerprint)
	fmt.Printf("wrote %s\nwrote %s\n", jsonPath, mdPath)
}
