package chunking

import (
	"strconv"
	"strings"
	"testing"
)

// benchCorpus builds a synthetic RAG-style corpus of docCount documents, each
// roughly 1-2 KB of prose with paragraph structure, mirroring the C1 benchmark
// fixture scale (about 10k docs).
func benchCorpus(docCount int) []string {
	paras := []string{
		"Retrieval quality in a RAG pipeline is bounded by the granularity of the units that reach the embedder. " +
			"Oversized chunks dilute the signal per token; undersized chunks lose the local context needed to disambiguate terms.",
		"TreeDB collections store documents as opaque JSON payloads with optional text, scalar, and vector indexes attached.\n" +
			"The chunking seam sits between raw source ingestion and index maintenance: it derives child documents that are " +
			"ordinary collection rows from the index layer's point of view.",
		"Determinism matters because golden fixtures compare chunk streams across runs and across machines.\n" +
			"A chunker that consults the clock, randomness, or map iteration order cannot produce hash-stable output.",
		"Re-chunking an updated parent must tombstone stale children before inserting replacements so that lexical, " +
			"scalar, and vector indexes never resolve a mixture of old and new content for the same parent document.",
	}
	docs := make([]string, docCount)
	for i := range docs {
		var b strings.Builder
		for p := 0; p < 4; p++ {
			b.WriteString(paras[(i+p)%len(paras)])
			if p < 3 {
				b.WriteString("\n\n")
			}
		}
		docs[i] = b.String()
	}
	return docs
}

const benchDocCount = 10_000

func BenchmarkChunkFixedWindow10K(b *testing.B) {
	docs := benchCorpus(benchDocCount)
	cfg := Config{Strategy: StrategyFixedWindow, SizeUnit: SizeUnitRunes, Size: 512, Overlap: 64}
	benchmarkChunkCorpus(b, docs, cfg)
}

func BenchmarkChunkRecursive10K(b *testing.B) {
	docs := benchCorpus(benchDocCount)
	cfg := Config{Strategy: StrategyRecursive, SizeUnit: SizeUnitRunes, Size: 512, Overlap: 64, Separators: DefaultSeparators()}
	benchmarkChunkCorpus(b, docs, cfg)
}

func benchmarkChunkCorpus(b *testing.B, docs []string, cfg Config) {
	b.Helper()
	sink := 0
	b.ReportAllocs()
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		count := 0
		for i, doc := range docs {
			chunks, err := SplitChunks("bench-parent-"+strconv.Itoa(i), doc, cfg)
			if err != nil {
				b.Fatalf("SplitChunks: %v", err)
			}
			count += len(chunks)
		}
		sink += count
	}
	b.StopTimer()
	if sink == 0 {
		b.Fatal("no chunks produced")
	}
	docsPerOp := float64(len(docs)) / float64(1) // per b.N iteration over full corpus
	b.ReportMetric(docsPerOp/1.0, "corpus-docs/op")
	b.ReportMetric(float64(len(docs))*float64(b.N)/b.Elapsed().Seconds(), "docs/sec")
}
