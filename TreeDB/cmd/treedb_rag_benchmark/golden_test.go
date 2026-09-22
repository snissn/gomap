package main

import (
	"testing"
)

// TestGoldenFixtureStability proves the committed fixture builder is
// byte-stable across independent runs: identical corpus content, embeddings,
// query set, and ground truth.
func TestGoldenFixtureStability(t *testing.T) {
	const docs, dims = 32, ragFixtureDims
	first, _, err := buildRagCorpus(docs, dims)
	if err != nil {
		t.Fatalf("build corpus (run 1): %v", err)
	}
	second, _, err := buildRagCorpus(docs, dims)
	if err != nil {
		t.Fatalf("build corpus (run 2): %v", err)
	}
	fp1 := corpusFingerprint(first)
	fp2 := corpusFingerprint(second)
	if fp1 != fp2 {
		t.Fatalf("corpus fingerprint differs across runs: %s vs %s", fp1, fp2)
	}
	if fp1 == "" {
		t.Fatal("empty corpus fingerprint")
	}

	// Structural invariants of the committed artifact.
	if first.Docs != docs || second.Docs != docs {
		t.Fatalf("docs=%d/%d want %d", first.Docs, second.Docs, docs)
	}
	if len(first.Chunks) != docs*chunksPerDoc {
		t.Fatalf("chunks=%d want %d", len(first.Chunks), docs*chunksPerDoc)
	}
	for i, ch := range first.Chunks {
		wantID := first.Chunks[i].ID
		if ch.ID != wantID {
			t.Fatalf("chunk order unstable at %d", i)
		}
		if ch.ParentDoc == "" || ch.Topic == "" {
			t.Fatalf("chunk %s missing parent/topic linkage", ch.ID)
		}
		if len(ch.Embedding) != dims {
			t.Fatalf("chunk %s embedding dim=%d want %d", ch.ID, len(ch.Embedding), dims)
		}
		var norm float64
		for _, f := range ch.Embedding {
			norm += float64(f) * float64(f)
		}
		if norm < 0.99 || norm > 1.01 {
			t.Fatalf("chunk %s embedding not L2-normalized: %f", ch.ID, norm)
		}
	}
}

// TestGroundTruthDerivation checks the documented relevance rule on the tiny
// corpus and verifies fail-closed behavior for degenerate corpora.
func TestGroundTruthDerivation(t *testing.T) {
	corpus, _, err := buildRagCorpus(32, ragFixtureDims)
	if err != nil {
		t.Fatalf("build corpus: %v", err)
	}
	relevance := relevanceMap(corpus.GroundTruth)
	if len(relevance) != len(corpus.Queries) {
		t.Fatalf("ground truth queries=%d want %d", len(relevance), len(corpus.Queries))
	}
	for qid, rel := range relevance {
		if len(rel) == 0 {
			t.Fatalf("query %s has empty relevant set", qid)
		}
		// Every relevant chunk must satisfy the documented rule.
		var q ragQuery
		for _, qq := range corpus.Queries {
			if qq.ID == qid {
				q = qq
				break
			}
		}
		for _, ch := range corpus.Chunks {
			want := ragRelevant(q, ch)
			got := rel[ch.ID]
			if want != got {
				t.Fatalf("query %s chunk %s: relevance mismatch rule=%v judged=%v", qid, ch.ID, want, got)
			}
		}
	}

	if _, _, err := buildRagCorpus(0, ragFixtureDims); err == nil {
		t.Fatal("zero-doc corpus must fail closed")
	}
	if _, _, err := buildRagCorpus(4, 0); err == nil {
		t.Fatal("zero-dim embedder must fail closed")
	}
}
