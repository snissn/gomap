package main

import (
	"strings"
	"testing"
)

func mustApplicationFixture(t *testing.T) applicationFixture {
	t.Helper()
	fixture := buildApplicationFixture()
	if err := validateApplicationFixture(&fixture); err != nil {
		t.Fatalf("build application fixture: %v", err)
	}
	return fixture
}

func TestApplicationContractRejectsOriginalHarnessDefects(t *testing.T) {
	t.Run("recall depth", func(t *testing.T) {
		if _, err := recallAtK([]string{"only-one"}, map[string]bool{"only-one": true}, 5); err == nil {
			t.Fatal("recall@5 accepted ranking depth 1")
		}
	})

	t.Run("dimension binding", func(t *testing.T) {
		good := dimensionContract{Config: 8, Corpus: 8, Queries: 8, Index: 8, Vectors: 8}
		if err := good.validate(); err != nil {
			t.Fatalf("valid dimensions: %v", err)
		}
		bad := good
		bad.Vectors = 7
		if err := bad.validate(); err == nil {
			t.Fatal("mismatched vectors accepted")
		}
	})

	t.Run("embedding timing boundary", func(t *testing.T) {
		if err := (timingBoundary{EmbeddingIncludesJudgments: true}).validate(); err == nil {
			t.Fatal("embedding timing accepted judgment derivation")
		}
	})

	t.Run("source ingestion label", func(t *testing.T) {
		if err := (ingestionBoundary{SourceRowUsesIngestSources: false, GeneratedChunkRowsLabel: "ingest_docs_per_sec"}).validate(); err == nil {
			t.Fatal("generated chunk inserts accepted as source ingestion")
		}
	})

	t.Run("filter-conditioned judgments", func(t *testing.T) {
		fixture := mustApplicationFixture(t)
		bad := fixture
		bad.Judgments = append([]applicationJudgment(nil), fixture.Judgments...)
		for i := range bad.Judgments {
			if bad.Judgments[i].Filter == filterTenantAlpha {
				bad.Judgments[i].RelevantChunks = append(bad.Judgments[i].RelevantChunks, "src-billing-beta-red-new#0")
				bad.Judgments[i].RelevantParents = append(bad.Judgments[i].RelevantParents, "src-billing-beta-red-new")
				break
			}
		}
		if err := validateApplicationFixture(&bad); err == nil || !strings.Contains(err.Error(), "tenant") {
			t.Fatalf("filter/judgment mismatch err=%v", err)
		}
	})

	t.Run("sample and repetition floor", func(t *testing.T) {
		if err := (measurementClaim{Samples: 999, Repetitions: 3, Label: "final_qps_p99"}).validate(); err == nil {
			t.Fatal("999 samples accepted for final p99/QPS")
		}
		if err := (measurementClaim{Samples: 1000, Repetitions: 2, Label: "final_qps_p99"}).validate(); err == nil {
			t.Fatal("two repetitions accepted for final p99/QPS")
		}
	})

	t.Run("base final comparability", func(t *testing.T) {
		base := comparisonIdentity{WorkDigest: "w", Projection: "score_only", QualityDigest: "q"}
		for name, candidate := range map[string]comparisonIdentity{
			"work":       {WorkDigest: "other", Projection: base.Projection, QualityDigest: base.QualityDigest},
			"projection": {WorkDigest: base.WorkDigest, Projection: "fetch_topk", QualityDigest: base.QualityDigest},
			"quality":    {WorkDigest: base.WorkDigest, Projection: base.Projection, QualityDigest: "other"},
		} {
			t.Run(name, func(t *testing.T) {
				if err := validateComparable(base, candidate); err == nil {
					t.Fatal("mismatched comparison accepted")
				}
			})
		}
	})
}

func TestApplicationContractRejectsDoctoredArtifacts(t *testing.T) {
	base := artifactGuard{
		Filter:                    filterTenantAlphaWorkspaceRed,
		CrossTenantResults:        0,
		CrossWorkspaceResults:     0,
		DocumentsFetched:          0,
		TopK:                      10,
		FullDocumentScanFallbacks: 0,
		ParentCap:                 2,
		PerParentCounts:           map[string]int{"p": 2},
		CollapseEnabled:           true,
	}
	mutations := map[string]func(*artifactGuard){
		"cross tenant":    func(g *artifactGuard) { g.CrossTenantResults = 1 },
		"cross workspace": func(g *artifactGuard) { g.CrossWorkspaceResults = 1 },
		"unbounded fetch": func(g *artifactGuard) { g.DocumentsFetched = 11 },
		"full scan":       func(g *artifactGuard) { g.FullDocumentScanFallbacks = 1 },
		"parent cap":      func(g *artifactGuard) { g.PerParentCounts["p"] = 3 },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			g := base
			g.PerParentCounts = map[string]int{"p": 2}
			mutate(&g)
			if err := g.validate(); err == nil {
				t.Fatal("doctored artifact accepted")
			}
		})
	}
}

func TestApplicationFixtureAndSemanticVectorsAreStable(t *testing.T) {
	fixture := mustApplicationFixture(t)
	if got, want := applicationFixtureDigest(&fixture), applicationFixtureExpectedDigest; got != want {
		t.Fatalf("fixture digest=%s want %s", got, want)
	}
	vectors, err := loadSemanticVectors()
	if err != nil {
		t.Fatalf("load semantic vectors: %v", err)
	}
	if got, want := vectors.Digest(), semanticVectorsExpectedDigest; got != want {
		t.Fatalf("semantic vector digest=%s want %s", got, want)
	}
	if err := validateSemanticVectors(&fixture, vectors); err != nil {
		t.Fatalf("validate semantic vectors: %v", err)
	}
	corrupt := *vectors
	corrupt.Dimensions--
	if err := validateSemanticVectors(&fixture, &corrupt); err == nil {
		t.Fatal("corrupt semantic dimensions accepted")
	}
}
