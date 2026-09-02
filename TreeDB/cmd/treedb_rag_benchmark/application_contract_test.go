package main

import (
	"reflect"
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

func TestApplicationEvidenceIntegrityContracts(t *testing.T) {
	t.Run("cold reopen and index parity fail closed", func(t *testing.T) {
		good := lifecycleEvidence{
			ColdReopenParity: true, TextIndexParity: true,
			VectorIndexParity: true, ScalarIndexParity: true, QueryCollectionReopened: true,
		}
		for name, mutate := range map[string]func(*lifecycleEvidence){
			"cold reopen":      func(e *lifecycleEvidence) { e.ColdReopenParity = false },
			"query collection": func(e *lifecycleEvidence) { e.QueryCollectionReopened = false },
			"text index":       func(e *lifecycleEvidence) { e.TextIndexParity = false },
			"vector index":     func(e *lifecycleEvidence) { e.VectorIndexParity = false },
			"scalar index":     func(e *lifecycleEvidence) { e.ScalarIndexParity = false },
		} {
			t.Run(name, func(t *testing.T) {
				bad := good
				mutate(&bad)
				if err := validateLifecycleEvidence("fixture", bad); err == nil {
					t.Fatal("non-parity lifecycle accepted")
				}
			})
		}
	})

	t.Run("queried index results must match across reopen and fixture", func(t *testing.T) {
		want := applicationIndexQuerySnapshot{
			TextChildIDs:      []string{"source#0", "source#1"},
			VectorChildIDs:    []string{"source#0", "source#1"},
			ScalarDocumentIDs: []string{"source", "source#0", "source#1"},
		}
		if err := validateApplicationIndexQueryParity(want, want, want); err != nil {
			t.Fatalf("valid parity: %v", err)
		}
		for name, mutate := range map[string]func(*applicationIndexQuerySnapshot){
			"text stale":     func(s *applicationIndexQuerySnapshot) { s.TextChildIDs = append(s.TextChildIDs, "deleted#0") },
			"vector missing": func(s *applicationIndexQuerySnapshot) { s.VectorChildIDs = s.VectorChildIDs[:1] },
			"scalar corrupt": func(s *applicationIndexQuerySnapshot) { s.ScalarDocumentIDs = []string{"other"} },
		} {
			t.Run(name, func(t *testing.T) {
				after := want
				after.TextChildIDs = append([]string(nil), want.TextChildIDs...)
				after.VectorChildIDs = append([]string(nil), want.VectorChildIDs...)
				after.ScalarDocumentIDs = append([]string(nil), want.ScalarDocumentIDs...)
				mutate(&after)
				if err := validateApplicationIndexQueryParity(want, after, want); err == nil {
					t.Fatal("bad queried index parity accepted")
				}
			})
		}
	})

	t.Run("build revision is exact and clean", func(t *testing.T) {
		const revision = "0123456789abcdef0123456789abcdef01234567"
		cfg := defaultApplicationConfig()
		cfg.HarnessRevision = revision
		settings := map[string]string{"vcs.revision": revision, "vcs.modified": "false"}
		if got, err := resolveApplicationHarnessRevision(cfg, settings, true); err != nil || got != revision {
			t.Fatalf("clean revision got=%q err=%v", got, err)
		}
		for name, mutate := range map[string]func(*applicationConfig, map[string]string, *bool){
			"dirty":   func(_ *applicationConfig, s map[string]string, _ *bool) { s["vcs.modified"] = "true" },
			"unbound": func(_ *applicationConfig, _ map[string]string, ok *bool) { *ok = false },
			"mismatch": func(c *applicationConfig, _ map[string]string, _ *bool) {
				c.HarnessRevision = "1123456789abcdef0123456789abcdef01234567"
			},
		} {
			t.Run(name, func(t *testing.T) {
				badCfg := cfg
				badSettings := map[string]string{"vcs.revision": revision, "vcs.modified": "false"}
				ok := true
				mutate(&badCfg, badSettings, &ok)
				if _, err := resolveApplicationHarnessRevision(badCfg, badSettings, ok); err == nil {
					t.Fatal("invalid final binary binding accepted")
				}
			})
		}
	})

	t.Run("linker revision is used only when debug VCS settings are absent", func(t *testing.T) {
		const revision = "0123456789abcdef0123456789abcdef01234567"
		cfg := defaultApplicationConfig()
		cfg.HarnessRevision = revision
		stamped, ok := effectiveApplicationBuildInfo(map[string]string{"CGO_ENABLED": "1"}, true, revision, "false")
		if got, err := resolveApplicationHarnessRevision(cfg, stamped, ok); err != nil || got != revision {
			t.Fatalf("clean stamped revision got=%q err=%v", got, err)
		}

		actual := "1123456789abcdef0123456789abcdef01234567"
		debugSettings := map[string]string{"vcs.revision": actual, "vcs.modified": "false"}
		effective, ok := effectiveApplicationBuildInfo(debugSettings, true, revision, "false")
		if effective["vcs.revision"] != actual || !ok {
			t.Fatalf("linker stamp overrode debug settings: %+v ok=%v", effective, ok)
		}

		for name, settings := range map[string]map[string]string{
			"unstamped":              {},
			"partial debug settings": {"vcs.revision": revision},
		} {
			t.Run(name, func(t *testing.T) {
				effective, buildInfoOK := effectiveApplicationBuildInfo(settings, true, "", "")
				if _, err := resolveApplicationHarnessRevision(cfg, effective, buildInfoOK); err == nil {
					t.Fatal("unstamped or partial final binary binding accepted")
				}
			})
		}

		dirty, ok := effectiveApplicationBuildInfo(nil, false, revision, "true")
		if _, err := resolveApplicationHarnessRevision(cfg, dirty, ok); err == nil {
			t.Fatal("dirty linker stamp accepted")
		}
	})

	t.Run("config digest contains workload only", func(t *testing.T) {
		base := defaultApplicationConfig()
		base.Dir = "/tmp/base"
		base.ProductBaseSHA = strings.Repeat("a", 40)
		base.HarnessRevision = strings.Repeat("b", 40)
		base.HostNote = "host a"
		base.Command = []string{"base", "--flag"}
		other := base
		other.Dir = "/different/path"
		other.KeepDir = !base.KeepDir
		other.ProductBaseSHA = strings.Repeat("c", 40)
		other.HarnessRevision = strings.Repeat("d", 40)
		other.HostNote = "host b"
		other.Command = []string{"candidate"}
		if got, want := applicationConfigDigest(other), applicationConfigDigest(base); got != want {
			t.Fatalf("provenance/path changed workload digest %s != %s", got, want)
		}
		other.TopK++
		if applicationConfigDigest(other) == applicationConfigDigest(base) {
			t.Fatal("workload mismatch produced identical digest")
		}
	})

	t.Run("warmup rotates through full query set", func(t *testing.T) {
		fixture := mustApplicationFixture(t)
		var got []string
		for i := range 5 {
			got = append(got, applicationWarmupQuery(&fixture, i).ID)
		}
		want := []string{"q-billing", "q-outage", "q-access", "q-billing", "q-outage"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("warmup order=%v want %v", got, want)
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
