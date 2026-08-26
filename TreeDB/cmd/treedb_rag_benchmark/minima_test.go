package main

import (
	"encoding/json"
	"math"
	"testing"
)

func TestMinimaFixtureIsFrozen(t *testing.T) {
	manifest := buildMinimaManifest()
	if err := validateMinimaManifest(&manifest); err != nil {
		t.Fatalf("validate Minima manifest: %v", err)
	}
	if got, want := manifest.CorpusSHA256, "8cec31c8c9f1b63dcc697e0036375bc9852ff00adc6c5656f88a94f7b7da45d8"; got != want {
		t.Fatalf("corpus hash=%s want %s", got, want)
	}
	if got, want := manifest.QuerySHA256, "ec9a00e8632dffb97d51fa9678bf3aa0e97e522ce98816b48f624762777cd387"; got != want {
		t.Fatalf("query hash=%s want %s", got, want)
	}
	if got, want := manifest.OperationSHA256, "745fec08d42ef88884155d34531fdc1367ea8599687df1534a6171fec97617aa"; got != want {
		t.Fatalf("operation hash=%s want %s", got, want)
	}
	if got, want := manifest.ExpectedStateSHA256, "1d8341ec8931db5cb552a81f29f11866a513a5f8251cd82289b9abb684ceddd5"; got != want {
		t.Fatalf("state hash=%s want %s", got, want)
	}

	scenarios := minimaScenarioMap(&manifest)
	for name, want := range map[string]struct {
		rows    int
		matches int
	}{
		"small":              {128, 16},
		"all_match":          {500000, 500000},
		"over_limit_4097":    {500000, 4097},
		"broad_10pct":        {500000, 50000},
		"sparse_over_limit":  {500000, 4097},
		"mixed_broad_narrow": {500000, 5},
		"empty_user":         {128, 0},
		"empty_file":         {128, 0},
	} {
		got := scenarios[name]
		if got.CorpusRows != want.rows || got.EligibleRows != want.matches {
			t.Fatalf("%s cardinality=%d/%d want %d/%d", name, got.EligibleRows, got.CorpusRows, want.matches, want.rows)
		}
	}

	distractor, err := minimaDocumentAt(scenarios["small"], 0)
	if err != nil {
		t.Fatal(err)
	}
	eligible, err := minimaDocumentAt(scenarios["small"], scenarios["small"].EligibleStart)
	if err != nil {
		t.Fatal(err)
	}
	if distractor.UserID == eligible.UserID || distractor.Vector[0] <= eligible.Vector[0] {
		t.Fatal("small fixture lacks a closer cross-tenant distractor")
	}
}
func TestMinimaGeneratorRowsAreFrozen(t *testing.T) {
	manifest := buildMinimaManifest()
	scenarios := minimaScenarioMap(&manifest)
	const descriptor = "ordinal-v1:id=minima/<scenario>/<ordinal:06d>;content=minima:<scenario>:<ordinal>;vector=unit(1,(ordinal+1)/1000000);defaults=other-user-%02d(ordinal%31),/other/%02d.txt(ordinal%97)"
	for _, scenario := range manifest.Corpora {
		if scenario.Generator != descriptor {
			t.Fatalf("%s generator=%q", scenario.Name, scenario.Generator)
		}
	}
	for _, test := range []struct {
		scenario string
		ordinal  int
		want     string
	}{
		{"all_match", 0, "f81213ad9e527715db1a23f266e6481c92eecab98bc347af6885199327bd301d"},
		{"over_limit_4097", 999, "8f7cd25b7d266fd68e52391bf217f20a683e28408670e0abc9ef84df4e85effc"},
		{"over_limit_4097", 1000, "f6fc74c883efb204ea705a2f71ec23ff0b2c7510943b0ba5790a5e0d195ae2c3"},
		{"mixed_broad_narrow", 10020, "acaa0537c20dcc8905c2551233aecc83d4e5fd2957577759c60b1364c6ad2bbb"},
	} {
		document, err := minimaDocumentAt(scenarios[test.scenario], test.ordinal)
		if err != nil {
			t.Fatal(err)
		}
		if got := minimaDigest(document); got != test.want {
			t.Errorf("%s/%d row hash=%s want %s", test.scenario, test.ordinal, got, test.want)
		}
	}
}

func TestMinimaContractRejectsDoctoredArtifacts(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*minimaArtifact)
	}{
		{"corpus hash mismatch", func(a *minimaArtifact) { a.Manifest.CorpusSHA256 = "bad" }},
		{"query hash mismatch", func(a *minimaArtifact) { a.Manifest.QuerySHA256 = "bad" }},
		{"operation hash mismatch", func(a *minimaArtifact) { a.Manifest.OperationSHA256 = "bad" }},
		{"post-operation state hash mismatch", func(a *minimaArtifact) { a.Manifest.ExpectedStateSHA256 = "bad" }},
		{"concrete insert range mismatch", func(a *minimaArtifact) {
			a.Manifest.Operations[1].InsertRanges[0].Rows--
			a.Manifest.OperationSHA256 = minimaDigest(a.Manifest.Operations)
		}},
		{"interleaving schedule mismatch", func(a *minimaArtifact) {
			a.Manifest.Operations[3].Schedule[0].Actor = "writer_first"
			a.Manifest.OperationSHA256 = minimaDigest(a.Manifest.Operations)
		}},
		{"replacement payload mismatch", func(a *minimaArtifact) {
			a.Manifest.Operations[5].Documents[0].Content = "doctored"
			a.Manifest.OperationSHA256 = minimaDigest(a.Manifest.Operations)
		}},
		{"update input mismatch", func(a *minimaArtifact) {
			a.Manifest.Operations[7].Documents[0].Content = "doctored"
			a.Manifest.OperationSHA256 = minimaDigest(a.Manifest.Operations)
		}},
		{"delete input mismatch", func(a *minimaArtifact) {
			a.Manifest.Operations[9].IDs[0] = "doctored"
			a.Manifest.OperationSHA256 = minimaDigest(a.Manifest.Operations)
		}},
		{"backend manifest mismatch", func(a *minimaArtifact) { minimaTestBackend(a, "qdrant").Manifest.OperationSHA256 = "bad" }},
		{"cross-user leakage", func(a *minimaArtifact) { minimaTestRow(a, "treedb", "small").Correctness.CrossUserResults = 1 }},
		{"stale insert", func(a *minimaArtifact) { minimaTestRow(a, "treedb", "small").Correctness.StaleInsertIDs = 1 }},
		{"stale update", func(a *minimaArtifact) { minimaTestRow(a, "treedb", "small").Correctness.StaleUpdateIDs = 1 }},
		{"stale delete", func(a *minimaArtifact) { minimaTestRow(a, "treedb", "small").Correctness.StaleDeleteIDs = 1 }},
		{"reported recall mismatch", func(a *minimaArtifact) { minimaTestRow(a, "treedb", "small").Recall = 0 }},
		{"reported overlap mismatch", func(a *minimaArtifact) { minimaTestRow(a, "treedb", "small").Overlap = 0 }},
		{"duplicate actual ID", func(a *minimaArtifact) {
			row := minimaTestRow(a, "treedb", "small")
			row.ActualIDs[1] = row.ActualIDs[0]
		}},
		{"ordering outside tolerance", func(a *minimaArtifact) {
			row := minimaTestRow(a, "treedb", "small")
			row.ActualIDs[0], row.ActualIDs[1] = row.ActualIDs[1], row.ActualIDs[0]
			row.ActualScores[0], row.ActualScores[1] = row.ActualScores[1], row.ActualScores[0]
		}},
		{"score outside tolerance", func(a *minimaArtifact) { minimaTestRow(a, "treedb", "small").ActualScores[0] += 0.01 }},
		{"inflated order tolerance", func(a *minimaArtifact) { minimaTestRow(a, "treedb", "small").OrderTolerance = 1 }},
		{"inflated score tolerance", func(a *minimaArtifact) { minimaTestRow(a, "treedb", "small").ScoreTolerance = 1 }},
		{"missing native route", func(a *minimaArtifact) { minimaTestRow(a, "treedb", "small").Route.Identity = "" }},
		{"wrong native route", func(a *minimaArtifact) { minimaTestRow(a, "treedb", "small").Route.Identity = "exact_fallback" }},
		{"missing fallback counter", func(a *minimaArtifact) { minimaTestRow(a, "treedb", "small").Route.FullDocumentScanFallbacks = nil }},
		{"fallback counter used", func(a *minimaArtifact) {
			minimaTestRow(a, "treedb", "small").Route.FullDocumentScanFallbacks = minimaTestInt(1)
		}},
		{"missing selectivity", func(a *minimaArtifact) { minimaTestRow(a, "treedb", "broad_10pct").Selectivity = 0 }},
		{"missing candidates", func(a *minimaArtifact) { minimaTestRow(a, "treedb", "broad_10pct").Route.CandidateIDs = nil }},
		{"over-limit missing vector membership", func(a *minimaArtifact) {
			minimaTestRow(a, "treedb", "over_limit_4097").Route.MembershipSource = "bounded_ids"
		}},
		{"over-limit cardinality-only failure", func(a *minimaArtifact) {
			minimaTestRow(a, "treedb", "over_limit_4097").Route.ScalarFilterUnbounded = minimaTestInt(1)
		}},
		{"mixed missing retained candidates", func(a *minimaArtifact) {
			minimaTestRow(a, "treedb", "mixed_broad_narrow").Route.RetainedCandidateIDs = minimaTestInt(0)
		}},
		{"mixed missing refined candidates", func(a *minimaArtifact) {
			minimaTestRow(a, "treedb", "mixed_broad_narrow").Route.RefinedCandidateIDs = minimaTestInt(0)
		}},
		{"mixed wrong plan", func(a *minimaArtifact) {
			minimaTestRow(a, "treedb", "mixed_broad_narrow").Route.Plan = "vector_aligned_ann"
		}},
		{"probe above limit", func(a *minimaArtifact) {
			minimaTestRow(a, "treedb", "over_limit_4097").Route.ProbeIDs = minimaTestInt(minimaLookupLimit + 1)
		}},
		{"collection materialization", func(a *minimaArtifact) {
			row := minimaTestRow(a, "treedb", "over_limit_4097")
			row.Route.AllowedIDMaterializationRows = minimaTestInt(row.CorpusRows)
		}},
		{"primary scan", func(a *minimaArtifact) {
			minimaTestRow(a, "treedb", "over_limit_4097").Route.PrimaryDocumentScans = minimaTestInt(1)
		}},
		{"mixed generation", func(a *minimaArtifact) { minimaTestRow(a, "treedb", "small").Visibility.GenerationConsistent = false }},
		{"missing mismatch counter", func(a *minimaArtifact) { minimaTestRow(a, "treedb", "small").Visibility.MismatchCount = nil }},
		{"missing retry counter", func(a *minimaArtifact) { minimaTestRow(a, "treedb", "small").Visibility.RetryCount = nil }},
		{"missing reopen", func(a *minimaArtifact) { minimaTestBackend(a, "treedb").Reopen.Attempted = false }},
		{"wrong nonempty reopen hash", func(a *minimaArtifact) { minimaTestBackend(a, "treedb").Reopen.ResultManifestHash = "wrong" }},
		{"backend reopen hash mismatch", func(a *minimaArtifact) { minimaTestBackend(a, "qdrant").Reopen.ResultManifestHash = "different" }},
		{"missing scenario reopen", func(a *minimaArtifact) { minimaTestRow(a, "treedb", "small").ReopenParity = false }},
		{"embedding timing contamination", func(a *minimaArtifact) { minimaTestRow(a, "treedb", "small").Timing.EmbeddingIncluded = true }},
		{"llm timing contamination", func(a *minimaArtifact) { minimaTestRow(a, "treedb", "small").Timing.LLMIncluded = true }},
		{"missing environment", func(a *minimaArtifact) { minimaTestBackend(a, "treedb").Environment = nil }},
		{"missing server version", func(a *minimaArtifact) { minimaTestBackend(a, "treedb").ServerVersion = "" }},
		{"missing client version", func(a *minimaArtifact) { minimaTestBackend(a, "treedb").ClientVersion = "" }},
		{"missing durability", func(a *minimaArtifact) { minimaTestBackend(a, "treedb").Durability = "" }},
		{"missing configuration", func(a *minimaArtifact) { minimaTestBackend(a, "treedb").Configuration = nil }},
		{"partial marked passing", func(a *minimaArtifact) { a.State = "partial"; a.Passing = true; a.Recommendation = "ready_direct" }},
	}

	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			artifact := cloneMinimaArtifact(t, validMinimaArtifact())
			test.mutate(&artifact)
			if err := validateMinimaArtifact(&artifact); err == nil {
				t.Fatal("doctored Minima artifact accepted")
			}
		})
	}
}

func validMinimaArtifact() minimaArtifact {
	manifest := buildMinimaManifest()
	hashes := minimaManifestHashes{CorpusSHA256: manifest.CorpusSHA256, QuerySHA256: manifest.QuerySHA256, OperationSHA256: manifest.OperationSHA256}
	operations := minimaOperationEvidence{
		ManifestOrdered: true, BatchInsertDuringSearch: true, ReindexDeleteReplace: true,
		ExplicitUpdateVisible: true, ExplicitDeleteVisible: true, EmptyCasesChecked: true,
	}
	backends := []minimaBackendEvidence{
		{Name: "treedb", ServerVersion: "test", ClientVersion: "test", Durability: "wal_sync", Configuration: map[string]string{"effective": "test"}, Environment: minimaTestEnvironment(), Manifest: hashes, Operations: operations, Reopen: minimaReopenEvidence{Attempted: true, CommittedParity: true, ResultManifestHash: manifest.ExpectedStateSHA256}},
		{Name: "qdrant", ServerVersion: "test", ClientVersion: "test", Durability: "wal", Configuration: map[string]string{"effective": "test"}, Environment: minimaTestEnvironment(), Manifest: hashes, Operations: operations, Reopen: minimaReopenEvidence{Attempted: true, CommittedParity: true, ResultManifestHash: manifest.ExpectedStateSHA256}},
	}
	artifact := minimaArtifact{Schema: minimaArtifactSchema, State: "pass", Passing: true, Manifest: manifest, Backends: backends, Recommendation: "ready_direct"}
	for _, backend := range backends {
		for _, spec := range manifest.Corpora {
			ids, scores := minimaOracle(spec)
			zeroMismatch, zeroRetry := 0, 0
			route := minimaRouteEvidence{
				Identity: "qdrant_filtered_hnsw", DeclaredScalarFiltering: true,
				FullDocumentScanFallbacks: minimaTestInt(0), ScalarFilterUnbounded: minimaTestInt(0),
				ProbeIDs: minimaTestInt(0), CandidateIDs: minimaTestInt(spec.EligibleRows),
				RetainedCandidateIDs: minimaTestInt(0), RefinedCandidateIDs: minimaTestInt(0),
				MembershipSource: "finite_scalar", Plan: "complete_finite_ann",
				AllowedIDMaterializationRows: minimaTestInt(0), PrimaryDocumentScans: minimaTestInt(0),
				VisitedCandidates: minimaTestInt(spec.EligibleRows), ScoredCandidates: minimaTestInt(spec.EligibleRows),
				AdmittedCandidates: minimaTestInt(spec.EligibleRows),
			}
			if backend.Name == "treedb" {
				route.Identity = "native_base_plus_live_delta"
				route.NativeBasePlusLiveDelta = true
				if spec.EligibleRows > minimaLookupLimit {
					route.MembershipSource = "vector_aligned_scalar"
					route.Plan = "vector_aligned_ann"
				}
				if spec.Name == "mixed_broad_narrow" {
					route.MembershipSource = "bounded_candidate_refinement"
					route.Plan = "mixed_refined"
					route.RetainedCandidateIDs = minimaTestInt(spec.NarrowRows)
					route.RefinedCandidateIDs = minimaTestInt(spec.EligibleRows)
				}
			}
			artifact.Scenarios = append(artifact.Scenarios, minimaScenarioEvidence{
				Backend: backend.Name, Scenario: spec.Name, CorpusRows: spec.CorpusRows, ExpectedMatches: spec.EligibleRows, Selectivity: spec.Selectivity,
				OracleIDs: ids, OracleScores: scores, ActualIDs: append([]string(nil), ids...), ActualScores: append([]float64(nil), scores...),
				ReopenIDs: append([]string(nil), ids...), ReopenParity: true,
				Recall: 1, Overlap: 1, OrderTolerance: 0, ScoreTolerance: 0.000001,
				Route: route, Visibility: minimaVisibilityEvidence{GenerationConsistent: true, MismatchCount: &zeroMismatch, RetryCount: &zeroRetry},
				Timing: minimaTimingEvidence{Captured: true}, Resource: minimaResourceEvidence{Captured: true},
			})
		}
	}
	return artifact
}

func minimaTestInt(value int) *int {
	return &value
}

func minimaTestEnvironment() map[string]string {
	return map[string]string{"os": "test", "arch": "test", "cpu": "test", "memory": "test"}
}

func minimaTestRow(artifact *minimaArtifact, backend, scenario string) *minimaScenarioEvidence {
	for i := range artifact.Scenarios {
		if artifact.Scenarios[i].Backend == backend && artifact.Scenarios[i].Scenario == scenario {
			return &artifact.Scenarios[i]
		}
	}
	panic("missing Minima test row")
}

func minimaTestBackend(artifact *minimaArtifact, name string) *minimaBackendEvidence {
	for i := range artifact.Backends {
		if artifact.Backends[i].Name == name {
			return &artifact.Backends[i]
		}
	}
	panic("missing Minima test backend")
}

func cloneMinimaArtifact(t *testing.T, artifact minimaArtifact) minimaArtifact {
	t.Helper()
	raw, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	var clone minimaArtifact
	if err := json.Unmarshal(raw, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}

func TestMinimaArtifactRejectsNonFiniteMetrics(t *testing.T) {
	artifact := validMinimaArtifact()
	minimaTestRow(&artifact, "treedb", "small").Recall = math.NaN()
	if err := validateMinimaArtifact(&artifact); err == nil {
		t.Fatal("non-finite quality metric accepted")
	}
}
