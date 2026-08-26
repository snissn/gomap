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
	if got, want := manifest.CorpusSHA256, "856df3d20b5177e0b7354aeac41b9d052e5f1075e00cec686ff823b110916ccc"; got != want {
		t.Fatalf("corpus hash=%s want %s", got, want)
	}
	if got, want := manifest.QuerySHA256, "eb4f076023e361b9a2cf18a06a5e1d69e5023c304da25d38848fc7011575288a"; got != want {
		t.Fatalf("query hash=%s want %s", got, want)
	}
	if got, want := manifest.OperationSHA256, "f2d85501ae55255784749f042892836078335a99e7603ac254bd1a88eafa9179"; got != want {
		t.Fatalf("operation hash=%s want %s", got, want)
	}
	if got, want := manifest.ExpectedStateSHA256, "e74c2b4aaea81c3ad4ee0444bb706ca936f652dfa7ee173bf52d686f3a14480f"; got != want {
		t.Fatalf("state hash=%s want %s", got, want)
	}
	timed := manifest.Operations[3].TimedPlan
	if timed == nil || timed.QueryCount != 1024 || timed.ReaderConcurrency != 4 || timed.WriterConcurrency != 1 || len(timed.Rounds) != 8 ||
		timed.Assignment != "round=ordinal/128;reader=ordinal%4;scenario=scenario_order[ordinal%8]" {
		t.Fatalf("timed repeat plan drifted: %+v", timed)
	}
	if got, want := minimaTimedExecutionDigest(minimaExpectedTimedExecution(timed)), "566493d1888714c6631a515c64ee8424dfb58614a392ddc6bf604f564ac75e6e"; got != want {
		t.Fatalf("timed execution hash=%s want %s", got, want)
	}
	for i, round := range timed.Rounds {
		if round.QueryStart != i*128 || round.QueryCount != 128 ||
			round.StartBarrier != "round_start_readers_and_writer" ||
			round.EndBarrier != "round_end_queries_and_insert_complete" {
			t.Fatalf("timed round %d drifted: %+v", i, round)
		}
	}
	for _, ordinal := range []int{4, 5} {
		plan := manifest.Operations[ordinal].ConcurrentPlan
		if plan == nil || plan.ReaderConcurrency != 4 || len(plan.ReaderAssignments) != 4 ||
			plan.StartBarrier != "reindex_start_all_readers_and_writer" ||
			plan.EndBarrier != "reindex_end_all_readers_and_mutation_complete" {
			t.Fatalf("concurrent reindex operation %d drifted: %+v", ordinal, plan)
		}
	}
	if got, want := minimaReindexExecutionDigest(minimaExpectedReindexExecution(&manifest)), "9ec2d96b41783bf9ac323f522244940b023c4d27efd759714c149e0ae4568ee0"; got != want {
		t.Fatalf("reindex execution hash=%s want %s", got, want)
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
	queries := minimaQueryMap(&manifest)
	smallFinal := []string{
		"minima/small/000016", "minima/small/000018", "minima/small/000019",
		"minima/small/000020", "minima/small/000021",
	}
	mixedFinal := []string{
		"minima/mixed_broad_narrow/replacement/000000",
		"minima/mixed_broad_narrow/replacement/000001",
		"minima/mixed_broad_narrow/replacement/000002",
		"minima/mixed_broad_narrow/replacement/000003",
		"minima/mixed_broad_narrow/replacement/000004",
	}
	if minimaDigest(queries["small"].FinalOracleIDs) != minimaDigest(smallFinal) ||
		minimaDigest(queries["mixed_broad_narrow"].FinalOracleIDs) != minimaDigest(mixedFinal) ||
		queries["mixed_broad_narrow"].InitialOracleIDs[0] != "minima/mixed_broad_narrow/010020" {
		t.Fatal("initial/final operation-derived oracles drifted")
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
	const descriptor = "ordinal-v2:id=minima/<scenario>/<ordinal:06d>;content=minima:<scenario>:<ordinal>;vector=[s,sqrt(1-s*s),0x6],s=0.9-ordinal*0.000003;oracle=cosine(float32(vector),float32([1,0x7]));defaults=other-user-%02d(ordinal%31),/other/%02d.txt(ordinal%97)"
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
		{"all_match", 0, "49c93c317d874812cff7048ea4900f3aed46ca87c0f263d112e5c90637c5d7f1"},
		{"over_limit_4097", 999, "dd794a8cfbe7cc12516c387f2ac55f409627d2ed1a6ed8e9fc6d0565a470e1d9"},
		{"over_limit_4097", 1000, "66beec1714f2f5f5e6bea1cb055d8e88e92652a12ba3719d6a3d0807d2363dda"},
		{"mixed_broad_narrow", 10020, "c55992ec81d614872e34bd00c69f3a52adf195feb8f48b08d5ad0e03a6b9bf3e"},
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
func TestMinimaOracleScoresRemainSeparatedAfterFloat32Normalization(t *testing.T) {
	manifest := buildMinimaManifest()
	for _, query := range manifest.Queries {
		for phase, scores := range map[string][]float64{
			"initial": query.InitialOracleScores,
			"final":   query.FinalOracleScores,
		} {
			for i := 1; i < len(scores); i++ {
				if gap := scores[i-1] - scores[i]; gap <= minimaScoreTolerance {
					t.Fatalf("%s/%s oracle score gap[%d]=%.9g <= tolerance %.9g", query.Scenario, phase, i, gap, minimaScoreTolerance)
				}
			}
		}
	}
}

func TestMinimaPayloadStateHashExcludesVectorsButOraclesDoNot(t *testing.T) {
	manifest := buildMinimaManifest()
	baselineState := manifest.ExpectedStateSHA256
	baselineQuery := minimaQueryMap(&manifest)["small"]

	mutated := manifest
	mutated.Operations[7].Documents[0].Vector = make([]float64, minimaDimension)
	mutated.Operations[7].Documents[0].Vector[1] = 1
	mutated.OperationSHA256 = minimaDigest(mutated.Operations)
	stateHash, err := minimaExpectedStateHash(&mutated)
	if err != nil {
		t.Fatal(err)
	}
	if stateHash != baselineState {
		t.Fatalf("payload-only state hash changed for vector mutation: %s != %s", stateHash, baselineState)
	}

	applied, err := minimaApplyOperations(&mutated)
	if err != nil {
		t.Fatal(err)
	}
	finalIDs, finalScores := minimaFinalOracleFromState(minimaScenarioMap(&mutated)["small"], applied)
	if minimaDigest(finalIDs) == minimaDigest(baselineQuery.FinalOracleIDs) && minimaDigest(finalScores) == minimaDigest(baselineQuery.FinalOracleScores) {
		t.Fatal("vector mutation escaped final retrieval oracle")
	}
	mutated.ExpectedStateSHA256 = stateHash
	if err := validateMinimaManifest(&mutated); err == nil {
		t.Fatal("strict manifest validator accepted vector-only mutation")
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
		{"missing timed reader plan", func(a *minimaArtifact) {
			a.Manifest.Operations[3].TimedPlan = nil
			a.Manifest.OperationSHA256 = minimaDigest(a.Manifest.Operations)
		}},
		{"truncated timed query plan", func(a *minimaArtifact) {
			a.Manifest.Operations[3].TimedPlan.QueryCount = 16
			a.Manifest.OperationSHA256 = minimaDigest(a.Manifest.Operations)
		}},
		{"timed barrier mismatch", func(a *minimaArtifact) {
			a.Manifest.Operations[3].TimedPlan.Rounds[0].StartBarrier = "writer_first"
			a.Manifest.OperationSHA256 = minimaDigest(a.Manifest.Operations)
		}},
		{"missing concurrent delete plan", func(a *minimaArtifact) {
			a.Manifest.Operations[4].ConcurrentPlan = nil
			a.Manifest.OperationSHA256 = minimaDigest(a.Manifest.Operations)
		}},
		{"concurrent replacement reader assignment mismatch", func(a *minimaArtifact) {
			a.Manifest.Operations[5].ConcurrentPlan.ReaderAssignments[3].Reader = 2
			a.Manifest.OperationSHA256 = minimaDigest(a.Manifest.Operations)
		}},
		{"concurrent replacement barrier mismatch", func(a *minimaArtifact) {
			a.Manifest.Operations[5].ConcurrentPlan.EndBarrier = "mutation_finished_before_readers"
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
		{"initial results use final oracle", func(a *minimaArtifact) {
			row := minimaTestRow(a, "treedb", "small")
			row.InitialActualIDs = append([]string(nil), row.ActualIDs...)
			row.InitialActualScores = append([]float64(nil), row.ActualScores...)
		}},
		{"final results use initial oracle", func(a *minimaArtifact) {
			row := minimaTestRow(a, "treedb", "small")
			row.ActualIDs = append([]string(nil), row.InitialActualIDs...)
			row.ActualScores = append([]float64(nil), row.InitialActualScores...)
		}},
		{"reopen results use initial oracle", func(a *minimaArtifact) {
			row := minimaTestRow(a, "treedb", "mixed_broad_narrow")
			row.ReopenIDs = append([]string(nil), row.InitialActualIDs...)
		}},
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
		{"one timed query reported", func(a *minimaArtifact) { minimaTestBackend(a, "treedb").Operations.TimedQueriesExecuted = 1 }},
		{"missing timed round reported", func(a *minimaArtifact) { minimaTestBackend(a, "treedb").Operations.TimedRoundsCompleted-- }},
		{"wrong timed assignment hash", func(a *minimaArtifact) { minimaTestBackend(a, "treedb").Operations.TimedExecutionSHA256 = "wrong" }},
		{"correct assertions with incomplete raw trace", func(a *minimaArtifact) {
			operations := &minimaTestBackend(a, "treedb").Operations
			operations.TimedExecutionTrace.Queries = operations.TimedExecutionTrace.Queries[:1]
		}},
		{"correct assertions with fake raw assignment", func(a *minimaArtifact) {
			minimaTestBackend(a, "treedb").Operations.TimedExecutionTrace.Queries[0].Scenario = "fake"
		}},
		{"timed reader did not overlap writer", func(a *minimaArtifact) {
			operations := &minimaTestBackend(a, "treedb").Operations
			for i := range operations.TimedExecutionTrace.Queries {
				query := &operations.TimedExecutionTrace.Queries[i]
				if query.Round == 0 && query.Reader == 3 {
					query.WriterInFlight = false
				}
			}
			operations.TimedExecutionSHA256 = minimaTimedExecutionDigest(operations.TimedExecutionTrace)
		}},
		{"correct assertions with incomplete raw reindex trace", func(a *minimaArtifact) {
			operations := &minimaTestBackend(a, "treedb").Operations
			operations.ReindexExecutionTrace.Operations[0].ReaderQueries =
				operations.ReindexExecutionTrace.Operations[0].ReaderQueries[:3]
		}},
		{"reindex reader did not overlap mutation", func(a *minimaArtifact) {
			operations := &minimaTestBackend(a, "treedb").Operations
			operations.ReindexExecutionTrace.Operations[1].ReaderQueries[2].MutationInFlight = false
			operations.ReindexExecutionSHA256 = minimaReindexExecutionDigest(operations.ReindexExecutionTrace)
		}},
		{"wrong reindex execution hash", func(a *minimaArtifact) {
			minimaTestBackend(a, "treedb").Operations.ReindexExecutionSHA256 = "wrong"
		}},
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
	timed := manifest.Operations[3].TimedPlan
	timedTrace := minimaExpectedTimedExecution(timed)
	reindexTrace := minimaExpectedReindexExecution(&manifest)
	operations := minimaOperationEvidence{
		ManifestOrdered: true, BatchInsertDuringSearch: true, ReindexDeleteReplace: true,
		ExplicitUpdateVisible: true, ExplicitDeleteVisible: true, EmptyCasesChecked: true,
		TimedQueriesExecuted: timed.QueryCount, TimedRoundsCompleted: len(timed.Rounds),
		TimedExecutionSHA256: minimaTimedExecutionDigest(timedTrace), TimedExecutionTrace: timedTrace,
		ReindexOperationsExecuted: len(reindexTrace.Operations),
		ReindexExecutionSHA256:    minimaReindexExecutionDigest(reindexTrace), ReindexExecutionTrace: reindexTrace,
	}
	backends := []minimaBackendEvidence{
		{Name: "treedb", ServerVersion: "test", ClientVersion: "test", Durability: "wal_sync", Configuration: map[string]string{"effective": "test"}, Environment: minimaTestEnvironment(), Manifest: hashes, Operations: operations, Reopen: minimaReopenEvidence{Attempted: true, CommittedParity: true, ResultManifestHash: manifest.ExpectedStateSHA256}},
		{Name: "qdrant", ServerVersion: "test", ClientVersion: "test", Durability: "wal", Configuration: map[string]string{"effective": "test"}, Environment: minimaTestEnvironment(), Manifest: hashes, Operations: operations, Reopen: minimaReopenEvidence{Attempted: true, CommittedParity: true, ResultManifestHash: manifest.ExpectedStateSHA256}},
	}
	artifact := minimaArtifact{Schema: minimaArtifactSchema, State: "pass", Passing: true, Manifest: manifest, Backends: backends, Recommendation: "ready_direct"}
	queries := minimaQueryMap(&manifest)
	for _, backend := range backends {
		for _, spec := range manifest.Corpora {
			query := queries[spec.Name]
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
				InitialOracleIDs: query.InitialOracleIDs, InitialOracleScores: query.InitialOracleScores,
				FinalOracleIDs: query.FinalOracleIDs, FinalOracleScores: query.FinalOracleScores,
				InitialActualIDs: append([]string(nil), query.InitialOracleIDs...), InitialActualScores: append([]float64(nil), query.InitialOracleScores...),
				ActualIDs: append([]string(nil), query.FinalOracleIDs...), ActualScores: append([]float64(nil), query.FinalOracleScores...),
				ReopenIDs: append([]string(nil), query.FinalOracleIDs...), ReopenParity: true,
				Recall: 1, Overlap: 1, OrderTolerance: minimaOrderTolerance, ScoreTolerance: minimaScoreTolerance,
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
