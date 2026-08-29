package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMinimaFixtureIsFrozen(t *testing.T) {
	manifest := buildMinimaManifest()
	if err := validateMinimaManifest(&manifest); err != nil {
		t.Fatalf("validate Minima manifest: %v", err)
	}
	if got, want := manifest.CorpusSHA256, "0b1a213652fc97a4460f254f4d9e90f027e4b30ef6111a26807591ade10923e1"; got != want {
		t.Fatalf("corpus hash=%s want %s", got, want)
	}
	if got, want := manifest.QuerySHA256, "eb4f076023e361b9a2cf18a06a5e1d69e5023c304da25d38848fc7011575288a"; got != want {
		t.Fatalf("query hash=%s want %s", got, want)
	}
	if got, want := manifest.OperationSHA256, "08f38acec8a5ad746dbffadef5ad9c198852c88d1920746229cb0733bfd9c434"; got != want {
		t.Fatalf("operation hash=%s want %s", got, want)
	}
	if got, want := manifest.ExpectedStateSHA256, "c2986f2b44e67b33e7bb3f92f5f92b1316e60117ed2505bef73327e0b1e5687f"; got != want {
		t.Fatalf("state hash=%s want %s", got, want)
	}
	timed := manifest.Operations[3].TimedPlan
	if timed == nil || timed.QueryCount != 1024 || timed.ReaderConcurrency != 4 || timed.WriterConcurrency != 1 || len(timed.Rounds) != 8 ||
		timed.Assignment != "round=ordinal/128;reader=ordinal%4;scenario=scenario_order[ordinal%8]" {
		t.Fatalf("timed repeat plan drifted: %+v", timed)
	}
	if got, want := minimaTimedExecutionDigest(minimaExpectedTimedExecution(&manifest)), "84b8eb10e5f86c558264d00e8cae2c6844683aff2b8bca1d76cafe6b06890ea4"; got != want {
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
	if got, want := minimaReindexExecutionDigest(minimaExpectedReindexExecution(&manifest)), "99823f1eac0fb27dce81e21e0cf5884019c6a911c410be11b675b2315cbde534"; got != want {
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
	if mustMinimaDigest(queries["small"].FinalOracleIDs) != mustMinimaDigest(smallFinal) ||
		mustMinimaDigest(queries["mixed_broad_narrow"].FinalOracleIDs) != mustMinimaDigest(mixedFinal) ||
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
func TestMinimaOraclesCoverSharedCollection(t *testing.T) {
	manifest := buildMinimaManifest()
	queries := minimaQueryMap(&manifest)
	for _, scenario := range manifest.Corpora {
		ids, scores, matches := minimaGlobalOracle(manifest.Corpora, scenario)
		query := queries[scenario.Name]
		if matches != scenario.EligibleRows ||
			mustMinimaDigest(ids) != mustMinimaDigest(query.InitialOracleIDs) ||
			mustMinimaDigest(scores) != mustMinimaDigest(query.InitialOracleScores) {
			t.Fatalf("%s global oracle rows=%d want %d", scenario.Name, matches, scenario.EligibleRows)
		}
	}
}

func TestMinimaGeneratorRowsAreFrozen(t *testing.T) {
	manifest := buildMinimaManifest()
	scenarios := minimaScenarioMap(&manifest)
	const descriptor = "ordinal-v3:id=minima/<scenario>/<ordinal:06d>;content=minima:<scenario>:<ordinal>;vector=[s,sqrt(1-s*s),0x6],s=0.9-ordinal*0.000003;oracle=cosine(float32(vector),float32([1,0x7]));defaults=<scenario>-other-user-%02d(ordinal%31),/<scenario>/other/%02d.txt(ordinal%97)"
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
		{"all_match", 0, "b913568b0ecd518b82885618db0811bde2f333397b7cc025c3e0d8fc8d9915da"},
		{"over_limit_4097", 999, "3d462831f43b17f5d5dc38c1b7a9a50be603fee60a2c3c7f371da2f6cfaa6685"},
		{"over_limit_4097", 1000, "02bf9f175ac99ae7b929d52fd5be179c24e894658b98a7be32c41ebc30cc2b3a"},
		{"mixed_broad_narrow", 10020, "c55992ec81d614872e34bd00c69f3a52adf195feb8f48b08d5ad0e03a6b9bf3e"},
	} {
		document, err := minimaDocumentAt(scenarios[test.scenario], test.ordinal)
		if err != nil {
			t.Fatal(err)
		}
		if got := mustMinimaDigest(document); got != test.want {
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
	mutated.OperationSHA256 = mustMinimaDigest(mutated.Operations)
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
	if mustMinimaDigest(finalIDs) == mustMinimaDigest(baselineQuery.FinalOracleIDs) && mustMinimaDigest(finalScores) == mustMinimaDigest(baselineQuery.FinalOracleScores) {
		t.Fatal("vector mutation escaped final retrieval oracle")
	}
	mutated.ExpectedStateSHA256 = stateHash
	if err := validateMinimaManifest(&mutated); err == nil {
		t.Fatal("strict manifest validator accepted vector-only mutation")
	}
}

func TestMinimaDigestRejectsUnsupportedJSONValues(t *testing.T) {
	if _, err := minimaDigest(math.NaN()); err == nil {
		t.Fatal("Minima digest accepted NaN")
	}
}

func TestMinimaPayloadEvidenceCacheIsManifestKeyed(t *testing.T) {
	one := minimaManifest{Corpora: []minimaScenarioSpec{{Name: "cache-one", CorpusRows: 1, Filter: "user_id"}}}
	two := minimaManifest{Corpora: []minimaScenarioSpec{{Name: "cache-two", CorpusRows: 2, Filter: "user_id"}}}
	oneHash, oneRows, err := minimaExpectedPayloadEvidence(&one)
	if err != nil {
		t.Fatal(err)
	}
	twoHash, twoRows, err := minimaExpectedPayloadEvidence(&two)
	if err != nil {
		t.Fatal(err)
	}
	if oneRows != 1 || twoRows != 2 || oneHash == twoHash {
		t.Fatalf("manifest-keyed payload evidence one=(%s,%d) two=(%s,%d)", oneHash, oneRows, twoHash, twoRows)
	}
}

func TestApplicationModeDetectsEveryMinimaFlag(t *testing.T) {
	if hasMinimaFlag("", "", "", "", "", "", "") {
		t.Fatal("empty Minima flags were reported as set")
	}
	for index := range 7 {
		values := make([]string, 7)
		values[index] = "set"
		if !hasMinimaFlag(values...) {
			t.Fatalf("Minima flag %d was ignored", index)
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
		{"cross-scenario tenant collision", func(a *minimaArtifact) {
			a.Manifest.Corpora[7].UserID = a.Manifest.Corpora[0].UserID
			a.Manifest.CorpusSHA256 = mustMinimaDigest(a.Manifest.Corpora)
		}},
		{"cross-scenario default namespace collision", func(a *minimaArtifact) {
			a.Manifest.Corpora[7].Name = a.Manifest.Corpora[0].Name
			a.Manifest.CorpusSHA256 = mustMinimaDigest(a.Manifest.Corpora)
		}},
		{"concrete insert range mismatch", func(a *minimaArtifact) {
			a.Manifest.Operations[1].InsertRanges[0].Rows--
			a.Manifest.OperationSHA256 = mustMinimaDigest(a.Manifest.Operations)
		}},
		{"missing timed reader plan", func(a *minimaArtifact) {
			a.Manifest.Operations[3].TimedPlan = nil
			a.Manifest.OperationSHA256 = mustMinimaDigest(a.Manifest.Operations)
		}},
		{"truncated timed query plan", func(a *minimaArtifact) {
			a.Manifest.Operations[3].TimedPlan.QueryCount = 16
			a.Manifest.OperationSHA256 = mustMinimaDigest(a.Manifest.Operations)
		}},
		{"timed barrier mismatch", func(a *minimaArtifact) {
			a.Manifest.Operations[3].TimedPlan.Rounds[0].StartBarrier = "writer_first"
			a.Manifest.OperationSHA256 = mustMinimaDigest(a.Manifest.Operations)
		}},
		{"missing concurrent delete plan", func(a *minimaArtifact) {
			a.Manifest.Operations[4].ConcurrentPlan = nil
			a.Manifest.OperationSHA256 = mustMinimaDigest(a.Manifest.Operations)
		}},
		{"concurrent replacement reader assignment mismatch", func(a *minimaArtifact) {
			a.Manifest.Operations[5].ConcurrentPlan.ReaderAssignments[3].Reader = 2
			a.Manifest.OperationSHA256 = mustMinimaDigest(a.Manifest.Operations)
		}},
		{"concurrent replacement barrier mismatch", func(a *minimaArtifact) {
			a.Manifest.Operations[5].ConcurrentPlan.EndBarrier = "mutation_finished_before_readers"
			a.Manifest.OperationSHA256 = mustMinimaDigest(a.Manifest.Operations)
		}},
		{"replacement payload mismatch", func(a *minimaArtifact) {
			a.Manifest.Operations[5].Documents[0].Content = "doctored"
			a.Manifest.OperationSHA256 = mustMinimaDigest(a.Manifest.Operations)
		}},
		{"update input mismatch", func(a *minimaArtifact) {
			a.Manifest.Operations[7].Documents[0].Content = "doctored"
			a.Manifest.OperationSHA256 = mustMinimaDigest(a.Manifest.Operations)
		}},
		{"delete input mismatch", func(a *minimaArtifact) {
			a.Manifest.Operations[9].IDs[0] = "doctored"
			a.Manifest.OperationSHA256 = mustMinimaDigest(a.Manifest.Operations)
		}},
		{"backend manifest mismatch", func(a *minimaArtifact) { minimaTestBackend(a, "qdrant").Manifest.OperationSHA256 = "bad" }},
		{"invalid qdrant route", func(a *minimaArtifact) {
			minimaTestRow(a, "qdrant", "small").Route.DeclaredScalarFiltering = false
		}},
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
		{"raw candidate ID count aliases visits", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			var route minimaRawNativeRouteResponse
			_ = json.Unmarshal(raw.NativeRouteResponses["small"], &route)
			route.CandidateIDs = route.Candidates
			raw.NativeRouteResponses["small"], _ = json.Marshal(route)
			a.RawEvidence["treedb"] = raw
		}},
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
		{"timed reader intervals after writer completion", func(a *minimaArtifact) {
			operations := &minimaTestBackend(a, "treedb").Operations
			writerEnd := operations.TimedExecutionTrace.Rounds[0].WriterEndedMonotonicNS
			for i := range operations.TimedExecutionTrace.Queries {
				query := &operations.TimedExecutionTrace.Queries[i]
				if query.Round == 0 && query.Reader == 3 {
					query.StartedMonotonicNS = writerEnd
					query.EndedMonotonicNS = writerEnd + 1
				}
			}
			operations.TimedExecutionSHA256 = minimaTimedExecutionDigest(operations.TimedExecutionTrace)
		}},
		{"missing timed writer interval", func(a *minimaArtifact) {
			operations := &minimaTestBackend(a, "treedb").Operations
			operations.TimedExecutionTrace.Rounds[0].WriterStartedMonotonicNS = 0
			operations.TimedExecutionTrace.Rounds[0].WriterEndedMonotonicNS = 0
			operations.TimedExecutionSHA256 = minimaTimedExecutionDigest(operations.TimedExecutionTrace)
		}},
		{"missing timed query interval", func(a *minimaArtifact) {
			operations := &minimaTestBackend(a, "treedb").Operations
			operations.TimedExecutionTrace.Queries[0].StartedMonotonicNS = 0
			operations.TimedExecutionTrace.Queries[0].EndedMonotonicNS = 0
			operations.TimedExecutionSHA256 = minimaTimedExecutionDigest(operations.TimedExecutionTrace)
		}},
		{"missing timed query result", func(a *minimaArtifact) {
			minimaTestBackend(a, "treedb").Operations.TimedExecutionTrace.Queries[0].ResultCaptured = false
		}},
		{"empty timed query result", func(a *minimaArtifact) {
			query := &minimaTestBackend(a, "treedb").Operations.TimedExecutionTrace.Queries[0]
			query.ActualIDs = nil
			query.ActualScores = nil
		}},
		{"stale timed query result", func(a *minimaArtifact) {
			query := &minimaTestBackend(a, "treedb").Operations.TimedExecutionTrace.Queries[0]
			query.ActualIDs = append([]string(nil), query.ActualIDs...)
			query.ActualIDs[0] = "stale"
		}},
		{"wrong timed query score", func(a *minimaArtifact) {
			query := &minimaTestBackend(a, "treedb").Operations.TimedExecutionTrace.Queries[0]
			query.ActualScores = append([]float64(nil), query.ActualScores...)
			query.ActualScores[0]++
		}},
		{"correct assertions with incomplete raw reindex trace", func(a *minimaArtifact) {
			operations := &minimaTestBackend(a, "treedb").Operations
			operations.ReindexExecutionTrace.Operations[0].ReaderQueries =
				operations.ReindexExecutionTrace.Operations[0].ReaderQueries[:3]
		}},
		{"reindex reader interval after mutation completion", func(a *minimaArtifact) {
			operations := &minimaTestBackend(a, "treedb").Operations
			mutationEnd := operations.ReindexExecutionTrace.Operations[1].MutationEndedMonotonicNS
			query := &operations.ReindexExecutionTrace.Operations[1].ReaderQueries[2]
			query.StartedMonotonicNS = mutationEnd
			query.EndedMonotonicNS = mutationEnd + 1
			operations.ReindexExecutionSHA256 = minimaReindexExecutionDigest(operations.ReindexExecutionTrace)
		}},
		{"missing reindex mutation interval", func(a *minimaArtifact) {
			operations := &minimaTestBackend(a, "treedb").Operations
			operations.ReindexExecutionTrace.Operations[0].MutationStartedMonotonicNS = 0
			operations.ReindexExecutionTrace.Operations[0].MutationEndedMonotonicNS = 0
			operations.ReindexExecutionSHA256 = minimaReindexExecutionDigest(operations.ReindexExecutionTrace)
		}},
		{"missing reindex query interval", func(a *minimaArtifact) {
			operations := &minimaTestBackend(a, "treedb").Operations
			query := &operations.ReindexExecutionTrace.Operations[0].ReaderQueries[0]
			query.StartedMonotonicNS = 0
			query.EndedMonotonicNS = 0
			operations.ReindexExecutionSHA256 = minimaReindexExecutionDigest(operations.ReindexExecutionTrace)
		}},
		{"missing reindex reader result", func(a *minimaArtifact) {
			minimaTestBackend(a, "treedb").Operations.ReindexExecutionTrace.Operations[0].ReaderQueries[0].ResultCaptured = false
		}},
		{"mixed pre/post reindex reader result", func(a *minimaArtifact) {
			query := &minimaTestBackend(a, "treedb").Operations.ReindexExecutionTrace.Operations[1].ReaderQueries[0]
			initial := minimaQueryMap(&a.Manifest)[query.Scenario]
			query.ActualIDs = append([]string(nil), query.ActualIDs...)
			query.ActualScores = append([]float64(nil), query.ActualScores...)
			query.ActualIDs[0] = initial.InitialOracleIDs[0]
			query.ActualScores[0] = initial.InitialOracleScores[0]
		}},
		{"wrong reindex execution hash", func(a *minimaArtifact) {
			minimaTestBackend(a, "treedb").Operations.ReindexExecutionSHA256 = "wrong"
		}},
		{"raw full-state actual hash mismatch", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			raw.FinalScrollState.ActualHash = "doctored"
			a.RawEvidence["treedb"] = raw
		}},
		{"raw overlap omits reader", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			raw.TimedOverlap.Rounds[0].OverlappingReaders = raw.TimedOverlap.Rounds[0].OverlappingReaders[:3]
			a.RawEvidence["treedb"] = raw
		}},
		{"missing TreeDB phase boundary", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			raw.PhaseAttribution.Phases = raw.PhaseAttribution.Phases[1:]
			a.RawEvidence["treedb"] = raw
		}},
		{"non-reconciling TreeDB phase total", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			raw.PhaseAttribution.UnattributedNanos++
			a.RawEvidence["treedb"] = raw
		}},
		{"excessive TreeDB unattributed overhead", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			extra := int64(61_000_000_000)
			raw.PhaseAttribution.TotalEndNanos += extra
			raw.PhaseAttribution.TotalDurationNanos += extra
			raw.PhaseAttribution.UnattributedNanos += extra
			a.RawEvidence["treedb"] = raw
		}},
		{"missing TreeDB phase resource boundary", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			raw.PhaseAttribution.Phases[0].ResourceSegments[0].End.Captured = false
			a.RawEvidence["treedb"] = raw
		}},
		{"TreeDB phase cumulative CPU decreases within one process", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			raw.PhaseAttribution.Phases[0].ResourceSegments[0].End.CPUSeconds = 0.5
			a.RawEvidence["treedb"] = raw
		}},
		{"TreeDB cumulative CPU resets between adjacent phases", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			segment := &raw.PhaseAttribution.Phases[1].ResourceSegments[0]
			segment.Start.CPUSeconds, segment.End.CPUSeconds = 0.5, 0.5
			a.RawEvidence["treedb"] = raw
		}},
		{"completed TreeDB phase marked resource-incomplete", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			complete := false
			raw.PhaseAttribution.Phases[0].ResourceEvidenceComplete = &complete
			a.RawEvidence["treedb"] = raw
		}},
		{"completed TreeDB phase carries an incomplete reason", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			raw.PhaseAttribution.Phases[0].IncompleteReason = "shutdown failed"
			a.RawEvidence["treedb"] = raw
		}},
		{"cross-PID unsplit TreeDB phase resource boundary", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			raw.PhaseAttribution.Phases[0].ResourceSegments[0].End.PID++
			a.RawEvidence["treedb"] = raw
		}},
		{"self-consistent pre-restart phase uses an unbound process", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			segment := &raw.PhaseAttribution.Phases[0].ResourceSegments[0]
			segment.Start.PID, segment.End.PID = 999, 999
			segment.Start.ProcessIdentity, segment.End.ProcessIdentity = "doctored", "doctored"
			a.RawEvidence["treedb"] = raw
		}},
		{"self-consistent post-restart phase uses an unbound process", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			segment := &raw.PhaseAttribution.Phases[6].ResourceSegments[0]
			segment.Start.PID, segment.End.PID = 999, 999
			segment.Start.ProcessIdentity, segment.End.ProcessIdentity = "doctored", "doctored"
			a.RawEvidence["treedb"] = raw
		}},
		{"cross-PID unsplit TreeDB restart resources", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			restart := raw.PhaseAttribution.Phases[5].ResourceSegments
			raw.PhaseAttribution.Phases[5].ResourceSegments = []minimaRawPhaseResourceSegment{{
				Start: restart[0].Start,
				End:   restart[1].End,
			}}
			a.RawEvidence["treedb"] = raw
		}},
		{"qualification-only TreeDB phase classified as production", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			raw.PhaseAttribution.Phases[len(raw.PhaseAttribution.Phases)-1].Classification = "production_path"
			a.RawEvidence["treedb"] = raw
		}},
		{"missing TreeDB product provenance", func(a *minimaArtifact) {
			minimaTestBackend(a, "treedb").Configuration["product_commit"] = ""
		}},
		{"TreeDB shutdown timeout diverges from operation bound", func(a *minimaArtifact) {
			minimaTestBackend(a, "treedb").Configuration["shutdown_timeout_seconds"] = "3600"
		}},
		{"raw resource disagrees with summary", func(a *minimaArtifact) {
			raw := a.RawEvidence["qdrant"]
			raw.ResourceMeasurement.DiskBytes = 1
			a.RawEvidence["qdrant"] = raw
		}},
		{"missing resource semantics", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			raw.ResourceMeasurement.Semantics = minimaRawResourceSemantics{}
			a.RawEvidence["treedb"] = raw
		}},
		{"inconsistent resource delta", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			raw.ResourceMeasurement.RSSBytes++
			a.RawEvidence["treedb"] = raw
		}},
		{"TreeDB phase restart old endpoint contradicts aggregate", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			segment := &raw.ResourceMeasurement.Segments[0]
			segment.End.CPUSeconds = 1.25
			segment.CPUSeconds = 0.25
			raw.ResourceMeasurement.CPUSeconds = 2.75
			a.RawEvidence["treedb"] = raw
			for index := range a.Scenarios {
				if a.Scenarios[index].Backend == "treedb" {
					a.Scenarios[index].Resource.CPUSeconds = 2.75
				}
			}
		}},
		{"TreeDB phase restart new baseline contradicts aggregate", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			segment := &raw.ResourceMeasurement.Segments[1]
			segment.Baseline.DiskBytes = 900
			segment.DiskBytes = 200
			raw.ResourceMeasurement.DiskBytes = 200
			a.RawEvidence["treedb"] = raw
			for index := range a.Scenarios {
				if a.Scenarios[index].Backend == "treedb" {
					a.Scenarios[index].Resource.DiskBytes = 200
				}
			}
		}},
		{"synthetic zero allocations marked unavailable", func(a *minimaArtifact) {
			zero := 0.0
			minimaTestRow(a, "qdrant", "small").Resource.BytesPerOp = &zero
		}},
		{"measured allocations omitted", func(a *minimaArtifact) {
			minimaTestRow(a, "treedb", "small").Resource.AllocationAvailability = "measured"
		}},
		{"unchanged restart PID", func(a *minimaArtifact) {
			raw := a.RawEvidence["qdrant"]
			raw.RestartBoundary.NewPID = raw.RestartBoundary.OldPID
			a.RawEvidence["qdrant"] = raw
		}},
		{"missing restart process identity", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			raw.RestartBoundary.NewProcessIdentity = ""
			a.RawEvidence["treedb"] = raw
		}},
		{"missing TreeDB service log tail", func(a *minimaArtifact) {
			raw := a.RawEvidence["treedb"]
			raw.ServiceLog.Tail = ""
			a.RawEvidence["treedb"] = raw
		}},
		{"missing Qdrant transition evidence", func(a *minimaArtifact) {
			raw := a.RawEvidence["qdrant"]
			raw.CollectionConfigurationTransition = nil
			a.RawEvidence["qdrant"] = raw
		}},
		{"missing Qdrant readiness evidence", func(a *minimaArtifact) {
			raw := a.RawEvidence["qdrant"]
			raw.Readiness = nil
			a.RawEvidence["qdrant"] = raw
		}},
		{"incomplete Qdrant production transition", func(a *minimaArtifact) {
			raw := a.RawEvidence["qdrant"]
			raw.CollectionConfigurationTransition.Completed = false
			a.RawEvidence["qdrant"] = raw
		}},
		{"Qdrant production transition mismatch", func(a *minimaArtifact) {
			raw := a.RawEvidence["qdrant"]
			raw.CollectionConfigurationTransition.ProductionHNSW = json.RawMessage(`{"m":32}`)
			a.RawEvidence["qdrant"] = raw
		}},
		{"non-ready Qdrant session", func(a *minimaArtifact) {
			raw := a.RawEvidence["qdrant"]
			raw.Readiness.Sessions[0].Outcome = "timeout"
			a.RawEvidence["qdrant"] = raw
		}},
		{"Qdrant transition and configuration jointly doctored", func(a *minimaArtifact) {
			doctored := `{"m":0,"ef_construct":100,"full_scan_threshold":10000,"max_indexing_threads":0,"on_disk":false}`
			raw := a.RawEvidence["qdrant"]
			raw.CollectionConfigurationTransition.ProductionHNSW = json.RawMessage(doctored)
			a.RawEvidence["qdrant"] = raw
			minimaTestBackend(a, "qdrant").Configuration["production_hnsw"] = doctored
		}},
		{"Qdrant effective configuration mismatch", func(a *minimaArtifact) {
			minimaTestBackend(a, "qdrant").Configuration["effective_collection"] = `{"hnsw_config":{},"optimizer_config":{}}`
		}},
		{"Qdrant later readiness phase omitted", func(a *minimaArtifact) {
			raw := a.RawEvidence["qdrant"]
			raw.Readiness.Sessions = raw.Readiness.Sessions[:len(raw.Readiness.Sessions)-1]
			a.RawEvidence["qdrant"] = raw
		}},
		{"Qdrant readiness snapshot empty", func(a *minimaArtifact) {
			raw := a.RawEvidence["qdrant"]
			raw.Readiness.Sessions[1].Snapshots[0] = minimaRawQdrantReadinessSnapshot{}
			a.RawEvidence["qdrant"] = raw
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
	timedTrace := minimaExpectedTimedExecution(&manifest)
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
		{Name: "treedb", ServerVersion: "test", ClientVersion: "test", Durability: "wal_sync", Configuration: map[string]string{
			"effective": "test", "product_commit": strings.Repeat("a", 40), "harness_commit": strings.Repeat("a", 40),
			"service_binary_sha256": strings.Repeat("b", 64), "runner_sha256": strings.Repeat("c", 64),
			"operation_timeout_seconds": "120", "startup_reopen_timeout_seconds": "3600", "shutdown_timeout_seconds": "120",
		}, Environment: minimaTestEnvironment(), Manifest: hashes, Operations: operations, Reopen: minimaReopenEvidence{Attempted: true, CommittedParity: true, ResultManifestHash: manifest.ExpectedStateSHA256}},
		{Name: "qdrant", ServerVersion: "test", ClientVersion: "test", Durability: "wal", Configuration: map[string]string{
			"effective":           "test",
			"initial_upload_hnsw": minimaQdrantInitialHNSWConfig, "initial_upload_optimizers": minimaQdrantInitialOptimizerConfig,
			"production_hnsw": minimaQdrantProductionHNSWConfig, "production_optimizers": minimaQdrantProductionOptimizerConfig,
			"effective_collection": fmt.Sprintf(`{"hnsw_config":%s,"optimizer_config":%s}`, minimaQdrantProductionHNSWConfig, minimaQdrantProductionOptimizerConfig),
		}, Environment: minimaTestEnvironment(), Manifest: hashes, Operations: operations, Reopen: minimaReopenEvidence{Attempted: true, CommittedParity: true, ResultManifestHash: manifest.ExpectedStateSHA256}},
	}
	queries := minimaQueryMap(&manifest)
	expectedPayloadHash, expectedRows, err := minimaExpectedPayloadEvidence(&manifest)
	if err != nil {
		panic(err)
	}
	resource := minimaTestResourceMeasurement()
	rawEvidence := make(map[string]minimaRawBackendEvidence, len(backends))
	for _, backend := range backends {
		backendResource := resource
		if backend.Name == "treedb" {
			backendResource = minimaTestTreeDBResourceMeasurement()
		}
		rounds := make([]minimaRawTimedOverlapRound, len(timed.Rounds))
		for ordinal, round := range timed.Rounds {
			readers := make([]int, timed.ReaderConcurrency)
			for reader := range readers {
				readers[reader] = reader
			}
			rounds[ordinal] = minimaRawTimedOverlapRound{
				Ordinal: ordinal, QueriesExecuted: round.QueryCount,
				OverlappingReaders: readers, AllReadersOverlapObserved: true,
			}
		}
		rawEvidence[backend.Name] = minimaRawBackendEvidence{
			TimedOverlap: minimaRawTimedOverlap{
				ConfiguredSearches: timed.QueryCount, ExecutedSearches: timed.QueryCount,
				ConfiguredReaderConcurrency: timed.ReaderConcurrency, ConfiguredWriterConcurrency: timed.WriterConcurrency,
				Rounds: rounds, AllRoundsWriterSearchOverlapObserved: true,
				TimedExecutionSHA256: operations.TimedExecutionSHA256,
			},
			FinalScrollState: minimaRawFinalState{
				Algorithm:    "payload digest plus normalized-float32 full-vector comparison",
				ExpectedHash: expectedPayloadHash, ActualHash: expectedPayloadHash,
				ExpectedRows: expectedRows, ActualRows: expectedRows,
				Payload: minimaRawPayloadState{ExpectedHash: expectedPayloadHash, ActualHash: expectedPayloadHash, Match: true},
				Vectors: minimaRawVectorState{
					Algorithm: "streaming normalized-float32 comparison", CheckedRows: expectedRows,
					ExpectedRows: expectedRows, Tolerance: minimaScoreTolerance, Match: true,
				},
				Match: true,
			},
			PhaseLatencyDistributions: map[string]minimaRawLatencyDistribution{
				"search": {Count: 1, TotalNanos: 7, MinimumNanos: 7, P50Nanos: 7, P95Nanos: 7, P99Nanos: 7, MaximumNanos: 7},
			},
			Events:              []json.RawMessage{json.RawMessage(`{"operation":"test"}`)},
			RestartBoundary:     minimaTestRestartBoundary(),
			ResourceMeasurement: backendResource,
			ResourceAvailability: map[string]map[string]string{
				"baseline": {"rss_bytes": "test"},
				"end":      {"rss_bytes": "test"},
			},
			PhaseAttribution: minimaTestPhaseAttribution(),
		}
		if backend.Name == "treedb" {
			evidence := rawEvidence[backend.Name]
			evidence.ServiceLog = minimaRawServiceLog{
				Path: "/tmp/treedb-service.log", Tail: "TreeDB Document Service listening", MaxTailBytes: 64 << 10,
			}
			rawEvidence[backend.Name] = evidence
		}
		if backend.Name == "qdrant" {
			evidence := rawEvidence[backend.Name]
			evidence.CollectionConfigurationTransition = &minimaRawQdrantConfigurationTransition{
				Boundary: "initial_batch_insert_to_warmup_search", Attempted: true, Completed: true,
				InitialUploadHNSW: json.RawMessage(minimaQdrantInitialHNSWConfig), InitialUploadOptimizers: json.RawMessage(minimaQdrantInitialOptimizerConfig),
				ProductionHNSW: json.RawMessage(minimaQdrantProductionHNSWConfig), ProductionOptimizers: json.RawMessage(minimaQdrantProductionOptimizerConfig),
			}
			intPointer := func(value int) *int { return &value }
			snapshot := func(points int, hnsw, optimizer string) minimaRawQdrantReadinessSnapshot {
				return minimaRawQdrantReadinessSnapshot{
					Status: "green", OptimizerStatus: json.RawMessage(`{"ok":true}`),
					PointsCount: intPointer(points), IndexedVectorsCount: intPointer(points), ExactPointsCount: intPointer(points), SegmentsCount: intPointer(1),
					PayloadSchema: map[string]json.RawMessage{"user_id": json.RawMessage(`"keyword"`), "fpath": json.RawMessage(`"keyword"`)},
					Config:        json.RawMessage(fmt.Sprintf(`{"hnsw_config":%s,"optimizer_config":%s}`, hnsw, optimizer)),
					Optimizations: minimaRawQdrantOptimizationSnapshot{
						Available: true, Detail: json.RawMessage(`{"summary":{},"running":[]}`),
					},
				}
			}
			sessions := []minimaRawQdrantReadinessSession{
				{
					Phase: "initial_upload_collection_created", DeadlineSeconds: 600, ExpectedPointsCount: intPointer(0),
					Snapshots:       []minimaRawQdrantReadinessSnapshot{snapshot(0, minimaQdrantInitialHNSWConfig, minimaQdrantInitialOptimizerConfig)},
					ResourceSamples: []json.RawMessage{json.RawMessage(`{"captured":true}`)}, Outcome: "ready", Disposition: "ready",
				},
				{
					Phase: "initial_load_to_query", DeadlineSeconds: 600, ExpectedPointsCount: intPointer(expectedRows),
					Snapshots:       []minimaRawQdrantReadinessSnapshot{snapshot(expectedRows, minimaQdrantProductionHNSWConfig, minimaQdrantProductionOptimizerConfig)},
					ResourceSamples: []json.RawMessage{json.RawMessage(`{"captured":true}`)}, Outcome: "ready", Disposition: "ready",
				},
			}
			for len(sessions) < len(timed.Rounds)+7 {
				sessions = append(sessions, minimaRawQdrantReadinessSession{
					Phase: "mutation_visibility", DeadlineSeconds: 600,
					Snapshots:       []minimaRawQdrantReadinessSnapshot{snapshot(expectedRows, minimaQdrantProductionHNSWConfig, minimaQdrantProductionOptimizerConfig)},
					ResourceSamples: []json.RawMessage{json.RawMessage(`{"captured":true}`)}, Outcome: "ready", Disposition: "ready",
				})
			}
			evidence.Readiness = &minimaRawQdrantReadiness{
				Sessions: sessions, LatestNonReadyDisposition: "none",
			}
			rawEvidence[backend.Name] = evidence
		}
	}
	artifact := minimaArtifact{
		Schema: minimaArtifactSchema, State: "pass", Passing: true, Manifest: manifest,
		Backends: backends, Recommendation: "ready_direct", RawEvidence: rawEvidence,
	}
	for _, backend := range backends {
		backendResource := resource
		if backend.Name == "treedb" {
			backendResource = minimaTestTreeDBResourceMeasurement()
		}
		for _, spec := range manifest.Corpora {
			query := queries[spec.Name]
			zeroMismatch, zeroRetry := 0, 0
			candidateIDs, visited, scored, admitted := spec.EligibleRows, spec.EligibleRows, spec.EligibleRows, spec.EligibleRows
			if backend.Name == "treedb" && spec.Name == "small" {
				candidateIDs, visited, scored, admitted = 5, 41, 41, 5
			}
			route := minimaRouteEvidence{
				Identity: "qdrant_filtered_hnsw", DeclaredScalarFiltering: true,
				FullDocumentScanFallbacks: minimaTestInt(0), ScalarFilterUnbounded: minimaTestInt(0),
				ProbeIDs: minimaTestInt(0), CandidateIDs: minimaTestInt(candidateIDs),
				RetainedCandidateIDs: minimaTestInt(0), RefinedCandidateIDs: minimaTestInt(0),
				MembershipSource: "finite_scalar", Plan: "complete_finite_ann",
				AllowedIDMaterializationRows: minimaTestInt(0), PrimaryDocumentScans: minimaTestInt(0),
				VisitedCandidates: minimaTestInt(visited), ScoredCandidates: minimaTestInt(scored),
				AdmittedCandidates: minimaTestInt(admitted),
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
			visibility := minimaVisibilityEvidence{GenerationConsistent: true, MismatchCount: &zeroMismatch, RetryCount: &zeroRetry}
			artifact.Scenarios = append(artifact.Scenarios, minimaScenarioEvidence{
				Backend: backend.Name, Scenario: spec.Name, CorpusRows: spec.CorpusRows, ExpectedMatches: spec.EligibleRows, Selectivity: spec.Selectivity,
				InitialOracleIDs: query.InitialOracleIDs, InitialOracleScores: query.InitialOracleScores,
				FinalOracleIDs: query.FinalOracleIDs, FinalOracleScores: query.FinalOracleScores,
				InitialActualIDs: append([]string(nil), query.InitialOracleIDs...), InitialActualScores: append([]float64(nil), query.InitialOracleScores...),
				ActualIDs: append([]string(nil), query.FinalOracleIDs...), ActualScores: append([]float64(nil), query.FinalOracleScores...),
				ReopenIDs: append([]string(nil), query.FinalOracleIDs...), ReopenParity: true,
				Recall: 1, Overlap: 1, OrderTolerance: minimaOrderTolerance, ScoreTolerance: minimaScoreTolerance,
				Route: route, Visibility: visibility,
				Timing: minimaTimingEvidence{Captured: true},
				Resource: minimaResourceEvidence{
					Captured: true, AllocationAvailability: "unavailable",
					RSSBytes: backendResource.RSSBytes, CPUSeconds: backendResource.CPUSeconds, DiskBytes: backendResource.DiskBytes,
				},
			})
			if backend.Name == "treedb" {
				evidence := artifact.RawEvidence["treedb"]
				if evidence.NativeRouteResponses == nil {
					evidence.NativeRouteResponses = make(map[string]json.RawMessage, len(manifest.Corpora))
				}
				evidence.NativeRouteResponses[spec.Name] = minimaTestNativeRouteResponse(route, visibility)
				artifact.RawEvidence["treedb"] = evidence
			}
		}
	}
	return artifact
}

func minimaTestRestartBoundary() minimaRawRestartBoundary {
	return minimaRawRestartBoundary{
		HookIdentity:       "test restart hook",
		OldPID:             100,
		NewPID:             101,
		OldProcessIdentity: "old process",
		NewProcessIdentity: "new process",
		PIDChanged:         true,
		Verified:           true,
	}
}

func minimaTestPhaseAttribution() minimaRawPhaseAttribution {
	names := []string{
		"initial_durable_load",
		"warmup_search",
		"timed_search_write_overlap",
		"lifecycle_mutations",
		"pre_close_queries",
		"restart_open_readiness",
		"post_reopen",
		"final_state_scroll_artifact_work",
	}
	restart := minimaTestRestartBoundary()
	oldResource := minimaRawPhaseResourceEndpoint{
		Captured: true, RSSBytes: 100, CPUSeconds: 1, DiskBytes: 1000,
		PID: restart.OldPID, ProcessIdentity: restart.OldProcessIdentity,
	}
	newResource := minimaRawPhaseResourceEndpoint{
		Captured: true, RSSBytes: 200, CPUSeconds: 2, DiskBytes: 1000,
		PID: restart.NewPID, ProcessIdentity: restart.NewProcessIdentity,
	}
	newBaseline := newResource
	newBaseline.RSSBytes = 0
	newBaseline.CPUSeconds = 0
	phases := make([]minimaRawPhaseBoundary, len(names))
	for ordinal, name := range names {
		start := int64(1000 + ordinal*110)
		classification := "production_path"
		if ordinal == len(names)-1 {
			classification = "qualification_only"
		}
		resource := oldResource
		if ordinal > 5 {
			resource = newResource
		}
		segments := []minimaRawPhaseResourceSegment{{Start: resource, End: resource}}
		if ordinal == 5 {
			segments = []minimaRawPhaseResourceSegment{
				{Start: oldResource, End: oldResource},
				{Start: newBaseline, End: newResource},
			}
		}
		phases[ordinal] = minimaRawPhaseBoundary{
			Name: name, Classification: classification, StartNanos: start, EndNanos: start + 100,
			DurationNanos: 100, SampleCount: 1, SampleDurationNanos: 50,
			ResourceSegments: segments,
		}
	}
	return minimaRawPhaseAttribution{
		Clock: "time.monotonic_ns", TotalStartNanos: 1000, TotalEndNanos: 1870,
		TotalDurationNanos: 870, UnattributedNanos: 70,
		UnattributedRule: minimaPhaseUnattributedRule, Phases: phases,
	}
}

func minimaTestInt(value int) *int {
	return &value
}

func minimaTestNativeRouteResponse(route minimaRouteEvidence, visibility minimaVisibilityEvidence) json.RawMessage {
	raw, err := json.Marshal(minimaRawNativeRouteResponse{
		MembershipSource:     route.MembershipSource,
		Plan:                 route.Plan,
		ProbeIDs:             *route.ProbeIDs,
		Candidates:           *route.VisitedCandidates,
		CandidateIDs:         *route.CandidateIDs,
		Retained:             *route.RetainedCandidateIDs,
		Refined:              *route.RefinedCandidateIDs,
		Visited:              *route.VisitedCandidates,
		Scored:               *route.ScoredCandidates,
		Admitted:             *route.AdmittedCandidates,
		VisibilityMismatches: *visibility.MismatchCount,
		VisibilityRetries:    *visibility.RetryCount,
	})
	if err != nil {
		panic(err)
	}
	return raw
}

func minimaTestEnvironment() map[string]string {
	return map[string]string{"os": "test", "arch": "test", "cpu": "test", "memory": "test", "host": "test-host"}
}

func minimaTestResourceMeasurement() minimaRawResourceMeasurement {
	baseline := minimaRawResourceSnapshot{Captured: true, RSSBytes: 100, CPUSeconds: 1, DiskBytes: 1000}
	end := minimaRawResourceSnapshot{Captured: true, RSSBytes: 125, CPUSeconds: 2.5, DiskBytes: 1100}
	segment := minimaRawResourceSegment{
		Captured: true, RSSBytes: 25, CPUSeconds: 1.5, DiskBytes: 100,
		Baseline: baseline, End: end,
	}
	return minimaRawResourceMeasurement{
		Captured: true, RSSBytes: 25, CPUSeconds: 1.5, DiskBytes: 100,
		Semantics: minimaRawResourceSemantics{
			RSSBytes: minimaResourceRSSSemantics, CPUSeconds: minimaResourceCPUSemantics, DiskBytes: minimaResourceDiskSemantics,
		},
		Segments: []minimaRawResourceSegment{segment},
		Baseline: &baseline,
		End:      &end,
	}
}

func minimaTestTreeDBResourceMeasurement() minimaRawResourceMeasurement {
	old := minimaRawResourceSnapshot{Captured: true, RSSBytes: 100, CPUSeconds: 1, DiskBytes: 1000}
	fresh := minimaRawResourceSnapshot{Captured: true, RSSBytes: 0, CPUSeconds: 0, DiskBytes: 1000}
	end := minimaRawResourceSnapshot{Captured: true, RSSBytes: 125, CPUSeconds: 2.5, DiskBytes: 1100}
	segments := []minimaRawResourceSegment{
		{Captured: true, Baseline: old, End: old},
		{
			Captured: true, RSSBytes: 125, CPUSeconds: 2.5, DiskBytes: 100,
			Baseline: fresh, End: end,
		},
	}
	return minimaRawResourceMeasurement{
		Captured: true, RSSBytes: 125, CPUSeconds: 2.5, DiskBytes: 100,
		Semantics: minimaRawResourceSemantics{
			RSSBytes: minimaResourceRSSSemantics, CPUSeconds: minimaResourceCPUSemantics, DiskBytes: minimaResourceDiskSemantics,
		},
		Segments: segments,
		Baseline: &old,
		End:      &end,
	}
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

func minimaPartialBackendEvidence(t *testing.T) (minimaArtifact, minimaArtifact) {
	t.Helper()
	full := validMinimaArtifact()
	tree := cloneMinimaArtifact(t, full)
	tree.State, tree.Passing, tree.Recommendation = "partial", false, "not_evaluated"
	tree.Backends, tree.Scenarios = tree.Backends[:1], tree.Scenarios[:len(tree.Manifest.Corpora)]
	tree.RawEvidence = map[string]minimaRawBackendEvidence{"treedb": tree.RawEvidence["treedb"]}
	qdrant := cloneMinimaArtifact(t, full)
	qdrant.State, qdrant.Passing, qdrant.Recommendation = "partial", false, "not_evaluated"
	qdrant.Backends, qdrant.Scenarios = qdrant.Backends[1:], qdrant.Scenarios[len(qdrant.Manifest.Corpora):]
	qdrant.RawEvidence = map[string]minimaRawBackendEvidence{"qdrant": qdrant.RawEvidence["qdrant"]}
	for i := range qdrant.Scenarios {
		qdrant.Scenarios[i].Route.FullDocumentScanFallbacks = nil
		qdrant.Scenarios[i].Route.ScalarFilterUnbounded = nil
		qdrant.Scenarios[i].Route.ProbeIDs = nil
		qdrant.Scenarios[i].Route.CandidateIDs = nil
		qdrant.Scenarios[i].Route.RetainedCandidateIDs = nil
		qdrant.Scenarios[i].Route.RefinedCandidateIDs = nil
		qdrant.Scenarios[i].Route.AllowedIDMaterializationRows = nil
		qdrant.Scenarios[i].Route.PrimaryDocumentScans = nil
		qdrant.Scenarios[i].Route.VisitedCandidates = nil
		qdrant.Scenarios[i].Route.ScoredCandidates = nil
		qdrant.Scenarios[i].Route.AdmittedCandidates = nil
	}
	return tree, qdrant
}

func TestMinimaExpectedCommitBinding(t *testing.T) {
	artifact := validMinimaArtifact()
	expected := strings.Repeat("a", 40)
	if err := validateMinimaExpectedCommit(&artifact, expected, true); err != nil {
		t.Fatal(err)
	}
	if err := validateMinimaExpectedCommit(&artifact, strings.Repeat("b", 40), true); err == nil {
		t.Fatal("wrong merged commit was accepted")
	}
	if err := validateMinimaExpectedCommit(&artifact, "", true); err == nil {
		t.Fatal("missing required merged commit was accepted")
	}
}

func TestMinimaPhaseIncompleteMarkerRoundTrip(t *testing.T) {
	complete := false
	input := minimaRawPhaseBoundary{
		Name:                     "restart_open_readiness",
		ResourceEvidenceComplete: &complete,
		IncompleteReason:         "graceful_shutdown_failed_before_reopen",
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	var decoded minimaRawPhaseBoundary
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ResourceEvidenceComplete == nil || *decoded.ResourceEvidenceComplete ||
		decoded.IncompleteReason != input.IncompleteReason {
		t.Fatalf("incomplete phase marker was not preserved: %+v", decoded)
	}
	rewritten, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != string(rewritten) {
		t.Fatalf("incomplete phase marker changed during round trip: %s != %s", encoded, rewritten)
	}
}

func TestMinimaPhaseUnattributedBound(t *testing.T) {
	const total = int64(7_000_000_000_000)
	if got := minimaPhaseUnattributedLimit(1_000_000_000_000); got != minimaPhaseUnattributedAbsoluteNanos {
		t.Fatalf("absolute unattributed boundary=%d", got)
	}
	if got := minimaPhaseUnattributedLimit(total); got != 70_000_000_000 {
		t.Fatalf("proportional unattributed boundary=%d", got)
	}
	attribution := minimaTestPhaseAttribution()
	attributed := total - minimaPhaseUnattributedLimit(total)
	cursor := attribution.TotalStartNanos
	for index := range attribution.Phases {
		duration := attributed / int64(len(attribution.Phases))
		if index == len(attribution.Phases)-1 {
			duration = attributed - (cursor - attribution.TotalStartNanos)
		}
		attribution.Phases[index].StartNanos = cursor
		attribution.Phases[index].EndNanos = cursor + duration
		attribution.Phases[index].DurationNanos = duration
		cursor += duration
	}
	attribution.TotalEndNanos = attribution.TotalStartNanos + total
	attribution.TotalDurationNanos = total
	attribution.UnattributedNanos = minimaPhaseUnattributedLimit(total)
	if err := validateMinimaTreeDBPhaseAttribution(attribution, minimaTestRestartBoundary()); err != nil {
		t.Fatalf("boundary attribution rejected: %v", err)
	}
	attribution.TotalEndNanos++
	attribution.TotalDurationNanos++
	attribution.UnattributedNanos++
	if err := validateMinimaTreeDBPhaseAttribution(attribution, minimaTestRestartBoundary()); err == nil {
		t.Fatal("over-limit unattributed overhead was accepted")
	}
}

func TestMinimaComparatorCombinesBackendEvidenceThroughValidator(t *testing.T) {
	tree, qdrant := minimaPartialBackendEvidence(t)
	dir := t.TempDir()
	treePath, qdrantPath := filepath.Join(dir, "tree.json"), filepath.Join(dir, "qdrant.json")
	for path, artifact := range map[string]minimaArtifact{treePath: tree, qdrantPath: qdrant} {
		raw, err := json.Marshal(artifact)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	out, report := filepath.Join(dir, "qualification.json"), filepath.Join(dir, "report.md")
	if err := compareMinimaEvidence(treePath, qdrantPath, out, report, "ready_with_alpha_limitations", strings.Repeat("a", 40)); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var combined minimaArtifact
	if err := json.Unmarshal(raw, &combined); err != nil {
		t.Fatal(err)
	}
	if len(combined.RawEvidence) != 2 ||
		combined.RawEvidence["treedb"].FinalScrollState.ActualHash == "" ||
		combined.RawEvidence["qdrant"].FinalScrollState.ActualHash == "" {
		t.Fatal("combined artifact dropped namespaced backend raw evidence")
	}
	if combined.RawEvidence["treedb"].PhaseLatencyDistributions["search"].Count != 1 ||
		len(combined.RawEvidence["qdrant"].Events) != 1 ||
		combined.RawEvidence["qdrant"].ResourceAvailability["end"]["rss_bytes"] != "test" {
		t.Fatal("combined artifact dropped typed backend raw evidence fields")
	}
	var wire struct {
		RawEvidence map[string]map[string]json.RawMessage `json:"backend_raw_evidence"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	treeRaw, qdrantRaw := wire.RawEvidence["treedb"], wire.RawEvidence["qdrant"]
	if _, leaked := treeRaw["collection_configuration_transition"]; leaked {
		t.Fatal("combined TreeDB raw evidence leaked Qdrant configuration transition")
	}
	if _, leaked := treeRaw["readiness"]; leaked {
		t.Fatal("combined TreeDB raw evidence leaked Qdrant readiness")
	}
	if _, ok := qdrantRaw["collection_configuration_transition"]; !ok {
		t.Fatal("combined Qdrant raw evidence dropped configuration transition")
	}
	if _, ok := qdrantRaw["readiness"]; !ok {
		t.Fatal("combined Qdrant raw evidence dropped readiness")
	}
	if err := validateMinimaArtifact(&combined); err != nil || combined.State != "pass" || !combined.Passing {
		t.Fatalf("combined artifact=%+v err=%v", combined, err)
	}
	if info, err := os.Stat(report); err != nil || info.Size() == 0 {
		t.Fatalf("report info=%v err=%v", info, err)
	}
}

func TestMinimaComparatorWritesPartialOracleFailureAndReturnsError(t *testing.T) {
	tree, qdrant := minimaPartialBackendEvidence(t)
	tree.Failures = append(tree.Failures, "treedb final oracle mismatch")
	dir := t.TempDir()
	treePath, qdrantPath := filepath.Join(dir, "tree.json"), filepath.Join(dir, "qdrant.json")
	for path, artifact := range map[string]minimaArtifact{treePath: tree, qdrantPath: qdrant} {
		raw, err := json.Marshal(artifact)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	out, report := filepath.Join(dir, "qualification.json"), filepath.Join(dir, "report.md")
	if err := compareMinimaEvidence(treePath, qdrantPath, out, report, "ready_with_alpha_limitations", strings.Repeat("a", 40)); err == nil {
		t.Fatal("partial oracle failure returned success")
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var combined minimaArtifact
	if err := json.Unmarshal(raw, &combined); err != nil {
		t.Fatal(err)
	}
	if combined.State != "partial" || combined.Passing || combined.Recommendation != "not_evaluated" ||
		len(combined.Failures) != 1 || combined.Failures[0] != "treedb final oracle mismatch" {
		t.Fatalf("combined partial artifact=%+v", combined)
	}
	if err := validateMinimaArtifact(&combined); err != nil {
		t.Fatalf("preserved partial artifact is invalid: %v", err)
	}
	if info, err := os.Stat(report); err != nil || info.Size() == 0 {
		t.Fatalf("report info=%v err=%v", info, err)
	}
}
