package mongogateway

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Explain is a stable diagnostic envelope, not a serialization of TreeDB
// internals. These tests deliberately assert only public planner vocabulary.
func TestMongoExplainFindPlannerAndExecutionStats(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)

	for _, tc := range []struct {
		name      string
		verbosity string
		filter    bson.D
		wantPlan  string
	}{
		{name: "primary", verbosity: "queryPlanner", filter: bson.D{{Key: "_id", Value: "u1"}}, wantPlan: "primary_lookup"},
		{name: "secondary equality", verbosity: "queryPlanner", filter: bson.D{{Key: "city", Value: "hnl"}}, wantPlan: "secondary_equality_lookup"},
		{name: "bounded scan", verbosity: "executionStats", filter: bson.D{{Key: "active", Value: true}}, wantPlan: "bounded_scan"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := serveCommand(t, server, 100, bson.D{
				{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: tc.filter}, {Key: "$db", Value: "app"}}},
				{Key: "verbosity", Value: tc.verbosity},
				{Key: "$db", Value: "app"},
			})
			assertOK(t, resp)
			planner, ok := bson.Raw(resp).Lookup("queryPlanner").DocumentOK()
			if !ok {
				t.Fatalf("queryPlanner missing from %s", resp)
			}
			winning, ok := planner.Lookup("winningPlan").DocumentOK()
			if !ok {
				t.Fatalf("winningPlan missing from %s", planner)
			}
			if got, ok := winning.Lookup("stage").StringValueOK(); !ok || got != tc.wantPlan {
				t.Fatalf("winning plan=%q ok=%v want %q", got, ok, tc.wantPlan)
			}
			if _, ok := winning.Lookup("root").Int64OK(); ok {
				t.Fatalf("winning plan leaks TreeDB internals: %s", winning)
			}
			for _, field := range []string{"usableIndexes", "rejectedIndexes", "scanBounds", "sort", "maxScanDocuments", "cursorWork"} {
				if bson.Raw(resp).Lookup("queryPlanner").Document().Lookup(field).IsZero() {
					t.Fatalf("queryPlanner missing stable field %q: %s", field, planner)
				}
			}
			if tc.verbosity == "executionStats" {
				stats, ok := bson.Raw(resp).Lookup("executionStats").DocumentOK()
				if !ok {
					t.Fatalf("executionStats missing from %s", resp)
				}
				if got, ok := stats.Lookup("nReturned").Int64OK(); !ok || got != 2 {
					t.Fatalf("nReturned=%d ok=%v want 2", got, ok)
				}
				if got, ok := stats.Lookup("candidateDocumentsExamined").Int64OK(); !ok || got < 2 {
					t.Fatalf("candidateDocumentsExamined=%d ok=%v want >=2", got, ok)
				}
				if got, ok := stats.Lookup("candidateDocumentsMaterialized").Int64OK(); !ok || got < 2 {
					t.Fatalf("candidateDocumentsMaterialized=%d ok=%v want >=2", got, ok)
				}
			}
		})
	}
}

func TestMongoExplainRejectsWritesAndUnsupportedVerbosity(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	for _, command := range []bson.D{
		{{Key: "explain", Value: bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "write"}}}}, {Key: "$db", Value: "app"}}}, {Key: "$db", Value: "app"}},
		{{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "$db", Value: "app"}}}, {Key: "verbosity", Value: "allPlansExecution"}, {Key: "$db", Value: "app"}},
	} {
		assertCommandError(t, serveCommand(t, server, 101, command), "BadValue")
	}
}

func TestMongoExplainRejectsUnsupportedOptionAndQueryWithoutMutation(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	before := serveCommand(t, server, 101, bson.D{{Key: "count", Value: "users"}, {Key: "$db", Value: "app"}})
	assertOK(t, before)
	maxTime := serveCommand(t, server, 101, bson.D{{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}}}, {Key: "maxTimeMS", Value: int32(1)}, {Key: "$db", Value: "app"}})
	assertCommandError(t, maxTime, "BadValue")
	hint := serveCommand(t, server, 101, bson.D{{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "hint", Value: "city_1"}}}, {Key: "$db", Value: "app"}})
	assertCommandError(t, hint, "BadValue")
	rejected := serveCommand(t, server, 101, bson.D{{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "$where", Value: "true"}}}}}, {Key: "$db", Value: "app"}})
	assertCommandError(t, rejected, "BadValue")
	planner, ok := bson.Raw(rejected).Lookup("queryPlanner").DocumentOK()
	if !ok || planner.Lookup("winningPlan").Document().Lookup("stage").String() == "" {
		t.Fatalf("rejected planner missing: %s", rejected)
	}
	after := serveCommand(t, server, 101, bson.D{{Key: "count", Value: "users"}, {Key: "$db", Value: "app"}})
	assertOK(t, after)
	if !bytes.Equal(before, after) {
		t.Fatalf("explain changed count response: before=%s after=%s", before, after)
	}
}

func TestMongoExplainCountersSeparateExaminedFromMaterializedAndPlannerDoesNotExecute(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	server.MaxFindScanDocuments = 1
	command := bson.D{{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "active", Value: true}}}}}, {Key: "$db", Value: "app"}}
	planner := serveCommand(t, server, 110, command)
	assertOK(t, planner)
	command = append(command, bson.E{Key: "verbosity", Value: "executionStats"})
	assertCommandError(t, serveCommand(t, server, 110, command), "BadValue")

	server.MaxFindScanDocuments = 10
	statsResponse := serveCommand(t, server, 111, command)
	assertOK(t, statsResponse)
	stats := bson.Raw(statsResponse).Lookup("executionStats").Document()
	examined, _ := stats.Lookup("candidateDocumentsExamined").Int64OK()
	materialized, _ := stats.Lookup("candidateDocumentsMaterialized").Int64OK()
	if examined <= materialized {
		t.Fatalf("examined=%d materialized=%d want rejected stored predicate work", examined, materialized)
	}
}

func TestMongoExplainExecutionStatsPreservesBoundedScanRejectionContext(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	server.MaxFindScanDocuments = 1
	resp := serveCommand(t, server, 102, bson.D{
		{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "active", Value: true}}}, {Key: "$db", Value: "app"}}},
		{Key: "verbosity", Value: "executionStats"}, {Key: "$db", Value: "app"},
	})
	assertCommandError(t, resp, "BadValue")
	stats, ok := bson.Raw(resp).Lookup("executionStats").DocumentOK()
	if !ok {
		t.Fatalf("executionStats missing from rejected explain: %s", resp)
	}
	if got, ok := stats.Lookup("rejectionReason").StringValueOK(); !ok || got != "scan_cap_exceeded" {
		t.Fatalf("rejectionReason=%q ok=%v want scan_cap_exceeded", got, ok)
	}
	if _, ok := bson.Raw(resp).Lookup("queryPlanner").DocumentOK(); !ok {
		t.Fatalf("queryPlanner missing from rejected explain: %s", resp)
	}
}

func TestMongoExplainFindRangeResidualAndSortVocabulary(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	resp := serveCommand(t, server, 103, bson.D{
		{Key: "explain", Value: bson.D{
			{Key: "find", Value: "users"},
			{Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int32(20)}}}, {Key: "active", Value: true}}},
			{Key: "sort", Value: bson.D{{Key: "name", Value: int32(1)}}},
			{Key: "$db", Value: "app"},
		}},
		{Key: "verbosity", Value: "executionStats"}, {Key: "$db", Value: "app"},
	})
	assertOK(t, resp)
	planner, ok := bson.Raw(resp).Lookup("queryPlanner").DocumentOK()
	if !ok {
		t.Fatalf("queryPlanner missing: %s", resp)
	}
	winning := planner.Lookup("winningPlan").Document()
	if got, ok := winning.Lookup("stage").StringValueOK(); !ok || got != "secondary_range_lookup" {
		t.Fatalf("stage=%q ok=%v want secondary_range_lookup", got, ok)
	}
	if residual, ok := winning.Lookup("residualFilter").BooleanOK(); !ok || !residual {
		t.Fatalf("residualFilter=%v ok=%v want true", residual, ok)
	}
	if inMemory, ok := winning.Lookup("inMemorySort").BooleanOK(); !ok || !inMemory {
		t.Fatalf("inMemorySort=%v ok=%v want true", inMemory, ok)
	}
	if got, ok := planner.Lookup("sort").DocumentOK(); !ok || got.Lookup("satisfied").Boolean() {
		t.Fatalf("sort=%s want unsatisfied public sort descriptor", got)
	}
	bounds := planner.Lookup("scanBounds").Array()
	values, err := bounds.Values()
	if err != nil || len(values) != 2 {
		t.Fatalf("scanBounds=%s err=%v", bounds, err)
	}
	rangeBound := values[0].Document()
	if count, ok := rangeBound.Lookup("valueCount").Int32OK(); !ok || count != 1 {
		t.Fatalf("range valueCount=%d ok=%v", count, ok)
	}
	if inclusive, ok := rangeBound.Lookup("lowerInclusive").BooleanOK(); !ok || !inclusive {
		t.Fatalf("range lowerInclusive=%v ok=%v", inclusive, ok)
	}
}

func TestMongoExplainScanBoundsSummarizeInValuesWithoutLeakingValues(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	resp := serveCommand(t, server, 108, bson.D{
		{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "city", Value: bson.D{{Key: "$in", Value: bson.A{"hnl", "sea"}}}}}}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, resp)
	bound := bson.Raw(resp).Lookup("queryPlanner").Document().Lookup("scanBounds").Array().Index(0).Document()
	if count, ok := bound.Lookup("valueCount").Int32OK(); !ok || count != 2 {
		t.Fatalf("in valueCount=%d ok=%v", count, ok)
	}
	fingerprints := bound.Lookup("valueFingerprints").Array()
	values, err := fingerprints.Values()
	if err != nil || len(values) != 2 {
		t.Fatalf("fingerprints=%s err=%v", fingerprints, err)
	}
	left, _ := values[0].StringValueOK()
	right, _ := values[1].StringValueOK()
	if left == right || len(left) != 24 {
		t.Fatalf("fingerprints must distinguish bounded values: %q %q", left, right)
	}
	if bson.Raw(bound).Lookup("value").Type != 0 {
		t.Fatalf("bound leaked filter value: %s", bound)
	}
}

func TestMongoExplainReadCommandAdapters(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	for _, tc := range []struct {
		name  string
		inner bson.D
		want  int64
	}{
		{"count", bson.D{{Key: "count", Value: "users"}, {Key: "query", Value: bson.D{{Key: "city", Value: "hnl"}}}}, 2},
		{"distinct", bson.D{{Key: "distinct", Value: "users"}, {Key: "key", Value: "city"}, {Key: "query", Value: bson.D{{Key: "active", Value: true}}}}, 2},
		{"aggregate", bson.D{{Key: "aggregate", Value: "users"}, {Key: "pipeline", Value: bson.A{bson.D{{Key: "$match", Value: bson.D{{Key: "city", Value: "hnl"}}}}, bson.D{{Key: "$count", Value: "n"}}}}, {Key: "cursor", Value: bson.D{}}}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := serveCommand(t, server, 104, bson.D{{Key: "explain", Value: tc.inner}, {Key: "verbosity", Value: "executionStats"}, {Key: "$db", Value: "app"}})
			assertOK(t, resp)
			stats, ok := bson.Raw(resp).Lookup("executionStats").DocumentOK()
			if !ok {
				t.Fatalf("executionStats missing: %s", resp)
			}
			if got, ok := stats.Lookup("nReturned").Int64OK(); !ok || got != tc.want {
				t.Fatalf("nReturned=%d ok=%v want %d", got, ok, tc.want)
			}
		})
	}
}

func TestMongoExplainAggregateRejectsUnrepresentablePipelineSort(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	resp := serveCommand(t, server, 107, bson.D{{Key: "explain", Value: bson.D{{Key: "aggregate", Value: "users"}, {Key: "pipeline", Value: bson.A{bson.D{{Key: "$sort", Value: bson.D{{Key: "name", Value: int32(1)}}}}}}, {Key: "cursor", Value: bson.D{}}}}, {Key: "$db", Value: "app"}})
	assertCommandError(t, resp, "BadValue")
	planner, ok := bson.Raw(resp).Lookup("queryPlanner").DocumentOK()
	if !ok {
		t.Fatalf("rejected aggregate must retain planner context: %s", resp)
	}
	if reason, ok := planner.Lookup("rejectionReason").StringValueOK(); !ok || reason != "unsupported_aggregate_pipeline" {
		t.Fatalf("rejection reason=%q ok=%v", reason, ok)
	}
}

func TestMongoExplainAdaptiveMultiIndexPlanDoesNotClaimAnUnexecutedWinner(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	command := bson.D{{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "city", Value: "hnl"}, {Key: "age", Value: bson.D{{Key: "$gte", Value: int32(20)}}}}}}}, {Key: "$db", Value: "app"}}
	plannerOnly := serveCommand(t, server, 105, command)
	assertOK(t, plannerOnly)
	planner := bson.Raw(plannerOnly).Lookup("queryPlanner").Document()
	if stage, ok := planner.Lookup("winningPlan").Document().Lookup("stage").StringValueOK(); !ok || stage != "adaptive_candidate_selection" {
		t.Fatalf("planner stage=%q ok=%v want adaptive candidate selection", stage, ok)
	}
	if candidatePlans, ok := planner.Lookup("candidatePlans").ArrayOK(); !ok || len(candidatePlans) < 2 {
		t.Fatalf("candidatePlans=%v ok=%v want >=2", candidatePlans, ok)
	}
	command = append(command, bson.E{Key: "verbosity", Value: "executionStats"})
	executed := serveCommand(t, server, 106, command)
	assertOK(t, executed)
	if got, ok := bson.Raw(executed).Lookup("queryPlanner").Document().Lookup("winningPlan").Document().Lookup("stage").StringValueOK(); ok && got == "adaptive_candidate_selection" {
		t.Fatalf("executionStats must report actual executor winner: %s", executed)
	}
}

func TestMongoExplainClusterRejectsBeforeLocalCollectionObservation(t *testing.T) {
	server, submitter := newMongoPlacementRouteTestServer(t, raftplacement.PlacementModeRingV1)
	lookups := 0
	server.clusterCollectionLookupHook = func() { lookups++ }
	resp := serveCommand(t, server, 102, bson.D{
		{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, resp, "BadValue")
	if lookups != 0 {
		t.Fatalf("local collection lookups=%d want 0", lookups)
	}
	_ = submitter // route preflight may resolve ownership, but must not open locally.
}
