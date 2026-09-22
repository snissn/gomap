package mongogateway

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
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
		{{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "$db", Value: "app"}}}, {Key: "verbosity", Value: ""}, {Key: "$db", Value: "app"}},
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
	if !ok {
		t.Fatalf("rejected planner missing: %s", rejected)
	}
	if stage, stageOK := planner.Lookup("winningPlan").Document().Lookup("stage").StringValueOK(); !stageOK || stage != "unsupported_route" {
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
	if got, ok := stats.Lookup("truncated").BooleanOK(); !ok || !got {
		t.Fatalf("truncated=%v ok=%v want true", got, ok)
	}
	for _, field := range []string{"nReturned", "candidateDocumentsExamined", "candidateDocumentsMaterialized", "cursorDocumentsMaterialized", "executionTimeMillis", "scanCap"} {
		if _, ok := stats.Lookup(field).Int64OK(); !ok {
			t.Fatalf("rejected executionStats missing integer %q: %s", field, stats)
		}
	}
	if _, ok := bson.Raw(resp).Lookup("queryPlanner").DocumentOK(); !ok {
		t.Fatalf("queryPlanner missing from rejected explain: %s", resp)
	}
}

func TestMongoExplainIndexedRangeOverflowRejectsRatherThanPrefix(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	server.MaxFindScanDocuments = 2
	resp := serveCommand(t, server, 102, bson.D{
		{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int64(0)}}}}}}},
		{Key: "verbosity", Value: "executionStats"}, {Key: "$db", Value: "app"},
	})
	assertCommandError(t, resp, "BadValue")
	stats := bson.Raw(resp).Lookup("executionStats").Document()
	if got, ok := stats.Lookup("rejectionReason").StringValueOK(); !ok || got != "scan_cap_exceeded" {
		t.Fatalf("range overflow rejection=%q ok=%v want scan_cap_exceeded", got, ok)
	}
}

func TestMongoExplainKnownEmptyNaNRangeAvoidsBoundedScan(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	server.MaxFindScanDocuments = 1
	command := bson.D{
		{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: math.NaN()}}}}}}},
		{Key: "$db", Value: "app"},
	}
	planner := serveCommand(t, server, 102, command)
	assertOK(t, planner)
	if got := bson.Raw(planner).Lookup("queryPlanner").Document().Lookup("winningPlan").Document().Lookup("stage").StringValue(); got != "secondary_range_lookup" {
		t.Fatalf("queryPlanner stage=%q want secondary_range_lookup: %s", got, planner)
	}
	command = append(command, bson.E{Key: "verbosity", Value: "executionStats"})
	statsResponse := serveCommand(t, server, 102, command)
	assertOK(t, statsResponse)
	stats := bson.Raw(statsResponse).Lookup("executionStats").Document()
	if got, ok := stats.Lookup("nReturned").Int64OK(); !ok || got != 0 {
		t.Fatalf("nReturned=%d ok=%v want 0: %s", got, ok, statsResponse)
	}
	if got, ok := stats.Lookup("candidateDocumentsExamined").Int64OK(); !ok || got != 0 {
		t.Fatalf("candidateDocumentsExamined=%d ok=%v want 0: %s", got, ok, statsResponse)
	}
}

func TestExplainExecutionRejectionClassifiesOnlyScanCapSentinel(t *testing.T) {
	if got := explainExecutionRejectionReason(errors.New("bounded scan and candidate set exceeded in unrelated adapter")); got != "execution_rejected" {
		t.Fatalf("text-only rejection=%q want execution_rejected", got)
	}
	if got := explainExecutionRejectionReason(fmt.Errorf("wrapped: %w", errMongoFindScanCapExceeded)); got != "scan_cap_exceeded" {
		t.Fatalf("sentinel rejection=%q want scan_cap_exceeded", got)
	}
}

func TestMongoExplainNullPredicateDoesNotAdvertiseSecondaryIndex(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	resp := serveCommand(t, server, 102, bson.D{
		{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "city", Value: nil}}}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, resp)
	planner := bson.Raw(resp).Lookup("queryPlanner").Document()
	winning := planner.Lookup("winningPlan").Document()
	if got, ok := winning.Lookup("stage").StringValueOK(); !ok || got != "bounded_scan" {
		t.Fatalf("null predicate plan=%q ok=%v want bounded_scan", got, ok)
	}
	items, err := planner.Lookup("usableIndexes").Array().Values()
	if err != nil {
		t.Fatalf("usableIndexes values: %v", err)
	}
	for _, item := range items {
		if item.Document().Lookup("name").StringValue() == "city_1" {
			t.Fatalf("null predicate advertised city index: %s", planner)
		}
	}
}

func TestMongoExplainResidualsAndCursorOptionsMatchReadAdmission(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	for _, tc := range []struct {
		name string
		cmd  bson.D
		want bool
	}{
		{name: "bounded scan predicate", cmd: bson.D{{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "active", Value: true}}}}}, {Key: "$db", Value: "app"}}, want: true},
		{name: "two sided indexed range", cmd: bson.D{{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int64(36)}, {Key: "$lt", Value: int64(43)}}}}}}}, {Key: "$db", Value: "app"}}, want: false},
		{name: "multiple same kind predicates", cmd: bson.D{{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "$and", Value: bson.A{bson.D{{Key: "age", Value: int64(36)}}, bson.D{{Key: "age", Value: int64(37)}}}}}}}}, {Key: "$db", Value: "app"}}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := serveCommand(t, server, 104, tc.cmd)
			assertOK(t, resp)
			got, ok := bson.Raw(resp).Lookup("queryPlanner").Document().Lookup("winningPlan").Document().Lookup("residualFilter").BooleanOK()
			if !ok || got != tc.want {
				t.Fatalf("residualFilter=%v ok=%v want %v: %s", got, ok, tc.want, resp)
			}
			execution := append(append(bson.D(nil), tc.cmd...), bson.E{Key: "verbosity", Value: "executionStats"})
			stats := serveCommand(t, server, 104, execution)
			assertOK(t, stats)
			got, ok = bson.Raw(stats).Lookup("queryPlanner").Document().Lookup("winningPlan").Document().Lookup("residualFilter").BooleanOK()
			if !ok || got != tc.want {
				t.Fatalf("execution residualFilter=%v ok=%v want %v: %s", got, ok, tc.want, stats)
			}
		})
	}
	for _, cmd := range []bson.D{
		{{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "batchSize", Value: "bad"}}}, {Key: "$db", Value: "app"}},
		{{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "singleBatch", Value: "bad"}}}, {Key: "$db", Value: "app"}},
		{{Key: "explain", Value: bson.D{{Key: "aggregate", Value: "users"}, {Key: "pipeline", Value: bson.A{}}, {Key: "cursor", Value: bson.D{{Key: "batchSize", Value: int32(-1)}}}}}, {Key: "$db", Value: "app"}},
	} {
		assertCommandError(t, serveCommand(t, server, 104, cmd), "BadValue")
	}
}

func TestMongoExplainSameIndexProbeAccountingAndWinnerStage(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	command := bson.D{
		{Key: "explain", Value: bson.D{
			{Key: "find", Value: "users"},
			{Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$in", Value: bson.A{int64(36), int64(37), int64(42)}}, {Key: "$lt", Value: int64(37)}}}}},
		}},
		{Key: "$db", Value: "app"},
	}
	plannerOnly := serveCommand(t, server, 105, command)
	assertOK(t, plannerOnly)
	plannerWinning := bson.Raw(plannerOnly).Lookup("queryPlanner").Document().Lookup("winningPlan").Document()
	if got, ok := plannerWinning.Lookup("stage").StringValueOK(); !ok || got != "adaptive_candidate_selection" {
		t.Fatalf("queryPlanner stage=%q ok=%v want adaptive_candidate_selection: %s", got, ok, plannerOnly)
	}
	if residual, ok := plannerWinning.Lookup("residualFilter").BooleanOK(); !ok || !residual {
		t.Fatalf("queryPlanner residual=%v ok=%v want true", residual, ok)
	}
	command = append(command, bson.E{Key: "verbosity", Value: "executionStats"})
	resp := serveCommand(t, server, 105, command)
	assertOK(t, resp)
	winning := bson.Raw(resp).Lookup("queryPlanner").Document().Lookup("winningPlan").Document()
	if got, ok := winning.Lookup("stage").StringValueOK(); !ok || got != "secondary_range_lookup" {
		t.Fatalf("winner stage=%q ok=%v want secondary_range_lookup: %s", got, ok, resp)
	}
	if residual, ok := winning.Lookup("residualFilter").BooleanOK(); !ok || !residual {
		t.Fatalf("execution winner residual=%v ok=%v want true", residual, ok)
	}
	stats := bson.Raw(resp).Lookup("executionStats").Document()
	if got, ok := stats.Lookup("candidateDocumentsMaterialized").Int64OK(); !ok || got < 4 {
		t.Fatalf("materialized=%d ok=%v want all equality/range probes", got, ok)
	}
}

func TestMongoExplainProbeCandidatesUseExecutableEligibility(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	incompatibleRange := serveCommand(t, server, 106, bson.D{{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$in", Value: bson.A{int64(36)}}, {Key: "$gt", Value: 36.5}}}}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, incompatibleRange)
	if got := bson.Raw(incompatibleRange).Lookup("queryPlanner").Document().Lookup("winningPlan").Document().Lookup("stage").StringValue(); got != "secondary_equality_lookup" {
		t.Fatalf("incompatible range stage=%q want equality", got)
	}
	incompatibleRange = serveCommand(t, server, 106, bson.D{{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$in", Value: bson.A{int64(36)}}, {Key: "$gt", Value: 36.5}}}}}}}, {Key: "verbosity", Value: "executionStats"}, {Key: "$db", Value: "app"}})
	assertOK(t, incompatibleRange)
	if got := bson.Raw(incompatibleRange).Lookup("queryPlanner").Document().Lookup("winningPlan").Document().Lookup("stage").StringValue(); got != "secondary_equality_lookup" {
		t.Fatalf("incompatible range execution stage=%q want equality", got)
	}
	mixedNull := serveCommand(t, server, 106, bson.D{{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$in", Value: bson.A{nil, int64(36)}}, {Key: "$lt", Value: int64(43)}}}}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, mixedNull)
	mixedPlanner := bson.Raw(mixedNull).Lookup("queryPlanner").Document()
	if got := mixedPlanner.Lookup("winningPlan").Document().Lookup("stage").StringValue(); got != "secondary_range_lookup" {
		t.Fatalf("null equality candidate stage=%q want range only: %s", got, mixedNull)
	}
	if got := mixedPlanner.Lookup("usableIndexes").Array().Index(0).Document().Lookup("kind").StringValue(); got != "secondary_range_lookup" {
		t.Fatalf("null equality usable kind=%q want range only: %s", got, mixedNull)
	}
	assertOK(t, serveCommand(t, server, 106, bson.D{{Key: "createIndexes", Value: "users"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "age", Value: int32(1)}}}, {Key: "name", Value: "age_second"}, {Key: "treedbValueType", Value: "int64"}}}}, {Key: "$db", Value: "app"}}))
	command := bson.D{{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int64(36)}}}}}}}, {Key: "$db", Value: "app"}}
	planner := serveCommand(t, server, 106, command)
	assertOK(t, planner)
	if got := bson.Raw(planner).Lookup("queryPlanner").Document().Lookup("winningPlan").Document().Lookup("stage").StringValue(); got != "adaptive_candidate_selection" {
		t.Fatalf("duplicate range planner stage=%q want adaptive", got)
	}
	command = append(command, bson.E{Key: "verbosity", Value: "executionStats"})
	stats := serveCommand(t, server, 106, command)
	assertOK(t, stats)
	winning := bson.Raw(stats).Lookup("queryPlanner").Document().Lookup("winningPlan").Document()
	if got := winning.Lookup("stage").StringValue(); got != "secondary_range_lookup" {
		t.Fatalf("duplicate range winner=%q want range", got)
	}
	if residual, ok := winning.Lookup("residualFilter").BooleanOK(); !ok || residual {
		t.Fatalf("duplicate range residual=%v ok=%v want false", residual, ok)
	}
}

func TestServerRejectsExplainOPMsgFeatures(t *testing.T) {
	command := mustDocument(t, bson.D{{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}}}, {Key: "$db", Value: "app"}})
	for _, tc := range []struct {
		name  string
		build func() ([]byte, error)
	}{
		{name: "moreToCome", build: func() ([]byte, error) { return wire.AppendMsgMessage(nil, 102, 0, wire.MsgFlagMoreToCome, command) }},
		{name: "documentSequences", build: func() ([]byte, error) {
			return wire.AppendMsgMessageWithSequences(nil, 102, 0, 0, command, []wire.DocumentSequence{{Identifier: "ignored", Documents: []wire.Document{mustDocument(t, bson.D{{Key: "x", Value: int32(1)}})}}})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request, err := tc.build()
			if err != nil {
				t.Fatal(err)
			}
			rw := &readWriter{r: bytes.NewReader(request)}
			if err := NewServer().ServeOne(rw); !errors.Is(err, wire.ErrUnsupported) || rw.w.Len() != 0 {
				t.Fatalf("ServeOne err=%v responseBytes=%d want ErrUnsupported/0", err, rw.w.Len())
			}
		})
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

func TestMongoExplainAggregateRepresentsInitialPipelineSort(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	resp := serveCommand(t, server, 107, bson.D{{Key: "explain", Value: bson.D{{Key: "aggregate", Value: "users"}, {Key: "pipeline", Value: bson.A{bson.D{{Key: "$sort", Value: bson.D{{Key: "name", Value: int32(1)}}}}}}, {Key: "cursor", Value: bson.D{}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, resp)
	sortInfo, ok := bson.Raw(resp).Lookup("queryPlanner").Document().Lookup("sort").DocumentOK()
	if !ok || sortInfo.Lookup("field").StringValue() != "name" {
		t.Fatalf("initial aggregate sort missing from explain: %s", resp)
	}
}

func TestMongoExplainAggregateRejectsNonInitialPipelineSort(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	resp := serveCommand(t, server, 108, bson.D{{Key: "explain", Value: bson.D{{Key: "aggregate", Value: "users"}, {Key: "pipeline", Value: bson.A{
		bson.D{{Key: "$match", Value: bson.D{{Key: "city", Value: "hnl"}}}},
		bson.D{{Key: "$project", Value: bson.D{{Key: "city", Value: int32(1)}}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "city", Value: int32(1)}}}},
	}}, {Key: "cursor", Value: bson.D{}}}}, {Key: "$db", Value: "app"}})
	assertCommandError(t, resp, "BadValue")
	if got, ok := bson.Raw(resp).Lookup("queryPlanner").Document().Lookup("rejectionReason").StringValueOK(); !ok || got != "unsupported_aggregate_pipeline" {
		t.Fatalf("reason=%q ok=%v response=%s", got, ok, resp)
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
	executedPlanner := bson.Raw(executed).Lookup("queryPlanner").Document()
	if got, ok := executedPlanner.Lookup("winningPlan").Document().Lookup("stage").StringValueOK(); ok && got == "adaptive_candidate_selection" {
		t.Fatalf("executionStats must report actual executor winner: %s", executed)
	}
	if !executedPlanner.Lookup("candidatePlans").IsZero() {
		t.Fatalf("candidatePlans must be omitted once execution resolves the adaptive winner: %s", executed)
	}
}

func TestMongoExplainAdaptiveCompoundWinnerRefreshesSortSatisfaction(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 1061, bson.D{{Key: "createIndexes", Value: "users"}, {Key: "indexes", Value: bson.A{
		bson.D{{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}, {Key: "age", Value: int32(1)}}}, {Key: "name", Value: "city_age"}},
		bson.D{{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}, {Key: "name", Value: int32(1)}}}, {Key: "name", Value: "city_name"}},
	}}, {Key: "$db", Value: "app"}}))
	command := bson.D{{Key: "explain", Value: bson.D{
		{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "city", Value: "hnl"}}},
		{Key: "sort", Value: bson.D{{Key: "age", Value: int32(1)}}},
	}}, {Key: "verbosity", Value: "executionStats"}, {Key: "$db", Value: "app"}}
	response := serveCommand(t, server, 1062, command)
	assertOK(t, response)
	planner := bson.Raw(response).Lookup("queryPlanner").Document()
	winning := planner.Lookup("winningPlan").Document()
	if got := winning.Lookup("stage").StringValue(); got != "compound_index_scan" {
		t.Fatalf("winning stage=%q want compound_index_scan: %s", got, response)
	}
	if inMemory, ok := winning.Lookup("inMemorySort").BooleanOK(); !ok || inMemory {
		t.Fatalf("winning inMemorySort=%v ok=%v want false: %s", inMemory, ok, response)
	}
	if satisfied, ok := planner.Lookup("sort").Document().Lookup("satisfied").BooleanOK(); !ok || !satisfied {
		t.Fatalf("planner sort satisfied=%v ok=%v want true: %s", satisfied, ok, response)
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
