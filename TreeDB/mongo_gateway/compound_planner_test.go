package mongogateway

import (
	"fmt"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// BenchmarkMongoCompoundPlannerCursor is the reproducible local shape used by
// #4065: an equality prefix, descending range/order and several getMore
// batches.  Setup is outside the timer; the reported allocations cover only
// planned cursor execution and BSON batch materialization.
func BenchmarkMongoCompoundPlannerCursor(b *testing.B) {
	for _, variant := range []string{"compound_stream", "bounded_scan_sort", "single_field"} {
		b.Run(variant, func(b *testing.B) { benchmarkMongoCompoundPlannerVariant(b, variant) })
	}
}

func benchmarkMongoCompoundPlannerVariant(b *testing.B, variant string) {
	server := newMongoCompatibilityMatrixServer(b)
	if variant != "bounded_scan_sort" {
		key := bson.D{{Key: "tenant", Value: int32(1)}}
		name := "tenant_1"
		index := bson.D{{Key: "key", Value: key}, {Key: "name", Value: name}, {Key: "treedbValueType", Value: "string"}}
		if variant == "compound_stream" {
			index = bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "score", Value: int32(-1)}}}, {Key: "name", Value: "tenant_score"}}
		}
		assertOK(b, serveCommand(b, server, 406590, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{index}}, {Key: "$db", Value: "app"}}))
	}
	docs := make(bson.A, 0, 128)
	for i := 0; i < cap(docs); i++ {
		docs = append(docs, bson.D{{Key: "_id", Value: fmt.Sprintf("%03d", i)}, {Key: "tenant", Value: "t"}, {Key: "score", Value: int32(i)}, {Key: "payload", Value: "benchmark"}})
	}
	assertOK(b, serveCommand(b, server, 406591, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: docs}, {Key: "$db", Value: "app"}}))
	command := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "sort", Value: bson.D{{Key: "score", Value: int32(-1)}}}, {Key: "skip", Value: int64(16)}, {Key: "limit", Value: int64(32)}, {Key: "batchSize", Value: int32(8)}, {Key: "$db", Value: "app"}}
	drain := func(response wire.Document, requestID int32) {
		assertOK(b, response)
		batches := append([]bson.Raw(nil), cursorFirstBatch(b, response)...)
		for cursorID := cursorIDFromResponse(b, response); cursorID != 0; {
			next := serveCommand(b, server, requestID, bson.D{{Key: "getMore", Value: cursorID}, {Key: "collection", Value: "events"}, {Key: "batchSize", Value: int32(8)}, {Key: "$db", Value: "app"}})
			assertOK(b, next)
			batches = append(batches, cursorNextBatch(b, next)...)
			cursorID = cursorIDFromResponse(b, next)
		}
		if len(batches) != 32 {
			b.Fatalf("%s result count=%d want 32", variant, len(batches))
		}
		for i, doc := range batches {
			want := fmt.Sprintf("%03d", 111-i)
			if got, ok := doc.Lookup("_id").StringValueOK(); !ok || got != want {
				b.Fatalf("%s result[%d]._id=%q ok=%v want %q", variant, i, got, ok, want)
			}
		}
	}
	// Preflight asserts the identical visible result before timing and records
	// the gateway-owned counters from the same command shape.
	explain := serveCommand(b, server, 406592, bson.D{{Key: "explain", Value: command}, {Key: "verbosity", Value: "executionStats"}, {Key: "$db", Value: "app"}})
	assertOK(b, explain)
	stats := bson.Raw(explain).Lookup("executionStats").Document()
	metric := func(name string) int64 {
		value, ok := stats.Lookup(name).Int64OK()
		if !ok || value <= 0 {
			b.Fatalf("%s executionStats.%s=%d ok=%v want positive: %s", variant, name, value, ok, explain)
		}
		return value
	}
	if returned := metric("nReturned"); returned != 32 {
		b.Fatalf("%s executionStats.nReturned=%d want 32: %s", variant, returned, explain)
	}
	candidatesExamined := metric("candidateDocumentsExamined")
	documentsMaterialized := metric("candidateDocumentsMaterialized")
	materializedBytes, ok := stats.Lookup("candidateMaterializedBytes").Int64OK()
	if !ok || materializedBytes < 0 {
		b.Fatalf("%s executionStats.candidateMaterializedBytes=%d ok=%v want non-negative: %s", variant, materializedBytes, ok, explain)
	}
	drain(serveCommand(b, server, 406599, command), 406598)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		drain(serveCommand(b, server, int32(406600+i), command), int32(506600+i))
	}
	b.ReportMetric(float64(candidatesExamined), "candidates_examined/op")
	b.ReportMetric(float64(documentsMaterialized), "documents_materialized/op")
	b.ReportMetric(float64(materializedBytes), "materialized_bytes/op")
}

// TestMongoCompoundPlanEqualityPrefixRangeAndSort is the first #4065 contract
// test.  The legacy planner intentionally declines this BSON-v2 definition;
// the standalone compound planner must select it and keep the index order.
func TestMongoCompoundPlanEqualityPrefixRangeAndSort(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 406501, bson.D{
		{Key: "createIndexes", Value: "events"},
		{Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "createdAt", Value: int32(-1)}}}, {Key: "name", Value: "tenant_created_desc"}}}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 406502, bson.D{
		{Key: "insert", Value: "events"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(1)}, {Key: "active", Value: true}},
			bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(2)}, {Key: "active", Value: true}},
			bson.D{{Key: "_id", Value: "c"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(3)}, {Key: "active", Value: false}},
			bson.D{{Key: "_id", Value: "d"}, {Key: "tenant", Value: "other"}, {Key: "createdAt", Value: int32(4)}, {Key: "active", Value: true}},
		}},
		{Key: "$db", Value: "app"},
	}))

	command := bson.D{
		{Key: "find", Value: "events"},
		{Key: "filter", Value: bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "tenant", Value: "acme"}},
			bson.D{{Key: "createdAt", Value: bson.D{{Key: "$gte", Value: int32(1)}}}},
			bson.D{{Key: "createdAt", Value: bson.D{{Key: "$lte", Value: int32(3)}}}},
			bson.D{{Key: "active", Value: true}},
		}}}},
		{Key: "sort", Value: bson.D{{Key: "createdAt", Value: int32(-1)}}},
		{Key: "$db", Value: "app"},
	}
	assertBatchIDs(t, cursorFirstBatch(t, serveCommand(t, server, 406503, command)), []string{"b", "a"})

	explain := bson.D{{Key: "explain", Value: command}, {Key: "verbosity", Value: "executionStats"}, {Key: "$db", Value: "app"}}
	response := serveCommand(t, server, 406504, explain)
	assertOK(t, response)
	winning := bson.Raw(response).Lookup("queryPlanner").Document().Lookup("winningPlan").Document()
	if got, ok := winning.Lookup("stage").StringValueOK(); !ok || got != "compound_index_scan" {
		t.Fatalf("winning stage=%q ok=%v want compound_index_scan: %s", got, ok, response)
	}
	if got, ok := winning.Lookup("indexName").StringValueOK(); !ok || got != "tenant_created_desc" {
		t.Fatalf("winning index=%q ok=%v want tenant_created_desc: %s", got, ok, response)
	}
	if inMemory, ok := winning.Lookup("inMemorySort").BooleanOK(); !ok || inMemory {
		t.Fatalf("inMemorySort=%v ok=%v want false: %s", inMemory, ok, response)
	}
	if bytes, ok := bson.Raw(response).Lookup("executionStats").Document().Lookup("candidateMaterializedBytes").Int64OK(); !ok || bytes <= 0 {
		t.Fatalf("candidateMaterializedBytes=%d ok=%v want positive: %s", bytes, ok, response)
	}
	// The complete reverse of the declared index order is equally compatible.
	reverse := bson.D{
		{Key: "find", Value: "events"},
		{Key: "filter", Value: bson.D{{Key: "tenant", Value: "acme"}}},
		{Key: "sort", Value: bson.D{{Key: "createdAt", Value: int32(1)}}},
		{Key: "$db", Value: "app"},
	}
	assertBatchIDs(t, cursorFirstBatch(t, serveCommand(t, server, 406505, reverse)), []string{"a", "b", "c"})
}

func TestMongoCompoundPlanReverseTieUsesStableDocumentID(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 4065051, bson.D{
		{Key: "createIndexes", Value: "events"},
		{Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "score", Value: int32(-1)}}}, {Key: "name", Value: "tenant_score_desc"}}}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 4065052, bson.D{
		{Key: "insert", Value: "events"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "acme"}, {Key: "score", Value: int32(1)}},
			bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "acme"}, {Key: "score", Value: int32(1)}},
			bson.D{{Key: "_id", Value: "c"}, {Key: "tenant", Value: "acme"}, {Key: "score", Value: int32(2)}},
		}}, {Key: "$db", Value: "app"},
	}))
	// score:1 is the complete reverse of the descending index. Equal scores
	// still use the gateway's stable ascending _id tiebreaker.
	response := serveCommand(t, server, 4065053, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "acme"}}}, {Key: "sort", Value: bson.D{{Key: "score", Value: int32(1)}}}, {Key: "$db", Value: "app"}})
	assertBatchIDs(t, cursorFirstBatch(t, response), []string{"a", "b", "c"})
}

func TestMongoCompoundPlanSortNormalizesMissingAndNullTiesBeforeLimit(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 40650531, bson.D{
		{Key: "createIndexes", Value: "events"},
		{Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "score", Value: int32(1)}}}, {Key: "name", Value: "tenant_score"}}}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 40650532, bson.D{
		{Key: "insert", Value: "events"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "z"}, {Key: "tenant", Value: "t"}},
			bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "t"}, {Key: "score", Value: nil}},
			bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "t"}, {Key: "score", Value: int32(1)}},
		}}, {Key: "$db", Value: "app"},
	}))
	base := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "limit", Value: int32(1)}, {Key: "$db", Value: "app"}}
	asc := append(bson.D(nil), base[:2]...)
	asc = append(asc, bson.E{Key: "sort", Value: bson.D{{Key: "score", Value: int32(1)}}})
	asc = append(asc, base[2:]...)
	assertBatchIDs(t, cursorFirstBatch(t, serveCommand(t, server, 40650533, asc)), []string{"a"})
	desc := append(bson.D(nil), base[:2]...)
	desc = append(desc, bson.E{Key: "sort", Value: bson.D{{Key: "score", Value: int32(-1)}}})
	desc = append(desc, base[2:]...)
	assertBatchIDs(t, cursorFirstBatch(t, serveCommand(t, server, 40650534, desc)), []string{"b"})
	// Once the numeric group is skipped, the equivalent missing/null group uses
	// its ascending _id tie order in either physical direction.
	withSkip := append(bson.D(nil), desc[:3]...)
	withSkip = append(withSkip, bson.E{Key: "skip", Value: int32(1)})
	withSkip = append(withSkip, desc[3:]...)
	assertBatchIDs(t, cursorFirstBatch(t, serveCommand(t, server, 40650535, withSkip)), []string{"a"})
}

func TestMongoCompoundPlanInCanonicalNumericPrefixesShareCandidateBudget(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	server.MaxFindScanDocuments = 2
	assertOK(t, serveCommand(t, server, 40650536, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "score", Value: int32(1)}}}, {Key: "name", Value: "tenant_score"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 40650537, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: int32(1)}, {Key: "score", Value: int32(1)}}, bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: int32(1)}, {Key: "score", Value: int32(2)}}}}, {Key: "$db", Value: "app"}}))
	response := serveCommand(t, server, 40650538, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: bson.D{{Key: "$in", Value: bson.A{int32(1), int64(1)}}}}}}, {Key: "$db", Value: "app"}})
	assertBatchIDs(t, cursorFirstBatch(t, response), []string{"a", "b"})
}

func TestMongoCompoundNonStreamingPlansWalkIndexOnceAndReportExactWork(t *testing.T) {
	tests := []struct {
		name         string
		filter       bson.D
		documents    bson.A
		wantExamined int64
	}{
		{
			name:   "residual",
			filter: bson.D{{Key: "tenant", Value: "t"}, {Key: "active", Value: true}},
			documents: bson.A{
				bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "t"}, {Key: "score", Value: int32(1)}, {Key: "active", Value: true}},
				bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "t"}, {Key: "score", Value: int32(2)}, {Key: "active", Value: false}},
				bson.D{{Key: "_id", Value: "c"}, {Key: "tenant", Value: "t"}, {Key: "score", Value: int32(3)}, {Key: "active", Value: true}},
			},
			wantExamined: 3,
		},
		{
			name:   "multi_in",
			filter: bson.D{{Key: "tenant", Value: bson.D{{Key: "$in", Value: bson.A{"a", "b"}}}}},
			documents: bson.A{
				bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "a"}, {Key: "score", Value: int32(1)}},
				bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "b"}, {Key: "score", Value: int32(2)}},
			},
			wantExamined: 2,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := newMongoCompatibilityMatrixServer(t)
			server.MaxFindScanDocuments = int(tc.wantExamined)
			assertOK(t, serveCommand(t, server, 40650540, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "score", Value: int32(1)}}}, {Key: "name", Value: "tenant_score"}}}}, {Key: "$db", Value: "app"}}))
			assertOK(t, serveCommand(t, server, 40650541, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: tc.documents}, {Key: "$db", Value: "app"}}))
			walks := 0
			server.compoundIndexPlanScanHook = func() { walks++ }
			command := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: tc.filter}, {Key: "batchSize", Value: int32(1)}, {Key: "$db", Value: "app"}}
			assertOK(t, serveCommand(t, server, 40650542, command))
			if walks != 1 {
				t.Fatalf("non-streaming %s index walks=%d want 1", tc.name, walks)
			}
			walks = 0
			explain := serveCommand(t, server, 40650543, bson.D{{Key: "explain", Value: command}, {Key: "verbosity", Value: "executionStats"}, {Key: "$db", Value: "app"}})
			assertOK(t, explain)
			if walks != 1 {
				t.Fatalf("executionStats %s index walks=%d want 1", tc.name, walks)
			}
			stats := bson.Raw(explain).Lookup("executionStats").Document()
			if got, ok := stats.Lookup("candidateDocumentsExamined").Int64OK(); !ok || got != tc.wantExamined {
				t.Fatalf("executionStats %s examined=%d ok=%v want %d: %s", tc.name, got, ok, tc.wantExamined, explain)
			}
		})
	}
}

func TestMongoCompoundMultiSortMissingNullFallsBackBeforeLimit(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 40650550, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "x", Value: int32(1)}, {Key: "y", Value: int32(1)}}}, {Key: "name", Value: "tenant_x_y"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 40650557, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "x", Value: int32(-1)}, {Key: "y", Value: int32(-1)}}}, {Key: "name", Value: "tenant_x_y_desc"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 40650551, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "z"}, {Key: "tenant", Value: "t"}, {Key: "y", Value: int32(100)}},
		bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "t"}, {Key: "x", Value: nil}, {Key: "y", Value: int32(0)}},
	}}, {Key: "$db", Value: "app"}}))
	command := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "sort", Value: bson.D{{Key: "x", Value: int32(1)}, {Key: "y", Value: int32(1)}}}, {Key: "limit", Value: int32(1)}, {Key: "$db", Value: "app"}}
	assertBatchIDs(t, cursorFirstBatch(t, serveCommand(t, server, 40650552, command)), []string{"a"})
	explain := serveCommand(t, server, 40650553, bson.D{{Key: "explain", Value: command}, {Key: "verbosity", Value: "executionStats"}, {Key: "$db", Value: "app"}})
	assertOK(t, explain)
	winning := bson.Raw(explain).Lookup("queryPlanner").Document().Lookup("winningPlan").Document()
	if sorted, ok := winning.Lookup("inMemorySort").BooleanOK(); !ok || !sorted {
		t.Fatalf("multi-sort inMemorySort=%v ok=%v want true: %s", sorted, ok, explain)
	}
	// The reverse physical encoding distinguishes missing from null too.  The
	// gateway comparator ties those values before considering y, so pagination
	// must take the in-memory path in either direction.
	reverse := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "sort", Value: bson.D{{Key: "x", Value: int32(-1)}, {Key: "y", Value: int32(-1)}}}, {Key: "limit", Value: int32(1)}, {Key: "$db", Value: "app"}}
	assertBatchIDs(t, cursorFirstBatch(t, serveCommand(t, server, 40650558, reverse)), []string{"z"})
	reverseExplain := serveCommand(t, server, 40650559, bson.D{{Key: "explain", Value: reverse}, {Key: "verbosity", Value: "executionStats"}, {Key: "$db", Value: "app"}})
	assertOK(t, reverseExplain)
	reverseWinning := bson.Raw(reverseExplain).Lookup("queryPlanner").Document().Lookup("winningPlan").Document()
	if sorted, ok := reverseWinning.Lookup("inMemorySort").BooleanOK(); !ok || !sorted {
		t.Fatalf("reverse multi-sort inMemorySort=%v ok=%v want true: %s", sorted, ok, reverseExplain)
	}
}

func TestMongoCompoundHintedExplainExcludesPrimaryCandidate(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 40650554, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "score", Value: int32(1)}}}, {Key: "name", Value: "tenant_score"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 40650555, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "t"}, {Key: "score", Value: int32(1)}}}}, {Key: "$db", Value: "app"}}))
	command := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "t"}}}, {Key: "hint", Value: "tenant_score"}, {Key: "$db", Value: "app"}}
	response := serveCommand(t, server, 40650556, bson.D{{Key: "explain", Value: command}, {Key: "verbosity", Value: "queryPlanner"}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	planner := bson.Raw(response).Lookup("queryPlanner").Document()
	if stage, ok := planner.Lookup("winningPlan").Document().Lookup("stage").StringValueOK(); !ok || stage != "compound_index_scan" {
		t.Fatalf("hinted winning stage=%q ok=%v want compound_index_scan: %s", stage, ok, response)
	}
	if candidatePlans := planner.Lookup("candidatePlans"); candidatePlans.Type != 0 {
		candidates, err := candidatePlans.Array().Values()
		if err != nil {
			t.Fatalf("hinted candidatePlans: %v: %s", err, response)
		}
		for _, candidate := range candidates {
			if stage, ok := candidate.Document().Lookup("stage").StringValueOK(); ok && stage == "primary_lookup" {
				t.Fatalf("hinted explain retained primary candidate: %s", response)
			}
		}
	}
}

func TestMongoCompoundPlanOneSidedRangeRemainsResidualBeforeLimit(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 4065054, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "x", Value: int32(1)}}}, {Key: "name", Value: "tenant_x"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 4065055, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "missing"}, {Key: "tenant", Value: "t"}},
		bson.D{{Key: "_id", Value: "null"}, {Key: "tenant", Value: "t"}, {Key: "x", Value: nil}},
		bson.D{{Key: "_id", Value: "number"}, {Key: "tenant", Value: "t"}, {Key: "x", Value: int32(4)}},
	}}, {Key: "$db", Value: "app"}}))
	filter := bson.D{{Key: "tenant", Value: "t"}, {Key: "x", Value: bson.D{{Key: "$lt", Value: int32(5)}}}}
	response := serveCommand(t, server, 4065056, bson.D{
		{Key: "find", Value: "events"},
		{Key: "filter", Value: filter},
		{Key: "limit", Value: int32(1)}, {Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, response), []string{"number"})
}

func TestMongoCompoundPlanMultiValuePrefixDoesNotConsumeResultLimitAsProbeBudget(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 4065057, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "x", Value: int32(1)}}}, {Key: "name", Value: "tenant_x"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 4065058, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "t"}, {Key: "x", Value: "a"}}, bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "t"}, {Key: "x", Value: "b"}}}}, {Key: "$db", Value: "app"}}))
	filter := bson.D{{Key: "tenant", Value: "t"}, {Key: "x", Value: bson.D{{Key: "$in", Value: bson.A{"a", "b"}}}}}
	response := serveCommand(t, server, 4065059, bson.D{
		{Key: "find", Value: "events"},
		{Key: "filter", Value: filter},
		{Key: "limit", Value: int32(1)}, {Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	if got := len(cursorFirstBatch(t, response)); got != 1 {
		t.Fatalf("limited $in first batch=%d want 1", got)
	}
}

func TestMongoCompoundPlannerCursorStreamsIDsAcrossGetMore(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 406506, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "created", Value: int32(-1)}}}, {Key: "name", Value: "tenant_created"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 406507, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(1)}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(2)}},
		bson.D{{Key: "_id", Value: "c"}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(3)}},
	}}, {Key: "$db", Value: "app"}}))
	find := serveCommand(t, server, 406508, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "$and", Value: bson.A{bson.D{{Key: "tenant", Value: "t"}}, bson.D{{Key: "created", Value: bson.D{{Key: "$gte", Value: int32(1)}}}}, bson.D{{Key: "created", Value: bson.D{{Key: "$lte", Value: int32(3)}}}}}}}}, {Key: "sort", Value: bson.D{{Key: "created", Value: int32(-1)}}}, {Key: "skip", Value: int64(1)}, {Key: "limit", Value: int64(2)}, {Key: "batchSize", Value: int32(1)}, {Key: "$db", Value: "app"}})
	assertBatchIDs(t, cursorFirstBatch(t, find), []string{"b"})
	cursorID := cursorIDFromResponse(t, find)
	if cursorID == 0 {
		t.Fatal("cursor id=0 want resumable compound cursor")
	}
	server.cursorMu.Lock()
	cursor := server.cursors[cursorID]
	server.cursorMu.Unlock()
	if cursor == nil || len(cursor.docs) != 0 || len(cursor.compoundIDs) != 2 {
		t.Fatalf("cursor did not retain bounded IDs only: %#v", cursor)
	}
	next := serveCommand(t, server, 406509, bson.D{{Key: "getMore", Value: cursorID}, {Key: "collection", Value: "events"}, {Key: "batchSize", Value: int32(1)}, {Key: "$db", Value: "app"}})
	assertBatchIDs(t, cursorNextBatch(t, next), []string{"a"})
	if got := cursorIDFromResponse(t, next); got != 0 {
		t.Fatalf("final cursor id=%d want 0", got)
	}
	assertCommandError(t, serveCommand(t, server, 406510, bson.D{{Key: "getMore", Value: cursorID}, {Key: "collection", Value: "events"}, {Key: "$db", Value: "app"}}), "CursorNotFound")
}

func TestMongoCompoundPlannerCursorMaterializationCapFailsBeforeCursorPublication(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	server.MaxCursorRetainedBytes = 32
	assertOK(t, serveCommand(t, server, 4065110, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "created", Value: int32(-1)}}}, {Key: "name", Value: "tenant_created"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 4065111, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(1)}, {Key: "payload", Value: "too-large"}}}}, {Key: "$db", Value: "app"}}))
	resp := serveCommand(t, server, 4065112, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "sort", Value: bson.D{{Key: "created", Value: int32(-1)}}}, {Key: "batchSize", Value: int32(1)}, {Key: "$db", Value: "app"}})
	assertCommandError(t, resp, "BadValue")
	server.cursorMu.Lock()
	defer server.cursorMu.Unlock()
	if len(server.cursors) != 0 {
		t.Fatalf("published cursor despite materialization cap: %d", len(server.cursors))
	}
}

func TestMongoCompoundPlannerCursorChargesRetainedPredicatePayload(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	server.MaxCursorRetainedBytes = 96
	value := string(make([]byte, 96))
	assertOK(t, serveCommand(t, server, 40651121, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "created", Value: int32(-1)}}}, {Key: "name", Value: "tenant_created"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 40651122, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: value}, {Key: "created", Value: int32(1)}}}}, {Key: "$db", Value: "app"}}))
	resp := serveCommand(t, server, 40651123, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: value}}}, {Key: "sort", Value: bson.D{{Key: "created", Value: int32(-1)}}}, {Key: "batchSize", Value: int32(0)}, {Key: "$db", Value: "app"}})
	assertCommandError(t, resp, "BadValue")
	server.cursorMu.Lock()
	defer server.cursorMu.Unlock()
	if len(server.cursors) != 0 {
		t.Fatalf("published cursor despite retained predicate cap: %d", len(server.cursors))
	}
}

func TestMongoCompoundPlannerCursorChargesRetainedProjectionFields(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	server.MaxCursorRetainedBytes = 96
	field := strings.Repeat("p", 96)
	assertOK(t, serveCommand(t, server, 40651124, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "created", Value: int32(-1)}}}, {Key: "name", Value: "tenant_created"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 40651125, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(1)}, {Key: field, Value: int32(1)}}}}, {Key: "$db", Value: "app"}}))
	resp := serveCommand(t, server, 40651126, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "sort", Value: bson.D{{Key: "created", Value: int32(-1)}}}, {Key: "projection", Value: bson.D{{Key: field, Value: int32(1)}}}, {Key: "batchSize", Value: int32(0)}, {Key: "$db", Value: "app"}})
	assertCommandError(t, resp, "BadValue")
	server.cursorMu.Lock()
	defer server.cursorMu.Unlock()
	if len(server.cursors) != 0 {
		t.Fatalf("published cursor despite retained projection cap: %d", len(server.cursors))
	}
}

func TestMongoCompoundPlannerCursorCumulativeGetMoreCap(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	server.MaxCursorRetainedBytes = 1000
	assertOK(t, serveCommand(t, server, 4065113, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "created", Value: int32(-1)}}}}}}, {Key: "$db", Value: "app"}}))
	payload := string(make([]byte, 600))
	assertOK(t, serveCommand(t, server, 4065114, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(2)}, {Key: "payload", Value: payload}}, bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(1)}, {Key: "payload", Value: payload}}}}, {Key: "$db", Value: "app"}}))
	first := serveCommand(t, server, 4065115, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "sort", Value: bson.D{{Key: "created", Value: int32(-1)}}}, {Key: "batchSize", Value: int32(1)}, {Key: "$db", Value: "app"}})
	assertBatchIDs(t, cursorFirstBatch(t, first), []string{"a"})
	id := cursorIDFromResponse(t, first)
	assertCommandError(t, serveCommand(t, server, 4065116, bson.D{{Key: "getMore", Value: id}, {Key: "collection", Value: "events"}, {Key: "$db", Value: "app"}}), "BadValue")
	server.cursorMu.Lock()
	defer server.cursorMu.Unlock()
	if _, ok := server.cursors[id]; ok {
		t.Fatal("cursor retained after cumulative cap failure")
	}
}

func TestMongoCompoundPlannerCursorRechecksPredicateAfterUpdate(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 4065117, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "created", Value: int32(-1)}}}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 4065118, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(2)}}, bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(1)}}}}, {Key: "$db", Value: "app"}}))
	first := serveCommand(t, server, 4065119, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "sort", Value: bson.D{{Key: "created", Value: int32(-1)}}}, {Key: "batchSize", Value: int32(1)}, {Key: "$db", Value: "app"}})
	assertBatchIDs(t, cursorFirstBatch(t, first), []string{"a"})
	id := cursorIDFromResponse(t, first)
	assertOK(t, serveCommand(t, server, 4065120, bson.D{
		{Key: "update", Value: "events"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "b"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "tenant", Value: "x"}}}}},
		}}}, {Key: "$db", Value: "app"},
	}))
	next := serveCommand(t, server, 4065121, bson.D{{Key: "getMore", Value: id}, {Key: "collection", Value: "events"}, {Key: "$db", Value: "app"}})
	assertBatchIDs(t, cursorNextBatch(t, next), []string{})
}

func TestMongoCompoundPlanHintForcesExactIndexOrRejectsBeforeExecution(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 406511, bson.D{
		{Key: "createIndexes", Value: "events"},
		{Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "createdAt", Value: int32(-1)}}}, {Key: "name", Value: "tenant_created_desc"}}}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 406512, bson.D{
		{Key: "insert", Value: "events"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(1)}}}},
		{Key: "$db", Value: "app"},
	}))
	base := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "acme"}}}, {Key: "$db", Value: "app"}}
	byName := append(append(bson.D(nil), base[:2]...), bson.E{Key: "hint", Value: "tenant_created_desc"}, base[2])
	assertBatchIDs(t, cursorFirstBatch(t, serveCommand(t, server, 406513, byName)), []string{"a"})
	byPattern := append(append(bson.D(nil), base[:2]...), bson.E{Key: "hint", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "createdAt", Value: int32(-1)}}}, base[2])
	assertBatchIDs(t, cursorFirstBatch(t, serveCommand(t, server, 406514, byPattern)), []string{"a"})
	bad := append(append(bson.D(nil), base[:2]...), bson.E{Key: "hint", Value: "missing"}, base[2])
	assertCommandError(t, serveCommand(t, server, 406515, bad), "BadValue")
	malformed := append(append(bson.D(nil), base[:2]...), bson.E{Key: "hint", Value: bson.D{{Key: "tenant", Value: int32(0)}}}, base[2])
	assertCommandError(t, serveCommand(t, server, 406516, malformed), "BadValue")
	incompatible := append(append(bson.D(nil), base[:2]...), bson.E{Key: "hint", Value: bson.D{{Key: "createdAt", Value: int64(-1)}}}, base[2])
	assertCommandError(t, serveCommand(t, server, 406517, incompatible), "BadValue")
}

func TestMongoCompoundPlanInEqualityPrefix(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 406521, bson.D{
		{Key: "createIndexes", Value: "events"},
		{Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "createdAt", Value: int32(1)}}}, {Key: "name", Value: "tenant_created"}}}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 406522, bson.D{
		{Key: "insert", Value: "events"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(1)}},
			bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "other"}, {Key: "createdAt", Value: int32(2)}},
			bson.D{{Key: "_id", Value: "c"}, {Key: "tenant", Value: "ignore"}, {Key: "createdAt", Value: int32(3)}},
		}},
		{Key: "$db", Value: "app"},
	}))
	resp := serveCommand(t, server, 406523, bson.D{
		{Key: "find", Value: "events"},
		{Key: "filter", Value: bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "tenant", Value: bson.D{{Key: "$in", Value: bson.A{"acme", "other"}}}}},
			bson.D{{Key: "createdAt", Value: bson.D{{Key: "$gte", Value: int32(1)}}}},
		}}}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, resp), []string{"a", "b"})
	explain := serveCommand(t, server, 406524, bson.D{
		{Key: "explain", Value: bson.D{
			{Key: "find", Value: "events"},
			{Key: "filter", Value: bson.D{{Key: "tenant", Value: bson.D{{Key: "$in", Value: bson.A{"acme", "other"}}}}}},
		}},
		{Key: "$db", Value: "app"},
	})
	if got := bson.Raw(explain).Lookup("queryPlanner").Document().Lookup("winningPlan").Document().Lookup("stage").StringValue(); got != "compound_index_scan" {
		t.Fatalf("explain stage=%q want compound_index_scan", got)
	}
}

func TestMongoCompoundPlanInPrefixSharesOneBoundedWorkBudget(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	server.MaxFindScanDocuments = 2
	assertOK(t, serveCommand(t, server, 406525, bson.D{
		{Key: "createIndexes", Value: "events"},
		{Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "createdAt", Value: int32(1)}}}, {Key: "name", Value: "tenant_created"}}}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 406526, bson.D{
		{Key: "insert", Value: "events"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(1)}},
			bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(2)}},
			bson.D{{Key: "_id", Value: "c"}, {Key: "tenant", Value: "other"}, {Key: "createdAt", Value: int32(1)}},
		}}, {Key: "$db", Value: "app"},
	}))
	resp := serveCommand(t, server, 406527, bson.D{
		{Key: "find", Value: "events"},
		{Key: "filter", Value: bson.D{{Key: "tenant", Value: bson.D{{Key: "$in", Value: bson.A{"acme", "other"}}}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, resp, "BadValue")
}

func TestMongoCompoundPlanNoSortLimitPushesExactPrefixWorkBound(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	server.MaxFindScanDocuments = 1
	assertOK(t, serveCommand(t, server, 4065261, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "createdAt", Value: int32(1)}}}, {Key: "name", Value: "tenant_created"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 4065262, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(1)}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(2)}},
		bson.D{{Key: "_id", Value: "c"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(3)}},
	}}, {Key: "$db", Value: "app"}}))
	command := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "acme"}}}, {Key: "limit", Value: int32(1)}, {Key: "$db", Value: "app"}}
	response := serveCommand(t, server, 4065263, command)
	assertOK(t, response)
	assertBatchIDs(t, cursorFirstBatch(t, response), []string{"a"})
	explain := serveCommand(t, server, 4065264, bson.D{{Key: "explain", Value: command}, {Key: "verbosity", Value: "executionStats"}, {Key: "$db", Value: "app"}})
	assertOK(t, explain)
	stats := bson.Raw(explain).Lookup("executionStats").Document()
	if got, ok := stats.Lookup("candidateDocumentsExamined").Int64OK(); !ok || got != 1 {
		t.Fatalf("candidateDocumentsExamined=%d ok=%v want 1: %s", got, ok, explain)
	}
}

func TestMongoCompoundPlanNoSortLimitRequiresGlobalPageBudget(t *testing.T) {
	newServer := func(t *testing.T, cap int) *Server {
		t.Helper()
		server := newMongoCompatibilityMatrixServer(t)
		server.MaxFindScanDocuments = cap
		assertOK(t, serveCommand(t, server, 40652641, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "createdAt", Value: int32(1)}}}, {Key: "name", Value: "tenant_created"}}}}, {Key: "$db", Value: "app"}}))
		assertOK(t, serveCommand(t, server, 40652642, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(1)}},
			bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(2)}},
			bson.D{{Key: "_id", Value: "c"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(3)}},
		}}, {Key: "$db", Value: "app"}}))
		return server
	}
	query := func(skip, limit int32) bson.D {
		return bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "acme"}}}, {Key: "skip", Value: skip}, {Key: "limit", Value: limit}, {Key: "$db", Value: "app"}}
	}

	// A direct range's truncation only establishes page completeness when the
	// global cap covers the entire skip+limit page. It must not turn a cap hit
	// into a successful empty result.
	assertCommandError(t, serveCommand(t, newServer(t, 2), 40652643, query(2, 1)), "BadValue")
	capExplain := serveCommand(t, newServer(t, 2), 406526430, bson.D{{Key: "explain", Value: query(2, 1)}, {Key: "verbosity", Value: "executionStats"}, {Key: "$db", Value: "app"}})
	assertCommandError(t, capExplain, "BadValue")
	capStats := bson.Raw(capExplain).Lookup("executionStats").Document()
	if got, ok := capStats.Lookup("candidateDocumentsExamined").Int64OK(); !ok || got != 2 {
		t.Fatalf("cap rejection examined=%d ok=%v want 2: %s", got, ok, capExplain)
	}
	if got, ok := capStats.Lookup("candidateDocumentsMaterialized").Int64OK(); !ok || got != 0 {
		t.Fatalf("cap rejection materialized=%d ok=%v want 0: %s", got, ok, capExplain)
	}
	assertBatchIDs(t, cursorFirstBatch(t, serveCommand(t, newServer(t, 3), 40652644, query(2, 1))), []string{"c"})

	// The wire values are individually valid int32s, but their page sum must
	// not wrap when deciding whether a direct truncation is safe to accept.
	assertCommandError(t, serveCommand(t, newServer(t, 2), 40652645, query(1<<31-1, 1<<31-1)), "BadValue")
}

func TestMongoCompoundPlanStableSortUsesBSONIDTieOrder(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 40652646, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "score", Value: int32(1)}}}, {Key: "name", Value: "tenant_score"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 40652647, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "z"}, {Key: "tenant", Value: "acme"}, {Key: "score", Value: int32(1)}},
		bson.D{{Key: "_id", Value: "aa"}, {Key: "tenant", Value: "acme"}, {Key: "score", Value: int32(1)}},
	}}, {Key: "$db", Value: "app"}}))
	response := serveCommand(t, server, 40652648, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "acme"}}}, {Key: "sort", Value: bson.D{{Key: "score", Value: int32(1)}}}, {Key: "limit", Value: int32(1)}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	assertBatchIDs(t, cursorFirstBatch(t, response), []string{"aa"})
}

func TestMongoCompoundPlanSortWithUnfixedTrailingComponentFallsBack(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 40652649, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "x", Value: int32(1)}, {Key: "y", Value: int32(1)}}}, {Key: "name", Value: "tenant_x_y"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 40652650, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "acme"}, {Key: "x", Value: int32(1)}, {Key: "y", Value: int32(2)}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "acme"}, {Key: "x", Value: int32(1)}, {Key: "y", Value: int32(1)}},
	}}, {Key: "$db", Value: "app"}}))
	command := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "acme"}}}, {Key: "sort", Value: bson.D{{Key: "x", Value: int32(1)}}}, {Key: "limit", Value: int32(1)}, {Key: "$db", Value: "app"}}
	response := serveCommand(t, server, 40652651, command)
	assertOK(t, response)
	assertBatchIDs(t, cursorFirstBatch(t, response), []string{"a"})
	explain := serveCommand(t, server, 40652652, bson.D{{Key: "explain", Value: command}, {Key: "verbosity", Value: "queryPlanner"}, {Key: "$db", Value: "app"}})
	assertOK(t, explain)
	if got, ok := bson.Raw(explain).Lookup("queryPlanner").Document().Lookup("sort").Document().Lookup("satisfied").BooleanOK(); !ok || got {
		t.Fatalf("sort satisfied=%v ok=%v want false: %s", got, ok, explain)
	}
}

func TestMongoCompoundHintExplainExcludesAllLegacyProbes(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 4065265, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{
		bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}}}, {Key: "name", Value: "tenant_legacy"}, {Key: "treedbValueType", Value: "string"}},
		bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "score", Value: int32(1)}}}, {Key: "name", Value: "tenant_score"}},
	}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 4065266, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "acme"}, {Key: "score", Value: int32(1)}}}}, {Key: "$db", Value: "app"}}))
	command := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "acme"}}}, {Key: "hint", Value: "tenant_score"}, {Key: "$db", Value: "app"}}
	for _, verbosity := range []string{"queryPlanner", "executionStats"} {
		t.Run(verbosity, func(t *testing.T) {
			response := serveCommand(t, server, 4065267, bson.D{{Key: "explain", Value: command}, {Key: "verbosity", Value: verbosity}, {Key: "$db", Value: "app"}})
			assertOK(t, response)
			planner := bson.Raw(response).Lookup("queryPlanner").Document()
			if got := planner.Lookup("winningPlan").Document().Lookup("stage").StringValue(); got != "compound_index_scan" {
				t.Fatalf("winning stage=%q want compound_index_scan: %s", got, response)
			}
			usable, err := planner.Lookup("usableIndexes").Array().Values()
			if err != nil || len(usable) != 1 || usable[0].Document().Lookup("name").StringValue() != "tenant_score" {
				t.Fatalf("hinted usable indexes=%v err=%v want tenant_score only: %s", usable, err, response)
			}
			if candidatePlans := planner.Lookup("candidatePlans"); candidatePlans.Type != 0 {
				candidates, err := candidatePlans.Array().Values()
				if err != nil || len(candidates) != 1 || candidates[0].Document().Lookup("indexName").StringValue() != "tenant_score" {
					t.Fatalf("hinted candidate plans=%v err=%v want tenant_score only: %s", candidates, err, response)
				}
			}
		})
	}
}

func TestMongoCompoundPlanDeduplicatesInChoicesBeforeFanoutLimit(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 40652670, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "score", Value: int32(1)}}}, {Key: "name", Value: "tenant_score"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 40652671, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: int32(1)}, {Key: "score", Value: int32(1)}}}}, {Key: "$db", Value: "app"}}))
	values := make(bson.A, 65)
	for i := range values {
		if i%2 == 0 {
			values[i] = int32(1)
		} else {
			values[i] = int64(1)
		}
	}
	response := serveCommand(t, server, 40652672, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: bson.D{{Key: "$in", Value: values}}}}}, {Key: "hint", Value: "tenant_score"}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	assertBatchIDs(t, cursorFirstBatch(t, response), []string{"a"})
}

func TestMongoCompoundPlanInPrefixUsesSharedRemainingBudget(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	server.MaxFindScanDocuments = 4
	assertOK(t, serveCommand(t, server, 4065271, bson.D{
		{Key: "createIndexes", Value: "events"},
		{Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "createdAt", Value: int32(1)}}}, {Key: "name", Value: "tenant_created"}}}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 4065272, bson.D{
		{Key: "insert", Value: "events"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(1)}},
			bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(2)}},
			bson.D{{Key: "_id", Value: "c"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(3)}},
		}}, {Key: "$db", Value: "app"},
	}))
	query := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: bson.D{{Key: "$in", Value: bson.A{"acme", "other"}}}}}}, {Key: "$db", Value: "app"}}
	response := serveCommand(t, server, 4065273, query)
	assertBatchIDs(t, cursorFirstBatch(t, response), []string{"a", "b", "c"})

	assertOK(t, serveCommand(t, server, 4065274, bson.D{
		{Key: "insert", Value: "events"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "d"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(4)}},
			bson.D{{Key: "_id", Value: "e"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(5)}},
		}}, {Key: "$db", Value: "app"},
	}))
	assertCommandError(t, serveCommand(t, server, 4065275, query), "BadValue")
}

func TestMongoCompoundPlanSupportsNullEqualityPrefix(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 406528, bson.D{
		{Key: "createIndexes", Value: "events"},
		{Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "createdAt", Value: int32(-1)}}}, {Key: "name", Value: "tenant_created"}}}}, {Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 406529, bson.D{
		{Key: "insert", Value: "events"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "null"}, {Key: "tenant", Value: nil}, {Key: "createdAt", Value: int32(2)}}, bson.D{{Key: "_id", Value: "value"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(1)}}}}, {Key: "$db", Value: "app"},
	}))
	resp := serveCommand(t, server, 406530, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: nil}}}, {Key: "$db", Value: "app"}})
	assertBatchIDs(t, cursorFirstBatch(t, resp), []string{"null"})
}

func TestMongoCompoundPlanMaterializationBytesAreCapped(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	server.MaxCursorRetainedBytes = 32
	assertOK(t, serveCommand(t, server, 406550, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "createdAt", Value: int32(1)}}}, {Key: "name", Value: "tenant_created"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 406551, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "large"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(1)}, {Key: "payload", Value: "this BSON document exceeds the bounded materialization budget"}}}}, {Key: "$db", Value: "app"}}))
	resp := serveCommand(t, server, 406552, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "acme"}}}, {Key: "$db", Value: "app"}})
	assertCommandError(t, resp, "BadValue")
}

func TestMongoCompoundPlanMultiFieldSortFallsBackBeforePagination(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 406531, bson.D{
		{Key: "createIndexes", Value: "events"},
		{Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "score", Value: int32(-1)}, {Key: "createdAt", Value: int32(1)}}}, {Key: "name", Value: "tenant_score_created"}}}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 406532, bson.D{
		{Key: "insert", Value: "events"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "acme"}, {Key: "score", Value: int32(9)}, {Key: "createdAt", Value: int32(2)}},
			bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "acme"}, {Key: "score", Value: int32(9)}, {Key: "createdAt", Value: int32(1)}},
			bson.D{{Key: "_id", Value: "c"}, {Key: "tenant", Value: "acme"}, {Key: "score", Value: int32(8)}, {Key: "createdAt", Value: int32(1)}},
		}},
		{Key: "$db", Value: "app"},
	}))
	command := bson.D{
		{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "acme"}}},
		{Key: "sort", Value: bson.D{{Key: "score", Value: int32(-1)}, {Key: "createdAt", Value: int32(1)}}},
		{Key: "skip", Value: int32(1)}, {Key: "limit", Value: int32(1)}, {Key: "$db", Value: "app"},
	}
	assertBatchIDs(t, cursorFirstBatch(t, serveCommand(t, server, 406533, command)), []string{"a"})
	explain := serveCommand(t, server, 406534, bson.D{{Key: "explain", Value: command}, {Key: "$db", Value: "app"}})
	assertOK(t, explain)
	sortInfo := bson.Raw(explain).Lookup("queryPlanner").Document().Lookup("sort").Document()
	if satisfied, ok := sortInfo.Lookup("satisfied").BooleanOK(); !ok || satisfied {
		t.Fatalf("sort satisfied=%v ok=%v want false: %s", satisfied, ok, explain)
	}
	winning := bson.Raw(explain).Lookup("queryPlanner").Document().Lookup("winningPlan").Document()
	if inMemory, ok := winning.Lookup("inMemorySort").BooleanOK(); !ok || !inMemory {
		t.Fatalf("winning inMemorySort=%v ok=%v want true: %s", inMemory, ok, explain)
	}
}

func TestMongoAggregateInitialMatchSortUsesCompoundPlan(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 406540, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "score", Value: int32(-1)}}}, {Key: "name", Value: "tenant_score"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 406541, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "acme"}, {Key: "score", Value: int32(1)}}, bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "acme"}, {Key: "score", Value: int32(2)}}}}, {Key: "$db", Value: "app"}}))
	pipeline := bson.A{bson.D{{Key: "$match", Value: bson.D{{Key: "tenant", Value: "acme"}}}}, bson.D{{Key: "$sort", Value: bson.D{{Key: "score", Value: int32(-1)}}}}}
	resp := serveCommand(t, server, 406542, bson.D{{Key: "aggregate", Value: "events"}, {Key: "pipeline", Value: pipeline}, {Key: "cursor", Value: bson.D{}}, {Key: "$db", Value: "app"}})
	assertBatchIDs(t, cursorFirstBatch(t, resp), []string{"b", "a"})
	explain := serveCommand(t, server, 406543, bson.D{{Key: "explain", Value: bson.D{{Key: "aggregate", Value: "events"}, {Key: "pipeline", Value: pipeline}, {Key: "cursor", Value: bson.D{}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, explain)
	if got := bson.Raw(explain).Lookup("queryPlanner").Document().Lookup("winningPlan").Document().Lookup("stage").StringValue(); got != "compound_index_scan" {
		t.Fatalf("aggregate explain stage=%q want compound_index_scan: %s", got, explain)
	}
}

func TestMongoCompoundPlanServesCountAndDistinct(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 406560, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "score", Value: int32(-1)}}}, {Key: "name", Value: "tenant_score"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 406561, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "acme"}, {Key: "score", Value: int32(1)}, {Key: "kind", Value: "x"}}, bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "acme"}, {Key: "score", Value: int32(2)}, {Key: "kind", Value: "y"}}, bson.D{{Key: "_id", Value: "c"}, {Key: "tenant", Value: "other"}, {Key: "score", Value: int32(3)}, {Key: "kind", Value: "z"}}}}, {Key: "$db", Value: "app"}}))
	count := serveCommand(t, server, 406562, bson.D{{Key: "count", Value: "events"}, {Key: "query", Value: bson.D{{Key: "tenant", Value: "acme"}}}, {Key: "$db", Value: "app"}})
	assertOK(t, count)
	if got, ok := bson.Raw(count).Lookup("n").Int64OK(); !ok || got != 2 {
		t.Fatalf("count n=%d ok=%v want 2: %s", got, ok, count)
	}
	distinct := serveCommand(t, server, 406563, bson.D{{Key: "distinct", Value: "events"}, {Key: "key", Value: "kind"}, {Key: "query", Value: bson.D{{Key: "tenant", Value: "acme"}}}, {Key: "$db", Value: "app"}})
	assertOK(t, distinct)
	values, err := bson.Raw(distinct).Lookup("values").Array().Values()
	if err != nil || len(values) != 2 {
		t.Fatalf("distinct values=%v err=%v want two: %s", values, err, distinct)
	}
}
