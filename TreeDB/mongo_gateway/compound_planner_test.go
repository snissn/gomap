package mongogateway

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/collections"
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

func TestMongoCompoundPlanScalarSortMatchesBoundedComparator(t *testing.T) {
	cases := []struct {
		name   string
		values []any
	}{
		{name: "datetime", values: []any{bson.DateTime(255), bson.DateTime(256)}},
		{name: "timestamp", values: []any{bson.Timestamp{T: 255, I: 1}, bson.Timestamp{T: 256, I: 1}}},
		// BSON-v2 places NaN before every finite numeric value. This must agree
		// with a bounded collection-scan comparator before limit/skip are applied.
		{name: "nan", values: []any{math.NaN(), float64(0)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newMongoCompatibilityMatrixServer(t)
			indexed := "indexed_" + tc.name
			scanned := "scanned_" + tc.name
			assertOK(t, serveCommand(t, server, 406505, bson.D{{Key: "createIndexes", Value: indexed}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "value", Value: int32(1)}}}, {Key: "name", Value: "tenant_value"}}}}, {Key: "$db", Value: "app"}}))
			docs := bson.A{
				bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "t"}, {Key: "value", Value: tc.values[0]}},
				bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "t"}, {Key: "value", Value: tc.values[1]}},
			}
			assertOK(t, serveCommand(t, server, 406506, bson.D{{Key: "insert", Value: indexed}, {Key: "documents", Value: docs}, {Key: "$db", Value: "app"}}))
			assertOK(t, serveCommand(t, server, 406507, bson.D{{Key: "insert", Value: scanned}, {Key: "documents", Value: docs}, {Key: "$db", Value: "app"}}))

			for _, direction := range []int32{1, -1} {
				for _, skip := range []int32{0, 1} {
					command := func(collection string) bson.D {
						return bson.D{{Key: "find", Value: collection}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "sort", Value: bson.D{{Key: "value", Value: direction}}}, {Key: "skip", Value: skip}, {Key: "limit", Value: int32(1)}, {Key: "$db", Value: "app"}}
					}
					indexedResponse := serveCommand(t, server, int32(406508+direction*10+skip), command(indexed))
					scannedResponse := serveCommand(t, server, int32(406528+direction*10+skip), command(scanned))
					indexedID := scalarSortBatchID(t, cursorFirstBatch(t, indexedResponse))
					scannedID := scalarSortBatchID(t, cursorFirstBatch(t, scannedResponse))
					if indexedID != scannedID {
						t.Fatalf("direction=%d skip=%d indexed=%q scanned=%q", direction, skip, indexedID, scannedID)
					}
					want := "a"
					if (direction < 0) != (skip > 0) {
						want = "b"
					}
					if indexedID != want {
						t.Fatalf("direction=%d skip=%d ID=%q want %q", direction, skip, indexedID, want)
					}
				}
			}
			explain := serveCommand(t, server, 406550, bson.D{{Key: "explain", Value: bson.D{{Key: "find", Value: indexed}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "sort", Value: bson.D{{Key: "value", Value: int32(1)}}}, {Key: "$db", Value: "app"}}}, {Key: "verbosity", Value: "queryPlanner"}, {Key: "$db", Value: "app"}})
			assertOK(t, explain)
			winning := bson.Raw(explain).Lookup("queryPlanner", "winningPlan").Document()
			if stage, ok := winning.Lookup("stage").StringValueOK(); !ok || stage != "compound_index_scan" {
				t.Fatalf("temporal sort stage=%q ok=%v response=%s", stage, ok, explain)
			}
		})
	}
}

func TestCompareRawValuesScalarOrderMatchesBSONV2Codec(t *testing.T) {
	cases := [][]bson.RawValue{
		{mustRawValue(t, bson.DateTime(-1)), mustRawValue(t, bson.DateTime(0)), mustRawValue(t, bson.DateTime(255)), mustRawValue(t, bson.DateTime(256))},
		{mustRawValue(t, bson.Timestamp{T: 1, I: 2}), mustRawValue(t, bson.Timestamp{T: 1, I: 3}), mustRawValue(t, bson.Timestamp{T: 2, I: 0})},
		{mustRawValue(t, math.NaN()), mustRawValue(t, math.Inf(-1)), mustRawValue(t, int32(0)), mustRawValue(t, math.Inf(1))},
	}
	for _, values := range cases {
		for left := range values {
			for right := range values {
				leftEncoded, err := collections.EncodeBSONIndexKeyComponentV2(values[left])
				if err != nil {
					t.Fatal(err)
				}
				rightEncoded, err := collections.EncodeBSONIndexKeyComponentV2(values[right])
				if err != nil {
					t.Fatal(err)
				}
				if got, want := scalarOrderSign(compareRawValues(values[left], values[right])), scalarOrderSign(bytes.Compare(leftEncoded, rightEncoded)); got != want {
					t.Fatalf("codec/comparator order got=%d want=%d left=%v right=%v", got, want, values[left], values[right])
				}
			}
		}
	}
}

func TestMongoCompoundPlanDecimal128SortMatchesBoundedComparator(t *testing.T) {
	decimal, err := bson.ParseDecimal128("0")
	if err != nil {
		t.Fatal(err)
	}
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 406555, bson.D{{Key: "createIndexes", Value: "indexed"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "value", Value: int32(1)}}}, {Key: "name", Value: "tenant_value"}}}}, {Key: "$db", Value: "app"}}))
	docs := bson.A{
		bson.D{{Key: "_id", Value: "decimal"}, {Key: "tenant", Value: "t"}, {Key: "value", Value: decimal}},
		bson.D{{Key: "_id", Value: "string"}, {Key: "tenant", Value: "t"}, {Key: "value", Value: "a"}},
	}
	assertOK(t, serveCommand(t, server, 406556, bson.D{{Key: "insert", Value: "indexed"}, {Key: "documents", Value: docs}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 406557, bson.D{{Key: "insert", Value: "scanned"}, {Key: "documents", Value: docs}, {Key: "$db", Value: "app"}}))
	for _, direction := range []int32{1, -1} {
		command := func(collection string) bson.D {
			return bson.D{{Key: "find", Value: collection}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "sort", Value: bson.D{{Key: "value", Value: direction}}}, {Key: "limit", Value: int32(1)}, {Key: "$db", Value: "app"}}
		}
		indexedID := scalarSortBatchID(t, cursorFirstBatch(t, serveCommand(t, server, 406558+direction, command("indexed"))))
		scannedID := scalarSortBatchID(t, cursorFirstBatch(t, serveCommand(t, server, 406560+direction, command("scanned"))))
		if indexedID != scannedID {
			t.Fatalf("direction=%d indexed=%q scanned=%q", direction, indexedID, scannedID)
		}
		want := "decimal"
		if direction < 0 {
			want = "string"
		}
		if indexedID != want {
			t.Fatalf("direction=%d ID=%q want %q", direction, indexedID, want)
		}
	}
}

func scalarSortBatchID(tb testing.TB, batch []bson.Raw) string {
	tb.Helper()
	if len(batch) != 1 {
		tb.Fatalf("temporal sort batch length=%d, want 1", len(batch))
	}
	id, ok := batch[0].Lookup("_id").StringValueOK()
	if !ok {
		tb.Fatalf("temporal sort _id is not a string: %v", batch[0])
	}
	return id
}

func scalarOrderSign(value int) int {
	if value < 0 {
		return -1
	}
	if value > 0 {
		return 1
	}
	return 0
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

func TestMongoUnhintedPrimaryExplainExcludesDeferredCompoundCandidate(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 40650560, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "score", Value: int32(1)}}}, {Key: "name", Value: "tenant_score"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 40650561, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "t"}, {Key: "score", Value: int32(1)}}}}, {Key: "$db", Value: "app"}}))
	command := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "t"}}}, {Key: "$db", Value: "app"}}
	for _, verbosity := range []string{"queryPlanner", "executionStats"} {
		t.Run(verbosity, func(t *testing.T) {
			response := serveCommand(t, server, 40650562, bson.D{{Key: "explain", Value: command}, {Key: "verbosity", Value: verbosity}, {Key: "$db", Value: "app"}})
			assertOK(t, response)
			planner := bson.Raw(response).Lookup("queryPlanner").Document()
			if stage, ok := planner.Lookup("winningPlan").Document().Lookup("stage").StringValueOK(); !ok || stage != "primary_lookup" {
				t.Fatalf("%s winning stage=%q ok=%v want primary_lookup: %s", verbosity, stage, ok, response)
			}
			if candidatePlans := planner.Lookup("candidatePlans"); candidatePlans.Type != 0 {
				t.Fatalf("%s retained unreachable compound candidate: %s", verbosity, response)
			}
			usable, err := planner.Lookup("usableIndexes").Array().Values()
			if err != nil || len(usable) != 0 {
				t.Fatalf("%s usable indexes=%v err=%v want none because primary lookup wins: %s", verbosity, usable, err, response)
			}
		})
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

func TestMongoCompoundPlanStableTieUsesGlobalWorkBudgetBeforeLimit(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	server.MaxFindScanDocuments = 65
	assertOK(t, serveCommand(t, server, 40650563, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "score", Value: int32(1)}}}, {Key: "name", Value: "tenant_score"}}}}, {Key: "$db", Value: "app"}}))
	docs := make(bson.A, 65)
	for i := range docs {
		docs[i] = bson.D{{Key: "_id", Value: fmt.Sprintf("id-%03d", 64-i)}, {Key: "tenant", Value: "t"}, {Key: "score", Value: int32(1)}}
	}
	assertOK(t, serveCommand(t, server, 40650564, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: docs}, {Key: "$db", Value: "app"}}))
	command := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "sort", Value: bson.D{{Key: "score", Value: int32(1)}}}, {Key: "limit", Value: int32(1)}, {Key: "$db", Value: "app"}}
	response := serveCommand(t, server, 40650565, command)
	assertOK(t, response)
	assertBatchIDs(t, cursorFirstBatch(t, response), []string{"id-000"})
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

func TestMongoCompoundPlannerCursorSerializesConcurrentGetMoreMaterialization(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 4065101, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "created", Value: int32(-1)}}}, {Key: "name", Value: "tenant_created"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 4065102, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(3)}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(2)}},
		bson.D{{Key: "_id", Value: "c"}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(1)}},
	}}, {Key: "$db", Value: "app"}}))
	first := serveCommand(t, server, 4065103, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "sort", Value: bson.D{{Key: "created", Value: int32(-1)}}}, {Key: "batchSize", Value: int32(1)}, {Key: "$db", Value: "app"}})
	assertBatchIDs(t, cursorFirstBatch(t, first), []string{"a"})
	cursorID := cursorIDFromResponse(t, first)
	if cursorID == 0 {
		t.Fatal("cursor id=0 want resumable compound cursor")
	}

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	server.compoundCursorBatchHook = func() {
		started <- struct{}{}
		<-release
	}
	type result struct {
		batch bson.A
		ok    bool
		err   error
	}
	results := make(chan result, 2)
	get := func() {
		_, batch, ok, err := server.getMore(cursorID, "app.events", 1, 1, true, defaultCursorBatchSize)
		results <- result{batch: batch, ok: ok, err: err}
	}
	go get()
	<-started
	go get()
	select {
	case <-started:
		t.Fatal("concurrent getMore began duplicate compound materialization")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	firstResult, secondResult := <-results, <-results
	for _, got := range []result{firstResult, secondResult} {
		if got.err != nil || !got.ok {
			t.Fatalf("getMore result ok=%v err=%v", got.ok, got.err)
		}
	}
	seen := make(map[string]struct{})
	for _, got := range []result{firstResult, secondResult} {
		for _, value := range got.batch {
			raw, ok := value.(bson.Raw)
			if !ok {
				t.Fatalf("compound batch value type %T", value)
			}
			id, ok := raw.Lookup("_id").StringValueOK()
			if !ok {
				t.Fatalf("compound batch missing string _id: %v", raw)
			}
			if _, duplicate := seen[id]; duplicate {
				t.Fatalf("concurrent getMore duplicated ID %q", id)
			}
			seen[id] = struct{}{}
		}
	}
	if len(seen) != 2 {
		t.Fatalf("concurrent getMore emitted IDs=%v want b,c", seen)
	}
	if _, ok := seen["b"]; !ok {
		t.Fatalf("concurrent getMore missing b: %v", seen)
	}
	if _, ok := seen["c"]; !ok {
		t.Fatalf("concurrent getMore missing c: %v", seen)
	}
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

func TestMongoCompoundPlannerDottedProjectionPreflightsBeforeZeroBatchCursorPublication(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 40651104, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "created", Value: int32(-1)}}}, {Key: "name", Value: "tenant_created"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 40651105, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "safe"}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(2)}, {Key: "profile", Value: bson.D{{Key: "name", Value: "Ada"}}}},
		bson.D{{Key: "_id", Value: "array"}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(1)}, {Key: "profile", Value: bson.A{bson.D{{Key: "name", Value: "Grace"}}}}},
	}}, {Key: "$db", Value: "app"}}))

	response := serveCommand(t, server, 40651106, bson.D{
		{Key: "find", Value: "events"},
		{Key: "filter", Value: bson.D{{Key: "tenant", Value: "t"}}},
		{Key: "sort", Value: bson.D{{Key: "created", Value: int32(-1)}}},
		{Key: "projection", Value: bson.D{{Key: "profile.name", Value: int32(1)}}},
		{Key: "batchSize", Value: int32(0)}, {Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")
	server.cursorMu.Lock()
	defer server.cursorMu.Unlock()
	if len(server.cursors) != 0 {
		t.Fatalf("published compound cursor before dotted projection preflight: %d", len(server.cursors))
	}
}

func TestMongoCompoundPlannerCursorCapsRetainedIDsBeforePublication(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	// This admits the cloned command plan and one small result, but not the
	// independently owned ID slice plus cross-prefix dedupe key for two long
	// candidates. The admission failure must occur during the index walk, before
	// openCompoundIDCursor can publish any cursor.
	server.MaxCursorRetainedBytes = 200
	assertOK(t, serveCommand(t, server, 40651101, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "created", Value: int32(-1)}}}, {Key: "name", Value: "tenant_created"}}}}, {Key: "$db", Value: "app"}}))
	longA := strings.Repeat("a", 40)
	longB := strings.Repeat("b", 40)
	assertOK(t, serveCommand(t, server, 40651102, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: longA}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(2)}},
		bson.D{{Key: "_id", Value: longB}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(1)}},
	}}, {Key: "$db", Value: "app"}}))
	resp := serveCommand(t, server, 40651103, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "hint", Value: "tenant_created"}, {Key: "batchSize", Value: int32(0)}, {Key: "$db", Value: "app"}})
	assertCommandError(t, resp, "BadValue")
	server.cursorMu.Lock()
	defer server.cursorMu.Unlock()
	if len(server.cursors) != 0 {
		t.Fatalf("published cursor despite retained-ID admission cap: %d", len(server.cursors))
	}
}

func TestMongoCompoundPlannerBoundsRetainedIDsForNonCursorExecutions(t *testing.T) {
	// Candidate IDs are retained before non-cursor plans materialize BSON too;
	// count, distinct, aggregate, and an ordinary single-batch find must not
	// bypass the same owned-ID admission limit used by compound cursors.
	commands := []bson.D{
		{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "$db", Value: "app"}},
		{{Key: "count", Value: "events"}, {Key: "query", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "$db", Value: "app"}},
		{{Key: "distinct", Value: "events"}, {Key: "key", Value: "created"}, {Key: "query", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "$db", Value: "app"}},
		{{Key: "aggregate", Value: "events"}, {Key: "pipeline", Value: bson.A{bson.D{{Key: "$match", Value: bson.D{{Key: "tenant", Value: "t"}}}}}}, {Key: "cursor", Value: bson.D{}}, {Key: "$db", Value: "app"}},
	}
	for i, command := range commands {
		t.Run(command[0].Key, func(t *testing.T) {
			server := newMongoCompatibilityMatrixServer(t)
			server.MaxCursorRetainedBytes = 200
			requestID := int32(40651140 + i*10)
			assertOK(t, serveCommand(t, server, requestID, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "created", Value: int32(1)}}}, {Key: "name", Value: "tenant_created"}}}}, {Key: "$db", Value: "app"}}))
			longA, longB := strings.Repeat("a", 40), strings.Repeat("b", 40)
			assertOK(t, serveCommand(t, server, requestID+1, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{
				bson.D{{Key: "_id", Value: longA}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(1)}},
				bson.D{{Key: "_id", Value: longB}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(2)}},
			}}, {Key: "$db", Value: "app"}}))
			assertCommandError(t, serveCommand(t, server, requestID+2, command), "BadValue")
			server.cursorMu.Lock()
			defer server.cursorMu.Unlock()
			if len(server.cursors) != 0 {
				t.Fatalf("published cursor despite non-cursor retained-ID admission cap: %d", len(server.cursors))
			}
		})
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

func TestMongoCompoundPlannerCursorChargesProjectionMapStructure(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	// Short field names keep the payload below this cap; the cloned projection
	// map's buckets and string headers must still prevent cursor publication.
	server.MaxCursorRetainedBytes = 160
	assertOK(t, serveCommand(t, server, 406511261, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "created", Value: int32(-1)}}}, {Key: "name", Value: "tenant_created"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 406511262, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(1)}}}}, {Key: "$db", Value: "app"}}))
	projection := bson.D{}
	for i := 0; i < 16; i++ {
		projection = append(projection, bson.E{Key: fmt.Sprintf("p%02d", i), Value: int32(1)})
	}
	resp := serveCommand(t, server, 406511263, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "sort", Value: bson.D{{Key: "created", Value: int32(-1)}}}, {Key: "projection", Value: projection}, {Key: "batchSize", Value: int32(0)}, {Key: "$db", Value: "app"}})
	assertCommandError(t, resp, "BadValue")
	server.cursorMu.Lock()
	defer server.cursorMu.Unlock()
	if len(server.cursors) != 0 {
		t.Fatalf("published cursor despite retained projection-map cap: %d", len(server.cursors))
	}
}

func TestMongoCompoundPlannerCursorChargesOneFieldProjectionMapBase(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	// The old unsafe.Sizeof(map)+per-entry estimate admitted this shape. A
	// single projection field still owns a complete runtime map group, so the
	// conservative fixed map charge must reject before cursor publication.
	server.MaxCursorRetainedBytes = 200
	assertOK(t, serveCommand(t, server, 406511264, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "created", Value: int32(-1)}}}, {Key: "name", Value: "tenant_created"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 406511265, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(1)}}}}, {Key: "$db", Value: "app"}}))
	resp := serveCommand(t, server, 406511266, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "sort", Value: bson.D{{Key: "created", Value: int32(-1)}}}, {Key: "projection", Value: bson.D{{Key: "p", Value: int32(1)}}}, {Key: "batchSize", Value: int32(0)}, {Key: "$db", Value: "app"}})
	assertCommandError(t, resp, "BadValue")
	server.cursorMu.Lock()
	defer server.cursorMu.Unlock()
	if len(server.cursors) != 0 {
		t.Fatalf("published cursor despite one-field projection-map cap: %d", len(server.cursors))
	}
}

func TestFindPlanCursorRetainedBytesChargesClonedCommandStringsOnce(t *testing.T) {
	// cloneFindPlanForCursor retains string headers from every command shape;
	// aliases must not consume the cap twice, while separately allocated equal
	// command strings must each be charged.
	shared := "shared"
	separateShared := strings.Clone(shared)
	plan := findPlan{
		predicates: []findPredicate{{field: "predicate", values: []bson.RawValue{{Value: []byte{1, 2}}}}},
		orBranches: [][]findPredicate{[]findPredicate{{field: "or-predicate"}}},
		sort: findSort{
			field: shared,
			terms: []findSortTerm{{field: "sort-term"}, {field: shared}},
		},
		hint: findHint{
			name:       "hint-name",
			components: []collections.IndexComponent{{Field: "component"}, {Field: separateShared}},
		},
		projection: compiledProjection{fields: map[string]struct{}{"projection": {}, shared: {}}},
	}
	want := len([]byte{1, 2}) + len("predicate") + len("or-predicate") + 2*len(shared) + len("sort-term") + len("hint-name") + len("component") + len("projection") +
		(len(plan.predicates)+len(plan.orBranches[0]))*int(unsafe.Sizeof(findPredicate{})) + len(plan.orBranches)*int(unsafe.Sizeof([]findPredicate{})) + len(plan.sort.terms)*int(unsafe.Sizeof(findSortTerm{})) + len(plan.hint.components)*int(unsafe.Sizeof(collections.IndexComponent{})) + int(unsafe.Sizeof(bson.RawValue{})) + 256 + len(plan.projection.fields)*64
	if got := findPlanCursorRetainedBytes(plan); got != want {
		t.Fatalf("retained cursor-plan bytes=%d, want %d", got, want)
	}
}

func TestCanonicalCompoundPrefixValuesBoundsRawDuplicateAllocationAndRejectsInvalid(t *testing.T) {
	// Eligibility accepts a bounded number of *canonical* choices, not a
	// bounded number of raw wire values.  Keep the transient allocation bounded
	// too: a maliciously duplicate-heavy $in must not reserve its raw length.
	duplicates := make([]bson.RawValue, 1024)
	for i := range duplicates {
		duplicates[i] = bson.RawValue{Type: bson.TypeInt32, Value: []byte{1, 0, 0, 0}}
	}
	got := canonicalCompoundPrefixValues(duplicates)
	if len(got) != 1 {
		t.Fatalf("canonical duplicate choices=%d, want 1", len(got))
	}
	if cap(got) > maxCompoundPlannerPrefixChoices+1 {
		t.Fatalf("canonical duplicate capacity=%d, want <= %d", cap(got), maxCompoundPlannerPrefixChoices+1)
	}

	// A partially encodable $in cannot be treated as a residual-free exact
	// prefix: that would silently omit the unsupported alternative.
	invalid := bson.RawValue{Type: bson.TypeRegex, Value: []byte{'x', 0, 'i', 0}}
	if _, err := collections.EncodeBSONIndexKeyComponentV2(invalid); err == nil {
		t.Fatal("test requires an unsupported BSON-v2 component")
	}
	if values := canonicalCompoundPrefixValues(append(got, invalid)); values != nil {
		t.Fatalf("mixed valid/invalid canonical choices=%v, want rejection", values)
	}
	idx := collections.IndexDefinition{
		Name:      "tenant_created",
		ValueType: collections.IndexValueBSONOrderedV2,
		Components: []collections.IndexComponent{
			{Field: "tenant", Direction: collections.IndexDirectionAscending},
			{Field: "created", Direction: collections.IndexDirectionDescending},
		},
	}
	plan := findPlan{predicates: []findPredicate{{field: "tenant", op: findPredicateIn, values: append(got, invalid)}}}
	if _, ok := buildCompoundIndexPlan(idx, plan); ok {
		t.Fatal("mixed valid/unsupported $in became a partial exact compound probe")
	}
}

func TestParseFindPlanCachesCanonicalInValuesOncePerPredicate(t *testing.T) {
	values := make(bson.A, 1024)
	for i := range values {
		// Distinct BSON wire numeric spellings intentionally canonicalize to one
		// BSON-v2 equality component.
		if i%2 == 0 {
			values[i] = int32(1)
		} else {
			values[i] = int64(1)
		}
	}
	filterBytes, err := bson.Marshal(bson.D{{Key: "tenant", Value: bson.D{{Key: "$in", Value: values}}}})
	if err != nil {
		t.Fatalf("marshal filter: %v", err)
	}
	commandBytes, err := bson.Marshal(bson.D{{Key: "find", Value: "events"}})
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	plan, err := parseFindPlan(wire.Document(commandBytes), wire.Document(filterBytes))
	if err != nil {
		t.Fatalf("parse find plan: %v", err)
	}
	if len(plan.predicates) != 1 || !plan.predicates[0].compoundCanonicalized {
		t.Fatalf("parsed predicate was not canonicalized once: %#v", plan.predicates)
	}
	if got := plan.predicates[0].compoundCanonicalValues; len(got) != 1 || cap(got) > maxCompoundPlannerPrefixChoices+1 {
		t.Fatalf("cached canonical values len/cap=%d/%d want 1/bounded", len(got), cap(got))
	}
	idx := collections.IndexDefinition{Name: "tenant_a", ValueType: collections.IndexValueBSONOrderedV2, Components: []collections.IndexComponent{{Field: "tenant", Direction: collections.IndexDirectionAscending}, {Field: "a", Direction: collections.IndexDirectionAscending}}}
	first, ok := buildCompoundIndexPlan(idx, plan)
	if !ok || len(first.prefixChoices) != 1 || len(first.prefixChoices[0]) != 1 {
		t.Fatalf("first cached plan=%#v ok=%v", first, ok)
	}
	second, ok := buildCompoundIndexPlan(idx, plan)
	if !ok || len(second.prefixChoices) != 1 || len(second.prefixChoices[0]) != 1 {
		t.Fatalf("second cached plan=%#v ok=%v", second, ok)
	}
}

func TestFindPlanFinalizationCachesCompoundInForReadAdapters(t *testing.T) {
	values := make(bson.A, 1024)
	for i := range values {
		values[i] = int32(1)
	}
	filterBytes, err := bson.Marshal(bson.D{{Key: "tenant", Value: bson.D{{Key: "$in", Value: values}}}})
	if err != nil {
		t.Fatalf("marshal filter: %v", err)
	}
	predicates, branches, _, err := parseFindFilter(wire.Document(filterBytes))
	if err != nil {
		t.Fatalf("parse filter: %v", err)
	}
	assertCached := func(t *testing.T, plan findPlan) {
		t.Helper()
		plan = finalizeFindPlan(plan)
		if len(plan.predicates) != 1 || !plan.predicates[0].compoundCanonicalized || len(plan.predicates[0].compoundCanonicalValues) != 1 {
			t.Fatalf("adapter plan did not cache bounded canonical $in: %#v", plan.predicates)
		}
	}
	// count, distinct, and their explain paths construct plans after
	// parseFindFilter rather than through parseFindPlan.
	assertCached(t, findPlan{predicates: predicates, orBranches: branches, skip: 1, limit: 1})
	assertCached(t, findPlan{predicates: predicates, orBranches: branches})

	aggregateCommandBytes, err := bson.Marshal(bson.D{{Key: "pipeline", Value: bson.A{bson.D{{Key: "$match", Value: bson.Raw(filterBytes)}}}}})
	if err != nil {
		t.Fatalf("marshal aggregate command: %v", err)
	}
	pipeline, err := commandBoundedDocumentArray(wire.Document(aggregateCommandBytes), "pipeline", mongoAggregateMaxStages)
	if err != nil {
		t.Fatalf("decode aggregate pipeline: %v", err)
	}
	stages, err := parseAggregateStages(pipeline)
	if err != nil {
		t.Fatalf("parse aggregate stages: %v", err)
	}
	if len(stages) != 1 {
		t.Fatalf("aggregate stages=%d want 1", len(stages))
	}
	assertCached(t, stages[0].plan)
}

func TestMongoCompoundPlannerCursorChargesPredicateSliceStructure(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	// Eight one-byte duplicate values leave the legacy payload-only accounting
	// below this cap, while the cloned predicate slice/header makes the actual
	// retained plan exceed it.  Cursor publication must therefore fail before
	// any cursor is observable.
	server.MaxCursorRetainedBytes = 300
	assertOK(t, serveCommand(t, server, 40651130, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "created", Value: int32(-1)}}}, {Key: "name", Value: "tenant_created"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 40651131, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(1)}}}}, {Key: "$db", Value: "app"}}))
	values := bson.A{"t", "t", "t", "t", "t", "t", "t", "t"}
	resp := serveCommand(t, server, 40651132, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: bson.D{{Key: "$in", Value: values}}}}}, {Key: "sort", Value: bson.D{{Key: "created", Value: int32(-1)}}}, {Key: "batchSize", Value: int32(0)}, {Key: "$db", Value: "app"}})
	assertCommandError(t, resp, "BadValue")
	server.cursorMu.Lock()
	defer server.cursorMu.Unlock()
	if len(server.cursors) != 0 {
		t.Fatalf("published cursor despite retained predicate structure cap: %d", len(server.cursors))
	}
}

func TestMongoCompoundPlannerCursorChargesRetainedPredicateField(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	server.MaxCursorRetainedBytes = 96
	field := strings.Repeat("f", 96)
	assertOK(t, serveCommand(t, server, 40651127, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: field, Value: int32(1)}, {Key: "created", Value: int32(-1)}}}, {Key: "name", Value: "field_created"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 40651128, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: field, Value: "t"}, {Key: "created", Value: int32(1)}}}}, {Key: "$db", Value: "app"}}))
	resp := serveCommand(t, server, 40651129, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: field, Value: "t"}}}, {Key: "sort", Value: bson.D{{Key: "created", Value: int32(-1)}}}, {Key: "batchSize", Value: int32(0)}, {Key: "$db", Value: "app"}})
	assertCommandError(t, resp, "BadValue")
	server.cursorMu.Lock()
	defer server.cursorMu.Unlock()
	if len(server.cursors) != 0 {
		t.Fatalf("published cursor despite retained predicate-field cap: %d", len(server.cursors))
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

func TestMongoCompoundPlannerCursorChargesFilteredDocumentAfterUpdate(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	server.MaxCursorRetainedBytes = 1024
	assertOK(t, serveCommand(t, server, 4065122, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "created", Value: int32(-1)}}}}}}, {Key: "$db", Value: "app"}}))
	docs := bson.A{
		bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(2)}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "t"}, {Key: "created", Value: int32(1)}},
	}
	assertOK(t, serveCommand(t, server, 4065123, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: docs}, {Key: "$db", Value: "app"}}))
	first := serveCommand(t, server, 4065124, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "sort", Value: bson.D{{Key: "created", Value: int32(-1)}}}, {Key: "batchSize", Value: int32(1)}, {Key: "$db", Value: "app"}})
	assertBatchIDs(t, cursorFirstBatch(t, first), []string{"a"})
	id := cursorIDFromResponse(t, first)
	update := bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "b"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "tenant", Value: "x"}, {Key: "payload", Value: strings.Repeat("p", 2048)}}}}}}
	assertOK(t, serveCommand(t, server, 4065125, bson.D{{Key: "update", Value: "events"}, {Key: "updates", Value: bson.A{update}}, {Key: "$db", Value: "app"}}))
	assertCommandError(t, serveCommand(t, server, 4065126, bson.D{{Key: "getMore", Value: id}, {Key: "collection", Value: "events"}, {Key: "$db", Value: "app"}}), "BadValue")
	server.cursorMu.Lock()
	defer server.cursorMu.Unlock()
	if _, ok := server.cursors[id]; ok {
		t.Fatal("cursor retained after filtered-document materialization cap failure")
	}
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

func TestMongoCompoundPlannerRejectsOversizedAndDuplicateSortOrHint(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 4065171, bson.D{{Key: "create", Value: "events"}, {Key: "$db", Value: "app"}}))
	base := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{}}, {Key: "$db", Value: "app"}}
	with := func(key string, value any) bson.D {
		command := append(bson.D(nil), base[:2]...)
		command = append(command, bson.E{Key: key, Value: value})
		return append(command, base[2:]...)
	}
	fiveFields := bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(1)}, {Key: "c", Value: int32(1)}, {Key: "d", Value: int32(1)}, {Key: "e", Value: int32(1)}}
	duplicateFields := bson.D{{Key: "a", Value: int32(1)}, {Key: "a", Value: int32(-1)}}
	assertCommandError(t, serveCommand(t, server, 4065172, with("sort", fiveFields)), "BadValue")
	assertCommandError(t, serveCommand(t, server, 4065173, with("sort", duplicateFields)), "BadValue")
	parseHint := func(value any) error {
		raw, err := bson.Marshal(bson.D{{Key: "hint", Value: value}})
		if err != nil {
			t.Fatal(err)
		}
		_, err = parseFindHint(wire.Document(raw))
		return err
	}
	if err := parseHint(fiveFields); err == nil || !strings.Contains(err.Error(), "one through four fields") {
		t.Fatalf("oversized hint parse error=%v want one-through-four-fields rejection", err)
	}
	if err := parseHint(duplicateFields); err == nil || !strings.Contains(err.Error(), "repeats field") {
		t.Fatalf("duplicate hint parse error=%v want repeated-field rejection", err)
	}
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

func TestMongoCompoundPlannerFailsClosedForArrayCapableBSONV2Indexes(t *testing.T) {
	// A BSON-v2 multikey index records array elements, while Mongo's visible
	// sort is a document operation (and empty arrays have no index entry).
	// The planner must therefore use the bounded document fallback, not apply
	// skip/limit to the physical index order.
	newServer := func(t *testing.T, multiKey, allowArrays bool) *Server {
		t.Helper()
		server := newMongoCompatibilityMatrixServer(t)
		meta := &collections.CollectionMeta{
			Name: "app.events",
			Options: collections.CollectionOptions{
				DocumentFormat:          collections.DocumentFormatBSON,
				AllowArrayValuesInIndex: allowArrays,
			},
			Indexes: []collections.IndexDefinition{{
				Name:      "score_1",
				Field:     "score",
				ValueType: collections.IndexValueBSONOrderedV2,
				MultiKey:  multiKey,
			}},
		}
		if !multiKey && !allowArrays {
			meta.Indexes = nil // bounded fallback baseline with identical documents.
		}
		if _, err := server.Collections.CreateCollection(meta); err != nil {
			t.Fatalf("create events collection: %v", err)
		}
		assertOK(t, serveCommand(t, server, 40652801, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "array"}, {Key: "tenant", Value: "t"}, {Key: "score", Value: bson.A{int32(2), int32(9)}}},
			bson.D{{Key: "_id", Value: "empty"}, {Key: "tenant", Value: "t"}, {Key: "score", Value: bson.A{}}},
			bson.D{{Key: "_id", Value: "scalar"}, {Key: "tenant", Value: "t"}, {Key: "score", Value: int32(4)}},
		}}, {Key: "$db", Value: "app"}}))
		return server
	}

	for _, tc := range []struct {
		name        string
		multiKey    bool
		allowArrays bool
	}{
		{name: "multikey", multiKey: true},
		{name: "collection_option", allowArrays: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			indexed := newServer(t, tc.multiKey, tc.allowArrays)
			baseline := newServer(t, false, false)
			for _, direction := range []int32{1, -1} {
				command := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "sort", Value: bson.D{{Key: "score", Value: direction}}}, {Key: "limit", Value: int32(1)}, {Key: "$db", Value: "app"}}
				got := serveCommand(t, indexed, 40652802+direction, command)
				want := serveCommand(t, baseline, 40652804+direction, command)
				assertOK(t, got)
				assertOK(t, want)
				if gotIDs, wantIDs := cursorFirstBatch(t, got), cursorFirstBatch(t, want); !bytes.Equal(gotIDs[0], wantIDs[0]) {
					t.Fatalf("direction=%d indexed fallback result differs from bounded scan: got=%s want=%s", direction, gotIDs[0], wantIDs[0])
				}
				explain := serveCommand(t, indexed, 40652806+direction, bson.D{{Key: "explain", Value: command}, {Key: "verbosity", Value: "queryPlanner"}, {Key: "$db", Value: "app"}})
				assertOK(t, explain)
				if stage := bson.Raw(explain).Lookup("queryPlanner").Document().Lookup("winningPlan").Document().Lookup("stage").StringValue(); stage == "compound_index_scan" {
					t.Fatalf("direction=%d selected array-capable compound index: %s", direction, explain)
				}
			}
			strict := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "t"}}}, {Key: "sort", Value: bson.D{{Key: "score", Value: int32(1)}}}, {Key: "hint", Value: "score_1"}, {Key: "$db", Value: "app"}}
			assertCommandError(t, serveCommand(t, indexed, 40652810, strict), "BadValue")
		})
	}
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

func TestMongoUnhintedCompoundDoesNotPreemptSelectiveLegacyCandidate(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	server.MaxFindScanDocuments = 2
	assertOK(t, serveCommand(t, server, 406570, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{
		bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "score", Value: int32(1)}}}, {Key: "name", Value: "tenant_score"}},
		bson.D{{Key: "key", Value: bson.D{{Key: "kind", Value: int32(1)}}}, {Key: "name", Value: "kind_1"}, {Key: "treedbValueType", Value: "string"}},
	}}, {Key: "$db", Value: "app"}}))
	docs := bson.A{}
	for i := 0; i < 4; i++ {
		kind := "common"
		if i == 3 {
			kind = "rare"
		}
		docs = append(docs, bson.D{{Key: "_id", Value: fmt.Sprintf("%d", i)}, {Key: "tenant", Value: "common"}, {Key: "score", Value: int32(i)}, {Key: "kind", Value: kind}})
	}
	assertOK(t, serveCommand(t, server, 406571, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: docs}, {Key: "$db", Value: "app"}}))
	command := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "common"}, {Key: "kind", Value: "rare"}}}, {Key: "$db", Value: "app"}}
	response := serveCommand(t, server, 406572, command)
	assertOK(t, response)
	assertBatchIDs(t, cursorFirstBatch(t, response), []string{"3"})
	explain := serveCommand(t, server, 406573, bson.D{{Key: "explain", Value: command}, {Key: "verbosity", Value: "executionStats"}, {Key: "$db", Value: "app"}})
	assertOK(t, explain)
	winning := bson.Raw(explain).Lookup("queryPlanner").Document().Lookup("winningPlan").Document()
	if got := winning.Lookup("indexName").StringValue(); got != "kind_1" {
		t.Fatalf("winner index=%q want kind_1: %s", got, explain)
	}
	if got := winning.Lookup("stage").StringValue(); got != "secondary_equality_lookup" {
		t.Fatalf("winner stage=%q want secondary_equality_lookup: %s", got, explain)
	}
}

func TestMongoUnhintedCompoundTriesLaterSelectiveCompoundCandidate(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	server.MaxFindScanDocuments = 2
	assertOK(t, serveCommand(t, server, 406574, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{
		bson.D{{Key: "key", Value: bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(1)}, {Key: "c", Value: int32(1)}}}, {Key: "name", Value: "a_b_c"}},
		bson.D{{Key: "key", Value: bson.D{{Key: "d", Value: int32(1)}, {Key: "e", Value: int32(1)}}}, {Key: "name", Value: "d_e"}},
	}}, {Key: "$db", Value: "app"}}))
	docs := bson.A{
		bson.D{{Key: "_id", Value: "0"}, {Key: "a", Value: "same"}, {Key: "b", Value: "same"}, {Key: "c", Value: "same"}, {Key: "d", Value: "other"}, {Key: "e", Value: "other"}},
		bson.D{{Key: "_id", Value: "1"}, {Key: "a", Value: "same"}, {Key: "b", Value: "same"}, {Key: "c", Value: "same"}, {Key: "d", Value: "other"}, {Key: "e", Value: "other"}},
		bson.D{{Key: "_id", Value: "2"}, {Key: "a", Value: "same"}, {Key: "b", Value: "same"}, {Key: "c", Value: "same"}, {Key: "d", Value: "wanted"}, {Key: "e", Value: "wanted"}},
	}
	assertOK(t, serveCommand(t, server, 406575, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: docs}, {Key: "$db", Value: "app"}}))
	command := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "a", Value: "same"}, {Key: "b", Value: "same"}, {Key: "c", Value: "same"}, {Key: "d", Value: "wanted"}, {Key: "e", Value: "wanted"}}}, {Key: "$db", Value: "app"}}
	response := serveCommand(t, server, 406576, command)
	assertOK(t, response)
	assertBatchIDs(t, cursorFirstBatch(t, response), []string{"2"})
	explain := serveCommand(t, server, 406577, bson.D{{Key: "explain", Value: command}, {Key: "verbosity", Value: "executionStats"}, {Key: "$db", Value: "app"}})
	assertOK(t, explain)
	winning := bson.Raw(explain).Lookup("queryPlanner").Document().Lookup("winningPlan").Document()
	if got := winning.Lookup("indexName").StringValue(); got != "d_e" {
		t.Fatalf("winner index=%q want d_e: %s", got, explain)
	}
}

func TestMongoSortedCompoundFallsBackToSelectiveLegacyCandidateOnCap(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	server.MaxFindScanDocuments = 2
	assertOK(t, serveCommand(t, server, 406578, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{
		bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "score", Value: int32(1)}}}, {Key: "name", Value: "tenant_score"}},
		bson.D{{Key: "key", Value: bson.D{{Key: "kind", Value: int32(1)}}}, {Key: "name", Value: "kind_1"}, {Key: "treedbValueType", Value: "string"}},
	}}, {Key: "$db", Value: "app"}}))
	docs := bson.A{}
	for i := 0; i < 3; i++ {
		kind := "common"
		if i == 2 {
			kind = "rare"
		}
		docs = append(docs, bson.D{{Key: "_id", Value: fmt.Sprintf("%d", i)}, {Key: "tenant", Value: "common"}, {Key: "kind", Value: kind}, {Key: "score", Value: int32(i)}})
	}
	assertOK(t, serveCommand(t, server, 406579, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: docs}, {Key: "$db", Value: "app"}}))
	command := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "common"}, {Key: "kind", Value: "rare"}}}, {Key: "sort", Value: bson.D{{Key: "score", Value: int32(1)}}}, {Key: "$db", Value: "app"}}
	response := serveCommand(t, server, 406580, command)
	assertOK(t, response)
	assertBatchIDs(t, cursorFirstBatch(t, response), []string{"2"})
	explain := serveCommand(t, server, 406581, bson.D{{Key: "explain", Value: command}, {Key: "verbosity", Value: "executionStats"}, {Key: "$db", Value: "app"}})
	assertOK(t, explain)
	winning := bson.Raw(explain).Lookup("queryPlanner").Document().Lookup("winningPlan").Document()
	if got := winning.Lookup("indexName").StringValue(); got != "kind_1" {
		t.Fatalf("winner index=%q want kind_1: %s", got, explain)
	}
}
