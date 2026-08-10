package mongogateway

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

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

func TestMongoCompoundPlanSatisfiesMultiFieldSortWithPagination(t *testing.T) {
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
	if satisfied, ok := sortInfo.Lookup("satisfied").BooleanOK(); !ok || !satisfied {
		t.Fatalf("sort satisfied=%v ok=%v want true: %s", satisfied, ok, explain)
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
