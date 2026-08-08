package mongogateway

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMongoWriteBudgetRejectsExpiredAndExhaustedWork(t *testing.T) {
	expired := &mongoWriteBudget{examinedRemaining: 1, deadline: time.Now().Add(-time.Second)}
	if err := expired.charge(); err == nil {
		t.Fatal("expired budget accepted work")
	}
	exhausted := &mongoWriteBudget{examinedRemaining: 0, deadline: time.Now().Add(time.Second)}
	if err := exhausted.charge(); err == nil {
		t.Fatal("exhausted budget accepted work")
	}
	capOne := newMongoWriteBudget(1)
	if err := capOne.charge(); err != nil {
		t.Fatalf("first examined document: %v", err)
	}
	if err := capOne.reserveTarget(); err != nil {
		t.Fatalf("first retained target: %v", err)
	}
	if err := capOne.charge(); err == nil {
		t.Fatal("second spec reset shared examined cap")
	}
	postSelection := newMongoWriteBudget(1)
	postSelection.deadline = time.Now().Add(-time.Second)
	if err := postSelection.checkDeadline(); err == nil {
		t.Fatal("post-selection deadline accepted mutation")
	}
	keyBytes := &mongoWriteBudget{targetsRemaining: 1, retainedKeyBytesRemaining: 1, deadline: time.Now().Add(time.Second)}
	if err := keyBytes.reserveTargetKey(2); err == nil {
		t.Fatal("retained-key byte budget accepted oversized key")
	}
	if keyBytes.targetsRemaining != 1 || keyBytes.retainedKeyBytesRemaining != 1 {
		t.Fatalf("failed retained-key reservation consumed capacity: %+v", keyBytes)
	}
}

func TestMongoWriteScanLookaheadSaturates(t *testing.T) {
	if got := mongoWriteScanLookahead(maxInt); got != maxInt {
		t.Fatalf("max-int lookahead=%d want %d", got, maxInt)
	}
	if got := mongoWriteScanLookahead(7); got != 8 {
		t.Fatalf("lookahead=%d want 8", got)
	}
}

func TestMongoWriteBudgetReservesBoundedResponseEnvelope(t *testing.T) {
	s := NewServer()
	// One slot is held back for the terminal indexed exhaustion error.
	s.MaxMessageLength = mongoWriteResponseMinimumBytes + 2*mongoWriteErrorResponseReserveBytes
	budget := s.newMongoWriteBudget()
	if err := budget.ensureMinimumResponse(); err != nil {
		t.Fatalf("minimum response rejected: %v", err)
	}
	if err := budget.reserveError(); err != nil {
		t.Fatalf("first bounded error: %v", err)
	}
	if err := budget.reserveError(); err == nil {
		t.Fatal("second error exceeded configured response envelope")
	}
	s.MaxMessageLength = mongoWriteResponseMinimumBytes - 1
	if err := s.newMongoWriteBudget().ensureMinimumResponse(); err == nil {
		t.Fatal("too-small response limit accepted mutation-capable command")
	}
}

func TestMongoMultiWriteUpdateManyDeleteManyAndParseBeforeExecute(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "a"}, {Key: "city", Value: "hnl"}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "city", Value: "hnl"}},
		bson.D{{Key: "_id", Value: "c"}, {Key: "city", Value: "sea"}},
	}}, {Key: "$db", Value: "app"}}))
	response := serveCommand(t, s, 2, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "city", Value: "hnl"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: true}}}}}, {Key: "multi", Value: true}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	assertInt32(t, response, "n", 2)
	assertInt32(t, response, "nModified", 2)
	response = serveCommand(t, s, 3, bson.D{{Key: "delete", Value: "users"}, {Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "city", Value: "hnl"}}}, {Key: "limit", Value: int32(0)}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	assertInt32(t, response, "n", 2)
	response = serveCommand(t, s, 4, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{
		bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "c"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: true}}}}}},
		bson.D{{Key: "q", Value: "malformed"}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: false}}}}}},
	}}, {Key: "$db", Value: "app"}})
	assertCommandError(t, response, "FailedToParse")
	find := serveCommand(t, s, 5, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "c"}}}, {Key: "$db", Value: "app"}})
	if got := cursorFirstBatch(t, find)[0].Lookup("seen"); !got.IsZero() {
		t.Fatalf("malformed later item mutated prior document: %s", got)
	}
}

func TestMongoMultiWriteOrderedAndUnorderedStableIndices(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "createIndexes", Value: "users"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}}, {Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"}, {Key: "unique", Value: true}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, s, 2, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: "email", Value: "a@example.com"}}, bson.D{{Key: "_id", Value: "b"}, {Key: "email", Value: "b@example.com"}}}}, {Key: "$db", Value: "app"}}))
	for _, ordered := range []bool{true, false} {
		response := serveCommand(t, s, 3, bson.D{{Key: "update", Value: "users"}, {Key: "ordered", Value: ordered}, {Key: "updates", Value: bson.A{
			bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "a"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "email", Value: "b@example.com"}}}}}},
			bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "b"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "ok", Value: true}}}}}},
		}}, {Key: "$db", Value: "app"}})
		assertOK(t, response)
		errs, ok := bson.Raw(response).Lookup("writeErrors").ArrayOK()
		values, valuesErr := errs.Values()
		if !ok || valuesErr != nil || len(values) != 1 {
			t.Fatalf("writeErrors=%s", response)
		}
		index, _ := values[0].Document().Lookup("index").Int32OK()
		if index != 0 {
			t.Fatalf("error index=%d", index)
		}
		find := serveCommand(t, s, 3, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "b"}}}, {Key: "$db", Value: "app"}})
		got := cursorFirstBatch(t, find)[0].Lookup("ok")
		if ordered && !got.IsZero() {
			t.Fatal("ordered command executed later item")
		}
		if !ordered && got.IsZero() {
			t.Fatal("unordered command did not continue")
		}
	}
}

func TestMongoInsertBatchOrderedAndUnorderedStableIndices(t *testing.T) {
	for _, ordered := range []bool{true, false} {
		t.Run(map[bool]string{true: "ordered", false: "unordered"}[ordered], func(t *testing.T) {
			db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			s := NewServer()
			s.Collections = collections.NewCollectionManager(db)
			assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "taken"}}}}, {Key: "$db", Value: "app"}}))
			response := serveCommand(t, s, 2, bson.D{{Key: "insert", Value: "users"}, {Key: "ordered", Value: ordered}, {Key: "documents", Value: bson.A{
				bson.D{{Key: "_id", Value: "taken"}},
				bson.D{{Key: "_id", Value: "later"}},
			}}, {Key: "$db", Value: "app"}})
			assertOK(t, response)
			wantN := int32(0)
			if !ordered {
				wantN = 1
			}
			assertInt32(t, response, "n", wantN)
			errs, ok := bson.Raw(response).Lookup("writeErrors").ArrayOK()
			values, valuesErr := errs.Values()
			if !ok || valuesErr != nil || len(values) != 1 {
				t.Fatalf("writeErrors=%s", response)
			}
			if index, indexOK := values[0].Document().Lookup("index").Int32OK(); !indexOK || index != 0 {
				t.Fatalf("writeErrors index=%d ok=%v, want 0", index, indexOK)
			}
			find := serveCommand(t, s, 3, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "later"}}}, {Key: "$db", Value: "app"}})
			if got := len(cursorFirstBatch(t, find)); (got == 1) != !ordered {
				t.Fatalf("later insert presence=%v, ordered=%v", got == 1, ordered)
			}
		})
	}
}

func TestMongoMultiUpdateDeleteRejectUnsupportedPerItemOptionsBeforeMutation(t *testing.T) {
	for _, tc := range []struct {
		name string
		cmd  bson.D
	}{
		{"update collation", bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "changed", Value: true}}}}}, {Key: "multi", Value: true}, {Key: "collation", Value: bson.D{{Key: "locale", Value: "en"}}}}}}, {Key: "$db", Value: "app"}}},
		{"delete hint", bson.D{{Key: "delete", Value: "users"}, {Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(0)}, {Key: "hint", Value: "_id_"}}}}, {Key: "$db", Value: "app"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			s := NewServer()
			s.Collections = collections.NewCollectionManager(db)
			assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}}, {Key: "$db", Value: "app"}}))
			assertCommandError(t, serveCommand(t, s, 2, tc.cmd), "BadValue")
			if got := len(cursorFirstBatch(t, serveCommand(t, s, 3, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}}))); got != 1 {
				t.Fatalf("unsupported option mutated document count=%d", got)
			}
		})
	}
}

func TestMongoInsertBatchSecondaryUniqueConflictRollsBackFastPath(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "createIndexes", Value: "users"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}}, {Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"}, {Key: "unique", Value: true}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, s, 2, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "seed"}, {Key: "email", Value: "taken@example.com"}}}}, {Key: "$db", Value: "app"}}))
	s.mongoWriteTargetLimit = 2 // exact native-batch boundary; fallback must refund it.
	response := serveCommand(t, s, 3, bson.D{{Key: "insert", Value: "users"}, {Key: "ordered", Value: false}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "conflict"}, {Key: "email", Value: "taken@example.com"}},
		bson.D{{Key: "_id", Value: "later"}, {Key: "email", Value: "later@example.com"}},
	}}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	assertInt32(t, response, "n", 1)
	errs, ok := bson.Raw(response).Lookup("writeErrors").ArrayOK()
	values, valuesErr := errs.Values()
	if !ok || valuesErr != nil || len(values) != 1 {
		t.Fatalf("secondary unique writeErrors=%s", response)
	}
	if index, indexOK := values[0].Document().Lookup("index").Int32OK(); !indexOK || index != 0 {
		t.Fatalf("secondary unique writeErrors index=%d ok=%v, want 0", index, indexOK)
	}
	find := serveCommand(t, s, 4, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "later"}}}, {Key: "$db", Value: "app"}})
	if len(cursorFirstBatch(t, find)) != 1 {
		t.Fatal("secondary unique fallback did not insert later document")
	}
}

func TestMongoInsertCommandExpiredBudgetStopsBeforeMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	s.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "seed"}}}}, {Key: "$db", Value: "app"}}))
	col, err := s.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatal(err)
	}
	doc := mustDocument(t, bson.D{{Key: "_id", Value: "expired"}})
	ids, stored, err := prepareInsertDocuments([]wire.Document{doc}, collections.DocumentFormatBSON)
	if err != nil {
		t.Fatal(err)
	}
	budget := newMongoWriteBudget(1)
	budget.deadline = time.Now().Add(-time.Second)
	response, err := s.runMongoInsertCommand("app.users", col, collections.DocumentFormatBSON, ids, stored, false, budget)
	if err != nil {
		t.Fatalf("run expired insert: %v", err)
	}
	assertOK(t, response)
	assertInt32(t, response, "n", 0)
	if bson.Raw(response).Lookup("writeErrors").IsZero() {
		t.Fatalf("expired insert omitted indexed error: %s", response)
	}
	find := serveCommand(t, s, 2, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "expired"}}}, {Key: "$db", Value: "app"}})
	if len(cursorFirstBatch(t, find)) != 0 {
		t.Fatal("expired insert mutated collection")
	}
}

func TestMongoInsertCommandNativeBatchReservesWholeTargetGranule(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	s.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "seed"}}}}, {Key: "$db", Value: "app"}}))
	col, err := s.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatal(err)
	}
	prepare := func(ids ...string) ([][]byte, [][]byte) {
		docs := make([]wire.Document, len(ids))
		for i, id := range ids {
			docs[i] = mustDocument(t, bson.D{{Key: "_id", Value: id}})
		}
		keys, stored, prepareErr := prepareInsertDocuments(docs, collections.DocumentFormatBSON)
		if prepareErr != nil {
			t.Fatal(prepareErr)
		}
		return keys, stored
	}
	keys, stored := prepare("boundary-a", "boundary-b")
	response, err := s.runMongoInsertCommand("app.users", col, collections.DocumentFormatBSON, keys, stored, true, newMongoWriteBudget(2))
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, response)
	assertInt32(t, response, "n", 2)
	keys, stored = prepare("over-a", "over-b")
	response, err = s.runMongoInsertCommand("app.users", col, collections.DocumentFormatBSON, keys, stored, true, newMongoWriteBudget(1))
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, response)
	assertInt32(t, response, "n", 0)
	if bson.Raw(response).Lookup("writeErrors").IsZero() {
		t.Fatalf("over-cap batch omitted indexed error: %s", response)
	}
	for _, id := range []string{"over-a", "over-b"} {
		find := serveCommand(t, s, 2, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: id}}}, {Key: "$db", Value: "app"}})
		if len(cursorFirstBatch(t, find)) != 0 {
			t.Fatalf("over-cap native batch mutated %s", id)
		}
	}
}

func TestMongoInsertOverCapRejectsBeforeFirstCollectionCreate(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	s.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	s.mongoWriteTargetLimit = 1
	response := serveCommand(t, s, 1, bson.D{{Key: "insert", Value: "missing"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "over-a"}},
		bson.D{{Key: "_id", Value: "over-b"}},
	}}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	assertInt32(t, response, "n", 0)
	if bson.Raw(response).Lookup("writeErrors").IsZero() {
		t.Fatalf("over-cap first write omitted indexed error: %s", response)
	}
	if _, openErr := s.Collections.OpenCollection("app.missing"); !errors.Is(openErr, collections.ErrCollectionNotFound) {
		t.Fatalf("over-cap insert created collection: err=%v", openErr)
	}
}

func TestMongoMultiUpdateRetainedKeyByteCapStopsBeforeMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	s.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	s.MaxFindScanDocuments = 2
	s.mongoWriteRetainedKeyBytesLimit = 1 // less than one encoded BSON string _id key
	assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "large-id-a"}, {Key: "active", Value: true}},
		bson.D{{Key: "_id", Value: "large-id-b"}, {Key: "active", Value: true}},
	}}, {Key: "$db", Value: "app"}}))
	response := serveCommand(t, s, 2, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{
		{Key: "q", Value: bson.D{{Key: "active", Value: true}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: true}}}}},
		{Key: "multi", Value: true},
	}}}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	assertInt32(t, response, "n", 0)
	if bson.Raw(response).Lookup("writeErrors").IsZero() {
		t.Fatalf("retained-key byte overflow omitted indexed error: %s", response)
	}
	find := serveCommand(t, s, 3, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "active", Value: true}}}, {Key: "$db", Value: "app"}})
	for _, document := range cursorFirstBatch(t, find) {
		if !document.Lookup("seen").IsZero() {
			t.Fatalf("retained-key byte overflow mutated selected document")
		}
	}
}

func TestMongoWriteErrorsResponseIsBoundedBelowWireLimit(t *testing.T) {
	longError := errors.New(strings.Repeat("界", 512))
	writeErrors := make([]mongoWriteError, mongoWriteCommandMaxErrorEntries)
	for i := range writeErrors {
		writeErrors[i] = mongoWriteError{index: i, err: longError}
	}
	response, err := marshalInsertResponseWithWriteErrors(0, writeErrors)
	if err != nil {
		t.Fatalf("marshal bounded writeErrors: %v", err)
	}
	message, err := wire.AppendMsgMessage(nil, 1, 0, 0, response)
	if err != nil {
		t.Fatalf("append bounded writeErrors message: %v", err)
	}
	if len(message) > wire.DefaultMaxMessageLength {
		t.Fatalf("writeErrors response bytes=%d exceeds wire max=%d", len(message), wire.DefaultMaxMessageLength)
	}
}

func TestMongoMultiWriteErrorBudgetTerminalExhaustion(t *testing.T) {
	for _, ordered := range []bool{true, false} {
		t.Run(map[bool]string{true: "ordered", false: "unordered"}[ordered], func(t *testing.T) {
			db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			s := NewServer()
			s.Collections = collections.NewCollectionManager(db)
			assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "createIndexes", Value: "users"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}}, {Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"}, {Key: "unique", Value: true}}}}, {Key: "$db", Value: "app"}}))
			assertOK(t, serveCommand(t, s, 2, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: "email", Value: "a@example.com"}}, bson.D{{Key: "_id", Value: "b"}, {Key: "email", Value: "b@example.com"}}}}, {Key: "$db", Value: "app"}}))
			updates := make(bson.A, 0, mongoWriteCommandMaxErrorEntries+1)
			for range mongoWriteCommandMaxErrorEntries {
				updates = append(updates, bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "a"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "email", Value: "b@example.com"}}}}}})
			}
			updates = append(updates, bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "b"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "later", Value: true}}}}}})
			response := serveCommand(t, s, 3, bson.D{{Key: "update", Value: "users"}, {Key: "ordered", Value: ordered}, {Key: "updates", Value: updates}, {Key: "$db", Value: "app"}})
			assertOK(t, response)
			errs, ok := bson.Raw(response).Lookup("writeErrors").ArrayOK()
			values, valuesErr := errs.Values()
			if !ok || valuesErr != nil {
				t.Fatalf("writeErrors=%s", response)
			}
			if ordered {
				if len(values) != 1 {
					t.Fatalf("ordered errors=%d want 1", len(values))
				}
			} else if len(values) != mongoWriteCommandMaxErrorEntries {
				t.Fatalf("unordered errors=%d want %d", len(values), mongoWriteCommandMaxErrorEntries)
			}
			last := values[len(values)-1].Document()
			index, _ := last.Lookup("index").Int32OK()
			wantIndex := int32(0)
			if !ordered {
				wantIndex = mongoWriteCommandMaxErrorEntries - 1
				if !strings.Contains(last.Lookup("errmsg").StringValue(), "budget") {
					t.Fatalf("terminal error=%s", last)
				}
			}
			if index != wantIndex {
				t.Fatalf("terminal index=%d want %d", index, wantIndex)
			}
			find := serveCommand(t, s, 4, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "b"}}}, {Key: "$db", Value: "app"}})
			if got := cursorFirstBatch(t, find)[0].Lookup("later"); !got.IsZero() {
				t.Fatalf("error budget executed later item: %s", got)
			}
			message, err := wire.AppendMsgMessage(nil, 4, 0, 0, response)
			if err != nil || len(message) > wire.DefaultMaxMessageLength {
				t.Fatalf("bounded response bytes=%d err=%v", len(message), err)
			}
		})
	}
}

func TestMongoMultiWriteSharedScanCapDoesNotResetForSecondSpec(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	s.MaxFindScanDocuments = 1
	s.mongoWriteTargetLimit = 1
	assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: "city", Value: "hnl"}}, bson.D{{Key: "_id", Value: "b"}, {Key: "city", Value: "sea"}}}}, {Key: "$db", Value: "app"}}))
	response := serveCommand(t, s, 2, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{
		bson.D{{Key: "q", Value: bson.D{{Key: "city", Value: "hnl"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: true}}}}}},
		bson.D{{Key: "q", Value: bson.D{{Key: "city", Value: "sea"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: true}}}}}},
	}}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	if raw := bson.Raw(response); raw.Lookup("writeErrors").IsZero() {
		t.Fatalf("second spec reset cap: %s", response)
	}
}

func TestMongoMultiWriteExactIDTargetCapPreservesPartialResult(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	s.MaxFindScanDocuments = 1
	s.mongoWriteTargetLimit = 1
	assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}}, bson.D{{Key: "_id", Value: "b"}}}}, {Key: "$db", Value: "app"}}))
	response := serveCommand(t, s, 2, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{
		bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "a"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: true}}}}}},
		bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "b"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: true}}}}}},
	}}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	assertInt32(t, response, "n", 1)
	if bson.Raw(response).Lookup("writeErrors").IsZero() {
		t.Fatalf("exact-id target cap did not report partial error: %s", response)
	}
}

func TestMongoMultiWriteSparseFilterDoesNotConsumeTargetBudgetForNonmatch(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	s.MaxFindScanDocuments = 2
	s.mongoWriteTargetLimit = 2
	docs := bson.A{bson.D{{Key: "_id", Value: "a"}, {Key: "city", Value: "sea"}}, bson.D{{Key: "_id", Value: "b"}, {Key: "city", Value: "hnl"}}}
	assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: docs}, {Key: "$db", Value: "app"}}))
	first := bson.D{{Key: "q", Value: bson.D{{Key: "city", Value: "hnl"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: true}}}}}, {Key: "multi", Value: true}}
	second := bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "a"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: true}}}}}}
	r := serveCommand(t, s, 2, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{first, second}}, {Key: "$db", Value: "app"}})
	assertOK(t, r)
	assertInt32(t, r, "n", 2)
	assertInt32(t, r, "nModified", 2)
	if !bson.Raw(r).Lookup("writeErrors").IsZero() {
		t.Fatalf("sparse nonmatch consumed target capacity: %s", r)
	}
}
