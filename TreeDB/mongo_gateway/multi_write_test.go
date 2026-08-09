package mongogateway

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
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

func TestMongoMultiWriteMaxTimeMSBoundsCommandDeadline(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "u1"}, {Key: "active", Value: true}},
		bson.D{{Key: "_id", Value: "u2"}, {Key: "active", Value: true}},
	}}, {Key: "$db", Value: "app"}}))
	// The hook runs after natural-order selection and before the checked atomic
	// mutation boundary. A short accepted maxTimeMS must become an indexed item
	// failure rather than silently using the gateway's five-second ceiling.
	s.filterWriteSelectedHook = func() { time.Sleep(10 * time.Millisecond) }
	update := serveCommand(t, s, 2, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{
		{Key: "q", Value: bson.D{{Key: "active", Value: true}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: true}}}}},
		{Key: "multi", Value: true},
	}}}, {Key: "maxTimeMS", Value: int32(1)}, {Key: "$db", Value: "app"}})
	assertOK(t, update)
	assertInt32(t, update, "n", 0)
	assertInt32(t, update, "nModified", 0)
	assertIndexedWriteError(t, update, 0)
	deleteResponse := serveCommand(t, s, 3, bson.D{{Key: "delete", Value: "users"}, {Key: "deletes", Value: bson.A{bson.D{
		{Key: "q", Value: bson.D{{Key: "active", Value: true}}},
		{Key: "limit", Value: int32(0)},
	}}}, {Key: "maxTimeMS", Value: int32(1)}, {Key: "$db", Value: "app"}})
	s.filterWriteSelectedHook = nil
	assertOK(t, deleteResponse)
	assertInt32(t, deleteResponse, "n", 0)
	assertIndexedWriteError(t, deleteResponse, 0)
	col, err := s.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open users: %v", err)
	}
	for _, id := range []string{"u1", "u2"} {
		key, err := encodePrimaryKey(bson.RawValue{Type: bson.TypeString, Value: bsoncore.AppendString(nil, id)})
		if err != nil {
			t.Fatalf("encode %s: %v", id, err)
		}
		stored, err := col.Get(key)
		if err != nil || stored == nil {
			t.Fatalf("maxTimeMS mutation changed/deleted %s: stored=%v err=%v", id, stored, err)
		}
	}
	start := time.Now()
	defaultBudget, err := s.newMongoWriteBudgetForCommand(context.Background(), mustDocument(t, bson.D{{Key: "update", Value: "users"}}))
	if err != nil {
		t.Fatalf("default deadline: %v", err)
	}
	if remaining := defaultBudget.deadline.Sub(start); remaining < 4*time.Second || remaining > mongoWriteCommandMaxDuration+time.Second {
		t.Fatalf("default deadline remaining=%s want five-second ceiling", remaining)
	}
	shortBudget, err := s.newMongoWriteBudgetForCommand(context.Background(), mustDocument(t, bson.D{{Key: "update", Value: "users"}, {Key: "maxTimeMS", Value: int32(10)}}))
	if err != nil {
		t.Fatalf("short maxTimeMS deadline: %v", err)
	}
	if remaining := time.Until(shortBudget.deadline); remaining <= 0 || remaining > 200*time.Millisecond {
		t.Fatalf("short maxTimeMS deadline remaining=%s want <=200ms", remaining)
	}
	longBudget, err := s.newMongoWriteBudgetForCommand(context.Background(), mustDocument(t, bson.D{{Key: "update", Value: "users"}, {Key: "maxTimeMS", Value: int64(10_000)}}))
	if err != nil {
		t.Fatalf("long maxTimeMS deadline: %v", err)
	}
	if remaining := time.Until(longBudget.deadline); remaining < 4*time.Second || remaining > mongoWriteCommandMaxDuration+time.Second {
		t.Fatalf("long maxTimeMS deadline remaining=%s want five-second ceiling", remaining)
	}
	for _, value := range []any{int32(0), int32(-1), "1", 1.5} {
		response := serveCommand(t, s, 10, bson.D{{Key: "update", Value: "missing"}, {Key: "updates", Value: bson.A{}}, {Key: "maxTimeMS", Value: value}, {Key: "$db", Value: "app"}})
		assertCommandError(t, response, "BadValue")
		if _, err := s.Collections.OpenCollection("app.missing"); !errors.Is(err, collections.ErrCollectionNotFound) {
			t.Fatalf("invalid maxTimeMS=%v opened missing collection: %v", value, err)
		}
	}
}

func TestMongoMissingCollectionWriteDeadlineStopsBeforeCatalogCreate(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	// This seam is immediately before the missing-namespace first-write path.
	// It makes expiry after parse/admission deterministic without relying on
	// scheduler timing or a slow catalog implementation.
	s.mongoWriteBeforeFirstCreateHook = func(budget *mongoWriteBudget) {
		budget.deadline = time.Now().Add(-time.Second)
	}
	insert := serveCommand(t, s, 1, bson.D{{Key: "insert", Value: "insert_missing"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "i1"}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, insert)
	assertInt32(t, insert, "n", 0)
	assertIndexedWriteError(t, insert, 0)
	if _, err := s.Collections.OpenCollection("app.insert_missing"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("expired insert created catalog entry: %v", err)
	}
	update := serveCommand(t, s, 2, bson.D{{Key: "update", Value: "update_missing"}, {Key: "updates", Value: bson.A{bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "value", Value: true}}}}},
		{Key: "upsert", Value: true},
	}}}, {Key: "$db", Value: "app"}})
	assertOK(t, update)
	assertInt32(t, update, "n", 0)
	assertInt32(t, update, "nModified", 0)
	assertIndexedWriteError(t, update, 0)
	if _, err := s.Collections.OpenCollection("app.update_missing"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("expired update upsert created catalog entry: %v", err)
	}
}

func TestMongoInsertUpsertDeadlineAfterResponseAdmissionRefundsWithoutPublication(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	col, err := s.openOrCreateCollection("app.users")
	if err != nil {
		t.Fatal(err)
	}
	item, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: "late"}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "value", Value: true}}}}},
		{Key: "upsert", Value: true},
	}))
	if err != nil {
		t.Fatal(err)
	}
	budget := s.newMongoWriteBudget()
	before := budget.responseBytesRemaining
	budget.beforeUpsertInsertHook = func() { budget.deadline = time.Now().Add(-time.Second) }
	item.budget = budget
	matched, modified, inserted, runErr := mongoInsertUpsert(col, item)
	if runErr == nil || matched || modified || inserted {
		t.Fatalf("late upsert outcome matched=%v modified=%v inserted=%v err=%v", matched, modified, inserted, runErr)
	}
	if budget.responseBytesRemaining != before {
		t.Fatalf("late upsert response reservation leaked: got %d want %d", budget.responseBytesRemaining, before)
	}
	if stored, err := col.Get(item.key); err != nil || stored != nil {
		t.Fatalf("late upsert published after deadline: stored=%v err=%v", stored, err)
	}
}

func TestMongoMissingCollectionUnorderedInvalidAdmittedUpsertDoesNotCreateCatalog(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	largeID := strings.Repeat("x", 3*mongoWriteErrorResponseReserveBytes)
	smallID := bson.RawValue{Type: bson.TypeString, Value: bsoncore.AppendString(nil, "small")}
	smallBytes, err := mongoUpdateUpsertResponseBytes(mongoUpdateUpserted{index: 1, id: smallID})
	if err != nil {
		t.Fatal(err)
	}
	// The first valid upsert cannot fit its successful response. The second
	// response can fit, but its replacement changes _id and must be validated
	// before a missing-collection first-write catalog entry is created.
	s.MaxMessageLength = int32(mongoWriteResponseMinimumBytes + 3*mongoWriteErrorResponseReserveBytes + smallBytes + 64)
	large := bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: largeID}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "value", Value: "large"}}}}}, {Key: "upsert", Value: true}}
	invalid := bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "small"}}}, {Key: "u", Value: bson.D{{Key: "_id", Value: "different"}, {Key: "value", Value: "small"}}}, {Key: "upsert", Value: true}}
	response := serveCommand(t, s, 1, bson.D{{Key: "update", Value: "missing"}, {Key: "ordered", Value: false}, {Key: "updates", Value: bson.A{large, invalid}}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	assertInt32(t, response, "n", 0)
	assertInt32(t, response, "nModified", 0)
	errs, ok := bson.Raw(response).Lookup("writeErrors").ArrayOK()
	values, valuesErr := errs.Values()
	if !ok || valuesErr != nil || len(values) != 2 || values[0].Document().Lookup("index").Int32() != 0 || values[1].Document().Lookup("index").Int32() != 1 {
		t.Fatalf("unordered invalid admission writeErrors=%s", response)
	}
	if _, err := s.Collections.OpenCollection("app.missing"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("invalid admitted upsert created catalog entry: %v", err)
	}
}

func TestMongoMissingCollectionUnorderedInvalidUpsertContinuesToValidUpsert(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	invalid := bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "bad"}}}, {Key: "u", Value: bson.D{{Key: "_id", Value: "different"}, {Key: "value", Value: "bad"}}}, {Key: "upsert", Value: true}}
	valid := bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "good"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "value", Value: "good"}}}}}, {Key: "upsert", Value: true}}
	response := serveCommand(t, s, 1, bson.D{{Key: "update", Value: "missing"}, {Key: "ordered", Value: false}, {Key: "updates", Value: bson.A{invalid, valid}}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	assertInt32(t, response, "n", 1)
	assertInt32(t, response, "nModified", 0)
	assertIndexedWriteError(t, response, 0)
	upserted, ok := bson.Raw(response).Lookup("upserted").ArrayOK()
	values, valuesErr := upserted.Values()
	if !ok || valuesErr != nil || len(values) != 1 || values[0].Document().Lookup("index").Int32() != 1 {
		t.Fatalf("unordered valid successor upserted=%s", response)
	}
	col, err := s.Collections.OpenCollection("app.missing")
	if err != nil {
		t.Fatalf("open valid successor collection: %v", err)
	}
	goodKey, err := encodePrimaryKey(bson.RawValue{Type: bson.TypeString, Value: bsoncore.AppendString(nil, "good")})
	if err != nil {
		t.Fatal(err)
	}
	badKey, err := encodePrimaryKey(bson.RawValue{Type: bson.TypeString, Value: bsoncore.AppendString(nil, "bad")})
	if err != nil {
		t.Fatal(err)
	}
	if stored, err := col.Get(goodKey); err != nil || stored == nil {
		t.Fatalf("valid successor not published: stored=%v err=%v", stored, err)
	}
	if stored, err := col.Get(badKey); err != nil || stored != nil {
		t.Fatalf("invalid predecessor published: stored=%v err=%v", stored, err)
	}
}

func TestMongoMissingCollectionAdmissionReplaysExactIDTargetBudget(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	s.mongoWriteTargetLimit = 1
	response := serveCommand(t, s, 1, bson.D{{Key: "update", Value: "missing"}, {Key: "ordered", Value: false}, {Key: "updates", Value: bson.A{
		bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "no-match"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "value", Value: 1}}}}}},
		bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "upsert"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "value", Value: 2}}}}}, {Key: "upsert", Value: true}},
	}}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	assertInt32(t, response, "n", 0)
	assertInt32(t, response, "nModified", 0)
	assertIndexedWriteError(t, response, 1)
	if _, err := s.Collections.OpenCollection("app.missing"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("target-budget rejected creator made catalog entry: %v", err)
	}
}

func TestMongoUnorderedNativeBSONInsertPlanningErrorContinues(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	s.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "createIndexes", Value: "users"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}}, {Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"}}}}, {Key: "$db", Value: "app"}}))
	tooLarge := strings.Repeat("x", 2<<20)
	response := serveCommand(t, s, 2, bson.D{{Key: "insert", Value: "users"}, {Key: "ordered", Value: false}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "oversized"}, {Key: "email", Value: tooLarge}},
		bson.D{{Key: "_id", Value: "later"}, {Key: "email", Value: "later@example.com"}},
	}}, {Key: "$db", Value: "app"}})
	assertIndexedWriteError(t, response, 0)
	assertInt32(t, response, "n", 1)
	assertMongoDocumentAbsent(t, s, "users", "oversized")
	assertMongoDocumentPresent(t, s, "users", "later")
}

func TestMongoUnorderedNativeBSONUpdatePlanningErrorContinues(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	s.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "createIndexes", Value: "users"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}}, {Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, s, 2, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "u1"}, {Key: "email", Value: "u1@example.com"}},
		bson.D{{Key: "_id", Value: "u2"}, {Key: "email", Value: "u2@example.com"}},
	}}, {Key: "$db", Value: "app"}}))
	response := serveCommand(t, s, 3, bson.D{{Key: "update", Value: "users"}, {Key: "ordered", Value: false}, {Key: "updates", Value: bson.A{
		// The string secondary index rejects this stored-document-dependent
		// value type during native batch planning. It is a valid update command,
		// so unordered execution must fall back to indexed per-item handling.
		bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "email", Value: int32(42)}}}}}},
		bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "email", Value: "updated@example.com"}}}}}},
	}}, {Key: "$db", Value: "app"}})
	assertIndexedWriteError(t, response, 0)
	assertInt32(t, response, "n", 1)
	assertMongoDocumentFieldString(t, s, "users", "u1", "email", "u1@example.com")
	assertMongoDocumentFieldString(t, s, "users", "u2", "email", "updated@example.com")
}

func TestMongoNativeUpdateBatchRechecksDeadlineBeforePublication(t *testing.T) {
	for _, format := range []collections.DocumentFormat{collections.DocumentFormatBSON, collections.DocumentFormatTemplateV1} {
		t.Run(string(format), func(t *testing.T) {
			db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			s := NewServer()
			s.Collections = collections.NewCollectionManager(db)
			s.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: format}
			assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{
				bson.D{{Key: "_id", Value: "u1"}, {Key: "v", Value: int32(0)}},
				bson.D{{Key: "_id", Value: "u2"}, {Key: "v", Value: int32(0)}},
			}}, {Key: "$db", Value: "app"}}))
			col, err := s.Collections.OpenCollection("app.users")
			if err != nil {
				t.Fatal(err)
			}
			parse := func(index int, id string) mongoUpdateItem {
				t.Helper()
				item, err := parseMongoUpdateItem(index, mustDocument(t, bson.D{
					{Key: "q", Value: bson.D{{Key: "_id", Value: id}}},
					{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "v", Value: int32(1)}}}}},
				}))
				if err != nil {
					t.Fatal(err)
				}
				return item
			}
			budget := newMongoWriteBudget(2)
			budget.beforeNativeUpdateBatchHook = func() { budget.deadline = time.Now().Add(-time.Second) }
			updates := []mongoUpdateItem{parse(0, "u1"), parse(1, "u2")}
			for i := range updates {
				updates[i].budget = budget
			}
			response, err := s.runMongoUpdateCommand("app.users", col, updates, false)
			if err != nil {
				t.Fatalf("run update command: %v", err)
			}
			assertOK(t, response)
			assertInt32(t, response, "n", 0)
			assertInt32(t, response, "nModified", 0)
			assertIndexedWriteError(t, response, 0)
			assertMongoDocumentFieldInt32(t, s, "users", "u1", "v", 0)
			assertMongoDocumentFieldInt32(t, s, "users", "u2", "v", 0)
		})
	}
}

func assertMongoDocumentAbsent(t *testing.T, s *Server, collection, id string) {
	t.Helper()
	response := serveCommand(t, s, 100, bson.D{{Key: "find", Value: collection}, {Key: "filter", Value: bson.D{{Key: "_id", Value: id}}}, {Key: "$db", Value: "app"}})
	if batch := cursorFirstBatch(t, response); len(batch) != 0 {
		t.Fatalf("%s/%s unexpectedly exists: %s", collection, id, response)
	}
}

func assertMongoDocumentPresent(t *testing.T, s *Server, collection, id string) {
	t.Helper()
	response := serveCommand(t, s, 100, bson.D{{Key: "find", Value: collection}, {Key: "filter", Value: bson.D{{Key: "_id", Value: id}}}, {Key: "$db", Value: "app"}})
	if batch := cursorFirstBatch(t, response); len(batch) != 1 {
		t.Fatalf("%s/%s not found: %s", collection, id, response)
	}
}

func assertMongoDocumentFieldString(t *testing.T, s *Server, collection, id, field, want string) {
	t.Helper()
	response := serveCommand(t, s, 100, bson.D{{Key: "find", Value: collection}, {Key: "filter", Value: bson.D{{Key: "_id", Value: id}}}, {Key: "$db", Value: "app"}})
	batch := cursorFirstBatch(t, response)
	if len(batch) != 1 {
		t.Fatalf("%s/%s not found: %s", collection, id, response)
	}
	if got, ok := batch[0].Lookup(field).StringValueOK(); !ok || got != want {
		t.Fatalf("%s/%s %s=%q ok=%v want %q", collection, id, field, got, ok, want)
	}
}

func assertMongoDocumentFieldInt32(t *testing.T, s *Server, collection, id, field string, want int32) {
	t.Helper()
	response := serveCommand(t, s, 100, bson.D{{Key: "find", Value: collection}, {Key: "filter", Value: bson.D{{Key: "_id", Value: id}}}, {Key: "$db", Value: "app"}})
	batch := cursorFirstBatch(t, response)
	if len(batch) != 1 {
		t.Fatalf("%s/%s not found: %s", collection, id, response)
	}
	if got, ok := batch[0].Lookup(field).Int32OK(); !ok || got != want {
		t.Fatalf("%s/%s %s=%d ok=%v want %d", collection, id, field, got, ok, want)
	}
}

func TestMongoInsertMinimumResponseEnvelopeRetainsTerminalError(t *testing.T) {
	// At the minimum accepted envelope there is no ordinary error reservation
	// left, but a pre-mutation runtime rejection must still be observable.
	budget := newMongoWriteBudget(0)
	budget.targetsRemaining = 0
	budget.responseBytesRemaining = 0
	response, err := (&Server{}).runMongoInsertCommand("app.users", nil, collections.DocumentFormatBSON, [][]byte{{1}, {2}}, [][]byte{{1}, {2}}, true, budget)
	if err != nil {
		t.Fatalf("minimum-envelope insert: %v", err)
	}
	assertOK(t, response)
	assertInt32(t, response, "n", 0)
	errs, ok := bson.Raw(response).Lookup("writeErrors").ArrayOK()
	values, valuesErr := errs.Values()
	if !ok || valuesErr != nil || len(values) != 1 {
		t.Fatalf("minimum-envelope writeErrors=%s", response)
	}
	if index, indexOK := values[0].Document().Lookup("index").Int32OK(); !indexOK || index != 0 {
		t.Fatalf("minimum-envelope error index=%d ok=%v", index, indexOK)
	}
}

func TestMongoUpdateUpsertMinimumResponseEnvelopeRejectsBeforeMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	// The terminal write-error slot exactly fits, but there is no capacity for
	// a successful upserted entry.  The upsert must not publish and then force
	// the transport to hide its outcome with an oversized response.
	s.MaxMessageLength = mongoWriteResponseMinimumBytes + mongoWriteErrorResponseReserveBytes
	assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "seed"}}}}, {Key: "$db", Value: "app"}}))
	response := serveCommand(t, s, 2, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "upsert"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "value", Value: true}}}}}, {Key: "upsert", Value: true}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	assertInt32(t, response, "n", 0)
	assertInt32(t, response, "nModified", 0)
	assertIndexedWriteError(t, response, 0)
	find := serveCommand(t, s, 3, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "upsert"}}}, {Key: "$db", Value: "app"}})
	if batch := cursorFirstBatch(t, find); len(batch) != 0 {
		t.Fatalf("response-budget rejected upsert published %d documents", len(batch))
	}
}

func TestMongoUpdateUpsertMinimumResponseEnvelopeRejectsBeforeMissingCollectionCreate(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	// There is room only for the terminal indexed error, not a successful
	// upserted entry. The rejection must happen before the missing collection is
	// opened or created, not merely before the document itself is inserted.
	s.MaxMessageLength = mongoWriteResponseMinimumBytes + mongoWriteErrorResponseReserveBytes
	response := serveCommand(t, s, 1, bson.D{{Key: "update", Value: "missing"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "upsert"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "value", Value: true}}}}}, {Key: "upsert", Value: true}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	assertInt32(t, response, "n", 0)
	assertInt32(t, response, "nModified", 0)
	assertIndexedWriteError(t, response, 0)
	if _, err := s.Collections.OpenCollection("app.missing"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("response-budget rejected missing-collection upsert created catalog entry: %v", err)
	}
}

func TestMongoUpdateUpsertMissingCollectionResponseAdmissionContinuesUnordered(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	largeID := strings.Repeat("x", 2*mongoWriteErrorResponseReserveBytes)
	smallID := bson.RawValue{Type: bson.TypeString, Value: bsoncore.AppendString(nil, "small")}
	smallBytes, err := mongoUpdateUpsertResponseBytes(mongoUpdateUpserted{index: 1, id: smallID})
	if err != nil {
		t.Fatalf("size small upsert response: %v", err)
	}
	// The command has room for its terminal indexed error, one ordinary error,
	// and the second upserted entry, but not the first large entry. Unordered
	// processing must record index 0 then create the collection for index 1.
	// Keep a small wire-command allowance in addition to the response budget;
	// the large selector is present in the request but not in the result.
	s.MaxMessageLength = int32(mongoWriteResponseMinimumBytes + 2*mongoWriteErrorResponseReserveBytes + smallBytes + 64)
	large := bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: largeID}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "value", Value: "large"}}}}}, {Key: "upsert", Value: true}}
	small := bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "small"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "value", Value: "small"}}}}}, {Key: "upsert", Value: true}}
	response := serveCommand(t, s, 1, bson.D{{Key: "update", Value: "missing"}, {Key: "ordered", Value: false}, {Key: "updates", Value: bson.A{large, small}}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	assertInt32(t, response, "n", 1)
	assertInt32(t, response, "nModified", 0)
	assertIndexedWriteError(t, response, 0)
	upserted, ok := bson.Raw(response).Lookup("upserted").ArrayOK()
	values, valuesErr := upserted.Values()
	if !ok || valuesErr != nil || len(values) != 1 || values[0].Document().Lookup("index").Int32() != 1 {
		t.Fatalf("unordered missing-collection upserted=%s", response)
	}
	col, err := s.Collections.OpenCollection("app.missing")
	if err != nil {
		t.Fatalf("open admitted collection: %v", err)
	}
	key, err := encodePrimaryKey(smallID)
	if err != nil {
		t.Fatalf("encode admitted id: %v", err)
	}
	if stored, err := col.Get(key); err != nil || stored == nil {
		t.Fatalf("unordered admission did not publish later upsert: stored=%v err=%v", stored, err)
	}
}

func TestMongoUpdateUpsertResponseBudgetPreservesUpsertAndLaterError(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	upsertID := bson.RawValue{Type: bson.TypeString, Value: bsoncore.AppendString(nil, "upsert")}
	upsertBytes, err := mongoUpdateUpsertResponseBytes(mongoUpdateUpserted{index: 0, id: upsertID})
	if err != nil {
		t.Fatalf("size upsert response: %v", err)
	}
	// One successful upsert plus one ordinary indexed error, in addition to
	// the terminal slot, must fit and retain both observable outcomes.
	s.MaxMessageLength = int32(mongoWriteResponseMinimumBytes + 2*mongoWriteErrorResponseReserveBytes + upsertBytes)
	assertOK(t, serveCommand(t, s, 10, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "bad"}, {Key: "count", Value: "not-a-number"}}}}, {Key: "$db", Value: "app"}}))
	upsert := bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "upsert"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "value", Value: true}}}}}, {Key: "upsert", Value: true}}
	runtimeFailure := bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "bad"}}}, {Key: "u", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "count", Value: int32(1)}}}}}}
	response := serveCommand(t, s, 11, bson.D{{Key: "update", Value: "users"}, {Key: "ordered", Value: false}, {Key: "updates", Value: bson.A{upsert, runtimeFailure}}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	assertInt32(t, response, "n", 1)
	assertInt32(t, response, "nModified", 0)
	upserted, ok := bson.Raw(response).Lookup("upserted").ArrayOK()
	values, valuesErr := upserted.Values()
	if !ok || valuesErr != nil || len(values) != 1 || values[0].Document().Lookup("index").Int32() != 0 {
		t.Fatalf("upserted=%s", response)
	}
	assertIndexedWriteError(t, response, 1)
	message, err := wire.AppendMsgMessage(nil, 11, 0, 0, response)
	if err != nil || len(message) > int(s.maxMessageLength()) {
		t.Fatalf("mixed upsert response bytes=%d max=%d err=%v", len(message), s.maxMessageLength(), err)
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

func TestMongoDeleteRequiresLimitBeforeMutation(t *testing.T) {
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
	}}, {Key: "$db", Value: "app"}}))
	response := serveCommand(t, s, 2, bson.D{{Key: "delete", Value: "users"}, {Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "city", Value: "hnl"}}}}}}, {Key: "$db", Value: "app"}})
	assertCommandError(t, response, "FailedToParse")
	find := serveCommand(t, s, 3, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "city", Value: "hnl"}}}, {Key: "$db", Value: "app"}})
	if got := len(cursorFirstBatch(t, find)); got != 2 {
		t.Fatalf("missing delete limit mutated %d documents", 2-got)
	}
}

func TestMongoMultiUpdateReplacementIsRejectedBeforeEarlierMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}}, bson.D{{Key: "_id", Value: "b"}}}}, {Key: "$db", Value: "app"}}))
	response := serveCommand(t, s, 2, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{
		bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "a"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: true}}}}}},
		bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "b"}}}, {Key: "u", Value: bson.D{{Key: "replacement", Value: true}}}, {Key: "multi", Value: true}},
	}}, {Key: "$db", Value: "app"}})
	assertCommandError(t, response, "BadValue")
	find := serveCommand(t, s, 3, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "a"}}}, {Key: "$db", Value: "app"}})
	if !cursorFirstBatch(t, find)[0].Lookup("seen").IsZero() {
		t.Fatalf("later multi replacement allowed earlier mutation: %s", find)
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
