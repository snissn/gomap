package mongogateway

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

const mongoFilterWriteTestTimeout = 5 * time.Second

func awaitMongoFilterWriteSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(mongoFilterWriteTestTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestMongoFilterWriteAttemptReset(t *testing.T) {
	for _, name := range []string{"update", "delete"} {
		predicateMatched := true                        // first callback matched
		resetMongoFilterWriteAttempt(&predicateMatched) // second callback starts nonmatching
		if predicateMatched {
			t.Fatalf("%s retry retained first callback match", name)
		}
	}
	predicateMatched := true
	var before, after wire.Document = wire.Document{1}, wire.Document{2}
	resetMongoFindAndModifyAttempt(&predicateMatched, &before, &after) // second callback is nonmatching
	if predicateMatched || before != nil || after != nil {
		t.Fatalf("findAndModify retry state=%v before=%v after=%v", predicateMatched, before, after)
	}
}

func TestMongoFilterWriteOutcomeReconcilesSkippedCallback(t *testing.T) {
	for _, name := range []string{"update", "delete"} {
		predicateMatched := true                                   // first internal attempt invoked and matched the callback
		reconcileMongoFilterWriteOutcome(&predicateMatched, false) // final attempt found no document
		if predicateMatched {
			t.Fatalf("%s retained callback match after missing final attempt", name)
		}
	}
	predicateMatched := true
	before, after := wire.Document{1}, wire.Document{2} // first internal callback populated images
	reconcileMongoFindAndModifyOutcome(&predicateMatched, &before, &after, false)
	if predicateMatched || before != nil || after != nil {
		t.Fatalf("findAndModify retained stale callback state=%v before=%v after=%v", predicateMatched, before, after)
	}
}

func TestMongoFilterDeleteOneColumnStoreReconstruction(t *testing.T) {
	dir := t.TempDir()
	backend, closeBackend, err := treedb.OpenBackend(treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir))
	if err != nil {
		t.Fatal(err)
	}
	defer closeBackend()
	mgr := collections.NewCollectionManager(backend)
	cfg := &collections.ColumnStoreConfig{Enabled: true, Columns: []collections.ColumnStoreColumn{
		{Name: "time_us", Path: "time_us", ValueType: collections.ColumnStoreValueInt64},
		{Name: "kind", Path: "kind", ValueType: collections.ColumnStoreValueString},
	}, SortKey: []collections.ColumnSortKey{{Column: "time_us"}}}
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "app.users", Options: collections.CollectionOptions{ColumnStore: cfg}}); err != nil {
		t.Fatal(err)
	}
	server := NewServer()
	server.Collections = mgr
	server.MaxFindScanDocuments = 1
	col, err := mgr.OpenCollection("app.users")
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 4; i++ {
		id := fmt.Sprintf("u%d", i)
		key, err := encodePrimaryKey(bson.Raw(mustDocument(t, bson.D{{Key: "_id", Value: id}})).Lookup("_id"))
		if err != nil {
			t.Fatal(err)
		}
		kind := "skip"
		if i == 1 {
			kind = "like"
		}
		if _, err := col.Insert(key, []byte(fmt.Sprintf(`{"_id":%q,"time_us":%d,"kind":%q,"payload":"row"}`, id, i, kind))); err != nil {
			t.Fatal(err)
		}
	}
	deleted := serveCommand(t, server, 2, bson.D{{Key: "delete", Value: "users"}, {Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "kind", Value: "like"}}}, {Key: "limit", Value: int32(1)}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, deleted)
	assertInt32(t, deleted, "n", 1)
	remaining := serveCommand(t, server, 3, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}})
	assertOK(t, remaining)
	if values, err := bson.Raw(remaining).Lookup("cursor").Document().Lookup("firstBatch").Array().Values(); err != nil || len(values) != 0 {
		t.Fatalf("column delete remained: values=%v err=%v", values, err)
	}
}

func TestMongoFilterWritesSelectOneAcrossUpdateDeleteAndFindAndModify(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	server.UpdateCoalescingMaxDelay = 5 * time.Second
	server.UpdateCoalescingMaxBatch = 2
	assertOK(t, serveCommand(t, server, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "u1"}, {Key: "age", Value: int64(30)}, {Key: "active", Value: true}},
		bson.D{{Key: "_id", Value: "u2"}, {Key: "age", Value: int64(20)}, {Key: "active", Value: true}},
	}}, {Key: "$db", Value: "app"}}))
	indexResponse := serveCommand(t, server, 11, bson.D{{Key: "createIndexes", Value: "users"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "age", Value: int64(1)}}}, {Key: "name", Value: "age"}, {Key: "treedbValueType", Value: "int64"}}}}, {Key: "$db", Value: "app"}})
	if ok, _ := bson.Raw(indexResponse).Lookup("ok").DoubleOK(); ok != 1 {
		t.Fatalf("create age index response=%s", indexResponse)
	}
	update := serveCommand(t, server, 2, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int64(20)}}}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "picked", Value: true}}}}}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, update)
	if n, _ := bson.Raw(update).Lookup("n").Int32OK(); n != 1 {
		t.Fatalf("update n=%d want 1", n)
	}
	for _, want := range []struct {
		id     string
		picked bool
	}{{"u1", true}, {"u2", false}} {
		found := serveCommand(t, server, 20, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: want.id}}}, {Key: "$db", Value: "app"}})
		doc := bson.Raw(found).Lookup("cursor").Document().Lookup("firstBatch").Array().Index(0).Document()
		picked, ok := doc.Lookup("picked").BooleanOK()
		if ok != want.picked || (ok && picked != want.picked) {
			t.Fatalf("%s picked=%v ok=%v want %v", want.id, picked, ok, want.picked)
		}
	}
	modify := serveCommand(t, server, 3, bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "active", Value: true}}}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "modified", Value: true}}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, modify)
	if id, _ := bson.Raw(modify).Lookup("value").Document().Lookup("_id").StringValueOK(); id != "u1" {
		t.Fatalf("findAndModify id=%q want u1", id)
	}
	deleted := serveCommand(t, server, 4, bson.D{{Key: "delete", Value: "users"}, {Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int64(20)}}}}}, {Key: "limit", Value: int32(1)}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, deleted)
	if n, _ := bson.Raw(deleted).Lookup("n").Int32OK(); n != 1 {
		t.Fatalf("delete n=%d want 1", n)
	}
	limitZero := serveCommand(t, server, 5, bson.D{{Key: "delete", Value: "users"}, {Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "age", Value: int64(20)}}}, {Key: "limit", Value: int32(0)}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, limitZero)
	remaining := serveCommand(t, server, 6, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "$db", Value: "app"}})
	assertOK(t, remaining)
	if values, err := bson.Raw(remaining).Lookup("cursor").Document().Lookup("firstBatch").Array().Values(); err != nil || len(values) != 0 {
		t.Fatalf("limit:0 did not delete matching document: values=%v err=%v", values, err)
	}
}

func TestMongoFilterWriteDeadlineRecheckedAfterSelection(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "active", Value: true}}}}, {Key: "$db", Value: "app"}}))
	col, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		run  func(*mongoWriteBudget) error
	}{
		{
			name: "update",
			run: func(budget *mongoWriteBudget) error {
				item, parseErr := parseMongoUpdateItem(0, mustDocument(t, bson.D{{Key: "q", Value: bson.D{{Key: "active", Value: true}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: true}}}}}}))
				if parseErr != nil {
					return parseErr
				}
				item.budget = budget
				_, _, runErr := server.runMongoFilterUpdateOne(col, item)
				return runErr
			},
		},
		{
			name: "delete",
			run: func(budget *mongoWriteBudget) error {
				item, parseErr := parseMongoDeleteItem(0, mustDocument(t, bson.D{{Key: "q", Value: bson.D{{Key: "active", Value: true}}}, {Key: "limit", Value: int32(1)}}))
				if parseErr != nil {
					return parseErr
				}
				_, runErr := server.deleteMongoFilterOneWithBudget(col, item.plan, budget)
				return runErr
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := newMongoWriteBudget(8)
			server.filterWriteSelectedHook = func() { budget.deadline = time.Now().Add(-time.Second) }
			if runErr := tc.run(budget); runErr == nil || !strings.Contains(runErr.Error(), "execution-time budget") {
				t.Fatalf("post-selection deadline error=%v", runErr)
			}
			server.filterWriteSelectedHook = nil
			find := serveCommand(t, server, 2, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}})
			batch := cursorFirstBatch(t, find)
			if len(batch) != 1 || !batch[0].Lookup("seen").IsZero() {
				t.Fatalf("deadline after selection mutated state: %s", find)
			}
		})
	}
}

func TestMongoFilterWriteDeadlineRecheckedAfterMaterializer(t *testing.T) {
	for _, format := range []collections.DocumentFormat{collections.DocumentFormatTemplateV1} {
		t.Run("template-v1", func(t *testing.T) {
			db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			server := NewServer()
			server.Collections = collections.NewCollectionManager(db)
			server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: format}
			assertOK(t, serveCommand(t, server, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "active", Value: true}}}}, {Key: "$db", Value: "app"}}))
			col, err := server.Collections.OpenCollection("app.users")
			if err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				name string
				run  func(*mongoWriteBudget) error
			}{
				{"update", func(budget *mongoWriteBudget) error {
					item, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{{Key: "q", Value: bson.D{{Key: "active", Value: true}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: true}}}}}}))
					if err != nil {
						return err
					}
					item.budget = budget
					_, _, err = server.runMongoFilterUpdateOne(col, item)
					return err
				}},
				{"delete", func(budget *mongoWriteBudget) error {
					item, err := parseMongoDeleteItem(0, mustDocument(t, bson.D{{Key: "q", Value: bson.D{{Key: "active", Value: true}}}, {Key: "limit", Value: int32(1)}}))
					if err != nil {
						return err
					}
					_, err = server.deleteMongoFilterOneWithBudget(col, item.plan, budget)
					return err
				}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					budget := newMongoWriteBudget(8)
					server.filterWriteAfterMaterializerHook = func() { budget.deadline = time.Now().Add(-time.Second) }
					err := tc.run(budget)
					server.filterWriteAfterMaterializerHook = nil
					if err == nil || !strings.Contains(err.Error(), "execution-time budget") {
						t.Fatalf("after-materializer deadline error=%v", err)
					}
					key, err := encodePrimaryKey(bson.RawValue{Type: bson.TypeString, Value: bsoncore.AppendString(nil, "u1")})
					if err != nil {
						t.Fatal(err)
					}
					stored, err := col.Get(key)
					if err != nil || stored == nil {
						t.Fatalf("post-materializer deadline changed state stored=%v err=%v", stored, err)
					}
					materializer, err := storedDocumentMaterializerForCollection(col)
					if err != nil {
						t.Fatal(err)
					}
					raw, err := storedDocumentToBSON(col, materializer, stored)
					_ = materializer.Close()
					if err != nil {
						t.Fatal(err)
					}
					if !bson.Raw(raw).Lookup("seen").IsZero() {
						t.Fatalf("post-materializer deadline updated document: %s", raw)
					}
				})
			}
		})
	}
}

func TestMongoFilterUpdateRechecksPredicateAfterDeterministicDrift(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "active", Value: true}}, bson.D{{Key: "_id", Value: "u2"}, {Key: "active", Value: true}}}}, {Key: "$db", Value: "app"}}))
	selected, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	server.filterWriteSelectedHook = func() {
		if calls.Add(1) == 1 {
			close(selected)
			<-release
		}
	}
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	result := make(chan commandResult, 1)
	go func() {
		doc, err := serveCommandResult(server, 2, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "active", Value: true}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "picked", Value: true}}}}}}}}, {Key: "$db", Value: "app"}})
		result <- commandResult{doc: doc, err: err}
	}()
	awaitMongoFilterWriteSignal(t, selected, "filter write selection")
	assertOK(t, serveCommand(t, server, 3, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "active", Value: false}}}}}}}}, {Key: "$db", Value: "app"}}))
	close(release)
	released = true
	var resultValue commandResult
	select {
	case resultValue = <-result:
	case <-time.After(mongoFilterWriteTestTimeout):
		t.Fatal("timed out waiting for predicate-drift update result")
	}
	if resultValue.err != nil {
		t.Fatal(resultValue.err)
	}
	assertOK(t, resultValue.doc)
	got := serveCommand(t, server, 4, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "$db", Value: "app"}})
	if picked, ok := bson.Raw(got).Lookup("cursor").Document().Lookup("firstBatch").Array().Index(0).Document().Lookup("picked").BooleanOK(); !ok || !picked {
		t.Fatalf("drift retry did not update u2: %s", got)
	}
	u1 := serveCommand(t, server, 5, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}})
	if _, ok := bson.Raw(u1).Lookup("cursor").Document().Lookup("firstBatch").Array().Index(0).Document().Lookup("picked").BooleanOK(); ok {
		t.Fatalf("predicate drift updated u1: %s", u1)
	}
}

func TestMongoFilterWritesScanCapFailsWithoutMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	server.MaxFindScanDocuments = 1
	assertOK(t, serveCommand(t, server, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "active", Value: false}}, bson.D{{Key: "_id", Value: "u2"}, {Key: "active", Value: true}}}}, {Key: "$db", Value: "app"}}))
	for _, command := range []bson.D{
		{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "active", Value: true}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "picked", Value: true}}}}}}}}, {Key: "$db", Value: "app"}},
		{{Key: "delete", Value: "users"}, {Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "active", Value: true}}}, {Key: "limit", Value: int32(1)}}}}, {Key: "$db", Value: "app"}},
	} {
		response := serveCommand(t, server, 2, command)
		assertIndexedWriteError(t, response, 0)
		message, _ := bson.Raw(response).Lookup("writeErrors").Array().Index(0).Document().Lookup("errmsg").StringValueOK()
		if !strings.Contains(message, "bounded scan") {
			t.Fatalf("scan cap response=%s", response)
		}
	}
	remaining := serveCommand(t, server, 3, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "$db", Value: "app"}})
	if values, err := bson.Raw(remaining).Lookup("cursor").Document().Lookup("firstBatch").Array().Values(); err != nil || len(values) != 1 {
		t.Fatalf("scan cap mutated/deleted matching doc: %v %v", values, err)
	}
}

func TestMongoFilterUpdateSupportedLogicalFilters(t *testing.T) {
	for _, tc := range []struct {
		name   string
		filter bson.D
	}{
		{name: "in", filter: bson.D{{Key: "age", Value: bson.D{{Key: "$in", Value: bson.A{int32(20)}}}}}},
		{name: "and", filter: bson.D{{Key: "$and", Value: bson.A{bson.D{{Key: "age", Value: int32(20)}}, bson.D{{Key: "active", Value: true}}}}}},
		{name: "or", filter: bson.D{{Key: "$or", Value: bson.A{bson.D{{Key: "age", Value: int32(20)}}, bson.D{{Key: "age", Value: int32(99)}}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			server := NewServer()
			server.Collections = collections.NewCollectionManager(db)
			server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
			assertOK(t, serveCommand(t, server, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "age", Value: int32(20)}, {Key: "active", Value: true}}}}, {Key: "$db", Value: "app"}}))
			response := serveCommand(t, server, 2, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: tc.filter}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "picked", Value: true}}}}}}}}, {Key: "$db", Value: "app"}})
			assertOK(t, response)
			assertInt32(t, response, "n", 1)
			assertInt32(t, response, "nModified", 1)
		})
	}
}

func TestMongoFilterWritesNoMatchLeaveDocumentUnchanged(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "active", Value: false}}}}, {Key: "$db", Value: "app"}}))
	update := serveCommand(t, server, 2, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "active", Value: true}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "picked", Value: true}}}}}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, update)
	assertInt32(t, update, "n", 0)
	assertInt32(t, update, "nModified", 0)
	deleted := serveCommand(t, server, 3, bson.D{{Key: "delete", Value: "users"}, {Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "active", Value: true}}}, {Key: "limit", Value: int32(1)}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, deleted)
	assertInt32(t, deleted, "n", 0)
	fam := serveCommand(t, server, 4, bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "active", Value: true}}}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "picked", Value: true}}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, fam)
	if bson.Raw(fam).Lookup("value").Type != bson.TypeNull {
		t.Fatalf("findAndModify value=%s want null", fam)
	}
	lastError, ok := bson.Raw(fam).Lookup("lastErrorObject").DocumentOK()
	if !ok {
		t.Fatalf("findAndModify missing lastErrorObject: %s", fam)
	}
	if updated, ok := lastError.Lookup("updatedExisting").BooleanOK(); !ok || updated {
		t.Fatalf("findAndModify updatedExisting=%v ok=%v want false", updated, ok)
	}
	stored := serveCommand(t, server, 5, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}})
	doc := bson.Raw(stored).Lookup("cursor").Document().Lookup("firstBatch").Array().Index(0).Document()
	if _, ok := doc.Lookup("picked").BooleanOK(); ok {
		t.Fatalf("no-match writes mutated document: %s", stored)
	}
}

func TestMongoFilterUpsertRejectsBeforeMissingCollectionCreation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	for _, command := range []bson.D{
		{{Key: "update", Value: "missing_update"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "active", Value: true}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "picked", Value: true}}}}}, {Key: "upsert", Value: true}}}}, {Key: "$db", Value: "app"}},
		{{Key: "findAndModify", Value: "missing_fam"}, {Key: "query", Value: bson.D{{Key: "active", Value: true}}}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "picked", Value: true}}}}}, {Key: "upsert", Value: true}, {Key: "$db", Value: "app"}},
	} {
		response := serveCommand(t, server, 1, command)
		assertCommandError(t, response, "BadValue")
	}
	for _, name := range []string{"app.missing_update", "app.missing_fam"} {
		if _, err := server.Collections.OpenCollection(name); err == nil {
			t.Fatalf("non-exact upsert created %s", name)
		}
	}
}

func TestMongoFilterUpdateManyRechecksPredicateAfterDeterministicDrift(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "active", Value: true}}, bson.D{{Key: "_id", Value: "u2"}, {Key: "active", Value: true}}}}, {Key: "$db", Value: "app"}}))
	selected, release := make(chan struct{}), make(chan struct{})
	server.filterWriteSelectedHook = func() { close(selected); <-release }
	result := make(chan commandResult, 1)
	go func() {
		doc, commandErr := serveCommandResult(server, 2, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "active", Value: true}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "picked", Value: true}}}}}, {Key: "multi", Value: true}}}}, {Key: "$db", Value: "app"}})
		result <- commandResult{doc: doc, err: commandErr}
	}()
	awaitMongoFilterWriteSignal(t, selected, "multi filter write selection")
	assertOK(t, serveCommand(t, server, 3, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "active", Value: false}}}}}}}}, {Key: "$db", Value: "app"}}))
	close(release)
	resultValue := <-result
	if resultValue.err != nil {
		t.Fatal(resultValue.err)
	}
	assertOK(t, resultValue.doc)
	assertInt32(t, resultValue.doc, "n", 1)
	find := serveCommand(t, server, 4, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "$db", Value: "app"}})
	if !bson.Raw(find).Lookup("cursor").Document().Lookup("firstBatch").Array().Index(0).Document().Lookup("picked").IsZero() {
		t.Fatalf("predicate-drift multi update mutated later u2: %s", find)
	}
}
