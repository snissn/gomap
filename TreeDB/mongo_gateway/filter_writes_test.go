package mongogateway

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

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
	assertCommandError(t, limitZero, "BadValue")
	remaining := serveCommand(t, server, 6, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "$db", Value: "app"}})
	assertOK(t, remaining)
	if values, err := bson.Raw(remaining).Lookup("cursor").Document().Lookup("firstBatch").Array().Values(); err != nil || len(values) != 1 {
		t.Fatalf("limit:0 mutated document: values=%v err=%v", values, err)
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
	<-selected
	assertOK(t, serveCommand(t, server, 3, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "active", Value: false}}}}}}}}, {Key: "$db", Value: "app"}}))
	close(release)
	released = true
	resultValue := <-result
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
		assertCommandError(t, response, "BadValue")
		if message, _ := bson.Raw(response).Lookup("errmsg").StringValueOK(); !strings.Contains(message, "bounded scan") {
			t.Fatalf("scan cap response=%s", response)
		}
	}
	remaining := serveCommand(t, server, 3, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "$db", Value: "app"}})
	if values, err := bson.Raw(remaining).Lookup("cursor").Document().Lookup("firstBatch").Array().Values(); err != nil || len(values) != 1 {
		t.Fatalf("scan cap mutated/deleted matching doc: %v %v", values, err)
	}
}

func TestMongoFilterUpdateSupportedLogicalFilters(t *testing.T) {
	for _, filter := range []bson.D{
		{{Key: "age", Value: bson.D{{Key: "$in", Value: bson.A{int32(20)}}}}},
		{{Key: "$and", Value: bson.A{bson.D{{Key: "age", Value: int32(20)}}, bson.D{{Key: "active", Value: true}}}}},
		{{Key: "$or", Value: bson.A{bson.D{{Key: "age", Value: int32(20)}}, bson.D{{Key: "age", Value: int32(99)}}}}},
	} {
		db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		server := NewServer()
		server.Collections = collections.NewCollectionManager(db)
		server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
		assertOK(t, serveCommand(t, server, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "age", Value: int32(20)}, {Key: "active", Value: true}}}}, {Key: "$db", Value: "app"}}))
		response := serveCommand(t, server, 2, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: filter}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "picked", Value: true}}}}}}}}, {Key: "$db", Value: "app"}})
		assertOK(t, response)
		assertInt32(t, response, "n", 1)
		assertInt32(t, response, "nModified", 1)
		_ = db.Close()
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
