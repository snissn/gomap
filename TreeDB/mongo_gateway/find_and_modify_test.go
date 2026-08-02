package mongogateway

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestFindAndModifyReturnsAtomicBeforeAndAfterImages(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "n", Value: int32(1)}, {Key: "name", Value: "ada"}}}}, {Key: "$db", Value: "app"}}))
	before := serveCommand(t, server, 2, bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "update", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(1)}}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, before)
	if n, ok := bson.Raw(before).Lookup("value").Document().Lookup("n").Int32OK(); !ok || n != 1 {
		t.Fatalf("before n=%d ok=%v want 1", n, ok)
	}
	last := bson.Raw(before).Lookup("lastErrorObject").Document()
	if n, ok := last.Lookup("n").Int32OK(); !ok || n != 1 {
		t.Fatalf("matched n=%d ok=%v", n, ok)
	}
	if updated, ok := last.Lookup("updatedExisting").BooleanOK(); !ok || !updated {
		t.Fatalf("matched updatedExisting=%v ok=%v", updated, ok)
	}
	if !last.Lookup("upserted").IsZero() {
		t.Fatalf("matched response unexpectedly has upserted")
	}
	after := serveCommand(t, server, 3, bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "grace"}}}}}, {Key: "new", Value: true}, {Key: "fields", Value: bson.D{{Key: "name", Value: int32(1)}}}, {Key: "$db", Value: "app"}})
	assertOK(t, after)
	value := bson.Raw(after).Lookup("value").Document()
	if name, ok := value.Lookup("name").StringValueOK(); !ok || name != "grace" {
		t.Fatalf("after name=%q ok=%v", name, ok)
	}
	if !value.Lookup("n").IsZero() {
		t.Fatalf("projection retained n")
	}
}

func TestFindAndModifyConcurrentNewImagesAreCommitted(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 100, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "n", Value: int32(0)}}}}, {Key: "$db", Value: "app"}}))
	const workers = 16
	values := make(chan int32, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			response, err := serveCommandResult(server, int32(101+i), bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "update", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(1)}}}}}, {Key: "new", Value: true}, {Key: "$db", Value: "app"}})
			if err != nil {
				errs <- err
				return
			}
			if ok, _ := bson.Raw(response).Lookup("ok").DoubleOK(); ok != 1 {
				errs <- fmt.Errorf("response=%v", response)
				return
			}
			n, ok := bson.Raw(response).Lookup("value").Document().Lookup("n").Int32OK()
			if !ok {
				errs <- fmt.Errorf("response=%v", response)
				return
			}
			values <- n
		}(i)
	}
	wg.Wait()
	close(values)
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	seen := make(map[int32]bool, workers)
	for n := range values {
		if seen[n] {
			t.Fatalf("duplicate returned committed image %d", n)
		}
		seen[n] = true
	}
	for n := int32(1); n <= workers; n++ {
		if !seen[n] {
			t.Fatalf("missing committed image %d", n)
		}
	}
	collection, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	key, _, err := prepareInsertDocument(mustDocument(t, bson.D{{Key: "_id", Value: "u1"}}), collections.DocumentFormatBSON)
	if err != nil {
		t.Fatalf("prepare key: %v", err)
	}
	stored, err := collection.Get(key)
	if err != nil {
		t.Fatalf("get final document: %v", err)
	}
	if n, ok := bson.Raw(stored).Lookup("n").Int32OK(); !ok || n != workers {
		t.Fatalf("final n=%d ok=%v want %d", n, ok, workers)
	}
}

func TestFindAndModifyFinalMissClearsStaleCallbackImages(t *testing.T) {
	before := mustDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "n", Value: int32(1)}})
	after := mustDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "n", Value: int32(2)}})
	gotBefore, gotAfter, matched, err := finalizeFindAndModifyImages(before, after, false, nil)
	if err != nil || matched || gotBefore != nil || gotAfter != nil {
		t.Fatalf("finalized images before=%v after=%v matched=%v err=%v want nil/nil/false/nil", gotBefore, gotAfter, matched, err)
	}
}

func TestFindAndModifyConcurrentSameIDUpsertsApplyEveryMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	if _, err := server.Collections.CreateCollection(&collections.CollectionMeta{Name: "app.users", Options: server.DefaultCollectionOptions}); err != nil {
		t.Fatal(err)
	}
	const workers = 16
	errs := make(chan error, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			response, err := serveCommandResult(server, int32(200+i), bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "upsert-race"}}}, {Key: "update", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(1)}}}}}, {Key: "upsert", Value: true}, {Key: "new", Value: true}, {Key: "$db", Value: "app"}})
			if err != nil {
				errs <- err
				return
			}
			if ok, _ := bson.Raw(response).Lookup("ok").DoubleOK(); ok != 1 {
				errs <- fmt.Errorf("response=%v", response)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	collection, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatal(err)
	}
	key, _, err := prepareInsertDocument(mustDocument(t, bson.D{{Key: "_id", Value: "upsert-race"}}), collections.DocumentFormatBSON)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := collection.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := bson.Raw(stored).Lookup("n").Int32OK(); !ok || n != workers {
		t.Fatalf("final n=%d ok=%v want %d", n, ok, workers)
	}
}

func TestFindAndModifyInsertConflictAppliesToExistingDocument(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 250, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "upsert-race"}, {Key: "n", Value: int32(1)}}}}, {Key: "$db", Value: "app"}}))
	collection, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatal(err)
	}
	item, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "upsert-race"}}}, {Key: "u", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(1)}}}}}, {Key: "upsert", Value: true}}))
	if err != nil {
		t.Fatal(err)
	}
	response, err := findAndModifyAfterInsertConflict(collection, item, false, compiledProjection{})
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, response)
	if n, ok := bson.Raw(response).Lookup("value").Document().Lookup("n").Int32OK(); !ok || n != 1 {
		t.Fatalf("pre-image n=%d ok=%v want 1", n, ok)
	}
	last := bson.Raw(response).Lookup("lastErrorObject").Document()
	if n, ok := last.Lookup("n").Int32OK(); !ok || n != 1 {
		t.Fatalf("matched n=%d ok=%v want 1", n, ok)
	}
	if updated, ok := last.Lookup("updatedExisting").BooleanOK(); !ok || !updated {
		t.Fatalf("updatedExisting=%v ok=%v want true", updated, ok)
	}
	if !last.Lookup("upserted").IsZero() {
		t.Fatalf("conflict retry unexpectedly reported upserted")
	}
	key, _, err := prepareInsertDocument(mustDocument(t, bson.D{{Key: "_id", Value: "upsert-race"}}), collections.DocumentFormatBSON)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := collection.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := bson.Raw(stored).Lookup("n").Int32OK(); !ok || n != 2 {
		t.Fatalf("stored n=%d ok=%v want 2", n, ok)
	}
}

func TestFindAndModifyNoMatchAndUpsert(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	miss := serveCommand(t, server, 4, bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "none"}}}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: int32(1)}}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, miss)
	if bson.Raw(miss).Lookup("value").Type != bson.TypeNull {
		t.Fatalf("no-match value=%v want null", bson.Raw(miss).Lookup("value").Type)
	}
	last := bson.Raw(miss).Lookup("lastErrorObject").Document()
	if n, ok := last.Lookup("n").Int32OK(); !ok || n != 0 {
		t.Fatalf("no-match n=%d ok=%v", n, ok)
	}
	if updated, ok := last.Lookup("updatedExisting").BooleanOK(); !ok || updated {
		t.Fatalf("no-match updatedExisting=%v ok=%v", updated, ok)
	}
	if !last.Lookup("upserted").IsZero() {
		t.Fatalf("no-match response unexpectedly has upserted")
	}
	upsert := serveCommand(t, server, 5, bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "update", Value: bson.D{{Key: "name", Value: "new"}}}, {Key: "upsert", Value: true}, {Key: "new", Value: true}, {Key: "$db", Value: "app"}})
	assertOK(t, upsert)
	last = bson.Raw(upsert).Lookup("lastErrorObject").Document()
	if n, ok := last.Lookup("n").Int32OK(); !ok || n != 1 {
		t.Fatalf("upsert n=%d ok=%v", n, ok)
	}
	if updated, ok := last.Lookup("updatedExisting").BooleanOK(); !ok || updated {
		t.Fatalf("upsert updatedExisting=%v ok=%v", updated, ok)
	}
	if id, ok := last.Lookup("upserted").StringValueOK(); !ok || id != "u2" {
		t.Fatalf("upserted=%q ok=%v", id, ok)
	}
	if name, ok := bson.Raw(upsert).Lookup("value").Document().Lookup("name").StringValueOK(); !ok || name != "new" {
		t.Fatalf("upsert value name=%q ok=%v", name, ok)
	}
}

func TestFindAndModifyRejectsUnsupportedModes(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	base := bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "x"}}}}}, {Key: "$db", Value: "app"}}
	for i, extra := range []bson.E{
		{Key: "remove", Value: true}, {Key: "sort", Value: bson.D{{Key: "name", Value: int32(1)}}}, {Key: "arrayFilters", Value: bson.A{}}, {Key: "hint", Value: "name_1"}, {Key: "collation", Value: bson.D{{Key: "locale", Value: "en"}}}, {Key: "fields", Value: bson.D{{Key: "name.first", Value: int32(1)}}}, {Key: "txnNumber", Value: int64(1)},
	} {
		command := append(append(bson.D(nil), base...), extra)
		assertCommandError(t, serveCommand(t, server, int32(10+i), command), "BadValue")
	}
	for i, query := range []bson.D{{{Key: "$expr", Value: bson.D{}}}, {{Key: "_id", Value: bson.Regex{Pattern: "^u"}}}} {
		command := bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: query}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "x"}}}}}, {Key: "$db", Value: "app"}}
		assertCommandError(t, serveCommand(t, server, int32(30+i), command), "BadValue")
	}
}

func TestFindAndModifyRejectsRegexIDQueryBeforeUpsert(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	regex := bson.Regex{Pattern: "^u", Options: ""}
	for i, upsert := range []bool{false, true} {
		response := serveCommand(t, server, int32(40+i), bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: regex}}}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "x"}}}}}, {Key: "upsert", Value: upsert}, {Key: "$db", Value: "app"}})
		assertCommandError(t, response, "BadValue")
	}
	if _, err := server.Collections.OpenCollection("app.users"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("regex query created collection: %v", err)
	}
}

func TestFindAndModifyAcceptsMetadataAndRejectsWriteConcernBeforeMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 60, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "n", Value: int32(1)}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 61, bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "ada"}}}}}, {Key: "$clusterTime", Value: bson.D{}}, {Key: "readConcern", Value: bson.D{{Key: "level", Value: "local"}}}, {Key: "$db", Value: "app"}}))
	response := serveCommand(t, server, 62, bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "update", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(1)}}}}}, {Key: "writeConcern", Value: bson.D{{Key: "j", Value: true}}}, {Key: "$db", Value: "app"}})
	assertCommandError(t, response, "WriteConcernFailed")
	collection, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatal(err)
	}
	key, _, err := prepareInsertDocument(mustDocument(t, bson.D{{Key: "_id", Value: "u1"}}), collections.DocumentFormatBSON)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := collection.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := bson.Raw(stored).Lookup("n").Int32OK(); !ok || n != 1 {
		t.Fatalf("writeConcern failure mutated n=%d ok=%v want 1", n, ok)
	}
}

func TestFindAndModifyRejectsUnsupportedReadConcernBeforeMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 70, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "n", Value: int32(1)}}}}, {Key: "$db", Value: "app"}}))
	for i, test := range []struct {
		concern any
		code    string
	}{{bson.D{{Key: "level", Value: "majority"}}, "BadValue"}, {"local", "FailedToParse"}} {
		response := serveCommand(t, server, int32(71+i), bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "update", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(1)}}}}}, {Key: "readConcern", Value: test.concern}, {Key: "$db", Value: "app"}})
		assertCommandError(t, response, test.code)
	}
	collection, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatal(err)
	}
	key, _, err := prepareInsertDocument(mustDocument(t, bson.D{{Key: "_id", Value: "u1"}}), collections.DocumentFormatBSON)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := collection.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := bson.Raw(stored).Lookup("n").Int32OK(); !ok || n != 1 {
		t.Fatalf("readConcern failure mutated n=%d ok=%v want 1", n, ok)
	}
}

func TestFindAndModifyClusterFailsClosed(t *testing.T) {
	server := NewServer()
	server.ClusterSubmitter = &mongoClusterFakeSubmitter{}
	response := serveCommand(t, server, 50, bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "x"}}}}}, {Key: "$db", Value: "app"}})
	assertCommandError(t, response, "BadValue")
}
