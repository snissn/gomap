package mongogateway

import (
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
			response := serveCommand(t, server, int32(101+i), bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "update", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(1)}}}}}, {Key: "new", Value: true}, {Key: "$db", Value: "app"}})
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
	upsert := serveCommand(t, server, 5, bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "update", Value: bson.D{{Key: "name", Value: "new"}}}, {Key: "upsert", Value: true}, {Key: "new", Value: true}, {Key: "$db", Value: "app"}})
	assertOK(t, upsert)
	if id, ok := bson.Raw(upsert).Lookup("lastErrorObject").Document().Lookup("upserted").StringValueOK(); !ok || id != "u2" {
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
	for i, query := range []bson.D{{{Key: "name", Value: "x"}}, {{Key: "_id", Value: bson.D{{Key: "$in", Value: bson.A{"u1"}}}}}} {
		command := bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: query}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "x"}}}}}, {Key: "$db", Value: "app"}}
		assertCommandError(t, serveCommand(t, server, int32(30+i), command), "BadValue")
	}
}

func TestFindAndModifyClusterFailsClosed(t *testing.T) {
	server := NewServer()
	server.ClusterSubmitter = &mongoClusterFakeSubmitter{}
	response := serveCommand(t, server, 50, bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "x"}}}}}, {Key: "$db", Value: "app"}})
	assertCommandError(t, response, "BadValue")
}
