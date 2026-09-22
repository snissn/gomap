package mongogateway

import (
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMongoFirstWriteConcurrentUpdateUpserts(t *testing.T) {
	server := newFirstWriteTestServer(t)
	const workers = 16
	responses := concurrentFirstWrites(t, workers, func(i int) (wire.Document, error) {
		response, err := serveCommandResult(server, int32(10+i), bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(1)}}}}}, {Key: "upsert", Value: true}}}}, {Key: "$db", Value: "app"}})
		return response, err
	})
	upserted := 0
	for _, response := range responses {
		if ok, _ := bson.Raw(response).Lookup("ok").DoubleOK(); ok == 1 {
			assertInt32(t, response, "n", 1)
			if !bson.Raw(response).Lookup("upserted").IsZero() {
				upserted++
				assertInt32(t, response, "nModified", 0)
			} else {
				assertInt32(t, response, "nModified", 1)
			}
			continue
		}
		t.Fatalf("concurrent first-write response: %v", response)
	}
	if len(responses) != workers {
		t.Fatalf("acknowledged responses=%d want %d", len(responses), workers)
	}
	if upserted != 1 {
		t.Fatalf("upserted responses=%d want 1", upserted)
	}
	assertFirstWriteDocument(t, server, "u1", workers)
}

func TestMongoFirstWriteConcurrentFindAndModifyUpserts(t *testing.T) {
	server := newFirstWriteTestServer(t)
	runConcurrentFirstWriteFindAndModifyUpserts(t, server)
	assertFirstWriteDocument(t, server, "u1", 16)
}

func TestMongoFirstWriteConcurrentFindAndModifyUpsertsReopen(t *testing.T) {
	dir := t.TempDir()
	opts := backenddb.Options{Dir: dir, ValueLog: backenddb.ValueLogOptions{PointerThreshold: 1}}
	db, err := backenddb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	server := newFirstWriteServer(db)
	runConcurrentFirstWriteFindAndModifyUpserts(t, server)
	assertFirstWriteDocument(t, server, "u1", 16)
	if err := server.Collections.FlushAll(); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := backenddb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopenedServer := newFirstWriteServer(reopened)
	assertFirstWriteDocument(t, reopenedServer, "u1", 16)
}

func TestMongoFirstWriteLateExistingMutationsWait(t *testing.T) {
	server := newFirstWriteTestServer(t)
	created := make(chan struct{})
	continueFirstWrite := make(chan struct{})
	server.firstWriteAfterCreateHook = func(string) {
		close(created)
		<-continueFirstWrite
	}
	creator := make(chan wire.Document, 1)
	go func() {
		response, _ := serveCommandResult(server, 300, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "creator"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int32(1)}}}}}, {Key: "upsert", Value: true}}}}, {Key: "$db", Value: "app"}})
		creator <- response
	}()
	<-created // The schema is visible, but the creator still owns the first-write gate.

	responses := make(chan wire.Document, 3)
	for requestID, command := range map[int32]bson.D{
		301: {{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "insert"}, {Key: "n", Value: int32(1)}}}}, {Key: "$db", Value: "app"}},
		302: {{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "update"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int32(1)}}}}}, {Key: "upsert", Value: true}}}}, {Key: "$db", Value: "app"}},
		303: {{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "modify"}}}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int32(1)}}}}}, {Key: "upsert", Value: true}, {Key: "new", Value: true}, {Key: "$db", Value: "app"}},
	} {
		go func(requestID int32, command bson.D) {
			response, _ := serveCommandResult(server, requestID, command)
			responses <- response
		}(requestID, command)
	}
	select {
	case response := <-responses:
		t.Fatalf("late mutation bypassed first-write gate: %v", response)
	case <-time.After(25 * time.Millisecond):
	}
	close(continueFirstWrite)
	assertOK(t, <-creator)
	for range 3 {
		assertOK(t, <-responses)
	}
	for _, id := range []string{"creator", "insert", "update", "modify"} {
		assertFirstWriteDocument(t, server, id, 1)
	}
}

func TestMongoFirstWriteUnrelatedExistingMutationsDoNotWait(t *testing.T) {
	server := newFirstWriteTestServer(t)
	assertOK(t, serveCommand(t, server, 400, bson.D{{Key: "insert", Value: "ready"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "update"}, {Key: "n", Value: int32(0)}},
		bson.D{{Key: "_id", Value: "modify"}, {Key: "n", Value: int32(0)}},
		bson.D{{Key: "_id", Value: "delete"}},
	}}, {Key: "$db", Value: "app"}}))

	created := make(chan struct{})
	continueFirstWrite := make(chan struct{})
	server.firstWriteAfterCreateHook = func(string) {
		close(created)
		<-continueFirstWrite
	}
	creator := make(chan wire.Document, 1)
	go func() {
		response, _ := serveCommandResult(server, 401, bson.D{{Key: "insert", Value: "cold"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "creator"}}}}, {Key: "$db", Value: "app"}})
		creator <- response
	}()
	<-created

	responses := make(chan wire.Document, 4)
	for requestID, command := range map[int32]bson.D{
		402: {{Key: "insert", Value: "ready"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "insert"}}}}, {Key: "$db", Value: "app"}},
		403: {{Key: "update", Value: "ready"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "update"}}}, {Key: "u", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(1)}}}}}}}}, {Key: "$db", Value: "app"}},
		404: {{Key: "findAndModify", Value: "ready"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "modify"}}}, {Key: "update", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(1)}}}}}, {Key: "new", Value: true}, {Key: "$db", Value: "app"}},
		405: {{Key: "delete", Value: "ready"}, {Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "delete"}}}, {Key: "limit", Value: int32(1)}}}}, {Key: "$db", Value: "app"}},
	} {
		go func(requestID int32, command bson.D) {
			response, _ := serveCommandResult(server, requestID, command)
			responses <- response
		}(requestID, command)
	}
	for range 4 {
		select {
		case response := <-responses:
			assertOK(t, response)
		case <-time.After(time.Second):
			t.Fatal("mutation on an unrelated existing collection waited for cold first write")
		}
	}
	close(continueFirstWrite)
	assertOK(t, <-creator)
}

func TestMongoFirstWriteUnrelatedColdMutationsDoNotWait(t *testing.T) {
	server := newFirstWriteTestServer(t)
	createdA := make(chan struct{})
	continueA := make(chan struct{})
	server.firstWriteAfterCreateHook = func(name string) {
		if name == "app.a" {
			close(createdA)
			<-continueA
		}
	}
	creatorA := make(chan wire.Document, 1)
	go func() {
		response, _ := serveCommandResult(server, 450, bson.D{{Key: "insert", Value: "a"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "creator"}}}}, {Key: "$db", Value: "app"}})
		creatorA <- response
	}()
	<-createdA

	creatorB := make(chan wire.Document, 1)
	go func() {
		response, _ := serveCommandResult(server, 451, bson.D{{Key: "insert", Value: "b"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "creator"}}}}, {Key: "$db", Value: "app"}})
		creatorB <- response
	}()
	select {
	case response := <-creatorB:
		assertOK(t, response)
	case <-time.After(time.Second):
		close(continueA)
		t.Fatal("first write to an unrelated cold collection waited for another namespace's mutation")
	}
	close(continueA)
	assertOK(t, <-creatorA)
}

func TestMongoFirstWriteStalePendingNamespaceDoesNotWait(t *testing.T) {
	server := newFirstWriteTestServer(t)
	createdA := make(chan struct{})
	continueA := make(chan struct{})
	server.firstWriteAfterCreateHook = func(string) {
		close(createdA)
		<-continueA
	}
	creatorA := make(chan wire.Document, 1)
	go func() {
		response, _ := serveCommandResult(server, 500, bson.D{{Key: "insert", Value: "a"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "creator"}}}}, {Key: "$db", Value: "app"}})
		creatorA <- response
	}()
	<-createdA

	observedA := make(chan struct{})
	continueHot := make(chan struct{})
	server.firstWriteBeforeWaitHook = func(pending *collectionFirstWritePending) {
		if pending.name == "app.a" {
			close(observedA)
			<-continueHot
		}
	}
	hot := make(chan wire.Document, 1)
	go func() {
		response, _ := serveCommandResult(server, 501, bson.D{{Key: "update", Value: "a"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "hot"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "ready", Value: true}}}}}, {Key: "upsert", Value: true}}}}, {Key: "$db", Value: "app"}})
		hot <- response
	}()
	<-observedA
	close(continueA)
	assertOK(t, <-creatorA)

	createdB := make(chan struct{})
	continueB := make(chan struct{})
	server.firstWriteAfterCreateHook = func(string) {
		close(createdB)
		<-continueB
	}
	creatorB := make(chan wire.Document, 1)
	go func() {
		response, _ := serveCommandResult(server, 502, bson.D{{Key: "insert", Value: "b"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "creator"}}}}, {Key: "$db", Value: "app"}})
		creatorB <- response
	}()
	<-createdB
	close(continueHot)
	select {
	case response := <-hot:
		assertOK(t, response)
	case <-time.After(time.Second):
		t.Fatal("mutation waited on a different namespace after its observed first write completed")
	}
	close(continueB)
	assertOK(t, <-creatorB)
}

func runConcurrentFirstWriteFindAndModifyUpserts(t *testing.T, server *Server) {
	t.Helper()
	const workers = 16
	responses := concurrentFirstWrites(t, workers, func(i int) (wire.Document, error) {
		response, err := serveCommandResult(server, int32(100+i), bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "update", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(1)}}}}}, {Key: "upsert", Value: true}, {Key: "new", Value: true}, {Key: "$db", Value: "app"}})
		return response, err
	})
	seen := make(map[int32]bool, workers)
	upserted := 0
	updated := 0
	for _, response := range responses {
		assertOK(t, response)
		last := bson.Raw(response).Lookup("lastErrorObject").Document()
		if n, ok := last.Lookup("n").Int32OK(); !ok || n != 1 {
			t.Fatalf("response n=%d ok=%v want 1", n, ok)
		}
		if last.Lookup("upserted").IsZero() {
			if updatedExisting, ok := last.Lookup("updatedExisting").BooleanOK(); !ok || !updatedExisting {
				t.Fatalf("matched updatedExisting=%v ok=%v", updatedExisting, ok)
			}
			updated++
		} else {
			if updatedExisting, ok := last.Lookup("updatedExisting").BooleanOK(); !ok || updatedExisting {
				t.Fatalf("upsert updatedExisting=%v ok=%v", updatedExisting, ok)
			}
			upserted++
		}
		if n, ok := bson.Raw(response).Lookup("value").Document().Lookup("n").Int32OK(); !ok || n < 1 || n > workers || seen[n] {
			t.Fatalf("returned n=%d ok=%v duplicate=%v", n, ok, seen[n])
		} else {
			seen[n] = true
		}
	}
	if len(seen) != workers {
		t.Fatalf("acknowledged images=%d want %d", len(seen), workers)
	}
	if upserted != 1 || updated != workers-1 {
		t.Fatalf("upserted/updated=%d/%d want 1/%d", upserted, updated, workers-1)
	}
}

func concurrentFirstWrites(t *testing.T, workers int, write func(int) (wire.Document, error)) []wire.Document {
	t.Helper()
	responses := make(chan wire.Document, workers)
	errs := make(chan error, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			response, err := write(i)
			if err != nil {
				errs <- err
				return
			}
			responses <- response
		}(i)
	}
	close(start)
	wg.Wait()
	close(responses)
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	var got []wire.Document
	for response := range responses {
		got = append(got, response)
	}
	if len(got) != workers {
		t.Fatalf("responses=%d want %d", len(got), workers)
	}
	return got
}

func newFirstWriteTestServer(t *testing.T) *Server {
	t.Helper()
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return newFirstWriteServer(db)
}

func newFirstWriteServer(db *backenddb.DB) *Server {
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	return server
}

func assertFirstWriteDocument(t *testing.T, server *Server, id string, want int32) {
	t.Helper()
	collection, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatal(err)
	}
	key, _, err := prepareInsertDocument(mustDocument(t, bson.D{{Key: "_id", Value: id}}), collections.DocumentFormatBSON)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := collection.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := bson.Raw(stored).Lookup("n").Int32OK(); !ok || n != want {
		t.Fatalf("stored n=%d ok=%v want %d", n, ok, want)
	}
}

func BenchmarkMongoOpenCollectionForMutation(b *testing.B) {
	db, err := backenddb.Open(backenddb.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	if _, err := server.openOrCreateCollection("app.users"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := server.openCollectionForMutation("app.users"); err != nil {
			b.Fatal(err)
		}
	}
}
