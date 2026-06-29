package nativewire

import (
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestInsertBatchCombinerBatchesConcurrentBSONNoResultRequests(t *testing.T) {
	server, col, cleanup := newInsertBatchCombinerTestServer(t)
	defer cleanup()

	const requests = 16
	errs := runConcurrentInsertBatchCombinerRequests(t, server, col, requests, func(i int) (string, []byte) {
		id := "user-" + strconv.Itoa(i)
		return id, insertBatchCombinerBSONDoc(t, id, "user-"+strconv.Itoa(i)+"@example.com")
	})
	for i, err := range errs {
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
	}
	for i := 0; i < requests; i++ {
		id := "user-" + strconv.Itoa(i)
		if _, err := col.Get([]byte(id)); err != nil {
			t.Fatalf("Get(%q): %v", id, err)
		}
	}
	if got := nativewireTestStatUint64(t, server, "insert_batch_combiner.requests_total"); got < 2 {
		t.Fatalf("combined requests=%d want at least 2", got)
	}
}

func TestInsertBatchCombinerFallbackPreservesDuplicateRequestOutcome(t *testing.T) {
	server, col, cleanup := newInsertBatchCombinerTestServer(t)
	defer cleanup()

	errs := applyInsertBatchCombinerRequestsDirect(t, server, col, 2, func(i int) (string, []byte) {
		return "same-id", insertBatchCombinerBSONDoc(t, "same-id", "dupe-"+strconv.Itoa(i)+"@example.com")
	})

	successes := 0
	conflicts := 0
	for _, err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, collections.ErrDocumentExists) || errors.Is(err, collections.ErrDuplicateDocumentID):
			conflicts++
		default:
			t.Fatalf("unexpected duplicate outcome err=%v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d want 1/1; errs=%v", successes, conflicts, errs)
	}
	if _, err := col.Get([]byte("same-id")); err != nil {
		t.Fatalf("Get(same-id): %v", err)
	}
	if got := nativewireTestStatUint64(t, server, "insert_batch_combiner.fallback_requests_total"); got == 0 {
		t.Fatalf("fallback requests=%d want nonzero", got)
	}
}

func TestInsertBatchCombinerFallbackPreservesUniqueConflictOutcome(t *testing.T) {
	server, col, cleanup := newInsertBatchCombinerTestServer(t)
	defer cleanup()

	errs := applyInsertBatchCombinerRequestsDirect(t, server, col, 2, func(i int) (string, []byte) {
		id := "user-" + strconv.Itoa(i)
		return id, insertBatchCombinerBSONDoc(t, id, "same@example.com")
	})

	successes := 0
	conflicts := 0
	for _, err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, collections.ErrUniqueIndexConflict):
			conflicts++
		default:
			t.Fatalf("unexpected unique conflict outcome err=%v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d want 1/1; errs=%v", successes, conflicts, errs)
	}
	if got := nativewireTestStatUint64(t, server, "insert_batch_combiner.fallback_requests_total"); got == 0 {
		t.Fatalf("fallback requests=%d want nonzero", got)
	}
}

func TestInsertBatchCombinerSkipsIncompatibleRequests(t *testing.T) {
	server, col, cleanup := newInsertBatchCombinerTestServer(t)
	defer cleanup()

	req := insertBatchCombinerFastRequest(t, col, "user-1", insertBatchCombinerBSONDoc(t, "user-1", "user-1@example.com"))
	if !canCombineInsertBatchFastRequest(server, req) {
		t.Fatal("baseline BSON batch-1 no-result request was not combinable")
	}

	req.includeResultIDs = true
	if canCombineInsertBatchFastRequest(server, req) {
		t.Fatal("request with result IDs should not combine")
	}
	req.includeResultIDs = false

	req.ack = AckFlushed
	if canCombineInsertBatchFastRequest(server, req) {
		t.Fatal("flushed ack request should not combine")
	}
	req.ack = AckVisible

	req.format = collections.DocumentFormatJSON
	if canCombineInsertBatchFastRequest(server, req) {
		t.Fatal("JSON request should not combine")
	}
}

func TestInsertBatchCombinerRejectsClusterModeBypass(t *testing.T) {
	server, col, cleanup := newInsertBatchCombinerTestServer(t)
	defer cleanup()
	server.clusterSubmitter = &admissionClusterSubmitter{
		fakeClusterSubmitter: &fakeClusterSubmitter{},
		status:               ClusterLeaderAdmission(),
	}

	req := insertBatchCombinerFastRequest(t, col, "user-cluster", insertBatchCombinerBSONDoc(t, "user-cluster", "cluster@example.com"))
	if canCombineInsertBatchFastRequest(server, req) {
		t.Fatal("cluster-owned insert request should not combine locally")
	}
	item := &nativewireInsertBatchCombineItem{
		req:  req,
		done: make(chan nativewireInsertBatchCombineResult, 1),
	}
	applyInsertBatchCombineItems(server, []*nativewireInsertBatchCombineItem{item})
	select {
	case result := <-item.done:
		if nativeCodeOf(result.err) != iwire.ErrReadOnly {
			t.Fatalf("combiner result err=%v code=%d want read-only", result.err, nativeCodeOf(result.err))
		}
	default:
		t.Fatal("cluster bypass item was not completed")
	}
	if got, err := col.Get([]byte("user-cluster")); err != nil || got != nil {
		t.Fatalf("user-cluster after rejected combiner got=%v err=%v want missing", got, err)
	}
	if calls := server.clusterSubmitter.(*admissionClusterSubmitter).snapshot(); len(calls) != 0 {
		t.Fatalf("submitter calls=%d want 0", len(calls))
	}
}

func TestInsertBatchCombinerStatsStartAtZero(t *testing.T) {
	server := NewServer(ServerOptions{})
	stats := server.Stats()
	for _, key := range []string{
		"insert_batch_combiner.batches_total",
		"insert_batch_combiner.requests_total",
		"insert_batch_combiner.single_requests_total",
		"insert_batch_combiner.fallback_requests_total",
	} {
		if got := stats[nativeStatsPrefix+key]; got != "0" {
			t.Fatalf("Stats()[%q]=%q want 0", nativeStatsPrefix+key, got)
		}
	}
}

func TestInsertBatchCombinerNonPositiveOptionsUseDefaults(t *testing.T) {
	server := NewServer(ServerOptions{
		InsertBatchCombineMaxBatch:    -1,
		InsertBatchCombineDrainYields: -1,
	})
	if server.insertBatchCombineMaxBatch != defaultInsertBatchCombineMaxBatch {
		t.Fatalf("max batch=%d want default %d", server.insertBatchCombineMaxBatch, defaultInsertBatchCombineMaxBatch)
	}
	if server.insertBatchCombineDrainYields != defaultInsertBatchCombineDrainYields {
		t.Fatalf("drain yields=%d want default %d", server.insertBatchCombineDrainYields, defaultInsertBatchCombineDrainYields)
	}
}

func newInsertBatchCombinerTestServer(t *testing.T) (*Server, *collections.Collection, func()) {
	t.Helper()
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name: "users",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatBSON,
		},
		Indexes: []collections.IndexDefinition{{
			Name:      "email_1",
			Field:     "email",
			ValueType: collections.IndexValueString,
			Unique:    true,
		}},
	}); err != nil {
		_ = db.Close()
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		_ = db.Close()
		t.Fatalf("OpenCollection: %v", err)
	}
	server := NewServer(ServerOptions{
		Collections:                   mgr,
		Backend:                       db,
		InsertBatchCombineDrainYields: 4096,
	})
	return server, col, func() {
		if err := errors.Join(server.Close(), db.Close()); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
}

func runConcurrentInsertBatchCombinerRequests(t *testing.T, server *Server, col *collections.Collection, requests int, build func(int) (string, []byte)) []error {
	t.Helper()
	states := make([]*connState, requests)
	bodies := make([][]byte, requests)
	for i := 0; i < requests; i++ {
		state := benchmarkConnState()
		handle := benchmarkAddCollectionHandle(t, state, server, "users", col)
		id, doc := build(i)
		guard := benchmarkMutationGuard(t, server, "insert_batch_combiner", i)
		body, err := appendInsertBatchRequestBodyRefFlags(nil, "", handle, true, collections.DocumentFormatBSON,
			[][]byte{[]byte(id)},
			[][]byte{doc},
			AckVisible,
			iwire.CommandFlagOmitResultIDs|iwire.CommandFlagOmitResponseMeta,
			guard,
		)
		if err != nil {
			t.Fatalf("append request %d: %v", i, err)
		}
		states[i] = state
		bodies[i] = body
	}

	start := make(chan struct{})
	errs := make([]error, requests)
	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, errs[i] = benchmarkDispatchRequestError(server, states[i], &benchmarkFrameSink{}, bodies[i])
		}()
	}
	close(start)
	wg.Wait()
	return errs
}

func applyInsertBatchCombinerRequestsDirect(t *testing.T, server *Server, col *collections.Collection, requests int, build func(int) (string, []byte)) []error {
	t.Helper()
	items := make([]*nativewireInsertBatchCombineItem, requests)
	for i := 0; i < requests; i++ {
		id, doc := build(i)
		items[i] = &nativewireInsertBatchCombineItem{
			req:  insertBatchCombinerFastRequest(t, col, id, doc),
			done: make(chan nativewireInsertBatchCombineResult, 1),
		}
	}

	applyInsertBatchCombineItems(server, items)

	errs := make([]error, requests)
	for i, item := range items {
		select {
		case result := <-item.done:
			errs[i] = result.err
		default:
			t.Fatalf("combined item %d was not completed", i)
		}
	}
	return errs
}

func insertBatchCombinerFastRequest(t *testing.T, col *collections.Collection, id string, doc []byte) insertBatchFastRequest {
	t.Helper()
	return insertBatchFastRequest{
		collectionName: "users",
		collection:     col,
		format:         collections.DocumentFormatBSON,
		ids:            [][]byte{[]byte(id)},
		docs:           [][]byte{doc},
		ack:            AckVisible,
	}
}

func insertBatchCombinerBSONDoc(t *testing.T, id, email string) []byte {
	t.Helper()
	doc, err := bson.Marshal(bson.D{
		{Key: "_id", Value: id},
		{Key: "email", Value: email},
		{Key: "name", Value: id},
	})
	if err != nil {
		t.Fatalf("marshal BSON: %v", err)
	}
	return doc
}

func nativewireTestStatUint64(t *testing.T, server *Server, key string) uint64 {
	t.Helper()
	raw := server.Stats()[nativeStatsPrefix+key]
	if raw == "" {
		return 0
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse stat %s=%q: %v", key, raw, err)
	}
	return value
}
