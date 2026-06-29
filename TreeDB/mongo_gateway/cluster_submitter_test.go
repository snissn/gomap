package mongogateway

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	treenativewire "github.com/snissn/gomap/TreeDB/nativewire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type mongoClusterSubmitterCall struct {
	entry    raftentry.CommandEntryV1
	metadata treenativewire.ClusterRequestMetadata
	ctxErr   error
}

type mongoClusterFakeSubmitter struct {
	mu                       sync.Mutex
	calls                    []mongoClusterSubmitterCall
	actualAck                iwire.AckPolicy
	committedRecoverable     bool
	responseSections         []iwire.Section
	overrideResponseSections bool
	status                   treenativewire.ClusterAdmissionStatus
	admissionErr             error
}

type mongoClusterAdmissionSubmitter struct {
	*mongoClusterFakeSubmitter
	status treenativewire.ClusterAdmissionStatus
	err    error
}

func (f *mongoClusterAdmissionSubmitter) ClusterAdmissionStatus(context.Context) (treenativewire.ClusterAdmissionStatus, error) {
	if f.err != nil {
		return treenativewire.ClusterAdmissionStatus{}, f.err
	}
	return f.status, nil
}

func (f *mongoClusterFakeSubmitter) ClusterAdmissionStatus(context.Context) (treenativewire.ClusterAdmissionStatus, error) {
	if f.admissionErr != nil {
		return treenativewire.ClusterAdmissionStatus{}, f.admissionErr
	}
	if f.status == (treenativewire.ClusterAdmissionStatus{}) {
		return treenativewire.ClusterLeaderAdmission(), nil
	}
	return f.status, nil
}

func (f *mongoClusterFakeSubmitter) SubmitCommandEntryV1(ctx context.Context, entry []byte, metadata treenativewire.ClusterRequestMetadata) (treenativewire.ClusterSubmitResult, error) {
	if ctx == nil {
		return treenativewire.ClusterSubmitResult{}, fmt.Errorf("nil submit context")
	}
	decoded, err := raftentry.DecodeCommandEntryV1(entry, raftentry.DecodeOptions{RequestMetadata: metadata})
	if err != nil {
		return treenativewire.ClusterSubmitResult{}, err
	}
	ctxErr := ctx.Err()
	f.mu.Lock()
	f.calls = append(f.calls, mongoClusterSubmitterCall{entry: decoded, metadata: metadata, ctxErr: ctxErr})
	f.mu.Unlock()
	if ctxErr != nil {
		return treenativewire.ClusterSubmitResult{}, ctxErr
	}
	actualAck := f.actualAck
	if actualAck == 0 {
		actualAck = iwire.AckVisible
	}
	responseSections := mongoClusterFakeResponseSections(decoded.Decoded)
	if f.overrideResponseSections {
		responseSections = append([]iwire.Section(nil), f.responseSections...)
	}
	return treenativewire.ClusterSubmitResult{
		ActualAck:            actualAck,
		CommittedRecoverable: f.committedRecoverable,
		ResponseSections:     responseSections,
	}, nil
}

func (f *mongoClusterFakeSubmitter) snapshotCalls() []mongoClusterSubmitterCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]mongoClusterSubmitterCall(nil), f.calls...)
}

func mongoClusterFakeResponseSections(entry iwire.DeterministicEntry) []iwire.Section {
	ids := mongoClusterTestIDs(entry.Sections)
	switch entry.CommandID {
	case iwire.CommandInsertBatch:
		return []iwire.Section{mongoClusterTestMeta("inserted_count", len(ids))}
	case iwire.CommandUpdateBSONSet:
		return []iwire.Section{mongoClusterTestMeta("matched_count", 1, "modified_count", 1)}
	case iwire.CommandDeleteBatch:
		return []iwire.Section{mongoClusterTestMeta("deleted_count", len(ids))}
	default:
		return []iwire.Section{mongoClusterTestMeta()}
	}
}

func mongoClusterTestMeta(counts ...any) iwire.Section {
	values := []struct {
		key   string
		value string
	}{{key: "actual_ack_policy", value: "1"}}
	for i := 0; i+1 < len(counts); i += 2 {
		values = append(values, struct {
			key   string
			value string
		}{key: counts[i].(string), value: fmt.Sprint(counts[i+1])})
	}
	payload := binary.AppendUvarint(nil, uint64(len(values)))
	for _, value := range values {
		payload = mongoClusterAppendString(payload, value.key)
		payload = mongoClusterAppendString(payload, value.value)
	}
	return iwire.Section{ID: iwire.SectionResponseMeta, Bytes: payload}
}

func mongoClusterAppendString(dst []byte, value string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func mongoClusterTestIDs(sections []iwire.Section) [][]byte {
	for _, section := range sections {
		if section.ID == iwire.SectionDocumentIDs {
			ids, err := iwire.DecodeByteVectorItems(section.Bytes, iwire.Limits{})
			if err != nil {
				return nil
			}
			return ids
		}
	}
	return nil
}

func mongoClusterStaticCatalogVersion(version uint64) ClusterCatalogVersionProvider {
	return func(context.Context) (uint64, error) {
		return version, nil
	}
}

func setMongoClusterTestSubmitter(server *Server, submitter *mongoClusterFakeSubmitter, catalogVersion uint64) {
	server.ClusterSubmitter = submitter
	server.ClusterCatalogVersion = mongoClusterStaticCatalogVersion(catalogVersion)
}

func setMongoClusterAdmissionTestSubmitter(server *Server, submitter *mongoClusterAdmissionSubmitter, catalogVersion uint64) {
	server.ClusterSubmitter = submitter
	server.ClusterCatalogVersion = mongoClusterStaticCatalogVersion(catalogVersion)
}

func assertErrmsgContains(tb testing.TB, doc wire.Document, want string) {
	tb.Helper()
	got, ok := bson.Raw(doc).Lookup("errmsg").StringValueOK()
	if !ok || !strings.Contains(got, want) {
		tb.Fatalf("errmsg=%q typeOK=%v want containing %q", got, ok, want)
	}
}

func assertMongoUsers(tb testing.TB, server *Server, want map[string]string) {
	tb.Helper()
	found := serveCommand(tb, server, 326899, bson.D{
		{Key: "find", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	batch := cursorFirstBatch(tb, found)
	got := make(map[string]string, len(batch))
	for _, doc := range batch {
		id, idOK := doc.Lookup("_id").StringValueOK()
		name, nameOK := doc.Lookup("name").StringValueOK()
		if !idOK || !nameOK {
			tb.Fatalf("user doc missing string _id/name: %v", doc)
		}
		got[id] = name
	}
	if len(got) != len(want) {
		tb.Fatalf("users=%v want %v", got, want)
	}
	for id, wantName := range want {
		if gotName, ok := got[id]; !ok || gotName != wantName {
			tb.Fatalf("user %q name=%q present=%v want %q", id, gotName, ok, wantName)
		}
	}
}

func mongoClusterCallCatalogVersion(tb testing.TB, call mongoClusterSubmitterCall) uint64 {
	tb.Helper()
	version, n := binary.Uvarint(call.entry.Target.ExpectedCatalogVersion)
	if n <= 0 || n != len(call.entry.Target.ExpectedCatalogVersion) {
		tb.Fatalf("expected_catalog_version bytes=%v decode n=%d", call.entry.Target.ExpectedCatalogVersion, n)
	}
	return version
}

func mongoClusterCallIdempotencyKey(tb testing.TB, call mongoClusterSubmitterCall) string {
	tb.Helper()
	if len(call.entry.IdempotencyKey) == 0 {
		tb.Fatal("missing idempotency key")
	}
	return string(call.entry.IdempotencyKey)
}

func TestClusterAdmissionMongoLeaderRoutesThroughSubmitter(t *testing.T) {
	submitter := &mongoClusterAdmissionSubmitter{
		mongoClusterFakeSubmitter: &mongoClusterFakeSubmitter{},
		status:                    treenativewire.ClusterLeaderAdmission(),
	}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterAdmissionTestSubmitter(server, submitter, 31)

	response := serveCommand(t, server, 326801, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	assertInt32(t, response, "n", 1)
	calls := submitter.snapshotCalls()
	if len(calls) != 2 {
		t.Fatalf("submit calls=%d want create+insert", len(calls))
	}
	if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandCreateCollection {
		t.Fatalf("first command id=%d want create_collection", got)
	}
	if got := calls[1].entry.Decoded.CommandID; got != iwire.CommandInsertBatch {
		t.Fatalf("second command id=%d want insert_batch", got)
	}
}

func TestClusterAdmissionMongoFollowerRejectsWritesBeforeLocalMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 326802, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
		{Key: "$db", Value: "app"},
	}))

	submitter := &mongoClusterAdmissionSubmitter{
		mongoClusterFakeSubmitter: &mongoClusterFakeSubmitter{},
		status:                    treenativewire.ClusterFollowerAdmission("node-a:27017", "not leader"),
	}
	setMongoClusterAdmissionTestSubmitter(server, submitter, 32)

	createResponse := serveCommand(t, server, 326803, bson.D{
		{Key: "create", Value: "admins"},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, createResponse, "BadValue")
	assertErrmsgContains(t, createResponse, "node-a:27017")
	if _, err := server.Collections.OpenCollection("app.admins"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection app.admins err=%v want collection not found", err)
	}

	insertResponse := serveCommand(t, server, 326804, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u2"}, {Key: "name", Value: "Grace"}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, insertResponse, "BadValue")
	assertMongoUsers(t, server, map[string]string{"u1": "Ada"})

	updateResponse := serveCommand(t, server, 326805, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Grace"}}}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, updateResponse, "BadValue")
	assertMongoUsers(t, server, map[string]string{"u1": "Ada"})

	deleteResponse := serveCommand(t, server, 326806, bson.D{
		{Key: "delete", Value: "users"},
		{Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(1)}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, deleteResponse, "BadValue")
	assertMongoUsers(t, server, map[string]string{"u1": "Ada"})
	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
}

func TestClusterAdmissionMongoUnavailableRejectsBeforeSubmit(t *testing.T) {
	submitter := &mongoClusterAdmissionSubmitter{
		mongoClusterFakeSubmitter: &mongoClusterFakeSubmitter{},
		status:                    treenativewire.ClusterUnavailableAdmission("cluster admission unavailable"),
	}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterAdmissionTestSubmitter(server, submitter, 33)

	response := serveCommand(t, server, 326807, bson.D{
		{Key: "create", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")
	assertErrmsgContains(t, response, "cluster admission unavailable")
	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
}

func mongoClusterCallCollectionMetaName(tb testing.TB, call mongoClusterSubmitterCall) string {
	tb.Helper()
	raw := call.entry.Target.CollectionMeta
	version, off, err := clusterReadUvarint(raw)
	if err != nil {
		tb.Fatalf("collection_meta version: %v", err)
	}
	if version != clusterCollectionMetaV5 {
		tb.Fatalf("collection_meta version=%d want %d", version, clusterCollectionMetaV5)
	}
	name, err := clusterReadString(raw, &off)
	if err != nil {
		tb.Fatalf("collection_meta name: %v", err)
	}
	return name
}

func TestClusterSubmitterInsertRoutesCommandEntryNoLocalMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 7)

	response := serveCommand(t, server, 325801, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	assertInt32(t, response, "n", 1)

	calls := submitter.snapshotCalls()
	if len(calls) != 2 {
		t.Fatalf("submit calls=%d want 2", len(calls))
	}
	if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandCreateCollection {
		t.Fatalf("first command id=%d want create_collection", got)
	}
	if got := mongoClusterCallCollectionMetaName(t, calls[0]); got != "app.users" {
		t.Fatalf("collection_meta name=%q want app.users", got)
	}
	if got := calls[1].entry.Decoded.CommandID; got != iwire.CommandInsertBatch {
		t.Fatalf("command id=%d want insert_batch", got)
	}
	if got := mongoClusterCallCatalogVersion(t, calls[0]); got != 7 {
		t.Fatalf("create expected catalog version=%d want 7", got)
	}
	if got := mongoClusterCallCatalogVersion(t, calls[1]); got != 7 {
		t.Fatalf("insert expected catalog version=%d want 7", got)
	}
	if _, err := server.Collections.OpenCollection("app.users"); err == nil {
		t.Fatal("cluster insert created local collection")
	} else if err != collections.ErrCollectionNotFound {
		t.Fatalf("OpenCollection after cluster insert err=%v want collection not found", err)
	}
}

func TestClusterSubmitterInsertSkipsAutoCreateForKnownLocalCollection(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	if _, err := server.Collections.CreateCollection(server.defaultCollectionMeta("app.users")); err != nil {
		t.Fatalf("create collection: %v", err)
	}

	submitter := &mongoClusterFakeSubmitter{}
	setMongoClusterTestSubmitter(server, submitter, 20)
	response := serveCommand(t, server, 325820, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)

	calls := submitter.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
	}
	if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandInsertBatch {
		t.Fatalf("command id=%d want insert_batch", got)
	}
}

func TestClusterSubmitterIdempotencyKeysIncludeServerNonce(t *testing.T) {
	serverA := NewServer()
	serverA.ClusterIdempotencyNonce = "gateway-epoch-a"
	serverA.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	submitterA := &mongoClusterFakeSubmitter{}
	setMongoClusterTestSubmitter(serverA, submitterA, 21)

	serverB := NewServer()
	serverB.ClusterIdempotencyNonce = "gateway-epoch-b"
	serverB.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	submitterB := &mongoClusterFakeSubmitter{}
	setMongoClusterTestSubmitter(serverB, submitterB, 21)

	for _, server := range []*Server{serverA, serverB} {
		response := serveCommand(t, server, 325821, bson.D{
			{Key: "insert", Value: "users"},
			{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
			{Key: "$db", Value: "app"},
		})
		assertOK(t, response)
	}

	callsA := submitterA.snapshotCalls()
	callsB := submitterB.snapshotCalls()
	if len(callsA) != 2 || len(callsB) != 2 {
		t.Fatalf("submit calls A=%d B=%d want 2 each", len(callsA), len(callsB))
	}
	keyA := mongoClusterCallIdempotencyKey(t, callsA[1])
	keyB := mongoClusterCallIdempotencyKey(t, callsB[1])
	if keyA == keyB {
		t.Fatalf("idempotency keys matched across gateway epochs: %q", keyA)
	}
	if !strings.HasPrefix(keyA, "mongo-gateway/gateway-epoch-a/insert_batch/") {
		t.Fatalf("idempotency key A=%q missing gateway epoch", keyA)
	}
	if !strings.HasPrefix(keyB, "mongo-gateway/gateway-epoch-b/insert_batch/") {
		t.Fatalf("idempotency key B=%q missing gateway epoch", keyB)
	}
}

func TestClusterSubmitterUpdateBSONSetRoutesCountsAndNoLocalMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 325802, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "Ada"}}}},
		{Key: "$db", Value: "app"},
	}))

	submitter := &mongoClusterFakeSubmitter{}
	setMongoClusterTestSubmitter(server, submitter, 8)
	response := serveCommand(t, server, 325803, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Grace"}}}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	assertInt32(t, response, "n", 1)
	assertInt32(t, response, "nModified", 1)

	calls := submitter.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
	}
	if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandUpdateBSONSet {
		t.Fatalf("command id=%d want update_bson_set", got)
	}
	found := serveCommand(t, server, 325804, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "$db", Value: "app"},
	})
	batch := cursorFirstBatch(t, found)
	if len(batch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(batch))
	}
	if got, ok := batch[0].Lookup("name").StringValueOK(); !ok || got != "Ada" {
		t.Fatalf("local name after cluster update=%q ok=%v want Ada", got, ok)
	}
}

func TestClusterSubmitterUpdateSubmitsPriorOrderedItemsBeforeUnsupported(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 18)

	response := serveCommand(t, server, 325818, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{
			bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Grace"}}}}},
			},
			bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}},
				{Key: "u", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "age", Value: int32(1)}}}}},
			},
		}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")

	calls := submitter.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
	}
	if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandUpdateBSONSet {
		t.Fatalf("command id=%d want update_bson_set", got)
	}
}

func TestClusterSubmitterUpdateMissingCollectionReturnsZeroCounts(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 23)

	response := serveCommand(t, server, 325823, bson.D{
		{Key: "update", Value: "missing"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "Grace"}}}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	assertInt32(t, response, "n", 0)
	assertInt32(t, response, "nModified", 0)

	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
}

func TestClusterSubmitterDeleteRoutesCommandEntry(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 9)

	response := serveCommand(t, server, 325805, bson.D{
		{Key: "delete", Value: "users"},
		{Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(1)}}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	assertInt32(t, response, "n", 1)

	calls := submitter.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
	}
	if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandDeleteBatch {
		t.Fatalf("command id=%d want delete_batch", got)
	}
}

func TestClusterSubmitterDeleteDeduplicatesDuplicateIDs(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 19)

	response := serveCommand(t, server, 325819, bson.D{
		{Key: "delete", Value: "users"},
		{Key: "deletes", Value: bson.A{
			bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(1)}},
			bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(1)}},
		}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	assertInt32(t, response, "n", 1)

	calls := submitter.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
	}
	ids := mongoClusterTestIDs(calls[0].entry.Decoded.Sections)
	if len(ids) != 1 {
		t.Fatalf("document ids=%d want 1", len(ids))
	}
}

func TestClusterSubmitterDeleteSubmitsPriorOrderedItemsBeforeUnsupported(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 22)

	response := serveCommand(t, server, 325822, bson.D{
		{Key: "delete", Value: "users"},
		{Key: "deletes", Value: bson.A{
			bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(1)}},
			bson.D{{Key: "q", Value: bson.D{{Key: "name", Value: "Ada"}}}, {Key: "limit", Value: int32(1)}},
		}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")

	calls := submitter.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
	}
	if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandDeleteBatch {
		t.Fatalf("command id=%d want delete_batch", got)
	}
	ids := mongoClusterTestIDs(calls[0].entry.Decoded.Sections)
	if len(ids) != 1 {
		t.Fatalf("document ids=%d want 1", len(ids))
	}
}

func TestClusterSubmitterDeleteMissingCollectionReturnsZeroCount(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 24)

	response := serveCommand(t, server, 325824, bson.D{
		{Key: "delete", Value: "missing"},
		{Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(1)}}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	assertInt32(t, response, "n", 0)

	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
}

func TestClusterSubmitterRejectsWriteConcern(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 10)

	response := serveCommand(t, server, 325806, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
		{Key: "writeConcern", Value: bson.D{{Key: "w", Value: "majority"}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")
	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
}

func TestClusterSubmitterUsesRequestContext(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 11)

	raw, err := bson.Marshal(bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
		{Key: "$db", Value: "app"},
	})
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	response, err := server.commandResponse(ctx, "insert", wire.Document(raw), nil, 1)
	if err != nil {
		t.Fatalf("commandResponse: %v", err)
	}
	assertCommandError(t, response, "BadValue")

	calls := submitter.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
	}
	if !errors.Is(calls[0].ctxErr, context.Canceled) {
		t.Fatalf("submit context err=%v want context canceled", calls[0].ctxErr)
	}
}

func TestClusterSubmitterRejectsUnsupportedClusterMutationNoLocalMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 325807, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "age", Value: int32(1)}}}},
		{Key: "$db", Value: "app"},
	}))

	submitter := &mongoClusterFakeSubmitter{}
	setMongoClusterTestSubmitter(server, submitter, 12)
	response := serveCommand(t, server, 325808, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
			{Key: "u", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "age", Value: int32(1)}}}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")
	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
	found := serveCommand(t, server, 325809, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "$db", Value: "app"},
	})
	batch := cursorFirstBatch(t, found)
	if len(batch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(batch))
	}
	if got, ok := batch[0].Lookup("age").Int32OK(); !ok || got != 1 {
		t.Fatalf("local age after rejected cluster update=%d ok=%v want 1", got, ok)
	}
}

func TestClusterSubmitterCreateRoutesCommandEntryNoLocalMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 13)

	response := serveCommand(t, server, 325810, bson.D{
		{Key: "create", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)

	calls := submitter.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
	}
	if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandCreateCollection {
		t.Fatalf("command id=%d want create_collection", got)
	}
	if got := mongoClusterCallCatalogVersion(t, calls[0]); got != 13 {
		t.Fatalf("expected catalog version=%d want 13", got)
	}
	if got := mongoClusterCallCollectionMetaName(t, calls[0]); got != "app.users" {
		t.Fatalf("collection_meta name=%q want app.users", got)
	}
	if _, err := server.Collections.OpenCollection("app.users"); err == nil {
		t.Fatal("cluster create created local collection")
	} else if err != collections.ErrCollectionNotFound {
		t.Fatalf("OpenCollection after cluster create err=%v want collection not found", err)
	}
}

func TestClusterSubmitterRejectsIndexDDLNoLocalMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 325811, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "name", Value: int32(1)}}},
			{Key: "name", Value: "name_1"},
			{Key: "treedbValueType", Value: "string"},
		}}},
		{Key: "$db", Value: "app"},
	}))

	submitter := &mongoClusterFakeSubmitter{}
	setMongoClusterTestSubmitter(server, submitter, 14)
	createResponse := serveCommand(t, server, 325812, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "age", Value: int32(1)}}},
			{Key: "name", Value: "age_1"},
			{Key: "treedbValueType", Value: "int64"},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, createResponse, "BadValue")
	dropResponse := serveCommand(t, server, 325813, bson.D{
		{Key: "dropIndexes", Value: "users"},
		{Key: "index", Value: "name_1"},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, dropResponse, "BadValue")
	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
	col, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open local collection: %v", err)
	}
	indexes := col.MetaView().Indexes
	if len(indexes) != 1 || indexes[0].Name != "name_1" {
		t.Fatalf("local indexes after rejected cluster DDL=%+v want only name_1", indexes)
	}
}

func TestClusterSubmitterRequiresCatalogVersionProvider(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	server.ClusterSubmitter = submitter

	response := serveCommand(t, server, 325814, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")
	if calls := submitter.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("submit calls=%d want 0", len(calls))
	}
}

func TestClusterSubmitterRequiresInsertedCount(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{
		responseSections:         []iwire.Section{mongoClusterTestMeta()},
		overrideResponseSections: true,
	}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 15)

	response := serveCommand(t, server, 325815, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")
	calls := submitter.snapshotCalls()
	if len(calls) != 2 {
		t.Fatalf("submit calls=%d want 2", len(calls))
	}
	if got := calls[1].entry.Decoded.CommandID; got != iwire.CommandInsertBatch {
		t.Fatalf("second command id=%d want insert_batch", got)
	}
}

func TestClusterSubmitterRejectsAckPolicyMismatch(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{
		actualAck:                iwire.AckSynced,
		responseSections:         []iwire.Section{mongoClusterTestMeta("inserted_count", 1)},
		overrideResponseSections: true,
	}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 16)

	response := serveCommand(t, server, 325816, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")
	if calls := submitter.snapshotCalls(); len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
	}
}

func TestClusterSubmitterRejectsRaftCommittedForVisibleRequest(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{
		actualAck:            iwire.AckRaftCommitted,
		committedRecoverable: true,
	}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 17)

	response := serveCommand(t, server, 325817, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")
	if calls := submitter.snapshotCalls(); len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
	}
}
