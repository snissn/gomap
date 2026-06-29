package mongogateway

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
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

func mongoClusterCallCatalogVersion(tb testing.TB, call mongoClusterSubmitterCall) uint64 {
	tb.Helper()
	version, n := binary.Uvarint(call.entry.Target.ExpectedCatalogVersion)
	if n <= 0 || n != len(call.entry.Target.ExpectedCatalogVersion) {
		tb.Fatalf("expected_catalog_version bytes=%v decode n=%d", call.entry.Target.ExpectedCatalogVersion, n)
	}
	return version
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
	if len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
	}
	if got := calls[0].entry.Decoded.CommandID; got != iwire.CommandInsertBatch {
		t.Fatalf("command id=%d want insert_batch", got)
	}
	if got := mongoClusterCallCatalogVersion(t, calls[0]); got != 7 {
		t.Fatalf("expected catalog version=%d want 7", got)
	}
	if _, err := server.Collections.OpenCollection("app.users"); err == nil {
		t.Fatal("cluster insert created local collection")
	} else if err != collections.ErrCollectionNotFound {
		t.Fatalf("OpenCollection after cluster insert err=%v want collection not found", err)
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
	if calls := submitter.snapshotCalls(); len(calls) != 1 {
		t.Fatalf("submit calls=%d want 1", len(calls))
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
