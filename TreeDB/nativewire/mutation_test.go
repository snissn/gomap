package nativewire

import (
	"bytes"
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func TestMutationCommandsRoundTrip(t *testing.T) {
	client, mgr, db := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{
		Name: "users",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
		},
		Indexes: []collections.IndexDefinition{{Name: "email", Field: "email", ValueType: collections.IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	ids, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com","name":"Ada"}`),
			[]byte(`{"email":"grace@example.com","name":"Grace"}`),
		},
		AckFlushed,
	)
	if err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if len(ids) != 2 || string(ids[0]) != "u1" || string(ids[1]) != "u2" {
		t.Fatalf("insert ids=%q", ids)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	doc, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("direct get: %v", err)
	}
	if !bytes.Contains(doc, []byte(`"Ada"`)) {
		t.Fatalf("u1 doc=%s", doc)
	}
	handle, err := client.OpenCollection(ctx, "users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	handleIDs, err := client.InsertBatchHandle(ctx, handle, collections.DocumentFormatJSON,
		[][]byte{[]byte("u3")},
		[][]byte{[]byte(`{"email":"katherine@example.com","name":"Katherine"}`)},
		AckVisible,
	)
	if err != nil {
		t.Fatalf("InsertBatchHandle: %v", err)
	}
	if len(handleIDs) != 1 || string(handleIDs[0]) != "u3" {
		t.Fatalf("handle insert ids=%q", handleIDs)
	}
	if err := client.InsertBatchHandleNoResult(ctx, handle, collections.DocumentFormatJSON,
		[][]byte{[]byte("u4")},
		[][]byte{[]byte(`{"email":"dorothy@example.com","name":"Dorothy"}`)},
		AckVisible,
	); err != nil {
		t.Fatalf("InsertBatchHandleNoResult: %v", err)
	}
	noResultDoc, err := col.Get([]byte("u4"))
	if err != nil {
		t.Fatalf("direct get no-result insert: %v", err)
	}
	if !bytes.Contains(noResultDoc, []byte(`"Dorothy"`)) {
		t.Fatalf("u4 doc=%s", noResultDoc)
	}
	noIDsBody, err := appendInsertBatchRequestBodyRefFlags(nil, "users", 0, false, collections.DocumentFormatJSON,
		[][]byte{[]byte("u5")},
		[][]byte{[]byte(`{"email":"mary@example.com","name":"Mary"}`)},
		AckVisible,
		iwire.CommandFlagOmitResultIDs,
	)
	if err != nil {
		t.Fatalf("append no-result insert: %v", err)
	}
	_, noIDsResponse, err := client.roundTrip(ctx, iwire.FrameRequest, noIDsBody, iwire.FrameResponse)
	if err != nil {
		t.Fatalf("roundTrip no-result insert: %v", err)
	}
	noIDsSections, err := iwire.DecodeSections(noIDsResponse, client.limits)
	if err != nil {
		t.Fatalf("decode no-result response: %v", err)
	}
	if _, ok, err := singletonSection(noIDsSections, iwire.SectionDocumentIDs); err != nil {
		t.Fatalf("no-result document_ids section: %v", err)
	} else if ok {
		t.Fatalf("no-result response unexpectedly included document_ids")
	}
	if inserted, err := responseCount(noIDsSections, "inserted_count"); err != nil || inserted != 1 {
		t.Fatalf("no-result inserted_count=%d err=%v want 1", inserted, err)
	}

	matched, modified, err := client.ReplaceBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1"), []byte("missing")},
		[][]byte{
			[]byte(`{"email":"ada@example.com","name":"Ada Lovelace"}`),
			[]byte(`{"email":"nobody@example.com","name":"Nobody"}`),
		},
		AckVisible,
	)
	if err != nil {
		t.Fatalf("ReplaceBatch: %v", err)
	}
	if matched != 1 || modified != 1 {
		t.Fatalf("matched=%d modified=%d want 1/1", matched, modified)
	}
	deleted, err := client.DeleteBatch(ctx, "users", [][]byte{[]byte("u2"), []byte("missing")}, AckSynced)
	if err != nil {
		t.Fatalf("DeleteBatch: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted=%d want 1", deleted)
	}
	if err := client.FlushCollection(ctx, "users"); err != nil {
		t.Fatalf("FlushCollection: %v", err)
	}
	if err := client.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	if err := client.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if stats := db.Stats(); len(stats) == 0 {
		t.Fatalf("empty backend stats after checkpoint")
	}
}

func TestMutationDuplicateIDRejected(t *testing.T) {
	client, _, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	_, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1"), []byte("u1")},
		[][]byte{[]byte(`{"x":1}`), []byte(`{"x":2}`)},
		AckVisible,
	)
	if !isRemoteError(err, iwire.ErrDuplicateDocumentID) {
		t.Fatalf("InsertBatch duplicate err=%v want duplicate document id", err)
	}
}

func TestInsertBatchFastDecodeErrorReturnsBeforeDispatch(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	body, err := appendInsertBatchRequestBody(nil, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		nil,
		AckVisible,
	)
	if err != nil {
		t.Fatalf("append request: %v", err)
	}
	_, _, err = client.roundTrip(ctx, iwire.FrameRequest, body, iwire.FrameResponse)
	if !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("InsertBatch malformed err=%v want invalid command", err)
	}
	assertDocumentMissing(t, mgr, "users", "u1")
}

func TestInsertBatchFastRejectsEmptyIDBeforeDispatch(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	_, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("")},
		[][]byte{[]byte(`{"x":1}`)},
		AckVisible,
	)
	if !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("InsertBatch empty ID err=%v want invalid command", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	records, truncated, err := col.ScanDocuments(10)
	if err != nil {
		t.Fatalf("ScanDocuments: %v", err)
	}
	if truncated || len(records) != 0 {
		t.Fatalf("records=%+v truncated=%v want empty collection", records, truncated)
	}
}

func TestMutationExplicitZeroAckUsesDefault(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	sections, err := client.commandSections(ctx, iwire.CommandInsertBatch,
		collectionNameRef("users"),
		documentFormatSection(collections.DocumentFormatJSON),
		iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("u1"))},
		iwire.Section{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte(`{"x":1}`))},
		ackSection(0),
	)
	if err != nil {
		t.Fatalf("InsertBatch explicit zero ack: %v", err)
	}
	if actual, err := responseAckPolicy(sections); err != nil || actual != AckVisible {
		t.Fatalf("actual ack=%v err=%v want visible", actual, err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if doc, err := col.Get([]byte("u1")); err != nil || len(doc) == 0 {
		t.Fatalf("Get u1 doc=%q err=%v", doc, err)
	}
}

func TestMutationInvalidBSONRejectedBeforeTrustedInsert(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{
		Name:    "users",
		Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	_, err := client.InsertBatch(ctx, "users", collections.DocumentFormatBSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte{0x05, 0x00}},
		AckVisible,
	)
	if !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("InsertBatch invalid BSON err=%v want invalid command", err)
	}
	assertDocumentMissing(t, mgr, "users", "u1")
}

func TestMutationRaftAckRejected(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	_, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"x":1}`)},
		AckRaftCommitted,
	)
	if !isRemoteError(err, iwire.ErrDurabilityUnavailable) {
		t.Fatalf("InsertBatch raft ack err=%v want durability unavailable", err)
	}
	assertDocumentMissing(t, mgr, "users", "u1")
}

func TestMutationSyncedAckWithoutBackendRejectedBeforeWrite(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	server := NewServer(ServerOptions{Collections: mgr})
	client, _ := servePipe(t, server)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	_, err = client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"x":1}`)},
		AckSynced,
	)
	if !isRemoteError(err, iwire.ErrDurabilityUnavailable) {
		t.Fatalf("InsertBatch synced ack err=%v want durability unavailable", err)
	}
	assertDocumentMissing(t, mgr, "users", "u1")
}

func TestMutationReplaceRejectsUnsupportedReplacementModeBeforeWrite(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	if _, err := client.InsertBatch(ctx, "users", collections.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"name":"Ada"}`)},
		AckVisible,
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	_, err := client.commandSections(ctx, iwire.CommandReplaceBatch,
		collectionNameRef("users"),
		documentFormatSection(collections.DocumentFormatJSON),
		iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("u1"))},
		iwire.Section{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte(`{"name":"Ada Changed"}`))},
		iwire.Section{ID: iwire.SectionReplacementMode, Bytes: []byte{2}},
		ackSection(AckVisible),
	)
	if !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("ReplaceBatch invalid replacement_mode err=%v want invalid command", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	doc, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get u1: %v", err)
	}
	if !bytes.Contains(doc, []byte(`"Ada"`)) || bytes.Contains(doc, []byte(`Changed`)) {
		t.Fatalf("u1 changed after rejected replace: %s", doc)
	}
}

func TestMutationBarrierRejectsUnsatisfiedAckPolicy(t *testing.T) {
	client, _, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	_, err := client.commandSections(ctx, iwire.CommandFlushCollection, collectionNameRef("users"), ackSection(AckSynced))
	if !isRemoteError(err, iwire.ErrDurabilityUnavailable) {
		t.Fatalf("FlushCollection synced ack err=%v want durability unavailable", err)
	}
	_, err = client.commandSections(ctx, iwire.CommandFlushAll, ackSection(AckSynced))
	if !isRemoteError(err, iwire.ErrDurabilityUnavailable) {
		t.Fatalf("FlushAll synced ack err=%v want durability unavailable", err)
	}
	_, err = client.commandSections(ctx, iwire.CommandCheckpoint, ackSection(AckRaftCommitted))
	if !isRemoteError(err, iwire.ErrDurabilityUnavailable) {
		t.Fatalf("Checkpoint raft ack err=%v want durability unavailable", err)
	}
}

func assertDocumentMissing(t *testing.T, mgr *collections.CollectionManager, collectionName, id string) {
	t.Helper()
	col, err := mgr.OpenCollection(collectionName)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	doc, err := col.Get([]byte(id))
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	if doc != nil {
		t.Fatalf("document %q exists after rejected mutation: %s", id, doc)
	}
}

func responseAckPolicy(sections []iwire.Section) (AckPolicy, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionResponseMeta)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, protocolError(iwire.ErrMalformedFrame, "missing response_meta")
	}
	values, err := decodeStringMap(raw)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseUint(values["actual_ack_policy"], 10, 64)
	if err != nil {
		return 0, err
	}
	return AckPolicy(n), nil
}
