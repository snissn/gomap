package nativewire

import (
	"bytes"
	"context"
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
