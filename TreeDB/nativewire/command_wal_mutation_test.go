package nativewire

import (
	"context"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNativewireCommandWALMutationsReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := backenddb.Open(backenddb.Options{
		Dir:                 dir,
		CommandWAL:          true,
		CommandWALStatsScan: true,
	})
	if err != nil {
		t.Fatalf("open command WAL db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	server := NewServer(ServerOptions{Collections: mgr, Backend: db})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, cleanup, err := NewInProcessClient(ctx, server)
	if err != nil {
		_ = server.Close()
		_ = db.Close()
		t.Fatalf("NewInProcessClient: %v", err)
	}

	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{
		Name: "users",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatBSON,
		},
	}); err != nil {
		_ = cleanup()
		_ = server.Close()
		_ = db.Close()
		t.Fatalf("CreateCollection: %v", err)
	}
	if _, err := client.InsertBatch(ctx, "users", collections.DocumentFormatBSON,
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			mustBSONDocument(t, bson.D{{Key: "name", Value: "Ada"}, {Key: "generation", Value: int32(1)}}),
			mustBSONDocument(t, bson.D{{Key: "name", Value: "Grace"}, {Key: "generation", Value: int32(1)}}),
		},
		AckVisible,
	); err != nil {
		_ = cleanup()
		_ = server.Close()
		_ = db.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	matched, modified, err := client.ReplaceBatch(ctx, "users", collections.DocumentFormatBSON,
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONDocument(t, bson.D{{Key: "name", Value: "Ada"}, {Key: "generation", Value: int32(2)}})},
		AckVisible,
	)
	if err != nil {
		_ = cleanup()
		_ = server.Close()
		_ = db.Close()
		t.Fatalf("ReplaceBatch: %v", err)
	}
	if matched != 1 || modified != 1 {
		_ = cleanup()
		_ = server.Close()
		_ = db.Close()
		t.Fatalf("ReplaceBatch matched=%d modified=%d, want 1/1", matched, modified)
	}
	deleted, err := client.DeleteBatch(ctx, "users", [][]byte{[]byte("u2")}, AckVisible)
	if err != nil {
		_ = cleanup()
		_ = server.Close()
		_ = db.Close()
		t.Fatalf("DeleteBatch: %v", err)
	}
	if deleted != 1 {
		_ = cleanup()
		_ = server.Close()
		_ = db.Close()
		t.Fatalf("DeleteBatch deleted=%d, want 1", deleted)
	}
	stats, err := client.Stats(ctx)
	if err != nil {
		_ = cleanup()
		_ = server.Close()
		_ = db.Close()
		t.Fatalf("Stats: %v", err)
	}
	if got := stats["treedb.command_wal.required_feature"]; got != "true" {
		_ = cleanup()
		_ = server.Close()
		_ = db.Close()
		t.Fatalf("required_feature=%q, want true", got)
	}
	if got := db.State().AppliedCommandLSN; got < 4 {
		_ = cleanup()
		_ = server.Close()
		_ = db.Close()
		t.Fatalf("AppliedCommandLSN=%d, want at least 4", got)
	}
	if err := cleanup(); err != nil {
		_ = server.Close()
		_ = db.Close()
		t.Fatalf("client cleanup: %v", err)
	}
	if err := server.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("server close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{
		Dir:                 dir,
		CommandWAL:          true,
		CommandWALStatsScan: true,
	})
	if err != nil {
		t.Fatalf("reopen command WAL db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedMgr := collections.NewCollectionManager(reopened)
	col, err := reopenedMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	stored, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1 after reopen: %v", err)
	}
	raw := bson.Raw(stored)
	if err := raw.Validate(); err != nil {
		t.Fatalf("reopened BSON validation: %v", err)
	}
	name, ok := raw.Lookup("name").StringValueOK()
	if !ok || name != "Ada" {
		t.Fatalf("reopened name=%q ok=%v, want Ada", name, ok)
	}
	generation, ok := raw.Lookup("generation").Int32OK()
	if !ok || generation != 2 {
		t.Fatalf("reopened generation=%d ok=%v, want 2", generation, ok)
	}
	missing, err := col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2 after reopen: %v", err)
	}
	if missing != nil {
		t.Fatalf("u2 exists after command-WAL delete replay: %s", missing)
	}
	if got := reopened.State().AppliedCommandLSN; got < 4 {
		t.Fatalf("reopened AppliedCommandLSN=%d, want at least 4", got)
	}
}

func mustBSONDocument(t *testing.T, doc bson.D) []byte {
	t.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal BSON document: %v", err)
	}
	return raw
}
