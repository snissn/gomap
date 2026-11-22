//go:build windows

package mongogateway

import (
	"context"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Windows does not use fixed preallocated command WAL segments, so the shared
// fixed-writer journal boundary must stay unselected and fall back cleanly to
// the maintained exclusive fence while preserving durable ACKs and reopen.
func TestStandaloneNativeJournalWindowsFallsBackToExclusiveBoundary(t *testing.T) {
	dir := t.TempDir()
	opts := StandaloneOptions{
		Dir:                      dir,
		Profile:                  treedb.ProfileCommandWALDurable,
		DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
	}
	s, err := OpenStandaloneServer(opts)
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	if s.Server.canShareStandaloneJournalBoundary("insert") || s.Server.canShareStandaloneJournalBoundary("update") {
		_ = s.Close()
		t.Fatal("Windows without fixed WAL must not select shared fixed-writer journal boundary")
	}
	assertOK(t, serveCommand(t, s.Server, 1, bson.D{{Key: "create", Value: "rows"}, {Key: "$db", Value: "app"}}))
	doc := standaloneReceiptTestDocument("win-1", 1)
	resp, err := s.Server.commandResponse(context.Background(), "insert", mustDocument(t, bson.D{
		{Key: "insert", Value: "rows"},
		{Key: "documents", Value: bson.A{doc}},
		{Key: "writeConcern", Value: bson.D{{Key: "w", Value: int32(1)}, {Key: "j", Value: true}}},
		{Key: "$db", Value: "app"},
	}), nil, 0)
	if err != nil || !mongoCommandResponseOK(resp) {
		_ = s.Close()
		t.Fatalf("Windows fallback insert: resp=%s err=%v", resp, err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("checked Close: %v", err)
	}
	reopened, err := OpenStandaloneServer(opts)
	if err != nil {
		t.Fatalf("reopen standalone: %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("reopened Close: %v", err)
		}
	}()
	findResp := serveCommand(t, reopened.Server, 2, bson.D{{Key: "find", Value: "rows"}, {Key: "filter", Value: bson.D{}}, {Key: "$db", Value: "app"}})
	assertOK(t, findResp)
	docs := cursorFirstBatch(t, findResp)
	if len(docs) != 1 {
		t.Fatalf("reopen count=%d want 1", len(docs))
	}
	requireStandaloneReceiptTestDocument(t, docs[0], "win-1", 1)
}
