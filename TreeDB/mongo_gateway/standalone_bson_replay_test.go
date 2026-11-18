package mongogateway

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestStandaloneExplicitFastBSONPrimaryReplay(t *testing.T) {
	const dirEnv = "GOMAP_MONGO_TEST_EXPLICIT_FAST_REPLAY_DIR"
	dir := os.Getenv(dirEnv)
	child := dir != ""
	if !child {
		dir = t.TempDir()
	}
	opts := StandaloneOptions{
		Dir:     dir,
		Profile: treedb.ProfileCommandWALDurable,
		DefaultCollectionOptions: collections.CollectionOptions{
			DocumentFormat:        collections.DocumentFormatBSON,
			DataRootStoragePolicy: collections.RootStorageFast,
		},
	}
	payload := bytes.Repeat([]byte{0x63, 0x51, 0x23, 0xd0}, 16<<10)
	if child {
		server, err := OpenStandaloneServer(opts)
		if err != nil {
			t.Fatal(err)
		}
		documents := make(bson.A, 0, 65)
		for i := range 64 {
			documents = append(documents, bson.D{
				{Key: "_id", Value: fmt.Sprintf("doc-%03d", i)},
				{Key: "payload", Value: string(bytes.Repeat([]byte{'x'}, 128))},
			})
		}
		documents = append(documents, bson.D{
			{Key: "_id", Value: "large"},
			{Key: "payload", Value: bson.Binary{Data: payload}},
		})
		reply := serveCommand(t, server.Server, 1, bson.D{
			{Key: "insert", Value: "docs"},
			{Key: "documents", Value: documents},
			{Key: "writeConcern", Value: bson.D{{Key: "w", Value: int32(1)}}},
			{Key: "$db", Value: "layout"},
		})
		assertOK(t, reply)
		assertInt32(t, reply, "n", int32(len(documents)))
		durable, err := strconv.ParseUint(server.Backend.Stats()["treedb.command_wal.durable_wal_lsn"], 10, 64)
		if applied := server.Backend.State().AppliedCommandLSN; err != nil || durable != 2 || applied != 0 {
			t.Fatalf("no durable work left for replay: applied=%d durable=%d err=%v", applied, durable, err)
		}
		// Exit without closing the server so reopen must apply the durable WAL.
		os.Exit(0)
	}

	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestStandaloneExplicitFastBSONPrimaryReplay$")
	cmd.Env = append(os.Environ(), dirEnv+"="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("write child: %v\n%s", err, output)
	}
	server, err := OpenStandaloneServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	})
	if applied := server.Backend.State().AppliedCommandLSN; applied != 2 {
		t.Fatalf("reopen applied LSN=%d, want 2", applied)
	}
	collection, err := server.Collections.OpenCollection("layout.docs")
	if err != nil {
		t.Fatal(err)
	}
	if policy := collection.Meta().Options.DataRootStoragePolicy; policy != collections.RootStorageFast {
		t.Fatalf("replayed primary policy=%q, want fast", policy)
	}
	reply := serveCommand(t, server.Server, 2, bson.D{
		{Key: "find", Value: "docs"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "large"}}},
		{Key: "$db", Value: "layout"},
	})
	assertOK(t, reply)
	docs := cursorFirstBatch(t, reply)
	if len(docs) != 1 {
		t.Fatalf("find returned %d documents, want 1", len(docs))
	}
	_, got, ok := docs[0].Lookup("payload").BinaryOK()
	if !ok || !bytes.Equal(got, payload) {
		t.Fatalf("replayed payload has %d bytes, valid binary=%t, want %d bytes", len(got), ok, len(payload))
	}

	capture, err := server.Backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := capture.Close(); err != nil {
			t.Error(err)
		}
	}()
	snapshot := capture.Snapshot()
	token, ok := snapshot.StateToken()
	if !ok {
		t.Fatal("missing physical snapshot token")
	}
	raw, err := snapshot.GetAtRoot(token.SystemRootPageID, []byte("collections/root/layout.docs/primary"))
	if err != nil || len(raw) != 8 {
		t.Fatalf("primary descriptor=%x err=%v", raw, err)
	}
	root := binary.BigEndian.Uint64(raw)
	var leafRefs int
	err = leafrefscan.Walk(t.Context(), root, snapshot.Pager().Get, nil, func(page.LeafLogPtr) error {
		leafRefs++
		return nil
	})
	if err != nil || leafRefs != 0 {
		t.Errorf("replayed fast primary root has %d external leaf references, err=%v", leafRefs, err)
	}
	reader, err := snapshot.NativeReaderAtRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	key, err := encodePrimaryKey(mustRawValue(t, "large"))
	if err != nil {
		t.Fatal(err)
	}
	var keyScratch, leafScratch [page.PageSize]byte
	entry, err := reader.GetEntryExactWithFixedScratch(key, keyScratch[:], leafScratch[:], capture.(backenddb.NativePhysicalLeafReader).NativeLeafPage)
	if err != nil || entry.Flags&node.FlagPointer == 0 {
		t.Fatalf("replayed large BSON entry flags=%d err=%v, want value-log pointer", entry.Flags, err)
	}
}
