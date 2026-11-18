package mongogateway

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestStandaloneBSONPrimaryLayout(t *testing.T) {
	for _, tc := range []struct {
		name       string
		policy     collections.RootStoragePolicy
		wantPolicy collections.RootStoragePolicy
		wantLeaf   bool
		existing   bool
	}{
		{name: "fast", policy: collections.RootStorageFast, wantPolicy: collections.RootStorageFast},
		{name: "default", wantPolicy: collections.RootStorageFast},
		{name: "compressed", policy: collections.RootStorageCompressed, wantPolicy: collections.RootStorageCompressed, wantLeaf: true},
		{name: "existing-default", wantLeaf: true, existing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := StandaloneOptions{
				Dir:     t.TempDir(),
				Profile: treedb.ProfileCommandWALRelaxed,
				DefaultCollectionOptions: collections.CollectionOptions{
					DataRootStoragePolicy: tc.policy,
				},
			}
			payload := bson.Binary{Data: bytes.Repeat([]byte{0x8f, 0x1d, 0xa7, 0x25}, 16<<10)}
			server := openStandaloneBSONLayoutTest(t, opts)
			if tc.existing {
				// Explicitly seed the original catalog spelling independently
				// of the default that the new gateway uses for new collections.
				if _, err := server.Collections.CreateCollection(&collections.CollectionMeta{
					Name:    "layout.docs",
					Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
				}); err != nil {
					t.Fatal(err)
				}
			}
			insertStandaloneBSONLayoutRows(t, server.Server, "docs", "old", payload)
			if err := server.Backend.SyncNativeSemanticPrefix(); err != nil {
				t.Fatal(err)
			}
			if got := standaloneBSONLayout(t, server, "docs", "old-large"); got != tc.wantLeaf {
				t.Errorf("implicit create has external leaf refs=%t, want %t", got, tc.wantLeaf)
			}
			if err := server.Close(); err != nil {
				t.Fatal(err)
			}

			server = openStandaloneBSONLayoutTest(t, opts)
			findStandaloneBSONLayoutPayload(t, server.Server, "docs", "old-large", payload)
			held := captureStandaloneBSONLayoutRead(t, server, "docs")
			updated := bson.Binary{Data: bytes.Repeat([]byte{0x44, 0x86, 0xc3, 0x79}, 16<<10)}
			checkedUpdateStandaloneBSONLayout(t, server, held, payload, updated)
			insertStandaloneBSONLayoutRows(t, server.Server, "docs", "new", payload)
			if err := server.Backend.SyncNativeSemanticPrefix(); err != nil {
				t.Fatal(err)
			}
			if got := standaloneBSONLayout(t, server, "docs", "new-large"); got != tc.wantLeaf {
				t.Errorf("writer after reopen has external leaf refs=%t, want %t", got, tc.wantLeaf)
			}
			findStandaloneBSONLayoutPayload(t, server.Server, "docs", "old-large", updated)
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			if err := server.Close(); err != nil {
				t.Fatal(err)
			}

			if err := treedb.VacuumIndexOffline(treedb.OptionsFor(opts.Profile, opts.Dir)); err != nil {
				t.Fatalf("vacuum closed standalone: %v", err)
			}
			server = openStandaloneBSONLayoutTest(t, opts)
			findStandaloneBSONLayoutPayload(t, server.Server, "docs", "old-large", updated)
			findStandaloneBSONLayoutPayload(t, server.Server, "docs", "new-large", payload)
			if got := standaloneBSONLayout(t, server, "docs", "new-large"); got != tc.wantLeaf {
				t.Errorf("reopen after vacuum has external leaf refs=%t, want %t", got, tc.wantLeaf)
			}
			collection, err := server.Collections.OpenCollection("layout.docs")
			if err != nil {
				t.Fatal(err)
			}
			if got := collection.Meta().Options.DataRootStoragePolicy; got != tc.wantPolicy {
				t.Errorf("stored primary policy=%q, want %q", got, tc.wantPolicy)
			}
			if err := server.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStandaloneBSONPrimaryLayoutDefaultScope(t *testing.T) {
	for _, tc := range []struct {
		name   string
		format collections.DocumentFormat
		policy collections.RootStoragePolicy
	}{
		{name: "local BSON", format: collections.DocumentFormatBSON, policy: collections.RootStorageFast},
		{name: "cluster BSON", format: collections.DocumentFormatBSON},
		{name: "JSON", format: collections.DocumentFormatJSON},
		{name: "template", format: collections.DocumentFormatTemplateV1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := StandaloneOptions{
				Dir: t.TempDir(),
				DefaultCollectionOptions: collections.CollectionOptions{
					DocumentFormat:          tc.format,
					IndexStateStoragePolicy: collections.RootStorageCompressed,
				},
				DefaultIndexStoragePolicy: collections.RootStorageCompressed,
			}
			if tc.name == "cluster BSON" {
				opts.ClusterSubmitter = &mongoClusterFakeSubmitter{}
				opts.ClusterCatalogVersion = mongoClusterStaticCatalogVersion(7)
			}
			server := openStandaloneBSONLayoutTest(t, opts)
			got := server.Server.DefaultCollectionOptions
			if got.DocumentFormat != tc.format || got.DataRootStoragePolicy != tc.policy ||
				got.IndexStateStoragePolicy != opts.DefaultCollectionOptions.IndexStateStoragePolicy ||
				server.Server.DefaultIndexStoragePolicy != opts.DefaultIndexStoragePolicy {
				t.Fatalf("collection defaults=%+v, index default=%q", got, server.Server.DefaultIndexStoragePolicy)
			}
		})
	}
}

func captureStandaloneBSONLayoutRead(t *testing.T, server *StandaloneServer, name string) *collections.NativeJSONRead {
	t.Helper()
	manager, err := collections.NewNativeJSONManager(server.Backend)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := manager.Capture("layout." + name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	})
	return reader
}

func checkedUpdateStandaloneBSONLayout(t *testing.T, server *StandaloneServer, held *collections.NativeJSONRead, old, updated bson.Binary) {
	t.Helper()
	key, err := encodePrimaryKey(mustRawValue(t, "old-large"))
	if err != nil {
		t.Fatal(err)
	}
	checkHeld := func() error {
		raw, err := held.Get(key)
		if err != nil {
			return err
		}
		subtype, data, ok := bson.Raw(raw).Lookup("payload").BinaryOK()
		if !ok || subtype != old.Subtype || !bytes.Equal(data, old.Data) {
			return fmt.Errorf("held root returned subtype=%d bytes=%d valid=%t", subtype, len(data), ok)
		}
		return nil
	}
	if err := checkHeld(); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	finish := make(chan struct{})
	readDone := make(chan error, 1)
	go func() {
		close(started)
		for {
			if err := checkHeld(); err != nil {
				readDone <- err
				return
			}
			select {
			case <-finish:
				readDone <- nil
				return
			default:
			}
		}
	}()
	<-started
	defer func() {
		close(finish)
		if err := <-readDone; err != nil {
			t.Error(err)
		}
	}()
	response := serveCommand(t, server.Server, 3, bson.D{
		{Key: "update", Value: "docs"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "old-large"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "payload", Value: updated}}}}},
		}}},
		{Key: "writeConcern", Value: bson.D{{Key: "w", Value: int32(1)}}},
		{Key: "$db", Value: "layout"},
	})
	assertOK(t, response)
	assertInt32(t, response, "nModified", 1)
	if err := server.Backend.SyncNativeSemanticPrefix(); err != nil {
		t.Fatal(err)
	}
	if err := checkHeld(); err != nil {
		t.Fatal(err)
	}
	findStandaloneBSONLayoutPayload(t, server.Server, "docs", "old-large", updated)
}

func openStandaloneBSONLayoutTest(t *testing.T, opts StandaloneOptions) *StandaloneServer {
	t.Helper()
	server, err := OpenStandaloneServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	})
	return server
}

func insertStandaloneBSONLayoutRows(t *testing.T, server *Server, collection, prefix string, payload bson.Binary) {
	t.Helper()
	documents := make(bson.A, 0, 65)
	for i := range 64 {
		documents = append(documents, bson.D{{Key: "_id", Value: fmt.Sprintf("%s-%03d", prefix, i)}, {Key: "payload", Value: string(bytes.Repeat([]byte("x"), 128))}})
	}
	documents = append(documents, bson.D{{Key: "_id", Value: prefix + "-large"}, {Key: "payload", Value: payload}})
	response := serveCommand(t, server, 1, bson.D{{Key: "insert", Value: collection}, {Key: "documents", Value: documents}, {Key: "$db", Value: "layout"}})
	assertOK(t, response)
	assertInt32(t, response, "n", int32(len(documents)))
}

func findStandaloneBSONLayoutPayload(t *testing.T, server *Server, collection, id string, want bson.Binary) {
	t.Helper()
	response := serveCommand(t, server, 2, bson.D{{Key: "find", Value: collection}, {Key: "filter", Value: bson.D{{Key: "_id", Value: id}}}, {Key: "$db", Value: "layout"}})
	assertOK(t, response)
	docs := cursorFirstBatch(t, response)
	if len(docs) != 1 {
		t.Fatalf("find %q returned %d documents", id, len(docs))
	}
	subtype, data, ok := docs[0].Lookup("payload").BinaryOK()
	if !ok || subtype != want.Subtype || !bytes.Equal(data, want.Data) {
		t.Fatalf("find %q payload subtype=%d bytes=%d ok=%t, want subtype=%d bytes=%d", id, subtype, len(data), ok, want.Subtype, len(want.Data))
	}
}

func standaloneBSONLayout(t *testing.T, server *StandaloneServer, collection, largeID string) bool {
	t.Helper()
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
	raw, err := snapshot.GetAtRoot(token.SystemRootPageID, []byte("collections/root/layout."+collection+"/primary"))
	if err != nil || len(raw) != 8 {
		t.Fatalf("primary descriptor=%x err=%v", raw, err)
	}
	root := binary.BigEndian.Uint64(raw)
	var leafRefs int
	var leafBytes int64
	leafFiles := make(map[uint32]bool)
	if err := leafrefscan.Walk(t.Context(), root, snapshot.Pager().Get, nil, func(ptr page.LeafLogPtr) error {
		leafRefs++
		id := ptr.ValueLogFileID()
		if leafFiles[id] {
			return nil
		}
		file, _, err := snapshot.PinnedValueLogFile(id)
		if err != nil {
			return err
		}
		info, err := file.Stat()
		if err != nil {
			return err
		}
		if info.Size() == 0 {
			return fmt.Errorf("referenced leaf file %q is empty", info.Name())
		}
		leafFiles[id] = true
		leafBytes += info.Size()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	reader, err := snapshot.NativeReaderAtRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	key, err := encodePrimaryKey(mustRawValue(t, largeID))
	if err != nil {
		t.Fatal(err)
	}
	var keyScratch, leafScratch [page.PageSize]byte
	entry, err := reader.GetEntryExactWithFixedScratch(key, keyScratch[:], leafScratch[:], capture.(backenddb.NativePhysicalLeafReader).NativeLeafPage)
	if err != nil || entry.Flags&node.FlagPointer == 0 {
		t.Fatalf("persisted large BSON entry flags=%d err=%v, want value-log pointer", entry.Flags, err)
	}
	file, _, err := snapshot.PinnedValueLogFile(entry.ValuePtr.FileID)
	if err != nil {
		t.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("appender=%t root=%d leaf_refs=%d leaf_files=%d leaf_file_bytes=%d payload_file=%s payload_file_bytes=%d", server.Backend.HasValueLogAppender(), root, leafRefs, len(leafFiles), leafBytes, info.Name(), info.Size())
	return leafRefs > 0
}
