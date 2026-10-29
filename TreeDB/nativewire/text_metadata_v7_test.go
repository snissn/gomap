package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func textMetadataV7Fixture(format collections.DocumentFormat, version collections.TextIndexVersion) collections.CollectionMeta {
	return collections.CollectionMeta{
		Name:    "articles",
		Options: collections.CollectionOptions{DocumentFormat: format, DataRootStoragePolicy: collections.RootStorageCompressed, IndexStateStoragePolicy: collections.RootStorageCompressed, DisableBufferedIndexedAsyncFlush: true},
		TextIndexes: []collections.TextIndexDefinition{{
			Name: "content", Fields: []collections.TextIndexField{{Field: "title", Weight: 3.5}, {Field: "body", Weight: 0.75}},
			Analyzer: collections.TextAnalyzerSimple, AnalyzerOptions: &collections.TextAnalyzerOptions{StopWords: []string{"and", "the"}},
			Version: version, Rollout: collections.TextIndexRolloutPrimary, StorePositions: true, StoreOffsets: true,
			StoragePolicy: collections.RootStorageCompressed, SchemaGeneration: 17,
		}},
	}
}

// Use the ordinary public opener and factory, retaining their shared COW owner.
// The same declarations must cross Client -> Server -> catalog -> reopen -> Client.
func TestTextMetadataV7PublicCreateListReopen(t *testing.T) {
	for _, format := range []collections.DocumentFormat{collections.DocumentFormatJSON, collections.DocumentFormatBSON, collections.DocumentFormatTemplateV1} {
		for _, version := range []collections.TextIndexVersion{collections.TextIndexVersionV1, collections.TextIndexVersionV2} {
			t.Run(string(format)+"/"+string(version), func(t *testing.T) {
				dir := t.TempDir()
				open := func() (*Client, *collections.CollectionManager, func()) {
					opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
					opts.MemtableMode = "cow_btree"
					owner, err := treedb.Open(opts)
					if err != nil {
						t.Fatal(err)
					}
					backend, err := owner.NativeSemanticBackend()
					if err != nil {
						t.Fatal(err)
					}
					mgr := collections.NewCollectionManager(backend)
					if !mgr.UsesNativeCommandWALApply() {
						t.Fatal("factory did not select shared native owner")
					}
					server := NewServer(ServerOptions{Collections: mgr, Backend: backend})
					client, done := servePipe(t, server)
					return client, mgr, func() {
						_ = client.Close()
						select {
						case <-done:
						case <-time.After(5 * time.Second):
							t.Fatal("connection did not close")
						}
						if err := server.Close(); err != nil {
							t.Fatal(err)
						}
						if err := owner.Close(); err != nil {
							t.Fatal(err)
						}
					}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				client, mgr, closeFirst := open()
				if err := client.Hello(ctx); err != nil {
					t.Fatal(err)
				}
				input := textMetadataV7Fixture(format, version)
				created, err := client.CreateCollection(ctx, input)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(created.TextIndexes, input.TextIndexes) {
					t.Fatalf("CreateCollection lost text declaration: got=%+v want=%+v", created.TextIndexes, input.TextIndexes)
				}
				if created.Options.DataRootStoragePolicy != collections.RootStorageFast || created.Options.IndexStateStoragePolicy != collections.RootStorageFast || !created.Options.DisableBufferedIndexedAsyncFlush {
					t.Fatalf("create options lost: %+v", created.Options)
				}
				local, err := mgr.OpenCollection(input.Name)
				if err != nil {
					t.Fatal(err)
				}
				want := local.Meta()
				if !reflect.DeepEqual(created, want) {
					t.Fatalf("create response differs from durable declaration: got=%+v want=%+v", created, want)
				}
				listed, err := client.ListCollections(ctx)
				if err != nil || len(listed) != 1 || !reflect.DeepEqual(listed[0], want) {
					t.Fatalf("ListCollections lost metadata: got=%+v err=%v", listed, err)
				}
				closeFirst()
				client, _, closeSecond := open()
				defer closeSecond()
				if err := client.Hello(ctx); err != nil {
					t.Fatal(err)
				}
				listed, err = client.ListCollections(ctx)
				if err != nil || len(listed) != 1 || !reflect.DeepEqual(listed[0], want) {
					t.Fatalf("reopen lost metadata: got=%+v want=%+v err=%v", listed, want, err)
				}
			})
		}
	}
}

func TestTextMetadataV7CanonicalCodecAndDeterministicCreate(t *testing.T) {
	meta := textMetadataV7Fixture(collections.DocumentFormatBSON, collections.TextIndexVersionV2)
	payload, err := collections.EncodeCatalogCreateCollectionCommandWALPayload(meta)
	if err != nil {
		t.Fatal(err)
	}
	want := append(binary.AppendUvarint(nil, 7), payload...)
	raw, err := encodeCollectionMeta(meta)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, want) {
		t.Fatalf("text metadata must reuse full canonical catalog payload: version=%d", raw[0])
	}
	decoded, err := decodeCollectionMeta(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded.TextIndexes, meta.TextIndexes) || decoded.Options.DocumentFormat != meta.Options.DocumentFormat || !decoded.Options.DisableBufferedIndexedAsyncFlush {
		t.Fatalf("full metadata lost: %+v", decoded)
	}
	again, err := encodeCollectionMeta(decoded)
	if err != nil || !bytes.Equal(again, raw) {
		t.Fatalf("nondeterministic metadata: %v", err)
	}
	sections := raftClusterCreateCollectionSectionsWithMeta(meta, 1, AckRaftCommitted)
	sections = append([]iwire.Section{{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandCreateCollection, Version: 1})}}, sections...)
	validated, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := iwire.AppendDeterministicEntry(nil, validated)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raftentry.DecodeCommandEntryV1(entry, raftentry.DecodeOptions{}); err != nil {
		t.Fatal(err)
	}
}

func serveTextMetadataV7CommandWALPipe(t *testing.T) (*Client, *Server, *collections.CollectionManager, *backenddb.DB) {
	t.Helper()
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), CommandWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mgr := collections.NewCollectionManager(db)
	server := NewServer(ServerOptions{Collections: mgr, Backend: db})
	client, _ := servePipe(t, server)
	return client, server, mgr, db
}

func TestTextMetadataV7InvalidDeclarationsHaveNoEffects(t *testing.T) {
	client, server, mgr, db := serveTextMetadataV7CommandWALPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*collections.CollectionMeta){
		func(m *collections.CollectionMeta) { m.TextIndexes[0].Version = "v99" },
		func(m *collections.CollectionMeta) { m.TextIndexes[0].Analyzer = "unknown" },
		func(m *collections.CollectionMeta) { m.TextIndexes[0].Rollout = collections.TextIndexRolloutShadow },
		func(m *collections.CollectionMeta) { m.TextIndexes[0].AnalyzerOptions.Stemmer = "porter" },
		func(m *collections.CollectionMeta) {
			m.TextIndexes[0].AnalyzerOptions.Synonyms = map[string][]string{"a": {"b"}}
		},
		func(m *collections.CollectionMeta) { m.TextIndexes[0].Fields[0].Weight = -1 },
		func(m *collections.CollectionMeta) { m.TextIndexes[0].StoragePolicy = "invalid" },
		func(m *collections.CollectionMeta) { m.TextIndexes[0].StorePositions = false },
		func(m *collections.CollectionMeta) { m.Options.BufferedIndexedWriteMaxBytes = -1 },
	} {
		meta := textMetadataV7Fixture(collections.DocumentFormatJSON, collections.TextIndexVersionV2)
		mutate(&meta)
		version, lsn := clientCatalogVersion(t, client, ctx), db.CommandWALNextLSN()
		if _, err := client.CreateCollection(ctx, meta); err == nil {
			t.Fatalf("invalid text declaration accepted: %+v", meta)
		}
		if db.CommandWALNextLSN() != lsn || clientCatalogVersion(t, client, ctx) != version {
			t.Fatal("invalid declaration crossed WAL/catalog boundary")
		}
		if _, err := mgr.OpenCollection(meta.Name); err == nil {
			t.Fatal("invalid declaration published")
		}
	}
	_ = server
}

func textMetadataV7Raw(t *testing.T, name string, disk []byte) []byte {
	t.Helper()
	payload, err := commitlog.EncodeCatalogCreateCollectionPayload(name, disk)
	if err != nil {
		t.Fatal(err)
	}
	return append(binary.AppendUvarint(nil, 7), payload...)
}

func TestTextMetadataV7HostilePayloadsHaveNoEffects(t *testing.T) {
	client, server, mgr, db := serveTextMetadataV7CommandWALPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatal(err)
	}
	meta := textMetadataV7Fixture(collections.DocumentFormatJSON, collections.TextIndexVersionV2)
	payload, err := collections.EncodeCatalogCreateCollectionCommandWALPayload(meta)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := commitlog.DecodeCatalogCreateCollectionPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	var disk map[string]any
	if err := json.Unmarshal(catalog.Metadata, &disk); err != nil {
		t.Fatal(err)
	}
	encodedDisk := func(change func(map[string]any)) []byte {
		var copy map[string]any
		if err := json.Unmarshal(catalog.Metadata, &copy); err != nil {
			t.Fatal(err)
		}
		change(copy)
		raw, err := json.Marshal(copy)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	valid := append(binary.AppendUvarint(nil, 7), payload...)
	cases := [][]byte{
		{7}, {8}, append([]byte(nil), valid[:len(valid)-1]...), append(append([]byte(nil), valid...), 0),
		textMetadataV7Raw(t, "other", catalog.Metadata),
		textMetadataV7Raw(t, meta.Name, append(append([]byte(nil), catalog.Metadata...), []byte(" {}")...)),
		textMetadataV7Raw(t, meta.Name, encodedDisk(func(m map[string]any) { m["unknown"] = true })),
		textMetadataV7Raw(t, meta.Name, encodedDisk(func(m map[string]any) { m["version"] = 999 })),
		textMetadataV7Raw(t, meta.Name, encodedDisk(func(m map[string]any) { m["options"].(map[string]any)["buffered_indexed_write_max_bytes"] = -1 })),
		textMetadataV7Raw(t, meta.Name, encodedDisk(func(m map[string]any) { m["text_indexes"].([]any)[0].(map[string]any)["version"] = "v99" })),
		textMetadataV7Raw(t, meta.Name, encodedDisk(func(m map[string]any) { m["text_indexes"].([]any)[0].(map[string]any)["unknown"] = true })),
		textMetadataV7Raw(t, meta.Name, bytes.Replace(catalog.Metadata, []byte(`"name":"articles"`), []byte(`"name":"articles","name":"articles"`), 1)),
		textMetadataV7Raw(t, meta.Name, []byte(strings.Repeat(" ", (1<<20)+1))),
	}
	for i, raw := range cases {
		version, lsn := clientCatalogVersion(t, client, ctx), db.CommandWALNextLSN()
		if _, err := decodeCollectionMeta(raw); err == nil {
			t.Fatalf("hostile payload %d decoded", i)
		}
		_, err := server.handleCreateCollection([]iwire.Section{
			{ID: iwire.SectionIdempotencyKey, Bytes: []byte(fmt.Sprintf("hostile-text-%d", i))},
			{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, version)},
			{ID: iwire.SectionCollectionMeta, Bytes: raw},
		})
		if err == nil {
			t.Fatalf("hostile payload %d accepted", i)
		}
		if db.CommandWALNextLSN() != lsn || clientCatalogVersion(t, client, ctx) != version {
			t.Fatalf("hostile payload %d crossed WAL/catalog boundary", i)
		}
		if _, err := mgr.OpenCollection(meta.Name); err == nil {
			t.Fatal("hostile create published collection")
		}
	}
}
