package nativewire

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func serveCollectionPipe(t *testing.T) (*Client, *collections.CollectionManager, *backenddb.DB) {
	t.Helper()
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	server := NewServer(ServerOptions{Collections: mgr, Backend: db})
	client, _ := servePipe(t, server)
	t.Cleanup(func() { _ = db.Close() })
	return client, mgr, db
}

func TestMetadataCommandsRoundTrip(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	meta, err := client.CreateCollection(ctx, collections.CollectionMeta{
		Name: "users",
		Options: collections.CollectionOptions{
			DocumentFormat:        collections.DocumentFormatJSON,
			DataRootStoragePolicy: collections.RootStorageCompressed,
		},
	})
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	if meta.Name != "users" || meta.Options.DataRootStoragePolicy != collections.RootStorageCompressed {
		t.Fatalf("created meta=%+v", meta)
	}

	handle, err := client.OpenCollection(ctx, "users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if handle == 0 {
		t.Fatal("zero collection handle")
	}
	if _, err := mgr.OpenCollection("users"); err != nil {
		t.Fatalf("direct OpenCollection: %v", err)
	}

	meta, err = client.CreateIndex(ctx, "users", collections.IndexDefinition{
		Name:      "email",
		Field:     "email",
		ValueType: collections.IndexValueString,
		Unique:    true,
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if len(meta.Indexes) != 1 || meta.Indexes[0].Name != "email" {
		t.Fatalf("indexes after create=%+v", meta.Indexes)
	}
	indexes, err := client.ListIndexes(ctx, "users")
	if err != nil {
		t.Fatalf("ListIndexes: %v", err)
	}
	if len(indexes) != 1 || indexes[0].Name != "email" || !indexes[0].Unique {
		t.Fatalf("indexes=%+v", indexes)
	}
	metas, err := client.ListCollections(ctx)
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	if len(metas) != 1 || metas[0].Name != "users" {
		t.Fatalf("collections=%+v", metas)
	}
	meta, err = client.DropIndex(ctx, "users", "email")
	if err != nil {
		t.Fatalf("DropIndex: %v", err)
	}
	if len(meta.Indexes) != 0 {
		t.Fatalf("indexes after drop=%+v", meta.Indexes)
	}
	if err := client.CloseCollection(ctx, handle); err != nil {
		t.Fatalf("CloseCollection: %v", err)
	}
}

func TestMetadataHandleRefWorksForListIndexes(t *testing.T) {
	client, _, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	handle, err := client.OpenCollection(ctx, "users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	sections, err := client.commandSections(ctx, iwire.CommandListIndexes, collectionHandleRef(handle))
	if err != nil {
		t.Fatalf("ListIndexes by handle: %v", err)
	}
	indexes, err := firstIndexVectorFromResponse(sections, client.limits)
	if err != nil {
		t.Fatalf("decode indexes: %v", err)
	}
	if len(indexes) != 0 {
		t.Fatalf("indexes=%+v want none", indexes)
	}
}

func TestDecodeCollectionMetaRejectsOversizedIndexCount(t *testing.T) {
	payload := testCollectionMetaPayload(0, 0, 0, maxCollectionMetaIndexDefinitions+1)
	if _, err := decodeCollectionMeta(payload); nativeCodeOf(err) != iwire.ErrResourceExhausted {
		t.Fatalf("decodeCollectionMeta err=%v code=%d want resource exhausted", err, nativeCodeOf(err))
	}
}

func TestDecodeCollectionMetaRejectsUnknownEnums(t *testing.T) {
	for _, tc := range []struct {
		name        string
		docFormat   uint64
		dataPolicy  uint64
		indexPolicy uint64
	}{
		{name: "document_format", docFormat: 99},
		{name: "data_root_storage", dataPolicy: 99},
		{name: "index_state_root_storage", indexPolicy: 99},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := testCollectionMetaPayload(tc.docFormat, tc.dataPolicy, tc.indexPolicy, 0)
			if _, err := decodeCollectionMeta(payload); nativeCodeOf(err) != iwire.ErrInvalidCommand {
				t.Fatalf("decodeCollectionMeta err=%v code=%d want invalid command", err, nativeCodeOf(err))
			}
		})
	}
}

func TestDropCollectionReserved(t *testing.T) {
	client, _, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _, err := client.roundTrip(ctx, iwire.FrameRequest, mustCommandBody(t, iwire.CommandDropCollection, collectionNameRef("users")), iwire.FrameResponse)
	if !isRemoteError(err, iwire.ErrUnsupportedFeature) {
		t.Fatalf("drop_collection error=%v want unsupported feature", err)
	}
}

func testCollectionMetaPayload(docFormat, dataPolicy, indexStatePolicy, indexCount uint64) []byte {
	dst := binary.AppendUvarint(nil, 1)
	dst = appendString(dst, "users")
	dst = binary.AppendUvarint(dst, docFormat)
	dst = binary.AppendUvarint(dst, dataPolicy)
	dst = binary.AppendUvarint(dst, indexStatePolicy)
	dst = appendBool(dst, false)
	dst = appendBool(dst, false)
	dst = appendBool(dst, false)
	dst = binary.AppendVarint(dst, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = appendBool(dst, false)
	dst = appendBool(dst, false)
	dst = binary.AppendVarint(dst, 0)
	return binary.AppendUvarint(dst, indexCount)
}

func nativeCodeOf(err error) iwire.ErrorCode {
	code, _ := iwire.ErrorCodeOf(err)
	return code
}

func mustCommandBody(t *testing.T, commandID iwire.CommandID, sections ...iwire.Section) []byte {
	t.Helper()
	body, err := appendCommandRequestBody(nil, commandID, sections...)
	if err != nil {
		t.Fatalf("append command body: %v", err)
	}
	return body
}
