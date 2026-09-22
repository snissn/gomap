package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/documentservice"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func typedGetManyFixture(t testing.TB) (*Server, []iwire.Section) {
	t.Helper()
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	mgr := collections.NewCollectionManager(db)
	svc := documentservice.New(mgr)
	server := NewServer(ServerOptions{Collections: mgr, Backend: db, DocumentService: svc})
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
		if err := svc.Close(); err != nil {
			t.Error(err)
		}
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	info, err := svc.CreateIndex(context.Background(), documentservice.CreateIndexRequest{Name: "batch", Dimension: 2, TypedInput: true, VectorIndexOptions: &documentservice.BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph}})
	if err != nil {
		t.Fatal(err)
	}
	docs := make([]documentservice.Document, 32)
	for i := range docs {
		docs[i] = documentservice.Document{ID: fmt.Sprint(i), Content: "owned", Embedding: []float32{1, 0}}
	}
	if _, err := svc.UpsertDocuments(context.Background(), info.Name, documentservice.UpsertDocumentsRequest{Documents: docs}); err != nil {
		t.Fatal(err)
	}
	return server, []iwire.Section{{ID: iwire.SectionCollectionRef, Bytes: []byte{1, 'b', 'a', 't', 'c', 'h'}}, {ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("0"), []byte("missing"), []byte("0"), []byte("1"))}, {ID: iwire.SectionExpectedGeneration, Bytes: binary.AppendUvarint(nil, info.Generation)}, {ID: iwire.SectionDeadline, Bytes: binary.AppendUvarint(nil, uint64(time.Now().Add(time.Minute).UnixNano()))}}
}

func TestTypedGetManyEncodingAndFailures(t *testing.T) {
	server, sections := typedGetManyFixture(t)
	v1, err := server.handleGetManyBody(&connState{}, sections[:2], nil, ReadMetadata{})
	if err != nil {
		t.Fatal(err)
	}
	v2, err := server.handleTypedGetManyBody(context.Background(), &connState{}, sections, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v1, v2) {
		t.Fatalf("response encoding changed: v1=%x v2=%x", v1, v2)
	}
	for _, name := range []string{"generation", "trailing", "deadline", "handle"} {
		t.Run(name, func(t *testing.T) {
			bad := append([]iwire.Section(nil), sections...)
			switch name {
			case "generation":
				bad[2].Bytes = []byte{0}
			case "trailing":
				bad[2].Bytes = []byte{1, 0}
			case "deadline":
				bad[3].Bytes = []byte{1}
			case "handle":
				bad[0].Bytes = []byte{2, 1}
			}
			if _, err := server.handleTypedGetManyBody(context.Background(), &connState{}, bad, nil); err == nil {
				t.Fatal("invalid read accepted")
			}
		})
	}
	old := server.limits.MaxByteVectorBytes
	server.limits.MaxByteVectorBytes = 32
	if _, err := server.handleTypedGetManyBody(context.Background(), &connState{}, sections, nil); err == nil {
		t.Fatal("oversized response accepted")
	}
	server.limits.MaxByteVectorBytes = old
	if _, err := server.handleTypedGetManyBody(context.Background(), &connState{}, sections, nil); err != nil {
		t.Fatalf("healthy retry after rejection: %v", err)
	}
}

func BenchmarkTypedGetManyVersionBoundary(b *testing.B) {
	for _, version := range []int{1, 2} {
		b.Run(fmt.Sprint(version), func(b *testing.B) {
			server, sections := typedGetManyFixture(b)
			state := &connState{}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				if version == 1 {
					_, err = server.handleGetManyBody(state, sections[:2], nil, ReadMetadata{})
				} else {
					_, err = server.handleTypedGetManyBody(context.Background(), state, sections, nil)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestTypedGetManyRegistered(t *testing.T) {
	if got := iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandGetMany, Version: 2}); !bytes.Equal(got, []byte{50, 2, 0}) {
		t.Fatalf("command header golden: %x", got)
	}
	schema, ok := iwire.MustV1Registry().LookupCommand(iwire.CommandGetMany, 2)
	if !ok || !schema.LocalOnly || schema.Kind != iwire.CommandKindRead {
		t.Fatalf("missing selected get many: %+v", schema)
	}
	sections := []iwire.Section{
		{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandGetMany, Version: 2})},
		{ID: iwire.SectionCollectionRef, Bytes: []byte{1, 'x'}},
		{ID: iwire.SectionDocumentIDs, Bytes: []byte{0}},
		{ID: iwire.SectionExpectedGeneration, Bytes: []byte{1}},
		{ID: iwire.SectionDeadline, Bytes: []byte{1}},
	}
	registry := iwire.MustV1Registry()
	if _, err := registry.ValidateRequestSections(sections); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(sections); i++ {
		missing := append([]iwire.Section(nil), sections[:i]...)
		missing = append(missing, sections[i+1:]...)
		if _, err := registry.ValidateRequestSections(missing); err == nil {
			t.Fatalf("missing required section %d accepted", sections[i].ID)
		}
	}
}

func TestGetManyV1BufferedVisibility(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	_, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "buffered", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON, BufferedIndexedWrites: true, DisableBufferedIndexedAsyncFlush: true}, Indexes: []collections.IndexDefinition{{Name: "name", Field: "name", ValueType: collections.IndexValueString}}})
	if err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("buffered")
	if err != nil {
		t.Fatal(err)
	}
	doc := []byte(`{"name":"unflushed"}`)
	if _, err := col.InsertBatch([][]byte{[]byte("a")}, [][]byte{doc}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatal(err)
	}
	docs, present, err := client.GetMany(ctx, "buffered", [][]byte{[]byte("a")})
	if err != nil || len(present) != 1 || !present[0] || !bytes.Equal(docs[0], doc) {
		t.Fatalf("buffered visibility: docs=%q present=%v err=%v", docs, present, err)
	}
}
