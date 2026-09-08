package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"github.com/snissn/gomap/TreeDB/documentservice"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"os"
	"strings"
	"testing"
	"time"
)

func TestTypedUpsertGoldenBoundsAndOwnership(t *testing.T) {
	fixture, err := os.ReadFile("testdata/typed_upsert_v1.hex")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := hex.DecodeString(strings.TrimSpace(string(fixture)))
	if err != nil {
		t.Fatal(err)
	}
	ids := []byte{1, 1, 'a'}
	residual := []byte{1, 10, '{', '"', 'i', 'd', '"', ':', '"', 'a', '"', '}'}
	index, req, err := decodeTypedUpsert(raw, ids, residual, iwire.DefaultLimits())
	if err != nil || index != "a" || req.ExpectedGeneration != 1 || len(req.Columns) != 2 || req.Columns[1].Strings[0] != "text" || req.Columns[0].Float32Vectors[0][0] != 1 {
		t.Fatalf("golden=%s %+v %v", index, req, err)
	}
	vector := req.Columns[0].Float32Vectors[0]
	if cap(vector) != len(vector) {
		t.Fatal("uncapped vector")
	}
	for i := 0; i < len(raw); i++ {
		if _, _, err := decodeTypedUpsert(raw[:i], ids, residual, iwire.DefaultLimits()); err == nil {
			t.Fatalf("accepted prefix %d", i)
		}
	}
	for _, bad := range [][]byte{append(bytes.Clone(raw), 0), append([]byte{1, 'a', 0}, raw[3:]...), append([]byte{1, 'a', 1, 127}, raw[4:]...)} {
		if _, _, err := decodeTypedUpsert(bad, ids, residual, iwire.DefaultLimits()); err == nil {
			t.Fatalf("accepted malformed %x", bad)
		}
	}
	if _, _, err := decodeTypedUpsert(raw, []byte{0}, residual, iwire.DefaultLimits()); err == nil {
		t.Fatal("ID shape accepted")
	}
	clear(raw)
	if vector[0] != 1 || req.Columns[1].Strings[0] != "text" {
		t.Fatal("declared carriers borrow mutable frame")
	}
}

func TestTypedUpsertUnconfiguredAndDeadline(t *testing.T) {
	server := NewServer(ServerOptions{})
	defer server.Close()
	if _, err := server.handleTypedDocumentUpsert(context.Background(), nil); err == nil {
		t.Fatal("unconfigured mutation accepted")
	}
	var output bytes.Buffer
	if err := server.writeHelloOK(&output, iwire.Header{}, nil); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(output.Bytes(), []byte("typed_document_upsert_versions")) {
		t.Fatal("unconfigured capability advertised")
	}
	configured := NewServer(ServerOptions{DocumentService: &documentservice.Service{}})
	defer configured.Close()
	if _, err := configured.handleTypedDocumentUpsert(context.Background(), []iwire.Section{{ID: iwire.SectionDeadline, Bytes: binary.AppendUvarint(nil, uint64(time.Now().Add(-time.Second).UnixNano()))}}); err != context.DeadlineExceeded {
		t.Fatalf("expired before decode: %v", err)
	}
}

func BenchmarkTypedUpsertDecode(b *testing.B) {
	raw, _ := hex.DecodeString("01610101020000803f000000000107636f6e74656e740474657874")
	ids, residual := []byte{1, 1, 'a'}, []byte{1, 2, '{', '}'}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, _, err := decodeTypedUpsert(raw, ids, residual, iwire.DefaultLimits()); err != nil {
			b.Fatal(err)
		}
	}
}

func TestTypedUpsertRegisteredLocalMutation(t *testing.T) {
	schema, ok := iwire.MustV1Registry().LookupCommand(65, 1)
	if !ok || schema.Name != "typed_document_upsert" || schema.Kind != iwire.CommandKindMutation || !schema.LocalOnly || schema.Replicated {
		t.Fatalf("missing local typed mutation: %+v", schema)
	}
	if _, err := iwire.AppendDeterministicEntry(nil, iwire.ValidatedCommand{Schema: schema}); err == nil {
		t.Fatal("local typed mutation replicated")
	}
	registry := iwire.MustV1Registry()
	sections := []iwire.Section{{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: 65, Version: 1})}, {ID: iwire.SectionDeadline}, {ID: iwire.SectionDocumentIDs}, {ID: iwire.SectionDocuments}, {ID: iwire.SectionTypedUpsertRequest}}
	if _, err := registry.ValidateRequestSections(sections); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(sections); i++ {
		missing := append(append([]iwire.Section(nil), sections[:i]...), sections[i+1:]...)
		if _, err := registry.ValidateRequestSections(missing); err == nil {
			t.Fatalf("missing section %d accepted", i)
		}
	}
	for _, header := range []iwire.CommandHeader{{ID: 65, Version: 2}, {ID: 65, Version: 1, Flags: 1}} {
		sections[0].Bytes = iwire.AppendCommandHeader(nil, header)
		if _, err := registry.ValidateRequestSections(sections); err == nil {
			t.Fatalf("invalid header %+v", header)
		}
	}
}
