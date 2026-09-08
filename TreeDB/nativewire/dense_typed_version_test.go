package nativewire

import (
	"bytes"
	"context"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/documentservice"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func TestDenseTypedVersionCapability(t *testing.T) {
	registry := iwire.MustV1Registry()
	for _, version := range []uint64{1, 2} {
		schema, ok := registry.LookupCommand(iwire.CommandDenseVectorSearch, version)
		if !ok || !schema.LocalOnly || schema.Replicated || schema.Kind != iwire.CommandKindRead {
			t.Fatalf("dense version %d must be registered as a local read: %+v", version, schema)
		}
		if _, err := iwire.AppendDeterministicEntry(nil, iwire.ValidatedCommand{Schema: schema}); err == nil {
			t.Fatalf("dense version %d admitted to deterministic entry", version)
		}
	}
	server := NewServer(ServerOptions{})
	defer server.Close()
	var out bytes.Buffer
	if err := server.writeHelloOK(&out, iwire.Header{}, nil); err != nil {
		t.Fatal(err)
	}
	// A registry entry alone is not an executable document-service capability.
	if bytes.Contains(out.Bytes(), []byte("dense_vector_search_versions")) {
		t.Fatal("server without a document service advertised dense search")
	}
}

func TestDenseTypedVersionGoldenAndCrossVersionRejection(t *testing.T) {
	// One result a / {} / score 1; v2 changes only the route tag.
	fixture, err := os.ReadFile("testdata/dense_v2_response_meta.hex")
	if err != nil {
		t.Fatal(err)
	}
	meta, err := hex.DecodeString(strings.TrimSpace(string(fixture)))
	if err != nil {
		t.Fatal(err)
	}
	ids := []byte{1, 1, 'a'}
	docs := []byte{1, 2, '{', '}'}
	out, _, _, _, err := decodeVersionedDenseVectorSearchResponse(2, ids, docs, meta, 1, iwire.DefaultLimits(), nil, nil, nil)
	if err != nil || !out.TypedColumnGraph || out.NativeBasePlusLiveDelta || len(out.Results) != 1 || out.Results[0].Score != 1 {
		t.Fatalf("typed golden: %+v %v", out, err)
	}
	for _, version := range []uint64{1, 2, 3} {
		for _, tag := range []byte{0, 1, 2, 3} {
			candidate := bytes.Clone(meta)
			candidate[0] = tag
			_, _, _, _, err := decodeVersionedDenseVectorSearchResponse(version, ids, docs, candidate, 1, iwire.DefaultLimits(), nil, nil, nil)
			want := (version == 1 && tag == 1) || (version == 2 && tag == 2)
			if (err == nil) != want {
				t.Fatalf("version=%d tag=%d err=%v", version, tag, err)
			}
		}
	}
	encoded := appendDenseVectorSearchResponse(nil, documentservice.RawDenseVectorSearchResponse{
		NativeBasePlusLiveDelta: true, Candidates: 1,
		Results: []documentservice.RawDenseVectorResult{{Score: 1}},
	})
	legacy := bytes.Clone(meta)
	legacy[0] = 1
	if !bytes.Equal(encoded, legacy) {
		t.Fatalf("legacy bytes changed: %x", encoded)
	}
}

func TestDenseTypedRequiresNegotiationBeforeSend(t *testing.T) {
	client := NewClient(nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := client.DenseVectorSearch(ctx, DenseVectorSearchRequest{TypedColumnGraph: true, TopK: 1, Query: []float32{1}}); err == nil {
		t.Fatal("unnegotiated typed request accepted")
	}
}

func TestDenseTypedResponseReusedDecodeAllocations(t *testing.T) {
	meta, _ := hex.DecodeString("0201000001000000000000f03f")
	idsRaw, docsRaw := []byte{1, 1, 'a'}, []byte{1, 2, '{', '}'}
	ids, docs := make([][]byte, 0, 1), make([][]byte, 0, 1)
	results := make([]DenseVectorSearchResult, 0, 1)
	var err error
	allocs := testing.AllocsPerRun(100, func() {
		_, ids, docs, results, err = decodeVersionedDenseVectorSearchResponse(2, idsRaw, docsRaw, meta, 1, iwire.DefaultLimits(), ids, docs, results)
	})
	if err != nil || allocs != 0 {
		t.Fatalf("reused typed decode: allocs=%g err=%v", allocs, err)
	}
}
