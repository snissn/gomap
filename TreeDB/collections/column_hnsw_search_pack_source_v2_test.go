package collections

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

func testSourceV2PackInput(t *testing.T) columnHNSWSearchPackBuildInput {
	t.Helper()
	input := testColumnHNSWSearchPackInput2312()
	input.SourceV2 = true
	input.BaseIdentity = columnHNSWSearchPackBaseIdentity{}
	input.RowRefGenerations, input.RowRefPartIDs, input.RowRefRowIndexes, input.RowRefAppliedCommandLSN = nil, nil, nil, nil
	input.M, input.EfConstruction = columnVamanaConnectivityPreservingPartitionM, columnVamanaConnectivityPreservingPartitionL
	input.MaxLayer = 0
	input.Levels = []uint16{0, 0, 0}
	input.AdjacencyLayers = []columnHNSWSearchPackLayerInput{{Offsets: []uint64{0, 1, 2, 3}, Neighbors: []uint32{1, 2, 0}}}
	input.MembershipDigest[0] = 1
	input.ConnectivityPreservingPartitionVamana = true
	return input
}

func TestColumnSearchPackSourceV2Binding(t *testing.T) {
	input := testSourceV2PackInput(t)
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		t.Fatal(err)
	}
	opts := columnHNSWSearchPackDecodeOptions{SourceV2: true, ExpectedMembershipDigest: input.MembershipDigest}
	pack, err := decodeColumnHNSWSearchPack(raw, opts)
	if err != nil {
		t.Fatal(err)
	}
	if pack.Header.Version != columnHNSWSearchPackVersionV7 || len(pack.Sections) != 6 || len(pack.RowRefGenerations) != 0 || pack.Header.BaseManifestGeneration != 0 {
		t.Fatalf("pack binding=%+v", pack.Header)
	}
	for _, section := range pack.Sections {
		if section.Kind >= columnHNSWSearchPackSectionRowRefGeneration && section.Kind <= columnHNSWSearchPackSectionRowRefAppliedLSN {
			t.Fatal("legacy section")
		}
	}
	if _, err := decodeColumnHNSWSearchPack(raw, columnHNSWSearchPackDecodeOptions{}); err == nil {
		t.Fatal("legacy decoder accepted V2")
	}
	if _, _, err := decodeColumnHNSWSearchPackEnvelopeWithContext(context.Background(), raw, columnHNSWSearchPackDecodeOptions{}); err == nil {
		t.Fatal("legacy envelope accepted V2")
	}
	if _, err := splitVectorPartitionDomainSearchPackV1(raw, 0, 1024); err == nil {
		t.Fatal("legacy splitter accepted V2")
	}
	chunks, err := splitVectorPartitionDomainSearchPackBinding(raw, 0, 1024, opts)
	if err != nil || len(chunks) != 7 {
		t.Fatalf("split chunks=%d err=%v", len(chunks), err)
	}
	legacy, err := encodeColumnHNSWSearchPack(testColumnHNSWSearchPackInput2312())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeColumnHNSWSearchPack(legacy, opts); err == nil {
		t.Fatal("V2 decoder accepted legacy")
	}
	for name, mutate := range map[string]func([]byte){
		"base":       func(b []byte) { putHNSWPackU64(b, columnHNSWSearchPackHeaderBaseGenerationOffset, 1) },
		"membership": func(b []byte) { b[columnHNSWSearchPackHeaderMembershipDigestOffset] ^= 1 },
		"profile":    func(b []byte) { putHNSWPackU32(b, columnHNSWSearchPackHeaderMOffset, 1) },
		"payload":    func(b []byte) { b[len(b)-1] ^= 1 },
	} {
		t.Run(name, func(t *testing.T) {
			bad := bytes.Clone(raw)
			mutate(bad)
			if _, err := decodeColumnHNSWSearchPack(bad, opts); err == nil {
				t.Fatal("accepted corrupt/mixed binding")
			}
		})
	}
	for name, mutate := range map[string]func(*columnHNSWSearchPackBuildInput){
		"base":    func(i *columnHNSWSearchPackBuildInput) { i.BaseIdentity.ManifestGeneration = 1 },
		"row ref": func(i *columnHNSWSearchPackBuildInput) { i.RowRefPartIDs = []int64{1} },
		"profile": func(i *columnHNSWSearchPackBuildInput) { i.ConnectivityPreservingPartitionVamana = false },
	} {
		t.Run(name, func(t *testing.T) {
			bad := input
			mutate(&bad)
			if _, err := encodeColumnHNSWSearchPack(bad); err == nil {
				t.Fatal("accepted mixed input")
			}
		})
	}
	rows := []columnVectorGraphAssetRow{{Vector: []float32{1, 0, 0}, InvNorm: 1}, {Vector: []float32{0, 2, 0}, InvNorm: .5}, {Vector: []float32{0, 0, 4}, InvNorm: .25}}
	input.NormalizedVectors = nil
	rowRaw, err := encodeColumnHNSWSearchPackRows(input, rows)
	if err != nil || !bytes.Equal(rowRaw, raw) {
		t.Fatalf("row encoder parity: %v", err)
	}
	f, err := os.Create(filepath.Join(t.TempDir(), "pack"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	_, err = writeColumnHNSWSearchPackRowsWithBackpatch(f, func(p []byte) error { _, err := f.WriteAt(p, 0); return err }, input, rows)
	if err != nil {
		t.Fatal(err)
	}
	streamRaw, err := os.ReadFile(f.Name())
	if err != nil || !bytes.Equal(streamRaw, raw) {
		t.Fatalf("stream encoder parity: %v", err)
	}
}

// Reopen each physical section from disk into a fresh resource manager. The V2
// binding must survive the same prepared-view path used by local graph readers.
func TestColumnSearchPackSourceV2PreparedSectionReopen(t *testing.T) {
	input := testSourceV2PackInput(t)
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		t.Fatal(err)
	}
	opts := columnHNSWSearchPackDecodeOptions{SourceV2: true, ExpectedMembershipDigest: input.MembershipDigest}
	payloads, err := splitVectorPartitionDomainSearchPackBinding(raw, 7, 1024, opts)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for i, payload := range payloads {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprint(i)), payload.payload, 0600); err != nil {
			t.Fatal(err)
		}
	}
	for reopen := 0; reopen < 2; reopen++ {
		manager := mappedresource.NewManager()
		scope := mappedresource.Scope{Kind: mappedresource.ScopePreparedSearch, ID: "source-v2", Namespace: "test", Generation: 11}
		acquire := func(i int, payload []byte) *mappedresource.Handle {
			t.Helper()
			checksum, err := columnHNSWSearchPackChecksumWithContext(t.Context(), payload)
			if err != nil {
				t.Fatal(err)
			}
			h, err := manager.AcquireBytes(mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: "test", Kind: "source-v2", Generation: 11, PartID: 8, FileID: uint32(i + 1), Length: int64(len(payload)), Checksum: uint64(checksum)}, scope, mappedresource.SourceHeapCopy, payload, mappedresource.AcquireOptions{Reason: "V2 prepared reopen"})
			if err != nil {
				t.Fatal(err)
			}
			return h
		}
		full := acquire(100, raw)
		monolithic, err := newColumnHNSWSearchPackPreparedViewFromHandle(manager, full, opts)
		if err != nil {
			_ = full.Release()
			t.Fatal(err)
		}
		if err := monolithic.validateLive(); err != nil {
			t.Fatal(err)
		}
		if err := monolithic.Close(); err != nil {
			t.Fatal(err)
		}
		var root *mappedresource.Handle
		sections := make(map[columnHNSWSearchPackSectionKey][]*mappedresource.Handle)
		for i, payload := range payloads {
			b, err := os.ReadFile(filepath.Join(dir, fmt.Sprint(i)))
			if err != nil {
				t.Fatal(err)
			}
			h := acquire(i, b)
			if i == 0 {
				root = h
				continue
			}
			key, chunk, err := parseVectorPartitionLocalSectionChunkAssetIDV1(7, payload.id)
			if err != nil || chunk != uint32(len(sections[key])) {
				t.Fatalf("section: %v", err)
			}
			sections[key] = append(sections[key], h)
		}
		if _, err := newColumnHNSWSearchPackPreparedViewFromSectionHandlesWithContext(t.Context(), manager, root, sections, columnHNSWSearchPackDecodeOptions{}); err == nil {
			t.Fatal("legacy prepared reader accepted V2")
		}
		key := columnHNSWSearchPackSectionKey{kind: columnHNSWSearchPackSectionNormalizedVectors}
		saved := sections[key]
		delete(sections, key)
		if _, err := newColumnHNSWSearchPackPreparedViewFromSectionHandlesWithContext(t.Context(), manager, root, sections, opts); err == nil {
			t.Fatal("missing section accepted")
		}
		corrupt := bytes.Clone(saved[0].Bytes())
		corrupt[0] ^= 1
		bad := acquire(101, corrupt)
		sections[key] = []*mappedresource.Handle{bad}
		if _, err := newColumnHNSWSearchPackPreparedViewFromSectionHandlesWithContext(t.Context(), manager, root, sections, opts); err == nil {
			t.Fatal("corrupt section accepted with valid physical checksum")
		}
		if err := bad.Release(); err != nil {
			t.Fatal(err)
		}
		sections[key] = saved
		view, err := newColumnHNSWSearchPackPreparedViewFromSectionHandlesWithContext(t.Context(), manager, root, sections, opts)
		if err != nil {
			t.Fatal(err)
		}
		if err := view.validateLive(); err != nil {
			t.Fatal(err)
		}
		if view.Header.Version != columnHNSWSearchPackVersionV7 || view.rowRefGenerationChunks.length != 0 || view.normalizedVectorChunks.length != uint64(input.Rows*input.VectorStride) {
			t.Fatalf("prepared V2 shape: %+v", view.Header)
		}
		if err := view.Close(); err != nil {
			t.Fatal(err)
		}
		if stats := manager.Stats(); stats.ActiveHandles != 0 {
			t.Fatalf("leaked handles: %+v", stats)
		}
	}
}
