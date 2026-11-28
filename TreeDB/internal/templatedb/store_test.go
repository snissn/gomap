package templatedb

import (
	"bytes"
	"context"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/template"
)

type memKV struct {
	data map[string][]byte
}

type memBatch struct {
	kv   *memKV
	sets map[string][]byte
	dels []string
}

func newMemKV() *memKV {
	return &memKV{data: make(map[string][]byte)}
}

func (m *memKV) Get(key []byte) ([]byte, error) {
	if v, ok := m.data[string(key)]; ok {
		cp := append([]byte(nil), v...)
		return cp, nil
	}
	return nil, nil
}

func (m *memKV) SetSync(key, value []byte) error {
	m.data[string(key)] = append([]byte(nil), value...)
	return nil
}

func (m *memKV) DeleteSync(key []byte) error {
	delete(m.data, string(key))
	return nil
}

func (m *memKV) NewBatch() Batch {
	return &memBatch{kv: m, sets: make(map[string][]byte)}
}

func (b *memBatch) Set(key, value []byte) error {
	b.sets[string(key)] = append([]byte(nil), value...)
	return nil
}

func (b *memBatch) Delete(key []byte) error {
	b.dels = append(b.dels, string(key))
	return nil
}

func (b *memBatch) WriteSync() error {
	for _, k := range b.dels {
		delete(b.kv.data, k)
	}
	for k, v := range b.sets {
		b.kv.data[k] = append([]byte(nil), v...)
	}
	return nil
}

func (b *memBatch) Close() error { return nil }

func TestCandidateListEvictionDeterminism(t *testing.T) {
	kv := newMemKV()
	store := New(kv, Config{MaxCandidatesPerFP: 2, MaxCandidateListBytes: 1024})
	cfg := template.Config{MinAnchorLen: 2, MaxAnchorLen: 64, MaxAnchorsPerTemplate: 4, MaxAnchorBytesTotal: 128}
	def1, err := template.EncodeTemplateDef(template.TemplateDef{Anchors: [][]byte{[]byte("aa"), []byte("bb")}}, cfg)
	if err != nil {
		t.Fatalf("encode def1: %v", err)
	}
	def2, err := template.EncodeTemplateDef(template.TemplateDef{Anchors: [][]byte{[]byte("aa"), []byte("bb"), []byte("cc")}}, cfg)
	if err != nil {
		t.Fatalf("encode def2: %v", err)
	}
	def3, err := template.EncodeTemplateDef(template.TemplateDef{Anchors: [][]byte{[]byte("aa")}}, cfg)
	if err != nil {
		t.Fatalf("encode def3: %v", err)
	}
	if _, err := store.PutTemplateDef(context.Background(), def1, []uint64{1}); err != nil {
		t.Fatalf("put def1: %v", err)
	}
	if _, err := store.PutTemplateDef(context.Background(), def2, []uint64{1}); err != nil {
		t.Fatalf("put def2: %v", err)
	}
	if _, err := store.PutTemplateDef(context.Background(), def3, []uint64{1}); err != nil {
		t.Fatalf("put def3: %v", err)
	}
	cands, err := store.GetCandidates(context.Background(), 1, 0)
	if err != nil {
		t.Fatalf("get candidates: %v", err)
	}
	if len(cands) != 2 {
		t.Fatalf("expected 2 candidates, got %d", len(cands))
	}
	if cands[0].Size > cands[1].Size {
		t.Fatalf("candidates not ordered by size")
	}
}

func TestBackendSystemRoot_InlineChunkedAndPointerRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	store := NewBackend(backend, Config{MaxCandidatesPerFP: 4, MaxCandidateListBytes: 4096})

	// 1) Small inline template definition (<= 2 KiB).
	inlineDef := bytes.Repeat([]byte("inline-template-anchor-"), 12)
	inlineID, err := store.PutTemplateDef(ctx, inlineDef, []uint64{101, 202})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("PutTemplateDef(inline): %v", err)
	}

	// 2) Multi-chunk inline template definition (> 2 KiB).
	chunkedDef := bytes.Repeat([]byte("chunked-template-anchor-payload-"), 256) // 8 KiB
	chunkedID, err := store.PutTemplateDef(ctx, chunkedDef, []uint64{101, 303})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("PutTemplateDef(chunked): %v", err)
	}

	// 3) Optional raw lane-254 value-log pointer template definition.
	ptrStore := NewBackend(backend, Config{MaxCandidatesPerFP: 4, MaxCandidateListBytes: 4096})
	ptrStore.SetPointerThreshold(1024)
	ptrDef := bytes.Repeat([]byte("pointer-template-anchor-payload-"), 256) // 8 KiB
	ptrID, err := ptrStore.PutTemplateDef(ctx, ptrDef, []uint64{404})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("PutTemplateDef(pointer): %v", err)
	}
	entry, ok, err := backend.GetSystemEntry(backenddb.SystemTemplateDefKey(ptrID))
	if err != nil || !ok {
		_ = backend.Close()
		t.Fatalf("GetSystemEntry(pointer): ok=%v err=%v", ok, err)
	}
	if entry.Flags&node.FlagPointer == 0 {
		_ = backend.Close()
		t.Fatalf("expected pointer-backed template def to set FlagPointer, got flags=%#x", entry.Flags)
	}

	// Verify ValueLogGC retains the raw system pointer segment.
	if _, err := backend.ValueLogGC(ctx, backenddb.ValueLogGCOptions{}); err != nil {
		_ = backend.Close()
		t.Fatalf("ValueLogGC: %v", err)
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen read-only and verify all 3 encodings + FP routing lists survive.
	ro, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		ReadOnly:               true,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open(ReadOnly): %v", err)
	}
	defer func() { _ = ro.Close() }()

	roStore := NewBackend(ro, Config{})
	for _, tc := range []struct {
		name string
		id   uint64
		want []byte
	}{
		{"inline", inlineID, inlineDef},
		{"chunked", chunkedID, chunkedDef},
		{"pointer", ptrID, ptrDef},
	} {
		gotStore, err := roStore.GetTemplateDef(ctx, tc.id)
		if err != nil || !bytes.Equal(gotStore, tc.want) {
			t.Fatalf("%s roStore.GetTemplateDef: len=%d want=%d err=%v", tc.name, len(gotStore), len(tc.want), err)
		}
		gotLookup, err := ro.LookupSystemTemplateDef(tc.id)
		if err != nil || !bytes.Equal(gotLookup, tc.want) {
			t.Fatalf("%s ro.LookupSystemTemplateDef: len=%d want=%d err=%v", tc.name, len(gotLookup), len(tc.want), err)
		}
	}

	cands, err := roStore.GetCandidates(ctx, 101, 0)
	if err != nil {
		t.Fatalf("roStore.GetCandidates(101): %v", err)
	}
	if len(cands) != 2 || cands[0].ID != inlineID || cands[1].ID != chunkedID {
		t.Fatalf("roStore.GetCandidates(101)=%+v want [%d %d]", cands, inlineID, chunkedID)
	}
	candsPtr, err := roStore.GetCandidates(ctx, 404, 0)
	if err != nil || len(candsPtr) != 1 || candsPtr[0].ID != ptrID {
		t.Fatalf("roStore.GetCandidates(404)=%+v err=%v want [%d]", candsPtr, err, ptrID)
	}
}
