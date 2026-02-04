package templatedb

import (
	"context"
	"testing"

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
