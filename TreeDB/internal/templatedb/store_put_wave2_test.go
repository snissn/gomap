package templatedb

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/template"
)

type hookKV struct {
	*memKV

	failSetAt int
	writeErr  error
	closeErr  error

	getErrPrefix byte
	getErr       error
}

func (k *hookKV) Get(key []byte) ([]byte, error) {
	if k.getErr != nil && len(key) > 1 && key[1] == k.getErrPrefix {
		return nil, k.getErr
	}
	return k.memKV.Get(key)
}

func (k *hookKV) NewBatch() Batch {
	return &hookBatch{
		kv:        k,
		sets:      make(map[string][]byte),
		failSetAt: k.failSetAt,
		writeErr:  k.writeErr,
		closeErr:  k.closeErr,
	}
}

type hookBatch struct {
	kv   *hookKV
	sets map[string][]byte
	dels []string

	setCalls  int
	failSetAt int
	writeErr  error
	closeErr  error
}

func (b *hookBatch) Set(key, value []byte) error {
	b.setCalls++
	if b.failSetAt > 0 && b.setCalls == b.failSetAt {
		return errors.New("set failure")
	}
	b.sets[string(key)] = append([]byte(nil), value...)
	return nil
}

func (b *hookBatch) Delete(key []byte) error {
	b.dels = append(b.dels, string(key))
	return nil
}

func (b *hookBatch) WriteSync() error {
	if b.writeErr != nil {
		return b.writeErr
	}
	for _, k := range b.dels {
		delete(b.kv.data, k)
	}
	for k, v := range b.sets {
		b.kv.data[k] = append([]byte(nil), v...)
	}
	return nil
}

func (b *hookBatch) Close() error {
	return b.closeErr
}

func encodeAnchorDefForStoreTest(t *testing.T, anchors ...string) []byte {
	t.Helper()
	cfg := template.NormalizeConfig(template.Config{
		MinAnchorLen:          2,
		MaxAnchorLen:          64,
		MaxAnchorsPerTemplate: 8,
		MaxAnchorBytesTotal:   256,
	})
	out := make([][]byte, len(anchors))
	for i, a := range anchors {
		out[i] = []byte(a)
	}
	def, err := template.EncodeTemplateDef(template.TemplateDef{
		Kind:    template.TemplateAnchors,
		Anchors: out,
	}, cfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef: %v", err)
	}
	return def
}

func TestPutTemplateDefBatchErrorPaths(t *testing.T) {
	def := encodeAnchorDefForStoreTest(t, "aa", "bb")
	cases := []struct {
		name      string
		failSetAt int
		writeErr  error
		closeErr  error
	}{
		{name: "set_error", failSetAt: 1},
		{name: "write_error", writeErr: errors.New("write failure")},
		{name: "close_error", closeErr: errors.New("close failure")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kv := &hookKV{
				memKV:     newMemKV(),
				failSetAt: tc.failSetAt,
				writeErr:  tc.writeErr,
				closeErr:  tc.closeErr,
			}
			s := New(kv, Config{})
			if _, err := s.PutTemplateDef(context.Background(), def, []uint64{11, 11, 12}); err == nil {
				t.Fatalf("expected error for case %s", tc.name)
			}
		})
	}
}

func TestPutTemplateDefIdempotentRefreshRouteLists(t *testing.T) {
	kv := &hookKV{memKV: newMemKV()}
	s := New(kv, Config{
		MaxCandidatesPerFP:    8,
		MaxCandidateListBytes: 1 << 10,
	})
	def := encodeAnchorDefForStoreTest(t, "aa", "bb", "cc")

	id1, err := s.PutTemplateDef(context.Background(), def, []uint64{5})
	if err != nil {
		t.Fatalf("first PutTemplateDef: %v", err)
	}
	id2, err := s.PutTemplateDef(context.Background(), def, []uint64{9, 9})
	if err != nil {
		t.Fatalf("second PutTemplateDef: %v", err)
	}
	if id1 != id2 {
		t.Fatalf("expected idempotent publish id match: %d vs %d", id1, id2)
	}

	c5, err := s.GetCandidates(context.Background(), 5, 0)
	if err != nil {
		t.Fatalf("GetCandidates(fp=5): %v", err)
	}
	if len(c5) != 1 || c5[0].ID != id1 {
		t.Fatalf("unexpected fp=5 candidates: %+v", c5)
	}
	c9, err := s.GetCandidates(context.Background(), 9, 0)
	if err != nil {
		t.Fatalf("GetCandidates(fp=9): %v", err)
	}
	if len(c9) != 1 || c9[0].ID != id1 {
		t.Fatalf("unexpected fp=9 candidates: %+v", c9)
	}
}

func TestPutTemplateDefsAdditionalBranches(t *testing.T) {
	def := encodeAnchorDefForStoreTest(t, "aa", "bb")

	// Duplicate defs in one batch should assign the same deterministic ID.
	kv := &hookKV{memKV: newMemKV()}
	s := New(kv, Config{
		MaxCandidatesPerFP:    8,
		MaxCandidateListBytes: 1 << 10,
	})
	ids, err := s.PutTemplateDefs(context.Background(), []template.PublishSpec{
		{DefBytes: def, RouteFPs: []uint64{3, 3, 2}},
		{DefBytes: def, RouteFPs: []uint64{2}},
	})
	if err != nil {
		t.Fatalf("PutTemplateDefs(duplicate): %v", err)
	}
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Fatalf("expected identical ids for duplicate defs, got %v", ids)
	}

	// Get error while loading existing fp list.
	kvErrGet := &hookKV{
		memKV:        newMemKV(),
		getErrPrefix: 'f',
		getErr:       errors.New("fp get failure"),
	}
	sErrGet := New(kvErrGet, Config{})
	if _, err := sErrGet.PutTemplateDefs(context.Background(), []template.PublishSpec{
		{DefBytes: def, RouteFPs: []uint64{1}},
	}); err == nil {
		t.Fatalf("expected fp get error")
	}

	// Batch write and close errors.
	kvWriteErr := &hookKV{memKV: newMemKV(), writeErr: errors.New("write failure")}
	sWriteErr := New(kvWriteErr, Config{})
	if _, err := sWriteErr.PutTemplateDefs(context.Background(), []template.PublishSpec{
		{DefBytes: def, RouteFPs: []uint64{1}},
	}); err == nil {
		t.Fatalf("expected batch write error")
	}

	kvCloseErr := &hookKV{memKV: newMemKV(), closeErr: errors.New("close failure")}
	sCloseErr := New(kvCloseErr, Config{})
	if _, err := sCloseErr.PutTemplateDefs(context.Background(), []template.PublishSpec{
		{DefBytes: def, RouteFPs: []uint64{1}},
	}); err == nil {
		t.Fatalf("expected batch close error")
	}
}
