package templatedb

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/template"
)

type closeableMemKV struct {
	*memKV
	closed bool
}

func (c *closeableMemKV) Close() error {
	c.closed = true
	return nil
}

type errGetKV struct {
	*memKV
	err error
}

func (e *errGetKV) Get(_ []byte) ([]byte, error) {
	return nil, e.err
}

func TestStoreCloseAndGetTemplateDefPaths(t *testing.T) {
	kv := &closeableMemKV{memKV: newMemKV()}
	s := New(kv, Config{})
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !kv.closed {
		t.Fatalf("expected underlying Close to be called")
	}

	s2 := New(newMemKV(), Config{})
	if _, err := s2.GetTemplateDef(context.Background(), 123); !errors.Is(err, template.ErrMissingTemplate) {
		t.Fatalf("expected ErrMissingTemplate, got %v", err)
	}

	def := []byte("def-bytes")
	id := uint64(55)
	s2.kv.(*memKV).data[string(templateKey(id))] = append([]byte(nil), def...)
	got, err := s2.GetTemplateDef(context.Background(), id)
	if err != nil {
		t.Fatalf("GetTemplateDef(existing): %v", err)
	}
	if string(got) != string(def) {
		t.Fatalf("GetTemplateDef(existing) mismatch: got=%q want=%q", got, def)
	}

	s3 := New(&errGetKV{memKV: newMemKV(), err: errors.New("boom")}, Config{})
	if _, err := s3.GetTemplateDef(context.Background(), 1); err == nil {
		t.Fatalf("expected GetTemplateDef to surface kv error")
	}
}

func TestStoreUvarintLenBoundaries(t *testing.T) {
	cases := []struct {
		v    uint64
		want int
	}{
		{0, 1},
		{1<<7 - 1, 1},
		{1 << 7, 2},
		{1<<14 - 1, 2},
		{1 << 14, 3},
		{1<<21 - 1, 3},
		{1 << 21, 4},
		{1<<28 - 1, 4},
		{1 << 28, 5},
		{1<<35 - 1, 5},
		{1 << 35, 6},
		{1<<42 - 1, 6},
		{1 << 42, 7},
		{1<<49 - 1, 7},
		{1 << 49, 8},
		{1<<56 - 1, 8},
		{1 << 56, 9},
		{1<<63 - 1, 9},
		{1 << 63, 10},
	}
	for _, tc := range cases {
		if got := uvarintLen(tc.v); got != tc.want {
			t.Fatalf("uvarintLen(%d)=%d, want %d", tc.v, got, tc.want)
		}
	}
}

func TestPutTemplateDefCollisionPath(t *testing.T) {
	kv := newMemKV()
	store := New(kv, Config{
		MaxIDAttempts:         1, // force immediate collision failure
		MaxCandidatesPerFP:    8,
		MaxCandidateListBytes: 1 << 10,
	})
	cfg := template.NormalizeConfig(template.Config{
		MinAnchorLen:          2,
		MaxAnchorLen:          64,
		MaxAnchorsPerTemplate: 8,
		MaxAnchorBytesTotal:   256,
	})
	def, err := template.EncodeTemplateDef(template.TemplateDef{
		Kind:    template.TemplateAnchors,
		Anchors: [][]byte{[]byte("aa"), []byte("bb")},
	}, cfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef: %v", err)
	}
	id := template.TemplateID(def, 0)
	kv.data[string(templateKey(id))] = []byte("different-def") // create collision for attempt 0

	if _, err := store.PutTemplateDef(context.Background(), def, []uint64{1}); err == nil {
		t.Fatalf("expected collision error")
	}
}

func TestPutTemplateDefsEmpty(t *testing.T) {
	store := New(newMemKV(), Config{})
	ids, err := store.PutTemplateDefs(context.Background(), nil)
	if err != nil {
		t.Fatalf("PutTemplateDefs(nil): %v", err)
	}
	if ids != nil {
		t.Fatalf("expected nil ids for empty defs")
	}
}
