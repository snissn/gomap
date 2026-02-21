package templatedb

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/template"
)

func TestNormalizeConfigDefaults(t *testing.T) {
	cfg := NormalizeConfig(Config{})
	if cfg.MaxCandidatesPerFP <= 0 {
		t.Fatalf("MaxCandidatesPerFP=%d, want >0", cfg.MaxCandidatesPerFP)
	}
	if cfg.MaxCandidateListBytes <= 0 {
		t.Fatalf("MaxCandidateListBytes=%d, want >0", cfg.MaxCandidateListBytes)
	}
	if cfg.MaxIDAttempts <= 0 {
		t.Fatalf("MaxIDAttempts=%d, want >0", cfg.MaxIDAttempts)
	}
}

func TestStoreUnavailableErrors(t *testing.T) {
	s := New(nil, Config{})
	if _, err := s.GetTemplateDef(context.Background(), 1); !errors.Is(err, errStoreUnavailable) {
		t.Fatalf("GetTemplateDef err=%v, want errStoreUnavailable", err)
	}
	if _, err := s.GetCandidates(context.Background(), 1, 0); !errors.Is(err, errStoreUnavailable) {
		t.Fatalf("GetCandidates err=%v, want errStoreUnavailable", err)
	}
	if _, err := s.PutTemplateDef(context.Background(), []byte("x"), []uint64{1}); !errors.Is(err, errStoreUnavailable) {
		t.Fatalf("PutTemplateDef err=%v, want errStoreUnavailable", err)
	}
	if _, err := s.PutTemplateDefs(context.Background(), []template.PublishSpec{{DefBytes: []byte("x"), RouteFPs: []uint64{1}}}); !errors.Is(err, errStoreUnavailable) {
		t.Fatalf("PutTemplateDefs err=%v, want errStoreUnavailable", err)
	}
}

func TestCandidateCodecRoundTripAndOrdering(t *testing.T) {
	in := []template.Candidate{
		{ID: 9, Size: 100},
		{ID: 2, Size: 5},
		{ID: 1, Size: 5},
		{ID: 4, Size: 30},
	}
	enc := encodeCandidates(append([]template.Candidate(nil), in...))
	out, err := decodeCandidates(enc)
	if err != nil {
		t.Fatalf("decodeCandidates: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("len(out)=%d, want %d", len(out), len(in))
	}
	// Sorted by size, then ID.
	want := []template.Candidate{
		{ID: 1, Size: 5},
		{ID: 2, Size: 5},
		{ID: 4, Size: 30},
		{ID: 9, Size: 100},
	}
	for i := range want {
		if out[i] != want[i] {
			t.Fatalf("out[%d]=%+v, want %+v", i, out[i], want[i])
		}
	}
}

func TestDecodeCandidatesCorrupt(t *testing.T) {
	// Unterminated uvarint
	if _, err := decodeCandidates([]byte{0x80}); err == nil {
		t.Fatalf("expected corrupt candidate list error")
	}
}

func TestUpsertCandidatePrefersSmallerSize(t *testing.T) {
	cands := []template.Candidate{{ID: 7, Size: 100}}
	cands = upsertCandidate(cands, template.Candidate{ID: 7, Size: 80})
	if cands[0].Size != 80 {
		t.Fatalf("size=%d, want 80", cands[0].Size)
	}
	// Zero-size update should not override.
	cands = upsertCandidate(cands, template.Candidate{ID: 7, Size: 0})
	if cands[0].Size != 80 {
		t.Fatalf("size=%d, want 80 after zero-size update", cands[0].Size)
	}
}

func TestPutTemplateDefsAndGetCandidates(t *testing.T) {
	kv := newMemKV()
	store := New(kv, Config{MaxCandidatesPerFP: 8, MaxCandidateListBytes: 1 << 10})
	cfg := template.NormalizeConfig(template.Config{
		MinAnchorLen:          2,
		MaxAnchorLen:          64,
		MaxAnchorsPerTemplate: 8,
		MaxAnchorBytesTotal:   256,
	})

	defA, err := template.EncodeTemplateDef(template.TemplateDef{
		Kind:    template.TemplateAnchors,
		Anchors: [][]byte{[]byte("aa"), []byte("bb")},
	}, cfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef A: %v", err)
	}
	defB, err := template.EncodeTemplateDef(template.TemplateDef{
		Kind:    template.TemplateAnchors,
		Anchors: [][]byte{[]byte("aa"), []byte("bb"), []byte("cc")},
	}, cfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef B: %v", err)
	}

	specs := []template.PublishSpec{
		{DefBytes: defA, RouteFPs: []uint64{7, 7, 9}},
		{DefBytes: defB, RouteFPs: []uint64{7}},
	}
	ids, err := store.PutTemplateDefs(context.Background(), specs)
	if err != nil {
		t.Fatalf("PutTemplateDefs: %v", err)
	}
	if len(ids) != 2 || ids[0] == 0 || ids[1] == 0 {
		t.Fatalf("invalid ids returned: %v", ids)
	}

	// Idempotent publish should return same IDs.
	ids2, err := store.PutTemplateDefs(context.Background(), specs)
	if err != nil {
		t.Fatalf("PutTemplateDefs idempotent: %v", err)
	}
	if ids2[0] != ids[0] || ids2[1] != ids[1] {
		t.Fatalf("idempotent IDs mismatch: first=%v second=%v", ids, ids2)
	}

	cands, err := store.GetCandidates(context.Background(), 7, 0)
	if err != nil {
		t.Fatalf("GetCandidates: %v", err)
	}
	if len(cands) != 2 {
		t.Fatalf("len(cands)=%d, want 2", len(cands))
	}
	if cands[0].Size > cands[1].Size {
		t.Fatalf("candidates not size-sorted: %v", cands)
	}

	limited, err := store.GetCandidates(context.Background(), 7, 1)
	if err != nil {
		t.Fatalf("GetCandidates(max): %v", err)
	}
	if len(limited) != 1 {
		t.Fatalf("len(limited)=%d, want 1", len(limited))
	}
}
