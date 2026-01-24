package template

import (
	"context"
	"testing"
)

type stubStore struct {
	defBytes []byte
	id       uint64
}

func (s *stubStore) GetCandidates(_ context.Context, _ uint64, _ int) ([]Candidate, error) {
	return []Candidate{{ID: s.id, Size: len(s.defBytes)}}, nil
}

func (s *stubStore) GetTemplateDef(_ context.Context, id uint64) ([]byte, error) {
	if id != s.id {
		return nil, ErrMissingTemplate
	}
	return s.defBytes, nil
}

func (s *stubStore) PutTemplateDef(_ context.Context, _ []byte, _ []uint64) (uint64, error) {
	return s.id, nil
}

func TestEngineEncodeDecode(t *testing.T) {
	cfg := Config{
		MinSavingsBytes:       1,
		FingerprintK:          4,
		FingerprintW:          4,
		MaxFingerprints:       16,
		MaxFPReads:            16,
		MaxTemplateFetch:      8,
		MaxCandidatesPerFP:    8,
		MinAnchorLen:          4,
		MaxAnchorLen:          64,
		MaxAnchorsPerTemplate: 8,
		MaxAnchorBytesTotal:   256,
		MaxGaps:               16,
	}
	def := TemplateDef{Anchors: [][]byte{[]byte("prefix-"), []byte("-suffix")}}
	defBytes, err := EncodeTemplateDef(def, cfg)
	if err != nil {
		t.Fatalf("encode template def: %v", err)
	}
	store := &stubStore{defBytes: defBytes, id: 1}
	engine := NewEngine(cfg)

	value := []byte("prefix-ABC-suffix")
	payload, ok := engine.Encode(context.Background(), value, store)
	if !ok {
		t.Fatalf("expected template encoding")
	}
	decoded, err := DecodePayload(payload, func(id uint64) ([]byte, error) {
		return store.GetTemplateDef(context.Background(), id)
	}, DecodeOptions{MaxDecodedBytes: 1 << 20, MaxGaps: 16})
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if string(decoded) != string(value) {
		t.Fatalf("decoded mismatch: %q != %q", decoded, value)
	}
}
