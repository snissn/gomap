package template

import (
	"bytes"
	"context"
	"sync/atomic"
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
	def := TemplateDef{Kind: TemplateAnchors, Anchors: [][]byte{[]byte("prefix-"), []byte("-suffix")}}
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

type countingStore struct {
	defBytes []byte
	id       uint64
	cands    []Candidate

	getCandidates atomic.Uint64
	getDef        atomic.Uint64
	putDef        atomic.Uint64
}

func (s *countingStore) GetCandidates(_ context.Context, _ uint64, _ int) ([]Candidate, error) {
	s.getCandidates.Add(1)
	return s.cands, nil
}

func (s *countingStore) GetTemplateDef(_ context.Context, id uint64) ([]byte, error) {
	s.getDef.Add(1)
	if id != s.id {
		return nil, ErrMissingTemplate
	}
	return s.defBytes, nil
}

func (s *countingStore) PutTemplateDef(_ context.Context, _ []byte, _ []uint64) (uint64, error) {
	s.putDef.Add(1)
	return s.id, nil
}

func TestEngineColdGateLimitsCandidateLookups(t *testing.T) {
	cfg := Config{
		MinSavingsBytes:       1,
		FingerprintK:          4,
		FingerprintW:          4,
		MaxFingerprints:       1,
		MaxFPReads:            1,
		MaxTemplateFetch:      1,
		MaxCandidatesPerFP:    1,
		MinAnchorLen:          4,
		MaxAnchorLen:          64,
		MaxAnchorsPerTemplate: 8,
		MaxAnchorBytesTotal:   256,
		MaxGaps:               16,
		ColdSearchAfter:       4,
		ColdSearchProbeEvery:  8,
	}
	def := TemplateDef{Kind: TemplateAnchors, Anchors: [][]byte{[]byte("never-match")}}
	defBytes, err := EncodeTemplateDef(def, cfg)
	if err != nil {
		t.Fatalf("encode template def: %v", err)
	}
	store := &countingStore{
		defBytes: defBytes,
		id:       1,
		cands:    []Candidate{{ID: 1, Size: len(defBytes)}},
	}
	engine := NewEngine(cfg)
	value := []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

	const iters = 100
	for i := 0; i < iters; i++ {
		_, ok := engine.Encode(nil, value, store)
		if ok {
			t.Fatalf("unexpected keep at iter=%d", i)
		}
	}

	// Without the cold gate, we'd do a candidate lookup on every call.
	// With ColdSearchAfter=4 and ColdSearchProbeEvery=8, we expect O(iters/8)
	// candidate lookups after the initial warm-up.
	if got := store.getCandidates.Load(); got > 25 {
		t.Fatalf("candidate lookups too high: got=%d", got)
	}
}

func TestEngineStartupColdGateLimitsCandidateLookups_DefaultConfig(t *testing.T) {
	// Default (normalized) cold settings are 256/256. The engine should apply a
	// startup guardrail before the first keep so we don't hit the store on every
	// value when templates are enabled but nothing matches.
	cfg := Config{
		MinSavingsBytes:       1,
		FingerprintK:          4,
		FingerprintW:          4,
		RouteFPCount:          1,
		MaxFPReads:            1,
		MaxTemplateFetch:      1,
		MaxCandidatesPerFP:    1,
		MinAnchorLen:          4,
		MaxAnchorLen:          64,
		MaxAnchorsPerTemplate: 8,
		MaxAnchorBytesTotal:   256,
		MaxGaps:               16,
		// ColdSearchAfter / ColdSearchProbeEvery intentionally left at 0 to use
		// defaults via NormalizeConfig.
	}
	def := TemplateDef{Kind: TemplateAnchors, Anchors: [][]byte{[]byte("never-match")}}
	defBytes, err := EncodeTemplateDef(def, cfg)
	if err != nil {
		t.Fatalf("encode template def: %v", err)
	}
	store := &countingStore{
		defBytes: defBytes,
		id:       1,
		cands:    []Candidate{{ID: 1, Size: len(defBytes)}},
	}
	engine := NewEngine(cfg)
	value := []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

	const iters = 200
	for i := 0; i < iters; i++ {
		_, ok := engine.Encode(nil, value, store)
		if ok {
			t.Fatalf("unexpected keep at iter=%d", i)
		}
	}

	// Expect store candidate lookups to be much less than iters due to the
	// startup cold gate (probes only).
	if got := store.getCandidates.Load(); got > 80 {
		t.Fatalf("candidate lookups too high: got=%d", got)
	}
}

func TestEncodeMaskTemplateSparse_DoesNotMutateInput(t *testing.T) {
	base := []byte("prefix-0000-suffix")
	varPositions := []uint16{7, 8, 9, 10}
	mask := buildMaskFromPositions(varPositions, len(base))
	def := TemplateDef{
		Kind:         TemplateMask,
		Base:         append([]byte(nil), base...),
		Mask:         mask,
		VarPositions: append([]uint16(nil), varPositions...),
	}
	value := []byte("prefix-9876-suffix")
	valueBefore := append([]byte(nil), value...)

	templateID := uint64(42)
	payload := encodeMaskTemplate(value, def, templateID, true)
	if len(payload) == 0 {
		t.Fatalf("expected sparse mask payload")
	}
	if !bytes.Equal(value, valueBefore) {
		t.Fatalf("encodeMaskTemplate mutated input value")
	}

	defBytes, err := EncodeTemplateDef(def, Config{})
	if err != nil {
		t.Fatalf("encode template def: %v", err)
	}
	decoded, err := DecodePayload(payload, func(id uint64) ([]byte, error) {
		if id != templateID {
			return nil, ErrMissingTemplate
		}
		return defBytes, nil
	}, DecodeOptions{MaxDecodedBytes: 1 << 20, MaxGaps: 64})
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if !bytes.Equal(decoded, valueBefore) {
		t.Fatalf("decoded mismatch: got=%q want=%q", decoded, valueBefore)
	}
}
