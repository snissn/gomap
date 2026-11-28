package template

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"
)

type benchMemStore struct {
	defs map[uint64][]byte
	fps  map[uint64][]Candidate
}

func newBenchMemStore() *benchMemStore {
	return &benchMemStore{
		defs: make(map[uint64][]byte),
		fps:  make(map[uint64][]Candidate),
	}
}

func (s *benchMemStore) GetCandidates(_ context.Context, fp uint64, max int) ([]Candidate, error) {
	c := s.fps[fp]
	if max > 0 && len(c) > max {
		c = c[:max]
	}
	return c, nil
}

func (s *benchMemStore) GetTemplateDef(_ context.Context, id uint64) ([]byte, error) {
	b, ok := s.defs[id]
	if !ok {
		return nil, ErrMissingTemplate
	}
	return b, nil
}

func (s *benchMemStore) PutTemplateDef(_ context.Context, defBytes []byte, routeFPs []uint64) (uint64, error) {
	id := TemplateID(defBytes, 0)
	s.defs[id] = append([]byte(nil), defBytes...)
	for _, fp := range routeFPs {
		s.fps[fp] = append(s.fps[fp], Candidate{ID: id, Size: len(defBytes)})
	}
	return id, nil
}

func makeStructuredJSON(i int) []byte {
	return []byte(fmt.Sprintf(
		`{"type":"cosmos-sdk/MsgDelegate","delegator_address":"celestia1qypqxpq9qcrsszg2pvxq6rs0zqg3yyc5%08d","validator_address":"celestiavaloper1qypqxpq9qcrsszg2pvxq6rs0zqg3yyc5%08d","amount":{"denom":"utia","amount":"%012d"},"memo":"block-height-%08d-tx-verified-consensus-state"}`,
		i, i%97, 1000000+i, 5000000+i,
	))
}

func makeFixedMaskRecord(size, i int) []byte {
	buf := bytes.Repeat([]byte("TEMPLATE-HEADER-FIXED-STRUCT-PAYLOAD-"), size/37+1)[:size]
	out := append([]byte(nil), buf...)
	binary.LittleEndian.PutUint64(out[16:24], uint64(i))
	binary.LittleEndian.PutUint64(out[size/2:size/2+8], uint64(i*17+3))
	binary.LittleEndian.PutUint64(out[size-16:size-8], uint64(i^0x5a5a5a5a))
	return out
}

func BenchmarkTemplateEncodeAnchorHit(b *testing.B) {
	cfg := NormalizeConfig(Config{DisableMaskTemplates: true, TrainSampleStride: 1 << 30})
	eng := NewEngine(cfg)
	defer eng.Close()
	store := newBenchMemStore()
	def := TemplateDef{
		Kind: TemplateAnchors,
		Anchors: [][]byte{
			[]byte(`{"type":"cosmos-sdk/MsgDelegate","delegator_address":"celestia1q`),
			[]byte(`","validator_address":"celestiavaloper1qypqxpq9qcrsszg2pvxq6rs0z`),
			[]byte(`","amount":{"denom":"utia","amount":"`),
			[]byte(`"},"memo":"block-height-`),
			[]byte(`-tx-verified-consensus-state"}`),
		},
	}
	defBytes, err := EncodeTemplateDef(def, cfg)
	if err != nil {
		b.Fatal(err)
	}
	sampleVal := makeStructuredJSON(1)
	id, err := store.PutTemplateDef(context.Background(), defBytes, RoutingFingerprints(sampleVal, cfg))
	if err != nil || id == 0 {
		b.Fatalf("put def: id=%d err=%v", id, err)
	}
	// Warm recent fast path.
	for i := 0; i < 4; i++ {
		if _, ok := eng.Encode(context.Background(), makeStructuredJSON(i), store); !ok {
			b.Fatal("expected anchor encode hit")
		}
	}
	vals := make([][]byte, 256)
	for i := range vals {
		vals[i] = makeStructuredJSON(100 + i)
	}
	b.SetBytes(int64(len(vals[0])))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, ok := eng.Encode(context.Background(), vals[i&255], store)
		if !ok || len(out) == 0 {
			b.Fatal("unexpected miss")
		}
	}
}

func BenchmarkTemplateEncodeMaskHitFreshlySynthesized(b *testing.B) {
	cfg := NormalizeConfig(Config{TrainSampleStride: 1 << 30})
	eng := NewEngine(cfg)
	defer eng.Close()
	store := newBenchMemStore()
	samples := make([]sample, 64)
	for i := range samples {
		samples[i] = sample{value: makeFixedMaskRecord(1024, i)}
	}
	def, routeVal, activated, ok := synthesizeMaskTemplate(samples, cfg)
	if !ok || !activated {
		b.Fatalf("synthesizeMaskTemplate failed: ok=%v activated=%v", ok, activated)
	}
	defBytes, err := EncodeTemplateDef(def, cfg)
	if err != nil {
		b.Fatal(err)
	}
	id, err := store.PutTemplateDef(context.Background(), defBytes, RoutingFingerprints(routeVal, cfg))
	if err != nil || id == 0 {
		b.Fatalf("put mask def: id=%d err=%v", id, err)
	}
	// Simulate trainer.onPublished caching the freshly synthesized TemplateDef.
	eng.defCache.Add(id, def)
	for i := 0; i < 4; i++ {
		if _, ok := eng.Encode(context.Background(), makeFixedMaskRecord(1024, 1000+i), store); !ok {
			b.Fatal("expected mask encode hit")
		}
	}
	vals := make([][]byte, 256)
	for i := range vals {
		vals[i] = makeFixedMaskRecord(1024, 2000+i)
	}
	b.SetBytes(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, ok := eng.Encode(context.Background(), vals[i&255], store)
		if !ok || len(out) == 0 {
			b.Fatal("unexpected miss")
		}
	}
}

func BenchmarkTemplateEncodeMaskHitSparseConstMatch(b *testing.B) {
	cfg := NormalizeConfig(Config{TrainSampleStride: 1 << 30})
	eng := NewEngine(cfg)
	defer eng.Close()
	store := newBenchMemStore()
	samples := make([]sample, 64)
	for i := range samples {
		samples[i] = sample{value: makeFixedMaskRecord(1024, i*0x0101010101010101)}
	}
	def, routeVal, activated, ok := synthesizeMaskTemplate(samples, cfg)
	if !ok || !activated {
		b.Fatalf("synthesizeMaskTemplate failed: ok=%v activated=%v", ok, activated)
	}
	defBytes, err := EncodeTemplateDef(def, cfg)
	if err != nil {
		b.Fatal(err)
	}
	id, err := store.PutTemplateDef(context.Background(), defBytes, RoutingFingerprints(routeVal, cfg))
	if err != nil || id == 0 {
		b.Fatalf("put mask def: id=%d err=%v", id, err)
	}
	eng.defCache.Add(id, def)
	for i := 0; i < 4; i++ {
		if _, ok := eng.Encode(context.Background(), makeFixedMaskRecord(1024, (100+i)*0x0101010101010101), store); !ok {
			b.Fatal("expected mask sparse hit")
		}
	}
	vals := make([][]byte, 256)
	for i := range vals {
		vals[i] = makeFixedMaskRecord(1024, (200+i)*0x0101010101010101)
	}
	b.SetBytes(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, ok := eng.Encode(context.Background(), vals[i&255], store)
		if !ok || len(out) == 0 {
			b.Fatal("unexpected miss")
		}
	}
}

func BenchmarkTemplateEncodeColdMiss(b *testing.B) {
	cfg := NormalizeConfig(Config{
		ColdSearchAfter:      1 << 30,
		ColdSearchProbeEvery: 1,
		TrainSampleStride:    1 << 30,
	})
	eng := NewEngine(cfg)
	defer eng.Close()
	store := newBenchMemStore()
	val := bytes.Repeat([]byte("unmatched-randomish-payload-0123456789abcdef"), 16)
	b.SetBytes(int64(len(val)))
	b.ReportAllocs()
	b.ResetTimer()
	// Also benchmark Encode when value < LengthBucketMinLen so legacy winnowing triggers on miss.
	b.Run("short_legacy_miss", func(b *testing.B) {
		shortVal := []byte("unmatched-short-value-0123456789abcdefghijklmnopqrstuvwxyz")
		b.SetBytes(int64(len(shortVal)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, ok := eng.Encode(context.Background(), shortVal, store); ok {
				b.Fatal("unexpected hit")
			}
		}
	})
}

func BenchmarkTemplateDecodeAnchors(b *testing.B) {
	cfg := NormalizeConfig(Config{})
	def := TemplateDef{
		Kind: TemplateAnchors,
		Anchors: [][]byte{
			[]byte(`{"type":"cosmos-sdk/MsgDelegate","delegator_address":"celestia1q`),
			[]byte(`","validator_address":"celestiavaloper1qypqxpq9qcrsszg2pvxq6rs0z`),
			[]byte(`","amount":{"denom":"utia","amount":"`),
			[]byte(`"},"memo":"block-height-`),
			[]byte(`-tx-verified-consensus-state"}`),
		},
	}
	defBytes, err := EncodeTemplateDef(def, cfg)
	if err != nil {
		b.Fatal(err)
	}
	decodedDef, err := DecodeTemplateDef(defBytes)
	if err != nil {
		b.Fatal(err)
	}
	raw := makeStructuredJSON(42)
	gaps, _, _, matched := matchTemplate(raw, decodedDef.Anchors, 7, cfg)
	if !matched {
		b.Fatal("match failed")
	}
	payload, err := EncodePayload(7, gaps)
	if err != nil {
		b.Fatal(err)
	}
	lookup := func(uint64) (TemplateDef, error) { return decodedDef, nil }
	dst := make([]byte, 0, len(raw)*2)
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		dst, err = DecodePayloadAppend(dst[:0], payload, lookup, DecodeOptions{MaxDecodedBytes: 1 << 20, MaxGaps: 64})
		if err != nil || len(dst) != len(raw) {
			b.Fatalf("decode failed: len=%d err=%v", len(dst), err)
		}
	}
}

func BenchmarkTemplateDecodeMaskFull(b *testing.B) {
	cfg := NormalizeConfig(Config{})
	base := makeFixedMaskRecord(2048, 0)
	val := makeFixedMaskRecord(2048, 999)
	mask := make([]byte, (len(base)+7)/8)
	var vars []byte
	for i := range base {
		if val[i] != base[i] {
			mask[i/8] |= 1 << uint(i%8)
			vars = append(vars, val[i])
		}
	}
	def := TemplateDef{Kind: TemplateMask, Base: base, Mask: mask}
	defBytes, err := EncodeTemplateDef(def, cfg)
	if err != nil {
		b.Fatal(err)
	}
	decodedDef, err := DecodeTemplateDef(defBytes)
	if err != nil {
		b.Fatal(err)
	}
	payload, err := EncodeMaskPayload(9, mask, vars)
	if err != nil {
		b.Fatal(err)
	}
	lookup := func(uint64) (TemplateDef, error) { return decodedDef, nil }
	dst := make([]byte, 0, len(base)*2)
	b.SetBytes(int64(len(base)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		dst, err = DecodePayloadAppend(dst[:0], payload, lookup, DecodeOptions{MaxDecodedBytes: 1 << 20})
		if err != nil || len(dst) != len(base) {
			b.Fatalf("decode failed: len=%d err=%v", len(dst), err)
		}
	}
}

func BenchmarkTemplateSynthesizeMask4K(b *testing.B) {
	cfg := NormalizeConfig(Config{})
	samples := make([]sample, 64)
	for i := range samples {
		samples[i] = sample{value: makeFixedMaskRecord(4096, i)}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		def, _, _, ok := synthesizeMaskTemplate(samples, cfg)
		if !ok || len(def.Base) != 4096 {
			b.Fatal("synthesizeMaskTemplate failed")
		}
	}
}

func BenchmarkTemplateFingerprintsWinnow(b *testing.B) {
	cfg := NormalizeConfig(Config{})
	val := makeStructuredJSON(12345)
	b.SetBytes(int64(len(val)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fps := Fingerprints(val, cfg)
		if len(fps) == 0 {
			b.Fatal("empty fps")
		}
	}
}
