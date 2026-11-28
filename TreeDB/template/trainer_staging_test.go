package template

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/compress/zstd"
)

type stagingMemStore struct {
	mu           sync.Mutex
	nextID       uint64
	defs         map[uint64][]byte
	cands        map[uint64][]Candidate
	syncCallback bool
	pendingCBs   []func()
	flushCalls   atomic.Int64
}

func newStagingMemStore(syncCallback bool) *stagingMemStore {
	return &stagingMemStore{
		defs:         make(map[uint64][]byte),
		cands:        make(map[uint64][]Candidate),
		syncCallback: syncCallback,
	}
}

func (s *stagingMemStore) GetCandidates(_ context.Context, fp uint64, max int) ([]Candidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.cands[fp]
	if max > 0 && len(list) > max {
		list = list[:max]
	}
	return append([]Candidate(nil), list...), nil
}

func (s *stagingMemStore) GetTemplateDef(_ context.Context, id uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.defs[id]
	if !ok {
		return nil, ErrMissingTemplate
	}
	return append([]byte(nil), b...), nil
}

func (s *stagingMemStore) PutTemplateDef(_ context.Context, defBytes []byte, routeFPs []uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	id := s.nextID
	s.defs[id] = append([]byte(nil), defBytes...)
	for _, fp := range routeFPs {
		s.cands[fp] = append(s.cands[fp], Candidate{ID: id, Size: len(defBytes)})
	}
	return id, nil
}

func (s *stagingMemStore) StageTemplateDef(_ context.Context, defBytes []byte, routeFPs []uint64, onDurable func()) (uint64, error) {
	s.mu.Lock()
	s.nextID++
	id := s.nextID
	defCopy := append([]byte(nil), defBytes...)
	fpsCopy := append([]uint64(nil), routeFPs...)
	commit := func() {
		s.mu.Lock()
		s.defs[id] = defCopy
		for _, fp := range fpsCopy {
			s.cands[fp] = append(s.cands[fp], Candidate{ID: id, Size: len(defCopy)})
		}
		s.mu.Unlock()
		if onDurable != nil {
			onDurable()
		}
	}
	if s.syncCallback {
		s.mu.Unlock()
		commit()
		return id, nil
	}
	s.pendingCBs = append(s.pendingCBs, commit)
	s.mu.Unlock()
	return id, nil
}

func (s *stagingMemStore) FlushStagedTemplateOps(_ bool) error {
	s.flushCalls.Add(1)
	s.mu.Lock()
	cbs := s.pendingCBs
	s.pendingCBs = nil
	s.mu.Unlock()
	for _, cb := range cbs {
		cb()
	}
	return nil
}

func (s *stagingMemStore) pendingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pendingCBs)
}

func makeFixedSchemaDoc(schemaID, seq int) []byte {
	return []byte(fmt.Sprintf(
		`{"schema":"event_type_%02d_service_telemetry_v2","region":"us-west-2","env":"prod","cluster":"alpha-%02d","seq":"%010d","user":"usr_%08d","ts":"2026-11-28T01:00:%02dZ","status":"ok","msg":"processed request cleanly for tenant_%02d"}`,
		schemaID, schemaID%8, seq, seq*7+schemaID, seq%60, schemaID%16,
	))
}

func TestTrainerFirstTemplateNotDelayedByCooldown(t *testing.T) {
	cfg := NormalizeConfig(Config{
		TrainSampleStride:      1,
		SynthesizeEverySamples: 64,
		CooldownValues:         512,
		PublishBatchSize:       1,
		PublishFlushEvery:      10 * time.Millisecond,
	})
	eng := NewEngine(cfg)
	defer eng.Close()

	store := newStagingMemStore(true)
	ctx := context.Background()

	// Feed 96 samples (< CooldownValues=512, >= SynthesizeEverySamples=64).
	for i := 0; i < 96; i++ {
		_, _ = eng.Encode(ctx, makeFixedSchemaDoc(1, i), store)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(eng.RecentTemplateIDs()) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := eng.RecentTemplateIDs(); len(got) == 0 {
		t.Fatalf("expected first template to synthesize before CooldownValues=512, stats=%v", eng.StatsSnapshot())
	}

	// Subsequent value must encode via template fast path and decode byte-exact.
	sample := makeFixedSchemaDoc(1, 9999)
	enc, ok := eng.Encode(ctx, sample, store)
	if !ok {
		t.Fatalf("expected template encode after initial publish")
	}
	if len(enc) >= len(sample) {
		t.Fatalf("expected encoded len %d < raw len %d", len(enc), len(sample))
	}
	dec, err := DecodePayload(enc, func(id uint64) ([]byte, error) {
		return store.GetTemplateDef(ctx, id)
	}, DecodeOptions{})
	if err != nil {
		t.Fatalf("DecodePayload: %v", err)
	}
	if !bytes.Equal(dec, sample) {
		t.Fatalf("decoded mismatch")
	}
}

func TestTrainerStagingStoreAsyncActivation(t *testing.T) {
	cfg := NormalizeConfig(Config{
		TrainSampleStride:      1,
		SynthesizeEverySamples: 64,
		PublishBatchSize:       1,
		PublishFlushEvery:      5 * time.Second, // Do not auto-flush before manual check
	})
	eng := NewEngine(cfg)
	defer eng.Close()

	store := newStagingMemStore(false)
	ctx := context.Background()

	for i := 0; i < 80; i++ {
		_, _ = eng.Encode(ctx, makeFixedSchemaDoc(2, i), store)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if store.pendingCount() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if store.pendingCount() == 0 {
		t.Fatalf("expected staged template waiting for durability barrier")
	}
	if got := eng.RecentTemplateIDs(); len(got) != 0 {
		t.Fatalf("staged template activated before onDurable barrier: %v", got)
	}

	// Simulate checkpoint durability barrier firing onDurable.
	if err := store.FlushStagedTemplateOps(true); err != nil {
		t.Fatalf("FlushStagedTemplateOps: %v", err)
	}
	if got := eng.RecentTemplateIDs(); len(got) == 0 {
		t.Fatalf("expected template to activate immediately when onDurable fired")
	}

	sample := makeFixedSchemaDoc(2, 12345)
	enc, ok := eng.Encode(ctx, sample, store)
	if !ok {
		t.Fatalf("expected encode after onDurable")
	}
	dec, err := DecodePayload(enc, func(id uint64) ([]byte, error) {
		return store.GetTemplateDef(ctx, id)
	}, DecodeOptions{})
	if err != nil || !bytes.Equal(dec, sample) {
		t.Fatalf("roundtrip failed: err=%v eq=%v", err, bytes.Equal(dec, sample))
	}
}

func BenchmarkTemplateDictCombined(b *testing.B) {
	const (
		numSchemas = 32
		numDocs    = 4096
		frameK     = 32
	)
	docs := make([][]byte, numDocs)
	var rawTotal int
	for i := 0; i < numDocs; i++ {
		schema := i % numSchemas
		doc := []byte(fmt.Sprintf(
			`{"schema":"service_%02d_telemetry_payload_v3","owner":"team_%02d_infra","region":"us-west-%d","cluster":"prod-cluster-%02d","component":"ingest-pipeline-%02d","version":"2026.11.%02d","trace":"%016x","seq":"%010d","user":"usr_%08d","latency_us":%06d,"status":"ok","notes":"deterministic boilerplate segment for schema %02d with shared structure across records in bucket %02d"}`,
			schema, schema, schema%4, schema%16, schema%8, schema%30, uint64(i)*2654435761, i, i*13+schema, (i*37)%999999, schema, schema%16,
		))
		docs[i] = doc
		rawTotal += len(doc)
	}

	// Train template engine.
	cfg := NormalizeConfig(Config{
		TrainSampleStride:      1,
		SynthesizeEverySamples: 32,
		MaxTemplatesPerBucket:  8,
		MaxTemplatesTotal:      128,
		RecentTemplates:        32,
		PublishBatchSize:       4,
		PublishFlushEvery:      10 * time.Millisecond,
	})
	eng := NewEngine(cfg)
	defer eng.Close()
	store := newStagingMemStore(true)
	ctx := context.Background()
	for i := 0; i < numDocs; i++ {
		_, _ = eng.Encode(ctx, docs[i], store)
	}
	time.Sleep(150 * time.Millisecond)

	tplDocs := make([][]byte, numDocs)
	var tplTotal int
	for i, d := range docs {
		enc, _ := eng.Encode(ctx, d, store)
		tplDocs[i] = enc
		tplTotal += len(enc)
	}

	buildDict := func(samples [][]byte, id uint32) []byte {
		var history []byte
		for i := 0; i < len(samples) && len(history) < 32<<10; i++ {
			history = append(history, samples[i]...)
		}
		if len(history) > 32<<10 {
			history = history[:32<<10]
		}
		dict, err := zstd.BuildDict(zstd.BuildDictOptions{
			ID:       id,
			Contents: samples[:256],
			History:  history,
			Offsets:  [3]int{1, 4, 8},
			Level:    zstd.SpeedFastest,
		})
		if err != nil {
			b.Fatalf("BuildDict: %v", err)
		}
		return dict
	}

	rawDict := buildDict(docs, 101)
	tplDict := buildDict(tplDocs, 102)

	compressGrouped := func(items [][]byte, dict []byte) int {
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderDict(dict), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithNoEntropyCompression(true))
		if err != nil {
			b.Fatalf("NewWriter: %v", err)
		}
		defer enc.Close()
		var buf, dst []byte
		total := 0
		for i := 0; i < len(items); i += frameK {
			buf = buf[:0]
			end := i + frameK
			if end > len(items) {
				end = len(items)
			}
			for j := i; j < end; j++ {
				buf = append(buf, items[j]...)
			}
			dst = enc.EncodeAll(buf, dst[:0])
			total += len(dst)
		}
		return total
	}

	b.Run("DictOnly", func(b *testing.B) {
		var compressed int
		for i := 0; i < b.N; i++ {
			compressed = compressGrouped(docs, rawDict)
		}
		b.ReportMetric(float64(compressed)/float64(numDocs), "B/doc")
		b.ReportMetric(float64(rawTotal)/float64(compressed), "ratio")
	})

	b.Run("TemplateOnly", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, d := range docs {
				_, _ = eng.Encode(ctx, d, store)
			}
		}
		b.ReportMetric(float64(tplTotal)/float64(numDocs), "B/doc")
		b.ReportMetric(float64(rawTotal)/float64(tplTotal), "ratio")
	})

	b.Run("TemplatePrepassPlusDict", func(b *testing.B) {
		var compressed int
		for i := 0; i < b.N; i++ {
			compressed = compressGrouped(tplDocs, tplDict)
		}
		b.ReportMetric(float64(compressed)/float64(numDocs), "B/doc")
		b.ReportMetric(float64(rawTotal)/float64(compressed), "ratio")
	})
}
