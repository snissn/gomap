package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
	"github.com/snissn/gomap/TreeDB/page"
)

func makeBenchOuterLeafPayload(tb testing.TB, codec uint8, offset uint64, entries []outerleaf.Entry) ([]byte, page.ValuePtr) {
	tb.Helper()
	encoded, err := outerleaf.EncodeEntries(nil, entries, codec, 16)
	if err != nil {
		tb.Fatalf("encode outer-leaf payload: %v", err)
	}
	return encoded, page.ValuePtr{
		FileID: page.ValueLogFileID(7),
		Offset: offset,
		Length: uint32(len(encoded)),
	}
}

func BenchmarkValueReaderReadUnsafeAppendForKey_CacheHitSkipsRawRead(b *testing.B) {
	payload, ptr := makeBenchOuterLeafPayload(
		b,
		uint8(ValueLogBlockSnappy),
		128,
		[]outerleaf.Entry{
			{Key: []byte("k1"), Value: []byte("v1")},
			{Key: []byte("k2"), Value: []byte("v2")},
			{Key: []byte("k3"), Value: []byte("v3")},
		},
	)
	reader := &stubValueLogAppendReader{
		stubValueLogReader: stubValueLogReader{
			payloads: map[page.ValuePtr][]byte{ptr: payload},
		},
	}
	cache := newOuterLeafBlockCache(8)
	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
		cache:         cache,
	}

	key := []byte("k2")
	dst := make([]byte, 0, 16)
	if _, err := r.ReadUnsafeAppendForKey(ptr, key, dst); err != nil {
		b.Fatalf("warmup: %v", err)
	}
	if _, misses, _, _ := cache.stats(); misses != 1 {
		b.Fatalf("warmup misses = %d, want 1", misses)
	}
	reader.readUnsafeAppendCalls = 0

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.ReadUnsafeAppendForKey(ptr, key, dst[:0]); err != nil {
			b.Fatalf("read %d: %v", i, err)
		}
	}
	b.ReportMetric(float64(reader.readUnsafeAppendCalls)/float64(b.N), "raw_reads/op")
	if reader.readUnsafeAppendCalls != 0 {
		b.Fatalf("raw read count = %d, want 0 in warmed cache path", reader.readUnsafeAppendCalls)
	}
}

func BenchmarkValueReaderReadUnsafeAppendForKey_NoCacheAlwaysReads(b *testing.B) {
	payload, ptr := makeBenchOuterLeafPayload(
		b,
		uint8(ValueLogBlockSnappy),
		256,
		[]outerleaf.Entry{
			{Key: []byte("k1"), Value: []byte("v1")},
			{Key: []byte("k2"), Value: []byte("v2")},
		},
	)
	reader := &stubValueLogAppendReader{
		stubValueLogReader: stubValueLogReader{
			payloads: map[page.ValuePtr][]byte{ptr: payload},
		},
	}
	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
	}

	key := []byte("k2")
	dst := make([]byte, 0, 16)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.ReadUnsafeAppendForKey(ptr, key, dst[:0]); err != nil {
			b.Fatalf("read %d: %v", i, err)
		}
	}
	b.ReportMetric(float64(reader.readUnsafeAppendCalls)/float64(b.N), "raw_reads/op")
	if reader.readUnsafeAppendCalls != b.N {
		b.Fatalf("raw read count = %d, want %d", reader.readUnsafeAppendCalls, b.N)
	}
}

func BenchmarkValueReaderReadUnsafeAppendBatchForKeys_RawReadsPerBatch(b *testing.B) {
	payloadA, ptrA := makeBenchOuterLeafPayload(
		b,
		uint8(ValueLogBlockSnappy),
		384,
		[]outerleaf.Entry{
			{Key: []byte("k1"), Value: []byte("v1")},
			{Key: []byte("k2"), Value: []byte("v2")},
			{Key: []byte("k3"), Value: []byte("v3")},
		},
	)
	payloadB, ptrB := makeBenchOuterLeafPayload(
		b,
		uint8(ValueLogBlockSnappy),
		448,
		[]outerleaf.Entry{
			{Key: []byte("k1"), Value: []byte("w1")},
			{Key: []byte("k2"), Value: []byte("w2")},
		},
	)
	reader := &stubValueLogAppendReader{
		stubValueLogReader: stubValueLogReader{
			payloads: map[page.ValuePtr][]byte{
				ptrA: payloadA,
				ptrB: payloadB,
			},
		},
	}
	cache := newOuterLeafBlockCache(8)
	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
		cache:         cache,
	}

	ptrs := []page.ValuePtr{ptrA, ptrB, ptrA, ptrB}
	keys := [][]byte{[]byte("k1"), []byte("k2"), []byte("k1"), []byte("k2")}
	dst := make([][]byte, len(ptrs))
	_, err := r.ReadUnsafeAppendBatchForKeys(ptrs, keys, dst)
	if err != nil {
		b.Fatalf("warmup: %v", err)
	}
	reader.readUnsafeAppendCalls = 0

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.ReadUnsafeAppendBatchForKeys(ptrs, keys, dst); err != nil {
			b.Fatalf("batch %d: %v", i, err)
		}
	}
	b.ReportMetric(float64(reader.readUnsafeAppendCalls)/float64(b.N*len(ptrs)), "raw_reads/op")
	if reader.readUnsafeAppendCalls <= 0 {
		b.Fatalf("raw read count = %d, want > 0 from batch reader path", reader.readUnsafeAppendCalls)
	}
}

func BenchmarkValueReaderReadUnsafeForKey_CacheHitSkipsRawRead(b *testing.B) {
	payload, ptr := makeBenchOuterLeafPayload(
		b,
		uint8(ValueLogBlockSnappy),
		128,
		[]outerleaf.Entry{
			{Key: []byte("k1"), Value: []byte("v1")},
			{Key: []byte("k2"), Value: []byte("v2")},
			{Key: []byte("k3"), Value: []byte("v3")},
		},
	)
	reader := &stubValueLogReader{
		payloads: map[page.ValuePtr][]byte{ptr: payload},
	}
	cache := newOuterLeafBlockCache(8)
	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
		cache:         cache,
	}

	key := []byte("k2")
	dst := make([]byte, 0, 16)
	if _, err := r.ReadUnsafeForKey(ptr, key); err != nil {
		b.Fatalf("warmup: %v", err)
	}
	if _, misses, _, _ := cache.stats(); misses != 1 {
		b.Fatalf("warmup misses = %d, want 1", misses)
	}
	reader.readUnsafeCalls = 0

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val, err := r.ReadUnsafeForKey(ptr, key)
		if err != nil {
			b.Fatalf("read %d: %v", i, err)
		}
		dst = append(dst[:0], val...)
	}
	b.ReportMetric(float64(reader.readUnsafeCalls)/float64(b.N), "raw_reads/op")
	if reader.readUnsafeCalls != 0 {
		b.Fatalf("raw read count = %d, want 0 in warmed cache path", reader.readUnsafeCalls)
	}
}

func BenchmarkValueReaderReadUnsafeForKey_NoCacheAlwaysReads(b *testing.B) {
	payload, ptr := makeBenchOuterLeafPayload(
		b,
		uint8(ValueLogBlockSnappy),
		256,
		[]outerleaf.Entry{
			{Key: []byte("k1"), Value: []byte("v1")},
			{Key: []byte("k2"), Value: []byte("v2")},
		},
	)
	reader := &stubValueLogReader{
		payloads: map[page.ValuePtr][]byte{ptr: payload},
	}
	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
	}

	key := []byte("k2")
	dst := make([]byte, 0, 16)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val, err := r.ReadUnsafeForKey(ptr, key)
		if err != nil {
			b.Fatalf("read %d: %v", i, err)
		}
		dst = append(dst[:0], val...)
	}
	b.ReportMetric(float64(reader.readUnsafeCalls)/float64(b.N), "raw_reads/op")
	if reader.readUnsafeCalls != b.N {
		b.Fatalf("raw read count = %d, want %d", reader.readUnsafeCalls, b.N)
	}
}
