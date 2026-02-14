package db

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
	"github.com/snissn/gomap/TreeDB/page"
)

type stubValueLogReader struct {
	payloads        map[page.ValuePtr][]byte
	readCalls       int
	readUnsafeCalls int
}

func (s *stubValueLogReader) Read(ptr page.ValuePtr) ([]byte, error) {
	s.readCalls++
	raw, ok := s.payloads[ptr]
	if !ok {
		return nil, fmt.Errorf("missing payload for ptr %+v", ptr)
	}
	return append([]byte(nil), raw...), nil
}

func (s *stubValueLogReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	s.readUnsafeCalls++
	raw, ok := s.payloads[ptr]
	if !ok {
		return nil, fmt.Errorf("missing payload for ptr %+v", ptr)
	}
	return raw, nil
}

func makeTestOuterLeafPayload(t *testing.T) ([]byte, *outerleaf.DecodedBlock, page.ValuePtr) {
	t.Helper()
	encoded, err := outerleaf.EncodeEntries(nil, []outerleaf.Entry{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	}, uint8(ValueLogBlockSnappy), 16)
	if err != nil {
		t.Fatalf("encode outer-leaf payload: %v", err)
	}
	block, err := outerleaf.DecodeBlock(encoded, nil)
	if err != nil {
		t.Fatalf("decode outer-leaf payload: %v", err)
	}
	ptr := page.ValuePtr{
		FileID: page.ValueLogFileID(7),
		Offset: 128,
		Length: uint32(len(encoded)),
	}
	return encoded, block, ptr
}

func TestValueReaderReadUnsafeAppendForKey_CacheHitSkipsReadUnsafe(t *testing.T) {
	payload, block, ptr := makeTestOuterLeafPayload(t)
	reader := &stubValueLogReader{payloads: map[page.ValuePtr][]byte{ptr: payload}}
	cache := newOuterLeafBlockCache(8)
	cache.put(newOuterLeafBlockKey(ptr), block)

	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
		cache:         cache,
	}
	got, err := r.ReadUnsafeAppendForKey(ptr, []byte("k2"), nil)
	if err != nil {
		t.Fatalf("ReadUnsafeAppendForKey: %v", err)
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("value = %q, want %q", got, "v2")
	}
	if reader.readUnsafeCalls != 0 {
		t.Fatalf("readUnsafeCalls = %d, want 0 (cache hit should bypass raw value-log read)", reader.readUnsafeCalls)
	}
}

func TestValueReaderReadUnsafeAppendForKey_CacheWarmsAfterMiss(t *testing.T) {
	payload, _, ptr := makeTestOuterLeafPayload(t)
	reader := &stubValueLogReader{payloads: map[page.ValuePtr][]byte{ptr: payload}}
	cache := newOuterLeafBlockCache(8)

	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
		cache:         cache,
	}

	first, err := r.ReadUnsafeAppendForKey(ptr, []byte("k3"), nil)
	if err != nil {
		t.Fatalf("first ReadUnsafeAppendForKey: %v", err)
	}
	if !bytes.Equal(first, []byte("v3")) {
		t.Fatalf("first value = %q, want %q", first, "v3")
	}
	if reader.readUnsafeCalls != 1 {
		t.Fatalf("readUnsafeCalls after first read = %d, want 1", reader.readUnsafeCalls)
	}

	second, err := r.ReadUnsafeAppendForKey(ptr, []byte("k3"), nil)
	if err != nil {
		t.Fatalf("second ReadUnsafeAppendForKey: %v", err)
	}
	if !bytes.Equal(second, []byte("v3")) {
		t.Fatalf("second value = %q, want %q", second, "v3")
	}
	if reader.readUnsafeCalls != 1 {
		t.Fatalf("readUnsafeCalls after cache warm = %d, want 1", reader.readUnsafeCalls)
	}
}

func TestValueReaderReadUnsafeFenceForKey_CacheHitSkipsReadUnsafe(t *testing.T) {
	payload, block, ptr := makeTestOuterLeafPayload(t)
	reader := &stubValueLogReader{payloads: map[page.ValuePtr][]byte{ptr: payload}}
	cache := newOuterLeafBlockCache(8)
	cache.put(newOuterLeafBlockKey(ptr), block)

	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
		cache:         cache,
	}

	got, found, err := r.ReadUnsafeFenceForKey(ptr, []byte("k1"))
	if err != nil {
		t.Fatalf("ReadUnsafeFenceForKey: %v", err)
	}
	if !found {
		t.Fatalf("found = false, want true")
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("value = %q, want %q", got, "v1")
	}
	if reader.readUnsafeCalls != 0 {
		t.Fatalf("readUnsafeCalls = %d, want 0 (cache hit should bypass raw value-log read)", reader.readUnsafeCalls)
	}
}

func TestValueReaderReadUnsafeFenceBlockKeys_CacheHitSkipsReadUnsafe(t *testing.T) {
	payload, block, ptr := makeTestOuterLeafPayload(t)
	reader := &stubValueLogReader{payloads: map[page.ValuePtr][]byte{ptr: payload}}
	cache := newOuterLeafBlockCache(8)
	cache.put(newOuterLeafBlockKey(ptr), block)

	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
		cache:         cache,
	}

	keys, ok, err := r.ReadUnsafeFenceBlockKeys(ptr)
	if err != nil {
		t.Fatalf("ReadUnsafeFenceBlockKeys: %v", err)
	}
	if !ok {
		t.Fatalf("ok = false, want true")
	}
	want := [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")}
	if len(keys) != len(want) {
		t.Fatalf("keys len = %d, want %d", len(keys), len(want))
	}
	for i := range want {
		if !bytes.Equal(keys[i], want[i]) {
			t.Fatalf("key[%d] = %q, want %q", i, keys[i], want[i])
		}
	}
	if reader.readUnsafeCalls != 0 {
		t.Fatalf("readUnsafeCalls = %d, want 0 (cache hit should bypass raw value-log read)", reader.readUnsafeCalls)
	}
}
