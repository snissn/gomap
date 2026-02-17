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

type stubValueLogAppendReader struct {
	stubValueLogReader
	readUnsafeAppendCalls int
}

func (s *stubValueLogAppendReader) ReadUnsafeAppend(ptr page.ValuePtr, dst []byte) ([]byte, error) {
	s.readUnsafeAppendCalls++
	raw, ok := s.payloads[ptr]
	if !ok {
		return nil, fmt.Errorf("missing payload for ptr %+v", ptr)
	}
	if cap(dst) < len(raw) {
		dst = make([]byte, len(raw))
	} else {
		dst = dst[:len(raw)]
	}
	copy(dst, raw)
	return dst, nil
}

type stubValueLogAppendBatchReader struct {
	stubValueLogAppendReader
	readUnsafeAppendBatchCalls int
}

func (s *stubValueLogAppendBatchReader) ReadUnsafeAppendBatch(ptrs []page.ValuePtr, dst [][]byte) ([][]byte, error) {
	s.readUnsafeAppendBatchCalls++
	if cap(dst) < len(ptrs) {
		dst = make([][]byte, len(ptrs))
	} else {
		dst = dst[:len(ptrs)]
	}
	for i := range ptrs {
		val, err := s.ReadUnsafeAppend(ptrs[i], dst[i][:0])
		if err != nil {
			return nil, err
		}
		dst[i] = val
	}
	return dst, nil
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

func TestValueReaderReadUnsafeAppendForKey_AppendPathDoesNotCacheOuterLeafBlock(t *testing.T) {
	payload, _, ptr := makeTestOuterLeafPayload(t)
	reader := &stubValueLogAppendReader{stubValueLogReader: stubValueLogReader{payloads: map[page.ValuePtr][]byte{ptr: payload}}}
	cache := newOuterLeafBlockCache(8)

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
	if _, _, entries, _ := cache.stats(); entries != 0 {
		t.Fatalf("cache entries = %d, want 0 for append decode path", entries)
	}
}

func TestValueReaderReadUnsafeAppendBatchForKeys_AppendBatchPathDoesNotCacheOuterLeafBlocks(t *testing.T) {
	payloadA, _, _ := makeTestOuterLeafPayload(t)
	payloadB, _, _ := makeTestOuterLeafPayload(t)
	ptrA := page.ValuePtr{FileID: page.ValueLogFileID(7), Offset: 128, Length: uint32(len(payloadA))}
	ptrB := page.ValuePtr{FileID: page.ValueLogFileID(7), Offset: 256, Length: uint32(len(payloadB))}

	reader := &stubValueLogAppendBatchReader{stubValueLogAppendReader: stubValueLogAppendReader{stubValueLogReader: stubValueLogReader{payloads: map[page.ValuePtr][]byte{
		ptrA: payloadA,
		ptrB: payloadB,
	}}}}
	cache := newOuterLeafBlockCache(8)

	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
		cache:         cache,
	}

	ptrs := []page.ValuePtr{ptrA, ptrB}
	keys := [][]byte{[]byte("k1"), []byte("k1")}
	out, err := r.ReadUnsafeAppendBatchForKeys(ptrs, keys, nil)
	if err != nil {
		t.Fatalf("ReadUnsafeAppendBatchForKeys: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("out len = %d, want 2", len(out))
	}
	if _, _, entries, _ := cache.stats(); entries != 0 {
		t.Fatalf("cache entries = %d, want 0 for append-batch decode path", entries)
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

func TestValueReaderReadUnsafeFenceBlockKeys_BlobRefWarmsCacheAfterMiss(t *testing.T) {
	key := []byte("blob-k")
	blobPtr := page.ValuePtr{FileID: page.ValueLogFileID(42), Offset: 99, Length: 7}
	outerPayload, outerPtr := makeTestOuterLeafBlobRefPayload(t, key, blobPtr)
	reader := &stubValueLogReader{payloads: map[page.ValuePtr][]byte{outerPtr: outerPayload}}
	cache := newOuterLeafBlockCache(8)
	keyCache := newOuterLeafKeyCache(8)

	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
		cache:         cache,
		keyCache:      keyCache,
	}

	firstKeys, ok, err := r.ReadUnsafeFenceBlockKeys(outerPtr)
	if err != nil {
		t.Fatalf("first ReadUnsafeFenceBlockKeys: %v", err)
	}
	if !ok {
		t.Fatalf("first ok = false, want true")
	}
	if len(firstKeys) != 1 || !bytes.Equal(firstKeys[0], key) {
		t.Fatalf("first keys = %q, want %q", firstKeys, key)
	}
	if reader.readUnsafeCalls != 1 {
		t.Fatalf("readUnsafeCalls after first read = %d, want 1", reader.readUnsafeCalls)
	}

	secondKeys, ok, err := r.ReadUnsafeFenceBlockKeys(outerPtr)
	if err != nil {
		t.Fatalf("second ReadUnsafeFenceBlockKeys: %v", err)
	}
	if !ok {
		t.Fatalf("second ok = false, want true")
	}
	if len(secondKeys) != 1 || !bytes.Equal(secondKeys[0], key) {
		t.Fatalf("second keys = %q, want %q", secondKeys, key)
	}
	if reader.readUnsafeCalls != 1 {
		t.Fatalf("readUnsafeCalls after cache warm = %d, want 1", reader.readUnsafeCalls)
	}
}

func TestValueReaderReadUnsafeFenceBlockKeysRange_CacheHitSkipsReadUnsafe(t *testing.T) {
	payload, block, ptr := makeTestOuterLeafPayload(t)
	reader := &stubValueLogReader{payloads: map[page.ValuePtr][]byte{ptr: payload}}
	cache := newOuterLeafBlockCache(8)
	cache.put(newOuterLeafBlockKey(ptr), block)

	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
		cache:         cache,
	}

	keys, ok, err := r.ReadUnsafeFenceBlockKeysRange(ptr, []byte("k2"), []byte("k3"))
	if err != nil {
		t.Fatalf("ReadUnsafeFenceBlockKeysRange: %v", err)
	}
	if !ok {
		t.Fatalf("ok = false, want true")
	}
	if len(keys) != 1 || !bytes.Equal(keys[0], []byte("k2")) {
		t.Fatalf("keys = %q, want [k2]", keys)
	}
	if reader.readUnsafeCalls != 0 {
		t.Fatalf("readUnsafeCalls = %d, want 0", reader.readUnsafeCalls)
	}
}

func TestValueReaderReadUnsafeFenceBlockKeysRangeLease_CacheHitSkipsReadUnsafe(t *testing.T) {
	payload, block, ptr := makeTestOuterLeafPayload(t)
	reader := &stubValueLogReader{payloads: map[page.ValuePtr][]byte{ptr: payload}}
	cache := newOuterLeafBlockCache(8)
	cache.put(newOuterLeafBlockKey(ptr), block)

	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
		cache:         cache,
	}

	lease, ok, err := r.ReadUnsafeFenceBlockKeysRangeLease(ptr, []byte("k2"), []byte("k3"))
	if err != nil {
		t.Fatalf("ReadUnsafeFenceBlockKeysRangeLease: %v", err)
	}
	if !ok {
		t.Fatalf("ok = false, want true")
	}
	if lease == nil {
		t.Fatalf("lease = nil, want non-nil")
	}
	keys := lease.Keys()
	if len(keys) != 1 || !bytes.Equal(keys[0], []byte("k2")) {
		t.Fatalf("keys = %q, want [k2]", keys)
	}
	lease.Release()
	if reader.readUnsafeCalls != 0 {
		t.Fatalf("readUnsafeCalls = %d, want 0", reader.readUnsafeCalls)
	}
}

func TestValueReaderReadUnsafeFenceBlockKeysRange_UsesKeyCacheWindow(t *testing.T) {
	payload, _, ptr := makeTestOuterLeafPayload(t)
	reader := &stubValueLogReader{payloads: map[page.ValuePtr][]byte{ptr: payload}}
	cache := newOuterLeafBlockCache(8)
	keyCache := newOuterLeafKeyCache(8)
	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
		cache:         cache,
		keyCache:      keyCache,
	}

	all, ok, err := r.ReadUnsafeFenceBlockKeys(ptr)
	if err != nil {
		t.Fatalf("ReadUnsafeFenceBlockKeys warm: %v", err)
	}
	if !ok || len(all) != 3 {
		t.Fatalf("warm keys ok=%v len=%d, want ok=true len=3", ok, len(all))
	}
	if reader.readUnsafeCalls != 1 {
		t.Fatalf("readUnsafeCalls after warm = %d, want 1", reader.readUnsafeCalls)
	}

	keys, ok, err := r.ReadUnsafeFenceBlockKeysRange(ptr, []byte("k2"), nil)
	if err != nil {
		t.Fatalf("ReadUnsafeFenceBlockKeysRange: %v", err)
	}
	if !ok {
		t.Fatalf("ok = false, want true")
	}
	want := [][]byte{[]byte("k2"), []byte("k3")}
	if len(keys) != len(want) {
		t.Fatalf("keys len = %d, want %d", len(keys), len(want))
	}
	for i := range want {
		if !bytes.Equal(keys[i], want[i]) {
			t.Fatalf("keys[%d] = %q, want %q", i, keys[i], want[i])
		}
	}
	if reader.readUnsafeCalls != 1 {
		t.Fatalf("readUnsafeCalls after key-cache window = %d, want 1", reader.readUnsafeCalls)
	}
}

func TestValueReaderReadUnsafeFenceBlockSeek_ClassifiesBoundsAndReusesCache(t *testing.T) {
	payload, _, ptr := makeTestOuterLeafPayload(t)
	reader := &stubValueLogReader{payloads: map[page.ValuePtr][]byte{ptr: payload}}
	cache := newOuterLeafBlockCache(8)
	keyCache := newOuterLeafKeyCache(8)
	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
		cache:         cache,
		keyCache:      keyCache,
	}

	pos, below, above, keys, ok, err := r.ReadUnsafeFenceBlockSeek(ptr, []byte("k0"))
	if err != nil {
		t.Fatalf("ReadUnsafeFenceBlockSeek(k0): %v", err)
	}
	if !ok {
		t.Fatalf("ok=false, want true")
	}
	if pos != 0 || !below || above || keys != nil {
		t.Fatalf("seek(k0) got pos=%d below=%v above=%v keys=%v, want pos=0 below=true above=false keys=nil", pos, below, above, keys)
	}
	if reader.readUnsafeCalls != 1 {
		t.Fatalf("readUnsafeCalls after first seek = %d, want 1", reader.readUnsafeCalls)
	}

	pos, below, above, keys, ok, err = r.ReadUnsafeFenceBlockSeek(ptr, []byte("k2"))
	if err != nil {
		t.Fatalf("ReadUnsafeFenceBlockSeek(k2): %v", err)
	}
	if !ok {
		t.Fatalf("ok=false, want true")
	}
	if pos != 1 || below || above || len(keys) != 3 {
		t.Fatalf("seek(k2) got pos=%d below=%v above=%v len(keys)=%d, want pos=1 below=false above=false len(keys)=3", pos, below, above, len(keys))
	}
	if reader.readUnsafeCalls != 2 {
		t.Fatalf("readUnsafeCalls after in-range materialization = %d, want 2", reader.readUnsafeCalls)
	}

	pos, below, above, keys, ok, err = r.ReadUnsafeFenceBlockSeek(ptr, []byte("k9"))
	if err != nil {
		t.Fatalf("ReadUnsafeFenceBlockSeek(k9): %v", err)
	}
	if !ok {
		t.Fatalf("ok=false, want true")
	}
	if pos != 3 || below || !above || keys != nil {
		t.Fatalf("seek(k9) got pos=%d below=%v above=%v keys=%v, want pos=3 below=false above=true keys=nil", pos, below, above, keys)
	}
	if reader.readUnsafeCalls != 2 {
		t.Fatalf("readUnsafeCalls after cache reuse = %d, want 2", reader.readUnsafeCalls)
	}
}

func TestValueReaderReadUnsafeFenceBlockSeekLease_CachesClonedKeys(t *testing.T) {
	payload, _, ptr := makeTestOuterLeafPayload(t)
	reader := &stubValueLogReader{payloads: map[page.ValuePtr][]byte{ptr: payload}}
	cache := newOuterLeafBlockCache(8)
	keyCache := newOuterLeafKeyCache(8)
	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
		cache:         cache,
		keyCache:      keyCache,
	}

	pos, below, above, lease, ok, err := r.ReadUnsafeFenceBlockSeekLease(ptr, []byte("k2"))
	if err != nil {
		t.Fatalf("ReadUnsafeFenceBlockSeekLease(k2): %v", err)
	}
	if !ok {
		t.Fatalf("ok=false, want true")
	}
	if pos != 1 || below || above || lease == nil {
		t.Fatalf("seek lease got pos=%d below=%v above=%v lease=%v, want pos=1 below=false above=false lease!=nil", pos, below, above, lease)
	}
	keys := lease.Keys()
	if len(keys) != 3 {
		t.Fatalf("lease keys len=%d want=3", len(keys))
	}
	keys[0][0] = 'x'
	lease.Release()

	pos, below, above, cachedKeys, ok, err := r.ReadUnsafeFenceBlockSeek(ptr, []byte("k2"))
	if err != nil {
		t.Fatalf("ReadUnsafeFenceBlockSeek(k2): %v", err)
	}
	if !ok {
		t.Fatalf("ok=false, want true")
	}
	if pos != 1 || below || above || len(cachedKeys) != 3 {
		t.Fatalf("seek cache got pos=%d below=%v above=%v len=%d", pos, below, above, len(cachedKeys))
	}
	if !bytes.Equal(cachedKeys[0], []byte("k1")) {
		t.Fatalf("cached key[0]=%q want=%q", cachedKeys[0], []byte("k1"))
	}
	if reader.readUnsafeCalls != 1 {
		t.Fatalf("readUnsafeCalls=%d want=1", reader.readUnsafeCalls)
	}
}

func TestValueReaderFenceMode_DirectPointerReadForKey(t *testing.T) {
	ptr := page.ValuePtr{
		FileID: page.ValueLogFileID(9),
		Offset: 64,
		Length: 5,
	}
	raw := []byte("value")
	reader := &stubValueLogReader{payloads: map[page.ValuePtr][]byte{ptr: raw}}
	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
	}

	got, err := r.ReadUnsafeForKey(ptr, []byte("k1"))
	if err != nil {
		t.Fatalf("ReadUnsafeForKey: %v", err)
	}
	if !bytes.Equal(got, raw) {
		t.Fatalf("value = %q, want %q", got, raw)
	}

	foundVal, found, err := r.ReadUnsafeFenceForKey(ptr, []byte("k1"))
	if err != nil {
		t.Fatalf("ReadUnsafeFenceForKey: %v", err)
	}
	if found {
		t.Fatalf("found = true, want false")
	}
	if foundVal != nil {
		t.Fatalf("fence value = %q, want nil", foundVal)
	}

	keys, ok, err := r.ReadUnsafeFenceBlockKeys(ptr)
	if err != nil {
		t.Fatalf("ReadUnsafeFenceBlockKeys: %v", err)
	}
	if ok {
		t.Fatalf("ok = true, want false")
	}
	if keys != nil {
		t.Fatalf("keys = %v, want nil", keys)
	}

	rangeKeys, ok, err := r.ReadUnsafeFenceBlockKeysRange(ptr, []byte("a"), []byte("z"))
	if err != nil {
		t.Fatalf("ReadUnsafeFenceBlockKeysRange: %v", err)
	}
	if ok {
		t.Fatalf("range ok = true, want false")
	}
	if rangeKeys != nil {
		t.Fatalf("range keys = %v, want nil", rangeKeys)
	}
}

func makeTestOuterLeafBlobRefPayload(t *testing.T, key []byte, blobPtr page.ValuePtr) ([]byte, page.ValuePtr) {
	t.Helper()
	encoded, err := outerleaf.EncodeSingleBlobRef(nil, key, blobPtr, uint8(ValueLogBlockSnappy), 16)
	if err != nil {
		t.Fatalf("encode blob-ref outer-leaf payload: %v", err)
	}
	outerPtr := page.ValuePtr{
		FileID: page.ValueLogFileID(17),
		Offset: 4096,
		Length: uint32(len(encoded)),
	}
	return encoded, outerPtr
}

func TestValueReaderBlobRefResolution_ReadForKey(t *testing.T) {
	key := []byte("blob-k")
	blobPtr := page.ValuePtr{FileID: page.ValueLogFileID(23), Offset: 8192, Length: 9}
	outerPayload, outerPtr := makeTestOuterLeafBlobRefPayload(t, key, blobPtr)
	want := []byte("blob-data")
	reader := &stubValueLogReader{
		payloads: map[page.ValuePtr][]byte{
			outerPtr: outerPayload,
			blobPtr:  want,
		},
	}
	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
	}
	got, err := r.ReadForKey(outerPtr, key)
	if err != nil {
		t.Fatalf("ReadForKey: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("value = %q, want %q", got, want)
	}
}

func TestValueReaderBlobRefResolution_MissingNestedPointer(t *testing.T) {
	key := []byte("blob-k")
	blobPtr := page.ValuePtr{FileID: page.ValueLogFileID(24), Offset: 2048, Length: 7}
	outerPayload, outerPtr := makeTestOuterLeafBlobRefPayload(t, key, blobPtr)
	reader := &stubValueLogReader{
		payloads: map[page.ValuePtr][]byte{
			outerPtr: outerPayload,
		},
	}
	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
	}
	if _, err := r.ReadForKey(outerPtr, key); err == nil {
		t.Fatalf("expected missing nested blob pointer read to fail")
	}
}

func TestValueReaderBlobRefResolution_InvalidNestedPointerFile(t *testing.T) {
	key := []byte("blob-k")
	invalidBlobPtr := page.ValuePtr{FileID: 1, Offset: 2048, Length: 7}
	outerPayload, outerPtr := makeTestOuterLeafBlobRefPayload(t, key, invalidBlobPtr)
	reader := &stubValueLogReader{
		payloads: map[page.ValuePtr][]byte{
			outerPtr: outerPayload,
		},
	}
	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
	}
	if _, err := r.ReadForKey(outerPtr, key); err == nil {
		t.Fatalf("expected invalid nested blob pointer file to fail")
	}
}

func TestValueReaderBlobRefResolution_ReadUnsafeAppendForKey_UsesAppendReader(t *testing.T) {
	key := []byte("blob-k")
	blobPtr := page.ValuePtr{FileID: page.ValueLogFileID(31), Offset: 16384, Length: 9}
	outerPayload, outerPtr := makeTestOuterLeafBlobRefPayload(t, key, blobPtr)
	want := []byte("blob-data")
	reader := &stubValueLogAppendReader{
		stubValueLogReader: stubValueLogReader{
			payloads: map[page.ValuePtr][]byte{
				outerPtr: outerPayload,
				blobPtr:  want,
			},
		},
	}
	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
	}

	got, err := r.ReadUnsafeAppendForKey(outerPtr, key, nil)
	if err != nil {
		t.Fatalf("ReadUnsafeAppendForKey: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("value = %q, want %q", got, want)
	}
	if reader.readUnsafeAppendCalls != 2 {
		t.Fatalf("readUnsafeAppendCalls = %d, want 2 (outer + blob)", reader.readUnsafeAppendCalls)
	}
	if reader.readUnsafeCalls != 0 {
		t.Fatalf("readUnsafeCalls = %d, want 0", reader.readUnsafeCalls)
	}
}

func TestValueReaderBlobRefResolution_ReadUnsafeAppendForKey_CacheHitUsesAppendReader(t *testing.T) {
	key := []byte("blob-k")
	blobPtr := page.ValuePtr{FileID: page.ValueLogFileID(33), Offset: 32768, Length: 9}
	outerPayload, outerPtr := makeTestOuterLeafBlobRefPayload(t, key, blobPtr)
	block, err := outerleaf.DecodeBlock(outerPayload, nil)
	if err != nil {
		t.Fatalf("decode outer block: %v", err)
	}
	want := []byte("blob-data")
	reader := &stubValueLogAppendReader{
		stubValueLogReader: stubValueLogReader{
			payloads: map[page.ValuePtr][]byte{
				blobPtr: want,
			},
		},
	}
	cache := newOuterLeafBlockCache(8)
	cache.put(newOuterLeafBlockKey(outerPtr), block)
	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
		cache:         cache,
	}

	got, err := r.ReadUnsafeAppendForKey(outerPtr, key, nil)
	if err != nil {
		t.Fatalf("ReadUnsafeAppendForKey: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("value = %q, want %q", got, want)
	}
	if reader.readUnsafeAppendCalls != 1 {
		t.Fatalf("readUnsafeAppendCalls = %d, want 1 (blob only)", reader.readUnsafeAppendCalls)
	}
	if reader.readUnsafeCalls != 0 {
		t.Fatalf("readUnsafeCalls = %d, want 0", reader.readUnsafeCalls)
	}
}
