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

func TestValueReaderOuterLeafBlock_CacheMissReturnsLease(t *testing.T) {
	payload, _, ptr := makeTestOuterLeafPayload(t)
	cache := newOuterLeafBlockCache(8)
	r := valueReader{cache: cache}

	block, lease, err := r.outerLeafBlock(ptr, payload)
	if err != nil {
		t.Fatalf("outerLeafBlock: %v", err)
	}
	if block == nil {
		t.Fatalf("block=nil want non-nil")
	}
	if lease.ref == nil {
		t.Fatalf("lease.ref=nil want non-nil for cache-admitted block")
	}
	lease.Release()
}

func TestOuterLeafFenceDecodeScratchPoolCapBounded(t *testing.T) {
	const maxExpected = 8 << 20
	if outerLeafFenceDecodeScratchMaxRetain > maxExpected {
		t.Fatalf("outerLeafFenceDecodeScratchMaxRetain=%d exceeds bound=%d", outerLeafFenceDecodeScratchMaxRetain, maxExpected)
	}
}

func TestOuterLeafFenceDecodeLeaseSetAcquireBounded(t *testing.T) {
	set := newOuterLeafFenceDecodeLeaseSet(1)
	ctx := set.acquire()
	if ctx == nil {
		t.Fatalf("first acquire returned nil")
	}
	if got := set.acquire(); got != nil {
		t.Fatalf("second acquire=%v want nil when lease set is saturated", got)
	}
	set.release(ctx)
	ctx = set.acquire()
	if ctx == nil {
		t.Fatalf("acquire after release returned nil")
	}
	set.release(ctx)
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

func TestOuterLeafBlockCachePut_EvictReleasesLeaseBlock(t *testing.T) {
	payloadA, _, ptrA := makeTestOuterLeafPayload(t)
	payloadB, _, _ := makeTestOuterLeafPayload(t)
	ptrB := page.ValuePtr{
		FileID: ptrA.FileID,
		Offset: ptrA.Offset + 4096,
		Length: ptrA.Length,
	}
	blockA, err := outerleaf.DecodeBlockLease(payloadA)
	if err != nil {
		t.Fatalf("DecodeBlockLease(A): %v", err)
	}
	blockB, err := outerleaf.DecodeBlockLease(payloadB)
	if err != nil {
		t.Fatalf("DecodeBlockLease(B): %v", err)
	}
	if len(blockA.RawBytes()) == 0 {
		t.Fatalf("blockA raw empty before cache insert")
	}

	cache := newOuterLeafBlockCache(1)
	cache.put(newOuterLeafBlockKey(ptrA), blockA)
	cache.put(newOuterLeafBlockKey(ptrB), blockB)

	if blockA.RawBytes() != nil {
		t.Fatalf("expected evicted blockA raw to be released")
	}
}

func TestValueReaderReadUnsafeFenceForKey_NoCacheInline(t *testing.T) {
	payload, _, ptr := makeTestOuterLeafPayload(t)
	reader := &stubValueLogReader{payloads: map[page.ValuePtr][]byte{ptr: payload}}
	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
	}

	got, found, err := r.ReadUnsafeFenceForKey(ptr, []byte("k2"))
	if err != nil {
		t.Fatalf("ReadUnsafeFenceForKey: %v", err)
	}
	if !found {
		t.Fatalf("found=false want true")
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("value=%q want=%q", got, "v2")
	}
	if reader.readUnsafeCalls != 1 {
		t.Fatalf("readUnsafeCalls=%d want=1", reader.readUnsafeCalls)
	}

	got, err = r.ReadUnsafeForKey(ptr, []byte("k3"))
	if err != nil {
		t.Fatalf("ReadUnsafeForKey: %v", err)
	}
	if !bytes.Equal(got, []byte("v3")) {
		t.Fatalf("value=%q want=%q", got, "v3")
	}
	if reader.readUnsafeCalls != 2 {
		t.Fatalf("readUnsafeCalls=%d want=2", reader.readUnsafeCalls)
	}
}

func TestValueReaderReadUnsafeFenceForKey_LeaseCtxCacheBlocksRemainStable(t *testing.T) {
	makePayload := func(prefix byte, offset uint64) (page.ValuePtr, []byte, []byte, []byte) {
		key1 := []byte{prefix, '1'}
		key2 := []byte{prefix, '2'}
		key3 := []byte{prefix, '3'}
		val1 := bytes.Repeat([]byte{prefix}, 1024)
		val2 := bytes.Repeat([]byte{prefix + 1}, 1024)
		val3 := bytes.Repeat([]byte{prefix + 2}, 1024)
		encoded, err := outerleaf.EncodeEntries(nil, []outerleaf.Entry{
			{Key: key1, Value: val1},
			{Key: key2, Value: val2},
			{Key: key3, Value: val3},
		}, uint8(ValueLogBlockSnappy), 16)
		if err != nil {
			t.Fatalf("encode outer-leaf payload: %v", err)
		}
		ptr := page.ValuePtr{
			FileID: page.ValueLogFileID(11),
			Offset: offset,
			Length: uint32(len(encoded)),
		}
		return ptr, encoded, key2, val2
	}

	ptrA, payloadA, keyA, wantA := makePayload('a', 4096)
	ptrB, payloadB, keyB, wantB := makePayload('b', 8192)

	reader := &stubValueLogReader{
		payloads: map[page.ValuePtr][]byte{
			ptrA: payloadA,
			ptrB: payloadB,
		},
	}
	cache := newOuterLeafBlockCache(8)
	r := newValueReader(reader, outerleaf.ModeV2FencePtr, false, cache, nil)
	defer (&r).releaseDecodeContext()

	gotA1, found, err := r.ReadUnsafeFenceForKey(ptrA, keyA)
	if err != nil {
		t.Fatalf("ReadUnsafeFenceForKey(A first): %v", err)
	}
	if !found || !bytes.Equal(gotA1, wantA) {
		t.Fatalf("A first read found=%v val=%q want=%q", found, gotA1, wantA)
	}

	gotB, found, err := r.ReadUnsafeFenceForKey(ptrB, keyB)
	if err != nil {
		t.Fatalf("ReadUnsafeFenceForKey(B): %v", err)
	}
	if !found || !bytes.Equal(gotB, wantB) {
		t.Fatalf("B read found=%v val=%q want=%q", found, gotB, wantB)
	}

	gotA2, found, err := r.ReadUnsafeFenceForKey(ptrA, keyA)
	if err != nil {
		t.Fatalf("ReadUnsafeFenceForKey(A cache): %v", err)
	}
	if !found || !bytes.Equal(gotA2, wantA) {
		t.Fatalf("A cache read found=%v val=%q want=%q", found, gotA2, wantA)
	}
	if reader.readUnsafeCalls != 2 {
		t.Fatalf("readUnsafeCalls=%d want=2 (A miss + B miss, then A cache hit)", reader.readUnsafeCalls)
	}
}

func TestValueReaderReadUnsafeFenceForKey_NoCacheLeaseCtxInlineValuesRemainStable(t *testing.T) {
	makePayload := func(prefix byte, offset uint64) (page.ValuePtr, []byte, []byte, []byte) {
		key1 := []byte{prefix, '1'}
		key2 := []byte{prefix, '2'}
		key3 := []byte{prefix, '3'}
		val1 := bytes.Repeat([]byte{prefix}, 1024)
		val2 := bytes.Repeat([]byte{prefix + 1}, 1024)
		val3 := bytes.Repeat([]byte{prefix + 2}, 1024)
		encoded, err := outerleaf.EncodeEntries(nil, []outerleaf.Entry{
			{Key: key1, Value: val1},
			{Key: key2, Value: val2},
			{Key: key3, Value: val3},
		}, uint8(ValueLogBlockSnappy), 16)
		if err != nil {
			t.Fatalf("encode outer-leaf payload: %v", err)
		}
		ptr := page.ValuePtr{
			FileID: page.ValueLogFileID(11),
			Offset: offset,
			Length: uint32(len(encoded)),
		}
		return ptr, encoded, key2, val2
	}

	ptrA, payloadA, keyA, wantA := makePayload('a', 4096)
	ptrB, payloadB, keyB, wantB := makePayload('b', 8192)
	reader := &stubValueLogReader{
		payloads: map[page.ValuePtr][]byte{
			ptrA: payloadA,
			ptrB: payloadB,
		},
	}
	r := newValueReader(reader, outerleaf.ModeV2FencePtr, false, nil, nil)
	defer (&r).releaseDecodeContext()
	if r.fenceDecodeLeases == nil {
		t.Fatalf("fenceDecodeLeases=nil want initialized for no-cache fence mode")
	}

	gotA1, found, err := r.ReadUnsafeFenceForKey(ptrA, keyA)
	if err != nil {
		t.Fatalf("ReadUnsafeFenceForKey(A first): %v", err)
	}
	if !found || !bytes.Equal(gotA1, wantA) {
		t.Fatalf("A first read found=%v val-len=%d want-len=%d", found, len(gotA1), len(wantA))
	}

	gotB, found, err := r.ReadUnsafeFenceForKey(ptrB, keyB)
	if err != nil {
		t.Fatalf("ReadUnsafeFenceForKey(B): %v", err)
	}
	if !found || !bytes.Equal(gotB, wantB) {
		t.Fatalf("B read found=%v val-len=%d want-len=%d", found, len(gotB), len(wantB))
	}

	if !bytes.Equal(gotA1, wantA) {
		t.Fatalf("A first value mutated after B read")
	}
	if reader.readUnsafeCalls != 2 {
		t.Fatalf("readUnsafeCalls=%d want=2 (A miss + B miss)", reader.readUnsafeCalls)
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

func TestValueReaderReadUnsafeFenceBlock_UsesVisitTypedEntries(t *testing.T) {
	blobPtr := page.ValuePtr{FileID: page.ValueLogFileID(42), Offset: 99, Length: 7}
	blobVal := []byte("blobval")
	outerPayload, outerPtr := makeTestOuterLeafBlobRefPayload(t, []byte("acct:0001"), blobPtr)

	reader := &stubValueLogReader{payloads: map[page.ValuePtr][]byte{
		outerPtr: outerPayload,
		blobPtr:  blobVal,
	}}
	cache := newOuterLeafBlockCache(8)

	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
		cache:         cache,
	}

	entries, ok, err := r.ReadUnsafeFenceBlock(outerPtr)
	if err != nil {
		t.Fatalf("ReadUnsafeFenceBlock: %v", err)
	}
	if !ok {
		t.Fatalf("ok=false want true")
	}
	if len(entries) != 1 {
		t.Fatalf("entries len=%d want 1", len(entries))
	}
	if !bytes.Equal(entries[0].Key, []byte("acct:0001")) {
		t.Fatalf("key=%q want %q", entries[0].Key, "acct:0001")
	}
	if !bytes.Equal(entries[0].Value, blobVal) {
		t.Fatalf("value=%q want %q", entries[0].Value, blobVal)
	}
}

func TestValueReaderReadUnsafeFenceBlockLeaseInto_CacheHitReturnsLease(t *testing.T) {
	payload, block, ptr := makeTestOuterLeafPayload(t)
	reader := &stubValueLogReader{payloads: map[page.ValuePtr][]byte{ptr: payload}}
	cache := newOuterLeafBlockCache(8)
	cache.put(newOuterLeafBlockKey(ptr), block)

	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
		cache:         cache,
	}

	entries, lease, ok, err := r.ReadUnsafeFenceBlockLeaseInto(ptr, nil)
	if err != nil {
		t.Fatalf("ReadUnsafeFenceBlockLeaseInto: %v", err)
	}
	if !ok {
		t.Fatalf("ok=false want true")
	}
	if lease == nil {
		t.Fatalf("lease=nil want non-nil")
	}
	if len(entries) != 3 {
		t.Fatalf("entries len=%d want 3", len(entries))
	}
	if reader.readUnsafeCalls != 0 {
		t.Fatalf("readUnsafeCalls=%d want 0", reader.readUnsafeCalls)
	}
	lease.Release()
}

func TestValueReaderReadUnsafeFenceBlockInto_CacheHitClonesInlineEntries(t *testing.T) {
	payload, block, ptr := makeTestOuterLeafPayload(t)
	reader := &stubValueLogReader{payloads: map[page.ValuePtr][]byte{ptr: payload}}
	cache := newOuterLeafBlockCache(8)
	cache.put(newOuterLeafBlockKey(ptr), block)

	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
		cache:         cache,
	}

	entries, ok, err := r.ReadUnsafeFenceBlockInto(ptr, nil)
	if err != nil {
		t.Fatalf("ReadUnsafeFenceBlockInto: %v", err)
	}
	if !ok {
		t.Fatalf("ok=false want true")
	}
	if len(entries) != 3 {
		t.Fatalf("entries len=%d want 3", len(entries))
	}

	entries[0].Key[0] = 'x'
	entries[0].Value[0] = 'y'

	fresh, lease, ok, err := r.ReadUnsafeFenceBlockLeaseInto(ptr, nil)
	if err != nil {
		t.Fatalf("ReadUnsafeFenceBlockLeaseInto after mutate: %v", err)
	}
	if !ok {
		t.Fatalf("ok=false want true")
	}
	if lease == nil {
		t.Fatalf("lease=nil want non-nil")
	}
	if !bytes.Equal(fresh[0].Key, []byte("k1")) {
		t.Fatalf("fresh key=%q want %q", fresh[0].Key, []byte("k1"))
	}
	if !bytes.Equal(fresh[0].Value, []byte("v1")) {
		t.Fatalf("fresh value=%q want %q", fresh[0].Value, []byte("v1"))
	}
	lease.Release()
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

func TestValueReaderBlobRefResolution_ReadUnsafeAppendForKey_RetainsOuterBufferCapacity(t *testing.T) {
	key := []byte("blob-k")
	blobPtr := page.ValuePtr{FileID: page.ValueLogFileID(32), Offset: 24576, Length: 9}
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

	dst := make([]byte, 0, 1)
	got, err := r.ReadUnsafeAppendForKey(outerPtr, key, dst)
	if err != nil {
		t.Fatalf("ReadUnsafeAppendForKey first: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("first value = %q, want %q", got, want)
	}
	if cap(got) < len(outerPayload) {
		t.Fatalf("first cap = %d, want >= outer payload size %d", cap(got), len(outerPayload))
	}

	got2, err := r.ReadUnsafeAppendForKey(outerPtr, key, got[:0])
	if err != nil {
		t.Fatalf("ReadUnsafeAppendForKey second: %v", err)
	}
	if !bytes.Equal(got2, want) {
		t.Fatalf("second value = %q, want %q", got2, want)
	}
	if cap(got2) < len(outerPayload) {
		t.Fatalf("second cap = %d, want >= outer payload size %d", cap(got2), len(outerPayload))
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
