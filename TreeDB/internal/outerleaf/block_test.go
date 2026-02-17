package outerleaf

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/golang/snappy"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/limits"
	"github.com/snissn/gomap/TreeDB/page"
)

func pseudoRandomBytes(n int) []byte {
	out := make([]byte, n)
	var x uint64 = 0x9e3779b97f4a7c15
	for i := 0; i < n; i++ {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		out[i] = byte((x * 0x2545F4914F6CDD1D) >> 56)
	}
	return out
}

func makeCodecNonePayloadForTest(t *testing.T, raw []byte) []byte {
	t.Helper()
	payload := make([]byte, blockHeaderSize+len(raw))
	copy(payload[:len(blockMagic)], blockMagic[:])
	payload[4] = blockVersionV1
	payload[5] = blockCodecNone
	binary.LittleEndian.PutUint16(payload[blockV1KeyLenOff:blockV1KeyLenOff+2], 0)
	binary.LittleEndian.PutUint32(payload[blockV1ValueLenOff:blockV1ValueLenOff+4], uint32(len(raw)))
	binary.LittleEndian.PutUint32(payload[blockV1RawLenOff:blockV1RawLenOff+4], uint32(len(raw)))
	copy(payload[blockHeaderSize:], raw)
	checksum := crc.ChecksumParts(payload[:blockChecksumOff], payload[blockHeaderSize:])
	binary.LittleEndian.PutUint32(payload[blockChecksumOff:blockChecksumOff+blockChecksumSize], checksum)
	return payload
}

func drainOuterLeafBytesPoolForTest() {
	for {
		v := outerLeafBytesPool.Get()
		if v == nil {
			return
		}
		entry, ok := v.(*bytesPoolEntry)
		if !ok || entry == nil {
			continue
		}
		entry.buf = nil
		entry.next = nil
		outerLeafBytesEntryPool.Put(entry)
	}
}

func TestEncodeDecodeSingleRoundTrip(t *testing.T) {
	codecs := []struct {
		name  string
		codec uint8
	}{
		{name: "snappy", codec: 0},
		{name: "lz4", codec: 1},
	}

	for _, tc := range codecs {
		t.Run(tc.name, func(t *testing.T) {
			key := []byte("user:123456")
			value := bytes.Repeat([]byte("abcdef0123456789"), 128)

			enc, err := EncodeSingle(nil, key, value, tc.codec, 16)
			if err != nil {
				t.Fatalf("EncodeSingle: %v", err)
			}
			gotKey, gotVal, ok, _, err := Decode(enc, nil)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !ok {
				t.Fatalf("Decode reported ok=false")
			}
			if !bytes.Equal(gotKey, key) {
				t.Fatalf("key mismatch")
			}
			if !bytes.Equal(gotVal, value) {
				t.Fatalf("value mismatch")
			}

			looked, ok, found, _, err := DecodeValueForKey(enc, key, nil)
			if err != nil {
				t.Fatalf("DecodeValueForKey: %v", err)
			}
			if !ok || !found || !bytes.Equal(looked, value) {
				t.Fatalf("DecodeValueForKey mismatch")
			}
		})
	}
}

func TestHasMagic(t *testing.T) {
	enc, err := EncodeSingle(nil, []byte("k"), []byte("v"), 0, 16)
	if err != nil {
		t.Fatalf("EncodeSingle: %v", err)
	}
	if !HasMagic(enc) {
		t.Fatalf("expected HasMagic=true for encoded payload")
	}
	if HasMagic([]byte("plain")) {
		t.Fatalf("expected HasMagic=false for plain payload")
	}
}

func TestEncodeSingleCodecHeaderMapping(t *testing.T) {
	key := []byte("user:codec")
	value := bytes.Repeat([]byte("v"), 256)

	snappyEnc, err := EncodeSingle(nil, key, value, 0, 16)
	if err != nil {
		t.Fatalf("EncodeSingle(snappy): %v", err)
	}
	if len(snappyEnc) < blockHeaderSize {
		t.Fatalf("snappy payload too short: %d", len(snappyEnc))
	}
	if got := snappyEnc[5]; got != blockCodecSnappy {
		t.Fatalf("snappy codec header=%d want=%d", got, blockCodecSnappy)
	}

	lz4Enc, err := EncodeSingle(nil, key, value, 1, 16)
	if err != nil {
		t.Fatalf("EncodeSingle(lz4): %v", err)
	}
	if len(lz4Enc) < blockHeaderSize {
		t.Fatalf("lz4 payload too short: %d", len(lz4Enc))
	}
	if got := lz4Enc[5]; got != blockCodecLZ4 {
		t.Fatalf("lz4 codec header=%d want=%d", got, blockCodecLZ4)
	}
}

func TestEncodePayloadIncompressibleBypass(t *testing.T) {
	raw := pseudoRandomBytes(8 << 10)
	cases := []struct {
		name  string
		codec uint8
	}{
		{name: "snappy", codec: 0},
		{name: "lz4", codec: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, gotCodec, err := encodePayload(tc.codec, raw, nil)
			if err != nil {
				t.Fatalf("encodePayload: %v", err)
			}
			if gotCodec != blockCodecNone {
				t.Fatalf("codec=%d want=%d", gotCodec, blockCodecNone)
			}
			if !bytes.Equal(encoded, raw) {
				t.Fatalf("expected raw payload bypass")
			}
		})
	}
}

func TestEncodePayloadCompressibleKeepsCodec(t *testing.T) {
	raw := bytes.Repeat([]byte("outerleaf-compressible-payload|"), 256)
	cases := []struct {
		name       string
		codec      uint8
		wantHeader uint8
	}{
		{name: "snappy", codec: 0, wantHeader: blockCodecSnappy},
		{name: "lz4", codec: 1, wantHeader: blockCodecLZ4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, gotCodec, err := encodePayload(tc.codec, raw, nil)
			if err != nil {
				t.Fatalf("encodePayload: %v", err)
			}
			if gotCodec != tc.wantHeader {
				t.Fatalf("codec=%d want=%d", gotCodec, tc.wantHeader)
			}
			if len(encoded) >= len(raw) {
				t.Fatalf("encoded len=%d raw len=%d", len(encoded), len(raw))
			}
		})
	}
}

func TestEncodePayloadSnappyReusesDstNoAllocs(t *testing.T) {
	raw := bytes.Repeat([]byte("outerleaf-compressible-payload|"), 256)
	dst := make([]byte, 0, snappy.MaxEncodedLen(len(raw)))
	encoded, gotCodec, err := encodePayload(0, raw, dst[:0])
	if err != nil {
		t.Fatalf("encodePayload: %v", err)
	}
	if gotCodec != blockCodecSnappy {
		t.Fatalf("codec=%d want=%d", gotCodec, blockCodecSnappy)
	}
	if len(encoded) == 0 {
		t.Fatalf("expected non-empty encoded payload")
	}
	backing := dst[:cap(dst)]
	if &encoded[0] != &backing[0] {
		t.Fatalf("snappy encode did not reuse caller dst backing buffer")
	}

	allocs := testing.AllocsPerRun(1000, func() {
		_, _, _ = encodePayload(0, raw, dst[:0])
	})
	if allocs != 0 {
		t.Fatalf("expected zero allocs per run, got %.2f", allocs)
	}
}

func TestDecodePayloadSnappyPreallocatedDstRoundTrip(t *testing.T) {
	raw := bytes.Repeat([]byte("outerleaf-decode-payload|"), 256)
	encoded := snappy.Encode(nil, raw)
	dst := make([]byte, 0, len(raw))

	decoded, err := decodePayload(blockCodecSnappy, encoded, len(raw), dst[:0])
	if err != nil {
		t.Fatalf("decodePayload: %v", err)
	}
	if !bytes.Equal(decoded, raw) {
		t.Fatalf("decoded payload mismatch")
	}
	if cap(decoded) < len(raw) {
		t.Fatalf("decoded cap=%d want at least %d", cap(decoded), len(raw))
	}
}

func TestDecodePayloadSnappyPreallocatedDstNoAllocs(t *testing.T) {
	raw := bytes.Repeat([]byte("outerleaf-decode-payload|"), 256)
	encoded := snappy.Encode(nil, raw)
	dst := make([]byte, len(raw))

	allocs := testing.AllocsPerRun(1000, func() {
		decoded, err := decodePayload(blockCodecSnappy, encoded, len(raw), dst[:len(raw)])
		if err != nil {
			t.Fatalf("decodePayload: %v", err)
		}
		if len(decoded) != len(raw) {
			t.Fatalf("decoded len=%d want=%d", len(decoded), len(raw))
		}
	})
	if allocs != 0 {
		t.Fatalf("expected zero allocs per run, got %.2f", allocs)
	}
}

func TestPayloadLeaseReleaseIdempotence(t *testing.T) {
	raw := bytes.Repeat([]byte("outerleaf-payload-lease|"), 256)
	encoded := snappy.Encode(nil, raw)

	decoded, lease, err := decodePayloadLease(blockCodecSnappy, encoded, len(raw), nil)
	if err != nil {
		t.Fatalf("decodePayloadLease: %v", err)
	}
	if lease == nil {
		t.Fatalf("lease=nil want non-nil")
	}
	if !bytes.Equal(decoded, raw) {
		t.Fatalf("decoded payload mismatch")
	}
	lease.Release()
	lease.Release()
}

func TestDecodeAndVerifyPayloadLeaseAllowRawViewNoCopy(t *testing.T) {
	raw := bytes.Repeat([]byte("outerleaf-raw-view"), 128)
	payload := makeCodecNonePayloadForTest(t, raw)

	decoded, lease, err := decodeAndVerifyPayloadLeaseWithChecksum(payload, len(raw), blockCodecNone, nil, true, true)
	if err != nil {
		t.Fatalf("decodeAndVerifyPayloadLeaseWithChecksum: %v", err)
	}
	if lease != nil {
		t.Fatalf("lease=%v want nil", lease)
	}
	want := payload[blockHeaderSize:]
	if !bytes.Equal(decoded, want) {
		t.Fatalf("decoded payload mismatch")
	}
	if len(decoded) > 0 && &decoded[0] != &want[0] {
		t.Fatalf("allowRawView codec none should return payload view without copy")
	}
}

func TestDecodePayloadLeaseSnappyPreallocatedDstNoAllocs(t *testing.T) {
	raw := bytes.Repeat([]byte("outerleaf-decode-payload-lease|"), 256)
	encoded := snappy.Encode(nil, raw)
	dst := make([]byte, len(raw))

	allocs := testing.AllocsPerRun(1000, func() {
		decoded, lease, err := decodePayloadLease(blockCodecSnappy, encoded, len(raw), dst[:len(raw)])
		if err != nil {
			t.Fatalf("decodePayloadLease: %v", err)
		}
		if lease != nil {
			t.Fatalf("lease=%v want nil", lease)
		}
		if len(decoded) != len(raw) {
			t.Fatalf("decoded len=%d want=%d", len(decoded), len(raw))
		}
	})
	if allocs != 0 {
		t.Fatalf("expected zero allocs per run, got %.2f", allocs)
	}
}

func TestDecodePayloadLeaseErrorReleasesBorrowedBuffer(t *testing.T) {
	drainOuterLeafBytesPoolForTest()
	t.Cleanup(drainOuterLeafBytesPoolForTest)

	rawLen := 128
	invalid := []byte{0x01, 0x02, 0x03}

	if _, lease, err := decodePayloadLease(blockCodecLZ4, invalid, rawLen, nil); err == nil {
		t.Fatalf("expected decode error")
	} else if lease != nil {
		t.Fatalf("lease=%v want nil", lease)
	}

	first := getPooledBytes(rawLen)
	if cap(first) < rawLen {
		t.Fatalf("cap(first)=%d want >=%d", cap(first), rawLen)
	}
	firstProbe := first[:1]
	firstAddr := &firstProbe[0]
	putPooledBytes(first[:0])

	if _, lease, err := decodePayloadLease(blockCodecLZ4, invalid, rawLen, nil); err == nil {
		t.Fatalf("expected decode error on second run")
	} else if lease != nil {
		t.Fatalf("lease=%v want nil on second run", lease)
	}

	second := getPooledBytes(rawLen)
	if cap(second) < rawLen {
		t.Fatalf("cap(second)=%d want >=%d", cap(second), rawLen)
	}
	secondProbe := second[:1]
	if &secondProbe[0] != firstAddr {
		t.Fatalf("borrowed decode buffer was not returned to pool on error path")
	}
	putPooledBytes(second[:0])
}

func TestDecodeAndVerifyPayloadModeWithChecksumCompatibility(t *testing.T) {
	raw := bytes.Repeat([]byte("outerleaf-wrapper-compat"), 128)
	payload := makeCodecNonePayloadForTest(t, raw)

	copied, err := decodeAndVerifyPayloadModeWithChecksum(payload, len(raw), blockCodecNone, nil, false, true)
	if err != nil {
		t.Fatalf("decodeAndVerifyPayloadModeWithChecksum copy path: %v", err)
	}
	if !bytes.Equal(copied, raw) {
		t.Fatalf("copy path payload mismatch")
	}
	want := payload[blockHeaderSize:]
	if len(copied) > 0 && &copied[0] == &want[0] {
		t.Fatalf("allowRawView=false should preserve copy semantics")
	}

	view, err := decodeAndVerifyPayloadModeWithChecksum(payload, len(raw), blockCodecNone, nil, true, true)
	if err != nil {
		t.Fatalf("decodeAndVerifyPayloadModeWithChecksum view path: %v", err)
	}
	if !bytes.Equal(view, raw) {
		t.Fatalf("view path payload mismatch")
	}
	if len(view) > 0 && &view[0] != &want[0] {
		t.Fatalf("allowRawView=true should preserve raw-view semantics")
	}
}

func TestDecodeBlockCompressedPayloadLeaseReleaseIdempotence(t *testing.T) {
	enc, err := EncodeEntries(nil, []Entry{
		{Key: []byte("k1"), Value: bytes.Repeat([]byte("v"), 512)},
		{Key: []byte("k2"), Value: bytes.Repeat([]byte("v"), 512)},
	}, 0, 4)
	if err != nil {
		t.Fatalf("EncodeEntries: %v", err)
	}
	if len(enc) < blockHeaderSize || enc[5] == blockCodecNone {
		t.Fatalf("expected compressed payload for lease coverage, codec=%d", enc[5])
	}
	blk, err := DecodeBlock(enc, nil)
	if err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}
	if blk.rawLease == nil {
		t.Fatalf("rawLease=nil want non-nil for compressed decode without caller scratch")
	}
	blk.Release()
	blk.Release()
	if blk.rawLease != nil {
		t.Fatalf("rawLease should be cleared after Release")
	}
}

func TestEncodeDecodeEntriesLookup(t *testing.T) {
	codecs := []struct {
		name  string
		codec uint8
	}{
		{name: "snappy", codec: 0},
		{name: "lz4", codec: 1},
	}
	entries := []Entry{
		{Key: []byte("acct:0001"), Value: bytes.Repeat([]byte("a"), 64)},
		{Key: []byte("acct:0002"), Value: bytes.Repeat([]byte("b"), 128)},
		{Key: []byte("acct:0003"), Value: bytes.Repeat([]byte("c"), 96)},
	}

	for _, tc := range codecs {
		t.Run(tc.name, func(t *testing.T) {
			enc, err := EncodeEntries(nil, entries, tc.codec, 2)
			if err != nil {
				t.Fatalf("EncodeEntries: %v", err)
			}
			for _, e := range entries {
				got, ok, found, _, err := DecodeValueForKey(enc, e.Key, nil)
				if err != nil {
					t.Fatalf("DecodeValueForKey(%q): %v", string(e.Key), err)
				}
				if !ok || !found {
					t.Fatalf("DecodeValueForKey(%q): ok=%v found=%v", string(e.Key), ok, found)
				}
				if !bytes.Equal(got, e.Value) {
					t.Fatalf("DecodeValueForKey(%q): value mismatch", string(e.Key))
				}
			}
			_, ok, found, _, err := DecodeValueForKey(enc, []byte("acct:9999"), nil)
			if err != nil {
				t.Fatalf("DecodeValueForKey(miss): %v", err)
			}
			if !ok || found {
				t.Fatalf("expected ok=true found=false for miss")
			}
		})
	}
}

func TestEncodeEntriesRequiresIncreasingKeys(t *testing.T) {
	entries := []Entry{
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k1"), Value: []byte("v1")},
	}
	if _, err := EncodeEntries(nil, entries, 0, 16); err == nil {
		t.Fatalf("expected error for unsorted keys")
	}
}

func TestEncoderReuseMatchesStatelessEncode(t *testing.T) {
	var enc Encoder
	batches := [][]Entry{
		{
			{Key: []byte("acct:0001"), Value: bytes.Repeat([]byte("a"), 64)},
			{Key: []byte("acct:0002"), Value: bytes.Repeat([]byte("b"), 96)},
			{Key: []byte("acct:0003"), Value: bytes.Repeat([]byte("c"), 48)},
		},
		{
			{Key: []byte("acct:0100"), Value: bytes.Repeat([]byte("x"), 32)},
			{Key: []byte("acct:0101"), Value: bytes.Repeat([]byte("y"), 40)},
		},
		{
			{Key: []byte("single:key"), Value: bytes.Repeat([]byte("z"), 80)},
		},
	}

	for i := range batches {
		want, err := EncodeEntriesAssumeSorted(nil, batches[i], 0, 8)
		if err != nil {
			t.Fatalf("EncodeEntriesAssumeSorted[%d]: %v", i, err)
		}
		got, err := enc.EncodeEntriesAssumeSorted(nil, batches[i], 0, 8)
		if err != nil {
			t.Fatalf("Encoder.EncodeEntriesAssumeSorted[%d]: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("encoded payload mismatch for batch %d", i)
		}
	}
}

func TestDecodeNonOuterPayload(t *testing.T) {
	raw := []byte("plain value payload")
	gotVal, ok, _, err := DecodeValue(raw, nil)
	if err != nil {
		t.Fatalf("DecodeValue err: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false")
	}
	if gotVal != nil {
		t.Fatalf("expected nil value when ok=false")
	}

	v, ok, found, _, err := DecodeValueForKey(raw, []byte("k"), nil)
	if err != nil {
		t.Fatalf("DecodeValueForKey err: %v", err)
	}
	if ok || found || v != nil {
		t.Fatalf("expected non-outer decode")
	}
}

func TestDecodePayloadRejectsOversizeRawLen(t *testing.T) {
	oldMax := limits.MaxRecordSize
	limits.MaxRecordSize = 16
	t.Cleanup(func() {
		limits.MaxRecordSize = oldMax
	})

	if _, err := decodePayload(blockCodecNone, bytes.Repeat([]byte("x"), 32), 32, nil); err == nil {
		t.Fatalf("expected oversize decodePayload to fail")
	}
}

func TestDecodeBlockValueForKey_RestartIndexed(t *testing.T) {
	entries := make([]Entry, 0, 128)
	for i := 0; i < 128; i++ {
		entries = append(entries, Entry{
			Key:   []byte(fmt.Sprintf("acct:%04d", i)),
			Value: bytes.Repeat([]byte{byte('a' + (i % 26))}, 96),
		})
	}
	enc, err := EncodeEntries(nil, entries, 0, 8)
	if err != nil {
		t.Fatalf("EncodeEntries: %v", err)
	}
	blk, err := DecodeBlock(enc, nil)
	if err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}
	first, err := blk.FirstValue()
	if err != nil {
		t.Fatalf("FirstValue: %v", err)
	}
	if !bytes.Equal(first, entries[0].Value) {
		t.Fatalf("FirstValue mismatch")
	}

	check := func(idx int) {
		t.Helper()
		got, found, err := blk.ValueForKey(entries[idx].Key)
		if err != nil {
			t.Fatalf("ValueForKey(%q): %v", string(entries[idx].Key), err)
		}
		if !found {
			t.Fatalf("ValueForKey(%q): expected found", string(entries[idx].Key))
		}
		if !bytes.Equal(got, entries[idx].Value) {
			t.Fatalf("ValueForKey(%q): value mismatch", string(entries[idx].Key))
		}
	}

	check(0)
	check(7)
	check(8)
	check(63)
	check(127)

	for _, miss := range [][]byte{[]byte("acct:-001"), []byte("acct:9999")} {
		got, found, err := blk.ValueForKey(miss)
		if err != nil {
			t.Fatalf("ValueForKey miss(%q): %v", string(miss), err)
		}
		if found || got != nil {
			t.Fatalf("ValueForKey miss(%q): found=%v got=%v", string(miss), found, got)
		}
	}
}

func TestDecodedBlockLowerBound(t *testing.T) {
	v2Enc, err := EncodeEntries(nil, []Entry{
		{Key: []byte("k10"), Value: []byte("v10")},
		{Key: []byte("k20"), Value: []byte("v20")},
		{Key: []byte("k30"), Value: []byte("v30")},
	}, 0, 4)
	if err != nil {
		t.Fatalf("EncodeEntries(v2): %v", err)
	}
	v2Blk, err := DecodeBlock(v2Enc, nil)
	if err != nil {
		t.Fatalf("DecodeBlock(v2): %v", err)
	}

	v3Enc, err := EncodeTypedEntries(nil, []TypedEntry{
		{Key: []byte("k10"), Kind: EntryKindInline, Value: []byte("v10")},
		{Key: []byte("k20"), Kind: EntryKindInline, Value: []byte("v20")},
		{Key: []byte("k30"), Kind: EntryKindInline, Value: []byte("v30")},
	}, 0, 4)
	if err != nil {
		t.Fatalf("EncodeTypedEntries(v3): %v", err)
	}
	v3Blk, err := DecodeBlock(v3Enc, nil)
	if err != nil {
		t.Fatalf("DecodeBlock(v3): %v", err)
	}

	type want struct {
		pos   int
		below bool
		above bool
	}
	cases := []struct {
		key  []byte
		want want
	}{
		{key: []byte("k05"), want: want{pos: 0, below: true, above: false}},
		{key: []byte("k10"), want: want{pos: 0, below: false, above: false}},
		{key: []byte("k25"), want: want{pos: 2, below: false, above: false}},
		{key: []byte("k30"), want: want{pos: 2, below: false, above: false}},
		{key: []byte("k99"), want: want{pos: 3, below: false, above: true}},
	}

	check := func(name string, blk *DecodedBlock) {
		t.Helper()
		for _, tc := range cases {
			pos, below, above, err := blk.LowerBound(tc.key)
			if err != nil {
				t.Fatalf("%s LowerBound(%q): %v", name, tc.key, err)
			}
			if pos != tc.want.pos || below != tc.want.below || above != tc.want.above {
				t.Fatalf("%s LowerBound(%q)=(pos=%d below=%v above=%v) want (pos=%d below=%v above=%v)",
					name, tc.key, pos, below, above, tc.want.pos, tc.want.below, tc.want.above)
			}
		}
	}

	check("v2", v2Blk)
	check("v3", v3Blk)
}

func TestDecodeLowerBoundAndKeysOnMatchWithVerify(t *testing.T) {
	v2Payload, err := EncodeEntries(nil, []Entry{
		{Key: []byte("k10"), Value: []byte("v10")},
		{Key: []byte("k20"), Value: []byte("v20")},
		{Key: []byte("k30"), Value: []byte("v30")},
	}, 0, 4)
	if err != nil {
		t.Fatalf("EncodeEntries(v2): %v", err)
	}

	pos, below, above, keys, err := DecodeLowerBoundAndKeysOnMatchWithVerify(v2Payload, []byte("k05"), true)
	if err != nil {
		t.Fatalf("DecodeLowerBoundAndKeysOnMatchWithVerify(v2 below): %v", err)
	}
	if pos != 0 || !below || above {
		t.Fatalf("v2 below got (pos=%d below=%v above=%v)", pos, below, above)
	}
	if keys != nil {
		t.Fatalf("v2 below expected nil keys")
	}

	pos, below, above, keys, err = DecodeLowerBoundAndKeysOnMatchWithVerify(v2Payload, []byte("k25"), true)
	if err != nil {
		t.Fatalf("DecodeLowerBoundAndKeysOnMatchWithVerify(v2 within): %v", err)
	}
	if pos != 2 || below || above {
		t.Fatalf("v2 within got (pos=%d below=%v above=%v)", pos, below, above)
	}
	wantV2 := [][]byte{[]byte("k10"), []byte("k20"), []byte("k30")}
	if len(keys) != len(wantV2) {
		t.Fatalf("v2 within keys len=%d want=%d", len(keys), len(wantV2))
	}
	for i := range wantV2 {
		if !bytes.Equal(keys[i], wantV2[i]) {
			t.Fatalf("v2 within keys[%d]=%q want=%q", i, keys[i], wantV2[i])
		}
	}

	pos, below, above, keys, err = DecodeLowerBoundAndKeysOnMatchWithVerify(v2Payload, []byte("k99"), true)
	if err != nil {
		t.Fatalf("DecodeLowerBoundAndKeysOnMatchWithVerify(v2 above): %v", err)
	}
	if pos != 3 || below || !above {
		t.Fatalf("v2 above got (pos=%d below=%v above=%v)", pos, below, above)
	}
	if keys != nil {
		t.Fatalf("v2 above expected nil keys")
	}

	v1Payload, err := EncodeSingle(nil, []byte("k10"), []byte("v10"), 0, 4)
	if err != nil {
		t.Fatalf("EncodeSingle(v1): %v", err)
	}
	pos, below, above, keys, err = DecodeLowerBoundAndKeysOnMatchWithVerify(v1Payload, []byte("k10"), true)
	if err != nil {
		t.Fatalf("DecodeLowerBoundAndKeysOnMatchWithVerify(v1 equal): %v", err)
	}
	if pos != 0 || below || above {
		t.Fatalf("v1 equal got (pos=%d below=%v above=%v)", pos, below, above)
	}
	if len(keys) != 1 || !bytes.Equal(keys[0], []byte("k10")) {
		t.Fatalf("v1 equal keys=%v", keys)
	}
}

func TestDecodeKeysRangeWithVerify(t *testing.T) {
	v2Payload, err := EncodeEntries(nil, []Entry{
		{Key: []byte("k10"), Value: []byte("v10")},
		{Key: []byte("k20"), Value: []byte("v20")},
		{Key: []byte("k30"), Value: []byte("v30")},
		{Key: []byte("k40"), Value: []byte("v40")},
	}, 0, 4)
	if err != nil {
		t.Fatalf("EncodeEntries(v2): %v", err)
	}

	keys, err := DecodeKeysRangeWithVerify(v2Payload, []byte("k15"), []byte("k35"), true)
	if err != nil {
		t.Fatalf("DecodeKeysRangeWithVerify(v2 bounded): %v", err)
	}
	want := [][]byte{[]byte("k20"), []byte("k30")}
	if len(keys) != len(want) {
		t.Fatalf("v2 bounded len=%d want=%d", len(keys), len(want))
	}
	for i := range want {
		if !bytes.Equal(keys[i], want[i]) {
			t.Fatalf("v2 bounded keys[%d]=%q want=%q", i, keys[i], want[i])
		}
	}

	keys, err = DecodeKeysRangeWithVerify(v2Payload, nil, []byte("k20"), true)
	if err != nil {
		t.Fatalf("DecodeKeysRangeWithVerify(v2 upper): %v", err)
	}
	if len(keys) != 1 || !bytes.Equal(keys[0], []byte("k10")) {
		t.Fatalf("v2 upper keys=%q want=[k10]", keys)
	}

	keys, err = DecodeKeysRangeWithVerify(v2Payload, []byte("k50"), nil, true)
	if err != nil {
		t.Fatalf("DecodeKeysRangeWithVerify(v2 empty): %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("v2 empty len=%d want=0", len(keys))
	}

	v1Payload, err := EncodeSingle(nil, []byte("k10"), []byte("v10"), 0, 4)
	if err != nil {
		t.Fatalf("EncodeSingle(v1): %v", err)
	}
	keys, err = DecodeKeysRangeWithVerify(v1Payload, []byte("k00"), []byte("k20"), true)
	if err != nil {
		t.Fatalf("DecodeKeysRangeWithVerify(v1 in-range): %v", err)
	}
	if len(keys) != 1 || !bytes.Equal(keys[0], []byte("k10")) {
		t.Fatalf("v1 in-range keys=%q want=[k10]", keys)
	}
	keys, err = DecodeKeysRangeWithVerify(v1Payload, []byte("k20"), []byte("k30"), true)
	if err != nil {
		t.Fatalf("DecodeKeysRangeWithVerify(v1 miss): %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("v1 miss len=%d want=0", len(keys))
	}
}

func TestDecodeKeysRangeLeaseWithVerify(t *testing.T) {
	payload, err := EncodeEntries(nil, []Entry{
		{Key: []byte("k10"), Value: []byte("v10")},
		{Key: []byte("k20"), Value: []byte("v20")},
		{Key: []byte("k30"), Value: []byte("v30")},
		{Key: []byte("k40"), Value: []byte("v40")},
	}, 0, 4)
	if err != nil {
		t.Fatalf("EncodeEntries(v2): %v", err)
	}

	lease, err := DecodeKeysRangeLeaseWithVerify(payload, []byte("k15"), []byte("k35"), true)
	if err != nil {
		t.Fatalf("DecodeKeysRangeLeaseWithVerify: %v", err)
	}
	if lease == nil {
		t.Fatalf("lease=nil want non-nil")
	}
	keys := lease.Keys()
	want := [][]byte{[]byte("k20"), []byte("k30")}
	if len(keys) != len(want) {
		t.Fatalf("keys len=%d want=%d", len(keys), len(want))
	}
	for i := range want {
		if !bytes.Equal(keys[i], want[i]) {
			t.Fatalf("keys[%d]=%q want=%q", i, keys[i], want[i])
		}
	}
	lease.Release()
	lease.Release()
}

func TestDecodeLowerBoundAndKeysOnMatchLeaseWithVerify(t *testing.T) {
	payload, err := EncodeEntries(nil, []Entry{
		{Key: []byte("k10"), Value: []byte("v10")},
		{Key: []byte("k20"), Value: []byte("v20")},
		{Key: []byte("k30"), Value: []byte("v30")},
	}, 0, 4)
	if err != nil {
		t.Fatalf("EncodeEntries(v2): %v", err)
	}

	pos, below, above, lease, err := DecodeLowerBoundAndKeysOnMatchLeaseWithVerify(payload, []byte("k20"), true)
	if err != nil {
		t.Fatalf("DecodeLowerBoundAndKeysOnMatchLeaseWithVerify: %v", err)
	}
	if pos != 1 || below || above {
		t.Fatalf("got pos=%d below=%v above=%v", pos, below, above)
	}
	if lease == nil {
		t.Fatalf("lease=nil want non-nil")
	}
	keys := lease.Keys()
	want := [][]byte{[]byte("k10"), []byte("k20"), []byte("k30")}
	if len(keys) != len(want) {
		t.Fatalf("keys len=%d want=%d", len(keys), len(want))
	}
	for i := range want {
		if !bytes.Equal(keys[i], want[i]) {
			t.Fatalf("keys[%d]=%q want=%q", i, keys[i], want[i])
		}
	}
	lease.Release()

	pos, below, above, lease, err = DecodeLowerBoundAndKeysOnMatchLeaseWithVerify(payload, []byte("k09"), true)
	if err != nil {
		t.Fatalf("DecodeLowerBoundAndKeysOnMatchLeaseWithVerify below: %v", err)
	}
	if pos != 0 || !below || above || lease != nil {
		t.Fatalf("below got pos=%d below=%v above=%v lease=%v", pos, below, above, lease)
	}
}

func TestGetPooledLeaseKeys_RequeuesUndersizedBuffer(t *testing.T) {
	for outerLeafLeaseKeysPool.Get() != nil {
	}
	t.Cleanup(func() {
		for outerLeafLeaseKeysPool.Get() != nil {
		}
	})

	undersized := make([][]byte, 0, 2)
	outerLeafLeaseKeysPool.Put(undersized)

	got := getPooledLeaseKeys(4)
	if cap(got) < 4 {
		t.Fatalf("cap(got)=%d want >=4", cap(got))
	}

	found := false
	for {
		v := outerLeafLeaseKeysPool.Get()
		if v == nil {
			break
		}
		keys, ok := v.([][]byte)
		if !ok {
			t.Fatalf("pool type=%T want [][]byte", v)
		}
		if cap(keys) == cap(undersized) {
			found = true
		}
	}
	if !found {
		t.Fatalf("undersized slice was not returned to pool")
	}
}

func TestGetPooledLeaseArena_RequeuesUndersizedArena(t *testing.T) {
	for outerLeafLeaseArenaPool.Get() != nil {
	}
	t.Cleanup(func() {
		for outerLeafLeaseArenaPool.Get() != nil {
		}
	})

	undersized := make([]byte, 0, 8)
	outerLeafLeaseArenaPool.Put(undersized)

	got := getPooledLeaseArena(16)
	if cap(got) < 16 {
		t.Fatalf("cap(got)=%d want >=16", cap(got))
	}

	found := false
	for {
		v := outerLeafLeaseArenaPool.Get()
		if v == nil {
			break
		}
		arena, ok := v.([]byte)
		if !ok {
			t.Fatalf("pool type=%T want []byte", v)
		}
		if cap(arena) == cap(undersized) {
			found = true
		}
	}
	if !found {
		t.Fatalf("undersized arena was not returned to pool")
	}
}

func TestDecodedBlockKeysRange(t *testing.T) {
	enc, err := EncodeEntries(nil, []Entry{
		{Key: []byte("k10"), Value: []byte("v10")},
		{Key: []byte("k20"), Value: []byte("v20")},
		{Key: []byte("k30"), Value: []byte("v30")},
		{Key: []byte("k40"), Value: []byte("v40")},
	}, 0, 4)
	if err != nil {
		t.Fatalf("EncodeEntries: %v", err)
	}
	blk, err := DecodeBlock(enc, nil)
	if err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}

	keys, err := blk.KeysRange(nil, []byte("k15"), []byte("k35"))
	if err != nil {
		t.Fatalf("KeysRange bounded: %v", err)
	}
	want := [][]byte{[]byte("k20"), []byte("k30")}
	if len(keys) != len(want) {
		t.Fatalf("KeysRange bounded len=%d want=%d", len(keys), len(want))
	}
	for i := range want {
		if !bytes.Equal(keys[i], want[i]) {
			t.Fatalf("KeysRange bounded[%d]=%q want=%q", i, keys[i], want[i])
		}
	}

	keys, err = blk.KeysRange(nil, []byte("k99"), nil)
	if err != nil {
		t.Fatalf("KeysRange empty: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("KeysRange empty len=%d want=0", len(keys))
	}
}

func TestDecodedBlockEntries(t *testing.T) {
	v2Entries := []Entry{
		{Key: []byte("acct:0001"), Value: bytes.Repeat([]byte("a"), 32)},
		{Key: []byte("acct:0002"), Value: bytes.Repeat([]byte("b"), 48)},
		{Key: []byte("acct:0100"), Value: bytes.Repeat([]byte("c"), 24)},
	}
	enc, err := EncodeEntries(nil, v2Entries, 0, 4)
	if err != nil {
		t.Fatalf("EncodeEntries: %v", err)
	}
	blk, err := DecodeBlock(enc, nil)
	if err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}
	got, err := blk.Entries(nil)
	if err != nil {
		t.Fatalf("Entries(v2): %v", err)
	}
	if len(got) != len(v2Entries) {
		t.Fatalf("Entries(v2) len=%d want=%d", len(got), len(v2Entries))
	}
	for i := range v2Entries {
		if !bytes.Equal(got[i].Key, v2Entries[i].Key) {
			t.Fatalf("Entries(v2)[%d] key mismatch got=%q want=%q", i, got[i].Key, v2Entries[i].Key)
		}
		if !bytes.Equal(got[i].Value, v2Entries[i].Value) {
			t.Fatalf("Entries(v2)[%d] value mismatch", i)
		}
	}
	keys, err := blk.Keys(nil)
	if err != nil {
		t.Fatalf("Keys(v2): %v", err)
	}
	if len(keys) != len(v2Entries) {
		t.Fatalf("Keys(v2) len=%d want=%d", len(keys), len(v2Entries))
	}
	for i := range v2Entries {
		if !bytes.Equal(keys[i], v2Entries[i].Key) {
			t.Fatalf("Keys(v2)[%d] key mismatch got=%q want=%q", i, keys[i], v2Entries[i].Key)
		}
	}
	// Repeated nil-dst calls should reuse decoded key materialization.
	keys2, err := blk.Keys(nil)
	if err != nil {
		t.Fatalf("Keys(v2) second call: %v", err)
	}
	if len(keys2) != len(keys) {
		t.Fatalf("Keys(v2) second call len=%d want=%d", len(keys2), len(keys))
	}
	for i := range keys {
		if !bytes.Equal(keys2[i], keys[i]) {
			t.Fatalf("Keys(v2) second call key[%d] mismatch", i)
		}
	}
	keysCopy, err := blk.Keys(make([][]byte, 0, len(v2Entries)))
	if err != nil {
		t.Fatalf("Keys(v2) dst call: %v", err)
	}
	if len(keysCopy) != len(v2Entries) {
		t.Fatalf("Keys(v2) dst call len=%d want=%d", len(keysCopy), len(v2Entries))
	}
	for i := range v2Entries {
		if !bytes.Equal(keysCopy[i], v2Entries[i].Key) {
			t.Fatalf("Keys(v2) dst call key[%d] mismatch got=%q want=%q", i, keysCopy[i], v2Entries[i].Key)
		}
	}

	v1Key := []byte("single:key")
	v1Val := bytes.Repeat([]byte("z"), 40)
	enc, err = EncodeSingle(nil, v1Key, v1Val, 0, 16)
	if err != nil {
		t.Fatalf("EncodeSingle: %v", err)
	}
	blk, err = DecodeBlock(enc, nil)
	if err != nil {
		t.Fatalf("DecodeBlock(v1): %v", err)
	}
	got, err = blk.Entries(nil)
	if err != nil {
		t.Fatalf("Entries(v1): %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Entries(v1) len=%d want=1", len(got))
	}
	if !bytes.Equal(got[0].Key, v1Key) || !bytes.Equal(got[0].Value, v1Val) {
		t.Fatalf("Entries(v1) mismatch")
	}
	keys, err = blk.Keys(nil)
	if err != nil {
		t.Fatalf("Keys(v1): %v", err)
	}
	if len(keys) != 1 || !bytes.Equal(keys[0], v1Key) {
		t.Fatalf("Keys(v1) mismatch")
	}
}

func TestEncodeDecodeTypedEntriesV3InlineRoundTrip(t *testing.T) {
	entries := []TypedEntry{
		{Key: []byte("k1"), Kind: EntryKindInline, Value: []byte("value-1")},
	}
	enc, err := EncodeTypedEntries(nil, entries, 0, 4)
	if err != nil {
		t.Fatalf("EncodeTypedEntries: %v", err)
	}
	blk, err := DecodeBlock(enc, nil)
	if err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}
	typed, err := blk.TypedEntries(nil)
	if err != nil {
		t.Fatalf("TypedEntries: %v", err)
	}
	if len(typed) != 1 {
		t.Fatalf("TypedEntries len=%d want=1", len(typed))
	}
	if typed[0].Kind != EntryKindInline {
		t.Fatalf("entry kind=%d want=%d", typed[0].Kind, EntryKindInline)
	}
	if !bytes.Equal(typed[0].Key, entries[0].Key) || !bytes.Equal(typed[0].Value, entries[0].Value) {
		t.Fatalf("typed entry mismatch")
	}
}

func TestEncodeDecodeTypedEntriesV3BlobRefRoundTrip(t *testing.T) {
	ptr := page.ValuePtr{FileID: page.ValueLogFileID(2), Offset: 1234, Length: 56}
	enc, err := EncodeSingleBlobRef(nil, []byte("blob-k"), ptr, 0, 4)
	if err != nil {
		t.Fatalf("EncodeSingleBlobRef: %v", err)
	}

	_, ok, found, _, err := DecodeValueForKey(enc, []byte("blob-k"), nil)
	if err == nil {
		t.Fatalf("expected DecodeValueForKey to require typed blob-ref resolution")
	}
	if err != ErrBlobRefEntry {
		t.Fatalf("DecodeValueForKey err=%v want=%v", err, ErrBlobRefEntry)
	}
	if !ok || !found {
		t.Fatalf("DecodeValueForKey ok=%v found=%v", ok, found)
	}

	entry, ok, found, _, err := DecodeEntryForKey(enc, []byte("blob-k"), nil)
	if err != nil {
		t.Fatalf("DecodeEntryForKey: %v", err)
	}
	if !ok || !found {
		t.Fatalf("DecodeEntryForKey ok=%v found=%v", ok, found)
	}
	if entry.Kind != EntryKindBlobRef {
		t.Fatalf("entry kind=%d want=%d", entry.Kind, EntryKindBlobRef)
	}
	if entry.BlobPtr != ptr {
		t.Fatalf("blob ptr mismatch got=%+v want=%+v", entry.BlobPtr, ptr)
	}
}

func TestEncodeDecodeTypedEntriesV3MixedLookup(t *testing.T) {
	ptr := page.ValuePtr{FileID: page.ValueLogFileID(3), Offset: 4321, Length: 88}
	entries := []TypedEntry{
		{Key: []byte("k1"), Kind: EntryKindInline, Value: []byte("inline-1")},
		{Key: []byte("k2"), Kind: EntryKindBlobRef, BlobPtr: ptr},
		{Key: []byte("k3"), Kind: EntryKindInline, Value: []byte("inline-3")},
	}
	enc, err := EncodeTypedEntries(nil, entries, 0, 2)
	if err != nil {
		t.Fatalf("EncodeTypedEntries: %v", err)
	}
	blk, err := DecodeBlock(enc, nil)
	if err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}
	got1, found, err := blk.EntryForKey([]byte("k1"))
	if err != nil || !found {
		t.Fatalf("EntryForKey(k1) found=%v err=%v", found, err)
	}
	if got1.Kind != EntryKindInline || !bytes.Equal(got1.Value, []byte("inline-1")) {
		t.Fatalf("EntryForKey(k1) mismatch")
	}
	got2, found, err := blk.EntryForKey([]byte("k2"))
	if err != nil || !found {
		t.Fatalf("EntryForKey(k2) found=%v err=%v", found, err)
	}
	if got2.Kind != EntryKindBlobRef || got2.BlobPtr != ptr {
		t.Fatalf("EntryForKey(k2) mismatch got=%+v", got2)
	}
	if _, _, err := blk.ValueForKey([]byte("k2")); err != ErrBlobRefEntry {
		t.Fatalf("ValueForKey(k2) err=%v want=%v", err, ErrBlobRefEntry)
	}
}

func BenchmarkDecodedBlockValueForKeyV2(b *testing.B) {
	entries := make([]Entry, 0, 256)
	for i := 0; i < 256; i++ {
		entries = append(entries, Entry{
			Key:   []byte(fmt.Sprintf("k:%06d", i)),
			Value: bytes.Repeat([]byte{byte('a' + (i % 26))}, 128),
		})
	}
	enc, err := EncodeEntries(nil, entries, 0, 16)
	if err != nil {
		b.Fatalf("EncodeEntries: %v", err)
	}
	blk, err := DecodeBlock(enc, nil)
	if err != nil {
		b.Fatalf("DecodeBlock: %v", err)
	}
	keys := make([][]byte, len(entries))
	for i := range entries {
		keys[i] = entries[i].Key
	}
	miss := []byte("k:999999")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		if i&7 == 0 {
			key = miss
		}
		if _, _, err := blk.ValueForKey(key); err != nil {
			b.Fatalf("ValueForKey: %v", err)
		}
	}
}
