package outerleaf

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/golang/snappy"
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
