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

func TestDecodeBlockLeaseRelease(t *testing.T) {
	entries := []Entry{
		{Key: []byte("acct:0001"), Value: bytes.Repeat([]byte("a"), 512)},
		{Key: []byte("acct:0002"), Value: bytes.Repeat([]byte("b"), 512)},
		{Key: []byte("acct:0003"), Value: bytes.Repeat([]byte("c"), 512)},
	}
	enc, err := EncodeEntries(nil, entries, 0, 2)
	if err != nil {
		t.Fatalf("EncodeEntries: %v", err)
	}
	if enc[5] == blockCodecNone {
		t.Fatalf("expected compressed payload for lease test")
	}
	blk, err := DecodeBlockLease(enc)
	if err != nil {
		t.Fatalf("DecodeBlockLease: %v", err)
	}
	if _, err := blk.FirstValue(); err != nil {
		t.Fatalf("FirstValue: %v", err)
	}
	if _, err := blk.lookupRestarts(); err != nil {
		t.Fatalf("lookupRestarts: %v", err)
	}
	if _, err := blk.lookupRestartKeys(); err != nil {
		t.Fatalf("lookupRestartKeys: %v", err)
	}
	blk.Release()
	blk.Release()
}

func TestDecodeBlockLeaseWithScratchAndVerify_TransfersPooledRawToCallerScratch(t *testing.T) {
	entries := []Entry{
		{Key: []byte("acct:0001"), Value: bytes.Repeat([]byte("a"), 1024)},
		{Key: []byte("acct:0002"), Value: bytes.Repeat([]byte("b"), 1024)},
		{Key: []byte("acct:0003"), Value: bytes.Repeat([]byte("c"), 1024)},
	}
	enc, err := EncodeEntries(nil, entries, 0, 2)
	if err != nil {
		t.Fatalf("EncodeEntries: %v", err)
	}
	if len(enc) < blockHeaderSize || enc[5] == blockCodecNone {
		t.Fatalf("expected compressed payload for scratch ownership transfer test")
	}

	dst := &DecodedBlock{}
	blk, nextScratch, err := DecodeBlockLeaseWithScratchAndVerify(enc, nil, dst, true)
	if err != nil {
		t.Fatalf("DecodeBlockLeaseWithScratchAndVerify(first): %v", err)
	}
	if blk == nil {
		t.Fatalf("decoded block=nil")
	}
	if blk != dst {
		t.Fatalf("decoded block pointer mismatch")
	}
	if cap(nextScratch) == 0 {
		t.Fatalf("nextScratch cap=0 want >0")
	}
	if blk.pooledRaw {
		t.Fatalf("blk.pooledRaw=true want false after ownership transfer to caller scratch")
	}
	if len(blk.raw) == 0 {
		t.Fatalf("blk.raw len=0 want >0")
	}
	if &blk.raw[0] != &nextScratch[:1:1][0] {
		t.Fatalf("nextScratch does not alias decoded raw backing")
	}
	if _, found, err := blk.EntryForKey([]byte("acct:0002")); err != nil {
		t.Fatalf("EntryForKey(first): %v", err)
	} else if !found {
		t.Fatalf("EntryForKey(first) found=false want true")
	}
	blk.Release()

	blk2, nextScratch2, err := DecodeBlockLeaseWithScratchAndVerify(enc, nextScratch, dst, true)
	if err != nil {
		t.Fatalf("DecodeBlockLeaseWithScratchAndVerify(second): %v", err)
	}
	if blk2 == nil {
		t.Fatalf("decoded block=nil on second decode")
	}
	if blk2.pooledRaw {
		t.Fatalf("blk2.pooledRaw=true want false")
	}
	if len(blk2.raw) == 0 {
		t.Fatalf("blk2.raw len=0 want >0")
	}
	if cap(nextScratch2) == 0 {
		t.Fatalf("nextScratch2 cap=0 want >0")
	}
	if &blk2.raw[0] != &nextScratch2[:1:1][0] {
		t.Fatalf("nextScratch2 does not alias decoded raw backing")
	}
	if _, found, err := blk2.EntryForKey([]byte("acct:0003")); err != nil {
		t.Fatalf("EntryForKey(second): %v", err)
	} else if !found {
		t.Fatalf("EntryForKey(second) found=false want true")
	}
	blk2.Release()
}

func TestDecodedBlockReclaimTransferredScratchForRelease_ZeroLenScratch(t *testing.T) {
	entries := []Entry{
		{Key: []byte("acct:0001"), Value: bytes.Repeat([]byte("a"), 1024)},
		{Key: []byte("acct:0002"), Value: bytes.Repeat([]byte("b"), 1024)},
		{Key: []byte("acct:0003"), Value: bytes.Repeat([]byte("c"), 1024)},
	}
	enc, err := EncodeEntries(nil, entries, 0, 2)
	if err != nil {
		t.Fatalf("EncodeEntries: %v", err)
	}
	if len(enc) < blockHeaderSize || enc[5] == blockCodecNone {
		t.Fatalf("expected compressed payload for scratch reclaim test")
	}

	dst := &DecodedBlock{}
	blk, nextScratch, err := DecodeBlockLeaseWithScratchAndVerify(enc, nil, dst, true)
	if err != nil {
		t.Fatalf("DecodeBlockLeaseWithScratchAndVerify: %v", err)
	}
	if blk == nil {
		t.Fatalf("decoded block=nil")
	}
	if cap(nextScratch) == 0 {
		t.Fatalf("nextScratch cap=0 want >0")
	}

	var reclaimed []byte
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				t.Fatalf("ReclaimTransferredScratchForRelease panicked: %v", rec)
			}
		}()
		reclaimed = blk.ReclaimTransferredScratchForRelease(nextScratch[:0])
	}()
	if reclaimed != nil {
		t.Fatalf("reclaimed scratch=%v want nil", reclaimed)
	}
	blk.Release()
}

func TestDecodeRestartsFromRawIntoReusesDst(t *testing.T) {
	enc, err := EncodeEntries(nil, []Entry{
		{Key: []byte("k10"), Value: []byte("v10")},
		{Key: []byte("k20"), Value: []byte("v20")},
		{Key: []byte("k30"), Value: []byte("v30")},
		{Key: []byte("k40"), Value: []byte("v40")},
	}, 0, 2)
	if err != nil {
		t.Fatalf("EncodeEntries: %v", err)
	}
	blk, err := DecodeBlock(enc, nil)
	if err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}
	if blk.restartCount <= 0 {
		t.Fatalf("restartCount=%d want >0", blk.restartCount)
	}

	dst := make([]uint32, blk.restartCount)
	restarts, err := decodeRestartsFromRawInto(blk.entries, blk.restartRaw, blk.restartCount, dst[:0])
	if err != nil {
		t.Fatalf("decodeRestartsFromRawInto: %v", err)
	}
	if len(restarts) != blk.restartCount {
		t.Fatalf("len(restarts)=%d want=%d", len(restarts), blk.restartCount)
	}
	if &restarts[0] != &dst[0] {
		t.Fatalf("decodeRestartsFromRawInto did not reuse caller dst")
	}
}

func TestDecodeV2RestartKeysIntoReusesDst(t *testing.T) {
	enc, err := EncodeEntries(nil, []Entry{
		{Key: []byte("k10"), Value: []byte("v10")},
		{Key: []byte("k20"), Value: []byte("v20")},
		{Key: []byte("k30"), Value: []byte("v30")},
		{Key: []byte("k40"), Value: []byte("v40")},
	}, 0, 2)
	if err != nil {
		t.Fatalf("EncodeEntries: %v", err)
	}
	blk, err := DecodeBlock(enc, nil)
	if err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}
	restarts, err := decodeRestartsFromRaw(blk.entries, blk.restartRaw, blk.restartCount)
	if err != nil {
		t.Fatalf("decodeRestartsFromRaw: %v", err)
	}
	if len(restarts) == 0 {
		t.Fatalf("expected non-empty restart table")
	}

	dst := make([][]byte, len(restarts))
	keys, err := decodeV2RestartKeysInto(blk.entries, restarts, dst[:0])
	if err != nil {
		t.Fatalf("decodeV2RestartKeysInto: %v", err)
	}
	if len(keys) != len(restarts) {
		t.Fatalf("len(keys)=%d want=%d", len(keys), len(restarts))
	}
	if &keys[0] != &dst[0] {
		t.Fatalf("decodeV2RestartKeysInto did not reuse caller dst")
	}
}

func TestRestartDecodeModeForCountBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		count   int
		want    restartDecodeMode
		wantErr string
	}{
		{name: "negative", count: -1, want: restartDecodeModeNone, wantErr: "outerleaf: invalid restart count -1"},
		{name: "zero", count: 0, want: restartDecodeModeNone},
		{name: "stack_cap", count: restartDecodeStackCap, want: restartDecodeModeStack},
		{name: "pooled", count: restartDecodeStackCap + 1, want: restartDecodeModePooled},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := restartDecodeModeForCount(tc.count)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("restartDecodeModeForCount(%d) err=nil want=%q", tc.count, tc.wantErr)
				}
				if err.Error() != tc.wantErr {
					t.Fatalf("restartDecodeModeForCount(%d) err=%q want=%q", tc.count, err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("restartDecodeModeForCount(%d): %v", tc.count, err)
			}
			if got != tc.want {
				t.Fatalf("restartDecodeModeForCount(%d)=%v want=%v", tc.count, got, tc.want)
			}
		})
	}
}

func TestLookupV2ValueFromRestartRaw_BoundaryMatchesDecodedRestarts(t *testing.T) {
	for _, count := range []int{restartDecodeStackCap, restartDecodeStackCap + 1} {
		t.Run(fmt.Sprintf("restart_count_%d", count), func(t *testing.T) {
			entries := make([]Entry, count)
			for i := 0; i < count; i++ {
				entries[i] = Entry{
					Key:   []byte(fmt.Sprintf("k%04d", i)),
					Value: []byte(fmt.Sprintf("v%04d", i)),
				}
			}
			enc, err := EncodeEntries(nil, entries, 0, 1)
			if err != nil {
				t.Fatalf("EncodeEntries: %v", err)
			}
			blk, err := DecodeBlock(enc, nil)
			if err != nil {
				t.Fatalf("DecodeBlock: %v", err)
			}
			if blk.restartCount != count {
				t.Fatalf("restartCount=%d want=%d", blk.restartCount, count)
			}

			key := []byte(fmt.Sprintf("k%04d", count-1))
			restarts, err := decodeRestartsFromRaw(blk.entries, blk.restartRaw, blk.restartCount)
			if err != nil {
				t.Fatalf("decodeRestartsFromRaw: %v", err)
			}
			want, wantFound, err := lookupV2Value(blk.entries, blk.entryCount, key, restarts, nil)
			if err != nil {
				t.Fatalf("lookupV2Value: %v", err)
			}
			got, gotFound, err := lookupV2ValueFromRestartRaw(blk.entries, blk.entryCount, key, blk.restartRaw, blk.restartCount)
			if err != nil {
				t.Fatalf("lookupV2ValueFromRestartRaw: %v", err)
			}
			if gotFound != wantFound {
				t.Fatalf("found=%v want=%v", gotFound, wantFound)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("value=%q want=%q", got, want)
			}
		})
	}
}

func TestLookupV3EntryFromRestartRaw_BoundaryMatchesDecodedRestarts(t *testing.T) {
	for _, count := range []int{restartDecodeStackCap, restartDecodeStackCap + 1} {
		t.Run(fmt.Sprintf("restart_count_%d", count), func(t *testing.T) {
			entries := make([]TypedEntry, count)
			for i := 0; i < count; i++ {
				entries[i] = TypedEntry{
					Key:   []byte(fmt.Sprintf("k%04d", i)),
					Kind:  EntryKindInline,
					Value: []byte(fmt.Sprintf("v%04d", i)),
				}
			}
			enc, err := EncodeTypedEntries(nil, entries, 0, 1)
			if err != nil {
				t.Fatalf("EncodeTypedEntries: %v", err)
			}
			blk, err := DecodeBlock(enc, nil)
			if err != nil {
				t.Fatalf("DecodeBlock: %v", err)
			}
			if blk.restartCount != count {
				t.Fatalf("restartCount=%d want=%d", blk.restartCount, count)
			}

			key := []byte(fmt.Sprintf("k%04d", count-1))
			restarts, err := decodeRestartsFromRaw(blk.entries, blk.restartRaw, blk.restartCount)
			if err != nil {
				t.Fatalf("decodeRestartsFromRaw: %v", err)
			}
			want, wantFound, err := lookupV3Entry(blk.entries, blk.entryCount, key, restarts, nil)
			if err != nil {
				t.Fatalf("lookupV3Entry: %v", err)
			}
			got, gotFound, err := lookupV3EntryFromRestartRaw(blk.entries, blk.entryCount, key, blk.restartRaw, blk.restartCount)
			if err != nil {
				t.Fatalf("lookupV3EntryFromRestartRaw: %v", err)
			}
			if gotFound != wantFound {
				t.Fatalf("found=%v want=%v", gotFound, wantFound)
			}
			if got.Kind != want.Kind {
				t.Fatalf("kind=%v want=%v", got.Kind, want.Kind)
			}
			if !bytes.Equal(got.Value, want.Value) {
				t.Fatalf("value=%q want=%q", got.Value, want.Value)
			}
			if got.BlobPtr != want.BlobPtr {
				t.Fatalf("blobPtr=%+v want=%+v", got.BlobPtr, want.BlobPtr)
			}
		})
	}
}

func TestLookupFromRestartRaw_RejectsNegativeRestartCount(t *testing.T) {
	v2Enc, err := EncodeEntries(nil, []Entry{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}, 0, 1)
	if err != nil {
		t.Fatalf("EncodeEntries: %v", err)
	}
	v2Block, err := DecodeBlock(v2Enc, nil)
	if err != nil {
		t.Fatalf("DecodeBlock(v2): %v", err)
	}
	if _, _, err := lookupV2ValueFromRestartRaw(v2Block.entries, v2Block.entryCount, []byte("k1"), v2Block.restartRaw, -1); err == nil {
		t.Fatalf("lookupV2ValueFromRestartRaw err=nil want invalid restart count")
	} else if got := err.Error(); got != "outerleaf: invalid restart count -1" {
		t.Fatalf("lookupV2ValueFromRestartRaw err=%q want=%q", got, "outerleaf: invalid restart count -1")
	}

	v3Enc, err := EncodeTypedEntries(nil, []TypedEntry{
		{Key: []byte("k1"), Kind: EntryKindInline, Value: []byte("v1")},
		{Key: []byte("k2"), Kind: EntryKindInline, Value: []byte("v2")},
	}, 0, 1)
	if err != nil {
		t.Fatalf("EncodeTypedEntries: %v", err)
	}
	v3Block, err := DecodeBlock(v3Enc, nil)
	if err != nil {
		t.Fatalf("DecodeBlock(v3): %v", err)
	}
	if _, _, err := lookupV3EntryFromRestartRaw(v3Block.entries, v3Block.entryCount, []byte("k1"), v3Block.restartRaw, -1); err == nil {
		t.Fatalf("lookupV3EntryFromRestartRaw err=nil want invalid restart count")
	} else if got := err.Error(); got != "outerleaf: invalid restart count -1" {
		t.Fatalf("lookupV3EntryFromRestartRaw err=%q want=%q", got, "outerleaf: invalid restart count -1")
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

func TestDecodeValueForEmptyKeyDoesNotMeanFirstEntry(t *testing.T) {
	v1, err := EncodeSingle(nil, []byte("a"), []byte("va"), 0, 16)
	if err != nil {
		t.Fatalf("EncodeSingle: %v", err)
	}
	if got, ok, found, _, err := DecodeValueForKey(v1, []byte{}, nil); err != nil || !ok || found || got != nil {
		t.Fatalf("v1 empty miss got value=%q ok=%v found=%v err=%v", got, ok, found, err)
	}
	if got, ok, found, _, err := DecodeValueForKey(v1, nil, nil); err != nil || !ok || !found || !bytes.Equal(got, []byte("va")) {
		t.Fatalf("v1 nil first got value=%q ok=%v found=%v err=%v", got, ok, found, err)
	}

	v2, err := EncodeEntries(nil, []Entry{
		{Key: []byte("a"), Value: []byte("va")},
		{Key: []byte("b"), Value: []byte("vb")},
	}, 0, 1)
	if err != nil {
		t.Fatalf("EncodeEntries(v2): %v", err)
	}
	if got, ok, found, _, err := DecodeValueForKey(v2, []byte{}, nil); err != nil || !ok || found || got != nil {
		t.Fatalf("v2 empty miss got value=%q ok=%v found=%v err=%v", got, ok, found, err)
	}

	v3, err := EncodeTypedEntries(nil, []TypedEntry{
		{Key: []byte("a"), Kind: EntryKindInline, Value: []byte("va")},
		{Key: []byte("b"), Kind: EntryKindInline, Value: []byte("vb")},
	}, 0, 1)
	if err != nil {
		t.Fatalf("EncodeTypedEntries(v3): %v", err)
	}
	if entry, ok, found, _, err := DecodeEntryForKey(v3, []byte{}, nil); err != nil || !ok || found || entry.Value != nil {
		t.Fatalf("v3 empty miss got entry=%+v ok=%v found=%v err=%v", entry, ok, found, err)
	}
}

func TestDecodeValueForConcreteEmptyKey(t *testing.T) {
	v1, err := EncodeSingle(nil, []byte{}, []byte("v-empty"), 0, 16)
	if err != nil {
		t.Fatalf("EncodeSingle: %v", err)
	}
	if got, ok, found, _, err := DecodeValueForKey(v1, []byte{}, nil); err != nil || !ok || !found || !bytes.Equal(got, []byte("v-empty")) {
		t.Fatalf("v1 empty hit got value=%q ok=%v found=%v err=%v", got, ok, found, err)
	}

	v2, err := EncodeEntries(nil, []Entry{
		{Key: []byte{}, Value: []byte("v-empty")},
		{Key: []byte("a"), Value: []byte("va")},
	}, 0, 1)
	if err != nil {
		t.Fatalf("EncodeEntries(v2): %v", err)
	}
	if got, ok, found, _, err := DecodeValueForKey(v2, []byte{}, nil); err != nil || !ok || !found || !bytes.Equal(got, []byte("v-empty")) {
		t.Fatalf("v2 empty hit got value=%q ok=%v found=%v err=%v", got, ok, found, err)
	}

	v3, err := EncodeTypedEntries(nil, []TypedEntry{
		{Key: []byte{}, Kind: EntryKindInline, Value: []byte("v-empty")},
		{Key: []byte("a"), Kind: EntryKindInline, Value: []byte("va")},
	}, 0, 1)
	if err != nil {
		t.Fatalf("EncodeTypedEntries(v3): %v", err)
	}
	entry, ok, found, _, err := DecodeEntryForKey(v3, []byte{}, nil)
	if err != nil || !ok || !found || entry.Kind != EntryKindInline || !bytes.Equal(entry.Value, []byte("v-empty")) {
		t.Fatalf("v3 empty hit got entry=%+v ok=%v found=%v err=%v", entry, ok, found, err)
	}
}

func TestDecodeKeysRangeEmptyUpperBoundIsNotUnbounded(t *testing.T) {
	payload, err := EncodeEntries(nil, []Entry{
		{Key: []byte{}, Value: []byte("v-empty")},
		{Key: []byte("a"), Value: []byte("va")},
	}, 0, 1)
	if err != nil {
		t.Fatalf("EncodeEntries: %v", err)
	}
	keys, err := DecodeKeysRangeWithVerify(payload, nil, []byte{}, true)
	if err != nil {
		t.Fatalf("DecodeKeysRangeWithVerify: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("upper empty returned %d keys, want 0: %q", len(keys), keys)
	}

	blk, err := DecodeBlock(payload, nil)
	if err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}
	keys, err = blk.KeysRange(nil, nil, []byte{})
	if err != nil {
		t.Fatalf("KeysRange: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("decoded upper empty returned %d keys, want 0: %q", len(keys), keys)
	}
}

func TestLowerBoundEmptyTargetIsConcrete(t *testing.T) {
	payload, err := EncodeEntries(nil, []Entry{
		{Key: []byte("a"), Value: []byte("va")},
		{Key: []byte("b"), Value: []byte("vb")},
	}, 0, 1)
	if err != nil {
		t.Fatalf("EncodeEntries: %v", err)
	}
	blk, err := DecodeBlock(payload, nil)
	if err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}
	pos, below, above, err := blk.LowerBound([]byte{})
	if err != nil {
		t.Fatalf("LowerBound(empty): %v", err)
	}
	if pos != 0 || !below || above {
		t.Fatalf("LowerBound(empty)=(pos=%d below=%v above=%v), want (0,true,false)", pos, below, above)
	}
	pos, below, above, err = blk.LowerBound(nil)
	if err != nil {
		t.Fatalf("LowerBound(nil): %v", err)
	}
	if pos != 0 || below || above {
		t.Fatalf("LowerBound(nil)=(pos=%d below=%v above=%v), want (0,false,false)", pos, below, above)
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

func TestDecodedBlockLowerBound_NoAllocsForSmallKeys(t *testing.T) {
	entries := []Entry{
		{Key: []byte("k10"), Value: []byte("v10")},
		{Key: []byte("k20"), Value: []byte("v20")},
		{Key: []byte("k30"), Value: []byte("v30")},
	}

	run := func(name string, payload []byte) {
		t.Helper()
		blk, err := DecodeBlock(payload, nil)
		if err != nil {
			t.Fatalf("%s DecodeBlock: %v", name, err)
		}
		allocs := testing.AllocsPerRun(1000, func() {
			pos, below, above, err := blk.LowerBound([]byte("k20"))
			if err != nil {
				t.Fatalf("%s LowerBound: %v", name, err)
			}
			if pos != 1 || below || above {
				t.Fatalf("%s LowerBound got (pos=%d below=%v above=%v)", name, pos, below, above)
			}
		})
		if allocs != 0 {
			t.Fatalf("%s expected zero allocs per run, got %.2f", name, allocs)
		}
	}

	v2Payload, err := EncodeEntries(nil, entries, 0, 4)
	if err != nil {
		t.Fatalf("EncodeEntries(v2): %v", err)
	}
	run("v2", v2Payload)

	typed := make([]TypedEntry, len(entries))
	for i := range entries {
		typed[i] = TypedEntry{
			Key:   entries[i].Key,
			Kind:  EntryKindInline,
			Value: entries[i].Value,
		}
	}
	v3Payload, err := EncodeTypedEntries(nil, typed, 0, 4)
	if err != nil {
		t.Fatalf("EncodeTypedEntries(v3): %v", err)
	}
	run("v3", v3Payload)
}

func TestDecodedBlockLowerBound_LongKeysUseHeapFallback(t *testing.T) {
	makeLongKey := func(last byte) []byte {
		key := make([]byte, 0, 71)
		key = append(key, bytes.Repeat([]byte("a"), 70)...)
		key = append(key, last)
		return key
	}

	entries := []Entry{
		{Key: makeLongKey('1'), Value: []byte("v1")},
		{Key: makeLongKey('2'), Value: []byte("v2")},
		{Key: makeLongKey('3'), Value: []byte("v3")},
	}
	targetBelow := makeLongKey('0')
	targetMatch := makeLongKey('2')
	targetAbove := makeLongKey('9')

	run := func(name string, payload []byte) {
		t.Helper()
		blk, err := DecodeBlock(payload, nil)
		if err != nil {
			t.Fatalf("%s DecodeBlock: %v", name, err)
		}

		pos, below, above, err := blk.LowerBound(targetBelow)
		if err != nil {
			t.Fatalf("%s LowerBound(below): %v", name, err)
		}
		if pos != 0 || !below || above {
			t.Fatalf("%s LowerBound(below) got (pos=%d below=%v above=%v)", name, pos, below, above)
		}

		pos, below, above, err = blk.LowerBound(targetMatch)
		if err != nil {
			t.Fatalf("%s LowerBound(match): %v", name, err)
		}
		if pos != 1 || below || above {
			t.Fatalf("%s LowerBound(match) got (pos=%d below=%v above=%v)", name, pos, below, above)
		}

		pos, below, above, err = blk.LowerBound(targetAbove)
		if err != nil {
			t.Fatalf("%s LowerBound(above): %v", name, err)
		}
		if pos != len(entries) || below || !above {
			t.Fatalf("%s LowerBound(above) got (pos=%d below=%v above=%v)", name, pos, below, above)
		}

		allocs := testing.AllocsPerRun(1000, func() {
			pos, below, above, err := blk.LowerBound(targetMatch)
			if err != nil {
				t.Fatalf("%s LowerBound(match allocs): %v", name, err)
			}
			if pos != 1 || below || above {
				t.Fatalf("%s LowerBound(match allocs) got (pos=%d below=%v above=%v)", name, pos, below, above)
			}
		})
		if allocs == 0 {
			t.Fatalf("%s expected heap fallback allocations for long keys, got %.2f", name, allocs)
		}
	}

	v2Payload, err := EncodeEntries(nil, entries, 0, 4)
	if err != nil {
		t.Fatalf("EncodeEntries(v2): %v", err)
	}
	run("v2", v2Payload)

	typed := make([]TypedEntry, len(entries))
	for i := range entries {
		typed[i] = TypedEntry{
			Key:   entries[i].Key,
			Kind:  EntryKindInline,
			Value: entries[i].Value,
		}
	}
	v3Payload, err := EncodeTypedEntries(nil, typed, 0, 4)
	if err != nil {
		t.Fatalf("EncodeTypedEntries(v3): %v", err)
	}
	run("v3", v3Payload)
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

func TestDecodeKeysRangeLeaseWithVerify_V3AndUpperBoundEarlyExit(t *testing.T) {
	blobPtr := page.ValuePtr{FileID: page.ValueLogFileID(11), Offset: 77, Length: 9}
	payload, err := EncodeTypedEntries(nil, []TypedEntry{
		{Key: []byte("k10"), Kind: EntryKindInline, Value: []byte("v10")},
		{Key: []byte("k20"), Kind: EntryKindBlobRef, BlobPtr: blobPtr},
		{Key: []byte("k30"), Kind: EntryKindInline, Value: []byte("v30")},
	}, 0, 2)
	if err != nil {
		t.Fatalf("EncodeTypedEntries(v3): %v", err)
	}

	lease, err := DecodeKeysRangeLeaseWithVerify(payload, []byte("k15"), []byte("k35"), true)
	if err != nil {
		t.Fatalf("DecodeKeysRangeLeaseWithVerify(v3): %v", err)
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

	lease, err = DecodeKeysRangeLeaseWithVerify(payload, nil, []byte("k05"), true)
	if err != nil {
		t.Fatalf("DecodeKeysRangeLeaseWithVerify(v3 upper-before-first): %v", err)
	}
	if lease != nil {
		t.Fatalf("lease=%v want nil for empty bounded range", lease)
	}
}

func TestDecodeLowerBoundAndKeysOnMatchLeaseWithVerify_V3(t *testing.T) {
	blobPtr := page.ValuePtr{FileID: page.ValueLogFileID(12), Offset: 88, Length: 9}
	payload, err := EncodeTypedEntries(nil, []TypedEntry{
		{Key: []byte("k10"), Kind: EntryKindInline, Value: []byte("v10")},
		{Key: []byte("k20"), Kind: EntryKindBlobRef, BlobPtr: blobPtr},
		{Key: []byte("k30"), Kind: EntryKindInline, Value: []byte("v30")},
	}, 0, 2)
	if err != nil {
		t.Fatalf("EncodeTypedEntries(v3): %v", err)
	}

	pos, below, above, lease, err := DecodeLowerBoundAndKeysOnMatchLeaseWithVerify(payload, []byte("k20"), true)
	if err != nil {
		t.Fatalf("DecodeLowerBoundAndKeysOnMatchLeaseWithVerify(v3): %v", err)
	}
	if pos != 1 || below || above {
		t.Fatalf("got pos=%d below=%v above=%v", pos, below, above)
	}
	if lease == nil {
		t.Fatalf("lease=nil want non-nil")
	}
	keys := lease.Keys()
	if len(keys) != 3 {
		t.Fatalf("keys len=%d want=3", len(keys))
	}
	lease.Release()

	pos, below, above, lease, err = DecodeLowerBoundAndKeysOnMatchLeaseWithVerify(payload, []byte("k99"), true)
	if err != nil {
		t.Fatalf("DecodeLowerBoundAndKeysOnMatchLeaseWithVerify(v3 above): %v", err)
	}
	if pos != 3 || below || !above || lease != nil {
		t.Fatalf("above got pos=%d below=%v above=%v lease=%v", pos, below, above, lease)
	}
}

func TestOuterLeafPoolCapsBounded(t *testing.T) {
	if maxPooledOuterLeafBytesCap > (1 << 20) {
		t.Fatalf("maxPooledOuterLeafBytesCap=%d want <=1MiB", maxPooledOuterLeafBytesCap)
	}
	if maxPooledOuterLeafRestartsCap > 4096 {
		t.Fatalf("maxPooledOuterLeafRestartsCap=%d want <=4096", maxPooledOuterLeafRestartsCap)
	}
	if maxPooledOuterLeafLeaseKeysCap > 4096 {
		t.Fatalf("maxPooledOuterLeafLeaseKeysCap=%d want <=4096", maxPooledOuterLeafLeaseKeysCap)
	}
	if maxPooledOuterLeafLeaseArenaCap > (1 << 20) {
		t.Fatalf("maxPooledOuterLeafLeaseArenaCap=%d want <=1MiB", maxPooledOuterLeafLeaseArenaCap)
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
	putPooledLeaseKeys(undersized)

	got := getPooledLeaseKeys(4)
	if cap(got) < 4 {
		t.Fatalf("cap(got)=%d want >=4", cap(got))
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
	putPooledLeaseArena(undersized)

	got := getPooledLeaseArena(16)
	if cap(got) < 16 {
		t.Fatalf("cap(got)=%d want >=16", cap(got))
	}
}

func TestGetPooledLeaseKeys_WarmReuseAndClearsBuffer(t *testing.T) {
	for outerLeafLeaseKeysPool.Get() != nil {
	}
	t.Cleanup(func() {
		for outerLeafLeaseKeysPool.Get() != nil {
		}
	})

	const warmCap = 32
	got := make([][]byte, 0, 1)
	reused := false
	for i := 0; i < 64; i++ {
		warm := make([][]byte, 2, warmCap)
		warm[0] = []byte("left")
		warm[1] = []byte("right")
		putPooledLeaseKeys(warm)
		got = getPooledLeaseKeys(1)
		if cap(got) >= warmCap {
			reused = true
			break
		}
	}
	if !reused {
		t.Skipf("sync.Pool did not return warmed keys buffer under current scheduler/race instrumentation (cap=%d)", cap(got))
	}
	if len(got) != 0 {
		t.Fatalf("len(got)=%d want=0", len(got))
	}
	got = got[:2]
	if got[0] != nil || got[1] != nil {
		t.Fatalf("expected cleared pooled entries, got=%v", got)
	}

	got[0] = []byte("mutated")
	putPooledLeaseKeys(got[:0])
	reused = false
	for i := 0; i < 64; i++ {
		got = getPooledLeaseKeys(1)
		if cap(got) >= warmCap {
			reused = true
			break
		}
	}
	if !reused {
		t.Skipf("sync.Pool did not return warmed keys buffer on second reuse attempt (cap=%d)", cap(got))
	}
	if len(got) != 0 {
		t.Fatalf("len(got)=%d want=0 on warm reuse", len(got))
	}
	got = got[:1]
	if got[0] != nil {
		t.Fatalf("expected cleared pooled entry on reuse, got=%v", got[0])
	}
}

func TestGetPooledLeaseArena_WarmReuse(t *testing.T) {
	for outerLeafLeaseArenaPool.Get() != nil {
	}
	t.Cleanup(func() {
		for outerLeafLeaseArenaPool.Get() != nil {
		}
	})

	const warmCap = 64
	got := make([]byte, 0, 1)
	reused := false
	for i := 0; i < 64; i++ {
		warm := make([]byte, warmCap)
		putPooledLeaseArena(warm)
		got = getPooledLeaseArena(1)
		if cap(got) >= warmCap {
			reused = true
			break
		}
	}
	if !reused {
		t.Skipf("sync.Pool did not return warmed arena under current scheduler/race instrumentation (cap=%d)", cap(got))
	}
	if len(got) != 0 {
		t.Fatalf("len(got)=%d want=0", len(got))
	}

	got = append(got, 'x')
	putPooledLeaseArena(got)

	reused = false
	for i := 0; i < 64; i++ {
		got = getPooledLeaseArena(1)
		if cap(got) >= warmCap {
			reused = true
			break
		}
	}
	if !reused {
		t.Skipf("sync.Pool did not return warmed arena on second reuse attempt (cap=%d)", cap(got))
	}
	if len(got) != 0 {
		t.Fatalf("len(got)=%d want=0 on warm reuse", len(got))
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

func TestDecodedBlockKeysRangeLease(t *testing.T) {
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

	lease, err := blk.KeysRangeLease([]byte("k15"), []byte("k35"))
	if err != nil {
		t.Fatalf("KeysRangeLease bounded: %v", err)
	}
	if lease == nil {
		t.Fatalf("KeysRangeLease bounded lease=nil want non-nil")
	}
	keys := lease.Keys()
	want := [][]byte{[]byte("k20"), []byte("k30")}
	if len(keys) != len(want) {
		t.Fatalf("KeysRangeLease bounded len=%d want=%d", len(keys), len(want))
	}
	for i := range want {
		if !bytes.Equal(keys[i], want[i]) {
			t.Fatalf("KeysRangeLease bounded[%d]=%q want=%q", i, keys[i], want[i])
		}
	}
	lease.Release()

	lease, err = blk.KeysRangeLease([]byte("k99"), nil)
	if err != nil {
		t.Fatalf("KeysRangeLease empty: %v", err)
	}
	if lease != nil {
		t.Fatalf("KeysRangeLease empty lease=%v want=nil", lease)
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
