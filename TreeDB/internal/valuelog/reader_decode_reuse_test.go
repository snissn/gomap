package valuelog

import (
	"bytes"
	"testing"
	"unsafe"
)

func mixedDecodeFrames(t testing.TB, codec BlockCodec) (FrameHeader, [2][]byte, [2][]byte) {
	t.Helper()
	header := FrameHeader{Flags: FrameFlagCompressed, Reserved: uint8(codec)}
	raw := [2][]byte{
		bytes.Repeat([]byte("mixed-frame-data"), 4096),
		bytes.Repeat([]byte("small-frame-data"), 256),
	}
	var payload [2][]byte
	for i := range raw {
		encodeCodec := codec
		if codec == BlockCodecNone {
			// Reserved=0 selects the legacy no-dictionary Zstd frame path.
			encodeCodec = BlockCodecZSTD
		}
		var err error
		payload[i], err = encodeBlockPayload(encodeCodec, raw[i], nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	return header, raw, payload
}

func TestDecodeFramePayloadToPreservesReusedBacking(t *testing.T) {
	for name, codec := range map[string]BlockCodec{
		"lz4": BlockCodecLZ4, "zstd": BlockCodecZSTD,
		"snappy": BlockCodecSnappy, "legacy_zstd": BlockCodecNone,
	} {
		t.Run(name, func(t *testing.T) {
			header, raw, payload := mixedDecodeFrames(t, codec)
			backing := make([]byte, 0, len(raw[0]))
			dst := backing
			for _, i := range []int{0, 1, 0, 1} {
				got, err := decodeFramePayloadTo(header, payload[i], nil, uint32(len(raw[i])), dst)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, raw[i]) {
					t.Fatal("decoded bytes differ")
				}
				if cap(got) != cap(backing) || unsafe.SliceData(got) != unsafe.SliceData(backing) {
					t.Fatalf("decode lost caller backing: len=%d cap=%d want cap=%d", len(got), cap(got), cap(backing))
				}
				dst = got
			}
		})
	}
}

func TestDecodeFramePayloadToKeepsNewBacking(t *testing.T) {
	for _, codec := range []BlockCodec{BlockCodecLZ4, BlockCodecZSTD, BlockCodecSnappy, BlockCodecNone} {
		header, raw, payload := mixedDecodeFrames(t, codec)
		for _, dst := range [][]byte{nil, bytes.Repeat([]byte{0xa5}, 32)} {
			got, err := decodeFramePayloadTo(header, payload[0], nil, uint32(len(raw[0])), dst)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, raw[0]) || cap(got) < len(raw[0]) {
				t.Fatal("new backing cannot hold decoded output")
			}
			if unsafe.SliceData(got) == unsafe.SliceData(dst) {
				t.Fatal("insufficient destination was reused")
			}
			if dst != nil && !bytes.Equal(dst, bytes.Repeat([]byte{0xa5}, 32)) {
				t.Fatal("insufficient destination was modified")
			}
		}
	}
}

func TestDecodeFramePayloadToRejectsOversizedReuse(t *testing.T) {
	for _, codec := range []BlockCodec{BlockCodecLZ4, BlockCodecZSTD, BlockCodecSnappy, BlockCodecNone} {
		header, raw, payload := mixedDecodeFrames(t, codec)
		backing := bytes.Repeat([]byte{0xa5}, len(raw[0]))
		got, err := decodeFramePayloadTo(header, payload[0], nil, uint32(len(raw[1])), backing)
		if err == nil || got != nil {
			t.Fatalf("codec %d: oversized decode returned output or no error: %v", codec, err)
		}
		if !bytes.Equal(backing[len(raw[1]):], bytes.Repeat([]byte{0xa5}, len(raw[0])-len(raw[1]))) {
			t.Fatalf("codec %d wrote beyond admitted raw length", codec)
		}
	}
}

func BenchmarkValueLogDecodeFrameMixedReuse(b *testing.B) {
	for name, codec := range map[string]BlockCodec{
		"lz4": BlockCodecLZ4, "zstd": BlockCodecZSTD,
		"snappy": BlockCodecSnappy, "legacy_zstd": BlockCodecNone,
	} {
		b.Run(name, func(b *testing.B) {
			header, raw, payload := mixedDecodeFrames(b, codec)
			dst := make([]byte, 0, len(raw[0]))
			// Warm pooled codecs before measuring alternating large/small frames.
			var err error
			dst, err = decodeFramePayloadTo(header, payload[0], nil, uint32(len(raw[0])), dst)
			if err != nil {
				b.Fatal(err)
			}
			var backingAllocs int
			b.ReportAllocs()
			b.SetBytes(int64((len(raw[0]) + len(raw[1])) / 2))
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				i := n % len(raw)
				previous := unsafe.SliceData(dst)
				dst, err = decodeFramePayloadTo(header, payload[i], nil, uint32(len(raw[i])), dst)
				if err != nil {
					b.Fatal(err)
				}
				if unsafe.SliceData(dst) != previous {
					backingAllocs++
				}
				if len(dst) != len(raw[i]) || dst[0] != raw[i][0] {
					b.Fatal("decoded output differs")
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(backingAllocs)/float64(b.N), "backing_allocs/op")
			b.ReportMetric(float64(cap(dst)), "scratch_cap_B")
		})
	}
}
