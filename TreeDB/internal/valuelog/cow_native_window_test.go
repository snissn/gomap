package valuelog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/page"
)

// Native EncodeAll and EncodeAllParts use a non-single-segment frame at exactly
// 1024 bytes, whose advertised window is 2048. A codec's window allowance must
// accommodate that header without increasing the caller's raw output capacity.
func TestCOWNativeWindowBoundary(t *testing.T) {
	samples := make([][]byte, 16)
	for i := range samples {
		samples[i] = bytes.Repeat([]byte("dictionary-owned-value-with-varied-content"), 256)
		samples[i][0] = byte(i)
	}
	definition, err := buildBenchDict(317, samples)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(definition)
	id := binary.BigEndian.Uint64(digest[:8])
	capacity := func(n uint64) uint64 {
		c := uint64(1024)
		for c < n {
			c *= 2
		}
		return c
	}
	for _, route := range []string{"raw", "dictionary", "block-zstd"} {
		for _, n := range []int{512, 1023, 1024, 1025} {
			t.Run(fmt.Sprintf("%s/%d", route, n), func(t *testing.T) {
				want := bytes.Repeat([]byte("dictionary-owned-value-with-varied-content"), n)[:n]
				path := filepath.Join(t.TempDir(), "value-000001.log")
				writer, err := NewWriter(path, page.ValueLogFileID(1))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = writer.Close() })
				var ptr page.ValuePtr
				if route == "block-zstd" {
					writer.SetBlockCompression(BlockCodecZSTD, true)
					ptrs, _, err := writer.AppendFrameWithStatsInto(0, nil, []Record{{RID: 1, Value: want}}, make([]page.ValuePtr, 1))
					if err != nil {
						t.Fatal(err)
					}
					ptr = ptrs[0]
				} else {
					var producerID uint64
					var dict []byte
					if route == "dictionary" {
						producerID, dict = id, definition
					}
					body, _, err := EncodeFrame(producerID, dict, []Record{{RID: 1, Value: want}})
					if err != nil {
						t.Fatal(err)
					}
					ptrs, err := writer.AppendEncodedFrameInto(body, 1, make([]page.ValuePtr, 1))
					if err != nil {
						t.Fatal(err)
					}
					ptr = ptrs[0]
				}
				if err := writer.Flush(); err != nil {
					t.Fatal(err)
				}
				f, err := os.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = f.Close() })
				shape, err := InspectCOWRecord(f, ptr, cowTestLimits)
				if err != nil {
					t.Fatal(err)
				}
				if shape.RawBytes != uint64(n) || shape.ValueBytes != uint64(n) || shape.Compressed != (route != "raw") {
					t.Fatalf("unexpected native shape: %+v", shape)
				}
				inputCap, rawCap := capacity(shape.PayloadBytes()), capacity(shape.RawBytes)
				windowCap := max(rawCap, 2048)
				input, raw, output := make([]byte, inputCap), make([]byte, rawCap), make([]byte, n)
				var decoder *COWDecoder
				var decode COWDecodeFunc
				if shape.Compressed {
					var dict []byte
					if shape.DictID != 0 {
						dict = definition
					}
					expected, err := COWDecoderRetentionSizes(uint64(len(dict)), rawCap, inputCap, windowCap)
					if err != nil {
						t.Fatal(err)
					}
					if expected.RawBacking != 2*max(rawCap, windowCap)+512<<10 {
						t.Fatalf("retained backing allowance does not match codec bounds: %+v", expected)
					}
					admissions := 0
					decoder, err = NewCOWDecoder(shape.DictID, dict, rawCap, inputCap, windowCap, func(got COWDecoderAllocationSizes) error {
						admissions++
						if got != expected {
							t.Fatalf("constructor admission %+v != preflight %+v", got, expected)
						}
						return nil
					})
					if err != nil || admissions != 1 {
						t.Fatalf("decoder construction err=%v admissions=%d", err, admissions)
					}
					t.Cleanup(decoder.Close)
					decode = decoder.Decode
				}
				got, err := ReadCOWRecord(f, ptr, shape, true, input, raw, output, decode)
				if err != nil || !bytes.Equal(got, want) || unsafe.SliceData(got) != unsafe.SliceData(output) || cap(got) != n {
					t.Fatalf("bounded native read err=%v len=%d cap=%d", err, len(got), cap(got))
				}
				if decoder == nil {
					return
				}
				payload := input[HeaderSize+int(shape.prefix) : shape.PayloadBytes()]
				var header zstd.Header
				if err := header.Decode(payload); err != nil {
					t.Fatal(err)
				}
				if n == 1024 && (header.SingleSegment || header.WindowSize != 2048) {
					t.Fatalf("fixture missed native boundary: %+v", header)
				}
				if header.FrameContentSize != uint64(n) {
					t.Fatalf("native content size %d != %d", header.FrameContentSize, n)
				}
				// Even with a larger codec window, insufficient caller capacity is
				// refused, and a subsequent valid decode must use the same backing.
				if _, err := decoder.Decode(shape.frame, payload, raw[:n-1:n-1]); err == nil {
					t.Fatal("codec window relaxed the caller output capacity")
				}
				for i := range raw {
					raw[i] = 0xa5
				}
				decoded, err := decoder.Decode(shape.frame, payload, raw[:n:n])
				if err != nil || !bytes.Equal(decoded, want) || unsafe.SliceData(decoded) != unsafe.SliceData(raw) || cap(decoded) != n {
					t.Fatalf("decode grew or replaced output: err=%v len=%d cap=%d", err, len(decoded), cap(decoded))
				}
				for _, b := range raw[n:] {
					if b != 0xa5 {
						t.Fatal("decode touched backing beyond the clipped caller output")
					}
				}
			})
		}
	}
}
