package valuelog

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// Include the real record read, revalidation, CRC and bounded decode. Setup and
// inspection are excluded, just as for an already-inspected physical leaf miss.
func BenchmarkCOWLeafPageRead(b *testing.B) {
	leaf := buildSparseLeafPageForPayloadBench(b)
	compact, ok, err := MaybeCompactLeafLogPayload(leaf)
	if err != nil || !ok {
		b.Fatalf("compact=%v err=%v", ok, err)
	}
	// A denser compact page complements the tiny sparse-page fixture and is
	// closer to the selected public collection leaves (reported separately).
	dense := make([]byte, page.PageSize)
	builder := node.NewBuilderWithOptions(dense, page.PageTypeLeaf, node.BuilderOptions{
		LeafColumnar: true, LeafPrefixCompression: true, EntryRevisions: true,
	})
	for i := 0; i < 40; i++ {
		if err := builder.AddLeafEntryWithRevision([]byte(fmt.Sprintf("key/%03d", i)), bytes.Repeat([]byte("v"), 64), node.FlagInline, page.ValuePtr{}, page.EntryRevision(i+1)); err != nil {
			b.Fatal(err)
		}
	}
	builder.FinishNoNode()
	builder.ReleaseScratch()
	denseCompact, ok, err := MaybeCompactLeafLogPayload(dense)
	if err != nil || !ok {
		b.Fatalf("dense compact=%v err=%v", ok, err)
	}
	for _, tc := range []struct {
		name  string
		value []byte
		codec BlockCodec
		sub   uint8
	}{
		{"raw", leaf, BlockCodecNone, 0},
		{"compact", compact, BlockCodecNone, 0},
		{"snappy-compact", compact, BlockCodecSnappy, 0},
		{"lz4-compact", compact, BlockCodecLZ4, 0},
		{"zstd-compact", compact, BlockCodecZSTD, 0},
		{"grouped-compact", compact, BlockCodecNone, 1},
		{"dense-compact", denseCompact, BlockCodecNone, 0},
		{"snappy-dense-compact", denseCompact, BlockCodecSnappy, 0},
	} {
		b.Run(tc.name, func(b *testing.B) {
			values := [][]byte{tc.value}
			if tc.sub != 0 {
				values = [][]byte{compact, tc.value, compact}
			}
			var encoded []byte
			if tc.codec != BlockCodecNone {
				encoded, err = encodeBlockPayload(tc.codec, bytes.Join(values, nil), nil)
				if err != nil {
					b.Fatal(err)
				}
			}
			f, ptr := cowTestLeafFrame(b, cowTestFrame(0, tc.codec, values, encoded), tc.sub)
			shape, err := InspectCOWLeafRecord(f, ptr, cowTestLimits)
			if err != nil {
				b.Fatal(err)
			}
			input, raw, dst := make([]byte, shape.PayloadBytes()), make([]byte, shape.RawBytes), make([]byte, page.PageSize)
			var decode COWDecodeFunc
			if shape.Compressed {
				decoder, err := NewCOWDecoder(0, nil, shape.RawBytes, shape.PayloadBytes(), 1<<20, func(COWDecoderAllocationSizes) error { return nil })
				if err != nil {
					b.Fatal(err)
				}
				defer decoder.Close()
				decode = decoder.Decode
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out, err := ReadCOWLeafPage(f, ptr, shape, true, input, raw, dst, decode)
				if err != nil {
					b.Fatal(err)
				}
				leafPagePayloadBenchSink = out
			}
			b.StopTimer()
			b.ReportMetric(float64(shape.ValueBytes), "value-B")
			b.ReportMetric(float64(shape.RecordBytes), "record-B")
			b.ReportMetric(page.PageSize, "page-B")
		})
	}
}
