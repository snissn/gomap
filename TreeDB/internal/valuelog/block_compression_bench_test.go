package valuelog

import (
	"bytes"
	"io"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func BenchmarkWriterBlockCompressionSingleRecord4096(b *testing.B) {
	writer := NewWriterWithSink(io.Discard, page.ValueLogFileID(1))
	writer.SetBlockCompression(BlockCodecSnappy, true)
	writer.SetEncodeSampleStride(0)
	value := bytes.Repeat([]byte("a"), 4096)
	records := []Record{{RID: 1, Value: value}}
	dst := make([]page.ValuePtr, 1)
	if _, _, err := writer.AppendFrameWithStatsInto(0, nil, records, dst); err != nil {
		b.Fatalf("warm AppendFrameWithStatsInto: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		records[0].RID = uint64(i + 2)
		if _, _, err := writer.AppendFrameWithStatsInto(0, nil, records, dst); err != nil {
			b.Fatalf("AppendFrameWithStatsInto: %v", err)
		}
	}
}

func BenchmarkFramePreparerBlockCompressionSingleRecord4096(b *testing.B) {
	prep := NewFramePreparer()
	prep.SetBlockCompression(BlockCodecSnappy, true)
	prep.SetEncodeSampleStride(0)
	value := bytes.Repeat([]byte("a"), 4096)
	records := []Record{{RID: 1, Value: value}}
	buf, _, err := prep.PrepareFrameInto(nil, 0, nil, records)
	if err != nil {
		b.Fatalf("warm PrepareFrameInto: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		records[0].RID = uint64(i + 2)
		buf, _, err = prep.PrepareFrameInto(buf[:0], 0, nil, records)
		if err != nil {
			b.Fatalf("PrepareFrameInto: %v", err)
		}
	}
}
