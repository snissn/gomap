package valuelog

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

const cohereBlockValueBytes = 8_704

func cohereBlockRecords(count int) []Record {
	records := make([]Record, count)
	for i := range records {
		value := make([]byte, cohereBlockValueBytes)
		state := uint32(i + 1)
		for j := range value[:len(value)/2] {
			state ^= state << 13
			state ^= state >> 17
			state ^= state << 5
			value[j] = byte(state)
		}
		for j := len(value) / 2; j < len(value); j++ {
			value[j] = byte((i*31 + j*17 + j>>5) % 251)
		}
		records[i] = Record{RID: uint64(i + 1), Value: value}
	}
	return records
}

func TestWriterBlockCompressionZSTDCohereShapeRoundTrip(t *testing.T) {
	const recordCount = 60
	records := cohereBlockRecords(recordCount)
	path := filepath.Join(t.TempDir(), "value-000001.log")
	w, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatal(err)
	}
	w.SetBlockCompression(BlockCodecZSTD, true)
	ptrs, stats, err := w.AppendFrameWithStatsInto(0, nil, records, make([]page.ValuePtr, recordCount))
	if err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if !stats.Kept || stats.StoredPayloadBytes*5 >= stats.RawPayloadBytes*3 {
		_ = w.Close()
		t.Fatalf("unexpected compression: %+v", stats)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for i, ptr := range ptrs {
		got, err := ReadAtWithDict(f, ptr, true, nil, nil, nil, templ.DecodeOptions{})
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !bytes.Equal(got, records[i].Value) {
			t.Fatalf("value %d mismatch", i)
		}
	}
}

func BenchmarkWriterBlockCompressionZSTDCohereShape(b *testing.B) {
	const recordCount = 60
	records := cohereBlockRecords(recordCount)
	rawBytes := recordCount * cohereBlockValueBytes
	w := NewWriterWithSink(io.Discard, page.ValueLogFileID(1))
	w.SetBlockCompression(BlockCodecZSTD, true)
	w.SetEncodeSampleStride(0)
	ptrs := make([]page.ValuePtr, recordCount)
	_, stats, err := w.AppendFrameWithStatsInto(0, nil, records, ptrs)
	if err != nil {
		b.Fatal(err)
	}
	if !stats.Kept {
		b.Fatalf("compression not kept: %+v", stats)
	}

	b.ReportAllocs()
	b.SetBytes(int64(rawBytes))
	b.ReportMetric(float64(stats.StoredPayloadBytes)/float64(rawBytes), "stored/raw")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base := uint64(i+1) * recordCount
		for j := range records {
			records[j].RID = base + uint64(j)
		}
		if _, _, err := w.AppendFrameWithStatsInto(0, nil, records, ptrs); err != nil {
			b.Fatal(err)
		}
	}
}
