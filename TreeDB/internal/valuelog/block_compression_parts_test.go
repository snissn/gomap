package valuelog

import (
	"bytes"
	"io"
	"math/rand"
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
	records[3].Value = nil
	records[17].Value = []byte{}
	path := filepath.Join(t.TempDir(), "value-000001.log")
	w, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatal(err)
	}
	w.SetBlockCompression(BlockCodecZSTD, true)
	var ptrs []page.ValuePtr
	var want []Record
	// A one-record frame uses EncodeAll; surrounding frames use streaming.
	single := []Record{{RID: 61, Value: bytes.Repeat([]byte("single-frame-"), 32<<10)}}
	for frame, input := range [][]Record{records, single, records} {
		framePtrs, stats, err := w.AppendFrameWithStatsInto(0, nil, input, make([]page.ValuePtr, len(input)))
		if err != nil {
			_ = w.Close()
			t.Fatal(err)
		}
		if !stats.Kept || stats.StoredPayloadBytes*5 >= stats.RawPayloadBytes*3 {
			_ = w.Close()
			t.Fatalf("frame %d: unexpected compression: %+v", frame, stats)
		}
		if frame == 0 && (cap(w.rawScratch) != 0 || cap(w.blockScratch) != 0) {
			_ = w.Close()
			t.Fatalf("eligible parts path staged payload copies: raw=%d encoded=%d", cap(w.rawScratch), cap(w.blockScratch))
		}
		ptrs = append(ptrs, framePtrs...)
		want = append(want, input...)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Errorf("close value log: %v", err)
		}
	})
	for i, ptr := range ptrs {
		got, err := ReadAtWithDict(f, ptr, true, nil, nil, nil, templ.DecodeOptions{})
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !bytes.Equal(got, want[i].Value) {
			t.Fatalf("value %d mismatch", i)
		}
	}
}

func TestWriterBlockCompressionZSTDPartsFallsBackToRaw(t *testing.T) {
	const recordCount = 60
	records := make([]Record, recordCount)
	rng := rand.New(rand.NewSource(1))
	for i := range records {
		value := make([]byte, cohereBlockValueBytes)
		if _, err := rng.Read(value); err != nil {
			t.Fatal(err)
		}
		records[i] = Record{RID: uint64(i + 1), Value: value}
	}

	path := filepath.Join(t.TempDir(), "value-000001.log")
	w, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	w.SetBlockCompression(BlockCodecZSTD, true)
	_, stats, err := w.AppendFrameWithStatsInto(0, nil, records, make([]page.ValuePtr, recordCount))
	if err != nil {
		t.Fatal(err)
	}
	if !stats.Attempted || stats.Kept || stats.StoredPayloadBytes != stats.RawPayloadBytes {
		t.Fatalf("unexpected raw fallback: %+v", stats)
	}
	if cap(w.blockScratch) != 0 {
		t.Fatalf("eligible raw fallback staged encoded payload: %d", cap(w.blockScratch))
	}
	// Reuse the pool after a streaming write hit the incompressible-size limit.
	raw := bytes.Repeat([]byte("after-fallback-"), 16<<10)
	encoded, err := encodeBlockPayload(BlockCodecZSTD, raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeBlockPayload(uint8(BlockCodecZSTD), encoded, uint32(len(raw)), nil)
	if err != nil || !bytes.Equal(decoded, raw) {
		t.Fatalf("EncodeAll after fallback: mismatch or error %v", err)
	}
	w.ResetCompressionHints()
	recovered := cohereBlockRecords(recordCount)
	ptrs, stats, err := w.AppendFrameWithStatsInto(0, nil, recovered, make([]page.ValuePtr, recordCount))
	if err != nil || !stats.Kept {
		t.Fatalf("stream after fallback: stats=%+v err=%v", stats, err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for i, ptr := range ptrs {
		got, err := ReadAtWithDict(reader, ptr, true, nil, nil, nil, templ.DecodeOptions{})
		if err != nil || !bytes.Equal(got, recovered[i].Value) {
			t.Fatalf("stream after fallback row %d: mismatch or error %v", i, err)
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
	b.ReportMetric(float64(stats.StoredPayloadBytes)/float64(rawBytes), "stored/raw")
}
