package valuelog

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func BenchmarkValueLogCompressedGroupedRead(b *testing.B) {
	benchmarkCompressedGroupedRead(b, false)
}

func BenchmarkValueLogCompressedGroupedReadAppend(b *testing.B) {
	benchmarkCompressedGroupedRead(b, true)
}

func benchmarkCompressedGroupedRead(b *testing.B, appendMode bool) {
	dir := b.TempDir()
	path := filepath.Join(dir, "value-l0-000001.log")
	fileID := page.ValueLogFileID(1)

	writer, err := NewWriter(path, fileID)
	if err != nil {
		b.Fatalf("NewWriter: %v", err)
	}
	writer.SetBlockCompression(BlockCodecSnappy, true)

	const (
		frames    = 1024
		k         = 4
		valueSize = 16 << 10
	)

	rng := rand.New(rand.NewSource(1))
	ptrs := make([]page.ValuePtr, 0, frames*k)
	records := make([]Record, k)
	var ptrScratch [k]page.ValuePtr
	for frame := 0; frame < frames; frame++ {
		for i := 0; i < k; i++ {
			value := bytes.Repeat([]byte("grouped-read-benchmark-payload-"), valueSize/31+1)
			value = value[:valueSize]
			rng.Read(value[valueSize-64:])
			records[i] = Record{
				RID:   uint64(frame*k + i + 1),
				Value: value,
			}
		}
		framePtrs, stats, err := writer.AppendFrameWithStatsInto(0, nil, records, ptrScratch[:])
		if err != nil {
			_ = writer.Close()
			b.Fatalf("AppendFrameWithStatsInto: %v", err)
		}
		if !stats.Kept {
			_ = writer.Close()
			b.Fatalf("expected compressed grouped frame to be kept")
		}
		ptrs = append(ptrs, framePtrs...)
	}
	if err := writer.Close(); err != nil {
		b.Fatalf("Close: %v", err)
	}

	fh, err := os.Open(path)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = fh.Close() })

	vf := &File{
		ID:                 fileID,
		File:               fh,
		templateDecodeOpts: templ.DecodeOptions{},
	}
	vf.mmapData.Store([]byte(nil))

	b.ReportAllocs()
	b.SetBytes(valueSize)
	b.ResetTimer()

	rngReads := rand.New(rand.NewSource(1))
	if appendMode {
		buf := make([]byte, 0, valueSize+64)
		sink := byte(0)
		for i := 0; i < b.N; i++ {
			ptr := ptrs[rngReads.Intn(len(ptrs))]
			out, err := vf.ReadAppend(ptr, false, buf[:0])
			if err != nil {
				b.Fatalf("ReadAppend: %v", err)
			}
			sink ^= out[0]
		}
		b.StopTimer()
		if sink == 0xff {
			b.Fatalf("sink")
		}
		return
	}

	sink := byte(0)
	for i := 0; i < b.N; i++ {
		ptr := ptrs[rngReads.Intn(len(ptrs))]
		out, err := vf.Read(ptr, false)
		if err != nil {
			b.Fatalf("Read: %v", err)
		}
		sink ^= out[0]
	}
	b.StopTimer()
	if sink == 0xff {
		b.Fatalf("sink")
	}
}
