package valuelog

import (
	"bytes"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func BenchmarkMmapReadUnsafe_CompressedGrouped(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip("mmap not supported on windows")
	}
	b.ReportAllocs()

	dir := b.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		b.Fatalf("encode file id: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")

	writer, err := NewWriter(path, fileID)
	if err != nil {
		b.Fatalf("new writer: %v", err)
	}
	writer.SetBlockCompression(BlockCodecSnappy, true)

	records := make([]Record, 16)
	for i := range records {
		v := make([]byte, 320)
		copy(v, []byte(fmt.Sprintf("record-%02d:", i)))
		for j := 32; j < len(v); j++ {
			v[j] = 'x'
		}
		records[i] = Record{RID: uint64(i + 1), Value: v}
	}
	dst := make([]page.ValuePtr, len(records))
	ptrs, stats, err := writer.AppendFrameWithStatsInto(0, nil, records, dst)
	if err != nil {
		_ = writer.Close()
		b.Fatalf("append frame: %v", err)
	}
	if !stats.Kept {
		_ = writer.Close()
		b.Fatalf("expected block-compressed frame to be kept")
	}
	if err := writer.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}

	m, err := NewManager(dir)
	if err != nil {
		b.Fatalf("new manager: %v", err)
	}
	m.SetDisableReadChecksum(true)
	m.SetTemplateLookup(nil, templ.DecodeOptions{})
	defer func() { _ = m.Close() }()

	// Force the segment mapping to exist before benchmarking.
	f := m.files[fileID]
	f.remapToFileSize()

	// Prime caches with one read.
	val0, err := m.ReadUnsafe(ptrs[0])
	if err != nil {
		b.Fatalf("prime read: %v", err)
	}
	if len(val0) == 0 {
		b.Fatalf("expected non-empty value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ptr := ptrs[i%len(ptrs)]
		got, err := m.ReadUnsafe(ptr)
		if err != nil {
			b.Fatalf("read unsafe: %v", err)
		}
		// Minimal use to keep compiler honest.
		if len(got) == 0 || !bytes.HasPrefix(got, []byte("record-")) {
			b.Fatalf("unexpected value")
		}
	}
}
