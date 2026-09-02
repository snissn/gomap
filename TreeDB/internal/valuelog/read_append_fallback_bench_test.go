package valuelog

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func BenchmarkFileReadAppendCompressedFallback(b *testing.B) {
	dir := b.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		b.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")

	writer, err := NewWriter(path, fileID)
	if err != nil {
		b.Fatalf("NewWriter: %v", err)
	}
	writer.SetBlockCompression(BlockCodecSnappy, true)

	records := make([]Record, 16)
	for i := range records {
		v := make([]byte, 512)
		copy(v, []byte(fmt.Sprintf("bench-record-%02d:", i)))
		for j := 32; j < len(v); j++ {
			v[j] = 'q'
		}
		records[i] = Record{RID: uint64(i + 1), Value: v}
	}
	ptrs, stats, err := writer.AppendFrameWithStatsInto(0, nil, records, make([]page.ValuePtr, len(records)))
	if err != nil {
		_ = writer.Close()
		b.Fatalf("AppendFrameWithStatsInto: %v", err)
	}
	if !stats.Kept {
		_ = writer.Close()
		b.Fatalf("expected compressed frame to be kept")
	}
	if err := writer.Close(); err != nil {
		b.Fatalf("Close writer: %v", err)
	}

	m, err := NewManager(dir)
	if err != nil {
		b.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = m.Close() }()

	f := m.files[fileID]
	mapped := []byte{0}
	f.mmapData.Store(mapped)
	f.deadMappingsCount.Store(uint64(effectiveMaxDeadMappings(len(mapped))))

	ptr := ptrs[len(ptrs)/2]

	b.Run("nil_dst", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			got, err := f.ReadAppend(ptr, true, nil)
			if err != nil {
				b.Fatalf("ReadAppend(nil): %v", err)
			}
			if len(got) == 0 {
				b.Fatalf("ReadAppend(nil) returned empty payload")
			}
		}
	})

	b.Run("reused_dst", func(b *testing.B) {
		dst := make([]byte, 0, len(records[0].Value))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			got, err := f.ReadAppend(ptr, true, dst[:0])
			if err != nil {
				b.Fatalf("ReadAppend(reused): %v", err)
			}
			if len(got) == 0 {
				b.Fatalf("ReadAppend(reused) returned empty payload")
			}
		}
	})
}
