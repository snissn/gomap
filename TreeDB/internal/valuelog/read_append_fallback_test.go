package valuelog

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestFileReadAppend_CompressedFallbackNilDstAvoidsExtraGrow(t *testing.T) {
	dir := t.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")

	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	writer.SetBlockCompression(BlockCodecSnappy, true)

	records := make([]Record, 8)
	for i := range records {
		v := make([]byte, 512)
		copy(v, []byte(fmt.Sprintf("compressed-record-%02d:", i)))
		for j := 32; j < len(v); j++ {
			v[j] = 'z'
		}
		records[i] = Record{RID: uint64(i + 1), Value: v}
	}
	ptrs, stats, err := writer.AppendFrameWithStatsInto(0, nil, records, make([]page.ValuePtr, len(records)))
	if err != nil {
		_ = writer.Close()
		t.Fatalf("AppendFrameWithStatsInto: %v", err)
	}
	if !stats.Kept {
		_ = writer.Close()
		t.Fatalf("expected compressed frame to be kept")
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = m.Close() }()

	f := m.files[fileID]
	// Force the read-at fallback path rather than the mmap view path.
	mapped := []byte{0}
	f.mmapData.Store(mapped)
	f.deadMappingsCount.Store(uint64(effectiveMaxDeadMappings(len(mapped))))

	before := GrowBufferStatsSnapshot()
	got, err := f.ReadAppend(ptrs[3], true, nil)
	if err != nil {
		t.Fatalf("ReadAppend: %v", err)
	}
	after := GrowBufferStatsSnapshot()

	want := records[3].Value
	if !bytes.Equal(got, want) {
		t.Fatalf("ReadAppend mismatch: got=%q want=%q", got, want)
	}
	if got == nil || cap(got) != len(got) {
		t.Fatalf("ReadAppend returned cap=%d len=%d want exact-sized owned slice", cap(got), len(got))
	}
	if delta := after.ReadAppendCompressedFallbackCallsTotal - before.ReadAppendCompressedFallbackCallsTotal; delta != 1 {
		t.Fatalf("compressed fallback calls delta=%d want 1", delta)
	}
}
