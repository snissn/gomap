package valuelog

import (
	"bytes"
	"errors"
	"fmt"
	"os"
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

func TestFileReadAppend_VerifiedCompressedGroupedFallbackUsesFrameCache(t *testing.T) {
	f, ptrs, want := openGroupedCompressedFileReadFallbackFixture(t)
	f.setGroupedFrameCacheEntries(4)
	mapped := []byte{0}
	f.mmapData.Store(mapped)
	f.deadMappingsCount.Store(uint64(effectiveMaxDeadMappings(len(mapped))))

	prefix := []byte("caller-prefix:")
	backing := make([]byte, len(prefix), len(prefix)+len(want[0]))
	copy(backing, prefix)
	expected := append(append([]byte(nil), prefix...), want[0]...)
	crcBefore := f.ReadStats().RecordCRCChecks

	got, err := f.ReadAppend(ptrs[0], true, backing[:len(prefix)])
	if err != nil {
		t.Fatalf("first ReadAppend: %v", err)
	}
	if !bytes.Equal(got, expected) || &got[0] != &backing[0] {
		t.Fatalf("first ReadAppend did not preserve prefix/dst ownership")
	}
	hitsBefore, _, entriesBefore, _ := f.groupedFrameCacheStats()
	if entriesBefore != 1 {
		t.Fatalf("first ReadAppend cached entries=%d want 1", entriesBefore)
	}

	got, err = f.ReadAppend(ptrs[1], true, backing[:len(prefix)])
	if err != nil {
		t.Fatalf("second ReadAppend: %v", err)
	}
	hitsAfter, _, _, _ := f.groupedFrameCacheStats()
	if hitsAfter != hitsBefore+1 {
		t.Fatalf("second ReadAppend cache hits=%d want %d", hitsAfter, hitsBefore+1)
	}
	if gotCRC := f.ReadStats().RecordCRCChecks - crcBefore; gotCRC != 2 {
		t.Fatalf("record CRC checks=%d want 2", gotCRC)
	}
	if !bytes.Equal(got, append(prefix, want[1]...)) || &got[0] != &backing[0] {
		t.Fatalf("second ReadAppend did not preserve prefix/dst ownership")
	}

	corrupt, err := os.OpenFile(f.Path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	corruptAt := int64(ptrs[0].Offset - 4 + HeaderSize + FrameHeaderSize)
	var byteAt [1]byte
	if _, err := corrupt.ReadAt(byteAt[:], corruptAt); err != nil {
		_ = corrupt.Close()
		t.Fatalf("read source for corruption: %v", err)
	}
	byteAt[0] ^= 0xff
	if _, err := corrupt.WriteAt(byteAt[:], corruptAt); err != nil {
		_ = corrupt.Close()
		t.Fatalf("corrupt source: %v", err)
	}
	if err := corrupt.Close(); err != nil {
		t.Fatalf("close corrupt source: %v", err)
	}
	if _, err := f.ReadAppend(ptrs[0], true, backing[:len(prefix)]); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("ReadAppend after cache warmup error=%v want ErrCorrupt", err)
	}
	if gotCRC := f.ReadStats().RecordCRCChecks - crcBefore; gotCRC != 3 {
		t.Fatalf("record CRC checks after corruption=%d want 3", gotCRC)
	}
}

func TestFileReadAppend_VerifiedGroupedUncompressedFallbackChecksCRCOnce(t *testing.T) {
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
	records := []Record{{RID: 1, Value: []byte("first")}, {RID: 2, Value: []byte("second")}}
	ptrs, _, err := writer.AppendFrameWithStatsInto(0, nil, records, make([]page.ValuePtr, len(records)))
	if err != nil {
		_ = writer.Close()
		t.Fatalf("AppendFrameWithStatsInto: %v", err)
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
	mapped := []byte{0}
	f.mmapData.Store(mapped)
	f.deadMappingsCount.Store(uint64(effectiveMaxDeadMappings(len(mapped))))

	var frameHeader [FrameHeaderSize]byte
	if _, err := f.File.ReadAt(frameHeader[:], int64(ptrs[0].Offset-4+HeaderSize)); err != nil {
		t.Fatalf("read frame header: %v", err)
	}
	if frameHeader[1]&FrameFlagCompressed != 0 {
		t.Fatal("expected grouped uncompressed frame")
	}

	prefix := []byte("caller-prefix:")
	backing := make([]byte, len(prefix), len(prefix)+len(records[1].Value))
	copy(backing, prefix)
	crcBefore := f.ReadStats().RecordCRCChecks
	got, err := f.ReadAppend(ptrs[1], true, backing[:len(prefix)])
	if err != nil {
		t.Fatalf("ReadAppend: %v", err)
	}
	if want := append(prefix, records[1].Value...); !bytes.Equal(got, want) || &got[0] != &backing[0] {
		t.Fatalf("ReadAppend did not preserve bytes/prefix/dst ownership")
	}
	if gotCRC := f.ReadStats().RecordCRCChecks - crcBefore; gotCRC != 1 {
		t.Fatalf("record CRC checks=%d want 1", gotCRC)
	}
	if hits, _, entries, _ := f.groupedFrameCacheStats(); hits != 0 || entries != 0 {
		t.Fatalf("uncompressed fallback populated grouped frame cache: hits=%d entries=%d", hits, entries)
	}
}

func BenchmarkFileReadAppend_VerifiedCompressedGroupedFallback(b *testing.B) {
	f, ptrs, want := openGroupedCompressedFileReadFallbackFixture(b)
	f.setGroupedFrameCacheEntries(64)
	mapped := []byte{0}
	f.mmapData.Store(mapped)
	f.deadMappingsCount.Store(uint64(effectiveMaxDeadMappings(len(mapped))))

	for _, tc := range []struct {
		name string
		ptr  func(int) page.ValuePtr
	}{
		{"same_entry", func(int) page.ValuePtr { return ptrs[0] }},
		{"same_frame_subrecords", func(i int) page.ValuePtr { return ptrs[i%len(ptrs)] }},
	} {
		b.Run(tc.name, func(b *testing.B) {
			dst := make([]byte, 0, len(want[0]))
			if _, err := f.ReadAppend(tc.ptr(0), true, dst[:0]); err != nil {
				b.Fatalf("warm ReadAppend: %v", err)
			}
			hitsBefore, _, _, _ := f.groupedFrameCacheStats()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := f.ReadAppend(tc.ptr(i), true, dst[:0])
				if err != nil {
					b.Fatalf("ReadAppend: %v", err)
				}
				if len(got) != len(want[0]) {
					b.Fatalf("ReadAppend len=%d want %d", len(got), len(want[0]))
				}
			}
			b.StopTimer()
			hitsAfter, _, _, _ := f.groupedFrameCacheStats()
			b.ReportMetric(float64(hitsAfter-hitsBefore)/float64(b.N), "cache_hits/op")
		})
	}
}
