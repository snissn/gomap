package valuelog

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/golang/snappy"
	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func TestWriterFlushFlushesBufferedFileBackedWriter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	// Public constructors no longer produce file-backed writers with bufio
	// attached. Set up that stale internal state explicitly so Flush protects
	// against older writers or future regressions.
	writer.bw = bufio.NewWriterSize(writer.f, 16)
	if _, err := writer.bw.WriteString("abc"); err != nil {
		_ = writer.Close()
		t.Fatalf("buffered write: %v", err)
	}
	if err := writer.Flush(); err != nil {
		_ = writer.Close()
		t.Fatalf("flush: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(got) != "abc" {
		t.Fatalf("file contents=%q want %q", string(got), "abc")
	}
}

func TestWriterRotateTo_FlushesSinkBufferBeforeSwitchingToFileBacked(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	var sink bytes.Buffer
	writer := NewWriterWithSink(&sink, page.ValueLogFileID(1))
	if writer == nil {
		t.Fatal("NewWriterWithSink returned nil")
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = writer.Close()
		}
	})
	if _, err := writer.bw.WriteString("sink-data"); err != nil {
		t.Fatalf("buffered sink write: %v", err)
	}
	if err := writer.RotateTo(path, page.ValueLogFileID(2)); err != nil {
		t.Fatalf("RotateTo: %v", err)
	}
	if got := sink.String(); got != "sink-data" {
		t.Fatalf("sink contents=%q want %q", got, "sink-data")
	}
	if writer.bw != nil {
		t.Fatalf("expected RotateTo to drop sink-backed bufio state")
	}
	if err := writer.writeBytes([]byte("file-data")); err != nil {
		t.Fatalf("writeBytes(file): %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	closed = true
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(got) != "file-data" {
		t.Fatalf("file contents=%q want %q", string(got), "file-data")
	}
}

func TestValueLogAppendRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	ptrs, err := writer.AppendFrame(0, nil, []Record{
		{RID: 1, Value: []byte("alpha")},
		{RID: 2, Value: []byte("beta")},
	})
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append: %v", err)
	}
	if len(ptrs) != 2 {
		_ = writer.Close()
		t.Fatalf("expected 2 ptrs, got %d", len(ptrs))
	}
	ptr3, err := writer.Append(0, nil, 3, []byte("gamma"))
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reader, err := NewReader(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	rid1, val1, gotPtr1, err := reader.ReadNext()
	if err != nil {
		_ = reader.Close()
		t.Fatalf("read next: %v", err)
	}
	if rid1 != 1 || string(val1) != "alpha" {
		_ = reader.Close()
		t.Fatalf("record1 mismatch")
	}
	rid2, val2, gotPtr2, err := reader.ReadNext()
	if err != nil {
		_ = reader.Close()
		t.Fatalf("read next: %v", err)
	}
	if rid2 != 2 || string(val2) != "beta" {
		_ = reader.Close()
		t.Fatalf("record2 mismatch")
	}
	rid3, val3, gotPtr3, err := reader.ReadNext()
	if err != nil {
		_ = reader.Close()
		t.Fatalf("read next: %v", err)
	}
	if rid3 != 3 || string(val3) != "gamma" {
		_ = reader.Close()
		t.Fatalf("record3 mismatch")
	}
	if _, _, _, err := reader.ReadNext(); !errors.Is(err, io.EOF) {
		_ = reader.Close()
		t.Fatalf("expected EOF, got %v", err)
	}
	_ = reader.Close()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	read1, err := ReadAt(f, ptrs[0], true)
	if err != nil {
		_ = f.Close()
		t.Fatalf("read at ptr1: %v", err)
	}
	read2, err := ReadAt(f, ptrs[1], true)
	if err != nil {
		_ = f.Close()
		t.Fatalf("read at ptr2: %v", err)
	}
	read3, err := ReadAt(f, ptr3, true)
	if err != nil {
		_ = f.Close()
		t.Fatalf("read at ptr3: %v", err)
	}
	_ = f.Close()

	if string(read1) != "alpha" {
		t.Fatalf("ptr1 read mismatch")
	}
	if string(read2) != "beta" {
		t.Fatalf("ptr2 read mismatch")
	}
	if string(read3) != "gamma" {
		t.Fatalf("ptr3 read mismatch")
	}

	if gotPtr1 != ptrs[0] {
		t.Fatalf("ptr1 mismatch")
	}
	if gotPtr2 != ptrs[1] {
		t.Fatalf("ptr2 mismatch")
	}
	if gotPtr3 != ptr3 {
		t.Fatalf("ptr3 mismatch")
	}
}

func TestValueLogReaderReadNextMeta(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	ptrs, err := writer.AppendFrame(0, nil, []Record{
		{RID: 1, Value: []byte("alpha")},
		{RID: 2, Value: []byte("beta")},
	})
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append frame: %v", err)
	}
	ptr3, err := writer.Append(0, nil, 3, []byte("gamma"))
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reader, err := NewReader(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	rid1, gotPtr1, err := reader.ReadNextMeta()
	if err != nil {
		_ = reader.Close()
		t.Fatalf("read next meta: %v", err)
	}
	if rid1 != 1 {
		_ = reader.Close()
		t.Fatalf("rid1 mismatch")
	}
	rid2, gotPtr2, err := reader.ReadNextMeta()
	if err != nil {
		_ = reader.Close()
		t.Fatalf("read next meta: %v", err)
	}
	if rid2 != 2 {
		_ = reader.Close()
		t.Fatalf("rid2 mismatch")
	}
	rid3, gotPtr3, err := reader.ReadNextMeta()
	if err != nil {
		_ = reader.Close()
		t.Fatalf("read next meta: %v", err)
	}
	if rid3 != 3 {
		_ = reader.Close()
		t.Fatalf("rid3 mismatch")
	}
	if _, _, err := reader.ReadNextMeta(); !errors.Is(err, io.EOF) {
		_ = reader.Close()
		t.Fatalf("expected EOF, got %v", err)
	}
	_ = reader.Close()

	if gotPtr1 != ptrs[0] {
		t.Fatalf("ptr1 mismatch")
	}
	if gotPtr2 != ptrs[1] {
		t.Fatalf("ptr2 mismatch")
	}
	if gotPtr3 != ptr3 {
		t.Fatalf("ptr3 mismatch")
	}
}

func TestValueLogManager_MmapReadAppend(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

	dir := t.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("encode file id: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")

	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	ptr, err := writer.Append(0, nil, 1, []byte("hello"))
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	m.SetDisableReadChecksum(true)
	defer func() { _ = m.Close() }()

	f := m.files[fileID]
	f.remapToFileSize()
	data, _ := f.mmapData.Load().([]byte)
	if len(data) == 0 {
		t.Fatalf("expected mmap data to be present")
	}

	got, err := m.ReadAppend(ptr, nil)
	if err != nil {
		t.Fatalf("read append: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("mmap read mismatch: %q", string(got))
	}
}

func TestValueLogManager_MmapReadUnsafeCompressedGroupedCache(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

	dir := t.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("encode file id: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")

	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	writer.SetBlockCompression(BlockCodecSnappy, true)

	records := make([]Record, 16)
	want := make([][]byte, len(records))
	for i := range records {
		v := make([]byte, 320)
		copy(v, []byte(fmt.Sprintf("record-%02d:", i)))
		for j := 32; j < len(v); j++ {
			v[j] = 'x'
		}
		records[i] = Record{RID: uint64(i + 1), Value: v}
		want[i] = append([]byte(nil), v...)
	}
	dst := make([]page.ValuePtr, len(records))
	ptrs, stats, err := writer.AppendFrameWithStatsInto(0, nil, records, dst)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append frame: %v", err)
	}
	if !stats.Kept {
		_ = writer.Close()
		t.Fatalf("expected block-compressed frame to be kept")
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	var hdr [HeaderSize + FrameHeaderSize]byte
	if _, err := file.ReadAt(hdr[:], int64(ptrs[0].Offset)-4); err != nil {
		_ = file.Close()
		t.Fatalf("read header: %v", err)
	}
	_ = file.Close()
	if hdr[5]&recordFlagGrouped == 0 {
		t.Fatalf("expected grouped record flag")
	}
	if hdr[HeaderSize+1]&FrameFlagCompressed == 0 {
		t.Fatalf("expected compressed grouped frame")
	}

	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	m.SetDisableReadChecksum(true)
	defer func() { _ = m.Close() }()

	f := m.files[fileID]
	f.remapToFileSize()

	for i, ptr := range ptrs {
		got, err := m.ReadUnsafe(ptr)
		if err != nil {
			t.Fatalf("read unsafe %d: %v", i, err)
		}
		if !bytes.Equal(got, want[i]) {
			t.Fatalf("value mismatch at %d: got=%q want=%q", i, got, want[i])
		}
	}

	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	if readViaMmapViewPrefixCacheEnabled {
		if f.cacheFlags&FrameFlagCompressed == 0 {
			t.Fatalf("expected compressed frame cache flags, got=%d", f.cacheFlags)
		}
		if f.cacheK != len(records) {
			t.Fatalf("cacheK=%d want=%d", f.cacheK, len(records))
		}
		if len(f.cacheRaw) == 0 {
			t.Fatalf("expected cached decoded raw payload for compressed frame")
		}
	} else {
		if f.cacheFlags != 0 || f.cacheK != 0 || len(f.cacheRaw) != 0 {
			t.Fatalf("expected mmap view prefix cache disabled, flags=%d cacheK=%d raw=%d", f.cacheFlags, f.cacheK, len(f.cacheRaw))
		}
	}
}

func TestValueLogManager_MmapReadAppendCompressedGroupedCache(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

	dir := t.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("encode file id: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")

	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	writer.SetBlockCompression(BlockCodecSnappy, true)

	records := make([]Record, 12)
	want := make([][]byte, len(records))
	for i := range records {
		v := make([]byte, 320)
		copy(v, []byte(fmt.Sprintf("append-record-%02d:", i)))
		for j := 40; j < len(v); j++ {
			v[j] = 'y'
		}
		records[i] = Record{RID: uint64(i + 1), Value: v}
		want[i] = append([]byte(nil), v...)
	}
	dst := make([]page.ValuePtr, len(records))
	ptrs, stats, err := writer.AppendFrameWithStatsInto(0, nil, records, dst)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append frame: %v", err)
	}
	if !stats.Kept {
		_ = writer.Close()
		t.Fatalf("expected block-compressed frame to be kept")
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	m.SetDisableReadChecksum(true)
	defer func() { _ = m.Close() }()

	f := m.files[fileID]
	f.remapToFileSize()

	var got []byte
	for i, ptr := range ptrs {
		got, err = m.ReadAppend(ptr, got[:0])
		if err != nil {
			t.Fatalf("read append %d: %v", i, err)
		}
		if !bytes.Equal(got, want[i]) {
			t.Fatalf("value mismatch at %d: got=%q want=%q", i, got, want[i])
		}
	}

	hits, misses, entries, capacity := f.groupedFrameCacheStats()
	if entries == 0 {
		t.Fatalf("expected grouped-frame cache entries > 0 for compressed frame reads (capacity=%d)", capacity)
	}
	if hits == 0 {
		t.Fatalf("expected grouped-frame cache hits > 0 for repeated compressed frame reads (misses=%d entries=%d capacity=%d)", misses, entries, capacity)
	}
}

func appendCompressedFrameForCacheTests(t *testing.T, writer *Writer, frame int, n int) ([]page.ValuePtr, [][]byte) {
	t.Helper()
	records := make([]Record, n)
	want := make([][]byte, n)
	for i := 0; i < n; i++ {
		v := make([]byte, 384)
		copy(v, []byte(fmt.Sprintf("frame-%02d-record-%02d:", frame, i)))
		for j := 48; j < len(v); j++ {
			v[j] = byte('a' + frame)
		}
		records[i] = Record{RID: uint64(frame*100 + i + 1), Value: v}
		want[i] = append([]byte(nil), v...)
	}
	dst := make([]page.ValuePtr, len(records))
	ptrs, stats, err := writer.AppendFrameWithStatsInto(0, nil, records, dst)
	if err != nil {
		t.Fatalf("append frame %d: %v", frame, err)
	}
	if !stats.Kept {
		t.Fatalf("expected block-compressed frame %d to be kept", frame)
	}
	return ptrs, want
}

func TestValueLogManager_GroupedFrameCache_HitAndEvict(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

	dir := t.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("encode file id: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")

	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	writer.SetBlockCompression(BlockCodecSnappy, true)

	ptrs0, want0 := appendCompressedFrameForCacheTests(t, writer, 0, 4)
	ptrs1, want1 := appendCompressedFrameForCacheTests(t, writer, 1, 4)
	ptrs2, want2 := appendCompressedFrameForCacheTests(t, writer, 2, 4)
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()
	m.SetDisableReadChecksum(true)
	m.SetGroupedFrameCacheEntries(2)

	f := m.files[fileID]
	f.remapToFileSize()

	readAndCheck := func(ptr page.ValuePtr, want []byte) {
		got, err := m.ReadUnsafe(ptr)
		if err != nil {
			t.Fatalf("read unsafe: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("value mismatch: got=%q want=%q", got, want)
		}
	}

	readAndCheck(ptrs0[0], want0[0]) // miss frame0
	readAndCheck(ptrs0[1], want0[1]) // hit frame0
	readAndCheck(ptrs1[0], want1[0]) // miss frame1
	readAndCheck(ptrs2[0], want2[0]) // miss frame2, evicts one frame

	hitsBefore, missesBefore, _, _ := f.groupedFrameCacheStats()
	readAndCheck(ptrs0[2], want0[2]) // expected miss if frame0 was evicted
	hitsAfter, missesAfter, entries, capacity := f.groupedFrameCacheStats()

	if capacity != 2 {
		t.Fatalf("cache capacity=%d want=2", capacity)
	}
	if entries > 2 {
		t.Fatalf("cache entries=%d exceed capacity", entries)
	}
	if hitsBefore == 0 {
		t.Fatalf("expected at least one cache hit before eviction check")
	}
	if missesAfter != missesBefore+1 {
		t.Fatalf("expected frame0 reread to miss after eviction: misses before=%d after=%d (hits before=%d after=%d)", missesBefore, missesAfter, hitsBefore, hitsAfter)
	}
}

func TestValueLogManager_GroupedFrameCache_ChecksumModeIsolation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

	dir := t.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("encode file id: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")

	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	writer.SetBlockCompression(BlockCodecSnappy, true)
	ptrs, want := appendCompressedFrameForCacheTests(t, writer, 0, 4)
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()
	m.SetGroupedFrameCacheEntries(4)

	f := m.files[fileID]
	f.remapToFileSize()

	readAndCheck := func(ptr page.ValuePtr, expected []byte) {
		got, err := m.ReadUnsafe(ptr)
		if err != nil {
			t.Fatalf("read unsafe: %v", err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("value mismatch: got=%q want=%q", got, expected)
		}
	}

	m.SetDisableReadChecksum(true) // verifyCRC=false
	readAndCheck(ptrs[0], want[0]) // miss(false)
	_, misses1, _, _ := f.groupedFrameCacheStats()

	m.SetDisableReadChecksum(false) // verifyCRC=true
	readAndCheck(ptrs[0], want[0])  // miss(true) due to mode isolation
	hits2, misses2, _, _ := f.groupedFrameCacheStats()
	if misses2 != misses1+1 {
		t.Fatalf("expected checksum-mode switch to miss: before=%d after=%d", misses1, misses2)
	}

	readAndCheck(ptrs[1], want[1]) // hit(true)
	hits3, misses3, entries, _ := f.groupedFrameCacheStats()
	if hits3 != hits2+1 {
		t.Fatalf("expected verify-on second read to hit: hits before=%d after=%d", hits2, hits3)
	}
	if misses3 != misses2 {
		t.Fatalf("unexpected miss increase: before=%d after=%d", misses2, misses3)
	}

	m.SetDisableReadChecksum(true) // verifyCRC=false
	readAndCheck(ptrs[2], want[2]) // hit(false)
	hits4, _, _, _ := f.groupedFrameCacheStats()
	if hits4 != hits3+1 {
		t.Fatalf("expected verify-off second read to hit: hits before=%d after=%d", hits3, hits4)
	}
	if entries < 2 {
		t.Fatalf("expected separate cache entries for checksum modes, got entries=%d", entries)
	}
}

func TestValueLogManager_GroupedFrameCache_DisabledConfigParity(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

	dir := t.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("encode file id: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")

	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	writer.SetBlockCompression(BlockCodecSnappy, true)
	ptrs, want := appendCompressedFrameForCacheTests(t, writer, 0, 5)
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	readAll := func(m *Manager) [][]byte {
		got := make([][]byte, 0, len(ptrs)*2)
		for round := 0; round < 2; round++ {
			for i, ptr := range ptrs {
				v, err := m.ReadUnsafe(ptr)
				if err != nil {
					t.Fatalf("read unsafe round=%d idx=%d: %v", round, i, err)
				}
				if !bytes.Equal(v, want[i]) {
					t.Fatalf("value mismatch round=%d idx=%d", round, i)
				}
				got = append(got, append([]byte(nil), v...))
			}
		}
		return got
	}

	mDisabled, err := NewManager(dir)
	if err != nil {
		t.Fatalf("new manager (disabled): %v", err)
	}
	mDisabled.SetDisableReadChecksum(true)
	mDisabled.SetGroupedFrameCacheEntries(0)
	fDisabled := mDisabled.files[fileID]
	fDisabled.remapToFileSize()
	gotDisabled := readAll(mDisabled)
	hitsDisabled, _, entriesDisabled, capDisabled := fDisabled.groupedFrameCacheStats()
	if hitsDisabled != 0 {
		t.Fatalf("expected disabled cache to produce zero hits, got=%d", hitsDisabled)
	}
	if entriesDisabled != 0 || capDisabled != 0 {
		t.Fatalf("expected disabled cache to have no entries/capacity, entries=%d capacity=%d", entriesDisabled, capDisabled)
	}
	_ = mDisabled.Close()

	mEnabled, err := NewManager(dir)
	if err != nil {
		t.Fatalf("new manager (enabled): %v", err)
	}
	defer func() { _ = mEnabled.Close() }()
	mEnabled.SetDisableReadChecksum(true)
	mEnabled.SetGroupedFrameCacheEntries(4)
	fEnabled := mEnabled.files[fileID]
	fEnabled.remapToFileSize()
	gotEnabled := readAll(mEnabled)
	hitsEnabled, _, _, _ := fEnabled.groupedFrameCacheStats()
	if hitsEnabled == 0 {
		t.Fatalf("expected enabled cache to produce hits")
	}

	if len(gotDisabled) != len(gotEnabled) {
		t.Fatalf("result length mismatch disabled=%d enabled=%d", len(gotDisabled), len(gotEnabled))
	}
	for i := range gotDisabled {
		if !bytes.Equal(gotDisabled[i], gotEnabled[i]) {
			t.Fatalf("disabled/enabled parity mismatch at %d", i)
		}
	}
}

func TestValueLogManager_GroupedFrameCache_MaxRawBytesSkipsOversize(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

	dir := t.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("encode file id: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")

	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	writer.SetBlockCompression(BlockCodecSnappy, true)
	ptrs, want := appendCompressedFrameForCacheTests(t, writer, 0, 4)
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()
	m.SetDisableReadChecksum(true)
	m.SetGroupedFrameCacheEntries(4)
	m.SetGroupedFrameCacheMaxRawBytes(8)

	f := m.files[fileID]
	f.remapToFileSize()

	for i := 0; i < 2; i++ {
		got, err := m.ReadUnsafe(ptrs[0])
		if err != nil {
			t.Fatalf("read unsafe #%d: %v", i+1, err)
		}
		if !bytes.Equal(got, want[0]) {
			t.Fatalf("value mismatch #%d", i+1)
		}
	}

	hits, misses, entries, capacity := f.groupedFrameCacheStats()
	if capacity != 4 {
		t.Fatalf("cache capacity=%d want=4", capacity)
	}
	if entries != 0 {
		t.Fatalf("expected no cached entries when frame exceeds max raw bytes, got=%d", entries)
	}
	if hits != 0 {
		t.Fatalf("expected zero cache hits for oversized frame, got=%d", hits)
	}
	if misses < 2 {
		t.Fatalf("expected misses on both reads for oversized frame, got=%d", misses)
	}
}

func TestReadAtGroupedFastPathWithoutChecksum(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	ptrs, err := writer.AppendFrame(0, nil, []Record{
		{RID: 1, Value: []byte("alpha")},
		{RID: 2, Value: []byte("beta")},
		{RID: 3, Value: []byte("gamma")},
	})
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if len(ptrs) != 3 {
		t.Fatalf("expected 3 ptrs, got %d", len(ptrs))
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	expect := []string{"alpha", "beta", "gamma"}
	for i, ptr := range ptrs {
		got, err := ReadAtWithDict(f, ptr, false, nil, nil, nil, templ.DecodeOptions{})
		if err != nil {
			t.Fatalf("read at ptr%d: %v", i+1, err)
		}
		if string(got) != expect[i] {
			t.Fatalf("ptr%d mismatch: got %q want %q", i+1, got, expect[i])
		}

		// Also cover legacy pointers where record length is unknown (0) but the
		// grouped flag and sub-index are still set.
		legacy := ptr
		legacy.Length = page.ValuePtrMarkGrouped(0, page.ValuePtrSubIndex(ptr))
		gotLegacy, err := ReadAtWithDict(f, legacy, false, nil, nil, nil, templ.DecodeOptions{})
		if err != nil {
			t.Fatalf("read at legacy ptr%d: %v", i+1, err)
		}
		if string(gotLegacy) != expect[i] {
			t.Fatalf("legacy ptr%d mismatch: got %q want %q", i+1, gotLegacy, expect[i])
		}
	}
}

func TestReadAtGroupedFastPathSubIndexRange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}

	records := make([]Record, MaxFrameK)
	expect := make([]string, len(records))
	for i := range records {
		expect[i] = fmt.Sprintf("val-%02d", i)
		records[i] = Record{RID: uint64(i + 1), Value: []byte(expect[i])}
	}

	ptrs, err := writer.AppendFrame(0, nil, records)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if len(ptrs) != len(records) {
		t.Fatalf("expected %d ptrs, got %d", len(records), len(ptrs))
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	for i, ptr := range ptrs {
		got, err := ReadAtWithDict(f, ptr, false, nil, nil, nil, templ.DecodeOptions{})
		if err != nil {
			t.Fatalf("read at ptr%d: %v", i+1, err)
		}
		if string(got) != expect[i] {
			t.Fatalf("ptr%d mismatch: got %q want %q", i+1, string(got), expect[i])
		}
	}
}

func TestReadAtGroupedK128WithDict(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	records := make([]Record, MaxFrameK)
	expect := make([]string, len(records))
	samples := make([][]byte, len(records))
	payload := bytes.Repeat([]byte("a"), 1024)
	for i := range records {
		expect[i] = fmt.Sprintf("{\"type\":\"example\",\"id\":%d,\"payload\":\"%s\"}", i, payload)
		records[i] = Record{RID: uint64(i + 1), Value: []byte(expect[i])}
		samples[i] = records[i].Value
	}

	const dictID = uint64(1)
	const dictBytes = 8 << 10 // 8KiB
	history := make([]byte, 0, dictBytes)
	for i := range samples {
		if len(history) >= dictBytes {
			break
		}
		need := dictBytes - len(history)
		sample := samples[i]
		if len(sample) > need {
			sample = sample[:need]
		}
		history = append(history, sample...)
	}
	if len(history) < dictBytes {
		history = append(history, bytes.Repeat([]byte("x"), dictBytes-len(history))...)
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: samples,
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}
	if len(dict) == 0 {
		t.Fatalf("BuildDict: empty dict")
	}

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	ptrScratch := make([]page.ValuePtr, len(records))
	ptrs, stats, err := writer.AppendFrameWithStatsInto(dictID, dict, records, ptrScratch)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append: %v", err)
	}
	if stats.Attempted && !stats.Kept {
		_ = writer.Close()
		t.Fatalf("expected dict compression to keep compressed bytes")
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	dictLookup := func(id uint64) ([]byte, error) {
		if id != dictID {
			return nil, ErrMissingDict
		}
		return dict, nil
	}
	for i, ptr := range ptrs {
		got, err := ReadAtWithDict(f, ptr, true, dictLookup, nil, nil, templ.DecodeOptions{})
		if err != nil {
			t.Fatalf("read at ptr%d: %v", i+1, err)
		}
		if string(got) != expect[i] {
			t.Fatalf("ptr%d mismatch: got %q want %q", i+1, string(got), expect[i])
		}
	}
}

func TestAppendEncodedFrameInto_RoundTripWithDict(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	records := make([]Record, 16)
	expect := make([][]byte, len(records))
	samples := make([][]byte, len(records))
	for i := range records {
		v := []byte(fmt.Sprintf("{\"kind\":\"evt\",\"id\":%d,\"payload\":\"%s\"}", i, bytes.Repeat([]byte("z"), 256)))
		records[i] = Record{RID: uint64(i + 1), Value: v}
		expect[i] = append([]byte(nil), v...)
		samples[i] = v
	}

	const dictID = uint64(11)
	history := make([]byte, 0, 8<<10)
	for i := range samples {
		if len(history) >= cap(history) {
			break
		}
		need := cap(history) - len(history)
		s := samples[i]
		if len(s) > need {
			s = s[:need]
		}
		history = append(history, s...)
	}
	if len(history) < 8 {
		history = append(history, bytes.Repeat([]byte("x"), 8-len(history))...)
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: samples,
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}
	if len(dict) == 0 {
		t.Fatalf("BuildDict: empty dict")
	}

	body, _, err := EncodeFrameWithOptions(dictID, dict, records, zstd.SpeedFastest, false)
	if err != nil {
		t.Fatalf("EncodeFrameWithOptions: %v", err)
	}

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	ptrScratch := make([]page.ValuePtr, len(records))
	ptrs, err := writer.AppendEncodedFrameInto(body, len(records), ptrScratch)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("AppendEncodedFrameInto: %v", err)
	}
	if len(ptrs) != len(records) {
		_ = writer.Close()
		t.Fatalf("unexpected ptr count: got=%d want=%d", len(ptrs), len(records))
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	dictLookup := func(id uint64) ([]byte, error) {
		if id != dictID {
			return nil, ErrMissingDict
		}
		return dict, nil
	}
	for i, ptr := range ptrs {
		got, err := ReadAtWithDict(f, ptr, true, dictLookup, nil, nil, templ.DecodeOptions{})
		if err != nil {
			t.Fatalf("read at ptr%d: %v", i+1, err)
		}
		if !bytes.Equal(got, expect[i]) {
			t.Fatalf("ptr%d mismatch", i+1)
		}
	}
}

func TestFramePreparer_PrepareFrame_RoundTripWithDict(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	records := make([]Record, 16)
	expect := make([][]byte, len(records))
	samples := make([][]byte, len(records))
	for i := range records {
		v := []byte(fmt.Sprintf("{\"kind\":\"evt\",\"id\":%d,\"payload\":\"%s\"}", i, bytes.Repeat([]byte("z"), 256)))
		records[i] = Record{RID: uint64(i + 1), Value: v}
		expect[i] = append([]byte(nil), v...)
		samples[i] = v
	}

	const dictID = uint64(77)
	history := make([]byte, 0, 8<<10)
	for i := range samples {
		if len(history) >= cap(history) {
			break
		}
		need := cap(history) - len(history)
		s := samples[i]
		if len(s) > need {
			s = s[:need]
		}
		history = append(history, s...)
	}
	if len(history) < 8 {
		history = append(history, bytes.Repeat([]byte("x"), 8-len(history))...)
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: samples,
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}
	if len(dict) == 0 {
		t.Fatalf("BuildDict: empty dict")
	}

	prep := NewFramePreparer()
	prep.SetDictFrameEncoderOptions(zstd.SpeedFastest, false)
	body, stats, err := prep.PrepareFrame(dictID, dict, records)
	if err != nil {
		t.Fatalf("PrepareFrame: %v", err)
	}
	if stats.Records != len(records) {
		t.Fatalf("records mismatch: got=%d want=%d", stats.Records, len(records))
	}
	if stats.RawPayloadBytes <= 0 {
		t.Fatalf("expected raw payload bytes > 0")
	}
	if !stats.Attempted {
		t.Fatalf("expected compression attempt")
	}

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	ptrScratch := make([]page.ValuePtr, len(records))
	ptrs, err := writer.AppendEncodedFrameInto(body, len(records), ptrScratch)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("AppendEncodedFrameInto: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	dictLookup := func(id uint64) ([]byte, error) {
		if id != dictID {
			return nil, ErrMissingDict
		}
		return dict, nil
	}
	for i, ptr := range ptrs {
		got, err := ReadAtWithDict(f, ptr, true, dictLookup, nil, nil, templ.DecodeOptions{})
		if err != nil {
			t.Fatalf("read at ptr%d: %v", i+1, err)
		}
		if !bytes.Equal(got, expect[i]) {
			t.Fatalf("ptr%d mismatch", i+1)
		}
	}
}

func TestFramePreparer_PrepareFrameInto_ReusesBuffer(t *testing.T) {
	records := []Record{
		{RID: 1, Value: bytes.Repeat([]byte("alpha-001|"), 64)},
		{RID: 2, Value: bytes.Repeat([]byte("bravo-002|"), 64)},
		{RID: 3, Value: bytes.Repeat([]byte("charlie-003|"), 64)},
		{RID: 4, Value: bytes.Repeat([]byte("delta-004|"), 64)},
	}

	prep := NewFramePreparer()

	buf := make([]byte, 0, 8<<10)
	body1, stats1, err := prep.PrepareFrameInto(buf, 0, nil, records)
	if err != nil {
		t.Fatalf("PrepareFrameInto first: %v", err)
	}
	if len(body1) == 0 {
		t.Fatalf("expected non-empty frame body")
	}
	if stats1.Attempted {
		t.Fatalf("did not expect compression attempt without dict")
	}
	firstPtr := &body1[0]

	body2, _, err := prep.PrepareFrameInto(body1[:0], 0, nil, records)
	if err != nil {
		t.Fatalf("PrepareFrameInto second: %v", err)
	}
	if len(body2) == 0 {
		t.Fatalf("expected non-empty second frame body")
	}
	if &body2[0] != firstPtr {
		t.Fatalf("expected frame body buffer reuse")
	}
}

func TestFramePreparer_KeepPolicySkipsCompression(t *testing.T) {
	records := []Record{
		{RID: 1, Value: bytes.Repeat([]byte("alpha-001|"), 32)},
		{RID: 2, Value: bytes.Repeat([]byte("bravo-002|"), 32)},
		{RID: 3, Value: bytes.Repeat([]byte("charlie-003|"), 32)},
		{RID: 4, Value: bytes.Repeat([]byte("delta-004|"), 32)},
	}
	const dictID = uint64(9)
	samples := make([][]byte, 16)
	for i := range samples {
		samples[i] = []byte(fmt.Sprintf("{\"kind\":\"evt\",\"id\":%d,\"payload\":\"%s\"}", i, bytes.Repeat([]byte("q"), 128)))
	}
	history := make([]byte, 0, 8<<10)
	for i := range samples {
		if len(history) >= cap(history) {
			break
		}
		need := cap(history) - len(history)
		s := samples[i]
		if len(s) > need {
			s = s[:need]
		}
		history = append(history, s...)
	}
	if len(history) < 8 {
		history = append(history, bytes.Repeat([]byte("x"), 8-len(history))...)
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: samples,
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}

	prep := NewFramePreparer()
	prep.SetDictFrameEncoderOptions(zstd.SpeedFastest, false)
	// Encode cost dominates IO savings: skip compression before encode.
	prep.SetKeepPolicy(0.01, 10.0, 0.0)

	body, stats, err := prep.PrepareFrame(dictID, dict, records)
	if err != nil {
		t.Fatalf("PrepareFrame: %v", err)
	}
	if stats.Attempted {
		t.Fatalf("expected no compression attempt under keep policy")
	}
	if stats.Kept {
		t.Fatalf("expected raw body to be kept")
	}
	hdr, _, _, _, err := DecodeFrame(body)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	if hdr.Flags&FrameFlagCompressed != 0 {
		t.Fatalf("expected uncompressed frame body")
	}
	if hdr.DictID != dictID {
		t.Fatalf("dict id mismatch: got=%d want=%d", hdr.DictID, dictID)
	}
}

func TestReadAtLargeValueLengthHintOmitted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}

	overhead := headerWithoutCRC + FrameHeaderSize + 8 + 8
	n := int(page.ValuePtrGroupedMaxRecordLen) - overhead + 1
	if n <= 0 {
		t.Fatalf("computed invalid payload size: %d", n)
	}
	value := bytes.Repeat([]byte("a"), n)

	ptr, err := writer.Append(0, nil, 1, value)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append: %v", err)
	}
	if got := page.ValuePtrRecordLength(ptr); got != 0 {
		_ = writer.Close()
		t.Fatalf("expected length hint to be omitted for large value, got %d", got)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	got, err := ReadAtWithDict(f, ptr, true, nil, nil, nil, templ.DecodeOptions{})
	if err != nil {
		t.Fatalf("read at: %v", err)
	}
	if len(got) != len(value) {
		t.Fatalf("value length mismatch: got=%d want=%d", len(got), len(value))
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("value bytes mismatch")
	}
}

func TestReadAtGroupedLengthHintBit23RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}

	const fixedOverhead = uint32(headerWithoutCRC + FrameHeaderSize + 8 + 8)
	const targetRecordLen = uint32(0x00800080) // grouped hint bit23 set, still <= 24-bit max
	if targetRecordLen <= fixedOverhead {
		_ = writer.Close()
		t.Fatalf("invalid target record length: %d", targetRecordLen)
	}
	value := bytes.Repeat([]byte("z"), int(targetRecordLen-fixedOverhead))

	ptr, err := writer.Append(0, nil, 1, value)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append: %v", err)
	}
	if !page.ValuePtrIsGrouped(ptr) {
		_ = writer.Close()
		t.Fatalf("expected grouped pointer")
	}
	if got, want := page.ValuePtrRecordLength(ptr), targetRecordLen; got != want {
		_ = writer.Close()
		t.Fatalf("expected grouped hint round-trip got=%d want=%d", got, want)
	}

	// Simulate a pointer whose grouped length hint has bit23 set.
	legacyPtr := ptr
	legacyPtr.Length = page.ValuePtrMarkGrouped(targetRecordLen, page.ValuePtrSubIndex(ptr))
	if got, want := page.ValuePtrRecordLength(legacyPtr), targetRecordLen; got != want {
		_ = writer.Close()
		t.Fatalf("legacy decoded record length mismatch got=%d want=%d", got, want)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	got, err := ReadAtWithDict(f, legacyPtr, true, nil, nil, nil, templ.DecodeOptions{})
	if err != nil {
		t.Fatalf("read at: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("read value bytes mismatch")
	}
}

func TestValueLogCorruptCRC(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	if _, err := writer.Append(0, nil, 1, []byte("alpha")); err != nil {
		_ = writer.Close()
		t.Fatalf("append: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if len(data) < HeaderSize {
		t.Fatalf("unexpected record size %d", len(data))
	}
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	reader, err := NewReader(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	_, _, _, err = reader.ReadNext()
	if !errors.Is(err, ErrCorrupt) {
		_ = reader.Close()
		t.Fatalf("expected corrupt error, got %v", err)
	}
	_ = reader.Close()
}

func TestValueLogTruncatedRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	var header [HeaderSize]byte
	binary.LittleEndian.PutUint32(header[0:4], 0)
	header[4] = Version
	binary.LittleEndian.PutUint64(header[8:16], 1)
	binary.LittleEndian.PutUint32(header[16:20], 8)
	if _, err := f.Write(header[:]); err != nil {
		_ = f.Close()
		t.Fatalf("write header: %v", err)
	}
	if _, err := f.Write([]byte{0x01, 0x02}); err != nil {
		_ = f.Close()
		t.Fatalf("write payload: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reader, err := NewReader(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	_, _, _, err = reader.ReadNext()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		_ = reader.Close()
		t.Fatalf("expected unexpected EOF, got %v", err)
	}
	_ = reader.Close()
}

func TestEncodeFrameSkipsCompressionWithoutDict(t *testing.T) {
	value := bytes.Repeat([]byte("a"), 2048)
	body, header, err := EncodeFrame(0, nil, []Record{{RID: 1, Value: value}})
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	if header.Flags&FrameFlagCompressed != 0 {
		t.Fatalf("expected no compression flag without dict")
	}
	decoded, rids, offsets, payload, err := DecodeFrame(body)
	if err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	if decoded.Flags&FrameFlagCompressed != 0 {
		t.Fatalf("expected no compression flag in decoded header")
	}
	if len(rids) != 1 || rids[0] != 1 {
		t.Fatalf("unexpected rids: %v", rids)
	}
	if len(offsets) != 2 || offsets[1] != uint32(len(value)) {
		t.Fatalf("unexpected offsets: %v", offsets)
	}
	if !bytes.Equal(payload, value) {
		t.Fatalf("payload mismatch")
	}
}

func TestDecodeFrameValueBounds(t *testing.T) {
	records := []Record{
		{RID: 1, Value: []byte("alpha")},
		{RID: 2, Value: []byte("beta")},
		{RID: 3, Value: []byte("gamma")},
	}
	body, header, err := EncodeFrame(0, nil, records)
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}

	gotHeader, start, end, rawLen, payload, err := decodeFrameValueBounds(body, 1)
	if err != nil {
		t.Fatalf("decode bounds: %v", err)
	}
	if gotHeader != header {
		t.Fatalf("header mismatch: got=%+v want=%+v", gotHeader, header)
	}
	if start != uint32(len(records[0].Value)) {
		t.Fatalf("unexpected start: got=%d", start)
	}
	if end != uint32(len(records[0].Value)+len(records[1].Value)) {
		t.Fatalf("unexpected end: got=%d", end)
	}
	wantRawLen := uint32(len(records[0].Value) + len(records[1].Value) + len(records[2].Value))
	if rawLen != wantRawLen {
		t.Fatalf("unexpected rawLen: got=%d want=%d", rawLen, wantRawLen)
	}
	if !bytes.Equal(payload[start:end], records[1].Value) {
		t.Fatalf("payload slice mismatch: got=%q want=%q", payload[start:end], records[1].Value)
	}
}

func TestDecodeFrameValueBoundsCompressed(t *testing.T) {
	const dictID = 7
	samples := make([][]byte, 16)
	for i := range samples {
		samples[i] = []byte(fmt.Sprintf("{\"kind\":\"frame\",\"id\":%d,\"payload\":\"%s\"}", i, bytes.Repeat([]byte("z"), 192)))
	}
	history := make([]byte, 0, 8<<10)
	for i := range samples {
		if len(history) >= cap(history) {
			break
		}
		need := cap(history) - len(history)
		sample := samples[i]
		if len(sample) > need {
			sample = sample[:need]
		}
		history = append(history, sample...)
	}
	if len(history) < 8 {
		history = append(history, bytes.Repeat([]byte("x"), 8-len(history))...)
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: samples,
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}
	if len(dict) == 0 {
		t.Fatalf("BuildDict: empty dict")
	}

	records := []Record{
		{RID: 1, Value: bytes.Repeat([]byte("alpha-"), 64)},
		{RID: 2, Value: bytes.Repeat([]byte("beta-"), 64)},
		{RID: 3, Value: bytes.Repeat([]byte("gamma-"), 64)},
	}
	body, header, err := EncodeFrameWithOptions(dictID, dict, records, zstd.SpeedFastest, false)
	if err != nil {
		t.Fatalf("EncodeFrameWithOptions: %v", err)
	}
	if header.Flags&FrameFlagCompressed == 0 {
		t.Fatalf("expected compressed frame")
	}

	gotHeader, start, end, rawLen, payload, err := decodeFrameValueBounds(body, 1)
	if err != nil {
		t.Fatalf("decode bounds: %v", err)
	}
	if gotHeader != header {
		t.Fatalf("header mismatch: got=%+v want=%+v", gotHeader, header)
	}
	wantStart := uint32(len(records[0].Value))
	if start != wantStart {
		t.Fatalf("unexpected start: got=%d want=%d", start, wantStart)
	}
	wantEnd := wantStart + uint32(len(records[1].Value))
	if end != wantEnd {
		t.Fatalf("unexpected end: got=%d want=%d", end, wantEnd)
	}
	wantRawLen := uint32(len(records[0].Value) + len(records[1].Value) + len(records[2].Value))
	if rawLen != wantRawLen {
		t.Fatalf("unexpected rawLen: got=%d want=%d", rawLen, wantRawLen)
	}
	if len(payload) == 0 {
		t.Fatalf("expected compressed payload bytes")
	}
}

func TestDecodeFrameValueBoundsRejectsCorruptInputs(t *testing.T) {
	body, _, err := EncodeFrame(0, nil, []Record{
		{RID: 1, Value: []byte("alpha")},
		{RID: 2, Value: []byte("beta")},
	})
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}

	if _, _, _, _, _, err := decodeFrameValueBounds(body, 2); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt for bad subIndex, got %v", err)
	}

	badRID := append([]byte(nil), body...)
	ridOff := FrameHeaderSize + 8
	for i := 0; i < 8; i++ {
		badRID[ridOff+i] = 0
	}
	if _, _, _, _, _, err := decodeFrameValueBounds(badRID, 0); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt for zero rid, got %v", err)
	}

	badOffsets := append([]byte(nil), body...)
	offsetOff := FrameHeaderSize + (2 * 8)
	binary.LittleEndian.PutUint32(badOffsets[offsetOff+8:offsetOff+12], 1)
	if _, _, _, _, _, err := decodeFrameValueBounds(badOffsets, 0); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt for non-monotonic offsets, got %v", err)
	}

	truncated := append([]byte(nil), body...)
	if len(truncated) == 0 {
		t.Fatal("encoded frame body is unexpectedly empty")
	}
	truncated = truncated[:len(truncated)-1]
	if _, _, _, _, _, err := decodeFrameValueBounds(truncated, 0); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt for truncated payload, got %v", err)
	}
}

func TestValueLogBlockCodecRoundTrip(t *testing.T) {
	codecs := []BlockCodec{BlockCodecSnappy, BlockCodecLZ4}
	for _, codec := range codecs {
		t.Run(fmt.Sprintf("codec_%d", codec), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "value-000001.log")

			records := make([]Record, 16)
			for i := range records {
				records[i] = Record{
					RID:   uint64(i + 1),
					Value: bytes.Repeat([]byte("block-compressible-payload|"), 128),
				}
			}
			writer, err := NewWriter(path, page.ValueLogFileID(1))
			if err != nil {
				t.Fatalf("new writer: %v", err)
			}
			writer.SetBlockCompression(codec, true)
			dst := make([]page.ValuePtr, len(records))
			ptrs, stats, err := writer.AppendFrameWithStatsInto(0, nil, records, dst)
			if err != nil {
				_ = writer.Close()
				t.Fatalf("append frame: %v", err)
			}
			if !stats.Kept {
				_ = writer.Close()
				t.Fatalf("expected compressed block frame to be kept")
			}
			if err := writer.Close(); err != nil {
				t.Fatalf("close writer: %v", err)
			}

			f, err := os.Open(path)
			if err != nil {
				t.Fatalf("open file: %v", err)
			}
			t.Cleanup(func() { _ = f.Close() })

			for i, ptr := range ptrs {
				got, err := ReadAtWithDict(f, ptr, true, nil, nil, nil, templ.DecodeOptions{})
				if err != nil {
					t.Fatalf("read ptr[%d]: %v", i, err)
				}
				if !bytes.Equal(got, records[i].Value) {
					t.Fatalf("value mismatch at %d", i)
				}
			}

			fileData, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read file: %v", err)
			}
			bodyLen := binary.LittleEndian.Uint32(fileData[16:20])
			frameBody := fileData[HeaderSize : HeaderSize+int(bodyLen)]
			hdr, _, _, _, err := DecodeFrame(frameBody)
			if err != nil {
				t.Fatalf("decode frame: %v", err)
			}
			if hdr.Reserved != byte(codec) {
				t.Fatalf("codec id mismatch: got=%d want=%d", hdr.Reserved, codec)
			}
		})
	}
}

func TestValueLogBlockCodecSnappy_ReusesProvidedBuffer(t *testing.T) {
	raw := bytes.Repeat([]byte("compressible-payload-"), 256)
	need := snappy.MaxEncodedLen(len(raw))
	dst := make([]byte, need)

	out, err := encodeBlockPayload(BlockCodecSnappy, raw, dst[:0])
	if err != nil {
		t.Fatalf("encode block payload: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected non-empty encoded payload")
	}
	if &out[0] != &dst[0] {
		t.Fatalf("expected encode to reuse provided destination buffer")
	}
}

func TestValueLogUnsupportedBlockCodecID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	writer.SetBlockCompression(BlockCodecSnappy, true)
	records := []Record{
		{RID: 1, Value: bytes.Repeat([]byte("alpha-"), 256)},
		{RID: 2, Value: bytes.Repeat([]byte("alpha-"), 256)},
	}
	dst := make([]page.ValuePtr, len(records))
	ptrs, stats, err := writer.AppendFrameWithStatsInto(0, nil, records, dst)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append: %v", err)
	}
	if !stats.Kept {
		_ = writer.Close()
		t.Fatalf("expected compressed frame")
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if len(data) < HeaderSize+FrameHeaderSize {
		t.Fatalf("unexpected record length: %d", len(data))
	}
	// Set an invalid codec id in frame header reserved byte.
	data[HeaderSize+3] = 0xFE
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	_, err = ReadAtWithDict(f, ptrs[0], false, nil, nil, nil, templ.DecodeOptions{})
	if err == nil {
		t.Fatalf("expected error for unsupported codec id")
	}
	var codecErr UnsupportedBlockCodecError
	if !errors.As(err, &codecErr) {
		t.Fatalf("expected UnsupportedBlockCodecError, got %v", err)
	}
}

func TestValueLogBlockCodec_IncompressibleBackoff(t *testing.T) {
	w := NewWriterWithSink(io.Discard, page.ValueLogFileID(1))
	w.SetBlockCompression(BlockCodecSnappy, true)

	makeBatch := func(seed int64) []Record {
		rng := rand.New(rand.NewSource(seed))
		records := make([]Record, 8)
		for i := range records {
			v := make([]byte, 4096)
			if _, err := rng.Read(v); err != nil {
				t.Fatalf("rng read: %v", err)
			}
			records[i] = Record{RID: uint64(i + 1), Value: v}
		}
		return records
	}

	dst := make([]page.ValuePtr, 8)
	_, stats1, err := w.AppendFrameWithStatsInto(0, nil, makeBatch(1), dst)
	if err != nil {
		t.Fatalf("append first batch: %v", err)
	}
	if !stats1.Attempted {
		t.Fatalf("expected initial block compression attempt")
	}
	if stats1.Kept {
		t.Fatalf("expected incompressible batch to fall back to raw")
	}

	_, stats2, err := w.AppendFrameWithStatsInto(0, nil, makeBatch(2), dst)
	if err != nil {
		t.Fatalf("append second batch: %v", err)
	}
	if stats2.Attempted {
		t.Fatalf("expected backoff skip after incompressible attempt")
	}
}

func TestFramePreparer_BlockCodec_IncompressibleBackoff(t *testing.T) {
	prep := NewFramePreparer()
	prep.SetBlockCompression(BlockCodecSnappy, true)

	makeBatch := func(seed int64) []Record {
		rng := rand.New(rand.NewSource(seed))
		records := make([]Record, 8)
		for i := range records {
			v := make([]byte, 4096)
			if _, err := rng.Read(v); err != nil {
				t.Fatalf("rng read: %v", err)
			}
			records[i] = Record{RID: uint64(i + 1), Value: v}
		}
		return records
	}

	_, stats1, err := prep.PrepareFrame(0, nil, makeBatch(11))
	if err != nil {
		t.Fatalf("prepare first batch: %v", err)
	}
	if !stats1.Attempted {
		t.Fatalf("expected initial block compression attempt")
	}
	if stats1.Kept {
		t.Fatalf("expected incompressible batch to fall back to raw")
	}

	_, stats2, err := prep.PrepareFrame(0, nil, makeBatch(12))
	if err != nil {
		t.Fatalf("prepare second batch: %v", err)
	}
	if stats2.Attempted {
		t.Fatalf("expected backoff skip after incompressible attempt")
	}
}
