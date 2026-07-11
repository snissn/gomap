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
	"time"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func TestWriterFlushFlushesBufferedFileBackedWriter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")
	fileID := page.ValueLogFileID(1)

	writer, err := NewWriter(path, fileID)
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

func TestWriterRotateToWithSyncFalseSkipsRotateSync(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "value-l0-000001.log")
	path2 := filepath.Join(dir, "value-l0-000002.log")
	path3 := filepath.Join(dir, "value-l0-000003.log")

	origSyncDir := syncDirFn
	defer func() { syncDirFn = origSyncDir }()
	var dirSyncs int
	syncDirFn = func(string) error {
		dirSyncs++
		return nil
	}

	writer, err := NewWriter(path1, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() {
		if err := writer.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()
	if got := writer.DurabilityStats().DirectorySyncCalls; got != 1 {
		t.Fatalf("initial directory sync calls=%d want 1", got)
	}

	var fileSyncs int
	writer.syncFn = func(*os.File) error {
		fileSyncs++
		return nil
	}

	dirSyncs = 0
	if _, err := writer.Append(0, nil, 1, []byte("value-1")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := writer.RotateToWithSync(path2, page.ValueLogFileID(2), false); err != nil {
		t.Fatalf("RotateToWithSync(false): %v", err)
	}
	if fileSyncs != 0 {
		t.Fatalf("fileSyncs after relaxed rotate=%d want 0", fileSyncs)
	}
	if dirSyncs != 0 {
		t.Fatalf("dirSyncs after relaxed rotate=%d want 0", dirSyncs)
	}
	if got := writer.DurabilityStats(); got.FileSyncCalls != 0 || got.DirectorySyncCalls != 1 {
		t.Fatalf("durability stats after relaxed rotate=%+v want file=0 directory=1", got)
	}

	if err := writer.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if fileSyncs != 1 {
		t.Fatalf("fileSyncs after explicit Sync=%d want 1", fileSyncs)
	}

	if _, err := writer.Append(0, nil, 2, []byte("value-2")); err != nil {
		t.Fatalf("Append second: %v", err)
	}
	if err := writer.RotateToWithSync(path3, page.ValueLogFileID(3), true); err != nil {
		t.Fatalf("RotateToWithSync(true): %v", err)
	}
	if fileSyncs != 2 {
		t.Fatalf("fileSyncs after synced rotate=%d want 2", fileSyncs)
	}
	if dirSyncs != 1 {
		t.Fatalf("dirSyncs after synced rotate=%d want 1", dirSyncs)
	}
	if got := writer.DurabilityStats(); got.FileSyncCalls != 2 || got.DirectorySyncCalls != 2 || got.FileSyncErrors != 0 || got.DirectorySyncErrors != 0 {
		t.Fatalf("durability stats after sync and rotate=%+v want file=2 directory=2 and no errors", got)
	}
}

func TestNewStagingWriterSkipsDirectorySync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "staging.log")

	original := syncDirFn
	dirSyncs := 0
	syncDirFn = func(string) error {
		dirSyncs++
		return nil
	}
	t.Cleanup(func() { syncDirFn = original })

	writer, err := NewStagingWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewStagingWriter: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if dirSyncs != 0 {
		t.Fatalf("staging directory syncs=%d want 0", dirSyncs)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("staging file: %v", err)
	}
}

func TestValueLogAppendRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")
	fileID := page.ValueLogFileID(1)

	writer, err := NewWriter(path, fileID)
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

	reader, err := NewReader(path, fileID)
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
	fileID := page.ValueLogFileID(1)

	writer, err := NewWriter(path, fileID)
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

	reader, err := NewReader(path, fileID)
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
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open for ReadRIDAtUnverified: %v", err)
	}
	defer f.Close()
	for _, tc := range []struct {
		ptr page.ValuePtr
		rid uint64
	}{
		{ptr: ptrs[0], rid: 1},
		{ptr: ptrs[1], rid: 2},
		{ptr: ptr3, rid: 3},
	} {
		gotRID, err := ReadRIDAtUnverified(f, fileID, tc.ptr)
		if err != nil {
			t.Fatalf("ReadRIDAtUnverified(%+v): %v", tc.ptr, err)
		}
		if gotRID != tc.rid {
			t.Fatalf("ReadRIDAtUnverified(%+v)=%d, want %d", tc.ptr, gotRID, tc.rid)
		}
	}
	badPtr := ptr3
	badPtr.Offset = 3
	if _, err := ReadRIDAtUnverified(f, fileID, badPtr); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("ReadRIDAtUnverified short offset error=%v, want ErrCorrupt", err)
	}
	badPtr = ptr3
	badPtr.FileID++
	if _, err := ReadRIDAtUnverified(f, fileID, badPtr); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("ReadRIDAtUnverified wrong fileID error=%v, want ErrCorrupt", err)
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

	before := GrowBufferStatsSnapshot()
	got, err := m.ReadAppend(ptr, nil)
	if err != nil {
		t.Fatalf("read append: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("mmap read mismatch: %q", string(got))
	}
	afterNil := GrowBufferStatsSnapshot()
	if got := afterNil.ReadAppendDecodedPayloadCallsTotal - before.ReadAppendDecodedPayloadCallsTotal; got != 1 {
		t.Fatalf("decoded payload calls after nil dst=%d want 1", got)
	}
	if got := afterNil.ReadAppendDecodedPayloadRequestedBytesTotal - before.ReadAppendDecodedPayloadRequestedBytesTotal; got != uint64(len("hello")) {
		t.Fatalf("decoded payload requested after nil dst=%d want %d", got, len("hello"))
	}
	if got := afterNil.ReadAppendDecodedPayloadDstFitCallsTotal - before.ReadAppendDecodedPayloadDstFitCallsTotal; got != 0 {
		t.Fatalf("decoded payload dst-fit calls after nil dst=%d want 0", got)
	}

	dst := make([]byte, 0, 16)
	got, err = m.ReadAppend(ptr, dst)
	if err != nil {
		t.Fatalf("read append reused dst: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("mmap read reused dst mismatch: %q", string(got))
	}
	afterDst := GrowBufferStatsSnapshot()
	if got := afterDst.ReadAppendDecodedPayloadDstPresentCallsTotal - afterNil.ReadAppendDecodedPayloadDstPresentCallsTotal; got != 1 {
		t.Fatalf("decoded payload dst-present calls after reused dst=%d want 1", got)
	}
	if got := afterDst.ReadAppendDecodedPayloadDstFitCallsTotal - afterNil.ReadAppendDecodedPayloadDstFitCallsTotal; got != 1 {
		t.Fatalf("decoded payload dst-fit calls after reused dst=%d want 1", got)
	}
	if got := afterDst.ReadAppendDecodedPayloadDstFitRequestedBytesTotal - afterNil.ReadAppendDecodedPayloadDstFitRequestedBytesTotal; got != uint64(len("hello")) {
		t.Fatalf("decoded payload dst-fit requested after reused dst=%d want %d", got, len("hello"))
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
	if capacity != 0 {
		t.Fatalf("oversized reads should not create idle cache metadata, capacity=%d", capacity)
	}
	if entries != 0 {
		t.Fatalf("expected no cached entries when frame exceeds max raw bytes, got=%d", entries)
	}
	if hits != 0 {
		t.Fatalf("expected zero cache hits for oversized frame, got=%d", hits)
	}
	if misses != 0 {
		t.Fatalf("oversized reads should not create miss stats without a cache, got=%d", misses)
	}
}

func TestValueLogManager_ReadUnsafeTo_CompressedGroupedFallbackSkipsOversizeCache(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

	// Force file-read fallback so this test exercises readGroupedCompressedFromFileTo.
	withMappedSealedBudget(t, 0)

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
	if f == nil {
		t.Fatalf("missing opened file for id=%d", fileID)
	}
	if cache := f.groupedFrameCache.Load(); cache != nil {
		t.Fatalf("oversize config created grouped cache before reads")
	}

	dst := make([]byte, 0, 512)
	for i := 0; i < 2; i++ {
		got, used, err := m.ReadUnsafeTo(ptrs[i], dst[:0])
		if err != nil {
			t.Fatalf("read unsafe to oversize fallback idx=%d: %v", i, err)
		}
		if !used {
			t.Fatalf("expected fallback read idx=%d to use dst", i)
		}
		if !bytes.Equal(got, want[i]) {
			t.Fatalf("value mismatch idx=%d: got=%q want=%q", i, got, want[i])
		}
	}

	hits, misses, entries, capacity := f.groupedFrameCacheStats()
	if hits != 0 || misses != 0 || entries != 0 || capacity != 0 {
		t.Fatalf("oversize fallback reads created grouped cache metadata/stats: hits=%d misses=%d entries=%d capacity=%d", hits, misses, entries, capacity)
	}
	if cache := f.groupedFrameCache.Load(); cache != nil {
		t.Fatalf("oversize fallback reads materialized grouped cache")
	}
	_, _, missNoMapping, _, fallbacks := m.MmapReadStats()
	if missNoMapping == 0 || fallbacks == 0 {
		t.Fatalf("expected fallback path stats to reflect no-mmap reads: miss_no_mapping=%d fallbacks=%d", missNoMapping, fallbacks)
	}
}

func TestValueLogManager_ReadUnsafeTo_CompressedGroupedFallbackUsesCache(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

	// Force file-read fallback so this test exercises the non-mmap path.
	withMappedSealedBudget(t, 0)

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

	f := m.files[fileID]
	if f == nil {
		t.Fatalf("missing opened file for id=%d", fileID)
	}

	dst := make([]byte, 0, 512)
	got0, used0, err := m.ReadUnsafeTo(ptrs[0], dst[:0])
	if err != nil {
		t.Fatalf("read unsafe to first: %v", err)
	}
	if !used0 {
		t.Fatalf("expected first read to use dst")
	}
	if !bytes.Equal(got0, want[0]) {
		t.Fatalf("first value mismatch: got=%q want=%q", got0, want[0])
	}

	hits0, misses0, entries0, _ := f.groupedFrameCacheStats()
	if hits0 != 0 {
		t.Fatalf("unexpected grouped cache hit on first read: %d", hits0)
	}
	if entries0 == 0 {
		t.Fatalf("expected first compressed grouped read to populate cache")
	}

	got1, used1, err := m.ReadUnsafeTo(ptrs[1], dst[:0])
	if err != nil {
		t.Fatalf("read unsafe to second: %v", err)
	}
	if !used1 {
		t.Fatalf("expected second read to use dst")
	}
	if !bytes.Equal(got1, want[1]) {
		t.Fatalf("second value mismatch: got=%q want=%q", got1, want[1])
	}

	hits1, misses1, entries1, _ := f.groupedFrameCacheStats()
	if hits1 <= hits0 {
		t.Fatalf("expected second read to hit grouped cache: hits before=%d after=%d", hits0, hits1)
	}
	if misses1 != misses0 {
		t.Fatalf("unexpected cache miss increase on second read: before=%d after=%d", misses0, misses1)
	}
	if entries1 == 0 {
		t.Fatalf("expected grouped cache entries to remain populated")
	}

	_, _, missNoMapping, _, fallbacks := m.MmapReadStats()
	if missNoMapping == 0 || fallbacks == 0 {
		t.Fatalf("expected fallback path stats to reflect no-mmap reads: miss_no_mapping=%d fallbacks=%d", missNoMapping, fallbacks)
	}
}

func TestValueLogManager_ReadUnsafeTo_CompressedGroupedMmapUsesCache(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

	withMappedSealedBudget(t, 8)

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

	f := m.files[fileID]
	if f == nil {
		t.Fatalf("missing opened file for id=%d", fileID)
	}
	f.remapToFileSize()
	hitsBefore, _, _, _, fallbacksBefore := m.MmapReadStats()

	dst := make([]byte, 0, 512)
	got0, used0, err := m.ReadUnsafeTo(ptrs[0], dst[:0])
	if err != nil {
		t.Fatalf("read unsafe to first: %v", err)
	}
	if !used0 {
		t.Fatalf("expected first read to use dst")
	}
	if !bytes.Equal(got0, want[0]) {
		t.Fatalf("first value mismatch: got=%q want=%q", got0, want[0])
	}
	hitsAfterFirst, _, _, _, fallbacksAfterFirst := m.MmapReadStats()
	if hitsAfterFirst <= hitsBefore {
		t.Fatalf("expected first read to use mmap path: hits before=%d after=%d", hitsBefore, hitsAfterFirst)
	}
	if fallbacksAfterFirst != fallbacksBefore {
		t.Fatalf("unexpected readat fallback on first read: before=%d after=%d", fallbacksBefore, fallbacksAfterFirst)
	}

	hits0, misses0, entries0, _ := f.groupedFrameCacheStats()
	if hits0 != 0 {
		t.Fatalf("unexpected grouped cache hit on first mmap read: %d", hits0)
	}
	if entries0 == 0 {
		t.Fatalf("expected first mmap grouped read to populate cache")
	}

	got1, used1, err := m.ReadUnsafeTo(ptrs[1], dst[:0])
	if err != nil {
		t.Fatalf("read unsafe to second: %v", err)
	}
	if !used1 {
		t.Fatalf("expected second read to use dst")
	}
	if !bytes.Equal(got1, want[1]) {
		t.Fatalf("second value mismatch: got=%q want=%q", got1, want[1])
	}
	hitsAfterSecond, _, _, _, fallbacksAfterSecond := m.MmapReadStats()
	if hitsAfterSecond <= hitsAfterFirst {
		t.Fatalf("expected second read to use mmap path: hits before=%d after=%d", hitsAfterFirst, hitsAfterSecond)
	}
	if fallbacksAfterSecond != fallbacksBefore {
		t.Fatalf("unexpected readat fallback after second read: before=%d after=%d", fallbacksBefore, fallbacksAfterSecond)
	}

	hits1, misses1, entries1, _ := f.groupedFrameCacheStats()
	if entries1 != entries0 {
		t.Fatalf("cache entries changed after hit: before=%d after=%d", entries0, entries1)
	}
	if hits1 <= hits0 {
		t.Fatalf("expected second mmap read to hit grouped cache: hits before=%d after=%d", hits0, hits1)
	}
	if misses1 != misses0 {
		t.Fatalf("unexpected cache miss increase on second mmap read: before=%d after=%d", misses0, misses1)
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

func TestAppendEncodedFrameOne_RoundTripAndAllocsBudget(t *testing.T) {
	records := []Record{{RID: 1, Value: []byte("single prepared frame payload")}}
	prep := NewFramePreparer()
	body, _, err := prep.PrepareFrameInto(nil, 0, nil, records)
	if err != nil {
		t.Fatalf("PrepareFrameInto: %v", err)
	}

	clock := NewVirtualClock(time.Unix(0, 0))
	writer := NewWriterWithSink(&VirtualSink{Clock: clock}, page.ValueLogFileID(1))
	if _, err := writer.AppendEncodedFrameOne(body); err != nil {
		t.Fatalf("warm AppendEncodedFrameOne: %v", err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := writer.AppendEncodedFrameOne(body); err != nil {
			t.Fatalf("AppendEncodedFrameOne: %v", err)
		}
	})
	if allocs > 0 {
		t.Fatalf("AppendEncodedFrameOne steady-state allocs = %.2f, want 0", allocs)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")
	fileWriter, err := NewWriter(path, page.ValueLogFileID(2))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	ptr, err := fileWriter.AppendEncodedFrameOne(body)
	if err != nil {
		_ = fileWriter.Close()
		t.Fatalf("AppendEncodedFrameOne file: %v", err)
	}
	if err := fileWriter.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	defer f.Close()
	got, err := ReadAtWithDict(f, ptr, true, nil, nil, nil, templ.DecodeOptions{})
	if err != nil {
		t.Fatalf("ReadAtWithDict: %v", err)
	}
	if !bytes.Equal(got, records[0].Value) {
		t.Fatalf("round trip mismatch: got %q want %q", got, records[0].Value)
	}
}

func TestFramePreparerAndAppendEncodedFrameIntoAllocsBudget(t *testing.T) {
	records := []Record{{RID: 1, Value: bytes.Repeat([]byte("x"), 256)}}
	prep := NewFramePreparer()
	body, _, err := prep.PrepareFrameInto(nil, 0, nil, records)
	if err != nil {
		t.Fatalf("warm PrepareFrameInto: %v", err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		var prepErr error
		body, _, prepErr = prep.PrepareFrameInto(body[:0], 0, nil, records)
		if prepErr != nil {
			t.Fatalf("PrepareFrameInto: %v", prepErr)
		}
	})
	if allocs > 0 {
		t.Fatalf("PrepareFrameInto steady-state allocs = %.2f, want 0", allocs)
	}

	clock := NewVirtualClock(time.Unix(0, 0))
	writer := NewWriterWithSink(&VirtualSink{Clock: clock}, page.ValueLogFileID(1))
	ptrScratch := make([]page.ValuePtr, len(records))
	if _, err := writer.AppendEncodedFrameInto(body, len(records), ptrScratch); err != nil {
		t.Fatalf("warm AppendEncodedFrameInto: %v", err)
	}
	allocs = testing.AllocsPerRun(1000, func() {
		if _, err := writer.AppendEncodedFrameInto(body, len(records), ptrScratch); err != nil {
			t.Fatalf("AppendEncodedFrameInto: %v", err)
		}
	})
	if allocs > 0 {
		t.Fatalf("AppendEncodedFrameInto steady-state allocs = %.2f, want 0", allocs)
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

func TestFramePreparer_ResetForReuseRestoresDefaultsAndKeepsBoundedScratch(t *testing.T) {
	records := []Record{
		{RID: 1, Value: bytes.Repeat([]byte("alpha-001|"), 256)},
		{RID: 2, Value: bytes.Repeat([]byte("bravo-002|"), 256)},
	}
	prep := NewFramePreparer()
	prep.SetBlockCompression(BlockCodecLZ4, true)
	prep.SetKeepPolicy(1e9, 0, 1)
	if _, stats, err := prep.PrepareFrameInto(nil, 0, nil, records); err != nil {
		t.Fatalf("PrepareFrameInto block: %v", err)
	} else if !stats.Attempted {
		t.Fatalf("expected block compression attempt")
	}
	rawCap := cap(prep.rawScratch)
	blockCap := cap(prep.blockScratch)
	if blockCap == 0 {
		t.Fatalf("expected block scratch allocation, rawCap=%d blockCap=%d", rawCap, blockCap)
	}
	prep.skipDictID = 99
	prep.noBenefit = 7
	prep.skipRemain = 13
	prep.encodeSampleStride = 5
	prep.encodeSampleCount = 11

	prep.ResetForReuse()
	if cap(prep.rawScratch) != rawCap || cap(prep.blockScratch) != blockCap {
		t.Fatalf("scratch caps after reset raw=%d/%d block=%d/%d", cap(prep.rawScratch), rawCap, cap(prep.blockScratch), blockCap)
	}
	if prep.blockCodec != BlockCodecSnappy || prep.blockCompression || prep.dictFrameEncodeLevel != zstd.SpeedFastest || prep.keepSafetyMargin != DefaultKeepSafetyMargin {
		t.Fatalf("defaults not restored: codec=%v block=%v level=%v margin=%v", prep.blockCodec, prep.blockCompression, prep.dictFrameEncodeLevel, prep.keepSafetyMargin)
	}
	if prep.skipDictID != 0 || prep.noBenefit != 0 || prep.skipRemain != 0 || prep.encodeSampleStride != 0 || prep.encodeSampleCount != 0 {
		t.Fatalf("hints/sampling not reset: skipDictID=%d noBenefit=%d skipRemain=%d stride=%d count=%d", prep.skipDictID, prep.noBenefit, prep.skipRemain, prep.encodeSampleStride, prep.encodeSampleCount)
	}
}

func TestFramePreparer_TrimScratchForReuseKeepsPolicyAndBoundsBuffers(t *testing.T) {
	prep := NewFramePreparer()
	prep.SetKeepPolicy(11, 13, 0.25)
	prep.SetBlockCompression(BlockCodecLZ4, true)
	prep.rawScratch = make([]byte, framePreparerScratchKeepCap+1)
	prep.encScratch = make([]byte, 0, framePreparerScratchKeepCap+1)
	prep.blockScratch = make([]byte, 0, framePreparerScratchKeepCap)
	prep.encLimiter.buf = []byte("retained")
	prep.TrimScratchForReuse()
	if prep.rawScratch != nil || prep.encScratch != nil {
		t.Fatalf("oversized scratch retained: raw=%d enc=%d", cap(prep.rawScratch), cap(prep.encScratch))
	}
	if cap(prep.blockScratch) != framePreparerScratchKeepCap || len(prep.blockScratch) != 0 {
		t.Fatalf("bounded block scratch not retained/reset: len=%d cap=%d", len(prep.blockScratch), cap(prep.blockScratch))
	}
	if prep.encLimiter.buf != nil || prep.encLimiter.limit != 0 {
		t.Fatalf("enc limiter retained state: %+v", prep.encLimiter)
	}
	if prep.keepIoNsPerStoredByte != 11 || prep.keepEncodeNsPerRawByte != 13 || prep.keepSafetyMargin != 0.25 {
		t.Fatalf("trim reset keep policy: got io=%v encode=%v safety=%v", prep.keepIoNsPerStoredByte, prep.keepEncodeNsPerRawByte, prep.keepSafetyMargin)
	}
	if prep.blockCodec != BlockCodecLZ4 || !prep.blockCompression {
		t.Fatalf("trim reset block compression state")
	}
}

func TestFramePreparer_DictEncoderRetainedAcrossTrimAndSameOptions(t *testing.T) {
	const dictID = uint64(3553)
	dict, records := buildLargeDictCompressionFixture(t, dictID)
	dict2, records2 := buildLargeDictCompressionFixture(t, dictID+1)

	prep := NewFramePreparer()
	prep.SetDictFrameEncoderOptions(zstd.SpeedFastest, false)
	body, stats, err := prep.PrepareFrame(dictID, dict, records)
	if err != nil {
		t.Fatalf("PrepareFrame: %v", err)
	}
	if !stats.Attempted || !stats.Kept {
		t.Fatalf("expected kept dict-compressed frame: attempted=%v kept=%v raw=%d stored=%d", stats.Attempted, stats.Kept, stats.RawPayloadBytes, stats.StoredPayloadBytes)
	}
	if prep.dictEncoder == nil || prep.dictEncoderCodecs == nil {
		t.Fatalf("expected retained dict encoder")
	}
	retained := prep.dictEncoder
	retainedKey := prep.dictEncoderKey

	prep.TrimScratchForReuse()
	if prep.dictEncoder != retained || prep.dictEncoderKey != retainedKey {
		t.Fatalf("trim released retained dict encoder")
	}

	prep.SetDictFrameEncoderOptions(zstd.SpeedFastest, false)
	if prep.dictEncoder != retained || prep.dictEncoderKey != retainedKey {
		t.Fatalf("same encoder options released retained dict encoder")
	}
	body, stats, err = prep.PrepareFrameInto(body[:0], dictID, dict, records)
	if err != nil {
		t.Fatalf("second PrepareFrameInto: %v", err)
	}
	if !stats.Attempted || !stats.Kept {
		t.Fatalf("expected second kept dict-compressed frame: attempted=%v kept=%v raw=%d stored=%d", stats.Attempted, stats.Kept, stats.RawPayloadBytes, stats.StoredPayloadBytes)
	}
	if prep.dictEncoder != retained || prep.dictEncoderKey != retainedKey {
		t.Fatalf("second frame did not reuse retained dict encoder")
	}

	prep.SetDictFrameEncoderOptions(zstd.SpeedDefault, false)
	if prep.dictEncoder != nil || prep.dictEncoderCodecs != nil || prep.dictEncoderKey != (dictCodecKey{}) {
		t.Fatalf("option change retained dict encoder state: encoder=%p codecs=%p key=%+v", prep.dictEncoder, prep.dictEncoderCodecs, prep.dictEncoderKey)
	}
	body, stats, err = prep.PrepareFrameInto(body[:0], dictID, dict, records)
	if err != nil {
		t.Fatalf("third PrepareFrameInto after option change: %v", err)
	}
	if !stats.Attempted || !stats.Kept {
		t.Fatalf("expected third kept dict-compressed frame: attempted=%v kept=%v raw=%d stored=%d", stats.Attempted, stats.Kept, stats.RawPayloadBytes, stats.StoredPayloadBytes)
	}
	if prep.dictEncoder == nil || prep.dictEncoderCodecs == nil {
		t.Fatalf("expected retained dict encoder after option change")
	}
	reacquired := prep.dictEncoder
	reacquiredKey := prep.dictEncoderKey
	if reacquiredKey == retainedKey {
		t.Fatalf("option change reused old encoder key: encoder=%p key=%+v", reacquired, reacquiredKey)
	}

	body, stats, err = prep.PrepareFrameInto(body[:0], dictID+1, dict2, records2)
	if err != nil {
		t.Fatalf("fourth PrepareFrameInto after dict change: %v", err)
	}
	if !stats.Attempted || !stats.Kept {
		t.Fatalf("expected fourth kept dict-compressed frame: attempted=%v kept=%v raw=%d stored=%d", stats.Attempted, stats.Kept, stats.RawPayloadBytes, stats.StoredPayloadBytes)
	}
	if prep.dictEncoder == nil || prep.dictEncoderCodecs == nil {
		t.Fatalf("expected retained dict encoder after dict change")
	}
	if prep.dictEncoderKey == reacquiredKey {
		t.Fatalf("dict change reused old encoder key: encoder=%p key=%+v", prep.dictEncoder, prep.dictEncoderKey)
	}

	prep.ResetForReuse()
	if prep.dictEncoder != nil || prep.dictEncoderCodecs != nil || prep.dictEncoderKey != (dictCodecKey{}) {
		t.Fatalf("reset retained dict encoder state: encoder=%p codecs=%p key=%+v", prep.dictEncoder, prep.dictEncoderCodecs, prep.dictEncoderKey)
	}
}

func TestWriter_DictEncoderRetainedAcrossFramesAndReleasedOnClose(t *testing.T) {
	const dictID = uint64(3554)
	dict, records := buildLargeDictCompressionFixture(t, dictID)

	w := NewWriterWithSink(io.Discard, page.ValueLogFileID(1))
	w.SetDictFrameEncoderOptions(zstd.SpeedFastest, false)
	dst := make([]page.ValuePtr, len(records))
	_, stats, err := w.AppendFrameWithStatsInto(dictID, dict, records, dst)
	if err != nil {
		t.Fatalf("AppendFrameWithStatsInto: %v", err)
	}
	if !stats.Attempted || !stats.Kept {
		t.Fatalf("expected kept dict-compressed frame: attempted=%v kept=%v raw=%d stored=%d", stats.Attempted, stats.Kept, stats.RawPayloadBytes, stats.StoredPayloadBytes)
	}
	if w.dictEncoder == nil || w.dictEncoderCodecs == nil {
		t.Fatalf("expected retained dict encoder")
	}
	retained := w.dictEncoder
	retainedKey := w.dictEncoderKey

	_, stats, err = w.AppendFrameWithStatsInto(dictID, dict, records, dst)
	if err != nil {
		t.Fatalf("second AppendFrameWithStatsInto: %v", err)
	}
	if !stats.Attempted || !stats.Kept {
		t.Fatalf("expected second kept dict-compressed frame: attempted=%v kept=%v raw=%d stored=%d", stats.Attempted, stats.Kept, stats.RawPayloadBytes, stats.StoredPayloadBytes)
	}
	if w.dictEncoder != retained || w.dictEncoderKey != retainedKey {
		t.Fatalf("second frame did not reuse retained dict encoder")
	}

	w.SetDictFrameEncoderOptions(zstd.SpeedFastest, false)
	if w.dictEncoder != retained || w.dictEncoderKey != retainedKey {
		t.Fatalf("same encoder options released retained dict encoder")
	}
	w.SetDictFrameEncoderOptions(zstd.SpeedDefault, false)
	if w.dictEncoder != nil || w.dictEncoderCodecs != nil || w.dictEncoderKey != (dictCodecKey{}) {
		t.Fatalf("option change retained dict encoder state: encoder=%p codecs=%p key=%+v", w.dictEncoder, w.dictEncoderCodecs, w.dictEncoderKey)
	}

	_, stats, err = w.AppendFrameWithStatsInto(dictID, dict, records, dst)
	if err != nil {
		t.Fatalf("third AppendFrameWithStatsInto: %v", err)
	}
	if !stats.Attempted || !stats.Kept {
		t.Fatalf("expected third kept dict-compressed frame: attempted=%v kept=%v raw=%d stored=%d", stats.Attempted, stats.Kept, stats.RawPayloadBytes, stats.StoredPayloadBytes)
	}
	if w.dictEncoder == nil || w.dictEncoderCodecs == nil {
		t.Fatalf("expected retained dict encoder after option change")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if w.dictEncoder != nil || w.dictEncoderCodecs != nil || w.dictEncoderKey != (dictCodecKey{}) {
		t.Fatalf("close retained dict encoder state: encoder=%p codecs=%p key=%+v", w.dictEncoder, w.dictEncoderCodecs, w.dictEncoderKey)
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

func buildLargeDictCompressionFixture(t *testing.T, dictID uint64) ([]byte, []Record) {
	t.Helper()
	const payloadSize = 43 << 10
	makePayload := func(variant int) []byte {
		prefix := []byte(fmt.Sprintf("{\"bucket\":\"state\",\"variant\":%d,\"payload\":\"", variant))
		suffix := []byte("\"}")
		chunk := []byte("ibc/client/07-tendermint/consensusStates/00000000000000000000|")
		out := make([]byte, 0, payloadSize)
		out = append(out, prefix...)
		for len(out)+len(suffix) < payloadSize {
			out = append(out, chunk...)
		}
		out = out[:payloadSize-len(suffix)]
		out = append(out, suffix...)
		// Keep values very compressible while ensuring record-to-record variance.
		out[payloadSize-64] = byte('a' + (variant % 26))
		out[payloadSize-63] = byte('0' + (variant % 10))
		return out
	}

	samples := make([][]byte, 32)
	history := make([]byte, 0, 8<<10)
	for i := range samples {
		s := makePayload(i)
		samples[i] = s
		if len(history) < cap(history) {
			need := cap(history) - len(history)
			if need > len(s) {
				need = len(s)
			}
			history = append(history, s[:need]...)
		}
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

	records := []Record{
		{RID: 1, Value: append([]byte(nil), samples[0]...)},
		{RID: 2, Value: append([]byte(nil), samples[1]...)},
	}
	return dict, records
}

func TestFramePreparer_KeepPolicyLargeDictOverride(t *testing.T) {
	const dictID = uint64(19)
	dict, records := buildLargeDictCompressionFixture(t, dictID)

	prep := NewFramePreparer()
	prep.SetDictFrameEncoderOptions(zstd.SpeedFastest, false)
	// Deliberately pessimistic keep policy: small payloads should skip.
	prep.SetKeepPolicy(0.01, 100.0, 0.0)

	body, stats, err := prep.PrepareFrame(dictID, dict, records)
	if err != nil {
		t.Fatalf("PrepareFrame: %v", err)
	}
	if !stats.Attempted {
		t.Fatalf("expected large dict payload to probe compression")
	}
	if !stats.Kept {
		t.Fatalf("expected strong large-payload ratio to stay compressed")
	}
	hdr, _, _, _, err := DecodeFrame(body)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	if hdr.Flags&FrameFlagCompressed == 0 {
		t.Fatalf("expected compressed frame body")
	}
}

func TestWriter_DictLargePayloadBackoffProbe(t *testing.T) {
	const dictID = uint64(23)
	dict, compressible := buildLargeDictCompressionFixture(t, dictID)

	w := NewWriterWithSink(io.Discard, page.ValueLogFileID(1))
	w.SetDictFrameEncoderOptions(zstd.SpeedFastest, false)
	// Keep policy should not prevent large dict probes.
	w.SetKeepPolicy(0.01, 100.0, 0.0)

	incompressible := make([]Record, len(compressible))
	rng := rand.New(rand.NewSource(4242))
	for i := range incompressible {
		payload := make([]byte, len(compressible[i].Value))
		if _, err := rng.Read(payload); err != nil {
			t.Fatalf("rng read: %v", err)
		}
		incompressible[i] = Record{RID: uint64(i + 1), Value: payload}
	}

	dst := make([]page.ValuePtr, len(compressible))
	_, stats1, err := w.AppendFrameWithStatsInto(dictID, dict, incompressible, dst)
	if err != nil {
		t.Fatalf("append incompressible: %v", err)
	}
	if !stats1.Attempted {
		t.Fatalf("expected initial incompressible frame to attempt compression")
	}
	if stats1.Kept {
		t.Fatalf("expected incompressible frame to fall back to raw")
	}
	if w.skipRemain == 0 {
		t.Fatalf("expected backoff skip window after no-benefit attempt")
	}

	_, stats2, err := w.AppendFrameWithStatsInto(dictID, dict, compressible, dst)
	if err != nil {
		t.Fatalf("append compressible: %v", err)
	}
	if !stats2.Attempted {
		t.Fatalf("expected large payload to probe during backoff")
	}
	if !stats2.Kept {
		t.Fatalf("expected compressible large payload to remain compressed")
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

func TestReadAtGroupedLengthHintBit23Omitted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}

	const fixedOverhead = uint32(headerWithoutCRC + FrameHeaderSize + 8 + 8)
	const targetRecordLen = uint32(0x00800080) // bit23 is now reserved for grouped sub-index expansion.
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
	if got := page.ValuePtrRecordLength(ptr); got != 0 {
		_ = writer.Close()
		t.Fatalf("expected grouped length hint to be omitted, got %d", got)
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

func TestValueLogGroupedFrameCRCMatchesContiguousRecordBytes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")
	fileID := page.ValueLogFileID(1)

	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	records := []Record{
		{RID: 1, Value: []byte("alpha")},
		{RID: 2, Value: []byte("bravo-bravo")},
	}
	if _, _, err := writer.AppendFrameWithStatsInto(0, nil, records, make([]page.ValuePtr, len(records))); err != nil {
		_ = writer.Close()
		t.Fatalf("append grouped frame: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if len(data) <= HeaderSize {
		t.Fatalf("unexpected record size %d", len(data))
	}
	got := binary.LittleEndian.Uint32(data[0:4])
	if want := crc.Checksum(data[4:]); got != want {
		t.Fatalf("stored crc=%08x want contiguous checksum=%08x", got, want)
	}

	data[5] ^= 0x40 // mutate grouped header bytes that are covered by the CRC.
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}
	reader, err := NewReader(path, fileID)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	_, _, _, err = reader.ReadNext()
	if !errors.Is(err, ErrCorrupt) {
		_ = reader.Close()
		t.Fatalf("expected corrupt header error, got %v", err)
	}
	_ = reader.Close()
}

func TestValueLogCompressedGroupedFrameCRCMatchesContiguousRecordBytes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")
	fileID := page.ValueLogFileID(1)

	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	writer.SetBlockCompression(BlockCodecSnappy, true)
	records := []Record{{RID: 1, Value: bytes.Repeat([]byte("A"), 4096)}}
	if _, stats, err := writer.AppendFrameWithStatsInto(0, nil, records, make([]page.ValuePtr, len(records))); err != nil {
		_ = writer.Close()
		t.Fatalf("append compressed grouped frame: %v", err)
	} else if !stats.Kept {
		_ = writer.Close()
		t.Fatalf("expected block-compressed frame to be kept: %+v", stats)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if len(data) <= HeaderSize {
		t.Fatalf("unexpected record size %d", len(data))
	}
	got := binary.LittleEndian.Uint32(data[0:4])
	if want := crc.Checksum(data[4:]); got != want {
		t.Fatalf("stored crc=%08x want contiguous checksum=%08x", got, want)
	}

	data[len(data)-1] ^= 0x01
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}
	reader, err := NewReader(path, fileID)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	_, _, _, err = reader.ReadNext()
	if !errors.Is(err, ErrCorrupt) {
		_ = reader.Close()
		t.Fatalf("expected corrupt compressed payload error, got %v", err)
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
		{RID: 1, Value: append([]byte(nil), samples[0]...)},
		{RID: 2, Value: append([]byte(nil), samples[1]...)},
		{RID: 3, Value: append([]byte(nil), samples[2]...)},
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
	codecs := []BlockCodec{BlockCodecSnappy, BlockCodecLZ4, BlockCodecZSTD}
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

func TestValueLogBlockCodecLZ4_ReusesWorkerLocalCompressorScratch(t *testing.T) {
	var scratch blockCodecScratch
	dst := make([]byte, lz4.CompressBlockBound(4096))
	for i, fill := range []byte{'a', 'b'} {
		raw := bytes.Repeat([]byte{fill}, 4096)
		out, err := encodeBlockPayloadWithScratch(BlockCodecLZ4, raw, dst[:0], &scratch)
		if err != nil {
			t.Fatalf("encode %d: %v", i, err)
		}
		if len(out) == 0 {
			t.Fatalf("encode %d returned empty output", i)
		}
		decoded, err := decodeBlockPayload(uint8(BlockCodecLZ4), out, uint32(len(raw)), nil)
		if err != nil {
			t.Fatalf("decode %d: %v", i, err)
		}
		if !bytes.Equal(decoded, raw) {
			t.Fatalf("decode %d mismatch", i)
		}
	}
	if scratch.lz4Compressor == nil {
		t.Fatal("expected worker-local lz4 compressor scratch to be initialized")
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
