package valuelog

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func writeGroupedBatchTestFrame(t *testing.T, dir string, records int) (uint32, string, []page.ValuePtr, [][]byte) {
	t.Helper()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")
	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	recs := make([]Record, records)
	want := make([][]byte, records)
	for i := range recs {
		value := []byte(fmt.Sprintf("grouped-value-%02d", i))
		recs[i] = Record{RID: uint64(i + 1), Value: value}
		want[i] = append([]byte(nil), value...)
	}
	ptrs, err := writer.AppendFrame(0, nil, recs)
	if err != nil {
		t.Fatalf("AppendFrame: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	return fileID, path, ptrs, want
}

func TestManagerReadUnsafeAppendBatch_GroupedRecordCRCOncePerBatchOperation(t *testing.T) {
	dir := t.TempDir()
	_, _, ptrs, want := writeGroupedBatchTestFrame(t, dir, 8)

	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = m.Close() }()

	dst := make([][]byte, len(ptrs))
	before := m.ReadStats().RecordCRCChecks
	got, err := m.ReadUnsafeAppendBatch(ptrs, dst)
	if err != nil {
		t.Fatalf("ReadUnsafeAppendBatch: %v", err)
	}
	if got := m.ReadStats().RecordCRCChecks - before; got != 1 {
		t.Fatalf("CRC checks after grouped batch=%d want 1", got)
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("value[%d]=%q want %q", i, got[i], want[i])
		}
	}

	before = m.ReadStats().RecordCRCChecks
	got, err = m.ReadUnsafeAppendBatch(ptrs, got)
	if err != nil {
		t.Fatalf("second ReadUnsafeAppendBatch: %v", err)
	}
	if got := m.ReadStats().RecordCRCChecks - before; got != 1 {
		t.Fatalf("CRC checks after second grouped batch=%d want 1; verified state must not persist across operations", got)
	}
}

func TestManagerReadUnsafeAppendBatch_GroupedRecordChecksumMismatchFailsClosed(t *testing.T) {
	dir := t.TempDir()
	_, path, ptrs, _ := writeGroupedBatchTestFrame(t, dir, 4)

	fh, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	corruptOff := int64(ptrs[0].Offset-valueLogRecordCRCPrefixBytes) + HeaderSize + FrameHeaderSize
	var b [1]byte
	if _, err := fh.ReadAt(b[:], corruptOff); err != nil {
		_ = fh.Close()
		t.Fatalf("ReadAt corrupt byte: %v", err)
	}
	b[0] ^= 0xff
	if _, err := fh.WriteAt(b[:], corruptOff); err != nil {
		_ = fh.Close()
		t.Fatalf("WriteAt corrupt byte: %v", err)
	}
	if err := fh.Close(); err != nil {
		t.Fatalf("Close corrupt file: %v", err)
	}

	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = m.Close() }()

	dst := make([][]byte, 2)
	_, err = m.ReadUnsafeAppendBatch(ptrs[:2], dst)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("ReadUnsafeAppendBatch error=%v want ErrCorrupt", err)
	}
	if got := m.ReadStats().RecordCRCChecks; got != 1 {
		t.Fatalf("CRC checks after corrupt grouped batch=%d want 1", got)
	}
	for i, val := range dst {
		if len(val) != 0 {
			t.Fatalf("dst[%d] was populated before checksum failure: %q", i, val)
		}
	}
}

func TestManagerReadUnsafeAppendBatch_GroupedRecordChecksumDisabledMatchesPointReads(t *testing.T) {
	for _, useMmap := range []bool{false, true} {
		name := "read-at"
		if useMmap {
			name = "mmap"
		}
		t.Run(name, func(t *testing.T) {
			if useMmap && runtime.GOOS == "windows" {
				t.Skip("mmap not supported on windows")
			}
			dir := t.TempDir()
			fileID, path, ptrs, want := writeGroupedBatchTestFrame(t, dir, 4)

			fh, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatalf("OpenFile: %v", err)
			}
			crcOff := int64(ptrs[0].Offset - valueLogRecordCRCPrefixBytes)
			var b [1]byte
			if _, err := fh.ReadAt(b[:], crcOff); err != nil {
				_ = fh.Close()
				t.Fatalf("ReadAt stored CRC: %v", err)
			}
			b[0] ^= 0xff
			if _, err := fh.WriteAt(b[:], crcOff); err != nil {
				_ = fh.Close()
				t.Fatalf("WriteAt stored CRC: %v", err)
			}
			if err := fh.Close(); err != nil {
				t.Fatalf("Close corrupt file: %v", err)
			}

			m, err := NewManager(dir)
			if err != nil {
				t.Fatalf("NewManager: %v", err)
			}
			defer func() { _ = m.Close() }()
			m.SetDisableReadChecksum(true)
			f := m.files[fileID]
			if useMmap {
				f.remapToFileSize()
			} else {
				f.mmapData.Store([]byte(nil))
			}

			point, _, err := m.ReadUnsafeTo(ptrs[0], nil)
			if err != nil {
				t.Fatalf("checksum-disabled point read: %v", err)
			}
			if !bytes.Equal(point, want[0]) {
				t.Fatalf("point value=%q want %q", point, want[0])
			}
			got, err := m.ReadUnsafeAppendBatch(ptrs[:2], make([][]byte, 2))
			if err != nil {
				t.Fatalf("checksum-disabled grouped batch: %v", err)
			}
			for i := range got {
				if !bytes.Equal(got[i], want[i]) {
					t.Fatalf("batch value[%d]=%q want %q", i, got[i], want[i])
				}
			}
			if got := m.ReadStats().RecordCRCChecks; got != 0 {
				t.Fatalf("checksum-disabled reads performed %d CRC checks, want 0", got)
			}
		})
	}
}
