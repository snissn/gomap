package valuelog

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

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
