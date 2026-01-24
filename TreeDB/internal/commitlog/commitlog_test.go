package commitlog

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestCommitLogWriteReadBatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")

	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	records := []Record{
		{Op: OpSetRID, Key: []byte("k1"), RID: 1},
		{Op: OpSetInline, Key: []byte("k2"), Value: []byte("v2")},
		{Op: OpDelete, Key: []byte("k3")},
	}
	if err := writer.AppendBatch(records); err != nil {
		_ = writer.Close()
		t.Fatalf("append: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reader, err := NewReader(path)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	got, err := reader.ReadBatch()
	if err != nil {
		_ = reader.Close()
		t.Fatalf("read batch: %v", err)
	}
	if len(got) != len(records) {
		_ = reader.Close()
		t.Fatalf("record count: got %d want %d", len(got), len(records))
	}
	for i := range records {
		if got[i].Op != records[i].Op {
			_ = reader.Close()
			t.Fatalf("record %d op: got %d want %d", i, got[i].Op, records[i].Op)
		}
		if string(got[i].Key) != string(records[i].Key) {
			_ = reader.Close()
			t.Fatalf("record %d key mismatch", i)
		}
		if string(got[i].Value) != string(records[i].Value) {
			_ = reader.Close()
			t.Fatalf("record %d value mismatch", i)
		}
		if got[i].RID != records[i].RID {
			_ = reader.Close()
			t.Fatalf("record %d rid mismatch", i)
		}
	}
	if _, err := reader.ReadBatch(); !errors.Is(err, io.EOF) {
		_ = reader.Close()
		t.Fatalf("expected EOF, got %v", err)
	}
	_ = reader.Close()
}

func TestCommitLogCorruptCRC(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")

	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	if err := writer.AppendBatch([]Record{{Op: OpSetRID, Key: []byte("k"), RID: 1}}); err != nil {
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
	if len(data) < segmentHeaderSize+1 {
		t.Fatalf("unexpected segment size %d", len(data))
	}
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	reader, err := NewReader(path)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	_, err = reader.ReadBatch()
	if !errors.Is(err, ErrCorrupt) {
		_ = reader.Close()
		t.Fatalf("expected corrupt error, got %v", err)
	}
	_ = reader.Close()
}

func TestCommitLogTruncatedPayload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	var header [segmentHeaderSize]byte
	binary.LittleEndian.PutUint32(header[0:4], 12)
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

	reader, err := NewReader(path)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	_, err = reader.ReadBatch()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		_ = reader.Close()
		t.Fatalf("expected unexpected EOF, got %v", err)
	}
	_ = reader.Close()
}
