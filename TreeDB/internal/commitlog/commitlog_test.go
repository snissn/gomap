package commitlog

import (
	"bytes"
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
		{Op: OpSetRID, Key: []byte("k1"), RID: 1, Seq: 1},
		{Op: OpSetInline, Key: []byte("k2"), Value: []byte("v2"), Seq: 1},
		{Op: OpDelete, Key: []byte("k3"), Seq: 1},
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
		if got[i].Seq != records[i].Seq {
			_ = reader.Close()
			t.Fatalf("record %d seq mismatch", i)
		}
	}
	if _, err := reader.ReadBatch(); !errors.Is(err, io.EOF) {
		_ = reader.Close()
		t.Fatalf("expected EOF, got %v", err)
	}
	_ = reader.Close()
}

func TestCommitLogWriteReadLargeRawBatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")

	writer, err := NewWriterWithOptions(path, Options{Compress: false})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	records := make([]Record, 2048)
	value := bytes.Repeat([]byte("v"), 128)
	for i := range records {
		records[i] = Record{
			Op:    OpSetInline,
			Key:   []byte{byte(i), byte(i >> 8), byte(i >> 16)},
			Value: value,
			Seq:   7,
		}
	}
	if err := writer.AppendBatch(records); err != nil {
		_ = writer.Close()
		t.Fatalf("append large raw batch: %v", err)
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
		if got[i].Op != records[i].Op || got[i].Seq != records[i].Seq ||
			!bytes.Equal(got[i].Key, records[i].Key) || !bytes.Equal(got[i].Value, records[i].Value) {
			_ = reader.Close()
			t.Fatalf("record %d mismatch", i)
		}
	}
	_ = reader.Close()
}

func TestWriterRotateToWithSyncSkipsDirSyncWhenRelaxed(t *testing.T) {
	dir := t.TempDir()
	path0 := filepath.Join(dir, "commit-0.log")
	path1 := filepath.Join(dir, "commit-1.log")
	path2 := filepath.Join(dir, "commit-2.log")

	oldSyncDirFn := syncDirFn
	defer func() { syncDirFn = oldSyncDirFn }()
	calls := 0
	syncDirFn = func(string) error {
		calls++
		return nil
	}

	writer, err := NewWriterWithOptions(path0, Options{})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	defer func() { _ = writer.Close() }()
	calls = 0
	if err := writer.RotateToWithSync(path1, false); err != nil {
		t.Fatalf("relaxed RotateToWithSync: %v", err)
	}
	if calls != 0 {
		t.Fatalf("relaxed RotateToWithSync syncDir calls=%d, want 0", calls)
	}
	if err := writer.RotateToWithSync(path2, true); err != nil {
		t.Fatalf("strict RotateToWithSync: %v", err)
	}
	if calls != 1 {
		t.Fatalf("strict RotateToWithSync syncDir calls=%d, want 1", calls)
	}
}

func TestCommitLogWriteReadBatchCompressed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")

	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	records0 := []Record{
		{Op: OpSetRID, Key: []byte("k1"), RID: 1, Seq: 1},
		{Op: OpSetInline, Key: []byte("k2"), Value: []byte("v2"), Seq: 1},
		{Op: OpDelete, Key: []byte("k3"), Seq: 1},
	}
	if err := writer.AppendBatch(records0); err != nil {
		_ = writer.Close()
		t.Fatalf("append: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	writer1, err := NewWriterWithOptions(path, Options{Compress: true})
	if err != nil {
		t.Fatalf("new writer compress: %v", err)
	}
	records1 := make([]Record, 0, 128)
	for i := 0; i < 128; i++ {
		records1 = append(records1, Record{
			Op:    OpSetInline,
			Key:   []byte("key-prefix-" + string('a'+byte(i%4))),
			Value: bytes.Repeat([]byte("AAAAAAAAAAAAAAAA"), 64),
			Seq:   2,
		})
	}
	if err := writer1.AppendBatch(records1); err != nil {
		_ = writer1.Close()
		t.Fatalf("append compressed: %v", err)
	}
	if err := writer1.Close(); err != nil {
		t.Fatalf("close compressed: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if len(data) < segmentHeaderSize {
		t.Fatalf("unexpected segment size %d", len(data))
	}
	lengthField := binary.LittleEndian.Uint32(data[0:4])
	if lengthField&segmentFlagCompressed != 0 {
		t.Fatalf("first segment unexpectedly compressed")
	}
	off := segmentHeaderSize + int(lengthField&segmentLenMask)
	if off+segmentHeaderSize > len(data) {
		t.Fatalf("missing second segment")
	}
	lengthField2 := binary.LittleEndian.Uint32(data[off : off+4])
	if lengthField2&segmentFlagCompressed == 0 {
		t.Fatalf("expected second segment to be compressed")
	}

	reader, err := NewReader(path)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	got0, err := reader.ReadBatch()
	if err != nil {
		t.Fatalf("read batch 0: %v", err)
	}
	if len(got0) != len(records0) {
		t.Fatalf("record count 0: got %d want %d", len(got0), len(records0))
	}

	got1, err := reader.ReadBatch()
	if err != nil {
		t.Fatalf("read batch 1: %v", err)
	}
	if len(got1) != len(records1) {
		t.Fatalf("record count 1: got %d want %d", len(got1), len(records1))
	}

	if _, err := reader.ReadBatch(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestCommitLogWriter_LazyCompressionEncoder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")

	writer, err := NewWriterWithOptions(path, Options{Compress: true})
	if err != nil {
		t.Fatalf("NewWriterWithOptions: %v", err)
	}
	defer func() { _ = writer.Close() }()

	if writer.enc != nil {
		t.Fatalf("encoder should be lazy and remain nil until compression is needed")
	}

	small := []Record{{
		Op:    OpSetInline,
		Key:   []byte("k"),
		Value: bytes.Repeat([]byte("x"), 1024),
		Seq:   1,
	}}
	if err := writer.AppendBatch(small); err != nil {
		t.Fatalf("AppendBatch(small): %v", err)
	}
	if writer.enc != nil {
		t.Fatalf("small payload should not initialize compression encoder")
	}

	large := []Record{{
		Op:    OpSetInline,
		Key:   []byte("big"),
		Value: bytes.Repeat([]byte("A"), defaultCompressMinLen),
		Seq:   2,
	}}
	if err := writer.AppendBatch(large); err != nil {
		t.Fatalf("AppendBatch(large): %v", err)
	}
	if writer.enc == nil {
		t.Fatalf("large payload should initialize compression encoder")
	}
}

func TestCommitLogCorruptCRC(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")

	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	if err := writer.AppendBatch([]Record{{Op: OpSetRID, Key: []byte("k"), RID: 1, Seq: 1}}); err != nil {
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

func TestCommitLogAppendBatchRejectsMixedSequence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")

	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	t.Cleanup(func() { _ = writer.Close() })

	err = writer.AppendBatch([]Record{
		{Op: OpSetInline, Key: []byte("k1"), Value: []byte("v1"), Seq: 1},
		{Op: OpSetInline, Key: []byte("k2"), Value: []byte("v2"), Seq: 2},
	})
	if !errors.Is(err, ErrMixedBatchSeq) {
		t.Fatalf("expected ErrMixedBatchSeq, got %v", err)
	}
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
