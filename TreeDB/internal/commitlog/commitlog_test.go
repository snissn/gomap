package commitlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/crc"
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

func TestCommitLogWriteReadZeroInlineBatchCompact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")

	writer, err := NewWriterWithOptions(path, Options{Compress: false})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	records := make([]Record, 8000)
	value := make([]byte, 128)
	for i := range records {
		var key [8]byte
		binary.LittleEndian.PutUint64(key[:], uint64(i))
		records[i] = Record{
			Op:    OpSetInline,
			Key:   bytes.Clone(key[:]),
			Value: value,
			Seq:   11,
		}
	}
	if err := writer.AppendBatch(records); err != nil {
		_ = writer.Close()
		t.Fatalf("append zero inline batch: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if len(data) < segmentHeaderSize+batchHeaderSize+recordHeaderSize {
		t.Fatalf("unexpected compact segment size %d", len(data))
	}
	payloadLen := int(binary.LittleEndian.Uint32(data[0:4]) & segmentLenMask)
	wantPayloadLen := zeroInlineBatchHeaderSize + len(records)*(zeroInlineRecordHeaderSize+8)
	if payloadLen != wantPayloadLen {
		t.Fatalf("payload len=%d, want compact len %d", payloadLen, wantPayloadLen)
	}
	if got := data[segmentHeaderSize]; got != zeroInlineBatchVersion {
		t.Fatalf("raw batch version=%d, want compact zero batch version %d", got, zeroInlineBatchVersion)
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
	for i := range got {
		if got[i].Op != OpSetInline || got[i].Seq != 11 || !bytes.Equal(got[i].Value, value) {
			_ = reader.Close()
			t.Fatalf("decoded record %d mismatch: op=%d seq=%d value_len=%d", i, got[i].Op, got[i].Seq, len(got[i].Value))
		}
	}
	_ = reader.Close()
}

func TestCommitLogWriteReadZeroInlineBatchFuncCompact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")

	writer, err := NewWriterWithOptions(path, Options{Compress: false})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	count := 8000
	keys := make([][]byte, count)
	for i := range keys {
		var key [8]byte
		binary.LittleEndian.PutUint64(key[:], uint64(i))
		keys[i] = bytes.Clone(key[:])
	}
	if err := writer.AppendZeroInlineBatchFunc(count, 22, 128, func(i int) []byte {
		return keys[i]
	}); err != nil {
		_ = writer.Close()
		t.Fatalf("append zero inline batch func: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	payloadLen := int(binary.LittleEndian.Uint32(data[0:4]) & segmentLenMask)
	wantPayloadLen := zeroInlineBatchHeaderSize + count*(zeroInlineRecordHeaderSize+8)
	if payloadLen != wantPayloadLen {
		t.Fatalf("payload len=%d, want compact len %d", payloadLen, wantPayloadLen)
	}
	if got := data[segmentHeaderSize]; got != zeroInlineBatchVersion {
		t.Fatalf("raw batch version=%d, want compact zero batch version %d", got, zeroInlineBatchVersion)
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
	if len(got) != count {
		_ = reader.Close()
		t.Fatalf("record count: got %d want %d", len(got), count)
	}
	value := make([]byte, 128)
	for i := range got {
		if got[i].Op != OpSetInline || got[i].Seq != 22 || !bytes.Equal(got[i].Key, keys[i]) || !bytes.Equal(got[i].Value, value) {
			_ = reader.Close()
			t.Fatalf("decoded record %d mismatch: op=%d seq=%d key_len=%d value_len=%d", i, got[i].Op, got[i].Seq, len(got[i].Key), len(got[i].Value))
		}
	}
	_ = reader.Close()
}

func TestCommitLogWriteReadMixedZeroInlineRecordCompact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")

	writer, err := NewWriterWithOptions(path, Options{Compress: false})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	records := []Record{
		{Op: OpSetInline, Key: []byte("zero"), Value: make([]byte, 16), Seq: 1},
		{Op: OpDelete, Key: []byte("gone"), Seq: 1},
	}
	if err := writer.AppendBatch(records); err != nil {
		_ = writer.Close()
		t.Fatalf("append mixed zero batch: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if got := data[segmentHeaderSize]; got != Version {
		t.Fatalf("raw batch version=%d, want v1", got)
	}
	if got := data[segmentHeaderSize+batchHeaderSize]; got != OpSetInlineZero {
		t.Fatalf("raw op=%d, want compact zero op %d", got, OpSetInlineZero)
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
	if got[0].Op != OpSetInline || !bytes.Equal(got[0].Value, records[0].Value) {
		_ = reader.Close()
		t.Fatalf("decoded zero record mismatch: op=%d value=%x", got[0].Op, got[0].Value)
	}
	if got[1].Op != OpDelete {
		_ = reader.Close()
		t.Fatalf("decoded delete op=%d, want delete", got[1].Op)
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

func TestCommitLogAppendSingleCRCMatchesPayloadBytes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")

	writer, err := NewWriterWithOptions(path, Options{Compress: false})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	record := Record{Op: OpSetInline, Key: []byte("single-key"), Value: []byte("single-value"), Seq: 7}
	if err := writer.Append(record); err != nil {
		_ = writer.Close()
		t.Fatalf("append single: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if len(data) <= segmentHeaderSize {
		t.Fatalf("unexpected segment size %d", len(data))
	}
	payloadLen := int(binary.LittleEndian.Uint32(data[0:4]) & segmentLenMask)
	if payloadLen != len(data)-segmentHeaderSize {
		t.Fatalf("payload len=%d want %d", payloadLen, len(data)-segmentHeaderSize)
	}
	gotCRC := binary.LittleEndian.Uint32(data[4:8])
	if wantCRC := crc.Checksum(data[segmentHeaderSize:]); gotCRC != wantCRC {
		t.Fatalf("stored crc=%08x want payload checksum=%08x", gotCRC, wantCRC)
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
	if len(got) != 1 || got[0].Op != record.Op || got[0].Seq != record.Seq || !bytes.Equal(got[0].Key, record.Key) || !bytes.Equal(got[0].Value, record.Value) {
		_ = reader.Close()
		t.Fatalf("decoded single record mismatch: %+v", got)
	}
	_ = reader.Close()

	data[segmentHeaderSize] ^= 0x80
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}
	reader, err = NewReader(path)
	if err != nil {
		t.Fatalf("new corrupt reader: %v", err)
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
