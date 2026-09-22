package commitlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/crc"
)

func fileHandleClosedForTest(file *os.File) bool {
	return errors.Is(file.Close(), os.ErrClosed)
}

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

func TestCommitLogWriteReadBatchWithEntryRevisions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")

	writer, err := NewWriterWithOptions(path, Options{Compress: false})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	records := []Record{
		{Op: OpSetRID, Key: []byte("k1"), RID: 1, Seq: 7, Revision: 41},
		{Op: OpSetInline, Key: []byte("k2"), Value: []byte("v2"), Seq: 7},
		{Op: OpSetInline, Key: []byte("k3"), Value: make([]byte, 8), Seq: 7, Revision: 42},
		{Op: OpDelete, Key: []byte("k4"), Seq: 7, Revision: 43},
	}
	if err := writer.AppendBatch(records); err != nil {
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
	if got := data[segmentHeaderSize]; got != recordRevisionBatchVersion {
		t.Fatalf("raw batch version=%d, want revision batch version %d", got, recordRevisionBatchVersion)
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
		if got[i].Op != records[i].Op || got[i].RID != records[i].RID ||
			got[i].Seq != records[i].Seq || got[i].Revision != records[i].Revision ||
			!bytes.Equal(got[i].Key, records[i].Key) || !bytes.Equal(got[i].Value, records[i].Value) {
			_ = reader.Close()
			t.Fatalf("record %d=%+v, want %+v", i, got[i], records[i])
		}
	}
	if _, err := reader.ReadBatch(); !errors.Is(err, io.EOF) {
		_ = reader.Close()
		t.Fatalf("expected EOF, got %v", err)
	}
	_ = reader.Close()
}

func TestCommitLogAppendEntryRevision(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")

	writer, err := NewWriterWithOptions(path, Options{Compress: false})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	record := Record{Op: OpSetInline, Key: []byte("k1"), Value: []byte("v1"), Seq: 300, Revision: 201}
	if err := writer.Append(record); err != nil {
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
	if got := data[segmentHeaderSize]; got != recordRevisionBatchVersion {
		t.Fatalf("raw batch version=%d, want revision batch version %d", got, recordRevisionBatchVersion)
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
	if len(got) != 1 {
		_ = reader.Close()
		t.Fatalf("record count: got %d want 1", len(got))
	}
	if got[0].Op != record.Op || got[0].Seq != record.Seq || got[0].Revision != record.Revision ||
		!bytes.Equal(got[0].Key, record.Key) || !bytes.Equal(got[0].Value, record.Value) {
		_ = reader.Close()
		t.Fatalf("record=%+v, want %+v", got[0], record)
	}
	if _, err := reader.ReadBatch(); !errors.Is(err, io.EOF) {
		_ = reader.Close()
		t.Fatalf("expected EOF, got %v", err)
	}
	_ = reader.Close()
}

func TestCommitLogAppendSeqBackedRevisionUsesCompactVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")

	writer, err := NewWriterWithOptions(path, Options{Compress: false})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	record := Record{Op: OpSetInline, Key: []byte("k1"), Value: []byte("v1"), Seq: 300, Revision: 300}
	if err := writer.Append(record); err != nil {
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
	if got := data[segmentHeaderSize]; got != Version {
		t.Fatalf("raw batch version=%d, want compact version %d", got, Version)
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
	if len(got) != 1 {
		_ = reader.Close()
		t.Fatalf("record count: got %d want 1", len(got))
	}
	if got[0].Op != record.Op || got[0].Seq != record.Seq || got[0].Revision != 0 ||
		!bytes.Equal(got[0].Key, record.Key) || !bytes.Equal(got[0].Value, record.Value) {
		_ = reader.Close()
		t.Fatalf("record=%+v, want seq-backed compact revision", got[0])
	}
	if _, err := reader.ReadBatch(); !errors.Is(err, io.EOF) {
		_ = reader.Close()
		t.Fatalf("expected EOF, got %v", err)
	}
	_ = reader.Close()
}

func TestCommitLogAppendCoalescesSameSeqSmallRecords(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")

	writer, err := NewWriterWithOptions(path, Options{Compress: false})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	records := []Record{
		{Op: OpSetInline, Key: []byte("k1"), Value: []byte("v1"), Seq: 42},
		{Op: OpDelete, Key: []byte("k2"), Seq: 42},
		{Op: OpSetInline, Key: []byte("k3"), Value: bytes.Repeat([]byte("v"), 64), Seq: 42},
	}
	for _, rec := range records {
		if err := writer.Append(rec); err != nil {
			_ = writer.Close()
			t.Fatalf("append: %v", err)
		}
	}
	if got := writer.Size(); got <= int64(segmentHeaderSize+batchHeaderSize) {
		_ = writer.Close()
		t.Fatalf("logical size before flush=%d, want pending bytes included", got)
	}
	if err := writer.Flush(); err != nil {
		_ = writer.Close()
		t.Fatalf("flush: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if len(data) < segmentHeaderSize+batchHeaderSize {
		t.Fatalf("coalesced segment too small: %d", len(data))
	}
	payloadLen := int(binary.LittleEndian.Uint32(data[0:4]) & segmentLenMask)
	if gotCRC, wantCRC := binary.LittleEndian.Uint32(data[4:8]), crc.Checksum(data[segmentHeaderSize:segmentHeaderSize+payloadLen]); gotCRC != wantCRC {
		t.Fatalf("coalesced crc=%08x want %08x", gotCRC, wantCRC)
	}
	if count := binary.LittleEndian.Uint32(data[segmentHeaderSize+1 : segmentHeaderSize+5]); count != uint32(len(records)) {
		t.Fatalf("coalesced count=%d want %d", count, len(records))
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
		if got[i].Op != records[i].Op || got[i].Seq != records[i].Seq || got[i].RID != records[i].RID || !bytes.Equal(got[i].Key, records[i].Key) || !bytes.Equal(got[i].Value, records[i].Value) {
			_ = reader.Close()
			t.Fatalf("record %d mismatch: got=%+v want=%+v", i, got[i], records[i])
		}
	}
	if _, err := reader.ReadBatch(); !errors.Is(err, io.EOF) {
		_ = reader.Close()
		t.Fatalf("expected EOF, got %v", err)
	}
	_ = reader.Close()
}

func TestCommitLogAppendFlushesCoalescedBatchOnSeqChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")

	writer, err := NewWriterWithOptions(path, Options{Compress: false})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	if err := writer.Append(Record{Op: OpSetInline, Key: []byte("k1"), Value: []byte("v1"), Seq: 1}); err != nil {
		_ = writer.Close()
		t.Fatalf("append seq1: %v", err)
	}
	if err := writer.Append(Record{Op: OpSetInline, Key: []byte("k2"), Value: []byte("v2"), Seq: 2}); err != nil {
		_ = writer.Close()
		t.Fatalf("append seq2: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reader, err := NewReader(path)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	first, err := reader.ReadBatch()
	if err != nil {
		_ = reader.Close()
		t.Fatalf("read first: %v", err)
	}
	second, err := reader.ReadBatch()
	if err != nil {
		_ = reader.Close()
		t.Fatalf("read second: %v", err)
	}
	if len(first) != 1 || first[0].Seq != 1 || string(first[0].Key) != "k1" {
		_ = reader.Close()
		t.Fatalf("first batch=%+v, want seq1/k1", first)
	}
	if len(second) != 1 || second[0].Seq != 2 || string(second[0].Key) != "k2" {
		_ = reader.Close()
		t.Fatalf("second batch=%+v, want seq2/k2", second)
	}
	if _, err := reader.ReadBatch(); !errors.Is(err, io.EOF) {
		_ = reader.Close()
		t.Fatalf("expected EOF, got %v", err)
	}
	_ = reader.Close()
}

func TestCommitLogCoalescedBatchChecksumMismatchFailsClosed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")

	writer, err := NewWriterWithOptions(path, Options{Compress: false})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	for _, key := range []string{"k1", "k2"} {
		if err := writer.Append(Record{Op: OpSetInline, Key: []byte(key), Value: []byte("value"), Seq: 9}); err != nil {
			_ = writer.Close()
			t.Fatalf("append %s: %v", key, err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open for corrupt: %v", err)
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], int64(segmentHeaderSize+batchHeaderSize)); err != nil {
		_ = f.Close()
		t.Fatalf("read corrupt byte: %v", err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b[:], int64(segmentHeaderSize+batchHeaderSize)); err != nil {
		_ = f.Close()
		t.Fatalf("write corrupt byte: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close corrupt file: %v", err)
	}

	reader, err := NewReader(path)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	if _, err := reader.ReadBatch(); !errors.Is(err, ErrCorrupt) {
		_ = reader.Close()
		t.Fatalf("ReadBatch error=%v, want ErrCorrupt", err)
	}
	_ = reader.Close()
}

func TestCommitLogCoalescedBatchTruncatedTailKeepsPriorBatchReadable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")

	writer, err := NewWriterWithOptions(path, Options{Compress: false})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	if err := writer.Append(Record{Op: OpSetInline, Key: []byte("first"), Value: []byte("value"), Seq: 1}); err != nil {
		_ = writer.Close()
		t.Fatalf("append first: %v", err)
	}
	if err := writer.Flush(); err != nil {
		_ = writer.Close()
		t.Fatalf("flush first: %v", err)
	}
	if err := writer.Append(Record{Op: OpSetInline, Key: []byte("second"), Value: bytes.Repeat([]byte("x"), 32), Seq: 2}); err != nil {
		_ = writer.Close()
		t.Fatalf("append second: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatalf("truncate tail: %v", err)
	}

	reader, err := NewReader(path)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	first, err := reader.ReadBatch()
	if err != nil {
		_ = reader.Close()
		t.Fatalf("read first: %v", err)
	}
	if len(first) != 1 || string(first[0].Key) != "first" || first[0].Seq != 1 {
		_ = reader.Close()
		t.Fatalf("first batch=%+v, want first seq1", first)
	}
	if _, err := reader.ReadBatch(); !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		_ = reader.Close()
		t.Fatalf("truncated tail error=%v, want EOF/UnexpectedEOF", err)
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
	if got := writer.DurabilityStats().DirectorySyncCalls; got != 1 {
		t.Fatalf("initial directory sync calls=%d, want 1", got)
	}
	fileSyncs := 0
	writer.syncFn = func(*os.File) error {
		fileSyncs++
		return nil
	}
	calls = 0
	if err := writer.RotateToWithSync(path1, false); err != nil {
		t.Fatalf("relaxed RotateToWithSync: %v", err)
	}
	if calls != 0 {
		t.Fatalf("relaxed RotateToWithSync syncDir calls=%d, want 0", calls)
	}
	if got := writer.DurabilityStats(); got.FileSyncCalls != 0 || got.DirectorySyncCalls != 1 {
		t.Fatalf("durability stats after relaxed rotate=%+v, want file=0 directory=1", got)
	}
	if err := writer.RotateToWithSync(path2, true); err != nil {
		t.Fatalf("strict RotateToWithSync: %v", err)
	}
	wantDirCalls, wantFileSyncs := 1, 1
	if runtime.GOOS == "windows" {
		// Windows persists the successor's creation metadata through that exact
		// child handle, so the injected file-sync seam observes both the old
		// segment content sync and the successor namespace proof.
		wantDirCalls, wantFileSyncs = 0, 2
	}
	if calls != wantDirCalls {
		t.Fatalf("strict RotateToWithSync syncDir calls=%d, want %d", calls, wantDirCalls)
	}
	if fileSyncs != wantFileSyncs {
		t.Fatalf("strict RotateToWithSync file sync calls=%d, want %d", fileSyncs, wantFileSyncs)
	}
	if got := writer.DurabilityStats(); got.FileSyncCalls != 1 || got.DirectorySyncCalls != 2 || got.FileSyncErrors != 0 || got.DirectorySyncErrors != 0 {
		t.Fatalf("durability stats after strict rotate=%+v, want file=1 directory=2 and no errors", got)
	}
}

func TestWriterRotateToWithSyncCloseErrorKeepsInstalledSuccessor(t *testing.T) {
	for _, tc := range []struct {
		name   string
		rotate func(*Writer, string) error
	}{
		{name: "RotateToWithSync", rotate: func(writer *Writer, path string) error {
			return writer.RotateToWithSync(path, false)
		}},
		{name: "RotateTo", rotate: func(writer *Writer, path string) error {
			return writer.RotateTo(path)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path0 := filepath.Join(dir, "commit-0.log")
			path1 := filepath.Join(dir, "commit-1.log")
			writer, err := NewWriter(path0)
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			t.Cleanup(func() {
				if !closed {
					_ = writer.Close()
				}
			})
			oldFile := writer.f
			injected := errors.New("injected public writer rotation close error")
			hookInstalled := true
			writer.closeRotateFn = func(file *os.File) error {
				return errors.Join(file.Close(), injected)
			}
			t.Cleanup(func() {
				if hookInstalled {
					writer.closeRotateFn = nil
				}
			})

			err = tc.rotate(writer, path1)
			writer.closeRotateFn = nil
			hookInstalled = false
			if !errors.Is(err, injected) {
				t.Fatalf("%s error=%v, want injected close error", tc.name, err)
			}
			if !RotationInstalled(err) {
				t.Fatalf("RotationInstalled(%v)=false, want installed successor marker", err)
			}
			if !RotationInstalled(fmt.Errorf("outer rotation context: %w", err)) {
				t.Fatal("RotationInstalled lost marker through errors.As wrapping")
			}
			if RotationInstalled(injected) {
				t.Fatal("unmarked predecessor cleanup error reported an installed rotation")
			}
			if writer.f == nil {
				t.Fatal("successor not installed after close error: current file is nil")
			}
			if writer.f == oldFile || writer.f.Name() != path1 {
				t.Fatalf("successor not installed after close error: current=%p name=%q old=%p", writer.f, writer.f.Name(), oldFile)
			}
			if !fileHandleClosedForTest(oldFile) {
				t.Fatal("old file remains open after injected close error")
			}
			if err := writer.AppendBatch([]Record{{Op: OpSetInline, Key: []byte("key"), Value: []byte("value"), Seq: 1}}); err != nil {
				t.Fatalf("append through installed successor: %v", err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			closed = true
		})
	}
}

func TestWriterSyncFallsBackToFileSyncWhenHookNil(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commit.log")
	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	defer func() { _ = writer.Close() }()

	if err := writer.AppendBatch([]Record{{
		Op:    OpSetInline,
		Key:   []byte("key"),
		Value: []byte("value"),
		Seq:   1,
	}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	writer.syncFn = nil
	before := writer.DurabilityStats()
	if err := writer.Sync(); err != nil {
		t.Fatalf("sync with nil hook: %v", err)
	}
	after := writer.DurabilityStats()
	if got := after.FileSyncCalls - before.FileSyncCalls; got != 1 {
		t.Fatalf("file sync call delta=%d, want 1", got)
	}
	if got := after.FileSyncErrors - before.FileSyncErrors; got != 0 {
		t.Fatalf("file sync error delta=%d, want 0", got)
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
	tests := []struct {
		name     string
		record   Record
		wantLong bool
	}{
		{
			name:   "small-direct-segment",
			record: Record{Op: OpSetInline, Key: []byte("single-key"), Value: []byte("single-value"), Seq: 7},
		},
		{
			name: "large-raw-segment",
			record: Record{
				Op:    OpSetInline,
				Key:   []byte("single-large-key"),
				Value: bytes.Repeat([]byte("large-value"), directSegmentPayloadMinLen/len("large-value")),
				Seq:   8,
			},
			wantLong: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "commit.log")

			writer, err := NewWriterWithOptions(path, Options{Compress: false})
			if err != nil {
				t.Fatalf("new writer: %v", err)
			}
			if err := writer.Append(tt.record); err != nil {
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
			if tt.wantLong && segmentHeaderSize+payloadLen < directSegmentPayloadMinLen {
				t.Fatalf("large append did not cover raw segment threshold: total=%d threshold=%d", segmentHeaderSize+payloadLen, directSegmentPayloadMinLen)
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
			if len(got) != 1 || got[0].Op != tt.record.Op || got[0].Seq != tt.record.Seq || !bytes.Equal(got[0].Key, tt.record.Key) || !bytes.Equal(got[0].Value, tt.record.Value) {
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
		})
	}
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
