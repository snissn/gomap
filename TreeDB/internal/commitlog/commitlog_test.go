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
	"github.com/snissn/gomap/TreeDB/internal/limits"
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

func TestFenceRIDGroupPayloadRoundTrip(t *testing.T) {
	entries := []FenceRIDGroupEntry{
		{Key: []byte("k001"), RID: 11},
		{Key: []byte("k002"), RID: 12},
		{Key: []byte("k003"), RID: 13},
	}
	encodings := []FenceRIDGroupEncoding{
		FenceRIDGroupEncodingSimple,
		FenceRIDGroupEncodingPrefix,
	}
	for _, enc := range encodings {
		payload, err := EncodeFenceRIDGroupPayload(entries, enc)
		if err != nil {
			t.Fatalf("encode %s: %v", enc.String(), err)
		}
		got, gotEnc, err := DecodeFenceRIDGroupPayload(payload, nil)
		if err != nil {
			t.Fatalf("decode %s: %v", enc.String(), err)
		}
		if gotEnc != enc {
			t.Fatalf("decode %s encoding mismatch: got=%s", enc.String(), gotEnc.String())
		}
		if len(got) != len(entries) {
			t.Fatalf("decode %s len=%d want=%d", enc.String(), len(got), len(entries))
		}
		for i := range entries {
			if string(got[i].Key) != string(entries[i].Key) || got[i].RID != entries[i].RID {
				t.Fatalf("decode %s entry[%d]=(%q,%d) want (%q,%d)", enc.String(), i, got[i].Key, got[i].RID, entries[i].Key, entries[i].RID)
			}
		}
	}
}

func TestFenceRIDGroupPayloadMalformed(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "empty", payload: nil},
		{name: "bad_version", payload: []byte{0xFF, 0, 1}},
		{name: "bad_encoding", payload: []byte{1, 99, 1}},
		{name: "truncated_count", payload: []byte{1, 0, 0x80}},
		{name: "truncated_key_len", payload: []byte{1, 0, 1, 0x80}},
		{name: "truncated_key", payload: []byte{1, 0, 1, 3, 'k'}},
		{name: "missing_rid", payload: []byte{1, 0, 1, 1, 'k'}},
		{name: "truncated_rid_varint", payload: []byte{1, 0, 1, 1, 'k', 0x80}},
		{name: "count_exceeds_payload_bound", payload: []byte{1, 0, 100, 1, 'k', 1}},
		{name: "prefix_bad_shared", payload: []byte{1, 1, 2, 1, 'a', 1, 2, 1, 'b', 2}},
		{name: "prefix_non_monotonic", payload: []byte{1, 1, 2, 1, 'b', 1, 0, 1, 'a', 2}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := DecodeFenceRIDGroupPayload(tc.payload, nil); err == nil {
				t.Fatalf("expected decode error")
			}
		})
	}
}

func TestCommitLogWriteReadBatchFenceRIDGroup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")

	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	payload, err := EncodeFenceRIDGroupPayload([]FenceRIDGroupEntry{
		{Key: []byte("k1"), RID: 1},
		{Key: []byte("k2"), RID: 1},
		{Key: []byte("k3"), RID: 2},
	}, FenceRIDGroupEncodingPrefix)
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	records := []Record{
		{Op: OpSetFenceRIDGroup, Value: payload, Seq: 7},
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
	defer func() { _ = reader.Close() }()
	got, err := reader.ReadBatch()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 1 || got[0].Op != OpSetFenceRIDGroup || got[0].Seq != 7 {
		t.Fatalf("unexpected records: %+v", got)
	}
	if len(got[0].Key) != 0 || got[0].RID != 0 {
		t.Fatalf("grouped record must keep key/rid header empty")
	}
	decoded, _, err := DecodeFenceRIDGroupPayload(got[0].Value, nil)
	if err != nil {
		t.Fatalf("decode grouped payload: %v", err)
	}
	if len(decoded) != 3 {
		t.Fatalf("grouped payload len=%d", len(decoded))
	}
}

func TestCommitLogFenceRIDGroupRejectsNonEmptyKeyWriter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")
	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	defer func() { _ = writer.Close() }()
	payload, err := EncodeFenceRIDGroupPayload([]FenceRIDGroupEntry{
		{Key: []byte("k"), RID: 1},
		{Key: []byte("k2"), RID: 2},
	}, FenceRIDGroupEncodingSimple)
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	err = writer.AppendBatch([]Record{
		{Op: OpSetFenceRIDGroup, Key: []byte("bad"), Value: payload, Seq: 1},
	})
	if err == nil {
		t.Fatalf("expected writer validation error")
	}
}

func TestCommitLogFenceRIDGroupReaderRejectsNonEmptyKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")

	payload, err := EncodeFenceRIDGroupPayload([]FenceRIDGroupEntry{
		{Key: []byte("k"), RID: 1},
		{Key: []byte("k2"), RID: 2},
	}, FenceRIDGroupEncodingSimple)
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	if err := w.AppendBatch([]Record{{Op: OpSetFenceRIDGroup, Value: payload, Seq: 1}}); err != nil {
		_ = w.Close()
		t.Fatalf("append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(data) < segmentHeaderSize+batchHeaderSize+recordHeaderSize {
		t.Fatalf("unexpected log size %d", len(data))
	}
	off := segmentHeaderSize + batchHeaderSize
	// Corrupt key length from 0 to 1 and keep CRC consistent.
	binary.LittleEndian.PutUint16(data[off+1:off+3], 1)
	payloadLen := int(binary.LittleEndian.Uint32(data[0:4]) & segmentLenMask)
	raw := data[segmentHeaderSize : segmentHeaderSize+payloadLen]
	binary.LittleEndian.PutUint32(data[4:8], crcChecksum(raw))
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("rewrite: %v", err)
	}

	r, err := NewReader(path)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	defer func() { _ = r.Close() }()
	if _, err := r.ReadBatch(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func TestCommitLogFenceRIDGroupSizeBoundaries(t *testing.T) {
	oldMax := limits.MaxRecordSize
	t.Cleanup(func() { limits.MaxRecordSize = oldMax })
	okPayload, err := EncodeFenceRIDGroupPayload([]FenceRIDGroupEntry{
		{Key: []byte("k1"), RID: 1},
	}, FenceRIDGroupEncodingSimple)
	if err != nil {
		t.Fatalf("encode ok payload: %v", err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "commit.log")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	limits.MaxRecordSize = int64(recordHeaderSize + len(okPayload))
	if err := w.AppendBatch([]Record{{Op: OpSetFenceRIDGroup, Value: okPayload, Seq: 1}}); err != nil {
		_ = w.Close()
		t.Fatalf("append ok payload: %v", err)
	}
	limits.MaxRecordSize = int64(recordHeaderSize + len(okPayload) - 1)
	err = w.AppendBatch([]Record{{Op: OpSetFenceRIDGroup, Value: okPayload, Seq: 2}})
	if !errors.Is(err, ErrRecordTooLarge) {
		_ = w.Close()
		t.Fatalf("expected ErrRecordTooLarge, got %v", err)
	}
	_ = w.Close()
}

func crcChecksum(payload []byte) uint32 {
	return crc.Checksum(payload)
}
