package valuelog

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestPreparedGroupedRecordAppend_RawParity(t *testing.T) {
	dir := t.TempDir()
	pathA := filepath.Join(dir, "a.vlog")
	pathB := filepath.Join(dir, "b.vlog")

	records := []Record{
		{RID: 1, Value: []byte("hello")},
		{RID: 2, Value: []byte("world")},
		{RID: 3, Value: []byte("goodbye")},
	}

	w1, err := NewWriter(pathA, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	ptrScratch1 := make([]page.ValuePtr, len(records))
	ptrs1, stats1, err := w1.AppendFrameWithStatsInto(0, nil, records, ptrScratch1)
	if err != nil {
		t.Fatalf("AppendFrameWithStatsInto: %v", err)
	}
	if err := w1.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	bytesA, err := os.ReadFile(pathA)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	var rids [MaxFrameK]uint64
	var offsets [MaxFrameK + 1]uint32
	rawPayloadBytes := 0
	offsets[0] = 0
	for i := range records {
		rids[i] = records[i].RID
		rawPayloadBytes += len(records[i].Value)
		offsets[i+1] = uint32(rawPayloadBytes)
	}
	rawPayload := make([]byte, rawPayloadBytes)
	off := 0
	for i := range records {
		off += copy(rawPayload[off:], records[i].Value)
	}
	prep, err := BuildPreparedGroupedRecordFromPayloadInto(nil, 0, rids[:len(records)], offsets[:len(records)+1], rawPayloadBytes, rawPayload, false, false, 0)
	if err != nil {
		t.Fatalf("BuildPreparedGroupedRecordFromPayloadInto: %v", err)
	}

	w2, err := NewWriter(pathB, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	ptrScratch2 := make([]page.ValuePtr, len(records))
	ptrs2, stats2, err := w2.AppendPreparedGroupedRecordInto(prep, ptrScratch2)
	if err != nil {
		t.Fatalf("AppendPreparedGroupedRecordInto: %v", err)
	}
	if err := w2.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	bytesB, err := os.ReadFile(pathB)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	if !bytes.Equal(bytesA, bytesB) {
		t.Fatalf("raw parity: bytes differ")
	}
	if !bytes.Equal(bytesB, prep.Record) {
		t.Fatalf("raw parity: file bytes differ from prepared record")
	}
	if stats1 != stats2 {
		t.Fatalf("raw parity: stats differ: a=%+v b=%+v", stats1, stats2)
	}
	if !bytes.Equal(ptrsToBytes(ptrs1), ptrsToBytes(ptrs2)) {
		t.Fatalf("raw parity: ptrs differ: a=%v b=%v", ptrs1, ptrs2)
	}
}

func TestPreparedGroupedRecordAppend_CompressedDecodes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.vlog")

	pattern := bytes.Repeat([]byte("abcdefghijklmnopqrstuvwxyz"), 40)
	records := []Record{
		{RID: 1, Value: pattern},
		{RID: 2, Value: pattern},
		{RID: 3, Value: pattern},
		{RID: 4, Value: pattern},
	}

	dictID := uint64(1)
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		History:  bytes.Repeat([]byte("0123456789abcdef"), 512),
		Contents: [][]byte{records[0].Value, records[1].Value},
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}

	var rids [MaxFrameK]uint64
	var offsets [MaxFrameK + 1]uint32
	rawPayloadBytes := 0
	offsets[0] = 0
	for i := range records {
		rids[i] = records[i].RID
		rawPayloadBytes += len(records[i].Value)
		offsets[i+1] = uint32(rawPayloadBytes)
	}
	rawPayload := make([]byte, rawPayloadBytes)
	off := 0
	for i := range records {
		off += copy(rawPayload[off:], records[i].Value)
	}

	encoded, err := CompressPayloadWithDictInto(dictID, dict, rawPayload, zstd.SpeedFastest, false, nil)
	if err != nil {
		t.Fatalf("CompressPayloadWithDictInto: %v", err)
	}
	if len(encoded) >= rawPayloadBytes {
		t.Fatalf("expected compressed payload smaller than raw: raw=%d encoded=%d", rawPayloadBytes, len(encoded))
	}

	prep, err := BuildPreparedGroupedRecordFromPayloadInto(nil, dictID, rids[:len(records)], offsets[:len(records)+1], rawPayloadBytes, encoded, true, true, 0)
	if err != nil {
		t.Fatalf("BuildPreparedGroupedRecordFromPayloadInto: %v", err)
	}

	w, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	ptrScratch := make([]page.ValuePtr, len(records))
	_, stats, err := w.AppendPreparedGroupedRecordInto(prep, ptrScratch)
	if err != nil {
		t.Fatalf("AppendPreparedGroupedRecordInto: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !stats.Kept || !stats.Attempted {
		t.Fatalf("unexpected stats: %+v", stats)
	}

	fileBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(fileBytes, prep.Record) {
		t.Fatalf("file bytes differ from prepared record")
	}

	bodyLen := binary.LittleEndian.Uint32(fileBytes[16:20])
	body := fileBytes[HeaderSize : HeaderSize+int(bodyLen)]
	frameHeader, _, outOffsets, payload, err := DecodeFrame(body)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	rawLen := outOffsets[len(outOffsets)-1]
	decoded, err := decodeFramePayload(frameHeader, payload, func(id uint64) ([]byte, error) {
		if id != dictID {
			return nil, ErrMissingDict
		}
		return dict, nil
	}, rawLen)
	if err != nil {
		t.Fatalf("decodeFramePayload: %v", err)
	}
	if !bytes.Equal(decoded, rawPayload) {
		t.Fatalf("decoded payload mismatch")
	}
}

func ptrsToBytes(ptrs []page.ValuePtr) []byte {
	var b []byte
	for i := range ptrs {
		p := ptrs[i]
		var buf [16]byte
		binary.LittleEndian.PutUint64(buf[0:8], p.Offset)
		binary.LittleEndian.PutUint32(buf[8:12], p.Length)
		binary.LittleEndian.PutUint32(buf[12:16], p.FileID)
		b = append(b, buf[:]...)
	}
	return b
}
