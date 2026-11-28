package valuelog

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/compress/zstd"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func TestScanSegmentCompressionIDs_DictAndTemplate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-l0-000001.log")
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Build a valid zstd dictionary from varied repetitive samples.
	samples := make([][]byte, 32)
	for i := range samples {
		samples[i] = append(bytes.Repeat([]byte("structured-dict-sample-payload-prefix-0123456789-"), 8), []byte(fmt.Sprintf("rec-%04d-val-%06d", i, i*37))...)
	}
	dictBytes, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       77,
		Contents: samples,
		History:  samples[0],
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil || len(dictBytes) == 0 {
		t.Fatalf("BuildDict: len=%d err=%v", len(dictBytes), err)
	}
	const dictID uint64 = 1001
	const tplID1 uint64 = 5001
	const tplID2 uint64 = 5002

	tplVal1, err := templ.EncodePayload(tplID1, [][]byte{[]byte("alpha"), []byte("beta")})
	if err != nil {
		t.Fatalf("EncodePayload 1: %v", err)
	}
	tplVal2, err := templ.EncodePayload(tplID2, [][]byte{[]byte("gamma"), []byte("delta")})
	if err != nil {
		t.Fatalf("EncodePayload 2: %v", err)
	}

	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	// Ungrouped template-encoded record.
	if _, err := w.Append(0, nil, 1, tplVal1); err != nil {
		t.Fatalf("Append ungrouped: %v", err)
	}
	// Grouped dict-compressed frame containing a template-encoded record + raw record.
	recs := []Record{
		{RID: 2, Value: append(append([]byte(nil), tplVal2...), bytes.Repeat([]byte("structured-dict-sample-payload-prefix-0123456789-"), 6)...)},
		{RID: 3, Value: bytes.Repeat([]byte("structured-dict-sample-payload-prefix-0123456789-"), 12)},
	}
	if _, err := w.AppendFrame(dictID, dictBytes, recs); err != nil {
		t.Fatalf("AppendFrame: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 1) Seek-only dict scan (scanTemplates=false, dictLookup=nil).
	gotSeek, err := ScanSegmentCompressionIDs(path, fileID, false, nil)
	if err != nil {
		t.Fatalf("ScanSegmentCompressionIDs(seek): %v", err)
	}
	if _, ok := gotSeek.DictIDs[dictID]; !ok || len(gotSeek.DictIDs) != 1 {
		t.Fatalf("seek DictIDs=%v want {%d}", gotSeek.DictIDs, dictID)
	}
	if len(gotSeek.TemplateIDs) != 0 {
		t.Fatalf("seek TemplateIDs=%v want empty", gotSeek.TemplateIDs)
	}
	if gotSeek.RecordsScanned != 2 || gotSeek.FramesScanned != 2 {
		t.Fatalf("seek counts records=%d frames=%d want 2,2", gotSeek.RecordsScanned, gotSeek.FramesScanned)
	}

	// 2) Full scan (scanTemplates=true) finds both dictID and both templateIDs.
	lookup := func(id uint64) ([]byte, error) {
		if id == dictID {
			return dictBytes, nil
		}
		return nil, ErrMissingDict
	}
	gotFull, err := ScanSegmentCompressionIDs(path, fileID, true, lookup)
	if err != nil {
		t.Fatalf("ScanSegmentCompressionIDs(full): %v", err)
	}
	if _, ok := gotFull.DictIDs[dictID]; !ok || len(gotFull.DictIDs) != 1 {
		t.Fatalf("full DictIDs=%v want {%d}", gotFull.DictIDs, dictID)
	}
	if _, ok := gotFull.TemplateIDs[tplID1]; !ok {
		t.Fatalf("full TemplateIDs missing %d: %v", tplID1, gotFull.TemplateIDs)
	}
	if _, ok := gotFull.TemplateIDs[tplID2]; !ok {
		t.Fatalf("full TemplateIDs missing %d: %v", tplID2, gotFull.TemplateIDs)
	}
	if len(gotFull.TemplateIDs) != 2 {
		t.Fatalf("full TemplateIDs=%v want 2 entries", gotFull.TemplateIDs)
	}
}
