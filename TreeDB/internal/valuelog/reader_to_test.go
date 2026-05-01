package valuelog

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func TestReadAtWithDictTo_UsesDstForCompressedGroupedSubrecord(t *testing.T) {
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

	records := make([]Record, 4)
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
	dstPtrs := make([]page.ValuePtr, len(records))
	ptrs, stats, err := writer.AppendFrameWithStatsInto(0, nil, records, dstPtrs)
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

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	defer func() { _ = f.Close() }()

	for i, ptr := range ptrs {
		dst := make([]byte, 0, len(want[i]))
		dstBacking := dst[:1]
		got, usedDst, err := ReadAtWithDictTo(f, ptr, false, nil, nil, nil, templ.DecodeOptions{}, dst)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !usedDst {
			t.Fatalf("read %d: expected usedDst=true", i)
		}
		if len(got) != len(want[i]) {
			t.Fatalf("read %d: got len=%d want=%d", i, len(got), len(want[i]))
		}
		if &got[0] != &dstBacking[0] {
			t.Fatalf("read %d: expected returned slice to be backed by dst", i)
		}
		if !bytes.Equal(got, want[i]) {
			t.Fatalf("read %d: value mismatch", i)
		}
	}
}

func TestReadAtWithDictTo_DecodesTemplatePayloadIntoDst(t *testing.T) {
	dir := t.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("encode file id: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")

	def := templ.TemplateDef{
		Kind:    templ.TemplateAnchors,
		Anchors: [][]byte{bytes.Repeat([]byte("A"), 16), bytes.Repeat([]byte("B"), 16)},
	}
	defBytes, err := templ.EncodeTemplateDef(def, templ.Config{})
	if err != nil {
		t.Fatalf("EncodeTemplateDef: %v", err)
	}
	payload, err := templ.EncodePayload(1, [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")})
	if err != nil {
		t.Fatalf("EncodePayload: %v", err)
	}
	want, err := templ.DecodePayloadAppend(nil, payload, func(id uint64) (templ.TemplateDef, error) {
		if id != 1 {
			return templ.TemplateDef{}, ErrMissingTemplate
		}
		return def, nil
	}, templ.DecodeOptions{})
	if err != nil {
		t.Fatalf("DecodePayloadAppend: %v", err)
	}

	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	ptr, err := writer.Append(0, nil, 1, payload)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	defer func() { _ = f.Close() }()

	lookup := func(id uint64) ([]byte, error) {
		if id != 1 {
			return nil, ErrMissingTemplate
		}
		return defBytes, nil
	}
	dst := make([]byte, 0, len(want))
	dstBacking := dst[:1]
	got, usedDst, err := ReadAtWithDictTo(f, ptr, true, nil, lookup, nil, templ.DecodeOptions{}, dst)
	if err != nil {
		t.Fatalf("ReadAtWithDictTo: %v", err)
	}
	if !usedDst {
		t.Fatalf("expected usedDst=true")
	}
	if len(got) != len(want) {
		t.Fatalf("got len=%d want=%d", len(got), len(want))
	}
	if &got[0] != &dstBacking[0] {
		t.Fatalf("expected returned slice to be backed by dst")
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("value mismatch: got %q want %q", got, want)
	}
}
