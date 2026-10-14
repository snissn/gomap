package valuelog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/page"
	"os"
	"path/filepath"
	"testing"
)

// Diagnostic fixture export, using the frozen production leaf-writer fixture.
// It is not a native decoder, work counter or runtime activation.
func TestNativeZSTDActualLeafFixtureExport(t *testing.T) {
	out := os.Getenv("NATIVE_ZSTD_LEAF_ARTIFACT")
	if out == "" {
		t.Skip("explicit artifact directory required")
	}
	if err := os.MkdirAll(out, 0755); err != nil {
		t.Fatal(err)
	}
	type member struct {
		Name   string
		Bytes  int
		SHA256 string
	}
	type receipt struct {
		Name                string
		Compact             bool
		K                   int
		RawBytes            int
		StoredBytes         int
		Header              zstd.Header
		Members             []member
		OrdinarySelectedCRC bool
		CorruptCRCRejected  bool
	}
	var receipts []receipt
	for _, route := range []string{"full", "compact", "dictionary"} {
		compact := route == "compact"
		name := fmt.Sprintf("leaf255-compact-%v", compact)
		if route == "dictionary" {
			name = "leaf255-dictionary"
		}
		var f *os.File
		var ptrs []page.ValuePtr
		var image, dict []byte
		if route == "dictionary" {
			f, ptrs, image, dict = continuationDictionaryProducer(t)
		} else {
			f, ptrs, image = continuationProducerFrame(t, BlockCodecZSTD, compact, MaxFrameK)
		}
		options := []zstd.DOption{zstd.WithDecoderConcurrency(1)}
		if dict != nil {
			options = append(options, zstd.WithDecoderDicts(dict))
		}
		selectedDecoder, err := zstd.NewReader(nil, options...)
		if err != nil {
			t.Fatal(err)
		}
		defer selectedDecoder.Close()
		limits := COWReadLimits{MaxRecordBytes: 2 << 20, MaxRawBytes: MaxFrameK * page.PageSize, MaxValueBytes: page.PageSize}
		shape, err := InspectCOWLeafRecord(f, ptrs[0], limits)
		if err != nil {
			t.Fatal(err)
		}
		if !shape.Compressed || shape.frame.K != MaxFrameK || (dict == nil && shape.frame.Reserved != byte(BlockCodecZSTD)) {
			t.Fatal("writer did not keep expected ZSTD group", shape)
		}
		for _, index := range []int{0, 127, 254} {
			selected, err := InspectCOWLeafRecord(f, ptrs[index], limits)
			if err != nil {
				t.Fatal(err)
			}
			got, err := ReadCOWLeafPage(f, ptrs[index], selected, true, make([]byte, selected.RecordBytes), make([]byte, selected.RawBytes), make([]byte, page.PageSize), func(h FrameHeader, input, dst []byte) ([]byte, error) {
				return selectedDecoder.DecodeAll(input, dst[:0])
			})
			if err != nil {
				t.Fatal(err)
			}
			expanded, _, decoded, err := decodeCompactLeafLogPayloadTo(got, nil)
			if err != nil {
				t.Fatal(err)
			}
			if decoded {
				got = expanded
			}
			requireLeafPagesLogicallyEqual(t, image, got)
		}
		file, err := os.ReadFile(f.Name())
		if err != nil {
			t.Fatal(err)
		}
		start := int(ptrs[0].Offset - 4)
		record := file[start : start+int(shape.RecordBytes)]
		prefix := FrameHeaderSize + int(shape.frame.K)*8 + (int(shape.frame.K)+1)*4
		payload := record[HeaderSize+prefix:]
		dec, err := zstd.NewReader(nil, options...)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := dec.DecodeAll(payload, nil)
		dec.Close()
		if err != nil || len(raw) != int(shape.RawBytes) {
			t.Fatal("ordinary whole group oracle", err)
		}
		want := image
		if compact {
			want, _, err = MaybeCompactLeafLogPayload(image)
			if err != nil {
				t.Fatal(err)
			}
		}
		if !bytes.Equal(raw, bytes.Repeat(want, MaxFrameK)) {
			t.Fatal("whole group mismatch")
		}
		var header zstd.Header
		if err = header.Decode(payload); err != nil {
			t.Fatal(err)
		}
		rw, err := os.OpenFile(f.Name(), os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		at := int64(start + int(shape.RecordBytes) - 1)
		original := file[at]
		if _, err = rw.WriteAt([]byte{original ^ 1}, at); err != nil {
			t.Fatal(err)
		}
		_, bad := ReadCOWLeafPage(f, ptrs[0], shape, true, make([]byte, shape.RecordBytes), make([]byte, shape.RawBytes), make([]byte, page.PageSize), nil)
		if _, err = rw.WriteAt([]byte{original}, at); err != nil {
			t.Fatal(err)
		}
		if err = rw.Close(); err != nil {
			t.Fatal(err)
		}
		if !errors.Is(bad, ErrCorrupt) {
			t.Fatal("actual stored CRC mutation accepted", bad)
		}
		r := receipt{Name: name, Compact: compact, K: MaxFrameK, RawBytes: len(raw), StoredBytes: len(payload), Header: header, OrdinarySelectedCRC: true, CorruptCRCRejected: true}
		if dict != nil {
			if err = os.WriteFile(filepath.Join(out, name+".dict"), dict, 0644); err != nil {
				t.Fatal(err)
			}
		}
		for _, part := range []struct {
			name string
			data []byte
		}{{"file", file}, {"zstd", payload}, {"raw", raw}} {
			target := name + "." + part.name
			if err = os.WriteFile(filepath.Join(out, target), part.data, 0644); err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(part.data)
			r.Members = append(r.Members, member{target, len(part.data), hex.EncodeToString(sum[:])})
		}
		receipts = append(receipts, r)
		t.Logf("%s actual K=%d raw=%d stored=%d header=%+v fixture=%s", name, MaxFrameK, len(raw), len(payload), header, f.Name())
	}
	data, err := json.MarshalIndent(receipts, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(out, "leaf-producer-receipts.json"), append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}

// SAME original production dictionary recipe as TestReadAtGroupedK128WithDict,
// with the original external-leaf page fixture rather than JSON pointed values.
func continuationDictionaryProducer(t *testing.T) (*os.File, []page.ValuePtr, []byte, []byte) {
	t.Helper()
	image := buildSparseLeafPageForPayloadTest(t)
	records := make([]Record, MaxFrameK)
	samples := make([][]byte, MaxFrameK)
	for i := range records {
		records[i] = Record{RID: uint64(i + 1), Value: image}
		samples[i] = []byte(fmt.Sprintf("{\"type\":\"example\",\"id\":%d,\"payload\":\"%s\"}", i, bytes.Repeat([]byte("a"), 1024)))
	}
	history := make([]byte, 0, 8<<10)
	for _, sample := range samples {
		need := (8 << 10) - len(history)
		if need == 0 {
			break
		}
		history = append(history, sample[:min(need, len(sample))]...)
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{ID: 73, Contents: samples, History: history, Offsets: [3]int{1, 4, 8}, Level: zstd.SpeedFastest})
	if err != nil {
		t.Fatal(err)
	}
	id, err := EncodeFileID(ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := compactLeafPayloadTestPath(t, t.TempDir(), 1)
	writer, err := NewWriter(path, id)
	if err != nil {
		t.Fatal(err)
	}
	ptrs, stats, err := writer.AppendFrameWithStats(73, dict, records)
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	if !stats.Kept {
		t.Fatal("actual leaf dictionary frame not kept")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f, ptrs, image, dict
}
