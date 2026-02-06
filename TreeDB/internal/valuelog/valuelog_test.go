package valuelog

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

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
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

func TestValueLogManager_MmapReadAppend(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

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
	ptr, err := writer.Append(0, nil, 1, []byte("hello"))
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	m.SetDisableReadChecksum(true)
	defer func() { _ = m.Close() }()

	f := m.files[fileID]
	f.remapToFileSize()
	data, _ := f.mmapData.Load().([]byte)
	if len(data) == 0 {
		t.Fatalf("expected mmap data to be present")
	}

	got, err := m.ReadAppend(ptr, nil)
	if err != nil {
		t.Fatalf("read append: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("mmap read mismatch: %q", string(got))
	}
}

func TestReadAtGroupedFastPathWithoutChecksum(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	ptrs, err := writer.AppendFrame(0, nil, []Record{
		{RID: 1, Value: []byte("alpha")},
		{RID: 2, Value: []byte("beta")},
		{RID: 3, Value: []byte("gamma")},
	})
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if len(ptrs) != 3 {
		t.Fatalf("expected 3 ptrs, got %d", len(ptrs))
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	expect := []string{"alpha", "beta", "gamma"}
	for i, ptr := range ptrs {
		got, err := ReadAtWithDict(f, ptr, false, nil, nil, nil, templ.DecodeOptions{})
		if err != nil {
			t.Fatalf("read at ptr%d: %v", i+1, err)
		}
		if string(got) != expect[i] {
			t.Fatalf("ptr%d mismatch: got %q want %q", i+1, got, expect[i])
		}

		// Also cover legacy pointers where record length is unknown (0) but the
		// grouped flag and sub-index are still set.
		legacy := ptr
		legacy.Length = page.ValuePtrMarkGrouped(0, page.ValuePtrSubIndex(ptr))
		gotLegacy, err := ReadAtWithDict(f, legacy, false, nil, nil, nil, templ.DecodeOptions{})
		if err != nil {
			t.Fatalf("read at legacy ptr%d: %v", i+1, err)
		}
		if string(gotLegacy) != expect[i] {
			t.Fatalf("legacy ptr%d mismatch: got %q want %q", i+1, gotLegacy, expect[i])
		}
	}
}

func TestReadAtGroupedFastPathSubIndexRange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}

	records := make([]Record, MaxFrameK)
	expect := make([]string, len(records))
	for i := range records {
		expect[i] = fmt.Sprintf("val-%02d", i)
		records[i] = Record{RID: uint64(i + 1), Value: []byte(expect[i])}
	}

	ptrs, err := writer.AppendFrame(0, nil, records)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if len(ptrs) != len(records) {
		t.Fatalf("expected %d ptrs, got %d", len(records), len(ptrs))
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	for i, ptr := range ptrs {
		got, err := ReadAtWithDict(f, ptr, false, nil, nil, nil, templ.DecodeOptions{})
		if err != nil {
			t.Fatalf("read at ptr%d: %v", i+1, err)
		}
		if string(got) != expect[i] {
			t.Fatalf("ptr%d mismatch: got %q want %q", i+1, string(got), expect[i])
		}
	}
}

func TestReadAtGroupedK128WithDict(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	records := make([]Record, MaxFrameK)
	expect := make([]string, len(records))
	samples := make([][]byte, len(records))
	payload := bytes.Repeat([]byte("a"), 1024)
	for i := range records {
		expect[i] = fmt.Sprintf("{\"type\":\"example\",\"id\":%d,\"payload\":\"%s\"}", i, payload)
		records[i] = Record{RID: uint64(i + 1), Value: []byte(expect[i])}
		samples[i] = records[i].Value
	}

	const dictID = uint64(1)
	const dictBytes = 8 << 10 // 8KiB
	history := make([]byte, 0, dictBytes)
	for i := range samples {
		if len(history) >= dictBytes {
			break
		}
		need := dictBytes - len(history)
		sample := samples[i]
		if len(sample) > need {
			sample = sample[:need]
		}
		history = append(history, sample...)
	}
	if len(history) < dictBytes {
		history = append(history, bytes.Repeat([]byte("x"), dictBytes-len(history))...)
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: samples,
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}
	if len(dict) == 0 {
		t.Fatalf("BuildDict: empty dict")
	}

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	ptrScratch := make([]page.ValuePtr, len(records))
	ptrs, stats, err := writer.AppendFrameWithStatsInto(dictID, dict, records, ptrScratch)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append: %v", err)
	}
	if stats.Attempted && !stats.Kept {
		_ = writer.Close()
		t.Fatalf("expected dict compression to keep compressed bytes")
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	dictLookup := func(id uint64) ([]byte, error) {
		if id != dictID {
			return nil, ErrMissingDict
		}
		return dict, nil
	}
	for i, ptr := range ptrs {
		got, err := ReadAtWithDict(f, ptr, true, dictLookup, nil, nil, templ.DecodeOptions{})
		if err != nil {
			t.Fatalf("read at ptr%d: %v", i+1, err)
		}
		if string(got) != expect[i] {
			t.Fatalf("ptr%d mismatch: got %q want %q", i+1, string(got), expect[i])
		}
	}
}

func TestAppendEncodedFrameInto_RoundTripWithDict(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	records := make([]Record, 16)
	expect := make([][]byte, len(records))
	samples := make([][]byte, len(records))
	for i := range records {
		v := []byte(fmt.Sprintf("{\"kind\":\"evt\",\"id\":%d,\"payload\":\"%s\"}", i, bytes.Repeat([]byte("z"), 256)))
		records[i] = Record{RID: uint64(i + 1), Value: v}
		expect[i] = append([]byte(nil), v...)
		samples[i] = v
	}

	const dictID = uint64(11)
	history := make([]byte, 0, 8<<10)
	for i := range samples {
		if len(history) >= cap(history) {
			break
		}
		need := cap(history) - len(history)
		s := samples[i]
		if len(s) > need {
			s = s[:need]
		}
		history = append(history, s...)
	}
	if len(history) < 8 {
		history = append(history, bytes.Repeat([]byte("x"), 8-len(history))...)
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: samples,
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}
	if len(dict) == 0 {
		t.Fatalf("BuildDict: empty dict")
	}

	body, _, err := EncodeFrameWithOptions(dictID, dict, records, zstd.SpeedFastest, false)
	if err != nil {
		t.Fatalf("EncodeFrameWithOptions: %v", err)
	}

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	ptrScratch := make([]page.ValuePtr, len(records))
	ptrs, err := writer.AppendEncodedFrameInto(body, len(records), ptrScratch)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("AppendEncodedFrameInto: %v", err)
	}
	if len(ptrs) != len(records) {
		_ = writer.Close()
		t.Fatalf("unexpected ptr count: got=%d want=%d", len(ptrs), len(records))
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	dictLookup := func(id uint64) ([]byte, error) {
		if id != dictID {
			return nil, ErrMissingDict
		}
		return dict, nil
	}
	for i, ptr := range ptrs {
		got, err := ReadAtWithDict(f, ptr, true, dictLookup, nil, nil, templ.DecodeOptions{})
		if err != nil {
			t.Fatalf("read at ptr%d: %v", i+1, err)
		}
		if !bytes.Equal(got, expect[i]) {
			t.Fatalf("ptr%d mismatch", i+1)
		}
	}
}

func TestFramePreparer_PrepareFrame_RoundTripWithDict(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	records := make([]Record, 16)
	expect := make([][]byte, len(records))
	samples := make([][]byte, len(records))
	for i := range records {
		v := []byte(fmt.Sprintf("{\"kind\":\"evt\",\"id\":%d,\"payload\":\"%s\"}", i, bytes.Repeat([]byte("z"), 256)))
		records[i] = Record{RID: uint64(i + 1), Value: v}
		expect[i] = append([]byte(nil), v...)
		samples[i] = v
	}

	const dictID = uint64(77)
	history := make([]byte, 0, 8<<10)
	for i := range samples {
		if len(history) >= cap(history) {
			break
		}
		need := cap(history) - len(history)
		s := samples[i]
		if len(s) > need {
			s = s[:need]
		}
		history = append(history, s...)
	}
	if len(history) < 8 {
		history = append(history, bytes.Repeat([]byte("x"), 8-len(history))...)
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: samples,
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}
	if len(dict) == 0 {
		t.Fatalf("BuildDict: empty dict")
	}

	prep := NewFramePreparer()
	prep.SetDictFrameEncoderOptions(zstd.SpeedFastest, false)
	body, stats, err := prep.PrepareFrame(dictID, dict, records)
	if err != nil {
		t.Fatalf("PrepareFrame: %v", err)
	}
	if stats.Records != len(records) {
		t.Fatalf("records mismatch: got=%d want=%d", stats.Records, len(records))
	}
	if stats.RawPayloadBytes <= 0 {
		t.Fatalf("expected raw payload bytes > 0")
	}
	if !stats.Attempted {
		t.Fatalf("expected compression attempt")
	}

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	ptrScratch := make([]page.ValuePtr, len(records))
	ptrs, err := writer.AppendEncodedFrameInto(body, len(records), ptrScratch)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("AppendEncodedFrameInto: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	dictLookup := func(id uint64) ([]byte, error) {
		if id != dictID {
			return nil, ErrMissingDict
		}
		return dict, nil
	}
	for i, ptr := range ptrs {
		got, err := ReadAtWithDict(f, ptr, true, dictLookup, nil, nil, templ.DecodeOptions{})
		if err != nil {
			t.Fatalf("read at ptr%d: %v", i+1, err)
		}
		if !bytes.Equal(got, expect[i]) {
			t.Fatalf("ptr%d mismatch", i+1)
		}
	}
}

func TestFramePreparer_PrepareFrameInto_ReusesBuffer(t *testing.T) {
	records := []Record{
		{RID: 1, Value: bytes.Repeat([]byte("alpha-001|"), 64)},
		{RID: 2, Value: bytes.Repeat([]byte("bravo-002|"), 64)},
		{RID: 3, Value: bytes.Repeat([]byte("charlie-003|"), 64)},
		{RID: 4, Value: bytes.Repeat([]byte("delta-004|"), 64)},
	}

	prep := NewFramePreparer()

	buf := make([]byte, 0, 8<<10)
	body1, stats1, err := prep.PrepareFrameInto(buf, 0, nil, records)
	if err != nil {
		t.Fatalf("PrepareFrameInto first: %v", err)
	}
	if len(body1) == 0 {
		t.Fatalf("expected non-empty frame body")
	}
	if stats1.Attempted {
		t.Fatalf("did not expect compression attempt without dict")
	}
	firstPtr := &body1[0]

	body2, _, err := prep.PrepareFrameInto(body1[:0], 0, nil, records)
	if err != nil {
		t.Fatalf("PrepareFrameInto second: %v", err)
	}
	if len(body2) == 0 {
		t.Fatalf("expected non-empty second frame body")
	}
	if &body2[0] != firstPtr {
		t.Fatalf("expected frame body buffer reuse")
	}
}

func TestFramePreparer_KeepPolicySkipsCompression(t *testing.T) {
	records := []Record{
		{RID: 1, Value: bytes.Repeat([]byte("alpha-001|"), 32)},
		{RID: 2, Value: bytes.Repeat([]byte("bravo-002|"), 32)},
		{RID: 3, Value: bytes.Repeat([]byte("charlie-003|"), 32)},
		{RID: 4, Value: bytes.Repeat([]byte("delta-004|"), 32)},
	}
	const dictID = uint64(9)
	samples := make([][]byte, 16)
	for i := range samples {
		samples[i] = []byte(fmt.Sprintf("{\"kind\":\"evt\",\"id\":%d,\"payload\":\"%s\"}", i, bytes.Repeat([]byte("q"), 128)))
	}
	history := make([]byte, 0, 8<<10)
	for i := range samples {
		if len(history) >= cap(history) {
			break
		}
		need := cap(history) - len(history)
		s := samples[i]
		if len(s) > need {
			s = s[:need]
		}
		history = append(history, s...)
	}
	if len(history) < 8 {
		history = append(history, bytes.Repeat([]byte("x"), 8-len(history))...)
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: samples,
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}

	prep := NewFramePreparer()
	prep.SetDictFrameEncoderOptions(zstd.SpeedFastest, false)
	// Encode cost dominates IO savings: skip compression before encode.
	prep.SetKeepPolicy(0.01, 10.0, 0.0)

	body, stats, err := prep.PrepareFrame(dictID, dict, records)
	if err != nil {
		t.Fatalf("PrepareFrame: %v", err)
	}
	if stats.Attempted {
		t.Fatalf("expected no compression attempt under keep policy")
	}
	if stats.Kept {
		t.Fatalf("expected raw body to be kept")
	}
	hdr, _, _, _, err := DecodeFrame(body)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	if hdr.Flags&FrameFlagCompressed != 0 {
		t.Fatalf("expected uncompressed frame body")
	}
	if hdr.DictID != dictID {
		t.Fatalf("dict id mismatch: got=%d want=%d", hdr.DictID, dictID)
	}
}

func TestReadAtLargeValueLengthHintOmitted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}

	overhead := headerWithoutCRC + FrameHeaderSize + 8 + 8
	n := int(page.ValuePtrGroupedMaxRecordLen) - overhead + 1
	if n <= 0 {
		t.Fatalf("computed invalid payload size: %d", n)
	}
	value := bytes.Repeat([]byte("a"), n)

	ptr, err := writer.Append(0, nil, 1, value)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append: %v", err)
	}
	if got := page.ValuePtrRecordLength(ptr); got != 0 {
		_ = writer.Close()
		t.Fatalf("expected length hint to be omitted for large value, got %d", got)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	got, err := ReadAtWithDict(f, ptr, true, nil, nil, nil, templ.DecodeOptions{})
	if err != nil {
		t.Fatalf("read at: %v", err)
	}
	if len(got) != len(value) {
		t.Fatalf("value length mismatch: got=%d want=%d", len(got), len(value))
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("value bytes mismatch")
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

func TestEncodeFrameSkipsCompressionWithoutDict(t *testing.T) {
	value := bytes.Repeat([]byte("a"), 2048)
	body, header, err := EncodeFrame(0, nil, []Record{{RID: 1, Value: value}})
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	if header.Flags&FrameFlagCompressed != 0 {
		t.Fatalf("expected no compression flag without dict")
	}
	decoded, rids, offsets, payload, err := DecodeFrame(body)
	if err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	if decoded.Flags&FrameFlagCompressed != 0 {
		t.Fatalf("expected no compression flag in decoded header")
	}
	if len(rids) != 1 || rids[0] != 1 {
		t.Fatalf("unexpected rids: %v", rids)
	}
	if len(offsets) != 2 || offsets[1] != uint32(len(value)) {
		t.Fatalf("unexpected offsets: %v", offsets)
	}
	if !bytes.Equal(payload, value) {
		t.Fatalf("payload mismatch")
	}
}
