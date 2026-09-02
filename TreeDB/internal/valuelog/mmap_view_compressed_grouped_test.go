package valuelog

import (
	"bytes"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func TestReadUnsafe_CompressedGrouped_DoesNotRetainFullFrame(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "value-l0-000001.log")

	records := make([]Record, MaxFrameK)
	samples := make([][]byte, len(records))
	for i := range records {
		v := make([]byte, 4<<10) // 4KiB
		v[0] = byte(i)
		copy(v[1:], bytes.Repeat([]byte("a"), len(v)-1))
		records[i] = Record{RID: uint64(i + 1), Value: v}
		samples[i] = v
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
		t.Fatalf("NewWriter: %v", err)
	}
	ptrScratch := make([]page.ValuePtr, len(records))
	ptrs, stats, err := writer.AppendFrameWithStatsInto(dictID, dict, records, ptrScratch)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("AppendFrameWithStatsInto: %v", err)
	}
	if !stats.Attempted || !stats.Kept {
		_ = writer.Close()
		t.Fatalf("expected compressed frame (attempted=%v kept=%v)", stats.Attempted, stats.Kept)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	dictLookup := func(id uint64) ([]byte, error) {
		if id != dictID {
			return nil, fmt.Errorf("unexpected dictID %d", id)
		}
		return dict, nil
	}

	f, err := openFile(path, page.ValueLogFileID(1), dictLookup, nil, templ.DecodeOptions{}, nil)
	if err != nil {
		t.Fatalf("openFile: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	// Pick a middle entry to ensure we don't accidentally decode only the full
	// frame payload by returning a view of the entire decoded raw buffer.
	i := len(ptrs) / 2
	got, err := f.ReadUnsafe(ptrs[i], false)
	if err != nil {
		t.Fatalf("ReadUnsafe: %v", err)
	}
	if !bytes.Equal(got, records[i].Value) {
		t.Fatalf("value mismatch")
	}
	if cap(got) != len(got) {
		t.Fatalf("expected ReadUnsafe to return a tight copy for compressed grouped frames (cap=%d len=%d)", cap(got), len(got))
	}
}
