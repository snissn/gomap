package valuelog

import (
	"bytes"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestDictAppendReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")
	w, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = w.Close() }()

	pattern := []byte("{\"key\":\"value\",\"active\":true}")
	const (
		valueSize = 1024
		k         = 4
		frames    = 512
		samples   = 256
	)
	values := make([][]byte, samples)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < samples; i++ {
		buf := make([]byte, valueSize)
		for off := 0; off < valueSize; {
			n := copy(buf[off:], pattern)
			off += n
		}
		rng.Read(buf[valueSize-64:])
		values[i] = buf
	}

	dict, err := buildFallbackBenchDict(1)
	if err != nil {
		t.Fatalf("build dict: %v", err)
	}
	if len(dict) == 0 {
		t.Fatalf("dict empty")
	}
	const dictID = uint64(1)

	records := make([]Record, k)
	var ptrs [k]page.ValuePtr
	collected := make([]page.ValuePtr, 0, frames*k)
	expected := make([][]byte, 0, frames*k)
	for i := 0; i < frames; i++ {
		for j := 0; j < k; j++ {
			val := values[(i+j)%len(values)]
			records[j] = Record{RID: uint64(i*k + j + 1), Value: val}
		}
		out, _, err := w.AppendFrameWithStatsInto(dictID, dict, records, ptrs[:])
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		collected = append(collected, out...)
		for j := 0; j < k; j++ {
			expected = append(expected, records[j].Value)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	mgr.SetDictLookup(func(id uint64) ([]byte, error) {
		if id == dictID {
			return dict, nil
		}
		return nil, ErrMissingDict
	})
	defer func() { _ = mgr.Close() }()

	for i, ptr := range collected {
		val, err := mgr.Read(ptr)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if len(val) != valueSize {
			t.Fatalf("read %d: size=%d", i, len(val))
		}
		if !bytes.Equal(val, expected[i]) {
			t.Fatalf("read %d: value mismatch", i)
		}
	}
}
