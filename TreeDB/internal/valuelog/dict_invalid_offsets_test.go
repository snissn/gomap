package valuelog

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/snissn/compress/zstd"
)

func makeSyntheticDictSample(i int, size int) []byte {
	base := []byte("template-heavy-validator-snapshot-entry/")
	buf := bytes.Repeat(base, size/len(base)+1)[:size]
	for off := 0; off+16 < size; off += 257 {
		x := uint64(i*1315423911 + off*2654435761)
		for j := 0; j < 8 && off+j < size; j++ {
			buf[off+j] = byte(x >> (8 * j))
		}
	}
	tail := fmt.Sprintf("/%08x/%08x", i, i*17)
	copy(buf[size-len(tail):], []byte(tail))
	return buf
}

func buildSyntheticDictHistory(samples [][]byte, n int) []byte {
	h := make([]byte, 0, n)
	for _, s := range samples {
		if len(h) >= n {
			break
		}
		need := n - len(h)
		if len(s) > need {
			h = append(h, s[:need]...)
		} else {
			h = append(h, s...)
		}
	}
	return h
}

func syntheticDictSamples(count, size int) [][]byte {
	out := make([][]byte, count)
	for i := 0; i < count; i++ {
		out[i] = makeSyntheticDictSample(i, size)
	}
	return out
}

var repeatOffsetsRE = regexp.MustCompile(`New repeat offsets \[([0-9 ]+)\]`)

func debugShowsZeroRepeatOffset(s string) bool {
	m := repeatOffsetsRE.FindStringSubmatch(s)
	if len(m) != 2 {
		return false
	}
	for _, field := range strings.Fields(m[1]) {
		n, err := strconv.Atoi(field)
		if err != nil {
			continue
		}
		if n == 0 {
			return true
		}
	}
	return false
}

func TestZstdBuildDict_InvalidRepeatOffsetsFromSmallHistory(t *testing.T) {
	samples := syntheticDictSamples(128, 43635)
	var dbg strings.Builder

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected BuildDict debug self-check to panic")
		}
		msg := fmt.Sprint(r)
		if !strings.Contains(msg, "invalid offset in dictionary") {
			t.Fatalf("panic=%q, want invalid offset in dictionary", msg)
		}
		if !debugShowsZeroRepeatOffset(dbg.String()) {
			t.Fatalf("debug output missing zero repeat-offset evidence:\n%s", dbg.String())
		}
	}()

	_, _ = zstd.BuildDict(zstd.BuildDictOptions{
		ID:       1,
		Contents: samples,
		History:  buildSyntheticDictHistory(samples, 32<<10),
		Level:    zstd.SpeedFastest,
		DebugOut: &dbg,
	})
}

func TestZstdBuildDict_LargerHistoryProducesValidRepeatOffsets(t *testing.T) {
	samples := syntheticDictSamples(128, 43635)
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       1,
		Contents: samples,
		History:  buildSyntheticDictHistory(samples, 128<<10),
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}
	info, err := zstd.InspectDictionary(dict)
	if err != nil {
		t.Fatalf("InspectDictionary: %v", err)
	}
	for i, off := range info.Offsets() {
		if off <= 0 {
			t.Fatalf("offset[%d]=%d, want > 0", i, off)
		}
		if off > info.ContentSize() {
			t.Fatalf("offset[%d]=%d exceeds content size %d", i, off, info.ContentSize())
		}
	}
}
