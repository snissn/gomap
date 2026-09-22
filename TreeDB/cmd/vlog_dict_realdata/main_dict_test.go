package main

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/snissn/compress/zstd"
)

func makeSyntheticDictSampleForHarness(i int, size int) []byte {
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

func syntheticHarnessSamples(count, size int) [][]byte {
	out := make([][]byte, count)
	for i := 0; i < count; i++ {
		out[i] = makeSyntheticDictSampleForHarness(i, size)
	}
	return out
}

func TestTrainDictFixedSize_UsesInitialOffsetsToAvoidInvalidDictionary(t *testing.T) {
	samples := syntheticHarnessSamples(128, 43635)
	dict, err := trainDictFixedSize(1, samples, 32<<10, zstd.SpeedFastest, 40<<10)
	if err != nil {
		t.Fatalf("trainDictFixedSize: %v", err)
	}
	if len(dict) != 40<<10 {
		t.Fatalf("dict len=%d, want %d", len(dict), 40<<10)
	}
	if err := validateDict(dict, zstd.SpeedFastest); err != nil {
		t.Fatalf("validateDict: %v", err)
	}
}
