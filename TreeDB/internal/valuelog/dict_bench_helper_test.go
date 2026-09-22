package valuelog

import (
	"bytes"
	"fmt"
	"testing"
)

func makeSyntheticDictSampleForBench(i int, size int) []byte {
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

func TestBuildBenchDict_UsesInitialOffsetsToAvoidInvalidDictionary(t *testing.T) {
	samples := make([][]byte, 128)
	for i := range samples {
		samples[i] = makeSyntheticDictSampleForBench(i, 43635)
	}
	dict, err := buildBenchDict(1, samples)
	if err != nil {
		t.Fatalf("buildBenchDict: %v", err)
	}
	if len(dict) != 40<<10 {
		t.Fatalf("dict len=%d, want %d", len(dict), 40<<10)
	}
	if err := validateBenchDict(dict); err != nil {
		t.Fatalf("validateBenchDict: %v", err)
	}
}

func TestBuildBenchDictWithHistory_UsesInitialOffsetsToAvoidInvalidDictionary(t *testing.T) {
	samples := make([][]byte, 128)
	for i := range samples {
		samples[i] = makeSyntheticDictSampleForBench(i, 43635)
	}
	dict, err := buildBenchDictWithHistory(1, samples, 32<<10)
	if err != nil {
		t.Fatalf("buildBenchDictWithHistory: %v", err)
	}
	if len(dict) != 40<<10 {
		t.Fatalf("dict len=%d, want %d", len(dict), 40<<10)
	}
	if err := validateBenchDict(dict); err != nil {
		t.Fatalf("validateBenchDict: %v", err)
	}
}
