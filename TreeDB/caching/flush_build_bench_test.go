package caching

import (
	"encoding/binary"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func BenchmarkBuildOpRunsAllocs(b *testing.B) {
	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		b.Fatalf("new memtable: %v", err)
	}

	var key [8]byte
	value := make([]byte, 256)
	for i := 0; i < 20000; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(i))
		mt.Set(key[:], value)
	}
	mt.Freeze()

	// Warm pools.
	runs, _, err := buildOpRuns(mt, 8192)
	if err != nil {
		b.Fatalf("warm buildOpRuns: %v", err)
	}
	for _, run := range runs {
		putEntrySlice(run)
	}
	putEntryRuns(runs)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runs, _, err := buildOpRuns(mt, 8192)
		if err != nil {
			b.Fatalf("buildOpRuns: %v", err)
		}
		for _, run := range runs {
			putEntrySlice(run)
		}
		putEntryRuns(runs)
	}
}
