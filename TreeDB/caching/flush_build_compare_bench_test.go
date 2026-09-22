package caching

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func populateMemtableForFlushBench(b *testing.B, mode memtable.Mode, writes, keySpace int, random bool) memtable.Table {
	b.Helper()
	mt, err := memtable.NewWithCapacityMode(0, mode)
	if err != nil {
		b.Fatalf("new memtable mode=%s: %v", mode.String(), err)
	}
	value := make([]byte, 256)
	for i := range value {
		value[i] = byte(i)
	}
	var key [8]byte
	seq := uint64(1)
	for i := 0; i < writes; i++ {
		var k int
		if random {
			// Deterministic cheap LCG to avoid sorted insertion order.
			seq = seq*6364136223846793005 + 1
			k = int(seq % uint64(keySpace))
		} else {
			k = i % keySpace
		}
		binary.BigEndian.PutUint64(key[:], uint64(k))
		mt.Set(key[:], value)
	}
	mt.Freeze()
	return mt
}

func BenchmarkBuildOpRuns_ModeCompare(b *testing.B) {
	const (
		chunkCap = 8192
		writes   = 400000
	)
	modes := []memtable.Mode{
		memtable.ModeAppendOnly,
		memtable.ModeHashSorted,
		memtable.ModeBTree,
		memtable.ModeSkiplist,
	}
	cases := []struct {
		name     string
		keySpace int
		random   bool
	}{
		{name: "mostly_unique_sorted", keySpace: writes, random: false},
		{name: "overwrite_hotset_random", keySpace: 1 << 14, random: true},
	}

	for _, mode := range modes {
		for _, tc := range cases {
			b.Run(fmt.Sprintf("%s/%s", mode.String(), tc.name), func(b *testing.B) {
				mt := populateMemtableForFlushBench(b, mode, writes, tc.keySpace, tc.random)
				runs, _, err := buildOpRuns(mt, chunkCap)
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
					runs, _, err := buildOpRuns(mt, chunkCap)
					if err != nil {
						b.Fatalf("buildOpRuns: %v", err)
					}
					for _, run := range runs {
						putEntrySlice(run)
					}
					putEntryRuns(runs)
				}
			})
		}
	}
}
