package memtable

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"testing"
)

func benchModeTable(b *testing.B, mode Mode) Table {
	b.Helper()
	mt, err := NewWithCapacityMode(0, mode)
	if err != nil {
		b.Fatalf("new memtable mode=%s: %v", mode.String(), err)
	}
	return mt
}

func BenchmarkMemtableSetParallel_ModeCompare(b *testing.B) {
	const (
		// Keep payloads small so long benchtimes (e.g. 2s+ at -cpu=16) do not
		// exhaust skiplist's fixed 4GiB arena during sustained overwrite loops.
		valueSize = 16
	)
	modes := []Mode{ModeAppendOnly, ModeHashSorted, ModeBTree, ModeSkiplist}
	patterns := []struct {
		name     string
		keySpace int
	}{
		{name: "unique_like", keySpace: 1 << 20},
		{name: "hot_overwrite", keySpace: 1 << 12},
	}

	for _, mode := range modes {
		for _, pattern := range patterns {
			b.Run(fmt.Sprintf("%s/%s", mode.String(), pattern.name), func(b *testing.B) {
				mt := benchModeTable(b, mode)
				keys := make([][]byte, pattern.keySpace)
				for i := 0; i < pattern.keySpace; i++ {
					k := make([]byte, 8)
					binary.BigEndian.PutUint64(k, uint64(i))
					keys[i] = k
				}
				value := make([]byte, valueSize)
				for i := range value {
					value[i] = byte(i)
				}

				var seq atomic.Uint64
				b.ReportAllocs()
				b.SetBytes(int64(valueSize))
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						n := seq.Add(1)
						mt.Set(keys[int(n%uint64(pattern.keySpace))], value)
					}
				})
			})
		}
	}
}
