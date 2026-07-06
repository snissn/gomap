package caching

import (
	"encoding/binary"
	"runtime"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
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

func BenchmarkEntrySliceLeaseHotClassAfterGC(b *testing.B) {
	entrySlicePoolTestMu.Lock()
	defer entrySlicePoolTestMu.Unlock()

	entrySliceLeaseMu.Lock()
	savedLeases := entrySliceLeases
	entrySliceLeases = [entrySliceLeaseClassCount][][]batch.Entry{}
	entrySliceLeaseMu.Unlock()
	savedBytes := entrySlicePoolBytes.Load()
	savedLastGC := entrySlicePoolLastGC.Load()
	savedBudget := entrySlicePoolBudgetBytes
	entrySlicePoolBytes.Store(0)
	entrySlicePoolLastGC.Store(0)
	entrySlicePoolBudgetBytes = 192 * 8192 * entrySliceEntrySizeBytes
	for i := range entrySlicePools {
		entrySlicePools[i] = sync.Pool{}
	}
	defer func() {
		entrySliceLeaseMu.Lock()
		entrySliceLeases = savedLeases
		entrySliceLeaseMu.Unlock()
		entrySlicePoolBytes.Store(savedBytes)
		entrySlicePoolLastGC.Store(savedLastGC)
		entrySlicePoolBudgetBytes = savedBudget
		for i := range entrySlicePools {
			entrySlicePools[i] = sync.Pool{}
		}
	}()

	const (
		chunkCap  = 8192
		hotSlices = 192
	)
	for i := 0; i < hotSlices; i++ {
		putEntrySlice(make([]batch.Entry, 0, chunkCap))
	}
	runtime.GC()
	runtime.GC()

	held := make([][]batch.Entry, hotSlices)
	b.SetBytes(int64(hotSlices * chunkCap * int(entrySliceEntrySizeBytes)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runtime.GC()
		runtime.GC()
		for j := range held {
			held[j] = getEntrySlice(chunkCap)
		}
		for j := range held {
			putEntrySlice(held[j])
			held[j] = nil
		}
	}
}
