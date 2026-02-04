package node

import (
	"bytes"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

const (
	benchKeySize   = 32
	benchValueSize = 128
	benchKeyCount  = 1 << 12
	benchPagePool  = 1024
)

func makeBenchKeys(count int, prefixBytes int) [][]byte {
	keys := make([][]byte, count)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < count; i++ {
		k := make([]byte, benchKeySize)
		for j := 0; j < prefixBytes && j < benchKeySize; j++ {
			k[j] = 0x42
		}
		_, _ = rng.Read(k[prefixBytes:])
		keys[i] = k
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	return keys
}

func makeBenchValues(count int) [][]byte {
	values := make([][]byte, count)
	rng := rand.New(rand.NewSource(2))
	for i := 0; i < count; i++ {
		v := make([]byte, benchValueSize)
		_, _ = rng.Read(v)
		values[i] = v
	}
	return values
}

func benchmarkAddLeafEntryWithPrefix(b *testing.B, prefixCompression bool, prefixBytes int) {
	keys := makeBenchKeys(benchKeyCount, prefixBytes)
	values := makeBenchValues(benchKeyCount)
	pageBuf := make([]byte, page.PageSize)

	b.ReportAllocs()
	b.ResetTimer()

	builder := NewBuilderWithOptions(pageBuf, page.PageTypeLeaf, BuilderOptions{
		LeafPrefixCompression: prefixCompression,
	})
	builder.SetPageID(1)

	for i := 0; i < b.N; i++ {
		key := keys[i&(len(keys)-1)]
		val := values[i&(len(values)-1)]
		entrySize, prefixLen, suffixLen := builder.LeafEntrySizeWithPrefix(key, val, FlagInline)
		if err := builder.AddLeafEntryWithPrefix(key, val, FlagInline, page.ValuePtr{}, entrySize, prefixLen, suffixLen); err != nil {
			if err != ErrNodeFull {
				b.Fatalf("unexpected error: %v", err)
			}
			builder = NewBuilderWithOptions(pageBuf, page.PageTypeLeaf, BuilderOptions{
				LeafPrefixCompression: prefixCompression,
			})
			builder.SetPageID(1)
			i--
		}
	}
}

func benchmarkAddLeafEntryWithPrefixMmap(b *testing.B, prefixCompression bool, prefixBytes int) {
	keys := makeBenchKeys(benchKeyCount, prefixBytes)
	values := makeBenchValues(benchKeyCount)

	dir := b.TempDir()
	path := filepath.Join(dir, "index.db")
	p, err := pager.Open(path, 4<<20)
	if err != nil {
		b.Fatalf("pager open: %v", err)
	}
	defer func() { _ = p.Close() }()

	startID, err := p.Alloc(benchPagePool)
	if err != nil {
		b.Fatalf("pager alloc: %v", err)
	}

	pageBufs := make([][]byte, benchPagePool)
	for i := 0; i < benchPagePool; i++ {
		buf, err := p.GetForWrite(startID + uint64(i))
		if err != nil {
			b.Fatalf("pager get: %v", err)
		}
		pageBufs[i] = buf
	}

	b.ReportAllocs()
	b.ResetTimer()

	pageIdx := 0
	builder := NewBuilderWithOptions(pageBufs[pageIdx], page.PageTypeLeaf, BuilderOptions{
		LeafPrefixCompression: prefixCompression,
	})
	builder.SetPageID(startID + uint64(pageIdx))

	for i := 0; i < b.N; i++ {
		key := keys[i&(len(keys)-1)]
		val := values[i&(len(values)-1)]
		entrySize, prefixLen, suffixLen := builder.LeafEntrySizeWithPrefix(key, val, FlagInline)
		if err := builder.AddLeafEntryWithPrefix(key, val, FlagInline, page.ValuePtr{}, entrySize, prefixLen, suffixLen); err != nil {
			if err != ErrNodeFull {
				b.Fatalf("unexpected error: %v", err)
			}
			pageIdx++
			if pageIdx >= len(pageBufs) {
				pageIdx = 0
			}
			builder = NewBuilderWithOptions(pageBufs[pageIdx], page.PageTypeLeaf, BuilderOptions{
				LeafPrefixCompression: prefixCompression,
			})
			builder.SetPageID(startID + uint64(pageIdx))
			i--
		}
	}
}

func BenchmarkAddLeafEntryWithPrefix_NoPrefix(b *testing.B) {
	benchmarkAddLeafEntryWithPrefix(b, false, 0)
}

func BenchmarkAddLeafEntryWithPrefix_PrefixHeavy(b *testing.B) {
	benchmarkAddLeafEntryWithPrefix(b, true, 16)
}

func BenchmarkAddLeafEntryWithPrefix_PrefixLight(b *testing.B) {
	benchmarkAddLeafEntryWithPrefix(b, true, 2)
}

func BenchmarkAddLeafEntryWithPrefixMmap_NoPrefix(b *testing.B) {
	benchmarkAddLeafEntryWithPrefixMmap(b, false, 0)
}

func BenchmarkAddLeafEntryWithPrefixMmap_PrefixHeavy(b *testing.B) {
	benchmarkAddLeafEntryWithPrefixMmap(b, true, 16)
}

func BenchmarkAddLeafEntryWithPrefixMmap_PrefixLight(b *testing.B) {
	benchmarkAddLeafEntryWithPrefixMmap(b, true, 2)
}
