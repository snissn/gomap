package slab

import (
	"bufio"
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/snissn/gomap/TreeDB/page"
)

// BenchmarkSlabGrouped writes N values with K>1 profile and verifies grouped pointers.
// It reports write/read timings and counts grouped vs legacy pointers.
func BenchmarkSlabGrouped(b *testing.B) {
	const (
		N           = 200000
		readSamples = 10000
		k           = 3
	)

	dictBytes, dictHash := loadDict()

	b.Run("structured", func(b *testing.B) {
		values, err := loadStructuredValues(N)
		if err != nil {
			b.Skipf("structured data unavailable: %v", err)
		}
		runGroupedBench(b, values, k, dictBytes, dictHash)
	})

	b.Run("random", func(b *testing.B) {
		values := make([][]byte, N)
		src := rand.New(rand.NewSource(1))
		for i := 0; i < N; i++ {
			v := make([]byte, 169)
			for j := range v {
				v[j] = byte(src.Intn(256))
			}
			values[i] = v
		}
		runGroupedBench(b, values, k, dictBytes, dictHash)
	})
}

func runGroupedBench(b *testing.B, values [][]byte, k int, dict []byte, dictHash uint64) {
	const readSamples = 10000
	keys := make([][]byte, len(values))
	dir := b.TempDir()
	opts := Options{
		Compression: CompressionOptions{
			Kind:            CompressionZSTD,
			MinBytes:        1,
			MinSavingsBytes: 0,
		},
		OmitSlabKeys: true,
	}
	sm, err := NewSlabManagerWithOptions(dir, opts)
	if err != nil {
		b.Fatalf("NewSlabManagerWithOptions: %v", err)
	}
	profile := &ActiveCompressionProfile{K: k, DictBytes: len(dict), DictHash: dictHash}
	trainer := &compressionTrainer{}
	trainer.lastProfile.Store(profile)
	sm.compressionTrainer = trainer

	b.ResetTimer()
	b.ReportAllocs()

	var ptrs []page.ValuePtr
	// Write pass.
	batch := 1024
	start := time.Now()
	for i := 0; i < len(values); i += batch {
		end := i + batch
		if end > len(values) {
			end = len(values)
		}
		p, err := sm.AppendMany(keys[i:end], values[i:end])
		if err != nil {
			b.Fatalf("AppendMany: %v", err)
		}
		ptrs = append(ptrs, p...)
	}
	writeElapsed := time.Since(start)

	groupedCnt := 0
	legacyCnt := 0
	for _, p := range ptrs {
		if page.ValuePtrIsGrouped(p) {
			groupedCnt++
		} else {
			legacyCnt++
		}
	}

	// Read random samples.
	idxCount := readSamples
	if len(ptrs) < idxCount {
		idxCount = len(ptrs)
	}
	idxs := rand.Perm(len(ptrs))[:idxCount]
	readStart := time.Now()
	for _, idx := range idxs {
		p := ptrs[idx]
		val, err := sm.Read(p)
		if err != nil {
			b.Fatalf("Read: %v", err)
		}
		_ = val
	}
	readElapsed := time.Since(readStart)

	// Slab size stats.
	info, err := os.Stat(filepath.Join(dir, "data-0000.slab"))
	if err != nil {
		b.Fatalf("stat slab: %v", err)
	}
	bytesPerRecord := float64(info.Size()) / float64(len(ptrs))

	compKind := "none"
	if opts.Compression.Kind == CompressionZSTD {
		compKind = "zstd"
	}
	b.Logf("mode=%s K=%d comp=%s dict_bytes=%d dict_hash=%x grouped=%d legacy=%d slab_bytes=%d bytes/rec=%.2f write_ns/op=%.1f read_ns/op=%.1f", b.Name(), k, compKind, len(dict), dictHash, groupedCnt, legacyCnt, info.Size(), bytesPerRecord, float64(writeElapsed.Nanoseconds())/float64(len(values)), float64(readElapsed.Nanoseconds())/float64(idxCount))
}

// loadStructuredValues reads up to n values from tmp/treedb_kv_full.jsonl (url-escaped fields).
func loadStructuredValues(n int) ([][]byte, error) {
	path := filepath.Join("tmp", "treedb_kv_full.jsonl")
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	// Allow long lines.
	buf := make([]byte, 0, 1<<20)
	scanner.Buffer(buf, 1<<20)
	type kv struct {
		Val string `json:"val"`
	}
	values := make([][]byte, 0, n)
	for scanner.Scan() {
		if len(values) >= n {
			break
		}
		var rec kv
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			continue
		}
		values = append(values, []byte(rec.Val))
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, os.ErrNotExist
	}
	return values, nil
}

func loadDict() ([]byte, uint64) {
	path := filepath.Join("tmp", "dict-32k.zdict")
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		return nil, 0
	}
	return data, xxhash.Sum64(data)
}
