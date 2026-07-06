package caching

import (
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

var batchSetWithRevisionBenchSink int

func BenchmarkBatchSetWithRevisionAllocationShape(b *testing.B) {
	const (
		entries   = 64
		keySize   = 32
		inlineVal = 96
		ptrVal    = 2048
	)

	cases := []struct {
		name             string
		valueSize        int
		pointerThreshold int
		set              func(*Batch, []byte, []byte, page.EntryRevision) error
	}{
		{
			name:             "SetWithRevision_inline",
			valueSize:        inlineVal,
			pointerThreshold: 4096,
			set: func(batch *Batch, key, value []byte, revision page.EntryRevision) error {
				return batch.SetWithRevision(key, value, revision)
			},
		},
		{
			name:             "SetWithRevision_pointer",
			valueSize:        ptrVal,
			pointerThreshold: 32,
			set: func(batch *Batch, key, value []byte, revision page.EntryRevision) error {
				return batch.SetWithRevision(key, value, revision)
			},
		},
		{
			name:             "SetViewValidatedWithRevision_pointer",
			valueSize:        ptrVal,
			pointerThreshold: 32,
			set: func(batch *Batch, key, value []byte, revision page.EntryRevision) error {
				return batch.SetViewValidatedWithRevision(key, value, revision)
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(fmt.Sprintf("%s_%dx%dx%d", tc.name, entries, keySize, tc.valueSize), func(b *testing.B) {
			db, err := Open(b.TempDir(), NewMockBackend(), Options{
				AllowUnsafe:              true,
				DisableWAL:               true,
				MemtableMode:             "btree",
				MemtableShards:           1,
				FlushThreshold:           1 << 30,
				ValueLogPointerThreshold: tc.pointerThreshold,
			})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			defer db.Close()

			keys, values := makeBatchSetWithRevisionBenchPayload(entries, keySize, tc.valueSize)

			b.ReportAllocs()
			b.SetBytes(int64(entries * (keySize + tc.valueSize)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := db.NewBatchWithSize(entries)
				for j := 0; j < entries; j++ {
					if err := tc.set(batch, keys[j], values[j], page.EntryRevision(j+1)); err != nil {
						b.Fatalf("set %d: %v", j, err)
					}
				}
				batchSetWithRevisionBenchSink += len(batch.entries) + batch.size + len(batch.ptrValueIdxs)
				if err := batch.Close(); err != nil {
					b.Fatalf("close: %v", err)
				}
			}
		})
	}
}

func makeBatchSetWithRevisionBenchPayload(entries, keySize, valueSize int) ([][]byte, [][]byte) {
	keys := make([][]byte, entries)
	values := make([][]byte, entries)
	for i := 0; i < entries; i++ {
		key := make([]byte, keySize)
		copy(key, "batch-setwithrevision-key:")
		encodeBenchUint(key[len(key)-8:], uint64(i))
		keys[i] = key

		value := make([]byte, valueSize)
		for j := range value {
			value[j] = byte((i*31 + j*17) & 0xff)
		}
		values[i] = value
	}
	return keys, values
}

func encodeBenchUint(dst []byte, value uint64) {
	for i := len(dst) - 1; i >= 0; i-- {
		dst[i] = byte(value)
		value >>= 8
	}
}
