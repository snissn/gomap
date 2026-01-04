package treedb

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

type benchWriteMix struct {
	name       string
	smallSize  int
	largeSize  int
	largeEvery int
}

func BenchmarkSplitValueLogWrite(b *testing.B) {
	mixes := []benchWriteMix{
		{name: "Small32", smallSize: 32, largeSize: 0, largeEvery: 0},
		{name: "Large8K", smallSize: 0, largeSize: 8 << 10, largeEvery: 1},
		{name: "Mixed20pctLarge", smallSize: 32, largeSize: 8 << 10, largeEvery: 5},
	}
	cases := []struct {
		name  string
		split bool
	}{
		{name: "Combined", split: false},
		{name: "Split", split: true},
	}

	for _, mix := range mixes {
		for _, tc := range cases {
			b.Run(fmt.Sprintf("%s/%s", mix.name, tc.name), func(b *testing.B) {
				benchmarkSplitValueLogWrite(b, mix, tc.split)
			})
		}
	}
}

func benchmarkSplitValueLogWrite(b *testing.B, mix benchWriteMix, split bool) {
	tmpDir, err := os.MkdirTemp("", "treedb-bench-split-wal-*")
	if err != nil {
		b.Fatal(err)
	}
	opts := Options{
		Dir:                              tmpDir,
		Mode:                             ModeCached,
		FlushThreshold:                   256 << 20,
		MemtableValueLogPointers:         true,
		SplitValueLog:                    split,
		BackgroundCheckpointInterval:     -1,
		BackgroundCheckpointIdleDuration: -1,
		MaxWALBytes:                      -1,
		AllowUnsafe:                      true,
	}
	db, err := Open(opts)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = db.Close()
		_ = os.RemoveAll(tmpDir)
	})

	const keyCount = 8192
	keys := make([][]byte, keyCount)
	vals := make([][]byte, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%06d", i))
		size := mix.smallSize
		if mix.largeEvery > 0 && i%mix.largeEvery == 0 {
			size = mix.largeSize
		}
		if size > 0 {
			vals[i] = bytes.Repeat([]byte{byte(i)}, size)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % keyCount
		if err := db.Set(keys[idx], vals[idx]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}
