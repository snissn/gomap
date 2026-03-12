package batch

import (
	"testing"
)

func BenchmarkBatchSet_ArenaCopy(b *testing.B) {
	b.ReportAllocs()

	bt := Acquire(nil, 1<<20)
	defer Release(bt)
	bt.Reserve(4096)

	key := make([]byte, 32)
	val := make([]byte, 128)
	for i := range key {
		key[i] = byte(i)
	}
	for i := range val {
		val[i] = byte(i)
	}

	const resetEvery = 4096
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key[0] = byte(i)
		if err := bt.Set(key, val); err != nil {
			b.Fatalf("set: %v", err)
		}
		if (i+1)%resetEvery == 0 {
			bt.Reset()
		}
	}
}

