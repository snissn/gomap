package batch

import (
	"encoding/binary"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func benchmarkBatchSetViewRevisionOverhead(b *testing.B, withRevision bool) {
	const keyCount = 1024
	keys := make([][]byte, keyCount)
	for i := range keys {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		keys[i] = key
	}
	value := []byte("0123456789abcdef")

	bt := New(newMapValueReader(), page.DefaultInlineThreshold)
	bt.Reserve(keyCount)
	b.Cleanup(func() { _ = bt.Close() })

	b.ReportAllocs()
	b.SetBytes(int64(len(value)))
	b.ResetTimer()
	for n := 0; n < b.N; {
		bt.Reset()
		for i := 0; i < keyCount && n < b.N; i++ {
			var err error
			if withRevision {
				err = bt.SetViewWithRevision(keys[i], value, page.EntryRevision(n+1))
			} else {
				err = bt.SetView(keys[i], value)
			}
			if err != nil {
				b.Fatal(err)
			}
			n++
		}
	}
}

func BenchmarkBatchSetViewRevisionOverhead(b *testing.B) {
	b.Run("legacy", func(b *testing.B) {
		benchmarkBatchSetViewRevisionOverhead(b, false)
	})
	b.Run("revision", func(b *testing.B) {
		benchmarkBatchSetViewRevisionOverhead(b, true)
	})
}

func benchmarkBatchAppendViewTrustedSortedUniqueRevisionOverhead(b *testing.B, withRevision bool) {
	const keyCount = 1024
	keys := make([][]byte, keyCount)
	for i := range keys {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		keys[i] = key
	}
	value := []byte("0123456789abcdef")

	bt := New(newMapValueReader(), page.DefaultInlineThreshold)
	bt.Reserve(keyCount)
	b.Cleanup(func() { _ = bt.Close() })

	b.ReportAllocs()
	b.SetBytes(int64(len(value)))
	b.ResetTimer()
	for n := 0; n < b.N; {
		bt.Reset()
		for i := 0; i < keyCount && n < b.N; i++ {
			var err error
			if withRevision {
				err = bt.AppendViewTrustedSortedUniqueWithRevision(keys[i], value, page.EntryRevision(n+1))
			} else {
				err = bt.AppendViewTrustedSortedUnique(keys[i], value)
			}
			if err != nil {
				b.Fatal(err)
			}
			n++
		}
	}
}

func BenchmarkBatchAppendViewTrustedSortedUniqueRevisionOverhead(b *testing.B) {
	b.Run("legacy", func(b *testing.B) {
		benchmarkBatchAppendViewTrustedSortedUniqueRevisionOverhead(b, false)
	})
	b.Run("revision", func(b *testing.B) {
		benchmarkBatchAppendViewTrustedSortedUniqueRevisionOverhead(b, true)
	})
}
