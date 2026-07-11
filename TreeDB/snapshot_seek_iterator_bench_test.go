package treedb

import (
	"encoding/binary"
	"fmt"
	"testing"
)

var snapshotIteratorBenchSink byte

func BenchmarkSnapshotIteratorSeekNext(b *testing.B) {
	for _, keyCount := range []int{1 << 10, 1 << 14} {
		b.Run(fmt.Sprintf("keys=%d", keyCount), func(b *testing.B) {
			d, err := Open(Options{Dir: b.TempDir(), FlushThreshold: 1 << 30})
			if err != nil {
				b.Fatal(err)
			}
			defer d.Close()
			keys := make([][]byte, keyCount)
			for i := range keys {
				key := make([]byte, 8)
				binary.BigEndian.PutUint64(key, uint64(i*2))
				keys[i] = key
				if err := d.Set(key, key); err != nil {
					b.Fatal(err)
				}
			}
			if err := d.Checkpoint(); err != nil {
				b.Fatal(err)
			}
			snap := d.AcquireSnapshot()
			if snap == nil {
				b.Fatal("AcquireSnapshot=nil")
			}
			defer snap.Close()

			snapshotIt, err := snap.Iterator(nil, nil)
			if err != nil {
				b.Fatal(err)
			}
			defer snapshotIt.Close()
			publicIt, err := d.Iterator(nil, nil)
			if err != nil {
				b.Fatal(err)
			}
			defer publicIt.Close()

			b.Run("snapshot_seek", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					snapshotIt.Seek(keys[i%len(keys)])
					if snapshotIt.Valid() {
						snapshotIteratorBenchSink ^= snapshotIt.Key()[0]
					}
				}
			})
			b.Run("public_seek_baseline", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					publicIt.Seek(keys[i%len(keys)])
					if publicIt.Valid() {
						snapshotIteratorBenchSink ^= publicIt.Key()[0]
					}
				}
			})
			b.Run("snapshot_next", func(b *testing.B) {
				b.ReportAllocs()
				snapshotIt.Seek(nil)
				for i := 0; i < b.N; i++ {
					if !snapshotIt.Valid() {
						snapshotIt.Seek(nil)
					}
					snapshotIteratorBenchSink ^= snapshotIt.Key()[0]
					snapshotIt.Next()
				}
			})
			b.Run("public_next_baseline", func(b *testing.B) {
				b.ReportAllocs()
				publicIt.Seek(nil)
				for i := 0; i < b.N; i++ {
					if !publicIt.Valid() {
						publicIt.Seek(nil)
					}
					snapshotIteratorBenchSink ^= publicIt.Key()[0]
					publicIt.Next()
				}
			})
		})
	}
}
