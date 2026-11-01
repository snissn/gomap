package collections

import (
	"bytes"
	"fmt"
	"runtime"
	"strconv"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// This component benchmark uses the actual public native collection consumer
// with independently owned output. Cold read observations are process allocation
// deltas and include concurrent background activity; warm B/op and allocs/op use
// the benchmark framework. Setup, checkpoint and lifecycle are outside timing.
func BenchmarkNativeBlockValueWorkspace(b *testing.B) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed} {
		for _, parallel := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/parallel=%t", profile, parallel), func(b *testing.B) {
				opts := treedb.OptionsFor(profile, b.TempDir())
				opts.MemtableMode = "cow_btree"
				owner, err := treedb.Open(opts)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() {
					if err := owner.Close(); err != nil {
						b.Error(err)
					}
				})
				backend, err := owner.NativeSemanticBackend()
				if err != nil {
					b.Fatal(err)
				}
				manager := NewCollectionManager(backend)
				b.Cleanup(func() {
					if err := manager.Close(); err != nil {
						b.Error(err)
					}
				})
				if _, err := manager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}); err != nil {
					b.Fatal(err)
				}
				col, err := manager.OpenCollection("docs")
				if err != nil {
					b.Fatal(err)
				}
				doc, err := bson.Marshal(bson.D{{Key: "payload", Value: bson.Binary{Data: bytes.Repeat([]byte("default-native-block-value|"), 2048)}}})
				if err != nil {
					b.Fatal(err)
				}
				id := []byte("value")
				if _, _, err := col.InsertBatchValidatedBSONWithJournalReceipt([][]byte{id}, [][]byte{doc}); err != nil {
					b.Fatal(err)
				}
				if err := owner.Checkpoint(); err != nil {
					b.Fatal(err)
				}
				var coldBefore, coldAfter runtime.MemStats
				runtime.ReadMemStats(&coldBefore)
				started := time.Now()
				got, err := col.Get(id)
				coldDuration := time.Since(started)
				runtime.ReadMemStats(&coldAfter)
				if err != nil || !bytes.Equal(got, doc) || len(got) != cap(got) {
					b.Fatal("cold public owned Get", err)
				}
				read := func() error {
					got, err := col.Get(id)
					if err != nil {
						return err
					}
					if !bytes.Equal(got, doc) || len(got) != cap(got) {
						return fmt.Errorf("public owned Get differs")
					}
					return nil
				}
				b.ReportAllocs()
				b.SetBytes(int64(len(doc)))
				b.ResetTimer()
				if parallel {
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							if err := read(); err != nil {
								b.Error(err)
								return
							}
						}
					})
				} else {
					for range b.N {
						if err := read(); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(coldAfter.TotalAlloc-coldBefore.TotalAlloc), "cold-process-B")
				b.ReportMetric(float64(coldAfter.Mallocs-coldBefore.Mallocs), "cold-process-allocs")
				b.ReportMetric(float64(coldDuration.Nanoseconds()), "cold-ns")
				stats := owner.Stats()
				for _, metric := range []struct{ key, unit string }{{"reserved_bytes", "retained-charge-B"}, {"peak_bytes", "peak-charge-B"}} {
					value, err := strconv.ParseUint(stats["treedb.cache.cow."+metric.key], 10, 64)
					if err != nil {
						b.Fatal(err)
					}
					b.ReportMetric(float64(value), metric.unit)
				}
				// Replace the physical basis once outside timing, then verify a new
				// ordinary public read and checked original/fresh lifecycle.
				if err := owner.Checkpoint(); err != nil {
					b.Fatal(err)
				}
				if err := read(); err != nil {
					b.Fatal(err)
				}
				if err := manager.Close(); err != nil {
					b.Fatal(err)
				}
				if err := owner.Close(); err != nil {
					b.Fatal(err)
				}
				fresh, err := treedb.Open(opts)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() {
					if err := fresh.Close(); err != nil {
						b.Error(err)
					}
				})
				freshBackend, err := fresh.NativeSemanticBackend()
				if err != nil {
					b.Fatal(err)
				}
				freshManager := NewCollectionManager(freshBackend)
				b.Cleanup(func() {
					if err := freshManager.Close(); err != nil {
						b.Error(err)
					}
				})
				freshCol, err := freshManager.OpenCollection("docs")
				if err != nil {
					b.Fatal(err)
				}
				if got, err := freshCol.Get(id); err != nil || !bytes.Equal(got, doc) {
					b.Fatal("fresh public Get", err)
				}
				if err := freshManager.Close(); err != nil {
					b.Fatal(err)
				}
				if err := fresh.Close(); err != nil {
					b.Fatal(err)
				}
			})
		}
	}
}
