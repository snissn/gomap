package collections

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Compare the default insertion plus fresh journal boundary with the operation
// receipt, on the same original general collection API and real new-key writes.
// Each timed operation includes durable completion. Checked original/fresh Close,
// exact first/last readback and final checkpoint stay outside the allocation timer.
func BenchmarkNativeBSONJournalReceiptInsert(b *testing.B) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		for _, receipt := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/receipt=%t", profile, receipt), func(b *testing.B) {
				for _, batch := range []int{1, 256} {
					b.Run(fmt.Sprintf("batch%d", batch), func(b *testing.B) {
						dir := b.TempDir()
						opts := treedb.OptionsFor(profile, dir)
						opts.MemtableMode = "cow_btree"
						owner, err := treedb.Open(opts)
						if err != nil {
							b.Fatal(err)
						}
						ownerOpen := true
						b.Cleanup(func() {
							if ownerOpen {
								if e := owner.Close(); e != nil {
									b.Error(e)
								}
							}
						})
						backend, err := owner.NativeSemanticBackend()
						if err != nil {
							b.Fatal(err)
						}
						manager := NewCollectionManager(backend)
						managerOpen := true
						b.Cleanup(func() {
							if managerOpen {
								if e := manager.Close(); e != nil {
									b.Error(e)
								}
							}
						})
						if _, err = manager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}); err != nil {
							b.Fatal(err)
						}
						col, err := manager.OpenCollection("docs")
						if err != nil {
							b.Fatal(err)
						}
						if col.nativeJSON == nil || !manager.UsesNativeCommandWALApply() {
							b.Fatal("native owner inactive")
						}
						doc, err := bson.Marshal(bson.D{{Key: "payload", Value: bytes.Repeat([]byte("x"), 1000)}})
						if err != nil {
							b.Fatal(err)
						}
						// Fixed request input backing is prepared once, outside measurement, while
						// TreeDB's owned staging/response allocations remain inside the timed call.
						ids, docs := make([][]byte, batch), make([][]byte, batch)
						arena := make([]byte, 8*batch)
						for i := range batch {
							ids[i] = arena[i*8 : (i+1)*8]
							docs[i] = doc
						}
						before := backend.CommandWALNextLSN()
						b.ReportAllocs()
						b.SetBytes(int64(batch * len(doc)))
						b.ResetTimer()
						started := time.Now()
						for n := range b.N {
							for i := range batch {
								binary.BigEndian.PutUint64(ids[i], uint64(n*batch+i+1))
							}
							if receipt {
								got, completed, err := col.InsertBatchValidatedBSONWithJournalReceipt(ids, docs)
								if err != nil || len(got) != batch || !completed.Completed() {
									b.Fatalf("receipt ACKs=%d expected=%d completion=%+v err=%v", len(got), batch, completed, err)
								}
							} else {
								if got, err := col.InsertBatchValidatedBSON(ids, docs); err != nil || len(got) != batch {
									b.Fatalf("ACKs=%d expected=%d err=%v", len(got), batch, err)
								}
								if physical, err := manager.SyncForStandaloneWriteConcern(); !physical || err != nil {
									b.Fatalf("fresh default boundary=%t/%v", physical, err)
								}
							}
						}
						elapsed := time.Since(started)
						b.StopTimer()
						b.ReportMetric(float64(b.N*batch)/elapsed.Seconds(), "documents/s")
						if backend.CommandWALNextLSN() != before+uint64(b.N) {
							b.Fatal("unexpected command assignment count")
						}
						if col.LastInsertStats().Documents != batch {
							b.Fatal("unexpected native insertion count")
						}
						verify := func(c *Collection) {
							b.Helper()
							for _, n := range []uint64{1, uint64(b.N * batch)} {
								var key [8]byte
								binary.BigEndian.PutUint64(key[:], n)
								got, e := c.Get(key[:])
								if e != nil || !bytes.Equal(got, doc) {
									b.Fatalf("exact value %d err=%v", n, e)
								}
							}
						}
						verify(col)
						if err = owner.Checkpoint(); err != nil {
							b.Fatal(err)
						}
						verify(col)
						managerOpen = false
						if err = manager.Close(); err != nil {
							b.Fatal(err)
						}
						ownerOpen = false
						if err = owner.Close(); err != nil {
							b.Fatal(err)
						}
						fresh, err := treedb.Open(opts)
						if err != nil {
							b.Fatal(err)
						}
						defer func() {
							if e := fresh.Close(); e != nil {
								b.Error(e)
							}
						}()
						freshBackend, err := fresh.NativeSemanticBackend()
						if err != nil {
							b.Fatal(err)
						}
						freshManager := NewCollectionManager(freshBackend)
						defer func() {
							if e := freshManager.Close(); e != nil {
								b.Error(e)
							}
						}()
						freshCol, err := freshManager.OpenCollection("docs")
						if err != nil {
							b.Fatal(err)
						}
						verify(freshCol)
					})
				}
			})
		}
	}
}
