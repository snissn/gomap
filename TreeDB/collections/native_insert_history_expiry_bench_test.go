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

// Both revisions use this identical public native owner and request body. Setup,
// checkpoint and checked Close/reopen verification are outside request timing;
// the durable command-WAL acknowledgement remains inside every measured request.
func BenchmarkNativeOrdinaryInsertRequest(b *testing.B) {
	for _, batch := range []int{1, 256} {
		b.Run(fmt.Sprintf("batch%d", batch), func(b *testing.B) {
			dir := b.TempDir()
			opts := nativeTypedGroupOptions(dir)
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
				if got, err := col.InsertBatchValidatedBSON(ids, docs); err != nil || len(got) != batch {
					b.Fatalf("ACKs=%d expected=%d err=%v", len(got), batch, err)
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
}
