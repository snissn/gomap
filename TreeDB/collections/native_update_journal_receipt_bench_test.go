package collections

import (
	"bytes"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Same original owner and alternating real binary mutations. Ordinary nil
// callers include their fresh Sync; selected callers include their own wait.
func BenchmarkCollectionBSONSetJournalReceipt(b *testing.B) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		for _, receipt := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/receipt=%t", profile, receipt), func(b *testing.B) {
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
				var values [2]bson.RawValue
				var documents [2][]byte
				var requests [2][]BSONSetUpdateBatchItem
				for i := range values {
					v := bson.Binary{Subtype: 0x80, Data: bytes.Repeat([]byte{byte(i + 1)}, 1000)}
					kind, raw, err := bson.MarshalValue(v)
					if err != nil {
						b.Fatal(err)
					}
					values[i] = bson.RawValue{Type: kind, Value: raw}
					documents[i], err = bson.Marshal(bson.D{{Key: "payload", Value: v}})
					if err != nil {
						b.Fatal(err)
					}
					requests[i] = []BSONSetUpdateBatchItem{{DocumentID: []byte("row"), Fields: []BSONSetField{{Key: "payload", Value: values[i]}}}}
				}
				if _, err := col.InsertBatchValidatedBSON([][]byte{[]byte("row")}, [][]byte{documents[1]}); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for n := range b.N {
					if receipt {
						results, batched, completion, err := col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChangesWithJournalReceipt(requests[n%2])
						if err != nil || !batched || len(results) != 1 || !results[0].Modified || !completion.Completed() {
							b.Fatalf("receipt mutation=%+v/%t/%+v/%v", results, batched, completion, err)
						}
					} else {
						results, batched, err := col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges(requests[n%2])
						if err != nil || !batched || len(results) != 1 || !results[0].Modified {
							b.Fatalf("ordinary mutation=%+v/%t/%v", results, batched, err)
						}
						if physical, err := manager.SyncForStandaloneWriteConcern(); !physical || err != nil {
							b.Fatalf("fresh=%t/%v", physical, err)
						}
					}
				}
				b.StopTimer()
				want := documents[1]
				if b.N > 0 {
					want = documents[(b.N-1)%2]
				}
				verify := func(c *Collection) {
					b.Helper()
					if got, err := c.Get([]byte("row")); err != nil || !bytes.Equal(got, want) {
						b.Fatalf("exact BSON=%x/%v", got, err)
					}
				}
				verify(col)
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
				defer func() {
					if err := fresh.Close(); err != nil {
						b.Error(err)
					}
				}()
				freshBackend, err := fresh.NativeSemanticBackend()
				if err != nil {
					b.Fatal(err)
				}
				freshManager := NewCollectionManager(freshBackend)
				defer func() {
					if err := freshManager.Close(); err != nil {
						b.Error(err)
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
}
