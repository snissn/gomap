package collections

import (
	"bytes"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNativeBSONSetJournalReceiptOwnerNoopAndNoWAL(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			owner, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			})
			backend, err := owner.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := NewCollectionManager(backend)
			t.Cleanup(func() {
				if err := manager.Close(); err != nil {
					t.Error(err)
				}
			})
			if _, err := manager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}); err != nil {
				t.Fatal(err)
			}
			col, err := manager.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			doc, err := bson.Marshal(bson.D{{Key: "n", Value: int32(1)}, {Key: "bin", Value: bson.Binary{Subtype: 0x80, Data: []byte{0, 255, 3}}}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := col.InsertBatchValidatedBSON([][]byte{[]byte("row")}, [][]byte{doc}); err != nil {
				t.Fatal(err)
			}
			_, value, err := bson.MarshalValue(int32(2))
			if err != nil {
				t.Fatal(err)
			}
			items := []BSONSetUpdateBatchItem{{DocumentID: []byte("row"), Fields: []BSONSetField{{Key: "n", Value: bson.RawValue{Type: bson.TypeInt32, Value: value}}}}}
			results, batched, completion, err := col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChangesWithJournalReceipt(items)
			if err != nil || !batched || len(results) != 1 || !results[0].Modified {
				t.Fatalf("update=%+v batched=%t receipt=%+v err=%v", results, batched, completion, err)
			}
			if profile == treedb.ProfileNoWALFast {
				if completion.Selected() {
					t.Fatal("NoWAL manufactured receipt")
				}
			} else if !completion.Selected() || !completion.Attempted() || !completion.Completed() || !backend.OwnsNativeJournalCompletion(completion) {
				t.Fatalf("missing original physical receipt: %+v", completion)
			}
			if results, batched, none, err := col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChangesWithJournalReceipt(items); err != nil || !batched || results[0].Modified || none.Selected() {
				t.Fatalf("noop borrowed completion=%+v/%v", none, err)
			}
			items[0].DocumentID = []byte("missing")
			if results, batched, none, err := col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChangesWithJournalReceipt(items); err != nil || !batched || results[0].Matched || none.Selected() {
				t.Fatalf("missing borrowed completion=%+v/%v", none, err)
			}
			want, err := bson.Marshal(bson.D{{Key: "n", Value: int32(2)}, {Key: "bin", Value: bson.Binary{Subtype: 0x80, Data: []byte{0, 255, 3}}}})
			if err != nil {
				t.Fatal(err)
			}
			if got, err := col.Get([]byte("row")); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("exact BSON=%x/%v", got, err)
			}
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			fresh, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := fresh.Close(); err != nil {
					t.Error(err)
				}
			}()
			freshBackend, err := fresh.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			if completion.Selected() && freshBackend.OwnsNativeJournalCompletion(completion) {
				t.Fatal("fresh owner accepted old receipt")
			}
			freshManager := NewCollectionManager(freshBackend)
			defer func() {
				if err := freshManager.Close(); err != nil {
					t.Error(err)
				}
			}()
			freshCol, err := freshManager.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			if got, err := freshCol.Get([]byte("row")); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("fresh BSON=%x/%v", got, err)
			}
		})
	}
}

func TestNativeBSONSetJournalReceiptTextFallback(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		t.Run(string(profile), func(t *testing.T) {
			owner, manager, col := openNativeTextPublicFormat(t, t.TempDir(), profile, true, DocumentFormatBSON)
			doc, err := bson.Marshal(bson.D{{Key: "tenant", Value: "one"}, {Key: "content", Value: "oldword"}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := col.InsertBatchValidatedBSON([][]byte{[]byte("row")}, [][]byte{doc}); err != nil {
				t.Fatal(err)
			}
			kind, value, err := bson.MarshalValue("newword")
			if err != nil {
				t.Fatal(err)
			}
			results, batched, none, err := col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChangesWithJournalReceipt([]BSONSetUpdateBatchItem{{DocumentID: []byte("row"), Fields: []BSONSetField{{Key: "content", Value: bson.RawValue{Type: kind, Value: value}}}}})
			if err != nil || !batched || len(results) != 1 || !results[0].Modified || none.Selected() {
				t.Fatalf("text fallback=%+v/%t/%+v/%v", results, batched, none, err)
			}
			requireNativeTextIDs(t, col, "newword", "row")
			requireNativeTextIDs(t, col, "oldword")
			if physical, err := manager.SyncForStandaloneWriteConcern(); !physical || err != nil {
				t.Fatalf("text fresh boundary=%t/%v", physical, err)
			}
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
