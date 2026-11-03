package collections

import (
	"bytes"
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// SOURCE-ONLY, UNRUN. Requires the proposed prepared-update finisher extraction.
// Uses the original real owner and the existing conditional transaction wrapper;
// no fabricated successful publication or manual receipt invalidation is used.
func TestNativeBSONSetJournalReceiptCheckedCleanupInvalidatesSuccess(t *testing.T) {
	_, backend, _, col := historyExpiryFixture(t, &CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}})
	doc, err := bson.Marshal(bson.D{{Key: "n", Value: int32(1)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatchValidatedBSON([][]byte{[]byte("cleanup")}, [][]byte{doc}); err != nil {
		t.Fatal(err)
	}
	_, value, err := bson.MarshalValue(int32(2))
	if err != nil {
		t.Fatal(err)
	}
	items := []BSONSetUpdateBatchItem{{DocumentID: []byte("cleanup"), Fields: []BSONSetField{{Key: "n", Value: bson.RawValue{Type: bson.TypeInt32, Value: value}}}}}
	unlock, _, err := col.prepareNativeProducerMutation()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { unlock() }()
	tx, err := col.nativeJSON.beginPrepared("docs")
	if err != nil {
		t.Fatal(err)
	}
	// Keep a checked fallback for an unexpected test panic; the finisher must
	// itself consume the original Close error before returning the receipt.
	t.Cleanup(func() {
		if err := tx.Close(); err != nil {
			t.Errorf("unconsumed original transaction Close: %v", err)
		}
	})
	failure := errors.New("original update conditional transaction Close failed")
	tx.core = &journalCleanupTxn{NativeConditionalTxn: tx.core, failure: failure}
	request := nativeOperationJournal{requested: true}
	_, complete, err := col.finishPreparedNativeUpdateBatchWithJournal(tx, nil, updateBatchModeNoSecondaryUniqueIndexChanges, items, &request)
	if !errors.Is(err, failure) || !request.completion.Selected() || !request.completion.Attempted() || request.completion.Completed() || !backend.OwnsNativeJournalCompletion(request.completion) {
		t.Fatalf("cleanup lost selected original update error or exported success: completion=%+v complete=%t err=%v", request.completion, complete, err)
	}
	if tx.core != nil {
		t.Fatal("finisher did not close original conditional transaction")
	}
	unlock()
	unlock = func() {}
	want, err := bson.Marshal(bson.D{{Key: "n", Value: int32(2)}})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := col.Get([]byte("cleanup")); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("actual accepted update bytes=%x want=%x err=%v", got, want, err)
	}
}
