package collections

import (
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Compose the unchanged genuine-eviction helper with the receipt-enabled API.
// This requires success after one clean reconstruction; it never accepts an
// error as success or alters the original helper's assignment/lifecycle checks.
func TestNativeBSONJournalReceiptGenuineHistoryExpiry(t *testing.T) {
	id := []byte("original-request")
	document, err := bson.Marshal(bson.D{{Key: "marker", Value: "exact acknowledged contents"}})
	if err != nil {
		t.Fatal(err)
	}
	testNativeOrdinaryInsertHistoryExpiry(t,
		&CollectionMeta{Name: "target", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}, id, document,
		func(target *Collection) ([][]byte, CollectionInsertStats, error) {
			ids, completion, err := target.InsertBatchValidatedBSONWithJournalReceipt([][]byte{id}, [][]byte{document})
			owned := target.nativeJSON.db.OwnsNativeJournalCompletion(completion)
			// This callback runs on the helper's producer goroutine. Log its actual
			// receipt, then return failures; never call Fatal/FailNow from here.
			t.Logf("receipt insert: selected=%t attempted=%t completed=%t assigned_lsn=%d original_owner=%t returned_ids=%d err=%v",
				completion.Selected(), completion.Attempted(), completion.Completed(), completion.AssignedLSN(), owned, len(ids), err)
			if !completion.Selected() || !owned {
				return ids, CollectionInsertStats{}, fmt.Errorf("receipt fixture did not authenticate its original owner: selected=%t owned=%t original_error=%v", completion.Selected(), owned, err)
			}
			if err != nil {
				return ids, CollectionInsertStats{}, err
			}
			if !completion.Attempted() || !completion.Completed() || completion.AssignedLSN() == 0 {
				return ids, CollectionInsertStats{}, fmt.Errorf("receipt insert lacks its assigned physical completion: attempted=%t completed=%t assigned_lsn=%d", completion.Attempted(), completion.Completed(), completion.AssignedLSN())
			}
			return ids, CollectionInsertStats{}, nil
		}, nil)
}
