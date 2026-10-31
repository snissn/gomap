package collections

import (
	"bytes"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Returned-ID callers share the marked no-index rejection seam with the wire
// omit-result path. Preserve their underlying conflict and result-ID contract.
func TestNoIndexReturnedIDsConflictCompatibility(t *testing.T) {
	for _, format := range []DocumentFormat{DocumentFormatJSON, DocumentFormatBSON} {
		for _, conflict := range []string{"same_new_id", "buffered_existing", "persisted_existing"} {
			t.Run(string(format)+"/"+conflict, func(t *testing.T) {
				d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), Durability: backenddb.DurabilityWALOffRelaxed})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := d.Close(); err != nil {
						t.Errorf("close: %v", err)
					}
				})
				mgr := NewCollectionManager(d)
				if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users", Options: CollectionOptions{DocumentFormat: format}}); err != nil {
					t.Fatal(err)
				}
				col, err := mgr.OpenCollection("users")
				if err != nil {
					t.Fatal(err)
				}
				if col.nativeJSON != nil || !col.canBufferNoIndexInsertBatchAck() || col.commandWALActive(nil) {
					t.Fatal("fixture requires legacy no-index WAL-off buffering")
				}
				insert := col.InsertBatch
				if format == DocumentFormatBSON {
					insert = col.InsertBatchValidatedBSON
				}
				document := func(value string) []byte {
					if format == DocumentFormatBSON {
						return mustBSONCollectionDocument(t, bson.D{{Key: "value", Value: value}})
					}
					return []byte(`{"value":"` + value + `"}`)
				}
				seed := document("seed")
				if conflict != "same_new_id" {
					gotIDs, err := insert([][]byte{[]byte("seed")}, [][]byte{seed})
					if err != nil || len(gotIDs) != 1 || !bytes.Equal(gotIDs[0], []byte("seed")) {
						t.Fatalf("seed returned IDs=%q err=%v", gotIDs, err)
					}
					if conflict == "persisted_existing" {
						if err := d.Checkpoint(); err != nil {
							t.Fatal(err)
						}
					}
				}
				col.writeDomain.mu.RLock()
				buffered := col.writeDomain.count
				col.writeDomain.mu.RUnlock()
				if conflict == "buffered_existing" && buffered != 1 || conflict == "persisted_existing" && buffered != 0 {
					t.Fatalf("wrong conflict boundary: %s buffered=%d", conflict, buffered)
				}
				ids := [][]byte{[]byte("seed"), []byte("valid")}
				want := ErrDocumentExists
				if conflict == "same_new_id" {
					ids = [][]byte{[]byte("valid"), []byte("valid")}
					want = ErrDuplicateDocumentID
				}
				gotIDs, err := insert(ids, [][]byte{document("replacement"), document("valid")})
				if gotIDs != nil || !errors.Is(err, want) || !errors.Is(err, ErrMutationNotAccepted) || errors.Is(err, ErrCommitAmbiguous) || errors.Is(err, ErrRecoveryRequired) || errors.Is(err, backenddb.ErrRecoveryRequired) || backenddb.CommitPublicationAccepted(err) {
					t.Fatalf("rejected returned IDs=%q err=%v want underlying %v and private-rejection marker", gotIDs, err, want)
				}
				if got, err := col.Get([]byte("valid")); err != nil || got != nil {
					t.Fatalf("rejected valid sibling visible: %x err=%v", got, err)
				}
				if conflict != "same_new_id" {
					if got, err := col.Get([]byte("seed")); err != nil || !bytes.Equal(got, seed) {
						t.Fatalf("existing document changed: %x err=%v", got, err)
					}
				}
				accepted := document("accepted")
				gotIDs, err = insert([][]byte{[]byte("valid")}, [][]byte{accepted})
				if err != nil || len(gotIDs) != 1 || !bytes.Equal(gotIDs[0], []byte("valid")) {
					t.Fatalf("subsequent successful returned IDs=%q err=%v", gotIDs, err)
				}
				if got, err := col.Get(gotIDs[0]); err != nil || !bytes.Equal(got, accepted) {
					t.Fatalf("successful returned ID document=%x err=%v", got, err)
				}
			})
		}
	}
}
