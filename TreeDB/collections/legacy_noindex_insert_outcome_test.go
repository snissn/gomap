package collections

import (
	"bytes"
	"errors"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNoIndexBufferedInsertConflictOutcomeBoundary(t *testing.T) {
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
				if !col.canBufferNoIndexInsertBatchAck() || col.commandWALActive(nil) {
					t.Fatal("fixture must enter no-index WAL-off buffering")
				}
				document := func(value string) []byte {
					if format == DocumentFormatBSON {
						return mustBSONCollectionDocument(t, bson.D{{Key: "value", Value: value}})
					}
					return []byte(`{"value":"` + value + `"}`)
				}
				seed := document("seed")
				if conflict != "same_new_id" {
					if err := col.NativewireInsertBatchNoResultIDs([][]byte{[]byte("seed")}, [][]byte{seed}, format == DocumentFormatBSON); err != nil {
						t.Fatal(err)
					}
					if conflict == "persisted_existing" {
						if err := d.Checkpoint(); err != nil {
							t.Fatal(err)
						}
					}
				}
				domain := col.writeDomain
				domain.mu.RLock()
				count, generation := domain.count, domain.writeGeneration
				domain.mu.RUnlock()
				if conflict == "buffered_existing" && count != 1 || conflict == "persisted_existing" && count != 0 {
					t.Fatalf("wrong seed boundary: conflict=%s buffered=%d", conflict, count)
				}
				before, ok := d.StateToken()
				if !ok {
					t.Fatal("missing state token")
				}
				ids := [][]byte{[]byte("seed"), []byte("valid")}
				want := ErrDocumentExists
				if conflict == "same_new_id" {
					ids = [][]byte{[]byte("valid"), []byte("valid")}
					want = ErrDuplicateDocumentID
				}
				var events atomic.Int32
				restore := durabilitycut.Install(func(durabilitycut.Event) error { events.Add(1); return nil })
				restored := false
				defer func() {
					if !restored {
						restore()
					}
				}()
				err = col.NativewireInsertBatchNoResultIDs(ids, [][]byte{document("replacement"), document("valid")}, format == DocumentFormatBSON)
				restore()
				restored = true
				if !errors.Is(err, want) || !errors.Is(err, ErrMutationNotAccepted) || errors.Is(err, ErrCommitAmbiguous) || errors.Is(err, ErrRecoveryRequired) || errors.Is(err, backenddb.ErrRecoveryRequired) || backenddb.CommitPublicationAccepted(err) {
					t.Fatalf("conflict=%v want not-accepted %v without publication/ambiguity", err, want)
				}
				after, ok := d.StateToken()
				if !ok || after != before || events.Load() != 0 {
					t.Fatalf("group rejection crossed publication: state=%+v before=%+v events=%d token=%t", after, before, events.Load(), ok)
				}
				domain.mu.RLock()
				unchanged := domain.count == count && domain.writeGeneration == generation
				domain.mu.RUnlock()
				if !unchanged {
					t.Fatal("rejected group changed buffered document count/generation")
				}
				if got, err := col.Get([]byte("valid")); err != nil || got != nil {
					t.Fatalf("rejected group installed a document: %x err=%v", got, err)
				}
				if conflict != "same_new_id" {
					if got, err := col.Get([]byte("seed")); err != nil || !bytes.Equal(got, seed) {
						t.Fatalf("existing document changed: %x err=%v", got, err)
					}
				}
			})
		}
	}
}
