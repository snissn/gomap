package nativewire

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func TestInsertBatchCombinerNoIndexWALOffConflictOutcomes(t *testing.T) {
	for _, conflict := range []string{"same_new_id", "buffered_existing", "persisted_existing"} {
		t.Run(conflict, func(t *testing.T) {
			dir := t.TempDir()
			options := backenddb.Options{Dir: dir, Durability: backenddb.DurabilityWALOffRelaxed}
			db, err := backenddb.Open(options)
			if err != nil {
				t.Fatal(err)
			}
			mgr := collections.NewCollectionManager(db)
			server := NewServer(ServerOptions{Collections: mgr, Backend: db})
			closed := false
			closeOriginal := func() {
				if !closed {
					closed = true
					if err := errors.Join(server.Close(), db.Close()); err != nil {
						t.Errorf("close original: %v", err)
					}
				}
			}
			t.Cleanup(closeOriginal)
			if db.CommandWALEnabled() || db.DurabilityMode() != backenddb.DurabilityWALOffRelaxed {
				t.Fatal("fixture requires legacy WAL-off buffering")
			}
			if _, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}); err != nil {
				t.Fatal(err)
			}
			col, err := mgr.OpenCollection("users")
			if err != nil {
				t.Fatal(err)
			}

			ids := []string{"same", "same"}
			docs := [][]byte{insertBatchCombinerBSONDoc(t, "same", "first@example.com"), insertBatchCombinerBSONDoc(t, "same", "second@example.com")}
			want := map[string][]byte{"same": docs[0]}
			if conflict != "same_new_id" {
				seed := insertBatchCombinerBSONDoc(t, "seed", "seed@example.com")
				if _, err := col.InsertBatchValidatedBSON([][]byte{[]byte("seed")}, [][]byte{seed}); err != nil {
					t.Fatal(err)
				}
				if conflict == "persisted_existing" {
					if err := db.Checkpoint(); err != nil {
						t.Fatal(err)
					}
				}
				ids = []string{"seed", "valid"}
				docs = [][]byte{insertBatchCombinerBSONDoc(t, "seed", "replacement@example.com"), insertBatchCombinerBSONDoc(t, "valid", "valid@example.com")}
				want = map[string][]byte{"seed": seed, "valid": docs[1]}
			}

			items := make([]*nativewireInsertBatchCombineItem, len(ids))
			for i, id := range ids {
				items[i] = &nativewireInsertBatchCombineItem{req: insertBatchCombinerFastRequest(t, col, id, docs[i]), done: make(chan nativewireInsertBatchCombineResult, 1)}
			}
			// Supply the exact group directly; scheduling cannot turn this into
			// two unrelated single-request calls.
			applyInsertBatchCombineItems(server, items)
			for i, item := range items {
				var result nativewireInsertBatchCombineResult
				select {
				case result = <-item.done:
				default:
					t.Fatalf("member %d not completed", i)
				}
				wantSuccess := (conflict == "same_new_id" && i == 0) || (conflict != "same_new_id" && i == 1)
				if wantSuccess {
					if result.err != nil || result.actualAck != iwire.AckVisible {
						t.Fatalf("independent member %d rejected: err=%v ack=%v", i, result.err, result.actualAck)
					}
				} else if !errors.Is(result.err, collections.ErrDocumentExists) || result.actualAck != 0 || result.hasCatalogVersion {
					t.Fatalf("conflicting member %d: err=%v ack=%v catalog=%t", i, result.err, result.actualAck, result.hasCatalogVersion)
				}
			}
			if got := nativewireTestStatUint64(t, server, "insert_batch_combiner.fallback_requests_total"); got != 2 {
				t.Fatalf("fallback requests=%d want exactly 2", got)
			}
			assertRows := func(col *collections.Collection) {
				t.Helper()
				rows, truncated, err := col.ScanDocuments(10)
				if err != nil || truncated || len(rows) != len(want) {
					t.Fatalf("rows=%d truncated=%t err=%v want=%d", len(rows), truncated, err, len(want))
				}
				for id, document := range want {
					if got, err := col.Get([]byte(id)); err != nil || !bytes.Equal(got, document) {
						t.Fatalf("row %s=%x err=%v want=%x", id, got, err, document)
					}
				}
			}
			assertRows(col)
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			closeOriginal()
			reopened, err := backenddb.Open(options)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := reopened.Close(); err != nil {
					t.Errorf("close reopened: %v", err)
				}
			})
			col, err = collections.NewCollectionManager(reopened).OpenCollection("users")
			if err != nil {
				t.Fatal(err)
			}
			assertRows(col)
		})
	}
}
