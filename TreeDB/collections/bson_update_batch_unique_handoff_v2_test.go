package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionBSONOrderedV2UpdateBatchAllowsUniqueHandoffAndSwap(t *testing.T) {
	for _, mode := range []struct {
		name                      string
		disableIndexedWriteTables bool
	}{
		{name: "buffered"},
		{name: "durable", disableIndexedWriteTables: true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			dir := t.TempDir()
			d, err := backenddb.Open(backenddb.Options{Dir: dir})
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer func() { _ = d.Close() }()

			mgr := NewCollectionManager(d)
			if _, err := mgr.CreateCollection(&CollectionMeta{
				Name: "users",
				Options: CollectionOptions{
					DocumentFormat:               DocumentFormatBSON,
					DisableIndexedWriteMemtables: mode.disableIndexedWriteTables,
				},
				Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueBSONOrderedV2, Unique: true}},
			}); err != nil {
				t.Fatalf("create collection: %v", err)
			}
			col, err := mgr.OpenCollection("users")
			if err != nil {
				t.Fatalf("open collection: %v", err)
			}
			if _, err := col.InsertBatch(
				[][]byte{[]byte("u1"), []byte("u2")},
				[][]byte{
					bsonUniqueHandoffV2Doc(t, "u1", "a@example.com"),
					bsonUniqueHandoffV2Doc(t, "u2", "b@example.com"),
				},
			); err != nil {
				t.Fatalf("insert: %v", err)
			}

			results, err := col.UpdateBatch([]UpdateBatchItem{
				{DocumentID: []byte("u1"), Update: bsonUniqueHandoffV2Update(t, "u1", "c@example.com")},
				{DocumentID: []byte("u2"), Update: bsonUniqueHandoffV2Update(t, "u2", "a@example.com")},
			})
			if err != nil {
				t.Fatalf("handoff UpdateBatch: %v", err)
			}
			bsonUniqueHandoffV2RequireModified(t, results)
			bsonUniqueHandoffV2RequireOwner(t, col, "a@example.com", "u2")
			bsonUniqueHandoffV2RequireOwner(t, col, "c@example.com", "u1")

			results, err = col.UpdateBatch([]UpdateBatchItem{
				{DocumentID: []byte("u1"), Update: bsonUniqueHandoffV2Update(t, "u1", "a@example.com")},
				{DocumentID: []byte("u2"), Update: bsonUniqueHandoffV2Update(t, "u2", "c@example.com")},
			})
			if err != nil {
				t.Fatalf("swap UpdateBatch: %v", err)
			}
			bsonUniqueHandoffV2RequireModified(t, results)
			bsonUniqueHandoffV2RequireOwner(t, col, "a@example.com", "u1")
			bsonUniqueHandoffV2RequireOwner(t, col, "c@example.com", "u2")
			if mode.disableIndexedWriteTables {
				if err := d.Checkpoint(); err != nil {
					t.Fatalf("checkpoint: %v", err)
				}
				if err := d.Close(); err != nil {
					t.Fatalf("close: %v", err)
				}
				reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
				if err != nil {
					t.Fatalf("reopen: %v", err)
				}
				defer func() { _ = reopened.Close() }()
				reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
				if err != nil {
					t.Fatalf("open reopened collection: %v", err)
				}
				bsonUniqueHandoffV2RequireOwner(t, reopenedCol, "a@example.com", "u1")
				bsonUniqueHandoffV2RequireOwner(t, reopenedCol, "c@example.com", "u2")
			}
		})
	}
}

func bsonUniqueHandoffV2Doc(tb testing.TB, id, email string) []byte {
	tb.Helper()
	return mustBSONCollectionDocument(tb, bson.D{{Key: "_id", Value: id}, {Key: "email", Value: email}})
}

func bsonUniqueHandoffV2Update(tb testing.TB, id, email string) func([]byte) ([]byte, bool, error) {
	tb.Helper()
	return func([]byte) ([]byte, bool, error) {
		return bsonUniqueHandoffV2Doc(tb, id, email), true, nil
	}
}

func bsonUniqueHandoffV2RequireModified(tb testing.TB, results []UpdateBatchResult) {
	tb.Helper()
	if len(results) != 2 || !results[0].Matched || !results[0].Modified || !results[1].Matched || !results[1].Modified {
		tb.Fatalf("results=%+v want two matched modified rows", results)
	}
}

func bsonUniqueHandoffV2RequireOwner(tb testing.TB, col *Collection, email, want string) {
	tb.Helper()
	ids, err := col.FindByIndexValue("email", mustBSONRawValue(tb, email))
	if err != nil {
		tb.Fatalf("find %q: %v", email, err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte(want)) {
		tb.Fatalf("find %q ids=%q want [%s]", email, ids, want)
	}
}
