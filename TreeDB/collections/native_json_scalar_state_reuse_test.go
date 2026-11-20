package collections

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func openNativeScalarStateReuse(t *testing.T, format DocumentFormat, multiKey bool) *Collection {
	t.Helper()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	c, _, _ := createNativeScalarStateReuse(t, opts, format, multiKey)
	return c
}

func openNativeScalarStateReuseOwner(t *testing.T, opts treedb.Options) (*treedb.DB, *CollectionManager, func()) {
	t.Helper()
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	var manager *CollectionManager
	closed := false
	closeOwner := func() {
		t.Helper()
		if closed {
			return
		}
		closed = true
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(closeOwner)
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = NewCollectionManager(backend)
	return owner, manager, closeOwner
}

func createNativeScalarStateReuse(t *testing.T, opts treedb.Options, format DocumentFormat, multiKey bool) (*Collection, *treedb.DB, func()) {
	t.Helper()
	owner, manager, closeOwner := openNativeScalarStateReuseOwner(t, opts)
	meta := &CollectionMeta{Name: "scalar_reuse", Options: CollectionOptions{DocumentFormat: format}, Indexes: []IndexDefinition{
		{Name: "tenant", Field: "meta.tenant", ValueType: IndexValueString, Unique: !multiKey, MultiKey: multiKey},
		{Name: "rank", Field: "meta.rank", ValueType: IndexValueInt64},
		{Name: "optional", Field: "meta.optional", ValueType: IndexValueString},
	}}
	if _, err := manager.CreateCollection(meta); err != nil {
		t.Fatal(err)
	}
	c, err := manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	return c, owner, closeOwner
}

func nativeScalarStateReuseDocument(t *testing.T, format DocumentFormat, tenant string, rank int64) []byte {
	t.Helper()
	raw := []byte(fmt.Sprintf(`{"meta":{"tenant":%q,"rank":%d},"irrelevant":{"nested":[1,2,3]}}`, tenant, rank))
	switch format {
	case DocumentFormatJSON:
		return raw
	case DocumentFormatBSON:
		value, err := bson.Marshal(bson.D{{Key: "meta", Value: bson.D{{Key: "tenant", Value: tenant}, {Key: "rank", Value: rank}}}})
		if err != nil {
			t.Fatal(err)
		}
		return value
	case DocumentFormatTemplateV1:
		value, err := EncodeTemplateV1DocumentJSON(raw)
		if err != nil {
			t.Fatal(err)
		}
		return value
	default:
		t.Fatalf("unsupported test format %q", format)
		return nil
	}
}

func requireNativeScalarStateReuseIndex(t *testing.T, c *Collection, index string, value any, id string) {
	t.Helper()
	ids, err := c.FindByIndexValue(index, value)
	if err != nil || len(ids) != 1 || string(ids[0]) != id {
		t.Fatalf("index %s=%v: ids=%q err=%v want=%q", index, value, ids, err, id)
	}
}

// All APIs and counters used here predate the optimization, so this file can
// run alone on the original source. The JSON Commit counter is the causal red:
// scalar extraction has already succeeded at staging and must not run again
// for those same current documents while producing their final index effects.
func TestNativeJSONScalarStateReuse(t *testing.T) {
	for _, format := range []DocumentFormat{DocumentFormatJSON, DocumentFormatBSON, DocumentFormatTemplateV1} {
		t.Run(string(format), func(t *testing.T) {
			c := openNativeScalarStateReuse(t, format, false)
			ids := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
			docs := [][]byte{nativeScalarStateReuseDocument(t, format, "first", 1), nativeScalarStateReuseDocument(t, format, "second", 2), nativeScalarStateReuseDocument(t, format, "third", 3)}
			txn, err := c.nativeJSON.Begin(c.name)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := txn.Close(); err != nil {
					t.Error(err)
				}
			})
			before := workstats.Read().IndexedJSON.ScalarRows
			if err := txn.InsertBatch(ids, docs); err != nil {
				t.Fatal(err)
			}
			staged := workstats.Read().IndexedJSON.ScalarRows
			if format == DocumentFormatJSON && staged-before != uint64(len(ids)) {
				t.Fatalf("staging scalar rows=%d want=%d", staged-before, len(ids))
			}
			// Each input and each validation arena has now been reused. Neither
			// can be the backing authority for a previously staged row's state.
			for _, doc := range docs {
				clear(doc)
			}
			if err := txn.Commit(); err != nil {
				t.Fatal(err)
			}
			if after := workstats.Read().IndexedJSON.ScalarRows; format == DocumentFormatJSON && after != staged {
				t.Fatalf("Commit repeated scalar extraction: staged=%d after=%d", staged, after)
			}
			for i, tenant := range []string{"first", "second", "third"} {
				requireNativeScalarStateReuseIndex(t, c, "tenant", tenant, string(ids[i]))
				requireNativeScalarStateReuseIndex(t, c, "rank", int64(i+1), string(ids[i]))
			}
			missing, err := c.FindByIndexValue("optional", "absent")
			if err != nil || len(missing) != 0 {
				t.Fatalf("missing field: %q %v", missing, err)
			}

			// Replacing an existing staged state and deleting/reinserting a row
			// must use only its latest state, with each callback invoked once.
			changes, err := c.nativeJSON.Begin(c.name)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := changes.Close(); err != nil {
					t.Error(err)
				}
			})
			calls := 0
			for i, tenant := range []string{"temporary", "final"} {
				replacement := nativeScalarStateReuseDocument(t, format, tenant, int64(10+i))
				result, err := changes.UpdateBatch([]UpdateBatchItem{{DocumentID: ids[0], Update: func([]byte) ([]byte, bool, error) { calls++; return replacement, true, nil }}})
				if err != nil || len(result) != 1 || !result[0].Modified {
					t.Fatalf("update: %+v %v", result, err)
				}
				clear(replacement)
			}
			if count, err := changes.DeleteBatch([][]byte{ids[1]}); err != nil || count != 1 {
				t.Fatalf("delete=%d %v", count, err)
			}
			if err := changes.InsertBatch([][]byte{ids[1]}, [][]byte{nativeScalarStateReuseDocument(t, format, "replacement", 20)}); err != nil {
				t.Fatal(err)
			}
			if count, err := changes.DeleteBatch([][]byte{ids[2]}); err != nil || count != 1 {
				t.Fatalf("delete=%d %v", count, err)
			}
			if err := changes.Commit(); err != nil {
				t.Fatal(err)
			}
			if calls != 2 {
				t.Fatalf("callback calls=%d want=2", calls)
			}
			requireNativeScalarStateReuseIndex(t, c, "tenant", "final", "a")
			requireNativeScalarStateReuseIndex(t, c, "rank", int64(11), "a")
			requireNativeScalarStateReuseIndex(t, c, "tenant", "replacement", "b")
			for _, tenant := range []string{"first", "second", "third", "temporary"} {
				got, err := c.FindByIndexValue("tenant", tenant)
				if err != nil || len(got) != 0 {
					t.Fatalf("obsolete state %q: %q %v", tenant, got, err)
				}
			}
			if doc, found, err := c.GetInto(ids[2], nil); err != nil || found || len(doc) != 0 {
				t.Fatalf("deleted document=%q found=%v err=%v", doc, found, err)
			}
		})
	}
}

func TestNativeJSONScalarStateReuseFallbackAndValidation(t *testing.T) {
	t.Run("multikey", func(t *testing.T) {
		c := openNativeScalarStateReuse(t, DocumentFormatJSON, true)
		doc := []byte(`{"meta":{"tenant":["a","b"],"rank":1}}`)
		if _, err := c.Insert([]byte("array"), doc); err != nil {
			t.Fatal(err)
		}
		for _, tenant := range []string{"a", "b"} {
			requireNativeScalarStateReuseIndex(t, c, "tenant", tenant, "array")
		}
	})
	t.Run("invalid_type", func(t *testing.T) {
		c := openNativeScalarStateReuse(t, DocumentFormatJSON, false)
		original := nativeScalarStateReuseDocument(t, DocumentFormatJSON, "valid", 1)
		if _, err := c.Insert([]byte("a"), original); err != nil {
			t.Fatal(err)
		}
		calls := 0
		_, _, err := c.Update([]byte("a"), func([]byte) ([]byte, bool, error) {
			calls++
			return []byte(`{"meta":{"tenant":"bad","rank":"wrong-type"}}`), true, nil
		})
		if err == nil || calls != 1 {
			t.Fatalf("invalid update calls=%d err=%v", calls, err)
		}
		got, err := c.Get([]byte("a"))
		if err != nil || !bytes.Equal(got, original) {
			t.Fatalf("invalid update changed document: %q %v", got, err)
		}
		requireNativeScalarStateReuseIndex(t, c, "tenant", "valid", "a")
	})
}

func TestNativeJSONScalarStateReuseAcrossGrowthAndReopen(t *testing.T) {
	for _, format := range []DocumentFormat{DocumentFormatBSON, DocumentFormatJSON} {
		t.Run(string(format), func(t *testing.T) {
			const batchSize = 137
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
			c, owner, closeOwner := createNativeScalarStateReuse(t, opts, format, false)
			txn, err := c.nativeJSON.Begin(c.name)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := txn.Close(); err != nil {
					t.Error(err)
				}
			})
			names := []string{"keep", "replace", "delete"}
			ids, docs := make([][]byte, len(names)), make([][]byte, len(names))
			for i, name := range names {
				ids[i] = []byte(name)
				docs[i] = nativeScalarStateReuseDocument(t, format, name, int64(i+1))
			}
			if err = txn.InsertBatch(ids, docs); err != nil {
				t.Fatal(err)
			}
			for i := range docs {
				clear(docs[i])
			}
			var finalDocs [][]byte
			var finalIDs [][]byte
			first := txn.bindings[0].rows.find(ids[0]).currentState
			for batch := range 2 {
				batchIDs, batchDocs := make([][]byte, batchSize), make([][]byte, batchSize)
				for i := range batchSize {
					n := batch*batchSize + i
					name := fmt.Sprintf("new%04d", n)
					batchIDs[i] = []byte(name)
					batchDocs[i] = nativeScalarStateReuseDocument(t, format, name+strings.Repeat("x", n%60), int64(n+100))
					finalDocs = append(finalDocs, bytes.Clone(batchDocs[i]))
					finalIDs = append(finalIDs, batchIDs[i])
				}
				if err = txn.InsertBatch(batchIDs, batchDocs); err != nil {
					t.Fatal(err)
				}
				for _, doc := range batchDocs {
					clear(doc)
				}
				if batch != 0 {
					continue
				}
				replacement := nativeScalarStateReuseDocument(t, format, "replaced", -1)
				result, err := txn.UpdateBatch([]UpdateBatchItem{{DocumentID: ids[1], Update: func([]byte) ([]byte, bool, error) {
					return replacement, true, nil
				}}})
				if err != nil || len(result) != 1 || !result[0].Modified {
					t.Fatalf("replace staged row: %+v %v", result, err)
				}
				clear(replacement)
				if count, err := txn.DeleteBatch([][]byte{ids[2]}); err != nil || count != 1 {
					t.Fatalf("delete staged row: %d %v", count, err)
				}
			}
			g := &txn.bindings[0]
			keep := g.rows.find(ids[0]).currentState
			if &keep[0] != &first[0] || &g.retainedScalar.states[0] == &first[0] {
				t.Fatal("mixed commands did not retain the first row across state backing growth")
			}
			first, keep, g = nil, nil, nil
			if err := txn.Commit(); err != nil {
				t.Fatal(err)
			}

			// Errors on the public entry neither publish a staged neighbor nor
			// overwrite the winning unique key.
			mustReject := func(conflictID []byte, tenant string, want error) {
				t.Helper()
				rejectedID := []byte("rejected")
				results, err := c.InsertBatch([][]byte{rejectedID, conflictID}, [][]byte{
					nativeScalarStateReuseDocument(t, format, "rejected", 10),
					nativeScalarStateReuseDocument(t, format, tenant, 11),
				})
				if len(results) != 0 || !errors.Is(err, want) {
					t.Fatalf("conflict: IDs=%q err=%v want=%v", results, err, want)
				}
				if got, found, err := c.GetInto(rejectedID, nil); err != nil || found || len(got) != 0 {
					t.Fatalf("conflict published its neighbor: doc=%q found=%t err=%v", got, found, err)
				}
			}
			mustReject(ids[0], "distinct", ErrDocumentExists)
			mustReject([]byte("conflict"), "keep", ErrUniqueIndexConflict)

			verify := func(c *Collection) {
				t.Helper()
				rows, truncated, err := c.FindByIndexRange("rank", IndexRangeOptions{
					Lower: IndexRangeBound{Unbounded: true},
					Upper: IndexRangeBound{Unbounded: true},
					Limit: len(finalIDs) + 2,
				})
				if err != nil || truncated || len(rows) != len(finalIDs)+2 || !bytes.Equal(rows[0], ids[1]) || !bytes.Equal(rows[1], ids[0]) {
					t.Fatalf("rank scan: rows=%q truncated=%t err=%v", rows, truncated, err)
				}
				for i, id := range finalIDs {
					if !bytes.Equal(id, rows[i+2]) {
						t.Fatalf("rank result [%d]=%q want=%q", i, rows[i+2], id)
					}
					got, err := c.Get(id)
					if err != nil || !bytes.Equal(got, finalDocs[i]) {
						t.Fatalf("readback %q: %q %v", id, got, err)
					}
				}
				requireNativeScalarStateReuseIndex(t, c, "tenant", "keep", "keep")
				requireNativeScalarStateReuseIndex(t, c, "tenant", "replaced", "replace")
				if missing, err := c.FindByIndexValue("optional", "absent"); err != nil || len(missing) != 0 {
					t.Fatalf("missing optional field: %q %v", missing, err)
				}
			}
			verify(c)
			if err := owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			closeOwner()
			_, manager, _ := openNativeScalarStateReuseOwner(t, opts)
			c, err = manager.OpenCollection("scalar_reuse")
			if err != nil {
				t.Fatal(err)
			}
			verify(c)
		})
	}
}
