// typed_rows is a small native indexed-row application using durable command WAL.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

type user struct {
	ID, Email, City, Name, Bio string
	Score                      float64
}

// Strings have one authoritative typed-row owner. Other fields remain JSON.
func batch(users ...user) ([][]byte, [][]byte, []collections.TypedColumnBatch, error) {
	ids, retained := make([][]byte, len(users)), make([][]byte, len(users))
	columns := []collections.TypedColumnBatch{{Name: "email"}, {Name: "city"}, {Name: "name"}, {Name: "bio"}}
	for i, u := range users {
		ids[i] = []byte(u.ID)
		var err error
		retained[i], err = json.Marshal(map[string]any{"id": u.ID, "score": u.Score, "active": true, "optional": nil})
		if err != nil {
			return nil, nil, nil, err
		}
		for j, value := range []string{u.Email, u.City, u.Name, u.Bio} {
			columns[j].Strings = append(columns[j].Strings, value)
		}
	}
	return ids, retained, columns, nil
}

func matches(raw []byte, u user) error {
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		return err
	}
	want := map[string]any{"id": u.ID, "email": u.Email, "city": u.City, "name": u.Name, "bio": u.Bio,
		"score": u.Score, "active": true, "optional": nil}
	if !reflect.DeepEqual(got, want) {
		return fmt.Errorf("complete row mismatch: %s", raw)
	}
	return nil
}

func run(dir string) (err error) {
	// Existing DBs are never replaced by this example.
	if dir == "" {
		dir, err = os.MkdirTemp("", "treedb-typed-rows-")
	} else {
		err = os.MkdirAll(dir, 0700)
	}
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return errors.New("example requires a new or empty directory")
	}
	fmt.Println("DB directory:", dir)
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	db, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		return err
	}
	defer func() {
		if cleanup != nil {
			err = errors.Join(err, cleanup())
		}
	}()
	manager := collections.NewCollectionManager(db)
	meta := collections.CollectionMeta{Name: "users", Options: collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatJSON,
		ColumnStore: &collections.ColumnStoreConfig{Enabled: true,
			RetainedPayload:         collections.ColumnRetainedPayloadNonColumn,
			RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingJSON}},
		Indexes: []collections.IndexDefinition{
			{Name: "email", Field: "email", ValueType: collections.IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: collections.IndexValueString}}}
	for _, field := range []string{"email", "city", "name", "bio"} {
		meta.Options.ColumnStore.Columns = append(meta.Options.ColumnStore.Columns, collections.ColumnStoreColumn{
			Name: field, Path: field, ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerRowAsset})
	}
	if _, err = manager.CreateCollection(&meta); err != nil {
		return err
	}
	col, err := manager.OpenCollection(meta.Name)
	if err != nil {
		return err
	}
	ada := user{"ada", "ada@example.test", "London", "Ada", "Analytical engines", 7.25}
	grace := user{"grace", "grace@example.test", "Paris", "Grace", "Compilers", 8.5}
	ids, retained, columns, err := batch(ada, grace)
	if err != nil {
		return err
	}
	if _, _, err = col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		return err
	}
	// Opening drains pending writes. One view per worker; this view is not concurrent-safe.
	view, err := col.OpenCollectionReadView()
	if err != nil {
		return err
	}
	defer func() {
		if view != nil {
			err = errors.Join(err, view.Close())
		}
	}()
	captured, err := view.FetchDocumentsByID(ids, collections.DocumentFetchOptions{})
	if err != nil {
		return err
	}
	if len(captured.Results) != 2 || !captured.Results[0].Found || !captured.Results[1].Found {
		return errors.New("captured rows missing")
	}
	owned := captured.Results[0].Document
	if err = matches(owned, ada); err != nil {
		return err
	}
	// Generic UpdateBatch supplies a complete document; meta.* updates are metadata-only.
	ada.City, ada.Email, ada.Score = "Boston", "ada-new@example.test", 9.25
	results, err := col.UpdateBatch([]collections.UpdateBatchItem{{DocumentID: []byte(ada.ID), Update: func(current []byte) ([]byte, bool, error) {
		var row map[string]any
		if err := json.Unmarshal(current, &row); err != nil {
			return nil, false, err
		}
		row["city"], row["email"], row["score"] = ada.City, ada.Email, ada.Score
		raw, err := json.Marshal(row)
		return raw, true, err
	}}})
	if err != nil {
		return err
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		return errors.New("update did not match and modify Ada")
	}
	grace.City = "Boston"
	lin := user{"lin", "lin@example.test", "Tokyo", "Lin", "New row", 5}
	ids, retained, columns, err = batch(grace, lin)
	if err != nil {
		return err
	}
	if matched, e := col.UpsertTypedBatch(ids, retained, columns); e != nil {
		return e
	} else if matched != 1 {
		return fmt.Errorf("upsert matched %d existing rows", matched)
	}
	if deleted, e := col.DeleteBatch([][]byte{[]byte(lin.ID)}); e != nil {
		return e
	} else if deleted != 1 {
		return errors.New("delete did not remove Lin")
	}
	// Captured index selection and fetch share the same old view after mutations.
	var oldIDs [][]byte
	if err = view.VisitIndexValueIDs("city", "London", func(id []byte) error {
		oldIDs = append(oldIDs, bytes.Clone(id)) // Visitor IDs are borrowed for this call.
		return nil
	}); err != nil {
		return err
	}
	oldRows, err := view.FetchDocumentsByID(oldIDs, collections.DocumentFetchOptions{})
	if err != nil {
		return err
	}
	if len(oldRows.Results) != 1 || !oldRows.Results[0].Found || !bytes.Equal(oldRows.Results[0].Document, owned) {
		return errors.New("captured view changed after mutation")
	}
	if err = view.Close(); err != nil {
		return err
	}
	view = nil // Returned documents remain owned after close; release the reader pin.
	fmt.Println("Captured Ada:", string(owned))
	buffer := make([]byte, 0, 512)
	current, found, err := col.GetInto([]byte(ada.ID), buffer)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("current Ada missing")
	}
	if err = matches(current, ada); err != nil {
		return err
	}
	// Ordinary range selection and complete rows use one current publication view.
	rangeRows, truncated, err := col.FindDocumentsByIndexRange("city", collections.IndexRangeOptions{
		Lower: collections.IndexRangeBound{Value: "Boston", Inclusive: true},
		Upper: collections.IndexRangeBound{Value: "Boston", Inclusive: true}, Limit: 10})
	if err != nil {
		return err
	}
	if truncated || len(rangeRows) != 2 {
		return errors.New("Boston range did not return both complete rows")
	}
	for i, expected := range []user{ada, grace} {
		if err = matches(rangeRows[i].Document, expected); err != nil {
			return err
		}
		fmt.Println("Boston:", string(rangeRows[i].Document))
	}
	if err = manager.FlushAll(); err != nil {
		return err
	}
	if err = db.Checkpoint(); err != nil {
		return err
	}
	err = cleanup()
	cleanup = nil
	if err != nil {
		return err
	}
	db, cleanup, err = treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		return err
	}
	col, err = collections.NewCollectionManager(db).OpenCollection(meta.Name)
	if err != nil {
		return err
	}
	for _, expected := range []user{ada, grace} {
		raw, found, e := col.GetInto([]byte(expected.ID), nil)
		if e != nil {
			return e
		}
		if !found {
			return fmt.Errorf("reopened row %s missing", expected.ID)
		}
		if err = matches(raw, expected); err != nil {
			return err
		}
	}
	if raw, found, e := col.GetInto([]byte(lin.ID), nil); e != nil || found || raw != nil {
		return fmt.Errorf("deleted row after reopen: found=%t err=%v", found, e)
	}
	fmt.Println("Verified durable reopen: 2 complete rows; deleted row absent.")
	return nil
}

func main() {
	dir := flag.String("dir", "", "new or empty DB directory; default creates and retains a temporary directory")
	flag.Parse()
	if err := run(*dir); err != nil {
		if errors.Is(err, collections.ErrCommitAmbiguous) {
			fmt.Fprintln(os.Stderr, "Commit outcome is uncertain. Stop, reopen, and reconcile IDs/indexes before deciding whether to retry.")
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
