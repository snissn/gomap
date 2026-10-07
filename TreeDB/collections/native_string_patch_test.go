package collections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/node"
	"reflect"
	"sync/atomic"
	"testing"
)

func TestNativeStringPatchRepeatedSourcesAndUniqueSwap(t *testing.T) {
	dir, db, col := r1MutationOpen5059(t, true)
	defer db.Close()
	rows := []map[string]any{r1MutationRow5059(0), r1MutationRow5059(1)}
	ids, retained, columns := r1MutationBatch5059(t, rows...)
	known := r1MutationKnown5059()
	r1MutationRemember5059(known, rows...)
	original := map[string]map[string]any{string(ids[0]): r1MutationCopy5059(rows[0]), string(ids[1]): r1MutationCopy5059(rows[1])}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	before := make([][]columnRowCoordinates, 2)
	ptrs := make([]any, 2)
	snap := db.AcquireSnapshot()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		raw, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, collectionColumnRowLocatorRootName(col.Meta().Name), id, nil)
		if err != nil || !found {
			t.Fatal(err)
		}
		before[i], err = columnFieldSourcesForLocator(id, raw, col.Meta().Options.ColumnStore.Columns)
		if err != nil {
			t.Fatal(err)
		}
		entry, _, err := collectionGetEntryAtCatalogRoot(snap, catalog, collectionPrimaryRootName(col.Meta().Name), id)
		if err != nil {
			t.Fatal(err)
		}
		if entry.Flags&node.FlagPointer == 0 {
			t.Fatalf("fixture must force persistent ValuePtr: flags=%d", entry.Flags)
		}
		ptrs[i] = entry.ValuePtr
	}
	snap.Close()
	held, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	for _, field := range []string{"name", "bio", "city"} {
		for i, id := range ids {
			value := field + " changed " + string(id)
			result, err := col.PatchTypedStringsBatch([]TypedStringPatch{{ID: id, Edits: []TypedStringEdit{{Column: field, Value: value}}}}, col.Meta().Options.ColumnStore.SchemaHash)
			if err != nil || result.ModifiedCount != 1 {
				t.Fatalf("patch %s: %+v %v", field, result, err)
			}
			rows[i][field] = value
			r1MutationRemember5059(known, rows[i])
			snap = db.AcquireSnapshot()
			catalog, err = col.catalogForSnapshot(snap)
			if err != nil {
				t.Fatal(err)
			}
			raw, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, collectionColumnRowLocatorRootName(col.Meta().Name), id, nil)
			if err != nil || !found {
				t.Fatal(err)
			}
			after, err := columnFieldSourcesForLocator(id, raw, col.Meta().Options.ColumnStore.Columns)
			if err != nil {
				t.Fatal(err)
			}
			for j, c := range col.Meta().Options.ColumnStore.Columns {
				if c.Name == field {
					if after[j] == before[i][j] {
						t.Fatal("changed source preserved")
					}
				} else if after[j] != before[i][j] {
					t.Fatalf("untouched %s source changed", c.Name)
				}
			}
			before[i] = after
			entry, _, err := collectionGetEntryAtCatalogRoot(snap, catalog, collectionPrimaryRootName(col.Meta().Name), id)
			if err != nil {
				t.Fatal(err)
			}
			if entry.ValuePtr != ptrs[i] {
				t.Fatal("persistent primary pointer changed")
			}
			snap.Close()
		}
	}
	beforeSeq, beforeRoot := dbCommitSeqAndSystemRoot(db)
	if _, err := col.PatchTypedStringsBatch([]TypedStringPatch{{ID: ids[0], Edits: []TypedStringEdit{{Column: "email", Value: rows[1]["email"].(string)}}}}, col.Meta().Options.ColumnStore.SchemaHash); err == nil {
		t.Fatal("conflicting final unique owner admitted")
	}
	if seq, root := dbCommitSeqAndSystemRoot(db); seq != beforeSeq || root != beforeRoot {
		t.Fatal("unique conflict changed authority")
	}
	result, err := col.PatchTypedStringsBatch([]TypedStringPatch{{ID: ids[0], Edits: []TypedStringEdit{{Column: "email", Value: rows[1]["email"].(string)}}}, {ID: ids[1], Edits: []TypedStringEdit{{Column: "email", Value: rows[0]["email"].(string)}}}}, col.Meta().Options.ColumnStore.SchemaHash)
	if err != nil || result.ModifiedCount != 2 {
		t.Fatalf("unique swap: %+v %v", result, err)
	}
	rows[0]["email"], rows[1]["email"] = rows[1]["email"], rows[0]["email"]
	r1MutationRemember5059(known, rows...)
	want := map[string]map[string]any{string(ids[0]): rows[0], string(ids[1]): rows[1]}
	r1MutationAssert5059(t, col, want, known)
	r1LifecycleAssert5060(t, held, original, known)
	result, err = col.PatchTypedStringsBatch([]TypedStringPatch{{ID: ids[0], Edits: []TypedStringEdit{{Column: "bio", Value: rows[0]["bio"].(string)}}}, {ID: []byte("absent"), Edits: []TypedStringEdit{{Column: "bio", Value: "ignored"}}}}, col.Meta().Options.ColumnStore.SchemaHash)
	if err != nil || result.MatchedCount != 1 || result.ModifiedCount != 0 {
		t.Fatalf("noop/missing: %+v %v", result, err)
	}
	refs, err := held.LookupDocumentRowRefsByID(ids, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = col.PatchTypedStringsBatch([]TypedStringPatch{{ID: ids[0], Expected: &refs.Results[0].RowRef, Edits: []TypedStringEdit{{Column: "bio", Value: "stale"}}}}, col.Meta().Options.ColumnStore.SchemaHash)
	if !errors.Is(err, ErrTypedStringPatchConflict) {
		t.Fatalf("stale: %v", err)
	}
	if err = held.Close(); err != nil {
		t.Fatal(err)
	}
	if err = db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	folded, err := col.ColumnStoreCompact(context.Background(), ColumnStoreCompactOptions{})
	if err != nil || !folded.Compacted {
		t.Fatalf("fold: %+v %v", folded, err)
	}
	r1MutationAssert5059(t, col, want, known)
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openTypedMinimaDB(t, dir)
	defer reopened.Close()
	current, err := NewCollectionManager(reopened).OpenCollection("r1")
	if err != nil {
		t.Fatal(err)
	}
	r1MutationAssert5059(t, current, want, known)
}

func TestNativeStringPatchOwnedReplaceAndAmbiguousReplay(t *testing.T) {
	dir, db, col := r1MutationOpen5059(t, true)
	defer func() { db.Close() }()
	row := r1MutationRow5059(0)
	ids, retained, columns := r1MutationBatch5059(t, row)
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	schema := col.Meta().Options.ColumnStore.SchemaHash
	beforeSeq, beforeRoot := dbCommitSeqAndSystemRoot(db)
	for _, request := range []TypedStringPatch{
		{ID: []byte("absent"), Edits: []TypedStringEdit{{Column: "undeclared", Value: "bad"}}},
		{ID: ids[0], Edits: []TypedStringEdit{{Column: "bio", Value: "a"}, {Column: "bio", Value: "b"}}},
		{ID: ids[0], ResidualMode: TypedStringResidualReplace},
		{ID: ids[0], ResidualMode: TypedStringResidualReplace, Residual: []byte(`{"id":"row-000","bio":"shadow"}`)},
	} {
		if _, err := col.PatchTypedStringsBatch([]TypedStringPatch{request}, schema); err == nil {
			t.Fatalf("accepted invalid request %+v", request)
		}
	}
	if seq, root := dbCommitSeqAndSystemRoot(db); seq != beforeSeq || root != beforeRoot {
		t.Fatal("rejected request changed authority")
	}
	next := r1MutationCopy5059(row)
	next["bio"] = "owned 雪"
	next["age"] = float64(71)
	_, nextResidual, _ := r1MutationBatch5059(t, next)
	request := TypedStringPatch{ID: append([]byte(nil), ids[0]...), Edits: []TypedStringEdit{{Column: "bio", Value: next["bio"].(string)}}, ResidualMode: TypedStringResidualReplace, Residual: append([]byte(nil), nextResidual[0]...)}
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.BeforeDependencyAppend {
			clear(request.ID)
			clear(request.Residual)
			request.Edits[0].Value = "caller changed"
		}
		return nil
	})
	result, err := col.PatchTypedStringsBatch([]TypedStringPatch{request}, schema)
	restore()
	if err != nil || result.ModifiedCount != 1 {
		t.Fatalf("owned replace %+v %v", result, err)
	}
	raw, err := col.Get(ids[0])
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err = json.Unmarshal(raw, &got); err != nil || got["bio"] != next["bio"] || got["age"] != next["age"] {
		t.Fatalf("owned result %s %v", raw, err)
	}
	fault := errors.New("native strings post-durable cut")
	var fired atomic.Bool
	restore = durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.AfterDependencyFileSync && fired.CompareAndSwap(false, true) {
			return fault
		}
		return nil
	})
	_, err = col.PatchTypedStringsBatch([]TypedStringPatch{{ID: ids[0], Edits: []TypedStringEdit{{Column: "name", Value: "replayed name"}}}}, schema)
	restore()
	if !errors.Is(err, ErrCommitAmbiguous) || !errors.Is(err, fault) {
		t.Fatalf("ambiguous cut fired=%v err=%v", fired.Load(), err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTypedMinimaDB(t, dir)
	col, err = NewCollectionManager(db).OpenCollection("r1")
	if err != nil {
		t.Fatal(err)
	}
	next["name"] = "replayed name"
	known := r1MutationKnown5059()
	r1MutationRemember5059(known, row, next)
	r1MutationAssert5059(t, col, map[string]map[string]any{string(ids[0]): next}, known)
}

func TestNativeStringPatchMetadataCoexistence(t *testing.T) {
	meta := r1MutationMeta5059(false)
	meta.Name = "minima"
	meta.Options.ColumnStore.Columns[3] = ColumnStoreColumn{Name: "meta.tag", Path: "meta.tag", ValueType: ColumnStoreValueString}
	dir, db, col := openTypedMinimaCollectionMeta(t, meta)
	defer func() { _ = db.Close() }()
	id := []byte("coexist")
	columns := []TypedColumnBatch{{Name: "email", Strings: []string{"owner@example.test"}}, {Name: "city", Strings: []string{"city"}}, {Name: "name", Strings: []string{"original"}}, {Name: "meta.tag", Strings: []string{"old"}}}
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{id}, [][]byte{[]byte(`{"id":"coexist","meta":{"extra":true},"nullable":null}`)}, columns); err != nil {
		t.Fatal(err)
	}
	schema := col.Meta().Options.ColumnStore.SchemaHash
	var previous []columnRowCoordinates
	assertSources := func(changed string) {
		t.Helper()
		snap := db.AcquireSnapshot()
		defer snap.Close()
		catalog, err := col.catalogForSnapshot(snap)
		if err != nil {
			t.Fatal(err)
		}
		raw, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, collectionColumnRowLocatorRootName(col.Meta().Name), id, nil)
		if err != nil || !found {
			t.Fatal(err)
		}
		sources, err := columnFieldSourcesForLocator(id, raw, col.Meta().Options.ColumnStore.Columns)
		if err != nil {
			t.Fatal(err)
		}
		if previous != nil {
			for j, col := range col.Meta().Options.ColumnStore.Columns {
				if col.Name != changed && sources[j] != previous[j] {
					t.Fatalf("%s rewrote untouched %s", changed, col.Name)
				}
			}
		}
		previous = sources
	}
	assertSources("")
	if _, err := col.PatchTypedStringsBatch([]TypedStringPatch{{ID: id, Edits: []TypedStringEdit{{Column: "name", Value: "patched"}}}}, schema); err != nil {
		t.Fatal(err)
	}
	assertSources("name")
	if _, err := col.UpdateTypedMetadataByID([][]byte{id}, map[string]any{"meta.tag": "metadata"}, nil, metadataGeneration4769(col)); err != nil {
		t.Fatal(err)
	}
	assertSources("meta.tag")
	if _, err := col.PatchTypedStringsBatch([]TypedStringPatch{{ID: id, Edits: []TypedStringEdit{{Column: "city", Value: "new city"}}}}, schema); err != nil {
		t.Fatal(err)
	}
	assertSources("city")
	if _, err := col.UpdateTypedMetadataByID([][]byte{id}, map[string]any{"meta.extra": false}, nil, metadataGeneration4769(col)); err != nil {
		t.Fatal(err)
	}
	assertSources("") // residual-only metadata change preserves every field source.
	want := map[string]any{"id": "coexist", "email": "owner@example.test", "city": "new city", "name": "patched", "nullable": nil, "meta": map[string]any{"tag": "metadata", "extra": false}}
	assertRow := func() {
		t.Helper()
		raw, err := col.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		if err = json.Unmarshal(raw, &got); err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("row %s want %#v err %v", raw, want, err)
		}
	}
	assertRow()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTypedMinimaDB(t, dir)
	col, err := NewCollectionManager(db).OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	assertRow()
}

func TestNativeStringPatchPreservesMissingAndNull(t *testing.T) {
	meta := r1MutationMeta5059(false)
	meta.Name = "minima"
	meta.Options.ColumnStore.Columns[1].Nullable = true
	meta.Options.ColumnStore.Columns[3].Nullable = true
	_, db, col := openTypedMinimaCollectionMeta(t, meta)
	defer db.Close()
	id := []byte("nullable")
	if _, err := col.InsertBatch([][]byte{id}, [][]byte{[]byte(`{"id":"nullable","email":"owner@example.test","city":null,"name":"old","residual":null}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := col.PatchTypedStringsBatch([]TypedStringPatch{{ID: id, Edits: []TypedStringEdit{{Column: "name", Value: ""}}}}, col.Meta().Options.ColumnStore.SchemaHash); err != nil {
		t.Fatal(err)
	}
	raw, err := col.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err = json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got["name"] != "" {
		t.Fatalf("empty value lost: %s", raw)
	}
	if value, present := got["city"]; !present || value != nil {
		t.Fatalf("null changed: %s", raw)
	}
	if _, present := got["bio"]; present {
		t.Fatalf("missing changed: %s", raw)
	}
	if _, err := col.ColumnStoreCompact(context.Background(), ColumnStoreCompactOptions{}); err != nil {
		t.Fatal(err)
	}
	after, err := col.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqualM13C(t, after, raw)
}

// Forty disjoint string sources exceed the simultaneous block cap. Full rows
// must remain owned while source-to-source eviction closes old assets.
func TestNativeStringPatchScatterEvictsAtOwnedAliasBoundary(t *testing.T) {
	testNativeStringPatchScatterOwned(t, false)
}

func TestNativeStringPatchMetadataScatterOwnedSession(t *testing.T) {
	testNativeStringPatchScatterOwned(t, true)
}

func testNativeStringPatchScatterOwned(t *testing.T, metadata bool) {
	t.Helper()
	meta := r1ReadCollectionMeta()
	meta.Indexes = nil
	meta.Options.ColumnStore.Columns = nil
	const n = 40
	cols := make([]TypedColumnBatch, n)
	want := make(map[string]any, n)
	original := make(map[string]any, n)
	names := make([]string, n)
	remember := func(object map[string]any, name, value string) {
		if name == "meta.tag" {
			object["meta"] = map[string]any{"tag": value}
		} else {
			object[name] = value
		}
	}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("field%02d", i)
		if metadata && i == n-1 {
			name = "meta.tag"
		}
		names[i] = name
		value := "original " + name
		meta.Options.ColumnStore.Columns = append(meta.Options.ColumnStore.Columns, ColumnStoreColumn{Name: name, Path: name, ValueType: ColumnStoreValueString})
		cols[i] = TypedColumnBatch{Name: name, Strings: []string{value}}
		remember(original, name, value)
		remember(want, name, value)
	}
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}, DurabilityProfile: backenddb.ProfileCommandWALDurable}); err != nil {
		t.Fatal(err)
	}
	db, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true, CommandWAL: true, ResolvedProfile: backenddb.ProfileCommandWALDurable, ValueLog: backenddb.ValueLogOptions{PointerThreshold: 1, ForcePointers: true}})
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(db)
	if _, err := manager.CreateCollection(&meta); err != nil {
		db.Close()
		t.Fatal(err)
	}
	col, err := manager.OpenCollection(meta.Name)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	defer db.Close()
	id := []byte("a")
	want["id"], original["id"] = "a", "a"
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{id}, [][]byte{[]byte(`{"id":"a"}`)}, cols); err != nil {
		t.Fatal(err)
	}
	held, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	schema := col.Meta().Options.ColumnStore.SchemaHash
	for i := 0; i < n; i++ {
		name := names[i]
		value := "new " + name
		if result, err := col.PatchTypedStringsBatch([]TypedStringPatch{{ID: id, Edits: []TypedStringEdit{{Column: name, Value: value}}}}, schema); err != nil || result.ModifiedCount != 1 {
			t.Fatalf("patch %d: %+v %v", i, result, err)
		}
		remember(want, name, value)
	}
	if metadata {
		// A metadata planner must read forty preserved sources through the
		// same bounded session. A second request also catches stale scratch
		// aliases after the first planner has closed its view and caches.
		for _, value := range []string{"metadata first", "metadata second"} {
			result, err := col.UpdateTypedMetadataByID([][]byte{id}, map[string]any{"meta.tag": value}, nil, metadataGeneration4769(col))
			if err != nil || result.ModifiedCount != 1 {
				t.Fatalf("metadata scatter: %+v %v", result, err)
			}
			remember(want, "meta.tag", value)
		}
	}
	current, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	got, err := current.FetchDocumentsByID([][]byte{id}, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	r1RequireCompleteRow(t, got.Results[0].Document, encoded)
	if len(current.pointRowBlocks) > 32 || current.pointRowOpenFiles() > 32 || current.assetManager.ActiveHandles() > 32 || current.pointRowCreditUsed > current.pointRowCreditLimit || current.pointRowCacheEvictions == 0 {
		t.Fatal("scatter escaped bounded credit/eviction")
	}
	owned := bytes.Clone(got.Results[0].Document)
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(owned, got.Results[0].Document) {
		t.Fatal("cache close changed owned row")
	}
	old, err := held.FetchDocumentsByID([][]byte{id}, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err = json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	r1RequireCompleteRow(t, old.Results[0].Document, encoded)
}
