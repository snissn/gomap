package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// R1's native carrier owns four non-null strings. Other scalar/null/missing
// values remain in the retained object; this fixture does not broaden carriers.
func r1MutationMeta5059(indexed bool) CollectionMeta {
	meta := CollectionMeta{Name: "r1", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, DisableBufferedIndexedAsyncFlush: true,
		ColumnStore: &ColumnStoreConfig{Enabled: true, RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON}}}
	for _, field := range []string{"email", "city", "name", "bio"} {
		meta.Options.ColumnStore.Columns = append(meta.Options.ColumnStore.Columns, ColumnStoreColumn{Name: field, Path: field, ValueType: ColumnStoreValueString})
	}
	if indexed {
		meta.Indexes = []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}, {Name: "city", Field: "city", ValueType: IndexValueString}}
	}
	return meta
}

func r1MutationRow5059(n int) map[string]any {
	row := map[string]any{"id": fmt.Sprintf("row-%03d", n), "email": fmt.Sprintf("user-%03d@example.test", n), "city": fmt.Sprintf("city-%d", n%8),
		"name": fmt.Sprintf("Name %d", n), "bio": fmt.Sprintf("Biography %d: 雪", n), "age": float64(20 + n), "score": float64(n) + .25, "revision": float64(0), "active": n%2 == 0}
	if n%2 == 0 {
		row["optional"] = nil
	}
	return row
}

func r1MutationCopy5059(row map[string]any) map[string]any {
	out := make(map[string]any, len(row))
	for k, v := range row {
		out[k] = v
	}
	return out
}

func r1MutationJSON5059(t testing.TB, row map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func r1MutationBatch5059(t testing.TB, rows ...map[string]any) ([][]byte, [][]byte, []TypedColumnBatch) {
	t.Helper()
	ids, retained := make([][]byte, len(rows)), make([][]byte, len(rows))
	columns := []TypedColumnBatch{{Name: "email"}, {Name: "city"}, {Name: "name"}, {Name: "bio"}}
	for i, row := range rows {
		ids[i] = []byte(row["id"].(string))
		residual := r1MutationCopy5059(row)
		for j := range columns {
			columns[j].Strings = append(columns[j].Strings, row[columns[j].Name].(string))
			delete(residual, columns[j].Name)
		}
		retained[i] = r1MutationJSON5059(t, residual)
	}
	return ids, retained, columns
}

func r1MutationOpen5059(t testing.TB, indexed bool) (string, *backenddb.DB, *Collection) {
	t.Helper()
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}, DurabilityProfile: backenddb.ProfileCommandWALDurable}); err != nil {
		t.Fatal(err)
	}
	db := openTypedMinimaDB(t, dir)
	meta := r1MutationMeta5059(indexed)
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
	return dir, db, col
}

// Check every primary row and every current/historical secondary key. This
// catches stale postings, missing postings, tombstone resurrection and loss of
// null versus missing, not merely the rows touched by the latest operation.
func r1MutationAssert5059(t testing.TB, col *Collection, want map[string]map[string]any, known map[string]map[string]bool) {
	t.Helper()
	for id := range known["id"] {
		raw, err := col.Get([]byte(id))
		if err != nil {
			t.Fatalf("Get(%s): %v", id, err)
		}
		row, found := want[id]
		if !found {
			if raw != nil {
				t.Fatalf("deleted/missing %s resurrected: %s", id, raw)
			}
			continue
		}
		var got map[string]any
		if err := json.Unmarshal(raw, &got); err != nil || !reflect.DeepEqual(got, row) {
			t.Fatalf("row %s got=%s want=%s err=%v", id, raw, r1MutationJSON5059(t, row), err)
		}
	}
	for _, index := range col.Meta().Indexes {
		for key := range known[index.Field] {
			var expected []string
			for id, row := range want {
				if row[index.Field] == key {
					expected = append(expected, id)
				}
			}
			ids, err := col.FindByIndex(index.Name, key)
			if err != nil {
				t.Fatal(err)
			}
			got := make([]string, 0, len(ids))
			for _, id := range ids {
				got = append(got, string(id))
			}
			slices.Sort(got)
			slices.Sort(expected)
			if !slices.Equal(got, expected) {
				t.Fatalf("index %s key %s got=%q want=%q", index.Name, key, got, expected)
			}
		}
	}
}

func r1MutationRemember5059(known map[string]map[string]bool, rows ...map[string]any) {
	for _, row := range rows {
		for _, field := range []string{"id", "email", "city"} {
			known[field][row[field].(string)] = true
		}
	}
}

func r1MutationKnown5059() map[string]map[string]bool {
	return map[string]map[string]bool{"id": {}, "email": {}, "city": {}}
}

func TestR1MutationMatrix5059(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("indexed=%t", indexed), func(t *testing.T) {
			dir, db, col := r1MutationOpen5059(t, indexed)
			defer func() { _ = db.Close() }()
			want, known := make(map[string]map[string]any), r1MutationKnown5059()
			var seed []map[string]any
			// Deliberately reverse input order; results preserve request order.
			for n := 15; n >= 0; n-- {
				row := r1MutationRow5059(n)
				seed = append(seed, row)
				want[row["id"].(string)] = row
			}
			r1MutationRemember5059(known, seed...)
			ids, retained, columns := r1MutationBatch5059(t, seed...)
			beforeWork := workstats.Read()
			out, stats, err := col.InsertTypedBatchWithStats(ids, retained, columns)
			if err != nil || !reflect.DeepEqual(out, ids) || stats.Documents != 16 {
				t.Fatalf("insert out=%q stats=%+v err=%v", out, stats, err)
			}
			afterWork := workstats.Read()
			if afterWork.IndexedJSON != beforeWork.IndexedJSON || (indexed && afterWork.Typed.ScalarRows <= beforeWork.Typed.ScalarRows) {
				t.Fatalf("typed insert route before=%+v/%+v after=%+v/%+v", beforeWork.IndexedJSON, beforeWork.Typed, afterWork.IndexedJSON, afterWork.Typed)
			}
			// Inputs are borrowed only for the call. Reuse cannot alter authority.
			columns[0].Strings[0] = "caller-reused"
			retained[0][0] = ' '
			ids[0][0] = 'X'
			r1MutationAssert5059(t, col, want, known)
			changed := r1MutationCopy5059(want["row-001"])
			changed["email"], changed["city"], changed["bio"], changed["revision"] = "changed@example.test", "city-7", "Changed 雪", float64(1)
			missing := r1MutationRow5059(99)
			r1MutationRemember5059(known, changed, missing)
			ids, retained, columns = r1MutationBatch5059(t, changed, want["row-000"], missing)
			results, err := col.ReplaceTypedBatch(ids, retained, columns)
			if err != nil || !reflect.DeepEqual(results, []UpdateBatchResult{{Matched: true, Modified: true}, {Matched: true}, {}}) {
				t.Fatalf("replace results=%+v err=%v", results, err)
			}
			want["row-001"] = changed
			r1MutationAssert5059(t, col, want, known)
			// Swap final unique owners in one batch rather than imposing input order.
			a, b := r1MutationCopy5059(want["row-002"]), r1MutationCopy5059(want["row-003"])
			a["email"], b["email"] = b["email"], a["email"]
			ids, retained, columns = r1MutationBatch5059(t, b, a)
			if result, err := col.ReplaceTypedBatch(ids, retained, columns); err != nil || !reflect.DeepEqual(result, []UpdateBatchResult{{true, true}, {true, true}}) {
				t.Fatalf("handoff results=%+v err=%v", result, err)
			}
			want["row-002"], want["row-003"] = a, b
			r1MutationAssert5059(t, col, want, known)
			mixed := r1MutationCopy5059(want["row-004"])
			mixed["revision"], mixed["score"], mixed["active"] = float64(2), 9.75, false
			delete(mixed, "optional")
			fresh := r1MutationRow5059(16)
			r1MutationRemember5059(known, fresh)
			ids, retained, columns = r1MutationBatch5059(t, fresh, want["row-005"], mixed)
			before := len(collectionCommandWALFrames(t, dir))
			if matched, err := col.UpsertTypedBatch(ids, retained, columns); err != nil || matched != 2 {
				t.Fatalf("mixed upsert matched=%d err=%v", matched, err)
			}
			if frames := len(collectionCommandWALFrames(t, dir)); frames != before+1 {
				t.Fatalf("mixed upsert frames=%d want=%d", frames, before+1)
			}
			want["row-004"], want["row-016"] = mixed, fresh
			r1MutationAssert5059(t, col, want, known)
			before = len(collectionCommandWALFrames(t, dir))
			if matched, err := col.UpsertTypedBatch(ids, retained, columns); err != nil || matched != 3 || len(collectionCommandWALFrames(t, dir)) != before {
				t.Fatalf("unchanged upsert matched=%d err=%v", matched, err)
			}
			updated := r1MutationCopy5059(want["row-006"])
			updated["email"], updated["city"], updated["age"], updated["optional"] = "updated@example.test", "city-0", float64(90), "present"
			r1MutationRemember5059(known, updated)
			results, err = col.UpdateBatch([]UpdateBatchItem{
				{DocumentID: []byte("row-006"), Update: func(current []byte) ([]byte, bool, error) {
					assertJSONEqualM13C(t, current, r1MutationJSON5059(t, want["row-006"]))
					return r1MutationJSON5059(t, updated), true, nil
				}},
				{DocumentID: []byte("row-007"), Update: func([]byte) ([]byte, bool, error) { return nil, false, nil }},
				{DocumentID: []byte("row-099"), Update: func([]byte) ([]byte, bool, error) { t.Fatal("missing callback invoked"); return nil, false, nil }},
			})
			if err != nil || !reflect.DeepEqual(results, []UpdateBatchResult{{true, true}, {Matched: true}, {}}) {
				t.Fatalf("update results=%+v err=%v", results, err)
			}
			want["row-006"] = updated
			r1MutationAssert5059(t, col, want, known)
			nonindexed := r1MutationCopy5059(want["row-007"])
			nonindexed["name"], nonindexed["age"], nonindexed["optional"] = "New name 雪", float64(30), nil
			results, err = col.UpdateBatch([]UpdateBatchItem{{DocumentID: []byte("row-007"), Update: func([]byte) ([]byte, bool, error) {
				return r1MutationJSON5059(t, nonindexed), true, nil
			}}})
			if err != nil || !reflect.DeepEqual(results, []UpdateBatchResult{{true, true}}) {
				t.Fatalf("nonindexed update results=%+v err=%v", results, err)
			}
			want["row-007"] = nonindexed
			r1MutationAssert5059(t, col, want, known)
			// A later callback failure rejects the earlier prepared row too.
			injected := errors.New("rejected second callback")
			before = len(collectionCommandWALFrames(t, dir))
			_, err = col.UpdateBatch([]UpdateBatchItem{
				{DocumentID: []byte("row-007"), Update: func([]byte) ([]byte, bool, error) { return r1MutationJSON5059(t, r1MutationRow5059(7)), true, nil }},
				{DocumentID: []byte("row-006"), Update: func([]byte) ([]byte, bool, error) { return nil, false, injected }},
			})
			if !errors.Is(err, injected) || errors.Is(err, ErrCommitAmbiguous) || len(collectionCommandWALFrames(t, dir)) != before {
				t.Fatalf("callback batch rejection err=%v", err)
			}
			r1MutationAssert5059(t, col, want, known)
			// Delete/insert/upsert without an explicit Flush exercises the newest
			// pending/tombstone visibility and reuse of released unique ownership.
			if deleted, err := col.DeleteBatch([][]byte{[]byte("row-008"), []byte("row-099")}); err != nil || deleted != 1 {
				t.Fatalf("delete count=%d err=%v", deleted, err)
			}
			delete(want, "row-008")
			r1MutationAssert5059(t, col, want, known)
			resurrect := r1MutationRow5059(8)
			resurrect["revision"] = float64(3)
			ids, retained, columns = r1MutationBatch5059(t, resurrect)
			if matched, err := col.UpsertTypedBatch(ids, retained, columns); err != nil || matched != 0 {
				t.Fatalf("tombstone upsert matched=%d err=%v", matched, err)
			}
			want["row-008"] = resurrect
			r1MutationAssert5059(t, col, want, known)
			if indexed {
				r1MutationRejected5059(t, col, dir, want, known)
			}
			if err := col.Flush(); err != nil {
				t.Fatal(err)
			}
			r1MutationAssert5059(t, col, want, known)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = openTypedMinimaDB(t, dir)
			col, err = NewCollectionManager(db).OpenCollection("r1")
			if err != nil {
				t.Fatal(err)
			}
			r1MutationAssert5059(t, col, want, known)
		})
	}
}

func r1MutationRejected5059(t *testing.T, col *Collection, dir string, want map[string]map[string]any, known map[string]map[string]bool) {
	t.Helper()
	a, b := r1MutationCopy5059(want["row-004"]), r1MutationCopy5059(want["row-005"])
	a["bio"], b["email"] = "must not publish", a["email"]
	for _, operation := range []string{"insert", "replace", "upsert", "source", "update", "delete"} {
		for _, duplicate := range []bool{false, true} {
			if operation == "delete" && !duplicate {
				continue
			}
			t.Run(operation+fmt.Sprintf("/duplicate=%t", duplicate), func(t *testing.T) {
				rows := []map[string]any{a, b}
				if operation == "insert" && !duplicate {
					rows = []map[string]any{r1MutationRow5059(20), r1MutationRow5059(21)}
					rows[1]["email"] = want["row-000"]["email"]
				}
				if duplicate {
					rows = []map[string]any{a, a}
				}
				r1MutationRemember5059(known, rows...)
				ids, retained, columns := r1MutationBatch5059(t, rows...)
				frames := len(collectionCommandWALFrames(t, dir))
				var err error
				switch operation {
				case "insert":
					_, _, err = col.InsertTypedBatchWithStats(ids, retained, columns)
				case "replace":
					_, err = col.ReplaceTypedBatch(ids, retained, columns)
				case "upsert":
					_, err = col.UpsertTypedBatch(ids, retained, columns)
				case "source":
					_, err = col.ReplaceTypedSourceByID(ids, ids, retained, columns)
				case "update":
					items := make([]UpdateBatchItem, len(rows))
					for i, row := range rows {
						items[i] = UpdateBatchItem{DocumentID: ids[i], Update: func([]byte) ([]byte, bool, error) { return r1MutationJSON5059(t, row), true, nil }}
					}
					_, err = col.UpdateBatch(items)
				case "delete":
					_, err = col.DeleteBatch(ids)
				}
				wantError := ErrUniqueIndexConflict
				if duplicate {
					wantError = ErrDuplicateDocumentID
				}
				if !errors.Is(err, wantError) || errors.Is(err, ErrCommitAmbiguous) || len(collectionCommandWALFrames(t, dir)) != frames {
					t.Fatalf("rejection err=%v want=%v WAL changed=%t", err, wantError, len(collectionCommandWALFrames(t, dir)) != frames)
				}
				r1MutationAssert5059(t, col, want, known)
			})
		}
	}
}

func TestR1MutationUnsupportedCarrier5059(t *testing.T) {
	for _, kind := range []ColumnStoreValueType{ColumnStoreValueInt64, ColumnStoreValueBool} {
		t.Run(string(kind), func(t *testing.T) {
			dir, db, col := r1MutationOpen5059(t, true)
			defer db.Close()
			// Validation is tested against the captured schema without inventing a
			// public numeric/null carrier. The public carrier shape has only strings/FP32.
			meta := col.Meta()
			meta.Options.ColumnStore.Columns[0].ValueType = kind
			ids, retained, columns := r1MutationBatch5059(t, r1MutationRow5059(0))
			before := len(collectionCommandWALFrames(t, dir))
			if _, err := newTrustedTypedProjection(meta, ids, retained, columns); err == nil {
				t.Fatalf("unsupported %s carrier accepted", kind)
			}
			if len(collectionCommandWALFrames(t, dir)) != before {
				t.Fatal("schema rejection appended WAL")
			}
		})
	}
}

func TestR1MutationPublicAdmission5059(t *testing.T) {
	dir, db, col := r1MutationOpen5059(t, true)
	defer db.Close()
	for _, invalid := range []string{"missing_string", "wrong_carrier", "invalid_utf8", "declared_null", "declared_number"} {
		t.Run(invalid, func(t *testing.T) {
			ids, retained, columns := r1MutationBatch5059(t, r1MutationRow5059(0))
			switch invalid {
			case "missing_string":
				columns[0].Strings = nil
			case "wrong_carrier":
				columns[0].Strings = nil
				columns[0].Float32Vectors = [][]float32{{1}}
			case "invalid_utf8":
				columns[0].Strings[0] = string([]byte{255})
			case "declared_null":
				retained[0] = []byte(`{"id":"row-000","email":null}`)
			case "declared_number":
				retained[0] = []byte(`{"id":"row-000","email":42}`)
			}
			before := len(collectionCommandWALFrames(t, dir))
			if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err == nil || errors.Is(err, ErrCommitAmbiguous) {
				t.Fatalf("public admission error=%v", err)
			}
			if len(collectionCommandWALFrames(t, dir)) != before {
				t.Fatal("invalid carrier appended command WAL")
			}
			if raw, err := col.Get(ids[0]); err != nil || raw != nil {
				t.Fatalf("rejected row visible: %s err=%v", raw, err)
			}
		})
	}
}

// A helper process exits without Close/Flush after a real public call. After
// sync but before ACK the caller observes ambiguity, yet replay must recover
// the entire indexed mutation. A pre-append rejection must recover no change.
func TestR1MutationCrashCuts5059(t *testing.T) {
	if dir := os.Getenv("GOMAP_R1_5059_CRASH_DIR"); dir != "" {
		db := openTypedMinimaDB(t, dir)
		col, err := NewCollectionManager(db).OpenCollection("r1")
		if err != nil {
			t.Fatal(err)
		}
		cut := os.Getenv("GOMAP_R1_5059_CUT")
		injected := errors.New("R1 command WAL cut")
		var fired atomic.Bool
		if cut != "ack" {
			point := durabilitycut.BeforeDependencyAppend
			if cut == "after_sync" {
				point = durabilitycut.AfterDependencyFileSync
			}
			durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == point && fired.CompareAndSwap(false, true) {
					return injected
				}
				return nil
			})
		}
		_, err = r1MutationCrashOperation5059(t, col, os.Getenv("GOMAP_R1_5059_OPERATION"))
		if cut == "ack" {
			if err != nil {
				t.Fatal(err)
			}
		} else if !fired.Load() || !errors.Is(err, injected) || errors.Is(err, ErrCommitAmbiguous) != (cut == "after_sync") {
			t.Fatalf("cut=%s fired=%t error=%v", cut, fired.Load(), err)
		}
		if cut == "after_sync" && isRetriableCollectionMutationError(err) {
			t.Fatal("accepted ambiguous command is automatically retryable")
		}
		os.Exit(0)
	}
	for _, operation := range []string{"insert", "replace", "upsert", "update", "source", "delete"} {
		for _, cut := range []string{"before_append", "after_sync", "ack"} {
			t.Run(operation+"/"+cut, func(t *testing.T) {
				dir, db, col := r1MutationOpen5059(t, true)
				want, known := make(map[string]map[string]any), r1MutationKnown5059()
				var rows []map[string]any
				for n := range 16 {
					row := r1MutationRow5059(n)
					rows = append(rows, row)
					want[row["id"].(string)] = row
				}
				r1MutationRemember5059(known, rows...)
				ids, retained, columns := r1MutationBatch5059(t, rows...)
				if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
					t.Fatal(err)
				}
				if err := col.Flush(); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				cmd := exec.Command(os.Args[0], "-test.run=^TestR1MutationCrashCuts5059$")
				cmd.Env = append(os.Environ(), "GOMAP_R1_5059_CRASH_DIR="+dir, "GOMAP_R1_5059_OPERATION="+operation, "GOMAP_R1_5059_CUT="+cut)
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("helper: %v\n%s", err, output)
				}
				changed, removed := r1MutationCrashRows5059(operation)
				r1MutationRemember5059(known, changed...)
				if cut != "before_append" {
					for _, id := range removed {
						delete(want, id)
					}
					for _, row := range changed {
						want[row["id"].(string)] = row
					}
				}
				before := workstats.Read()
				db = openTypedMinimaDB(t, dir)
				defer db.Close()
				after := workstats.Read()
				// A successful ACK may finish publication before process exit
				// (notably during the race runtime's exit delay). The ambiguous
				// post-sync cut must instead recover its unapplied frame.
				if cut == "after_sync" && after.Replay.FramesApplied <= before.Replay.FramesApplied {
					t.Fatal("ambiguous durable command did not replay")
				}
				col, err := NewCollectionManager(db).OpenCollection("r1")
				if err != nil {
					t.Fatal(err)
				}
				r1MutationAssert5059(t, col, want, known)
			})
		}
	}
}

func r1MutationCrashRows5059(operation string) ([]map[string]any, []string) {
	a, b, fresh := r1MutationRow5059(0), r1MutationRow5059(1), r1MutationRow5059(16)
	a["revision"], a["city"] = float64(1), "city-7"
	switch operation {
	case "insert":
		return []map[string]any{fresh, r1MutationRow5059(17)}, nil
	case "replace":
		a["email"], b["email"] = b["email"], a["email"]
		return []map[string]any{b, a}, nil
	case "upsert":
		return []map[string]any{fresh, a}, nil
	case "update":
		a["email"], a["age"], a["optional"] = "crash-updated@example.test", float64(90), "present"
		b["bio"], b["score"], b["active"], b["optional"] = "residual update", 4.75, true, nil
		return []map[string]any{a, b}, nil
	case "source":
		fresh["email"] = b["email"] // released unique owner may transfer to a new ID.
		return []map[string]any{fresh, a}, []string{"row-000", "row-001"}
	case "delete":
		return nil, []string{"row-000", "row-001"}
	default:
		panic("unknown R1 crash operation")
	}
}

func r1MutationCrashOperation5059(t *testing.T, col *Collection, operation string) (int, error) {
	rows, removed := r1MutationCrashRows5059(operation)
	ids, retained, columns := r1MutationBatch5059(t, rows...)
	switch operation {
	case "insert":
		out, _, err := col.InsertTypedBatchWithStats(ids, retained, columns)
		return len(out), err
	case "replace":
		out, err := col.ReplaceTypedBatch(ids, retained, columns)
		return updateBatchModifiedCount(out), err
	case "upsert":
		return col.UpsertTypedBatch(ids, retained, columns)
	case "update":
		items := make([]UpdateBatchItem, len(rows))
		for i, row := range rows {
			items[i] = UpdateBatchItem{DocumentID: ids[i], Update: func(current []byte) ([]byte, bool, error) {
				if !bytes.Contains(current, []byte(`"id"`)) {
					t.Fatal("generic update did not receive full row")
				}
				return r1MutationJSON5059(t, row), true, nil
			}}
		}
		out, err := col.UpdateBatch(items)
		return updateBatchModifiedCount(out), err
	case "source", "delete":
		deleteIDs := make([][]byte, len(removed))
		for i, id := range removed {
			deleteIDs[i] = []byte(id)
		}
		if operation == "delete" {
			return col.DeleteBatch(deleteIDs)
		}
		return col.ReplaceTypedSourceByID(deleteIDs, ids, retained, columns)
	default:
		panic("unknown R1 crash operation")
	}
}
