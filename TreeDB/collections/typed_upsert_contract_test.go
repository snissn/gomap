package collections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// The first row is always unchanged; the second changes and the third is new
// when the initial two-row fixture is upserted as a complete batch.
func typedUpsertRecoveryBatch(word string, count int) ([][]byte, [][]byte, []TypedColumnBatch) {
	ids := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	retained := [][]byte{[]byte(`{"id":"a"}`), []byte(`{"id":"b"}`), []byte(`{"id":"c"}`)}
	columns := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}, {0, 1, 0, 0, 0, 0, 0, 0}, {0, 0, 1, 0, 0, 0, 0, 0}}[:count]},
		{Name: "content", Strings: []string{"stable", word, word}[:count]},
		{Name: "user", Strings: []string{"stable", word, word}[:count]},
		{Name: "path", Strings: []string{"p1", "p2", "p3"}[:count]},
	}
	return ids[:count], retained[:count], columns
}

func TestTypedUpsertCrashReplay(t *testing.T) {
	if dir := os.Getenv("GOMAP_TYPED_UPSERT_CRASH_DIR"); dir != "" {
		db := openTypedMinimaDB(t, dir)
		col, err := NewCollectionManager(db).OpenCollection("minima")
		if err != nil {
			t.Fatal(err)
		}
		mode := os.Getenv("GOMAP_TYPED_UPSERT_CRASH_MODE")
		injected := errors.New("typed upsert WAL cut")
		var fired atomic.Bool
		if mode != "ack" {
			point := durabilitycut.BeforeDependencyAppend
			if mode == "after_sync" {
				point = durabilitycut.AfterDependencyFileSync
			}
			durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == point && fired.CompareAndSwap(false, true) {
					return injected
				}
				return nil
			})
		}
		matched, err := col.UpsertTypedBatch(typedUpsertRecoveryBatch("beta", 3))
		if mode == "ack" {
			if err != nil || matched != 2 {
				t.Fatalf("ack matched=%d err=%v", matched, err)
			}
		} else if !fired.Load() || !errors.Is(err, injected) || errors.Is(err, ErrCommitAmbiguous) != (mode == "after_sync") {
			t.Fatalf("cut fired=%t err=%v", fired.Load(), err)
		}
		os.Exit(0) // Process loss: intentionally no Close or cleanup.
	}
	for _, mode := range []string{"before_append", "after_sync", "ack"} {
		t.Run(mode, func(t *testing.T) {
			dir, db, col := openTypedMinimaCollection(t)
			if _, err := col.UpsertTypedBatch(typedUpsertRecoveryBatch("alpha", 2)); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestTypedUpsertCrashReplay$")
			cmd.Env = append(os.Environ(), "GOMAP_TYPED_UPSERT_CRASH_DIR="+dir, "GOMAP_TYPED_UPSERT_CRASH_MODE="+mode)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("child: %v\n%s", err, out)
			}
			db = openTypedMinimaDB(t, dir)
			defer db.Close()
			col, err := NewCollectionManager(db).OpenCollection("minima")
			if err != nil {
				t.Fatal(err)
			}
			word, count := "beta", 3
			if mode == "before_append" {
				word, count = "alpha", 2
			}
			ids, _, columns := typedUpsertRecoveryBatch(word, 3)
			for i, id := range ids {
				doc, err := col.Get(id)
				if err != nil || (i >= count && doc != nil) {
					t.Fatalf("recovered %s=%s err=%v", id, doc, err)
				}
				if i < count {
					want, err := json.Marshal(map[string]any{"id": string(id), "content": columns[1].Strings[i], "embedding": columns[0].Float32Vectors[i], "meta": map[string]any{"user_id": columns[2].Strings[i], "fpath": columns[3].Strings[i]}})
					if err != nil {
						t.Fatal(err)
					}
					assertJSONEqualM13C(t, doc, want)
				}
			}
			found, err := col.FindByIndex("user", word)
			if err != nil || len(found) != count-1 {
				t.Fatalf("scalar count=%d want=%d err=%v", len(found), count-1, err)
			}
			text, err := col.SearchText(TextSearchOptions{IndexName: "content", Query: word, TopK: 3})
			if err != nil || len(text.Results) != count-1 {
				t.Fatalf("text=%+v err=%v", text, err)
			}
		})
	}
}

func TestTypedUpsertOverlappingBatches(t *testing.T) {
	dir, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	if _, err := col.UpsertTypedBatch(typedUpsertRecoveryBatch("alpha", 2)); err != nil {
		t.Fatal(err)
	}
	other, err := NewCollectionManager(db).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	frames := len(collectionCommandWALFrames(t, dir))
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var held atomic.Bool
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyAppend && held.CompareAndSwap(false, true) {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	defer restore()
	done := make(chan error, 2)
	write := func(c *Collection, word string, want int) {
		n, err := c.UpsertTypedBatch(typedUpsertRecoveryBatch(word, 3))
		if err == nil && n != want {
			err = fmt.Errorf("%s matched=%d want=%d", word, n, want)
		}
		done <- err
	}
	go write(col, "beta", 2)
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first writer did not reach WAL boundary")
	}
	secondAdmission := make(chan struct{})
	hook := func(c *Collection) {
		if c == other {
			close(secondAdmission)
		}
	}
	if !typedSourceBeforeAdmissionTestHook.CompareAndSwap(nil, &hook) {
		t.Fatal("typed source admission hook already installed")
	}
	defer typedSourceBeforeAdmissionTestHook.CompareAndSwap(&hook, nil)
	go write(other, "gamma", 3)
	select {
	case <-secondAdmission:
	case <-ctx.Done():
		t.Fatal("second writer did not reach shared mutation admission")
	}
	unblock()
	for range 2 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("overlapping upserts did not complete")
		}
	}
	if n := len(collectionCommandWALFrames(t, dir)); n != frames+2 {
		t.Fatalf("WAL frames=%d want=%d", n, frames+2)
	}
	ids, _, _ := typedUpsertRecoveryBatch("gamma", 3)
	old, err := view.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil || len(old.Results) != 3 || !old.Results[0].Found || !old.Results[1].Found || old.Results[2].Found || !bytes.Contains(old.Results[1].Document, []byte(`"content":"alpha"`)) {
		t.Fatalf("old view=%+v err=%v", old, err)
	}
	for _, id := range ids[1:] {
		doc, err := col.Get(id)
		if err != nil || !bytes.Contains(doc, []byte(`"content":"gamma"`)) {
			t.Fatalf("latest %s=%s err=%v", id, doc, err)
		}
	}
	if found, err := col.FindByIndex("user", "beta"); err != nil || len(found) != 0 {
		t.Fatalf("stale scalar rows=%d err=%v", len(found), err)
	}
}

func TestTypedUpsertNoopAndMixedAtomicity(t *testing.T) {
	meta := typedMinimaCollectionMeta()
	meta.Indexes[0].Unique = true
	dir, db, col := openTypedMinimaCollectionMeta(t, meta)
	defer db.Close()
	api, ok := any(col).(interface {
		UpsertTypedBatch([][]byte, [][]byte, []TypedColumnBatch) (int, error)
	})
	if !ok {
		t.Fatal("no-op preserving atomic typed upsert unavailable")
	}
	ids := [][]byte{[]byte("a"), []byte("b")}
	retained := [][]byte{[]byte(`{"id":"a"}`), []byte(`{"id":"b"}`)}
	columns := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}, {0, 1, 0, 0, 0, 0, 0, 0}}},
		{Name: "content", Strings: []string{"alpha", "beta"}},
		{Name: "user", Strings: []string{"u1", "u2"}},
		{Name: "path", Strings: []string{"p1", "p2"}},
	}
	if matched, err := api.UpsertTypedBatch(ids, retained, columns); err != nil || matched != 0 {
		t.Fatalf("initial matched=%d err=%v", matched, err)
	}
	frames := len(collectionCommandWALFrames(t, dir))
	seq, root := dbCommitSeqAndSystemRoot(db)
	if matched, err := api.UpsertTypedBatch(ids, retained, columns); err != nil || matched != 2 {
		t.Fatalf("no-op matched=%d err=%v", matched, err)
	}
	if n := len(collectionCommandWALFrames(t, dir)); n != frames {
		t.Fatalf("no-op appended WAL: %d != %d", n, frames)
	}
	if afterSeq, afterRoot := dbCommitSeqAndSystemRoot(db); afterSeq != seq || afterRoot != root {
		t.Fatal("no-op changed publication authority")
	}
	columns[2].Strings = []string{"u2", "u1"}
	if matched, err := api.UpsertTypedBatch(ids, retained, columns); err != nil || matched != 2 {
		t.Fatalf("unique swap matched=%d err=%v", matched, err)
	}
	// a is unchanged, b changes, c is new: counts include the no-op but the
	// durable frame includes only changed/new inputs.
	ids = append(ids, []byte("c"))
	retained = append(retained, []byte(`{"id":"c"}`))
	columns[0].Float32Vectors = append(columns[0].Float32Vectors, []float32{0, 0, 1, 0, 0, 0, 0, 0})
	columns[1].Strings = []string{"alpha", "changed", "gamma"}
	columns[2].Strings = []string{"u2", "u1", "u3"}
	columns[3].Strings = []string{"p1", "p2", "p3"}
	frames = len(collectionCommandWALFrames(t, dir))
	if matched, err := api.UpsertTypedBatch(ids, retained, columns); err != nil || matched != 2 {
		t.Fatalf("mixed matched=%d err=%v", matched, err)
	}
	if n := len(collectionCommandWALFrames(t, dir)); n != frames+1 {
		t.Fatalf("mixed appended %d frames want one", n-frames)
	}
	accepted := collectionCommandWALFrames(t, dir)
	payload, err := commitlog.DecodeCollectionTypedSourcePayload(accepted[len(accepted)-1].Payload)
	if err != nil || len(payload.DeleteIDs) != 2 || len(payload.Inserted.Documents) != 2 {
		t.Fatalf("mixed frame includes unchanged row: %+v err=%v", payload, err)
	}
	for _, id := range payload.DeleteIDs {
		if bytes.Equal(id, ids[0]) {
			t.Fatal("replay would delete unchanged row")
		}
	}
	before, err := col.Get(ids[0])
	if err != nil {
		t.Fatal(err)
	}
	columns[2].Strings[2] = "u2"
	if _, err := api.UpsertTypedBatch(ids, retained, columns); err == nil {
		t.Fatal("accepted duplicate unique value")
	}
	after, err := col.Get(ids[0])
	if err != nil || !bytes.Equal(before, after) || len(collectionCommandWALFrames(t, dir)) != frames+1 {
		t.Fatal("rejected unique conflict changed state/WAL")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTypedMinimaDB(t, dir)
	defer db.Close()
	col, err = NewCollectionManager(db).OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		got, err := col.Get(id)
		if err != nil || !bytes.Contains(got, []byte(columns[1].Strings[i])) {
			t.Fatalf("reopened %s=%s err=%v", id, got, err)
		}
	}
}

// The service can reuse the atomic source planner for ordinary mixed upsert:
// the same ID list names both replaced and inserted rows, not a source filter.
func TestTypedSourceMixedUpsertContract(t *testing.T) {
	dir, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	ids := [][]byte{[]byte("a"), []byte("b")}
	retained := [][]byte{[]byte(`{"id":"a"}`), []byte(`{"id":"b"}`)}
	columns := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}, {0, 1, 0, 0, 0, 0, 0, 0}}},
		{Name: "content", Strings: []string{"old", "new"}},
		{Name: "user", Strings: []string{"u1", "u2"}},
		{Name: "path", Strings: []string{"p1", "p2"}},
	}
	first := make([]TypedColumnBatch, len(columns))
	for i, column := range columns {
		first[i] = column
		if column.Strings != nil {
			first[i].Strings = column.Strings[:1]
		} else {
			first[i].Float32Vectors = column.Float32Vectors[:1]
		}
	}
	if _, _, err := col.InsertTypedBatchWithStats(ids[:1], retained[:1], first); err != nil {
		t.Fatal(err)
	}
	columns[1].Strings[0] = "replaced"
	before := len(collectionCommandWALFrames(t, dir))
	updated, err := col.ReplaceTypedSourceByID(ids, ids, retained, columns)
	if err != nil || updated != 1 {
		t.Fatalf("mixed upsert updated=%d inserted=%d err=%v", updated, len(ids)-updated, err)
	}
	if got := len(collectionCommandWALFrames(t, dir)); got != before+1 {
		t.Fatalf("mixed upsert WAL frames=%d want=%d", got, before+1)
	}
	for i, id := range ids {
		got, err := col.Get(id)
		if err != nil || !bytes.Contains(got, []byte(columns[1].Strings[i])) {
			t.Fatalf("row %s: %s %v", id, got, err)
		}
	}
	before = len(collectionCommandWALFrames(t, dir))
	_, err = col.ReplaceTypedSourceByID(ids, [][]byte{ids[0], ids[0]}, retained, columns)
	if !errors.Is(err, ErrDuplicateDocumentID) {
		t.Fatalf("duplicate ID err=%v", err)
	}
	if got := len(collectionCommandWALFrames(t, dir)); got != before {
		t.Fatal("duplicate input appended WAL")
	}
	// Unlike ReplaceTypedBatch, the source primitive intentionally creates a
	// version even for identical values. Record this cost before choosing the
	// service integration; do not claim no-op admission has been minimized.
	updated, err = col.ReplaceTypedSourceByID(ids, ids, retained, columns)
	if err != nil || updated != 2 {
		t.Fatalf("repeat upsert matched=%d err=%v", updated, err)
	}
	if got := len(collectionCommandWALFrames(t, dir)); got != before+1 {
		t.Fatalf("repeat source publication frames=%d want=%d", got, before+1)
	}
	t.Log("mixed existing/new uses one WAL frame; duplicate rejects before WAL; identical source replacement creates one additional version")
}

func TestTypedSourceReplacementRejectsNonReplacingPrimary(t *testing.T) {
	meta := typedMinimaCollectionMeta()
	// Reach the primary conflict check without an earlier text-ordinal conflict.
	meta.TextIndexes = nil
	dir, db, col := openTypedMinimaCollectionMeta(t, meta)
	defer db.Close()
	ids := [][]byte{[]byte("a"), []byte("b")}
	docs := [][]byte{[]byte(`{"id":"a"}`), []byte(`{"id":"b"}`)}
	columns := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}, {0, 1, 0, 0, 0, 0, 0, 0}}},
		{Name: "content", Strings: []string{"alpha", "beta"}},
		{Name: "user", Strings: []string{"u1", "u2"}},
		{Name: "path", Strings: []string{"p1", "p2"}},
	}
	if _, err := col.ReplaceTypedSourceByID(nil, ids, docs, columns); err != nil {
		t.Fatal(err)
	}
	frames := len(collectionCommandWALFrames(t, dir))
	seq, root := dbCommitSeqAndSystemRoot(db)
	// a is a valid replacement; b exists outside the replacement set.
	_, err := col.ReplaceTypedSourceByID(ids[:1], ids, docs, columns)
	if !errors.Is(err, ErrDocumentExists) {
		t.Fatalf("non-replacing primary conflict err=%v", err)
	}
	if afterSeq, afterRoot := dbCommitSeqAndSystemRoot(db); afterSeq != seq || afterRoot != root {
		t.Fatal("primary conflict changed publication authority")
	}
	if len(collectionCommandWALFrames(t, dir)) != frames {
		t.Fatal("primary conflict appended WAL")
	}
}

func TestTypedUpsertGroupedPrimaryPreservesInputMapping(t *testing.T) {
	dir, db, col := openTypedMinimaCollection(t)
	defer func() { _ = db.Close() }()
	batch := func(mixed bool) ([][]byte, [][]byte, []TypedColumnBatch) {
		ids, docs := make([][]byte, 96), make([][]byte, 96)
		columns := []TypedColumnBatch{
			{Name: "embedding", Float32Vectors: make([][]float32, 96)},
			{Name: "content", Strings: make([]string, 96)},
			{Name: "user", Strings: make([]string, 96)},
			{Name: "path", Strings: make([]string, 96)},
		}
		for i := range ids {
			n := (i * 37) % len(ids) // Permute adjacent keys across grouped leaves.
			word := "alpha"
			if mixed && n >= 32 {
				word = "beta"
				if n >= 80 {
					n += 16 // Sixteen new rows; the old 80..95 rows remain live.
				}
			}
			ids[i] = fmt.Appendf(nil, "row%03d", n)
			docs[i] = fmt.Appendf(nil, `{"id":"row%03d"}`, n)
			columns[0].Float32Vectors[i] = make([]float32, 8)
			columns[0].Float32Vectors[i][n%8] = 1
			columns[1].Strings[i] = fmt.Sprintf("%s token%03d", word, n)
			columns[2].Strings[i] = fmt.Sprintf("%s%03d", word, n)
			columns[3].Strings[i] = fmt.Sprintf("path%03d", n)
		}
		return ids, docs, columns
	}
	if matched, err := col.UpsertTypedBatch(batch(false)); err != nil || matched != 0 {
		t.Fatalf("initial matched=%d err=%v", matched, err)
	}
	frames := len(collectionCommandWALFrames(t, dir))
	seq, root := dbCommitSeqAndSystemRoot(db)
	if matched, err := col.UpsertTypedBatch(batch(false)); err != nil || matched != 96 {
		t.Fatalf("no-op matched=%d err=%v", matched, err)
	}
	if afterSeq, afterRoot := dbCommitSeqAndSystemRoot(db); afterSeq != seq || afterRoot != root || len(collectionCommandWALFrames(t, dir)) != frames {
		t.Fatal("grouped no-op changed publication or WAL")
	}
	if matched, err := col.UpsertTypedBatch(batch(true)); err != nil || matched != 80 {
		t.Fatalf("mixed matched=%d err=%v", matched, err)
	}
	accepted := collectionCommandWALFrames(t, dir)
	if len(accepted) != frames+1 {
		t.Fatalf("mixed appended %d frames want one", len(accepted)-frames)
	}
	payload, err := commitlog.DecodeCollectionTypedSourcePayload(accepted[len(accepted)-1].Payload)
	if err != nil || len(payload.DeleteIDs) != 64 || len(payload.Inserted.Documents) != 64 {
		t.Fatalf("mixed frame includes unchanged rows: %+v err=%v", payload, err)
	}
	ids, docs, _ := batch(true)
	wantRetained := make(map[string][]byte, 64)
	for i, id := range ids {
		if bytes.Compare(id, []byte("row032")) < 0 {
			continue
		}
		wantRetained[string(id)] = docs[i]
	}
	// WAL encoding sorts IDs; each ID must still retain its own input values.
	for _, id := range payload.DeleteIDs {
		if _, ok := wantRetained[string(id)]; !ok {
			t.Fatalf("mixed frame deletes unchanged ID %q", id)
		}
	}
	for _, doc := range payload.Inserted.Documents {
		if !bytes.Equal(doc.Retained, wantRetained[string(doc.ID)]) {
			t.Fatalf("mixed frame changed input mapping for %q", doc.ID)
		}
	}
	check := func(n int) {
		t.Helper()
		word := "alpha"
		if n >= 32 && (n < 80 || n >= 96) {
			word = "beta"
		}
		id := fmt.Sprintf("row%03d", n)
		got, err := col.Get([]byte(id))
		if err != nil {
			t.Fatal(err)
		}
		vector := make([]float32, 8)
		vector[n%8] = 1
		want, err := json.Marshal(map[string]any{"id": id, "content": fmt.Sprintf("%s token%03d", word, n), "embedding": vector, "meta": map[string]any{"user_id": fmt.Sprintf("%s%03d", word, n), "fpath": fmt.Sprintf("path%03d", n)}})
		if err != nil {
			t.Fatal(err)
		}
		assertJSONEqualM13C(t, got, want)
		found, err := col.FindByIndex("user", fmt.Sprintf("%s%03d", word, n))
		if err != nil || len(found) != 1 {
			t.Fatalf("scalar %s rows=%d err=%v", id, len(found), err)
		}
	}
	for n := 0; n < 112; n++ {
		check(n)
	}
	for word, count := range map[string]int{"alpha": 48, "beta": 64} {
		result, err := col.SearchText(TextSearchOptions{IndexName: "content", Query: word, TopK: 112})
		if err != nil || len(result.Results) != count {
			t.Fatalf("text %s rows=%d want=%d err=%v", word, len(result.Results), count, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTypedMinimaDB(t, dir)
	col, err = NewCollectionManager(db).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{0, 47, 80, 95, 111} {
		check(n)
	}
}

func TestTypedUpsertGroupedPrimaryRejectsMissingIndexState(t *testing.T) {
	dir, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	if _, err := col.UpsertTypedBatch(typedUpsertRecoveryBatch("alpha", 2)); err != nil {
		t.Fatal(err)
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing snapshot")
	}
	defer snap.Close()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	// Present a damaged catalog to the planner without changing stored roots.
	catalog = cloneCatalogWithRootUpdates(catalog, catalog.meta, []string{collectionIndexStateRootName("minima")}, []uint64{0})
	opts, err := collectionPlannerOptionsForDB(db, catalog.meta)
	if err != nil {
		t.Fatal(err)
	}
	plan := &sourceReplacementPlan{snap: snap, catalog: catalog, meta: catalog.meta}
	ids, docs := make([][]byte, 96), make([][]byte, 96)
	for i := range ids {
		ids[i], docs[i] = fmt.Appendf(nil, "missing%03d", i), []byte(`{"changed":true}`)
	}
	ids[0] = []byte("a")
	frames := len(collectionCommandWALFrames(t, dir))
	seq, root := dbCommitSeqAndSystemRoot(db)
	err = col.appendSourceDeleteDeltas(plan, ids, opts, docs)
	if err == nil || err.Error() != `collections: typed scalar old index state missing for "a"` {
		t.Fatalf("missing typed index state err=%v", err)
	}
	if afterSeq, afterRoot := dbCommitSeqAndSystemRoot(db); afterSeq != seq || afterRoot != root || len(collectionCommandWALFrames(t, dir)) != frames {
		t.Fatal("failed planning changed publication or WAL")
	}
}
