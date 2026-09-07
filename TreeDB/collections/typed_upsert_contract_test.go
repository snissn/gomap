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
