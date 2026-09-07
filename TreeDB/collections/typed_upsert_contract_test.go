package collections

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

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
