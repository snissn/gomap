package collections

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"reflect"
	"runtime"
	"testing"
	"unsafe"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/page"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Reuse the real relaxed COW owner and checked manager/database cleanup. The
// measured API still owns all staging, publication and returned-ID allocations.
func newNativeUniqueCursorFixture(t testing.TB) *insertPhysicalFixture {
	t.Helper()
	f := newInsertPhysicalFixture(t, 1, false)
	if _, err := f.manager.CreateCollection(&CollectionMeta{Name: "unique_cursor", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}); err != nil {
		t.Fatal(err)
	}
	var err error
	f.c, err = f.manager.OpenCollection("unique_cursor")
	if err != nil || f.c.nativeJSON == nil || !f.manager.UsesNativeCommandWALApply() {
		t.Fatalf("native BSON owner inactive: %v", err)
	}
	return f
}

func nativeUniqueCursorInput(t testing.TB, count int) ([][]byte, [][]byte) {
	t.Helper()
	doc, err := bson.Marshal(bson.D{{Key: "payload", Value: bytes.Repeat([]byte("x"), 1000)}})
	if err != nil {
		t.Fatal(err)
	}
	ids, docs := make([][]byte, count), make([][]byte, count)
	for i := range ids {
		ids[i] = make([]byte, 8)
		binary.BigEndian.PutUint64(ids[i], uint64(i+1))
		docs[i] = doc
	}
	return ids, docs
}

func TestNativeUniqueCursorPublicAllocation(t *testing.T) {
	f := newNativeUniqueCursorFixture(t)
	const requests = 64
	ids, docs := nativeUniqueCursorInput(t, requests+1)
	backend, err := f.owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	beforeLSN := backend.CommandWALNextLSN()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if got, err := f.c.InsertBatchValidatedBSON(ids[:1], docs[:1]); err != nil || len(got) != 1 {
		t.Fatalf("first ACK=%q err=%v", got, err)
	}
	runtime.ReadMemStats(&after)
	t.Logf("first-use public bytes=%d objects=%d cursor-size=%d", after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs, unsafe.Sizeof([page.PageSize + 1]byte{}))
	runtime.ReadMemStats(&before)
	for i := 1; i <= requests; i++ {
		if got, err := f.c.InsertBatchValidatedBSON(ids[i:i+1], docs[i:i+1]); err != nil || len(got) != 1 {
			t.Fatalf("ACK%d=%q err=%v", i, got, err)
		}
	}
	runtime.ReadMemStats(&after)
	bytesPerRequest := (after.TotalAlloc - before.TotalAlloc) / requests
	objectsPerRequest := float64(after.Mallocs-before.Mallocs) / requests
	t.Logf("repeat public bytes/request=%d objects/request=%.3f", bytesPerRequest, objectsPerRequest)
	// Go1.26.0 native relaxed baseline measured 25,793..25,795 B/request
	// across three fresh fixtures. The 23,500 budget excludes the unused
	// 4,864-byte allocator class while leaving over 2 KiB ordinary-path headroom.
	const maxBytesPerRequest = uint64(23500)
	if bytesPerRequest > maxBytesPerRequest {
		t.Errorf("public no-unique allocation=%d B/request exceeds budget=%d", bytesPerRequest, maxBytesPerRequest)
	}
	if backend.CommandWALNextLSN() != beforeLSN+requests+1 {
		t.Fatal("wrong command assignment count")
	}
	for _, i := range []int{0, requests} {
		requireInsertPhysicalValue(t, f.c, ids[i], docs[i])
	}
	if err := f.owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	f.close(t)
	f.open(t)
	f.c, err = f.manager.OpenCollection("unique_cursor")
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{0, requests} {
		requireInsertPhysicalValue(t, f.c, ids[i], docs[i])
	}
}

// Use -benchtime=1000x for a bounded fresh no-index public fixture. Setup/input
// construction and checked checkpoint/Close/reopen remain outside the timer.
func BenchmarkNativeUniqueCursorPublic(b *testing.B) {
	f := newNativeUniqueCursorFixture(b)
	ids, docs := nativeUniqueCursorInput(b, b.N)
	backend, err := f.owner.NativeSemanticBackend()
	if err != nil {
		b.Fatal(err)
	}
	before := backend.CommandWALNextLSN()
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if got, err := f.c.InsertBatchValidatedBSON(ids[i:i+1], docs[i:i+1]); err != nil || len(got) != 1 {
			b.Fatalf("ACK=%q err=%v", got, err)
		}
	}
	b.StopTimer()
	if backend.CommandWALNextLSN() != before+uint64(b.N) {
		b.Fatal("wrong command assignment count")
	}
	for _, i := range []int{0, b.N - 1} {
		requireInsertPhysicalValue(b, f.c, ids[i], docs[i])
	}
	if err := f.owner.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	f.close(b)
	f.open(b)
	f.c, err = f.manager.OpenCollection("unique_cursor")
	if err != nil {
		b.Fatal(err)
	}
	for _, i := range []int{0, b.N - 1} {
		requireInsertPhysicalValue(b, f.c, ids[i], docs[i])
	}
}

// Allocation controls use an original real native binding; an empty changed set
// isolates only cursor ownership, without replacing the public regression.
func TestNativeUniqueCursorHelperAllocation(t *testing.T) {
	f := newNativeUniqueCursorFixture(t)
	tx, err := f.c.nativeJSON.Begin(f.c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tx.Close(); err != nil {
			t.Error(err)
		}
	}()
	g := &tx.bindings[0]
	for _, tc := range []struct {
		name     string
		runtimes []indexRuntime
		want     float64
	}{
		{"zero", nil, 0},
		{"nonunique", []indexRuntime{{def: indexDefinition{name: "a"}}, {def: indexDefinition{name: "b"}}}, 0},
		{"multiple-unique", []indexRuntime{{def: indexDefinition{name: "a", unique: true}}, {def: indexDefinition{name: "b", unique: true}}}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			if err := tx.checkNativeUpdateUnique(g, tc.runtimes, nil, nil); err != nil {
				t.Fatal(err)
			}
			runtime.ReadMemStats(&after)
			t.Logf("first bytes=%d objects=%d", after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs)
			got := testing.AllocsPerRun(100, func() {
				if err := tx.checkNativeUpdateUnique(g, tc.runtimes, nil, nil); err != nil {
					panic(err)
				}
			})
			if got != tc.want {
				t.Fatalf("cursor allocs=%v want=%v", got, tc.want)
			}
		})
	}
}

func TestNativeUniqueCursorBSONSemantics(t *testing.T) {
	for _, mode := range []string{"zero", "nonunique", "two-unique"} {
		t.Run(mode, func(t *testing.T) {
			f := newNativeUniqueCursorFixture(t)
			indexes := []IndexDefinition{}
			if mode != "zero" {
				indexes = []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: mode == "two-unique"}, {Name: "city", Field: "city", ValueType: IndexValueString, Unique: mode == "two-unique"}}
			}
			for _, index := range indexes {
				if _, err := f.c.CreateIndex(index); err != nil {
					t.Fatal(err)
				}
			}
			doc := func(email, city string) []byte {
				raw, err := bson.Marshal(bson.D{{Key: "email", Value: email}, {Key: "city", Value: city}})
				if err != nil {
					t.Fatal(err)
				}
				return raw
			}
			a, b := doc("a", "east"), doc("b", "west")
			if _, err := f.c.InsertBatchValidatedBSON([][]byte{[]byte("a"), []byte("b")}, [][]byte{a, b}); err != nil {
				t.Fatal(err)
			}
			if _, err := f.c.InsertBatchValidatedBSON([][]byte{[]byte("a")}, [][]byte{doc("other", "other")}); !errors.Is(err, ErrDocumentExists) {
				t.Fatalf("duplicate ID=%v", err)
			}
			requireInsertPhysicalValue(t, f.c, []byte("a"), a)
			if mode == "two-unique" {
				// Empty-domain first insertion above and checkpointed physical conflicts
				// below share exactly the original captured uniqueness scan.
				if err := f.owner.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				f.close(t)
				f.open(t)
				var err error
				f.c, err = f.manager.OpenCollection("unique_cursor")
				if err != nil {
					t.Fatal(err)
				}
				for _, bad := range [][]byte{doc("a", "new"), doc("new", "east")} {
					if _, err := f.c.InsertBatchValidatedBSON([][]byte{[]byte("c")}, [][]byte{bad}); !errors.Is(err, ErrUniqueIndexConflict) {
						t.Fatalf("unique insert=%v", err)
					}
				}
				if _, _, err := f.c.Update([]byte("b"), func([]byte) ([]byte, bool, error) { return doc("a", "west"), true, nil }); !errors.Is(err, ErrUniqueIndexConflict) {
					t.Fatalf("unique update=%v", err)
				}
				requireInsertPhysicalValue(t, f.c, []byte("b"), b)
				// Whole-batch owners permit b to acquire values released by a.
				a, b = doc("c", "north"), doc("a", "east")
				results, err := f.c.UpdateBatch([]UpdateBatchItem{
					{DocumentID: []byte("a"), Update: func([]byte) ([]byte, bool, error) { return a, true, nil }},
					{DocumentID: []byte("b"), Update: func([]byte) ([]byte, bool, error) { return b, true, nil }},
				})
				if err != nil || len(results) != 2 || !results[0].Modified || !results[1].Modified {
					t.Fatalf("handoff=%+v/%v", results, err)
				}
			} else {
				b = doc("a", "south")
				if matched, modified, err := f.c.Update([]byte("b"), func([]byte) ([]byte, bool, error) { return b, true, nil }); err != nil || !matched || !modified {
					t.Fatalf("nonunique update=%v/%v/%v", matched, modified, err)
				}
			}
			verify := func() {
				t.Helper()
				requireInsertPhysicalValue(t, f.c, []byte("a"), a)
				requireInsertPhysicalValue(t, f.c, []byte("b"), b)
				if mode != "zero" {
					want := 1
					if mode == "nonunique" {
						want = 2
					}
					got, err := f.c.FindByIndexValue("email", "a")
					if err != nil || len(got) != want {
						t.Fatalf("index parity=%q/%v want=%d", got, err, want)
					}
				}
			}
			verify()
			if err := f.owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			f.close(t)
			f.open(t)
			var err error
			f.c, err = f.manager.OpenCollection("unique_cursor")
			if err != nil {
				t.Fatal(err)
			}
			verify()
		})
	}
}

func TestNativeUniqueCursorTypedSemantics(t *testing.T) {
	// Typed native publication retains its existing durable-profile boundary.
	f := &insertPhysicalFixture{opts: treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())}
	f.opts.MemtableMode = "cow_btree"
	f.opts.BackgroundCheckpointInterval = -1
	f.opts.BackgroundCheckpointIdleDuration = -1
	f.opts.ValueLog.PointerThreshold = 1
	f.open(t)
	t.Cleanup(func() { f.close(t) })
	meta := r1MutationMeta5059(true)
	// The legacy R1 metadata defaults column ownership; the native indexed
	// string carrier requires the maintained explicit RowAsset owners.
	for i := range meta.Options.ColumnStore.Columns {
		meta.Options.ColumnStore.Columns[i].Owner = TypedStorageOwnerRowAsset
	}
	if _, err := f.manager.CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	c, err := f.manager.OpenCollection(meta.Name)
	if err != nil || c.nativeJSON == nil {
		t.Fatalf("typed native=%v", err)
	}
	rows := []map[string]any{r1MutationRow5059(0), r1MutationRow5059(1)}
	ids, retained, columns := r1MutationBatch5059(t, rows...)
	if _, _, err := c.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	verify := func() {
		t.Helper()
		for i, id := range ids {
			raw, err := c.Get(id)
			if err != nil {
				t.Fatal(err)
			}
			var got map[string]any
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, rows[i]) {
				t.Fatalf("typed reconstruction=%v want=%v", got, rows[i])
			}
		}
	}
	verify()
	bad := r1MutationCopy5059(rows[1])
	bad["email"] = rows[0]["email"]
	badIDs, badRetained, badColumns := r1MutationBatch5059(t, bad)
	if _, err := c.ReplaceTypedBatch(badIDs, badRetained, badColumns); !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("typed unique conflict=%v", err)
	}
	verify()
	rows[1]["city"] = "changed-nonunique"
	updateIDs, updateRetained, updateColumns := r1MutationBatch5059(t, rows[1])
	if result, err := c.ReplaceTypedBatch(updateIDs, updateRetained, updateColumns); err != nil || len(result) != 1 || !result[0].Modified {
		t.Fatalf("typed nonunique update=%v/%v", result, err)
	}
	verify()
	if err := f.owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	f.close(t)
	f.open(t)
	c, err = f.manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	verify()
}
