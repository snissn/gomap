package collections

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestTypedSourceNativeAuthority(t *testing.T) {
	dir, d, c := openTypedMinimaCollection(t)
	defer d.Close()
	api, ok := any(c).(interface {
		ReplaceTypedSourceByID([][]byte, [][]byte, [][]byte, []TypedColumnBatch) (int, error)
	})
	if !ok {
		t.Fatal("atomic typed source API unavailable")
	}
	ids := [][]byte{[]byte("source")}
	retained := [][]byte{[]byte(`{"id":"source","extra":"kept"}`)}
	columns := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}},
		{Name: "content", Strings: []string{"alpha"}},
		{Name: "user", Strings: []string{"u1"}},
		{Name: "path", Strings: []string{"file1"}},
	}
	for i, word := range []string{"alpha", "beta"} {
		columns[1].Strings[0] = word
		before := len(collectionCommandWALFrames(t, dir))
		deleted, err := api.ReplaceTypedSourceByID(ids, ids, retained, columns)
		if err != nil || deleted != i {
			t.Fatalf("replacement deleted=%d want=%d err=%v", deleted, i, err)
		}
		frames := collectionCommandWALFrames(t, dir)
		if len(frames) != before+1 || frames[len(frames)-1].PayloadFormat != commitlog.PayloadFormatCollectionTypedSourceByIDV1 {
			t.Fatal("typed source did not publish exactly one format-12 frame")
		}
		payload, err := commitlog.DecodeCollectionTypedSourcePayload(frames[len(frames)-1].Payload)
		if err != nil {
			t.Fatal(err)
		}
		payload.Inserted.SchemaHash++
		if _, _, _, err := typedProjectionFromPayload(c.Meta(), payload.Inserted); err == nil {
			t.Fatal("accepted source replay schema mismatch")
		}
		got, err := c.Get(ids[0])
		if err != nil || !strings.Contains(string(got), `"content":"`+word+`"`) || !strings.Contains(string(got), `"extra":"kept"`) {
			t.Fatalf("materialized %s: %v", got, err)
		}
		found, err := c.FindByIndex("user", "u1")
		if err != nil || len(found) != 1 {
			t.Fatalf("scalar rows=%d err=%v", len(found), err)
		}
		text, err := c.SearchText(TextSearchOptions{IndexName: "content", Query: word, TopK: 2})
		if err != nil || len(text.Results) != 1 {
			t.Fatalf("text %+v err=%v", text, err)
		}
	}
	old, err := c.SearchText(TextSearchOptions{IndexName: "content", Query: "alpha", TopK: 2})
	if err != nil || len(old.Results) != 0 {
		t.Fatalf("stale text %+v err=%v", old, err)
	}
	var scans atomic.Uint64
	restore := setColumnVectorGraphCanonicalRowsTestHook(func() { scans.Add(1) })
	_, err = c.RebuildVectorIndex("embedding_graph")
	restore()
	if err != nil || scans.Load() != 0 {
		t.Fatalf("rebuild err=%v canonical scans=%d", err, scans.Load())
	}
	result, err := c.SearchVectorIndex(VectorIndexSearchOptions{IndexName: "embedding_graph", Query: columns[0].Float32Vectors[0], TopK: 1, EfSearch: 8, IncludeDocuments: true})
	if err != nil || len(result.Results) != 1 || string(result.Results[0].ID) != "source" || result.Results[0].Score < .999 {
		t.Fatalf("typed graph result=%+v err=%v", result, err)
	}
	columns[1].Strings[0] = "caller changed"
	columns[0].Float32Vectors[0][0] = 0
	got, err := c.Get(ids[0])
	if err != nil || !strings.Contains(string(got), `"content":"beta"`) {
		t.Fatalf("caller mutation changed stored values: %s %v", got, err)
	}
	if n, err := api.ReplaceTypedSourceByID(ids, nil, nil, nil); err != nil || n != 1 {
		t.Fatalf("delete-only n=%d err=%v", n, err)
	}
	frames := collectionCommandWALFrames(t, dir)
	if frames[len(frames)-1].PayloadFormat != commitlog.PayloadFormatCollectionReplaceSourceByIDV1 {
		t.Fatal("delete-only did not reuse format 10")
	}
}

func TestTypedSourceAdmissionAndAtomicity(t *testing.T) {
	meta := typedMinimaCollectionMeta()
	meta.Indexes[0].Unique = true
	dir, d, c := openTypedMinimaCollectionMeta(t, meta)
	defer d.Close()
	ids := [][]byte{[]byte("a"), []byte("b")}
	retained := [][]byte{[]byte(`{"id":"a"}`), []byte(`{"id":"b"}`)}
	columns := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}, {0, 1, 0, 0, 0, 0, 0, 0}}},
		{Name: "content", Strings: []string{"alpha", "beta"}}, {Name: "user", Strings: []string{"u1", "u2"}}, {Name: "path", Strings: []string{"p1", "p2"}},
	}
	if _, err := c.ReplaceTypedSourceByID(nil, ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	for i := range columns {
		if columns[i].Strings != nil {
			columns[i].Strings = columns[i].Strings[:1]
		} else {
			columns[i].Float32Vectors = columns[i].Float32Vectors[:1]
		}
	}
	frames := len(collectionCommandWALFrames(t, dir))
	var appends atomic.Uint64
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeDependencyAppend {
			appends.Add(1)
		}
		return nil
	})
	defer restore()
	for _, mode := range []string{"retained", "schema", "unique"} {
		doc := retained[:1]
		if mode == "retained" {
			doc = [][]byte{[]byte(`{"id":"a","content":"forbidden"}`)}
		}
		if mode == "schema" {
			columns[0].Name = "wrong"
		}
		if mode == "unique" {
			columns[2].Strings[0] = "u2"
			columns[3].Strings[0] = "p2"
		}
		if _, err := c.ReplaceTypedSourceByID(ids[:1], ids[:1], doc, columns); err == nil {
			t.Fatalf("accepted %s", mode)
		}
		columns[0].Name = "embedding"
	}
	if appends.Load() != 0 || len(collectionCommandWALFrames(t, dir)) != frames {
		t.Fatal("rejected source appended dependencies or WAL")
	}
	for i, id := range ids {
		doc, err := c.Get(id)
		want := []string{"alpha", "beta"}[i]
		if err != nil || !strings.Contains(string(doc), `"content":"`+want+`"`) {
			t.Fatalf("partial source mutation: %s %v", doc, err)
		}
	}
}

func TestTypedSourceCrashReplay(t *testing.T) {
	if dir := os.Getenv("GOMAP_TYPED_SOURCE_CRASH_DIR"); dir != "" {
		d := openTypedMinimaDB(t, dir)
		c, err := NewCollectionManager(d).OpenCollection("minima")
		if err != nil {
			t.Fatal(err)
		}
		mode := os.Getenv("GOMAP_TYPED_SOURCE_CRASH_MODE")
		injected := errors.New("typed source WAL cut")
		var fired atomic.Bool
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
		_, err = c.ReplaceTypedSourceByID([][]byte{[]byte("source")}, [][]byte{[]byte("source")}, [][]byte{[]byte(`{"id":"source","extra":"new"}`)}, []TypedColumnBatch{
			{Name: "embedding", Float32Vectors: [][]float32{{0, 1, 0, 0, 0, 0, 0, 0}}},
			{Name: "content", Strings: []string{"beta"}}, {Name: "user", Strings: []string{"u2"}}, {Name: "path", Strings: []string{"file2"}},
		})
		if !fired.Load() || !errors.Is(err, injected) || errors.Is(err, ErrCommitAmbiguous) != (mode == "after_sync") {
			t.Fatalf("fired=%t err=%v", fired.Load(), err)
		}
		os.Exit(0)
	}
	for _, mode := range []string{"before_append", "after_sync"} {
		t.Run(mode, func(t *testing.T) {
			dir, d, c := openTypedMinimaCollection(t)
			_, err := c.ReplaceTypedSourceByID(nil, [][]byte{[]byte("source")}, [][]byte{[]byte(`{"id":"source","extra":"old"}`)}, []TypedColumnBatch{
				{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}},
				{Name: "content", Strings: []string{"alpha"}}, {Name: "user", Strings: []string{"u1"}}, {Name: "path", Strings: []string{"file1"}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestTypedSourceCrashReplay$")
			cmd.Env = append(os.Environ(), "GOMAP_TYPED_SOURCE_CRASH_DIR="+dir, "GOMAP_TYPED_SOURCE_CRASH_MODE="+mode)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("child: %v\n%s", err, out)
			}
			reopened := openTypedMinimaDB(t, dir)
			defer reopened.Close()
			c, err = NewCollectionManager(reopened).OpenCollection("minima")
			if err != nil {
				t.Fatal(err)
			}
			word, user := "alpha", "u1"
			if mode == "after_sync" {
				word, user = "beta", "u2"
			}
			doc, err := c.Get([]byte("source"))
			if err != nil || !strings.Contains(string(doc), `"content":"`+word+`"`) {
				t.Fatalf("recovered %s: %v", doc, err)
			}
			want := []byte(`{"id":"source","extra":"old","embedding":[1,0,0,0,0,0,0,0],"content":"alpha","meta":{"user_id":"u1","fpath":"file1"}}`)
			if mode == "after_sync" {
				want = []byte(`{"id":"source","extra":"new","embedding":[0,1,0,0,0,0,0,0],"content":"beta","meta":{"user_id":"u2","fpath":"file2"}}`)
			}
			assertJSONEqualM13C(t, doc, want)
			found, err := c.FindByIndex("user", user)
			if err != nil || len(found) != 1 {
				t.Fatalf("recovered scalar %d: %v", len(found), err)
			}
			foundText, err := c.SearchText(TextSearchOptions{IndexName: "content", Query: word, TopK: 2})
			if err != nil || len(foundText.Results) != 1 {
				t.Fatalf("recovered text %+v: %v", foundText, err)
			}
		})
	}
}
