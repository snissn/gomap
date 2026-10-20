package treedb

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

func TestNativeExistingTemplateV1Lifecycle(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) { runNativeTemplateV1(t, profile, t.TempDir(), false) })
	}
}

func TestNativeExistingTemplateV1UnappliedReplay(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
		if p := os.Getenv("GOMAP_TEMPLATE_PROFILE"); p != "" && p != string(profile) {
			continue
		}
		t.Run(string(profile), func(t *testing.T) {
			if dir := os.Getenv("GOMAP_TEMPLATE_DIR"); dir != "" {
				runNativeTemplateV1(t, profile, dir, true)
				return
			}
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestNativeExistingTemplateV1UnappliedReplay$", "-test.v")
			cmd.Env = append(os.Environ(), "GOMAP_TEMPLATE_DIR="+dir, "GOMAP_TEMPLATE_PROFILE="+string(profile))
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 97 || !bytes.Contains(output, []byte("TEMPLATE UNAPPLIED basis=2 accepted=5")) || bytes.Contains(output, []byte("DATA RACE")) {
				t.Fatalf("child: %v\n%s", err, output)
			}
			before := workstats.Read().Replay
			database, err := Open(indexedSliceOptions(profile, dir))
			if err != nil {
				t.Fatal(err)
			}
			after := workstats.Read().Replay
			if after.FramesApplied-before.FramesApplied != 3 || after.FrameErrors != before.FrameErrors {
				t.Fatalf("replay: %+v -> %+v", before, after)
			}
			handle, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(handle)
			col, err := manager.OpenCollection("templates")
			if err != nil {
				t.Fatal(err)
			}
			checkNativeTemplateFinal(t, database, col)
			if applied := indexedCRUDAppliedLSN(t, handle); applied != 5 {
				t.Fatalf("applied=%d", applied)
			}
			cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
			if err = manager.Close(); err != nil {
				t.Fatal(err)
			}
			if err = database.Close(); err != nil {
				t.Fatal(err)
			}
			requirePublicCOWFinalZero(t, cache)
			if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
				t.Fatalf("registry: %+v", s)
			}
		})
	}
}

func nativeTemplateDoc(t *testing.T, source string) []byte {
	t.Helper()
	out, err := collections.EncodeTemplateV1DocumentJSON([]byte(source))
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func checkNativeTemplateJSON(t *testing.T, materializer *collections.StoredDocumentJSONMaterializer, stored []byte, want string) {
	t.Helper()
	if !bytes.HasPrefix(stored, []byte("TD1D")) {
		t.Fatalf("stored prefix=%q", stored[:min(4, len(stored))])
	}
	actual, err := materializer.StoredDocumentJSON(stored)
	if err != nil {
		t.Fatal(err)
	}
	var a, b any
	if err = json.Unmarshal(actual, &a); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal([]byte(want), &b); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("document=%s want%s", actual, want)
	}
}
func checkNativeTemplateFinal(t *testing.T, database *DB, col *collections.Collection) {
	t.Helper()
	raw, err := database.Get([]byte("template/raw"))
	if err != nil || string(raw) != "one cut" {
		t.Fatalf("raw aggregate=%q %v", raw, err)
	}
	raw[0] ^= 1
	if again, err := database.Get([]byte("template/raw")); err != nil || string(again) != "one cut" {
		t.Fatalf("owned raw=%q %v", again, err)
	}
	m, err := col.NewStoredDocumentJSONMaterializer()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for id, want := range map[string]string{
		"a": `{"email":"ub","city":"sea","new":{"tag":"shape"}}`,
		"b": `{"email":"ua","city":"hnl","tags":["x","x","y"]}`,
		"c": `{"email":"uc","city":"sea"}`,
	} {
		got, err := col.Get([]byte(id))
		if err != nil {
			t.Fatal(err)
		}
		checkNativeTemplateJSON(t, m, got, want)
		got[0] ^= 1
	}
	for value, id := range map[string]string{"ua": "b", "ub": "a", "uc": "c"} {
		ids, err := col.FindByIndexValue("email", value)
		if err != nil || !reflect.DeepEqual(ids, [][]byte{[]byte(id)}) {
			t.Fatalf("email/%s=%q %v", value, ids, err)
		}
	}
	ids, err := col.FindByIndexValue("city_backfill", "sea")
	if err != nil || !reflect.DeepEqual(ids, [][]byte{[]byte("a"), []byte("c")}) {
		t.Fatalf("backfill=%q %v", ids, err)
	}
}
func runNativeTemplateV1(t *testing.T, profile Profile, dir string, crash bool) {
	t.Helper()
	opts := indexedSliceOptions(profile, dir)
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	var manager *collections.CollectionManager
	t.Cleanup(func() {
		if manager != nil {
			_ = manager.Close()
		}
		if database != nil {
			_ = database.Close()
		}
	})
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(handle)
	meta := collections.CollectionMeta{Name: "templates", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatTemplateV1}, Indexes: []collections.IndexDefinition{{Name: "email", Field: "email", ValueType: collections.IndexValueString, Unique: true}, {Name: "city", Field: "city", ValueType: collections.IndexValueString}}}
	if _, err = manager.CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("templates")
	if err != nil {
		t.Fatal(err)
	}
	encoder := &collections.TemplateV1Encoder{}
	// Canonical ID order deliberately puts a hash-only document before the
	// same member's full envelope. Maintained batch preparation resolves both.
	seedEncoder := &collections.TemplateV1Encoder{}
	seedB, err := seedEncoder.EncodeDocument([]string{"email", "city"}, []any{"ub", "nyc"})
	if err != nil {
		t.Fatal(err)
	}
	seedA, err := seedEncoder.EncodeDocument([]string{"email", "city"}, []any{"ua", "hnl"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = col.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("a"), []byte("b")}, [][]byte{seedA, seedB}, encoder); err != nil {
		t.Fatal(err)
	}
	if err = database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if profile != ProfileNoWALFast && indexedCRUDAppliedLSN(t, handle) != 2 {
		t.Fatal("seed basis !=2")
	}
	old, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if old != nil {
			_ = old.Close()
		}
	}()
	oldMaterializer, err := col.NewStoredDocumentJSONMaterializer()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if oldMaterializer != nil {
			_ = oldMaterializer.Close()
		}
	}()
	compact, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"uc", "sea"})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(compact, []byte("TD1D")) {
		t.Fatal("successful insert did not teach compact IDs")
	}
	if _, err = col.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("c")}, [][]byte{compact}, encoder); err != nil {
		t.Fatal(err)
	}
	native, err := collections.NewNativeJSONManager(handle)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := native.Begin("templates")
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	finalA := nativeTemplateDoc(t, `{"email":"ub","city":"sea","new":{"tag":"shape"}}`)
	if _, err = tx.UpdateBatch([]collections.UpdateBatchItem{{DocumentID: []byte("a"), Update: func([]byte) ([]byte, bool, error) { return finalA, true, nil }}}); err != nil {
		t.Fatal(err)
	}
	if n, err := tx.DeleteBatch([][]byte{[]byte("b")}); err != nil || n != 1 {
		t.Fatalf("delete=%d %v", n, err)
	}
	// The first carrier of this shape disappears. The following TD1H member
	// still needs its staged records in foreground and original envelope replay.
	var staged collections.TemplateV1Encoder
	transient, err := staged.EncodeDocument([]string{"city", "email", "tags"}, []any{"sea", "temporary", []any{"x"}})
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.InsertBatch([][]byte{[]byte("transient")}, [][]byte{transient}); err != nil {
		t.Fatal(err)
	}
	if count, err := tx.DeleteBatch([][]byte{[]byte("transient")}); err != nil || count != 1 {
		t.Fatalf("cancel template carrier=%d %v", count, err)
	}
	withoutRecords, err := staged.EncodeDocument([]string{"city", "email", "tags"}, []any{"hnl", "ua", []any{"x", "x", "y"}})
	if err != nil || !bytes.HasPrefix(withoutRecords, []byte("TD1H")) {
		t.Fatalf("hash-only staged input=%q %v", withoutRecords, err)
	}
	if err = tx.InsertBatch([][]byte{[]byte("b")}, [][]byte{withoutRecords}); err != nil {
		t.Fatal(err)
	}
	if err = tx.SetRaw([]byte("template/raw"), []byte("one cut")); err != nil {
		t.Fatal(err)
	}
	if got, err := tx.Get([]byte("a")); err != nil || !bytes.HasPrefix(got, []byte("TD1D")) {
		t.Fatalf("pending stored=%q %v", got, err)
	} else {
		got[0] ^= 1
	}
	next := handle.CommandWALNextLSN()
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = tx.Close(); err != nil {
		t.Fatal(err)
	}
	if profile != ProfileNoWALFast && handle.CommandWALNextLSN() != next+1 {
		t.Fatal("aggregate identity")
	}
	if _, err = col.CreateIndex(collections.IndexDefinition{Name: "city_backfill", Field: "city", ValueType: collections.IndexValueString}); err != nil {
		t.Fatal(err)
	}
	checkNativeTemplateFinal(t, database, col)
	next = handle.CommandWALNextLSN()
	bad, err := native.Begin("templates")
	if err != nil {
		t.Fatal(err)
	}
	err = bad.InsertBatch([][]byte{[]byte("bad")}, [][]byte{[]byte("TD1I\x01")})
	if err == nil {
		t.Fatal("malformed template accepted")
	}
	// A later delete cannot erase the malformed member or make it publishable.
	if _, err = bad.DeleteBatch([][]byte{[]byte("bad")}); err == nil {
		t.Fatal("malformed intermediate was erased by delete")
	}
	if err = bad.Commit(); err == nil {
		t.Fatal("malformed intermediate committed")
	}
	if err = bad.Close(); err != nil {
		t.Fatal(err)
	}
	if handle.CommandWALNextLSN() != next {
		t.Fatal("malformed input acquired identity")
	}
	next = handle.CommandWALNextLSN()
	if _, err = col.InsertBatch([][]byte{[]byte("conflict")}, [][]byte{nativeTemplateDoc(t, `{"email":"ua","city":"sea"}`)}); !errors.Is(err, collections.ErrUniqueIndexConflict) {
		t.Fatalf("unique conflict=%v", err)
	}
	if handle.CommandWALNextLSN() != next {
		t.Fatal("unique refusal acquired identity")
	}
	checkOld := func() {
		t.Helper()
		response, err := old.FetchDocumentsByID([][]byte{[]byte("a"), []byte("c")}, collections.DocumentFetchOptions{})
		if err != nil || len(response.Results) != 2 || !response.Results[0].Found || response.Results[1].Found {
			t.Fatalf("old view: %+v %v", response, err)
		}
		checkNativeTemplateJSON(t, oldMaterializer, response.Results[0].Document, `{"email":"ua","city":"hnl"}`)
	}
	checkOld()
	if crash {
		if err = handle.FlushCommandWAL(true); err != nil {
			t.Fatal(err)
		}
		if applied, accepted := indexedCRUDAppliedLSN(t, handle), handle.CommandWALNextLSN()-1; applied != 2 || accepted != 5 {
			t.Fatalf("basis=%d accepted=%d", applied, accepted)
		}
		t.Log("TEMPLATE UNAPPLIED basis=2 accepted=5")
		os.Exit(97)
	}
	if err = database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkNativeTemplateFinal(t, database, col)
	checkOld()
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
	old = nil
	if err = oldMaterializer.Close(); err != nil {
		t.Fatal(err)
	}
	oldMaterializer = nil
	cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager = nil
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	requirePublicCOWFinalZero(t, cache)
	if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
		t.Fatalf("registry: %+v", s)
	}
	database, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	handle, err = database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(handle)
	col, err = manager.OpenCollection("templates")
	if err != nil {
		t.Fatal(err)
	}
	checkNativeTemplateFinal(t, database, col)
	cache, registry = database.cached, handle.ValueLogIdentityPinRegistry()
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager = nil
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	requirePublicCOWFinalZero(t, cache)
	if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
		t.Fatalf("reopened registry: %+v", s)
	}
}

func TestNativeExistingTemplateV1EncoderScope(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	var learned collections.TemplateV1Encoder
	for databaseIndex := 0; databaseIndex < 2; databaseIndex++ {
		func() {
			database, err := Open(indexedSliceOptions(ProfileCommandWALDurable, t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			handle, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(handle)
			defer func() { _ = manager.Close(); _ = database.Close() }()
			for _, name := range []string{"templates", "other"} {
				if _, err = manager.CreateCollection(&collections.CollectionMeta{Name: name, Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatTemplateV1}}); err != nil {
					t.Fatal(err)
				}
			}
			col, err := manager.OpenCollection("templates")
			if err != nil {
				t.Fatal(err)
			}
			if databaseIndex == 0 {
				if _, err = col.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("seed")}, [][]byte{nativeTemplateDoc(t, `{"city":"sea"}`)}, &learned); err != nil {
					t.Fatal(err)
				}
			}
			compact, err := learned.EncodeDocument([]string{"city"}, []any{"hnl"})
			if err != nil || !bytes.HasPrefix(compact, []byte("TD1D")) {
				t.Fatalf("learned compact=%q %v", compact, err)
			}
			name := "other"
			if databaseIndex == 1 {
				name = "templates" // same name, different original backend owner
			}
			target, err := manager.OpenCollection(name)
			if err != nil {
				t.Fatal(err)
			}
			next := handle.CommandWALNextLSN()
			if _, err = target.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("refused")}, [][]byte{compact}, &learned); err == nil {
				t.Fatal("foreign encoder scope accepted")
			}
			if handle.CommandWALNextLSN() != next {
				t.Fatal("scope refusal acquired identity")
			}
			if value, err := target.Get([]byte("refused")); err != nil || value != nil {
				t.Fatalf("scope refusal visible=%q %v", value, err)
			}
			cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
			if err = manager.Close(); err != nil {
				t.Fatal(err)
			}
			if err = database.Close(); err != nil {
				t.Fatal(err)
			}
			requirePublicCOWFinalZero(t, cache)
			if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
				t.Fatalf("scope registry: %+v", stats)
			}
		}()
	}
}
