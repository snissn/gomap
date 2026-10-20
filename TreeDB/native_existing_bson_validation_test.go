package treedb

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Exercise the public admission/copy boundary with actual V2 numeric expansion
// and escaped ID bytes, rather than asserting a private size helper's formula.
func TestNativeExistingBSONValidationAndOwnedFetch(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	database, err := Open(indexedSliceOptions(ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(handle)
	cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
		if err := database.Close(); err != nil {
			t.Error(err)
		}
		requirePublicCOWFinalZero(t, cache)
		if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
			t.Errorf("registry: %+v", stats)
		}
	})
	if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "bson", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}, Indexes: []collections.IndexDefinition{
		{Name: "label", Field: "label", ValueType: collections.IndexValueBSONOrderedV2, Unique: true},
		{Name: "number", Field: "number", ValueType: collections.IndexValueBSONOrderedV2},
	}}); err != nil {
		t.Fatal(err)
	}
	collection, err := manager.OpenCollection("bson")
	if err != nil {
		t.Fatal(err)
	}
	id := []byte{'a', 0, 'b', 0}
	label := strings.Repeat("x", 1024) + "\x00tail"
	document := nativeBSONDocument(t, bson.D{{Key: "_id", Value: "a"}, {Key: "label", Value: label}, {Key: "number", Value: math.SmallestNonzeroFloat64}})
	sequence := func() uint64 {
		capture, err := handle.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		seq := capture.LogicalSequence()
		if err := capture.Close(); err != nil {
			t.Fatal(err)
		}
		return seq
	}
	next, seq := handle.CommandWALNextLSN(), sequence()
	state, ok := handle.StateToken()
	if !ok {
		t.Fatal("missing physical state")
	}
	unchanged := func() {
		t.Helper()
		after, ok := handle.StateToken()
		if !ok || after != state || handle.CommandWALNextLSN() != next || sequence() != seq {
			t.Fatal("refused/advisory operation advanced an original frontier")
		}
	}
	if _, err := collection.InsertBatch([][]byte{id}, [][]byte{document[:len(document)-1]}); err == nil {
		t.Fatal("invalid BSON accepted")
	}
	unchanged()
	if err := manager.PreflightNativeCommandWALMutation("bson", collections.DocumentFormatJSON, commitlog.CommandKindCollectionInsertBatchByID, [][]byte{id}, [][]byte{document}); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("request/captured format mismatch: %v", err)
	}
	unchanged()
	if err := manager.PreflightNativeCommandWALMutation("bson", collections.DocumentFormatBSON, commitlog.CommandKindCollectionInsertBatchByID, [][]byte{id}, [][]byte{document}); err != nil {
		t.Fatal(err)
	}
	unchanged()
	if _, err := collection.InsertBatch([][]byte{id}, [][]byte{document}); err != nil {
		t.Fatal(err)
	}
	// Replacement preserves the stored BSON _id independently of the primary ID.
	next, seq = handle.CommandWALNextLSN(), sequence()
	state, ok = handle.StateToken()
	if !ok {
		t.Fatal("missing inserted state")
	}
	changedID := nativeBSONDocument(t, bson.D{{Key: "_id", Value: "changed"}, {Key: "label", Value: label}, {Key: "number", Value: math.SmallestNonzeroFloat64}})
	if _, err := collection.UpdateBatch([]collections.UpdateBatchItem{{DocumentID: id, Update: func([]byte) ([]byte, bool, error) { return changedID, true, nil }}}); err == nil {
		t.Fatal("BSON replacement changed _id")
	}
	unchanged()
	if err := manager.PreflightNativeCommandWALMutation("bson", collections.DocumentFormatBSON, commitlog.CommandKindCollectionUpdateBatchByID, [][]byte{id}, [][]byte{changedID}); err == nil {
		t.Fatal("advisory BSON replacement changed _id")
	}
	unchanged()
	for _, physical := range []bool{false, true} {
		if physical {
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
		}
		view, err := collection.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
		func() {
			defer func() {
				if err := view.Close(); err != nil {
					t.Error(err)
				}
			}()
			for _, format := range []collections.DocumentFormat{collections.DocumentFormatDefault, collections.DocumentFormatBSON} {
				response, err := view.FetchDocumentsByID([][]byte{id}, collections.DocumentFetchOptions{Format: format})
				if err != nil || len(response.Results) != 1 || !response.Results[0].Found || !bytes.Equal(response.Results[0].Document, document) {
					t.Fatalf("owned raw BSON physical=%v format=%q: %+v %v", physical, format, response, err)
				}
				response.Results[0].Document[0] ^= 0x80
			}
			if _, err := view.FetchDocumentsByID([][]byte{id}, collections.DocumentFetchOptions{Format: collections.DocumentFormatJSON}); err == nil {
				t.Fatal("mismatched fetch format accepted")
			}
			if _, err := view.FetchDocumentsByID([][]byte{id}, collections.DocumentFetchOptions{Format: collections.DocumentFormatBSON, ExcludePaths: []string{"label"}}); err == nil {
				t.Fatal("BSON JSON-projection accepted")
			}
		}()
		for name, value := range map[string]any{"label": label, "number": math.SmallestNonzeroFloat64} {
			raw := nativeBSONQuery(t, value)
			ids, err := collection.FindByIndexValue(name, raw)
			if err != nil || len(ids) != 1 || !bytes.Equal(ids[0], id) {
				t.Fatalf("V2 %s escaped ID physical=%v: %q %v", name, physical, ids, err)
			}
		}
		if got, err := collection.Get(id); err != nil || !bytes.Equal(got, document) {
			t.Fatalf("returned BSON mutation reached original: %v", err)
		}
	}
}

// The same public BSON collection must support structured updates and the two
// conditional update contracts, including decline BEFORE any canonical identity.
func TestNativeExistingBSONSetConditionalLifecycle(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := indexedSliceOptions(profile, t.TempDir())
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
			if _, err = manager.CreateCollection(&collections.CollectionMeta{Name: "users", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}, Indexes: []collections.IndexDefinition{
				{Name: "email", Field: "email", ValueType: collections.IndexValueString, Unique: true},
				{Name: "city", Field: "city", ValueType: collections.IndexValueString},
				{Name: "number", Field: "number", ValueType: collections.IndexValueBSONOrderedV2, Unique: true},
			}}); err != nil {
				t.Fatal(err)
			}
			users, err := manager.OpenCollection("users")
			if err != nil {
				t.Fatal(err)
			}
			doc := func(id, email, city string, number any) []byte {
				return nativeBSONDocument(t, bson.D{{Key: "_id", Value: id}, {Key: "email", Value: email}, {Key: "city", Value: city}, {Key: "number", Value: number}})
			}
			original := [][]byte{doc("u1", "a", "hnl", int32(1)), doc("u2", "b", "sea", int64(2))}
			ids := [][]byte{[]byte("u1"), []byte("u2")}
			if _, err = users.InsertBatch(ids, original); err != nil {
				t.Fatal(err)
			}
			if err = database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			old, err := users.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = old.Close() })
			raw := func(v any) bson.RawValue { return nativeBSONQuery(t, v) }
			fields := func(key string, v any) []collections.BSONSetField {
				return []collections.BSONSetField{{Key: key, Value: raw(v)}}
			}
			sequence := func() uint64 {
				capture, e := handle.CaptureNativeSemanticState()
				if e != nil {
					t.Fatal(e)
				}
				seq := capture.LogicalSequence()
				if e = capture.Close(); e != nil {
					t.Fatal(e)
				}
				return seq
			}
			noIdentity := func(run func()) {
				t.Helper()
				next, seq := handle.CommandWALNextLSN(), sequence()
				token, ok := handle.StateToken()
				if !ok {
					t.Fatal("missing state")
				}
				run()
				after, ok := handle.StateToken()
				if !ok || after != token || next != handle.CommandWALNextLSN() || seq != sequence() {
					t.Fatal("declined/no-op/refused update claimed identity")
				}
			}
			noIdentity(func() {
				if matched, modified, e := users.UpdateBSONSet([]byte("missing"), fields("city", "bos")); e != nil || matched || modified {
					t.Fatalf("missing=%v/%v %v", matched, modified, e)
				}
				if matched, modified, e := users.UpdateBSONSet(ids[0], fields("city", "hnl")); e != nil || !matched || modified {
					t.Fatalf("noop=%v/%v %v", matched, modified, e)
				}
				calls := 0
				results, complete, e := users.UpdateBatchIfNoSecondaryUniqueIndexes([]collections.UpdateBatchItem{{DocumentID: ids[0], Update: func([]byte) ([]byte, bool, error) { calls++; return nil, false, nil }}})
				if e != nil || complete || calls != 0 || len(results) != 1 || results[0] != (collections.UpdateBatchResult{}) {
					t.Fatalf("unique schema decline=%+v/%v callbacks=%d %v", results, complete, calls, e)
				}
				results, complete, e = users.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]collections.BSONSetUpdateBatchItem{{DocumentID: ids[0], Fields: fields("email", "free")}})
				if e != nil || complete || len(results) != 1 || results[0] != (collections.UpdateBatchResult{}) {
					t.Fatalf("set unique change decline=%+v/%v %v", results, complete, e)
				}
				replacement := doc("u1", "free", "hnl", int32(1))
				results, complete, e = users.UpdateBatchIfNoSecondaryUniqueIndexChanges([]collections.UpdateBatchItem{{DocumentID: ids[0], Update: func([]byte) ([]byte, bool, error) { return replacement, true, nil }}})
				if e != nil || complete || len(results) != 1 || results[0] != (collections.UpdateBatchResult{}) {
					t.Fatalf("callback unique change decline=%+v/%v %v", results, complete, e)
				}
				if _, _, e = users.UpdateBSONSet(ids[0], fields("email", "b")); !errors.Is(e, collections.ErrUniqueIndexConflict) {
					t.Fatalf("unique conflict=%v", e)
				}
				for _, invalid := range [][]collections.BSONSetField{fields("_id", "other"), fields("nested.path", 1), {{Key: "city", Value: bson.RawValue{Type: bson.TypeString, Value: []byte{1}}}}} {
					if _, _, e := users.UpdateBSONSet(ids[0], invalid); e == nil {
						t.Fatal("invalid set accepted")
					}
				}
			})
			// Numeric equality follows the maintained V2 key, not BSON type-byte equality:
			// int32(1)->int64(1) modifies the document but not the unique index value.
			results, complete, err := users.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]collections.BSONSetUpdateBatchItem{{DocumentID: ids[0], Fields: fields("number", int64(1))}})
			if err != nil || !complete || len(results) != 1 || !results[0].Matched || !results[0].Modified {
				t.Fatalf("V2 equivalent conditional=%+v/%v %v", results, complete, err)
			}
			results, complete, err = users.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]collections.BSONSetUpdateBatchItem{{DocumentID: ids[0], Fields: fields("city", "bos")}, {DocumentID: ids[1], Fields: fields("city", "bos")}})
			if err != nil || !complete || len(results) != 2 || !results[0].Modified || !results[1].Modified {
				t.Fatalf("nonunique conditional=%+v/%v %v", results, complete, err)
			}
			// Ordinary structured updates can release and hand off a unique owner.
			for _, update := range []struct{ id, email string }{{"u1", "free"}, {"u2", "a"}, {"u1", "b"}} {
				if matched, modified, e := users.UpdateBSONSet([]byte(update.id), fields("email", update.email)); e != nil || !matched || !modified {
					t.Fatalf("handoff=%v/%v %v", matched, modified, e)
				}
			}
			want := [][]byte{doc("u1", "b", "bos", int64(1)), doc("u2", "a", "bos", int64(2))}
			check := func() {
				t.Helper()
				for i, id := range ids {
					got, e := users.Get(id)
					if e != nil || !bytes.Equal(got, want[i]) {
						t.Fatalf("current BSON %s=%x want %x err=%v", id, got, want[i], e)
					}
					got[0] ^= 1
				}
				checkNativeBSONIndex(t, users, "email", "a", "u2")
				checkNativeBSONIndex(t, users, "email", "b", "u1")
				checkNativeBSONIndex(t, users, "city", "bos", "u1", "u2")
			}
			check()
			checkOld := func() {
				t.Helper()
				response, e := old.FetchDocumentsByID(ids, collections.DocumentFetchOptions{})
				if e != nil || len(response.Results) != 2 {
					t.Fatalf("held=%+v %v", response, e)
				}
				for i, row := range response.Results {
					if !row.Found || !bytes.Equal(row.Document, original[i]) {
						t.Fatalf("held original row%d changed", i)
					}
					row.Document[0] ^= 1
				}
			}
			checkOld()
			if err = database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			check()
			checkOld()
			if err = old.Close(); err != nil {
				t.Fatal(err)
			}
			if err = manager.Close(); err != nil {
				t.Fatal(err)
			}
			manager = nil
			cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
			if err = database.Close(); err != nil {
				t.Fatal(err)
			}
			database = nil
			requirePublicCOWFinalZero(t, cache)
			if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
				t.Fatalf("registry=%+v", s)
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
			users, err = manager.OpenCollection("users")
			if err != nil {
				t.Fatal(err)
			}
			check()
			// No unique schema: the original conditional callback API must take its
			// successful route as well as the schema-based decline exercised above.
			if _, err = users.DropAllIndexes(); err != nil {
				t.Fatal(err)
			}
			results, complete, err = users.UpdateBatchIfNoSecondaryUniqueIndexes([]collections.UpdateBatchItem{{DocumentID: ids[0], Update: func(current []byte) ([]byte, bool, error) { return current, false, nil }}})
			if err != nil || !complete || len(results) != 1 || !results[0].Matched || results[0].Modified {
				t.Fatalf("no-unique success=%+v/%v %v", results, complete, err)
			}
			if err = manager.Close(); err != nil {
				t.Fatal(err)
			}
			manager = nil
			cache, registry = database.cached, handle.ValueLogIdentityPinRegistry()
			if err = database.Close(); err != nil {
				t.Fatal(err)
			}
			database = nil
			requirePublicCOWFinalZero(t, cache)
			if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
				t.Fatalf("reopen registry=%+v", s)
			}
		})
	}
}
