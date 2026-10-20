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
