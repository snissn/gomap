package collections

import (
	"context"
	"errors"
	"testing"
)

func TestVectorPartitionLiveDocumentContentV1(t *testing.T) {
	meta := CollectionMeta{Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}}
	if !VectorPartitionLiveDocumentProofSupportedV1(meta) {
		t.Fatal("plain JSON document proof was refused")
	}
	for _, tc := range []struct {
		name, expected, current string
		equal, invalid          bool
	}{
		{"semantic", `{"n":1e0,"a":[true,null,{"x":"y"}]}`, " {\"a\":[true,null,{\"x\":\"y\"}],\"n\":1.0} \n", true, false},
		{"large_integer", `{"n":9007199254740992}`, `{"n":9007199254740993}`, false, false},
		{"huge_exponent", `{"n":1e100000000}`, `{"n":10e99999999}`, true, false},
		{"huge_exponent_identical", `{"n":1e100000000}`, `{"n":1e100000000}`, true, false},
		{"negative_zero", `{"n":-0.000e+999}`, `{"n":0}`, true, false},
		{"fraction_scale", `{"n":-0.00120}`, `{"n":-12e-4}`, true, false},
		{"huge_exponent_different", `{"n":1e100000000}`, `{"n":1e99999999}`, false, false},
		{"array_order", `{"a":[1,2]}`, `{"a":[2,1]}`, false, false},
		{"missing_not_null", `{"a":null}`, `{}`, false, false},
		{"type", `{"a":1}`, `{"a":"1"}`, false, false},
		{"trailing_expected", `{} {}`, `{}`, false, true},
		{"trailing_current", `{}`, `{} {}`, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			equal, err := vectorPartitionLiveDocumentEqualV1(meta, []byte("id"), []byte(tc.expected), []byte(tc.current))
			if equal != tc.equal || (err != nil) != tc.invalid {
				t.Fatalf("equal=%v err=%v", equal, err)
			}
		})
	}
	meta.Options.DocumentFormat = DocumentFormatBSON
	if VectorPartitionLiveDocumentProofSupportedV1(meta) {
		t.Fatal("BSON document proof was advertised")
	}
	if _, err := vectorPartitionLiveDocumentEqualV1(meta, []byte("id"), []byte(`{}`), []byte(`{}`)); !errors.Is(err, ErrVectorIndexPartitionLiveUnavailableV1) {
		t.Fatalf("unsupported format error=%v", err)
	}
	meta.Options.DocumentFormat = DocumentFormatJSON
	meta.Options.ColumnStore = &ColumnStoreConfig{
		Enabled: true, RetainedPayload: ColumnRetainedPayloadNonColumn,
		Reconstruction: ColumnReconstructionRetainedPayloadAndColumns,
		ActiveManifest: &ColumnManifestIdentity{}, RecoveryAuthoritativeManifest: &ColumnManifestIdentity{},
		Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueFloat32Vector, VectorDims: 2}},
	}
	if !VectorPartitionLiveDocumentProofSupportedV1(meta) {
		t.Fatal("reconstructable column document proof was refused")
	}
	expected := []byte(`{"embedding":[0.10000000149011612,1],"n":9007199254740993}`)
	current := []byte(`{"n":9007199254740993,"embedding":[0.1,1]}`)
	if equal, err := vectorPartitionLiveDocumentEqualV1(meta, []byte("id"), expected, current); err != nil || !equal {
		t.Fatalf("FP32 reconstruction equal=%v err=%v", equal, err)
	}
	meta.Options.ColumnStore.RetainedPayload = ColumnRetainedPayloadNone
	if VectorPartitionLiveDocumentProofSupportedV1(meta) {
		t.Fatal("dropped column payload proof was advertised")
	}
	if _, err := vectorPartitionLiveDocumentEqualV1(meta, []byte("id"), expected, current); !errors.Is(err, ErrVectorIndexPartitionLiveUnavailableV1) {
		t.Fatalf("dropped payload error=%v", err)
	}
}

func TestVectorPartitionLiveDocumentProofCurrentContentV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, _, manifest := newVectorPartitionLiveProductionFixtureV1(t)
	defer database.Close()
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	id := []byte("content-proof")
	document := []byte(`{"time_us":1,"kind":"proof","did":"proof","embedding":[0.1,1],"metadata":9007199254740992}`)
	if _, err := collection.Insert(id, document); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if status, err := collection.ProveVectorPartitionLiveDocumentV1(t.Context(), manifest, id, document); err != nil || status.Revision == 0 {
			t.Fatalf("unchanged proof status=%+v err=%v", status, err)
		}
	}
	// Retain the exact snapshot/pin pair before later publication. The helper
	// must continue reading that pair, not ordinary Get's newer state.
	unlockSchema := collection.lockCollectionSchemaRead()
	unlockAdmission := collection.lockVectorIndexSynchronousPublicationAdmission()
	oldSnapshot := collection.db.AcquireSnapshot()
	oldPin, pinErr := collection.AcquireVectorPartitionLiveSearchPinV1(manifest)
	unlockAdmission()
	unlockSchema()
	if oldSnapshot == nil {
		if oldPin != nil {
			oldPin.Release()
		}
		t.Fatal("missing retained snapshot")
	}
	defer oldSnapshot.Close()
	if pinErr != nil {
		t.Fatal(pinErr)
	}
	defer oldPin.Release()
	replacement := []byte(`{"time_us":1,"kind":"proof","did":"proof","embedding":[0.1,1],"metadata":9007199254740993}`)
	if matched, modified, err := collection.Update(id, func([]byte) ([]byte, bool, error) {
		return replacement, true, nil
	}); err != nil || !matched || !modified {
		t.Fatalf("replacement matched=%v modified=%v err=%v", matched, modified, err)
	}
	if _, err := collection.ProveVectorPartitionLiveDocumentV1(t.Context(), manifest, id, replacement); err != nil {
		t.Fatalf("replacement must still have complete live coverage: %v", err)
	}
	if _, err := collection.ProveVectorPartitionLiveDocumentV1(t.Context(), manifest, id, document); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("same-vector replacement proof=%v", err)
	}
	if _, err := collection.proveVectorPartitionLiveDocumentAtSnapshotV1(t.Context(), oldSnapshot, oldPin, manifest, id, document); err != nil {
		t.Fatalf("retained old content proof: %v", err)
	}
	if _, err := collection.proveVectorPartitionLiveDocumentAtSnapshotV1(t.Context(), oldSnapshot, oldPin, manifest, id, replacement); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("retained snapshot must not read replacement: %v", err)
	}
	currentPin, err := collection.AcquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer currentPin.Release()
	if _, err := collection.proveVectorPartitionLiveDocumentAtSnapshotV1(t.Context(), oldSnapshot, currentPin, manifest, id, document); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("mixed snapshot/pin revisions must fail: %v", err)
	}
	if err := collection.Delete(id); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.ProveVectorPartitionLiveDocumentV1(t.Context(), manifest, id, document); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("deleted content proof=%v", err)
	}
	if _, err := collection.Insert(id, replacement); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.ProveVectorPartitionLiveDocumentV1(t.Context(), manifest, id, replacement); err != nil {
		t.Fatalf("reinserted content proof: %v", err)
	}
	if _, err := collection.ProveVectorPartitionLiveDocumentV1(t.Context(), manifest, id, document); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("old retry after delete/reinsert proof=%v", err)
	}
	if _, err := collection.proveVectorPartitionLiveDocumentAtSnapshotV1(t.Context(), oldSnapshot, oldPin, manifest, id, document); err != nil {
		t.Fatalf("retained old content after delete/reinsert: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := collection.ProveVectorPartitionLiveDocumentV1(ctx, manifest, id, replacement); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled proof=%v", err)
	}
}
