package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

func TestTypedColumnPublicationCheckpointReopen(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	col := createTypedColumnPartCollection1755(t, d)

	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true,"payload":"alpha"}`),
		[]byte(`{"time_us":2,"kind":"post","score":3.5,"flag":false,"payload":"beta"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	assertTypedColumnManifestShape1755(t, d, col, 1, 1)
	assertTypedColumnLatestRows1755(t, d, col, 1, []typedColumnExpectedRow1755{
		{PrimaryID: 0, Kind: "like", Score: 2.5, Flag: true},
		{PrimaryID: 1, Kind: "post", Score: 3.5, Flag: false},
	})
	got, err := col.Get([]byte("e1"))
	if err != nil {
		_ = d.Close()
		t.Fatalf("Get e1: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true,"payload":"alpha"}`))

	updated, changed, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
		assertJSONEqualM13C(t, current, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true,"payload":"alpha"}`))
		return []byte(`{"time_us":3,"kind":"share","score":6.5,"flag":false,"payload":"alpha2"}`), true, nil
	})
	if err != nil || !updated || !changed {
		_ = d.Close()
		t.Fatalf("Update e1 updated=%v changed=%v err=%v", updated, changed, err)
	}
	assertTypedColumnManifestShape1755(t, d, col, 2, 2)
	got, err = col.Get([]byte("e1"))
	if err != nil {
		_ = d.Close()
		t.Fatalf("Get updated e1: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":3,"kind":"share","score":6.5,"flag":false,"payload":"alpha2"}`))
	got, err = col.Get([]byte("e2"))
	if err != nil {
		_ = d.Close()
		t.Fatalf("Get e2: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":2,"kind":"post","score":3.5,"flag":false,"payload":"beta"}`))

	deleted, err := col.DeleteDocument([]byte("e2"))
	if err != nil || !deleted {
		_ = d.Close()
		t.Fatalf("DeleteDocument e2 deleted=%v err=%v", deleted, err)
	}
	if got, err := col.Get([]byte("e2")); err != nil || got != nil {
		_ = d.Close()
		t.Fatalf("Get deleted e2 got=%s err=%v, want missing", got, err)
	}
	assertTypedColumnManifestShape1755(t, d, col, 3, 2)
	got, err = col.Get([]byte("e1"))
	if err != nil {
		_ = d.Close()
		t.Fatalf("Get e1 after delete: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":3,"kind":"share","score":6.5,"flag":false,"payload":"alpha2"}`))

	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	d = nil

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	assertTypedColumnManifestShape1755(t, reopened, reopenedCol, 3, 2)
	reopenedGot, err := reopenedCol.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("reopened Get e1: %v", err)
	}
	assertJSONEqualM13C(t, reopenedGot, []byte(`{"time_us":3,"kind":"share","score":6.5,"flag":false,"payload":"alpha2"}`))
	if reopenedGot, err := reopenedCol.Get([]byte("e2")); err != nil || reopenedGot != nil {
		t.Fatalf("reopened Get deleted e2 got=%s err=%v, want missing", reopenedGot, err)
	}
}

func TestTypedColumnVectorDensePublicationCheckpointReopen1756(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	col := createTypedColumnVectorPartCollection1756(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("v1"), []byte("v2")}, [][]byte{
		[]byte(`{"embedding":[1,0.5,-0.25],"payload":"alpha"}`),
		[]byte(`{"embedding":[2,3,4],"payload":"beta"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	assertTypedColumnManifestShape1755(t, d, col, 1, 1)
	got, err := col.Get([]byte("v1"))
	if err != nil {
		_ = d.Close()
		t.Fatalf("Get v1: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"embedding":[1,0.5,-0.25],"payload":"alpha"}`))
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("vectors")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	assertTypedColumnManifestShape1755(t, reopened, reopenedCol, 1, 1)
	reopenedGot, err := reopenedCol.Get([]byte("v2"))
	if err != nil {
		t.Fatalf("reopened Get v2: %v", err)
	}
	assertJSONEqualM13C(t, reopenedGot, []byte(`{"embedding":[2,3,4],"payload":"beta"}`))
	reopenedGot, err = reopenedCol.Get([]byte("v1"))
	if err != nil {
		t.Fatalf("reopened Get v1: %v", err)
	}
	assertJSONEqualM13C(t, reopenedGot, []byte(`{"embedding":[1,0.5,-0.25],"payload":"alpha"}`))
}

func TestTypedColumnPublicationCommandWALReopenWithoutCheckpoint(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	col := createTypedColumnPartCollection1755(t, d)
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint baseline collection: %v", err)
	}

	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true,"payload":"alpha"}`),
		[]byte(`{"time_us":2,"kind":"post","score":3.5,"flag":false,"payload":"beta"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, changed, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
		assertJSONEqualM13C(t, current, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true,"payload":"alpha"}`))
		return []byte(`{"time_us":3,"kind":"share","score":6.5,"flag":false,"payload":"alpha2"}`), true, nil
	}); err != nil || !changed {
		_ = d.Close()
		t.Fatalf("Update changed=%v err=%v", changed, err)
	}
	assertTypedColumnManifestShape1755(t, d, col, 2, 2)
	preCloseRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(preCloseRefs) != 2 {
		_ = d.Close()
		t.Fatalf("pre-close typed refs=%+v want two", preCloseRefs)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close without checkpoint: %v", err)
	}
	d = nil

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	assertTypedColumnManifestShape1755(t, reopened, reopenedCol, 2, 2)
	got, err := reopenedCol.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("reopened Get e1: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":3,"kind":"share","score":6.5,"flag":false,"payload":"alpha2"}`))
	got, err = reopenedCol.Get([]byte("e2"))
	if err != nil {
		t.Fatalf("reopened Get e2: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":2,"kind":"post","score":3.5,"flag":false,"payload":"beta"}`))
	reopenedRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, reopened, reopenedCol))
	assertColumnAssetRefsEqual1755(t, preCloseRefs, reopenedRefs)
}

func TestTypedColumnPublicationScalarMatrixCheckpointReopen(t *testing.T) {
	cases := []typedColumnScalarMatrixCase1778{
		{
			name:      "bool",
			column:    "flag",
			valueType: ColumnStoreValueBool,
			docs: [2][]byte{
				[]byte(`{"row_id":1,"flag":true,"payload":"bool-a"}`),
				[]byte(`{"row_id":2,"flag":false,"payload":"bool-b"}`),
			},
			wantValues: []columnDeclaredValue{
				{Type: ColumnStoreValueBool, Present: true, Bool: true},
				{Type: ColumnStoreValueBool, Present: true, Bool: false},
			},
		},
		{
			name:      "int64",
			column:    "metric",
			valueType: ColumnStoreValueInt64,
			docs: [2][]byte{
				[]byte(`{"row_id":1,"metric":42,"payload":"int-a"}`),
				[]byte(`{"row_id":2,"metric":-7,"payload":"int-b"}`),
			},
			wantValues: []columnDeclaredValue{
				{Type: ColumnStoreValueInt64, Present: true, Int64: 42},
				{Type: ColumnStoreValueInt64, Present: true, Int64: -7},
			},
		},
		{
			name:      "float32",
			column:    "ratio",
			valueType: ColumnStoreValueFloat32,
			docs: [2][]byte{
				[]byte(`{"row_id":1,"ratio":1.5,"payload":"float32-a"}`),
				[]byte(`{"row_id":2,"ratio":2.25,"payload":"float32-b"}`),
			},
			wantValues: []columnDeclaredValue{
				{Type: ColumnStoreValueFloat32, Present: true, Float32: 1.5},
				{Type: ColumnStoreValueFloat32, Present: true, Float32: 2.25},
			},
		},
		{
			name:      "double",
			column:    "score",
			valueType: ColumnStoreValueDouble,
			docs: [2][]byte{
				[]byte(`{"row_id":1,"score":2.5,"payload":"double-a"}`),
				[]byte(`{"row_id":2,"score":3.75,"payload":"double-b"}`),
			},
			wantValues: []columnDeclaredValue{
				{Type: ColumnStoreValueDouble, Present: true, Double: 2.5},
				{Type: ColumnStoreValueDouble, Present: true, Double: 3.75},
			},
		},
		{
			name:      "string",
			column:    "kind",
			valueType: ColumnStoreValueString,
			docs: [2][]byte{
				[]byte(`{"row_id":1,"kind":"like","payload":"string-a"}`),
				[]byte(`{"row_id":2,"kind":"post","payload":"string-b"}`),
			},
			wantValues: []columnDeclaredValue{
				{Type: ColumnStoreValueString, Present: true, String: "like"},
				{Type: ColumnStoreValueString, Present: true, String: "post"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
				t.Fatalf("SaveFormatConfig: %v", err)
			}
			d := openCollectionCommandWALDB(t, dir)
			col := createTypedColumnScalarCollection1778(t, d, "events_"+tc.name, tc.column, tc.valueType, ColumnRetainedPayloadNonColumn)
			if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{tc.docs[0], tc.docs[1]}); err != nil {
				_ = d.Close()
				t.Fatalf("InsertBatch: %v", err)
			}
			assertTypedColumnManifestShape1755(t, d, col, 1, 1)
			assertTypedColumnPartFieldValues1778(t, d, col, 1, tc.column, tc.wantValues)
			for i, id := range [][]byte{[]byte("e1"), []byte("e2")} {
				got, err := col.Get(id)
				if err != nil {
					_ = d.Close()
					t.Fatalf("Get %s: %v", id, err)
				}
				assertJSONEqualM13C(t, got, tc.docs[i])
			}
			if err := d.Checkpoint(); err != nil {
				_ = d.Close()
				t.Fatalf("Checkpoint: %v", err)
			}
			if err := d.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			reopened := openCollectionCommandWALDB(t, dir)
			defer func() { _ = reopened.Close() }()
			reopenedCol, err := NewCollectionManager(reopened).OpenCollection("events_" + tc.name)
			if err != nil {
				t.Fatalf("OpenCollection reopened: %v", err)
			}
			assertTypedColumnManifestShape1755(t, reopened, reopenedCol, 1, 1)
			assertTypedColumnPartFieldValues1778(t, reopened, reopenedCol, 1, tc.column, tc.wantValues)
			for i, id := range [][]byte{[]byte("e1"), []byte("e2")} {
				got, err := reopenedCol.Get(id)
				if err != nil {
					t.Fatalf("reopened Get %s: %v", id, err)
				}
				assertJSONEqualM13C(t, got, tc.docs[i])
			}
		})
	}
}

func TestTypedColumnPublicationCommandWALReplayWithoutCheckpoint(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	col := createTypedColumnScalarCollection1778(t, d, "events_replay", "metric", ColumnStoreValueInt64, ColumnRetainedPayloadNonColumn)
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint baseline collection: %v", err)
	}

	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"row_id":1,"metric":10,"payload":"alpha"}`),
		[]byte(`{"row_id":2,"metric":20,"payload":"beta"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, changed, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
		assertJSONEqualM13C(t, current, []byte(`{"row_id":1,"metric":10,"payload":"alpha"}`))
		return []byte(`{"row_id":3,"metric":30,"payload":"alpha2"}`), true, nil
	}); err != nil || !changed {
		_ = d.Close()
		t.Fatalf("Update changed=%v err=%v", changed, err)
	}
	preCloseRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(preCloseRefs) != 2 {
		_ = d.Close()
		t.Fatalf("pre-close typed refs=%+v want two", preCloseRefs)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close without checkpoint: %v", err)
	}
	d = nil

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("events_replay")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	assertTypedColumnManifestShape1755(t, reopened, reopenedCol, 2, 2)
	for id, want := range map[string][]byte{
		"e1": []byte(`{"row_id":3,"metric":30,"payload":"alpha2"}`),
		"e2": []byte(`{"row_id":2,"metric":20,"payload":"beta"}`),
	} {
		got, err := reopenedCol.Get([]byte(id))
		if err != nil {
			t.Fatalf("reopened Get %s: %v", id, err)
		}
		assertJSONEqualM13C(t, got, want)
	}
	reopenedRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, reopened, reopenedCol))
	assertColumnAssetRefsEqual1755(t, preCloseRefs, reopenedRefs)
}

func TestTypedColumnPublicationMissingNullTypeMismatchAtomic(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := createTypedColumnPartCollection1755(t, d)
	insertFailures := []struct {
		name string
		id   []byte
		doc  []byte
	}{
		{name: "missing typed string", id: []byte("missing"), doc: []byte(`{"time_us":1,"score":4.5,"flag":true}`)},
		{name: "null typed double", id: []byte("null"), doc: []byte(`{"time_us":1,"kind":"bad","score":null,"flag":true}`)},
		{name: "typed bool mismatch", id: []byte("mismatch"), doc: []byte(`{"time_us":1,"kind":"bad","score":4.5,"flag":"true"}`)},
	}
	for _, tc := range insertFailures {
		t.Run("insert "+tc.name, func(t *testing.T) {
			_, err := col.InsertBatch([][]byte{tc.id}, [][]byte{tc.doc})
			if !errors.Is(err, ErrColumnDeclaredValueUnsupported) {
				t.Fatalf("InsertBatch err=%v want ErrColumnDeclaredValueUnsupported", err)
			}
			if got, err := col.Get(tc.id); err != nil || got != nil {
				t.Fatalf("Get failed insert got=%s err=%v want missing", got, err)
			}
			if refs := typedColumnPartRefsIfManifest1778(t, d, col); len(refs) != 0 {
				t.Fatalf("typed refs after failed insert=%+v want none", refs)
			}
		})
	}

	if _, err := col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{[]byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`)}); err != nil {
		t.Fatalf("valid InsertBatch: %v", err)
	}
	beforeIdentity, ok := col.ColumnStoreCacheIdentity()
	if !ok || beforeIdentity.ManifestRoot == 0 {
		t.Fatalf("ColumnStoreCacheIdentity=%+v ok=%v want manifest", beforeIdentity, ok)
	}
	beforeRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	beforeDoc, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get before: %v", err)
	}
	for _, tc := range insertFailures {
		t.Run("update "+tc.name, func(t *testing.T) {
			_, _, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
				assertJSONEqualM13C(t, current, beforeDoc)
				return tc.doc, true, nil
			})
			if !errors.Is(err, ErrColumnDeclaredValueUnsupported) {
				t.Fatalf("Update err=%v want ErrColumnDeclaredValueUnsupported", err)
			}
			afterIdentity, ok := col.ColumnStoreCacheIdentity()
			if !ok || afterIdentity != beforeIdentity {
				t.Fatalf("identity after failed update=%+v ok=%v want %+v", afterIdentity, ok, beforeIdentity)
			}
			afterRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
			assertColumnAssetRefsEqual1755(t, beforeRefs, afterRefs)
			afterDoc, err := col.Get([]byte("e1"))
			if err != nil {
				t.Fatalf("Get after failed update: %v", err)
			}
			assertJSONEqualM13C(t, afterDoc, beforeDoc)
		})
	}
}

func TestTypedColumnPublicationRetainedPayloadPolicyMatrix(t *testing.T) {
	cases := []struct {
		name               string
		policy             ColumnRetainedPayloadPolicy
		wantAfterReopen    []byte
		corruptShouldError bool
	}{
		{name: "non_column", policy: ColumnRetainedPayloadNonColumn, wantAfterReopen: []byte(`{"row_id":1,"kind":"alpha","payload":"non_column"}`), corruptShouldError: true},
		{name: "none", policy: ColumnRetainedPayloadNone, wantAfterReopen: []byte(`{"row_id":1,"kind":"alpha"}`), corruptShouldError: true},
		{name: "full", policy: ColumnRetainedPayloadFull, wantAfterReopen: []byte(`{"row_id":1,"kind":"alpha","payload":"full"}`), corruptShouldError: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
				t.Fatalf("SaveFormatConfig: %v", err)
			}
			d := openCollectionCommandWALDB(t, dir)
			col := createTypedColumnScalarCollection1778(t, d, "events_"+tc.name, "kind", ColumnStoreValueString, tc.policy)
			doc := []byte(fmt.Sprintf(`{"row_id":1,"kind":"alpha","payload":%q}`, tc.name))
			if _, err := col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{doc}); err != nil {
				_ = d.Close()
				t.Fatalf("InsertBatch: %v", err)
			}
			typedRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
			if len(typedRefs) != 1 {
				_ = d.Close()
				t.Fatalf("typed refs=%+v want one", typedRefs)
			}
			got, err := col.Get([]byte("e1"))
			if err != nil {
				_ = d.Close()
				t.Fatalf("Get before reopen: %v", err)
			}
			assertJSONEqualM13C(t, got, tc.wantAfterReopen)
			if err := d.Checkpoint(); err != nil {
				_ = d.Close()
				t.Fatalf("Checkpoint: %v", err)
			}
			if err := d.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			reopened := openCollectionCommandWALDB(t, dir)
			defer func() { _ = reopened.Close() }()
			reopenedCol, err := NewCollectionManager(reopened).OpenCollection("events_" + tc.name)
			if err != nil {
				t.Fatalf("OpenCollection reopened: %v", err)
			}
			reopenedGot, err := reopenedCol.Get([]byte("e1"))
			if err != nil {
				t.Fatalf("reopened Get: %v", err)
			}
			assertJSONEqualM13C(t, reopenedGot, tc.wantAfterReopen)
			corruptTypedColumnAssetPayload1755(t, reopened, typedRefs[0])
			corruptGot, err := reopenedCol.Get([]byte("e1"))
			if tc.corruptShouldError {
				if err == nil || corruptGot != nil || !strings.Contains(err.Error(), "typed-column reconstruction") {
					t.Fatalf("corrupt typed part got=%s err=%v want fail-closed reconstruction error", corruptGot, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("RetainedPayloadFull should read compatibility document despite corrupt typed part: %v", err)
			}
			assertJSONEqualM13C(t, corruptGot, tc.wantAfterReopen)
		})
	}
}

func TestTypedColumnPublicationMultiGenerationDictionaryRecode(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	col := createTypedColumnScalarCollection1778(t, d, "events_dict", "kind", ColumnStoreValueString, ColumnRetainedPayloadNonColumn)
	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"row_id":1,"kind":"alpha","payload":"one"}`),
		[]byte(`{"row_id":2,"kind":"beta","payload":"two"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	assertTypedColumnPartStringSet1778(t, d, col, 1, "kind", []string{"alpha", "beta"})

	updates := []UpdateBatchItem{
		{DocumentID: []byte("e2"), Update: func(current []byte) ([]byte, bool, error) {
			assertJSONEqualM13C(t, current, []byte(`{"row_id":2,"kind":"beta","payload":"two"}`))
			return []byte(`{"row_id":20,"kind":"delta","payload":"two-new"}`), true, nil
		}},
		{DocumentID: []byte("e1"), Update: func(current []byte) ([]byte, bool, error) {
			assertJSONEqualM13C(t, current, []byte(`{"row_id":1,"kind":"alpha","payload":"one"}`))
			return []byte(`{"row_id":10,"kind":"gamma","payload":"one-new"}`), true, nil
		}},
	}
	results, err := col.UpdateBatch(updates)
	if err != nil {
		_ = d.Close()
		t.Fatalf("UpdateBatch: %v", err)
	}
	if len(results) != 2 || !results[0].Modified || !results[1].Modified {
		_ = d.Close()
		t.Fatalf("UpdateBatch results=%+v want two modified", results)
	}
	assertTypedColumnPartStringSet1778(t, d, col, 1, "kind", []string{"alpha", "beta"})
	assertTypedColumnPartStringSet1778(t, d, col, 2, "kind", []string{"delta", "gamma"})
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("events_dict")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	for id, want := range map[string][]byte{
		"e1": []byte(`{"row_id":10,"kind":"gamma","payload":"one-new"}`),
		"e2": []byte(`{"row_id":20,"kind":"delta","payload":"two-new"}`),
	} {
		got, err := reopenedCol.Get([]byte(id))
		if err != nil {
			t.Fatalf("reopened Get %s: %v", id, err)
		}
		assertJSONEqualM13C(t, got, want)
	}
}

func TestTypedColumnPublicationBatchUpdateRowIndexMapping(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := createTypedColumnPartCollection1755(t, d)
	ids := [][]byte{[]byte("e0"), []byte("e1"), []byte("e2"), []byte("e3")}
	docs := [][]byte{
		[]byte(`{"time_us":0,"kind":"k0","score":0.5,"flag":true,"payload":"p0"}`),
		[]byte(`{"time_us":1,"kind":"k1","score":1.5,"flag":false,"payload":"p1"}`),
		[]byte(`{"time_us":2,"kind":"k2","score":2.5,"flag":true,"payload":"p2"}`),
		[]byte(`{"time_us":3,"kind":"k3","score":3.5,"flag":false,"payload":"p3"}`),
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	wantByID := map[string]struct {
		doc  []byte
		kind string
	}{
		"e3": {doc: []byte(`{"time_us":30,"kind":"k3-updated","score":30.5,"flag":true,"payload":"p3-new"}`), kind: "k3-updated"},
		"e1": {doc: []byte(`{"time_us":10,"kind":"k1-updated","score":10.5,"flag":true,"payload":"p1-new"}`), kind: "k1-updated"},
	}
	results, err := col.UpdateBatch([]UpdateBatchItem{
		{DocumentID: []byte("e3"), Update: func(current []byte) ([]byte, bool, error) { return wantByID["e3"].doc, true, nil }},
		{DocumentID: []byte("e1"), Update: func(current []byte) ([]byte, bool, error) { return wantByID["e1"].doc, true, nil }},
	})
	if err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	if len(results) != 2 || !results[0].Modified || !results[1].Modified {
		t.Fatalf("UpdateBatch results=%+v want two modified", results)
	}
	visible, err := col.scanColumnPhysicalVisibleRows(nil)
	if err != nil {
		t.Fatalf("scanColumnPhysicalVisibleRows: %v", err)
	}
	typedRows := typedColumnPartRowsForGeneration1778(t, d, col, 2)
	seenUpdated := 0
	for _, row := range visible.Rows {
		want, ok := wantByID[string(row.ID)]
		if !ok {
			continue
		}
		if row.Generation != 2 {
			t.Fatalf("updated row %q generation=%d want 2 row=%+v", string(row.ID), row.Generation, row)
		}
		if row.RowIndex < 0 || row.RowIndex >= len(typedRows) {
			t.Fatalf("updated row %q row_index=%d outside typed rows=%d", string(row.ID), row.RowIndex, len(typedRows))
		}
		if gotKind := typedRows[row.RowIndex].Values["kind"].String; gotKind != want.kind {
			t.Fatalf("updated row %q row_index=%d typed kind=%q want %q typedRows=%+v", string(row.ID), row.RowIndex, gotKind, want.kind, typedRows)
		}
		got, err := col.Get(row.ID)
		if err != nil {
			t.Fatalf("Get %q: %v", string(row.ID), err)
		}
		assertJSONEqualM13C(t, got, want.doc)
		seenUpdated++
	}
	if seenUpdated != len(wantByID) {
		t.Fatalf("seen updated rows=%d want %d visible=%+v", seenUpdated, len(wantByID), visible.Rows)
	}
}

func TestTypedColumnPublicationDeleteDoesNotRequireTypedPart(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	col := createTypedColumnPartCollection1755(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{[]byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`)}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	deleted, err := col.DeleteDocument([]byte("e1"))
	if err != nil || !deleted {
		_ = d.Close()
		t.Fatalf("DeleteDocument deleted=%v err=%v", deleted, err)
	}
	assertTypedColumnManifestShape1755(t, d, col, 2, 1)
	for _, ref := range typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col)) {
		if ref.Generation == 2 {
			_ = d.Close()
			t.Fatalf("delete generation unexpectedly published typed-column ref %+v", ref)
		}
	}
	if got, err := col.Get([]byte("e1")); err != nil || got != nil {
		_ = d.Close()
		t.Fatalf("Get after delete got=%s err=%v want missing", got, err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	assertTypedColumnManifestShape1755(t, reopened, reopenedCol, 2, 1)
	if got, err := reopenedCol.Get([]byte("e1")); err != nil || got != nil {
		t.Fatalf("reopened Get after delete got=%s err=%v want missing", got, err)
	}
}

func TestTypedColumnPublicationSnapshotIsolation(t *testing.T) {
	d, col, _ := setupSingleTypedColumnPart1755(t)
	defer func() { _ = d.Close() }()
	oldSnap := d.AcquireSnapshot()
	if oldSnap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	defer func() { _ = oldSnap.Close() }()
	oldDoc, found := typedColumnDocumentAtSnapshot1755(t, col, oldSnap, []byte("e1"))
	if !found {
		t.Fatalf("old snapshot did not find e1")
	}
	assertJSONEqualM13C(t, oldDoc, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`))
	if _, changed, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
		assertJSONEqualM13C(t, current, oldDoc)
		return []byte(`{"time_us":5,"kind":"share","score":7.5,"flag":false}`), true, nil
	}); err != nil || !changed {
		t.Fatalf("Update changed=%v err=%v", changed, err)
	}
	currentDoc, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("current Get after update: %v", err)
	}
	assertJSONEqualM13C(t, currentDoc, []byte(`{"time_us":5,"kind":"share","score":7.5,"flag":false}`))
	deleted, err := col.DeleteDocument([]byte("e1"))
	if err != nil || !deleted {
		t.Fatalf("DeleteDocument deleted=%v err=%v", deleted, err)
	}
	if got, err := col.Get([]byte("e1")); err != nil || got != nil {
		t.Fatalf("current Get after delete got=%s err=%v want missing", got, err)
	}
	oldDocAgain, found := typedColumnDocumentAtSnapshot1755(t, col, oldSnap, []byte("e1"))
	if !found {
		t.Fatalf("old snapshot lost e1 after update/delete")
	}
	assertJSONEqualM13C(t, oldDocAgain, oldDoc)
}

func TestTypedColumnPublicationManifestCorruptionMatrix(t *testing.T) {
	const namespace = "events/column-assets"
	physical := ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: namespace, Generation: 1, PartID: columnPhysicalRowAssetPartID, FileID: 1, Offset: 0, Length: 64, Checksum: 11}
	typed := ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: namespace, Generation: 1, PartID: typedColumnPartAssetPartID, FileID: 1, Offset: 64, Length: 64, Checksum: 22}
	validRecords := func() []columnManifestRecord {
		return []columnManifestRecord{
			typedColumnManifestPartRecord1778(t, physical, 2, ColumnPublishOperationInsert, physical.Generation, physical.PartID),
			typedColumnManifestPartRecord1778(t, typed, 2, ColumnPublishOperationInsert, typed.Generation, typed.PartID),
		}
	}
	cases := []struct {
		name    string
		records func() []columnManifestRecord
		want    string
	}{
		{
			name: "wrong namespace",
			records: func() []columnManifestRecord {
				bad := typed
				bad.Namespace = "other/namespace"
				return []columnManifestRecord{
					typedColumnManifestPartRecord1778(t, physical, 2, ColumnPublishOperationInsert, physical.Generation, physical.PartID),
					typedColumnManifestPartRecord1778(t, bad, 2, ColumnPublishOperationInsert, bad.Generation, bad.PartID),
				}
			},
			want: "namespace",
		},
		{
			name: "wrong generation",
			records: func() []columnManifestRecord {
				bad := typed
				bad.Generation = 2
				return []columnManifestRecord{
					typedColumnManifestPartRecord1778(t, physical, 2, ColumnPublishOperationInsert, physical.Generation, physical.PartID),
					typedColumnManifestPartRecord1778(t, bad, 2, ColumnPublishOperationInsert, 1, bad.PartID),
				}
			},
			want: "key generation/part mismatch",
		},
		{
			name: "wrong part id",
			records: func() []columnManifestRecord {
				bad := typed
				bad.PartID = 99
				return []columnManifestRecord{
					typedColumnManifestPartRecord1778(t, physical, 2, ColumnPublishOperationInsert, physical.Generation, physical.PartID),
					typedColumnManifestPartRecord1778(t, bad, 2, ColumnPublishOperationInsert, bad.Generation, bad.PartID),
				}
			},
			want: "unexpected part_id=99",
		},
		{
			name: "duplicate typed refs",
			records: func() []columnManifestRecord {
				records := validRecords()
				return append(records, typedColumnManifestPartRecord1778(t, typed, 2, ColumnPublishOperationInsert, typed.Generation, typed.PartID))
			},
			want: "duplicate typed-column manifest ref",
		},
		{
			name: "wrong reason",
			records: func() []columnManifestRecord {
				return []columnManifestRecord{
					typedColumnManifestPartRecord1778(t, physical, 2, ColumnPublishOperationInsert, physical.Generation, physical.PartID),
					typedColumnManifestPartRecordWithReason1778(t, typed, 2, "future", typed.Generation, typed.PartID),
				}
			},
			want: "unsupported typed-column manifest reason",
		},
		{
			name: "missing typed_row_asset ref",
			records: func() []columnManifestRecord {
				return []columnManifestRecord{
					typedColumnManifestPartRecord1778(t, typed, 2, ColumnPublishOperationInsert, typed.Generation, typed.PartID),
				}
			},
			want: "missing typed_row_asset ref for generation=1",
		},
		{
			name: "row count mismatch",
			records: func() []columnManifestRecord {
				return []columnManifestRecord{
					typedColumnManifestPartRecord1778(t, physical, 2, ColumnPublishOperationInsert, physical.Generation, physical.PartID),
					typedColumnManifestPartRecord1778(t, typed, 1, ColumnPublishOperationInsert, typed.Generation, typed.PartID),
				}
			},
			want: "rows=1 does not match physical rows=2",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := typedColumnPartRefsByGenerationFromManifestRecords(tc.records(), namespace)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("typedColumnPartRefsByGenerationFromManifestRecords err=%v want containing %q", err, tc.want)
			}
		})
	}
	t.Run("checksum mismatch", func(t *testing.T) {
		d, col, typedRef := setupSingleTypedColumnPart1755(t)
		defer func() { _ = d.Close() }()
		corruptTypedColumnAssetPayload1755(t, d, typedRef)
		if got, err := col.Get([]byte("e1")); err == nil || got != nil || !strings.Contains(err.Error(), "checksum") {
			t.Fatalf("Get with checksum mismatch got=%s err=%v want checksum fail-closed", got, err)
		}
	})
}

func TestTypedColumnPublicationReadCacheNoLeakDuringScan(t *testing.T) {
	const rows = 16
	col, snap, visibleRows, cfg, manifestRootID := typedColumnReconstructionCacheFixture1781(t, rows)
	cache := &typedColumnPartReconstructionCache{Parts: make(map[uint64]typedColumnPartDecodedValues, 1)}
	for _, row := range visibleRows {
		if _, err := col.typedColumnPartValuesForVisibleRowAtSnapshotWithCache(snap, manifestRootID, cfg, row, cache); err != nil {
			t.Fatalf("typedColumnPartValuesForVisibleRowAtSnapshotWithCache: %v", err)
		}
	}
	if cache.PartLoads != 1 || cache.TypedPartDecodes != 1 || cache.CacheMisses != 1 || cache.CacheHits != rows-1 || len(cache.Parts) != 1 {
		t.Fatalf("cache counters loads=%d decodes=%d hits=%d misses=%d cached_generations=%d want one load/decode/miss/generation and %d hits", cache.PartLoads, cache.TypedPartDecodes, cache.CacheHits, cache.CacheMisses, len(cache.Parts), rows-1)
	}

	d, _, typedRef := setupSingleTypedColumnPart1755(t)
	defer func() { _ = d.Close() }()
	manager := mappedresource.NewManager()
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(d.ColumnAssetRootDir(), typedRef.Namespace, ColumnAssetReadIntegrityVerify)
	if err != nil {
		t.Fatalf("new read cache: %v", err)
	}
	if err := readCache.useMappedResourceManager(manager, mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: "read-cache-no-leak-1778", Namespace: typedRef.Namespace, Generation: typedRef.Generation, Reason: "typed-column publication read-cache test"}, "typed-column publication read-cache test"); err != nil {
		_ = readCache.close()
		t.Fatalf("useMappedResourceManager: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := readCache.read(typedRef, nil); err != nil {
			_ = readCache.close()
			t.Fatalf("read typed ref iteration %d: %v", i, err)
		}
	}
	if pins := manager.PinSummary(); len(pins) == 0 {
		_ = readCache.close()
		t.Fatalf("expected active pin before read cache close")
	}
	if err := readCache.close(); err != nil {
		t.Fatalf("readCache close: %v", err)
	}
	if pins := manager.PinSummary(); len(pins) != 0 {
		t.Fatalf("pins after close=%+v want none", pins)
	}
}

func TestTypedColumnReconstructionHybridOwners(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := createTypedColumnPartCollection1755(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{[]byte(`{"time_us":7,"kind":"like","score":9.25,"flag":true,"payload":"hybrid"}`)}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	got, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":7,"kind":"like","score":9.25,"flag":true,"payload":"hybrid"}`))
	assertTypedColumnManifestShape1755(t, d, col, 1, 1)
}

func TestTypedColumnReconstructionNullableScalarHybridOwnersCheckpointReopen(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	col := createTypedColumnNullableScalarPartCollection1784(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2"), []byte("e3")}, [][]byte{
		[]byte(`{"time_us":1,"kind":null,"score":2.5,"flag":true,"payload":"explicit-null"}`),
		[]byte(`{"time_us":2,"kind":"post","flag":false,"payload":"missing-score"}`),
		[]byte(`{"time_us":3,"score":null,"flag":null,"payload":"mixed-null"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	assertTypedColumnManifestShape1755(t, d, col, 1, 1)
	assertTypedColumnNullableRows1784(t, d, col, 1)

	got, err := col.Get([]byte("e1"))
	if err != nil {
		_ = d.Close()
		t.Fatalf("Get e1: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":1,"kind":null,"score":2.5,"flag":true,"payload":"explicit-null"}`))
	got, err = col.Get([]byte("e2"))
	if err != nil {
		_ = d.Close()
		t.Fatalf("Get e2: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":2,"kind":"post","flag":false,"payload":"missing-score"}`))
	got, err = col.Get([]byte("e3"))
	if err != nil {
		_ = d.Close()
		t.Fatalf("Get e3: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":3,"score":null,"flag":null,"payload":"mixed-null"}`))

	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	d = nil

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("nullable_events")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	assertTypedColumnManifestShape1755(t, reopened, reopenedCol, 1, 1)
	for _, tc := range []struct {
		id   string
		want []byte
	}{
		{id: "e1", want: []byte(`{"time_us":1,"kind":null,"score":2.5,"flag":true,"payload":"explicit-null"}`)},
		{id: "e2", want: []byte(`{"time_us":2,"kind":"post","flag":false,"payload":"missing-score"}`)},
		{id: "e3", want: []byte(`{"time_us":3,"score":null,"flag":null,"payload":"mixed-null"}`)},
	} {
		t.Run(tc.id, func(t *testing.T) {
			got, err := reopenedCol.Get([]byte(tc.id))
			if err != nil {
				t.Fatalf("reopened Get %s: %v", tc.id, err)
			}
			assertJSONEqualM13C(t, got, tc.want)
		})
	}
}

func TestTypedColumnReconstructionScanHybridOwners(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	col := createTypedColumnPartCollection1755(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true,"payload":"alpha"}`),
		[]byte(`{"time_us":2,"kind":"post","score":3.5,"flag":false,"payload":"beta"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	records, truncated, err := reopenedCol.ScanDocuments(10)
	if err != nil {
		t.Fatalf("ScanDocuments: %v", err)
	}
	if truncated || len(records) != 2 {
		t.Fatalf("ScanDocuments truncated=%v records=%d want 2", truncated, len(records))
	}
	assertJSONEqualM13C(t, records[0].Document, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true,"payload":"alpha"}`))
	assertJSONEqualM13C(t, records[1].Document, []byte(`{"time_us":2,"kind":"post","score":3.5,"flag":false,"payload":"beta"}`))
}

func TestTypedColumnReconstructionCacheDecodesOncePerGeneration1781(t *testing.T) {
	const rows = 8
	col, snap, visibleRows, cfg, manifestRootID := typedColumnReconstructionCacheFixture1781(t, rows)
	typedFields := columnStoreTypedColumnPartFields(cfg)
	cache := &typedColumnPartReconstructionCache{Parts: make(map[uint64]typedColumnPartDecodedValues, 1), Fields: typedFields}
	typedScratch := make([]columnDeclaredValue, 0, len(typedFields))
	mergeScratch := make([]columnDeclaredValue, 0, len(cfg.Columns))
	for _, row := range visibleRows {
		typedValues, err := col.typedColumnPartValuesForVisibleRowAtSnapshotIntoWithCache(snap, manifestRootID, cfg, row, cache, typedScratch)
		if err != nil {
			t.Fatalf("typedColumnPartValuesForVisibleRowAtSnapshotIntoWithCache: %v", err)
		}
		fullValues, err := mergeColumnReconstructionValuesInto(cfg, row.Values, typedValues.Values, mergeScratch)
		if err != nil {
			t.Fatalf("mergeColumnReconstructionValuesInto: %v", err)
		}
		if len(fullValues) != len(cfg.Columns) {
			t.Fatalf("full values=%d want columns=%d", len(fullValues), len(cfg.Columns))
		}
	}
	if cache.PartLoads != 1 || cache.TypedPartDecodes != 1 || cache.CacheMisses != 1 || cache.CacheHits != rows-1 {
		t.Fatalf("cache counters loads=%d decodes=%d hits=%d misses=%d want one load/decode/miss and %d hits", cache.PartLoads, cache.TypedPartDecodes, cache.CacheHits, cache.CacheMisses, rows-1)
	}
}

func BenchmarkTypedColumnReconstructionCache1781(b *testing.B) {
	const rows = 128
	col, snap, visibleRows, cfg, manifestRootID := typedColumnReconstructionCacheFixture1781(b, rows)
	typedFields := columnStoreTypedColumnPartFields(cfg)
	b.ReportAllocs()
	b.ResetTimer()
	var reconstructedRows int64
	var partLoads, typedPartDecodes, cacheHits, cacheMisses uint64
	for i := 0; i < b.N; i++ {
		cache := &typedColumnPartReconstructionCache{Parts: make(map[uint64]typedColumnPartDecodedValues, 1), Fields: typedFields}
		typedScratch := make([]columnDeclaredValue, 0, len(typedFields))
		mergeScratch := make([]columnDeclaredValue, 0, len(cfg.Columns))
		for _, row := range visibleRows {
			typedValues, err := col.typedColumnPartValuesForVisibleRowAtSnapshotIntoWithCache(snap, manifestRootID, cfg, row, cache, typedScratch)
			if err != nil {
				b.Fatalf("typedColumnPartValuesForVisibleRowAtSnapshotIntoWithCache: %v", err)
			}
			fullValues, err := mergeColumnReconstructionValuesInto(cfg, row.Values, typedValues.Values, mergeScratch)
			if err != nil {
				b.Fatalf("mergeColumnReconstructionValuesInto: %v", err)
			}
			if len(fullValues) == 0 {
				b.Fatal("empty reconstruction values")
			}
			reconstructedRows++
		}
		partLoads += cache.PartLoads
		typedPartDecodes += cache.TypedPartDecodes
		cacheHits += cache.CacheHits
		cacheMisses += cache.CacheMisses
	}
	b.StopTimer()
	if typedPartDecodes != uint64(b.N) || partLoads != uint64(b.N) {
		b.Fatalf("typed-column part decodes=%d loads=%d want one per benchmark op (%d), not per row", typedPartDecodes, partLoads, b.N)
	}
	elapsed := b.Elapsed()
	b.ReportMetric(float64(reconstructedRows)/elapsed.Seconds(), "rows/s")
	b.ReportMetric(float64(reconstructedRows)/float64(b.N), "rows/op")
	b.ReportMetric(float64(partLoads)/float64(b.N), "part_loads/op")
	b.ReportMetric(float64(typedPartDecodes)/float64(b.N), "typed_part_decodes/op")
	b.ReportMetric(float64(cacheHits)/float64(b.N), "cache_hits/op")
	b.ReportMetric(float64(cacheMisses)/float64(b.N), "cache_misses/op")
}

func TestTypedColumnPublicationRejectsOverlappingOwners(t *testing.T) {
	_, err := NormalizeTypedStorageLayout(TypedStorageLayout{
		Collection: "events",
		Fields: []TypedStorageField{
			{Name: "score_row", Path: "score", Owner: TypedStorageOwnerRowAsset, ValueType: ColumnStoreValueDouble},
			{Name: "score_column", Path: "score", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueDouble},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "overlapping authoritative typed-storage owners") {
		t.Fatalf("NormalizeTypedStorageLayout error=%v want overlapping owners rejection", err)
	}
}

func TestTypedColumnPublicationMissingAssetFailsClosed(t *testing.T) {
	d, col, typedRef := setupSingleTypedColumnPart1755(t)
	defer func() { _ = d.Close() }()
	removeTypedColumnAssetPayload1755(t, d, col, typedRef)
	if got, err := col.Get([]byte("e1")); err == nil || got != nil || !strings.Contains(err.Error(), "typed-column reconstruction") {
		t.Fatalf("Get with missing typed-column asset got=%s err=%v, want typed-column fail-closed error", got, err)
	}
}

func TestTypedColumnPublicationCorruptAssetFailsClosed(t *testing.T) {
	d, col, typedRef := setupSingleTypedColumnPart1755(t)
	defer func() { _ = d.Close() }()
	corruptTypedColumnAssetPayload1755(t, d, typedRef)
	if got, err := col.Get([]byte("e1")); err == nil || got != nil || !strings.Contains(err.Error(), "typed-column reconstruction") {
		t.Fatalf("Get with corrupt typed-column asset got=%s err=%v, want typed-column fail-closed error", got, err)
	}
}

func TestTypedColumnFailedExtractionLeavesDocumentAndManifestUnchanged(t *testing.T) {
	d, col, _ := setupSingleTypedColumnPart1755(t)
	defer func() { _ = d.Close() }()
	beforeIdentity, ok := col.ColumnStoreCacheIdentity()
	if !ok || beforeIdentity.ManifestRoot == 0 {
		t.Fatalf("ColumnStoreCacheIdentity=%+v ok=%v want manifest", beforeIdentity, ok)
	}
	beforeRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	beforeDoc, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get before: %v", err)
	}
	assertJSONEqualM13C(t, beforeDoc, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`))

	cases := []struct {
		name string
		doc  []byte
	}{
		{name: "missing typed string", doc: []byte(`{"time_us":2,"score":4.5,"flag":false}`)},
		{name: "null typed double", doc: []byte(`{"time_us":2,"kind":"bad","score":null,"flag":false}`)},
		{name: "typed bool mismatch", doc: []byte(`{"time_us":2,"kind":"bad","score":4.5,"flag":"false"}`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
				assertJSONEqualM13C(t, current, beforeDoc)
				return tc.doc, true, nil
			})
			if !errors.Is(err, ErrColumnDeclaredValueUnsupported) {
				t.Fatalf("Update err=%v want ErrColumnDeclaredValueUnsupported", err)
			}
			afterIdentity, ok := col.ColumnStoreCacheIdentity()
			if !ok || afterIdentity != beforeIdentity {
				t.Fatalf("identity after failed update=%+v ok=%v want %+v", afterIdentity, ok, beforeIdentity)
			}
			afterRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
			assertColumnAssetRefsEqual1755(t, beforeRefs, afterRefs)
			afterDoc, err := col.Get([]byte("e1"))
			if err != nil {
				t.Fatalf("Get after failed update: %v", err)
			}
			assertJSONEqualM13C(t, afterDoc, beforeDoc)
		})
	}
}

func TestTypedColumnManifestRecoveryRefsSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	col := createTypedColumnPartCollection1755(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{[]byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`)}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	typedRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(typedRefs) != 1 {
		_ = d.Close()
		t.Fatalf("typed refs=%+v want one", typedRefs)
	}
	wantRef := typedRefs[0]
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	reopenedTypedRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, reopened, reopenedCol))
	if len(reopenedTypedRefs) != 1 || reopenedTypedRefs[0] != wantRef {
		t.Fatalf("reopened typed refs=%+v want [%+v]", reopenedTypedRefs, wantRef)
	}
}

func TestTypedColumnManifestRejectsUnexpectedTypedPartID1755(t *testing.T) {
	asset := ColumnPreparedAsset{
		Ref: ColumnAssetRef{
			Kind:       ColumnAssetKindTCS1TypedColumnPart,
			Namespace:  "events/column-assets",
			Generation: 1,
			PartID:     99,
			FileID:     1,
			Length:     64,
			Checksum:   7,
		},
		Rows:         1,
		Bytes:        64,
		PublishID:    1,
		GenerationID: 1,
		Reason:       string(ColumnPublishOperationInsert),
	}
	raw, err := encodeColumnManifestPartRecord(asset)
	if err != nil {
		t.Fatalf("encodeColumnManifestPartRecord: %v", err)
	}
	_, err = typedColumnPartRefsByGenerationFromManifestRecords([]columnManifestRecord{{
		key:   columnManifestPartRecordKey(asset.Ref.Generation, asset.Ref.PartID),
		value: raw,
	}}, asset.Ref.Namespace)
	if err == nil || !strings.Contains(err.Error(), "unexpected part_id=99") {
		t.Fatalf("typedColumnPartRefsByGenerationFromManifestRecords err=%v want unexpected part_id", err)
	}
}

func TestTypedColumnManifestSnapshotViewAcceptsTypedPartRefs1755(t *testing.T) {
	d, col, _ := setupSingleTypedColumnPart1755(t)
	defer func() { _ = d.Close() }()
	id, ok := col.ColumnStoreCacheIdentity()
	if !ok || id.ManifestRoot == 0 {
		t.Fatalf("ColumnStoreCacheIdentity=%+v ok=%v want manifest root", id, ok)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	records, err := loadColumnManifestRecordsFromRoot(snap, id.ManifestRoot)
	if err != nil {
		t.Fatalf("loadColumnManifestRecordsFromRoot: %v", err)
	}
	snapshot, refs, mutationParts, err := decodeColumnManifestSnapshotViewForScan(records, col.Meta().Options.ColumnStore.AssetManager.Namespace)
	if err != nil {
		t.Fatalf("decodeColumnManifestSnapshotViewForScan: %v", err)
	}
	if snapshot.ExpectedParts != 2 || len(refs) != 1 || refs[0].Ref.Kind != ColumnAssetKindTCS1PartImage || mutationParts != 0 {
		t.Fatalf("snapshot expected_parts=%d refs=%+v mutationParts=%d; want one physical ref and typed part counted", snapshot.ExpectedParts, refs, mutationParts)
	}
}

func TestTypedColumnReachabilityRefsExposedForMaintenance(t *testing.T) {
	d, col, typedRef := setupSingleTypedColumnPart1755(t)
	defer func() { _ = d.Close() }()
	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	for _, entry := range plan.Entries {
		if entry.Ref == typedRef {
			if entry.Status != ColumnAssetReachabilityProtected {
				t.Fatalf("typed ref reachability status=%s want protected entry=%+v", entry.Status, entry)
			}
			return
		}
	}
	t.Fatalf("typed ref %+v not exposed in reachability entries=%+v", typedRef, plan.Entries)
}

func TestTypedColumnPublicationColumnAssetReachabilityWithPinnedSnapshot(t *testing.T) {
	runTypedColumnSnapshotReadsOldRefsAndReachabilityPinsCandidates1778(t)
}

func TestTypedColumnSnapshotReadsOldRefsAndReachabilityPinsCandidates(t *testing.T) {
	runTypedColumnSnapshotReadsOldRefsAndReachabilityPinsCandidates1778(t)
}

func runTypedColumnSnapshotReadsOldRefsAndReachabilityPinsCandidates1778(t *testing.T) {
	d, col, firstRef := setupSingleTypedColumnPart1755(t)
	defer func() { _ = d.Close() }()
	oldSnap := d.AcquireSnapshot()
	if oldSnap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	defer func() { _ = oldSnap.Close() }()
	oldDoc, found := typedColumnDocumentAtSnapshot1755(t, col, oldSnap, []byte("e1"))
	if !found {
		t.Fatalf("old snapshot did not find e1")
	}
	assertJSONEqualM13C(t, oldDoc, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`))

	if _, changed, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
		assertJSONEqualM13C(t, current, oldDoc)
		return []byte(`{"time_us":5,"kind":"share","score":7.5,"flag":false}`), true, nil
	}); err != nil || !changed {
		t.Fatalf("Update changed=%v err=%v", changed, err)
	}
	currentDoc, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("current Get e1: %v", err)
	}
	assertJSONEqualM13C(t, currentDoc, []byte(`{"time_us":5,"kind":"share","score":7.5,"flag":false}`))
	oldDocAgain, found := typedColumnDocumentAtSnapshot1755(t, col, oldSnap, []byte("e1"))
	if !found {
		t.Fatalf("old snapshot lost e1 after update")
	}
	assertJSONEqualM13C(t, oldDocAgain, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`))

	candidate := writeTypedColumnAssetCandidate1755(t, d, col, 3, 99)
	rewrite, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetRewrite with pinned old snapshot: %v", err)
	}
	if rewrite.RefsRemapped == 0 || len(rewrite.SupersededRefs) == 0 {
		t.Fatalf("rewrite stats=%+v want remapped/superseded refs", rewrite)
	}
	oldDocAfterRewrite, found := typedColumnDocumentAtSnapshot1755(t, col, oldSnap, []byte("e1"))
	if !found {
		t.Fatalf("old snapshot lost e1 after rewrite")
	}
	assertJSONEqualM13C(t, oldDocAfterRewrite, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`))
	if !columnAssetRefsContain1755(rewrite.SupersededRefs, firstRef) {
		t.Fatalf("superseded refs=%+v want initial typed ref %+v", rewrite.SupersededRefs, firstRef)
	}

	unpinned, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{
		Detailed:      true,
		CandidateRefs: rewrite.SupersededRefs,
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability unpinned superseded: %v", err)
	}
	assertColumnAssetReachabilityEntry1755(t, unpinned, firstRef, ColumnAssetReachabilityReclaimable, false)

	pinned, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{
		Detailed:                              true,
		ProtectCandidateRefsForOlderSnapshots: true,
		CandidateRefs:                         rewrite.SupersededRefs,
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability pinned superseded: %v", err)
	}
	assertColumnAssetReachabilityEntry1755(t, pinned, firstRef, ColumnAssetReachabilityProtected, true)
}

func TestTypedColumnSnapshotReadsOldRefsAfterDelete(t *testing.T) {
	d, col, _ := setupSingleTypedColumnPart1755(t)
	defer func() { _ = d.Close() }()
	oldSnap := d.AcquireSnapshot()
	if oldSnap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	defer func() { _ = oldSnap.Close() }()

	deleted, err := col.DeleteDocument([]byte("e1"))
	if err != nil || !deleted {
		t.Fatalf("DeleteDocument deleted=%v err=%v", deleted, err)
	}
	if got, err := col.Get([]byte("e1")); err != nil || got != nil {
		t.Fatalf("current Get after delete got=%s err=%v want missing", got, err)
	}
	oldDoc, found := typedColumnDocumentAtSnapshot1755(t, col, oldSnap, []byte("e1"))
	if !found {
		t.Fatalf("old snapshot lost e1 after delete")
	}
	assertJSONEqualM13C(t, oldDoc, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`))
}

func TestTypedColumnPublicationColumnAssetRewriteRoundTrip(t *testing.T) {
	runTypedColumnColumnAssetRewriteRoundTripMixedRefs1778(t)
}

func TestTypedColumnColumnAssetRewriteRoundTripMixedRefs(t *testing.T) {
	runTypedColumnColumnAssetRewriteRoundTripMixedRefs1778(t)
}

func runTypedColumnColumnAssetRewriteRoundTripMixedRefs1778(t *testing.T) {
	d, col, _ := setupSingleTypedColumnPart1755(t)
	dir := d.Dir()
	dClosed := false
	defer func() {
		if !dClosed {
			_ = d.Close()
		}
	}()
	beforeRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	beforePhysical := columnManifestPhysicalAssetRefsForTestM1634(beforeRefs)
	beforeTyped := typedColumnPartRefs1755(beforeRefs)
	if len(beforePhysical) != 1 || len(beforeTyped) != 1 {
		t.Fatalf("before physical=%+v typed=%+v want one each all=%+v", beforePhysical, beforeTyped, beforeRefs)
	}
	candidate := writeTypedColumnAssetCandidate1755(t, d, col, 2, 99)
	if candidate.FileID != beforeRefs[0].FileID {
		t.Fatalf("candidate file_id=%d live file_id=%d, test requires mixed segment", candidate.FileID, beforeRefs[0].FileID)
	}
	oldSegmentPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}

	rewrite, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetRewrite: %v", err)
	}
	if rewrite.SegmentsRewritten != 1 || rewrite.RefsRemapped != len(beforeRefs) {
		t.Fatalf("rewrite stats=%+v want one rewritten segment and %d remapped refs", rewrite, len(beforeRefs))
	}
	afterRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if afterPhysical := columnManifestPhysicalAssetRefsForTestM1634(afterRefs); len(afterPhysical) != 1 || afterPhysical[0] == beforePhysical[0] {
		t.Fatalf("after physical=%+v before=%+v want remapped physical ref", afterPhysical, beforePhysical)
	}
	if afterTyped := typedColumnPartRefs1755(afterRefs); len(afterTyped) != 1 || afterTyped[0] == beforeTyped[0] {
		t.Fatalf("after typed=%+v before=%+v want remapped typed ref", afterTyped, beforeTyped)
	}
	got, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get after rewrite: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`))
	if err := d.Close(); err != nil {
		t.Fatalf("Close after rewrite: %v", err)
	}
	dClosed = true

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	reopenedGot, err := reopenedCol.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("reopened Get e1 after rewrite: %v", err)
	}
	assertJSONEqualM13C(t, reopenedGot, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`))
	gcStats, err := reopenedCol.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		CandidateRefs: append(append([]ColumnAssetRef(nil), rewrite.SupersededRefs...), candidate),
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC: %v", err)
	}
	if gcStats.SegmentsDeleted != 1 || gcStats.BytesDeleted == 0 {
		t.Fatalf("gc stats=%+v want old mixed segment deleted", gcStats)
	}
	if _, err := os.Stat(oldSegmentPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old mixed segment stat err=%v want not exist", err)
	}
	afterGC, err := reopenedCol.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get after GC: %v", err)
	}
	assertJSONEqualM13C(t, afterGC, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`))
}

func TestTypedColumnPhysicalQueryTypedColumnFieldsFailClosed(t *testing.T) {
	runTypedColumnPhysicalQueryFailsClosedForColumnPartFields1778(t)
}

func TestTypedColumnPhysicalQueryFailsClosedForColumnPartFields(t *testing.T) {
	runTypedColumnPhysicalQueryFailsClosedForColumnPartFields1778(t)
}

func runTypedColumnPhysicalQueryFailsClosedForColumnPartFields1778(t *testing.T) {
	d, col, _ := setupSingleTypedColumnPart1755(t)
	defer func() { _ = d.Close() }()
	if _, err := col.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"}); !errors.Is(err, ErrColumnQueryPlanUnsupported) {
		t.Fatalf("RunColumnPhysicalQuery typed-column group err=%v want ErrColumnQueryPlanUnsupported", err)
	}
	result, err := col.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryHourCount, ValueColumn: "time_us"})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery row-owned time_us: %v", err)
	}
	if result.Diagnostics.ReduceRows != 1 || result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.ReconstructionRows != 0 {
		t.Fatalf("row-owned query diagnostics=%+v want one direct physical row and no reconstruction", result.Diagnostics)
	}
	count := 0
	for _, group := range result.Groups {
		count += group.Count
	}
	if count != 1 {
		t.Fatalf("row-owned query total count=%d groups=%+v want 1", count, result.Groups)
	}
}

func TestTypedColumnPublicationExistingTypedRowCompatibility(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: testColumnStoreConfig(nil)}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{[]byte(`{"time_us":1,"kind":"like","did":"d1","payload":"row"}`)}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	refs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if typedRefs := typedColumnPartRefs1755(refs); len(typedRefs) != 0 {
		t.Fatalf("existing typed-row config published typed-column refs=%+v", typedRefs)
	}
	if physicalRefs := columnManifestPhysicalAssetRefsForTestM1634(refs); len(physicalRefs) != 1 {
		t.Fatalf("physical refs=%+v want one", physicalRefs)
	}
	got, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":1,"kind":"like","did":"d1","payload":"row"}`))
}

func TestTypedColumnPartMappedResourceReadUsesColumnPartClass1755(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := createTypedColumnPartCollection1755(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{[]byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`)}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	typedRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(typedRefs) != 1 {
		t.Fatalf("typed refs=%+v want one", typedRefs)
	}
	typedRef := typedRefs[0]
	manager := mappedresource.NewManager()
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(d.ColumnAssetRootDir(), col.Meta().Options.ColumnStore.AssetManager.Namespace, ColumnAssetReadIntegrityVerify)
	if err != nil {
		t.Fatalf("new read cache: %v", err)
	}
	scope := mappedresource.Scope{
		Kind:       mappedresource.ScopeColumnPartReader,
		ID:         "typed-column-part-1755",
		Namespace:  typedRef.Namespace,
		Collection: "events",
		Generation: typedRef.Generation,
		Reason:     "typed-column-publication-test",
	}
	if err := readCache.useMappedResourceManager(manager, scope, "typed-column-part-read"); err != nil {
		_ = readCache.close()
		t.Fatalf("useMappedResourceManager: %v", err)
	}
	if _, err := readCache.read(typedRef, nil); err != nil {
		_ = readCache.close()
		t.Fatalf("read typed part: %v", err)
	}
	pins := readCache.mappedResourcePins()
	if len(pins) != 1 {
		_ = readCache.close()
		t.Fatalf("pins=%d want 1", len(pins))
	}
	pin := pins[0]
	if pin.Key.Class != mappedresource.ClassTypedColumnAsset || pin.Scope.Kind != mappedresource.ScopeColumnPartReader || pin.Reason != "typed-column-part-read" {
		_ = readCache.close()
		t.Fatalf("unexpected mappedresource pin: %+v", pin)
	}
	if err := readCache.close(); err != nil {
		t.Fatalf("close read cache: %v", err)
	}
	if pins := manager.PinSummary(); len(pins) != 0 {
		t.Fatalf("pins after close=%d want 0", len(pins))
	}
}

func TestTypedColumnPublicationUnsupportedValueFailsClosed(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{{
		Name:      "embedding_neighbors",
		Path:      "embedding_neighbors",
		ValueType: ColumnStoreValueAdjacencyList,
		Owner:     TypedStorageOwnerColumnPart,
	}}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	_, err = col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{[]byte(`{"embedding_neighbors":[1,2,3]}`)})
	if !errors.Is(err, backenddb.ErrCommandWALRejected) {
		t.Fatalf("InsertBatch error=%v want ErrCommandWALRejected", err)
	}
}

func typedColumnReconstructionCacheFixture1781(t testing.TB, rows int) (*Collection, *backenddb.Snapshot, []columnPhysicalVisibleRow, ColumnStoreConfig, uint64) {
	t.Helper()
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	t.Cleanup(func() { _ = d.Close() })
	col := createTypedColumnPartCollection1755(t, d)
	ids := make([][]byte, rows)
	docs := make([][]byte, rows)
	for i := 0; i < rows; i++ {
		ids[i] = []byte(fmt.Sprintf("e%06d", i))
		docs[i] = []byte(fmt.Sprintf(`{"time_us":%d,"kind":"kind-%02d","score":%.2f,"flag":%t,"payload":"payload-%d"}`, i+1, i%7, float64(i)+0.25, i%2 == 0, i))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	t.Cleanup(func() { _ = snap.Close() })
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		t.Fatalf("catalogForSnapshot: %v", err)
	}
	if catalog == nil {
		t.Fatalf("catalogForSnapshot returned nil")
	}
	cfg := catalog.meta.Options.ColumnStore.copy()
	manifestRootID := catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name))
	visible, err := col.scanColumnPhysicalVisibleRowsAtSnapshot(snap, catalog, catalog.meta.Name, manifestRootID, cfg, true, nil)
	if err != nil {
		t.Fatalf("scanColumnPhysicalVisibleRowsAtSnapshot: %v", err)
	}
	if len(visible.Rows) != rows {
		t.Fatalf("visible rows=%d want %d", len(visible.Rows), rows)
	}
	return col, snap, visible.Rows, cfg, manifestRootID
}

func typedColumnDocumentAtSnapshot1755(t testing.TB, col *Collection, snap *backenddb.Snapshot, id []byte) ([]byte, bool) {
	t.Helper()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		t.Fatalf("catalogForSnapshot: %v", err)
	}
	if catalog == nil {
		t.Fatalf("catalogForSnapshot returned nil")
	}
	retained, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, collectionPrimaryRootName(catalog.meta.Name), id, nil)
	if err != nil {
		t.Fatalf("collectionGetAppendAtCatalogRoot(%q): %v", string(id), err)
	}
	if !found {
		return nil, false
	}
	if !columnStoreCanReconstructDocument(catalog.meta) {
		return bytes.Clone(retained), true
	}
	reconstructed, err := col.reconstructColumnDocumentAtSnapshot(snap, catalog, id, retained)
	if err != nil {
		t.Fatalf("reconstructColumnDocumentAtSnapshot(%q): %v", string(id), err)
	}
	return reconstructed, true
}

func assertColumnAssetReachabilityEntry1755(t testing.TB, plan ColumnAssetReachabilityPlan, ref ColumnAssetRef, status ColumnAssetReachabilityStatus, wantPinned bool) {
	t.Helper()
	for _, entry := range plan.Entries {
		if entry.Ref != ref {
			continue
		}
		if entry.Status != status {
			t.Fatalf("entry %+v status=%s want %s plan=%+v", entry.Ref, entry.Status, status, plan)
		}
		if wantPinned && !columnAssetReachabilitySourcesContain1755(entry.Sources, ColumnAssetReachabilitySourcePinnedSnapshot) {
			t.Fatalf("entry %+v sources=%v want pinned_snapshot", entry.Ref, entry.Sources)
		}
		return
	}
	t.Fatalf("ref %+v not found in reachability entries=%+v", ref, plan.Entries)
}

func columnAssetReachabilitySourcesContain1755(sources []ColumnAssetReachabilitySource, want ColumnAssetReachabilitySource) bool {
	for _, source := range sources {
		if source == want {
			return true
		}
	}
	return false
}

func writeTypedColumnAssetCandidate1755(t testing.TB, d *backenddb.DB, col *Collection, generation, partID uint64) ColumnAssetRef {
	t.Helper()
	cfg := col.Meta().Options.ColumnStore
	if cfg == nil || cfg.AssetManager == nil {
		t.Fatalf("missing column store config: %+v", cfg)
	}
	payload := []byte("typed-column-candidate-1755")
	ref, err := writeColumnAssetToManager(d.ColumnAssetRootDir(), *cfg, payload, ColumnAssetKindTCS1TypedColumnPart, generation, partID)
	if err != nil {
		t.Fatalf("writeColumnAssetToManager candidate: %v", err)
	}
	if ref.Length != int64(len(payload)) {
		t.Fatalf("candidate ref length=%d want %d", ref.Length, len(payload))
	}
	return ref
}

func columnAssetRefsContain1755(refs []ColumnAssetRef, want ColumnAssetRef) bool {
	for _, ref := range refs {
		if ref == want {
			return true
		}
	}
	return false
}

func assertColumnAssetRefsEqual1755(t testing.TB, want, got []ColumnAssetRef) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("refs=%+v want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("refs[%d]=%+v want %+v (all got %+v)", i, got[i], want[i], got)
		}
	}
}

func setupSingleTypedColumnPart1755(t testing.TB) (*backenddb.DB, *Collection, ColumnAssetRef) {
	t.Helper()
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	col := createTypedColumnPartCollection1755(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{[]byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`)}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	typedRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(typedRefs) != 1 {
		_ = d.Close()
		t.Fatalf("typed refs=%+v want one", typedRefs)
	}
	return d, col, typedRefs[0]
}

func removeTypedColumnAssetPayload1755(t testing.TB, d *backenddb.DB, col *Collection, typedRef ColumnAssetRef) {
	t.Helper()
	path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), typedRef)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	for _, physicalRef := range columnManifestPhysicalAssetRefsForTestM1634(columnManifestAssetRefsForCollectionM12A(t, d, col)) {
		if physicalRef.FileID == typedRef.FileID && physicalRef.Offset+physicalRef.Length > typedRef.Offset {
			t.Fatalf("cannot remove typed payload without damaging physical row asset: physical=%+v typed=%+v", physicalRef, typedRef)
		}
	}
	if err := os.Truncate(path, typedRef.Offset); err != nil {
		t.Fatalf("truncate typed-column asset payload: %v", err)
	}
}

func corruptTypedColumnAssetPayload1755(t testing.TB, d *backenddb.DB, typedRef ColumnAssetRef) {
	t.Helper()
	path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), typedRef)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open typed-column asset segment: %v", err)
	}
	defer func() { _ = f.Close() }()
	buf := []byte{0}
	if _, err := f.ReadAt(buf, typedRef.Offset); err != nil {
		t.Fatalf("read typed-column asset byte: %v", err)
	}
	buf[0] ^= 0xff
	if _, err := f.WriteAt(buf, typedRef.Offset); err != nil {
		t.Fatalf("corrupt typed-column asset byte: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync corrupt typed-column asset: %v", err)
	}
}

type typedColumnExpectedRow1755 struct {
	PrimaryID int64
	Kind      string
	Score     float64
	Flag      bool
}

type typedColumnScalarMatrixCase1778 struct {
	name       string
	column     string
	valueType  ColumnStoreValueType
	docs       [2][]byte
	wantValues []columnDeclaredValue
}

func createTypedColumnScalarCollection1778(t testing.TB, d *backenddb.DB, name, typedColumn string, valueType ColumnStoreValueType, retainedPayload ColumnRetainedPayloadPolicy) *Collection {
	t.Helper()
	cfg := testColumnStoreConfig(nil)
	cfg.RetainedPayload = retainedPayload
	cfg.Columns = []ColumnStoreColumn{
		{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
		{Name: typedColumn, Path: typedColumn, ValueType: valueType, Owner: TypedStorageOwnerColumnPart},
	}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: name, Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection(name)
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	return col
}

func typedColumnPartRefsIfManifest1778(t testing.TB, d *backenddb.DB, col *Collection) []ColumnAssetRef {
	t.Helper()
	id, ok := col.ColumnStoreCacheIdentity()
	if !ok || id.ManifestRoot == 0 {
		return nil
	}
	return typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
}

func typedColumnPartRowsForGeneration1778(t testing.TB, d *backenddb.DB, col *Collection, generation uint64) []typedColumnAdapterRow {
	t.Helper()
	var ref ColumnAssetRef
	found := false
	for _, candidate := range typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col)) {
		if candidate.Generation == generation {
			ref = candidate
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("missing typed-column part for generation=%d", generation)
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref)
	if err != nil {
		t.Fatalf("read typed-column part: %v", err)
	}
	part, err := typedColumnAdapterPartFromBytes(typedColumnAdapterOptions{Fields: columnStoreTypedColumnPartFields(*col.Meta().Options.ColumnStore)}, raw)
	if err != nil {
		t.Fatalf("typedColumnAdapterPartFromBytes: %v", err)
	}
	rows, err := part.scanRows()
	if err != nil {
		t.Fatalf("scanRows: %v", err)
	}
	return rows
}

func assertTypedColumnPartFieldValues1778(t testing.TB, d *backenddb.DB, col *Collection, generation uint64, field string, want []columnDeclaredValue) {
	t.Helper()
	rows := typedColumnPartRowsForGeneration1778(t, d, col, generation)
	if len(rows) != len(want) {
		t.Fatalf("typed rows=%d want %d", len(rows), len(want))
	}
	for i, row := range rows {
		if row.PrimaryID != int64(i) {
			t.Fatalf("row[%d] primary_id=%d want %d", i, row.PrimaryID, i)
		}
		got, ok := row.Values[field]
		if !ok {
			t.Fatalf("row[%d] missing field %q values=%+v", i, field, row.Values)
		}
		assertColumnDeclaredScalarValueEqual1778(t, got, want[i], fmt.Sprintf("row[%d].%s", i, field))
	}
}

func assertColumnDeclaredScalarValueEqual1778(t testing.TB, got, want columnDeclaredValue, label string) {
	t.Helper()
	if got.Type != want.Type || got.Present != want.Present || got.Null != want.Null {
		t.Fatalf("%s value=%+v want %+v", label, got, want)
	}
	switch want.Type {
	case ColumnStoreValueBool:
		if got.Bool != want.Bool {
			t.Fatalf("%s bool=%v want %v", label, got.Bool, want.Bool)
		}
	case ColumnStoreValueInt64:
		if got.Int64 != want.Int64 {
			t.Fatalf("%s int64=%d want %d", label, got.Int64, want.Int64)
		}
	case ColumnStoreValueFloat32:
		if got.Float32 != want.Float32 {
			t.Fatalf("%s float32=%v want %v", label, got.Float32, want.Float32)
		}
	case ColumnStoreValueDouble:
		if got.Double != want.Double {
			t.Fatalf("%s double=%v want %v", label, got.Double, want.Double)
		}
	case ColumnStoreValueString:
		if got.String != want.String {
			t.Fatalf("%s string=%q want %q", label, got.String, want.String)
		}
	default:
		t.Fatalf("%s unsupported scalar value type %q", label, want.Type)
	}
}

func assertTypedColumnPartStringSet1778(t testing.TB, d *backenddb.DB, col *Collection, generation uint64, field string, want []string) {
	t.Helper()
	rows := typedColumnPartRowsForGeneration1778(t, d, col, generation)
	counts := make(map[string]int, len(rows))
	for _, row := range rows {
		value, ok := row.Values[field]
		if !ok {
			t.Fatalf("row primary_id=%d missing field %q values=%+v", row.PrimaryID, field, row.Values)
		}
		counts[value.String]++
	}
	for _, value := range want {
		counts[value]--
		if counts[value] < 0 {
			t.Fatalf("generation=%d field=%q saw extra/missing value %q counts=%+v want=%+v", generation, field, value, counts, want)
		}
	}
	for value, count := range counts {
		if count != 0 {
			t.Fatalf("generation=%d field=%q value %q count delta=%d counts=%+v want=%+v", generation, field, value, count, counts, want)
		}
	}
}

func typedColumnManifestPartRecord1778(t testing.TB, ref ColumnAssetRef, rows int, operation ColumnPublishOperation, keyGeneration, keyPartID uint64) columnManifestRecord {
	t.Helper()
	return typedColumnManifestPartRecordWithReason1778(t, ref, rows, string(operation), keyGeneration, keyPartID)
}

func typedColumnManifestPartRecordWithReason1778(t testing.TB, ref ColumnAssetRef, rows int, reason string, keyGeneration, keyPartID uint64) columnManifestRecord {
	t.Helper()
	raw, err := encodeColumnManifestPartRecord(ColumnPreparedAsset{
		Ref:          ref,
		Rows:         rows,
		Bytes:        ref.Length,
		PublishID:    ref.Generation,
		GenerationID: ref.Generation,
		Reason:       reason,
	})
	if err != nil {
		t.Fatalf("encodeColumnManifestPartRecord: %v", err)
	}
	return columnManifestRecord{key: columnManifestPartRecordKey(keyGeneration, keyPartID), value: raw}
}

func createTypedColumnPartCollection1755(t testing.TB, d *backenddb.DB) *Collection {
	t.Helper()
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{
		{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart},
		{Name: "score", Path: "score", ValueType: ColumnStoreValueDouble, Owner: TypedStorageOwnerColumnPart},
		{Name: "flag", Path: "flag", ValueType: ColumnStoreValueBool, Owner: TypedStorageOwnerColumnPart},
	}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	return col
}

func createTypedColumnNullableScalarPartCollection1784(t testing.TB, d *backenddb.DB) *Collection {
	t.Helper()
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{
		{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Nullable: true},
		{Name: "score", Path: "score", ValueType: ColumnStoreValueDouble, Owner: TypedStorageOwnerColumnPart, Nullable: true},
		{Name: "flag", Path: "flag", ValueType: ColumnStoreValueBool, Owner: TypedStorageOwnerColumnPart, Nullable: true},
	}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "nullable_events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("nullable_events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	return col
}

func createTypedColumnVectorPartCollection1756(t testing.TB, d *backenddb.DB) *Collection {
	t.Helper()
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, Owner: TypedStorageOwnerColumnPart, VectorDims: 3}}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "vectors", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("vectors")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	return col
}

func assertTypedColumnManifestShape1755(t testing.TB, d *backenddb.DB, col *Collection, wantGeneration uint64, wantTypedParts int) {
	t.Helper()
	id, ok := col.ColumnStoreCacheIdentity()
	if !ok || id.ManifestRoot == 0 || id.ManifestGeneration != wantGeneration {
		t.Fatalf("ColumnStoreCacheIdentity=%+v ok=%v want generation=%d manifest root", id, ok, wantGeneration)
	}
	refs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	physicalRefs := columnManifestPhysicalAssetRefsForTestM1634(refs)
	typedRefs := typedColumnPartRefs1755(refs)
	if len(physicalRefs) != int(wantGeneration) {
		t.Fatalf("physical refs=%+v want %d", physicalRefs, wantGeneration)
	}
	if len(typedRefs) != wantTypedParts {
		t.Fatalf("typed refs=%+v want %d all refs=%+v", typedRefs, wantTypedParts, refs)
	}
}

func assertTypedColumnLatestRows1755(t testing.TB, d *backenddb.DB, col *Collection, generation uint64, want []typedColumnExpectedRow1755) {
	t.Helper()
	var ref ColumnAssetRef
	found := false
	for _, candidate := range typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col)) {
		if candidate.Generation == generation {
			ref = candidate
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("missing typed-column part for generation=%d", generation)
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref)
	if err != nil {
		t.Fatalf("read typed-column part: %v", err)
	}
	part, err := typedColumnAdapterPartFromBytes(typedColumnAdapterOptions{Fields: columnStoreTypedColumnPartFields(*col.Meta().Options.ColumnStore)}, raw)
	if err != nil {
		t.Fatalf("typedColumnAdapterPartFromBytes: %v", err)
	}
	rows, err := part.scanRows()
	if err != nil {
		t.Fatalf("scanRows: %v", err)
	}
	if len(rows) != len(want) {
		t.Fatalf("typed rows=%d want %d", len(rows), len(want))
	}
	for i, row := range rows {
		if row.PrimaryID != want[i].PrimaryID {
			t.Fatalf("row[%d] primary_id=%d want %d", i, row.PrimaryID, want[i].PrimaryID)
		}
		kind := row.Values["kind"]
		score := row.Values["score"]
		flag := row.Values["flag"]
		if kind.String != want[i].Kind || score.Double != want[i].Score || flag.Bool != want[i].Flag {
			t.Fatalf("row[%d] values kind=%+v score=%+v flag=%+v want %+v", i, kind, score, flag, want[i])
		}
	}
}

func assertTypedColumnNullableRows1784(t testing.TB, d *backenddb.DB, col *Collection, generation uint64) {
	t.Helper()
	var ref ColumnAssetRef
	found := false
	for _, candidate := range typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col)) {
		if candidate.Generation == generation {
			ref = candidate
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("missing typed-column part for generation=%d", generation)
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref)
	if err != nil {
		t.Fatalf("read typed-column part: %v", err)
	}
	part, err := typedColumnAdapterPartFromBytes(typedColumnAdapterOptions{Fields: columnStoreTypedColumnPartFields(*col.Meta().Options.ColumnStore)}, raw)
	if err != nil {
		t.Fatalf("typedColumnAdapterPartFromBytes: %v", err)
	}
	rows, err := part.scanRows()
	if err != nil {
		t.Fatalf("scanRows: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("typed nullable rows=%d want 3", len(rows))
	}
	if kind := rows[0].Values["kind"]; !kind.Present || !kind.Null {
		t.Fatalf("row0 kind=%+v want explicit null", kind)
	}
	if score := rows[1].Values["score"]; score.Present || !score.Null {
		t.Fatalf("row1 score=%+v want missing/null marker", score)
	}
	if flag := rows[2].Values["flag"]; !flag.Present || !flag.Null {
		t.Fatalf("row2 flag=%+v want explicit null", flag)
	}
}

func typedColumnPartRefs1755(refs []ColumnAssetRef) []ColumnAssetRef {
	out := make([]ColumnAssetRef, 0, len(refs))
	for _, ref := range refs {
		if ref.Kind == ColumnAssetKindTCS1TypedColumnPart {
			out = append(out, ref)
		}
	}
	return out
}
