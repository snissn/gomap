package collections

import (
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

func TestTypedColumnPartDurablePublicationReopenAndReconstruction1755(t *testing.T) {
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
	typedRef := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))[0]
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

func TestTypedColumnPartUnsupportedValueFailsClosed1755(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{{
		Name:       "embedding",
		Path:       "embedding",
		ValueType:  ColumnStoreValueFloat32Vector,
		Owner:      TypedStorageOwnerColumnPart,
		VectorDims: 3,
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
	_, err = col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{[]byte(`{"embedding":[1,2,3]}`)})
	if !errors.Is(err, backenddb.ErrCommandWALRejected) {
		t.Fatalf("InsertBatch error=%v want ErrCommandWALRejected", err)
	}
}

type typedColumnExpectedRow1755 struct {
	PrimaryID int64
	Kind      string
	Score     float64
	Flag      bool
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

func typedColumnPartRefs1755(refs []ColumnAssetRef) []ColumnAssetRef {
	out := make([]ColumnAssetRef, 0, len(refs))
	for _, ref := range refs {
		if ref.Kind == ColumnAssetKindTCS1TypedColumnPart {
			out = append(out, ref)
		}
	}
	return out
}
