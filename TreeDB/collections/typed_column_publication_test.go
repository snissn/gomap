package collections

import (
	"context"
	"errors"
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
