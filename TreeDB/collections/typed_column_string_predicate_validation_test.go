package collections

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestTypedColumnStringPredicatePrepareAndCodeScan(t *testing.T) {
	field := typedColumnStringPredicateField(false)
	part := typedColumnStringPredicateBuildPart(t, field, []string{"alpha", "beta", "gamma", "beta"})
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}

	prepared, err := typedColumnAdapterPrepareStringPredicateScanPart([]TypedStorageField{field}, image.Bytes, image.PartID, image.Rows, image.Rows, uint64(part.Part.Descriptor.SchemaVersion), "kind", "beta")
	if err != nil {
		t.Fatalf("typedColumnAdapterPrepareStringPredicateScanPart: %v", err)
	}
	if !prepared.QueryCodeFound || prepared.QueryCode != uint32(part.Dictionary["kind"]["beta"]) || prepared.ManifestBytes == 0 {
		t.Fatalf("prepared=%+v dictionary=%+v want resolved beta code and manifest bytes", prepared, part.Dictionary["kind"])
	}
	var gotRows []int
	var gotIDs []int64
	pruned, scanned, matched, err := scanTypedColumnStringEqualityPredicateCodes(prepared.AdapterPart.Part, prepared.Column.Definition.Name, prepared.QueryCode, prepared.QueryCodeFound, func(rowIndex int, primaryID int64) error {
		gotRows = append(gotRows, rowIndex)
		gotIDs = append(gotIDs, primaryID)
		return nil
	})
	if err != nil {
		t.Fatalf("scanTypedColumnStringEqualityPredicateCodes: %v", err)
	}
	if pruned || scanned != 4 || matched != 2 || !slices.Equal(gotRows, []int{1, 3}) || !slices.Equal(gotIDs, []int64{1, 3}) {
		t.Fatalf("pruned=%v scanned=%d matched=%d rows=%v ids=%v", pruned, scanned, matched, gotRows, gotIDs)
	}

	absent, err := typedColumnAdapterPrepareStringPredicateScanPart([]TypedStorageField{field}, image.Bytes, image.PartID, image.Rows, image.Rows, uint64(part.Part.Descriptor.SchemaVersion), "kind", "missing")
	if err != nil {
		t.Fatalf("prepare absent query: %v", err)
	}
	pruned, scanned, matched, err = scanTypedColumnStringEqualityPredicateCodes(absent.AdapterPart.Part, absent.Column.Definition.Name, absent.QueryCode, absent.QueryCodeFound, nil)
	if err != nil {
		t.Fatalf("scan absent query: %v", err)
	}
	if !pruned || scanned != 0 || matched != 0 {
		t.Fatalf("absent query pruned=%v scanned=%d matched=%d want dictionary-level prune without row decode", pruned, scanned, matched)
	}
}

func TestTypedColumnStringPredicatePrepareFailsClosedMetadata(t *testing.T) {
	field := typedColumnStringPredicateField(false)
	part := typedColumnStringPredicateBuildPart(t, field, []string{"alpha", "beta", "gamma"})
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	fields := []TypedStorageField{field}
	schema := uint64(part.Part.Descriptor.SchemaVersion)

	for _, tc := range []struct {
		name    string
		fields  []TypedStorageField
		raw     []byte
		partID  uint64
		rows    int
		phys    int
		schema  uint64
		wantErr string
		wantIs  error
	}{
		{name: "nullable_unsupported", fields: []TypedStorageField{typedColumnStringPredicateField(true)}, raw: image.Bytes, partID: image.PartID, rows: image.Rows, phys: image.Rows, schema: schema, wantErr: "nullable=true", wantIs: ErrColumnQueryPlanUnsupported},
		{name: "schema_hash_mismatch", fields: fields, raw: image.Bytes, partID: image.PartID, rows: image.Rows, phys: image.Rows, schema: schema + 1, wantErr: "schema_version"},
		{name: "part_mismatch", fields: fields, raw: image.Bytes, partID: image.PartID + 1, rows: image.Rows, phys: image.Rows, schema: schema, wantErr: "image/ref mismatch"},
		{name: "typed_rows_mismatch", fields: fields, raw: image.Bytes, partID: image.PartID, rows: image.Rows + 1, phys: image.Rows, schema: schema, wantErr: "image/ref mismatch"},
		{name: "physical_rows_mismatch", fields: fields, raw: image.Bytes, partID: image.PartID, rows: image.Rows, phys: image.Rows + 1, schema: schema, wantErr: "image/ref mismatch"},
		{name: "corrupt_bytes", fields: fields, raw: typedColumnStringPredicateCorruptMagic(image.Bytes), partID: image.PartID, rows: image.Rows, phys: image.Rows, schema: schema, wantErr: "invalid part image magic"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := typedColumnAdapterPrepareStringPredicateScanPart(tc.fields, tc.raw, tc.partID, tc.rows, tc.phys, tc.schema, "kind", "beta")
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) || (tc.wantIs != nil && !errors.Is(err, tc.wantIs)) {
				t.Fatalf("prepare err=%v want contains %q is %v", err, tc.wantErr, tc.wantIs)
			}
		})
	}
}

func TestTypedColumnStringPredicateDictionaryMetadataFailsClosed(t *testing.T) {
	field := typedColumnStringPredicateField(false)
	part := typedColumnStringPredicateBuildPart(t, field, []string{"alpha", "beta", "gamma"})
	fields := []TypedStorageField{field}
	schema := uint64(part.Part.Descriptor.SchemaVersion)

	for _, tc := range []struct {
		name    string
		mutate  func(map[string]map[string]int64)
		wantErr string
	}{
		{
			name: "missing_value_type_metadata",
			mutate: func(dicts map[string]map[string]int64) {
				delete(dicts[typedColumnAdapterMetadataDictionary], typedColumnAdapterMetadataKey(part.Columns[0]))
			},
			wantErr: "value type metadata mismatch",
		},
		{
			name: "missing_dictionary_identity_metadata",
			mutate: func(dicts map[string]map[string]int64) {
				delete(dicts[typedColumnAdapterMetadataDictionary], typedColumnAdapterMetadataEntryKey(part.Columns[0], typedColumnAdapterMetadataDictionaryIdentityMark, typedColumnAdapterStringDictionaryIdentity))
			},
			wantErr: "dictionary identity metadata mismatch",
		},
		{
			name: "mismatched_dictionary_order_metadata",
			mutate: func(dicts map[string]map[string]int64) {
				delete(dicts[typedColumnAdapterMetadataDictionary], typedColumnAdapterMetadataEntryKey(part.Columns[0], typedColumnAdapterMetadataDictionaryOrderMark, typedColumnAdapterStringDictionaryOrder))
				dicts[typedColumnAdapterMetadataDictionary][typedColumnAdapterMetadataEntryKey(part.Columns[0], typedColumnAdapterMetadataDictionaryOrderMark, "lexical")] = 99
			},
			wantErr: "dictionary order metadata mismatch",
		},
		{
			name: "mismatched_dictionary_collation_metadata",
			mutate: func(dicts map[string]map[string]int64) {
				delete(dicts[typedColumnAdapterMetadataDictionary], typedColumnAdapterMetadataEntryKey(part.Columns[0], typedColumnAdapterMetadataDictionaryCollationMark, typedColumnAdapterStringDictionaryCollation))
				dicts[typedColumnAdapterMetadataDictionary][typedColumnAdapterMetadataEntryKey(part.Columns[0], typedColumnAdapterMetadataDictionaryCollationMark, "unicode-codepoint-v1")] = 100
			},
			wantErr: "dictionary collation metadata mismatch",
		},
		{
			name: "missing_dictionary",
			mutate: func(dicts map[string]map[string]int64) {
				delete(dicts, "kind")
			},
			wantErr: "missing dictionary for low-cardinality column kind",
		},
		{
			name: "missing_dictionary_entry",
			mutate: func(dicts map[string]map[string]int64) {
				delete(dicts["kind"], "gamma")
			},
			wantErr: "missing dictionary code",
		},
		{
			name: "duplicate_dictionary_code",
			mutate: func(dicts map[string]map[string]int64) {
				dicts["kind"]["beta"] = dicts["kind"]["alpha"]
			},
			wantErr: "duplicate dictionary code",
		},
		{
			name: "dictionary_code_outside_cardinality",
			mutate: func(dicts map[string]map[string]int64) {
				dicts["kind"]["gamma"] = 3
			},
			wantErr: "outside cardinality",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dicts := typedColumnStringPredicateCloneDictionaries(part.Dictionary)
			tc.mutate(dicts)
			image, err := typedcolumn.BuildColumnPartImage(part.Part, typedcolumn.ColumnPartImageOptions{Dictionaries: dicts})
			if err != nil {
				t.Fatalf("BuildColumnPartImage: %v", err)
			}
			_, err = typedColumnAdapterPrepareStringPredicateScanPart(fields, image.Bytes, image.PartID, image.Rows, image.Rows, schema, "kind", "beta")
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("prepare err=%v want %q", err, tc.wantErr)
			}
		})
	}
}

func TestTypedColumnStringPredicateLayoutDictionaryIdentityFailsClosed(t *testing.T) {
	field := typedColumnStringPredicateField(false)
	part := typedColumnStringPredicateBuildPart(t, field, []string{"alpha", "beta", "gamma"})
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	corrupt := image
	corrupt.Bytes = bytes.Clone(image.Bytes)
	for _, section := range corrupt.Sections {
		if section.Kind == typedcolumn.ColumnPartImageSectionDictionaries {
			if section.Length == 0 {
				t.Fatalf("dictionary section is empty: %+v", section)
			}
			corrupt.Bytes[section.Offset+section.Length-1] ^= 0x01
			_, err = typedColumnAdapterPrepareStringPredicateScanPart([]TypedStorageField{field}, corrupt.Bytes, corrupt.PartID, corrupt.Rows, corrupt.Rows, uint64(part.Part.Descriptor.SchemaVersion), "kind", "beta")
			if err == nil || !strings.Contains(err.Error(), "dictionary/layout identity validation") || !strings.Contains(err.Error(), "checksum") {
				t.Fatalf("prepare corrupt dictionary layout err=%v want identity checksum failure", err)
			}
			return
		}
	}
	t.Fatal("missing dictionaries section")
}

func TestTypedColumnStringPredicatePayloadCodeCorruptionFailsClosedAtLayoutValidation(t *testing.T) {
	field := typedColumnStringPredicateField(false)
	part := typedColumnStringPredicateBuildPart(t, field, []string{"alpha", "beta", "gamma"})
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	corrupt := image
	corrupt.Bytes = bytes.Clone(image.Bytes)
	corrupt.Sections = slices.Clone(image.Sections)
	section := typedColumnAdapterFindColumnSection(t, corrupt, "kind")
	if section.Length < 3 {
		t.Fatalf("kind section too short: %+v", section)
	}
	// Low-cardinality payload is width byte, cardinality uvarint, then code bytes.
	// The adapter builds uncompressed uint8 code payloads for this small dictionary.
	corrupt.Bytes[section.Offset+2] = byte(part.Part.Columns["kind"].Definition.Cardinality)

	_, err = typedColumnAdapterPrepareStringPredicateScanPart([]TypedStorageField{field}, corrupt.Bytes, corrupt.PartID, corrupt.Rows, corrupt.Rows, uint64(part.Part.Descriptor.SchemaVersion), "kind", "alpha")
	if err == nil || !strings.Contains(err.Error(), "dictionary/layout identity validation") || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("prepare corrupt payload err=%v want layout checksum rejection", err)
	}
}

func TestTypedColumnStringPredicateProductionScanMinMaxFailsClosed(t *testing.T) {
	field := typedColumnStringPredicateField(false)
	part := typedColumnStringPredicateBuildPart(t, field, []string{"alpha", "beta", "gamma"})
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	prepared, err := typedColumnAdapterPrepareStringPredicateScanPart([]TypedStorageField{field}, image.Bytes, image.PartID, image.Rows, image.Rows, uint64(part.Part.Descriptor.SchemaVersion), "kind", "alpha")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	col := prepared.AdapterPart.Part.Columns[prepared.Column.Definition.Name]
	if len(col.Blocks) == 0 || !col.Blocks[0].Granule.HasMinMax {
		t.Fatalf("prepared column blocks=%+v want min/max block", col.Blocks)
	}
	col.Blocks[0].Granule.Min = -1
	prepared.AdapterPart.Part.Columns[prepared.Column.Definition.Name] = col
	_, _, _, err = scanTypedColumnStringEqualityPredicateCodes(prepared.AdapterPart.Part, prepared.Column.Definition.Name, prepared.QueryCode, prepared.QueryCodeFound, nil)
	if err == nil || !strings.Contains(err.Error(), "outside cardinality") {
		t.Fatalf("code scan min/max err=%v want outside cardinality", err)
	}
	result := TypedColumnStringPredicateScanResult{}
	_, err = scanTypedColumnStringPredicatePartWithVisibility(prepared.AdapterPart.Part, prepared.Column.Definition.Name, prepared.QueryCode, "alpha", image.PartID, image.PartID, &result, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "outside cardinality") {
		t.Fatalf("production scan min/max err=%v want outside cardinality", err)
	}
}

func TestTypedColumnStringPredicateDurableValidationFailsClosed(t *testing.T) {
	d, col := setupTypedColumnStringPredicateCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnStringPredicateRows(t, col, []string{"alpha", "beta", "beta"})
	refs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(refs) != 1 {
		t.Fatalf("typed refs=%+v want one", refs)
	}
	corruptTypedColumnAssetPayload1755(t, d, refs[0])
	if _, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), refs[0]); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("read corrupt typed_column_part err=%v want checksum failure", err)
	}
}

func TestTypedColumnStringPredicateDurableReopenHelper(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	col := createTypedColumnStringPredicateCollection(t, d)
	insertTypedColumnStringPredicateRows(t, col, []string{"alpha", "beta", "gamma", "beta"})
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
		t.Fatalf("OpenCollection: %v", err)
	}
	prepared := typedColumnStringPredicatePrepareDurablePart(t, reopened, reopenedCol, "beta")
	var gotRows []int
	pruned, scanned, matched, err := scanTypedColumnStringEqualityPredicateCodes(prepared.AdapterPart.Part, prepared.Column.Definition.Name, prepared.QueryCode, prepared.QueryCodeFound, func(rowIndex int, primaryID int64) error {
		gotRows = append(gotRows, rowIndex)
		if primaryID != int64(rowIndex) {
			return fmt.Errorf("primary_id=%d want row_index=%d", primaryID, rowIndex)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan reopened: %v", err)
	}
	if pruned || scanned != 4 || matched != 2 || !slices.Equal(gotRows, []int{1, 3}) {
		t.Fatalf("reopened pruned=%v scanned=%d matched=%d rows=%v", pruned, scanned, matched, gotRows)
	}
}

func TestTypedColumnStringPredicateGenerationPairingFailsClosed(t *testing.T) {
	d, col := setupTypedColumnStringPredicateCollection(t)
	defer func() { _ = d.Close() }()
	insertTypedColumnStringPredicateRows(t, col, []string{"alpha", "beta"})
	view, closeView, err := col.prepareColumnPhysicalScanSnapshotViewWithSidecars(columnManifestScanNoSidecars())
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		t.Fatalf("prepareColumnPhysicalScanSnapshotViewWithSidecars: %v", err)
	}
	refsByGeneration := make(map[uint64]columnManifestAssetRefForScan)
	for _, ref := range view.TypedColumnPartRefs {
		if ref.Ref.Kind == ColumnAssetKindTCS1TypedColumnPart && ref.Ref.PartID == typedColumnPartAssetPartID {
			refsByGeneration[ref.Ref.Generation] = ref
		}
	}
	if len(refsByGeneration) != 1 || len(view.AssetRefs) != 1 {
		t.Fatalf("view typed=%+v physical=%+v want one each", refsByGeneration, view.AssetRefs)
	}
	physical := append([]columnManifestAssetRefForScan(nil), view.AssetRefs...)
	physical[0].Ref.Generation++
	_, err = validateTypedColumnPhysicalAssetPairing(refsByGeneration, physical)
	if err == nil || !strings.Contains(err.Error(), "missing typed_column_part asset") {
		t.Fatalf("validate generation mismatch err=%v want missing typed_column_part asset", err)
	}
}

func typedColumnStringPredicateField(nullable bool) TypedStorageField {
	field := TypedStorageField{Name: "kind", Path: "kind", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueString, Nullable: nullable}
	return field
}

func typedColumnStringPredicateBuildPart(t testing.TB, field TypedStorageField, values []string) *typedColumnAdapterPart {
	t.Helper()
	rows := make([]typedColumnAdapterRow, len(values))
	for i, value := range values {
		rows[i] = typedColumnAdapterRow{PrimaryID: int64(i), Values: map[string]columnDeclaredValue{field.Path: {Type: ColumnStoreValueString, Present: true, String: value}}}
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 7, SchemaVersion: 123, RowsPerGranule: 32, Fields: []TypedStorageField{field}}, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	return part
}

func typedColumnStringPredicateCloneDictionaries(in map[string]map[string]int64) map[string]map[string]int64 {
	out := make(map[string]map[string]int64, len(in))
	for name, dict := range in {
		clone := make(map[string]int64, len(dict))
		for value, code := range dict {
			clone[value] = code
		}
		out[name] = clone
	}
	return out
}

func typedColumnStringPredicateCorruptMagic(raw []byte) []byte {
	out := bytes.Clone(raw)
	if len(out) != 0 {
		out[0] ^= 0xff
	}
	return out
}

func setupTypedColumnStringPredicateCollection(tb testing.TB) (*backenddb.DB, *Collection) {
	tb.Helper()
	d := openTypedColumnInt64ScanDB(tb)
	return d, createTypedColumnStringPredicateCollection(tb, d)
}

func createTypedColumnStringPredicateCollection(tb testing.TB, d *backenddb.DB) *Collection {
	tb.Helper()
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart}}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		tb.Fatalf("OpenCollection: %v", err)
	}
	return col
}

func insertTypedColumnStringPredicateRows(tb testing.TB, col *Collection, values []string) {
	tb.Helper()
	ids := make([][]byte, len(values))
	docs := make([][]byte, len(values))
	for i, value := range values {
		ids[i] = []byte(fmt.Sprintf("kind-%06d", i))
		docs[i] = []byte(fmt.Sprintf(`{"kind":%q}`, value))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		tb.Fatalf("InsertBatch: %v", err)
	}
}

func typedColumnStringPredicatePrepareDurablePart(tb testing.TB, d *backenddb.DB, col *Collection, query string) typedColumnStringPredicatePreparedPart {
	tb.Helper()
	refs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(tb, d, col))
	if len(refs) != 1 {
		tb.Fatalf("typed refs=%+v want one", refs)
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), refs[0])
	if err != nil {
		tb.Fatalf("read typed-column part: %v", err)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		tb.Fatalf("ParseColumnPartImage: %v", err)
	}
	cfg := col.Meta().Options.ColumnStore
	if cfg == nil {
		tb.Fatal("missing column store config")
	}
	prepared, err := typedColumnAdapterPrepareStringPredicateScanPart(columnStoreTypedColumnPartFields(*cfg), raw, refs[0].PartID, image.Rows, image.Rows, cfg.SchemaHash, "kind", query)
	if err != nil {
		tb.Fatalf("typedColumnAdapterPrepareStringPredicateScanPart: %v", err)
	}
	return prepared
}
