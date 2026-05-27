package collections

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func testTypedStorageCompatibilityConfig(policy ColumnRetainedPayloadPolicy) *ColumnStoreConfig {
	return &ColumnStoreConfig{
		Enabled:         true,
		RetainedPayload: policy,
		Columns: []ColumnStoreColumn{
			{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
			{Name: "tag", Path: "tag", ValueType: ColumnStoreValueString, Dictionary: true},
		},
		AggregateMetadata: []ColumnAggregateMetadata{
			{Name: "time_count", Column: "time_us", Kind: ColumnAggregateCount},
		},
	}
}

func requireTypedStorageOwner(t *testing.T, layout TypedStorageLayout, path string, want TypedStorageFieldOwner) {
	t.Helper()
	got, ok := layout.OwnerForPath(path)
	if !ok {
		t.Fatalf("OwnerForPath(%q) missing; layout rows=%v", path, layout.FieldOwnerDebugRows())
	}
	if got != want {
		t.Fatalf("OwnerForPath(%q)=%q want %q; layout rows=%v", path, got, want, layout.FieldOwnerDebugRows())
	}
}

func TestTypedStorageLayoutNormalizeDocumentOnly(t *testing.T) {
	layout, err := ResolveTypedStorageLayout(CollectionMeta{Name: "docs"})
	if err != nil {
		t.Fatalf("ResolveTypedStorageLayout: %v", err)
	}
	if layout.Enabled {
		t.Fatalf("document-only layout enabled=%v want false", layout.Enabled)
	}
	if len(layout.Fields) != 0 {
		t.Fatalf("document-only layout fields=%v want none", layout.Fields)
	}
	requireTypedStorageOwner(t, layout, "anything", TypedStorageOwnerRetainedDocument)
	if !layout.RetainedDocumentOwnsRemainder {
		t.Fatalf("document-only layout should retain document ownership for the remainder")
	}
	if layout.RetainedDocumentCompatibilityDuplicate {
		t.Fatalf("document-only layout should not mark compatibility duplication")
	}
	if rows := layout.FieldOwnerDebugRows(); !reflect.DeepEqual(rows, []string{"* -> retained_document(remainder)"}) {
		t.Fatalf("debug rows=%v", rows)
	}
}

func TestTypedStorageLayoutNormalizeExistingColumnStoreConfigUsesTypedRowAsset(t *testing.T) {
	layout, err := ResolveTypedStorageLayout(CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: testTypedStorageCompatibilityConfig(ColumnRetainedPayloadNonColumn)},
	})
	if err != nil {
		t.Fatalf("ResolveTypedStorageLayout: %v", err)
	}
	if !layout.Enabled {
		t.Fatalf("typed-storage compatibility layout enabled=false")
	}
	requireTypedStorageOwner(t, layout, "time_us", TypedStorageOwnerRowAsset)
	requireTypedStorageOwner(t, layout, "tag", TypedStorageOwnerRowAsset)
	requireTypedStorageOwner(t, layout, "payload", TypedStorageOwnerRetainedDocument)
	if layout.RetainedPayload != ColumnRetainedPayloadNonColumn {
		t.Fatalf("retained payload=%q want %q", layout.RetainedPayload, ColumnRetainedPayloadNonColumn)
	}
	if layout.RetainedDocumentCompatibilityDuplicate {
		t.Fatalf("non-column retained payload must not mark compatibility duplication")
	}
	if err := layout.EnsureReadSupported(); err != nil {
		t.Fatalf("typed-row layout read support: %v", err)
	}
	if err := layout.EnsurePublicationSupported(); err != nil {
		t.Fatalf("typed-row layout publication support: %v", err)
	}
}

func TestTypedStorageLayoutTypedColumnScalarSupported(t *testing.T) {
	layout, err := NormalizeTypedStorageLayout(TypedStorageLayout{
		Collection: "events",
		Fields: []TypedStorageField{{
			Name:      "score",
			Path:      "score",
			Owner:     TypedStorageOwnerColumnPart,
			ValueType: ColumnStoreValueDouble,
		}},
	})
	if err != nil {
		t.Fatalf("NormalizeTypedStorageLayout: %v", err)
	}
	requireTypedStorageOwner(t, layout, "score", TypedStorageOwnerColumnPart)
	if !layout.HasTypedColumnPartOwners() {
		t.Fatalf("typed-column part owner not detected")
	}
	if err := layout.EnsureReadSupported(); err != nil {
		t.Fatalf("EnsureReadSupported: %v", err)
	}
	if err := layout.EnsurePublicationSupported(); err != nil {
		t.Fatalf("EnsurePublicationSupported: %v", err)
	}
}

func TestTypedStorageLayoutTypedColumnVectorSupported(t *testing.T) {
	layout, err := NormalizeTypedStorageLayout(TypedStorageLayout{
		Collection: "events",
		Fields: []TypedStorageField{{
			Name:       "embedding",
			Path:       "embedding",
			Owner:      TypedStorageOwnerColumnPart,
			ValueType:  ColumnStoreValueFloat32Vector,
			VectorDims: 3,
		}},
	})
	if err != nil {
		t.Fatalf("NormalizeTypedStorageLayout: %v", err)
	}
	if err := layout.EnsureReadSupported(); err != nil {
		t.Fatalf("EnsureReadSupported: %v", err)
	}
	if err := layout.EnsurePublicationSupported(); err != nil {
		t.Fatalf("EnsurePublicationSupported: %v", err)
	}
}

func TestTypedStorageLayoutTypedColumnNullableVectorFailsClosed(t *testing.T) {
	layout, err := NormalizeTypedStorageLayout(TypedStorageLayout{
		Collection: "events",
		Fields: []TypedStorageField{{
			Name:       "embedding",
			Path:       "embedding",
			Owner:      TypedStorageOwnerColumnPart,
			ValueType:  ColumnStoreValueFloat32Vector,
			VectorDims: 3,
			Nullable:   true,
		}},
	})
	if err != nil {
		t.Fatalf("NormalizeTypedStorageLayout: %v", err)
	}
	if err := layout.EnsureReadSupported(); !errors.Is(err, ErrTypedStorageColumnPartUnsupported) {
		t.Fatalf("EnsureReadSupported error=%v want %v", err, ErrTypedStorageColumnPartUnsupported)
	}
	if err := layout.EnsurePublicationSupported(); !errors.Is(err, ErrTypedStorageColumnPartUnsupported) {
		t.Fatalf("EnsurePublicationSupported error=%v want %v", err, ErrTypedStorageColumnPartUnsupported)
	}
}

func TestTypedStorageLayoutTypedColumnAdjacencySupportedWithDegree(t *testing.T) {
	layout, err := NormalizeTypedStorageLayout(TypedStorageLayout{
		Collection: "events",
		Fields: []TypedStorageField{{
			Name:            "embedding_neighbors",
			Path:            "embedding_neighbors",
			Owner:           TypedStorageOwnerColumnPart,
			ValueType:       ColumnStoreValueAdjacencyList,
			AdjacencyDegree: 16,
		}},
	})
	if err != nil {
		t.Fatalf("NormalizeTypedStorageLayout: %v", err)
	}
	if err := layout.EnsureReadSupported(); err != nil {
		t.Fatalf("EnsureReadSupported: %v", err)
	}
	if err := layout.EnsurePublicationSupported(); err != nil {
		t.Fatalf("EnsurePublicationSupported: %v", err)
	}
}

func TestTypedStorageLayoutAdjacencyOffsetsListSelectorSupported1915(t *testing.T) {
	layout, err := NormalizeTypedStorageLayout(TypedStorageLayout{
		Collection: "events",
		Fields: []TypedStorageField{{
			Name:            "embedding_neighbors",
			Path:            "embedding_neighbors",
			Owner:           TypedStorageOwnerColumnPart,
			ValueType:       ColumnStoreValueAdjacencyList,
			AdjacencyLayout: ColumnAdjacencyListLayoutUint32OffsetsList,
		}},
	})
	if err != nil {
		t.Fatalf("NormalizeTypedStorageLayout: %v", err)
	}
	field := layout.Fields[0]
	if field.AdjacencyLayout != ColumnAdjacencyListLayoutUint32OffsetsList || field.AdjacencyDegree != 0 {
		t.Fatalf("normalized offsets-list field=%+v", field)
	}
	if err := layout.EnsureReadSupported(); err != nil {
		t.Fatalf("EnsureReadSupported: %v", err)
	}
	if err := layout.EnsurePublicationSupported(); err != nil {
		t.Fatalf("EnsurePublicationSupported: %v", err)
	}
}

func TestTypedStorageLayoutTypedColumnAdjacencyRequiresDegreeAndNonNullable(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field TypedStorageField
		want  string
	}{
		{name: "missing_degree", field: TypedStorageField{Name: "embedding_neighbors", Path: "embedding_neighbors", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueAdjacencyList}, want: "adjacency_degree"},
		{name: "row_asset_degree", field: TypedStorageField{Name: "embedding_neighbors", Path: "embedding_neighbors", ValueType: ColumnStoreValueAdjacencyList, AdjacencyDegree: 16}, want: "only adjacency_list typed_column_part fields may set adjacency_degree"},
		{name: "nullable", field: TypedStorageField{Name: "embedding_neighbors", Path: "embedding_neighbors", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueAdjacencyList, Nullable: true, AdjacencyDegree: 16}, want: "nullable adjacency_list"},
		{name: "offsets_list_with_degree", field: TypedStorageField{Name: "embedding_neighbors", Path: "embedding_neighbors", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueAdjacencyList, AdjacencyLayout: ColumnAdjacencyListLayoutUint32OffsetsList, AdjacencyDegree: 16}, want: "must be zero for adjacency_layout"},
		{name: "offsets_list_row_asset", field: TypedStorageField{Name: "embedding_neighbors", Path: "embedding_neighbors", ValueType: ColumnStoreValueAdjacencyList, AdjacencyLayout: ColumnAdjacencyListLayoutUint32OffsetsList}, want: "uint32_offsets_list requires owner"},
		{name: "adjacency_layout_non_adjacency", field: TypedStorageField{Name: "count", Path: "count", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueInt64, AdjacencyLayout: ColumnAdjacencyListLayoutUint32OffsetsList}, want: "only adjacency_list fields may set adjacency_layout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NormalizeTypedStorageLayout(TypedStorageLayout{Collection: "events", Fields: []TypedStorageField{tc.field}})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("NormalizeTypedStorageLayout error=%v want containing %q", err, tc.want)
			}
		})
	}
}

func TestTypedStorageLayoutHybridOwners(t *testing.T) {
	layout, err := NormalizeTypedStorageLayout(TypedStorageLayout{
		Collection:      "events",
		RetainedPayload: ColumnRetainedPayloadNonColumn,
		Fields: []TypedStorageField{
			{Name: "profile_name", Path: "profile.name", Owner: TypedStorageOwnerRetainedDocument},
			{Name: "time_us", Path: "time_us", Owner: TypedStorageOwnerRowAsset, ValueType: ColumnStoreValueInt64},
			{Name: "embedding", Path: "embedding", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueFloat32Vector, VectorDims: 3},
		},
	})
	if err != nil {
		t.Fatalf("NormalizeTypedStorageLayout: %v", err)
	}
	requireTypedStorageOwner(t, layout, "profile.name", TypedStorageOwnerRetainedDocument)
	requireTypedStorageOwner(t, layout, "time_us", TypedStorageOwnerRowAsset)
	requireTypedStorageOwner(t, layout, "embedding", TypedStorageOwnerColumnPart)
	requireTypedStorageOwner(t, layout, "unmodeled", TypedStorageOwnerRetainedDocument)
	rows := strings.Join(layout.FieldOwnerDebugRows(), "\n")
	for _, want := range []string{
		"profile.name -> retained_document",
		"time_us -> typed_row_asset",
		"embedding -> typed_column_part",
		"* -> retained_document(remainder)",
	} {
		if !strings.Contains(rows, want) {
			t.Fatalf("debug rows missing %q in:\n%s", want, rows)
		}
	}
}

func TestTypedStorageLayoutRejectsOverlappingAuthoritativeOwners(t *testing.T) {
	_, err := NormalizeTypedStorageLayout(TypedStorageLayout{
		Collection: "events",
		Fields: []TypedStorageField{
			{Name: "time_row", Path: "time_us", Owner: TypedStorageOwnerRowAsset, ValueType: ColumnStoreValueInt64},
			{Name: "time_column", Path: "time_us", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueInt64},
		},
	})
	if err == nil {
		t.Fatalf("expected overlapping owner error")
	}
	if !strings.Contains(err.Error(), "overlapping authoritative typed-storage owners") {
		t.Fatalf("error=%v", err)
	}
}

func TestTypedStorageLayoutColumnRetainedPayloadFullCompatibility(t *testing.T) {
	layout, err := ResolveTypedStorageLayout(CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: testTypedStorageCompatibilityConfig(ColumnRetainedPayloadFull)},
	})
	if err != nil {
		t.Fatalf("ResolveTypedStorageLayout: %v", err)
	}
	requireTypedStorageOwner(t, layout, "time_us", TypedStorageOwnerRowAsset)
	requireTypedStorageOwner(t, layout, "undeclared", TypedStorageOwnerRetainedDocument)
	if !layout.RetainedDocumentOwnsRemainder {
		t.Fatalf("ColumnRetainedPayloadFull should keep retained document remainder ownership")
	}
	if !layout.RetainedDocumentCompatibilityDuplicate {
		t.Fatalf("ColumnRetainedPayloadFull should mark compatibility duplication")
	}
	for _, row := range layout.FieldOwnerDebugRows() {
		if row == "time_us -> retained_document" {
			t.Fatalf("declared field was treated as overlapping retained ownership: %v", layout.FieldOwnerDebugRows())
		}
	}

	retainedOnly, err := NormalizeTypedStorageLayout(TypedStorageLayout{
		Collection:      "events",
		RetainedPayload: ColumnRetainedPayloadFull,
		Fields: []TypedStorageField{{
			Name:  "profile_name",
			Path:  "profile.name",
			Owner: TypedStorageOwnerRetainedDocument,
		}},
	})
	if err != nil {
		t.Fatalf("NormalizeTypedStorageLayout retained-only full payload: %v", err)
	}
	if retainedOnly.RetainedDocumentCompatibilityDuplicate {
		t.Fatalf("retained-only full payload should not mark typed-field compatibility duplication")
	}
}

func TestTypedStorageDerivedAcceleratorNotAuthoritative(t *testing.T) {
	_, err := NormalizeTypedStorageLayout(TypedStorageLayout{
		Collection: "events",
		Fields: []TypedStorageField{{
			Name:  "bad",
			Path:  "bad",
			Owner: TypedStorageFieldOwner(TypedStorageAssetClassDerivedAccelerator),
		}},
	})
	if err == nil {
		t.Fatalf("expected derived-accelerator owner rejection")
	}
	if !strings.Contains(err.Error(), "derived_accelerator is not an authoritative field owner") {
		t.Fatalf("error=%v", err)
	}

	layout, err := NormalizeTypedStorageLayout(TypedStorageLayout{
		Collection: "events",
		Fields: []TypedStorageField{{
			Name:      "time_us",
			Path:      "time_us",
			Owner:     TypedStorageOwnerRowAsset,
			ValueType: ColumnStoreValueInt64,
		}},
		DerivedAccelerators: []TypedStorageDerivedAccelerator{{
			Name:            "time_us:int64_values",
			Class:           TypedStorageAssetClassDerivedAccelerator,
			SourceFieldPath: "time_us",
			Generation:      7,
		}},
	})
	if err != nil {
		t.Fatalf("NormalizeTypedStorageLayout with derived accelerator: %v", err)
	}
	requireTypedStorageOwner(t, layout, "time_us", TypedStorageOwnerRowAsset)
	if got := len(layout.DerivedAccelerators); got != 1 {
		t.Fatalf("derived accelerators=%d want 1; layout=%v", got, layout.FieldOwnerDebugRows())
	}
	if got := layout.DerivedAccelerators[0].Class; got != TypedStorageAssetClassDerivedAccelerator {
		t.Fatalf("derived class=%q want %q", got, TypedStorageAssetClassDerivedAccelerator)
	}
	if got := layout.DerivedAccelerators[0].SourceOwner; got != TypedStorageOwnerRowAsset {
		t.Fatalf("derived source owner=%q want %q", got, TypedStorageOwnerRowAsset)
	}
}

func TestTypedStorageCompatibilityAliases(t *testing.T) {
	var cfg ColumnStoreConfig
	cfg.Enabled = true
	cfg.Columns = []ColumnStoreColumn{{
		Name:      "compat_field",
		Path:      "compat_field",
		ValueType: ColumnStoreValueString,
	}}

	layout, err := ResolveTypedStorageLayout(CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: &cfg},
	})
	if err != nil {
		t.Fatalf("ResolveTypedStorageLayout with public compatibility config: %v", err)
	}
	requireTypedStorageOwner(t, layout, "compat_field", TypedStorageOwnerRowAsset)
}

func TestTypedStorageCompatibilityConfigCanOptIntoColumnPartOwner(t *testing.T) {
	cfg := ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{{
			Name:      "score",
			Path:      "score",
			ValueType: ColumnStoreValueDouble,
			Owner:     TypedStorageOwnerColumnPart,
		}},
	}
	layout, err := ResolveTypedStorageLayout(CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: &cfg},
	})
	if err != nil {
		t.Fatalf("ResolveTypedStorageLayout with typed-column owner: %v", err)
	}
	requireTypedStorageOwner(t, layout, "score", TypedStorageOwnerColumnPart)
	if err := layout.EnsureReadSupported(); err != nil {
		t.Fatalf("EnsureReadSupported: %v", err)
	}
	if err := layout.EnsurePublicationSupported(); err != nil {
		t.Fatalf("EnsurePublicationSupported: %v", err)
	}
}

func TestTypedStorageDebugRowsUseCanonicalVocabularyOnly(t *testing.T) {
	cases := []struct {
		name   string
		layout TypedStorageLayout
		want   []string
	}{
		{
			name: "retained document remainder only",
			layout: TypedStorageLayout{
				Collection:      "events",
				RetainedPayload: ColumnRetainedPayloadFull,
			},
			want: []string{"* -> retained_document(remainder)"},
		},
		{
			name: "typed row typed column and compatibility duplicate",
			layout: TypedStorageLayout{
				Collection:      "events",
				RetainedPayload: ColumnRetainedPayloadFull,
				Fields: []TypedStorageField{
					{Name: "time_us", Path: "time_us", Owner: TypedStorageOwnerRowAsset, ValueType: ColumnStoreValueInt64},
					{Name: "embedding", Path: "embedding", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueFloat32Vector, VectorDims: 3},
				},
			},
			want: []string{
				"* -> retained_document(remainder)",
				"document_payload -> compatibility_duplicate",
				"embedding -> typed_column_part",
				"time_us -> typed_row_asset",
			},
		},
		{
			name: "no retained document remainder",
			layout: TypedStorageLayout{
				Collection:      "events",
				RetainedPayload: ColumnRetainedPayloadNone,
				Fields: []TypedStorageField{
					{Name: "score", Path: "score", Owner: TypedStorageOwnerRowAsset, ValueType: ColumnStoreValueDouble},
				},
			},
			want: []string{"score -> typed_row_asset"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			layout, err := NormalizeTypedStorageLayout(tc.layout)
			if err != nil {
				t.Fatalf("NormalizeTypedStorageLayout: %v", err)
			}
			rows := layout.FieldOwnerDebugRows()
			if !reflect.DeepEqual(rows, tc.want) {
				t.Fatalf("status/debug rows = %v, want exact canonical rows %v", rows, tc.want)
			}
			joinedRows := strings.Join(rows, "\n")
			for _, legacyUmbrella := range []string{"column " + "store", "column-" + "store", "Column" + "Store"} {
				if strings.Contains(joinedRows, legacyUmbrella) {
					t.Fatalf("status/debug rows contain legacy umbrella %q in:\n%s", legacyUmbrella, joinedRows)
				}
			}
		})
	}
}

func TestTypedStorageLayoutResolverIsPureMetadata(t *testing.T) {
	cfg := &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{{
			Name:      "time_us",
			Path:      "time_us",
			ValueType: ColumnStoreValueInt64,
		}},
		AssetManager: &ColumnAssetManagerConfig{
			Kind:      ColumnAssetManagerValueLogShaped,
			Namespace: "does/not/exist/on/disk",
		},
	}
	layout, err := ResolveTypedStorageLayout(CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: cfg},
	})
	if err != nil {
		t.Fatalf("ResolveTypedStorageLayout should validate metadata only without opening asset namespace: %v", err)
	}
	requireTypedStorageOwner(t, layout, "time_us", TypedStorageOwnerRowAsset)
	if rows := layout.FieldOwnerDebugRows(); len(rows) == 0 {
		t.Fatalf("expected debug rows from pure metadata resolver")
	}
}

func TestTypedStorageLayoutRetainedPayloadPolicyMatrix(t *testing.T) {
	cases := []struct {
		name          string
		policy        ColumnRetainedPayloadPolicy
		wantRemainder bool
		wantDuplicate bool
		wantEnabled   bool
		wantRows      []string
	}{
		{
			name:          "full duplicates retained document",
			policy:        "full",
			wantRemainder: true,
			wantDuplicate: true,
			wantEnabled:   true,
			wantRows: []string{
				"* -> retained_document(remainder)",
				"document_payload -> compatibility_duplicate",
				"tag -> typed_row_asset",
				"time_us -> typed_row_asset",
			},
		},
		{
			name:          "non-column retains only undeclared fields",
			policy:        "non-column",
			wantRemainder: true,
			wantDuplicate: false,
			wantEnabled:   true,
			wantRows: []string{
				"* -> retained_document(remainder)",
				"tag -> typed_row_asset",
				"time_us -> typed_row_asset",
			},
		},
		{
			name:          "none leaves no undeclared owner",
			policy:        "none",
			wantRemainder: false,
			wantDuplicate: false,
			wantEnabled:   true,
			wantRows: []string{
				"tag -> typed_row_asset",
				"time_us -> typed_row_asset",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			layout, err := ResolveTypedStorageLayout(CollectionMeta{
				Name:    "events",
				Options: CollectionOptions{ColumnStore: testTypedStorageCompatibilityConfig(tc.policy)},
			})
			if err != nil {
				t.Fatalf("ResolveTypedStorageLayout: %v", err)
			}
			if layout.Enabled != tc.wantEnabled {
				t.Fatalf("enabled=%v want %v", layout.Enabled, tc.wantEnabled)
			}
			if layout.RetainedPayload != tc.policy {
				t.Fatalf("retained payload=%q want %q", layout.RetainedPayload, tc.policy)
			}
			if layout.RetainedDocumentOwnsRemainder != tc.wantRemainder {
				t.Fatalf("retained document remainder=%v want %v", layout.RetainedDocumentOwnsRemainder, tc.wantRemainder)
			}
			if layout.RetainedDocumentCompatibilityDuplicate != tc.wantDuplicate {
				t.Fatalf("compatibility duplicate=%v want %v", layout.RetainedDocumentCompatibilityDuplicate, tc.wantDuplicate)
			}
			requireTypedStorageOwner(t, layout, "time_us", TypedStorageOwnerRowAsset)
			owner, ok := layout.OwnerForPath("undeclared")
			if tc.wantRemainder {
				if !ok || owner != TypedStorageOwnerRetainedDocument {
					t.Fatalf("undeclared owner=(%q,%v) want retained document", owner, ok)
				}
			} else if ok {
				t.Fatalf("undeclared owner=(%q,%v) want no owner", owner, ok)
			}
			if rows := layout.FieldOwnerDebugRows(); !reflect.DeepEqual(rows, tc.wantRows) {
				t.Fatalf("debug rows=%v want %v", rows, tc.wantRows)
			}
		})
	}
}

func TestTypedStorageLayoutColumnPartSupportedScalarMatrix(t *testing.T) {
	for _, field := range []TypedStorageField{
		{Name: "flag", Path: "flag", Owner: TypedStorageOwnerColumnPart, ValueType: "bool"},
		{Name: "time_us", Path: "time_us", Owner: TypedStorageOwnerColumnPart, ValueType: "int64"},
		{Name: "score32", Path: "score32", Owner: TypedStorageOwnerColumnPart, ValueType: "float32"},
		{Name: "score", Path: "score", Owner: TypedStorageOwnerColumnPart, ValueType: "double"},
		{Name: "kind", Path: "kind", Owner: TypedStorageOwnerColumnPart, ValueType: "string"},
	} {
		t.Run(string(field.ValueType), func(t *testing.T) {
			layout, err := NormalizeTypedStorageLayout(TypedStorageLayout{Collection: "events", Fields: []TypedStorageField{field}})
			if err != nil {
				t.Fatalf("NormalizeTypedStorageLayout: %v", err)
			}
			if err := layout.EnsureReadSupported(); err != nil {
				t.Fatalf("EnsureReadSupported: %v", err)
			}
			if err := layout.EnsurePublicationSupported(); err != nil {
				t.Fatalf("EnsurePublicationSupported: %v", err)
			}
		})
		field.Nullable = true
		t.Run(string(field.ValueType)+" nullable", func(t *testing.T) {
			layout, err := NormalizeTypedStorageLayout(TypedStorageLayout{Collection: "events", Fields: []TypedStorageField{field}})
			if err != nil {
				t.Fatalf("NormalizeTypedStorageLayout: %v", err)
			}
			if err := layout.EnsureReadSupported(); err != nil {
				t.Fatalf("EnsureReadSupported nullable scalar: %v", err)
			}
			if err := layout.EnsurePublicationSupported(); err != nil {
				t.Fatalf("EnsurePublicationSupported nullable scalar: %v", err)
			}
		})
	}
	for _, field := range []TypedStorageField{
		{Name: "missing_type", Path: "missing_type", Owner: TypedStorageOwnerColumnPart},
		{Name: "empty_type", Path: "empty_type", Owner: TypedStorageOwnerColumnPart, ValueType: ""},
	} {
		t.Run(field.Name, func(t *testing.T) {
			layout, err := NormalizeTypedStorageLayout(TypedStorageLayout{Collection: "events", Fields: []TypedStorageField{field}})
			if err != nil {
				t.Fatalf("NormalizeTypedStorageLayout: %v", err)
			}
			if err := layout.EnsureReadSupported(); !errors.Is(err, ErrTypedStorageColumnPartUnsupported) {
				t.Fatalf("EnsureReadSupported error=%v want %v", err, ErrTypedStorageColumnPartUnsupported)
			}
			if err := layout.EnsurePublicationSupported(); !errors.Is(err, ErrTypedStorageColumnPartUnsupported) {
				t.Fatalf("EnsurePublicationSupported error=%v want %v", err, ErrTypedStorageColumnPartUnsupported)
			}
		})
	}
}

func TestTypedStorageLayoutValueTypeValidationMatrix(t *testing.T) {
	cases := []struct {
		name  string
		field TypedStorageField
		want  string
	}{
		{name: "invalid vector dims", field: TypedStorageField{Name: "embedding", Path: "embedding", Owner: TypedStorageOwnerColumnPart, ValueType: "float32_vector"}, want: "vector_dims: must be positive"},
		{name: "vector dims on non-vector", field: TypedStorageField{Name: "score", Path: "score", Owner: TypedStorageOwnerColumnPart, ValueType: "double", VectorDims: 3}, want: "vector_dims: only float32_vector fields may set vector_dims"},
		{name: "dictionary on non-string", field: TypedStorageField{Name: "score", Path: "score", Owner: TypedStorageOwnerColumnPart, ValueType: "double", Dictionary: true}, want: "dictionary: unsupported for value_type"},
		{name: "invalid fixed-width encoding", field: TypedStorageField{Name: "embedding", Path: "embedding", Owner: TypedStorageOwnerColumnPart, ValueType: "float32_vector", VectorDims: 3, FixedWidthEncoding: "future"}, want: "fixed_width_encoding"},
		{name: "fixed-width encoding on unsupported type", field: TypedStorageField{Name: "kind", Path: "kind", Owner: TypedStorageOwnerColumnPart, ValueType: "string", FixedWidthEncoding: "little_endian"}, want: "fixed_width_encoding: unsupported for value_type"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NormalizeTypedStorageLayout(TypedStorageLayout{Collection: "events", Fields: []TypedStorageField{tc.field}})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("NormalizeTypedStorageLayout error=%v want containing %q", err, tc.want)
			}
		})
	}
}

func TestTypedStorageDerivedAcceleratorSourceOwnerMatrix(t *testing.T) {
	baseField := TypedStorageField{Name: "score", Path: "score", Owner: TypedStorageOwnerColumnPart, ValueType: "double"}
	cases := []struct {
		name    string
		accel   TypedStorageDerivedAccelerator
		wantErr string
	}{
		{name: "default source owner", accel: TypedStorageDerivedAccelerator{Name: "score:codes", SourceFieldPath: "score"}},
		{name: "explicit matching source owner", accel: TypedStorageDerivedAccelerator{Name: "score:codes", SourceFieldPath: "score", SourceOwner: TypedStorageOwnerColumnPart}},
		{name: "unknown source path", accel: TypedStorageDerivedAccelerator{Name: "missing:codes", SourceFieldPath: "missing"}, wantErr: "references unknown authoritative field path"},
		{name: "mismatched source owner", accel: TypedStorageDerivedAccelerator{Name: "score:codes", SourceFieldPath: "score", SourceOwner: TypedStorageOwnerRowAsset}, wantErr: "does not match authoritative owner"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			layout, err := NormalizeTypedStorageLayout(TypedStorageLayout{Collection: "events", Fields: []TypedStorageField{baseField}, DerivedAccelerators: []TypedStorageDerivedAccelerator{tc.accel}})
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("NormalizeTypedStorageLayout error=%v want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("NormalizeTypedStorageLayout: %v", err)
			}
			if got := layout.DerivedAccelerators[0].SourceOwner; got != TypedStorageOwnerColumnPart {
				t.Fatalf("source owner=%q want %q", got, TypedStorageOwnerColumnPart)
			}
		})
	}
}

func TestResolveTypedStorageLayoutDoesNotMutateInput(t *testing.T) {
	cfg := testTypedStorageCompatibilityConfig("non-column")
	cfg.AssetManager = &ColumnAssetManagerConfig{Kind: "", Namespace: ""}
	cfg.ManifestRoot = &ColumnManifestRootDescriptor{}
	meta := CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}
	metaBefore := mustMarshalTypedStorageLayoutTestJSON(t, meta)
	if _, err := ResolveTypedStorageLayout(meta); err != nil {
		t.Fatalf("ResolveTypedStorageLayout: %v", err)
	}
	if metaAfter := mustMarshalTypedStorageLayoutTestJSON(t, meta); string(metaAfter) != string(metaBefore) {
		t.Fatalf("ResolveTypedStorageLayout mutated input meta/config:\n got: %s\nwant: %s", metaAfter, metaBefore)
	}

	in := TypedStorageLayout{
		Collection:          "events",
		RetainedPayload:     "full",
		Fields:              []TypedStorageField{{Name: "score", Path: "score", Owner: "", ValueType: "double"}},
		DerivedAccelerators: []TypedStorageDerivedAccelerator{{Name: "score:codes", SourceFieldPath: "score"}},
	}
	before := in.copy()
	if _, err := NormalizeTypedStorageLayout(in); err != nil {
		t.Fatalf("NormalizeTypedStorageLayout: %v", err)
	}
	if !reflect.DeepEqual(in, before) {
		t.Fatalf("NormalizeTypedStorageLayout mutated input:\n got: %#v\nwant: %#v", in, before)
	}
}

func mustMarshalTypedStorageLayoutTestJSON(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("Marshal snapshot: %v", err)
	}
	return data
}

func TestTypedStorageLayoutJSONRoundTrip(t *testing.T) {
	want := TypedStorageLayout{
		Collection:                             "events",
		Enabled:                                true,
		RetainedPayload:                        "full",
		RetainedDocumentOwnsRemainder:          true,
		RetainedDocumentCompatibilityDuplicate: true,
		Fields: []TypedStorageField{
			{Name: "time_us", Path: "time_us", Owner: TypedStorageOwnerRowAsset, ValueType: "int64"},
			{Name: "score", Path: "score", Owner: TypedStorageOwnerColumnPart, ValueType: "double", Nullable: true},
			{Name: "kind", Path: "kind", Owner: TypedStorageOwnerRetainedDocument, ValueType: "string", Dictionary: true},
		},
		DerivedAccelerators: []TypedStorageDerivedAccelerator{{
			Name:            "time_us:int64_values",
			Class:           TypedStorageAssetClassDerivedAccelerator,
			SourceFieldPath: "time_us",
			SourceOwner:     TypedStorageOwnerRowAsset,
			Generation:      42,
		}},
	}
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got TypedStorageLayout
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("roundtrip mismatch:\n got: %#v\nwant: %#v\njson: %s", got, want, data)
	}
}
