package collections

import (
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

func TestTypedStorageLayoutTypedColumnUnsupportedValueFailsClosed(t *testing.T) {
	layout, err := NormalizeTypedStorageLayout(TypedStorageLayout{
		Collection: "events",
		Fields: []TypedStorageField{{
			Name:      "embedding_neighbors",
			Path:      "embedding_neighbors",
			Owner:     TypedStorageOwnerColumnPart,
			ValueType: ColumnStoreValueAdjacencyList,
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
				"time_us -> typed_row_asset",
				"embedding -> typed_column_part",
				"* -> retained_document(remainder)",
				"document_payload -> compatibility_duplicate",
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
			rows := strings.Join(layout.FieldOwnerDebugRows(), "\n")
			for _, want := range tc.want {
				if !strings.Contains(rows, want) {
					t.Fatalf("status/debug rows missing %q in:\n%s", want, rows)
				}
			}
			for _, legacyUmbrella := range []string{"column " + "store", "column-" + "store", "Column" + "Store"} {
				if strings.Contains(rows, legacyUmbrella) {
					t.Fatalf("status/debug rows contain legacy umbrella %q in:\n%s", legacyUmbrella, rows)
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
