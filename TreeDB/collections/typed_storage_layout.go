package collections

import (
	"errors"
	"fmt"
	"sort"
)

// ErrTypedStorageColumnPartUnsupported is returned by fail-closed guards when a
// normalized layout contains authoritative typed-column ownership. PR #1751 only
// introduces the pure-metadata resolver; durable typed-column reads and
// publication land in later #1744 children.
var ErrTypedStorageColumnPartUnsupported = errors.New("collections: typed_column_part ownership is not supported yet")

// TypedStorageFieldOwner names the authoritative physical owner for one logical
// field in a normalized typed-storage layout.
type TypedStorageFieldOwner string

const (
	// TypedStorageOwnerRetainedDocument means the field is authoritatively owned
	// by the retained document/document_payload path.
	TypedStorageOwnerRetainedDocument TypedStorageFieldOwner = "retained_document"
	// TypedStorageOwnerRowAsset means the field is authoritatively owned by the
	// current typed-row physical asset path.
	TypedStorageOwnerRowAsset TypedStorageFieldOwner = "typed_row_asset"
	// TypedStorageOwnerColumnPart is a future typed-column owner placeholder. It
	// may be represented by the resolver, but read/publication support fails
	// closed until the typed-column format and publication path land.
	TypedStorageOwnerColumnPart TypedStorageFieldOwner = "typed_column_part"
)

// TypedStorageAssetClass classifies non-authoritative assets associated with a
// typed-storage owner/generation.
type TypedStorageAssetClass string

const (
	// TypedStorageAssetClassDerivedAccelerator marks dictionaries, int64 values,
	// aggregate metadata, vector graphs, and caches as derived sidecars. It is not
	// an authoritative field owner.
	TypedStorageAssetClassDerivedAccelerator TypedStorageAssetClass = "derived_accelerator"
)

// TypedStorageField describes one explicitly resolved logical field owner. The
// zero Owner is normalized to TypedStorageOwnerRowAsset for compatibility with
// existing ColumnStoreConfig declared typed fields.
type TypedStorageField struct {
	Name               string                   `json:"name,omitempty"`
	Path               string                   `json:"path"`
	Owner              TypedStorageFieldOwner   `json:"owner"`
	ValueType          ColumnStoreValueType     `json:"value_type,omitempty"`
	Nullable           bool                     `json:"nullable,omitempty"`
	Dictionary         bool                     `json:"dictionary,omitempty"`
	VectorDims         int                      `json:"vector_dims,omitempty"`
	FixedWidthEncoding ColumnFixedWidthEncoding `json:"fixed_width_encoding,omitempty"`
}

// TypedStorageDerivedAccelerator describes a non-authoritative sidecar tied to
// an authoritative owner and optional generation.
type TypedStorageDerivedAccelerator struct {
	Name            string                 `json:"name"`
	Class           TypedStorageAssetClass `json:"class"`
	SourceFieldPath string                 `json:"source_field_path,omitempty"`
	SourceOwner     TypedStorageFieldOwner `json:"source_owner,omitempty"`
	Generation      uint64                 `json:"generation,omitempty"`
}

// TypedStorageLayout is the pure-metadata resolved ownership view for a
// collection generation. It does not open assets, read sections, mutate DB
// state, publish roots, or acquire mmap/resource handles.
type TypedStorageLayout struct {
	Collection string `json:"collection,omitempty"`
	Enabled    bool   `json:"enabled,omitempty"`

	// Fields contains explicit authoritative owners for declared/logical fields.
	// Unknown document fields are represented by RetainedDocumentOwnsRemainder.
	Fields []TypedStorageField `json:"fields,omitempty"`

	// RetainedPayload records the compatibility retained-payload policy that
	// produced the document_payload behavior below.
	RetainedPayload ColumnRetainedPayloadPolicy `json:"retained_payload,omitempty"`
	// RetainedDocumentOwnsRemainder means document_payload remains the
	// authoritative owner for fields not listed in Fields.
	RetainedDocumentOwnsRemainder bool `json:"retained_document_owns_remainder,omitempty"`
	// RetainedDocumentCompatibilityDuplicate means document_payload may contain
	// bytes for fields whose authoritative owner is a typed asset. This models
	// ColumnRetainedPayloadFull compatibility duplication without creating
	// overlapping authoritative owners.
	RetainedDocumentCompatibilityDuplicate bool `json:"retained_document_compatibility_duplicate,omitempty"`

	DerivedAccelerators []TypedStorageDerivedAccelerator `json:"derived_accelerators,omitempty"`
}

// ResolveTypedStorageLayout maps collection metadata to a normalized
// typed-storage ownership layout. Existing ColumnStoreConfig metadata is kept as
// compatibility input: declared columns resolve to typed_row_asset ownership.
func ResolveTypedStorageLayout(meta CollectionMeta) (TypedStorageLayout, error) {
	if err := ValidateCollectionName(meta.Name); err != nil {
		return TypedStorageLayout{}, err
	}
	cfg, err := normalizeColumnStoreConfig(meta.Name, meta.Options.ColumnStore)
	if err != nil {
		return TypedStorageLayout{}, err
	}
	if cfg == nil {
		return NormalizeTypedStorageLayout(TypedStorageLayout{
			Collection:                             meta.Name,
			Enabled:                                false,
			RetainedPayload:                        ColumnRetainedPayloadFull,
			RetainedDocumentOwnsRemainder:          true,
			RetainedDocumentCompatibilityDuplicate: false,
		})
	}
	fields := make([]TypedStorageField, 0, len(cfg.Columns))
	for _, col := range cfg.Columns {
		fields = append(fields, TypedStorageField{
			Name:               col.Name,
			Path:               col.Path,
			Owner:              TypedStorageOwnerRowAsset,
			ValueType:          col.ValueType,
			Nullable:           col.Nullable,
			Dictionary:         col.Dictionary,
			VectorDims:         col.VectorDims,
			FixedWidthEncoding: col.FixedWidthEncoding,
		})
	}
	layout := TypedStorageLayout{
		Collection:          meta.Name,
		Enabled:             cfg.Enabled,
		Fields:              fields,
		RetainedPayload:     cfg.RetainedPayload,
		DerivedAccelerators: derivedAcceleratorsFromColumnStoreConfig(*cfg),
	}
	applyRetainedPayloadToTypedStorageLayout(&layout)
	return NormalizeTypedStorageLayout(layout)
}

// NormalizeTypedStorageLayout normalizes and validates an explicit
// pure-metadata typed-storage layout. It accepts typed_column_part placeholders
// so future metadata can be represented, while fail-closed read/publication
// guards below prevent accidental use as a supported data path.
func NormalizeTypedStorageLayout(in TypedStorageLayout) (TypedStorageLayout, error) {
	out := in.copy()
	if out.Collection != "" {
		if err := ValidateCollectionName(out.Collection); err != nil {
			return TypedStorageLayout{}, err
		}
	}
	if out.RetainedPayload == "" {
		if len(out.Fields) == 0 {
			out.RetainedPayload = ColumnRetainedPayloadFull
		} else {
			out.RetainedPayload = ColumnRetainedPayloadNonColumn
		}
	}
	if err := validateTypedStorageRetainedPayload(out.RetainedPayload); err != nil {
		return TypedStorageLayout{}, err
	}
	applyRetainedPayloadToTypedStorageLayout(&out)
	seenPaths := make(map[string]TypedStorageFieldOwner, len(out.Fields))
	for i := range out.Fields {
		field := &out.Fields[i]
		if field.Owner == "" {
			field.Owner = TypedStorageOwnerRowAsset
		}
		owner, err := normalizeTypedStorageFieldOwner(field.Owner)
		if err != nil {
			name := field.Name
			if name == "" {
				name = field.Path
			}
			return TypedStorageLayout{}, fmt.Errorf("collections: invalid typed-storage field %q owner: %w", name, err)
		}
		field.Owner = owner
		if field.Name != "" {
			if err := ValidateIndexName(field.Name); err != nil {
				return TypedStorageLayout{}, fmt.Errorf("collections: invalid typed-storage field %q name: %w", field.Name, err)
			}
		}
		if err := ValidateIndexPath(field.Path); err != nil {
			name := field.Name
			if name == "" {
				name = field.Path
			}
			return TypedStorageLayout{}, fmt.Errorf("collections: invalid typed-storage field %q path: %w", name, err)
		}
		if field.ValueType != "" {
			valueType, err := normalizeColumnStoreValueType(field.ValueType)
			if err != nil {
				name := field.Name
				if name == "" {
					name = field.Path
				}
				return TypedStorageLayout{}, fmt.Errorf("collections: invalid typed-storage field %q value_type: %w", name, err)
			}
			field.ValueType = valueType
			if valueType == ColumnStoreValueFloat32Vector {
				if field.VectorDims <= 0 {
					name := field.Name
					if name == "" {
						name = field.Path
					}
					return TypedStorageLayout{}, fmt.Errorf("collections: invalid typed-storage field %q vector_dims: must be positive", name)
				}
			} else if field.VectorDims != 0 {
				name := field.Name
				if name == "" {
					name = field.Path
				}
				return TypedStorageLayout{}, fmt.Errorf("collections: invalid typed-storage field %q vector_dims: only float32_vector fields may set vector_dims", name)
			}
			if field.Dictionary && valueType != ColumnStoreValueString {
				name := field.Name
				if name == "" {
					name = field.Path
				}
				return TypedStorageLayout{}, fmt.Errorf("collections: invalid typed-storage field %q dictionary: unsupported for value_type %q", name, valueType)
			}
		}
		fixedWidthEncoding, err := normalizeColumnFixedWidthEncoding(field.FixedWidthEncoding)
		if err != nil {
			name := field.Name
			if name == "" {
				name = field.Path
			}
			return TypedStorageLayout{}, fmt.Errorf("collections: invalid typed-storage field %q fixed_width_encoding: %w", name, err)
		}
		field.FixedWidthEncoding = fixedWidthEncoding
		if field.FixedWidthEncoding != ColumnFixedWidthEncodingDefault && !columnStoreValueTypeSupportsFixedWidthEncoding(field.ValueType) {
			name := field.Name
			if name == "" {
				name = field.Path
			}
			return TypedStorageLayout{}, fmt.Errorf("collections: invalid typed-storage field %q fixed_width_encoding: unsupported for value_type %q", name, field.ValueType)
		}
		if prior, ok := seenPaths[field.Path]; ok {
			return TypedStorageLayout{}, fmt.Errorf("collections: overlapping authoritative typed-storage owners for field path %q: %s and %s", field.Path, prior, field.Owner)
		}
		seenPaths[field.Path] = field.Owner
	}
	for i := range out.DerivedAccelerators {
		accel := &out.DerivedAccelerators[i]
		if accel.Class == "" {
			accel.Class = TypedStorageAssetClassDerivedAccelerator
		}
		if accel.Class != TypedStorageAssetClassDerivedAccelerator {
			return TypedStorageLayout{}, fmt.Errorf("collections: unsupported typed-storage asset class %q", accel.Class)
		}
		if accel.Name == "" {
			return TypedStorageLayout{}, errors.New("collections: typed-storage derived accelerator requires name")
		}
		if accel.SourceFieldPath != "" {
			if err := ValidateIndexPath(accel.SourceFieldPath); err != nil {
				return TypedStorageLayout{}, fmt.Errorf("collections: invalid typed-storage derived accelerator %q source path: %w", accel.Name, err)
			}
			owner, ok := seenPaths[accel.SourceFieldPath]
			if !ok {
				return TypedStorageLayout{}, fmt.Errorf("collections: typed-storage derived accelerator %q references unknown authoritative field path %q", accel.Name, accel.SourceFieldPath)
			}
			if accel.SourceOwner == "" {
				accel.SourceOwner = owner
			}
		}
		if accel.SourceOwner != "" {
			owner, err := normalizeTypedStorageFieldOwner(accel.SourceOwner)
			if err != nil {
				return TypedStorageLayout{}, fmt.Errorf("collections: invalid typed-storage derived accelerator %q source owner: %w", accel.Name, err)
			}
			accel.SourceOwner = owner
			if accel.SourceFieldPath != "" && seenPaths[accel.SourceFieldPath] != owner {
				return TypedStorageLayout{}, fmt.Errorf("collections: typed-storage derived accelerator %q source owner %q does not match authoritative owner %q for field path %q", accel.Name, owner, seenPaths[accel.SourceFieldPath], accel.SourceFieldPath)
			}
		}
	}
	out.Enabled = out.Enabled || len(out.Fields) > 0 || len(out.DerivedAccelerators) > 0 || out.RetainedPayload != ColumnRetainedPayloadFull || !out.RetainedDocumentOwnsRemainder
	return out, nil
}

func (l TypedStorageLayout) copy() TypedStorageLayout {
	out := l
	out.Fields = append([]TypedStorageField(nil), l.Fields...)
	out.DerivedAccelerators = append([]TypedStorageDerivedAccelerator(nil), l.DerivedAccelerators...)
	return out
}

func validateTypedStorageRetainedPayload(policy ColumnRetainedPayloadPolicy) error {
	switch policy {
	case ColumnRetainedPayloadNonColumn, ColumnRetainedPayloadFull, ColumnRetainedPayloadNone:
		return nil
	default:
		return fmt.Errorf("collections: unsupported retained payload policy %q", policy)
	}
}

func applyRetainedPayloadToTypedStorageLayout(layout *TypedStorageLayout) {
	if layout == nil {
		return
	}
	switch layout.RetainedPayload {
	case ColumnRetainedPayloadFull:
		layout.RetainedDocumentOwnsRemainder = true
		layout.RetainedDocumentCompatibilityDuplicate = typedStorageLayoutHasTypedAssetOwnedFields(layout)
	case ColumnRetainedPayloadNonColumn:
		layout.RetainedDocumentOwnsRemainder = true
		layout.RetainedDocumentCompatibilityDuplicate = false
	case ColumnRetainedPayloadNone:
		layout.RetainedDocumentOwnsRemainder = false
		layout.RetainedDocumentCompatibilityDuplicate = false
	}
}

func typedStorageLayoutHasTypedAssetOwnedFields(layout *TypedStorageLayout) bool {
	if layout == nil {
		return false
	}
	for _, field := range layout.Fields {
		switch field.Owner {
		case "", TypedStorageOwnerRowAsset, TypedStorageOwnerColumnPart:
			return true
		case TypedStorageOwnerRetainedDocument, "document_payload":
			continue
		}
	}
	return false
}

func normalizeTypedStorageFieldOwner(owner TypedStorageFieldOwner) (TypedStorageFieldOwner, error) {
	switch owner {
	case TypedStorageOwnerRetainedDocument, TypedStorageOwnerRowAsset, TypedStorageOwnerColumnPart:
		return owner, nil
	case "document_payload":
		return TypedStorageOwnerRetainedDocument, nil
	case TypedStorageFieldOwner(TypedStorageAssetClassDerivedAccelerator):
		return "", errors.New("derived_accelerator is not an authoritative field owner")
	case "":
		return TypedStorageOwnerRowAsset, nil
	default:
		return "", fmt.Errorf("unsupported typed-storage field owner %q", owner)
	}
}

func derivedAcceleratorsFromColumnStoreConfig(cfg ColumnStoreConfig) []TypedStorageDerivedAccelerator {
	var out []TypedStorageDerivedAccelerator
	for _, col := range cfg.Columns {
		if col.Dictionary {
			out = append(out, TypedStorageDerivedAccelerator{
				Name:            col.Name + ":dictionary",
				Class:           TypedStorageAssetClassDerivedAccelerator,
				SourceFieldPath: col.Path,
				SourceOwner:     TypedStorageOwnerRowAsset,
			})
		}
		if col.ValueType == ColumnStoreValueInt64 && !col.Nullable {
			out = append(out, TypedStorageDerivedAccelerator{
				Name:            col.Name + ":int64_values",
				Class:           TypedStorageAssetClassDerivedAccelerator,
				SourceFieldPath: col.Path,
				SourceOwner:     TypedStorageOwnerRowAsset,
			})
		}
	}
	for _, aggregate := range cfg.AggregateMetadata {
		accel := TypedStorageDerivedAccelerator{
			Name:  aggregate.Name,
			Class: TypedStorageAssetClassDerivedAccelerator,
		}
		if aggregate.Column != "" {
			if col, ok := columnStoreColumnByName(cfg.Columns, aggregate.Column); ok {
				accel.SourceFieldPath = col.Path
				accel.SourceOwner = TypedStorageOwnerRowAsset
			}
		}
		out = append(out, accel)
	}
	return out
}

func columnStoreColumnByName(columns []ColumnStoreColumn, name string) (ColumnStoreColumn, bool) {
	for _, col := range columns {
		if col.Name == name {
			return col, true
		}
	}
	return ColumnStoreColumn{}, false
}

// OwnerForPath returns the authoritative owner for a logical field path when it
// is explicitly declared or is covered by the retained document remainder.
func (l TypedStorageLayout) OwnerForPath(path string) (TypedStorageFieldOwner, bool) {
	for _, field := range l.Fields {
		if field.Path == path {
			return field.Owner, true
		}
	}
	if l.RetainedDocumentOwnsRemainder {
		return TypedStorageOwnerRetainedDocument, true
	}
	return "", false
}

// HasTypedColumnPartOwners reports whether the layout contains future
// typed-column authoritative owners.
func (l TypedStorageLayout) HasTypedColumnPartOwners() bool {
	for _, field := range l.Fields {
		if field.Owner == TypedStorageOwnerColumnPart {
			return true
		}
	}
	return false
}

// EnsureReadSupported fails closed for layouts that mention typed_column_part
// ownership before the production typed-column read path exists.
func (l TypedStorageLayout) EnsureReadSupported() error {
	if l.HasTypedColumnPartOwners() {
		return ErrTypedStorageColumnPartUnsupported
	}
	return nil
}

// EnsurePublicationSupported fails closed for layouts that mention
// typed_column_part ownership before the production typed-column publication
// path exists.
func (l TypedStorageLayout) EnsurePublicationSupported() error {
	if l.HasTypedColumnPartOwners() {
		return ErrTypedStorageColumnPartUnsupported
	}
	return nil
}

// FieldOwnerDebugRows returns deterministic status/debug rows for tests and PR
// evidence without touching DB files or opening assets.
func (l TypedStorageLayout) FieldOwnerDebugRows() []string {
	rows := make([]string, 0, len(l.Fields)+1)
	if l.RetainedDocumentOwnsRemainder {
		rows = append(rows, "* -> retained_document(remainder)")
	}
	for _, field := range l.Fields {
		rows = append(rows, fmt.Sprintf("%s -> %s", field.Path, field.Owner))
	}
	if l.RetainedDocumentCompatibilityDuplicate {
		rows = append(rows, "document_payload -> compatibility_duplicate")
	}
	sort.Strings(rows)
	return rows
}
