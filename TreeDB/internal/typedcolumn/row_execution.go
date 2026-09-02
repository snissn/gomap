package typedcolumn

import "fmt"

// RowMaskRole identifies how a mask participates in selection composition.
type RowMaskRole uint8

type rowMaskRole = RowMaskRole

const (
	rowMaskPredicate RowMaskRole = iota
	rowMaskVisibility
	rowMaskDelete
	rowMaskNull
	rowMaskDefault
)

const (
	RowMaskPredicate  RowMaskRole = rowMaskPredicate
	RowMaskVisibility RowMaskRole = rowMaskVisibility
	RowMaskDelete     RowMaskRole = rowMaskDelete
	RowMaskNull       RowMaskRole = rowMaskNull
	RowMaskDefault    RowMaskRole = rowMaskDefault
)

// RowSelectionComponents centralizes block-local composition. Predicate and
// visibility selections are included; delete/null/default selections name rows
// that must be excluded from value semantics.
type RowSelectionComponents struct {
	Predicate  *RowSelection
	Visibility *RowSelection
	Deletes    *RowSelection
	Nulls      *RowSelection
	Defaults   *RowSelection
}

type rowSelectionComponents = RowSelectionComponents

// ComposeRowSelections composes predicate, visibility/delete, and null/default
// masks over a block-local row domain. Mismatched domains return an empty
// fail-closed selection plus an error.
func ComposeRowSelections(rows int, components RowSelectionComponents) (RowSelection, error) {
	return ComposeRowSelectionsInto(rows, components, nil)
}

// ComposeRowSelectionsInto is the scratch-backed form of ComposeRowSelections.
// Returned selections may alias scratch until scratch is reused.
func ComposeRowSelectionsInto(rows int, components RowSelectionComponents, scratch *RowSelectionScratch) (RowSelection, error) {
	current, err := makeAllRowSelection(rows)
	if err != nil {
		return current, err
	}
	if components.Predicate != nil {
		current, err = includeSelectionInto(current, *components.Predicate, rowMaskPredicate, scratch)
		if err != nil {
			return failClosedSelection(rows, components.Predicate.rows), err
		}
	}
	if components.Visibility != nil {
		current, err = includeSelectionInto(current, *components.Visibility, rowMaskVisibility, scratch)
		if err != nil {
			return failClosedSelection(rows, components.Visibility.rows), err
		}
	}
	if components.Deletes != nil {
		current, err = excludeSelectionInto(current, *components.Deletes, rowMaskDelete, scratch)
		if err != nil {
			return failClosedSelection(rows, components.Deletes.rows), err
		}
	}
	if components.Nulls != nil {
		current, err = excludeSelectionInto(current, *components.Nulls, rowMaskNull, scratch)
		if err != nil {
			return failClosedSelection(rows, components.Nulls.rows), err
		}
	}
	if components.Defaults != nil {
		current, err = excludeSelectionInto(current, *components.Defaults, rowMaskDefault, scratch)
		if err != nil {
			return failClosedSelection(rows, components.Defaults.rows), err
		}
	}
	return current, nil
}

func composeRowSelections(rows int, components rowSelectionComponents) (rowSelection, error) {
	return ComposeRowSelections(rows, components)
}

func includeSelection(current rowSelection, mask rowSelection, role rowMaskRole) (rowSelection, error) {
	return includeSelectionInto(current, mask, role, nil)
}

func includeSelectionInto(current RowSelection, mask RowSelection, role RowMaskRole, scratch *RowSelectionScratch) (RowSelection, error) {
	if err := validateSameSelectionRows(current, mask); err != nil {
		return failClosedSelection(current.rows, mask.rows), fmt.Errorf("typedcolumn: %s mask row mismatch: %w", role, err)
	}
	return AndRowSelectionsInto(current, mask, scratch)
}

func excludeSelection(current rowSelection, mask rowSelection, role rowMaskRole) (rowSelection, error) {
	return excludeSelectionInto(current, mask, role, nil)
}

func excludeSelectionInto(current RowSelection, mask RowSelection, role RowMaskRole, scratch *RowSelectionScratch) (RowSelection, error) {
	if err := validateSameSelectionRows(current, mask); err != nil {
		return failClosedSelection(current.rows, mask.rows), fmt.Errorf("typedcolumn: %s mask row mismatch: %w", role, err)
	}
	out, err := SubtractRowSelectionsInto(current, mask, scratch)
	if err != nil {
		return failClosedSelection(current.rows, mask.rows), fmt.Errorf("typedcolumn: exclude %s mask: %w", role, err)
	}
	return out, nil
}

// RowSpan is a half-open row span over a column section or block.
type RowSpan struct {
	FirstRow int
	RowCount int
}

type rowSpan = RowSpan

// ColumnExecutionRole classifies how a column participates in a future
// multi-column execution plan. It is descriptive; it is not a planner.
type ColumnExecutionRole uint8

type sectionDependencyRole = ColumnExecutionRole

const (
	sectionDependencyPredicate ColumnExecutionRole = iota
	sectionDependencyMeasure
	sectionDependencyProjection
	sectionDependencyVisibility
	sectionDependencyNull
	sectionDependencyDefault
)

const (
	ColumnRolePredicate  ColumnExecutionRole = sectionDependencyPredicate
	ColumnRoleMeasure    ColumnExecutionRole = sectionDependencyMeasure
	ColumnRoleProjection ColumnExecutionRole = sectionDependencyProjection
	ColumnRoleVisibility ColumnExecutionRole = sectionDependencyVisibility
	ColumnRoleNull       ColumnExecutionRole = sectionDependencyNull
	ColumnRoleDefault    ColumnExecutionRole = sectionDependencyDefault
)

// SectionDependencyKind names the logical section/payload class needed by an
// operation. It intentionally covers future codecs without requiring a planner.
type SectionDependencyKind string

const (
	SectionDependencyValues           SectionDependencyKind = "values"
	SectionDependencyDictionaries     SectionDependencyKind = "dictionaries"
	SectionDependencyOffsets          SectionDependencyKind = "offsets"
	SectionDependencyNullMask         SectionDependencyKind = "null_mask"
	SectionDependencyDefaultMask      SectionDependencyKind = "default_mask"
	SectionDependencyVisibility       SectionDependencyKind = "visibility"
	SectionDependencyStats            SectionDependencyKind = "stats"
	SectionDependencyPruningMetadata  SectionDependencyKind = "pruning_metadata"
	SectionDependencyVectorPayload    SectionDependencyKind = "vector_payload"
	SectionDependencyAdjacencyPayload SectionDependencyKind = "adjacency_payload"
)

// ColumnRowDescriptor binds one column role to a row span and optional immutable
// asset identity. Non-zero identity fields must align across descriptors.
type ColumnRowDescriptor struct {
	Column             string
	Type               ColumnType
	Span               RowSpan
	SnapshotGeneration uint64
	AssetGeneration    uint64
	PartID             uint64
	SchemaVersion      uint32
	AlignmentKey       string
}

type columnRowDescriptor = ColumnRowDescriptor

// SectionDependencyDescriptor describes a section read dependency for a column
// role and row span. SectionKind identifies the tcs1 section class, while Kind
// identifies the logical payload/metadata need.
type SectionDependencyDescriptor struct {
	Role        ColumnExecutionRole
	Column      string
	Type        ColumnType
	Kind        SectionDependencyKind
	SectionKind ColumnPartImageSectionKind
	Span        RowSpan
	Required    bool
}

type sectionDependencyDescriptor = SectionDependencyDescriptor

func makeRowSpan(firstRow int, rowCount int) (rowSpan, error) {
	return NewRowSpan(firstRow, rowCount)
}

func NewRowSpan(firstRow int, rowCount int) (RowSpan, error) {
	span := RowSpan{FirstRow: firstRow, RowCount: rowCount}
	if err := validateRowSpan(span); err != nil {
		return RowSpan{}, err
	}
	return span, nil
}

func (s rowSpan) endRow() int {
	return s.FirstRow + s.RowCount
}

// EndRow returns the exclusive row end.
func (s RowSpan) EndRow() int { return s.FirstRow + s.RowCount }

func validateRowSpan(span rowSpan) error {
	if span.FirstRow < 0 {
		return fmt.Errorf("typedcolumn: negative row span first row %d", span.FirstRow)
	}
	if span.RowCount < 0 {
		return fmt.Errorf("typedcolumn: negative row span count %d", span.RowCount)
	}
	if span.FirstRow > int(^uint(0)>>1)-span.RowCount {
		return fmt.Errorf("typedcolumn: row span [%d,+%d) overflows int", span.FirstRow, span.RowCount)
	}
	return nil
}

// AlignColumnRowSpans validates that all columns share the same row span and
// optional snapshot/asset/schema identity. It fails closed on any mismatch.
func AlignColumnRowSpans(columns []ColumnRowDescriptor) (RowSpan, bool, error) {
	return alignColumnRowSpans(columns)
}

func alignColumnRowSpans(columns []columnRowDescriptor) (rowSpan, bool, error) {
	if len(columns) == 0 {
		return rowSpan{}, false, fmt.Errorf("typedcolumn: no column row spans to align")
	}
	if err := validateColumnRowDescriptor(columns[0]); err != nil {
		return rowSpan{}, false, err
	}
	want := columns[0]
	for i := 1; i < len(columns); i++ {
		if err := validateColumnRowDescriptor(columns[i]); err != nil {
			return rowSpan{}, false, err
		}
		if columns[i].Span != want.Span {
			return rowSpan{}, false, fmt.Errorf("typedcolumn: column %s span [%d,%d) mismatches [%d,%d)", columns[i].Column, columns[i].Span.FirstRow, columns[i].Span.endRow(), want.Span.FirstRow, want.Span.endRow())
		}
		if mismatchIdentity(want.SnapshotGeneration, columns[i].SnapshotGeneration) {
			return rowSpan{}, false, fmt.Errorf("typedcolumn: column %s snapshot generation %d mismatches %d", columns[i].Column, columns[i].SnapshotGeneration, want.SnapshotGeneration)
		}
		if mismatchIdentity(want.AssetGeneration, columns[i].AssetGeneration) {
			return rowSpan{}, false, fmt.Errorf("typedcolumn: column %s asset generation %d mismatches %d", columns[i].Column, columns[i].AssetGeneration, want.AssetGeneration)
		}
		if mismatchIdentity(want.PartID, columns[i].PartID) {
			return rowSpan{}, false, fmt.Errorf("typedcolumn: column %s part id %d mismatches %d", columns[i].Column, columns[i].PartID, want.PartID)
		}
		if mismatchIdentity32(want.SchemaVersion, columns[i].SchemaVersion) {
			return rowSpan{}, false, fmt.Errorf("typedcolumn: column %s schema version %d mismatches %d", columns[i].Column, columns[i].SchemaVersion, want.SchemaVersion)
		}
		if want.AlignmentKey != "" && columns[i].AlignmentKey != "" && columns[i].AlignmentKey != want.AlignmentKey {
			return rowSpan{}, false, fmt.Errorf("typedcolumn: column %s asset ref %q mismatches %q", columns[i].Column, columns[i].AlignmentKey, want.AlignmentKey)
		}
	}
	return want.Span, true, nil
}

func mismatchIdentity(a, b uint64) bool   { return a != 0 && b != 0 && a != b }
func mismatchIdentity32(a, b uint32) bool { return a != 0 && b != 0 && a != b }

func validateColumnRowDescriptor(desc columnRowDescriptor) error {
	if desc.Column == "" {
		return fmt.Errorf("typedcolumn: empty column row descriptor column")
	}
	if err := validateRowSpan(desc.Span); err != nil {
		return err
	}
	return nil
}

func makeValuesSectionDependency(role sectionDependencyRole, column string, columnType ColumnType, sectionKind ColumnPartImageSectionKind, span rowSpan, required bool) (sectionDependencyDescriptor, error) {
	return NewSectionDependency(role, column, columnType, SectionDependencyValues, sectionKind, span, required)
}

func NewSectionDependency(role ColumnExecutionRole, column string, columnType ColumnType, kind SectionDependencyKind, sectionKind ColumnPartImageSectionKind, span RowSpan, required bool) (SectionDependencyDescriptor, error) {
	dep := SectionDependencyDescriptor{Role: role, Column: column, Type: columnType, Kind: kind, SectionKind: sectionKind, Span: span, Required: required}
	if err := validateSectionDependency(dep); err != nil {
		return SectionDependencyDescriptor{}, err
	}
	return dep, nil
}

// ValidateSectionDependencies validates dependency descriptors and their row
// alignment. It does not read any payloads.
func ValidateSectionDependencies(deps []SectionDependencyDescriptor) (RowSpan, bool, error) {
	return validateSectionDependencies(deps)
}

func validateSectionDependencies(deps []sectionDependencyDescriptor) (rowSpan, bool, error) {
	if len(deps) == 0 {
		return rowSpan{}, false, fmt.Errorf("typedcolumn: no section dependencies")
	}
	columns := make([]columnRowDescriptor, 0, len(deps))
	for _, dep := range deps {
		if err := validateSectionDependency(dep); err != nil {
			return rowSpan{}, false, err
		}
		columns = append(columns, columnRowDescriptor{Column: dep.Column, Type: dep.Type, Span: dep.Span})
	}
	return alignColumnRowSpans(columns)
}

func validateSectionDependency(dep sectionDependencyDescriptor) error {
	if dep.Column == "" {
		return fmt.Errorf("typedcolumn: empty %s dependency column", dep.Role)
	}
	if !dep.Role.valid() {
		return fmt.Errorf("typedcolumn: invalid section dependency role %d", dep.Role)
	}
	if dep.Kind == "" {
		return fmt.Errorf("typedcolumn: empty %s dependency kind", dep.Role)
	}
	if dep.SectionKind == ColumnPartImageSectionManifest {
		return fmt.Errorf("typedcolumn: %s dependency cannot target manifest section", dep.Role)
	}
	if err := validateRowSpan(dep.Span); err != nil {
		return err
	}
	return nil
}

// PredicateColumnDependencies describes the ordinary sections needed to produce
// a predicate selection for one column. Callers may add dictionary/vector/etc.
// dependencies when the operation requires them.
func PredicateColumnDependencies(column string, columnType ColumnType, span RowSpan, nullable bool) ([]SectionDependencyDescriptor, error) {
	return standardColumnDependencies(ColumnRolePredicate, column, columnType, span, nullable, true)
}

func MeasureColumnDependencies(column string, columnType ColumnType, span RowSpan, nullable bool) ([]SectionDependencyDescriptor, error) {
	return standardColumnDependencies(ColumnRoleMeasure, column, columnType, span, nullable, false)
}

func ProjectionColumnDependencies(column string, columnType ColumnType, span RowSpan, nullable bool) ([]SectionDependencyDescriptor, error) {
	return standardColumnDependencies(ColumnRoleProjection, column, columnType, span, nullable, false)
}

func standardColumnDependencies(role ColumnExecutionRole, column string, columnType ColumnType, span RowSpan, nullable bool, pruning bool) ([]SectionDependencyDescriptor, error) {
	kinds := []SectionDependencyKind{SectionDependencyValues}
	if pruning {
		kinds = append(kinds, SectionDependencyPruningMetadata)
	}
	if nullable {
		kinds = append(kinds, SectionDependencyNullMask, SectionDependencyDefaultMask)
	}
	out := make([]SectionDependencyDescriptor, 0, len(kinds))
	for _, kind := range kinds {
		sectionKind := ColumnPartImageSectionColumnData
		if kind == SectionDependencyPruningMetadata {
			sectionKind = ColumnPartImageSectionPruningMetadata
		}
		dep, err := NewSectionDependency(role, column, columnType, kind, sectionKind, span, true)
		if err != nil {
			return nil, err
		}
		out = append(out, dep)
	}
	return out, nil
}

func appendSelectedBools(dst []bool, values []bool, selection rowSelection) ([]bool, error) {
	if len(values) != selection.rows {
		return dst[:0], fmt.Errorf("typedcolumn: bool values rows=%d selection rows=%d", len(values), selection.rows)
	}
	out := dst[:0]
	switch selection.kind {
	case rowSelectionEmpty:
		return out, nil
	case rowSelectionAll:
		return append(out, values...), nil
	case rowSelectionRange:
		return append(out, values[selection.start:selection.end]...), nil
	case rowSelectionRanges:
		for _, r := range selection.ranges {
			out = append(out, values[r.Start:r.End]...)
		}
	case rowSelectionBitmap, rowSelectionSparse:
		selection.forEach(func(row int) {
			out = append(out, values[row])
		})
	}
	return out, nil
}

func appendSelectedUint32s(dst []uint32, values []uint32, selection rowSelection) ([]uint32, error) {
	if len(values) != selection.rows {
		return dst[:0], fmt.Errorf("typedcolumn: uint32 values rows=%d selection rows=%d", len(values), selection.rows)
	}
	out := dst[:0]
	switch selection.kind {
	case rowSelectionEmpty:
		return out, nil
	case rowSelectionAll:
		return append(out, values...), nil
	case rowSelectionRange:
		return append(out, values[selection.start:selection.end]...), nil
	case rowSelectionRanges:
		for _, r := range selection.ranges {
			out = append(out, values[r.Start:r.End]...)
		}
	case rowSelectionBitmap, rowSelectionSparse:
		selection.forEach(func(row int) {
			out = append(out, values[row])
		})
	}
	return out, nil
}

func (r RowMaskRole) String() string {
	switch r {
	case rowMaskPredicate:
		return "predicate"
	case rowMaskVisibility:
		return "visibility"
	case rowMaskDelete:
		return "delete"
	case rowMaskNull:
		return "null"
	case rowMaskDefault:
		return "default"
	default:
		return "unknown"
	}
}

func (r ColumnExecutionRole) valid() bool {
	return r <= sectionDependencyDefault
}

func (r ColumnExecutionRole) String() string {
	switch r {
	case sectionDependencyPredicate:
		return "predicate"
	case sectionDependencyMeasure:
		return "measure"
	case sectionDependencyProjection:
		return "projection"
	case sectionDependencyVisibility:
		return "visibility"
	case sectionDependencyNull:
		return "null"
	case sectionDependencyDefault:
		return "default"
	default:
		return "unknown"
	}
}
