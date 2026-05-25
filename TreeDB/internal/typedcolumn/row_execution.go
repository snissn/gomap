package typedcolumn

import "fmt"

type rowMaskRole uint8

const (
	rowMaskPredicate rowMaskRole = iota
	rowMaskVisibility
	rowMaskDelete
	rowMaskNull
	rowMaskDefault
)

type rowSelectionComponents struct {
	Predicate  *rowSelection
	Visibility *rowSelection
	Deletes    *rowSelection
	Nulls      *rowSelection
	Defaults   *rowSelection
}

type rowSpan struct {
	FirstRow int
	RowCount int
}

type columnRowDescriptor struct {
	Column string
	Type   ColumnType
	Span   rowSpan
}

type sectionDependencyRole uint8

const (
	sectionDependencyPredicate sectionDependencyRole = iota
	sectionDependencyMeasure
	sectionDependencyProjection
	sectionDependencyVisibility
	sectionDependencyNull
	sectionDependencyDefault
)

type sectionDependencyDescriptor struct {
	Role        sectionDependencyRole
	Column      string
	Type        ColumnType
	SectionKind ColumnPartImageSectionKind
	Span        rowSpan
	Required    bool
}

func composeRowSelections(rows int, components rowSelectionComponents) (rowSelection, error) {
	current, err := makeAllRowSelection(rows)
	if err != nil {
		return current, err
	}
	if components.Predicate != nil {
		current, err = includeSelection(current, *components.Predicate, rowMaskPredicate)
		if err != nil {
			return failClosedSelection(rows, components.Predicate.rows), err
		}
	}
	if components.Visibility != nil {
		current, err = includeSelection(current, *components.Visibility, rowMaskVisibility)
		if err != nil {
			return failClosedSelection(rows, components.Visibility.rows), err
		}
	}
	if components.Deletes != nil {
		current, err = excludeSelection(current, *components.Deletes, rowMaskDelete)
		if err != nil {
			return failClosedSelection(rows, components.Deletes.rows), err
		}
	}
	if components.Nulls != nil {
		current, err = excludeSelection(current, *components.Nulls, rowMaskNull)
		if err != nil {
			return failClosedSelection(rows, components.Nulls.rows), err
		}
	}
	if components.Defaults != nil {
		current, err = excludeSelection(current, *components.Defaults, rowMaskDefault)
		if err != nil {
			return failClosedSelection(rows, components.Defaults.rows), err
		}
	}
	return current, nil
}

func includeSelection(current rowSelection, mask rowSelection, role rowMaskRole) (rowSelection, error) {
	if err := validateSameSelectionRows(current, mask); err != nil {
		return failClosedSelection(current.rows, mask.rows), fmt.Errorf("typedcolumn: %s mask row mismatch: %w", role, err)
	}
	return andRowSelections(current, mask)
}

func excludeSelection(current rowSelection, mask rowSelection, role rowMaskRole) (rowSelection, error) {
	if err := validateSameSelectionRows(current, mask); err != nil {
		return failClosedSelection(current.rows, mask.rows), fmt.Errorf("typedcolumn: %s mask row mismatch: %w", role, err)
	}
	inverted, err := notRowSelection(mask)
	if err != nil {
		return failClosedSelection(current.rows, mask.rows), fmt.Errorf("typedcolumn: invert %s mask: %w", role, err)
	}
	return andRowSelections(current, inverted)
}

func makeRowSpan(firstRow int, rowCount int) (rowSpan, error) {
	span := rowSpan{FirstRow: firstRow, RowCount: rowCount}
	if err := validateRowSpan(span); err != nil {
		return rowSpan{}, err
	}
	return span, nil
}

func (s rowSpan) endRow() int {
	return s.FirstRow + s.RowCount
}

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

func alignColumnRowSpans(columns []columnRowDescriptor) (rowSpan, bool, error) {
	if len(columns) == 0 {
		return rowSpan{}, false, fmt.Errorf("typedcolumn: no column row spans to align")
	}
	if err := validateColumnRowDescriptor(columns[0]); err != nil {
		return rowSpan{}, false, err
	}
	want := columns[0].Span
	for i := 1; i < len(columns); i++ {
		if err := validateColumnRowDescriptor(columns[i]); err != nil {
			return rowSpan{}, false, err
		}
		if columns[i].Span != want {
			return rowSpan{}, false, fmt.Errorf("typedcolumn: column %s span [%d,%d) mismatches [%d,%d)", columns[i].Column, columns[i].Span.FirstRow, columns[i].Span.endRow(), want.FirstRow, want.endRow())
		}
	}
	return want, true, nil
}

func validateColumnRowDescriptor(desc columnRowDescriptor) error {
	if desc.Column == "" {
		return fmt.Errorf("typedcolumn: empty column row descriptor column")
	}
	if err := validateRowSpan(desc.Span); err != nil {
		return err
	}
	return nil
}

func makeSectionDependency(role sectionDependencyRole, column string, columnType ColumnType, sectionKind ColumnPartImageSectionKind, span rowSpan, required bool) (sectionDependencyDescriptor, error) {
	dep := sectionDependencyDescriptor{Role: role, Column: column, Type: columnType, SectionKind: sectionKind, Span: span, Required: required}
	if err := validateSectionDependency(dep); err != nil {
		return sectionDependencyDescriptor{}, err
	}
	return dep, nil
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
	if dep.SectionKind == ColumnPartImageSectionManifest {
		return fmt.Errorf("typedcolumn: %s dependency cannot target manifest section", dep.Role)
	}
	if err := validateRowSpan(dep.Span); err != nil {
		return err
	}
	return nil
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

func (r rowMaskRole) String() string {
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

func (r sectionDependencyRole) valid() bool {
	return r <= sectionDependencyDefault
}

func (r sectionDependencyRole) String() string {
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
