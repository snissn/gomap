package typedcolumn

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
)

const (
	queryReadyExecutionImageMagic          = uint32(0x53585251) // "QRXS", little-endian.
	queryReadyExecutionImageVersion        = uint16(1)
	queryReadyExecutionImageHeaderBytes    = 48
	queryReadyExecutionImageColumnBytes    = 64
	queryReadyExecutionImagePayloadAlign   = 8
	queryReadyExecutionImageColumnCode     = uint8(1)
	queryReadyExecutionImageColumnInt64    = uint8(2)
	queryReadyExecutionImageNullable       = uint8(1)
	queryReadyExecutionImageMaxColumnCount = 1 << 16
)

// QueryReadyExecutionColumnKind identifies a query-independent direct vector.
// It is physical state, not an operator or a precomputed result.
type QueryReadyExecutionColumnKind uint8

const (
	QueryReadyExecutionColumnCode  QueryReadyExecutionColumnKind = QueryReadyExecutionColumnKind(queryReadyExecutionImageColumnCode)
	QueryReadyExecutionColumnInt64 QueryReadyExecutionColumnKind = QueryReadyExecutionColumnKind(queryReadyExecutionImageColumnInt64)
)

// QueryReadyExecutionColumnView is a validated read-only vector backed by the
// generation mapping. Code vectors use the minimum fixed width that covers the
// part-local dictionary domain. Absence combines nullable null/default state.
type QueryReadyExecutionColumnView struct {
	name        string
	kind        QueryReadyExecutionColumnKind
	codeWidth   int
	cardinality uint32
	rows        int
	values      []byte
	absent      []byte
}

func (v QueryReadyExecutionColumnView) Name() string                        { return v.name }
func (v QueryReadyExecutionColumnView) Kind() QueryReadyExecutionColumnKind { return v.kind }
func (v QueryReadyExecutionColumnView) CodeWidth() int                      { return v.codeWidth }
func (v QueryReadyExecutionColumnView) Cardinality() uint32                 { return v.cardinality }
func (v QueryReadyExecutionColumnView) Rows() int                           { return v.rows }

func (v QueryReadyExecutionColumnView) CodeAt(row int) (uint32, bool, error) {
	if v.kind != QueryReadyExecutionColumnCode {
		return 0, false, fmt.Errorf("typedcolumn: query-ready execution column %s is not a code vector", v.name)
	}
	if row < 0 || row >= v.rows {
		return 0, false, fmt.Errorf("typedcolumn: query-ready execution column %s row=%d outside [0,%d)", v.name, row, v.rows)
	}
	return v.codeAtUnchecked(row), v.absentAtUnchecked(row), nil
}

func (v QueryReadyExecutionColumnView) Int64At(row int) (int64, bool, error) {
	if v.kind != QueryReadyExecutionColumnInt64 {
		return 0, false, fmt.Errorf("typedcolumn: query-ready execution column %s is not an int64 vector", v.name)
	}
	if row < 0 || row >= v.rows {
		return 0, false, fmt.Errorf("typedcolumn: query-ready execution column %s row=%d outside [0,%d)", v.name, row, v.rows)
	}
	return int64(binary.LittleEndian.Uint64(v.values[row*8:])), v.absentAtUnchecked(row), nil
}

func (v QueryReadyExecutionColumnView) codeAtUnchecked(row int) uint32 {
	switch v.codeWidth {
	case 1:
		return uint32(v.values[row])
	case 2:
		return uint32(binary.LittleEndian.Uint16(v.values[row*2:]))
	default:
		return binary.LittleEndian.Uint32(v.values[row*4:])
	}
}

func (v QueryReadyExecutionColumnView) int64AtUnchecked(row int) int64 {
	return int64(binary.LittleEndian.Uint64(v.values[row*8:]))
}

func (v QueryReadyExecutionColumnView) absentAtUnchecked(row int) bool {
	return len(v.absent) != 0 && v.absent[row>>3]&(uint8(1)<<uint(row&7)) != 0
}

// QueryReadyExecutionPartView holds query-independent vectors for one source
// part. All slices point into the immutable generation bytes.
type QueryReadyExecutionPartView struct {
	rows    int
	columns map[string]QueryReadyExecutionColumnView
	bytes   []byte
}

func (v QueryReadyExecutionPartView) Rows() int { return v.rows }

func (v QueryReadyExecutionPartView) Column(name string) (QueryReadyExecutionColumnView, bool) {
	column, ok := v.columns[name]
	return column, ok
}

func (v QueryReadyExecutionPartView) Bytes() int64 { return int64(len(v.bytes)) }

type queryReadyExecutionImageBuildColumn struct {
	name        string
	kind        QueryReadyExecutionColumnKind
	codeWidth   int
	cardinality uint32
	values      []byte
	absent      []byte
}

// EstimateQueryReadyExecutionImageUpperBound returns an allocation-free upper
// bound for the query-independent sidecar derived from one immutable part
// image. Every fixed-width column-data section is conservatively charged as an
// eight-byte vector plus an absence bitmap; primary keys and narrower code
// vectors therefore only reduce the actual encoded size.
func EstimateQueryReadyExecutionImageUpperBound(image ColumnPartImage) (int64, error) {
	if image.Rows < 0 {
		return 0, errors.New("typedcolumn: negative rows in query-ready execution estimate")
	}
	var columns, nameBytes int64
	for _, section := range image.Sections {
		if section.Kind != ColumnPartImageSectionColumnData {
			continue
		}
		if len(section.Column) > math.MaxInt64 || nameBytes > math.MaxInt64-int64(len(section.Column)) {
			return 0, errors.New("typedcolumn: query-ready execution estimate name size overflow")
		}
		columns++
		nameBytes += int64(len(section.Column))
	}
	if columns > queryReadyExecutionImageMaxColumnCount {
		return 0, fmt.Errorf("typedcolumn: query-ready execution estimate columns=%d exceed format bound", columns)
	}
	add := func(total, value int64) (int64, error) {
		if value < 0 || total > math.MaxInt64-value {
			return 0, errors.New("typedcolumn: query-ready execution estimate size overflow")
		}
		return total + value, nil
	}
	align := func(value int64) (int64, error) {
		const mask = int64(queryReadyExecutionImagePayloadAlign - 1)
		if value < 0 || value > math.MaxInt64-mask {
			return 0, errors.New("typedcolumn: query-ready execution estimate alignment overflow")
		}
		return (value + mask) &^ mask, nil
	}
	total, err := add(queryReadyExecutionImageHeaderBytes, columns*queryReadyExecutionImageColumnBytes)
	if err != nil {
		return 0, err
	}
	total, err = add(total, nameBytes)
	if err != nil {
		return 0, err
	}
	total, err = align(total)
	if err != nil {
		return 0, err
	}
	rows := int64(image.Rows)
	if rows > math.MaxInt64/8 || rows > math.MaxInt64-7 {
		return 0, errors.New("typedcolumn: query-ready execution estimate row size overflow")
	}
	valueBytes := rows * 8
	bitmapBytes := (rows + 7) / 8
	for index := int64(0); index < columns; index++ {
		total, err = align(total)
		if err != nil {
			return 0, err
		}
		total, err = add(total, valueBytes)
		if err != nil {
			return 0, err
		}
		total, err = align(total)
		if err != nil {
			return 0, err
		}
		total, err = add(total, bitmapBytes)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

func buildQueryReadyExecutionImage(part *ColumnPart) ([]byte, int, error) {
	if part == nil || part.Descriptor.RowCount < 0 {
		return nil, 0, errors.New("typedcolumn: invalid part for query-ready execution image")
	}
	primary := make(map[string]struct{}, len(part.Options.LogicalPrimaryKey.Columns))
	for _, name := range part.Options.LogicalPrimaryKey.Columns {
		primary[name] = struct{}{}
	}
	definitions := append([]ColumnDefinition(nil), part.Options.Columns...)
	sort.Slice(definitions, func(i, j int) bool { return definitions[i].Name < definitions[j].Name })
	columns := make([]queryReadyExecutionImageBuildColumn, 0, len(definitions))
	scanner := part.NewScanner()
	var valuesScratch []int64
	var nullsScratch, defaultsScratch []bool
	for _, definition := range definitions {
		if _, isPrimary := primary[definition.Name]; isPrimary {
			continue
		}
		if definition.Type != ColumnTypeLowCardinalityCode && definition.Type != ColumnTypeInt64 {
			continue
		}
		column, values, nulls, defaults, err := buildQueryReadyExecutionImageColumn(scanner, definition, valuesScratch, nullsScratch, defaultsScratch)
		if err != nil {
			return nil, 0, fmt.Errorf("typedcolumn: build query-ready execution column %s: %w", definition.Name, err)
		}
		valuesScratch, nullsScratch, defaultsScratch = values[:0], nulls[:0], defaults[:0]
		columns = append(columns, column)
	}
	if len(columns) > queryReadyExecutionImageMaxColumnCount {
		return nil, 0, fmt.Errorf("typedcolumn: query-ready execution columns=%d exceed format bound", len(columns))
	}
	descriptorBytes := len(columns) * queryReadyExecutionImageColumnBytes
	namesEnd := queryReadyExecutionImageHeaderBytes + descriptorBytes
	for _, column := range columns {
		if len(column.name) == 0 || len(column.name) > math.MaxUint32 || namesEnd > math.MaxInt-len(column.name) {
			return nil, 0, fmt.Errorf("typedcolumn: query-ready execution column name %q exceeds format bound", column.name)
		}
		namesEnd += len(column.name)
	}
	payloadOffset, err := queryReadyBaseAlign(namesEnd, queryReadyExecutionImagePayloadAlign)
	if err != nil {
		return nil, 0, err
	}
	total := payloadOffset
	for i := range columns {
		total, err = queryReadyBaseAlign(total, queryReadyExecutionImagePayloadAlign)
		if err != nil || len(columns[i].values) > math.MaxInt-total {
			return nil, 0, errors.New("typedcolumn: query-ready execution values exceed host size")
		}
		total += len(columns[i].values)
		if len(columns[i].absent) != 0 {
			total, err = queryReadyBaseAlign(total, queryReadyExecutionImagePayloadAlign)
			if err != nil || len(columns[i].absent) > math.MaxInt-total {
				return nil, 0, errors.New("typedcolumn: query-ready execution absence bitmap exceeds host size")
			}
			total += len(columns[i].absent)
		}
	}
	out := make([]byte, total)
	binary.LittleEndian.PutUint32(out[0:4], queryReadyExecutionImageMagic)
	binary.LittleEndian.PutUint16(out[4:6], queryReadyExecutionImageVersion)
	binary.LittleEndian.PutUint64(out[8:16], uint64(part.Descriptor.RowCount))
	binary.LittleEndian.PutUint32(out[16:20], uint32(len(columns)))
	binary.LittleEndian.PutUint64(out[24:32], queryReadyExecutionImageHeaderBytes)
	binary.LittleEndian.PutUint64(out[32:40], uint64(payloadOffset))
	binary.LittleEndian.PutUint64(out[40:48], uint64(total))
	nameOffset, dataOffset := queryReadyExecutionImageHeaderBytes+descriptorBytes, payloadOffset
	for i, column := range columns {
		descriptor := out[queryReadyExecutionImageHeaderBytes+i*queryReadyExecutionImageColumnBytes:]
		binary.LittleEndian.PutUint32(descriptor[0:4], uint32(nameOffset))
		binary.LittleEndian.PutUint32(descriptor[4:8], uint32(len(column.name)))
		descriptor[8] = byte(column.kind)
		descriptor[9] = byte(column.codeWidth)
		if len(column.absent) != 0 {
			descriptor[10] = queryReadyExecutionImageNullable
		}
		binary.LittleEndian.PutUint32(descriptor[12:16], column.cardinality)
		copy(out[nameOffset:], column.name)
		nameOffset += len(column.name)
		dataOffset, _ = queryReadyBaseAlign(dataOffset, queryReadyExecutionImagePayloadAlign)
		binary.LittleEndian.PutUint64(descriptor[16:24], uint64(dataOffset))
		binary.LittleEndian.PutUint64(descriptor[24:32], uint64(len(column.values)))
		copy(out[dataOffset:], column.values)
		dataOffset += len(column.values)
		if len(column.absent) != 0 {
			dataOffset, _ = queryReadyBaseAlign(dataOffset, queryReadyExecutionImagePayloadAlign)
			binary.LittleEndian.PutUint64(descriptor[32:40], uint64(dataOffset))
			binary.LittleEndian.PutUint64(descriptor[40:48], uint64(len(column.absent)))
			copy(out[dataOffset:], column.absent)
			dataOffset += len(column.absent)
		}
		binary.LittleEndian.PutUint64(descriptor[48:56], uint64(len(column.values)+len(column.absent)))
	}
	return out, len(columns), nil
}

func buildQueryReadyExecutionImageColumn(scanner *ColumnPartScanner, definition ColumnDefinition, valuesScratch []int64, nullsScratch, defaultsScratch []bool) (queryReadyExecutionImageBuildColumn, []int64, []bool, []bool, error) {
	if scanner == nil || scanner.part == nil {
		return queryReadyExecutionImageBuildColumn{}, nil, nil, nil, errors.New("nil query-ready execution scanner")
	}
	rows := scanner.part.Descriptor.RowCount
	values, nulls, defaults, _, err := scanQueryReadyColumn(scanner, definition.Name, valuesScratch, nullsScratch, defaultsScratch, partSetVisibleRows{All: true})
	if err != nil {
		return queryReadyExecutionImageBuildColumn{}, nil, nil, nil, err
	}
	if len(values) != rows {
		return queryReadyExecutionImageBuildColumn{}, nil, nil, nil, fmt.Errorf("decoded rows=%d want %d", len(values), rows)
	}
	hasAbsent := false
	for row := 0; row < rows; row++ {
		if len(nulls) != 0 && (nulls[row] || defaults[row]) {
			hasAbsent = true
			break
		}
	}
	var absent []byte
	if hasAbsent {
		absent = make([]byte, (rows+7)/8)
		for row := 0; row < rows; row++ {
			if nulls[row] || defaults[row] {
				absent[row>>3] |= uint8(1) << uint(row&7)
			}
		}
	}
	column := queryReadyExecutionImageBuildColumn{name: definition.Name, absent: absent}
	switch definition.Type {
	case ColumnTypeLowCardinalityCode:
		column.kind = QueryReadyExecutionColumnCode
		column.cardinality = definition.Cardinality
		column.codeWidth = queryReadyExecutionCodeWidth(definition.Cardinality)
		if rows > math.MaxInt/column.codeWidth {
			return queryReadyExecutionImageBuildColumn{}, nil, nil, nil, errors.New("code vector exceeds host size")
		}
		column.values = make([]byte, rows*column.codeWidth)
		for row, value := range values {
			if value < 0 || value > math.MaxUint32 || ((!hasAbsent || absent[row>>3]&(uint8(1)<<uint(row&7)) == 0) && uint64(value) >= uint64(definition.Cardinality)) {
				return queryReadyExecutionImageBuildColumn{}, nil, nil, nil, fmt.Errorf("code row=%d value=%d outside cardinality=%d", row, value, definition.Cardinality)
			}
			switch column.codeWidth {
			case 1:
				column.values[row] = byte(value)
			case 2:
				binary.LittleEndian.PutUint16(column.values[row*2:], uint16(value))
			case 4:
				binary.LittleEndian.PutUint32(column.values[row*4:], uint32(value))
			}
		}
	case ColumnTypeInt64:
		column.kind, column.codeWidth = QueryReadyExecutionColumnInt64, 8
		if rows > math.MaxInt/8 {
			return queryReadyExecutionImageBuildColumn{}, nil, nil, nil, errors.New("int64 vector exceeds host size")
		}
		column.values = make([]byte, rows*8)
		for row, value := range values {
			binary.LittleEndian.PutUint64(column.values[row*8:], uint64(value))
		}
	default:
		return queryReadyExecutionImageBuildColumn{}, nil, nil, nil, fmt.Errorf("unsupported type %s", definition.Type)
	}
	return column, values, nulls, defaults, nil
}

func queryReadyExecutionCodeWidth(cardinality uint32) int {
	switch {
	case cardinality <= 1<<8:
		return 1
	case cardinality <= 1<<16:
		return 2
	default:
		return 4
	}
}

func queryReadyExecutionCheckedEnd(offset, length, limit uint64) (uint64, bool) {
	if offset > limit || length > limit-offset {
		return 0, false
	}
	return offset + length, true
}

func queryReadyExecutionCheckedMul(left, right uint64) (uint64, bool) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, false
	}
	return left * right, true
}

func parseQueryReadyExecutionImage(data []byte, expectedRows int) (QueryReadyExecutionPartView, error) {
	if len(data) < queryReadyExecutionImageHeaderBytes {
		return QueryReadyExecutionPartView{}, errors.New("typedcolumn: truncated query-ready execution image header")
	}
	if binary.LittleEndian.Uint32(data[0:4]) != queryReadyExecutionImageMagic {
		return QueryReadyExecutionPartView{}, errors.New("typedcolumn: invalid query-ready execution image magic")
	}
	if version := binary.LittleEndian.Uint16(data[4:6]); version != queryReadyExecutionImageVersion {
		return QueryReadyExecutionPartView{}, fmt.Errorf("typedcolumn: unsupported query-ready execution image version %d", version)
	}
	if binary.LittleEndian.Uint16(data[6:8]) != 0 || binary.LittleEndian.Uint32(data[20:24]) != 0 {
		return QueryReadyExecutionPartView{}, errors.New("typedcolumn: query-ready execution image reserved header bytes are nonzero")
	}
	rows64, columns64 := binary.LittleEndian.Uint64(data[8:16]), uint64(binary.LittleEndian.Uint32(data[16:20]))
	if rows64 > math.MaxInt || int(rows64) != expectedRows || columns64 > queryReadyExecutionImageMaxColumnCount {
		return QueryReadyExecutionPartView{}, fmt.Errorf("typedcolumn: query-ready execution image rows=%d columns=%d mismatch expected rows=%d", rows64, columns64, expectedRows)
	}
	descriptorOffset, payloadOffset, total := binary.LittleEndian.Uint64(data[24:32]), binary.LittleEndian.Uint64(data[32:40]), binary.LittleEndian.Uint64(data[40:48])
	if descriptorOffset != queryReadyExecutionImageHeaderBytes || total != uint64(len(data)) || payloadOffset > total || payloadOffset%queryReadyExecutionImagePayloadAlign != 0 {
		return QueryReadyExecutionPartView{}, errors.New("typedcolumn: invalid query-ready execution image offsets")
	}
	descriptorBytes, ok := queryReadyExecutionCheckedMul(columns64, queryReadyExecutionImageColumnBytes)
	if !ok {
		return QueryReadyExecutionPartView{}, errors.New("typedcolumn: query-ready execution column table size overflows")
	}
	descriptorEnd, ok := queryReadyExecutionCheckedEnd(descriptorOffset, descriptorBytes, total)
	if !ok || descriptorEnd > payloadOffset {
		return QueryReadyExecutionPartView{}, errors.New("typedcolumn: truncated query-ready execution column table")
	}
	view := QueryReadyExecutionPartView{rows: int(rows64), columns: make(map[string]QueryReadyExecutionColumnView, int(columns64)), bytes: data}
	previousName := ""
	nameCursor, dataCursor := descriptorEnd, payloadOffset
	for i := uint64(0); i < columns64; i++ {
		descriptor := data[int(descriptorOffset+i*queryReadyExecutionImageColumnBytes):]
		nameOffset, nameLength := uint64(binary.LittleEndian.Uint32(descriptor[0:4])), uint64(binary.LittleEndian.Uint32(descriptor[4:8]))
		kind, width, nullable := descriptor[8], int(descriptor[9]), descriptor[10]
		if descriptor[11] != 0 || binary.LittleEndian.Uint32(descriptor[56:60]) != 0 || binary.LittleEndian.Uint32(descriptor[60:64]) != 0 {
			return QueryReadyExecutionPartView{}, fmt.Errorf("typedcolumn: query-ready execution column[%d] reserved bytes are nonzero", i)
		}
		nameEnd, ok := queryReadyExecutionCheckedEnd(nameOffset, nameLength, payloadOffset)
		if nameLength == 0 || nameOffset != nameCursor || !ok {
			return QueryReadyExecutionPartView{}, fmt.Errorf("typedcolumn: query-ready execution column[%d] name range is invalid", i)
		}
		nameCursor = nameEnd
		name := string(data[int(nameOffset):int(nameEnd)])
		if previousName != "" && name <= previousName {
			return QueryReadyExecutionPartView{}, fmt.Errorf("typedcolumn: query-ready execution column names are not strictly ordered at %q", name)
		}
		previousName = name
		valuesOffset, valuesLength := binary.LittleEndian.Uint64(descriptor[16:24]), binary.LittleEndian.Uint64(descriptor[24:32])
		absentOffset, absentLength := binary.LittleEndian.Uint64(descriptor[32:40]), binary.LittleEndian.Uint64(descriptor[40:48])
		cardinality := binary.LittleEndian.Uint32(descriptor[12:16])
		wantValues := uint64(0)
		switch kind {
		case queryReadyExecutionImageColumnCode:
			if width != queryReadyExecutionCodeWidth(cardinality) {
				return QueryReadyExecutionPartView{}, fmt.Errorf("typedcolumn: query-ready execution code column %s width=%d does not match cardinality=%d", name, width, cardinality)
			}
			wantValues, ok = queryReadyExecutionCheckedMul(rows64, uint64(width))
			if !ok {
				return QueryReadyExecutionPartView{}, fmt.Errorf("typedcolumn: query-ready execution code column %s size overflows", name)
			}
		case queryReadyExecutionImageColumnInt64:
			if width != 8 || cardinality != 0 {
				return QueryReadyExecutionPartView{}, fmt.Errorf("typedcolumn: invalid query-ready execution int64 column %s", name)
			}
			wantValues, ok = queryReadyExecutionCheckedMul(rows64, 8)
			if !ok {
				return QueryReadyExecutionPartView{}, fmt.Errorf("typedcolumn: query-ready execution int64 column %s size overflows", name)
			}
		default:
			return QueryReadyExecutionPartView{}, fmt.Errorf("typedcolumn: unsupported query-ready execution column kind %d", kind)
		}
		valuesEnd, valuesRangeOK := queryReadyExecutionCheckedEnd(valuesOffset, valuesLength, total)
		alignedValues, err := queryReadyBaseAlign(int(dataCursor), queryReadyExecutionImagePayloadAlign)
		if err != nil || valuesOffset != uint64(alignedValues) || valuesLength != wantValues || !valuesRangeOK {
			return QueryReadyExecutionPartView{}, fmt.Errorf("typedcolumn: query-ready execution column %s values range is invalid", name)
		}
		if err := queryReadyBaseValidateZeroPadding(data[int(dataCursor):alignedValues], fmt.Sprintf("query-ready execution column %s values", name)); err != nil {
			return QueryReadyExecutionPartView{}, err
		}
		dataCursor = valuesEnd
		wantAbsent := uint64((int(rows64) + 7) / 8)
		if nullable == 0 {
			if absentOffset != 0 || absentLength != 0 {
				return QueryReadyExecutionPartView{}, fmt.Errorf("typedcolumn: nonnullable query-ready execution column %s has absence bytes", name)
			}
		} else {
			absentEnd, absentRangeOK := queryReadyExecutionCheckedEnd(absentOffset, absentLength, total)
			alignedAbsent, err := queryReadyBaseAlign(int(dataCursor), queryReadyExecutionImagePayloadAlign)
			if err != nil || nullable != queryReadyExecutionImageNullable || absentLength != wantAbsent || absentOffset != uint64(alignedAbsent) || !absentRangeOK {
				return QueryReadyExecutionPartView{}, fmt.Errorf("typedcolumn: query-ready execution column %s absence range is invalid", name)
			}
			if err := queryReadyBaseValidateZeroPadding(data[int(dataCursor):alignedAbsent], fmt.Sprintf("query-ready execution column %s absence", name)); err != nil {
				return QueryReadyExecutionPartView{}, err
			}
			dataCursor = absentEnd
		}
		accounted, accountingOK := queryReadyExecutionCheckedEnd(valuesLength, absentLength, math.MaxUint64)
		if !accountingOK || binary.LittleEndian.Uint64(descriptor[48:56]) != accounted {
			return QueryReadyExecutionPartView{}, fmt.Errorf("typedcolumn: query-ready execution column %s byte accounting mismatch", name)
		}
		column := QueryReadyExecutionColumnView{name: name, kind: QueryReadyExecutionColumnKind(kind), codeWidth: width, cardinality: cardinality, rows: int(rows64), values: data[int(valuesOffset):int(valuesEnd)]}
		if absentLength != 0 {
			absentEnd, _ := queryReadyExecutionCheckedEnd(absentOffset, absentLength, total)
			column.absent = data[int(absentOffset):int(absentEnd)]
		}
		view.columns[name] = column
	}
	if err := queryReadyBaseValidateZeroPadding(data[int(nameCursor):int(payloadOffset)], "query-ready execution names"); err != nil {
		return QueryReadyExecutionPartView{}, err
	}
	if dataCursor != total {
		return QueryReadyExecutionPartView{}, fmt.Errorf("typedcolumn: query-ready execution image has %d unaccounted trailing bytes", total-dataCursor)
	}
	return view, nil
}
