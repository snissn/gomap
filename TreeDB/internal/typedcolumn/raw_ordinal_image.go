package typedcolumn

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// ErrRawOrdinalImageUnsupported leaves selection of the general constructor to
// the caller. This constructor never silently changes a requested codec/layout.
var ErrRawOrdinalImageUnsupported = errors.New("typedcolumn: raw ordinal image layout unsupported")

// RawOrdinalColumnValues borrows one value column. Primary IDs are implicit
// ascending ordinals [0, Rows). Exactly the carrier selected by Options is used.
// Neither a plan nor a result owns these borrowed source slices.
type RawOrdinalColumnValues struct {
	Rows       int
	Int64      []int64
	Float32    []float32
	Uint32List RawUint32OffsetsList
	Bytes      RawBytesOffsets
}

// RawOrdinalColumnShape describes source lengths without requiring source
// carriers. Type must match Options.Columns[1].Type. ValueCount counts scalar
// values (exactly Rows), flattened uint32 elements, or bytes, respectively.
// Offsets are implicit in this plan: offset columns require Rows+1 uint64s.
// A shape proves allocation geometry, not validity of any future source data.
// Source, Config, Options and dictionary ownership remain with the caller.
// A caller reserving this plan must preserve these counts and metadata through
// construction and reconcile the returned Charge with its reservation.
type RawOrdinalColumnShape struct {
	Rows       int
	Type       ColumnType
	ValueCount uint64
}

// RawOrdinalImagePlan is a checked preallocation bound, not encoded bytes alone.
// ConstructorBytes conservatively sums all constructor allocation cohorts,
// including final image/certification/accounting. RetainedBytes bounds result
// storage. Caller-owned Options/Config/dictionaries/source backing is excluded;
// the caller keeps its original ownership charge while this operation borrows it.
type RawOrdinalImagePlan struct {
	ConstructorBytes     uint64
	RetainedBytes        uint64
	ImageBytesUpperBound int
	MetadataCapacity     int
	Granules             int
	primaryBytes         int
	valueBytes           int
	offsetsBytes         int
}

type RawOrdinalColumnPartImageResult struct {
	Image      ColumnPartImage
	Accounting ColumnPartByteAccounting
	Charge     RawOrdinalImagePlan
}

// PlanRawOrdinalColumnPartImage validates actual carrier shape and complete
// offsets, then returns the same allocation ledger as the lengths-only planner.
// It does not allocate successful-path output.
func PlanRawOrdinalColumnPartImage(opts Options, values RawOrdinalColumnValues, imageOpts ColumnPartImageOptions) (RawOrdinalImagePlan, error) {
	if len(opts.Columns) != 2 {
		return RawOrdinalImagePlan{}, ErrRawOrdinalImageUnsupported
	}
	shape := RawOrdinalColumnShape{Rows: values.Rows, Type: opts.Columns[1].Type}
	switch shape.Type {
	case ColumnTypeInt64:
		shape.ValueCount = uint64(len(values.Int64))
	case ColumnTypeFloat32:
		shape.ValueCount = uint64(len(values.Float32))
	case ColumnTypeUint32List, ColumnTypeAdjacencyList:
		shape.ValueCount = uint64(len(values.Uint32List.Values))
	case ColumnTypeBytes:
		shape.ValueCount = uint64(len(values.Bytes.Values))
	}
	plan, err := PlanRawOrdinalColumnPartImageShape(opts, shape, imageOpts)
	if err != nil {
		return RawOrdinalImagePlan{}, err
	}
	switch shape.Type {
	case ColumnTypeInt64:
		if len(values.Float32) != 0 || values.Uint32List.Offsets != nil || values.Bytes.Offsets != nil {
			return RawOrdinalImagePlan{}, ErrRawOrdinalImageUnsupported
		}
	case ColumnTypeFloat32:
		if len(values.Int64) != 0 || values.Uint32List.Offsets != nil || values.Bytes.Offsets != nil {
			return RawOrdinalImagePlan{}, ErrRawOrdinalImageUnsupported
		}
	case ColumnTypeUint32List, ColumnTypeAdjacencyList:
		if values.Uint32List.Rows != values.Rows || len(values.Int64) != 0 || len(values.Float32) != 0 || values.Bytes.Offsets != nil {
			return RawOrdinalImagePlan{}, ErrRawOrdinalImageUnsupported
		}
		if err := values.Uint32List.Validate(); err != nil {
			return RawOrdinalImagePlan{}, err
		}
	case ColumnTypeBytes:
		if values.Bytes.Rows != values.Rows || len(values.Int64) != 0 || len(values.Float32) != 0 || values.Uint32List.Offsets != nil {
			return RawOrdinalImagePlan{}, ErrRawOrdinalImageUnsupported
		}
		if err := values.Bytes.Validate(); err != nil {
			return RawOrdinalImagePlan{}, err
		}
	}
	return plan, nil
}

// PlanRawOrdinalColumnPartImageShape plans from checked lengths before source
// carriers exist. It shares the complete constructor ledger with the actual-
// values planner; no dummy slices or payload allocation are required. It covers
// supported constructor allocations only, not the caller's source/Config or
// metadata prelude. BuildRawOrdinalColumnPartImage always validates actual data.
func PlanRawOrdinalColumnPartImageShape(opts Options, shape RawOrdinalColumnShape, imageOpts ColumnPartImageOptions) (RawOrdinalImagePlan, error) {
	var plan RawOrdinalImagePlan
	if len(opts.Columns) != 2 || (opts.SchemaMode != "" && opts.SchemaMode != ColumnSchemaFixed) || opts.BuildCodec != nil || len(opts.AggregateMetadata) != 0 || opts.Compression.Default != CompressionNone || opts.PartPolicy.DefaultCodecBlockRows != 0 || opts.PartPolicy.AdaptiveMarkSizing != (ColumnAdaptiveMarkSizing{}) || (opts.PartPolicy.RowsPerGranule != 0 && opts.PartPolicy.RowsPerGranule != DefaultRowsPerGranule) {
		return plan, ErrRawOrdinalImageUnsupported
	}
	p, v := opts.Columns[0], opts.Columns[1]
	if p.Name == "" || v.Name == "" || p.Name == v.Name || p != (ColumnDefinition{Name: p.Name, Type: ColumnTypeInt64, Encoding: EncodingRawInt64, CompressionSet: p.CompressionSet, StatsDisabled: true}) || v.Compression != CompressionNone || v.Cardinality != 0 || v.FixedWidthElements != 0 || v.BitsPerElement != 0 || v.CodecBlockRows != 0 {
		return plan, ErrRawOrdinalImageUnsupported
	}
	if len(opts.LogicalPrimaryKey.Columns) != 1 || opts.LogicalPrimaryKey.Columns[0] != p.Name || len(opts.SortKey.Columns) > 1 {
		return plan, ErrRawOrdinalImageUnsupported
	}
	if len(opts.SortKey.Columns) == 1 {
		s := opts.SortKey.Columns[0]
		if s.Column != p.Name || (s.Direction != "" && s.Direction != SortKeyAsc) || s.Nulls != SortKeyNullsDefault {
			return plan, ErrRawOrdinalImageUnsupported
		}
	}
	if imageOpts.BuildCodec != nil || imageOpts.SectionCompression != CompressionNone || imageOpts.RowLocatorSectionCompression != CompressionNone || imageOpts.DictionarySectionCompression != CompressionNone || imageOpts.PruningMetadataSectionCompression != CompressionNone {
		return plan, ErrRawOrdinalImageUnsupported
	}
	for _, name := range []string{p.Name, v.Name} {
		if imageOpts.DictionaryOrder[name] && imageOpts.DictionaryCollation[name] == "" {
			return plan, ErrRawOrdinalImageUnsupported
		}
	}
	align, err := columnPartImageAlignment(imageOpts.SectionAlignment)
	if err != nil {
		return plan, err
	}
	if shape.Type != v.Type {
		return plan, ErrRawOrdinalImageUnsupported
	}
	if shape.Rows < 0 || uint64(shape.Rows) > math.MaxUint32 {
		return plan, fmt.Errorf("typedcolumn: invalid raw ordinal rows %d", shape.Rows)
	}
	rows := shape.Rows
	list := false
	switch v.Type {
	case ColumnTypeInt64:
		if v.Encoding != EncodingRawInt64 || shape.ValueCount != uint64(rows) {
			return plan, ErrRawOrdinalImageUnsupported
		}
	case ColumnTypeFloat32:
		if v.Encoding != EncodingRawFloat32 || shape.ValueCount != uint64(rows) {
			return plan, ErrRawOrdinalImageUnsupported
		}
	case ColumnTypeUint32List, ColumnTypeAdjacencyList:
		if v.Encoding != EncodingRawUint32OffsetsList {
			return plan, ErrRawOrdinalImageUnsupported
		}
		list = true
	case ColumnTypeBytes:
		if v.Encoding != EncodingRawBytesOffsets {
			return plan, ErrRawOrdinalImageUnsupported
		}
		list = true
	default:
		return plan, ErrRawOrdinalImageUnsupported
	}
	// A zero-row offsets source starts and ends at offset zero, so it cannot
	// carry any flattened values. Complete offsets remain a Build validation.
	if rows == 0 && list && shape.ValueCount != 0 {
		return plan, ErrRawOrdinalImageUnsupported
	}
	// The general constructor admits empty offset-column parts, but rejects
	// empty scalar parts. Preserve that distinction.
	if rows == 0 && !list {
		return plan, fmt.Errorf("typedcolumn: invalid part rows 0")
	}
	g := 0
	if rows > 0 {
		g = 1 + (rows-1)/DefaultRowsPerGranule
	}
	plan.Granules = g
	bound := rawOrdinalByteBound{}
	primary := bound.product(uint64(rows), 8)
	value := uint64(0)
	offsets := uint64(0)
	switch v.Type {
	case ColumnTypeInt64:
		value = bound.product(uint64(rows), 8)
	case ColumnTypeFloat32:
		value = bound.product(uint64(rows), 4)
	case ColumnTypeUint32List, ColumnTypeAdjacencyList:
		value = bound.product(shape.ValueCount, 4)
		offsets = bound.product(uint64(rows)+1, 8)
	case ColumnTypeBytes:
		value = shape.ValueCount
		offsets = bound.product(uint64(rows)+1, 8)
	}
	// Metadata lengths are sums of the same fixed fields/string frames as the
	// existing encoders. Their sum bounds each encoder, avoiding append growth.
	pn, vn := uint64(len(p.Name)), uint64(len(v.Name))
	for _, s := range []string{p.Name, v.Name, imageOpts.LayoutLogicalTypes[p.Name], imageOpts.LayoutLogicalTypes[v.Name], imageOpts.DictionaryCollation[p.Name], imageOpts.DictionaryCollation[v.Name]} {
		if len(s) > maxColumnPartImageStringBytes {
			return plan, ErrRawOrdinalImageUnsupported
		}
	}
	meta := uint64(0)
	bound.add(&meta, 90, 2*pn, vn, bound.product(uint64(g), 252)) // descriptor
	bound.add(&meta, 16, pn, uint64(len(SortKeyAsc)))             // sort key
	bound.add(&meta, 4, bound.product(uint64(g), 60+2*pn))        // marks
	bound.add(&meta, rowLocatorContiguousPayloadBytes+4)          // locators, including empty representation
	bound.add(&meta, 56+2*152, pn, vn, uint64(len(imageOpts.LayoutLogicalTypes[p.Name])), uint64(len(imageOpts.LayoutLogicalTypes[v.Name])), uint64(len(imageOpts.DictionaryCollation[p.Name])), uint64(len(imageOpts.DictionaryCollation[v.Name])), bound.product(uint64(g), 2*88))
	if list {
		bound.add(&meta, 56)
	}
	// At most eleven sections: four structural, two stats/pruning, dictionary,
	// layout, primary data and two value sections. Each directory entry has 64
	// fixed/framing bytes; names and column strings are accounted separately.
	bound.add(&meta, 32+11*64, uint64(len("part_descriptor")+len("sort_key")+len("sort_key_marks")+len("primary_id_locators")+len("column_stats")+len("column_pruning")+len("part_dictionaries")+len("writer_layout_contract")), pn, 2*vn)
	if uint64(len(imageOpts.Dictionaries)) > math.MaxUint32 {
		return plan, ErrRawOrdinalImageUnsupported
	}
	for name, entries := range imageOpts.Dictionaries {
		if uint64(len(entries)) > math.MaxUint32 {
			return plan, ErrRawOrdinalImageUnsupported
		}
		if len(name) > maxColumnPartImageStringBytes {
			return plan, ErrRawOrdinalImageUnsupported
		}
		bound.add(&meta, 2*(8+uint64(len(name)))) // raw and compact name/count
		for text := range entries {
			if len(text) > maxColumnPartImageStringBytes {
				return plan, ErrRawOrdinalImageUnsupported
			}
			bound.add(&meta, 8+2*(4+uint64(len(text))))
		}
	}
	bound.add(&meta, 4+12) // raw/compact dictionary headers
	stats := v.Type == ColumnTypeInt64 && !v.StatsDisabled
	pruningPayload := uint64(0)
	if stats {
		// Eight operations and both selection shapes cover overflow and zero
		// block variants; each is a four-byte frame plus its literal spelling.
		envelope := uint64(100) + vn + uint64(len(ColumnStatsPayloadInt64V1))
		for _, op := range []ColumnStatsOperation{ColumnStatsOpCountRows, ColumnStatsOpCountNonNull, ColumnStatsOpMin, ColumnStatsOpMax, ColumnStatsOpStatsMinMax, ColumnStatsOpSum, ColumnStatsOpAvg, ColumnStatsOpStatsSum} {
			bound.add(&envelope, 4+uint64(len(op)))
		}
		for _, shape := range []ColumnStatsSelectionShape{ColumnStatsSelectionFullBlock, ColumnStatsSelectionAllRows} {
			bound.add(&envelope, 4+uint64(len(shape)))
		}
		bound.add(&meta, 28, envelope, 76, bound.product(uint64(g), 84))
		pruningPayload = 36
		bound.add(&pruningPayload, bound.product(uint64(g), uint64(int64PruningBlockEncodedBytes)), bound.product(uint64(rows), 16))
		pruningEnvelope := uint64(80) + vn + uint64(len(ColumnPruningPayloadInt64ValueRowsV1))
		for _, op := range []ColumnPruningOperation{ColumnPruningOpEquality, ColumnPruningOpOrderedRange} {
			bound.add(&pruningEnvelope, 4+uint64(len(op)))
		}
		bound.add(&meta, 28, pruningEnvelope, pruningPayload)
	}
	imageBound := meta
	bound.add(&imageBound, primary, value, offsets, 11*uint64(align-1))
	if bound.failed || imageBound > uint64(int(^uint(0)>>1)) || meta > uint64(int(^uint(0)>>1)) {
		return plan, errRawOrdinalImageCapacity
	}
	plan.MetadataCapacity = int(meta)
	plan.ImageBytesUpperBound = int(imageBound)
	plan.primaryBytes = int(primary)
	plan.valueBytes = int(value)
	plan.offsetsBytes = int(offsets)
	// Sum all cohorts, not just simultaneous live bytes. This deliberately
	// avoids reliance on compiler liveness or collection of discarded objects.
	charge := rawOrdinalByteBound{}
	add := func(n uint64) { charge.add(&plan.ConstructorBytes, retainedalloc.AllocationCharge(n)) }
	array := func(n uint64, width uintptr) { add(charge.product(n, uint64(width))) }
	add(uint64(unsafe.Sizeof(ColumnPart{})))
	array(2, unsafe.Sizeof(ColumnDefinition{}))
	array(1, unsafe.Sizeof(string("")))
	array(1, unsafe.Sizeof(SortKeyColumn{}))
	array(uint64(g), unsafe.Sizeof(GranuleDescriptor{}))
	array(2, unsafe.Sizeof(ColumnPartColumnDescriptor{}))
	array(uint64(g), unsafe.Sizeof(SortKeyMark{}))
	for i := 0; i < 2; i++ {
		array(uint64(g), unsafe.Sizeof(ColumnBlock{}))
		array(uint64(g), unsafe.Sizeof(ColumnBlockDescriptor{}))
	}
	// Each mark owns names, prefix names, prefix descriptors and two bounds.
	for i := 0; i < g; i++ {
		array(1, unsafe.Sizeof(string("")))
		array(1, unsafe.Sizeof(string("")))
		array(1, unsafe.Sizeof(SortKeyPrefixSummary{}))
		array(1, 8)
		array(1, 8)
	}
	add(primary)
	if !list {
		add(value)
	} else {
		add(offsets)
		add(value)
	}
	// Three structural encoders; stats section+payload; pruning section;
	// two dictionary alternatives; initial/refreshed layout; two manifests.
	for i := 0; i < 3+2+1+2+2+2; i++ {
		add(meta)
	}
	add(pruningPayload)
	add(rowLocatorContiguousPayloadBytes + 4)
	array(11, unsafe.Sizeof(columnPartImageSectionData{}))
	array(11, unsafe.Sizeof(ColumnPartImageSection{}))
	add(imageBound)
	// Encoder dictionary/name sorting and stats/pruning envelope conversion.
	array(uint64(len(imageOpts.Dictionaries)), unsafe.Sizeof(string("")))
	for _, entries := range imageOpts.Dictionaries {
		array(uint64(len(entries)), unsafe.Sizeof(dictionaryImageEntry{}))
	}
	array(1, unsafe.Sizeof(string("")))
	array(1, unsafe.Sizeof(string("")))
	array(8, unsafe.Sizeof(string("")))
	array(2, unsafe.Sizeof(string("")))
	array(2, unsafe.Sizeof(string("")))
	if stats {
		array(uint64(g), unsafe.Sizeof(Int64BlockStats{}))
		array(uint64(g), unsafe.Sizeof(Int64PruningBlock{}))
		array(uint64(rows), unsafe.Sizeof(Int64PruningEntry{}))
		array(uint64(min(rows, DefaultRowsPerGranule)), 8)
		array(uint64(rows), 1)
		// The shared builder's operation and selection append cohorts.
		array(5, unsafe.Sizeof(ColumnStatsOperation("")))
		array(10, unsafe.Sizeof(ColumnStatsOperation("")))
		array(1, unsafe.Sizeof(ColumnStatsSelectionShape("")))
		array(2, unsafe.Sizeof(ColumnStatsSelectionShape("")))
		array(2, unsafe.Sizeof(ColumnPruningOperation("")))
	}
	// Both layout builds allocate two exact block vectors. The existing final
	// certifier decodes one descriptor and one contract, plus bounded names.
	for i := 0; i < 2*2+2; i++ {
		array(uint64(g), unsafe.Sizeof(ColumnPartLayoutContractBlock{}))
	}
	array(uint64(g), unsafe.Sizeof(GranuleDescriptor{}))
	array(1, unsafe.Sizeof(ColumnPartColumnDescriptor{}))
	array(2, unsafe.Sizeof(ColumnPartColumnDescriptor{}))
	array(1, unsafe.Sizeof(string("")))
	for i := 0; i < 2; i++ {
		array(uint64(g), unsafe.Sizeof(ColumnBlock{}))
		array(uint64(g), unsafe.Sizeof(ColumnBlockDescriptor{}))
	}
	array(2, unsafe.Sizeof(ColumnPartLayoutContractColumn{}))
	for _, s := range []string{p.Name, p.Name, v.Name, p.Name, v.Name, imageOpts.LayoutLogicalTypes[p.Name], imageOpts.LayoutLogicalTypes[v.Name], imageOpts.DictionaryCollation[p.Name], imageOpts.DictionaryCollation[v.Name]} {
		add(uint64(len(s)))
	}
	// Success-path certification labels (scalar or offsets and values).
	for _, name := range []string{p.Name, v.Name} {
		add(uint64(len("column ") + len(name) + len(" offsets")))
		add(uint64(len("column ") + len(name) + len(" values")))
		add(uint64(len(name) + len(":no_fast_layout")))
	}
	array(1, unsafe.Sizeof(string("")))
	array(2, unsafe.Sizeof(string(""))) // fallback append cohorts
	// sectionsByKind temporary results: descriptor, layout lookup and certify.
	for i := 0; i < 4; i++ {
		array(1, unsafe.Sizeof(ColumnPartImageSection{}))
	}
	// Accounting returns two columns, two compression groups and image rows;
	// SectionByteAccounting may grow once for its padding accounting record.
	array(2, unsafe.Sizeof(ColumnPartColumnByteAccounting{}))
	array(2, unsafe.Sizeof(ColumnPartCompressionByteAccounting{}))
	for i := 0; i < 2; i++ {
		add(uint64(unsafe.Sizeof(ColumnPartCompressionByteAccounting{})))
	}
	array(12, unsafe.Sizeof(ColumnPartImageSectionByteAccounting{}))
	array(24, unsafe.Sizeof(ColumnPartImageSectionByteAccounting{}))
	// Repository policy: four slots per logical map entry, with a minimum
	// group, plus 1KiB per entry/control allowance (native_hnsw.go and
	// column_retained_semantic_stream.go). Values are charged at their full
	// source type size even when runtime maps store them indirectly. There is
	// no runtime ABI/version dispatch. All maps here have fixed cardinality.
	mapCharge := func(entries uint64, key, value uintptr) {
		slots := max(uint64(8), 4*entries)
		array(slots, key+value+8)
		array(max(entries, uint64(1)), 1024)
	}
	mapCharge(2, unsafe.Sizeof(""), unsafe.Sizeof(ColumnPartColumn{})) // construction columns
	if stats {
		mapCharge(1, unsafe.Sizeof(""), unsafe.Sizeof(Int64ColumnStats{}))
		mapCharge(1, unsafe.Sizeof(""), unsafe.Sizeof(Int64ValueRowIndex{}))
	}
	for i := 0; i < 2; i++ {
		mapCharge(2, unsafe.Sizeof(""), unsafe.Sizeof(columnPartImageColumnSections{}))
	}
	mapCharge(11, unsafe.Sizeof(ColumnPartImageSectionKind("")), unsafe.Sizeof(int(0)))
	mapCharge(2, 2*unsafe.Sizeof(""), unsafe.Sizeof(int(0)))
	mapCharge(2, unsafe.Sizeof(""), unsafe.Sizeof(ColumnPartColumn{})) // decoded descriptor
	mapCharge(2, unsafe.Sizeof(""), 0)
	mapCharge(2, unsafe.Sizeof(""), unsafe.Sizeof(ColumnPartLayoutContractColumn{}))
	mapCharge(2, unsafe.Sizeof(columnCompressionKey{}), unsafe.Sizeof(uintptr(0)))
	for i := 0; i < 2; i++ {
		mapCharge(1, unsafe.Sizeof(""), unsafe.Sizeof(int(0)))
		mapCharge(0, unsafe.Sizeof(""), unsafe.Sizeof(int(0)))
	}
	// Bounded helper/sort controls and boxed reflection values: one per actual
	// sort invocation, plus builder/certification/accounting control records.
	array(uint64(len(imageOpts.Dictionaries))+1+1+1+1+1+uint64(g), 1024)
	add(uint64(unsafe.Sizeof(columnPartImageBuilder{})))
	add(uint64(unsafe.Sizeof(RawOrdinalColumnPartImageResult{})))
	if charge.failed {
		return RawOrdinalImagePlan{}, errRawOrdinalImageCapacity
	}
	// Only final image/directory/accounting storage escapes. Names borrow the
	// caller metadata already charged by its owner; temporary maps and Part
	// payload buffers do not escape. Charge both possible accounting cohorts
	// conservatively so the retained bound is valid with or without padding.
	retained := rawOrdinalByteBound{}
	retain := func(n uint64) { retained.add(&plan.RetainedBytes, retainedalloc.AllocationCharge(n)) }
	retain(imageBound)
	retain(11 * uint64(unsafe.Sizeof(ColumnPartImageSection{})))
	retain(2 * uint64(unsafe.Sizeof(ColumnPartColumnByteAccounting{})))
	retain(2 * uint64(unsafe.Sizeof(ColumnPartCompressionByteAccounting{})))
	retain(12 * uint64(unsafe.Sizeof(ColumnPartImageSectionByteAccounting{})))
	retain(24 * uint64(unsafe.Sizeof(ColumnPartImageSectionByteAccounting{})))
	for i := 0; i < 2; i++ {
		retain(8 * uint64(unsafe.Sizeof("")+unsafe.Sizeof(int(0))+8))
		retain(1024)
	}
	if retained.failed {
		return RawOrdinalImagePlan{}, errRawOrdinalImageCapacity
	}
	return plan, nil
}

var errRawOrdinalImageCapacity = errors.New("typedcolumn: raw ordinal image capacity overflow")

type rawOrdinalByteBound struct{ failed bool }

func (b *rawOrdinalByteBound) add(dst *uint64, items ...uint64) {
	for _, n := range items {
		if n > math.MaxUint64-*dst {
			b.failed = true
			return
		}
		*dst += n
	}
}
func (b *rawOrdinalByteBound) product(n, width uint64) uint64 {
	if n != 0 && width > math.MaxUint64/n {
		b.failed = true
		return 0
	}
	return n * width
}

// BuildRawOrdinalColumnPartImage uses the same raw codecs, section encoders,
// writer certifier and accounting as the general path. It avoids sorted-order,
// locator and gather maps, block-payload clones and offset decode/repack arrays.
func BuildRawOrdinalColumnPartImage(partID uint64, opts Options, values RawOrdinalColumnValues, imageOpts ColumnPartImageOptions) (RawOrdinalColumnPartImageResult, error) {
	plan, err := PlanRawOrdinalColumnPartImage(opts, values, imageOpts)
	if err != nil {
		return RawOrdinalColumnPartImageResult{}, err
	}
	rows, g := values.Rows, plan.Granules
	// Normalization for this gated shape requires only exact descriptor copies.
	opts.Columns = append([]ColumnDefinition(nil), opts.Columns...)
	opts.LogicalPrimaryKey.Columns = append([]string(nil), opts.LogicalPrimaryKey.Columns...)
	opts.SortKey.Columns = []SortKeyColumn{{Column: opts.Columns[0].Name, Direction: SortKeyAsc}}
	if opts.SchemaVersion == 0 {
		opts.SchemaVersion = 1
	}
	opts.SchemaMode = ColumnSchemaFixed
	opts.PartPolicy.RowsPerGranule = DefaultRowsPerGranule
	part := &ColumnPart{Options: opts, Descriptor: ColumnPartDescriptor{Version: columnPartDescriptorVersion, PartID: partID, SchemaVersion: opts.SchemaVersion, RowCount: rows, VisibleRowCount: rows, LogicalPrimaryKey: opts.LogicalPrimaryKey.Columns, SortKey: opts.SortKey.Columns, Granules: make([]GranuleDescriptor, g), Columns: make([]ColumnPartColumnDescriptor, 2)}, Columns: make(map[string]ColumnPartColumn, 2), Marks: make([]SortKeyMark, g)}
	for i := 0; i < g; i++ {
		start := i * DefaultRowsPerGranule
		end := min(start+DefaultRowsPerGranule, rows)
		part.Descriptor.Granules[i] = GranuleDescriptor{Ordinal: i, FirstRow: start, RowCount: end - start, VisibleRows: end - start, IDLower: int64(start), IDUpperExclusive: int64(end), MarkOrdinal: i}
		names := []string{opts.Columns[0].Name}
		part.Marks[i] = SortKeyMark{Rows: end - start, Columns: names, Prefixes: []SortKeyPrefixSummary{{Columns: []string{opts.Columns[0].Name}, Lower: SortKeyBound{Values: []int64{int64(start)}}, UpperExclusive: SortKeyBound{Values: []int64{int64(end)}, Exclusive: true}}}}
	}
	primary := make([]byte, plan.primaryBytes)
	for i := 0; i < rows; i++ {
		binary.LittleEndian.PutUint64(primary[i*8:], uint64(i))
	}
	var valueData, offsetData []byte
	v := opts.Columns[1]
	switch v.Type {
	case ColumnTypeInt64:
		valueData, err = encodeInt64Payload(make([]byte, 0, plan.valueBytes), values.Int64, EncodingRawInt64)
	case ColumnTypeFloat32:
		valueData, err = encodeFloat32Payload(make([]byte, 0, plan.valueBytes), values.Float32)
	case ColumnTypeUint32List, ColumnTypeAdjacencyList:
		offsetData, err = EncodeRawUint32OffsetsListOffsets(make([]byte, 0, plan.offsetsBytes), values.Uint32List.Offsets)
		if err == nil {
			valueData, err = EncodeRawUint32OffsetsListValues(make([]byte, 0, plan.valueBytes), values.Uint32List.Values)
		}
	case ColumnTypeBytes:
		offsetData, err = EncodeRawBytesOffsetsOffsets(make([]byte, 0, plan.offsetsBytes), values.Bytes.Offsets)
		if err == nil {
			valueData, err = EncodeRawBytesOffsetsValues(make([]byte, 0, plan.valueBytes), values.Bytes.Values)
		}
	}
	if err != nil {
		return RawOrdinalColumnPartImageResult{}, err
	}
	for col, def := range opts.Columns {
		column := ColumnPartColumn{Definition: def, Blocks: make([]ColumnBlock, g)}
		desc := ColumnPartColumnDescriptor{Name: def.Name, Type: def.Type, Blocks: make([]ColumnBlockDescriptor, g)}
		for i := 0; i < g; i++ {
			start := i * DefaultRowsPerGranule
			end := min(start+DefaultRowsPerGranule, rows)
			count := end - start
			var payload []byte
			rawBytes := 0
			lo, hi := int64(0), int64(0)
			has := false
			if col == 0 {
				payload = primary[start*8 : end*8]
				rawBytes = len(payload)
				lo = int64(start)
				hi = int64(end - 1)
				has = true
			} else {
				switch def.Type {
				case ColumnTypeInt64:
					payload = valueData[start*8 : end*8]
					rawBytes = len(payload)
					lo, hi = minMax(values.Int64[start:end])
					has = true
				case ColumnTypeFloat32:
					payload = valueData[start*4 : end*4]
					rawBytes = len(payload)
				case ColumnTypeUint32List, ColumnTypeAdjacencyList:
					rawBytes = (count+1)*8 + int(values.Uint32List.Offsets[end]-values.Uint32List.Offsets[start])*4
				case ColumnTypeBytes:
					rawBytes = (count+1)*8 + int(values.Bytes.Offsets[end]-values.Bytes.Offsets[start])
				}
			}
			granule := rawOrdinalGranule(count, rawBytes, def.Encoding, payload, lo, hi, has)
			bd := ColumnBlockDescriptor{FirstRow: start, RowCount: count, FirstGranule: i, LastGranule: i, Encoding: def.Encoding, Compression: CompressionNone, RawBytes: rawBytes, StoredBytes: rawBytes, CodecBlockOrdinal: i}
			column.Blocks[i] = ColumnBlock{Descriptor: bd, Granule: granule}
			desc.Blocks[i] = bd
		}
		part.Columns[def.Name] = column
		part.Descriptor.Columns[col] = desc
	}
	part.ColumnStats, err = buildColumnPartStats(part)
	if err != nil {
		return RawOrdinalColumnPartImageResult{}, err
	}
	part.PruningMetadata, err = buildColumnPartPruning(part)
	if err != nil {
		return RawOrdinalColumnPartImageResult{}, err
	}
	align, _ := columnPartImageAlignment(imageOpts.SectionAlignment)
	b := columnPartImageBuilder{part: part, opts: imageOpts, sectionAlignment: align, sections: make([]columnPartImageSectionData, 0, 11), encoderCapacity: plan.MetadataCapacity}
	if err = b.addDescriptorSection(); err != nil {
		return RawOrdinalColumnPartImageResult{}, err
	}
	if err = b.addSortKeyMetadataSection(); err != nil {
		return RawOrdinalColumnPartImageResult{}, err
	}
	if err = b.addSortKeyMarksSection(); err != nil {
		return RawOrdinalColumnPartImageResult{}, err
	}
	enc := columnPartImageEncoder{buf: make([]byte, 0, rowLocatorContiguousPayloadBytes+4)}
	locator := ColumnPartImageSection{Kind: ColumnPartImageSectionRowLocators, Category: ColumnPartImageCategoryLocators, Name: "primary_id_locators", Rows: rows}
	if rows == 0 {
		enc.u32(0)
	} else {
		locator.Encoding = EncodingRowLocatorContiguous
		enc.u32(rowLocatorContiguousMagic)
		enc.u16(rowLocatorContiguousVersion)
		enc.u16(0)
		enc.u64(partID)
		enc.u64(uint64(rows))
		enc.i64(0)
	}
	if err = b.appendSectionWithOptionalCompression(locator, enc.bytes(), CompressionNone); err != nil {
		return RawOrdinalColumnPartImageResult{}, err
	}
	if err = b.addColumnStatsSection(); err != nil {
		return RawOrdinalColumnPartImageResult{}, err
	}
	if err = b.addPruningMetadataSection(); err != nil {
		return RawOrdinalColumnPartImageResult{}, err
	}
	if err = b.addDictionarySection(); err != nil {
		return RawOrdinalColumnPartImageResult{}, err
	}
	b.appendSection(ColumnPartImageSection{Kind: ColumnPartImageSectionColumnData, Category: ColumnPartImageCategoryDeclaredColumns, Column: opts.Columns[0].Name, Rows: rows, Granules: g, Blocks: g, Encoding: EncodingRawInt64}, primary)
	if offsetData == nil {
		b.appendSection(ColumnPartImageSection{Kind: ColumnPartImageSectionColumnData, Category: ColumnPartImageCategoryDeclaredColumns, Column: v.Name, Rows: rows, Granules: g, Blocks: g, Encoding: v.Encoding}, valueData)
	} else {
		var os, vs ColumnPartImageSection
		if v.Type == ColumnTypeBytes {
			os, vs, err = NewRawBytesOffsetsImageSections(v.Name, rows, len(offsetData), len(valueData))
		} else {
			os, vs, err = NewRawUint32OffsetsListImageSections(v.Name, rows, len(offsetData), len(valueData))
		}
		if err != nil {
			return RawOrdinalColumnPartImageResult{}, err
		}
		os.Granules = g
		os.Blocks = g
		vs.Granules = g
		vs.Blocks = g
		b.appendSection(os, offsetData)
		b.appendSection(vs, valueData)
	}
	if err = b.addLayoutContractSection(); err != nil {
		return RawOrdinalColumnPartImageResult{}, err
	}
	sections, manifest, err := b.layoutManifestAndSections()
	if err != nil {
		return RawOrdinalColumnPartImageResult{}, err
	}
	imageBytes := len(manifest)
	if len(sections) > 0 {
		last := sections[len(sections)-1]
		imageBytes = last.Offset + last.Length
	}
	if imageBytes > plan.ImageBytesUpperBound {
		return RawOrdinalColumnPartImageResult{}, errRawOrdinalImageCapacity
	}
	out := make([]byte, imageBytes)
	copy(out, manifest)
	for _, section := range b.sections {
		copy(out[section.section.Offset:], section.data)
	}
	image := ColumnPartImage{Version: columnPartImageVersion, PartID: partID, Rows: rows, ManifestBytes: len(manifest), Sections: sections, Bytes: out}
	if _, err = CertifyColumnPartLayoutContractFromImage(image); err != nil {
		return RawOrdinalColumnPartImageResult{}, fmt.Errorf("typedcolumn: writer-certified raw ordinal layout: %w", err)
	}
	accounting := columnPartAccountingWithImage(part.byteAccounting(true), image)
	return RawOrdinalColumnPartImageResult{Image: image, Accounting: accounting, Charge: plan}, nil
}

func rawOrdinalGranule(rows, rawBytes int, encoding Encoding, payload []byte, lo, hi int64, has bool) EncodedGranule {
	selection, _ := admitCompressionInto(nil, payload, encoding, CompressionNone)
	g := newEncodedGranule(rows, lo, hi, has, encoding, selection)
	// Offset-column blocks carry metadata only: their canonical combined bytes
	// are written once as the existing split global sections. No fake byte view.
	g.RawBytes = rawBytes
	g.StoredBytes = rawBytes
	g.PayloadRef.Length = rawBytes
	g.CodecReport.RawBytes = rawBytes
	g.CodecReport.StoredBytes = rawBytes
	return g
}
