package collections

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/columnlayout"
	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typedkernel"
)

const maxTypedColumnPreparedCompressedDictionarySectionRawBytes = 256 << 20

const (
	maxTypedColumnPreparedMetadataBatchGapBytes      = 8 << 10
	maxTypedColumnPreparedMetadataBatchOverreadBytes = 8 << 10
	maxTypedColumnPreparedMetadataBatchSpanBytes     = 64 << 10
	maxTypedColumnPreparedMetadataBatchSections      = 6
	maxTypedColumnPreparedRangeCacheEntries          = 8
	maxTypedColumnPreparedRangeCacheEntryBytes       = maxTypedColumnPreparedMetadataBatchSpanBytes
)

// typedColumnPreparedRangeReader is the caller-owned byte access boundary used
// while preparing immutable typed-column state. It deliberately has no package
// global cache; prepared callers decide how long returned metadata and mapped
// resources stay alive.
type typedColumnPreparedRangeReader func(offset int, length int, section bool) ([]byte, error)

type typedColumnPreparePartStateOptions struct {
	CoalescePreparedMetadataReads bool
}

type typedColumnPreparedRangeReadCache struct {
	readRange typedColumnPreparedRangeReader
	entries   [maxTypedColumnPreparedRangeCacheEntries]typedColumnPreparedRangeReadCacheEntry
	entryN    int
}

type typedColumnPreparedRangeReadCacheEntry struct {
	offset int
	raw    []byte
}

func newTypedColumnPreparedRangeReadCache(readRange typedColumnPreparedRangeReader) *typedColumnPreparedRangeReadCache {
	return &typedColumnPreparedRangeReadCache{readRange: readRange}
}

func (c *typedColumnPreparedRangeReadCache) read(offset int, length int, section bool) ([]byte, error) {
	if c == nil || c.readRange == nil {
		return nil, errors.New("collections: typed-column prepared range cache missing reader")
	}
	if err := validateTypedColumnPreparedRange(offset, length); err != nil {
		return nil, err
	}
	for i := 0; i < c.entryN; i++ {
		entry := c.entries[i]
		if entry.raw == nil || offset < entry.offset {
			continue
		}
		start := offset - entry.offset
		if start > len(entry.raw) || length > len(entry.raw)-start {
			continue
		}
		return entry.raw[start : start+length], nil
	}
	return c.readAndStore(offset, length, section)
}

func (c *typedColumnPreparedRangeReadCache) readAndStore(offset int, length int, section bool) ([]byte, error) {
	if c == nil || c.readRange == nil {
		return nil, errors.New("collections: typed-column prepared range cache missing reader")
	}
	raw, err := c.readRange(offset, length, section)
	if err != nil {
		return nil, err
	}
	if len(raw) != length {
		return nil, fmt.Errorf("collections: typed-column prepared range offset=%d length=%d returned bytes=%d", offset, length, len(raw))
	}
	if length > maxTypedColumnPreparedRangeCacheEntryBytes || c.entryN >= len(c.entries) {
		return raw, nil
	}
	entry := typedColumnPreparedRangeReadCacheEntry{offset: offset, raw: raw}
	c.entries[c.entryN] = entry
	c.entryN++
	return raw, nil
}

func (c *typedColumnPreparedRangeReadCache) prefetchSections(sections []typedcolumn.ColumnPartImageSection) error {
	if c == nil || len(sections) <= 1 {
		return nil
	}
	var ordered [maxTypedColumnPreparedMetadataBatchSections]typedcolumn.ColumnPartImageSection
	if len(sections) > len(ordered) {
		return fmt.Errorf("collections: typed-column prepared metadata sections=%d exceed batch cap=%d", len(sections), len(ordered))
	}
	n := 0
	for _, section := range sections {
		if err := validateTypedColumnPreparedRange(section.Offset, section.Length); err != nil {
			return err
		}
		if section.Length > maxTypedColumnPreparedRangeCacheEntryBytes {
			continue
		}
		ordered[n] = section
		n++
	}
	if n <= 1 {
		return nil
	}
	sort.Slice(ordered[:n], func(i, j int) bool {
		if ordered[i].Offset != ordered[j].Offset {
			return ordered[i].Offset < ordered[j].Offset
		}
		return ordered[i].Length < ordered[j].Length
	})

	groupStart := ordered[0].Offset
	groupEnd := ordered[0].Offset + ordered[0].Length
	groupPayloadBytes := ordered[0].Length
	flush := func() error {
		_, err := c.read(groupStart, groupEnd-groupStart, true)
		return err
	}
	for _, section := range ordered[1:n] {
		sectionStart := section.Offset
		sectionEnd := section.Offset + section.Length
		if sectionStart < groupEnd {
			if sectionEnd > groupEnd {
				groupPayloadBytes += sectionEnd - groupEnd
				groupEnd = sectionEnd
			}
			continue
		}
		gap := sectionStart - groupEnd
		span := sectionEnd - groupStart
		overread := span - (groupPayloadBytes + section.Length)
		if gap <= maxTypedColumnPreparedMetadataBatchGapBytes &&
			overread <= maxTypedColumnPreparedMetadataBatchOverreadBytes &&
			span <= maxTypedColumnPreparedMetadataBatchSpanBytes {
			groupEnd = sectionEnd
			groupPayloadBytes += section.Length
			continue
		}
		if err := flush(); err != nil {
			return err
		}
		groupStart = sectionStart
		groupEnd = sectionEnd
		groupPayloadBytes = section.Length
	}
	return flush()
}

func validateTypedColumnPreparedRange(offset int, length int) error {
	if offset < 0 || length <= 0 {
		return fmt.Errorf("collections: typed-column prepared range offset=%d length=%d is invalid", offset, length)
	}
	if offset > maxCollectionInt-length {
		return fmt.Errorf("collections: typed-column prepared range offset=%d length=%d overflows int", offset, length)
	}
	return nil
}

// typedColumnPreparedColumnRequest describes one logical column role needed by a
// prepared operation. It separates collection semantics from typedcolumn
// physical definitions and from the concrete section dependencies selected by
// the operation.
type typedColumnPreparedColumnRequest struct {
	Field                    TypedStorageField
	Role                     typedcolumn.ColumnExecutionRole
	Operation                columnsemantics.Operation
	IncludeDictionaries      bool
	DictionaryValuesByCode   bool
	IncludeVisibility        bool
	IncludeStats             bool
	IncludePruning           bool
	IncludeSortKeyMetadata   bool
	IncludeSortKeyMarks      bool
	HasInt64PruningPredicate bool
	Int64PruningPredicate    typedcolumn.Int64PruningPredicate
	IncludeVectorPayload     bool
	IncludeAdjacencyPayload  bool
}

type typedColumnPreparedColumnPlan struct {
	Field            TypedStorageField
	Logical          columnsemantics.LogicalType
	Operation        columnsemantics.Operation
	Definition       typedcolumn.ColumnDefinition
	Capability       columnsemantics.Capability
	Layout           columnlayout.Capabilities
	LayoutCapability columnlayout.Capability
	Dependencies     []typedcolumn.SectionDependencyDescriptor
}

type typedColumnPreparedStateDiagnostics struct {
	PartsPrepared                  int
	ColumnsPrepared                int
	BlocksPrepared                 int
	CandidateBlocks                int
	PrunedBlocks                   int
	SectionDependencies            int
	CandidateRanges                int
	CandidateRangeBytes            uint64
	DecodedMetadataBytes           uint64
	ReadImageNanos                 int64
	StateBuildNanos                int64
	DictionaryNanos                int64
	PruningNanos                   int64
	SortKeyNanos                   int64
	StatsNanos                     int64
	ManifestBytes                  uint64
	DescriptorBytes                uint64
	ContractBytes                  uint64
	DirectViewCertified            int
	StreamingCertified             int
	StatsCertified                 int
	PruningCertified               int
	CertificationFailures          int
	CertificationFailureReason     string
	StatsValidationFailures        int
	StatsValidationFailureReason   string
	PruningBlocks                  int
	PruningRows                    int
	PruningFallbackBlocks          int
	PruningFallbackReason          string
	PruningValidationFailures      int
	PruningValidationFailureReason string
	Fallback                       bool
	FallbackReason                 string
}

type typedColumnPreparedBlockPlan struct {
	Index              int
	Descriptor         typedcolumn.ColumnBlockDescriptor
	Granule            typedcolumn.EncodedGranule
	PayloadOffset      int
	PayloadLength      int
	CandidateSelection typedcolumn.RowSelection
	NeedsPredicate     bool
	PruningExact       bool
	PruningExactCount  int64
	PruningExactSum    int64
}

type typedColumnPreparedColumnState struct {
	Plan                   typedColumnPreparedColumnPlan
	Column                 typedcolumn.ColumnPartColumn
	Section                typedcolumn.ColumnPartImageSection
	BlockPlans             []typedColumnPreparedBlockPlan
	Certification          typedcolumn.ColumnPartLayoutContractColumn
	AggregateReducer       typedkernel.PreparedReducer
	AggregateReducerReady  bool
	Int64Stats             typedcolumn.Int64ColumnStats
	Int64StatsReady        bool
	StatsFallbackReason    string
	PruningFallbackReason  string
	Int64PruningReady      bool
	Dictionaries           map[string]int64
	ReverseDictionaries    map[int64]string
	DictionaryValuesByCode []string
}

type typedColumnPreparedPartState struct {
	Ref             ColumnAssetRef
	PhysicalRef     ColumnAssetRef
	Image           typedcolumn.ColumnPartImage
	Descriptor      typedcolumn.ColumnPartDescriptor
	RowSpan         typedcolumn.RowSpan
	PhysicalColumns map[string]typedcolumn.ColumnPartColumn
	Columns         map[string]*typedColumnPreparedColumnState
	Marks           []typedcolumn.SortKeyMark
	Certification   typedcolumn.ColumnPartLayoutCertification
	Dependencies    []typedcolumn.SectionDependencyDescriptor
	ManifestBytes   int
	DescriptorBytes int
}

type typedColumnPreparedScanState struct {
	partsByRef  map[ColumnAssetRef]*typedColumnPreparedPartState
	diagnostics typedColumnPreparedStateDiagnostics
	closed      bool
}

func (s *typedColumnPreparedScanState) close() {
	if s == nil || s.closed {
		return
	}
	s.closed = true
	for ref, part := range s.partsByRef {
		if part != nil {
			part.close()
		}
		delete(s.partsByRef, ref)
	}
	s.partsByRef = nil
}

func (p *typedColumnPreparedPartState) close() {
	if p == nil {
		return
	}
	p.Ref = ColumnAssetRef{}
	p.PhysicalRef = ColumnAssetRef{}
	p.Image = typedcolumn.ColumnPartImage{}
	p.Descriptor = typedcolumn.ColumnPartDescriptor{}
	p.RowSpan = typedcolumn.RowSpan{}
	for name := range p.PhysicalColumns {
		delete(p.PhysicalColumns, name)
	}
	p.PhysicalColumns = nil
	p.Marks = nil
	p.Certification = typedcolumn.ColumnPartLayoutCertification{}
	for name, column := range p.Columns {
		if column != nil {
			column.close()
		}
		delete(p.Columns, name)
	}
	p.Columns = nil
	p.Dependencies = nil
	p.ManifestBytes = 0
	p.DescriptorBytes = 0
}

func (c *typedColumnPreparedColumnState) close() {
	if c == nil {
		return
	}
	c.Plan = typedColumnPreparedColumnPlan{}
	c.Column = typedcolumn.ColumnPartColumn{}
	c.Section = typedcolumn.ColumnPartImageSection{}
	c.BlockPlans = nil
	c.Certification = typedcolumn.ColumnPartLayoutContractColumn{}
	c.AggregateReducer = typedkernel.PreparedReducer{}
	c.AggregateReducerReady = false
	c.Int64Stats = typedcolumn.Int64ColumnStats{}
	c.Int64StatsReady = false
	c.StatsFallbackReason = ""
	c.PruningFallbackReason = ""
	c.Int64PruningReady = false
	c.Dictionaries = nil
	c.ReverseDictionaries = nil
	c.DictionaryValuesByCode = nil
}

func typedColumnPreparedStatePart(state *typedColumnPreparedScanState, ref ColumnAssetRef) (*typedColumnPreparedPartState, bool) {
	if state == nil || state.closed || state.partsByRef == nil {
		return nil, false
	}
	part, ok := state.partsByRef[ref]
	return part, ok
}

func typedColumnPreparedLogicalTypeForValueType(valueType ColumnStoreValueType) (columnsemantics.LogicalType, bool) {
	switch valueType {
	case ColumnStoreValueBool:
		return columnsemantics.LogicalBool, true
	case ColumnStoreValueInt64:
		return columnsemantics.LogicalInt64, true
	case ColumnStoreValueFloat32:
		return columnsemantics.LogicalFloat32, true
	case ColumnStoreValueDouble:
		return columnsemantics.LogicalDouble, true
	case ColumnStoreValueString:
		return columnsemantics.LogicalString, true
	case ColumnStoreValueInt8:
		return columnsemantics.LogicalInt8, true
	case ColumnStoreValueUint8:
		return columnsemantics.LogicalUint8, true
	case ColumnStoreValueInt16:
		return columnsemantics.LogicalInt16, true
	case ColumnStoreValueUint16:
		return columnsemantics.LogicalUint16, true
	case ColumnStoreValueInt32:
		return columnsemantics.LogicalInt32, true
	case ColumnStoreValueUint32:
		return columnsemantics.LogicalUint32, true
	case ColumnStoreValueUint64:
		return columnsemantics.LogicalUint64, true
	case ColumnStoreValueFloat16:
		return columnsemantics.LogicalFloat16, true
	case ColumnStoreValueBFloat16:
		return columnsemantics.LogicalBFloat16, true
	case ColumnStoreValueUint8Vector:
		return columnsemantics.LogicalUint8Vector, true
	case ColumnStoreValueInt8Vector:
		return columnsemantics.LogicalInt8Vector, true
	case ColumnStoreValueUint16Vector:
		return columnsemantics.LogicalUint16Vector, true
	case ColumnStoreValueInt16Vector:
		return columnsemantics.LogicalInt16Vector, true
	case ColumnStoreValueUint32Vector:
		return columnsemantics.LogicalUint32Vector, true
	case ColumnStoreValueInt32Vector:
		return columnsemantics.LogicalInt32Vector, true
	case ColumnStoreValueUint64Vector:
		return columnsemantics.LogicalUint64Vector, true
	case ColumnStoreValueInt64Vector:
		return columnsemantics.LogicalInt64Vector, true
	case ColumnStoreValueFloat16Vector:
		return columnsemantics.LogicalFloat16Vector, true
	case ColumnStoreValueBFloat16Vector:
		return columnsemantics.LogicalBFloat16Vector, true
	case ColumnStoreValueFloat32Vector:
		return columnsemantics.LogicalFloat32Vector, true
	case ColumnStoreValueFloat64Vector:
		return columnsemantics.LogicalFloat64Vector, true
	// uint32_list uses split offsets/value sections; prepared-state dependency
	// planning remains fail-closed until a prepared consumer owns that path.
	case ColumnStoreValueAdjacencyList:
		return columnsemantics.LogicalAdjacencyList, true
	default:
		return "", false
	}
}

func typedColumnDescribePreparedColumn(req typedColumnPreparedColumnRequest, span typedcolumn.RowSpan) (typedColumnPreparedColumnPlan, error) {
	adapterColumn, err := typedColumnAdapterMapField(req.Field)
	if err != nil {
		return typedColumnPreparedColumnPlan{}, err
	}
	return typedColumnDescribePreparedColumnWithDefinition(req, span, adapterColumn.Definition)
}

func typedColumnDescribePreparedColumnWithDefinition(req typedColumnPreparedColumnRequest, span typedcolumn.RowSpan, def typedcolumn.ColumnDefinition) (typedColumnPreparedColumnPlan, error) {
	logical, ok := typedColumnPreparedLogicalTypeForValueType(req.Field.ValueType)
	if !ok {
		capability := columnsemantics.Unsupported(req.Operation, columnsemantics.ReasonUnknownLogicalType, fmt.Sprintf("logical_type=%q", req.Field.ValueType))
		return typedColumnPreparedColumnPlan{Field: req.Field, Definition: def, Capability: capability}, nil
	}
	capability := columnsemantics.CapabilityFor(columnsemantics.Descriptor{
		Logical:  logical,
		Physical: def.Type,
		Encoding: def.Encoding,
		Nullable: req.Field.Nullable,
	}, req.Operation)
	layout := typedColumnLayoutCapabilitiesForAdapterColumn(typedColumnAdapterColumn{Field: req.Field, Definition: def})
	layoutCapability := layout.SupportsSemanticOperation(req.Operation)
	plan := typedColumnPreparedColumnPlan{
		Field:            req.Field,
		Logical:          logical,
		Operation:        req.Operation,
		Definition:       def,
		Capability:       capability,
		Layout:           layout,
		LayoutCapability: layoutCapability,
	}
	if !capability.Supported() {
		return plan, nil
	}
	if !layoutCapability.Supported() {
		return plan, nil
	}
	deps, err := typedColumnPreparedDependenciesForRequest(req, def, span)
	if err != nil {
		return typedColumnPreparedColumnPlan{}, err
	}
	plan.Dependencies = deps
	return plan, nil
}

func typedColumnPreparedDependenciesForRequest(req typedColumnPreparedColumnRequest, def typedcolumn.ColumnDefinition, span typedcolumn.RowSpan) ([]typedcolumn.SectionDependencyDescriptor, error) {
	deps := make([]typedcolumn.SectionDependencyDescriptor, 0, 8)
	// Vector and adjacency operations name their dense payload dependency
	// explicitly. Do not also add the generic scalar "values" dependency: callers
	// use these descriptors to plan narrow section reads, and vector/graph paths
	// must not look like scalar row-loop consumers.
	includeValues := !req.IncludeVectorPayload && !req.IncludeAdjacencyPayload
	if includeValues {
		values, err := typedcolumn.NewSectionDependency(req.Role, def.Name, def.Type, typedcolumn.SectionDependencyValues, typedcolumn.ColumnPartImageSectionColumnData, span, true)
		if err != nil {
			return nil, err
		}
		deps = append(deps, values)
	}
	if req.IncludePruning {
		dep, err := typedcolumn.NewSectionDependency(req.Role, def.Name, def.Type, typedcolumn.SectionDependencyPruningMetadata, typedcolumn.ColumnPartImageSectionPruningMetadata, span, false)
		if err != nil {
			return nil, err
		}
		deps = append(deps, dep)
	}
	if req.IncludeDictionaries {
		dep, err := typedcolumn.NewSectionDependency(req.Role, def.Name, def.Type, typedcolumn.SectionDependencyDictionaries, typedcolumn.ColumnPartImageSectionDictionaries, span, true)
		if err != nil {
			return nil, err
		}
		deps = append(deps, dep)
	}
	if req.Field.Nullable {
		for _, kind := range []typedcolumn.SectionDependencyKind{typedcolumn.SectionDependencyNullMask, typedcolumn.SectionDependencyDefaultMask} {
			dep, err := typedcolumn.NewSectionDependency(req.Role, def.Name, def.Type, kind, typedcolumn.ColumnPartImageSectionColumnData, span, true)
			if err != nil {
				return nil, err
			}
			deps = append(deps, dep)
		}
	}
	if req.IncludeVisibility {
		dep, err := typedcolumn.NewSectionDependency(typedcolumn.ColumnRoleVisibility, def.Name, def.Type, typedcolumn.SectionDependencyVisibility, typedcolumn.ColumnPartImageSectionColumnData, span, true)
		if err != nil {
			return nil, err
		}
		deps = append(deps, dep)
	}
	if req.IncludeStats {
		dep, err := typedcolumn.NewSectionDependency(req.Role, def.Name, def.Type, typedcolumn.SectionDependencyStats, typedcolumn.ColumnPartImageSectionColumnStats, span, false)
		if err != nil {
			return nil, err
		}
		deps = append(deps, dep)
	}
	if req.IncludeVectorPayload {
		dep, err := typedcolumn.NewSectionDependency(req.Role, def.Name, def.Type, typedcolumn.SectionDependencyVectorPayload, typedcolumn.ColumnPartImageSectionColumnData, span, true)
		if err != nil {
			return nil, err
		}
		deps = append(deps, dep)
	}
	if req.IncludeAdjacencyPayload {
		dep, err := typedcolumn.NewSectionDependency(req.Role, def.Name, def.Type, typedcolumn.SectionDependencyAdjacencyPayload, typedcolumn.ColumnPartImageSectionColumnData, span, true)
		if err != nil {
			return nil, err
		}
		deps = append(deps, dep)
	}
	if _, _, err := typedcolumn.ValidateSectionDependencies(deps); err != nil {
		return nil, err
	}
	return deps, nil
}

func typedColumnPreparedPrefetchMetadataSections(image typedcolumn.ColumnPartImage, requests []typedColumnPreparedColumnRequest, readCache *typedColumnPreparedRangeReadCache) error {
	if readCache == nil {
		return nil
	}
	var sections [maxTypedColumnPreparedMetadataBatchSections]typedcolumn.ColumnPartImageSection
	sectionN := 0
	appendSection := func(section typedcolumn.ColumnPartImageSection) error {
		if sectionN >= len(sections) {
			return fmt.Errorf("collections: typed-column prepared metadata prefetch section count exceeds %d", len(sections))
		}
		sections[sectionN] = section
		sectionN++
		return nil
	}
	descriptorSection, err := typedColumnAdapterImageSingleSection(image, typedcolumn.ColumnPartImageSectionDescriptor)
	if err != nil {
		return err
	}
	if err := appendSection(descriptorSection); err != nil {
		return err
	}
	if typedColumnPreparedRequestsIncludeSortKeyMetadata(requests) {
		metadataSection, err := typedColumnAdapterImageSingleSection(image, typedcolumn.ColumnPartImageSectionSortKeyMetadata)
		if err != nil {
			return err
		}
		if err := appendSection(metadataSection); err != nil {
			return err
		}
	}
	if typedColumnPreparedRequestsIncludeSortKeyMarks(requests) {
		marksSection, err := typedColumnAdapterImageSingleSection(image, typedcolumn.ColumnPartImageSectionSortKeyMarks)
		if err != nil {
			return err
		}
		if err := appendSection(marksSection); err != nil {
			return err
		}
	}
	if typedColumnPreparedRequestsIncludeStats(requests) {
		statsSection, ok, err := image.ColumnStatsSection()
		if err != nil {
			return err
		}
		if ok {
			if err := appendSection(statsSection); err != nil {
				return err
			}
		}
	}
	if typedColumnPreparedRequestsIncludePruning(requests) {
		pruningSection, ok, err := image.PruningMetadataSection()
		if err != nil {
			return err
		}
		if ok {
			if err := appendSection(pruningSection); err != nil {
				return err
			}
		}
	}
	if typedColumnPreparedRequestsIncludeDictionaries(requests) {
		dictionarySection, err := typedColumnAdapterImageSingleSection(image, typedcolumn.ColumnPartImageSectionDictionaries)
		if err != nil {
			return err
		}
		if dictionarySection.Length <= maxTypedColumnPreparedRangeCacheEntryBytes {
			if err := appendSection(dictionarySection); err != nil {
				return err
			}
		}
	}
	return readCache.prefetchSections(sections[:sectionN])
}

func typedColumnPreparedReadImageAndDescriptor(ref ColumnAssetRef, readRange typedColumnPreparedRangeReader) (typedcolumn.ColumnPartImage, typedcolumn.ColumnPartDescriptor, map[string]typedcolumn.ColumnPartColumn, int, []byte, []byte, error) {
	return typedColumnPreparedReadImageAndDescriptorWithPrefetch(ref, readRange, nil, nil)
}

func typedColumnPreparedReadImageAndDescriptorWithPrefetch(ref ColumnAssetRef, readRange typedColumnPreparedRangeReader, readCache *typedColumnPreparedRangeReadCache, requests []typedColumnPreparedColumnRequest) (typedcolumn.ColumnPartImage, typedcolumn.ColumnPartDescriptor, map[string]typedcolumn.ColumnPartColumn, int, []byte, []byte, error) {
	if readRange == nil {
		return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, nil, errors.New("collections: typed-column prepared state requires range reader")
	}
	if ref.Length > int64(maxCollectionInt) {
		return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, nil, fmt.Errorf("collections: typed-column part length=%d overflows int", ref.Length)
	}
	header, err := readRange(0, typedcolumn.ColumnPartImageManifestHeaderBytes, true)
	if err != nil {
		return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, nil, err
	}
	manifestBytes, err := typedcolumn.ColumnPartImageManifestLength(header)
	if err != nil {
		return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, nil, err
	}
	if manifestBytes > int(ref.Length) {
		return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, nil, fmt.Errorf("collections: typed-column part manifest bytes=%d exceed ref length=%d", manifestBytes, ref.Length)
	}
	manifest := make([]byte, manifestBytes)
	copy(manifest, header)
	if manifestBytes > len(header) {
		tail, err := readRange(len(header), manifestBytes-len(header), true)
		if err != nil {
			return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, nil, err
		}
		copy(manifest[len(header):], tail)
	}
	image, err := typedcolumn.ParseColumnPartImageManifest(manifest, int(ref.Length))
	if err != nil {
		return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, nil, err
	}
	if err := typedColumnPreparedPrefetchMetadataSections(image, requests, readCache); err != nil {
		return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, nil, err
	}
	descriptorSection, err := typedColumnAdapterImageSingleSection(image, typedcolumn.ColumnPartImageSectionDescriptor)
	if err != nil {
		return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, nil, err
	}
	descriptorRaw, err := readRange(descriptorSection.Offset, descriptorSection.Length, true)
	if err != nil {
		return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, nil, err
	}
	desc, columns, err := typedcolumn.DecodeColumnPartDescriptorSection(descriptorRaw)
	if err != nil {
		return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, nil, err
	}
	contractSection, err := image.LayoutContractSection()
	if err != nil {
		return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, nil, err
	}
	contractRaw, err := readRange(contractSection.Offset, contractSection.Length, true)
	if err != nil {
		return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, nil, err
	}
	return image, desc, columns, manifestBytes, descriptorRaw, contractRaw, nil
}

func typedColumnPreparePartStateFromRanges(ref ColumnAssetRef, physical ColumnAssetRef, typedRows int, physicalRows int, fields []TypedStorageField, schemaHash uint64, columnRequests []typedColumnPreparedColumnRequest, readRange typedColumnPreparedRangeReader, blockSelection func(typedcolumn.EncodedGranule, int) (typedcolumn.RowSelection, bool, error)) (*typedColumnPreparedPartState, typedColumnPreparedStateDiagnostics, error) {
	return typedColumnPreparePartStateFromRangesWithOptions(ref, physical, typedRows, physicalRows, fields, schemaHash, columnRequests, readRange, blockSelection, typedColumnPreparePartStateOptions{})
}

func typedColumnPreparePartStateFromRangesWithOptions(ref ColumnAssetRef, physical ColumnAssetRef, typedRows int, physicalRows int, fields []TypedStorageField, schemaHash uint64, columnRequests []typedColumnPreparedColumnRequest, readRange typedColumnPreparedRangeReader, blockSelection func(typedcolumn.EncodedGranule, int) (typedcolumn.RowSelection, bool, error), opts typedColumnPreparePartStateOptions) (*typedColumnPreparedPartState, typedColumnPreparedStateDiagnostics, error) {
	preparedReadRange := readRange
	var readCache *typedColumnPreparedRangeReadCache
	if opts.CoalescePreparedMetadataReads {
		readCache = newTypedColumnPreparedRangeReadCache(readRange)
		preparedReadRange = readCache.read
	}
	phaseStart := time.Now()
	image, desc, columns, manifestBytes, descriptorRaw, contractRaw, err := typedColumnPreparedReadImageAndDescriptorWithPrefetch(ref, preparedReadRange, readCache, columnRequests)
	readImageNanos := time.Since(phaseStart).Nanoseconds()
	if err != nil {
		return nil, typedColumnPreparedStateDiagnostics{ReadImageNanos: readImageNanos}, err
	}
	phaseStart = time.Now()
	part, diag, err := typedColumnPreparePartStateFromParsed(ref, physical, typedRows, physicalRows, fields, schemaHash, image, desc, columns, manifestBytes, descriptorRaw, contractRaw, columnRequests, blockSelection)
	diag.ReadImageNanos += readImageNanos
	diag.StateBuildNanos += time.Since(phaseStart).Nanoseconds()
	if err != nil || part == nil || diag.Fallback {
		return part, diag, err
	}
	if typedColumnPreparedRequestsIncludeDictionaries(columnRequests) {
		phaseStart = time.Now()
		dictDiag, err := typedColumnAttachPreparedDictionaries(part, image, preparedReadRange, columnRequests)
		dictDiag.DictionaryNanos += time.Since(phaseStart).Nanoseconds()
		typedColumnPreparedStateDiagnosticsAdd(&diag, dictDiag)
		if err != nil {
			return nil, diag, fmt.Errorf("collections: typed_column_part dictionary validation failed: %w", err)
		}
	}
	if typedColumnPreparedRequestsIncludePruning(columnRequests) {
		phaseStart = time.Now()
		pruningDiag, err := typedColumnAttachPreparedPruning(part, image, preparedReadRange, columnRequests)
		pruningDiag.PruningNanos += time.Since(phaseStart).Nanoseconds()
		typedColumnPreparedStateDiagnosticsAdd(&diag, pruningDiag)
		if err != nil {
			diag.PruningValidationFailures++
			diag.PruningValidationFailureReason = err.Error()
			return nil, diag, fmt.Errorf("collections: typed_column_part pruning validation failed: %w", err)
		}
	}
	if typedColumnPreparedRequestsIncludeSortKeyMetadata(columnRequests) || typedColumnPreparedRequestsIncludeSortKeyMarks(columnRequests) {
		phaseStart = time.Now()
		sortKeyDiag, err := typedColumnAttachPreparedSortKey(part, image, preparedReadRange, typedColumnPreparedRequestsIncludeSortKeyMarks(columnRequests))
		sortKeyDiag.SortKeyNanos += time.Since(phaseStart).Nanoseconds()
		typedColumnPreparedStateDiagnosticsAdd(&diag, sortKeyDiag)
		if err != nil {
			return nil, diag, fmt.Errorf("collections: typed_column_part sort-key validation failed: %w", err)
		}
	}
	if !typedColumnPreparedRequestsIncludeStats(columnRequests) {
		return part, diag, nil
	}
	phaseStart = time.Now()
	if err := typedColumnAttachPreparedStats(part, image, preparedReadRange); err != nil {
		diag.StatsNanos += time.Since(phaseStart).Nanoseconds()
		diag.StatsValidationFailures++
		diag.StatsValidationFailureReason = err.Error()
		return nil, diag, fmt.Errorf("collections: typed_column_part stats validation failed: %w", err)
	}
	diag.StatsNanos += time.Since(phaseStart).Nanoseconds()
	return part, diag, nil
}

func typedColumnPreparePartStateFromParsed(ref ColumnAssetRef, physical ColumnAssetRef, typedRows int, physicalRows int, _ []TypedStorageField, schemaHash uint64, image typedcolumn.ColumnPartImage, desc typedcolumn.ColumnPartDescriptor, columns map[string]typedcolumn.ColumnPartColumn, manifestBytes int, descriptorRaw []byte, contractRaw []byte, columnRequests []typedColumnPreparedColumnRequest, blockSelection func(typedcolumn.EncodedGranule, int) (typedcolumn.RowSelection, bool, error)) (*typedColumnPreparedPartState, typedColumnPreparedStateDiagnostics, error) {
	var diag typedColumnPreparedStateDiagnostics
	if image.PartID != ref.PartID || image.Rows != typedRows {
		return nil, diag, fmt.Errorf("collections: typed_column_part prepared image/ref mismatch image_part=%d ref_part=%d image_rows=%d typed_manifest_rows=%d", image.PartID, ref.PartID, image.Rows, typedRows)
	}
	if physical != (ColumnAssetRef{}) && image.Rows != physicalRows {
		return nil, diag, fmt.Errorf("collections: typed_column_part prepared image/physical row mismatch image_rows=%d physical_rows=%d", image.Rows, physicalRows)
	}
	if desc.PartID != image.PartID || desc.RowCount != image.Rows {
		return nil, diag, fmt.Errorf("collections: typed_column_part prepared descriptor/image mismatch descriptor_part=%d image_part=%d descriptor_rows=%d image_rows=%d", desc.PartID, image.PartID, desc.RowCount, image.Rows)
	}
	// The typed-column descriptor stores the schema identity in its uint32
	// SchemaVersion carrier; publication writes uint32(cfg.SchemaHash), so
	// prepared readers must compare against the same persisted representation.
	if desc.SchemaVersion != uint32(schemaHash) {
		return nil, diag, fmt.Errorf("collections: typed_column_part schema_version=%d want %d", desc.SchemaVersion, uint32(schemaHash))
	}
	if err := typedColumnPreparedValidateColumnDataSections(image, desc, columns); err != nil {
		return nil, diag, err
	}
	storedColumns, err := typedColumnPreparedColumnsWithSectionCompression(image, columns)
	if err != nil {
		return nil, diag, err
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContract(image, desc, storedColumns, descriptorRaw, contractRaw)
	if err != nil {
		diag.CertificationFailures++
		diag.CertificationFailureReason = err.Error()
		return nil, diag, fmt.Errorf("collections: typed_column_part layout certification failed: %w", err)
	}
	span, err := typedcolumn.NewRowSpan(0, desc.RowCount)
	if err != nil {
		return nil, diag, err
	}
	part := &typedColumnPreparedPartState{
		Ref:             ref,
		PhysicalRef:     physical,
		Image:           image,
		Descriptor:      desc,
		RowSpan:         span,
		PhysicalColumns: storedColumns,
		Columns:         make(map[string]*typedColumnPreparedColumnState, len(columnRequests)),
		Certification:   certification,
		ManifestBytes:   manifestBytes,
		DescriptorBytes: len(descriptorRaw),
	}
	diag.PartsPrepared = 1
	diag.ManifestBytes = uint64(manifestBytes)
	diag.DescriptorBytes = uint64(len(descriptorRaw))
	diag.ContractBytes = uint64(len(contractRaw))
	diag.DirectViewCertified = certification.DirectViewCertified
	diag.StreamingCertified = certification.StreamingCertified
	diag.StatsCertified = certification.StatsCertified
	diag.PruningCertified = certification.PruningCertified
	for _, request := range columnRequests {
		plan, err := typedColumnDescribePreparedColumn(request, span)
		if err != nil {
			return nil, diag, err
		}
		if !plan.Capability.Supported() {
			diag.Fallback = true
			diag.FallbackReason = plan.Capability.Error()
			return part, diag, nil
		}
		if !plan.LayoutCapability.Supported() {
			diag.Fallback = true
			diag.FallbackReason = "layout capability " + plan.LayoutCapability.Error()
			return part, diag, nil
		}
		if existing := part.Columns[plan.Definition.Name]; existing != nil {
			if request.Role == typedcolumn.ColumnRoleMeasure {
				// A column can be requested once as a predicate and again as a
				// measure. Keep the prepared reducer operation aligned with the
				// measure request while preserving the union of section
				// dependencies needed by both roles.
				existing.Plan.Operation = plan.Operation
				existing.Plan.Capability = plan.Capability
				existing.Plan.LayoutCapability = plan.LayoutCapability
			}
			existing.Plan.Dependencies = append(existing.Plan.Dependencies, plan.Dependencies...)
			part.Dependencies = append(part.Dependencies, plan.Dependencies...)
			diag.SectionDependencies += len(plan.Dependencies)
			continue
		}
		column, ok := storedColumns[plan.Definition.Name]
		if !ok {
			return nil, diag, fmt.Errorf("collections: typed-column prepared state missing column %q", plan.Definition.Name)
		}
		section, ok := typedColumnAdapterColumnDataSection(image, plan.Definition.Name)
		if !ok {
			return nil, diag, fmt.Errorf("collections: typed-column prepared state missing column data section %q", plan.Definition.Name)
		}
		storedDefinition, err := typedColumnPreparedStoredDefinitionForSection(plan.Field, column, section)
		if err != nil {
			return nil, diag, err
		}
		if storedDefinition.Encoding != plan.Definition.Encoding || storedDefinition.Compression != plan.Definition.Compression {
			plan, err = typedColumnDescribePreparedColumnWithDefinition(request, span, storedDefinition)
			if err != nil {
				return nil, diag, err
			}
			if !plan.Capability.Supported() {
				diag.Fallback = true
				diag.FallbackReason = plan.Capability.Error()
				return part, diag, nil
			}
			if !plan.LayoutCapability.Supported() {
				diag.Fallback = true
				diag.FallbackReason = "layout capability " + plan.LayoutCapability.Error()
				return part, diag, nil
			}
		}
		if err := typedColumnPreparedValidateColumnDefinition(plan.Field, plan.Definition, storedDefinition); err != nil {
			return nil, diag, err
		}
		validationColumn := column
		validationColumn.Definition = storedDefinition
		if err := validateTypedColumnProductionPartColumnLayout(plan.Field, validationColumn); err != nil {
			return nil, diag, fmt.Errorf("collections: typed-column prepared state column %q layout validation failed: %w", plan.Definition.Name, err)
		}
		columnCertification, _ := certification.Column(plan.Definition.Name)
		state, columnDiag, err := buildTypedColumnPreparedColumnState(plan, validationColumn, section, columnCertification, blockSelection)
		if err != nil {
			return nil, diag, err
		}
		part.Columns[plan.Definition.Name] = state
		part.Dependencies = append(part.Dependencies, plan.Dependencies...)
		diag.ColumnsPrepared++
		diag.BlocksPrepared += columnDiag.BlocksPrepared
		diag.CandidateBlocks += columnDiag.CandidateBlocks
		diag.PrunedBlocks += columnDiag.PrunedBlocks
		diag.CandidateRanges += columnDiag.CandidateRanges
		diag.CandidateRangeBytes += columnDiag.CandidateRangeBytes
		diag.SectionDependencies += len(plan.Dependencies)
	}
	if len(part.Dependencies) != 0 {
		if _, _, err := typedcolumn.ValidateSectionDependencies(part.Dependencies); err != nil {
			return nil, diag, err
		}
	}
	return part, diag, nil
}

func typedColumnPreparedValidateColumnDefinition(field TypedStorageField, want typedcolumn.ColumnDefinition, got typedcolumn.ColumnDefinition) error {
	return typedColumnAdapterValidateStoredDefinition(field, want, got, "typed-column prepared state column")
}

func typedColumnPreparedStoredDefinitionForSection(field TypedStorageField, column typedcolumn.ColumnPartColumn, section typedcolumn.ColumnPartImageSection) (typedcolumn.ColumnDefinition, error) {
	def := column.Definition
	if section.Encoding != def.Encoding {
		return typedcolumn.ColumnDefinition{}, fmt.Errorf("collections: typed-column prepared state column %q section encoding=%s want %s", def.Name, section.Encoding, def.Encoding)
	}
	if err := validateTypedColumnProductionCompression(section.Compression); err != nil {
		return typedcolumn.ColumnDefinition{}, fmt.Errorf("collections: typed-column prepared state column %q section compression=%s unsupported: %w", def.Name, section.Compression, err)
	}
	def.Compression = section.Compression
	if err := validateTypedColumnProductionDefinition(field, def); err != nil {
		return typedcolumn.ColumnDefinition{}, err
	}
	return def, nil
}

func typedColumnPreparedColumnsWithSectionCompression(image typedcolumn.ColumnPartImage, columns map[string]typedcolumn.ColumnPartColumn) (map[string]typedcolumn.ColumnPartColumn, error) {
	out := make(map[string]typedcolumn.ColumnPartColumn, len(columns))
	for name, column := range columns {
		out[name] = column
	}
	for _, section := range image.Sections {
		if section.Kind != typedcolumn.ColumnPartImageSectionColumnData {
			continue
		}
		column, ok := out[section.Column]
		if !ok {
			return nil, fmt.Errorf("collections: typed-column prepared state image unexpected column data section %q", section.Column)
		}
		if section.Encoding != column.Definition.Encoding {
			return nil, fmt.Errorf("collections: typed-column prepared state column %q section encoding=%s want %s", section.Column, section.Encoding, column.Definition.Encoding)
		}
		if err := validateTypedColumnProductionCompression(section.Compression); err != nil {
			return nil, fmt.Errorf("collections: typed-column prepared state column %q section compression=%s unsupported: %w", section.Column, section.Compression, err)
		}
		column.Definition.Compression = section.Compression
		out[section.Column] = column
	}
	return out, nil
}

func typedColumnPreparedGranuleLayout(plan typedColumnPreparedColumnPlan, granule typedcolumn.EncodedGranule) columnlayout.Capabilities {
	if granule.Encoding == plan.Definition.Encoding && granule.Compression == plan.Definition.Compression {
		return plan.Layout
	}
	def := plan.Definition
	def.Encoding = granule.Encoding
	def.Compression = granule.Compression
	return typedColumnLayoutCapabilitiesForAdapterColumn(typedColumnAdapterColumn{Field: plan.Field, Definition: def})
}

func buildTypedColumnPreparedColumnState(plan typedColumnPreparedColumnPlan, column typedcolumn.ColumnPartColumn, section typedcolumn.ColumnPartImageSection, certification typedcolumn.ColumnPartLayoutContractColumn, blockSelection func(typedcolumn.EncodedGranule, int) (typedcolumn.RowSelection, bool, error)) (*typedColumnPreparedColumnState, typedColumnPreparedStateDiagnostics, error) {
	state := &typedColumnPreparedColumnState{Plan: plan, Column: column, Section: section, BlockPlans: make([]typedColumnPreparedBlockPlan, 0, len(column.Blocks)), Certification: certification}
	var diag typedColumnPreparedStateDiagnostics
	offset := section.Offset
	sectionEnd := section.Offset + section.Length
	for i := range column.Blocks {
		block := column.Blocks[i]
		length := block.Descriptor.StoredBytes
		if plan.Layout.Descriptor.Logical != "" {
			if err := typedColumnPreparedGranuleLayout(plan, block.Granule).ValidateGranule(block.Granule); err != nil {
				return nil, diag, fmt.Errorf("collections: typed-column prepared state column %q block %d layout validation: %w", plan.Definition.Name, i, err)
			}
		}
		if length < 0 || offset > sectionEnd || length > sectionEnd-offset {
			return nil, diag, fmt.Errorf("collections: typed-column prepared state column %q block %d length=%d outside section", plan.Definition.Name, i, length)
		}
		if length == 0 && block.Descriptor.RowCount != 0 {
			return nil, diag, fmt.Errorf("collections: typed-column prepared state column %q block %d has zero-length payload with row_count=%d", plan.Definition.Name, i, block.Descriptor.RowCount)
		}
		selection := typedcolumn.RowSelection{}
		needsPredicate := true
		var err error
		if blockSelection != nil {
			selection, needsPredicate, err = blockSelection(block.Granule, block.Descriptor.RowCount)
			if err != nil {
				return nil, diag, err
			}
		} else {
			selection, err = typedcolumn.NewAllRowSelection(block.Descriptor.RowCount)
			if err != nil {
				return nil, diag, err
			}
		}
		state.BlockPlans = append(state.BlockPlans, typedColumnPreparedBlockPlan{
			Index:              i,
			Descriptor:         block.Descriptor,
			Granule:            block.Granule,
			PayloadOffset:      offset,
			PayloadLength:      length,
			CandidateSelection: selection,
			NeedsPredicate:     needsPredicate,
		})
		diag.BlocksPrepared++
		if selection.IsEmpty() {
			diag.PrunedBlocks++
		} else {
			diag.CandidateBlocks++
			diag.CandidateRanges++
			diag.CandidateRangeBytes += uint64(length)
		}
		offset += length
	}
	if offset != sectionEnd {
		return nil, diag, fmt.Errorf("collections: typed-column prepared state column %q consumed=%d section=%d", plan.Definition.Name, offset-section.Offset, section.Length)
	}
	return state, diag, nil
}

func typedColumnPreparedRequestsIncludeDictionaries(requests []typedColumnPreparedColumnRequest) bool {
	for _, request := range requests {
		if request.IncludeDictionaries {
			return true
		}
	}
	return false
}

func typedColumnPreparedRequestsIncludeStats(requests []typedColumnPreparedColumnRequest) bool {
	for _, request := range requests {
		if request.IncludeStats {
			return true
		}
	}
	return false
}

func typedColumnPreparedColumnWantsStats(column *typedColumnPreparedColumnState) bool {
	if column == nil {
		return false
	}
	for _, dep := range column.Plan.Dependencies {
		if dep.Kind == typedcolumn.SectionDependencyStats {
			return true
		}
	}
	return false
}

func typedColumnPreparedRequestsIncludePruning(requests []typedColumnPreparedColumnRequest) bool {
	for _, request := range requests {
		if request.IncludePruning {
			return true
		}
	}
	return false
}

func typedColumnPreparedColumnWantsPruning(column *typedColumnPreparedColumnState) bool {
	if column == nil {
		return false
	}
	for _, dep := range column.Plan.Dependencies {
		if dep.Kind == typedcolumn.SectionDependencyPruningMetadata {
			return true
		}
	}
	return false
}

func typedColumnPreparedRequestsIncludeSortKeyMetadata(requests []typedColumnPreparedColumnRequest) bool {
	for _, request := range requests {
		if request.IncludeSortKeyMetadata || request.IncludeSortKeyMarks {
			return true
		}
	}
	return false
}

func typedColumnPreparedRequestsIncludeSortKeyMarks(requests []typedColumnPreparedColumnRequest) bool {
	for _, request := range requests {
		if request.IncludeSortKeyMarks {
			return true
		}
	}
	return false
}

func typedColumnAttachPreparedSortKey(part *typedColumnPreparedPartState, image typedcolumn.ColumnPartImage, readRange typedColumnPreparedRangeReader, includeMarks bool) (typedColumnPreparedStateDiagnostics, error) {
	var diag typedColumnPreparedStateDiagnostics
	if part == nil {
		return diag, errors.New("collections: typed-column prepared sort-key missing part")
	}
	if readRange == nil {
		return diag, errors.New("collections: typed-column prepared sort-key requires range reader")
	}
	metadataSection, err := typedColumnAdapterImageSingleSection(image, typedcolumn.ColumnPartImageSectionSortKeyMetadata)
	if err != nil {
		return diag, err
	}
	metadataRaw, err := readRange(metadataSection.Offset, metadataSection.Length, true)
	if err != nil {
		return diag, err
	}
	sortKey, err := typedcolumn.DecodeColumnPartSortKeyMetadataSectionPayload(metadataRaw)
	if err != nil {
		return diag, err
	}
	diag.DecodedMetadataBytes += uint64(len(metadataRaw))
	part.Descriptor.SortKey = sortKey
	if !includeMarks {
		return diag, nil
	}
	marksSection, err := typedColumnAdapterImageSingleSection(image, typedcolumn.ColumnPartImageSectionSortKeyMarks)
	if err != nil {
		return diag, err
	}
	marksRaw, err := readRange(marksSection.Offset, marksSection.Length, true)
	if err != nil {
		return diag, err
	}
	marks, err := typedcolumn.DecodeColumnPartSortKeyMarksSectionPayload(marksRaw)
	if err != nil {
		return diag, err
	}
	if err := typedcolumn.ValidateColumnPartSortKeyMarks(part.Descriptor, marks); err != nil {
		return diag, err
	}
	diag.DecodedMetadataBytes += uint64(len(marksRaw))
	part.Marks = append(part.Marks[:0], marks...)
	return diag, nil
}

func typedColumnAttachPreparedPruning(part *typedColumnPreparedPartState, image typedcolumn.ColumnPartImage, readRange typedColumnPreparedRangeReader, requests []typedColumnPreparedColumnRequest) (typedColumnPreparedStateDiagnostics, error) {
	var diag typedColumnPreparedStateDiagnostics
	if part == nil {
		return diag, errors.New("collections: typed-column prepared pruning missing part")
	}
	section, ok, err := image.PruningMetadataSection()
	if err != nil {
		return diag, err
	}
	if !ok {
		for _, column := range part.Columns {
			if typedColumnPreparedColumnWantsPruning(column) {
				typedColumnPreparedPruningFallback(column, &diag, typedcolumn.ColumnPruningReasonMissingMetadata)
			}
		}
		return diag, nil
	}
	if readRange == nil {
		return diag, errors.New("collections: typed-column prepared pruning requires range reader")
	}
	raw, err := readRange(section.Offset, section.Length, true)
	if err != nil {
		return diag, err
	}
	diag.DecodedMetadataBytes += uint64(len(raw))
	pruning, err := typedcolumn.DecodeColumnPartPruningImageSection(section, raw)
	if err != nil {
		return diag, err
	}
	if err := typedcolumn.ValidateColumnPartPruning(pruning, part.Descriptor, part.PhysicalColumns); err != nil {
		return diag, err
	}
	for _, request := range requests {
		if !request.IncludePruning || !request.HasInt64PruningPredicate {
			continue
		}
		if err := typedColumnApplyPreparedInt64Pruning(part, request, pruning, &diag); err != nil {
			return diag, err
		}
	}
	return diag, nil
}

func typedColumnApplyPreparedInt64Pruning(part *typedColumnPreparedPartState, request typedColumnPreparedColumnRequest, pruning typedcolumn.ColumnPartPruning, diag *typedColumnPreparedStateDiagnostics) error {
	adapterColumn, err := typedColumnAdapterMapField(request.Field)
	if err != nil {
		return err
	}
	column := part.Columns[adapterColumn.Definition.Name]
	if column == nil {
		return fmt.Errorf("collections: typed-column prepared pruning missing column %q", adapterColumn.Definition.Name)
	}
	semOp := typedcolumn.ColumnPruningOpOrderedRange
	if request.Int64PruningPredicate.Kind == typedcolumn.Int64PruningPredicateEqual {
		semOp = typedcolumn.ColumnPruningOpEquality
	}
	semDesc := columnsemantics.Descriptor{Logical: column.Plan.Logical, Physical: column.Plan.Definition.Type, Encoding: column.Plan.Definition.Encoding, Nullable: column.Plan.Field.Nullable, DictionaryOrder: column.Plan.Layout.Descriptor.DictionaryOrder, DictionaryCollation: column.Plan.Layout.Descriptor.DictionaryCollation}
	semanticsOp := columnsemantics.OpPruneOrderedRange
	if semOp == typedcolumn.ColumnPruningOpEquality {
		semanticsOp = columnsemantics.OpPruneEquality
	}
	if cap := columnsemantics.CapabilityFor(semDesc, semanticsOp); !cap.Supported() {
		typedColumnPreparedPruningFallback(column, diag, cap.Error())
		return nil
	}
	if cap := column.Plan.Layout.SupportsSemanticOperation(semanticsOp); !cap.Supported() {
		typedColumnPreparedPruningFallback(column, diag, cap.Error())
		return nil
	}
	if !column.Certification.PruningCertified {
		typedColumnPreparedPruningFallback(column, diag, "layout_pruning_not_certified")
		return nil
	}
	index, ok := pruning.Int64Column(adapterColumn.Definition.Name)
	if !ok {
		typedColumnPreparedPruningFallback(column, diag, typedcolumn.ColumnPruningReasonMissingMetadata)
		return nil
	}
	plan, err := index.PlanInt64Predicate(request.Int64PruningPredicate)
	if err != nil {
		return err
	}
	if plan.Reason != "" && plan.Reason != typedcolumn.ColumnPruningReasonSupported {
		typedColumnPreparedPruningFallback(column, diag, plan.Reason)
		return nil
	}
	if len(plan.Blocks) != len(column.BlockPlans) {
		return fmt.Errorf("collections: typed-column prepared pruning column %q block candidates=%d want %d", adapterColumn.Definition.Name, len(plan.Blocks), len(column.BlockPlans))
	}
	candidates := make(map[int]typedcolumn.ColumnPruningBlockCandidate, len(plan.Blocks))
	for _, candidate := range plan.Blocks {
		candidates[candidate.BlockIndex] = candidate
	}
	var scratch typedcolumn.RowSelectionScratch
	for i := range column.BlockPlans {
		block := &column.BlockPlans[i]
		candidate, ok := candidates[block.Index]
		if !ok {
			return fmt.Errorf("collections: typed-column prepared pruning column %q missing block candidate %d", adapterColumn.Definition.Name, block.Index)
		}
		if candidate.FirstRow != block.Descriptor.FirstRow || candidate.RowCount != block.Descriptor.RowCount {
			return fmt.Errorf("collections: typed-column prepared pruning column %q block %d identity mismatch", adapterColumn.Definition.Name, block.Index)
		}
		oldNonEmpty := !block.CandidateSelection.IsEmpty()
		newSelection := candidate.Selection
		if oldNonEmpty && !block.CandidateSelection.IsAll() {
			composed, err := typedcolumn.ComposeRowSelectionsInto(block.Descriptor.RowCount, typedcolumn.RowSelectionComponents{Predicate: &newSelection, Visibility: &block.CandidateSelection}, &scratch)
			if err != nil {
				return err
			}
			newSelection, err = typedColumnPreparedCloneRowSelection(composed)
			if err != nil {
				return err
			}
		}
		if !oldNonEmpty {
			empty, err := typedcolumn.NewEmptyRowSelection(block.Descriptor.RowCount)
			if err != nil {
				return err
			}
			newSelection = empty
		}
		block.CandidateSelection = newSelection
		if request.Int64PruningPredicate.Kind != typedcolumn.Int64PruningPredicateAll && !newSelection.IsAll() {
			block.NeedsPredicate = true
		}
		if diag != nil && !newSelection.IsAll() {
			diag.PruningBlocks++
			diag.PruningRows += newSelection.Count()
		}
		block.PruningExact = candidate.Exact
		block.PruningExactCount = candidate.ExactCount
		block.PruningExactSum = candidate.ExactSum
	}
	column.PruningFallbackReason = ""
	column.Int64PruningReady = true
	return nil
}

func typedColumnPreparedCloneRowSelection(selection typedcolumn.RowSelection) (typedcolumn.RowSelection, error) {
	switch selection.Kind() {
	case typedcolumn.RowSelectionEmpty:
		return typedcolumn.NewEmptyRowSelection(selection.Rows())
	case typedcolumn.RowSelectionAll:
		return typedcolumn.NewAllRowSelection(selection.Rows())
	case typedcolumn.RowSelectionRange:
		start, end, ok := selection.SingleRange()
		if !ok {
			return typedcolumn.RowSelection{}, fmt.Errorf("collections: typed-column prepared selection range shape missing range")
		}
		return typedcolumn.NewRangeRowSelection(selection.Rows(), start, end)
	case typedcolumn.RowSelectionRanges:
		return typedcolumn.NewRangesRowSelection(selection.Rows(), selection.Ranges())
	case typedcolumn.RowSelectionBitmap:
		return typedcolumn.NewBitmapRowSelection(selection.Rows(), selection.BitmapWords())
	case typedcolumn.RowSelectionSparse:
		return typedcolumn.NewSparseRowSelection(selection.Rows(), selection.SparseRows())
	default:
		return typedcolumn.RowSelection{}, fmt.Errorf("collections: typed-column prepared unsupported row selection shape %s", selection.Shape().Kind)
	}
}

func typedColumnAttachPreparedDictionaries(part *typedColumnPreparedPartState, image typedcolumn.ColumnPartImage, readRange typedColumnPreparedRangeReader, requests []typedColumnPreparedColumnRequest) (typedColumnPreparedStateDiagnostics, error) {
	var diag typedColumnPreparedStateDiagnostics
	if part == nil {
		return diag, errors.New("collections: typed-column prepared dictionaries missing part")
	}
	section, err := typedColumnAdapterImageSingleSection(image, typedcolumn.ColumnPartImageSectionDictionaries)
	if err != nil {
		return diag, err
	}
	if readRange == nil {
		return diag, errors.New("collections: typed-column prepared dictionaries require range reader")
	}
	stored, err := readRange(section.Offset, section.Length, true)
	if err != nil {
		return diag, err
	}
	raw, err := decodeTypedColumnPreparedDictionarySectionBytes(section, stored)
	if err != nil {
		return diag, err
	}
	diag.DecodedMetadataBytes += uint64(len(raw))
	requestedModes, err := typedColumnPreparedDictionaryRequestModes(requests)
	if err != nil {
		return diag, err
	}
	decoded, err := decodeTypedColumnPreparedDictionariesForModes(section.Encoding, raw, requestedModes)
	if err != nil {
		return diag, err
	}
	type preparedDictionaryAttachRequest struct {
		adapterColumn typedColumnAdapterColumn
		column        *typedColumnPreparedColumnState
		mode          typedColumnPreparedDictionaryRequestMode
	}
	attachRequests := make([]preparedDictionaryAttachRequest, 0, len(requests))
	metadataColumns := make([]typedColumnAdapterColumn, 0, len(requests))
	for _, request := range requests {
		if !request.IncludeDictionaries {
			continue
		}
		adapterColumn, err := typedColumnAdapterMapField(request.Field)
		if err != nil {
			return diag, err
		}
		column := part.Columns[adapterColumn.Definition.Name]
		if column == nil {
			return diag, fmt.Errorf("collections: typed-column prepared dictionaries missing column %q", adapterColumn.Definition.Name)
		}
		metadataColumns = append(metadataColumns, adapterColumn)
		attachRequests = append(attachRequests, preparedDictionaryAttachRequest{
			adapterColumn: adapterColumn,
			column:        column,
			mode:          requestedModes[adapterColumn.Definition.Name],
		})
	}
	if err := validateTypedColumnAdapterMetadata(decoded.Forward, metadataColumns); err != nil {
		return diag, err
	}
	for _, attach := range attachRequests {
		adapterColumn := attach.adapterColumn
		column := attach.column
		mode := attach.mode
		if mode.Forward {
			dict, ok := decoded.Forward[adapterColumn.Definition.Name]
			if !ok {
				return diag, fmt.Errorf("collections: typed-column prepared dictionaries missing dictionary for column %q", adapterColumn.Definition.Name)
			}
			if err := validateTypedColumnPreparedDictionaryForColumn(adapterColumn.Definition.Name, column.Column.Definition.Cardinality, dict); err != nil {
				return diag, err
			}
			column.Dictionaries = dict
		}
		if mode.Reverse {
			reverse, ok := decoded.Reverse[adapterColumn.Definition.Name]
			if !ok {
				return diag, fmt.Errorf("collections: typed-column prepared dictionaries missing reverse dictionary for column %q", adapterColumn.Definition.Name)
			}
			if err := validateTypedColumnPreparedReverseDictionaryForColumn(adapterColumn.Definition.Name, column.Column.Definition.Cardinality, reverse); err != nil {
				return diag, err
			}
			column.ReverseDictionaries = reverse
		}
		if mode.ValuesByCode {
			valuesByCode, ok := decoded.ValuesByCode[adapterColumn.Definition.Name]
			if !ok {
				return diag, fmt.Errorf("collections: typed-column prepared dictionaries missing values-by-code dictionary for column %q", adapterColumn.Definition.Name)
			}
			if err := validateTypedColumnPreparedValuesByCodeDictionaryForColumn(adapterColumn.Definition.Name, column.Column.Definition.Cardinality, valuesByCode); err != nil {
				return diag, err
			}
			column.DictionaryValuesByCode = valuesByCode
		}
	}
	return diag, nil
}

type typedColumnPreparedDictionaryRequestMode struct {
	Forward      bool
	Reverse      bool
	ValuesByCode bool
}

func typedColumnPreparedDictionaryRequestNames(requests []typedColumnPreparedColumnRequest) (map[string]struct{}, error) {
	modes, err := typedColumnPreparedDictionaryRequestModes(requests)
	if err != nil {
		return nil, err
	}
	names := make(map[string]struct{}, len(modes))
	for name := range modes {
		names[name] = struct{}{}
	}
	return names, nil
}

func typedColumnPreparedDictionaryRequestModes(requests []typedColumnPreparedColumnRequest) (map[string]typedColumnPreparedDictionaryRequestMode, error) {
	names := make(map[string]struct{}, len(requests)+1)
	modes := make(map[string]typedColumnPreparedDictionaryRequestMode, len(requests)+1)
	for _, request := range requests {
		if !request.IncludeDictionaries {
			continue
		}
		adapterColumn, err := typedColumnAdapterMapField(request.Field)
		if err != nil {
			return nil, err
		}
		name := adapterColumn.Definition.Name
		mode := modes[name]
		if typedColumnPreparedDictionaryRequestNeedsForward(request) {
			mode.Forward = true
		}
		if request.DictionaryValuesByCode {
			mode.ValuesByCode = true
		}
		if typedColumnPreparedDictionaryRequestNeedsReverse(request) {
			mode.Reverse = true
		}
		modes[name] = mode
		names[name] = struct{}{}
	}
	if len(names) != 0 {
		modes[typedColumnAdapterMetadataDictionary] = typedColumnPreparedDictionaryRequestMode{Forward: true}
	}
	return modes, nil
}

func typedColumnPreparedDictionaryRequestNeedsForward(request typedColumnPreparedColumnRequest) bool {
	switch request.Role {
	case typedcolumn.ColumnRolePredicate:
		return true
	default:
		return false
	}
}

func typedColumnPreparedDictionaryRequestNeedsReverse(request typedColumnPreparedColumnRequest) bool {
	if request.DictionaryValuesByCode {
		return false
	}
	switch request.Role {
	case typedcolumn.ColumnRolePredicate:
		return false
	default:
		return true
	}
}

func decodeTypedColumnPreparedDictionarySectionBytes(section typedcolumn.ColumnPartImageSection, stored []byte) ([]byte, error) {
	rawBytes := section.RawBytes
	if rawBytes == 0 && section.Compression == typedcolumn.CompressionNone {
		rawBytes = len(stored)
	}
	switch section.Compression {
	case typedcolumn.CompressionNone:
		if len(stored) != rawBytes {
			return nil, fmt.Errorf("collections: typed-column prepared dictionaries section bytes=%d want raw bytes=%d", len(stored), rawBytes)
		}
		return stored, nil
	case typedcolumn.CompressionSnappy:
		if rawBytes <= 0 {
			return nil, fmt.Errorf("collections: typed-column prepared dictionaries compressed raw bytes=%d is invalid", rawBytes)
		}
		if rawBytes > maxTypedColumnPreparedCompressedDictionarySectionRawBytes {
			return nil, fmt.Errorf("collections: typed-column prepared dictionaries compressed raw bytes=%d exceeds max=%d", rawBytes, maxTypedColumnPreparedCompressedDictionarySectionRawBytes)
		}
		decodedLen, err := snappy.DecodedLen(stored)
		if err != nil {
			return nil, fmt.Errorf("collections: typed-column prepared dictionaries snappy decoded length: %w", err)
		}
		if decodedLen != rawBytes {
			return nil, fmt.Errorf("collections: typed-column prepared dictionaries snappy decoded length=%d want=%d", decodedLen, rawBytes)
		}
		out, err := snappy.Decode(make([]byte, decodedLen), stored)
		if err != nil {
			return nil, fmt.Errorf("collections: typed-column prepared dictionaries snappy decode: %w", err)
		}
		if len(out) != rawBytes {
			return nil, fmt.Errorf("collections: typed-column prepared dictionaries snappy decoded length=%d want=%d", len(out), rawBytes)
		}
		return out, nil
	case typedcolumn.CompressionLZ4:
		if rawBytes <= 0 {
			return nil, fmt.Errorf("collections: typed-column prepared dictionaries compressed raw bytes=%d is invalid", rawBytes)
		}
		if rawBytes > maxTypedColumnPreparedCompressedDictionarySectionRawBytes {
			return nil, fmt.Errorf("collections: typed-column prepared dictionaries compressed raw bytes=%d exceeds max=%d", rawBytes, maxTypedColumnPreparedCompressedDictionarySectionRawBytes)
		}
		out := make([]byte, rawBytes)
		n, err := lz4.UncompressBlock(stored, out)
		if err != nil {
			return nil, fmt.Errorf("collections: typed-column prepared dictionaries lz4 decode: %w", err)
		}
		if n != rawBytes {
			return nil, fmt.Errorf("collections: typed-column prepared dictionaries lz4 decoded length=%d want=%d", n, rawBytes)
		}
		return out, nil
	case typedcolumn.CompressionZSTD:
		if rawBytes <= 0 {
			return nil, fmt.Errorf("collections: typed-column prepared dictionaries compressed raw bytes=%d is invalid", rawBytes)
		}
		if rawBytes > maxTypedColumnPreparedCompressedDictionarySectionRawBytes {
			return nil, fmt.Errorf("collections: typed-column prepared dictionaries compressed raw bytes=%d exceeds max=%d", rawBytes, maxTypedColumnPreparedCompressedDictionarySectionRawBytes)
		}
		maxDecodedBytes := rawBytes
		if maxDecodedBytes < 1<<20 {
			maxDecodedBytes = 1 << 20
		}
		dec, err := zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderMaxMemory(uint64(maxDecodedBytes)),
			zstd.WithDecodeAllCapLimit(true),
		)
		if err != nil {
			return nil, fmt.Errorf("collections: typed-column prepared dictionaries zstd decoder: %w", err)
		}
		out, err := dec.DecodeAll(stored, make([]byte, 0, rawBytes))
		dec.Close()
		if err != nil {
			return nil, fmt.Errorf("collections: typed-column prepared dictionaries zstd decode: %w", err)
		}
		if len(out) != rawBytes {
			return nil, fmt.Errorf("collections: typed-column prepared dictionaries zstd decoded length=%d want=%d", len(out), rawBytes)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("collections: typed-column prepared dictionaries section compression=%s is unsupported", section.Compression)
	}
}

type typedColumnPreparedDictionaryDecoder struct {
	data []byte
	off  int
}

type typedColumnPreparedDecodedDictionaries struct {
	Forward      map[string]map[string]int64
	Reverse      map[string]map[int64]string
	ValuesByCode map[string][]string
}

func decodeTypedColumnPreparedDictionariesSection(raw []byte) (map[string]map[string]int64, error) {
	decoded, err := decodeTypedColumnPreparedRawDictionariesSection(raw, nil)
	if err != nil {
		return nil, err
	}
	return decoded.Forward, nil
}

func decodeTypedColumnPreparedDictionariesSectionForEncoding(encoding typedcolumn.Encoding, raw []byte, requested map[string]struct{}) (map[string]map[string]int64, error) {
	modes := typedColumnPreparedDictionaryModesFromNames(requested)
	decoded, err := decodeTypedColumnPreparedDictionariesForModes(encoding, raw, modes)
	if err != nil {
		return nil, err
	}
	return decoded.Forward, nil
}

func decodeTypedColumnPreparedDictionariesForModes(encoding typedcolumn.Encoding, raw []byte, modes map[string]typedColumnPreparedDictionaryRequestMode) (typedColumnPreparedDecodedDictionaries, error) {
	switch encoding {
	case 0:
		return decodeTypedColumnPreparedRawDictionariesSection(raw, modes)
	case typedcolumn.EncodingDictionaryDense:
		return decodeTypedColumnPreparedDenseDictionariesSection(raw, modes)
	default:
		return typedColumnPreparedDecodedDictionaries{}, fmt.Errorf("collections: typed-column prepared dictionaries encoding=%s is unsupported", encoding)
	}
}

func decodeTypedColumnPreparedRawDictionariesSection(raw []byte, modes map[string]typedColumnPreparedDictionaryRequestMode) (typedColumnPreparedDecodedDictionaries, error) {
	dec := typedColumnPreparedDictionaryDecoder{data: raw}
	count, err := dec.u32()
	if err != nil {
		return typedColumnPreparedDecodedDictionaries{}, err
	}
	if uint64(int(count)) != uint64(count) || int(count) > len(raw)/8+1 {
		return typedColumnPreparedDecodedDictionaries{}, fmt.Errorf("collections: typed-column prepared dictionaries count=%d exceeds section bytes=%d", count, len(raw))
	}
	out := typedColumnPreparedDecodedDictionaries{
		Forward:      make(map[string]map[string]int64, typedColumnPreparedDictionaryDecodeCapacity(int(count), modes, func(mode typedColumnPreparedDictionaryRequestMode) bool { return mode.Forward })),
		Reverse:      make(map[string]map[int64]string, typedColumnPreparedDictionaryDecodeCapacity(int(count), modes, func(mode typedColumnPreparedDictionaryRequestMode) bool { return mode.Reverse })),
		ValuesByCode: make(map[string][]string, typedColumnPreparedDictionaryDecodeCapacity(int(count), modes, func(mode typedColumnPreparedDictionaryRequestMode) bool { return mode.ValuesByCode })),
	}
	seenNames := make(map[string]struct{}, int(count))
	for i := 0; i < int(count); i++ {
		name, err := dec.str()
		if err != nil {
			return typedColumnPreparedDecodedDictionaries{}, err
		}
		if _, exists := seenNames[name]; exists {
			return typedColumnPreparedDecodedDictionaries{}, fmt.Errorf("collections: typed-column prepared duplicate dictionary %s", name)
		}
		seenNames[name] = struct{}{}
		entryCount, err := dec.u32()
		if err != nil {
			return typedColumnPreparedDecodedDictionaries{}, err
		}
		if uint64(int(entryCount)) != uint64(entryCount) || int(entryCount) > (len(raw)-dec.off)/12+1 {
			return typedColumnPreparedDecodedDictionaries{}, fmt.Errorf("collections: typed-column prepared dictionary %s entries=%d exceeds remaining bytes=%d", name, entryCount, len(raw)-dec.off)
		}
		mode := typedColumnPreparedDictionaryModeFor(modes, name)
		if !typedColumnPreparedDictionaryModeRequested(mode) {
			for j := 0; j < int(entryCount); j++ {
				if _, err := dec.i64(); err != nil {
					return typedColumnPreparedDecodedDictionaries{}, err
				}
				if err := dec.skipStr(); err != nil {
					return typedColumnPreparedDecodedDictionaries{}, err
				}
			}
			continue
		}
		var values map[string]int64
		if mode.Forward {
			values = make(map[string]int64, int(entryCount))
		}
		var codes map[int64]string
		if mode.Reverse || mode.Forward {
			codes = make(map[int64]string, int(entryCount))
		}
		var valuesByCode []string
		var valuesByCodeSeen []bool
		if mode.ValuesByCode {
			valuesByCode = make([]string, int(entryCount))
			valuesByCodeSeen = make([]bool, int(entryCount))
		}
		for j := 0; j < int(entryCount); j++ {
			code, err := dec.i64()
			if err != nil {
				return typedColumnPreparedDecodedDictionaries{}, err
			}
			value, err := dec.str()
			if err != nil {
				return typedColumnPreparedDecodedDictionaries{}, err
			}
			if mode.Forward {
				if _, exists := values[value]; exists {
					return typedColumnPreparedDecodedDictionaries{}, fmt.Errorf("collections: typed-column prepared duplicate dictionary value %s in %s", value, name)
				}
				values[value] = code
			}
			if codes != nil {
				if previous, exists := codes[code]; exists {
					return typedColumnPreparedDecodedDictionaries{}, fmt.Errorf("collections: typed-column prepared duplicate dictionary code %d in %s for %q and %q", code, name, previous, value)
				}
				codes[code] = value
			}
			if valuesByCode != nil {
				if code < 0 || uint64(code) >= uint64(len(valuesByCode)) {
					return typedColumnPreparedDecodedDictionaries{}, fmt.Errorf("collections: typed-column prepared dictionary code %d in %s outside cardinality=%d", code, name, len(valuesByCode))
				}
				codeIdx := int(code)
				if valuesByCodeSeen[codeIdx] {
					return typedColumnPreparedDecodedDictionaries{}, fmt.Errorf("collections: typed-column prepared duplicate dictionary code %d in %s", code, name)
				}
				valuesByCodeSeen[codeIdx] = true
				valuesByCode[codeIdx] = value
			}
		}
		if mode.Forward {
			out.Forward[name] = values
		}
		if mode.Reverse {
			out.Reverse[name] = codes
		}
		if mode.ValuesByCode {
			out.ValuesByCode[name] = valuesByCode
		}
	}
	if dec.off != len(raw) {
		return typedColumnPreparedDecodedDictionaries{}, fmt.Errorf("collections: typed-column prepared dictionaries trailing bytes=%d", len(raw)-dec.off)
	}
	return out, nil
}

func decodeTypedColumnPreparedDenseDictionariesSection(raw []byte, modes map[string]typedColumnPreparedDictionaryRequestMode) (typedColumnPreparedDecodedDictionaries, error) {
	dec := typedColumnPreparedDictionaryDecoder{data: raw}
	magic, err := dec.u32()
	if err != nil {
		return typedColumnPreparedDecodedDictionaries{}, err
	}
	if magic != 0x54434944 {
		return typedColumnPreparedDecodedDictionaries{}, fmt.Errorf("collections: typed-column prepared dense dictionaries invalid magic 0x%x", magic)
	}
	version, err := dec.u16()
	if err != nil {
		return typedColumnPreparedDecodedDictionaries{}, err
	}
	if version != 1 {
		return typedColumnPreparedDecodedDictionaries{}, fmt.Errorf("collections: typed-column prepared dense dictionaries unsupported version %d", version)
	}
	reserved, err := dec.u16()
	if err != nil {
		return typedColumnPreparedDecodedDictionaries{}, err
	}
	if reserved != 0 {
		return typedColumnPreparedDecodedDictionaries{}, fmt.Errorf("collections: typed-column prepared dense dictionaries reserved=%d want 0", reserved)
	}
	count, err := dec.u32()
	if err != nil {
		return typedColumnPreparedDecodedDictionaries{}, err
	}
	if uint64(int(count)) != uint64(count) || int(count) > len(raw)/8+1 {
		return typedColumnPreparedDecodedDictionaries{}, fmt.Errorf("collections: typed-column prepared dense dictionaries count=%d exceeds section bytes=%d", count, len(raw))
	}
	out := typedColumnPreparedDecodedDictionaries{
		Forward:      make(map[string]map[string]int64, typedColumnPreparedDictionaryDecodeCapacity(int(count), modes, func(mode typedColumnPreparedDictionaryRequestMode) bool { return mode.Forward })),
		Reverse:      make(map[string]map[int64]string, typedColumnPreparedDictionaryDecodeCapacity(int(count), modes, func(mode typedColumnPreparedDictionaryRequestMode) bool { return mode.Reverse })),
		ValuesByCode: make(map[string][]string, typedColumnPreparedDictionaryDecodeCapacity(int(count), modes, func(mode typedColumnPreparedDictionaryRequestMode) bool { return mode.ValuesByCode })),
	}
	seenNames := make(map[string]struct{}, int(count))
	for i := 0; i < int(count); i++ {
		name, err := dec.str()
		if err != nil {
			return typedColumnPreparedDecodedDictionaries{}, err
		}
		if _, exists := seenNames[name]; exists {
			return typedColumnPreparedDecodedDictionaries{}, fmt.Errorf("collections: typed-column prepared duplicate dictionary %s", name)
		}
		seenNames[name] = struct{}{}
		entryCount, err := dec.u32()
		if err != nil {
			return typedColumnPreparedDecodedDictionaries{}, err
		}
		if uint64(int(entryCount)) != uint64(entryCount) || int(entryCount) > (len(raw)-dec.off)/4+1 {
			return typedColumnPreparedDecodedDictionaries{}, fmt.Errorf("collections: typed-column prepared dense dictionary %s entries=%d exceeds remaining bytes=%d", name, entryCount, len(raw)-dec.off)
		}
		mode := typedColumnPreparedDictionaryModeFor(modes, name)
		if !typedColumnPreparedDictionaryModeRequested(mode) {
			for j := 0; j < int(entryCount); j++ {
				if err := dec.skipStr(); err != nil {
					return typedColumnPreparedDecodedDictionaries{}, err
				}
			}
			continue
		}
		var values map[string]int64
		if mode.Forward {
			values = make(map[string]int64, int(entryCount))
		}
		var codes map[int64]string
		if mode.Reverse {
			codes = make(map[int64]string, int(entryCount))
		}
		var valuesByCode []string
		if mode.ValuesByCode {
			valuesByCode = make([]string, int(entryCount))
		}
		for j := 0; j < int(entryCount); j++ {
			value, err := dec.str()
			if err != nil {
				return typedColumnPreparedDecodedDictionaries{}, err
			}
			if mode.Forward {
				if _, exists := values[value]; exists {
					return typedColumnPreparedDecodedDictionaries{}, fmt.Errorf("collections: typed-column prepared duplicate dictionary value %s in %s", value, name)
				}
				values[value] = int64(j)
			}
			if mode.Reverse {
				codes[int64(j)] = value
			}
			if valuesByCode != nil {
				valuesByCode[j] = value
			}
		}
		if mode.Forward {
			out.Forward[name] = values
		}
		if mode.Reverse {
			out.Reverse[name] = codes
		}
		if mode.ValuesByCode {
			out.ValuesByCode[name] = valuesByCode
		}
	}
	if dec.off != len(raw) {
		return typedColumnPreparedDecodedDictionaries{}, fmt.Errorf("collections: typed-column prepared dense dictionaries trailing bytes=%d", len(raw)-dec.off)
	}
	return out, nil
}

func typedColumnPreparedDictionaryModesFromNames(requested map[string]struct{}) map[string]typedColumnPreparedDictionaryRequestMode {
	if requested == nil {
		return nil
	}
	modes := make(map[string]typedColumnPreparedDictionaryRequestMode, len(requested))
	for name := range requested {
		modes[name] = typedColumnPreparedDictionaryRequestMode{Forward: true}
	}
	return modes
}

func typedColumnPreparedDictionaryModeFor(modes map[string]typedColumnPreparedDictionaryRequestMode, name string) typedColumnPreparedDictionaryRequestMode {
	if modes == nil {
		return typedColumnPreparedDictionaryRequestMode{Forward: true}
	}
	return modes[name]
}

func typedColumnPreparedDictionaryModeRequested(mode typedColumnPreparedDictionaryRequestMode) bool {
	return mode.Forward || mode.Reverse || mode.ValuesByCode
}

func typedColumnPreparedDictionaryDecodeCapacity(total int, modes map[string]typedColumnPreparedDictionaryRequestMode, include func(typedColumnPreparedDictionaryRequestMode) bool) int {
	if modes == nil {
		return total
	}
	n := 0
	for _, mode := range modes {
		if include(mode) {
			n++
		}
	}
	return min(total, n)
}

func (d *typedColumnPreparedDictionaryDecoder) u16() (uint16, error) {
	if len(d.data)-d.off < 2 {
		return 0, fmt.Errorf("collections: typed-column prepared dictionary truncated u16 at offset=%d", d.off)
	}
	v := binary.LittleEndian.Uint16(d.data[d.off:])
	d.off += 2
	return v, nil
}

func (d *typedColumnPreparedDictionaryDecoder) u32() (uint32, error) {
	if len(d.data)-d.off < 4 {
		return 0, fmt.Errorf("collections: typed-column prepared dictionary truncated u32 at offset=%d", d.off)
	}
	v := binary.LittleEndian.Uint32(d.data[d.off:])
	d.off += 4
	return v, nil
}

func (d *typedColumnPreparedDictionaryDecoder) i64() (int64, error) {
	if len(d.data)-d.off < 8 {
		return 0, fmt.Errorf("collections: typed-column prepared dictionary truncated i64 at offset=%d", d.off)
	}
	v := int64(binary.LittleEndian.Uint64(d.data[d.off:]))
	d.off += 8
	return v, nil
}

func (d *typedColumnPreparedDictionaryDecoder) str() (string, error) {
	n, err := d.u32()
	if err != nil {
		return "", err
	}
	if uint64(int(n)) != uint64(n) || int(n) > len(d.data)-d.off {
		return "", fmt.Errorf("collections: typed-column prepared dictionary string bytes=%d exceed remaining=%d", n, len(d.data)-d.off)
	}
	value := string(d.data[d.off : d.off+int(n)])
	d.off += int(n)
	return value, nil
}

func (d *typedColumnPreparedDictionaryDecoder) skipStr() error {
	n, err := d.u32()
	if err != nil {
		return err
	}
	if uint64(int(n)) != uint64(n) || int(n) > len(d.data)-d.off {
		return fmt.Errorf("collections: typed-column prepared dictionary string bytes=%d exceed remaining=%d", n, len(d.data)-d.off)
	}
	d.off += int(n)
	return nil
}

func validateTypedColumnPreparedDictionaryForColumn(name string, cardinality uint32, dict map[string]int64) error {
	if cardinality == 0 {
		return fmt.Errorf("collections: typed-column prepared dictionary %s has zero cardinality", name)
	}
	seen := make([]bool, int(cardinality))
	for value, code := range dict {
		if code < 0 || uint64(code) >= uint64(cardinality) {
			return fmt.Errorf("collections: typed-column prepared dictionary %s value %q code %d outside cardinality %d", name, value, code, cardinality)
		}
		seen[int(code)] = true
	}
	for code, ok := range seen {
		if !ok {
			return fmt.Errorf("collections: typed-column prepared missing dictionary code %d in %s", code, name)
		}
	}
	return nil
}

func validateTypedColumnPreparedReverseDictionaryForColumn(name string, cardinality uint32, dict map[int64]string) error {
	if cardinality == 0 {
		return fmt.Errorf("collections: typed-column prepared dictionary %s has zero cardinality", name)
	}
	if uint64(len(dict)) != uint64(cardinality) {
		return fmt.Errorf("collections: typed-column prepared reverse dictionary %s cardinality=%d want %d", name, len(dict), cardinality)
	}
	var previous string
	for code := int64(0); code < int64(cardinality); code++ {
		value, ok := dict[code]
		if !ok {
			return fmt.Errorf("collections: typed-column prepared missing reverse dictionary code %d in %s", code, name)
		}
		if code > 0 && previous >= value {
			return fmt.Errorf("collections: typed-column prepared reverse dictionary %s is not strictly ordered at code %d (%q >= %q)", name, code, previous, value)
		}
		previous = value
	}
	return nil
}

func validateTypedColumnPreparedValuesByCodeDictionaryForColumn(name string, cardinality uint32, valuesByCode []string) error {
	if uint64(len(valuesByCode)) != uint64(cardinality) {
		return fmt.Errorf("collections: typed-column prepared values-by-code dictionary %s cardinality=%d want %d", name, len(valuesByCode), cardinality)
	}
	var previous string
	for code, value := range valuesByCode {
		if code > 0 && previous >= value {
			return fmt.Errorf("collections: typed-column prepared values-by-code dictionary %s is not strictly ordered at code %d (%q >= %q)", name, code, previous, value)
		}
		previous = value
	}
	return nil
}

func typedColumnPreparedPruningFallback(column *typedColumnPreparedColumnState, diag *typedColumnPreparedStateDiagnostics, reason string) {
	if column != nil {
		column.PruningFallbackReason = reason
	}
	if diag != nil {
		blocks := 0
		if column != nil {
			blocks = len(column.BlockPlans)
		}
		diag.PruningFallbackBlocks += blocks
		diag.PruningFallbackReason = reason
	}
}

func typedColumnAttachPreparedStats(part *typedColumnPreparedPartState, image typedcolumn.ColumnPartImage, readRange typedColumnPreparedRangeReader) error {
	if part == nil {
		return errors.New("collections: typed-column prepared stats missing part")
	}
	section, ok, err := image.ColumnStatsSection()
	if err != nil {
		return err
	}
	if !ok {
		for _, column := range part.Columns {
			if typedColumnPreparedColumnWantsStats(column) {
				column.StatsFallbackReason = "missing_stats"
			}
		}
		return nil
	}
	if readRange == nil {
		return errors.New("collections: typed-column prepared stats requires range reader")
	}
	raw, err := readRange(section.Offset, section.Length, true)
	if err != nil {
		return err
	}
	stats, err := typedcolumn.DecodeColumnPartStatsSection(raw)
	if err != nil {
		return err
	}
	if err := typedcolumn.ValidateColumnPartStats(stats, part.Descriptor, part.PhysicalColumns); err != nil {
		return err
	}
	for name, column := range part.Columns {
		if !typedColumnPreparedColumnWantsStats(column) {
			continue
		}
		int64Stats, ok := stats.Int64Column(name)
		if !ok {
			column.StatsFallbackReason = "missing_stats"
			continue
		}
		semDesc := columnsemantics.Descriptor{
			Logical:             column.Plan.Logical,
			Physical:            column.Plan.Definition.Type,
			Encoding:            column.Plan.Definition.Encoding,
			Nullable:            column.Plan.Field.Nullable,
			DictionaryOrder:     column.Plan.Layout.Descriptor.DictionaryOrder,
			DictionaryCollation: column.Plan.Layout.Descriptor.DictionaryCollation,
		}
		if cap := columnsemantics.CapabilityFor(semDesc, columnsemantics.OpStatsSum); !cap.Supported() {
			column.StatsFallbackReason = cap.Error()
			continue
		}
		if cap := column.Plan.Layout.SupportsSemanticOperation(columnsemantics.OpStatsSum); !cap.Supported() {
			column.StatsFallbackReason = cap.Error()
			continue
		}
		if !column.Certification.StatsCertified {
			column.StatsFallbackReason = "layout_stats_not_certified"
			continue
		}
		column.Int64Stats = int64Stats
		column.Int64StatsReady = true
	}
	return nil
}

func typedColumnPreparedValidateColumnDataSections(image typedcolumn.ColumnPartImage, desc typedcolumn.ColumnPartDescriptor, columns map[string]typedcolumn.ColumnPartColumn) error {
	sectionsByColumn := make(map[string]typedcolumn.ColumnPartImageSection, len(columns))
	for _, section := range image.Sections {
		if section.Kind != typedcolumn.ColumnPartImageSectionColumnData {
			continue
		}
		column, ok := columns[section.Column]
		if !ok {
			return fmt.Errorf("collections: typed-column prepared state image unexpected column data section %q", section.Column)
		}
		if _, exists := sectionsByColumn[section.Column]; exists {
			return fmt.Errorf("collections: typed-column prepared state image duplicate column data section %q", section.Column)
		}
		if section.Encoding != column.Definition.Encoding {
			return fmt.Errorf("collections: typed-column prepared state image column %q section encoding=%s want %s", section.Column, section.Encoding, column.Definition.Encoding)
		}
		if err := validateTypedColumnProductionCompression(section.Compression); err != nil {
			return fmt.Errorf("collections: typed-column prepared state image column %q section compression=%s unsupported: %w", section.Column, section.Compression, err)
		}
		if section.Rows != 0 && section.Rows != desc.RowCount {
			return fmt.Errorf("collections: typed-column prepared state image column %q section rows=%d want %d", section.Column, section.Rows, desc.RowCount)
		}
		if section.Blocks != 0 && section.Blocks != len(column.Blocks) {
			return fmt.Errorf("collections: typed-column prepared state image column %q section blocks=%d want %d", section.Column, section.Blocks, len(column.Blocks))
		}
		expectedBytes := 0
		for i, block := range column.Blocks {
			if block.Descriptor.StoredBytes < 0 {
				return fmt.Errorf("collections: typed-column prepared state image column %q block %d stored_bytes=%d", section.Column, i, block.Descriptor.StoredBytes)
			}
			if expectedBytes > maxCollectionInt-block.Descriptor.StoredBytes {
				return fmt.Errorf("collections: typed-column prepared state image column %q stored bytes overflow", section.Column)
			}
			expectedBytes += block.Descriptor.StoredBytes
		}
		if err := validateTypedColumnProductionBlocks(section.Column, section.Encoding, section.Compression, column.Blocks); err != nil {
			return fmt.Errorf("collections: typed-column prepared state image column %q blocks validation failed: %w", section.Column, err)
		}
		if section.Length != expectedBytes {
			return fmt.Errorf("collections: typed-column prepared state image column %q section length=%d want %d", section.Column, section.Length, expectedBytes)
		}
		sectionsByColumn[section.Column] = section
	}
	for _, column := range desc.Columns {
		if _, ok := sectionsByColumn[column.Name]; !ok {
			return fmt.Errorf("collections: typed-column prepared state image missing column data section %q", column.Name)
		}
	}
	return nil
}

func typedColumnPreparedStateDiagnosticsAdd(dst *typedColumnPreparedStateDiagnostics, src typedColumnPreparedStateDiagnostics) {
	if dst == nil {
		return
	}
	dst.PartsPrepared += src.PartsPrepared
	dst.ColumnsPrepared += src.ColumnsPrepared
	dst.BlocksPrepared += src.BlocksPrepared
	dst.CandidateBlocks += src.CandidateBlocks
	dst.PrunedBlocks += src.PrunedBlocks
	dst.SectionDependencies += src.SectionDependencies
	dst.CandidateRanges += src.CandidateRanges
	dst.CandidateRangeBytes += src.CandidateRangeBytes
	dst.DecodedMetadataBytes += src.DecodedMetadataBytes
	dst.ReadImageNanos += src.ReadImageNanos
	dst.StateBuildNanos += src.StateBuildNanos
	dst.DictionaryNanos += src.DictionaryNanos
	dst.PruningNanos += src.PruningNanos
	dst.SortKeyNanos += src.SortKeyNanos
	dst.StatsNanos += src.StatsNanos
	dst.ManifestBytes += src.ManifestBytes
	dst.DescriptorBytes += src.DescriptorBytes
	dst.ContractBytes += src.ContractBytes
	dst.DirectViewCertified += src.DirectViewCertified
	dst.StreamingCertified += src.StreamingCertified
	dst.StatsCertified += src.StatsCertified
	dst.PruningCertified += src.PruningCertified
	dst.CertificationFailures += src.CertificationFailures
	if src.CertificationFailureReason != "" {
		dst.CertificationFailureReason = src.CertificationFailureReason
	}
	dst.StatsValidationFailures += src.StatsValidationFailures
	if src.StatsValidationFailureReason != "" {
		dst.StatsValidationFailureReason = src.StatsValidationFailureReason
	}
	dst.PruningBlocks += src.PruningBlocks
	dst.PruningRows += src.PruningRows
	dst.PruningFallbackBlocks += src.PruningFallbackBlocks
	if src.PruningFallbackReason != "" {
		dst.PruningFallbackReason = src.PruningFallbackReason
	}
	dst.PruningValidationFailures += src.PruningValidationFailures
	if src.PruningValidationFailureReason != "" {
		dst.PruningValidationFailureReason = src.PruningValidationFailureReason
	}
	if src.Fallback {
		dst.Fallback = true
		dst.FallbackReason = src.FallbackReason
	}
}
