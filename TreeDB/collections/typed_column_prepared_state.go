package collections

import (
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// typedColumnPreparedRangeReader is the caller-owned byte access boundary used
// while preparing immutable typed-column state. It deliberately has no package
// global cache; prepared callers decide how long returned metadata and mapped
// resources stay alive.
type typedColumnPreparedRangeReader func(offset int, length int, section bool) ([]byte, error)

// typedColumnPreparedColumnRequest describes one logical column role needed by a
// prepared operation. It separates collection semantics from typedcolumn
// physical definitions and from the concrete section dependencies selected by
// the operation.
type typedColumnPreparedColumnRequest struct {
	Field                   TypedStorageField
	Role                    typedcolumn.ColumnExecutionRole
	Operation               columnsemantics.Operation
	IncludeDictionaries     bool
	IncludeVisibility       bool
	IncludeStats            bool
	IncludePruning          bool
	IncludeVectorPayload    bool
	IncludeAdjacencyPayload bool
}

type typedColumnPreparedColumnPlan struct {
	Field        TypedStorageField
	Logical      columnsemantics.LogicalType
	Definition   typedcolumn.ColumnDefinition
	Capability   columnsemantics.Capability
	Dependencies []typedcolumn.SectionDependencyDescriptor
}

type typedColumnPreparedStateDiagnostics struct {
	PartsPrepared       int
	ColumnsPrepared     int
	BlocksPrepared      int
	CandidateBlocks     int
	PrunedBlocks        int
	SectionDependencies int
	CandidateRanges     int
	CandidateRangeBytes uint64
	ManifestBytes       uint64
	DescriptorBytes     uint64
	Fallback            bool
	FallbackReason      string
}

type typedColumnPreparedBlockPlan struct {
	Index              int
	Descriptor         typedcolumn.ColumnBlockDescriptor
	Granule            typedcolumn.EncodedGranule
	PayloadOffset      int
	PayloadLength      int
	CandidateSelection typedcolumn.RowSelection
	NeedsPredicate     bool
}

type typedColumnPreparedColumnState struct {
	Plan         typedColumnPreparedColumnPlan
	Column       typedcolumn.ColumnPartColumn
	Section      typedcolumn.ColumnPartImageSection
	BlockPlans   []typedColumnPreparedBlockPlan
	Dictionaries map[string]int64
}

type typedColumnPreparedPartState struct {
	Ref             ColumnAssetRef
	PhysicalRef     ColumnAssetRef
	Image           typedcolumn.ColumnPartImage
	Descriptor      typedcolumn.ColumnPartDescriptor
	RowSpan         typedcolumn.RowSpan
	PhysicalColumns map[string]typedcolumn.ColumnPartColumn
	Columns         map[string]*typedColumnPreparedColumnState
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
	c.Dictionaries = nil
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
	case ColumnStoreValueFloat32Vector:
		return columnsemantics.LogicalFloat32Vector, true
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
	logical, ok := typedColumnPreparedLogicalTypeForValueType(req.Field.ValueType)
	if !ok {
		capability := columnsemantics.Unsupported(req.Operation, columnsemantics.ReasonUnknownLogicalType, fmt.Sprintf("logical_type=%q", req.Field.ValueType))
		return typedColumnPreparedColumnPlan{Field: req.Field, Definition: adapterColumn.Definition, Capability: capability}, nil
	}
	capability := columnsemantics.CapabilityFor(columnsemantics.Descriptor{
		Logical:  logical,
		Physical: adapterColumn.Definition.Type,
		Encoding: adapterColumn.Definition.Encoding,
		Nullable: req.Field.Nullable,
	}, req.Operation)
	plan := typedColumnPreparedColumnPlan{
		Field:      req.Field,
		Logical:    logical,
		Definition: adapterColumn.Definition,
		Capability: capability,
	}
	if !capability.Supported() {
		return plan, nil
	}
	deps, err := typedColumnPreparedDependenciesForRequest(req, adapterColumn.Definition, span)
	if err != nil {
		return typedColumnPreparedColumnPlan{}, err
	}
	plan.Dependencies = deps
	return plan, nil
}

func typedColumnPreparedDependenciesForRequest(req typedColumnPreparedColumnRequest, def typedcolumn.ColumnDefinition, span typedcolumn.RowSpan) ([]typedcolumn.SectionDependencyDescriptor, error) {
	deps := make([]typedcolumn.SectionDependencyDescriptor, 0, 8)
	values, err := typedcolumn.NewSectionDependency(req.Role, def.Name, def.Type, typedcolumn.SectionDependencyValues, typedcolumn.ColumnPartImageSectionColumnData, span, true)
	if err != nil {
		return nil, err
	}
	deps = append(deps, values)
	if req.IncludePruning {
		dep, err := typedcolumn.NewSectionDependency(req.Role, def.Name, def.Type, typedcolumn.SectionDependencyPruningMetadata, typedcolumn.ColumnPartImageSectionColumnData, span, true)
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
		dep, err := typedcolumn.NewSectionDependency(req.Role, def.Name, def.Type, typedcolumn.SectionDependencyStats, typedcolumn.ColumnPartImageSectionAggregateMetadata, span, false)
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

func typedColumnPreparedReadImageAndDescriptor(ref ColumnAssetRef, readRange typedColumnPreparedRangeReader) (typedcolumn.ColumnPartImage, typedcolumn.ColumnPartDescriptor, map[string]typedcolumn.ColumnPartColumn, int, []byte, error) {
	if readRange == nil {
		return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, errors.New("collections: typed-column prepared state requires range reader")
	}
	if ref.Length > int64(maxCollectionInt) {
		return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, fmt.Errorf("collections: typed-column part length=%d overflows int", ref.Length)
	}
	header, err := readRange(0, typedcolumn.ColumnPartImageManifestHeaderBytes, true)
	if err != nil {
		return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, err
	}
	manifestBytes, err := typedcolumn.ColumnPartImageManifestLength(header)
	if err != nil {
		return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, err
	}
	if manifestBytes > int(ref.Length) {
		return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, fmt.Errorf("collections: typed-column part manifest bytes=%d exceed ref length=%d", manifestBytes, ref.Length)
	}
	manifest := make([]byte, manifestBytes)
	copy(manifest, header)
	if manifestBytes > len(header) {
		tail, err := readRange(len(header), manifestBytes-len(header), true)
		if err != nil {
			return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, err
		}
		copy(manifest[len(header):], tail)
	}
	image, err := typedcolumn.ParseColumnPartImageManifest(manifest, int(ref.Length))
	if err != nil {
		return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, err
	}
	descriptorSection, err := typedColumnAdapterImageSingleSection(image, typedcolumn.ColumnPartImageSectionDescriptor)
	if err != nil {
		return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, err
	}
	descriptorRaw, err := readRange(descriptorSection.Offset, descriptorSection.Length, true)
	if err != nil {
		return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, err
	}
	desc, columns, err := typedcolumn.DecodeColumnPartDescriptorSection(descriptorRaw)
	if err != nil {
		return typedcolumn.ColumnPartImage{}, typedcolumn.ColumnPartDescriptor{}, nil, 0, nil, err
	}
	return image, desc, columns, manifestBytes, descriptorRaw, nil
}

func typedColumnPreparePartStateFromRanges(ref ColumnAssetRef, physical ColumnAssetRef, typedRows int, physicalRows int, fields []TypedStorageField, schemaHash uint64, columnRequests []typedColumnPreparedColumnRequest, readRange typedColumnPreparedRangeReader, blockSelection func(typedcolumn.EncodedGranule, int) (typedcolumn.RowSelection, bool, error)) (*typedColumnPreparedPartState, typedColumnPreparedStateDiagnostics, error) {
	image, desc, columns, manifestBytes, descriptorRaw, err := typedColumnPreparedReadImageAndDescriptor(ref, readRange)
	if err != nil {
		return nil, typedColumnPreparedStateDiagnostics{}, err
	}
	return typedColumnPreparePartStateFromParsed(ref, physical, typedRows, physicalRows, fields, schemaHash, image, desc, columns, manifestBytes, len(descriptorRaw), columnRequests, blockSelection)
}

func typedColumnPreparePartStateFromParsed(ref ColumnAssetRef, physical ColumnAssetRef, typedRows int, physicalRows int, _ []TypedStorageField, schemaHash uint64, image typedcolumn.ColumnPartImage, desc typedcolumn.ColumnPartDescriptor, columns map[string]typedcolumn.ColumnPartColumn, manifestBytes int, descriptorBytes int, columnRequests []typedColumnPreparedColumnRequest, blockSelection func(typedcolumn.EncodedGranule, int) (typedcolumn.RowSelection, bool, error)) (*typedColumnPreparedPartState, typedColumnPreparedStateDiagnostics, error) {
	var diag typedColumnPreparedStateDiagnostics
	if image.PartID != ref.PartID || (typedRows != 0 && image.Rows != typedRows) {
		return nil, diag, fmt.Errorf("collections: typed_column_part prepared image/ref mismatch image_part=%d ref_part=%d image_rows=%d typed_manifest_rows=%d", image.PartID, ref.PartID, image.Rows, typedRows)
	}
	if physical != (ColumnAssetRef{}) && physicalRows != 0 && image.Rows != physicalRows {
		return nil, diag, fmt.Errorf("collections: typed_column_part prepared image/physical row mismatch image_rows=%d physical_rows=%d", image.Rows, physicalRows)
	}
	if desc.PartID != image.PartID || desc.RowCount != image.Rows {
		return nil, diag, fmt.Errorf("collections: typed_column_part prepared descriptor/image mismatch descriptor_part=%d image_part=%d descriptor_rows=%d image_rows=%d", desc.PartID, image.PartID, desc.RowCount, image.Rows)
	}
	if desc.SchemaVersion != uint32(schemaHash) {
		return nil, diag, fmt.Errorf("collections: typed_column_part schema_version=%d want %d", desc.SchemaVersion, uint32(schemaHash))
	}
	if err := typedColumnPreparedValidateColumnDataSections(image, desc, columns); err != nil {
		return nil, diag, err
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
		PhysicalColumns: columns,
		Columns:         make(map[string]*typedColumnPreparedColumnState, len(columnRequests)),
		ManifestBytes:   manifestBytes,
		DescriptorBytes: descriptorBytes,
	}
	diag.PartsPrepared = 1
	diag.ManifestBytes = uint64(manifestBytes)
	diag.DescriptorBytes = uint64(descriptorBytes)
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
		if existing := part.Columns[plan.Definition.Name]; existing != nil {
			existing.Plan.Dependencies = append(existing.Plan.Dependencies, plan.Dependencies...)
			part.Dependencies = append(part.Dependencies, plan.Dependencies...)
			diag.SectionDependencies += len(plan.Dependencies)
			continue
		}
		column, ok := columns[plan.Definition.Name]
		if !ok {
			return nil, diag, fmt.Errorf("collections: typed-column prepared state missing column %q", plan.Definition.Name)
		}
		if err := typedColumnPreparedValidateColumnDefinition(plan.Definition, column.Definition); err != nil {
			return nil, diag, err
		}
		section, ok := typedColumnAdapterColumnDataSection(image, plan.Definition.Name)
		if !ok {
			return nil, diag, fmt.Errorf("collections: typed-column prepared state missing column data section %q", plan.Definition.Name)
		}
		state, columnDiag, err := buildTypedColumnPreparedColumnState(plan, column, section, blockSelection)
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

func typedColumnPreparedValidateColumnDefinition(want typedcolumn.ColumnDefinition, got typedcolumn.ColumnDefinition) error {
	if got.Name != want.Name || got.Type != want.Type || got.Encoding != want.Encoding || got.Compression != want.Compression || got.FixedWidthElements != want.FixedWidthElements {
		return fmt.Errorf("collections: typed-column prepared state column %q schema mismatch: got type=%s encoding=%s compression=%s fixed_width_elements=%d want type=%s encoding=%s compression=%s fixed_width_elements=%d", want.Name, got.Type, got.Encoding, got.Compression, got.FixedWidthElements, want.Type, want.Encoding, want.Compression, want.FixedWidthElements)
	}
	return nil
}

func buildTypedColumnPreparedColumnState(plan typedColumnPreparedColumnPlan, column typedcolumn.ColumnPartColumn, section typedcolumn.ColumnPartImageSection, blockSelection func(typedcolumn.EncodedGranule, int) (typedcolumn.RowSelection, bool, error)) (*typedColumnPreparedColumnState, typedColumnPreparedStateDiagnostics, error) {
	state := &typedColumnPreparedColumnState{Plan: plan, Column: column, Section: section, BlockPlans: make([]typedColumnPreparedBlockPlan, 0, len(column.Blocks))}
	var diag typedColumnPreparedStateDiagnostics
	offset := section.Offset
	sectionEnd := section.Offset + section.Length
	for i := range column.Blocks {
		block := column.Blocks[i]
		length := block.Descriptor.StoredBytes
		if length <= 0 || offset > sectionEnd || length > sectionEnd-offset {
			return nil, diag, fmt.Errorf("collections: typed-column prepared state column %q block %d length=%d outside section", plan.Definition.Name, i, length)
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
		if section.Encoding != column.Definition.Encoding || section.Compression != column.Definition.Compression {
			return fmt.Errorf("collections: typed-column prepared state image column %q section encoding=%s compression=%s want encoding=%s compression=%s", section.Column, section.Encoding, section.Compression, column.Definition.Encoding, column.Definition.Compression)
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
	dst.ManifestBytes += src.ManifestBytes
	dst.DescriptorBytes += src.DescriptorBytes
	if src.Fallback {
		dst.Fallback = true
		dst.FallbackReason = src.FallbackReason
	}
}
