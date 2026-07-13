package typedcolumn

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"slices"
	"sort"
	"sync"
	"time"
)

// Query-ready base generation images are rebuildable, non-authoritative
// derived assets. They deliberately do not participate in WAL, root
// publication, recovery selection, or garbage collection.
const (
	queryReadyBaseMagic            = uint32(0x47425251) // "QRBG", little-endian.
	queryReadyBaseVersion          = uint16(1)
	queryReadyBaseHeaderBytes      = 80
	queryReadyBasePartEntryBytes   = 80
	queryReadyBasePayloadAlignment = 4096
)

var queryReadyBaseCRCTable = crc32.MakeTable(crc32.Castagnoli)

// QueryReadyBaseIdentity binds a rebuildable image to the collection schema
// and snapshot generation from which its visible typed-column parts were
// selected.
type QueryReadyBaseIdentity struct {
	Generation uint64
	SchemaHash [sha256.Size]byte
}

// QueryReadyBasePartInput is one snapshot-visible typed-column part. Build
// sorts inputs by source generation and part ID, so caller iteration order is
// not encoded into the durable image.
type QueryReadyBasePartInput struct {
	SourceGeneration uint64
	Image            ColumnPartImage
}

// QueryReadyBaseDependency is the complete rebuild dependency for one embedded
// immutable typed-column image.
type QueryReadyBaseDependency struct {
	SourceGeneration uint64
	PartID           uint64
	Rows             int
	ImageBytes       int
	ImageChecksum    [sha256.Size]byte
}

type QueryReadyBaseBuildStats struct {
	Parts            int
	Rows             int64
	InputBytes       int64
	OutputBytes      int64
	BytesCopied      int64
	BytesHashed      int64
	BytesChecksummed int64
	ValidationTime   time.Duration
	BuildTime        time.Duration
}

type QueryReadyBaseBuildResult struct {
	Bytes        []byte
	Dependencies []QueryReadyBaseDependency
	Stats        QueryReadyBaseBuildStats
}

type QueryReadyBaseOpenStats struct {
	Parts                   int
	Rows                    int64
	BytesMapped             int64
	BytesRead               int64
	BytesDecoded            int64
	BytesCopied             int64
	BytesValidated          int64
	WholePartDecodes        int
	DictionaryConstructions int
	ValidationTime          time.Duration
	OpenTime                time.Duration
	Mapped                  bool
}

type QueryReadyBasePartView struct {
	Dependency QueryReadyBaseDependency
	Offset     int
	Image      ColumnPartImage
}

// QueryReadyBaseGeneration is a validated read-only view. For file opens, all
// embedded image byte slices point directly into the read-only mapping and are
// valid until Close.
type QueryReadyBaseGeneration struct {
	Identity     QueryReadyBaseIdentity
	Dependencies []QueryReadyBaseDependency
	Parts        []QueryReadyBasePartView
	Stats        QueryReadyBaseOpenStats

	data      []byte
	release   func() error
	closeOnce sync.Once
	closeErr  error
}

func (g *QueryReadyBaseGeneration) Bytes() []byte {
	if g == nil {
		return nil
	}
	return g.data
}

func (g *QueryReadyBaseGeneration) Close() error {
	if g == nil {
		return nil
	}
	g.closeOnce.Do(func() {
		if g.release != nil {
			g.closeErr = g.release()
		}
		g.data = nil
		for i := range g.Parts {
			g.Parts[i].Image.Bytes = nil
		}
	})
	return g.closeErr
}

type queryReadyBaseBuildPart struct {
	input      QueryReadyBasePartInput
	parsed     ColumnPartImage
	checksum   [sha256.Size]byte
	offset     int
	dependency QueryReadyBaseDependency
}

type queryReadyBaseBuildPlan struct {
	parts          []queryReadyBaseBuildPart
	payloadOffset  int
	totalBytes     int
	validationTime time.Duration
}

// BuildQueryReadyBaseGeneration validates and embeds an already
// snapshot-visible set of typed-column images. It is deterministic for the
// same identity and logical input set.
func BuildQueryReadyBaseGeneration(identity QueryReadyBaseIdentity, inputs []QueryReadyBasePartInput) (QueryReadyBaseBuildResult, error) {
	started := time.Now()
	plan, err := prepareQueryReadyBaseGeneration(identity, inputs)
	if err != nil {
		return QueryReadyBaseBuildResult{}, err
	}
	out := make([]byte, plan.totalBytes)
	result := encodeQueryReadyBaseGeneration(identity, plan, out)
	result.Stats.BuildTime = time.Since(started)
	return result, nil
}

// prepareQueryReadyBaseGeneration validates the complete logical input set and
// computes the exact deterministic layout without allocating the output image.
// Envelope builders use the plan to encode QRBG directly into their final
// buffer, avoiding a second whole-generation allocation and copy.
func prepareQueryReadyBaseGeneration(identity QueryReadyBaseIdentity, inputs []QueryReadyBasePartInput) (queryReadyBaseBuildPlan, error) {
	if err := validateQueryReadyBaseIdentity(identity); err != nil {
		return queryReadyBaseBuildPlan{}, err
	}
	parts := make([]queryReadyBaseBuildPart, len(inputs))
	var validationTime time.Duration
	for i, input := range inputs {
		if input.SourceGeneration == 0 {
			return queryReadyBaseBuildPlan{}, fmt.Errorf("typedcolumn: query-ready base part[%d] source generation is zero", i)
		}
		if input.SourceGeneration > identity.Generation {
			return queryReadyBaseBuildPlan{}, fmt.Errorf("typedcolumn: query-ready base part[%d] source generation=%d exceeds base generation=%d", i, input.SourceGeneration, identity.Generation)
		}
		validationStarted := time.Now()
		parsed, err := ParseColumnPartImage(input.Image.Bytes)
		if err != nil {
			return queryReadyBaseBuildPlan{}, fmt.Errorf("typedcolumn: query-ready base part[%d] validate image: %w", i, err)
		}
		if _, err := ColumnPartFromImageWithOptions(parsed, ColumnPartImageReadOptions{}); err != nil {
			return queryReadyBaseBuildPlan{}, fmt.Errorf("typedcolumn: query-ready base part[%d] validate structures: %w", i, err)
		}
		if _, err := CertifyColumnPartLayoutContractFromImage(parsed); err != nil {
			return queryReadyBaseBuildPlan{}, fmt.Errorf("typedcolumn: query-ready base part[%d] certify layout: %w", i, err)
		}
		validationTime += time.Since(validationStarted)
		parts[i] = queryReadyBaseBuildPart{
			input:    input,
			parsed:   parsed,
			checksum: sha256.Sum256(input.Image.Bytes),
		}
		parts[i].dependency = QueryReadyBaseDependency{
			SourceGeneration: input.SourceGeneration,
			PartID:           parsed.PartID,
			Rows:             parsed.Rows,
			ImageBytes:       len(input.Image.Bytes),
			ImageChecksum:    parts[i].checksum,
		}
	}
	sort.Slice(parts, func(i, j int) bool {
		if parts[i].input.SourceGeneration != parts[j].input.SourceGeneration {
			return parts[i].input.SourceGeneration < parts[j].input.SourceGeneration
		}
		return parts[i].parsed.PartID < parts[j].parsed.PartID
	})
	for i := 1; i < len(parts); i++ {
		if parts[i-1].input.SourceGeneration == parts[i].input.SourceGeneration && parts[i-1].parsed.PartID == parts[i].parsed.PartID {
			return queryReadyBaseBuildPlan{}, fmt.Errorf("typedcolumn: query-ready base duplicate dependency generation=%d part_id=%d", parts[i].input.SourceGeneration, parts[i].parsed.PartID)
		}
	}
	if len(parts) > math.MaxUint32 || len(parts) > (math.MaxInt-queryReadyBaseHeaderBytes)/queryReadyBasePartEntryBytes {
		return queryReadyBaseBuildPlan{}, fmt.Errorf("typedcolumn: query-ready base parts=%d exceed format bounds", len(parts))
	}
	tableBytes := len(parts) * queryReadyBasePartEntryBytes
	payloadOffset, err := queryReadyBaseAlign(queryReadyBaseHeaderBytes+tableBytes, queryReadyBasePayloadAlignment)
	if err != nil {
		return queryReadyBaseBuildPlan{}, err
	}
	totalBytes := payloadOffset
	for i := range parts {
		totalBytes, err = queryReadyBaseAlign(totalBytes, columnPartImageSectionAlignment)
		if err != nil {
			return queryReadyBaseBuildPlan{}, err
		}
		parts[i].offset = totalBytes
		if len(parts[i].input.Image.Bytes) > math.MaxInt-totalBytes {
			return queryReadyBaseBuildPlan{}, errors.New("typedcolumn: query-ready base image exceeds host size")
		}
		totalBytes += len(parts[i].input.Image.Bytes)
	}
	return queryReadyBaseBuildPlan{parts: parts, payloadOffset: payloadOffset, totalBytes: totalBytes, validationTime: validationTime}, nil
}

func encodeQueryReadyBaseGeneration(identity QueryReadyBaseIdentity, plan queryReadyBaseBuildPlan, out []byte) QueryReadyBaseBuildResult {
	parts := plan.parts
	payloadOffset := plan.payloadOffset
	totalBytes := plan.totalBytes
	if len(out) != totalBytes {
		panic("typedcolumn: internal query-ready base output length mismatch")
	}
	tableBytes := len(parts) * queryReadyBasePartEntryBytes
	binary.LittleEndian.PutUint32(out[0:4], queryReadyBaseMagic)
	binary.LittleEndian.PutUint16(out[4:6], queryReadyBaseVersion)
	binary.LittleEndian.PutUint64(out[8:16], identity.Generation)
	copy(out[16:48], identity.SchemaHash[:])
	binary.LittleEndian.PutUint32(out[48:52], uint32(len(parts)))
	binary.LittleEndian.PutUint64(out[64:72], uint64(payloadOffset))
	binary.LittleEndian.PutUint64(out[72:80], uint64(totalBytes))
	dependencies := make([]QueryReadyBaseDependency, len(parts))
	var rows, inputBytes int64
	for i := range parts {
		entry := out[queryReadyBaseHeaderBytes+i*queryReadyBasePartEntryBytes:]
		binary.LittleEndian.PutUint64(entry[0:8], parts[i].input.SourceGeneration)
		binary.LittleEndian.PutUint64(entry[8:16], parts[i].parsed.PartID)
		binary.LittleEndian.PutUint64(entry[16:24], uint64(parts[i].offset))
		binary.LittleEndian.PutUint64(entry[24:32], uint64(len(parts[i].input.Image.Bytes)))
		binary.LittleEndian.PutUint64(entry[32:40], uint64(parts[i].parsed.Rows))
		binary.LittleEndian.PutUint64(entry[40:48], uint64(parts[i].parsed.ManifestBytes))
		copy(entry[48:80], parts[i].checksum[:])
		copy(out[parts[i].offset:], parts[i].input.Image.Bytes)
		dependencies[i] = parts[i].dependency
		rows += int64(parts[i].parsed.Rows)
		inputBytes += int64(len(parts[i].input.Image.Bytes))
	}
	table := out[queryReadyBaseHeaderBytes : queryReadyBaseHeaderBytes+tableBytes]
	binary.LittleEndian.PutUint32(out[56:60], crc32.Checksum(table, queryReadyBaseCRCTable))
	binary.LittleEndian.PutUint32(out[52:56], queryReadyBaseHeaderChecksum(out[:queryReadyBaseHeaderBytes]))
	return QueryReadyBaseBuildResult{
		Bytes:        out,
		Dependencies: dependencies,
		Stats: QueryReadyBaseBuildStats{
			Parts:            len(parts),
			Rows:             rows,
			InputBytes:       inputBytes,
			OutputBytes:      int64(len(out)),
			BytesCopied:      inputBytes,
			BytesHashed:      inputBytes,
			BytesChecksummed: int64(tableBytes + queryReadyBaseHeaderBytes),
			ValidationTime:   plan.validationTime,
		},
	}
}

func OpenQueryReadyBaseGeneration(data []byte, expected QueryReadyBaseIdentity) (*QueryReadyBaseGeneration, error) {
	return openQueryReadyBaseGeneration(data, expected, false, nil)
}

func OpenQueryReadyBaseGenerationFile(path string, expected QueryReadyBaseIdentity) (*QueryReadyBaseGeneration, error) {
	return OpenQueryReadyBaseGenerationFileRange(path, 0, 0, expected)
}

// OpenQueryReadyBaseGenerationFileRange maps one exact QRBG image embedded in
// a segment file. Offset and length must either both be zero (whole file) or
// describe a non-empty in-bounds range. The mapping owner may include page
// alignment prefix bytes; Stats.BytesMapped reports that actual mapped span.
func OpenQueryReadyBaseGenerationFileRange(path string, offset, length int64, expected QueryReadyBaseIdentity) (*QueryReadyBaseGeneration, error) {
	started := time.Now()
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	logicalLength, err := queryReadyGenerationFileRange(file, offset, length)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("typedcolumn: query-ready base range %q: %w", path, err)
	}
	data, mapping, err := mmapQueryReadyBaseFileRange(file, offset, logicalLength)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("typedcolumn: mmap query-ready base %q offset=%d length=%d: %w", path, offset, logicalLength, err)
	}
	release := func() error {
		unmapErr := munmapQueryReadyBaseFile(mapping)
		closeErr := file.Close()
		return errors.Join(unmapErr, closeErr)
	}
	base, err := openQueryReadyBaseGeneration(data, expected, true, release)
	if err != nil {
		_ = release()
		return nil, err
	}
	base.Stats.OpenTime = time.Since(started)
	base.Stats.BytesMapped = int64(len(mapping))
	return base, nil
}

func queryReadyGenerationFileRange(file *os.File, offset, length int64) (int, error) {
	if file == nil || offset < 0 || length < 0 || (length == 0 && offset != 0) {
		return 0, os.ErrInvalid
	}
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	size := info.Size()
	if length == 0 {
		length = size
	}
	if length <= 0 || offset > size || length > size-offset || length > int64(math.MaxInt) {
		return 0, fmt.Errorf("offset=%d length=%d file_bytes=%d: %w", offset, length, size, os.ErrInvalid)
	}
	return int(length), nil
}

func openQueryReadyBaseGeneration(data []byte, expected QueryReadyBaseIdentity, mapped bool, release func() error) (*QueryReadyBaseGeneration, error) {
	started := time.Now()
	if err := validateQueryReadyBaseIdentity(expected); err != nil {
		return nil, err
	}
	if len(data) < queryReadyBaseHeaderBytes {
		return nil, fmt.Errorf("typedcolumn: query-ready base bytes=%d shorter than header=%d", len(data), queryReadyBaseHeaderBytes)
	}
	if got := binary.LittleEndian.Uint32(data[0:4]); got != queryReadyBaseMagic {
		return nil, fmt.Errorf("typedcolumn: invalid query-ready base magic 0x%x", got)
	}
	if got := binary.LittleEndian.Uint16(data[4:6]); got != queryReadyBaseVersion {
		return nil, fmt.Errorf("typedcolumn: unsupported query-ready base version %d", got)
	}
	if binary.LittleEndian.Uint16(data[6:8]) != 0 || binary.LittleEndian.Uint32(data[60:64]) != 0 {
		return nil, errors.New("typedcolumn: query-ready base reserved header bytes are nonzero")
	}
	if got, want := binary.LittleEndian.Uint32(data[52:56]), queryReadyBaseHeaderChecksum(data[:queryReadyBaseHeaderBytes]); got != want {
		return nil, fmt.Errorf("typedcolumn: query-ready base header checksum=%08x want %08x", got, want)
	}
	identity := QueryReadyBaseIdentity{Generation: binary.LittleEndian.Uint64(data[8:16])}
	copy(identity.SchemaHash[:], data[16:48])
	if identity.Generation != expected.Generation {
		return nil, fmt.Errorf("typedcolumn: query-ready base generation=%d want %d", identity.Generation, expected.Generation)
	}
	if identity.SchemaHash != expected.SchemaHash {
		return nil, fmt.Errorf("typedcolumn: query-ready base schema hash=%x want %x", identity.SchemaHash, expected.SchemaHash)
	}
	partCount := uint64(binary.LittleEndian.Uint32(data[48:52]))
	if partCount > uint64((math.MaxInt-queryReadyBaseHeaderBytes)/queryReadyBasePartEntryBytes) {
		return nil, fmt.Errorf("typedcolumn: query-ready base part count=%d exceeds host bounds", partCount)
	}
	tableEnd := queryReadyBaseHeaderBytes + int(partCount)*queryReadyBasePartEntryBytes
	if tableEnd > len(data) {
		return nil, fmt.Errorf("typedcolumn: query-ready base part table bytes=%d exceed image bytes=%d", tableEnd, len(data))
	}
	table := data[queryReadyBaseHeaderBytes:tableEnd]
	if got, want := crc32.Checksum(table, queryReadyBaseCRCTable), binary.LittleEndian.Uint32(data[56:60]); got != want {
		return nil, fmt.Errorf("typedcolumn: query-ready base table checksum=%08x want %08x", got, want)
	}
	payloadOffset64 := binary.LittleEndian.Uint64(data[64:72])
	totalBytes64 := binary.LittleEndian.Uint64(data[72:80])
	if totalBytes64 != uint64(len(data)) {
		return nil, fmt.Errorf("typedcolumn: query-ready base total bytes=%d want provided bytes=%d", totalBytes64, len(data))
	}
	if payloadOffset64 > uint64(len(data)) || payloadOffset64 < uint64(tableEnd) || payloadOffset64%queryReadyBasePayloadAlignment != 0 {
		return nil, fmt.Errorf("typedcolumn: query-ready base payload offset=%d invalid for table_end=%d total=%d", payloadOffset64, tableEnd, len(data))
	}
	if err := queryReadyBaseValidateZeroPadding(data[tableEnd:int(payloadOffset64)], "header"); err != nil {
		return nil, err
	}
	parts := make([]QueryReadyBasePartView, 0, int(partCount))
	dependencies := make([]QueryReadyBaseDependency, 0, int(partCount))
	stats := QueryReadyBaseOpenStats{
		Mapped:         mapped,
		BytesRead:      int64(len(data)),
		BytesDecoded:   queryReadyBaseHeaderBytes + int64(len(table)),
		BytesValidated: int64(len(data)),
	}
	if mapped {
		stats.BytesMapped = int64(len(data))
	}
	previousEnd := int(payloadOffset64)
	var previousSource, previousPart uint64
	for i := 0; i < int(partCount); i++ {
		entry := table[i*queryReadyBasePartEntryBytes : (i+1)*queryReadyBasePartEntryBytes]
		sourceGeneration := binary.LittleEndian.Uint64(entry[0:8])
		partID := binary.LittleEndian.Uint64(entry[8:16])
		offset64 := binary.LittleEndian.Uint64(entry[16:24])
		length64 := binary.LittleEndian.Uint64(entry[24:32])
		rows64 := binary.LittleEndian.Uint64(entry[32:40])
		manifest64 := binary.LittleEndian.Uint64(entry[40:48])
		if sourceGeneration == 0 || sourceGeneration > identity.Generation {
			return nil, fmt.Errorf("typedcolumn: query-ready base part[%d] source generation=%d invalid for base generation=%d", i, sourceGeneration, identity.Generation)
		}
		if i > 0 && (sourceGeneration < previousSource || (sourceGeneration == previousSource && partID <= previousPart)) {
			return nil, fmt.Errorf("typedcolumn: query-ready base part[%d] dependency order is not strictly increasing", i)
		}
		if offset64 > math.MaxInt || length64 > math.MaxInt || rows64 > math.MaxInt || manifest64 > math.MaxInt {
			return nil, fmt.Errorf("typedcolumn: query-ready base part[%d] fields exceed host bounds", i)
		}
		offset, length := int(offset64), int(length64)
		if offset < previousEnd || offset%columnPartImageSectionAlignment != 0 || length < ColumnPartImageManifestHeaderBytes || length > len(data)-offset {
			return nil, fmt.Errorf("typedcolumn: query-ready base part[%d] offset=%d length=%d invalid for previous_end=%d total=%d", i, offset, length, previousEnd, len(data))
		}
		if err := queryReadyBaseValidateZeroPadding(data[previousEnd:offset], fmt.Sprintf("before part[%d]", i)); err != nil {
			return nil, err
		}
		partBytes := data[offset : offset+length]
		var wantChecksum [sha256.Size]byte
		copy(wantChecksum[:], entry[48:80])
		gotChecksum := sha256.Sum256(partBytes)
		if gotChecksum != wantChecksum {
			return nil, fmt.Errorf("typedcolumn: query-ready base part[%d] checksum=%x want %x", i, gotChecksum, wantChecksum)
		}
		manifestLength, err := ColumnPartImageManifestLength(partBytes[:ColumnPartImageManifestHeaderBytes])
		if err != nil {
			return nil, fmt.Errorf("typedcolumn: query-ready base part[%d] manifest header: %w", i, err)
		}
		if manifestLength != int(manifest64) || manifestLength > len(partBytes) {
			return nil, fmt.Errorf("typedcolumn: query-ready base part[%d] manifest bytes=%d want %d", i, manifestLength, manifest64)
		}
		image, err := ParseColumnPartImageManifest(partBytes[:manifestLength], len(partBytes))
		if err != nil {
			return nil, fmt.Errorf("typedcolumn: query-ready base part[%d] manifest: %w", i, err)
		}
		image.Bytes = partBytes
		if image.PartID != partID || image.Rows != int(rows64) {
			return nil, fmt.Errorf("typedcolumn: query-ready base part[%d] identity part/rows=%d/%d want %d/%d", i, image.PartID, image.Rows, partID, rows64)
		}
		// Decode only bounded structural metadata. Encoded column payloads and
		// dictionaries remain mmap-backed and are not decoded or copied.
		if err := validateQueryReadyBasePartStructures(image); err != nil {
			return nil, fmt.Errorf("typedcolumn: query-ready base part[%d] validate structures: %w", i, err)
		}
		dependency := QueryReadyBaseDependency{SourceGeneration: sourceGeneration, PartID: partID, Rows: image.Rows, ImageBytes: len(partBytes), ImageChecksum: wantChecksum}
		dependencies = append(dependencies, dependency)
		parts = append(parts, QueryReadyBasePartView{Dependency: dependency, Offset: offset, Image: image})
		stats.Parts++
		stats.Rows += int64(image.Rows)
		stats.BytesDecoded += int64(manifestLength + queryReadyBaseStructuralBytes(image))
		previousSource, previousPart = sourceGeneration, partID
		previousEnd = offset + length
	}
	if previousEnd != len(data) {
		return nil, fmt.Errorf("typedcolumn: query-ready base has %d trailing bytes after final part", len(data)-previousEnd)
	}
	stats.ValidationTime = time.Since(started)
	stats.OpenTime = stats.ValidationTime
	return &QueryReadyBaseGeneration{Identity: identity, Dependencies: dependencies, Parts: parts, Stats: stats, data: data, release: release}, nil
}

// validateQueryReadyBasePartStructures validates the metadata and physical
// layout needed to publish a query-ready direct view. It deliberately does not
// attach block payloads: variable-width attachment decodes global offsets and
// re-encodes every block, turning open into an O(payload) allocation path.
func validateQueryReadyBasePartStructures(image ColumnPartImage) error {
	if image.TotalBytes() == 0 {
		return errors.New("typedcolumn: empty part image")
	}
	if err := image.validateForRead(); err != nil {
		return err
	}
	descriptorSection, err := image.singleSection(ColumnPartImageSectionDescriptor)
	if err != nil {
		return err
	}
	descriptorRaw := image.sectionBytes(descriptorSection)
	desc, columns, err := decodeColumnPartDescriptorSection(descriptorRaw)
	if err != nil {
		return err
	}
	if desc.PartID != image.PartID {
		return fmt.Errorf("typedcolumn: descriptor part id=%d manifest part id=%d", desc.PartID, image.PartID)
	}
	if desc.RowCount != image.Rows {
		return fmt.Errorf("typedcolumn: descriptor rows=%d manifest rows=%d", desc.RowCount, image.Rows)
	}
	sortKey, err := decodeSortKeyMetadataSection(image)
	if err != nil {
		return err
	}
	desc.SortKey = sortKey
	marks, err := decodeSortKeyMarksSection(image)
	if err != nil {
		return err
	}
	if err := validateDecodedSortKeyMarks(desc, marks); err != nil {
		return err
	}
	if err := validateColumnDataSectionsForColumns(image, columns); err != nil {
		return err
	}
	if err := restoreColumnDefinitionCompressionFromImageSections(image, columns); err != nil {
		return err
	}
	contractSection, err := image.LayoutContractSection()
	if err != nil {
		return err
	}
	if _, err := CertifyColumnPartLayoutContract(image, desc, columns, descriptorRaw, image.sectionBytes(contractSection)); err != nil {
		return fmt.Errorf("certify layout: %w", err)
	}
	for _, columnDesc := range desc.Columns {
		column, ok := columns[columnDesc.Name]
		if !ok {
			return fmt.Errorf("typedcolumn: descriptor column %s missing decoded column", columnDesc.Name)
		}
		switch column.Definition.Encoding {
		case EncodingRawBytesOffsets:
			offsetsSection, valuesSection, ok := image.columnOffsetsListSections(columnDesc.Name)
			if !ok {
				return fmt.Errorf("typedcolumn: image missing bytes sections %s", columnDesc.Name)
			}
			offsetsRaw := image.sectionBytes(offsetsSection)
			if err := ValidateRawBytesOffsetsSections(offsetsSection, valuesSection, offsetsRaw, image.sectionBytes(valuesSection), image.Rows); err != nil {
				return err
			}
			if err := validateQueryReadyBaseVariableWidthBlocks(columnDesc.Name, column, offsetsRaw, 1); err != nil {
				return err
			}
		case EncodingRawUint32OffsetsList:
			offsetsSection, valuesSection, ok := image.columnOffsetsListSections(columnDesc.Name)
			if !ok {
				return fmt.Errorf("typedcolumn: image missing offsets-list sections %s", columnDesc.Name)
			}
			offsetsRaw := image.sectionBytes(offsetsSection)
			if err := ValidateRawUint32OffsetsListSections(offsetsSection, valuesSection, offsetsRaw, image.sectionBytes(valuesSection), image.Rows); err != nil {
				return err
			}
			if err := validateQueryReadyBaseVariableWidthBlocks(columnDesc.Name, column, offsetsRaw, 4); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateQueryReadyBaseVariableWidthBlocks(name string, column ColumnPartColumn, offsetsRaw []byte, valueWidth int) error {
	for i, block := range column.Blocks {
		first := block.Descriptor.FirstRow
		last := first + block.Descriptor.RowCount
		begin := binary.LittleEndian.Uint64(offsetsRaw[first*8 : first*8+8])
		end := binary.LittleEndian.Uint64(offsetsRaw[last*8 : last*8+8])
		values := end - begin // Global offset validation already proved end >= begin.
		if values > uint64(math.MaxInt/valueWidth) {
			return fmt.Errorf("typedcolumn: image column %s block %d values=%d exceed host bounds", name, i, values)
		}
		offsetsBytes, err := checkedMulInt(block.Descriptor.RowCount+1, 8, "query-ready base block offsets bytes")
		if err != nil {
			return err
		}
		wantStored, err := checkedAddInt(offsetsBytes, int(values)*valueWidth, "query-ready base variable-width block bytes")
		if err != nil {
			return err
		}
		if block.Descriptor.StoredBytes != wantStored {
			return fmt.Errorf("typedcolumn: image column %s block %d stored bytes=%d want %d from global offsets", name, i, block.Descriptor.StoredBytes, wantStored)
		}
	}
	return nil
}

func queryReadyBaseStructuralBytes(image ColumnPartImage) int {
	total := 0
	for _, section := range image.Sections {
		switch section.Kind {
		case ColumnPartImageSectionDescriptor, ColumnPartImageSectionSortKeyMetadata, ColumnPartImageSectionSortKeyMarks, ColumnPartImageSectionLayoutContract:
			total += section.Length
		}
	}
	return total
}

func validateQueryReadyBaseIdentity(identity QueryReadyBaseIdentity) error {
	if identity.Generation == 0 {
		return errors.New("typedcolumn: query-ready base generation is zero")
	}
	if identity.SchemaHash == ([sha256.Size]byte{}) {
		return errors.New("typedcolumn: query-ready base schema hash is zero")
	}
	return nil
}

func queryReadyBaseHeaderChecksum(header []byte) uint32 {
	copyHeader := slices.Clone(header)
	for i := 52; i < 56; i++ {
		copyHeader[i] = 0
	}
	return crc32.Checksum(copyHeader, queryReadyBaseCRCTable)
}

func queryReadyBaseAlign(value, alignment int) (int, error) {
	if value < 0 || alignment <= 0 {
		return 0, errors.New("typedcolumn: invalid query-ready base alignment")
	}
	remainder := value % alignment
	if remainder == 0 {
		return value, nil
	}
	add := alignment - remainder
	if value > math.MaxInt-add {
		return 0, errors.New("typedcolumn: query-ready base aligned size exceeds host bounds")
	}
	return value + add, nil
}

func queryReadyBaseValidateZeroPadding(data []byte, label string) error {
	for i, value := range data {
		if value != 0 {
			return fmt.Errorf("typedcolumn: query-ready base %s padding byte[%d]=%d want 0", label, i, value)
		}
	}
	return nil
}
