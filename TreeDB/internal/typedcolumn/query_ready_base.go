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
	queryReadyBaseVersion          = uint16(3)
	queryReadyBaseHeaderBytes      = 80
	queryReadyBasePartEntryBytes   = 144
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

// QueryReadyPrimaryIDMode records how a part's encoded row-locator primary IDs
// map into the logical identity domain shared by a base-plus-delta reader.
// Preserve retains the encoded IDs. DensePartLocal translates a validated
// zero-based dense part-local domain by PrimaryIDBase.
type QueryReadyPrimaryIDMode uint8

const (
	QueryReadyPrimaryIDPreserve QueryReadyPrimaryIDMode = iota
	QueryReadyPrimaryIDDensePartLocal
)

// QueryReadyBasePartInput is one snapshot-visible typed-column part. Build
// sorts inputs by source generation and part ID, so caller iteration order is
// not encoded into the durable image.
type QueryReadyBasePartInput struct {
	SourceGeneration uint64
	Image            ColumnPartImage
	PrimaryIDMode    QueryReadyPrimaryIDMode
	PrimaryIDBase    int64
}

// QueryReadyBaseDependency is the complete rebuild dependency for one embedded
// immutable typed-column image.
type QueryReadyBaseDependency struct {
	SourceGeneration uint64
	PartID           uint64
	Rows             int
	ImageBytes       int
	ImageChecksum    [sha256.Size]byte
	PrimaryIDMode    QueryReadyPrimaryIDMode
	PrimaryIDBase    int64
}

type QueryReadyBaseBuildStats struct {
	Parts            int
	Rows             int64
	InputBytes       int64
	OutputBytes      int64
	BytesCopied      int64
	BytesHashed      int64
	BytesChecksummed int64
	ExecutionBytes   int64
	ExecutionColumns int
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
	ExecutionBytes          int64
	ExecutionColumns        int
	ValidationTime          time.Duration
	OpenTime                time.Duration
	Mapped                  bool
}

type QueryReadyBasePartView struct {
	Dependency QueryReadyBaseDependency
	Offset     int
	Image      ColumnPartImage
	Execution  QueryReadyExecutionPartView
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
			g.Parts[i].Execution = QueryReadyExecutionPartView{}
		}
	})
	return g.closeErr
}

type queryReadyBaseBuildPart struct {
	input             QueryReadyBasePartInput
	parsed            ColumnPartImage
	checksum          [sha256.Size]byte
	offset            int
	execution         []byte
	executionChecksum [sha256.Size]byte
	executionOffset   int
	executionColumns  int
	dependency        QueryReadyBaseDependency
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

// QueryReadyBaseStreamingPlanner retains only deterministic QRBG layout
// metadata between source passes. It deliberately never retains a source image
// or execution sidecar after Add returns.
type QueryReadyBaseStreamingPlanner struct {
	identity       QueryReadyBaseIdentity
	parts          []queryReadyBaseStreamingPart
	validationTime time.Duration
}

type queryReadyBaseStreamingPart struct {
	ordinal           int
	sourceGeneration  uint64
	primaryIDMode     QueryReadyPrimaryIDMode
	primaryIDBase     int64
	partID            uint64
	rows              int
	imageBytes        int
	imageChecksum     [sha256.Size]byte
	executionBytes    int
	executionUpper    int64
	executionChecksum [sha256.Size]byte
	executionColumns  int
	imageOffset       int
	executionOffset   int
}

// QueryReadyBaseStreamingPlan is a completed two-pass QRBG layout. Its
// methods retain only fixed plan metadata; callers supply each source again to
// Emit so source images and sidecars have bounded lifetimes.
type QueryReadyBaseStreamingPlan struct {
	identity       QueryReadyBaseIdentity
	parts          []queryReadyBaseStreamingPart
	payloadOffset  int
	totalBytes     int
	maxLiveBytes   int64
	validationTime time.Duration
}

func NewQueryReadyBaseStreamingPlanner(identity QueryReadyBaseIdentity, capacity int) (*QueryReadyBaseStreamingPlanner, error) {
	if err := validateQueryReadyBaseIdentity(identity); err != nil {
		return nil, err
	}
	if capacity < 0 {
		return nil, errors.New("typedcolumn: negative query-ready streaming planner capacity")
	}
	return &QueryReadyBaseStreamingPlanner{identity: identity, parts: make([]queryReadyBaseStreamingPart, 0, capacity)}, nil
}

// Add validates one source and records only its deterministic output metadata.
func (p *QueryReadyBaseStreamingPlanner) Add(input QueryReadyBasePartInput) error {
	if p == nil {
		return errors.New("typedcolumn: nil query-ready streaming planner")
	}
	started := time.Now()
	parsed, execution, executionColumns, err := inspectQueryReadyBasePart(p.identity, input)
	p.validationTime += time.Since(started)
	if err != nil {
		return err
	}
	executionUpper, err := EstimateQueryReadyExecutionImageUpperBound(parsed)
	if err != nil {
		return err
	}
	part := queryReadyBaseStreamingPart{
		ordinal:          len(p.parts),
		sourceGeneration: input.SourceGeneration, primaryIDMode: input.PrimaryIDMode, primaryIDBase: input.PrimaryIDBase,
		partID: parsed.PartID, rows: parsed.Rows, imageBytes: len(input.Image.Bytes), imageChecksum: sha256.Sum256(input.Image.Bytes),
		executionBytes: len(execution), executionUpper: executionUpper, executionChecksum: sha256.Sum256(execution), executionColumns: executionColumns,
	}
	p.parts = append(p.parts, part)
	return nil
}

func (p *QueryReadyBaseStreamingPlanner) Finish() (*QueryReadyBaseStreamingPlan, error) {
	if p == nil {
		return nil, errors.New("typedcolumn: nil query-ready streaming planner")
	}
	parts := append([]queryReadyBaseStreamingPart(nil), p.parts...)
	sort.Slice(parts, func(i, j int) bool {
		if parts[i].sourceGeneration != parts[j].sourceGeneration {
			return parts[i].sourceGeneration < parts[j].sourceGeneration
		}
		return parts[i].partID < parts[j].partID
	})
	for i := 1; i < len(parts); i++ {
		if parts[i-1].sourceGeneration == parts[i].sourceGeneration && parts[i-1].partID == parts[i].partID {
			return nil, fmt.Errorf("typedcolumn: query-ready base duplicate dependency generation=%d part_id=%d", parts[i].sourceGeneration, parts[i].partID)
		}
	}
	if len(parts) > math.MaxUint32 || len(parts) > (math.MaxInt-queryReadyBaseHeaderBytes)/queryReadyBasePartEntryBytes {
		return nil, fmt.Errorf("typedcolumn: query-ready base parts=%d exceed format bounds", len(parts))
	}
	payloadOffset, err := queryReadyBaseAlign(queryReadyBaseHeaderBytes+len(parts)*queryReadyBasePartEntryBytes, queryReadyBasePayloadAlignment)
	if err != nil {
		return nil, err
	}
	total := payloadOffset
	var maxLive int64
	for i := range parts {
		total, err = queryReadyBaseAlign(total, columnPartImageSectionAlignment)
		if err != nil || parts[i].imageBytes > math.MaxInt-total {
			return nil, errors.New("typedcolumn: query-ready base image exceeds host size")
		}
		parts[i].imageOffset = total
		total += parts[i].imageBytes
		total, err = queryReadyBaseAlign(total, queryReadyExecutionImagePayloadAlign)
		if err != nil || parts[i].executionBytes > math.MaxInt-total {
			return nil, errors.New("typedcolumn: query-ready execution image exceeds host size")
		}
		parts[i].executionOffset = total
		total += parts[i].executionBytes
		if parts[i].executionUpper < 0 || parts[i].executionUpper > math.MaxInt64/2 || int64(parts[i].imageBytes) > math.MaxInt64-parts[i].executionUpper*2 {
			return nil, errors.New("typedcolumn: query-ready streaming live size overflow")
		}
		live := int64(parts[i].imageBytes) + parts[i].executionUpper*2
		if live > maxLive {
			maxLive = live
		}
	}
	return &QueryReadyBaseStreamingPlan{identity: p.identity, parts: parts, payloadOffset: payloadOffset, totalBytes: total, maxLiveBytes: maxLive, validationTime: p.validationTime}, nil
}

func (p *QueryReadyBaseStreamingPlan) OutputBytes() int64 {
	if p == nil {
		return 0
	}
	return int64(p.totalBytes)
}

// EstimatedPeakBytes covers the final output, one live source image, its
// execution sidecar plus conservative workspace, and fixed/dependency plan
// metadata. It intentionally does not use cumulative source bytes.
func (p *QueryReadyBaseStreamingPlan) EstimatedPeakBytes() (int64, error) {
	if p == nil || p.totalBytes < 0 {
		return 0, errors.New("typedcolumn: invalid query-ready streaming plan")
	}
	const fixedBuildBytes = int64(64 << 10)
	metadata := int64(len(p.parts)) * (1024 + 64)
	if len(p.parts) != 0 && metadata/int64(len(p.parts)) != 1088 {
		return 0, errors.New("typedcolumn: query-ready streaming metadata size overflow")
	}
	if int64(p.totalBytes) > math.MaxInt64-p.maxLiveBytes || int64(p.totalBytes)+p.maxLiveBytes > math.MaxInt64-fixedBuildBytes || int64(p.totalBytes)+p.maxLiveBytes+fixedBuildBytes > math.MaxInt64-metadata {
		return 0, errors.New("typedcolumn: query-ready streaming peak size overflow")
	}
	return int64(p.totalBytes) + p.maxLiveBytes + fixedBuildBytes + metadata, nil
}

// Emit rereads every source through load, validates it against pass-one
// metadata, and writes the deterministic QRBG payload without retaining prior
// sources. A mismatch fails closed before that source is emitted.
func (p *QueryReadyBaseStreamingPlan) Emit(load func(int) (QueryReadyBasePartInput, error)) (QueryReadyBaseBuildResult, error) {
	if p == nil || load == nil {
		return QueryReadyBaseBuildResult{}, errors.New("typedcolumn: invalid query-ready streaming emit")
	}
	started := time.Now()
	out := make([]byte, p.totalBytes)
	binary.LittleEndian.PutUint32(out[0:4], queryReadyBaseMagic)
	binary.LittleEndian.PutUint16(out[4:6], queryReadyBaseVersion)
	binary.LittleEndian.PutUint64(out[8:16], p.identity.Generation)
	copy(out[16:48], p.identity.SchemaHash[:])
	binary.LittleEndian.PutUint32(out[48:52], uint32(len(p.parts)))
	binary.LittleEndian.PutUint64(out[64:72], uint64(p.payloadOffset))
	binary.LittleEndian.PutUint64(out[72:80], uint64(p.totalBytes))
	dependencies := make([]QueryReadyBaseDependency, len(p.parts))
	var rows, inputBytes, executionBytes int64
	var executionColumns int
	for i, expected := range p.parts {
		input, err := load(expected.ordinal)
		if err != nil {
			return QueryReadyBaseBuildResult{}, err
		}
		parsed, execution, columns, err := inspectQueryReadyBasePart(p.identity, input)
		if err != nil {
			return QueryReadyBaseBuildResult{}, err
		}
		if input.SourceGeneration != expected.sourceGeneration || input.PrimaryIDMode != expected.primaryIDMode || input.PrimaryIDBase != expected.primaryIDBase || parsed.PartID != expected.partID || parsed.Rows != expected.rows || len(input.Image.Bytes) != expected.imageBytes || sha256.Sum256(input.Image.Bytes) != expected.imageChecksum || len(execution) != expected.executionBytes || sha256.Sum256(execution) != expected.executionChecksum || columns != expected.executionColumns {
			return QueryReadyBaseBuildResult{}, fmt.Errorf("typedcolumn: query-ready streaming source[%d] changed between plan and emit", i)
		}
		entry := out[queryReadyBaseHeaderBytes+i*queryReadyBasePartEntryBytes:]
		binary.LittleEndian.PutUint64(entry[0:8], expected.sourceGeneration)
		binary.LittleEndian.PutUint64(entry[8:16], expected.partID)
		binary.LittleEndian.PutUint64(entry[16:24], uint64(expected.imageOffset))
		binary.LittleEndian.PutUint64(entry[24:32], uint64(expected.imageBytes))
		binary.LittleEndian.PutUint64(entry[32:40], uint64(expected.rows))
		binary.LittleEndian.PutUint64(entry[40:48], uint64(parsed.ManifestBytes))
		copy(entry[48:80], expected.imageChecksum[:])
		binary.LittleEndian.PutUint64(entry[80:88], uint64(expected.primaryIDBase))
		entry[88] = byte(expected.primaryIDMode)
		binary.LittleEndian.PutUint64(entry[96:104], uint64(expected.executionOffset))
		binary.LittleEndian.PutUint64(entry[104:112], uint64(expected.executionBytes))
		copy(entry[112:144], expected.executionChecksum[:])
		copy(out[expected.imageOffset:], input.Image.Bytes)
		copy(out[expected.executionOffset:], execution)
		dependencies[i] = QueryReadyBaseDependency{SourceGeneration: expected.sourceGeneration, PartID: expected.partID, Rows: expected.rows, ImageBytes: expected.imageBytes, ImageChecksum: expected.imageChecksum, PrimaryIDMode: expected.primaryIDMode, PrimaryIDBase: expected.primaryIDBase}
		rows += int64(expected.rows)
		inputBytes += int64(expected.imageBytes)
		executionBytes += int64(expected.executionBytes)
		executionColumns += expected.executionColumns
	}
	table := out[queryReadyBaseHeaderBytes : queryReadyBaseHeaderBytes+len(p.parts)*queryReadyBasePartEntryBytes]
	binary.LittleEndian.PutUint32(out[56:60], crc32.Checksum(table, queryReadyBaseCRCTable))
	binary.LittleEndian.PutUint32(out[52:56], queryReadyBaseHeaderChecksum(out[:queryReadyBaseHeaderBytes]))
	return QueryReadyBaseBuildResult{Bytes: out, Dependencies: dependencies, Stats: QueryReadyBaseBuildStats{Parts: len(p.parts), Rows: rows, InputBytes: inputBytes, OutputBytes: int64(len(out)), BytesCopied: inputBytes + executionBytes, BytesHashed: inputBytes + executionBytes, BytesChecksummed: int64(len(table) + queryReadyBaseHeaderBytes), ExecutionBytes: executionBytes, ExecutionColumns: executionColumns, ValidationTime: p.validationTime, BuildTime: time.Since(started) + p.validationTime}}, nil
}

func inspectQueryReadyBasePart(identity QueryReadyBaseIdentity, input QueryReadyBasePartInput) (ColumnPartImage, []byte, int, error) {
	if input.SourceGeneration == 0 || input.SourceGeneration > identity.Generation {
		return ColumnPartImage{}, nil, 0, fmt.Errorf("typedcolumn: query-ready base source generation=%d exceeds base generation=%d", input.SourceGeneration, identity.Generation)
	}
	parsed, err := ParseColumnPartImage(input.Image.Bytes)
	if err != nil {
		return ColumnPartImage{}, nil, 0, err
	}
	readOptions := ColumnPartImageReadOptions{}
	if input.PrimaryIDMode == QueryReadyPrimaryIDDensePartLocal {
		readOptions.IncludeRowLocators, readOptions.ValidateRowLocators = true, true
	}
	decoded, err := ColumnPartFromImageWithOptions(parsed, readOptions)
	if err != nil {
		return ColumnPartImage{}, nil, 0, err
	}
	if err := validateQueryReadyPrimaryIDInput(input, decoded); err != nil {
		return ColumnPartImage{}, nil, 0, err
	}
	if _, err := CertifyColumnPartLayoutContractFromImage(parsed); err != nil {
		return ColumnPartImage{}, nil, 0, err
	}
	execution, columns, err := buildQueryReadyExecutionImage(decoded)
	if err != nil {
		return ColumnPartImage{}, nil, 0, err
	}
	return parsed, execution, columns, nil
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
		readOptions := ColumnPartImageReadOptions{}
		if input.PrimaryIDMode == QueryReadyPrimaryIDDensePartLocal {
			readOptions.IncludeRowLocators = true
			readOptions.ValidateRowLocators = true
		}
		decoded, err := ColumnPartFromImageWithOptions(parsed, readOptions)
		if err != nil {
			return queryReadyBaseBuildPlan{}, fmt.Errorf("typedcolumn: query-ready base part[%d] validate structures: %w", i, err)
		}
		if err := validateQueryReadyPrimaryIDInput(input, decoded); err != nil {
			return queryReadyBaseBuildPlan{}, fmt.Errorf("typedcolumn: query-ready base part[%d] primary IDs: %w", i, err)
		}
		if _, err := CertifyColumnPartLayoutContractFromImage(parsed); err != nil {
			return queryReadyBaseBuildPlan{}, fmt.Errorf("typedcolumn: query-ready base part[%d] certify layout: %w", i, err)
		}
		execution, executionColumns, err := buildQueryReadyExecutionImage(decoded)
		if err != nil {
			return queryReadyBaseBuildPlan{}, fmt.Errorf("typedcolumn: query-ready base part[%d] build execution image: %w", i, err)
		}
		validationTime += time.Since(validationStarted)
		parts[i] = queryReadyBaseBuildPart{
			input:             input,
			parsed:            parsed,
			checksum:          sha256.Sum256(input.Image.Bytes),
			execution:         execution,
			executionChecksum: sha256.Sum256(execution),
			executionColumns:  executionColumns,
		}
		parts[i].dependency = QueryReadyBaseDependency{
			SourceGeneration: input.SourceGeneration,
			PartID:           parsed.PartID,
			Rows:             parsed.Rows,
			ImageBytes:       len(input.Image.Bytes),
			ImageChecksum:    parts[i].checksum,
			PrimaryIDMode:    input.PrimaryIDMode,
			PrimaryIDBase:    input.PrimaryIDBase,
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
		totalBytes, err = queryReadyBaseAlign(totalBytes, queryReadyExecutionImagePayloadAlign)
		if err != nil {
			return queryReadyBaseBuildPlan{}, err
		}
		parts[i].executionOffset = totalBytes
		if len(parts[i].execution) > math.MaxInt-totalBytes {
			return queryReadyBaseBuildPlan{}, errors.New("typedcolumn: query-ready execution image exceeds host size")
		}
		totalBytes += len(parts[i].execution)
	}
	return queryReadyBaseBuildPlan{parts: parts, payloadOffset: payloadOffset, totalBytes: totalBytes, validationTime: validationTime}, nil
}

func validateQueryReadyPrimaryIDInput(input QueryReadyBasePartInput, part *ColumnPart) error {
	if part == nil {
		return errors.New("nil typed-column part")
	}
	if err := validateQueryReadyPrimaryIDMetadata(input.PrimaryIDMode, input.PrimaryIDBase, int64(part.Descriptor.RowCount)); err != nil {
		return err
	}
	if input.PrimaryIDMode != QueryReadyPrimaryIDDensePartLocal {
		return nil
	}
	if len(part.Locators) != part.Descriptor.RowCount {
		return fmt.Errorf("dense part-local locator count=%d want rows=%d", len(part.Locators), part.Descriptor.RowCount)
	}
	for primaryID := range part.Locators {
		if primaryID < 0 || primaryID >= int64(part.Descriptor.RowCount) {
			return fmt.Errorf("dense part-local ID %d outside [0,%d)", primaryID, part.Descriptor.RowCount)
		}
	}
	return nil
}

func validateQueryReadyPrimaryIDMetadata(mode QueryReadyPrimaryIDMode, base, rows int64) error {
	if rows < 0 {
		return fmt.Errorf("negative row count %d", rows)
	}
	switch mode {
	case QueryReadyPrimaryIDPreserve:
		if base != 0 {
			return fmt.Errorf("preserved primary IDs have nonzero base %d", base)
		}
	case QueryReadyPrimaryIDDensePartLocal:
		if base < 0 {
			return fmt.Errorf("dense part-local primary ID base %d is negative", base)
		}
		if rows > 0 && base > math.MaxInt64-(rows-1) {
			return fmt.Errorf("dense part-local range base=%d rows=%d overflows int64", base, rows)
		}
	default:
		return fmt.Errorf("unsupported primary ID mode %d", mode)
	}
	return nil
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
	var rows, inputBytes, executionBytes int64
	var executionColumns int
	for i := range parts {
		entry := out[queryReadyBaseHeaderBytes+i*queryReadyBasePartEntryBytes:]
		binary.LittleEndian.PutUint64(entry[0:8], parts[i].input.SourceGeneration)
		binary.LittleEndian.PutUint64(entry[8:16], parts[i].parsed.PartID)
		binary.LittleEndian.PutUint64(entry[16:24], uint64(parts[i].offset))
		binary.LittleEndian.PutUint64(entry[24:32], uint64(len(parts[i].input.Image.Bytes)))
		binary.LittleEndian.PutUint64(entry[32:40], uint64(parts[i].parsed.Rows))
		binary.LittleEndian.PutUint64(entry[40:48], uint64(parts[i].parsed.ManifestBytes))
		copy(entry[48:80], parts[i].checksum[:])
		binary.LittleEndian.PutUint64(entry[80:88], uint64(parts[i].input.PrimaryIDBase))
		entry[88] = byte(parts[i].input.PrimaryIDMode)
		binary.LittleEndian.PutUint64(entry[96:104], uint64(parts[i].executionOffset))
		binary.LittleEndian.PutUint64(entry[104:112], uint64(len(parts[i].execution)))
		copy(entry[112:144], parts[i].executionChecksum[:])
		copy(out[parts[i].offset:], parts[i].input.Image.Bytes)
		copy(out[parts[i].executionOffset:], parts[i].execution)
		dependencies[i] = parts[i].dependency
		rows += int64(parts[i].parsed.Rows)
		inputBytes += int64(len(parts[i].input.Image.Bytes))
		executionBytes += int64(len(parts[i].execution))
		executionColumns += parts[i].executionColumns
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
			BytesCopied:      inputBytes + executionBytes,
			BytesHashed:      inputBytes + executionBytes,
			BytesChecksummed: int64(tableBytes + queryReadyBaseHeaderBytes),
			ExecutionBytes:   executionBytes,
			ExecutionColumns: executionColumns,
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
		primaryIDBase := int64(binary.LittleEndian.Uint64(entry[80:88]))
		primaryIDMode := QueryReadyPrimaryIDMode(entry[88])
		executionOffset64 := binary.LittleEndian.Uint64(entry[96:104])
		executionLength64 := binary.LittleEndian.Uint64(entry[104:112])
		if slices.ContainsFunc(entry[89:96], func(value byte) bool { return value != 0 }) {
			return nil, fmt.Errorf("typedcolumn: query-ready base part[%d] reserved entry bytes are nonzero", i)
		}
		if err := validateQueryReadyPrimaryIDMetadata(primaryIDMode, primaryIDBase, int64(rows64)); err != nil {
			return nil, fmt.Errorf("typedcolumn: query-ready base part[%d] primary IDs: %w", i, err)
		}
		if sourceGeneration == 0 || sourceGeneration > identity.Generation {
			return nil, fmt.Errorf("typedcolumn: query-ready base part[%d] source generation=%d invalid for base generation=%d", i, sourceGeneration, identity.Generation)
		}
		if i > 0 && (sourceGeneration < previousSource || (sourceGeneration == previousSource && partID <= previousPart)) {
			return nil, fmt.Errorf("typedcolumn: query-ready base part[%d] dependency order is not strictly increasing", i)
		}
		if offset64 > math.MaxInt || length64 > math.MaxInt || rows64 > math.MaxInt || manifest64 > math.MaxInt || executionOffset64 > math.MaxInt || executionLength64 > math.MaxInt {
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
		executionOffset, executionLength := int(executionOffset64), int(executionLength64)
		imageEnd := offset + length
		if executionOffset < imageEnd || executionOffset%queryReadyExecutionImagePayloadAlign != 0 || executionLength < queryReadyExecutionImageHeaderBytes || executionLength > len(data)-executionOffset {
			return nil, fmt.Errorf("typedcolumn: query-ready base part[%d] execution offset=%d length=%d invalid for image_end=%d total=%d", i, executionOffset, executionLength, imageEnd, len(data))
		}
		if err := queryReadyBaseValidateZeroPadding(data[imageEnd:executionOffset], fmt.Sprintf("before part[%d] execution", i)); err != nil {
			return nil, err
		}
		executionBytes := data[executionOffset : executionOffset+executionLength]
		var wantExecutionChecksum [sha256.Size]byte
		copy(wantExecutionChecksum[:], entry[112:144])
		if got := sha256.Sum256(executionBytes); got != wantExecutionChecksum {
			return nil, fmt.Errorf("typedcolumn: query-ready base part[%d] execution checksum=%x want %x", i, got, wantExecutionChecksum)
		}
		execution, err := parseQueryReadyExecutionImage(executionBytes, image.Rows)
		if err != nil {
			return nil, fmt.Errorf("typedcolumn: query-ready base part[%d] execution image: %w", i, err)
		}
		dependency := QueryReadyBaseDependency{SourceGeneration: sourceGeneration, PartID: partID, Rows: image.Rows, ImageBytes: len(partBytes), ImageChecksum: wantChecksum, PrimaryIDMode: primaryIDMode, PrimaryIDBase: primaryIDBase}
		dependencies = append(dependencies, dependency)
		parts = append(parts, QueryReadyBasePartView{Dependency: dependency, Offset: offset, Image: image, Execution: execution})
		stats.Parts++
		stats.Rows += int64(image.Rows)
		stats.BytesDecoded += int64(manifestLength + queryReadyBaseStructuralBytes(image))
		stats.ExecutionBytes += int64(executionLength)
		stats.ExecutionColumns += len(execution.columns)
		previousSource, previousPart = sourceGeneration, partID
		previousEnd = executionOffset + executionLength
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
