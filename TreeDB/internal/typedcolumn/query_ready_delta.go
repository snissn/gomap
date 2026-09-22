package typedcolumn

import (
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

// Query-ready delta generation envelopes are rebuildable, non-authoritative
// derived assets. They do not publish roots, participate in WAL/recovery, or
// own reclamation. The embedded QRBG keeps M1's schema and part validation.
const (
	queryReadyDeltaMagic            = uint32(0x47445251) // "QRDG", little-endian.
	queryReadyDeltaVersion          = uint16(1)
	queryReadyDeltaHeaderBytes      = 96
	queryReadyDeltaTombstoneBytes   = 16
	queryReadyDeltaPayloadAlignment = 4096
)

type QueryReadyGenerationKind uint16

const (
	QueryReadyGenerationDelta            QueryReadyGenerationKind = 1
	QueryReadyGenerationConsolidatedBase QueryReadyGenerationKind = 2
)

type QueryReadyDeltaBuildStats struct {
	Parts                  int
	Rows                   int64
	Tombstones             int
	OriginBaseParts        int
	AccumulatedDeltaParts  int
	InputBytes             int64
	OutputBytes            int64
	BytesCopied            int64
	BytesHashed            int64
	BytesChecksummed       int64
	BytesCompressed        int64
	ExecutionBytes         int64
	ExecutionColumns       int
	PeakEncodedBufferBytes int64
	WriteAmplification     float64
	ValidationTime         time.Duration
	BaseBuildTime          time.Duration
	TombstonePrepareTime   time.Duration
	EnvelopeBuildTime      time.Duration
	BuildTime              time.Duration
}

type QueryReadyDeltaBuildResult struct {
	Bytes        []byte
	Dependencies []QueryReadyBaseDependency
	Stats        QueryReadyDeltaBuildStats
}

type QueryReadyDeltaOpenStats struct {
	Parts          int
	Rows           int64
	Tombstones     int
	BytesRead      int64
	BytesDecoded   int64
	BytesCopied    int64
	BytesValidated int64
	BytesMapped    int64
	Mapped         bool
	OpenTime       time.Duration
}

// QueryReadyDeltaGeneration is either one immutable delta or a consolidated
// replacement base. Base contains the M1 QRBG part set; Tombstones is the
// deterministic latest tombstone per primary ID needed to reproduce visibility.
type QueryReadyDeltaGeneration struct {
	Kind                  QueryReadyGenerationKind
	Identity              QueryReadyBaseIdentity
	Base                  *QueryReadyBaseGeneration
	Tombstones            []Tombstone
	OriginBaseParts       int
	AccumulatedDeltaParts int
	Stats                 QueryReadyDeltaOpenStats
	data                  []byte
	release               func() error
	closeOnce             sync.Once
	closeErr              error
}

func (g *QueryReadyDeltaGeneration) Bytes() []byte {
	if g == nil {
		return nil
	}
	return g.data
}

func (g *QueryReadyDeltaGeneration) Close() error {
	if g == nil {
		return nil
	}
	g.closeOnce.Do(func() {
		var baseErr error
		if g.Base != nil {
			baseErr = g.Base.Close()
		}
		var releaseErr error
		if g.release != nil {
			releaseErr = g.release()
		}
		g.closeErr = errors.Join(baseErr, releaseErr)
		g.data = nil
	})
	return g.closeErr
}

func BuildQueryReadyDeltaGeneration(identity QueryReadyBaseIdentity, parts []QueryReadyBasePartInput, tombstones []Tombstone) (QueryReadyDeltaBuildResult, error) {
	return buildQueryReadyDeltaEnvelope(QueryReadyGenerationDelta, identity, parts, tombstones, queryReadyDeltaLineage{})
}

type queryReadyDeltaLineage struct {
	OriginBaseParts       int
	AccumulatedDeltaParts int
}

func buildQueryReadyDeltaEnvelope(kind QueryReadyGenerationKind, identity QueryReadyBaseIdentity, parts []QueryReadyBasePartInput, tombstones []Tombstone, lineage queryReadyDeltaLineage) (QueryReadyDeltaBuildResult, error) {
	started := time.Now()
	if kind != QueryReadyGenerationDelta && kind != QueryReadyGenerationConsolidatedBase {
		return QueryReadyDeltaBuildResult{}, fmt.Errorf("typedcolumn: unsupported query-ready generation kind %d", kind)
	}
	if err := validateQueryReadyBaseIdentity(identity); err != nil {
		return QueryReadyDeltaBuildResult{}, err
	}
	if lineage.OriginBaseParts < 0 || lineage.AccumulatedDeltaParts < 0 || lineage.OriginBaseParts > math.MaxUint32 || lineage.AccumulatedDeltaParts > math.MaxUint32 {
		return QueryReadyDeltaBuildResult{}, fmt.Errorf("typedcolumn: query-ready lineage parts origin=%d accumulated_delta=%d exceed format bounds", lineage.OriginBaseParts, lineage.AccumulatedDeltaParts)
	}
	if kind == QueryReadyGenerationDelta && lineage != (queryReadyDeltaLineage{}) {
		return QueryReadyDeltaBuildResult{}, errors.New("typedcolumn: query-ready delta cannot carry consolidated-base lineage")
	}
	if kind == QueryReadyGenerationDelta {
		for i, part := range parts {
			if part.SourceGeneration != identity.Generation {
				return QueryReadyDeltaBuildResult{}, fmt.Errorf("typedcolumn: query-ready delta part[%d] source generation=%d want envelope generation=%d", i, part.SourceGeneration, identity.Generation)
			}
		}
	}
	if kind == QueryReadyGenerationConsolidatedBase && lineage.OriginBaseParts+lineage.AccumulatedDeltaParts != len(parts) {
		return QueryReadyDeltaBuildResult{}, fmt.Errorf("typedcolumn: query-ready consolidated lineage parts=%d+%d want embedded=%d", lineage.OriginBaseParts, lineage.AccumulatedDeltaParts, len(parts))
	}
	basePlan, err := prepareQueryReadyBaseGeneration(identity, parts)
	if err != nil {
		return QueryReadyDeltaBuildResult{}, fmt.Errorf("typedcolumn: build query-ready generation parts: %w", err)
	}
	tombstoneStarted := time.Now()
	normalized, err := normalizeQueryReadyTombstones(tombstones, identity.Generation)
	if err != nil {
		return QueryReadyDeltaBuildResult{}, err
	}
	tombstonePrepareTime := time.Since(tombstoneStarted)
	if len(normalized) > (math.MaxInt-queryReadyDeltaHeaderBytes)/queryReadyDeltaTombstoneBytes || len(normalized) > math.MaxUint32 {
		return QueryReadyDeltaBuildResult{}, fmt.Errorf("typedcolumn: query-ready tombstones=%d exceed format bounds", len(normalized))
	}
	tableBytes := len(normalized) * queryReadyDeltaTombstoneBytes
	payloadOffset, err := queryReadyBaseAlign(queryReadyDeltaHeaderBytes+tableBytes, queryReadyDeltaPayloadAlignment)
	if err != nil {
		return QueryReadyDeltaBuildResult{}, err
	}
	if basePlan.totalBytes > math.MaxInt-payloadOffset {
		return QueryReadyDeltaBuildResult{}, errors.New("typedcolumn: query-ready delta image exceeds host size")
	}
	totalBytes := payloadOffset + basePlan.totalBytes
	envelopeStarted := time.Now()
	out := make([]byte, totalBytes)
	binary.LittleEndian.PutUint32(out[0:4], queryReadyDeltaMagic)
	binary.LittleEndian.PutUint16(out[4:6], queryReadyDeltaVersion)
	binary.LittleEndian.PutUint16(out[6:8], uint16(kind))
	binary.LittleEndian.PutUint64(out[8:16], identity.Generation)
	copy(out[16:48], identity.SchemaHash[:])
	binary.LittleEndian.PutUint32(out[48:52], uint32(len(normalized)))
	binary.LittleEndian.PutUint64(out[64:72], uint64(payloadOffset))
	binary.LittleEndian.PutUint64(out[72:80], uint64(totalBytes))
	binary.LittleEndian.PutUint64(out[80:88], uint64(basePlan.totalBytes))
	binary.LittleEndian.PutUint32(out[88:92], uint32(lineage.OriginBaseParts))
	binary.LittleEndian.PutUint32(out[92:96], uint32(lineage.AccumulatedDeltaParts))
	table := out[queryReadyDeltaHeaderBytes : queryReadyDeltaHeaderBytes+tableBytes]
	for i, tombstone := range normalized {
		entry := table[i*queryReadyDeltaTombstoneBytes : (i+1)*queryReadyDeltaTombstoneBytes]
		binary.LittleEndian.PutUint64(entry[0:8], uint64(tombstone.PrimaryID))
		binary.LittleEndian.PutUint64(entry[8:16], tombstone.GenerationID)
	}
	binary.LittleEndian.PutUint32(out[56:60], crc32.Checksum(table, queryReadyBaseCRCTable))
	baseEncodeStarted := time.Now()
	inner := encodeQueryReadyBaseGeneration(identity, basePlan, out[payloadOffset:])
	baseEncodeTime := time.Since(baseEncodeStarted)
	binary.LittleEndian.PutUint32(out[52:56], queryReadyDeltaHeaderChecksum(out[:queryReadyDeltaHeaderBytes]))
	inputBytes := inner.Stats.InputBytes + int64(len(normalized)*queryReadyDeltaTombstoneBytes)
	writeAmplification := float64(0)
	if inputBytes > 0 {
		writeAmplification = float64(len(out)) / float64(inputBytes)
	}
	return QueryReadyDeltaBuildResult{Bytes: out, Dependencies: inner.Dependencies, Stats: QueryReadyDeltaBuildStats{
		Parts: len(parts), Rows: inner.Stats.Rows, Tombstones: len(normalized),
		OriginBaseParts: lineage.OriginBaseParts, AccumulatedDeltaParts: lineage.AccumulatedDeltaParts,
		InputBytes: inputBytes, OutputBytes: int64(len(out)),
		BytesCopied:            inner.Stats.BytesCopied + int64(len(table)),
		BytesHashed:            inner.Stats.BytesHashed,
		BytesChecksummed:       inner.Stats.BytesChecksummed + int64(len(table)+queryReadyDeltaHeaderBytes),
		ExecutionBytes:         inner.Stats.ExecutionBytes,
		ExecutionColumns:       inner.Stats.ExecutionColumns,
		PeakEncodedBufferBytes: int64(len(out)),
		WriteAmplification:     writeAmplification,
		ValidationTime:         basePlan.validationTime,
		BaseBuildTime:          basePlan.validationTime + baseEncodeTime,
		TombstonePrepareTime:   tombstonePrepareTime,
		EnvelopeBuildTime:      time.Since(envelopeStarted),
		BuildTime:              time.Since(started),
	}}, nil
}

func OpenQueryReadyDeltaGeneration(data []byte, expected QueryReadyBaseIdentity) (*QueryReadyDeltaGeneration, error) {
	return openQueryReadyDeltaEnvelope(data, expected, QueryReadyGenerationDelta)
}

// OpenQueryReadyDeltaGenerationFile opens a QRDG through a read-only mapping.
// The returned generation owns the mapping until Close. Publication, pinning,
// and deletion safety remain responsibilities of the caller's existing asset
// lifecycle; this helper only owns its file descriptor and mapping.
func OpenQueryReadyDeltaGenerationFile(path string, expected QueryReadyBaseIdentity) (*QueryReadyDeltaGeneration, error) {
	return OpenQueryReadyDeltaGenerationFileRange(path, 0, 0, expected)
}

func OpenQueryReadyDeltaGenerationFileRange(path string, offset, length int64, expected QueryReadyBaseIdentity) (*QueryReadyDeltaGeneration, error) {
	return openQueryReadyDeltaEnvelopeFileRange(path, offset, length, expected, QueryReadyGenerationDelta)
}

func OpenQueryReadyConsolidatedBaseGeneration(data []byte, expected QueryReadyBaseIdentity) (*QueryReadyDeltaGeneration, error) {
	return openQueryReadyDeltaEnvelope(data, expected, QueryReadyGenerationConsolidatedBase)
}

func OpenQueryReadyConsolidatedBaseGenerationFile(path string, expected QueryReadyBaseIdentity) (*QueryReadyDeltaGeneration, error) {
	return OpenQueryReadyConsolidatedBaseGenerationFileRange(path, 0, 0, expected)
}

func OpenQueryReadyConsolidatedBaseGenerationFileRange(path string, offset, length int64, expected QueryReadyBaseIdentity) (*QueryReadyDeltaGeneration, error) {
	return openQueryReadyDeltaEnvelopeFileRange(path, offset, length, expected, QueryReadyGenerationConsolidatedBase)
}

func openQueryReadyDeltaEnvelopeFileRange(path string, offset, length int64, expected QueryReadyBaseIdentity, kind QueryReadyGenerationKind) (*QueryReadyDeltaGeneration, error) {
	started := time.Now()
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	logicalLength, err := queryReadyGenerationFileRange(file, offset, length)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("typedcolumn: query-ready delta range %q: %w", path, err)
	}
	data, mapping, err := mmapQueryReadyBaseFileRange(file, offset, logicalLength)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("typedcolumn: mmap query-ready delta %q offset=%d length=%d: %w", path, offset, logicalLength, err)
	}
	release := func() error {
		return errors.Join(munmapQueryReadyBaseFile(mapping), file.Close())
	}
	generation, err := openQueryReadyDeltaEnvelope(data, expected, kind)
	if err != nil {
		_ = release()
		return nil, err
	}
	generation.release = release
	generation.Stats.OpenTime = time.Since(started)
	generation.Stats.Mapped = true
	generation.Stats.BytesMapped = int64(len(mapping))
	if generation.Base != nil {
		generation.Base.Stats.Mapped = true
		generation.Base.Stats.BytesMapped = int64(len(mapping))
	}
	return generation, nil
}

func openQueryReadyDeltaEnvelope(data []byte, expected QueryReadyBaseIdentity, expectedKind QueryReadyGenerationKind) (*QueryReadyDeltaGeneration, error) {
	started := time.Now()
	if err := validateQueryReadyBaseIdentity(expected); err != nil {
		return nil, err
	}
	if len(data) < queryReadyDeltaHeaderBytes {
		return nil, fmt.Errorf("typedcolumn: query-ready delta bytes=%d shorter than header=%d", len(data), queryReadyDeltaHeaderBytes)
	}
	if got := binary.LittleEndian.Uint32(data[0:4]); got != queryReadyDeltaMagic {
		return nil, fmt.Errorf("typedcolumn: invalid query-ready delta magic 0x%x", got)
	}
	if got := binary.LittleEndian.Uint16(data[4:6]); got != queryReadyDeltaVersion {
		return nil, fmt.Errorf("typedcolumn: unsupported query-ready delta version %d", got)
	}
	kind := QueryReadyGenerationKind(binary.LittleEndian.Uint16(data[6:8]))
	if kind != expectedKind {
		return nil, fmt.Errorf("typedcolumn: query-ready generation kind=%d want %d", kind, expectedKind)
	}
	if binary.LittleEndian.Uint32(data[60:64]) != 0 {
		return nil, errors.New("typedcolumn: query-ready delta reserved header bytes are nonzero")
	}
	if got, want := binary.LittleEndian.Uint32(data[52:56]), queryReadyDeltaHeaderChecksum(data[:queryReadyDeltaHeaderBytes]); got != want {
		return nil, fmt.Errorf("typedcolumn: query-ready delta header checksum=%08x want %08x", got, want)
	}
	identity := QueryReadyBaseIdentity{Generation: binary.LittleEndian.Uint64(data[8:16])}
	copy(identity.SchemaHash[:], data[16:48])
	if identity.Generation != expected.Generation {
		return nil, fmt.Errorf("typedcolumn: query-ready delta generation=%d want %d", identity.Generation, expected.Generation)
	}
	if identity.SchemaHash != expected.SchemaHash {
		return nil, fmt.Errorf("typedcolumn: query-ready delta schema hash=%x want %x", identity.SchemaHash, expected.SchemaHash)
	}
	tombstoneCount := uint64(binary.LittleEndian.Uint32(data[48:52]))
	if tombstoneCount > uint64((math.MaxInt-queryReadyDeltaHeaderBytes)/queryReadyDeltaTombstoneBytes) {
		return nil, fmt.Errorf("typedcolumn: query-ready delta tombstone count=%d exceeds host bounds", tombstoneCount)
	}
	tableEnd := queryReadyDeltaHeaderBytes + int(tombstoneCount)*queryReadyDeltaTombstoneBytes
	if tableEnd > len(data) {
		return nil, fmt.Errorf("typedcolumn: query-ready delta tombstone table bytes=%d exceed image=%d", tableEnd, len(data))
	}
	table := data[queryReadyDeltaHeaderBytes:tableEnd]
	if got, want := crc32.Checksum(table, queryReadyBaseCRCTable), binary.LittleEndian.Uint32(data[56:60]); got != want {
		return nil, fmt.Errorf("typedcolumn: query-ready delta tombstone checksum=%08x want %08x", got, want)
	}
	payloadOffset64 := binary.LittleEndian.Uint64(data[64:72])
	totalBytes64 := binary.LittleEndian.Uint64(data[72:80])
	innerBytes64 := binary.LittleEndian.Uint64(data[80:88])
	originBaseParts := int(binary.LittleEndian.Uint32(data[88:92]))
	accumulatedDeltaParts := int(binary.LittleEndian.Uint32(data[92:96]))
	if totalBytes64 != uint64(len(data)) {
		return nil, fmt.Errorf("typedcolumn: query-ready delta total bytes=%d want provided=%d", totalBytes64, len(data))
	}
	if payloadOffset64 > uint64(len(data)) || payloadOffset64 < uint64(tableEnd) || payloadOffset64%queryReadyDeltaPayloadAlignment != 0 {
		return nil, fmt.Errorf("typedcolumn: query-ready delta payload offset=%d invalid table_end=%d total=%d", payloadOffset64, tableEnd, len(data))
	}
	if innerBytes64 != uint64(len(data))-payloadOffset64 {
		return nil, fmt.Errorf("typedcolumn: query-ready delta inner bytes=%d want %d", innerBytes64, uint64(len(data))-payloadOffset64)
	}
	if err := queryReadyBaseValidateZeroPadding(data[tableEnd:int(payloadOffset64)], "query-ready delta header"); err != nil {
		return nil, err
	}
	tombstones := make([]Tombstone, int(tombstoneCount))
	for i := range tombstones {
		entry := table[i*queryReadyDeltaTombstoneBytes : (i+1)*queryReadyDeltaTombstoneBytes]
		tombstones[i] = Tombstone{PrimaryID: int64(binary.LittleEndian.Uint64(entry[0:8])), GenerationID: binary.LittleEndian.Uint64(entry[8:16])}
		if tombstones[i].GenerationID == 0 || tombstones[i].GenerationID > identity.Generation {
			return nil, fmt.Errorf("typedcolumn: query-ready delta tombstone[%d] generation=%d invalid for generation=%d", i, tombstones[i].GenerationID, identity.Generation)
		}
		if i > 0 && tombstones[i-1].PrimaryID >= tombstones[i].PrimaryID {
			return nil, fmt.Errorf("typedcolumn: query-ready delta tombstones are not strictly ordered at %d", i)
		}
	}
	innerData := data[int(payloadOffset64):]
	base, err := OpenQueryReadyBaseGeneration(innerData, identity)
	if err != nil {
		return nil, fmt.Errorf("typedcolumn: open query-ready delta parts: %w", err)
	}
	if kind == QueryReadyGenerationDelta && (originBaseParts != 0 || accumulatedDeltaParts != 0) {
		return nil, errors.New("typedcolumn: query-ready delta carries consolidated-base lineage")
	}
	if kind == QueryReadyGenerationDelta {
		for i, dependency := range base.Dependencies {
			if dependency.SourceGeneration != identity.Generation {
				return nil, fmt.Errorf("typedcolumn: query-ready delta part[%d] source generation=%d want envelope generation=%d", i, dependency.SourceGeneration, identity.Generation)
			}
		}
	}
	if kind == QueryReadyGenerationConsolidatedBase && originBaseParts+accumulatedDeltaParts != len(base.Parts) {
		return nil, fmt.Errorf("typedcolumn: query-ready consolidated lineage parts=%d+%d want embedded=%d", originBaseParts, accumulatedDeltaParts, len(base.Parts))
	}
	return &QueryReadyDeltaGeneration{
		Kind: kind, Identity: identity, Base: base, Tombstones: tombstones,
		OriginBaseParts: originBaseParts, AccumulatedDeltaParts: accumulatedDeltaParts, data: data,
		Stats: QueryReadyDeltaOpenStats{
			Parts: len(base.Parts), Rows: base.Stats.Rows, Tombstones: len(tombstones),
			BytesRead: int64(len(data)), BytesDecoded: int64(queryReadyDeltaHeaderBytes+len(table)) + base.Stats.BytesDecoded,
			BytesValidated: int64(len(data)), OpenTime: time.Since(started),
		},
	}, nil
}

func normalizeQueryReadyTombstones(input []Tombstone, generation uint64) ([]Tombstone, error) {
	latest := make(map[int64]uint64, len(input))
	for i, tombstone := range input {
		if tombstone.GenerationID == 0 || tombstone.GenerationID > generation {
			return nil, fmt.Errorf("typedcolumn: query-ready tombstone[%d] generation=%d invalid for generation=%d", i, tombstone.GenerationID, generation)
		}
		if tombstone.GenerationID > latest[tombstone.PrimaryID] {
			latest[tombstone.PrimaryID] = tombstone.GenerationID
		}
	}
	out := make([]Tombstone, 0, len(latest))
	for primaryID, tombstoneGeneration := range latest {
		out = append(out, Tombstone{PrimaryID: primaryID, GenerationID: tombstoneGeneration})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PrimaryID < out[j].PrimaryID })
	return out, nil
}

func queryReadyDeltaHeaderChecksum(header []byte) uint32 {
	copyHeader := slices.Clone(header)
	clear(copyHeader[52:56])
	return crc32.Checksum(copyHeader, queryReadyBaseCRCTable)
}

type QueryReadyDeltaBoundPolicy struct {
	MaxVisibleGenerations    int
	MaxAccumulatedDeltaParts int
	MaxRows                  int64
	MaxBytes                 int64
}

// DefaultQueryReadyDeltaBoundPolicy is intentionally small and observable.
// The generation bound is selected from the checked-in N=0..8 focused curve;
// byte and row limits are workload-specific overrides rather than hidden caps.
func DefaultQueryReadyDeltaBoundPolicy() QueryReadyDeltaBoundPolicy {
	return QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4, MaxAccumulatedDeltaParts: 8}
}

type QueryReadyDeltaBoundDecision struct {
	VisibleGenerations      int
	BaseInternalParts       int
	OriginBaseParts         int
	BaseDeltaDerivedParts   int
	DeltaParts              int
	TotalParts              int
	AccumulatedDeltaParts   int
	Rows                    int64
	Bytes                   int64
	Triggered               bool
	GenerationLimitTriggers int
	PartLimitTriggers       int
	RowLimitTriggers        int
	ByteLimitTriggers       int
	Reason                  string
}

type QueryReadyDeltaBoundError struct {
	Phase    string
	Decision QueryReadyDeltaBoundDecision
}

func (e *QueryReadyDeltaBoundError) Error() string {
	if e == nil {
		return "typedcolumn: query-ready bound triggered"
	}
	d := e.Decision
	return fmt.Sprintf("typedcolumn: query-ready %s bound triggered reason=%s origin_base_parts=%d base_delta_parts=%d new_delta_parts=%d accumulated_delta_parts=%d total_parts=%d generations=%d rows=%d bytes=%d", e.Phase, d.Reason, d.OriginBaseParts, d.BaseDeltaDerivedParts, d.DeltaParts, d.AccumulatedDeltaParts, d.TotalParts, d.VisibleGenerations, d.Rows, d.Bytes)
}

func EvaluateQueryReadyDeltaBound(deltas []*QueryReadyDeltaGeneration, snapshot uint64, policy QueryReadyDeltaBoundPolicy) QueryReadyDeltaBoundDecision {
	if policy == (QueryReadyDeltaBoundPolicy{}) {
		policy = DefaultQueryReadyDeltaBoundPolicy()
	}
	decision := QueryReadyDeltaBoundDecision{}
	seen := make(map[uint64]struct{}, len(deltas))
	for _, delta := range deltas {
		if delta == nil || delta.Identity.Generation > snapshot {
			continue
		}
		if _, ok := seen[delta.Identity.Generation]; !ok {
			seen[delta.Identity.Generation] = struct{}{}
			decision.VisibleGenerations++
		}
		if delta.Base != nil {
			decision.DeltaParts += len(delta.Base.Parts)
			decision.TotalParts += len(delta.Base.Parts)
		}
		decision.Rows += delta.Stats.Rows
		if delta.Base != nil {
			for _, view := range delta.Base.Parts {
				decision.Bytes += int64(view.Dependency.ImageBytes)
			}
		}
		decision.Bytes += int64(len(delta.Tombstones) * queryReadyDeltaTombstoneBytes)
	}
	if policy.MaxVisibleGenerations > 0 && decision.VisibleGenerations > policy.MaxVisibleGenerations {
		decision.Triggered = true
		decision.GenerationLimitTriggers = 1
		decision.Reason = "visible_generations"
	}
	decision.AccumulatedDeltaParts = decision.DeltaParts
	if policy.MaxAccumulatedDeltaParts > 0 && decision.AccumulatedDeltaParts > policy.MaxAccumulatedDeltaParts {
		decision.Triggered = true
		decision.PartLimitTriggers = 1
		if decision.Reason == "" {
			decision.Reason = "accumulated_delta_parts"
		}
	}
	if policy.MaxRows > 0 && decision.Rows > policy.MaxRows {
		decision.Triggered = true
		decision.RowLimitTriggers = 1
		if decision.Reason == "" {
			decision.Reason = "rows"
		}
	}
	if policy.MaxBytes > 0 && decision.Bytes > policy.MaxBytes {
		decision.Triggered = true
		decision.ByteLimitTriggers = 1
		if decision.Reason == "" {
			decision.Reason = "bytes"
		}
	}
	return decision
}

func evaluateQueryReadyBaseDeltaBound(base *QueryReadyBaseGeneration, baseTombstones []Tombstone, baseOriginParts, baseAccumulatedDeltaParts int, deltas []*QueryReadyDeltaGeneration, snapshot uint64, policy QueryReadyDeltaBoundPolicy) QueryReadyDeltaBoundDecision {
	decision := EvaluateQueryReadyDeltaBound(deltas, snapshot, policy)
	if base == nil {
		return decision
	}
	decision.BaseInternalParts = len(base.Parts)
	decision.OriginBaseParts = baseOriginParts
	if decision.OriginBaseParts == 0 && baseAccumulatedDeltaParts == 0 {
		decision.OriginBaseParts = len(base.Parts)
	}
	decision.BaseDeltaDerivedParts = baseAccumulatedDeltaParts
	decision.TotalParts = decision.BaseInternalParts + decision.DeltaParts
	decision.AccumulatedDeltaParts = decision.BaseDeltaDerivedParts + decision.DeltaParts
	decision.Rows += base.Stats.Rows
	for _, view := range base.Parts {
		decision.Bytes += int64(view.Dependency.ImageBytes)
	}
	decision.Bytes += int64(len(baseTombstones) * queryReadyDeltaTombstoneBytes)
	// Re-evaluate total-work axes now that the base's internal multipart work
	// is accounted for. Delta-only generation triggering above is preserved.
	decision.PartLimitTriggers = 0
	decision.RowLimitTriggers = 0
	decision.ByteLimitTriggers = 0
	if decision.GenerationLimitTriggers == 0 {
		decision.Triggered = false
		decision.Reason = ""
	}
	if policy == (QueryReadyDeltaBoundPolicy{}) {
		policy = DefaultQueryReadyDeltaBoundPolicy()
	}
	if policy.MaxAccumulatedDeltaParts > 0 && decision.AccumulatedDeltaParts > policy.MaxAccumulatedDeltaParts {
		decision.Triggered = true
		decision.PartLimitTriggers = 1
		if decision.Reason == "" {
			decision.Reason = "accumulated_delta_parts"
		}
	}
	if policy.MaxRows > 0 && decision.Rows > policy.MaxRows {
		decision.Triggered = true
		decision.RowLimitTriggers = 1
		if decision.Reason == "" {
			decision.Reason = "rows"
		}
	}
	if policy.MaxBytes > 0 && decision.Bytes > policy.MaxBytes {
		decision.Triggered = true
		decision.ByteLimitTriggers = 1
		if decision.Reason == "" {
			decision.Reason = "bytes"
		}
	}
	return decision
}

type QueryReadyBaseDeltaOptions struct {
	SnapshotGeneration uint64
	Bound              QueryReadyDeltaBoundPolicy
}

type QueryReadyBaseDeltaStats struct {
	VisibleBaseGenerations        int
	VisibleDeltaGenerations       int
	BaseInternalParts             int
	OriginBaseParts               int
	BaseDeltaDerivedParts         int
	DeltaParts                    int
	TotalParts                    int
	AccumulatedDeltaParts         int
	RowsMerged                    int
	TombstonesApplied             int
	PartsDecoded                  int
	BytesDecoded                  int64
	BytesCopied                   int64
	LocalDictionaryDecodes        int
	GlobalDictionaryConstructions int
	CodeTranslations              int
	ThresholdTriggers             int
	GenerationLimitTriggers       int
	PartLimitTriggers             int
	RowLimitTriggers              int
	ByteLimitTriggers             int
	Fallbacks                     int
}

type QueryReadyBaseDeltaReader struct {
	reader       *PartSetReader
	dictionaries []map[string]map[int64]string
	executions   []QueryReadyExecutionPartView
	domains      map[string]queryReadyExecutionDomain
	stats        QueryReadyBaseDeltaStats
}

func (r *QueryReadyBaseDeltaReader) Stats() QueryReadyBaseDeltaStats {
	if r == nil {
		return QueryReadyBaseDeltaStats{}
	}
	return r.stats
}

func (r *QueryReadyBaseDeltaReader) ValueAtLatest(primaryID int64, column string) (int64, bool, error) {
	if r == nil || r.reader == nil {
		return 0, false, nil
	}
	return r.reader.ValueAtLatest(primaryID, column)
}

func (r *QueryReadyBaseDeltaReader) NullableInt64AtLatest(primaryID int64, column string) (value int64, null, defaulted, ok bool, err error) {
	if r == nil || r.reader == nil {
		return 0, false, false, false, nil
	}
	return r.reader.NullableInt64AtLatest(primaryID, column)
}

// DictionaryValueAtLatest resolves a low-cardinality code in the selected
// row's part-local domain. Divergent code assignments across parts therefore
// retain their semantic values without a global dictionary or code rewrite.
func (r *QueryReadyBaseDeltaReader) DictionaryValueAtLatest(primaryID int64, column string) (string, bool, error) {
	if r == nil || r.reader == nil {
		return "", false, nil
	}
	ref, ok := r.reader.latest[primaryID]
	if !ok {
		return "", false, nil
	}
	if ref.PartIndex < 0 || ref.PartIndex >= len(r.dictionaries) {
		return "", false, fmt.Errorf("typedcolumn: dictionary row ref part index=%d outside %d", ref.PartIndex, len(r.dictionaries))
	}
	code, err := r.reader.valueAtRowRef(ref, column)
	if err != nil {
		return "", false, err
	}
	values, ok := r.dictionaries[ref.PartIndex][column]
	if !ok {
		return "", false, fmt.Errorf("typedcolumn: missing part-local dictionary for column %s", column)
	}
	value, ok := values[code]
	if !ok {
		return "", false, fmt.Errorf("typedcolumn: part-local dictionary column %s missing code %d", column, code)
	}
	return value, true, nil
}

func NewQueryReadyBaseDeltaReader(base *QueryReadyBaseGeneration, deltas []*QueryReadyDeltaGeneration, opts QueryReadyBaseDeltaOptions) (*QueryReadyBaseDeltaReader, error) {
	if base == nil {
		return nil, errors.New("typedcolumn: nil query-ready base")
	}
	return newQueryReadyBaseDeltaReader(base, nil, len(base.Parts), 0, deltas, opts)
}

func NewQueryReadyConsolidatedBaseDeltaReader(base *QueryReadyDeltaGeneration, deltas []*QueryReadyDeltaGeneration, opts QueryReadyBaseDeltaOptions) (*QueryReadyBaseDeltaReader, error) {
	if base == nil || base.Kind != QueryReadyGenerationConsolidatedBase || base.Base == nil {
		return nil, errors.New("typedcolumn: invalid consolidated query-ready base")
	}
	return newQueryReadyBaseDeltaReader(base.Base, base.Tombstones, base.OriginBaseParts, base.AccumulatedDeltaParts, deltas, opts)
}

func newQueryReadyBaseDeltaReader(base *QueryReadyBaseGeneration, baseTombstones []Tombstone, baseOriginParts, baseAccumulatedDeltaParts int, deltas []*QueryReadyDeltaGeneration, opts QueryReadyBaseDeltaOptions) (*QueryReadyBaseDeltaReader, error) {
	if opts.SnapshotGeneration == 0 {
		return nil, errors.New("typedcolumn: query-ready snapshot generation is zero")
	}
	if base.Identity.Generation > opts.SnapshotGeneration {
		return nil, fmt.Errorf("typedcolumn: query-ready base generation=%d exceeds snapshot=%d", base.Identity.Generation, opts.SnapshotGeneration)
	}
	selected := make([]*QueryReadyDeltaGeneration, 0, len(deltas))
	seenGeneration := make(map[uint64]struct{}, len(deltas))
	for i, delta := range deltas {
		if delta == nil || delta.Base == nil || delta.Kind != QueryReadyGenerationDelta {
			return nil, fmt.Errorf("typedcolumn: invalid query-ready delta at index %d", i)
		}
		if delta.Identity.SchemaHash != base.Identity.SchemaHash {
			return nil, fmt.Errorf("typedcolumn: query-ready delta generation=%d schema mismatch", delta.Identity.Generation)
		}
		if delta.Identity.Generation <= base.Identity.Generation {
			return nil, fmt.Errorf("typedcolumn: query-ready delta generation=%d not newer than base=%d", delta.Identity.Generation, base.Identity.Generation)
		}
		if _, exists := seenGeneration[delta.Identity.Generation]; exists {
			return nil, fmt.Errorf("typedcolumn: duplicate query-ready delta generation=%d", delta.Identity.Generation)
		}
		seenGeneration[delta.Identity.Generation] = struct{}{}
		if delta.Identity.Generation <= opts.SnapshotGeneration {
			selected = append(selected, delta)
		}
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Identity.Generation < selected[j].Identity.Generation })
	decision := evaluateQueryReadyBaseDeltaBound(base, baseTombstones, baseOriginParts, baseAccumulatedDeltaParts, selected, opts.SnapshotGeneration, opts.Bound)
	if decision.Triggered {
		return nil, &QueryReadyDeltaBoundError{Phase: "merge", Decision: decision}
	}
	refs := make([]PartRef, 0, len(base.Parts)+len(selected))
	dictionaries := make([]map[string]map[int64]string, 0, len(base.Parts)+len(selected))
	executions := make([]QueryReadyExecutionPartView, 0, len(base.Parts)+len(selected))
	tombstones := append([]Tombstone(nil), baseTombstones...)
	stats := QueryReadyBaseDeltaStats{
		VisibleBaseGenerations: 1, VisibleDeltaGenerations: len(selected),
		BaseInternalParts: decision.BaseInternalParts, OriginBaseParts: decision.OriginBaseParts,
		BaseDeltaDerivedParts: decision.BaseDeltaDerivedParts, DeltaParts: decision.DeltaParts,
		TotalParts: decision.TotalParts, AccumulatedDeltaParts: decision.AccumulatedDeltaParts,
	}
	appendParts := func(role PartRole, parts []QueryReadyBasePartView) error {
		for _, view := range parts {
			part, err := ColumnPartFromImage(view.Image)
			if err != nil {
				return err
			}
			refs = append(refs, PartRef{Role: role, GenerationID: view.Dependency.SourceGeneration, Part: part, PrimaryIDMode: view.Dependency.PrimaryIDMode, PrimaryIDBase: view.Dependency.PrimaryIDBase})
			executions = append(executions, view.Execution)
			decoded, err := view.Image.Dictionaries()
			if err != nil {
				return err
			}
			inverse := make(map[string]map[int64]string, len(decoded))
			for column, values := range decoded {
				byCode := make(map[int64]string, len(values))
				for value, code := range values {
					byCode[code] = value
				}
				inverse[column] = byCode
				stats.LocalDictionaryDecodes++
			}
			dictionaries = append(dictionaries, inverse)
			stats.PartsDecoded++
			stats.RowsMerged += view.Dependency.Rows
			stats.BytesDecoded += int64(view.Dependency.ImageBytes)
		}
		return nil
	}
	if err := appendParts(PartRoleBase, base.Parts); err != nil {
		return nil, fmt.Errorf("typedcolumn: decode query-ready base: %w", err)
	}
	for _, delta := range selected {
		if err := appendParts(PartRoleDelta, delta.Base.Parts); err != nil {
			return nil, fmt.Errorf("typedcolumn: decode query-ready delta %d: %w", delta.Identity.Generation, err)
		}
		tombstones = append(tombstones, delta.Tombstones...)
	}
	reader, err := NewPartSetReader(refs, tombstones)
	if err != nil {
		return nil, err
	}
	stats.TombstonesApplied = len(tombstones)
	return &QueryReadyBaseDeltaReader{reader: reader, dictionaries: dictionaries, executions: executions, stats: stats}, nil
}

type QueryReadyConsolidationStats struct {
	SelectedDeltaGenerations int
	InputGenerations         int
	OutputGenerations        int
	PartsMerged              int
	RowsMerged               int64
	TombstonesMerged         int
	InputBytes               int64
	OutputBytes              int64
	BytesCopied              int64
	BytesHashed              int64
	BytesChecksummed         int64
	ExecutionBytes           int64
	ExecutionColumns         int
	WriteAmplification       float64
	PeakEncodedBufferBytes   int64
	CodeTranslations         int
	DocumentMaterializations int
	Fallbacks                int
	BuildTime                time.Duration
}

type QueryReadyConsolidationResult struct {
	Bytes        []byte
	Dependencies []QueryReadyBaseDependency
	Stats        QueryReadyConsolidationStats
}

// ConsolidateQueryReadyBaseDelta deterministically folds the selected delta
// prefix into one standalone replacement base envelope. It preserves encoded
// part images and local dictionary domains byte-for-byte and persists the
// latest tombstones; no document materialization or lifecycle publication is
// performed. Existing base/delta objects remain valid for old snapshots.
func ConsolidateQueryReadyBaseDelta(base *QueryReadyBaseGeneration, deltas []*QueryReadyDeltaGeneration, throughGeneration uint64) (QueryReadyConsolidationResult, error) {
	return ConsolidateQueryReadyBaseDeltaWithPolicy(base, deltas, throughGeneration, DefaultQueryReadyDeltaBoundPolicy())
}

func ConsolidateQueryReadyBaseDeltaWithPolicy(base *QueryReadyBaseGeneration, deltas []*QueryReadyDeltaGeneration, throughGeneration uint64, policy QueryReadyDeltaBoundPolicy) (QueryReadyConsolidationResult, error) {
	if base == nil {
		return QueryReadyConsolidationResult{}, errors.New("typedcolumn: nil query-ready base")
	}
	return consolidateQueryReadyBaseDelta(base, nil, len(base.Parts), 0, deltas, throughGeneration, policy)
}

func ConsolidateQueryReadyConsolidatedBaseDelta(base *QueryReadyDeltaGeneration, deltas []*QueryReadyDeltaGeneration, throughGeneration uint64) (QueryReadyConsolidationResult, error) {
	return ConsolidateQueryReadyConsolidatedBaseDeltaWithPolicy(base, deltas, throughGeneration, DefaultQueryReadyDeltaBoundPolicy())
}

func ConsolidateQueryReadyConsolidatedBaseDeltaWithPolicy(base *QueryReadyDeltaGeneration, deltas []*QueryReadyDeltaGeneration, throughGeneration uint64, policy QueryReadyDeltaBoundPolicy) (QueryReadyConsolidationResult, error) {
	if base == nil || base.Kind != QueryReadyGenerationConsolidatedBase || base.Base == nil {
		return QueryReadyConsolidationResult{}, errors.New("typedcolumn: invalid consolidated query-ready base")
	}
	return consolidateQueryReadyBaseDelta(base.Base, base.Tombstones, base.OriginBaseParts, base.AccumulatedDeltaParts, deltas, throughGeneration, policy)
}

func consolidateQueryReadyBaseDelta(base *QueryReadyBaseGeneration, baseTombstones []Tombstone, originBaseParts, accumulatedDeltaParts int, deltas []*QueryReadyDeltaGeneration, throughGeneration uint64, policy QueryReadyDeltaBoundPolicy) (QueryReadyConsolidationResult, error) {
	started := time.Now()
	if base == nil {
		return QueryReadyConsolidationResult{}, errors.New("typedcolumn: nil query-ready base")
	}
	if throughGeneration < base.Identity.Generation {
		return QueryReadyConsolidationResult{}, fmt.Errorf("typedcolumn: consolidation generation=%d before base=%d", throughGeneration, base.Identity.Generation)
	}
	selected := make([]*QueryReadyDeltaGeneration, 0, len(deltas))
	seen := make(map[uint64]struct{}, len(deltas))
	for i, delta := range deltas {
		if delta == nil || delta.Kind != QueryReadyGenerationDelta || delta.Base == nil {
			return QueryReadyConsolidationResult{}, fmt.Errorf("typedcolumn: invalid consolidation delta at %d", i)
		}
		if delta.Identity.SchemaHash != base.Identity.SchemaHash {
			return QueryReadyConsolidationResult{}, fmt.Errorf("typedcolumn: consolidation delta %d schema mismatch", delta.Identity.Generation)
		}
		if delta.Identity.Generation <= base.Identity.Generation {
			return QueryReadyConsolidationResult{}, fmt.Errorf("typedcolumn: consolidation delta %d not newer than base %d", delta.Identity.Generation, base.Identity.Generation)
		}
		if _, ok := seen[delta.Identity.Generation]; ok {
			return QueryReadyConsolidationResult{}, fmt.Errorf("typedcolumn: duplicate consolidation delta generation=%d", delta.Identity.Generation)
		}
		seen[delta.Identity.Generation] = struct{}{}
		if delta.Identity.Generation <= throughGeneration {
			selected = append(selected, delta)
		}
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Identity.Generation < selected[j].Identity.Generation })
	wantGeneration := base.Identity.Generation
	if len(selected) > 0 {
		wantGeneration = selected[len(selected)-1].Identity.Generation
	}
	if throughGeneration != wantGeneration {
		return QueryReadyConsolidationResult{}, fmt.Errorf("typedcolumn: consolidation generation=%d overclaims selected prefix ending at %d", throughGeneration, wantGeneration)
	}
	decision := evaluateQueryReadyBaseDeltaBound(base, baseTombstones, originBaseParts, accumulatedDeltaParts, selected, throughGeneration, policy)
	if decision.Triggered {
		return QueryReadyConsolidationResult{}, &QueryReadyDeltaBoundError{Phase: "consolidation", Decision: decision}
	}
	parts := make([]QueryReadyBasePartInput, 0, len(base.Parts))
	tombstones := append([]Tombstone(nil), baseTombstones...)
	inputBytes := int64(len(baseTombstones) * queryReadyDeltaTombstoneBytes)
	for _, view := range base.Parts {
		parts = append(parts, QueryReadyBasePartInput{SourceGeneration: view.Dependency.SourceGeneration, Image: view.Image, PrimaryIDMode: view.Dependency.PrimaryIDMode, PrimaryIDBase: view.Dependency.PrimaryIDBase})
		inputBytes += int64(view.Dependency.ImageBytes)
	}
	for _, delta := range selected {
		for _, view := range delta.Base.Parts {
			parts = append(parts, QueryReadyBasePartInput{SourceGeneration: view.Dependency.SourceGeneration, Image: view.Image, PrimaryIDMode: view.Dependency.PrimaryIDMode, PrimaryIDBase: view.Dependency.PrimaryIDBase})
			inputBytes += int64(view.Dependency.ImageBytes)
		}
		tombstones = append(tombstones, delta.Tombstones...)
	}
	identity := QueryReadyBaseIdentity{Generation: throughGeneration, SchemaHash: base.Identity.SchemaHash}
	built, err := buildQueryReadyDeltaEnvelope(QueryReadyGenerationConsolidatedBase, identity, parts, tombstones, queryReadyDeltaLineage{
		OriginBaseParts: originBaseParts, AccumulatedDeltaParts: accumulatedDeltaParts + decision.DeltaParts,
	})
	if err != nil {
		return QueryReadyConsolidationResult{}, err
	}
	writeAmplification := float64(0)
	if inputBytes > 0 {
		writeAmplification = float64(len(built.Bytes)) / float64(inputBytes)
	}
	return QueryReadyConsolidationResult{Bytes: built.Bytes, Dependencies: built.Dependencies, Stats: QueryReadyConsolidationStats{
		SelectedDeltaGenerations: len(selected), InputGenerations: 1 + len(selected), OutputGenerations: 1,
		PartsMerged: len(parts), RowsMerged: built.Stats.Rows, TombstonesMerged: built.Stats.Tombstones,
		InputBytes: inputBytes, OutputBytes: int64(len(built.Bytes)), BytesCopied: built.Stats.BytesCopied,
		BytesHashed: built.Stats.BytesHashed, BytesChecksummed: built.Stats.BytesChecksummed,
		ExecutionBytes: built.Stats.ExecutionBytes, ExecutionColumns: built.Stats.ExecutionColumns,
		WriteAmplification: writeAmplification, PeakEncodedBufferBytes: built.Stats.PeakEncodedBufferBytes, BuildTime: time.Since(started),
	}}, nil
}
