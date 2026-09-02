package collections

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cespare/xxhash/v2"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

const (
	columnManifestFormatTCS1                    = "tcs1"
	columnManifestIdentityMagic                 = uint32(0x54434d49) // TCMI
	columnManifestIdentityVersion               = uint16(1)
	columnManifestIdentityMagicOffset           = 0
	columnManifestIdentityEncodingVersionOffset = 4
	columnManifestIdentityManifestVersionOffset = 6
	columnManifestIdentityGenerationOffset      = 8
	columnManifestIdentityChecksumOffset        = 16
	columnManifestIdentityReservedOffset        = 24
	columnManifestIdentityReservedSize          = 4
	columnManifestIdentityRecordSize            = columnManifestIdentityReservedOffset + columnManifestIdentityReservedSize
	columnManifestIdentityRecordKey             = "\x00column-manifest/identity"

	// typedcolumn currently encodes sort-key marks with a fixed eight-column cap.
	// Keep typed-column-owned publication validation aligned with that engine cap
	// until the internal mark format grows.
	typedColumnPartSortKeyMaxColumns = 8
)

func newColumnManifestIdentityRecordKey() []byte {
	return []byte(columnManifestIdentityRecordKey)
}

var (
	// ErrColumnManifestIdentityMissing is returned when a published column manifest
	// root is missing its required identity record.
	ErrColumnManifestIdentityMissing = errors.New("collections: column manifest missing identity record")
	// ErrColumnManifestIdentityMalformed is returned when a column manifest
	// identity record has the wrong binary shape.
	ErrColumnManifestIdentityMalformed = errors.New("collections: malformed column manifest identity record")
	// ErrColumnManifestIdentityBadMagic is returned when a column manifest
	// identity record has an unexpected magic value.
	ErrColumnManifestIdentityBadMagic = errors.New("collections: bad column manifest identity magic")
	// ErrColumnManifestIdentityUnsupportedVersion is returned when a column
	// manifest identity record uses an unsupported encoding version.
	ErrColumnManifestIdentityUnsupportedVersion = errors.New("collections: unsupported column manifest identity version")
	// ErrColumnManifestIdentityNonZeroReserved is returned when a column manifest
	// identity record has non-zero reserved trailer bytes.
	ErrColumnManifestIdentityNonZeroReserved = errors.New("collections: non-zero column manifest identity reserved trailer")
)

type ColumnStoreValueType string

const (
	ColumnStoreValueBool    ColumnStoreValueType = "bool"
	ColumnStoreValueInt64   ColumnStoreValueType = "int64"
	ColumnStoreValueFloat32 ColumnStoreValueType = "float32"
	ColumnStoreValueDouble  ColumnStoreValueType = "double"
	ColumnStoreValueString  ColumnStoreValueType = "string"
	ColumnStoreValueInt8    ColumnStoreValueType = "int8"
	ColumnStoreValueUint8   ColumnStoreValueType = "uint8"
	ColumnStoreValueInt16   ColumnStoreValueType = "int16"
	ColumnStoreValueUint16  ColumnStoreValueType = "uint16"
	ColumnStoreValueInt32   ColumnStoreValueType = "int32"
	ColumnStoreValueUint32  ColumnStoreValueType = "uint32"
	ColumnStoreValueUint64  ColumnStoreValueType = "uint64"
	// Float16 and BFloat16 are storage-only raw 16-bit bit payloads.
	ColumnStoreValueFloat16           ColumnStoreValueType = "float16"
	ColumnStoreValueBFloat16          ColumnStoreValueType = "bfloat16"
	ColumnStoreValueUint8Vector       ColumnStoreValueType = "uint8_vector"
	ColumnStoreValueInt8Vector        ColumnStoreValueType = "int8_vector"
	ColumnStoreValueUint16Vector      ColumnStoreValueType = "uint16_vector"
	ColumnStoreValueInt16Vector       ColumnStoreValueType = "int16_vector"
	ColumnStoreValueUint32Vector      ColumnStoreValueType = "uint32_vector"
	ColumnStoreValueInt32Vector       ColumnStoreValueType = "int32_vector"
	ColumnStoreValueUint64Vector      ColumnStoreValueType = "uint64_vector"
	ColumnStoreValueInt64Vector       ColumnStoreValueType = "int64_vector"
	ColumnStoreValueFloat16Vector     ColumnStoreValueType = "float16_vector"
	ColumnStoreValueBFloat16Vector    ColumnStoreValueType = "bfloat16_vector"
	ColumnStoreValueFloat32Vector     ColumnStoreValueType = "float32_vector"
	ColumnStoreValueFloat64Vector     ColumnStoreValueType = "float64_vector"
	ColumnStoreValueByteVector        ColumnStoreValueType = "byte_vector"
	ColumnStoreValuePackedBitVector   ColumnStoreValueType = "packed_bit_vector"
	ColumnStoreValuePackedUint2Vector ColumnStoreValueType = "packed_uint2_vector"
	ColumnStoreValuePackedUint4Vector ColumnStoreValueType = "packed_uint4_vector"
	ColumnStoreValueUint32List        ColumnStoreValueType = "uint32_list"
	ColumnStoreValueBytes             ColumnStoreValueType = "bytes"
	// AdjacencyList is the current compatibility name for graph adjacency data.
	// It must not become the target variable-list datastore primitive; #1984/#1985
	// own the generic uint32_list logical type and raw_uint32_offsets_list encoding.
	ColumnStoreValueAdjacencyList ColumnStoreValueType = "adjacency_list"
)

type ColumnFixedWidthEncoding string

const (
	// Empty means the current compatibility encoding: manifest-style big-endian
	// fixed-width element bytes inside the physical part image.
	ColumnFixedWidthEncodingDefault      ColumnFixedWidthEncoding = ""
	ColumnFixedWidthEncodingLittleEndian ColumnFixedWidthEncoding = "little_endian"
)

type ColumnAdjacencyListLayout string

const (
	// Empty preserves the existing fixed-degree dense adjacency layout selected
	// by adjacency_degree. The #1901 v1 variable-list primitive must be selected
	// explicitly and must not be inferred from a missing degree.
	ColumnAdjacencyListLayoutFixedDense        ColumnAdjacencyListLayout = ""
	ColumnAdjacencyListLayoutUint32OffsetsList ColumnAdjacencyListLayout = "uint32_offsets_list"
)

type ColumnSortDirection string

const (
	ColumnSortAscending  ColumnSortDirection = ""
	ColumnSortDescending ColumnSortDirection = "desc"
)

type ColumnRetainedPayloadPolicy string

type ColumnRetainedPayloadEncoding string

const (
	ColumnRetainedPayloadNonColumn ColumnRetainedPayloadPolicy = "non-column"
	ColumnRetainedPayloadFull      ColumnRetainedPayloadPolicy = "full"
	ColumnRetainedPayloadNone      ColumnRetainedPayloadPolicy = "none"

	// ColumnRetainedPayloadEncodingJSON records retained payload bodies stored as
	// JSON objects after applying the retained-payload policy.
	ColumnRetainedPayloadEncodingJSON = "json"
	// ColumnRetainedPayloadEncodingTemplateV1 records retained payload bodies
	// stored as Template-v1 documents with templates in the collection template root.
	ColumnRetainedPayloadEncodingTemplateV1 = "template-v1"
	// ColumnRetainedPayloadEncodingSemanticStreamV1 records multi-row retained
	// payload bodies as semantic path streams in a dedicated collection root.
	// Side-root blocks may be legacy raw crss1blk blocks or crss1zst blocks
	// containing a zstd-compressed crss1blk payload.
	ColumnRetainedPayloadEncodingSemanticStreamV1 = "semantic-stream-v1"
	// ColumnRetainedPayloadEncodingNone records that no retained body is active.
	ColumnRetainedPayloadEncodingNone = "none"
	// ColumnRetainedPayloadEncodingUnavailable is a reporting sentinel for
	// unsupported or unrecognized retained-payload encoding metadata.
	ColumnRetainedPayloadEncodingUnavailable = "unavailable"
)

type ColumnReconstructionPolicy string

const (
	ColumnReconstructionRetainedPayloadAndColumns ColumnReconstructionPolicy = "retained-payload-and-columns"
)

type ColumnAggregateKind string

const (
	ColumnAggregateCount          ColumnAggregateKind = "count"
	ColumnAggregateGroupHourCount ColumnAggregateKind = "group-hour-count"
	ColumnAggregateMin            ColumnAggregateKind = "min"
	ColumnAggregateMax            ColumnAggregateKind = "max"
	ColumnAggregateSum            ColumnAggregateKind = "sum"
	ColumnAggregateCountDistinct  ColumnAggregateKind = "count-distinct"
)

type ColumnAssetManagerKind string

const (
	// ColumnAssetManagerValueLogShaped is an isolated typed column asset manager
	// that reuses value-log-like segment refs. It must not mean ordinary TreeDB
	// value_vlog ownership.
	ColumnAssetManagerValueLogShaped ColumnAssetManagerKind = "value-log-shaped"
	// ColumnAssetManagerValueLog is kept as a source-compatible alias for the
	// value-log-shaped column asset manager.
	ColumnAssetManagerValueLog = ColumnAssetManagerValueLogShaped

	columnAssetManagerLegacyValueLog ColumnAssetManagerKind = "value-log"
)

type ColumnLocatorStrategy string

const (
	ColumnLocatorStrategySideIndex ColumnLocatorStrategy = "side-index"
	ColumnLocatorStrategyRoot      ColumnLocatorStrategy = "primary-id-row-root"
)

type ColumnStoreProfileSupport string

const (
	ColumnStoreProfileDurableOnly      ColumnStoreProfileSupport = "durable-only"
	ColumnStoreProfileBenchmarkRelaxed ColumnStoreProfileSupport = "benchmark-relaxed"
)

type ColumnStoreTypedColumnCompression string

const (
	ColumnStoreTypedColumnCompressionDefault ColumnStoreTypedColumnCompression = ""
	ColumnStoreTypedColumnCompressionNone    ColumnStoreTypedColumnCompression = "none"
	ColumnStoreTypedColumnCompressionSnappy  ColumnStoreTypedColumnCompression = "snappy"
	ColumnStoreTypedColumnCompressionLZ4     ColumnStoreTypedColumnCompression = "lz4"
	ColumnStoreTypedColumnCompressionZSTD    ColumnStoreTypedColumnCompression = "zstd"
)

// ColumnAssetReadIntegrity controls hot physical column asset read validation.
// It does not change durability, manifest/header/schema validation, or the
// checksum fields written into typed column asset refs.
type ColumnAssetReadIntegrity string

const (
	ColumnAssetReadIntegrityVerify ColumnAssetReadIntegrity = "verify"
	// ColumnAssetReadIntegrityCachedVerify verifies each immutable typed asset
	// ref on first process read, then skips repeated hot-read CRC work for the
	// same ref. Explicit prepared read sessions may also reuse the file identity
	// captured when the segment reader was opened. It is a benchmark-relaxed mode:
	// post-verification file corruption can be missed until the cache entry is
	// evicted, the prepared session is closed, or the process restarts.
	ColumnAssetReadIntegrityCachedVerify ColumnAssetReadIntegrity = "cached_verify"
	// ColumnAssetReadIntegritySkipChecksums is an unsafe relaxed hot-read mode
	// for benchmark/performance attribution. It skips per-read payload checksum
	// verification and may skip redundant per-row tags after the asset header and
	// schema have been validated.
	ColumnAssetReadIntegritySkipChecksums ColumnAssetReadIntegrity = "skip_checksums"
)

type ColumnStoreConfig struct {
	Enabled                                bool                              `json:"enabled,omitempty"`
	Columns                                []ColumnStoreColumn               `json:"columns,omitempty"`
	SortKey                                []ColumnSortKey                   `json:"sort_key,omitempty"`
	AggregateMetadata                      []ColumnAggregateMetadata         `json:"aggregate_metadata,omitempty"`
	RetainedPayload                        ColumnRetainedPayloadPolicy       `json:"retained_payload,omitempty"`
	RetainedPayloadEncoding                ColumnRetainedPayloadEncoding     `json:"retained_payload_encoding,omitempty"`
	Reconstruction                         ColumnReconstructionPolicy        `json:"reconstruction,omitempty"`
	AssetManager                           *ColumnAssetManagerConfig         `json:"asset_manager,omitempty"`
	ManifestRoot                           *ColumnManifestRootDescriptor     `json:"manifest_root,omitempty"`
	ActiveManifest                         *ColumnManifestIdentity           `json:"active_manifest,omitempty"`
	RecoveryAuthoritativeManifest          *ColumnManifestIdentity           `json:"recovery_authoritative_manifest,omitempty"`
	RecoveryAuthoritativeAppliedCommandLSN uint64                            `json:"recovery_authoritative_applied_command_lsn,omitempty"`
	PhysicalMutationParts                  uint64                            `json:"physical_mutation_parts,omitempty"`
	ProfileSupport                         ColumnStoreProfileSupport         `json:"profile_support,omitempty"`
	TypedColumnCompression                 ColumnStoreTypedColumnCompression `json:"typed_column_compression,omitempty"`
	TypedColumnSectionCompression          ColumnStoreTypedColumnCompression `json:"typed_column_section_compression,omitempty"`
	Locator                                *ColumnLocatorConfig              `json:"locator,omitempty"`
	ControlRootStoragePolicy               RootStoragePolicy                 `json:"control_root_storage_policy,omitempty"`
	SchemaHash                             uint64                            `json:"schema_hash,omitempty"`
}

type ColumnStoreColumn struct {
	Name      string               `json:"name"`
	Path      string               `json:"path"`
	ValueType ColumnStoreValueType `json:"value_type"`
	// Owner is typed-storage metadata. The zero value preserves compatibility and
	// resolves to typed_row_asset; typed_column_part is opt-in/experimental.
	Owner      TypedStorageFieldOwner `json:"owner,omitempty"`
	Nullable   bool                   `json:"nullable,omitempty"`
	Dictionary bool                   `json:"dictionary,omitempty"`
	VectorDims int                    `json:"vector_dims,omitempty"`
	// ElementsPerRow is the generic logical row width for dense numeric and
	// packed-code typed-column vector payloads.
	ElementsPerRow int `json:"elements_per_row,omitempty"`
	// BytesPerRow is the fixed physical row width for byte_vector/fixed_bytes
	// typed-column payloads.
	BytesPerRow int `json:"bytes_per_row,omitempty"`
	// BitsPerElement records packed-code element width. For packed_uint{1,2,4}_vector
	// value types it is optional metadata and, when present, must match the type.
	BitsPerElement int `json:"bits_per_element,omitempty"`
	// AdjacencyDegree is the fixed number of uint32 neighbors per row for the
	// legacy/fallback dense adjacency_list typed_column_part layout.
	AdjacencyDegree int `json:"adjacency_degree,omitempty"`
	// AdjacencyLayout selects the explicit adjacency_list physical layout. Empty
	// means fixed dense compatibility; uint32_offsets_list is the current
	// quarantined compatibility selector for adapter writer/fallback/direct reads,
	// not the generic uint32_list API.
	AdjacencyLayout    ColumnAdjacencyListLayout `json:"adjacency_layout,omitempty"`
	FixedWidthEncoding ColumnFixedWidthEncoding  `json:"fixed_width_encoding,omitempty"`
}

type ColumnSortKey struct {
	Column    string              `json:"column"`
	Direction ColumnSortDirection `json:"direction,omitempty"`
}

type ColumnAggregateMetadata struct {
	Name        string                         `json:"name"`
	Column      string                         `json:"column,omitempty"`
	GroupColumn string                         `json:"group_column,omitempty"`
	Kind        ColumnAggregateKind            `json:"kind"`
	Predicates  []ColumnPhysicalQueryPredicate `json:"predicates,omitempty"`
}

type ColumnAssetManagerConfig struct {
	// Kind names the typed column asset manager backend. The V1 value-log-shaped
	// backend owns an isolated column asset namespace; it is not ordinary value_vlog.
	Kind              ColumnAssetManagerKind `json:"kind,omitempty"`
	IsolatedNamespace bool                   `json:"isolated_namespace,omitempty"`
	Namespace         string                 `json:"namespace,omitempty"`
}

type ColumnManifestRootDescriptor struct {
	Name          string            `json:"name,omitempty"`
	StoragePolicy RootStoragePolicy `json:"storage_policy,omitempty"`
}

type ColumnManifestIdentity struct {
	Generation uint64 `json:"generation,omitempty"`
	Format     string `json:"format,omitempty"`
	Version    uint16 `json:"version,omitempty"`
	Checksum   uint64 `json:"checksum,omitempty"`
}

type ColumnLocatorConfig struct {
	Strategy ColumnLocatorStrategy `json:"strategy,omitempty"`
}

type ColumnStoreCacheIdentity struct {
	Collection                             string
	SchemaHash                             uint64
	CatalogSystemRoot                      uint64
	CatalogCommitSeq                       uint64
	ManifestGeneration                     uint64
	ManifestChecksum                       uint64
	RecoveryAuthoritativeGeneration        uint64
	RecoveryAuthoritativeChecksum          uint64
	RecoveryAuthoritativeAppliedCommandLSN uint64
	ManifestRoot                           uint64
	ManifestRootName                       string
}

type ColumnCacheEntryKind string

const (
	ColumnCacheEntryMarks        ColumnCacheEntryKind = "marks"
	ColumnCacheEntryDictionary   ColumnCacheEntryKind = "dictionary"
	ColumnCacheEntryDecodedBlock ColumnCacheEntryKind = "decoded-block"
)

type ColumnAssetCacheIdentity struct {
	Segment  uint32
	Offset   uint64
	Length   uint32
	Checksum uint32
}

type ColumnBlockCacheKey struct {
	Identity ColumnStoreCacheIdentity
	Kind     ColumnCacheEntryKind
	PartID   uint64
	Asset    ColumnAssetCacheIdentity
	Block    uint32
}

type columnManifestIdentityRecord struct {
	Generation uint64
	Version    uint16
	Checksum   uint64
}

func (id ColumnStoreCacheIdentity) BlockKey(kind ColumnCacheEntryKind, partID uint64, asset ColumnAssetCacheIdentity, block uint32) ColumnBlockCacheKey {
	return ColumnBlockCacheKey{
		Identity: id,
		Kind:     kind,
		PartID:   partID,
		Asset:    asset,
		Block:    block,
	}
}

func (c *Collection) ColumnStoreCacheIdentity() (ColumnStoreCacheIdentity, bool) {
	if c == nil {
		return ColumnStoreCacheIdentity{}, false
	}
	c.catalogMu.RLock()
	catalog := c.catalog
	systemRoot := c.catalogSystemRoot
	commitSeq := c.catalogCommitSeq
	c.catalogMu.RUnlock()
	return columnStoreCacheIdentity(catalog, systemRoot, commitSeq)
}

func columnStoreCacheIdentity(catalog *collectionCatalog, systemRoot, commitSeq uint64) (ColumnStoreCacheIdentity, bool) {
	if catalog == nil || catalog.meta.Options.ColumnStore == nil || !catalog.meta.Options.ColumnStore.Enabled {
		return ColumnStoreCacheIdentity{}, false
	}
	cfg := catalog.meta.Options.ColumnStore
	manifestRootName := catalog.columnManifestRootName
	if manifestRootName == "" && cfg.ManifestRoot != nil {
		manifestRootName = cfg.ManifestRoot.Name
	}
	if manifestRootName == "" {
		manifestRootName = collectionColumnManifestRootName(catalog.meta.Name)
	}
	id := ColumnStoreCacheIdentity{
		Collection:        catalog.meta.Name,
		SchemaHash:        cfg.SchemaHash,
		CatalogSystemRoot: systemRoot,
		CatalogCommitSeq:  commitSeq,
		ManifestRoot:      catalog.rootID(manifestRootName),
		ManifestRootName:  manifestRootName,
	}
	if id.SchemaHash == 0 {
		id.SchemaHash = hashColumnStoreSchema(cfg)
	}
	if cfg.ActiveManifest != nil {
		id.ManifestGeneration = cfg.ActiveManifest.Generation
		id.ManifestChecksum = cfg.ActiveManifest.Checksum
	}
	if cfg.RecoveryAuthoritativeManifest != nil {
		id.RecoveryAuthoritativeGeneration = cfg.RecoveryAuthoritativeManifest.Generation
		id.RecoveryAuthoritativeChecksum = cfg.RecoveryAuthoritativeManifest.Checksum
		id.RecoveryAuthoritativeAppliedCommandLSN = cfg.RecoveryAuthoritativeAppliedCommandLSN
	}
	return id, true
}

func validateColumnStoreProfileSupportForDB(db *backenddb.DB, cfg *ColumnStoreConfig, operation string) error {
	if cfg == nil || !cfg.Enabled {
		return nil
	}
	if db == nil {
		return errCollectionDBNil
	}
	profileSupport := cfg.ProfileSupport
	if profileSupport == "" {
		profileSupport = ColumnStoreProfileDurableOnly
	}
	switch db.DurabilityMode() {
	case backenddb.DurabilityDurable:
		return nil
	case backenddb.DurabilityWALOnRelaxed, backenddb.DurabilityWALOffRelaxed:
		if profileSupport == ColumnStoreProfileBenchmarkRelaxed {
			return nil
		}
		return fmt.Errorf("collections: column_store profile support %q requires durable backend for %s (durability=%s)", profileSupport, operation, columnStoreDurabilityModeName(db.DurabilityMode()))
	default:
		return fmt.Errorf("collections: unsupported backend durability mode %s for column_store %s", columnStoreDurabilityModeName(db.DurabilityMode()), operation)
	}
}

func columnStoreDurabilityModeName(mode backenddb.DurabilityMode) string {
	switch mode {
	case backenddb.DurabilityDurable:
		return "durable"
	case backenddb.DurabilityWALOnRelaxed:
		return "wal_on_fast"
	case backenddb.DurabilityWALOffRelaxed:
		return "fast"
	default:
		return fmt.Sprintf("unknown(%d)", mode)
	}
}

func normalizeColumnStoreConfig(collection string, in *ColumnStoreConfig) (*ColumnStoreConfig, error) {
	if in == nil {
		return nil, nil
	}
	if !in.Enabled {
		if columnStoreConfigEmpty(*in) {
			return nil, nil
		}
		return nil, errors.New("collections: column_store metadata requires enabled=true")
	}
	out := in.copy()
	if len(out.Columns) == 0 {
		return nil, errors.New("collections: column_store requires at least one declared column")
	}
	if _, err := backendRootStoragePolicy(out.ControlRootStoragePolicy); err != nil {
		return nil, err
	}
	if out.RetainedPayload == "" {
		out.RetainedPayload = ColumnRetainedPayloadNonColumn
	}
	if out.RetainedPayloadEncoding == "" {
		out.RetainedPayloadEncoding = defaultColumnRetainedPayloadEncoding(out.RetainedPayload)
	}
	if out.Reconstruction == "" {
		out.Reconstruction = ColumnReconstructionRetainedPayloadAndColumns
	}
	if out.AssetManager == nil {
		out.AssetManager = &ColumnAssetManagerConfig{}
	}
	if out.AssetManager.Kind == "" {
		out.AssetManager.Kind = ColumnAssetManagerValueLogShaped
	}
	if out.AssetManager.Kind == columnAssetManagerLegacyValueLog {
		out.AssetManager.Kind = ColumnAssetManagerValueLogShaped
	}
	if out.AssetManager.Namespace == "" {
		out.AssetManager.Namespace = defaultColumnAssetNamespace(collection)
	}
	out.AssetManager.IsolatedNamespace = true
	if out.ManifestRoot == nil {
		out.ManifestRoot = &ColumnManifestRootDescriptor{}
	}
	if out.ManifestRoot.Name == "" {
		out.ManifestRoot.Name = collectionColumnManifestRootName(collection)
	}
	if out.ManifestRoot.StoragePolicy != "" && out.ControlRootStoragePolicy == "" {
		out.ControlRootStoragePolicy = out.ManifestRoot.StoragePolicy
	}
	if out.ManifestRoot.StoragePolicy == "" {
		out.ManifestRoot.StoragePolicy = out.ControlRootStoragePolicy
	}
	if out.Locator == nil {
		out.Locator = &ColumnLocatorConfig{Strategy: ColumnLocatorStrategySideIndex}
	}
	if out.Locator.Strategy == "" {
		out.Locator.Strategy = ColumnLocatorStrategySideIndex
	}
	if out.ProfileSupport == "" {
		out.ProfileSupport = ColumnStoreProfileDurableOnly
	}
	typedColumnCompressionDefault := isDefaultColumnStoreTypedColumnCompression(out.TypedColumnCompression)
	typedColumnCompression, err := canonicalColumnStoreTypedColumnCompression("typed_column_compression", out.TypedColumnCompression)
	if err != nil {
		return nil, err
	}
	out.TypedColumnCompression = typedColumnCompression
	if isDefaultColumnStoreTypedColumnCompression(out.TypedColumnSectionCompression) {
		if typedColumnCompressionDefault {
			out.TypedColumnSectionCompression = ColumnStoreTypedColumnCompressionZSTD
		} else {
			out.TypedColumnSectionCompression = out.TypedColumnCompression
		}
	} else {
		typedColumnSectionCompression, err := canonicalColumnStoreTypedColumnCompression("typed_column_section_compression", out.TypedColumnSectionCompression)
		if err != nil {
			return nil, err
		}
		out.TypedColumnSectionCompression = typedColumnSectionCompression
	}
	if out.ActiveManifest != nil {
		normalizeColumnManifestIdentityFormat(out.ActiveManifest)
	}
	if out.RecoveryAuthoritativeManifest != nil {
		normalizeColumnManifestIdentityFormat(out.RecoveryAuthoritativeManifest)
	}
	for i := range out.Columns {
		if out.Columns[i].ValueType == ColumnStoreValueFloat32Vector && out.Columns[i].VectorDims == 0 && out.Columns[i].ElementsPerRow > 0 {
			out.Columns[i].VectorDims = out.Columns[i].ElementsPerRow
		}
		if bitsPerElement, ok := columnStorePackedUintVectorBits(out.Columns[i].ValueType); ok && out.Columns[i].BitsPerElement == 0 {
			out.Columns[i].BitsPerElement = bitsPerElement
		}
	}
	if err := validateColumnStoreConfig(collection, out); err != nil {
		return nil, err
	}
	out.SchemaHash = hashColumnStoreSchema(&out)
	return &out, nil
}

func defaultColumnRetainedPayloadEncoding(policy ColumnRetainedPayloadPolicy) ColumnRetainedPayloadEncoding {
	switch policy {
	case ColumnRetainedPayloadNone:
		return ColumnRetainedPayloadEncodingNone
	case ColumnRetainedPayloadFull:
		return ColumnRetainedPayloadEncodingJSON
	case ColumnRetainedPayloadNonColumn:
		return ColumnRetainedPayloadEncodingSemanticStreamV1
	default:
		return ColumnRetainedPayloadEncodingUnavailable
	}
}

func validateColumnRetainedPayloadEncoding(policy ColumnRetainedPayloadPolicy, encoding ColumnRetainedPayloadEncoding) error {
	switch policy {
	case ColumnRetainedPayloadNone:
		if encoding == ColumnRetainedPayloadEncodingNone {
			return nil
		}
		return fmt.Errorf("collections: retained payload policy %q requires encoding %q, got %q", policy, ColumnRetainedPayloadEncodingNone, encoding)
	case ColumnRetainedPayloadFull:
		if encoding == ColumnRetainedPayloadEncodingJSON {
			return nil
		}
		return fmt.Errorf("collections: retained payload policy %q requires encoding %q, got %q", policy, ColumnRetainedPayloadEncodingJSON, encoding)
	case ColumnRetainedPayloadNonColumn:
		switch encoding {
		case ColumnRetainedPayloadEncodingJSON, ColumnRetainedPayloadEncodingTemplateV1, ColumnRetainedPayloadEncodingSemanticStreamV1:
			return nil
		default:
			return fmt.Errorf("collections: retained payload policy %q does not support encoding %q", policy, encoding)
		}
	default:
		return fmt.Errorf("collections: unsupported retained payload policy %q", policy)
	}
}

func columnRetainedPayloadEffectiveEncoding(cfg *ColumnStoreConfig) ColumnRetainedPayloadEncoding {
	if cfg == nil {
		return ColumnRetainedPayloadEncodingNone
	}
	if cfg.RetainedPayloadEncoding != "" {
		return cfg.RetainedPayloadEncoding
	}
	return defaultColumnRetainedPayloadEncoding(cfg.RetainedPayload)
}

func columnStoreRetainedPayloadUsesTemplateV1(cfg *ColumnStoreConfig) bool {
	return cfg != nil && cfg.Enabled && cfg.RetainedPayload == ColumnRetainedPayloadNonColumn && columnRetainedPayloadEffectiveEncoding(cfg) == ColumnRetainedPayloadEncodingTemplateV1
}

func columnStoreRetainedPayloadUsesSemanticStreamV1(cfg *ColumnStoreConfig) bool {
	return cfg != nil && cfg.Enabled && cfg.RetainedPayload == ColumnRetainedPayloadNonColumn && columnRetainedPayloadEffectiveEncoding(cfg) == ColumnRetainedPayloadEncodingSemanticStreamV1
}

func validateColumnStoreConfig(collection string, cfg ColumnStoreConfig) error {
	if !cfg.Enabled {
		return nil
	}
	columnNames := make(map[string]struct{}, len(cfg.Columns))
	columnTypes := make(map[string]ColumnStoreValueType, len(cfg.Columns))
	columnPaths := make(map[string]struct{}, len(cfg.Columns))
	for i := range cfg.Columns {
		col := cfg.Columns[i]
		if err := ValidateIndexName(col.Name); err != nil {
			return fmt.Errorf("collections: invalid column %q name: %w", col.Name, err)
		}
		if err := ValidateIndexPath(col.Path); err != nil {
			return fmt.Errorf("collections: invalid column %q path: %w", col.Name, err)
		}
		valueType, err := normalizeColumnStoreValueType(col.ValueType)
		if err != nil {
			return fmt.Errorf("collections: invalid column %q value_type: %w", col.Name, err)
		}
		owner, err := columnStoreColumnOwner(col)
		if err != nil {
			return fmt.Errorf("collections: invalid column %q owner: %w", col.Name, err)
		}
		if owner != TypedStorageOwnerRowAsset && owner != TypedStorageOwnerColumnPart {
			return fmt.Errorf("collections: invalid column %q owner: %s is not a typed asset owner", col.Name, owner)
		}
		fixedWidthEncoding, err := normalizeColumnFixedWidthEncoding(col.FixedWidthEncoding)
		if err != nil {
			return fmt.Errorf("collections: invalid column %q fixed_width_encoding: %w", col.Name, err)
		}
		adjacencyLayout, err := normalizeColumnAdjacencyListLayout(col.AdjacencyLayout)
		if err != nil {
			return fmt.Errorf("collections: invalid column %q adjacency_layout: %w", col.Name, err)
		}
		if adjacencyLayout != ColumnAdjacencyListLayoutFixedDense && valueType != ColumnStoreValueAdjacencyList {
			return fmt.Errorf("collections: invalid column %q adjacency_layout: only adjacency_list columns may set adjacency_layout", col.Name)
		}
		if fixedWidthEncoding != ColumnFixedWidthEncodingDefault && !columnStoreValueTypeSupportsFixedWidthEncoding(valueType) {
			return fmt.Errorf("collections: invalid column %q fixed_width_encoding: unsupported for value_type %q", col.Name, valueType)
		}
		if fixedWidthEncoding != ColumnFixedWidthEncodingDefault && columnStoreValueTypeHasScalarFixedWidthPayload(valueType) {
			if owner != TypedStorageOwnerColumnPart {
				return fmt.Errorf("collections: invalid column %q fixed_width_encoding: %s raw fixed-width encoding requires owner %q", col.Name, valueType, TypedStorageOwnerColumnPart)
			}
			if col.Nullable {
				return fmt.Errorf("collections: invalid column %q fixed_width_encoding: nullable %s raw fixed-width encoding is unsupported", col.Name, valueType)
			}
		}
		if columnStoreValueTypeIsPrimitiveScalar(valueType) && owner == TypedStorageOwnerColumnPart && col.Nullable {
			return fmt.Errorf("collections: invalid column %q nullable %s typed_column_part is unsupported", col.Name, valueType)
		}
		if valueType == ColumnStoreValueFloat32Vector {
			if col.VectorDims < 0 {
				return fmt.Errorf("collections: invalid column %q vector_dims: must be non-negative for float32_vector", col.Name)
			}
			if col.ElementsPerRow < 0 {
				return fmt.Errorf("collections: invalid column %q elements_per_row: must be non-negative for float32_vector", col.Name)
			}
			if col.VectorDims <= 0 && col.ElementsPerRow <= 0 {
				return fmt.Errorf("collections: invalid column %q vector_dims: must be positive", col.Name)
			}
			if col.VectorDims > 0 && col.ElementsPerRow > 0 && col.VectorDims != col.ElementsPerRow {
				return fmt.Errorf("collections: invalid column %q elements_per_row: must match vector_dims for float32_vector", col.Name)
			}
		} else if col.VectorDims != 0 {
			return fmt.Errorf("collections: invalid column %q vector_dims: only float32_vector columns may set vector_dims", col.Name)
		}
		if columnStoreValueTypeIsDenseNumericVector(valueType) {
			if owner != TypedStorageOwnerColumnPart {
				return fmt.Errorf("collections: invalid column %q value_type %q requires owner %q", col.Name, valueType, TypedStorageOwnerColumnPart)
			}
			if col.Nullable {
				return fmt.Errorf("collections: invalid column %q nullable %s typed_column_part is unsupported", col.Name, valueType)
			}
			if col.ElementsPerRow <= 0 {
				return fmt.Errorf("collections: invalid column %q elements_per_row: must be positive for value_type %q", col.Name, valueType)
			}
		} else if columnStoreValueTypeIsPackedUintVector(valueType) {
			if owner != TypedStorageOwnerColumnPart {
				return fmt.Errorf("collections: invalid column %q value_type %q requires owner %q", col.Name, valueType, TypedStorageOwnerColumnPart)
			}
			if col.Nullable {
				return fmt.Errorf("collections: invalid column %q nullable %s typed_column_part is unsupported", col.Name, valueType)
			}
			if col.ElementsPerRow <= 0 {
				return fmt.Errorf("collections: invalid column %q elements_per_row: must be positive for value_type %q", col.Name, valueType)
			}
			wantBits, _ := columnStorePackedUintVectorBits(valueType)
			if col.BitsPerElement != 0 && col.BitsPerElement != wantBits {
				return fmt.Errorf("collections: invalid column %q bits_per_element=%d want %d for value_type %q", col.Name, col.BitsPerElement, wantBits, valueType)
			}
		} else if valueType != ColumnStoreValueFloat32Vector && col.ElementsPerRow != 0 {
			return fmt.Errorf("collections: invalid column %q elements_per_row: only dense or packed vector columns may set elements_per_row", col.Name)
		}
		if valueType == ColumnStoreValueByteVector {
			if owner != TypedStorageOwnerColumnPart {
				return fmt.Errorf("collections: invalid column %q value_type %q requires owner %q", col.Name, valueType, TypedStorageOwnerColumnPart)
			}
			if col.Nullable {
				return fmt.Errorf("collections: invalid column %q nullable byte_vector typed_column_part is unsupported", col.Name)
			}
			if col.BytesPerRow <= 0 {
				return fmt.Errorf("collections: invalid column %q bytes_per_row: must be positive for byte_vector", col.Name)
			}
		} else if col.BytesPerRow != 0 {
			return fmt.Errorf("collections: invalid column %q bytes_per_row: only byte_vector columns may set bytes_per_row", col.Name)
		}
		if !columnStoreValueTypeIsPackedUintVector(valueType) && col.BitsPerElement != 0 {
			return fmt.Errorf("collections: invalid column %q bits_per_element: only packed_uint vector columns may set bits_per_element", col.Name)
		}
		if valueType == ColumnStoreValueUint32List {
			if col.Nullable {
				return fmt.Errorf("collections: invalid column %q nullable uint32_list is unsupported", col.Name)
			}
			if col.AdjacencyDegree != 0 {
				return fmt.Errorf("collections: invalid column %q adjacency_degree: only adjacency_list columns may set adjacency_degree", col.Name)
			}
			if fixedWidthEncoding != ColumnFixedWidthEncodingDefault {
				return fmt.Errorf("collections: invalid column %q fixed_width_encoding: unsupported for value_type %q", col.Name, valueType)
			}
		}
		if valueType == ColumnStoreValueBytes {
			if col.Nullable {
				return fmt.Errorf("collections: invalid column %q nullable bytes is unsupported", col.Name)
			}
			if owner != TypedStorageOwnerColumnPart {
				return fmt.Errorf("collections: invalid column %q bytes requires owner %q", col.Name, TypedStorageOwnerColumnPart)
			}
			if col.AdjacencyDegree != 0 {
				return fmt.Errorf("collections: invalid column %q adjacency_degree: only adjacency_list columns may set adjacency_degree", col.Name)
			}
			if adjacencyLayout != ColumnAdjacencyListLayoutFixedDense {
				return fmt.Errorf("collections: invalid column %q adjacency_layout: only adjacency_list columns may set adjacency_layout", col.Name)
			}
			if fixedWidthEncoding != ColumnFixedWidthEncodingDefault {
				return fmt.Errorf("collections: invalid column %q fixed_width_encoding: unsupported for value_type %q", col.Name, valueType)
			}
		}
		if valueType == ColumnStoreValueAdjacencyList {
			if col.AdjacencyDegree < 0 {
				return fmt.Errorf("collections: invalid column %q adjacency_degree: must be non-negative", col.Name)
			}
			switch adjacencyLayout {
			case ColumnAdjacencyListLayoutFixedDense:
				if owner != TypedStorageOwnerColumnPart {
					if col.AdjacencyDegree != 0 {
						return fmt.Errorf("collections: invalid column %q adjacency_degree: only adjacency_list typed_column_part columns may set adjacency_degree", col.Name)
					}
				} else {
					if col.Nullable {
						return fmt.Errorf("collections: invalid column %q nullable adjacency_list typed_column_part is unsupported", col.Name)
					}
					if col.AdjacencyDegree <= 0 {
						return fmt.Errorf("collections: invalid column %q adjacency_degree: must be positive for adjacency_list typed_column_part", col.Name)
					}
				}
			case ColumnAdjacencyListLayoutUint32OffsetsList:
				if owner != TypedStorageOwnerColumnPart {
					return fmt.Errorf("collections: invalid column %q adjacency_layout: uint32_offsets_list requires owner %q", col.Name, TypedStorageOwnerColumnPart)
				}
				if col.Nullable {
					return fmt.Errorf("collections: invalid column %q nullable adjacency_list typed_column_part is unsupported", col.Name)
				}
				if col.AdjacencyDegree != 0 {
					return fmt.Errorf("collections: invalid column %q adjacency_degree: must be zero for adjacency_layout %q", col.Name, adjacencyLayout)
				}
				if fixedWidthEncoding != ColumnFixedWidthEncodingDefault {
					return fmt.Errorf("collections: invalid column %q fixed_width_encoding: unsupported for adjacency_layout %q", col.Name, adjacencyLayout)
				}
			}
		} else if col.AdjacencyDegree != 0 {
			return fmt.Errorf("collections: invalid column %q adjacency_degree: only adjacency_list columns may set adjacency_degree", col.Name)
		}
		if col.Dictionary && valueType != ColumnStoreValueString {
			return fmt.Errorf("collections: invalid column %q dictionary: unsupported for value_type %q", col.Name, valueType)
		}
		if _, ok := columnNames[col.Name]; ok {
			return fmt.Errorf("collections: duplicate column %q", col.Name)
		}
		if _, ok := columnPaths[col.Path]; ok {
			return fmt.Errorf("collections: duplicate column path %q", col.Path)
		}
		columnNames[col.Name] = struct{}{}
		columnTypes[col.Name] = valueType
		columnPaths[col.Path] = struct{}{}
	}
	columnByName := make(map[string]ColumnStoreColumn, len(cfg.Columns))
	for _, col := range cfg.Columns {
		columnByName[col.Name] = col
	}
	if uint64(len(cfg.SortKey)) > columnManifestSortKeyMaxColumns {
		return fmt.Errorf("collections: sort key columns=%d exceeds cap %d", len(cfg.SortKey), columnManifestSortKeyMaxColumns)
	}
	sortKeyColumns := make(map[string]struct{}, len(cfg.SortKey))
	allSortKeyColumnsTypedPart := len(cfg.SortKey) != 0
	for _, sortKey := range cfg.SortKey {
		col, ok := columnByName[sortKey.Column]
		if !ok {
			return fmt.Errorf("collections: sort key references unknown column %q", sortKey.Column)
		}
		if _, exists := sortKeyColumns[sortKey.Column]; exists {
			return fmt.Errorf("collections: duplicate sort key column %q", sortKey.Column)
		}
		sortKeyColumns[sortKey.Column] = struct{}{}
		if !columnStoreValueTypeSupportsSort(columnTypes[sortKey.Column]) {
			return fmt.Errorf("collections: sort key column %q value_type %q is not orderable", sortKey.Column, columnTypes[sortKey.Column])
		}
		switch sortKey.Direction {
		case ColumnSortAscending, ColumnSortDescending:
		default:
			return fmt.Errorf("collections: unsupported sort direction %q", sortKey.Direction)
		}
		if !columnStoreColumnIsTypedColumnPart(col) {
			allSortKeyColumnsTypedPart = false
		}
	}
	if allSortKeyColumnsTypedPart {
		if len(cfg.SortKey) > typedColumnPartSortKeyMaxColumns {
			return fmt.Errorf("collections: typed_column_part sort key columns=%d exceeds cap %d", len(cfg.SortKey), typedColumnPartSortKeyMaxColumns)
		}
		for _, sortKey := range cfg.SortKey {
			col := columnByName[sortKey.Column]
			if sortKey.Direction == ColumnSortDescending {
				return fmt.Errorf("collections: descending typed_column_part sort key column %q is not supported yet", sortKey.Column)
			}
			if col.Nullable {
				return fmt.Errorf("collections: typed_column_part sort key column %q is nullable; null/default ordering is not defined", sortKey.Column)
			}
			if !columnStoreValueTypeSupportsTypedColumnPartSort(col.ValueType) {
				return fmt.Errorf("collections: typed_column_part sort key column %q value_type %q is not supported yet", sortKey.Column, col.ValueType)
			}
		}
	}
	aggregateNames := make(map[string]struct{}, len(cfg.AggregateMetadata))
	for _, aggregate := range cfg.AggregateMetadata {
		if err := ValidateIndexName(aggregate.Name); err != nil {
			return fmt.Errorf("collections: invalid aggregate metadata %q name: %w", aggregate.Name, err)
		}
		if aggregate.Column != "" {
			if _, ok := columnNames[aggregate.Column]; !ok {
				return fmt.Errorf("collections: aggregate metadata %q references unknown column %q", aggregate.Name, aggregate.Column)
			}
		}
		if aggregate.GroupColumn != "" {
			if _, ok := columnNames[aggregate.GroupColumn]; !ok {
				return fmt.Errorf("collections: aggregate metadata %q references unknown group column %q", aggregate.Name, aggregate.GroupColumn)
			}
		}
		if err := validateColumnAggregateKind(aggregate.Kind, aggregate.Column); err != nil {
			return fmt.Errorf("collections: invalid aggregate metadata %q: %w", aggregate.Name, err)
		}
		if err := validateColumnAggregateMetadataPhysicalSpec(aggregate, columnTypes); err != nil {
			return err
		}
		if _, err := columnAggregateMetadataCanonicalPredicates(cfg, aggregate.Predicates); err != nil {
			return fmt.Errorf("collections: invalid aggregate metadata %q predicate coverage: %w", aggregate.Name, err)
		}
		if _, ok := aggregateNames[aggregate.Name]; ok {
			return fmt.Errorf("collections: duplicate aggregate metadata %q", aggregate.Name)
		}
		aggregateNames[aggregate.Name] = struct{}{}
	}
	switch cfg.RetainedPayload {
	case ColumnRetainedPayloadNonColumn, ColumnRetainedPayloadFull, ColumnRetainedPayloadNone:
	default:
		return fmt.Errorf("collections: unsupported retained payload policy %q", cfg.RetainedPayload)
	}
	if err := validateColumnRetainedPayloadEncoding(cfg.RetainedPayload, cfg.RetainedPayloadEncoding); err != nil {
		return err
	}
	switch cfg.Reconstruction {
	case ColumnReconstructionRetainedPayloadAndColumns:
	default:
		return fmt.Errorf("collections: unsupported reconstruction policy %q", cfg.Reconstruction)
	}
	if cfg.AssetManager == nil {
		return errors.New("collections: column_store requires asset manager metadata")
	}
	if cfg.AssetManager.Kind != ColumnAssetManagerValueLogShaped {
		return fmt.Errorf("collections: unsupported column asset manager %q", cfg.AssetManager.Kind)
	}
	if !cfg.AssetManager.IsolatedNamespace {
		return errors.New("collections: column asset manager requires isolated namespace")
	}
	if _, err := cleanColumnAssetNamespace(cfg.AssetManager.Namespace); err != nil {
		return err
	}
	if cfg.ManifestRoot == nil {
		return errors.New("collections: column_store requires column manifest root descriptor")
	}
	expectedRoot := collectionColumnManifestRootName(collection)
	if cfg.ManifestRoot.Name != expectedRoot {
		return fmt.Errorf("collections: column manifest root descriptor name %q does not match %q", cfg.ManifestRoot.Name, expectedRoot)
	}
	if _, err := backendRootStoragePolicy(cfg.ManifestRoot.StoragePolicy); err != nil {
		return err
	}
	if cfg.ManifestRoot.StoragePolicy != cfg.ControlRootStoragePolicy {
		return fmt.Errorf("collections: column manifest root descriptor storage policy %q does not match control root storage policy %q", cfg.ManifestRoot.StoragePolicy, cfg.ControlRootStoragePolicy)
	}
	switch cfg.ProfileSupport {
	case ColumnStoreProfileDurableOnly, ColumnStoreProfileBenchmarkRelaxed:
	default:
		return fmt.Errorf("collections: unsupported column profile support %q", cfg.ProfileSupport)
	}
	if _, err := parseColumnStoreTypedColumnCompression("typed_column_compression", cfg.TypedColumnCompression); err != nil {
		return err
	}
	if _, err := parseColumnStoreTypedColumnCompression("typed_column_section_compression", cfg.TypedColumnSectionCompression); err != nil {
		return err
	}
	if cfg.Locator == nil {
		return errors.New("collections: column_store requires locator strategy metadata")
	}
	switch cfg.Locator.Strategy {
	case ColumnLocatorStrategySideIndex, ColumnLocatorStrategyRoot:
	default:
		return fmt.Errorf("collections: unsupported column locator strategy %q", cfg.Locator.Strategy)
	}
	if cfg.ActiveManifest != nil {
		if err := validateColumnManifestIdentity(*cfg.ActiveManifest); err != nil {
			return err
		}
	}
	if cfg.RecoveryAuthoritativeManifest != nil {
		if err := validateColumnManifestIdentityFor("recovery-authoritative", *cfg.RecoveryAuthoritativeManifest); err != nil {
			return err
		}
		if cfg.ActiveManifest == nil {
			return errors.New("collections: recovery-authoritative column manifest without active column manifest")
		}
	}
	if cfg.ActiveManifest != nil {
		if cfg.RecoveryAuthoritativeManifest == nil {
			return errors.New("collections: active column manifest requires recovery-authoritative metadata")
		}
		if cfg.RecoveryAuthoritativeAppliedCommandLSN == 0 {
			return errors.New("collections: active column manifest requires recovery-authoritative AppliedCommandLSN")
		}
		if !columnManifestIdentityValueEqual(*cfg.ActiveManifest, *cfg.RecoveryAuthoritativeManifest) {
			return errors.New("collections: recovery-authoritative column manifest must match active column manifest")
		}
	} else if cfg.RecoveryAuthoritativeAppliedCommandLSN != 0 {
		return errors.New("collections: recovery-authoritative AppliedCommandLSN without active column manifest")
	}
	return nil
}

func normalizeColumnStoreValueType(valueType ColumnStoreValueType) (ColumnStoreValueType, error) {
	switch valueType {
	case ColumnStoreValueBool, ColumnStoreValueInt64, ColumnStoreValueFloat32, ColumnStoreValueDouble, ColumnStoreValueString,
		ColumnStoreValueInt8, ColumnStoreValueUint8, ColumnStoreValueInt16, ColumnStoreValueUint16, ColumnStoreValueInt32, ColumnStoreValueUint32, ColumnStoreValueUint64, ColumnStoreValueFloat16, ColumnStoreValueBFloat16,
		ColumnStoreValueUint8Vector, ColumnStoreValueInt8Vector, ColumnStoreValueUint16Vector, ColumnStoreValueInt16Vector, ColumnStoreValueUint32Vector, ColumnStoreValueInt32Vector, ColumnStoreValueUint64Vector, ColumnStoreValueInt64Vector, ColumnStoreValueFloat16Vector, ColumnStoreValueBFloat16Vector, ColumnStoreValueFloat32Vector, ColumnStoreValueFloat64Vector,
		ColumnStoreValueByteVector, ColumnStoreValuePackedBitVector, ColumnStoreValuePackedUint2Vector, ColumnStoreValuePackedUint4Vector,
		ColumnStoreValueUint32List, ColumnStoreValueBytes, ColumnStoreValueAdjacencyList:
		return valueType, nil
	case "":
		return "", errors.New("value_type is required")
	default:
		return "", fmt.Errorf("unsupported value_type %q", valueType)
	}
}

func normalizeColumnFixedWidthEncoding(encoding ColumnFixedWidthEncoding) (ColumnFixedWidthEncoding, error) {
	switch encoding {
	case ColumnFixedWidthEncodingDefault, ColumnFixedWidthEncodingLittleEndian:
		return encoding, nil
	default:
		return "", fmt.Errorf("unsupported fixed_width_encoding %q", encoding)
	}
}

func normalizeColumnAdjacencyListLayout(layout ColumnAdjacencyListLayout) (ColumnAdjacencyListLayout, error) {
	switch layout {
	case ColumnAdjacencyListLayoutFixedDense, ColumnAdjacencyListLayoutUint32OffsetsList:
		return layout, nil
	default:
		return "", fmt.Errorf("unsupported adjacency_layout %q", layout)
	}
}

func columnFixedWidthEncodingIsLittleEndian(encoding ColumnFixedWidthEncoding) (bool, error) {
	normalized, err := normalizeColumnFixedWidthEncoding(encoding)
	if err != nil {
		return false, err
	}
	return normalized == ColumnFixedWidthEncodingLittleEndian, nil
}

func columnStoreColumnOwner(col ColumnStoreColumn) (TypedStorageFieldOwner, error) {
	return normalizeTypedStorageFieldOwner(col.Owner)
}

func columnStoreColumnOwnerOrRowAsset(col ColumnStoreColumn) TypedStorageFieldOwner {
	owner, err := columnStoreColumnOwner(col)
	if err != nil || owner == "" {
		return TypedStorageOwnerRowAsset
	}
	return owner
}

func columnStoreColumnIsTypedColumnPart(col ColumnStoreColumn) bool {
	return columnStoreColumnOwnerOrRowAsset(col) == TypedStorageOwnerColumnPart
}

func columnStoreColumnIsTypedRowAsset(col ColumnStoreColumn) bool {
	return columnStoreColumnOwnerOrRowAsset(col) == TypedStorageOwnerRowAsset
}

func columnStoreValueTypeSupportsFixedWidthEncoding(valueType ColumnStoreValueType) bool {
	switch valueType {
	case ColumnStoreValueInt64, ColumnStoreValueFloat32, ColumnStoreValueDouble, ColumnStoreValueFloat32Vector, ColumnStoreValueAdjacencyList:
		return true
	default:
		return columnStoreValueTypeIsPrimitiveScalar(valueType) || columnStoreValueTypeIsDenseNumericVector(valueType)
	}
}

func columnStoreValueTypeHasScalarFixedWidthPayload(valueType ColumnStoreValueType) bool {
	switch valueType {
	case ColumnStoreValueInt64, ColumnStoreValueFloat32, ColumnStoreValueDouble:
		return true
	default:
		return columnStoreValueTypeIsPrimitiveScalar(valueType)
	}
}

func columnStoreValueTypeSupportsDictionary(valueType ColumnStoreValueType) bool {
	return valueType == ColumnStoreValueString
}

func columnStoreValueTypeSupportsSort(valueType ColumnStoreValueType) bool {
	switch valueType {
	case ColumnStoreValueBool, ColumnStoreValueInt64, ColumnStoreValueFloat32, ColumnStoreValueDouble, ColumnStoreValueString:
		return true
	default:
		return false
	}
}

func columnStoreValueTypeSupportsTypedColumnPartSort(valueType ColumnStoreValueType) bool {
	switch valueType {
	case ColumnStoreValueBool, ColumnStoreValueInt64, ColumnStoreValueString:
		return true
	default:
		return false
	}
}

func validateColumnAggregateKind(kind ColumnAggregateKind, column string) error {
	switch kind {
	case ColumnAggregateCount:
		return nil
	case ColumnAggregateGroupHourCount, ColumnAggregateMin, ColumnAggregateMax, ColumnAggregateSum, ColumnAggregateCountDistinct:
		if column == "" {
			return fmt.Errorf("aggregate kind %q requires a column", kind)
		}
		return nil
	default:
		return fmt.Errorf("unsupported aggregate kind %q", kind)
	}
}

func validateColumnAggregateMetadataPhysicalSpec(aggregate ColumnAggregateMetadata, columnTypes map[string]ColumnStoreValueType) error {
	switch aggregate.Kind {
	case ColumnAggregateCount:
		if aggregate.GroupColumn != "" {
			if aggregate.Column != "" {
				return fmt.Errorf("collections: aggregate metadata %q kind %q does not support a value column", aggregate.Name, aggregate.Kind)
			}
			groupType, ok := columnTypes[aggregate.GroupColumn]
			if !ok {
				return fmt.Errorf("collections: aggregate metadata %q references unknown group column %q", aggregate.Name, aggregate.GroupColumn)
			}
			if groupType != ColumnStoreValueString {
				return fmt.Errorf("collections: aggregate metadata %q group column %q has type %q, want %q", aggregate.Name, aggregate.GroupColumn, groupType, ColumnStoreValueString)
			}
		}
	case ColumnAggregateGroupHourCount:
		valueType, ok := columnTypes[aggregate.Column]
		if !ok {
			return fmt.Errorf("collections: aggregate metadata %q references unknown column %q", aggregate.Name, aggregate.Column)
		}
		if aggregate.GroupColumn == "" {
			return fmt.Errorf("collections: aggregate metadata %q kind %q requires a group column", aggregate.Name, aggregate.Kind)
		}
		groupType, ok := columnTypes[aggregate.GroupColumn]
		if !ok {
			return fmt.Errorf("collections: aggregate metadata %q references unknown group column %q", aggregate.Name, aggregate.GroupColumn)
		}
		if groupType != ColumnStoreValueString {
			return fmt.Errorf("collections: aggregate metadata %q group column %q has type %q, want %q", aggregate.Name, aggregate.GroupColumn, groupType, ColumnStoreValueString)
		}
		if valueType != ColumnStoreValueInt64 {
			return fmt.Errorf("collections: aggregate metadata %q value column %q has type %q, want %q", aggregate.Name, aggregate.Column, valueType, ColumnStoreValueInt64)
		}
	case ColumnAggregateMin, ColumnAggregateMax:
		valueType, ok := columnTypes[aggregate.Column]
		if !ok {
			return fmt.Errorf("collections: aggregate metadata %q references unknown column %q", aggregate.Name, aggregate.Column)
		}
		if valueType == ColumnStoreValueFloat32Vector || columnStoreValueTypeIsDenseNumericVector(valueType) || valueType == ColumnStoreValueUint32List || valueType == ColumnStoreValueBytes || valueType == ColumnStoreValueAdjacencyList {
			return fmt.Errorf("collections: aggregate metadata %q kind %q does not support value_type %q", aggregate.Name, aggregate.Kind, valueType)
		}
		if aggregate.GroupColumn == "" {
			return fmt.Errorf("collections: aggregate metadata %q kind %q requires a group column", aggregate.Name, aggregate.Kind)
		}
		groupType, ok := columnTypes[aggregate.GroupColumn]
		if !ok {
			return fmt.Errorf("collections: aggregate metadata %q references unknown group column %q", aggregate.Name, aggregate.GroupColumn)
		}
		if groupType != ColumnStoreValueString {
			return fmt.Errorf("collections: aggregate metadata %q group column %q has type %q, want %q", aggregate.Name, aggregate.GroupColumn, groupType, ColumnStoreValueString)
		}
		if valueType != ColumnStoreValueInt64 && valueType != ColumnStoreValueFloat32 && valueType != ColumnStoreValueDouble {
			return fmt.Errorf("collections: aggregate metadata %q value column %q has type %q, want %q, %q, or %q", aggregate.Name, aggregate.Column, valueType, ColumnStoreValueInt64, ColumnStoreValueFloat32, ColumnStoreValueDouble)
		}
	case ColumnAggregateSum:
		valueType, ok := columnTypes[aggregate.Column]
		if !ok {
			return fmt.Errorf("collections: aggregate metadata %q references unknown column %q", aggregate.Name, aggregate.Column)
		}
		if valueType != ColumnStoreValueInt64 && valueType != ColumnStoreValueFloat32 && valueType != ColumnStoreValueDouble {
			return fmt.Errorf("collections: aggregate metadata %q kind %q does not support value_type %q", aggregate.Name, aggregate.Kind, valueType)
		}
	case ColumnAggregateCountDistinct:
		valueType, ok := columnTypes[aggregate.Column]
		if !ok {
			return fmt.Errorf("collections: aggregate metadata %q references unknown column %q", aggregate.Name, aggregate.Column)
		}
		if valueType == ColumnStoreValueFloat32Vector || columnStoreValueTypeIsDenseNumericVector(valueType) || valueType == ColumnStoreValueUint32List || valueType == ColumnStoreValueBytes || valueType == ColumnStoreValueAdjacencyList {
			return fmt.Errorf("collections: aggregate metadata %q kind %q does not support value_type %q", aggregate.Name, aggregate.Kind, valueType)
		}
	}
	return nil
}

func validateColumnManifestIdentity(identity ColumnManifestIdentity) error {
	return validateColumnManifestIdentityFor("active", identity)
}

func validateColumnManifestIdentityFor(label string, identity ColumnManifestIdentity) error {
	if identity.Generation == 0 {
		return fmt.Errorf("collections: %s column manifest generation is required", label)
	}
	if identity.Format == "" {
		identity.Format = columnManifestFormatTCS1
	}
	if identity.Format != columnManifestFormatTCS1 {
		return fmt.Errorf("collections: unsupported %s column manifest format %q", label, identity.Format)
	}
	if identity.Version == 0 {
		return fmt.Errorf("collections: %s column manifest version is required", label)
	}
	if identity.Version != columnManifestIdentityVersion {
		return fmt.Errorf("collections: unsupported %s column manifest version %d", label, identity.Version)
	}
	if identity.Checksum == 0 {
		return fmt.Errorf("collections: %s column manifest checksum is required", label)
	}
	return nil
}

// normalizeColumnManifestIdentityDefaults is used by M10A publish-plan
// assembly before fail-closed identity validation.
func normalizeColumnManifestIdentityDefaults(identity *ColumnManifestIdentity) {
	if identity == nil {
		return
	}
	normalizeColumnManifestIdentityFormat(identity)
	if identity.Version == 0 {
		identity.Version = columnManifestIdentityVersion
	}
}

func normalizeColumnManifestIdentityFormat(identity *ColumnManifestIdentity) {
	if identity == nil {
		return
	}
	if identity.Format == "" {
		identity.Format = columnManifestFormatTCS1
	}
}

func columnManifestIdentityValueEqual(a, b ColumnManifestIdentity) bool {
	return a.Generation == b.Generation &&
		a.Format == b.Format &&
		a.Version == b.Version &&
		a.Checksum == b.Checksum
}

func validateColumnStoreCatalogRoot(snap *backenddb.Snapshot, catalog *collectionCatalog) error {
	if catalog == nil || catalog.meta.Options.ColumnStore == nil || !catalog.meta.Options.ColumnStore.Enabled {
		return nil
	}
	identity := catalog.meta.Options.ColumnStore.ActiveManifest
	if identity == nil {
		return nil
	}
	rootName := collectionColumnManifestRootName(catalog.meta.Name)
	rootID := catalog.rootID(rootName)
	if rootID == 0 {
		return fmt.Errorf("collections: active column manifest generation %d for %q is missing root descriptor %q", identity.Generation, catalog.meta.Name, rootName)
	}
	entry, err := snap.GetEntryAtRoot(rootID, newColumnManifestIdentityRecordKey())
	if errors.Is(err, tree.ErrKeyNotFound) {
		return fmt.Errorf("%w: active column manifest root %d for %q", ErrColumnManifestIdentityMissing, rootID, catalog.meta.Name)
	}
	if err != nil {
		return fmt.Errorf("collections: active column manifest root %d for %q is unreadable: %w", rootID, catalog.meta.Name, err)
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return fmt.Errorf("collections: active column manifest root %d for %q has deleted identity record", rootID, catalog.meta.Name)
	}
	record, err := decodeColumnManifestIdentityRecord(entry.Value)
	if err != nil {
		return fmt.Errorf("collections: active column manifest root %d for %q has invalid identity record: %w", rootID, catalog.meta.Name, err)
	}
	if record.Generation != identity.Generation || record.Version != identity.Version || record.Checksum != identity.Checksum {
		return fmt.Errorf("collections: active column manifest identity mismatch for %q", catalog.meta.Name)
	}
	return nil
}

func encodeColumnManifestIdentityRecord(identity ColumnManifestIdentity) []byte {
	record := encodeColumnManifestIdentityRecordArray(identity)
	return record[:]
}

func encodeColumnManifestIdentityRecordArray(identity ColumnManifestIdentity) [columnManifestIdentityRecordSize]byte {
	var out [columnManifestIdentityRecordSize]byte
	binary.BigEndian.PutUint32(out[columnManifestIdentityMagicOffset:columnManifestIdentityEncodingVersionOffset], columnManifestIdentityMagic)
	binary.BigEndian.PutUint16(out[columnManifestIdentityEncodingVersionOffset:columnManifestIdentityManifestVersionOffset], columnManifestIdentityVersion)
	binary.BigEndian.PutUint16(out[columnManifestIdentityManifestVersionOffset:columnManifestIdentityGenerationOffset], identity.Version)
	binary.BigEndian.PutUint64(out[columnManifestIdentityGenerationOffset:columnManifestIdentityChecksumOffset], identity.Generation)
	binary.BigEndian.PutUint64(out[columnManifestIdentityChecksumOffset:columnManifestIdentityReservedOffset], identity.Checksum)
	binary.BigEndian.PutUint32(out[columnManifestIdentityReservedOffset:columnManifestIdentityRecordSize], 0)
	return out
}

func decodeColumnManifestIdentityRecord(raw []byte) (columnManifestIdentityRecord, error) {
	if len(raw) != columnManifestIdentityRecordSize {
		return columnManifestIdentityRecord{}, fmt.Errorf("%w: length=%d", ErrColumnManifestIdentityMalformed, len(raw))
	}
	if magic := binary.BigEndian.Uint32(raw[columnManifestIdentityMagicOffset:columnManifestIdentityEncodingVersionOffset]); magic != columnManifestIdentityMagic {
		return columnManifestIdentityRecord{}, fmt.Errorf("%w: magic=0x%08x", ErrColumnManifestIdentityBadMagic, magic)
	}
	if version := binary.BigEndian.Uint16(raw[columnManifestIdentityEncodingVersionOffset:columnManifestIdentityManifestVersionOffset]); version != columnManifestIdentityVersion {
		return columnManifestIdentityRecord{}, fmt.Errorf("%w: version=%d", ErrColumnManifestIdentityUnsupportedVersion, version)
	}
	if reserved := binary.BigEndian.Uint32(raw[columnManifestIdentityReservedOffset:columnManifestIdentityRecordSize]); reserved != 0 {
		return columnManifestIdentityRecord{}, fmt.Errorf("%w: reserved=0x%08x", ErrColumnManifestIdentityNonZeroReserved, reserved)
	}
	return columnManifestIdentityRecord{
		Version:    binary.BigEndian.Uint16(raw[columnManifestIdentityManifestVersionOffset:columnManifestIdentityGenerationOffset]),
		Generation: binary.BigEndian.Uint64(raw[columnManifestIdentityGenerationOffset:columnManifestIdentityChecksumOffset]),
		Checksum:   binary.BigEndian.Uint64(raw[columnManifestIdentityChecksumOffset:columnManifestIdentityReservedOffset]),
	}, nil
}

func defaultColumnAssetNamespace(collection string) string {
	return collection + "/column-assets"
}

func columnStoreConfigEmpty(cfg ColumnStoreConfig) bool {
	return len(cfg.Columns) == 0 &&
		len(cfg.SortKey) == 0 &&
		len(cfg.AggregateMetadata) == 0 &&
		cfg.RetainedPayload == "" &&
		cfg.RetainedPayloadEncoding == "" &&
		cfg.Reconstruction == "" &&
		cfg.AssetManager == nil &&
		cfg.ManifestRoot == nil &&
		cfg.ActiveManifest == nil &&
		cfg.RecoveryAuthoritativeManifest == nil &&
		cfg.RecoveryAuthoritativeAppliedCommandLSN == 0 &&
		cfg.PhysicalMutationParts == 0 &&
		cfg.ProfileSupport == "" &&
		cfg.TypedColumnCompression == ColumnStoreTypedColumnCompressionDefault &&
		cfg.TypedColumnSectionCompression == ColumnStoreTypedColumnCompressionDefault &&
		cfg.Locator == nil &&
		cfg.ControlRootStoragePolicy == "" &&
		cfg.SchemaHash == 0
}

func cloneColumnSortKeys(sortKeys []ColumnSortKey) []ColumnSortKey {
	if len(sortKeys) == 0 {
		return nil
	}
	return append([]ColumnSortKey(nil), sortKeys...)
}

func (cfg ColumnStoreConfig) copy() ColumnStoreConfig {
	out := cfg
	out.Columns = append([]ColumnStoreColumn(nil), cfg.Columns...)
	out.SortKey = append([]ColumnSortKey(nil), cfg.SortKey...)
	out.AggregateMetadata = cloneColumnAggregateMetadata(cfg.AggregateMetadata)
	if cfg.AssetManager != nil {
		assetManager := *cfg.AssetManager
		out.AssetManager = &assetManager
	}
	if cfg.ManifestRoot != nil {
		manifestRoot := *cfg.ManifestRoot
		out.ManifestRoot = &manifestRoot
	}
	if cfg.ActiveManifest != nil {
		identity := *cfg.ActiveManifest
		out.ActiveManifest = &identity
	}
	if cfg.RecoveryAuthoritativeManifest != nil {
		identity := *cfg.RecoveryAuthoritativeManifest
		out.RecoveryAuthoritativeManifest = &identity
	}
	if cfg.Locator != nil {
		locator := *cfg.Locator
		out.Locator = &locator
	}
	return out
}

func copyCollectionOptions(opts CollectionOptions) CollectionOptions {
	if opts.ColumnStore != nil {
		columnStore := opts.ColumnStore.copy()
		opts.ColumnStore = &columnStore
	}
	return opts
}

func collectionOptionsEqual(a, b CollectionOptions) bool {
	if a.AllowArrayValuesInIndex != b.AllowArrayValuesInIndex ||
		a.DocumentFormat != b.DocumentFormat ||
		a.DataRootStoragePolicy != b.DataRootStoragePolicy ||
		a.IndexStateStoragePolicy != b.IndexStateStoragePolicy ||
		a.DisableIndexedWriteMemtables != b.DisableIndexedWriteMemtables ||
		a.DisableBufferedIndexedAsyncFlush != b.DisableBufferedIndexedAsyncFlush ||
		a.BufferedIndexedWrites != b.BufferedIndexedWrites ||
		a.BufferedIndexedWriteMaxDocuments != b.BufferedIndexedWriteMaxDocuments ||
		a.BufferedIndexedWriteMaxBytes != b.BufferedIndexedWriteMaxBytes ||
		a.BufferedIndexedWriteMaxRootRuns != b.BufferedIndexedWriteMaxRootRuns ||
		a.BufferedIndexedAsyncFlush != b.BufferedIndexedAsyncFlush ||
		a.BufferedIndexedOverlayRoots != b.BufferedIndexedOverlayRoots ||
		a.BufferedIndexedAsyncFlushMaxQueuedUnits != b.BufferedIndexedAsyncFlushMaxQueuedUnits {
		return false
	}
	return columnStoreConfigEqual(a.ColumnStore, b.ColumnStore)
}

func columnStoreConfigEqual(a, b *ColumnStoreConfig) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Enabled != b.Enabled ||
		a.RetainedPayload != b.RetainedPayload ||
		a.RetainedPayloadEncoding != b.RetainedPayloadEncoding ||
		a.Reconstruction != b.Reconstruction ||
		a.ControlRootStoragePolicy != b.ControlRootStoragePolicy ||
		a.RecoveryAuthoritativeAppliedCommandLSN != b.RecoveryAuthoritativeAppliedCommandLSN ||
		a.PhysicalMutationParts != b.PhysicalMutationParts ||
		a.ProfileSupport != b.ProfileSupport ||
		a.TypedColumnCompression != b.TypedColumnCompression ||
		a.TypedColumnSectionCompression != b.TypedColumnSectionCompression ||
		a.SchemaHash != b.SchemaHash ||
		!columnAssetManagerConfigEqual(a.AssetManager, b.AssetManager) ||
		!columnManifestRootDescriptorEqual(a.ManifestRoot, b.ManifestRoot) ||
		!columnManifestIdentityEqual(a.ActiveManifest, b.ActiveManifest) ||
		!columnManifestIdentityEqual(a.RecoveryAuthoritativeManifest, b.RecoveryAuthoritativeManifest) ||
		!columnLocatorConfigEqual(a.Locator, b.Locator) ||
		len(a.Columns) != len(b.Columns) ||
		len(a.SortKey) != len(b.SortKey) ||
		len(a.AggregateMetadata) != len(b.AggregateMetadata) {
		return false
	}
	for i := range a.Columns {
		if a.Columns[i] != b.Columns[i] {
			return false
		}
	}
	for i := range a.SortKey {
		if a.SortKey[i] != b.SortKey[i] {
			return false
		}
	}
	for i := range a.AggregateMetadata {
		if !columnAggregateMetadataEqual(a.AggregateMetadata[i], b.AggregateMetadata[i]) {
			return false
		}
	}
	return true
}

func columnManifestRootDescriptorEqual(a, b *ColumnManifestRootDescriptor) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func columnAssetManagerConfigEqual(a, b *ColumnAssetManagerConfig) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func columnManifestIdentityEqual(a, b *ColumnManifestIdentity) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func columnLocatorConfigEqual(a, b *ColumnLocatorConfig) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func hashColumnStoreSchema(cfg *ColumnStoreConfig) uint64 {
	if cfg == nil {
		return 0
	}
	var d xxhash.Digest
	writeHashString(&d, string(cfg.RetainedPayload))
	writeHashString(&d, string(cfg.RetainedPayloadEncoding))
	writeHashString(&d, string(cfg.Reconstruction))
	writeHashString(&d, string(cfg.ControlRootStoragePolicy))
	writeHashString(&d, string(cfg.TypedColumnCompression))
	writeHashString(&d, string(cfg.TypedColumnSectionCompression))
	if cfg.ManifestRoot != nil {
		writeHashString(&d, cfg.ManifestRoot.Name)
		writeHashString(&d, string(cfg.ManifestRoot.StoragePolicy))
	}
	for _, col := range cfg.Columns {
		writeHashString(&d, col.Name)
		writeHashString(&d, col.Path)
		writeHashString(&d, string(col.ValueType))
		writeHashString(&d, string(columnStoreColumnOwnerOrRowAsset(col)))
		if col.ValueType == ColumnStoreValueFloat32Vector {
			writeHashUint64(&d, uint64(columnStoreFloat32VectorElementsPerRow(col)))
			writeHashUint64(&d, 0)
		} else {
			writeHashUint64(&d, uint64(col.VectorDims))
			writeHashUint64(&d, uint64(col.ElementsPerRow))
		}
		writeHashUint64(&d, uint64(col.BytesPerRow))
		writeHashUint64(&d, uint64(col.BitsPerElement))
		writeHashUint64(&d, uint64(col.AdjacencyDegree))
		if col.AdjacencyLayout != ColumnAdjacencyListLayoutFixedDense {
			writeHashString(&d, string(col.AdjacencyLayout))
		}
		writeHashBool(&d, col.Nullable)
		writeHashBool(&d, col.Dictionary)
		if col.FixedWidthEncoding != ColumnFixedWidthEncodingDefault {
			writeHashString(&d, string(col.FixedWidthEncoding))
		}
	}
	for _, sortKey := range cfg.SortKey {
		writeHashString(&d, sortKey.Column)
		writeHashString(&d, string(sortKey.Direction))
	}
	for _, aggregate := range cfg.AggregateMetadata {
		writeHashString(&d, aggregate.Name)
		writeHashString(&d, aggregate.Column)
		writeHashString(&d, aggregate.GroupColumn)
		writeHashString(&d, string(aggregate.Kind))
		predicateCoverage, err := columnAggregateMetadataCanonicalPredicates(*cfg, aggregate.Predicates)
		if err != nil {
			predicateCoverage = aggregate.Predicates
		}
		writeHashUint64(&d, uint64(len(predicateCoverage)))
		for _, predicate := range predicateCoverage {
			kind := columnPhysicalQueryPredicateKindOrDefault(predicate.Kind)
			writeHashString(&d, predicate.Column)
			writeHashString(&d, string(kind))
			if kind == ColumnPhysicalQueryPredicateInList {
				writeHashUint64(&d, uint64(len(predicate.Values)))
				for _, value := range predicate.Values {
					writeHashString(&d, value)
				}
			} else {
				writeHashUint64(&d, 1)
				writeHashString(&d, predicate.Value)
			}
		}
	}
	if cfg.AssetManager != nil {
		writeHashString(&d, string(cfg.AssetManager.Kind))
		writeHashString(&d, cfg.AssetManager.Namespace)
		writeHashBool(&d, cfg.AssetManager.IsolatedNamespace)
	}
	if cfg.Locator != nil {
		writeHashString(&d, string(cfg.Locator.Strategy))
	}
	return d.Sum64()
}

func writeHashString(d *xxhash.Digest, s string) {
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(s)))
	_, _ = d.Write(lenBuf[:])
	_, _ = d.WriteString(s)
}

func writeHashBytes(d *xxhash.Digest, b []byte) {
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(b)))
	_, _ = d.Write(lenBuf[:])
	_, _ = d.Write(b)
}

func writeHashUint64(d *xxhash.Digest, value uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	_, _ = d.Write(buf[:])
}

func writeHashBool(d *xxhash.Digest, value bool) {
	var b [1]byte
	if value {
		b[0] = 1
	}
	_, _ = d.Write(b[:])
}

func columnManifestIdentityIterator(identity ColumnManifestIdentity) *systemTargetIterator {
	return columnManifestIdentityRecordIterator(encodeColumnManifestIdentityRecordArray(identity))
}

func columnManifestIdentityRecordIterator(record [columnManifestIdentityRecordSize]byte) *systemTargetIterator {
	value := record[:]
	return &systemTargetIterator{entries: []systemTargetEntry{{
		key:   newColumnManifestIdentityRecordKey(),
		value: value,
	}}}
}
