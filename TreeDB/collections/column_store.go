package collections

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

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
)

func newColumnManifestIdentityRecordKey() []byte {
	return []byte(columnManifestIdentityRecordKey)
}

type ColumnStoreValueType string

const (
	ColumnStoreValueBool   ColumnStoreValueType = "bool"
	ColumnStoreValueInt64  ColumnStoreValueType = "int64"
	ColumnStoreValueDouble ColumnStoreValueType = "double"
	ColumnStoreValueString ColumnStoreValueType = "string"
)

type ColumnSortDirection string

const (
	ColumnSortAscending  ColumnSortDirection = ""
	ColumnSortDescending ColumnSortDirection = "desc"
)

type ColumnRetainedPayloadPolicy string

const (
	ColumnRetainedPayloadNonColumn ColumnRetainedPayloadPolicy = "non-column"
	ColumnRetainedPayloadFull      ColumnRetainedPayloadPolicy = "full"
	ColumnRetainedPayloadNone      ColumnRetainedPayloadPolicy = "none"
)

type ColumnReconstructionPolicy string

const (
	ColumnReconstructionRetainedPayloadAndColumns ColumnReconstructionPolicy = "retained-payload-and-columns"
)

type ColumnAggregateKind string

const (
	ColumnAggregateCount         ColumnAggregateKind = "count"
	ColumnAggregateMin           ColumnAggregateKind = "min"
	ColumnAggregateMax           ColumnAggregateKind = "max"
	ColumnAggregateSum           ColumnAggregateKind = "sum"
	ColumnAggregateCountDistinct ColumnAggregateKind = "count-distinct"
)

type ColumnAssetManagerKind string

const (
	ColumnAssetManagerValueLog ColumnAssetManagerKind = "value-log"
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

type ColumnStoreConfig struct {
	Enabled                                bool                          `json:"enabled,omitempty"`
	Columns                                []ColumnStoreColumn           `json:"columns,omitempty"`
	SortKey                                []ColumnSortKey               `json:"sort_key,omitempty"`
	AggregateMetadata                      []ColumnAggregateMetadata     `json:"aggregate_metadata,omitempty"`
	RetainedPayload                        ColumnRetainedPayloadPolicy   `json:"retained_payload,omitempty"`
	Reconstruction                         ColumnReconstructionPolicy    `json:"reconstruction,omitempty"`
	AssetManager                           *ColumnAssetManagerConfig     `json:"asset_manager,omitempty"`
	ManifestRoot                           *ColumnManifestRootDescriptor `json:"manifest_root,omitempty"`
	ActiveManifest                         *ColumnManifestIdentity       `json:"active_manifest,omitempty"`
	RecoveryAuthoritativeManifest          *ColumnManifestIdentity       `json:"recovery_authoritative_manifest,omitempty"`
	RecoveryAuthoritativeAppliedCommandLSN uint64                        `json:"recovery_authoritative_applied_command_lsn,omitempty"`
	ProfileSupport                         ColumnStoreProfileSupport     `json:"profile_support,omitempty"`
	Locator                                *ColumnLocatorConfig          `json:"locator,omitempty"`
	ControlRootStoragePolicy               RootStoragePolicy             `json:"control_root_storage_policy,omitempty"`
	SchemaHash                             uint64                        `json:"schema_hash,omitempty"`
}

type ColumnStoreColumn struct {
	Name       string               `json:"name"`
	Path       string               `json:"path"`
	ValueType  ColumnStoreValueType `json:"value_type"`
	Nullable   bool                 `json:"nullable,omitempty"`
	Dictionary bool                 `json:"dictionary,omitempty"`
}

type ColumnSortKey struct {
	Column    string              `json:"column"`
	Direction ColumnSortDirection `json:"direction,omitempty"`
}

type ColumnAggregateMetadata struct {
	Name   string              `json:"name"`
	Column string              `json:"column,omitempty"`
	Kind   ColumnAggregateKind `json:"kind"`
}

type ColumnAssetManagerConfig struct {
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
	RecoveryAuthoritativeGeneration        uint64
	RecoveryAuthoritativeAppliedCommandLSN uint64
	ManifestRoot                           uint64
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
	id := ColumnStoreCacheIdentity{
		Collection:        catalog.meta.Name,
		SchemaHash:        cfg.SchemaHash,
		CatalogSystemRoot: systemRoot,
		CatalogCommitSeq:  commitSeq,
		ManifestRoot:      catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name)),
	}
	if id.SchemaHash == 0 {
		id.SchemaHash = hashColumnStoreSchema(cfg)
	}
	if cfg.ActiveManifest != nil {
		id.ManifestGeneration = cfg.ActiveManifest.Generation
	}
	if cfg.RecoveryAuthoritativeManifest != nil {
		id.RecoveryAuthoritativeGeneration = cfg.RecoveryAuthoritativeManifest.Generation
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
	if out.Reconstruction == "" {
		out.Reconstruction = ColumnReconstructionRetainedPayloadAndColumns
	}
	if out.AssetManager == nil {
		out.AssetManager = &ColumnAssetManagerConfig{}
	}
	if out.AssetManager.Kind == "" {
		out.AssetManager.Kind = ColumnAssetManagerValueLog
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
	if out.ActiveManifest != nil {
		normalizeColumnManifestIdentityFormat(out.ActiveManifest)
	}
	if out.RecoveryAuthoritativeManifest != nil {
		normalizeColumnManifestIdentityFormat(out.RecoveryAuthoritativeManifest)
	}
	if err := validateColumnStoreConfig(collection, out); err != nil {
		return nil, err
	}
	out.SchemaHash = hashColumnStoreSchema(&out)
	return &out, nil
}

func validateColumnStoreConfig(collection string, cfg ColumnStoreConfig) error {
	if !cfg.Enabled {
		return nil
	}
	columnNames := make(map[string]struct{}, len(cfg.Columns))
	columnPaths := make(map[string]struct{}, len(cfg.Columns))
	for i := range cfg.Columns {
		col := cfg.Columns[i]
		if err := ValidateIndexName(col.Name); err != nil {
			return fmt.Errorf("collections: invalid column %q name: %w", col.Name, err)
		}
		if err := ValidateIndexPath(col.Path); err != nil {
			return fmt.Errorf("collections: invalid column %q path: %w", col.Name, err)
		}
		if _, err := normalizeColumnStoreValueType(col.ValueType); err != nil {
			return fmt.Errorf("collections: invalid column %q value_type: %w", col.Name, err)
		}
		if _, ok := columnNames[col.Name]; ok {
			return fmt.Errorf("collections: duplicate column %q", col.Name)
		}
		if _, ok := columnPaths[col.Path]; ok {
			return fmt.Errorf("collections: duplicate column path %q", col.Path)
		}
		columnNames[col.Name] = struct{}{}
		columnPaths[col.Path] = struct{}{}
	}
	for _, sortKey := range cfg.SortKey {
		if _, ok := columnNames[sortKey.Column]; !ok {
			return fmt.Errorf("collections: sort key references unknown column %q", sortKey.Column)
		}
		switch sortKey.Direction {
		case ColumnSortAscending, ColumnSortDescending:
		default:
			return fmt.Errorf("collections: unsupported sort direction %q", sortKey.Direction)
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
		if err := validateColumnAggregateKind(aggregate.Kind, aggregate.Column); err != nil {
			return fmt.Errorf("collections: invalid aggregate metadata %q: %w", aggregate.Name, err)
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
	switch cfg.Reconstruction {
	case ColumnReconstructionRetainedPayloadAndColumns:
	default:
		return fmt.Errorf("collections: unsupported reconstruction policy %q", cfg.Reconstruction)
	}
	if cfg.AssetManager == nil {
		return errors.New("collections: column_store requires asset manager metadata")
	}
	if cfg.AssetManager.Kind != ColumnAssetManagerValueLog {
		return fmt.Errorf("collections: unsupported column asset manager %q", cfg.AssetManager.Kind)
	}
	if !cfg.AssetManager.IsolatedNamespace {
		return errors.New("collections: column asset manager requires isolated namespace")
	}
	if strings.TrimSpace(cfg.AssetManager.Namespace) == "" || strings.Contains(cfg.AssetManager.Namespace, "\x00") {
		return errors.New("collections: invalid column asset namespace")
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
	case ColumnStoreValueBool, ColumnStoreValueInt64, ColumnStoreValueDouble, ColumnStoreValueString:
		return valueType, nil
	case "":
		return "", errors.New("value_type is required")
	default:
		return "", fmt.Errorf("unsupported value_type %q", valueType)
	}
}

func validateColumnAggregateKind(kind ColumnAggregateKind, column string) error {
	switch kind {
	case ColumnAggregateCount:
		return nil
	case ColumnAggregateMin, ColumnAggregateMax, ColumnAggregateSum, ColumnAggregateCountDistinct:
		if column == "" {
			return fmt.Errorf("aggregate kind %q requires a column", kind)
		}
		return nil
	default:
		return fmt.Errorf("unsupported aggregate kind %q", kind)
	}
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
		return fmt.Errorf("collections: active column manifest root %d for %q is missing identity record", rootID, catalog.meta.Name)
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
		return columnManifestIdentityRecord{}, fmt.Errorf("malformed identity record length %d", len(raw))
	}
	if magic := binary.BigEndian.Uint32(raw[columnManifestIdentityMagicOffset:columnManifestIdentityEncodingVersionOffset]); magic != columnManifestIdentityMagic {
		return columnManifestIdentityRecord{}, fmt.Errorf("bad identity magic 0x%x", magic)
	}
	if version := binary.BigEndian.Uint16(raw[columnManifestIdentityEncodingVersionOffset:columnManifestIdentityManifestVersionOffset]); version != columnManifestIdentityVersion {
		return columnManifestIdentityRecord{}, fmt.Errorf("unsupported identity version %d", version)
	}
	if reserved := binary.BigEndian.Uint32(raw[columnManifestIdentityReservedOffset:columnManifestIdentityRecordSize]); reserved != 0 {
		return columnManifestIdentityRecord{}, fmt.Errorf("non-zero identity reserved trailer field 0x%08x", reserved)
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
		cfg.Reconstruction == "" &&
		cfg.AssetManager == nil &&
		cfg.ManifestRoot == nil &&
		cfg.ActiveManifest == nil &&
		cfg.RecoveryAuthoritativeManifest == nil &&
		cfg.RecoveryAuthoritativeAppliedCommandLSN == 0 &&
		cfg.ProfileSupport == "" &&
		cfg.Locator == nil &&
		cfg.ControlRootStoragePolicy == "" &&
		cfg.SchemaHash == 0
}

func (cfg ColumnStoreConfig) copy() ColumnStoreConfig {
	out := cfg
	out.Columns = append([]ColumnStoreColumn(nil), cfg.Columns...)
	out.SortKey = append([]ColumnSortKey(nil), cfg.SortKey...)
	out.AggregateMetadata = append([]ColumnAggregateMetadata(nil), cfg.AggregateMetadata...)
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
		a.Reconstruction != b.Reconstruction ||
		a.ControlRootStoragePolicy != b.ControlRootStoragePolicy ||
		a.RecoveryAuthoritativeAppliedCommandLSN != b.RecoveryAuthoritativeAppliedCommandLSN ||
		a.ProfileSupport != b.ProfileSupport ||
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
		if a.AggregateMetadata[i] != b.AggregateMetadata[i] {
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
	writeHashString(&d, string(cfg.Reconstruction))
	writeHashString(&d, string(cfg.ControlRootStoragePolicy))
	if cfg.ManifestRoot != nil {
		writeHashString(&d, cfg.ManifestRoot.Name)
		writeHashString(&d, string(cfg.ManifestRoot.StoragePolicy))
	}
	for _, col := range cfg.Columns {
		writeHashString(&d, col.Name)
		writeHashString(&d, col.Path)
		writeHashString(&d, string(col.ValueType))
		writeHashBool(&d, col.Nullable)
		writeHashBool(&d, col.Dictionary)
	}
	for _, sortKey := range cfg.SortKey {
		writeHashString(&d, sortKey.Column)
		writeHashString(&d, string(sortKey.Direction))
	}
	for _, aggregate := range cfg.AggregateMetadata {
		writeHashString(&d, aggregate.Name)
		writeHashString(&d, aggregate.Column)
		writeHashString(&d, string(aggregate.Kind))
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
