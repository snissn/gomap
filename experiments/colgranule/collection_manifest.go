package colgranule

import (
	"bytes"
	"encoding/json"
	"fmt"
	crc32 "github.com/snissn/go-crc32-asm"
	"os"
	"path/filepath"
	"time"
)

const (
	columnCollectionManifestFile    = "column-collection-manifest.json"
	columnCollectionManifestMagic   = "TCCM1"
	columnCollectionManifestVersion = 1
)

type ColumnPartRole string

const (
	ColumnPartRoleBase  ColumnPartRole = "base"
	ColumnPartRoleDelta ColumnPartRole = "delta"
)

type ColumnCollectionAttachment struct {
	Model             string   `json:"model"`
	SystemMetadataKey string   `json:"system_metadata_key,omitempty"`
	RowPrimaryRoot    string   `json:"row_primary_root,omitempty"`
	ManifestRootRef   string   `json:"column_manifest_root_ref,omitempty"`
	LocatorRootRef    string   `json:"locator_root_ref,omitempty"`
	SecondaryRoots    []string `json:"secondary_roots,omitempty"`
}

type ColumnCollectionManifest struct {
	Magic             string                         `json:"magic"`
	Version           uint16                         `json:"version"`
	Collection        string                         `json:"collection"`
	SchemaVersion     uint32                         `json:"schema_version"`
	SchemaMode        ColumnSchemaMode               `json:"schema_mode"`
	LogicalPrimaryKey LogicalPrimaryKey              `json:"logical_primary_key"`
	SortKey           []SortKeyColumn                `json:"sort_key,omitempty"`
	DeclaredColumns   []ColumnDefinition             `json:"declared_columns"`
	ActiveGeneration  uint64                         `json:"active_generation"`
	Attachment        ColumnCollectionAttachment     `json:"attachment"`
	PartSet           ColumnPartSetManifest          `json:"part_set"`
	ByteAccounting    ColumnCollectionByteAccounting `json:"byte_accounting"`
	CreatedUnix       int64                          `json:"created_unix_nano"`
	UpdatedUnix       int64                          `json:"updated_unix_nano"`
}

type ColumnPartSetManifest struct {
	BaseParts  []ColumnManifestPartRef `json:"base_parts,omitempty"`
	DeltaParts []ColumnManifestPartRef `json:"delta_parts,omitempty"`
	Tombstones []ColumnTombstone       `json:"tombstones,omitempty"`
}

type ColumnManifestPartRef struct {
	Role         ColumnPartRole               `json:"role"`
	GenerationID uint64                       `json:"generation_id"`
	Coverage     ColumnPartCoverageDescriptor `json:"coverage"`
	Part         ColumnWorkspacePartManifest  `json:"part"`
}

type ColumnPartCoverageDescriptor struct {
	Role                    ColumnPartRole               `json:"role"`
	GenerationID            uint64                       `json:"generation_id"`
	CompactionLevel         uint8                        `json:"compaction_level,omitempty"`
	SourceParts             []ColumnSourcePartGeneration `json:"source_parts,omitempty"`
	SourceRowRootGeneration uint64                       `json:"source_row_root_generation,omitempty"`
	SourceRowVersionLower   uint64                       `json:"source_row_version_lower,omitempty"`
	SourceRowVersionUpper   uint64                       `json:"source_row_version_upper_exclusive,omitempty"`
	PrimaryIDLower          int64                        `json:"primary_id_lower"`
	PrimaryIDUpperExclusive int64                        `json:"primary_id_upper_exclusive"`
	SortKeyColumns          []string                     `json:"sort_key_columns,omitempty"`
	SortKeyLower            []int64                      `json:"sort_key_lower,omitempty"`
	SortKeyUpperExclusive   []int64                      `json:"sort_key_upper_exclusive,omitempty"`
	SortKeyUpperUnbounded   bool                         `json:"sort_key_upper_unbounded,omitempty"`
	Rows                    int                          `json:"rows"`
	VisibleRows             int                          `json:"visible_rows"`
	DeletedRows             int                          `json:"deleted_rows,omitempty"`
	AssetRefs               []ColumnAssetRef             `json:"asset_refs"`
	Checksums               []uint32                     `json:"checksums"`
}

type ColumnSourcePartGeneration struct {
	PartID       uint64 `json:"part_id"`
	GenerationID uint64 `json:"generation_id"`
}

type ColumnPartCoverageOptions struct {
	SourceParts             []ColumnSourcePartGeneration
	CompactionLevel         uint8
	SourceRowRootGeneration uint64
	SourceRowVersionLower   uint64
	SourceRowVersionUpper   uint64
}

type ColumnTombstone struct {
	PrimaryID     int64  `json:"primary_id"`
	GenerationID  uint64 `json:"generation_id"`
	Reason        string `json:"reason,omitempty"`
	PreparedBytes int    `json:"prepared_bytes,omitempty"`
}

type ColumnCollectionByteAccounting struct {
	Parts                     int `json:"parts"`
	BaseParts                 int `json:"base_parts"`
	DeltaParts                int `json:"delta_parts"`
	Rows                      int `json:"rows"`
	VisibleRows               int `json:"visible_rows"`
	Tombstones                int `json:"tombstones"`
	DeclaredColumns           int `json:"declared_columns"`
	DescriptorBytes           int `json:"descriptor_bytes"`
	BaseAssetBytes            int `json:"base_asset_bytes"`
	DeltaAssetBytes           int `json:"delta_asset_bytes"`
	TotalAssetBytes           int `json:"total_asset_bytes"`
	ReclaimableCandidateBytes int `json:"reclaimable_candidate_bytes,omitempty"`
}

type columnCollectionManifestEnvelope struct {
	Magic    string                   `json:"magic"`
	Version  uint16                   `json:"version"`
	Checksum uint32                   `json:"checksum"`
	Manifest ColumnCollectionManifest `json:"manifest"`
}

type columnCollectionManifestRawEnvelope struct {
	Magic    string          `json:"magic"`
	Version  uint16          `json:"version"`
	Checksum uint32          `json:"checksum"`
	Manifest json.RawMessage `json:"manifest"`
}

func NewColumnManifestPartRef(role ColumnPartRole, generationID uint64, part ColumnWorkspacePartManifest) ColumnManifestPartRef {
	return NewColumnManifestPartRefWithCoverage(role, generationID, part, nil, 0)
}

func NewColumnManifestPartRefWithCoverage(role ColumnPartRole, generationID uint64, part ColumnWorkspacePartManifest, sourceParts []ColumnSourcePartGeneration, compactionLevel uint8) ColumnManifestPartRef {
	return NewColumnManifestPartRefWithCoverageOptions(role, generationID, part, ColumnPartCoverageOptions{
		SourceParts:     sourceParts,
		CompactionLevel: compactionLevel,
	})
}

func NewColumnManifestPartRefWithCoverageOptions(role ColumnPartRole, generationID uint64, part ColumnWorkspacePartManifest, opts ColumnPartCoverageOptions) ColumnManifestPartRef {
	return ColumnManifestPartRef{
		Role:         role,
		GenerationID: generationID,
		Coverage:     columnPartCoverageDescriptor(role, generationID, part, opts),
		Part:         part,
	}
}

func NewColumnCollectionManifest(collection string, opts ColumnStoreOptions, baseParts []ColumnManifestPartRef, deltaParts []ColumnManifestPartRef, tombstones []ColumnTombstone) (ColumnCollectionManifest, error) {
	normalized, err := normalizeColumnStoreOptions(opts)
	if err != nil {
		return ColumnCollectionManifest{}, err
	}
	now := time.Now().UnixNano()
	manifest := ColumnCollectionManifest{
		Magic:             columnCollectionManifestMagic,
		Version:           columnCollectionManifestVersion,
		Collection:        collection,
		SchemaVersion:     normalized.SchemaVersion,
		SchemaMode:        normalized.SchemaMode,
		LogicalPrimaryKey: cloneLogicalPrimaryKey(normalized.LogicalPrimaryKey),
		SortKey:           append([]SortKeyColumn(nil), normalized.SortKey.Columns...),
		DeclaredColumns:   append([]ColumnDefinition(nil), normalized.Columns...),
		Attachment: ColumnCollectionAttachment{
			Model:             "hybrid_row_column",
			SystemMetadataKey: "collection/" + collection + "/column_manifest",
		},
		PartSet: ColumnPartSetManifest{
			BaseParts:  cloneColumnManifestPartRefs(baseParts),
			DeltaParts: cloneColumnManifestPartRefs(deltaParts),
			Tombstones: append([]ColumnTombstone(nil), tombstones...),
		},
		CreatedUnix: now,
		UpdatedUnix: now,
	}
	manifest.ActiveGeneration = columnManifestActiveGeneration(manifest.PartSet)
	manifest.ByteAccounting = columnManifestByteAccounting(manifest)
	if err := validateColumnCollectionManifest(manifest); err != nil {
		return ColumnCollectionManifest{}, err
	}
	return manifest, nil
}

func (m ColumnCollectionManifest) ColumnStoreOptions() ColumnStoreOptions {
	return ColumnStoreOptions{
		SchemaVersion:     m.SchemaVersion,
		SchemaMode:        m.SchemaMode,
		Columns:           append([]ColumnDefinition(nil), m.DeclaredColumns...),
		LogicalPrimaryKey: cloneLogicalPrimaryKey(m.LogicalPrimaryKey),
		SortKey:           SortKey{Columns: append([]SortKeyColumn(nil), m.SortKey...)},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: DefaultRowsPerGranule, DefaultCodecBlockRows: DefaultRowsPerGranule},
	}
}

func (w *ColumnWorkspace) SaveCollectionManifest(manifest ColumnCollectionManifest) error {
	if w == nil {
		return fmt.Errorf("colgranule: nil column workspace")
	}
	if err := validateColumnCollectionManifest(manifest); err != nil {
		return err
	}
	if err := w.validateCollectionManifestPartRefs(manifest); err != nil {
		return err
	}
	payload, err := EncodeColumnCollectionManifest(manifest)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(w.namespace.TempDir, ".column-collection-manifest-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err = tmp.Write(payload); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err = w.syncManifestTempFile(tmp); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err = tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err = os.Rename(tmpPath, w.collectionManifestPath()); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

func (w *ColumnWorkspace) LoadCollectionManifest() (ColumnCollectionManifest, error) {
	if w == nil {
		return ColumnCollectionManifest{}, fmt.Errorf("colgranule: nil column workspace")
	}
	data, err := os.ReadFile(w.collectionManifestPath())
	if err != nil {
		return ColumnCollectionManifest{}, err
	}
	manifest, err := DecodeColumnCollectionManifest(data)
	if err != nil {
		return ColumnCollectionManifest{}, err
	}
	if err := w.validateCollectionManifestPartRefs(manifest); err != nil {
		return ColumnCollectionManifest{}, err
	}
	return manifest, nil
}

func EncodeColumnCollectionManifest(manifest ColumnCollectionManifest) ([]byte, error) {
	return encodeColumnCollectionManifestBinaryEnvelope(manifest)
}

func encodeColumnCollectionManifestJSONEnvelope(manifest ColumnCollectionManifest) ([]byte, error) {
	if err := validateColumnCollectionManifest(manifest); err != nil {
		return nil, err
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	env := columnCollectionManifestRawEnvelope{
		Magic:    columnCollectionManifestMagic,
		Version:  columnCollectionManifestVersion,
		Checksum: crc32.ChecksumIEEE(manifestBytes),
		Manifest: manifestBytes,
	}
	return json.Marshal(env)
}

func DecodeColumnCollectionManifest(data []byte) (ColumnCollectionManifest, error) {
	if isColumnControlPlaneBinary(data, columnCollectionManifestBinaryMagic) {
		return decodeColumnCollectionManifestBinaryEnvelope(data)
	}
	var env columnCollectionManifestRawEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return ColumnCollectionManifest{}, err
	}
	if env.Magic != columnCollectionManifestMagic {
		return ColumnCollectionManifest{}, fmt.Errorf("colgranule: invalid collection manifest magic %q", env.Magic)
	}
	if env.Version != columnCollectionManifestVersion {
		return ColumnCollectionManifest{}, fmt.Errorf("colgranule: unsupported collection manifest version %d", env.Version)
	}
	if len(env.Manifest) == 0 {
		return ColumnCollectionManifest{}, fmt.Errorf("colgranule: collection manifest missing manifest payload")
	}
	checksum := crc32.ChecksumIEEE(env.Manifest)
	if checksum != env.Checksum {
		// Support earlier pretty-printed experiment payloads without paying this
		// compaction cost on newly encoded compact manifests.
		var compact bytes.Buffer
		if err := json.Compact(&compact, env.Manifest); err != nil {
			return ColumnCollectionManifest{}, err
		}
		checksum = crc32.ChecksumIEEE(compact.Bytes())
	}
	if checksum != env.Checksum {
		return ColumnCollectionManifest{}, fmt.Errorf("colgranule: collection manifest checksum=%08x want %08x", checksum, env.Checksum)
	}
	var manifest ColumnCollectionManifest
	if err := json.Unmarshal(env.Manifest, &manifest); err != nil {
		return ColumnCollectionManifest{}, err
	}
	if err := validateColumnCollectionManifest(manifest); err != nil {
		return ColumnCollectionManifest{}, err
	}
	return manifest, nil
}

func (w *ColumnWorkspace) collectionManifestPath() string {
	if w == nil {
		return ""
	}
	return filepath.Join(w.namespace.ManifestDir, columnCollectionManifestFile)
}

func (w *ColumnWorkspace) validateCollectionManifestPartRefs(manifest ColumnCollectionManifest) error {
	refs := append([]ColumnManifestPartRef(nil), manifest.PartSet.BaseParts...)
	refs = append(refs, manifest.PartSet.DeltaParts...)
	for _, ref := range refs {
		idx, ok := w.partByID[ref.Part.PartID]
		if !ok {
			return fmt.Errorf("colgranule: collection manifest references missing workspace part %d", ref.Part.PartID)
		}
		workspacePart := w.manifest.Parts[idx]
		if workspacePart.AssetRef != ref.Part.AssetRef || workspacePart.TCS1 != ref.Part.TCS1 || workspacePart.AssetBytes != ref.Part.AssetBytes {
			return fmt.Errorf("colgranule: collection manifest part %d does not match workspace manifest", ref.Part.PartID)
		}
	}
	return nil
}

func validateColumnCollectionManifest(manifest ColumnCollectionManifest) error {
	if manifest.Magic != columnCollectionManifestMagic {
		return fmt.Errorf("colgranule: invalid collection manifest magic %q", manifest.Magic)
	}
	if manifest.Version != columnCollectionManifestVersion {
		return fmt.Errorf("colgranule: unsupported collection manifest version %d", manifest.Version)
	}
	if manifest.Collection == "" {
		return fmt.Errorf("colgranule: empty collection manifest collection")
	}
	if manifest.SchemaVersion == 0 {
		return fmt.Errorf("colgranule: collection manifest missing schema version")
	}
	if manifest.SchemaMode != ColumnSchemaFixed {
		return fmt.Errorf("colgranule: unsupported collection manifest schema mode %s", manifest.SchemaMode)
	}
	if len(manifest.LogicalPrimaryKey.Columns) != 1 {
		return fmt.Errorf("colgranule: collection manifest requires one logical primary key, got %d", len(manifest.LogicalPrimaryKey.Columns))
	}
	if len(manifest.DeclaredColumns) == 0 {
		return fmt.Errorf("colgranule: collection manifest has no declared columns")
	}
	seenColumns := make(map[string]struct{}, len(manifest.DeclaredColumns))
	for _, def := range manifest.DeclaredColumns {
		if def.Name == "" {
			return fmt.Errorf("colgranule: collection manifest has empty declared column")
		}
		if _, ok := seenColumns[def.Name]; ok {
			return fmt.Errorf("colgranule: collection manifest duplicate column %s", def.Name)
		}
		seenColumns[def.Name] = struct{}{}
	}
	if _, ok := seenColumns[manifest.LogicalPrimaryKey.Columns[0]]; !ok {
		return fmt.Errorf("colgranule: collection manifest primary key column %s is not declared", manifest.LogicalPrimaryKey.Columns[0])
	}
	if len(manifest.SortKey) == 0 {
		return fmt.Errorf("colgranule: collection manifest has no sort key")
	}
	for _, c := range manifest.SortKey {
		if _, ok := seenColumns[c.Column]; !ok {
			return fmt.Errorf("colgranule: collection manifest sort key column %s is not declared", c.Column)
		}
	}
	if columnManifestPartIDsStrictlyIncreasing(manifest.PartSet) {
		for _, ref := range manifest.PartSet.BaseParts {
			if err := validateColumnManifestPartRef(ref, ColumnPartRoleBase); err != nil {
				return err
			}
		}
		for _, ref := range manifest.PartSet.DeltaParts {
			if err := validateColumnManifestPartRef(ref, ColumnPartRoleDelta); err != nil {
				return err
			}
		}
	} else {
		seenParts := make(map[uint64]struct{}, len(manifest.PartSet.BaseParts)+len(manifest.PartSet.DeltaParts))
		for _, ref := range manifest.PartSet.BaseParts {
			if err := validateColumnManifestPartRef(ref, ColumnPartRoleBase); err != nil {
				return err
			}
			if _, ok := seenParts[ref.Part.PartID]; ok {
				return fmt.Errorf("colgranule: duplicate collection manifest part %d", ref.Part.PartID)
			}
			seenParts[ref.Part.PartID] = struct{}{}
		}
		for _, ref := range manifest.PartSet.DeltaParts {
			if err := validateColumnManifestPartRef(ref, ColumnPartRoleDelta); err != nil {
				return err
			}
			if _, ok := seenParts[ref.Part.PartID]; ok {
				return fmt.Errorf("colgranule: duplicate collection manifest part %d", ref.Part.PartID)
			}
			seenParts[ref.Part.PartID] = struct{}{}
		}
	}
	for _, tombstone := range manifest.PartSet.Tombstones {
		if tombstone.GenerationID == 0 {
			return fmt.Errorf("colgranule: tombstone for primary id %d has zero generation", tombstone.PrimaryID)
		}
	}
	if manifest.ActiveGeneration != columnManifestActiveGeneration(manifest.PartSet) {
		return fmt.Errorf("colgranule: collection manifest active generation=%d want %d", manifest.ActiveGeneration, columnManifestActiveGeneration(manifest.PartSet))
	}
	wantAccounting := columnManifestByteAccounting(manifest)
	if manifest.ByteAccounting != wantAccounting {
		return fmt.Errorf("colgranule: collection manifest byte accounting=%+v want %+v", manifest.ByteAccounting, wantAccounting)
	}
	return nil
}

func validateColumnManifestPartRef(ref ColumnManifestPartRef, role ColumnPartRole) error {
	if err := validateColumnPartRole(role); err != nil {
		return err
	}
	if ref.Role != role {
		return fmt.Errorf("colgranule: collection manifest part %d role=%s want %s", ref.Part.PartID, ref.Role, role)
	}
	if ref.GenerationID == 0 {
		return fmt.Errorf("colgranule: collection manifest part %d has zero generation", ref.Part.PartID)
	}
	if err := validateColumnWorkspacePartManifest(ref.Part); err != nil {
		return fmt.Errorf("colgranule: collection manifest part %d invalid: %w", ref.Part.PartID, err)
	}
	if err := validateColumnPartCoverageDescriptor(ref.Coverage, ref.Role, ref.GenerationID, ref.Part); err != nil {
		return fmt.Errorf("colgranule: collection manifest part %d coverage invalid: %w", ref.Part.PartID, err)
	}
	return nil
}

func columnManifestPartIDsStrictlyIncreasing(partSet ColumnPartSetManifest) bool {
	var last uint64
	var haveLast bool
	for _, ref := range partSet.BaseParts {
		if haveLast && ref.Part.PartID <= last {
			return false
		}
		last = ref.Part.PartID
		haveLast = true
	}
	for _, ref := range partSet.DeltaParts {
		if haveLast && ref.Part.PartID <= last {
			return false
		}
		last = ref.Part.PartID
		haveLast = true
	}
	return true
}

func validateColumnPartRole(role ColumnPartRole) error {
	switch role {
	case ColumnPartRoleBase, ColumnPartRoleDelta:
		return nil
	default:
		return fmt.Errorf("colgranule: unsupported collection manifest part role %s", role)
	}
}

func columnPartCoverageDescriptor(role ColumnPartRole, generationID uint64, part ColumnWorkspacePartManifest, opts ColumnPartCoverageOptions) ColumnPartCoverageDescriptor {
	coverage := ColumnPartCoverageDescriptor{
		Role:                    role,
		GenerationID:            generationID,
		CompactionLevel:         opts.CompactionLevel,
		SourceParts:             append([]ColumnSourcePartGeneration(nil), opts.SourceParts...),
		SourceRowRootGeneration: opts.SourceRowRootGeneration,
		SourceRowVersionLower:   opts.SourceRowVersionLower,
		SourceRowVersionUpper:   opts.SourceRowVersionUpper,
		PrimaryIDLower:          part.Coverage.PrimaryIDLower,
		PrimaryIDUpperExclusive: part.Coverage.PrimaryIDUpperExclusive,
		SortKeyColumns:          append([]string(nil), part.Coverage.SortKeyColumns...),
		SortKeyLower:            append([]int64(nil), part.Coverage.SortKeyLower...),
		SortKeyUpperExclusive:   append([]int64(nil), part.Coverage.SortKeyUpperExclusive...),
		SortKeyUpperUnbounded:   part.Coverage.SortKeyUpperUnbounded,
		Rows:                    part.Rows,
		VisibleRows:             part.VisibleRows,
		DeletedRows:             part.Rows - part.VisibleRows,
		AssetRefs:               []ColumnAssetRef{part.AssetRef},
		Checksums:               []uint32{part.AssetRef.Checksum},
	}
	if len(coverage.SortKeyColumns) == 0 && len(part.SortKey) != 0 {
		for _, sortKey := range part.SortKey {
			coverage.SortKeyColumns = append(coverage.SortKeyColumns, sortKey.Column)
		}
	}
	return coverage
}

func validateColumnPartCoverageDescriptor(coverage ColumnPartCoverageDescriptor, role ColumnPartRole, generationID uint64, part ColumnWorkspacePartManifest) error {
	if coverage.Role != role {
		return fmt.Errorf("role=%s want %s", coverage.Role, role)
	}
	if coverage.GenerationID != generationID {
		return fmt.Errorf("generation=%d want %d", coverage.GenerationID, generationID)
	}
	if coverage.SourceRowVersionUpper != 0 && coverage.SourceRowVersionLower >= coverage.SourceRowVersionUpper {
		return fmt.Errorf("source row version lower=%d upper=%d", coverage.SourceRowVersionLower, coverage.SourceRowVersionUpper)
	}
	if coverage.SourceRowVersionLower != 0 && coverage.SourceRowVersionUpper == 0 {
		return fmt.Errorf("source row version lower=%d without upper bound", coverage.SourceRowVersionLower)
	}
	if coverage.SourceRowRootGeneration == 0 && (coverage.SourceRowVersionLower != 0 || coverage.SourceRowVersionUpper != 0) {
		return fmt.Errorf("source row versions require source row root generation")
	}
	if coverage.Rows != part.Rows || coverage.VisibleRows != part.VisibleRows || coverage.DeletedRows != part.Rows-part.VisibleRows {
		return fmt.Errorf("rows/visible/deleted=(%d,%d,%d) want (%d,%d,%d)", coverage.Rows, coverage.VisibleRows, coverage.DeletedRows, part.Rows, part.VisibleRows, part.Rows-part.VisibleRows)
	}
	if len(coverage.SortKeyColumns) != len(part.SortKey) {
		return fmt.Errorf("sort key columns=%d want %d", len(coverage.SortKeyColumns), len(part.SortKey))
	}
	for i, sortKey := range part.SortKey {
		if coverage.SortKeyColumns[i] != sortKey.Column {
			return fmt.Errorf("sort key column %d=%s want %s", i, coverage.SortKeyColumns[i], sortKey.Column)
		}
	}
	if len(coverage.SortKeyLower) != 0 && len(coverage.SortKeyLower) != len(part.SortKey) {
		return fmt.Errorf("sort key lower width=%d want %d", len(coverage.SortKeyLower), len(part.SortKey))
	}
	if len(coverage.SortKeyUpperExclusive) != 0 && len(coverage.SortKeyUpperExclusive) != len(part.SortKey) {
		return fmt.Errorf("sort key upper width=%d want %d", len(coverage.SortKeyUpperExclusive), len(part.SortKey))
	}
	if len(coverage.AssetRefs) != 1 || coverage.AssetRefs[0] != part.AssetRef {
		return fmt.Errorf("asset refs=%+v want [%+v]", coverage.AssetRefs, part.AssetRef)
	}
	if len(coverage.Checksums) != 1 || coverage.Checksums[0] != part.AssetRef.Checksum {
		return fmt.Errorf("checksums=%+v want [%08x]", coverage.Checksums, part.AssetRef.Checksum)
	}
	return nil
}

func columnManifestActiveGeneration(partSet ColumnPartSetManifest) uint64 {
	var max uint64
	for _, ref := range partSet.BaseParts {
		if ref.GenerationID > max {
			max = ref.GenerationID
		}
	}
	for _, ref := range partSet.DeltaParts {
		if ref.GenerationID > max {
			max = ref.GenerationID
		}
	}
	for _, tombstone := range partSet.Tombstones {
		if tombstone.GenerationID > max {
			max = tombstone.GenerationID
		}
	}
	return max
}

func columnManifestByteAccounting(manifest ColumnCollectionManifest) ColumnCollectionByteAccounting {
	accounting := ColumnCollectionByteAccounting{
		BaseParts:       len(manifest.PartSet.BaseParts),
		DeltaParts:      len(manifest.PartSet.DeltaParts),
		Tombstones:      len(manifest.PartSet.Tombstones),
		DeclaredColumns: len(manifest.DeclaredColumns),
	}
	accounting.Parts = accounting.BaseParts + accounting.DeltaParts
	for _, ref := range manifest.PartSet.BaseParts {
		accounting.Rows += ref.Part.Rows
		accounting.VisibleRows += ref.Part.VisibleRows
		accounting.DescriptorBytes += ref.Part.ManifestBytes
		accounting.BaseAssetBytes += ref.Part.AssetBytes
	}
	for _, ref := range manifest.PartSet.DeltaParts {
		accounting.Rows += ref.Part.Rows
		accounting.VisibleRows += ref.Part.VisibleRows
		accounting.DescriptorBytes += ref.Part.ManifestBytes
		accounting.DeltaAssetBytes += ref.Part.AssetBytes
	}
	accounting.TotalAssetBytes = accounting.BaseAssetBytes + accounting.DeltaAssetBytes
	return accounting
}

func cloneLogicalPrimaryKey(key LogicalPrimaryKey) LogicalPrimaryKey {
	return LogicalPrimaryKey{Columns: append([]string(nil), key.Columns...)}
}

func cloneColumnManifestPartRefs(refs []ColumnManifestPartRef) []ColumnManifestPartRef {
	out := append([]ColumnManifestPartRef(nil), refs...)
	for i := range out {
		out[i].Part.SortKey = append([]SortKeyColumn(nil), out[i].Part.SortKey...)
		out[i].Part.Coverage = cloneColumnWorkspacePartCoverage(out[i].Part.Coverage)
		out[i].Coverage = cloneColumnPartCoverageDescriptor(out[i].Coverage)
	}
	return out
}

func cloneColumnPartCoverageDescriptor(coverage ColumnPartCoverageDescriptor) ColumnPartCoverageDescriptor {
	coverage.SourceParts = append([]ColumnSourcePartGeneration(nil), coverage.SourceParts...)
	coverage.SortKeyColumns = append([]string(nil), coverage.SortKeyColumns...)
	coverage.SortKeyLower = append([]int64(nil), coverage.SortKeyLower...)
	coverage.SortKeyUpperExclusive = append([]int64(nil), coverage.SortKeyUpperExclusive...)
	coverage.AssetRefs = append([]ColumnAssetRef(nil), coverage.AssetRefs...)
	coverage.Checksums = append([]uint32(nil), coverage.Checksums...)
	return coverage
}
