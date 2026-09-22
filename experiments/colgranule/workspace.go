package colgranule

import (
	"encoding/json"
	"errors"
	"fmt"
	crc32 "github.com/snissn/go-crc32-asm"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

const (
	columnWorkspaceManifestFile    = "column-workspace-manifest.json"
	columnWorkspaceManifestMagic   = "TCWS1"
	columnWorkspaceManifestVersion = 1
	columnWorkspacePreparedFile    = "column-prepared-assets.json"
	columnWorkspacePreparedMagic   = "TCPR1"
	columnWorkspacePreparedVersion = 1

	columnWorkspaceManifestDirName   = "manifests"
	columnWorkspaceAssetsDirName     = "assets"
	columnWorkspaceSegmentsDirName   = "segments"
	columnWorkspaceIndexesDirName    = "indexes"
	columnWorkspacePreparedDirName   = "prepared"
	columnWorkspaceQuarantineDirName = "quarantine"
	columnWorkspaceTmpDirName        = "tmp"
)

type ColumnWorkspaceOptions struct {
	Collection       string
	ValidationMode   ColumnWorkspaceValidationMode
	ManifestSyncMode ColumnWorkspaceManifestSyncMode
	syncTempFile     func(*os.File) error
}

type ColumnWorkspaceValidationMode string
type ColumnWorkspaceManifestSyncMode string

const (
	ColumnWorkspaceValidateFullImage  ColumnWorkspaceValidationMode = "full_image"
	ColumnWorkspaceValidateTCS1Header ColumnWorkspaceValidationMode = "tcs1_header"

	ColumnWorkspaceManifestSyncDurable              ColumnWorkspaceManifestSyncMode = "durable"
	ColumnWorkspaceManifestSyncDisabledForBenchmark ColumnWorkspaceManifestSyncMode = "benchmark_no_sync"
)

var columnWorkspaceSyncTempFile = func(file *os.File) error {
	return file.Sync()
}

type ColumnWorkspaceNamespace struct {
	RootDir       string `json:"root_dir"`
	ManifestDir   string `json:"manifest_dir"`
	AssetDir      string `json:"asset_dir"`
	SegmentDir    string `json:"segment_dir"`
	IndexDir      string `json:"index_dir"`
	PreparedDir   string `json:"prepared_dir"`
	QuarantineDir string `json:"quarantine_dir"`
	TempDir       string `json:"temp_dir"`
}

func ColumnWorkspaceNamespaceForDir(dir string) ColumnWorkspaceNamespace {
	assetDir := filepath.Join(dir, columnWorkspaceAssetsDirName)
	return ColumnWorkspaceNamespace{
		RootDir:       dir,
		ManifestDir:   filepath.Join(dir, columnWorkspaceManifestDirName),
		AssetDir:      assetDir,
		SegmentDir:    filepath.Join(assetDir, columnWorkspaceSegmentsDirName),
		IndexDir:      filepath.Join(assetDir, columnWorkspaceIndexesDirName),
		PreparedDir:   filepath.Join(dir, columnWorkspacePreparedDirName),
		QuarantineDir: filepath.Join(dir, columnWorkspaceQuarantineDirName),
		TempDir:       filepath.Join(dir, columnWorkspaceTmpDirName),
	}
}

func ensureColumnWorkspaceNamespace(namespace ColumnWorkspaceNamespace) error {
	dirs := []string{
		namespace.RootDir,
		namespace.ManifestDir,
		namespace.AssetDir,
		namespace.SegmentDir,
		namespace.IndexDir,
		namespace.PreparedDir,
		namespace.QuarantineDir,
		namespace.TempDir,
	}
	for _, dir := range dirs {
		if dir == "" {
			return fmt.Errorf("colgranule: empty column workspace namespace dir")
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return nil
}

func normalizeColumnWorkspaceOptions(opts ColumnWorkspaceOptions) (ColumnWorkspaceOptions, error) {
	switch opts.ValidationMode {
	case "":
		opts.ValidationMode = ColumnWorkspaceValidateFullImage
	case ColumnWorkspaceValidateFullImage, ColumnWorkspaceValidateTCS1Header:
	default:
		return ColumnWorkspaceOptions{}, fmt.Errorf("colgranule: unsupported workspace validation mode %s", opts.ValidationMode)
	}
	switch opts.ManifestSyncMode {
	case "":
		opts.ManifestSyncMode = ColumnWorkspaceManifestSyncDurable
	case ColumnWorkspaceManifestSyncDurable, ColumnWorkspaceManifestSyncDisabledForBenchmark:
	default:
		return ColumnWorkspaceOptions{}, fmt.Errorf("colgranule: unsupported workspace manifest sync mode %s", opts.ManifestSyncMode)
	}
	return opts, nil
}

type ColumnWorkspace struct {
	dir            string
	namespace      ColumnWorkspaceNamespace
	assets         *ColumnAssetManager
	manifest       ColumnWorkspaceManifest
	validationMode ColumnWorkspaceValidationMode
	manifestSync   ColumnWorkspaceManifestSyncMode
	syncTempFile   func(*os.File) error
	partByID       map[uint64]int
	cacheSeen      map[string]struct{}
	cache          ColumnWorkspaceCacheStats
}

type ColumnWorkspaceManifest struct {
	Magic       string                        `json:"magic"`
	Version     uint16                        `json:"version"`
	Collection  string                        `json:"collection,omitempty"`
	Generation  uint64                        `json:"generation_id"`
	PublishID   uint64                        `json:"publish_id"`
	CreatedUnix int64                         `json:"created_unix_nano"`
	UpdatedUnix int64                         `json:"updated_unix_nano"`
	Parts       []ColumnWorkspacePartManifest `json:"parts,omitempty"`
}

type ColumnPreparedAssetRegistry struct {
	Magic        string                `json:"magic"`
	Version      uint16                `json:"version"`
	Collection   string                `json:"collection,omitempty"`
	PublishID    uint64                `json:"publish_id"`
	GenerationID uint64                `json:"generation_id"`
	UpdatedUnix  int64                 `json:"updated_unix_nano"`
	Assets       []ColumnPreparedAsset `json:"assets,omitempty"`
}

type ColumnWorkspacePartManifest struct {
	PartID        uint64                      `json:"part_id"`
	Rows          int                         `json:"rows"`
	VisibleRows   int                         `json:"visible_rows"`
	SchemaVersion uint32                      `json:"schema_version"`
	SortKey       []SortKeyColumn             `json:"sort_key,omitempty"`
	Coverage      ColumnWorkspacePartCoverage `json:"coverage"`
	AssetRef      ColumnAssetRef              `json:"asset_ref"`
	TCS1          TCS1PartRecord              `json:"tcs1"`
	ImageBytes    int                         `json:"image_bytes"`
	ManifestBytes int                         `json:"manifest_bytes"`
	Sections      int                         `json:"sections"`
	AssetBytes    int                         `json:"asset_bytes"`
	PublishedUnix int64                       `json:"published_unix_nano"`
}

type ColumnWorkspacePartCoverage struct {
	PrimaryIDLower          int64    `json:"primary_id_lower"`
	PrimaryIDUpperExclusive int64    `json:"primary_id_upper_exclusive"`
	SortKeyColumns          []string `json:"sort_key_columns,omitempty"`
	SortKeyLower            []int64  `json:"sort_key_lower,omitempty"`
	SortKeyUpperExclusive   []int64  `json:"sort_key_upper_exclusive,omitempty"`
	SortKeyUpperUnbounded   bool     `json:"sort_key_upper_unbounded,omitempty"`
}

type columnWorkspaceManifestEnvelope struct {
	Magic    string                  `json:"magic"`
	Version  uint16                  `json:"version"`
	Checksum uint32                  `json:"checksum"`
	Manifest ColumnWorkspaceManifest `json:"manifest"`
}

type columnPreparedAssetRegistryEnvelope struct {
	Magic    string                      `json:"magic"`
	Version  uint16                      `json:"version"`
	Checksum uint32                      `json:"checksum"`
	Registry ColumnPreparedAssetRegistry `json:"registry"`
}

type ColumnWorkspaceNamespaceInventory struct {
	SegmentFiles            []ColumnWorkspaceSegmentFile `json:"segment_files"`
	PreparedRegistryPresent bool                         `json:"prepared_registry_present"`
	PreparedAssets          []ColumnPreparedAsset        `json:"prepared_assets,omitempty"`
	OrphanPreparedAssets    []ColumnPreparedAsset        `json:"orphan_prepared_assets,omitempty"`
	ReferencedAssets        int                          `json:"referenced_assets"`
}

type ColumnWorkspaceSegmentFile struct {
	FileID uint32 `json:"file_id"`
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
}

type ColumnWorkspaceLoadResult struct {
	Part       *ColumnPart
	Manifest   ColumnWorkspacePartManifest
	TCS1       TCS1PartRecord
	CacheState string
	CacheStats ColumnWorkspaceCacheStats
}

type ColumnWorkspaceCacheStats struct {
	MarkCache       ColumnWorkspaceCacheCounter `json:"mark_cache"`
	CompressedCache ColumnWorkspaceCacheCounter `json:"compressed_block_cache"`
	DecodedCache    ColumnWorkspaceCacheCounter `json:"decoded_block_cache"`
	DictionaryCache ColumnWorkspaceCacheCounter `json:"dictionary_cache"`
}

type ColumnWorkspaceCacheCounter struct {
	Hits   uint64 `json:"hits"`
	Misses uint64 `json:"misses"`
}

func OpenColumnWorkspace(dir string, opts ColumnWorkspaceOptions) (*ColumnWorkspace, error) {
	if dir == "" {
		return nil, fmt.Errorf("colgranule: empty column workspace dir")
	}
	normalized, err := normalizeColumnWorkspaceOptions(opts)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	namespace := ColumnWorkspaceNamespaceForDir(dir)
	if err := ensureColumnWorkspaceNamespace(namespace); err != nil {
		return nil, err
	}
	assets, err := OpenSegmentColumnAssetStore(namespace.SegmentDir)
	if err != nil {
		return nil, err
	}
	assetManager, err := NewColumnAssetManager(assets)
	if err != nil {
		_ = assets.Close()
		return nil, err
	}
	w := &ColumnWorkspace{
		dir:            dir,
		namespace:      namespace,
		assets:         assetManager,
		validationMode: normalized.ValidationMode,
		manifestSync:   normalized.ManifestSyncMode,
		syncTempFile:   normalized.syncTempFile,
		partByID:       make(map[uint64]int),
		cacheSeen:      make(map[string]struct{}),
	}
	if err := w.loadOrInitManifest(normalized); err != nil {
		_ = assetManager.Close()
		return nil, err
	}
	return w, nil
}

func (w *ColumnWorkspace) ManifestSyncMode() ColumnWorkspaceManifestSyncMode {
	if w == nil {
		return ""
	}
	if w.manifestSync == "" {
		return ColumnWorkspaceManifestSyncDurable
	}
	return w.manifestSync
}

func (w *ColumnWorkspace) Close() error {
	if w == nil {
		return nil
	}
	if w.assets == nil {
		return nil
	}
	err := w.assets.Close()
	w.assets = nil
	return err
}

func (w *ColumnWorkspace) Dir() string {
	if w == nil {
		return ""
	}
	return w.dir
}

func (w *ColumnWorkspace) Namespace() ColumnWorkspaceNamespace {
	if w == nil {
		return ColumnWorkspaceNamespace{}
	}
	return w.namespace
}

func (w *ColumnWorkspace) Manifest() ColumnWorkspaceManifest {
	if w == nil {
		return ColumnWorkspaceManifest{}
	}
	return cloneColumnWorkspaceManifest(w.manifest)
}

func (w *ColumnWorkspace) CacheStats() ColumnWorkspaceCacheStats {
	if w == nil {
		return ColumnWorkspaceCacheStats{}
	}
	return w.cache
}

func (w *ColumnWorkspace) PublishPart(part *ColumnPart, dictionaries map[string]map[string]int64) (ColumnWorkspacePartManifest, error) {
	if w == nil || w.assets == nil {
		return ColumnWorkspacePartManifest{}, fmt.Errorf("colgranule: closed column workspace")
	}
	if part == nil {
		return ColumnWorkspacePartManifest{}, fmt.Errorf("colgranule: nil part")
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{Dictionaries: dictionaries})
	if err != nil {
		return ColumnWorkspacePartManifest{}, err
	}
	ref, record, err := StoreTCS1ColumnPartImage(w.assets, image)
	if err != nil {
		return ColumnWorkspacePartManifest{}, err
	}
	coverage, err := columnWorkspacePartCoverageFromPart(part)
	if err != nil {
		return ColumnWorkspacePartManifest{}, err
	}
	entry := ColumnWorkspacePartManifest{
		PartID:        part.Descriptor.PartID,
		Rows:          part.Descriptor.RowCount,
		VisibleRows:   part.Descriptor.VisibleRowCount,
		SchemaVersion: part.Descriptor.SchemaVersion,
		SortKey:       append([]SortKeyColumn(nil), part.Descriptor.SortKey...),
		Coverage:      coverage,
		AssetRef:      ref,
		TCS1:          record,
		ImageBytes:    image.TotalBytes(),
		ManifestBytes: image.ManifestBytes,
		Sections:      len(image.Sections),
		AssetBytes:    record.TotalBytes,
		PublishedUnix: time.Now().UnixNano(),
	}
	if err := validateColumnWorkspacePartManifest(entry); err != nil {
		return ColumnWorkspacePartManifest{}, err
	}
	prepared := []ColumnPreparedAsset{{
		Ref:          entry.AssetRef,
		Bytes:        entry.AssetBytes,
		GenerationID: w.manifest.Generation + 1,
		PublishID:    w.manifest.PublishID + 1,
		Reason:       "workspace part publish",
	}}
	synced, tracked, err := w.syncPreparedAssetsForManifest(prepared)
	if err != nil {
		if tracked {
			markErr := w.assets.MarkPublishFailed(prepared, "workspace part asset sync failed")
			return ColumnWorkspacePartManifest{}, errors.Join(err, markErr)
		}
		return ColumnWorkspacePartManifest{}, err
	}
	oldGeneration := w.manifest.Generation
	oldPublishID := w.manifest.PublishID
	oldUpdatedUnix := w.manifest.UpdatedUnix
	idx, existed := w.partByID[entry.PartID]
	var oldEntry ColumnWorkspacePartManifest
	insertIdx := -1
	if existed {
		oldEntry = w.manifest.Parts[idx]
		w.manifest.Parts[idx] = entry
	} else {
		insertIdx = sort.Search(len(w.manifest.Parts), func(i int) bool {
			return w.manifest.Parts[i].PartID >= entry.PartID
		})
		w.manifest.Parts = append(w.manifest.Parts, ColumnWorkspacePartManifest{})
		copy(w.manifest.Parts[insertIdx+1:], w.manifest.Parts[insertIdx:])
		w.manifest.Parts[insertIdx] = entry
		for partID, partIdx := range w.partByID {
			if partIdx >= insertIdx {
				w.partByID[partID] = partIdx + 1
			}
		}
		w.partByID[entry.PartID] = insertIdx
	}
	w.manifest.Generation++
	w.manifest.PublishID++
	w.manifest.UpdatedUnix = time.Now().UnixNano()
	if err := w.saveManifest(); err != nil {
		w.manifest.Generation = oldGeneration
		w.manifest.PublishID = oldPublishID
		w.manifest.UpdatedUnix = oldUpdatedUnix
		if existed {
			w.manifest.Parts[idx] = oldEntry
		} else {
			copy(w.manifest.Parts[insertIdx:], w.manifest.Parts[insertIdx+1:])
			w.manifest.Parts[len(w.manifest.Parts)-1] = ColumnWorkspacePartManifest{}
			w.manifest.Parts = w.manifest.Parts[:len(w.manifest.Parts)-1]
			delete(w.partByID, entry.PartID)
			for partID, partIdx := range w.partByID {
				if partIdx > insertIdx {
					w.partByID[partID] = partIdx - 1
				}
			}
		}
		if tracked {
			if markErr := w.assets.MarkPublishFailed(prepared, "workspace part manifest publish failed"); markErr != nil {
				return ColumnWorkspacePartManifest{}, errors.Join(err, markErr)
			}
		}
		return ColumnWorkspacePartManifest{}, err
	}
	if tracked {
		if err := w.assets.MarkPublishSucceeded(synced); err != nil {
			return ColumnWorkspacePartManifest{}, err
		}
	}
	return entry, nil
}

func (w *ColumnWorkspace) syncPreparedAssetsForManifest(prepared []ColumnPreparedAsset) (ColumnAssetSyncedPublishClosure, bool, error) {
	if w == nil || w.assets == nil {
		return ColumnAssetSyncedPublishClosure{}, false, fmt.Errorf("colgranule: closed column workspace")
	}
	if len(prepared) == 0 || w.ManifestSyncMode() == ColumnWorkspaceManifestSyncDisabledForBenchmark {
		return ColumnAssetSyncedPublishClosure{}, false, nil
	}
	closure, err := w.assets.PreparePublishClosure(prepared)
	if err != nil {
		return ColumnAssetSyncedPublishClosure{}, false, err
	}
	synced, err := w.assets.SyncPublishClosure(closure)
	if err != nil {
		return ColumnAssetSyncedPublishClosure{}, true, err
	}
	return synced, true, nil
}

func (w *ColumnWorkspace) LoadPart(partID uint64) (ColumnWorkspaceLoadResult, error) {
	return w.LoadPartWithOptions(partID, ColumnPartImageReadOptions{
		IncludeRowLocators:       true,
		ValidateRowLocators:      true,
		IncludeAggregateMetadata: true,
	})
}

func (w *ColumnWorkspace) LoadPartWithOptions(partID uint64, opts ColumnPartImageReadOptions) (ColumnWorkspaceLoadResult, error) {
	if w == nil || w.assets == nil {
		return ColumnWorkspaceLoadResult{}, fmt.Errorf("colgranule: closed column workspace")
	}
	idx, ok := w.partByID[partID]
	if !ok {
		return ColumnWorkspaceLoadResult{}, fmt.Errorf("colgranule: missing workspace part %d", partID)
	}
	entry := w.manifest.Parts[idx]
	part, record, err := ColumnPartFromTCS1AssetWithOptions(w.assets, entry.AssetRef, opts)
	if err != nil {
		return ColumnWorkspaceLoadResult{}, err
	}
	if err := validateColumnWorkspaceLoadedPart(entry, record, part); err != nil {
		return ColumnWorkspaceLoadResult{}, err
	}
	cacheState := w.observePartCache(entry)
	return ColumnWorkspaceLoadResult{
		Part:       part,
		Manifest:   entry,
		TCS1:       record,
		CacheState: cacheState,
		CacheStats: w.cache,
	}, nil
}

func (w *ColumnWorkspace) loadOrInitManifest(opts ColumnWorkspaceOptions) error {
	path := w.manifestPath()
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		now := time.Now().UnixNano()
		w.manifest = ColumnWorkspaceManifest{
			Magic:       columnWorkspaceManifestMagic,
			Version:     columnWorkspaceManifestVersion,
			Collection:  opts.Collection,
			CreatedUnix: now,
			UpdatedUnix: now,
		}
		return w.saveManifest()
	}
	env, err := decodeColumnWorkspaceManifestEnvelope(data)
	if err != nil {
		return err
	}
	if opts.Collection != "" && env.Manifest.Collection != "" && opts.Collection != env.Manifest.Collection {
		return fmt.Errorf("colgranule: workspace collection=%q want %q", env.Manifest.Collection, opts.Collection)
	}
	if err := validateColumnWorkspaceManifest(env.Manifest); err != nil {
		return err
	}
	w.manifest = env.Manifest
	w.rebuildPartIndex()
	return w.validateManifestAssets()
}

func (w *ColumnWorkspace) saveManifest() error {
	if w == nil {
		return fmt.Errorf("colgranule: nil column workspace")
	}
	if err := validateColumnWorkspaceManifest(w.manifest); err != nil {
		return err
	}
	payload, err := encodeColumnWorkspaceManifestEnvelope(w.manifest)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(w.namespace.TempDir, ".column-workspace-manifest-*.tmp")
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
	if err = os.Rename(tmpPath, w.manifestPath()); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

func (w *ColumnWorkspace) SavePreparedAssetRegistry(publishID uint64, generationID uint64, assets []ColumnPreparedAsset) error {
	if w == nil {
		return fmt.Errorf("colgranule: nil column workspace")
	}
	registry := ColumnPreparedAssetRegistry{
		Magic:        columnWorkspacePreparedMagic,
		Version:      columnWorkspacePreparedVersion,
		Collection:   w.manifest.Collection,
		PublishID:    publishID,
		GenerationID: generationID,
		UpdatedUnix:  time.Now().UnixNano(),
		Assets:       cloneColumnPreparedAssets(assets),
	}
	if err := validateColumnPreparedAssetRegistry(registry); err != nil {
		return err
	}
	payload, err := encodeColumnPreparedAssetRegistryEnvelope(registry)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(w.namespace.TempDir, ".column-prepared-assets-*.tmp")
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
	if err = os.Rename(tmpPath, w.preparedRegistryPath()); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

func (w *ColumnWorkspace) syncManifestTempFile(file *os.File) error {
	if w == nil {
		return fmt.Errorf("colgranule: nil column workspace")
	}
	if w.ManifestSyncMode() == ColumnWorkspaceManifestSyncDisabledForBenchmark {
		return nil
	}
	syncTempFile := w.syncTempFile
	if syncTempFile == nil {
		syncTempFile = columnWorkspaceSyncTempFile
	}
	return syncTempFile(file)
}

func (w *ColumnWorkspace) LoadPreparedAssetRegistry() (ColumnPreparedAssetRegistry, error) {
	if w == nil {
		return ColumnPreparedAssetRegistry{}, fmt.Errorf("colgranule: nil column workspace")
	}
	data, err := os.ReadFile(w.preparedRegistryPath())
	if err != nil {
		if os.IsNotExist(err) {
			return ColumnPreparedAssetRegistry{
				Magic:      columnWorkspacePreparedMagic,
				Version:    columnWorkspacePreparedVersion,
				Collection: w.manifest.Collection,
			}, nil
		}
		return ColumnPreparedAssetRegistry{}, err
	}
	registry, err := decodeColumnPreparedAssetRegistryEnvelope(data)
	if err != nil {
		return ColumnPreparedAssetRegistry{}, err
	}
	if w.manifest.Collection != "" && registry.Collection != "" && registry.Collection != w.manifest.Collection {
		return ColumnPreparedAssetRegistry{}, fmt.Errorf("colgranule: prepared registry collection=%q want %q", registry.Collection, w.manifest.Collection)
	}
	return registry, nil
}

func (w *ColumnWorkspace) ClearPreparedAssetRegistry() error {
	if w == nil {
		return fmt.Errorf("colgranule: nil column workspace")
	}
	err := os.Remove(w.preparedRegistryPath())
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (w *ColumnWorkspace) InventoryNamespace(manifests ...ColumnCollectionManifest) (ColumnWorkspaceNamespaceInventory, error) {
	if w == nil {
		return ColumnWorkspaceNamespaceInventory{}, fmt.Errorf("colgranule: nil column workspace")
	}
	inventory := ColumnWorkspaceNamespaceInventory{}
	entries, err := os.ReadDir(w.namespace.SegmentDir)
	if err != nil {
		return ColumnWorkspaceNamespaceInventory{}, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		fileID, ok := columnWorkspaceSegmentFileID(entry.Name())
		if !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return ColumnWorkspaceNamespaceInventory{}, err
		}
		inventory.SegmentFiles = append(inventory.SegmentFiles, ColumnWorkspaceSegmentFile{
			FileID: fileID,
			Path:   filepath.Join(w.namespace.SegmentDir, entry.Name()),
			Bytes:  info.Size(),
		})
	}
	sort.Slice(inventory.SegmentFiles, func(i, j int) bool {
		return inventory.SegmentFiles[i].FileID < inventory.SegmentFiles[j].FileID
	})
	registryPath := w.preparedRegistryPath()
	if _, err := os.Stat(registryPath); err == nil {
		inventory.PreparedRegistryPresent = true
	} else if !os.IsNotExist(err) {
		return ColumnWorkspaceNamespaceInventory{}, err
	}
	registry, err := w.LoadPreparedAssetRegistry()
	if err != nil {
		return ColumnWorkspaceNamespaceInventory{}, err
	}
	inventory.PreparedAssets = cloneColumnPreparedAssets(registry.Assets)
	referenced := make(map[ColumnAssetRef]struct{})
	for _, part := range w.manifest.Parts {
		referenced[part.AssetRef] = struct{}{}
	}
	for _, manifest := range manifests {
		refs, err := ColumnCollectionManifestAssetRefs(manifest)
		if err != nil {
			return ColumnWorkspaceNamespaceInventory{}, err
		}
		for _, ref := range refs {
			referenced[ref.Ref] = struct{}{}
		}
	}
	inventory.ReferencedAssets = len(referenced)
	for _, prepared := range registry.Assets {
		if _, ok := referenced[prepared.Ref]; !ok {
			inventory.OrphanPreparedAssets = append(inventory.OrphanPreparedAssets, prepared)
		}
	}
	return inventory, nil
}

func (w *ColumnWorkspace) manifestPath() string {
	if w == nil {
		return ""
	}
	return filepath.Join(w.namespace.ManifestDir, columnWorkspaceManifestFile)
}

func (w *ColumnWorkspace) preparedRegistryPath() string {
	if w == nil {
		return ""
	}
	return filepath.Join(w.namespace.PreparedDir, columnWorkspacePreparedFile)
}

func (w *ColumnWorkspace) rebuildPartIndex() {
	w.partByID = make(map[uint64]int, len(w.manifest.Parts))
	for i := range w.manifest.Parts {
		w.partByID[w.manifest.Parts[i].PartID] = i
	}
}

func (w *ColumnWorkspace) validateManifestAssets() error {
	for _, entry := range w.manifest.Parts {
		switch w.validationMode {
		case ColumnWorkspaceValidateTCS1Header:
			record, err := LoadTCS1ColumnPartHeader(w.assets, entry.AssetRef)
			if err != nil {
				return fmt.Errorf("colgranule: validate workspace part %d asset header: %w", entry.PartID, err)
			}
			if err := validateColumnWorkspacePartHeader(entry, record); err != nil {
				return err
			}
		default:
			image, record, err := LoadTCS1ColumnPartImage(w.assets, entry.AssetRef)
			if err != nil {
				return fmt.Errorf("colgranule: validate workspace part %d asset: %w", entry.PartID, err)
			}
			if err := validateColumnWorkspacePartImage(entry, record, image); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *ColumnWorkspace) observePartCache(entry ColumnWorkspacePartManifest) string {
	key := columnWorkspaceCacheKey(entry)
	if _, ok := w.cacheSeen[key]; ok {
		w.cache.MarkCache.Hits++
		w.cache.CompressedCache.Hits++
		w.cache.DecodedCache.Hits++
		if entry.Sections > 0 {
			w.cache.DictionaryCache.Hits++
		}
		return "warm"
	}
	w.cacheSeen[key] = struct{}{}
	w.cache.MarkCache.Misses++
	w.cache.CompressedCache.Misses++
	w.cache.DecodedCache.Misses++
	if entry.Sections > 0 {
		w.cache.DictionaryCache.Misses++
	}
	return "cold"
}

func columnWorkspaceCacheKey(entry ColumnWorkspacePartManifest) string {
	return strconv.FormatUint(entry.PartID, 10) + "/" +
		strconv.FormatUint(uint64(entry.AssetRef.FileID), 10) + "/" +
		strconv.FormatInt(entry.AssetRef.Offset, 10) + "/" +
		strconv.FormatInt(entry.AssetRef.Length, 10) + "/" +
		strconv.FormatUint(uint64(entry.AssetRef.Checksum), 10)
}

func encodeColumnWorkspaceManifestEnvelope(manifest ColumnWorkspaceManifest) ([]byte, error) {
	return encodeColumnWorkspaceManifestBinaryEnvelope(manifest)
}

func encodeColumnWorkspaceManifestJSONEnvelope(manifest ColumnWorkspaceManifest) ([]byte, error) {
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	env := columnWorkspaceManifestEnvelope{
		Magic:    columnWorkspaceManifestMagic,
		Version:  columnWorkspaceManifestVersion,
		Checksum: crc32.ChecksumIEEE(manifestBytes),
		Manifest: manifest,
	}
	return json.MarshalIndent(env, "", "  ")
}

func decodeColumnWorkspaceManifestEnvelope(data []byte) (columnWorkspaceManifestEnvelope, error) {
	if isColumnControlPlaneBinary(data, columnWorkspaceManifestBinaryMagic) {
		manifest, err := decodeColumnWorkspaceManifestBinaryEnvelope(data)
		if err != nil {
			return columnWorkspaceManifestEnvelope{}, err
		}
		return columnWorkspaceManifestEnvelope{
			Magic:    columnWorkspaceManifestMagic,
			Version:  columnWorkspaceManifestVersion,
			Manifest: manifest,
		}, nil
	}
	var env columnWorkspaceManifestEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return columnWorkspaceManifestEnvelope{}, err
	}
	if env.Magic != columnWorkspaceManifestMagic {
		return columnWorkspaceManifestEnvelope{}, fmt.Errorf("colgranule: invalid workspace manifest magic %q", env.Magic)
	}
	if env.Version != columnWorkspaceManifestVersion {
		return columnWorkspaceManifestEnvelope{}, fmt.Errorf("colgranule: unsupported workspace manifest version %d", env.Version)
	}
	manifestBytes, err := json.Marshal(env.Manifest)
	if err != nil {
		return columnWorkspaceManifestEnvelope{}, err
	}
	if checksum := crc32.ChecksumIEEE(manifestBytes); checksum != env.Checksum {
		return columnWorkspaceManifestEnvelope{}, fmt.Errorf("colgranule: workspace manifest checksum=%08x want %08x", checksum, env.Checksum)
	}
	return env, nil
}

func encodeColumnPreparedAssetRegistryEnvelope(registry ColumnPreparedAssetRegistry) ([]byte, error) {
	return encodeColumnPreparedAssetRegistryBinaryEnvelope(registry)
}

func encodeColumnPreparedAssetRegistryJSONEnvelope(registry ColumnPreparedAssetRegistry) ([]byte, error) {
	if err := validateColumnPreparedAssetRegistry(registry); err != nil {
		return nil, err
	}
	registryBytes, err := json.Marshal(registry)
	if err != nil {
		return nil, err
	}
	env := columnPreparedAssetRegistryEnvelope{
		Magic:    columnWorkspacePreparedMagic,
		Version:  columnWorkspacePreparedVersion,
		Checksum: crc32.ChecksumIEEE(registryBytes),
		Registry: registry,
	}
	return json.MarshalIndent(env, "", "  ")
}

func decodeColumnPreparedAssetRegistryEnvelope(data []byte) (ColumnPreparedAssetRegistry, error) {
	if isColumnControlPlaneBinary(data, columnWorkspacePreparedBinaryMagic) {
		return decodeColumnPreparedAssetRegistryBinaryEnvelope(data)
	}
	var env columnPreparedAssetRegistryEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return ColumnPreparedAssetRegistry{}, err
	}
	if env.Magic != columnWorkspacePreparedMagic {
		return ColumnPreparedAssetRegistry{}, fmt.Errorf("colgranule: invalid prepared registry magic %q", env.Magic)
	}
	if env.Version != columnWorkspacePreparedVersion {
		return ColumnPreparedAssetRegistry{}, fmt.Errorf("colgranule: unsupported prepared registry version %d", env.Version)
	}
	registryBytes, err := json.Marshal(env.Registry)
	if err != nil {
		return ColumnPreparedAssetRegistry{}, err
	}
	if checksum := crc32.ChecksumIEEE(registryBytes); checksum != env.Checksum {
		return ColumnPreparedAssetRegistry{}, fmt.Errorf("colgranule: prepared registry checksum=%08x want %08x", checksum, env.Checksum)
	}
	if err := validateColumnPreparedAssetRegistry(env.Registry); err != nil {
		return ColumnPreparedAssetRegistry{}, err
	}
	return env.Registry, nil
}

func validateColumnPreparedAssetRegistry(registry ColumnPreparedAssetRegistry) error {
	if registry.Magic != columnWorkspacePreparedMagic {
		return fmt.Errorf("colgranule: invalid prepared registry magic %q", registry.Magic)
	}
	if registry.Version != columnWorkspacePreparedVersion {
		return fmt.Errorf("colgranule: unsupported prepared registry version %d", registry.Version)
	}
	if len(registry.Assets) == 0 {
		return nil
	}
	seen := make(map[ColumnAssetRef]struct{}, len(registry.Assets))
	for _, asset := range registry.Assets {
		if err := validateColumnAssetRef(asset.Ref); err != nil {
			return fmt.Errorf("colgranule: invalid prepared asset ref: %w", err)
		}
		if asset.Bytes < 0 {
			return fmt.Errorf("colgranule: negative prepared asset bytes %d", asset.Bytes)
		}
		if asset.Bytes == 0 && asset.Ref.Length > int64(^uint(0)>>1) {
			return fmt.Errorf("colgranule: prepared asset length=%d exceeds host int", asset.Ref.Length)
		}
		if _, ok := seen[asset.Ref]; ok {
			return fmt.Errorf("colgranule: duplicate prepared asset ref %+v", asset.Ref)
		}
		seen[asset.Ref] = struct{}{}
	}
	return nil
}

func cloneColumnPreparedAssets(assets []ColumnPreparedAsset) []ColumnPreparedAsset {
	return append([]ColumnPreparedAsset(nil), assets...)
}

func cloneColumnWorkspaceManifest(manifest ColumnWorkspaceManifest) ColumnWorkspaceManifest {
	out := manifest
	out.Parts = append([]ColumnWorkspacePartManifest(nil), manifest.Parts...)
	for i := range out.Parts {
		out.Parts[i].SortKey = append([]SortKeyColumn(nil), out.Parts[i].SortKey...)
		out.Parts[i].Coverage = cloneColumnWorkspacePartCoverage(out.Parts[i].Coverage)
	}
	return out
}

func columnWorkspaceSegmentFileID(name string) (uint32, bool) {
	var fileID uint32
	var suffix string
	if _, err := fmt.Sscanf(name, "column-assets-%06d%s", &fileID, &suffix); err != nil {
		return 0, false
	}
	if suffix != ".seg" || fileID == 0 {
		return 0, false
	}
	return fileID, true
}

func validateColumnWorkspaceManifest(manifest ColumnWorkspaceManifest) error {
	if manifest.Magic != columnWorkspaceManifestMagic {
		return fmt.Errorf("colgranule: invalid workspace manifest magic %q", manifest.Magic)
	}
	if manifest.Version != columnWorkspaceManifestVersion {
		return fmt.Errorf("colgranule: unsupported workspace manifest version %d", manifest.Version)
	}
	seen := make(map[uint64]struct{}, len(manifest.Parts))
	for _, part := range manifest.Parts {
		if _, ok := seen[part.PartID]; ok {
			return fmt.Errorf("colgranule: duplicate workspace part id %d", part.PartID)
		}
		seen[part.PartID] = struct{}{}
		if err := validateColumnWorkspacePartManifest(part); err != nil {
			return err
		}
	}
	return nil
}

func validateColumnWorkspacePartManifest(part ColumnWorkspacePartManifest) error {
	if part.PartID == 0 {
		return fmt.Errorf("colgranule: workspace part id is zero")
	}
	if part.Rows < 0 || part.VisibleRows < 0 || part.VisibleRows > part.Rows {
		return fmt.Errorf("colgranule: workspace part %d invalid rows visible=%d rows=%d", part.PartID, part.VisibleRows, part.Rows)
	}
	if part.SchemaVersion == 0 {
		return fmt.Errorf("colgranule: workspace part %d missing schema version", part.PartID)
	}
	if err := validateColumnWorkspacePartCoverage(part); err != nil {
		return err
	}
	if err := validateColumnAssetRef(part.AssetRef); err != nil {
		return fmt.Errorf("colgranule: workspace part %d invalid asset ref: %w", part.PartID, err)
	}
	if part.AssetRef.Kind != ColumnAssetKindTCS1PartImage {
		return fmt.Errorf("colgranule: workspace part %d asset kind=%s want %s", part.PartID, part.AssetRef.Kind, ColumnAssetKindTCS1PartImage)
	}
	if part.TCS1.AssetRef != part.AssetRef {
		return fmt.Errorf("colgranule: workspace part %d tcs1 ref=%+v want %+v", part.PartID, part.TCS1.AssetRef, part.AssetRef)
	}
	if part.TCS1.PartID != part.PartID || part.TCS1.Rows != part.Rows {
		return fmt.Errorf("colgranule: workspace part %d tcs1 part/rows=(%d,%d) want (%d,%d)", part.PartID, part.TCS1.PartID, part.TCS1.Rows, part.PartID, part.Rows)
	}
	if part.ImageBytes <= 0 || part.ManifestBytes <= 0 || part.Sections <= 0 || part.AssetBytes <= 0 {
		return fmt.Errorf("colgranule: workspace part %d invalid image/asset bytes image=%d manifest=%d sections=%d asset=%d", part.PartID, part.ImageBytes, part.ManifestBytes, part.Sections, part.AssetBytes)
	}
	if part.AssetBytes != part.TCS1.TotalBytes {
		return fmt.Errorf("colgranule: workspace part %d asset bytes=%d want tcs1 total=%d", part.PartID, part.AssetBytes, part.TCS1.TotalBytes)
	}
	if part.ImageBytes != part.TCS1.PayloadBytes {
		return fmt.Errorf("colgranule: workspace part %d image bytes=%d want tcs1 payload=%d", part.PartID, part.ImageBytes, part.TCS1.PayloadBytes)
	}
	return nil
}

func validateColumnWorkspacePartCoverage(part ColumnWorkspacePartManifest) error {
	if len(part.Coverage.SortKeyColumns) == 0 && len(part.Coverage.SortKeyLower) == 0 && len(part.Coverage.SortKeyUpperExclusive) == 0 && part.Coverage.PrimaryIDLower == 0 && part.Coverage.PrimaryIDUpperExclusive == 0 {
		return nil
	}
	if len(part.Coverage.SortKeyColumns) != len(part.SortKey) {
		return fmt.Errorf("colgranule: workspace part %d coverage sort key columns=%d want %d", part.PartID, len(part.Coverage.SortKeyColumns), len(part.SortKey))
	}
	for i, sortKey := range part.SortKey {
		if part.Coverage.SortKeyColumns[i] != sortKey.Column {
			return fmt.Errorf("colgranule: workspace part %d coverage sort key column %d=%s want %s", part.PartID, i, part.Coverage.SortKeyColumns[i], sortKey.Column)
		}
	}
	if len(part.Coverage.SortKeyLower) != 0 && len(part.Coverage.SortKeyLower) != len(part.SortKey) {
		return fmt.Errorf("colgranule: workspace part %d coverage lower sort key width=%d want %d", part.PartID, len(part.Coverage.SortKeyLower), len(part.SortKey))
	}
	if len(part.Coverage.SortKeyUpperExclusive) != 0 && len(part.Coverage.SortKeyUpperExclusive) != len(part.SortKey) {
		return fmt.Errorf("colgranule: workspace part %d coverage upper sort key width=%d want %d", part.PartID, len(part.Coverage.SortKeyUpperExclusive), len(part.SortKey))
	}
	return nil
}

func validateColumnWorkspacePartHeader(entry ColumnWorkspacePartManifest, record TCS1PartRecord) error {
	if record.AssetRef != entry.AssetRef {
		return fmt.Errorf("colgranule: workspace part %d header ref=%+v want %+v", entry.PartID, record.AssetRef, entry.AssetRef)
	}
	if record.Version != entry.TCS1.Version || record.Kind != entry.TCS1.Kind || record.Flags != entry.TCS1.Flags {
		return fmt.Errorf("colgranule: workspace part %d header version/kind/flags=(%d,%d,%d) want (%d,%d,%d)", entry.PartID, record.Version, record.Kind, record.Flags, entry.TCS1.Version, entry.TCS1.Kind, entry.TCS1.Flags)
	}
	if record.PartID != entry.PartID || record.Rows != entry.Rows || record.ImageVersion != entry.TCS1.ImageVersion {
		return fmt.Errorf("colgranule: workspace part %d header part/rows/image=(%d,%d,%d) want (%d,%d,%d)", entry.PartID, record.PartID, record.Rows, record.ImageVersion, entry.PartID, entry.Rows, entry.TCS1.ImageVersion)
	}
	if record.PayloadBytes != entry.ImageBytes || record.TotalBytes != entry.AssetBytes || record.PayloadCRC32 != entry.TCS1.PayloadCRC32 {
		return fmt.Errorf("colgranule: workspace part %d header payload/total/crc=(%d,%d,%08x) want (%d,%d,%08x)", entry.PartID, record.PayloadBytes, record.TotalBytes, record.PayloadCRC32, entry.ImageBytes, entry.AssetBytes, entry.TCS1.PayloadCRC32)
	}
	return nil
}

func validateColumnWorkspaceLoadedPart(entry ColumnWorkspacePartManifest, record TCS1PartRecord, part *ColumnPart) error {
	if part == nil {
		return fmt.Errorf("colgranule: workspace part %d loaded nil part", entry.PartID)
	}
	if record.AssetRef != entry.AssetRef {
		return fmt.Errorf("colgranule: workspace part %d loaded ref=%+v want %+v", entry.PartID, record.AssetRef, entry.AssetRef)
	}
	if part.Descriptor.PartID != entry.PartID || part.Descriptor.RowCount != entry.Rows {
		return fmt.Errorf("colgranule: workspace loaded part/rows=(%d,%d) want (%d,%d)", part.Descriptor.PartID, part.Descriptor.RowCount, entry.PartID, entry.Rows)
	}
	if part.Descriptor.SchemaVersion != entry.SchemaVersion {
		return fmt.Errorf("colgranule: workspace loaded schema=%d want %d", part.Descriptor.SchemaVersion, entry.SchemaVersion)
	}
	return nil
}

func validateColumnWorkspacePartImage(entry ColumnWorkspacePartManifest, record TCS1PartRecord, image ColumnPartImage) error {
	if err := validateColumnWorkspacePartHeader(entry, record); err != nil {
		return err
	}
	if record.AssetRef != entry.AssetRef {
		return fmt.Errorf("colgranule: workspace part %d asset record ref=%+v want %+v", entry.PartID, record.AssetRef, entry.AssetRef)
	}
	if record.PartID != entry.PartID || image.PartID != entry.PartID {
		return fmt.Errorf("colgranule: workspace part id mismatch manifest=%d record=%d image=%d", entry.PartID, record.PartID, image.PartID)
	}
	if record.Rows != entry.Rows || image.Rows != entry.Rows {
		return fmt.Errorf("colgranule: workspace rows mismatch manifest=%d record=%d image=%d", entry.Rows, record.Rows, image.Rows)
	}
	if record.PayloadBytes != entry.ImageBytes || image.TotalBytes() != entry.ImageBytes {
		return fmt.Errorf("colgranule: workspace image bytes mismatch manifest=%d record=%d image=%d", entry.ImageBytes, record.PayloadBytes, image.TotalBytes())
	}
	if image.ManifestBytes != entry.ManifestBytes || len(image.Sections) != entry.Sections {
		return fmt.Errorf("colgranule: workspace image shape mismatch manifest bytes/sections=(%d,%d) image=(%d,%d)", entry.ManifestBytes, entry.Sections, image.ManifestBytes, len(image.Sections))
	}
	return nil
}

func columnWorkspacePartCoverageFromPart(part *ColumnPart) (ColumnWorkspacePartCoverage, error) {
	if part == nil {
		return ColumnWorkspacePartCoverage{}, fmt.Errorf("colgranule: nil part coverage")
	}
	if len(part.Descriptor.Granules) == 0 {
		return ColumnWorkspacePartCoverage{}, fmt.Errorf("colgranule: part %d has no granules for coverage", part.Descriptor.PartID)
	}
	if len(part.Marks) == 0 {
		return ColumnWorkspacePartCoverage{}, fmt.Errorf("colgranule: part %d has no sort-key marks for coverage", part.Descriptor.PartID)
	}
	coverage := ColumnWorkspacePartCoverage{
		PrimaryIDLower:          part.Descriptor.Granules[0].IDLower,
		PrimaryIDUpperExclusive: part.Descriptor.Granules[0].IDUpperExclusive,
	}
	for _, granule := range part.Descriptor.Granules[1:] {
		if granule.IDLower < coverage.PrimaryIDLower {
			coverage.PrimaryIDLower = granule.IDLower
		}
		if granule.IDUpperExclusive > coverage.PrimaryIDUpperExclusive {
			coverage.PrimaryIDUpperExclusive = granule.IDUpperExclusive
		}
	}
	fullPrefix := len(part.Marks[0].Prefixes) - 1
	if fullPrefix < 0 {
		return ColumnWorkspacePartCoverage{}, fmt.Errorf("colgranule: part %d has empty sort-key mark prefix", part.Descriptor.PartID)
	}
	first := part.Marks[0].Prefixes[fullPrefix]
	last := part.Marks[len(part.Marks)-1].Prefixes[fullPrefix]
	coverage.SortKeyColumns = append([]string(nil), first.Columns...)
	coverage.SortKeyLower = append([]int64(nil), first.Lower.Values...)
	coverage.SortKeyUpperExclusive = append([]int64(nil), last.UpperExclusive.Values...)
	coverage.SortKeyUpperUnbounded = last.UpperExclusive.Unbounded
	return coverage, nil
}

func cloneColumnWorkspacePartCoverage(coverage ColumnWorkspacePartCoverage) ColumnWorkspacePartCoverage {
	coverage.SortKeyColumns = append([]string(nil), coverage.SortKeyColumns...)
	coverage.SortKeyLower = append([]int64(nil), coverage.SortKeyLower...)
	coverage.SortKeyUpperExclusive = append([]int64(nil), coverage.SortKeyUpperExclusive...)
	return coverage
}
