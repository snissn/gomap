package colgranule

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
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
)

type ColumnWorkspaceOptions struct {
	Collection string
}

type ColumnWorkspace struct {
	dir       string
	assets    *SegmentColumnAssetStore
	manifest  ColumnWorkspaceManifest
	partByID  map[uint64]int
	cacheSeen map[string]struct{}
	cache     ColumnWorkspaceCacheStats
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

type ColumnWorkspacePartManifest struct {
	PartID        uint64          `json:"part_id"`
	Rows          int             `json:"rows"`
	VisibleRows   int             `json:"visible_rows"`
	SchemaVersion uint32          `json:"schema_version"`
	SortKey       []SortKeyColumn `json:"sort_key,omitempty"`
	AssetRef      ColumnAssetRef  `json:"asset_ref"`
	TCS1          TCS1PartRecord  `json:"tcs1"`
	ImageBytes    int             `json:"image_bytes"`
	ManifestBytes int             `json:"manifest_bytes"`
	Sections      int             `json:"sections"`
	AssetBytes    int             `json:"asset_bytes"`
	PublishedUnix int64           `json:"published_unix_nano"`
}

type columnWorkspaceManifestEnvelope struct {
	Magic    string                  `json:"magic"`
	Version  uint16                  `json:"version"`
	Checksum uint32                  `json:"checksum"`
	Manifest ColumnWorkspaceManifest `json:"manifest"`
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
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	assets, err := OpenSegmentColumnAssetStore(filepath.Join(dir, "assets"))
	if err != nil {
		return nil, err
	}
	w := &ColumnWorkspace{
		dir:       dir,
		assets:    assets,
		partByID:  make(map[uint64]int),
		cacheSeen: make(map[string]struct{}),
	}
	if err := w.loadOrInitManifest(opts); err != nil {
		_ = assets.Close()
		return nil, err
	}
	return w, nil
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

func (w *ColumnWorkspace) Manifest() ColumnWorkspaceManifest {
	if w == nil {
		return ColumnWorkspaceManifest{}
	}
	out := w.manifest
	out.Parts = append([]ColumnWorkspacePartManifest(nil), w.manifest.Parts...)
	for i := range out.Parts {
		out.Parts[i].SortKey = append([]SortKeyColumn(nil), out.Parts[i].SortKey...)
	}
	return out
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
	entry := ColumnWorkspacePartManifest{
		PartID:        part.Descriptor.PartID,
		Rows:          part.Descriptor.RowCount,
		VisibleRows:   part.Descriptor.VisibleRowCount,
		SchemaVersion: part.Descriptor.SchemaVersion,
		SortKey:       append([]SortKeyColumn(nil), part.Descriptor.SortKey...),
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
	if idx, ok := w.partByID[entry.PartID]; ok {
		w.manifest.Parts[idx] = entry
	} else {
		w.partByID[entry.PartID] = len(w.manifest.Parts)
		w.manifest.Parts = append(w.manifest.Parts, entry)
	}
	w.manifest.Generation++
	w.manifest.PublishID++
	w.manifest.UpdatedUnix = time.Now().UnixNano()
	sort.Slice(w.manifest.Parts, func(i, j int) bool {
		return w.manifest.Parts[i].PartID < w.manifest.Parts[j].PartID
	})
	w.rebuildPartIndex()
	if err := w.saveManifest(); err != nil {
		return ColumnWorkspacePartManifest{}, err
	}
	return entry, nil
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
	tmp, err := os.CreateTemp(w.dir, ".column-workspace-manifest-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err = tmp.Write(payload); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err = tmp.Sync(); err != nil {
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

func (w *ColumnWorkspace) manifestPath() string {
	if w == nil {
		return ""
	}
	return filepath.Join(w.dir, columnWorkspaceManifestFile)
}

func (w *ColumnWorkspace) rebuildPartIndex() {
	w.partByID = make(map[uint64]int, len(w.manifest.Parts))
	for i := range w.manifest.Parts {
		w.partByID[w.manifest.Parts[i].PartID] = i
	}
}

func (w *ColumnWorkspace) validateManifestAssets() error {
	for _, entry := range w.manifest.Parts {
		image, record, err := LoadTCS1ColumnPartImage(w.assets, entry.AssetRef)
		if err != nil {
			return fmt.Errorf("colgranule: validate workspace part %d asset: %w", entry.PartID, err)
		}
		if err := validateColumnWorkspacePartImage(entry, record, image); err != nil {
			return err
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
