package collections

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	vectorIndexFormatVersion  = 1
	vectorIndexDirName        = "vector_indexes"
	vectorIndexManifestFile   = "manifest.json"
	vectorIndexMetaFile       = "meta.json"
	vectorIndexNodesFile      = "nodes.json"
	vectorIndexEdgesFile      = "edges.json"
	vectorIndexTombstonesFile = "tombstones.json"
	vectorIndexDocMapFile     = "docmap.json"
)

// VectorIndexLoadStatus reports whether a persisted vector index loaded or why
// callers should use exact search as the safe fallback.
type VectorIndexLoadStatus struct {
	Loaded              bool
	ExactFallbackReason string
	ManifestPath        string
	Epoch               uint64
	BytesDisk           int64
}

// VectorIndexPruneStatus reports persisted vector-index epoch cleanup.
type VectorIndexPruneStatus struct {
	IndexDir      string
	ActiveEpoch   string
	RemovedEpochs int
	RemovedBytes  int64
}

// SaveSnapshot persists the current in-memory vector index as an immutable JSON
// epoch and atomically publishes a manifest that points at it. Document IDs are
// hex-encoded so arbitrary collection primary keys round-trip losslessly. The
// JSON format is a pre-alpha/debuggable persistence format; large operational
// indexes should treat it as a rebuild accelerator until a compact binary
// snapshot format lands.
func (idx *VectorIndex) SaveSnapshot() (VectorIndexLoadStatus, error) {
	status := VectorIndexLoadStatus{}
	if idx == nil {
		return status, errors.New("collections: vector index is nil")
	}
	if idx.collection == nil || idx.collection.db == nil {
		return status, errCollectionDBNil
	}
	indexDir, err := idx.persistDir()
	if err != nil {
		return status, err
	}
	status.ManifestPath = filepath.Join(indexDir, vectorIndexManifestFile)
	unlockVectorMutation := idx.collection.lockVectorIndexMutationBarrier()
	defer unlockVectorMutation()
	if err := os.MkdirAll(indexDir, 0o755); err != nil {
		return status, err
	}
	epoch, epochName, err := nextVectorIndexSnapshotEpoch(indexDir)
	if err != nil {
		return status, err
	}
	epochDir := filepath.Join(indexDir, epochName)
	tmpDir := filepath.Join(indexDir, ".tmp-"+epochName)
	if err := os.RemoveAll(tmpDir); err != nil {
		return status, err
	}
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return status, err
	}
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			_ = os.RemoveAll(tmpDir)
		}
	}()

	if err := idx.ensureSnapshotSaveable(); err != nil {
		return status, err
	}
	marker, err := idx.collection.vectorIndexCollectionMarker()
	if err != nil {
		return status, err
	}
	snapshot, snapshotSeq := idx.persistSnapshot()
	afterMarker, err := idx.collection.vectorIndexCollectionMarker()
	if err != nil {
		return status, err
	}
	if afterMarker != marker {
		return status, errors.New("collections: collection changed while saving vector index snapshot")
	}
	files := map[string]any{
		vectorIndexMetaFile:       snapshot.Meta,
		vectorIndexNodesFile:      snapshot.Nodes,
		vectorIndexEdgesFile:      snapshot.Edges,
		vectorIndexTombstonesFile: snapshot.Tombstones,
		vectorIndexDocMapFile:     snapshot.DocMap,
	}
	fileEntries := make([]vectorIndexManifestFileEntry, 0, len(files))
	for name, payload := range files {
		data, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return status, err
		}
		data = append(data, '\n')
		path := filepath.Join(tmpDir, name)
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return status, err
		}
		if err := fsyncFile(path); err != nil {
			return status, err
		}
		sum := sha256.Sum256(data)
		fileEntries = append(fileEntries, vectorIndexManifestFileEntry{
			Name:   name,
			Size:   int64(len(data)),
			SHA256: hex.EncodeToString(sum[:]),
		})
	}
	sort.Slice(fileEntries, func(i, j int) bool {
		return fileEntries[i].Name < fileEntries[j].Name
	})
	if err := fsyncDir(tmpDir); err != nil {
		return status, err
	}
	if err := os.Rename(tmpDir, epochDir); err != nil {
		return status, err
	}
	cleanupTmp = false
	if err := fsyncDir(indexDir); err != nil {
		return status, err
	}

	manifest := vectorIndexManifest{
		FormatVersion:         vectorIndexFormatVersion,
		Collection:            idx.collection.meta.Name,
		IndexName:             idx.name,
		Epoch:                 epoch,
		EpochDir:              epochName,
		Dims:                  snapshot.Meta.Dimensions,
		Metric:                idx.metric,
		Encoding:              snapshot.Meta.Encoding,
		M:                     idx.m,
		EfConstruction:        idx.efConstruction,
		EfSearch:              idx.efSearch,
		MaxLevel:              snapshot.Meta.MaxLevel,
		NodeCount:             len(snapshot.Nodes),
		LiveDocCount:          len(snapshot.DocMap.Current),
		DeletedCount:          len(snapshot.Tombstones.NodeIDs),
		CreatedAtUnix:         time.Now().Unix(),
		CollectionCommitSeq:   marker.CommitSeq,
		CollectionSystemRoot:  marker.SystemRoot,
		CollectionPrimaryRoot: marker.PrimaryRoot,
		Files:                 fileEntries,
	}
	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return status, err
	}
	manifestData = append(manifestData, '\n')
	bytesDisk := vectorIndexSnapshotBytes(manifestData, fileEntries)
	tmpManifest := filepath.Join(indexDir, ".manifest.tmp")
	if err := os.WriteFile(tmpManifest, manifestData, 0o644); err != nil {
		return status, err
	}
	if err := fsyncFile(tmpManifest); err != nil {
		return status, err
	}
	if err := os.Rename(tmpManifest, status.ManifestPath); err != nil {
		return status, err
	}
	if err := fsyncDir(indexDir); err != nil {
		return status, err
	}
	status.Loaded = true
	status.Epoch = epoch
	status.BytesDisk = bytesDisk
	idx.recordPersistedSnapshot(epoch, bytesDisk, snapshotSeq)
	return status, nil
}

// LoadVectorIndexSnapshot loads the currently published persisted vector index
// epoch. Missing, incomplete, or corrupt snapshots return a non-loaded status
// with ExactFallbackReason set and no error, so callers can safely use exact
// search as the correctness fallback.
func (c *Collection) LoadVectorIndexSnapshot(opts VectorIndexOptions) (*VectorIndex, VectorIndexLoadStatus, error) {
	status := VectorIndexLoadStatus{}
	if c == nil {
		return nil, status, errCollectionNil
	}
	if c.db == nil {
		return nil, status, errCollectionDBNil
	}
	if opts.Name == "" {
		opts.Name = vectorIndexDefaultName(opts.Field)
	}
	indexDir, err := vectorIndexPersistDir(c.db.Dir(), c.meta.Name, opts.Name)
	if err != nil {
		return nil, status, err
	}
	status.ManifestPath = filepath.Join(indexDir, vectorIndexManifestFile)
	index, err := newVectorIndex(c, opts)
	if err != nil {
		return nil, status, err
	}
	manifestData, err := os.ReadFile(status.ManifestPath)
	if errors.Is(err, os.ErrNotExist) {
		status.ExactFallbackReason = "missing_manifest"
		return nil, status, nil
	}
	if err != nil {
		return nil, status, err
	}
	var manifest vectorIndexManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		status.ExactFallbackReason = "invalid_manifest"
		return nil, status, nil
	}
	if reason := validateVectorIndexManifest(manifest, c.meta.Name, index.name, index.metric, index.encoding, index.dimensions); reason != "" {
		status.ExactFallbackReason = reason
		return nil, status, nil
	}
	unlockVectorMutation := c.lockVectorIndexMutationBarrier()
	defer unlockVectorMutation()
	marker, err := c.vectorIndexCollectionMarker()
	if err != nil {
		return nil, status, err
	}
	if reason := validateVectorIndexManifestFreshness(manifest, marker); reason != "" {
		status.ExactFallbackReason = reason
		return nil, status, nil
	}
	epochDir := filepath.Join(indexDir, manifest.EpochDir)
	files, reason, err := readVectorIndexSnapshotFiles(epochDir, manifest.Files)
	if err != nil {
		return nil, status, err
	}
	if reason != "" {
		status.ExactFallbackReason = reason
		return nil, status, nil
	}
	if reason := validateVectorIndexSnapshotManifestCounts(manifest, files); reason != "" {
		status.ExactFallbackReason = reason
		return nil, status, nil
	}
	if reason := index.loadPersistSnapshot(files); reason != "" {
		status.ExactFallbackReason = reason
		return nil, status, nil
	}
	status.Loaded = true
	status.Epoch = manifest.Epoch
	status.BytesDisk = vectorIndexSnapshotBytes(manifestData, manifest.Files)
	index.recordLoadedSnapshot(status.Epoch, status.BytesDisk)
	c.vectorIndexesMu.Lock()
	c.registerVectorIndexLocked(index)
	c.vectorIndexesMu.Unlock()
	return index, status, nil
}

func (idx *VectorIndex) persistDir() (string, error) {
	if idx == nil || idx.collection == nil || idx.collection.db == nil {
		return "", errCollectionDBNil
	}
	return vectorIndexPersistDir(idx.collection.db.Dir(), idx.collection.meta.Name, idx.name)
}

// PruneOldSnapshots removes older immutable epoch directories for this vector
// index while preserving the currently published manifest epoch and the newest
// keep-1 additional epochs. It never removes temp directories.
func (idx *VectorIndex) PruneOldSnapshots(keep int) (VectorIndexPruneStatus, error) {
	status := VectorIndexPruneStatus{}
	if idx == nil {
		return status, errors.New("collections: vector index is nil")
	}
	if keep <= 0 {
		return status, errors.New("collections: vector index snapshot keep count must be positive")
	}
	indexDir, err := idx.persistDir()
	if err != nil {
		return status, err
	}
	status.IndexDir = indexDir
	manifestPath := filepath.Join(indexDir, vectorIndexManifestFile)
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		return status, err
	}
	var manifest vectorIndexManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return status, err
	}
	if manifest.EpochDir == "" || strings.ContainsAny(manifest.EpochDir, `/\`) {
		return status, errors.New("collections: invalid vector index manifest epoch dir")
	}
	status.ActiveEpoch = manifest.EpochDir

	epochs, err := vectorIndexEpochDirs(indexDir)
	if err != nil {
		return status, err
	}
	preserve := map[string]struct{}{manifest.EpochDir: {}}
	for i := len(epochs) - 1; i >= 0 && len(preserve) < keep; i-- {
		preserve[epochs[i]] = struct{}{}
	}
	for _, epoch := range epochs {
		if _, ok := preserve[epoch]; ok {
			continue
		}
		path := filepath.Join(indexDir, epoch)
		size, err := dirSize(path)
		if err != nil {
			return status, err
		}
		if err := os.RemoveAll(path); err != nil {
			return status, err
		}
		status.RemovedEpochs++
		status.RemovedBytes += size
	}
	if status.RemovedEpochs > 0 {
		if err := fsyncDir(indexDir); err != nil {
			return status, err
		}
	}
	return status, nil
}

func vectorIndexEpochDirs(indexDir string) ([]string, error) {
	entries, err := os.ReadDir(indexDir)
	if err != nil {
		return nil, err
	}
	var epochs []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, "epoch-") {
			epochs = append(epochs, name)
		}
	}
	sort.Strings(epochs)
	return epochs, nil
}

func nextVectorIndexSnapshotEpoch(indexDir string) (uint64, string, error) {
	now := time.Now().UnixNano()
	var epoch uint64
	if now > 0 {
		epoch = uint64(now)
	}
	advanceAfter := func(previous uint64) error {
		if previous < epoch {
			return nil
		}
		if previous == ^uint64(0) {
			return errors.New("collections: vector index snapshot epoch overflow")
		}
		epoch = previous + 1
		return nil
	}

	manifestData, err := os.ReadFile(filepath.Join(indexDir, vectorIndexManifestFile))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, "", err
	}
	if err == nil {
		var manifest vectorIndexManifest
		if json.Unmarshal(manifestData, &manifest) == nil {
			if err := advanceAfter(manifest.Epoch); err != nil {
				return 0, "", err
			}
		}
	}
	epochs, err := vectorIndexEpochDirs(indexDir)
	if err != nil {
		return 0, "", err
	}
	for _, name := range epochs {
		previous, ok := parseVectorIndexEpochDir(name)
		if !ok {
			continue
		}
		if err := advanceAfter(previous); err != nil {
			return 0, "", err
		}
	}
	for {
		epochName := fmt.Sprintf("epoch-%020d", epoch)
		_, err := os.Stat(filepath.Join(indexDir, epochName))
		if errors.Is(err, os.ErrNotExist) {
			return epoch, epochName, nil
		}
		if err != nil {
			return 0, "", err
		}
		if err := advanceAfter(epoch); err != nil {
			return 0, "", err
		}
	}
}

func parseVectorIndexEpochDir(name string) (uint64, bool) {
	if !strings.HasPrefix(name, "epoch-") {
		return 0, false
	}
	epoch, err := strconv.ParseUint(strings.TrimPrefix(name, "epoch-"), 10, 64)
	if err != nil {
		return 0, false
	}
	return epoch, true
}

func dirSize(path string) (int64, error) {
	var total int64
	err := filepath.WalkDir(path, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

func vectorIndexPersistDir(dbDir, collection, indexName string) (string, error) {
	if dbDir == "" {
		return "", errors.New("collections: vector index persistence requires a database directory")
	}
	collectionComponent, err := vectorIndexSafePathComponent(collection)
	if err != nil {
		return "", err
	}
	indexComponent, err := vectorIndexSafePathComponent(indexName)
	if err != nil {
		return "", err
	}
	return filepath.Join(dbDir, vectorIndexDirName, collectionComponent, indexComponent), nil
}

func vectorIndexDefaultName(field string) string {
	if field == "" {
		return "default"
	}
	return field
}

func vectorIndexSafePathComponent(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "." || s == ".." || strings.ContainsAny(s, `/\`) {
		return "", fmt.Errorf("collections: invalid vector index path component %q", s)
	}
	return s, nil
}

type vectorIndexManifest struct {
	FormatVersion         int                            `json:"format_version"`
	Collection            string                         `json:"collection"`
	IndexName             string                         `json:"index_name"`
	Epoch                 uint64                         `json:"epoch"`
	EpochDir              string                         `json:"epoch_dir"`
	Dims                  int                            `json:"dims"`
	Metric                VectorMetric                   `json:"metric"`
	Encoding              VectorIndexEncoding            `json:"encoding"`
	M                     int                            `json:"m"`
	EfConstruction        int                            `json:"ef_construction"`
	EfSearch              int                            `json:"ef_search"`
	MaxLevel              int                            `json:"max_level"`
	NodeCount             int                            `json:"node_count"`
	LiveDocCount          int                            `json:"live_doc_count"`
	DeletedCount          int                            `json:"deleted_doc_count"`
	CreatedAtUnix         int64                          `json:"created_at_unix"`
	CollectionCommitSeq   uint64                         `json:"collection_commit_seq"`
	CollectionSystemRoot  uint64                         `json:"collection_system_root"`
	CollectionPrimaryRoot uint64                         `json:"collection_primary_root"`
	Files                 []vectorIndexManifestFileEntry `json:"files"`
}

type vectorIndexCollectionMarker struct {
	CommitSeq   uint64
	SystemRoot  uint64
	PrimaryRoot uint64
}

type vectorIndexManifestFileEntry struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type vectorIndexPersistSnapshot struct {
	Meta       vectorIndexPersistMeta
	Nodes      []vectorIndexPersistNode
	Edges      []vectorIndexPersistEdges
	Tombstones vectorIndexPersistTombstones
	DocMap     vectorIndexPersistDocMap
}

type vectorIndexPersistMeta struct {
	Name                string              `json:"name"`
	Field               string              `json:"field"`
	Metric              VectorMetric        `json:"metric"`
	Encoding            VectorIndexEncoding `json:"encoding"`
	Dimensions          int                 `json:"dimensions"`
	M                   int                 `json:"m"`
	EfConstruction      int                 `json:"ef_construction"`
	EfSearch            int                 `json:"ef_search"`
	RebuildDeletedRatio float64             `json:"rebuild_deleted_ratio"`
	Entry               int                 `json:"entry"`
	MaxLevel            int                 `json:"max_level"`
}

type vectorIndexPersistNode struct {
	DocumentID string    `json:"document_id"`
	Vector     []float32 `json:"vector,omitempty"`
	Quantized  []int8    `json:"quantized,omitempty"`
	QuantScale float32   `json:"quant_scale,omitempty"`
	Level      int       `json:"level"`
	Deleted    bool      `json:"deleted,omitempty"`
}

type vectorIndexPersistEdges struct {
	NodeID   int   `json:"node_id"`
	Layer    int   `json:"layer"`
	Neighbor []int `json:"neighbors"`
}

type vectorIndexPersistTombstones struct {
	NodeIDs []int `json:"node_ids"`
}

type vectorIndexPersistDocMap struct {
	Current map[string]int `json:"current"`
}

func (idx *VectorIndex) persistSnapshot() (vectorIndexPersistSnapshot, uint64) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	seq := idx.mutationSeq
	snapshot := vectorIndexPersistSnapshot{
		Meta: vectorIndexPersistMeta{
			Name:                idx.name,
			Field:               idx.field,
			Metric:              idx.metric,
			Encoding:            idx.encoding,
			Dimensions:          idx.dimensions,
			M:                   idx.m,
			EfConstruction:      idx.efConstruction,
			EfSearch:            idx.efSearch,
			RebuildDeletedRatio: idx.rebuildDeletedRatio,
			Entry:               idx.entry,
			MaxLevel:            idx.maxLevel,
		},
		Nodes: make([]vectorIndexPersistNode, len(idx.nodes)),
		DocMap: vectorIndexPersistDocMap{
			Current: make(map[string]int, len(idx.currentNode)),
		},
	}
	for i, node := range idx.nodes {
		snapshot.Nodes[i] = vectorIndexPersistNode{
			DocumentID: encodeVectorIndexDocumentID(node.documentID),
			Vector:     append([]float32(nil), node.vector...),
			Quantized:  append([]int8(nil), node.quantized...),
			QuantScale: node.quantScale,
			Level:      node.level,
			Deleted:    node.deleted,
		}
		for layer, neighbors := range node.neighbors {
			snapshot.Edges = append(snapshot.Edges, vectorIndexPersistEdges{
				NodeID:   i,
				Layer:    layer,
				Neighbor: append([]int(nil), neighbors...),
			})
		}
		if node.deleted {
			snapshot.Tombstones.NodeIDs = append(snapshot.Tombstones.NodeIDs, i)
		}
	}
	for docID, nodeID := range idx.currentNode {
		snapshot.DocMap.Current[encodeVectorIndexDocumentID([]byte(docID))] = nodeID
	}
	sort.Ints(snapshot.Tombstones.NodeIDs)
	return snapshot, seq
}

func encodeVectorIndexDocumentID(documentID []byte) string {
	return hex.EncodeToString(documentID)
}

func decodeVectorIndexDocumentID(encoded string) ([]byte, bool) {
	documentID, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, false
	}
	return documentID, true
}

func vectorIndexSnapshotBytes(manifestData []byte, entries []vectorIndexManifestFileEntry) int64 {
	total := int64(len(manifestData))
	for _, entry := range entries {
		total += entry.Size
	}
	return total
}

func (idx *VectorIndex) recordPersistedSnapshot(epoch uint64, bytesDisk int64, snapshotSeq uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.persistedEpoch = epoch
	idx.persistedBytesDisk = bytesDisk
	idx.persistedSnapshotDirty = idx.mutationSeq != snapshotSeq
}

func (idx *VectorIndex) recordLoadedSnapshot(epoch uint64, bytesDisk int64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.persistedEpoch = epoch
	idx.persistedBytesDisk = bytesDisk
	idx.persistedSnapshotDirty = false
	idx.mutationSeq = 0
}

func (idx *VectorIndex) ensureSnapshotSaveable() error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.trackedMutationErrors > 0 {
		return fmt.Errorf("collections: vector index %q has %d tracked mutation errors; rebuild before saving snapshot", idx.name, idx.trackedMutationErrors)
	}
	return nil
}

func (c *Collection) vectorIndexCollectionMarker() (vectorIndexCollectionMarker, error) {
	var marker vectorIndexCollectionMarker
	if c == nil {
		return marker, errCollectionNil
	}
	if c.db == nil {
		return marker, errCollectionDBNil
	}
	if err := c.flushBufferedWrites(); err != nil {
		return marker, err
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return marker, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return marker, err
	}
	if catalog == nil {
		return marker, errCollectionNotFound
	}
	marker.CommitSeq = snapshotCommitSeq(snap)
	marker.SystemRoot = snapshotSystemRoot(snap)
	marker.PrimaryRoot = catalog.rootID(collectionPrimaryRootName(catalog.meta.Name))
	return marker, nil
}

func validateVectorIndexManifest(manifest vectorIndexManifest, collection, indexName string, metric VectorMetric, encoding VectorIndexEncoding, dimensions int) string {
	if manifest.FormatVersion != vectorIndexFormatVersion {
		return "unsupported_format_version"
	}
	if manifest.Collection != collection || manifest.IndexName != indexName {
		return "manifest_scope_mismatch"
	}
	if manifest.Metric != metric {
		return "manifest_metric_mismatch"
	}
	if manifest.Encoding != encoding {
		return "manifest_encoding_mismatch"
	}
	if dimensions != 0 && manifest.Dims != dimensions {
		return "manifest_dimension_mismatch"
	}
	if manifest.EpochDir == "" || strings.ContainsAny(manifest.EpochDir, `/\`) {
		return "invalid_epoch_dir"
	}
	if manifest.M <= 0 || manifest.EfConstruction <= 0 || manifest.EfSearch <= 0 {
		return "invalid_manifest_hnsw_params"
	}
	if manifest.NodeCount < 0 || manifest.LiveDocCount < 0 || manifest.DeletedCount < 0 {
		return "invalid_manifest_counts"
	}
	if len(manifest.Files) == 0 {
		return "manifest_missing_files"
	}
	return ""
}

func validateVectorIndexManifestFreshness(manifest vectorIndexManifest, marker vectorIndexCollectionMarker) string {
	if manifest.CollectionCommitSeq == 0 || manifest.CollectionSystemRoot == 0 {
		return "missing_collection_freshness"
	}
	if manifest.CollectionCommitSeq != marker.CommitSeq ||
		manifest.CollectionSystemRoot != marker.SystemRoot ||
		manifest.CollectionPrimaryRoot != marker.PrimaryRoot {
		return "stale_collection_snapshot"
	}
	return ""
}

func validateVectorIndexSnapshotManifestCounts(manifest vectorIndexManifest, snapshot vectorIndexPersistSnapshot) string {
	if manifest.NodeCount != len(snapshot.Nodes) {
		return "manifest_node_count_mismatch"
	}
	if manifest.LiveDocCount != len(snapshot.DocMap.Current) {
		return "manifest_live_count_mismatch"
	}
	if manifest.DeletedCount != len(snapshot.Tombstones.NodeIDs) {
		return "manifest_deleted_count_mismatch"
	}
	if snapshot.Meta.EfConstruction != manifest.EfConstruction || snapshot.Meta.EfSearch != manifest.EfSearch || snapshot.Meta.M != manifest.M {
		return "manifest_meta_param_mismatch"
	}
	if snapshot.Meta.Encoding != manifest.Encoding {
		return "manifest_meta_encoding_mismatch"
	}
	if snapshot.Meta.MaxLevel != manifest.MaxLevel {
		return "manifest_max_level_mismatch"
	}
	return ""
}

func validateVectorIndexPersistNode(node vectorIndexPersistNode, meta vectorIndexPersistMeta) string {
	switch meta.Encoding {
	case VectorIndexEncodingFloat32:
		if len(node.Vector) != meta.Dimensions || len(node.Quantized) != 0 {
			return "node_dimension_mismatch"
		}
		if err := validateFloat32Vector(node.Vector); err != nil {
			return "invalid_node_vector"
		}
		if meta.Metric == VectorMetricCosine && vectorNormSquared(node.Vector) == 0 {
			return "invalid_zero_cosine_vector"
		}
	case VectorIndexEncodingInt8:
		if len(node.Quantized) != meta.Dimensions || len(node.Vector) != 0 {
			return "node_dimension_mismatch"
		}
		if node.QuantScale <= 0 || math.IsNaN(float64(node.QuantScale)) || math.IsInf(float64(node.QuantScale), 0) {
			return "invalid_quant_scale"
		}
		if meta.Metric == VectorMetricCosine {
			var norm float32
			for _, value := range node.Quantized {
				scaled := float32(value) * node.QuantScale
				norm += scaled * scaled
			}
			if norm == 0 {
				return "invalid_zero_cosine_vector"
			}
		}
	default:
		return "invalid_encoding"
	}
	return ""
}

func readVectorIndexSnapshotFiles(epochDir string, entries []vectorIndexManifestFileEntry) (vectorIndexPersistSnapshot, string, error) {
	var snapshot vectorIndexPersistSnapshot
	for _, entry := range entries {
		if entry.Name == "" || strings.ContainsAny(entry.Name, `/\`) {
			return snapshot, "invalid_file_name", nil
		}
		data, err := os.ReadFile(filepath.Join(epochDir, entry.Name))
		if errors.Is(err, os.ErrNotExist) {
			return snapshot, "missing_epoch_file", nil
		}
		if err != nil {
			return snapshot, "", err
		}
		if int64(len(data)) != entry.Size {
			return snapshot, "file_size_mismatch", nil
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != entry.SHA256 {
			return snapshot, "file_checksum_mismatch", nil
		}
		switch entry.Name {
		case vectorIndexMetaFile:
			if err := json.Unmarshal(data, &snapshot.Meta); err != nil {
				return snapshot, "invalid_meta_file", nil
			}
		case vectorIndexNodesFile:
			if err := json.Unmarshal(data, &snapshot.Nodes); err != nil {
				return snapshot, "invalid_nodes_file", nil
			}
		case vectorIndexEdgesFile:
			if err := json.Unmarshal(data, &snapshot.Edges); err != nil {
				return snapshot, "invalid_edges_file", nil
			}
		case vectorIndexTombstonesFile:
			if err := json.Unmarshal(data, &snapshot.Tombstones); err != nil {
				return snapshot, "invalid_tombstones_file", nil
			}
		case vectorIndexDocMapFile:
			if err := json.Unmarshal(data, &snapshot.DocMap); err != nil {
				return snapshot, "invalid_docmap_file", nil
			}
		}
	}
	return snapshot, "", nil
}

func (idx *VectorIndex) loadPersistSnapshot(snapshot vectorIndexPersistSnapshot) string {
	if snapshot.Meta.Field != idx.field || snapshot.Meta.Metric != idx.metric {
		return "meta_mismatch"
	}
	encoding, err := normalizeVectorIndexEncoding(snapshot.Meta.Encoding)
	if err != nil {
		return "invalid_encoding"
	}
	if encoding != idx.encoding {
		return "meta_encoding_mismatch"
	}
	if idx.dimensions != 0 && snapshot.Meta.Dimensions != idx.dimensions {
		return "meta_dimension_mismatch"
	}
	if snapshot.Meta.Dimensions < 0 {
		return "invalid_dimensions"
	}
	if len(snapshot.Nodes) == 0 {
		if snapshot.Meta.Entry >= 0 || snapshot.Meta.MaxLevel >= 0 || len(snapshot.Edges) != 0 || len(snapshot.Tombstones.NodeIDs) != 0 || len(snapshot.DocMap.Current) != 0 {
			return "invalid_empty_index"
		}
		idx.mu.Lock()
		defer idx.mu.Unlock()
		idx.name = snapshot.Meta.Name
		idx.encoding = encoding
		idx.dimensions = snapshot.Meta.Dimensions
		idx.m = snapshot.Meta.M
		idx.efConstruction = snapshot.Meta.EfConstruction
		idx.efSearch = snapshot.Meta.EfSearch
		idx.rebuildDeletedRatio = snapshot.Meta.RebuildDeletedRatio
		idx.nodes = nil
		idx.currentNode = make(map[string]int)
		idx.entry = -1
		idx.maxLevel = -1
		idx.persistedEpoch = 0
		idx.persistedBytesDisk = 0
		idx.persistedSnapshotDirty = false
		idx.lastRebuildDuration = 0
		idx.mutationSeq = 0
		return ""
	}
	if snapshot.Meta.Dimensions <= 0 {
		return "invalid_dimensions"
	}
	tombstoned := make(map[int]struct{}, len(snapshot.Tombstones.NodeIDs))
	for _, nodeID := range snapshot.Tombstones.NodeIDs {
		if nodeID < 0 || nodeID >= len(snapshot.Nodes) {
			return "invalid_tombstone"
		}
		tombstoned[nodeID] = struct{}{}
	}
	nodes := make([]vectorIndexNode, len(snapshot.Nodes))
	for i, node := range snapshot.Nodes {
		if reason := validateVectorIndexPersistNode(node, snapshot.Meta); reason != "" {
			return reason
		}
		documentID, ok := decodeVectorIndexDocumentID(node.DocumentID)
		if !ok || len(documentID) == 0 {
			return "invalid_document_id"
		}
		_, deletedByTombstone := tombstoned[i]
		if node.Deleted != deletedByTombstone {
			return "tombstone_mismatch"
		}
		if node.Level < 0 {
			return "invalid_node_level"
		}
		nodes[i] = vectorIndexNode{
			documentID: documentID,
			vector:     append([]float32(nil), node.Vector...),
			quantized:  append([]int8(nil), node.Quantized...),
			quantScale: node.QuantScale,
			level:      node.Level,
			neighbors:  make([][]int, node.Level+1),
			deleted:    node.Deleted,
		}
	}
	for _, edge := range snapshot.Edges {
		if edge.NodeID < 0 || edge.NodeID >= len(nodes) {
			return "invalid_edge_node"
		}
		if edge.Layer < 0 || edge.Layer > nodes[edge.NodeID].level {
			return "invalid_edge_layer"
		}
		for _, neighbor := range edge.Neighbor {
			if neighbor < 0 || neighbor >= len(nodes) {
				return "invalid_edge_neighbor"
			}
			if nodes[neighbor].level < edge.Layer {
				return "edge_neighbor_missing_layer"
			}
		}
		nodes[edge.NodeID].neighbors[edge.Layer] = append([]int(nil), edge.Neighbor...)
	}
	current := make(map[string]int, len(snapshot.DocMap.Current))
	for encodedDocID, nodeID := range snapshot.DocMap.Current {
		if nodeID < 0 || nodeID >= len(nodes) {
			return "invalid_docmap_node"
		}
		if nodes[nodeID].deleted {
			return "docmap_points_to_deleted_node"
		}
		documentID, ok := decodeVectorIndexDocumentID(encodedDocID)
		if !ok || len(documentID) == 0 {
			return "invalid_docmap_document_id"
		}
		if !bytes.Equal(nodes[nodeID].documentID, documentID) {
			return "docmap_document_mismatch"
		}
		current[string(documentID)] = nodeID
	}
	entry := snapshot.Meta.Entry
	if entry < 0 || entry >= len(nodes) || nodes[entry].deleted {
		entry = -1
		for i := range nodes {
			if !nodes[i].deleted {
				entry = i
				break
			}
		}
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.name = snapshot.Meta.Name
	idx.encoding = encoding
	idx.dimensions = snapshot.Meta.Dimensions
	idx.m = snapshot.Meta.M
	idx.efConstruction = snapshot.Meta.EfConstruction
	idx.efSearch = snapshot.Meta.EfSearch
	idx.rebuildDeletedRatio = snapshot.Meta.RebuildDeletedRatio
	idx.nodes = nodes
	idx.currentNode = current
	idx.entry = entry
	idx.maxLevel = idx.maxLiveLevelLocked()
	idx.persistedEpoch = 0
	idx.persistedBytesDisk = 0
	idx.persistedSnapshotDirty = false
	idx.lastRebuildDuration = 0
	idx.mutationSeq = 0
	return ""
}

func fsyncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}

func fsyncDir(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}
