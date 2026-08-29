package collections

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	columnAssetManagerAssetsDirName        = "assets"
	columnAssetManagerSegmentsDirName      = "segments"
	columnAssetManagerIndexesDirName       = "indexes"
	columnAssetManagerPreparedDirName      = "prepared"
	columnAssetManagerQuarantineDirName    = "quarantine"
	columnAssetManagerTempDirName          = "tmp"
	columnAssetSegmentFilePrefix           = "segment-"
	columnAssetSegmentFileSuffix           = ".tca"
	columnAssetM12ASegmentFileID           = uint32(1)
	columnAssetDirectViewSegmentFileIDBase = uint32(1 << 20)
)

const columnAssetSegmentWriteLockStripes = 64
const columnPhysicalAssetReadScratchPoolMaxRetainBytes = 16 << 20
const columnAssetVerifiedChecksumCacheSlots = 4096
const typedColumnPartDirectViewAssetAlignment = 8
const dictionaryCodesDirectViewAssetAlignment = columnDictionaryCodesPayloadAlignment
const int64ValuesDirectViewAssetAlignment = columnInt64ValuesPayloadAlignment

// columnAssetSegmentWriteLocks is a bounded stripe set keyed by canonical
// segment path. Writers to the same segment share a process-local offset lock
// without retaining one mutex per temp dir or segment path forever.
var columnAssetSegmentWriteLocks [columnAssetSegmentWriteLockStripes]sync.Mutex

var columnPhysicalAssetReadScratchPool sync.Pool

// columnAssetSegmentAllocationLocks serialize same-process segment file-id
// allocation by namespace without retaining one lock per temp dir forever.
// O_EXCL still protects against external process races.
var columnAssetSegmentAllocationLocks [columnAssetSegmentWriteLockStripes]sync.Mutex

// columnAssetSegmentAllocationCaches avoid repeated directory scans for hot
// same-namespace segment allocation while keeping cache memory bounded by the
// stripe count. Collisions only evict the cached hint; O_EXCL remains the source
// of truth for file-id ownership.
var columnAssetSegmentAllocationCaches [columnAssetSegmentWriteLockStripes]columnAssetSegmentAllocationCache

var columnAssetVerifiedChecksumCache = struct {
	sync.Mutex
	entries [columnAssetVerifiedChecksumCacheSlots]columnAssetVerifiedChecksumEntry
}{}

var columnAssetManagerNamespacePathCaches [columnAssetSegmentWriteLockStripes]columnAssetManagerNamespacePathCache
var columnAssetSegmentDirSyncCaches [columnAssetSegmentWriteLockStripes]columnAssetSegmentDirSyncCache
var syncColumnAssetSegmentFileForPublish = syncColumnAssetSegmentFile
var syncColumnAssetSegmentDirForPublish = syncColumnAssetDir
var openStableColumnAssetParent = rootpublication.OpenStableParent
var stableColumnAssetResourceTokenForPublish = stableColumnAssetResourceToken
var stableColumnAssetResourceTokenWithRegistryForPublish = stableColumnAssetResourceTokenWithRegistry

type columnAssetVerifiedChecksumEntry struct {
	key   columnAssetVerifiedChecksumKey
	valid bool
}

type columnAssetVerifiedChecksumFileIdentity struct {
	dev             uint64
	ino             uint64
	size            int64
	modTimeUnixNano int64
	valid           bool
}

type columnAssetVerifiedChecksumKey struct {
	rootDir    string
	kind       ColumnAssetKind
	namespace  string
	generation uint64
	partID     uint64
	fileID     uint32
	offset     int64
	length     int64
	checksum   uint32
	fileDev    uint64
	fileIno    uint64
	fileSize   int64
	fileModNS  int64
}

type columnAssetSegmentAllocationCache struct {
	segmentDir string
	nextFileID uint32
	valid      bool
}

type columnAssetSegmentDirSyncCache struct {
	sync.Mutex
	assetPath string
	known     bool
}

type columnAssetManagerNamespacePathCache struct {
	sync.Mutex
	rootDir   string
	namespace string
	value     columnAssetManagerNamespace
	valid     bool
}

type columnAssetManagerNamespace struct {
	ManagerRootDir string
	RootDir        string
	AssetDir       string
	SegmentDir     string
	IndexDir       string
	PreparedDir    string
	QuarantineDir  string
	TempDir        string
}

func columnAssetManagerNamespaceForRoot(rootDir, namespace string) (columnAssetManagerNamespace, error) {
	if rootDir == "" {
		return columnAssetManagerNamespace{}, errors.New("collections: column asset manager root dir is required")
	}
	cache := &columnAssetManagerNamespacePathCaches[columnAssetNamespacePathCacheIndex(rootDir, namespace)]
	cache.Lock()
	if cache.valid && cache.rootDir == rootDir && cache.namespace == namespace {
		value := cache.value
		cache.Unlock()
		return value, nil
	}
	cache.Unlock()
	cleanNamespace, err := cleanColumnAssetNamespace(namespace)
	if err != nil {
		return columnAssetManagerNamespace{}, err
	}
	namespaceRoot := filepath.Join(rootDir, filepath.FromSlash(cleanNamespace))
	assetDir := filepath.Join(namespaceRoot, columnAssetManagerAssetsDirName)
	value := columnAssetManagerNamespace{
		ManagerRootDir: rootDir,
		RootDir:        namespaceRoot,
		AssetDir:       assetDir,
		SegmentDir:     filepath.Join(assetDir, columnAssetManagerSegmentsDirName),
		IndexDir:       filepath.Join(assetDir, columnAssetManagerIndexesDirName),
		PreparedDir:    filepath.Join(namespaceRoot, columnAssetManagerPreparedDirName),
		QuarantineDir:  filepath.Join(namespaceRoot, columnAssetManagerQuarantineDirName),
		TempDir:        filepath.Join(namespaceRoot, columnAssetManagerTempDirName),
	}
	cache.Lock()
	cache.rootDir = rootDir
	cache.namespace = namespace
	cache.value = value
	cache.valid = true
	cache.Unlock()
	return value, nil
}

func columnAssetNamespacePathCacheIndex(rootDir, namespace string) uint64 {
	const (
		fnvOffset64 = 14695981039346656037
		fnvPrime64  = 1099511628211
	)
	hash := uint64(fnvOffset64)
	for i := 0; i < len(rootDir); i++ {
		hash ^= uint64(rootDir[i])
		hash *= fnvPrime64
	}
	hash ^= '/'
	hash *= fnvPrime64
	for i := 0; i < len(namespace); i++ {
		hash ^= uint64(namespace[i])
		hash *= fnvPrime64
	}
	return hash % uint64(len(columnAssetManagerNamespacePathCaches))
}

func cleanColumnAssetNamespace(namespace string) (string, error) {
	if namespace == "" {
		return "", errors.New("collections: column asset namespace is required")
	}
	if strings.TrimSpace(namespace) != namespace || strings.Contains(namespace, "\x00") ||
		strings.Contains(namespace, `\`) || strings.Contains(namespace, ":") || strings.HasPrefix(namespace, "/") {
		return "", fmt.Errorf("collections: invalid column asset namespace %q", namespace)
	}
	clean := path.Clean(namespace)
	if clean == "." || clean != namespace {
		return "", fmt.Errorf("collections: invalid column asset namespace %q", namespace)
	}
	for _, part := range strings.Split(clean, "/") {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("collections: invalid column asset namespace %q", namespace)
		}
	}
	return clean, nil
}

func ensureColumnAssetManagerNamespace(namespace columnAssetManagerNamespace) error {
	dirs, err := columnAssetManagerNamespaceDirs(namespace)
	if err != nil {
		return err
	}
	syncParents := make([]string, 0, len(dirs))
	seenSyncParents := make(map[string]struct{}, len(dirs))
	for _, dir := range dirs {
		if dir == "" {
			return errors.New("collections: column asset manager namespace has empty dir")
		}
		info, err := os.Stat(dir)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("collections: column asset manager path %q is not a directory", dir)
			}
			continue
		}
		if !os.IsNotExist(err) {
			return err
		}
		created := false
		if err := os.Mkdir(dir, 0o700); err != nil {
			if !errors.Is(err, os.ErrExist) {
				return err
			}
			info, statErr := os.Stat(dir)
			if statErr != nil {
				return statErr
			}
			if !info.IsDir() {
				return fmt.Errorf("collections: column asset manager path %q is not a directory", dir)
			}
		} else {
			created = true
		}
		if created {
			if err := durabilitycut.EmitNamespace(durabilitycut.NamespaceCreate, durabilitycut.ResourceAuxiliary, filepath.Dir(dir), "", dir); err != nil {
				return err
			}
		}
		parent := filepath.Dir(dir)
		if parent != "" && parent != dir {
			if _, ok := seenSyncParents[parent]; !ok {
				seenSyncParents[parent] = struct{}{}
				syncParents = append(syncParents, parent)
			}
		}
	}
	for _, parent := range syncParents {
		if err := syncColumnAssetDir(parent); err != nil {
			return err
		}
	}
	return nil
}

// ensureStableColumnAssetManagerNamespace establishes every ancestor required
// by a stable column child through a chain of exact retained-parent handles.
// This lets Windows use its narrower create-through-child persistence contract
// without treating prior os.Stat success as durability authority.
func ensureStableColumnAssetManagerNamespace(namespace columnAssetManagerNamespace, registry *rootpublication.IdentityPinRegistry) (*os.File, error) {
	if !rootpublication.StableNamespaceCreationSupported() {
		return nil, fmt.Errorf("%w: stable column asset publication requires exact child creation persistence", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	dirs, err := columnAssetManagerNamespaceDirs(namespace)
	if err != nil {
		return nil, err
	}
	anchorPath := filepath.Clean(filepath.Dir(namespace.ManagerRootDir))
	anchor, err := openStableColumnAssetParent(anchorPath)
	if err != nil {
		return nil, err
	}
	handles := map[string]*os.File{anchorPath: anchor}
	defer func() {
		for _, handle := range handles {
			_ = handle.Close()
		}
	}()
	for _, dir := range dirs {
		cleanDir := filepath.Clean(dir)
		parentPath := filepath.Clean(filepath.Dir(cleanDir))
		parent := handles[parentPath]
		if parent == nil {
			return nil, fmt.Errorf("%w: stable column namespace parent %q has no retained authority", rootpublication.ErrUnresolvedResource, parentPath)
		}
		_, statErr := os.Stat(cleanDir)
		missingBefore := errors.Is(statErr, os.ErrNotExist)
		if statErr != nil && !missingBefore {
			return nil, statErr
		}
		child, err := rootpublication.EnsureStableChildDirectory(parent, filepath.Base(cleanDir), 0o700, registry)
		if err != nil {
			return nil, err
		}
		if missingBefore {
			if err := durabilitycut.EmitNamespace(durabilitycut.NamespaceCreate, durabilitycut.ResourceAuxiliary, parentPath, "", cleanDir); err != nil {
				_ = child.Close()
				return nil, err
			}
		}
		persistencePath := parentPath
		if runtime.GOOS == "windows" {
			persistencePath = cleanDir
		}
		emitPersistenceBoundary := func(point durabilitycut.Point) error {
			if runtime.GOOS == "windows" && missingBefore {
				return durabilitycut.EmitCreatedDirectoryPath(point, durabilitycut.ResourceAuxiliary, persistencePath, persistencePath, cleanDir)
			}
			return durabilitycut.EmitPath(point, durabilitycut.ResourceAuxiliary, persistencePath, persistencePath)
		}
		if err := emitPersistenceBoundary(durabilitycut.BeforeNewFileDirectorySync); err != nil {
			_ = child.Close()
			return nil, err
		}
		if err := emitPersistenceBoundary(durabilitycut.AfterNewFileDirectorySync); err != nil {
			_ = child.Close()
			return nil, err
		}
		handles[cleanDir] = child
	}
	segmentDir := filepath.Clean(namespace.SegmentDir)
	segmentParent := handles[segmentDir]
	if segmentParent == nil {
		return nil, fmt.Errorf("%w: stable column segment directory %q has no retained authority", rootpublication.ErrUnresolvedResource, segmentDir)
	}
	delete(handles, segmentDir)
	return segmentParent, nil
}

func columnAssetManagerNamespaceDirs(namespace columnAssetManagerNamespace) ([]string, error) {
	if namespace.ManagerRootDir == "" {
		return nil, errors.New("collections: column asset manager root dir is required")
	}
	if namespace.RootDir == "" {
		return nil, errors.New("collections: column asset manager namespace root dir is required")
	}
	dirs := []string{namespace.ManagerRootDir}
	relRoot, err := filepath.Rel(namespace.ManagerRootDir, namespace.RootDir)
	if err != nil {
		return nil, err
	}
	if relRoot == "." || relRoot == ".." || strings.HasPrefix(relRoot, ".."+string(filepath.Separator)) || filepath.IsAbs(relRoot) {
		return nil, fmt.Errorf("collections: column asset manager namespace root %q escapes manager root %q", namespace.RootDir, namespace.ManagerRootDir)
	}
	current := namespace.ManagerRootDir
	for _, part := range strings.Split(relRoot, string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		dirs = append(dirs, current)
	}
	dirs = append(dirs,
		namespace.AssetDir,
		namespace.SegmentDir,
		namespace.IndexDir,
		namespace.PreparedDir,
		namespace.QuarantineDir,
		namespace.TempDir,
	)
	return dirs, nil
}

func writeColumnPhysicalAssetToManager(rootDir string, cfg ColumnStoreConfig, payload []byte, generation, partID uint64) (ColumnAssetRef, error) {
	return writeColumnAssetToManager(rootDir, cfg, payload, ColumnAssetKindTCS1PartImage, generation, partID)
}

func writeTypedColumnPartAssetToManager(rootDir string, cfg ColumnStoreConfig, payload []byte, generation, partID uint64) (ColumnAssetRef, error) {
	ref, _, err := writeTypedColumnPartAssetToManagerWithStats(rootDir, cfg, payload, generation, partID)
	return ref, err
}

func writeTypedColumnPartAssetToManagerWithStats(rootDir string, cfg ColumnStoreConfig, payload []byte, generation, partID uint64) (ColumnAssetRef, columnPhysicalAssetSegmentCloseStats, error) {
	if columnStoreConfigNeedsDirectViewTypedColumnAlignment(cfg) {
		fileID, err := directViewTypedColumnSegmentFileID(generation)
		if err != nil {
			return ColumnAssetRef{}, columnPhysicalAssetSegmentCloseStats{}, err
		}
		return writeColumnAssetToManagerSegmentWithStats(rootDir, cfg, payload, ColumnAssetKindTCS1TypedColumnPart, generation, partID, fileID)
	}
	return writeColumnAssetToManagerWithStats(rootDir, cfg, payload, ColumnAssetKindTCS1TypedColumnPart, generation, partID)
}

func columnStoreConfigNeedsDirectViewTypedColumnAlignment(cfg ColumnStoreConfig) bool {
	for _, column := range cfg.Columns {
		if !columnStoreColumnIsTypedColumnPart(column) || column.Nullable {
			continue
		}
		switch column.ValueType {
		case ColumnStoreValueInt64, ColumnStoreValueFloat32, ColumnStoreValueDouble:
			if column.FixedWidthEncoding == ColumnFixedWidthEncodingLittleEndian {
				return true
			}
		case ColumnStoreValueFloat32Vector:
			if column.VectorDims > 0 || column.ElementsPerRow > 0 {
				return true
			}
		case ColumnStoreValueUint8Vector, ColumnStoreValueInt8Vector, ColumnStoreValueUint16Vector, ColumnStoreValueInt16Vector, ColumnStoreValueUint32Vector, ColumnStoreValueInt32Vector, ColumnStoreValueUint64Vector, ColumnStoreValueInt64Vector, ColumnStoreValueFloat16Vector, ColumnStoreValueBFloat16Vector, ColumnStoreValueFloat64Vector:
			return true
		case ColumnStoreValueUint32List, ColumnStoreValueBytes:
			return true
		case ColumnStoreValueInt8, ColumnStoreValueUint8, ColumnStoreValueInt16, ColumnStoreValueUint16, ColumnStoreValueInt32, ColumnStoreValueUint32, ColumnStoreValueUint64, ColumnStoreValueFloat16, ColumnStoreValueBFloat16:
			return true
		case ColumnStoreValueAdjacencyList:
			if column.AdjacencyLayout == ColumnAdjacencyListLayoutUint32OffsetsList {
				return true
			}
		}
	}
	return false
}

func directViewTypedColumnSegmentFileID(generation uint64) (uint32, error) {
	base := uint64(columnAssetDirectViewSegmentFileIDBase)
	if generation == 0 || generation > uint64(^uint32(0))-base {
		return 0, fmt.Errorf("collections: typed-column direct-view generation=%d cannot form segment file id", generation)
	}
	return uint32(base + generation), nil
}

func columnAssetSegmentFileIDIsDirectView(fileID uint32) bool {
	return fileID >= columnAssetDirectViewSegmentFileIDBase
}

func writeColumnAggregateMetadataAssetToManager(rootDir string, cfg ColumnStoreConfig, payload []byte, generation, partID uint64) (ColumnAssetRef, error) {
	return writeColumnAssetToManager(rootDir, cfg, payload, ColumnAssetKindTCS1AggregateMetadata, generation, partID)
}

func writeColumnDictionaryCodesAssetToManager(rootDir string, cfg ColumnStoreConfig, payload []byte, generation, partID uint64) (ColumnAssetRef, error) {
	return writeColumnAssetToManager(rootDir, cfg, payload, ColumnAssetKindTCS1DictionaryCodes, generation, partID)
}

func writeColumnInt64ValuesAssetToManager(rootDir string, cfg ColumnStoreConfig, payload []byte, generation, partID uint64) (ColumnAssetRef, error) {
	return writeColumnAssetToManager(rootDir, cfg, payload, ColumnAssetKindTCS1Int64Values, generation, partID)
}

func writeColumnAssetToManager(rootDir string, cfg ColumnStoreConfig, payload []byte, kind ColumnAssetKind, generation, partID uint64) (ColumnAssetRef, error) {
	ref, _, err := writeColumnAssetToManagerWithStats(rootDir, cfg, payload, kind, generation, partID)
	return ref, err
}

func writeColumnAssetToManagerWithStats(rootDir string, cfg ColumnStoreConfig, payload []byte, kind ColumnAssetKind, generation, partID uint64) (ColumnAssetRef, columnPhysicalAssetSegmentCloseStats, error) {
	return writeColumnAssetToManagerSegmentWithStats(rootDir, cfg, payload, kind, generation, partID, columnAssetM12ASegmentFileID)
}

func writeColumnAssetToManagerWithStableResource(rootDir string, cfg ColumnStoreConfig, payload []byte, kind ColumnAssetKind, generation, partID uint64) (ColumnAssetRef, *rootpublication.StableResourceToken, error) {
	var token *rootpublication.StableResourceToken
	capture := func(file, parent *os.File, ref ColumnAssetRef, namespace columnAssetManagerNamespace, namespaceNeedsSync bool) (bool, error) {
		var namespaceToken *rootpublication.StableNamespaceToken
		if namespaceNeedsSync {
			var err error
			namespaceToken, err = stableColumnAssetNamespaceToken(parent, file, ref)
			if err != nil {
				return false, err
			}
			if err := stabilizeColumnAssetNamespaceForPublish(namespaceToken, namespace); err != nil {
				namespaceToken.Release()
				return false, err
			}
			defer namespaceToken.Release()
		}
		var err error
		token, err = stableColumnAssetResourceTokenForPublish(file, ref, namespaceToken)
		return namespaceNeedsSync, err
	}
	ref, _, err := writeColumnAssetToManagerSegmentWithStatsAndCapture(rootDir, cfg, payload, kind, generation, partID, columnAssetM12ASegmentFileID, capture)
	if err != nil {
		token.Release()
		return ColumnAssetRef{}, nil, err
	}
	return ref, token, nil
}

func writeColumnPhysicalAssetToManagerSegment(rootDir string, cfg ColumnStoreConfig, payload []byte, generation, partID uint64, fileID uint32) (ColumnAssetRef, error) {
	return writeColumnAssetToManagerSegment(rootDir, cfg, payload, ColumnAssetKindTCS1PartImage, generation, partID, fileID)
}

func writeColumnAssetToManagerSegment(rootDir string, cfg ColumnStoreConfig, payload []byte, kind ColumnAssetKind, generation, partID uint64, fileID uint32) (ColumnAssetRef, error) {
	ref, _, err := writeColumnAssetToManagerSegmentWithStats(rootDir, cfg, payload, kind, generation, partID, fileID)
	return ref, err
}

func writeColumnAssetToManagerSegmentWithStats(rootDir string, cfg ColumnStoreConfig, payload []byte, kind ColumnAssetKind, generation, partID uint64, fileID uint32) (ColumnAssetRef, columnPhysicalAssetSegmentCloseStats, error) {
	return writeColumnAssetToManagerSegmentWithStatsAndCapture(rootDir, cfg, payload, kind, generation, partID, fileID, nil)
}

type columnAssetStableResourceCapture func(*os.File, *os.File, ColumnAssetRef, columnAssetManagerNamespace, bool) (bool, error)

func writeColumnAssetToManagerSegmentWithStatsAndCapture(rootDir string, cfg ColumnStoreConfig, payload []byte, kind ColumnAssetKind, generation, partID uint64, fileID uint32, capture columnAssetStableResourceCapture) (ColumnAssetRef, columnPhysicalAssetSegmentCloseStats, error) {
	if cfg.AssetManager == nil {
		return ColumnAssetRef{}, columnPhysicalAssetSegmentCloseStats{}, errors.New("collections: column physical asset write requires asset manager")
	}
	if cfg.AssetManager.Kind != ColumnAssetManagerValueLogShaped {
		return ColumnAssetRef{}, columnPhysicalAssetSegmentCloseStats{}, fmt.Errorf("collections: unsupported column asset manager %q", cfg.AssetManager.Kind)
	}
	if !cfg.AssetManager.IsolatedNamespace {
		return ColumnAssetRef{}, columnPhysicalAssetSegmentCloseStats{}, errors.New("collections: column physical asset write requires isolated namespace")
	}
	if len(payload) == 0 {
		return ColumnAssetRef{}, columnPhysicalAssetSegmentCloseStats{}, errors.New("collections: column physical asset payload is empty")
	}
	if generation == 0 || partID == 0 {
		return ColumnAssetRef{}, columnPhysicalAssetSegmentCloseStats{}, errors.New("collections: column physical asset write requires generation and part_id")
	}
	if fileID == 0 {
		return ColumnAssetRef{}, columnPhysicalAssetSegmentCloseStats{}, errors.New("collections: column physical asset write requires file_id")
	}
	checksum := page.Checksum(payload)
	namespace, err := columnAssetManagerNamespaceForRoot(rootDir, cfg.AssetManager.Namespace)
	if err != nil {
		return ColumnAssetRef{}, columnPhysicalAssetSegmentCloseStats{}, err
	}
	var stableParent *os.File
	if capture != nil {
		stableParent, err = ensureStableColumnAssetManagerNamespace(namespace, nil)
	} else {
		err = ensureColumnAssetManagerNamespace(namespace)
	}
	if err != nil {
		return ColumnAssetRef{}, columnPhysicalAssetSegmentCloseStats{}, err
	}
	if stableParent != nil {
		defer stableParent.Close()
	}
	ref := ColumnAssetRef{
		Kind:       kind,
		Namespace:  cfg.AssetManager.Namespace,
		Generation: generation,
		PartID:     partID,
		FileID:     fileID,
		Length:     int64(len(payload)),
		Checksum:   checksum,
	}
	assetPath, err := columnAssetSegmentPath(rootDir, ref)
	if err != nil {
		return ColumnAssetRef{}, columnPhysicalAssetSegmentCloseStats{}, err
	}
	segmentLock := columnAssetSegmentWriteLock(assetPath)
	segmentLock.Lock()
	defer segmentLock.Unlock()
	if capture != nil {
		clearColumnAssetSegmentDirSyncKnown(assetPath)
	}
	var file *os.File
	var needsDirSync, created bool
	if stableParent != nil {
		file, needsDirSync, created, err = openColumnAssetSegmentAppendFileAt(stableParent, assetPath)
	} else {
		file, needsDirSync, created, err = openColumnAssetSegmentAppendFile(assetPath)
	}
	if err != nil {
		return ColumnAssetRef{}, columnPhysicalAssetSegmentCloseStats{}, err
	}
	if created {
		if err := durabilitycut.EmitNamespace(durabilitycut.NamespaceCreate, durabilitycut.ResourceAuxiliary, namespace.SegmentDir, "", assetPath); err != nil {
			_ = file.Close()
			return ColumnAssetRef{}, columnPhysicalAssetSegmentCloseStats{}, err
		}
	}
	if created {
		// Creation does not establish pathname durability. Keep the cache
		// unknown until the successful publish path stabilizes the directory.
		clearColumnAssetSegmentDirSyncKnown(assetPath)
	}
	var closeStats columnPhysicalAssetSegmentCloseStats
	closeFile := true
	defer func() {
		if closeFile {
			_ = file.Close()
		}
	}()
	rollbackOffset := int64(-1)
	fail := func(cause error) (ColumnAssetRef, columnPhysicalAssetSegmentCloseStats, error) {
		var cleanupErr error
		if created {
			if closeFile {
				cleanupErr = errors.Join(cleanupErr, file.Close())
				closeFile = false
			}
			clearColumnAssetSegmentDirSyncKnown(assetPath)
		} else if rollbackOffset >= 0 {
			cleanupErr = errors.Join(cleanupErr, file.Truncate(rollbackOffset), file.Sync())
		}
		return ColumnAssetRef{}, closeStats, errors.Join(cause, cleanupErr)
	}
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return fail(err)
	}
	rollbackOffset = offset
	alignment := columnAssetSegmentPayloadAlignment(kind, cfg)
	padding := columnAssetSegmentPrefixPadding(offset, alignment)
	if padding > 0 {
		written, err := writeColumnAssetSegmentZeroPadding(file, padding)
		if err != nil {
			return fail(err)
		}
		if written != padding {
			return fail(io.ErrShortWrite)
		}
		offset += int64(padding)
	}
	written, err := writeColumnAssetSegmentPayload(file, payload)
	if err != nil {
		return fail(err)
	}
	if written != len(payload) {
		return fail(io.ErrShortWrite)
	}
	ref.Offset = offset
	start := time.Now()
	closeStats.FileSyncCount++
	if err := syncColumnAssetSegmentFileObserved(file, namespace.SegmentDir); err != nil {
		closeStats.FileSync += time.Since(start)
		return fail(err)
	}
	closeStats.FileSync += time.Since(start)
	directoryStable := false
	if capture != nil {
		start = time.Now()
		directoryStable, err = capture(file, stableParent, ref, namespace, needsDirSync)
		if needsDirSync {
			closeStats.DirSync += time.Since(start)
		}
		if err != nil {
			return fail(err)
		}
	}
	if needsDirSync {
		if !directoryStable {
			if capture != nil {
				return fail(fmt.Errorf("%w: stable column asset capture did not stabilize namespace", rootpublication.ErrUnresolvedResource))
			}
			start = time.Now()
			if err := syncColumnAssetSegmentDirForPublish(namespace.SegmentDir); err != nil {
				closeStats.DirSync += time.Since(start)
				return fail(err)
			}
			closeStats.DirSync += time.Since(start)
		}
	}
	start = time.Now()
	if err := file.Close(); err != nil {
		closeStats.FileClose += time.Since(start)
		return fail(err)
	}
	closeStats.FileClose += time.Since(start)
	closeFile = false
	if needsDirSync {
		if capture != nil {
			// A retained parent may no longer be reachable through assetPath. Do
			// not let handle-relative stabilization certify a pathname cache entry.
			clearColumnAssetSegmentDirSyncKnown(assetPath)
		} else {
			markColumnAssetSegmentDirSynced(assetPath)
		}
	}
	closeStats.SyncEpochCount = 1
	return ref, closeStats, nil
}

func nextColumnAssetSegmentFileID(namespace columnAssetManagerNamespace) (uint32, error) {
	segments, err := listColumnAssetReachabilitySegments(context.Background(), namespace.SegmentDir)
	if err != nil {
		return 0, err
	}
	maxFileID := uint32(0)
	for _, segment := range segments {
		if segment.fileID >= columnAssetDirectViewSegmentFileIDBase {
			continue
		}
		if segment.fileID > maxFileID {
			maxFileID = segment.fileID
		}
	}
	if maxFileID < columnAssetM12ASegmentFileID {
		maxFileID = columnAssetM12ASegmentFileID
	}
	if maxFileID >= columnAssetDirectViewSegmentFileIDBase-1 {
		return 0, errors.New("collections: column asset segment file_id exhausted before direct-view reserved band")
	}
	return maxFileID + 1, nil
}

func newNextColumnPhysicalAssetSegmentAppender(rootDir string, cfg ColumnStoreConfig) (*columnPhysicalAssetSegmentAppender, error) {
	if cfg.AssetManager == nil {
		return nil, errors.New("collections: column physical asset segment allocation requires asset manager")
	}
	if cfg.AssetManager.Kind != ColumnAssetManagerValueLogShaped {
		return nil, fmt.Errorf("collections: unsupported column asset manager %q", cfg.AssetManager.Kind)
	}
	if !cfg.AssetManager.IsolatedNamespace {
		return nil, errors.New("collections: column physical asset segment allocation requires isolated namespace")
	}
	namespace, err := columnAssetManagerNamespaceForRoot(rootDir, cfg.AssetManager.Namespace)
	if err != nil {
		return nil, err
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		return nil, err
	}
	cleanSegmentDir := filepath.Clean(namespace.SegmentDir)
	allocatorIndex := columnAssetSegmentAllocationLockIndex(cleanSegmentDir)
	allocatorLock := &columnAssetSegmentAllocationLocks[allocatorIndex]
	allocatorCache := &columnAssetSegmentAllocationCaches[allocatorIndex]
	allocatorLock.Lock()
	defer allocatorLock.Unlock()
	fileID, err := nextColumnAssetSegmentFileIDCached(namespace, cleanSegmentDir, allocatorCache)
	if err != nil {
		return nil, err
	}
	for {
		appender, err := newColumnPhysicalAssetSegmentAppender(rootDir, cfg, fileID)
		if err == nil {
			advanceColumnAssetSegmentFileIDCache(cleanSegmentDir, allocatorCache, fileID)
			return appender, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		fileID, err = resetColumnAssetSegmentFileIDCache(namespace, cleanSegmentDir, allocatorCache)
		if err != nil {
			return nil, err
		}
	}
}

// newNextColumnPhysicalAssetSegmentAppenderWithStableResources allocates one
// fresh segment with O_EXCL while binding construction and final publication
// authority to the exact parent and child handles. Unlike the append-session
// constructor, this helper never falls back to an existing segment.
func newNextColumnPhysicalAssetSegmentAppenderWithStableResources(rootDir string, cfg ColumnStoreConfig, registry *rootpublication.IdentityPinRegistry) (*columnPhysicalAssetSegmentAppender, error) {
	if registry == nil {
		return nil, errors.New("collections: stable fresh column physical asset allocation requires identity pin registry")
	}
	if !rootpublication.StableNamespaceCreationSupported() {
		return nil, fmt.Errorf("%w: stable fresh column physical asset allocation requires exact child creation authority", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	if cfg.AssetManager == nil {
		return nil, errors.New("collections: column physical asset segment allocation requires asset manager")
	}
	if cfg.AssetManager.Kind != ColumnAssetManagerValueLogShaped {
		return nil, fmt.Errorf("collections: unsupported column asset manager %q", cfg.AssetManager.Kind)
	}
	if !cfg.AssetManager.IsolatedNamespace {
		return nil, errors.New("collections: column physical asset segment allocation requires isolated namespace")
	}
	namespace, err := columnAssetManagerNamespaceForRoot(rootDir, cfg.AssetManager.Namespace)
	if err != nil {
		return nil, err
	}
	stableParent, err := ensureStableColumnAssetManagerNamespace(namespace, registry)
	if err != nil {
		return nil, err
	}
	_ = stableParent.Close()
	cleanSegmentDir := filepath.Clean(namespace.SegmentDir)
	allocatorIndex := columnAssetSegmentAllocationLockIndex(cleanSegmentDir)
	allocatorLock := &columnAssetSegmentAllocationLocks[allocatorIndex]
	allocatorCache := &columnAssetSegmentAllocationCaches[allocatorIndex]
	allocatorLock.Lock()
	defer allocatorLock.Unlock()
	fileID, err := nextColumnAssetSegmentFileIDCached(namespace, cleanSegmentDir, allocatorCache)
	if err != nil {
		return nil, err
	}
	for {
		appender, err := newColumnPhysicalAssetSegmentAppenderWithStableResources(rootDir, cfg, fileID, registry)
		if err == nil {
			advanceColumnAssetSegmentFileIDCache(cleanSegmentDir, allocatorCache, fileID)
			return appender, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		fileID, err = resetColumnAssetSegmentFileIDCache(namespace, cleanSegmentDir, allocatorCache)
		if err != nil {
			return nil, err
		}
	}
}

func nextColumnAssetSegmentFileIDCached(namespace columnAssetManagerNamespace, cleanSegmentDir string, cache *columnAssetSegmentAllocationCache) (uint32, error) {
	if cache != nil && cache.valid && cache.segmentDir == cleanSegmentDir {
		if cache.nextFileID == 0 || cache.nextFileID >= columnAssetDirectViewSegmentFileIDBase {
			cache.nextFileID = 0
			return 0, errors.New("collections: column asset segment file_id exhausted before direct-view reserved band")
		}
		return cache.nextFileID, nil
	}
	return resetColumnAssetSegmentFileIDCache(namespace, cleanSegmentDir, cache)
}

func resetColumnAssetSegmentFileIDCache(namespace columnAssetManagerNamespace, cleanSegmentDir string, cache *columnAssetSegmentAllocationCache) (uint32, error) {
	fileID, err := nextColumnAssetSegmentFileID(namespace)
	if err != nil {
		if cache != nil && cache.segmentDir == cleanSegmentDir {
			cache.valid = false
			cache.nextFileID = 0
		}
		return 0, err
	}
	if cache != nil {
		cache.segmentDir = cleanSegmentDir
		cache.nextFileID = fileID
		cache.valid = true
	}
	return fileID, nil
}

func advanceColumnAssetSegmentFileIDCache(cleanSegmentDir string, cache *columnAssetSegmentAllocationCache, allocatedFileID uint32) {
	if cache == nil {
		return
	}
	cache.segmentDir = cleanSegmentDir
	cache.valid = true
	if allocatedFileID >= columnAssetDirectViewSegmentFileIDBase-1 {
		cache.nextFileID = 0
		return
	}
	cache.nextFileID = allocatedFileID + 1
}

type columnPhysicalAssetSegmentAppender struct {
	cfg                        ColumnStoreConfig
	namespace                  columnAssetManagerNamespace
	fileID                     uint32
	assetPath                  string
	file                       *os.File
	offset                     int64
	appendStart                int64
	failed                     bool
	lock                       *sync.Mutex
	syncDirOnClose             bool
	closeFile                  bool
	unlockLock                 bool
	created                    bool
	stableParent               *os.File
	stableRegistry             *rootpublication.IdentityPinRegistry
	stableChildName            string
	stableNamespaceNeedsSync   bool
	stableNamespaceProofAdded  bool
	stableParentIdentity       rootpublication.StableIdentity
	stableChildIdentity        rootpublication.StableIdentity
	stableConstructionPin      *rootpublication.IdentityPin
	stableConstructionObserved bool
	stableRefs                 []ColumnAssetRef
	stableNamespaceRef         *ColumnAssetRef
	stableResources            *rootpublication.StableResourceSet
	stableVectorGraphAuthority bool
	stableRollbackHookRan      bool
	stableRollbackTruncated    bool
	stableRollbackContentSync  bool
	stableRollbackUnlinked     bool
	stableRollbackParentSync   bool
	stableRecoveryRetained     bool
	stableRecoveryRetainer     StableResourceCaptureRecoveryRetainer
	closeStats                 columnPhysicalAssetSegmentCloseStats
}

var (
	stableColumnAssetConstructionHookMu sync.RWMutex
	stableColumnAssetConstructionHook   func(uint32)
	stableColumnAssetBeforeObserveHook  func(*os.File, *os.File, string)
	stableColumnAssetObligationHook     func(ColumnAssetRef, rootpublication.StableLogicalObligation, *rootpublication.StableNamespaceToken) columnAssetStableCaptureTestDecision
	stableColumnAssetBeforeRollbackHook func(*os.File, *os.File, string)

	truncateStableColumnAssetForRollback = func(file *os.File, size int64) error { return file.Truncate(size) }
	syncStableColumnAssetForRollback     = func(file *os.File) error { return file.Sync() }
	removeStableColumnAssetForRollback   = rootpublication.RemoveStableChildFile
	syncStableColumnAssetParentRollback  = func(parent *os.File) error { return parent.Sync() }
)

type columnAssetStableCaptureTestDecision uint8

const (
	columnAssetStableCaptureKeep columnAssetStableCaptureTestDecision = iota
	columnAssetStableCaptureOmitObligation
	columnAssetStableCaptureOmitToken
)

func columnAssetStableConstructionHook() func(uint32) {
	stableColumnAssetConstructionHookMu.RLock()
	defer stableColumnAssetConstructionHookMu.RUnlock()
	return stableColumnAssetConstructionHook
}

func columnAssetStableBeforeObserveHook() func(*os.File, *os.File, string) {
	stableColumnAssetConstructionHookMu.RLock()
	defer stableColumnAssetConstructionHookMu.RUnlock()
	return stableColumnAssetBeforeObserveHook
}

// columnAssetStableObligationHook is a fault-injection seam at the real token
// capture boundary. It can omit the producer-created logical obligation or the
// exact token/ref while retaining ordinary construction and cleanup behavior,
// so closure validation must fail before publication can make the ref visible.
func columnAssetStableObligationHook() func(ColumnAssetRef, rootpublication.StableLogicalObligation, *rootpublication.StableNamespaceToken) columnAssetStableCaptureTestDecision {
	stableColumnAssetConstructionHookMu.RLock()
	defer stableColumnAssetConstructionHookMu.RUnlock()
	return stableColumnAssetObligationHook
}

func columnAssetStableBeforeRollbackTestHook() func(*os.File, *os.File, string) {
	stableColumnAssetConstructionHookMu.RLock()
	defer stableColumnAssetConstructionHookMu.RUnlock()
	return stableColumnAssetBeforeRollbackHook
}

func setColumnAssetStableConstructionTestHook(hook func(uint32)) func() {
	stableColumnAssetConstructionHookMu.Lock()
	previous := stableColumnAssetConstructionHook
	stableColumnAssetConstructionHook = hook
	stableColumnAssetConstructionHookMu.Unlock()
	return func() {
		stableColumnAssetConstructionHookMu.Lock()
		stableColumnAssetConstructionHook = previous
		stableColumnAssetConstructionHookMu.Unlock()
	}
}

func setColumnAssetStableBeforeObserveTestHook(hook func(*os.File, *os.File, string)) func() {
	stableColumnAssetConstructionHookMu.Lock()
	previous := stableColumnAssetBeforeObserveHook
	stableColumnAssetBeforeObserveHook = hook
	stableColumnAssetConstructionHookMu.Unlock()
	return func() {
		stableColumnAssetConstructionHookMu.Lock()
		stableColumnAssetBeforeObserveHook = previous
		stableColumnAssetConstructionHookMu.Unlock()
	}
}

func setColumnAssetStableObligationTestHook(hook func(ColumnAssetRef, rootpublication.StableLogicalObligation, *rootpublication.StableNamespaceToken) columnAssetStableCaptureTestDecision) func() {
	stableColumnAssetConstructionHookMu.Lock()
	previous := stableColumnAssetObligationHook
	stableColumnAssetObligationHook = hook
	stableColumnAssetConstructionHookMu.Unlock()
	return func() {
		stableColumnAssetConstructionHookMu.Lock()
		stableColumnAssetObligationHook = previous
		stableColumnAssetConstructionHookMu.Unlock()
	}
}

func setColumnAssetStableBeforeRollbackTestHook(hook func(*os.File, *os.File, string)) func() {
	stableColumnAssetConstructionHookMu.Lock()
	previous := stableColumnAssetBeforeRollbackHook
	stableColumnAssetBeforeRollbackHook = hook
	stableColumnAssetConstructionHookMu.Unlock()
	return func() {
		stableColumnAssetConstructionHookMu.Lock()
		stableColumnAssetBeforeRollbackHook = previous
		stableColumnAssetConstructionHookMu.Unlock()
	}
}

type columnPhysicalAssetAppendItem struct {
	payload    []byte
	kind       ColumnAssetKind
	generation uint64
	partID     uint64
}

type columnPhysicalAssetSegmentCloseStats struct {
	FileSync       time.Duration
	FileClose      time.Duration
	DirSync        time.Duration
	Remove         time.Duration
	RemoveDirSync  time.Duration
	CloseCount     int
	FileSyncCount  int
	SyncEpochCount int

	Total             columnPhysicalAssetSegmentCloseStatBucket
	SharedSegment     columnPhysicalAssetSegmentCloseStatBucket
	DirectViewSegment columnPhysicalAssetSegmentCloseStatBucket
}

type columnPhysicalAssetSegmentCloseStatBucket struct {
	FileSync       time.Duration
	FileClose      time.Duration
	DirSync        time.Duration
	Remove         time.Duration
	RemoveDirSync  time.Duration
	CloseCount     int
	FileSyncCount  int
	SyncEpochCount int
}

func (s *columnPhysicalAssetSegmentCloseStats) Add(other columnPhysicalAssetSegmentCloseStats) {
	if s == nil {
		return
	}
	s.FileSync += other.FileSync
	s.FileClose += other.FileClose
	s.DirSync += other.DirSync
	s.Remove += other.Remove
	s.RemoveDirSync += other.RemoveDirSync
	s.CloseCount += other.CloseCount
	s.FileSyncCount += other.FileSyncCount
	s.SyncEpochCount += other.SyncEpochCount
}

func (s columnPhysicalAssetSegmentCloseStats) CleanupDuration() time.Duration {
	return s.Remove + s.RemoveDirSync
}

func (s *columnPhysicalAssetSegmentCloseStats) AddSegment(fileID uint32, other columnPhysicalAssetSegmentCloseStats) {
	if s == nil {
		return
	}
	s.Add(other)
	s.Total.Add(other)
	if columnAssetSegmentFileIDIsDirectView(fileID) {
		s.DirectViewSegment.Add(other)
		return
	}
	s.SharedSegment.Add(other)
}

func (s *columnPhysicalAssetSegmentCloseStatBucket) Add(other columnPhysicalAssetSegmentCloseStats) {
	if s == nil {
		return
	}
	s.FileSync += other.FileSync
	s.FileClose += other.FileClose
	s.DirSync += other.DirSync
	s.Remove += other.Remove
	s.RemoveDirSync += other.RemoveDirSync
	s.CloseCount += other.CloseCount
	s.FileSyncCount += other.FileSyncCount
	s.SyncEpochCount += other.SyncEpochCount
}

func (s columnPhysicalAssetSegmentCloseStatBucket) CleanupDuration() time.Duration {
	return s.Remove + s.RemoveDirSync
}

type columnPhysicalAssetAppendSession struct {
	rootDir                string
	cfg                    ColumnStoreConfig
	active                 *columnPhysicalAssetSegmentAppender
	activeFile             uint32
	closeStats             columnPhysicalAssetSegmentCloseStats
	closeErr               error
	stableRegistry         *rootpublication.IdentityPinRegistry
	stableBuilder          *rootpublication.StableResourceSetBuilder
	stableRecoveryRetainer StableResourceCaptureRecoveryRetainer
}

func newColumnPhysicalAssetAppendSession(rootDir string, cfg ColumnStoreConfig) *columnPhysicalAssetAppendSession {
	return &columnPhysicalAssetAppendSession{
		rootDir: rootDir,
		cfg:     cfg,
	}
}

func newColumnPhysicalAssetAppendSessionWithStableResources(rootDir string, cfg ColumnStoreConfig, registry *rootpublication.IdentityPinRegistry, recoveryRetainers ...StableResourceCaptureRecoveryRetainer) *columnPhysicalAssetAppendSession {
	var recoveryRetainer StableResourceCaptureRecoveryRetainer
	if len(recoveryRetainers) != 0 {
		recoveryRetainer = recoveryRetainers[0]
	}
	return &columnPhysicalAssetAppendSession{
		rootDir:                rootDir,
		cfg:                    cfg,
		stableRegistry:         registry,
		stableBuilder:          rootpublication.NewStableResourceSetBuilder(),
		stableRecoveryRetainer: recoveryRetainer,
	}
}

func (s *columnPhysicalAssetAppendSession) appender(fileID uint32) (*columnPhysicalAssetSegmentAppender, error) {
	if s == nil {
		return nil, errors.New("collections: nil column physical asset append session")
	}
	if fileID == 0 {
		return nil, errors.New("collections: column physical asset append session requires file_id")
	}
	if s.active != nil && s.activeFile == fileID {
		return s.active, nil
	}
	if err := s.closeActive(); err != nil {
		return nil, err
	}
	var appender *columnPhysicalAssetSegmentAppender
	var err error
	if s.stableRegistry != nil {
		appender, err = newColumnPhysicalAssetSegmentAppendWriterWithStableResources(s.rootDir, s.cfg, fileID, s.stableRegistry)
	} else {
		appender, err = newColumnPhysicalAssetSegmentAppendWriter(s.rootDir, s.cfg, fileID)
	}
	if err != nil {
		return nil, err
	}
	appender.stableRecoveryRetainer = s.stableRecoveryRetainer
	s.active = appender
	s.activeFile = fileID
	return appender, nil
}

func (s *columnPhysicalAssetAppendSession) appendKinds(fileID uint32, items []columnPhysicalAssetAppendItem) ([]ColumnAssetRef, error) {
	refs, _, _, err := s.appendKindsMeasured(fileID, items)
	return refs, err
}

func (s *columnPhysicalAssetAppendSession) appendKindsMeasured(fileID uint32, items []columnPhysicalAssetAppendItem) ([]ColumnAssetRef, time.Duration, time.Duration, error) {
	if len(items) == 0 {
		return nil, 0, 0, nil
	}
	openStart := time.Now()
	appender, err := s.appender(fileID)
	openDuration := time.Since(openStart)
	if err != nil {
		return nil, openDuration, 0, err
	}
	writeStart := time.Now()
	refs, err := appender.appendKinds(items)
	return refs, openDuration, time.Since(writeStart), err
}

func (s *columnPhysicalAssetAppendSession) close() (columnPhysicalAssetSegmentCloseStats, error) {
	if s == nil {
		return columnPhysicalAssetSegmentCloseStats{}, nil
	}
	_ = s.closeActive()
	return s.closeStats, s.closeErr
}

func (s *columnPhysicalAssetAppendSession) closeWithStableResources() (columnPhysicalAssetSegmentCloseStats, *rootpublication.StableResourceSet, error) {
	return s.closeWithStableResourcesValidated(nil)
}

func (s *columnPhysicalAssetAppendSession) closeWithStableResourcesValidated(validate func(*rootpublication.StableResourceSet) error) (columnPhysicalAssetSegmentCloseStats, *rootpublication.StableResourceSet, error) {
	if s == nil {
		return columnPhysicalAssetSegmentCloseStats{}, nil, nil
	}
	_ = s.closeActiveWithStableValidation(validate)
	if s.closeErr != nil {
		if s.stableBuilder != nil {
			s.stableBuilder.Abandon()
		}
		return s.closeStats, nil, s.closeErr
	}
	if s.stableBuilder == nil {
		return s.closeStats, nil, errors.New("collections: column physical asset append session has no stable resource builder")
	}
	resources, err := s.stableBuilder.Freeze()
	if err != nil {
		s.stableBuilder.Abandon()
		return s.closeStats, nil, err
	}
	s.stableBuilder = nil
	return s.closeStats, resources, nil
}

func (s *columnPhysicalAssetAppendSession) abort() error {
	if s == nil {
		return nil
	}
	if s.stableBuilder != nil {
		s.stableBuilder.Abandon()
		s.stableBuilder = nil
	}
	if s.active == nil {
		return nil
	}
	appender := s.active
	s.active = nil
	s.activeFile = 0
	return appender.abort()
}

func (s *columnPhysicalAssetAppendSession) closeActive() error {
	return s.closeActiveWithStableValidation(nil)
}

func (s *columnPhysicalAssetAppendSession) closeActiveWithStableValidation(validate func(*rootpublication.StableResourceSet) error) error {
	if s == nil || s.active == nil {
		return nil
	}
	appender := s.active
	activeFile := s.activeFile
	s.active = nil
	s.activeFile = 0
	err := appender.closeWithStableValidation(validate)
	s.closeStats.AddSegment(activeFile, appender.closeStats)
	if err == nil && appender.stableResources != nil {
		if s.stableBuilder == nil {
			err = errors.New("collections: stable column segment closed without session resource builder")
			appender.stableResources.Release()
		} else if mergeErr := s.stableBuilder.Merge(appender.stableResources); mergeErr != nil {
			err = mergeErr
			appender.stableResources.Release()
		}
		appender.stableResources = nil
	}
	s.closeErr = errors.Join(s.closeErr, err)
	return err
}

func newColumnPhysicalAssetSegmentAppender(rootDir string, cfg ColumnStoreConfig, fileID uint32) (*columnPhysicalAssetSegmentAppender, error) {
	if cfg.AssetManager == nil {
		return nil, errors.New("collections: column physical asset append requires asset manager")
	}
	if cfg.AssetManager.Kind != ColumnAssetManagerValueLogShaped {
		return nil, fmt.Errorf("collections: unsupported column asset manager %q", cfg.AssetManager.Kind)
	}
	if !cfg.AssetManager.IsolatedNamespace {
		return nil, errors.New("collections: column physical asset append requires isolated namespace")
	}
	if fileID == 0 {
		return nil, errors.New("collections: column physical asset append requires file_id")
	}
	namespace, err := columnAssetManagerNamespaceForRoot(rootDir, cfg.AssetManager.Namespace)
	if err != nil {
		return nil, err
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		return nil, err
	}
	ref := ColumnAssetRef{
		Kind:      ColumnAssetKindTCS1PartImage,
		Namespace: cfg.AssetManager.Namespace,
		FileID:    fileID,
		Offset:    0,
		Length:    1,
	}
	assetPath, err := columnAssetSegmentPath(rootDir, ref)
	if err != nil {
		return nil, err
	}
	segmentLock := columnAssetSegmentWriteLock(assetPath)
	segmentLock.Lock()
	appender := &columnPhysicalAssetSegmentAppender{
		cfg:            cfg,
		namespace:      namespace,
		fileID:         fileID,
		assetPath:      assetPath,
		lock:           segmentLock,
		syncDirOnClose: true,
		unlockLock:     true,
		created:        true,
	}
	file, err := os.OpenFile(assetPath, os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		appender.releaseLock()
		return nil, err
	}
	appender.file = file
	appender.closeFile = true
	clearColumnAssetSegmentDirSyncKnown(assetPath)
	return appender, nil
}

func newColumnPhysicalAssetSegmentAppendWriter(rootDir string, cfg ColumnStoreConfig, fileID uint32) (*columnPhysicalAssetSegmentAppender, error) {
	if cfg.AssetManager == nil {
		return nil, errors.New("collections: column physical asset append requires asset manager")
	}
	if cfg.AssetManager.Kind != ColumnAssetManagerValueLogShaped {
		return nil, fmt.Errorf("collections: unsupported column asset manager %q", cfg.AssetManager.Kind)
	}
	if !cfg.AssetManager.IsolatedNamespace {
		return nil, errors.New("collections: column physical asset append requires isolated namespace")
	}
	if fileID == 0 {
		return nil, errors.New("collections: column physical asset append requires file_id")
	}
	namespace, err := columnAssetManagerNamespaceForRoot(rootDir, cfg.AssetManager.Namespace)
	if err != nil {
		return nil, err
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		return nil, err
	}
	ref := ColumnAssetRef{
		Kind:      ColumnAssetKindTCS1PartImage,
		Namespace: cfg.AssetManager.Namespace,
		FileID:    fileID,
		Offset:    0,
		Length:    1,
	}
	assetPath, err := columnAssetSegmentPath(rootDir, ref)
	if err != nil {
		return nil, err
	}
	segmentLock := columnAssetSegmentWriteLock(assetPath)
	segmentLock.Lock()
	appender := &columnPhysicalAssetSegmentAppender{
		cfg:        cfg,
		namespace:  namespace,
		fileID:     fileID,
		assetPath:  assetPath,
		lock:       segmentLock,
		unlockLock: true,
	}
	file, needsDirSync, created, err := openColumnAssetSegmentAppendFile(assetPath)
	if err != nil {
		appender.releaseLock()
		return nil, err
	}
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		_ = file.Close()
		appender.releaseLock()
		return nil, err
	}
	appender.file = file
	appender.offset = offset
	appender.syncDirOnClose = needsDirSync
	appender.closeFile = true
	appender.created = created
	if created {
		clearColumnAssetSegmentDirSyncKnown(assetPath)
	}
	return appender, nil
}

func newColumnPhysicalAssetSegmentAppendWriterWithStableResources(rootDir string, cfg ColumnStoreConfig, fileID uint32, registry *rootpublication.IdentityPinRegistry) (*columnPhysicalAssetSegmentAppender, error) {
	if registry == nil {
		return nil, errors.New("collections: stable column physical asset append requires identity pin registry")
	}
	if cfg.AssetManager == nil {
		return nil, errors.New("collections: column physical asset append requires asset manager")
	}
	if cfg.AssetManager.Kind != ColumnAssetManagerValueLogShaped {
		return nil, fmt.Errorf("collections: unsupported column asset manager %q", cfg.AssetManager.Kind)
	}
	if !cfg.AssetManager.IsolatedNamespace {
		return nil, errors.New("collections: column physical asset append requires isolated namespace")
	}
	if fileID == 0 {
		return nil, errors.New("collections: column physical asset append requires file_id")
	}
	namespace, err := columnAssetManagerNamespaceForRoot(rootDir, cfg.AssetManager.Namespace)
	if err != nil {
		return nil, err
	}
	parent, err := ensureStableColumnAssetManagerNamespace(namespace, registry)
	if err != nil {
		return nil, err
	}
	ref := ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: cfg.AssetManager.Namespace, FileID: fileID, Length: 1}
	assetPath, err := columnAssetSegmentPath(rootDir, ref)
	if err != nil {
		_ = parent.Close()
		return nil, err
	}
	segmentLock := columnAssetSegmentWriteLock(assetPath)
	segmentLock.Lock()
	appender := &columnPhysicalAssetSegmentAppender{
		cfg: cfg, namespace: namespace, fileID: fileID, assetPath: assetPath,
		lock: segmentLock, unlockLock: true, stableRegistry: registry,
	}
	file, namespaceNeedsSync, created, err := openColumnAssetSegmentAppendFileAt(parent, assetPath)
	if err != nil {
		_ = parent.Close()
		appender.releaseLock()
		return nil, err
	}
	return initializeColumnPhysicalAssetSegmentAppenderStableOpen(appender, parent, file, namespaceNeedsSync, created)
}

func newColumnPhysicalAssetSegmentAppenderWithStableResources(rootDir string, cfg ColumnStoreConfig, fileID uint32, registry *rootpublication.IdentityPinRegistry) (*columnPhysicalAssetSegmentAppender, error) {
	if registry == nil {
		return nil, errors.New("collections: stable fresh column physical asset append requires identity pin registry")
	}
	if !rootpublication.StableNamespaceCreationSupported() {
		return nil, fmt.Errorf("%w: stable fresh column physical asset append requires exact child creation authority", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	if cfg.AssetManager == nil {
		return nil, errors.New("collections: column physical asset append requires asset manager")
	}
	if cfg.AssetManager.Kind != ColumnAssetManagerValueLogShaped {
		return nil, fmt.Errorf("collections: unsupported column asset manager %q", cfg.AssetManager.Kind)
	}
	if !cfg.AssetManager.IsolatedNamespace {
		return nil, errors.New("collections: column physical asset append requires isolated namespace")
	}
	if fileID == 0 {
		return nil, errors.New("collections: column physical asset append requires file_id")
	}
	namespace, err := columnAssetManagerNamespaceForRoot(rootDir, cfg.AssetManager.Namespace)
	if err != nil {
		return nil, err
	}
	parent, err := ensureStableColumnAssetManagerNamespace(namespace, registry)
	if err != nil {
		return nil, err
	}
	ref := ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: cfg.AssetManager.Namespace, FileID: fileID, Length: 1}
	assetPath, err := columnAssetSegmentPath(rootDir, ref)
	if err != nil {
		_ = parent.Close()
		return nil, err
	}
	segmentLock := columnAssetSegmentWriteLock(assetPath)
	segmentLock.Lock()
	appender := &columnPhysicalAssetSegmentAppender{
		cfg: cfg, namespace: namespace, fileID: fileID, assetPath: assetPath,
		lock: segmentLock, unlockLock: true, stableRegistry: registry,
	}
	name := filepath.Base(assetPath)
	file, err := rootpublication.OpenStableChildFile(parent, name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		_ = parent.Close()
		appender.releaseLock()
		return nil, err
	}
	return initializeColumnPhysicalAssetSegmentAppenderStableOpen(appender, parent, file, true, true)
}

func initializeColumnPhysicalAssetSegmentAppenderStableOpen(appender *columnPhysicalAssetSegmentAppender, parent, file *os.File, namespaceNeedsSync, created bool) (*columnPhysicalAssetSegmentAppender, error) {
	if appender == nil || appender.stableRegistry == nil || parent == nil || file == nil {
		if file != nil {
			_ = file.Close()
		}
		if parent != nil {
			_ = parent.Close()
		}
		if appender != nil {
			appender.releaseLock()
		}
		return nil, errors.New("collections: incomplete stable column physical asset construction")
	}
	appender.file = file
	appender.syncDirOnClose = namespaceNeedsSync
	appender.closeFile = true
	appender.created = created
	appender.stableParent = parent
	appender.stableChildName = filepath.Base(appender.assetPath)
	if created {
		if err := durabilitycut.EmitNamespace(durabilitycut.NamespaceCreate, durabilitycut.ResourceAuxiliary, appender.namespace.SegmentDir, "", appender.assetPath); err != nil {
			return nil, errors.Join(err, appender.abort())
		}
	}
	var err error
	appender.stableParentIdentity, err = rootpublication.StableIdentityFromFile(parent)
	if err != nil {
		return nil, errors.Join(err, appender.abort())
	}
	appender.stableChildIdentity, err = rootpublication.StableIdentityFromFile(file)
	if err != nil {
		return nil, errors.Join(err, appender.abort())
	}
	if hook := columnAssetStableBeforeObserveHook(); hook != nil {
		hook(parent, file, appender.stableChildName)
	}
	if err := appender.stableRegistry.Observe(appender.stableChildIdentity); err != nil {
		return nil, errors.Join(err, appender.abort())
	}
	appender.stableConstructionObserved = true
	appender.stableConstructionPin, err = appender.stableRegistry.Pin(appender.stableChildIdentity)
	if err != nil {
		return nil, errors.Join(err, appender.abort())
	}
	if err := rootpublication.ValidateStableChildLink(parent, file, appender.stableChildName); err != nil {
		return nil, errors.Join(err, appender.abort())
	}
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, errors.Join(err, appender.abort())
	}
	appender.offset = offset
	appender.appendStart = offset
	// A cold/unknown exact binding remains conservative. Once this DB-scoped
	// registry has proof for the same parent identity, child identity, and name,
	// ordinary appends can skip the structural namespace sync.
	if !created && namespaceNeedsSync {
		known, knownErr := appender.stableRegistry.StableNamespaceLinkKnown(parent, file, appender.stableChildName)
		if knownErr != nil {
			return nil, errors.Join(knownErr, appender.abort())
		}
		namespaceNeedsSync = !known
	}
	appender.stableNamespaceNeedsSync = namespaceNeedsSync
	if created {
		clearColumnAssetSegmentDirSyncKnown(appender.assetPath)
	}
	if hook := columnAssetStableConstructionHook(); hook != nil {
		hook(appender.fileID)
	}
	return appender, nil
}

func openColumnAssetSegmentAppendFile(assetPath string) (*os.File, bool, bool, error) {
	file, err := os.OpenFile(assetPath, os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0o600)
	if err == nil {
		return file, true, true, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return nil, false, false, err
	}
	file, err = os.OpenFile(assetPath, os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, false, false, err
	}
	return file, !columnAssetSegmentDirSyncKnown(assetPath), false, nil
}

func openColumnAssetSegmentAppendFileAt(parent *os.File, assetPath string) (*os.File, bool, bool, error) {
	name := filepath.Base(assetPath)
	file, err := rootpublication.OpenStableChildFile(parent, name, os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0o600)
	if err == nil {
		return file, true, true, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return nil, false, false, err
	}
	file, err = rootpublication.OpenStableChildFile(parent, name, os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, false, false, err
	}
	// A pathname-only cache cannot prove that the entry opened through this
	// retained parent has a durable namespace.
	return file, true, false, nil
}

func (a *columnPhysicalAssetSegmentAppender) append(payload []byte, generation, partID uint64) (ColumnAssetRef, error) {
	return a.appendKind(payload, ColumnAssetKindTCS1PartImage, generation, partID)
}

func (a *columnPhysicalAssetSegmentAppender) appendKind(payload []byte, kind ColumnAssetKind, generation, partID uint64) (ColumnAssetRef, error) {
	alignment := int64(0)
	if a != nil {
		alignment = columnAssetSegmentPayloadAlignment(kind, a.cfg)
	}
	return a.appendKindWithAlignment(payload, kind, generation, partID, alignment)
}

func (a *columnPhysicalAssetSegmentAppender) appendKindWithAlignment(payload []byte, kind ColumnAssetKind, generation, partID uint64, alignment int64) (ColumnAssetRef, error) {
	if a == nil || a.file == nil {
		return ColumnAssetRef{}, errors.New("collections: nil column physical asset appender")
	}
	if a.failed {
		return ColumnAssetRef{}, errors.New("collections: column physical asset appender is failed")
	}
	if len(payload) == 0 {
		return ColumnAssetRef{}, errors.New("collections: column physical asset payload is empty")
	}
	if generation == 0 || partID == 0 {
		return ColumnAssetRef{}, errors.New("collections: column physical asset append requires generation and part_id")
	}
	if a.stableRegistry != nil {
		if _, _, _, err := stableColumnAssetResourceClassification(kind); err != nil {
			return ColumnAssetRef{}, err
		}
	}
	padding := columnAssetSegmentPrefixPadding(a.offset, alignment)
	if padding > 0 {
		written, err := writeColumnAssetSegmentZeroPadding(a.file, padding)
		a.offset += int64(written)
		if err != nil {
			a.failed = true
			return ColumnAssetRef{}, err
		}
		if written != padding {
			a.failed = true
			return ColumnAssetRef{}, io.ErrShortWrite
		}
	}
	ref := ColumnAssetRef{
		Kind:       kind,
		Namespace:  a.cfg.AssetManager.Namespace,
		Generation: generation,
		PartID:     partID,
		FileID:     a.fileID,
		Offset:     a.offset,
		Length:     int64(len(payload)),
		Checksum:   page.Checksum(payload),
	}
	written, err := writeColumnAssetSegmentPayload(a.file, payload)
	a.offset += int64(written)
	if err != nil {
		a.failed = true
		return ColumnAssetRef{}, err
	}
	a.recordStableRef(ref)
	return ref, nil
}

type columnAssetReservedPayload struct {
	writer *columnAssetChecksumWriter
	file   *os.File
	start  int64
	length int64
}

func (p *columnAssetReservedPayload) Write(b []byte) (int, error) { return p.writer.Write(b) }

func (p *columnAssetReservedPayload) Backpatch(offset int64, b []byte) error {
	if p == nil || offset < 0 || offset > p.length-int64(len(b)) {
		return io.ErrShortWrite
	}
	for len(b) > 0 {
		n, err := p.file.WriteAt(b, p.start+offset)
		if n > 0 {
			b = b[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func (a *columnPhysicalAssetSegmentAppender) appendKindWithReservedPayload(length int64, kind ColumnAssetKind, generation, partID uint64, alignment int64, emit func(*columnAssetReservedPayload) error) (ColumnAssetRef, error) {
	if a == nil || a.file == nil || a.failed {
		return ColumnAssetRef{}, errors.New("collections: column physical asset appender is unavailable")
	}
	if length <= 0 || emit == nil || generation == 0 || partID == 0 {
		return ColumnAssetRef{}, errors.New("collections: column physical asset reserved append is invalid")
	}
	if a.stableRegistry != nil {
		if _, _, _, err := stableColumnAssetResourceClassification(kind); err != nil {
			return ColumnAssetRef{}, err
		}
	}
	if padding := columnAssetSegmentPrefixPadding(a.offset, alignment); padding > 0 {
		n, err := writeColumnAssetSegmentZeroPadding(a.file, padding)
		a.offset += int64(n)
		if err != nil || n != padding {
			a.failed = true
			if err == nil {
				err = io.ErrShortWrite
			}
			return ColumnAssetRef{}, err
		}
	}
	start := a.offset
	reservedFile, err := a.openReservedPayloadFile()
	if err != nil {
		a.failed = true
		return ColumnAssetRef{}, err
	}
	payload := &columnAssetReservedPayload{writer: &columnAssetChecksumWriter{dst: a.file, limit: length}, file: reservedFile, start: start, length: length}
	if err := emit(payload); err != nil || payload.writer.written != length {
		_ = reservedFile.Close()
		a.offset += payload.writer.written
		a.failed = true
		if err == nil {
			err = io.ErrShortWrite
		}
		return ColumnAssetRef{}, err
	}
	checksum, err := checksumColumnAssetSegmentRange(reservedFile, start, length)
	closeReservedErr := reservedFile.Close()
	if err == nil {
		err = closeReservedErr
	}
	if err != nil {
		a.offset += payload.writer.written
		a.failed = true
		return ColumnAssetRef{}, err
	}
	a.offset += payload.writer.written
	ref := ColumnAssetRef{Kind: kind, Namespace: a.cfg.AssetManager.Namespace, Generation: generation, PartID: partID, FileID: a.fileID, Offset: start, Length: length, Checksum: checksum}
	a.recordStableRef(ref)
	return ref, nil
}

func (a *columnPhysicalAssetSegmentAppender) openReservedPayloadFile() (*os.File, error) {
	if a.stableParent != nil && a.stableChildName != "" {
		return rootpublication.OpenStableChildFile(a.stableParent, a.stableChildName, os.O_RDWR, 0o600)
	}
	return os.OpenFile(a.assetPath, os.O_RDWR, 0o600)
}

func checksumColumnAssetSegmentRange(file *os.File, offset, length int64) (uint32, error) {
	if file == nil || offset < 0 || length <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	buf := make([]byte, 64<<10)
	var sum uint32
	for read := int64(0); read < length; {
		n := int64(len(buf))
		if n > length-read {
			n = length - read
		}
		got, err := file.ReadAt(buf[:n], offset+read)
		if got > 0 {
			sum = crc.Update(sum, buf[:got])
			read += int64(got)
		}
		if err != nil && !(err == io.EOF && read == length) {
			return 0, err
		}
		if got == 0 {
			return 0, io.ErrUnexpectedEOF
		}
	}
	return sum, nil
}

type columnAssetChecksumWriter struct {
	dst      io.Writer
	limit    int64
	written  int64
	checksum uint32
}

func (w *columnAssetChecksumWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.limit-w.written {
		return 0, io.ErrShortWrite
	}
	n, err := writeColumnAssetSegmentPayload(w.dst, p)
	if n > 0 {
		w.written += int64(n)
		w.checksum = crc.Update(w.checksum, p[:n])
	}
	return n, err
}

func (a *columnPhysicalAssetSegmentAppender) appendKinds(items []columnPhysicalAssetAppendItem) ([]ColumnAssetRef, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if a == nil || a.file == nil {
		return nil, errors.New("collections: nil column physical asset appender")
	}
	if a.failed {
		return nil, errors.New("collections: column physical asset appender is failed")
	}
	const maxInt64 = int64(1<<63 - 1)
	maxInt := int(^uint(0) >> 1)
	refs := make([]ColumnAssetRef, len(items))
	totalLength := 0
	nextOffset := a.offset
	for i, item := range items {
		if len(item.payload) == 0 {
			return nil, errors.New("collections: column physical asset payload is empty")
		}
		if item.generation == 0 || item.partID == 0 {
			return nil, errors.New("collections: column physical asset append requires generation and part_id")
		}
		if a.stableRegistry != nil {
			if _, _, _, err := stableColumnAssetResourceClassification(item.kind); err != nil {
				return nil, err
			}
		}
		padding := columnAssetSegmentPrefixPadding(nextOffset, columnAssetSegmentPayloadAlignment(item.kind, a.cfg))
		if totalLength > maxInt-padding-len(item.payload) {
			return nil, errors.New("collections: column physical asset append batch is too large")
		}
		if nextOffset > maxInt64-int64(padding)-int64(len(item.payload)) {
			return nil, errors.New("collections: column physical asset append offset overflow")
		}
		nextOffset += int64(padding)
		refs[i] = ColumnAssetRef{
			Kind:       item.kind,
			Namespace:  a.cfg.AssetManager.Namespace,
			Generation: item.generation,
			PartID:     item.partID,
			FileID:     a.fileID,
			Offset:     nextOffset,
			Length:     int64(len(item.payload)),
			Checksum:   page.Checksum(item.payload),
		}
		nextOffset += int64(len(item.payload))
		totalLength += padding + len(item.payload)
	}
	payload := make([]byte, 0, totalLength)
	cursor := a.offset
	var zeros [typedColumnPartDirectViewAssetAlignment]byte
	for i, item := range items {
		padding := int(refs[i].Offset - cursor)
		for padding > 0 {
			chunk := padding
			if chunk > len(zeros) {
				chunk = len(zeros)
			}
			payload = append(payload, zeros[:chunk]...)
			padding -= chunk
		}
		payload = append(payload, item.payload...)
		cursor = refs[i].Offset + refs[i].Length
	}
	written, err := writeColumnAssetSegmentPayload(a.file, payload)
	a.offset += int64(written)
	if err != nil {
		a.failed = true
		return nil, err
	}
	if written != len(payload) {
		a.failed = true
		return nil, io.ErrShortWrite
	}
	a.offset = nextOffset
	for _, ref := range refs {
		a.recordStableRef(ref)
	}
	return refs, nil
}

func (a *columnPhysicalAssetSegmentAppender) recordStableRef(ref ColumnAssetRef) {
	if a == nil || a.stableRegistry == nil {
		return
	}
	if a.stableNamespaceRef == nil {
		captured := ref
		a.stableNamespaceRef = &captured
	}
	_, _, classification, err := stableColumnAssetResourceClassification(ref.Kind)
	if err != nil || classification == "rebuildable-non-authoritative" {
		return
	}
	a.stableRefs = append(a.stableRefs, ref)
}

func columnAssetSegmentPayloadAlignment(kind ColumnAssetKind, cfg ColumnStoreConfig) int64 {
	switch kind {
	case ColumnAssetKindTCS1DictionaryCodes:
		return dictionaryCodesDirectViewAssetAlignment
	case ColumnAssetKindTCS1Int64Values:
		return int64ValuesDirectViewAssetAlignment
	case ColumnAssetKindTCS1TypedColumnPart:
		if columnStoreConfigNeedsDirectViewTypedColumnAlignment(cfg) {
			return typedColumnPartDirectViewAssetAlignment
		}
	case ColumnAssetKindTCS1HNSWSearchPack:
		return int64(columnHNSWSearchPackVectorSectionAlignment)
	}
	return 0
}

func columnAssetSegmentPrefixPadding(offset int64, alignment int64) int {
	if alignment <= 1 {
		return 0
	}
	rem := offset % alignment
	if rem == 0 {
		return 0
	}
	return int(alignment - rem)
}

func writeColumnAssetSegmentZeroPadding(w io.Writer, length int) (int, error) {
	if length <= 0 {
		return 0, nil
	}
	var zeros [typedColumnPartDirectViewAssetAlignment]byte
	written := 0
	for written < length {
		chunk := length - written
		if chunk > len(zeros) {
			chunk = len(zeros)
		}
		n, err := w.Write(zeros[:chunk])
		if n > 0 {
			written += n
		}
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func writeColumnAssetSegmentPayload(w io.Writer, payload []byte) (int, error) {
	written := 0
	for written < len(payload) {
		n, err := w.Write(payload[written:])
		if n > 0 {
			written += n
		}
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func (a *columnPhysicalAssetSegmentAppender) captureStableResources() (*rootpublication.StableResourceSet, error) {
	if a == nil || a.stableRegistry == nil {
		return nil, nil
	}
	if a.file == nil || a.stableParent == nil {
		return nil, errors.New("collections: stable column segment capture requires exact file and parent handles")
	}
	var namespaceToken *rootpublication.StableNamespaceToken
	if a.stableNamespaceRef == nil && len(a.stableRefs) != 0 {
		return nil, errors.New("collections: stable column segment has no namespace ref")
	}
	if a.stableNamespaceNeedsSync {
		if a.stableNamespaceRef == nil {
			return nil, errors.New("collections: created stable column segment has no appended ref")
		}
		var err error
		namespaceToken, err = stableColumnAssetNamespaceToken(a.stableParent, a.file, *a.stableNamespaceRef)
		if err != nil {
			return nil, err
		}
		if err := stabilizeColumnAssetNamespaceForPublish(namespaceToken, a.namespace); err != nil {
			namespaceToken.Release()
			return nil, err
		}
		a.stableParentIdentity, err = rootpublication.StableIdentityFromFile(a.stableParent)
		if err != nil {
			namespaceToken.Release()
			return nil, err
		}
		a.stableChildIdentity, err = rootpublication.StableIdentityFromFile(a.file)
		if err != nil {
			namespaceToken.Release()
			return nil, err
		}
		if err := a.stableRegistry.RememberStableNamespaceLink(a.stableParent, a.file, a.stableChildName); err != nil {
			namespaceToken.Release()
			return nil, err
		}
		a.stableNamespaceProofAdded = true
	} else if a.stableNamespaceRef != nil {
		var err error
		namespaceToken, err = stableColumnAssetKnownNamespaceToken(a.stableRegistry, a.stableParent, a.file, *a.stableNamespaceRef)
		if err != nil {
			return nil, err
		}
	}
	if namespaceToken != nil {
		defer namespaceToken.Release()
	}
	refs := append([]ColumnAssetRef(nil), a.stableRefs...)
	sort.SliceStable(refs, func(i, j int) bool {
		if a.stableVectorGraphAuthority {
			leftEnd := refs[i].Offset + refs[i].Length
			rightEnd := refs[j].Offset + refs[j].Length
			return leftEnd > rightEnd
		}
		leftKind, _, _, _ := stableColumnAssetResourceClassification(refs[i].Kind)
		rightKind, _, _, _ := stableColumnAssetResourceClassification(refs[j].Kind)
		if leftKind != rightKind {
			return leftKind < rightKind
		}
		leftEnd := refs[i].Offset + refs[i].Length
		rightEnd := refs[j].Offset + refs[j].Length
		if leftEnd != rightEnd {
			return leftEnd > rightEnd
		}
		return false
	})
	builder := rootpublication.NewStableResourceSetBuilder()
	for _, ref := range refs {
		var token *rootpublication.StableResourceToken
		var err error
		if a.stableVectorGraphAuthority {
			token, err = stableVectorGraphResourceTokenWithRegistry(a.file, ref, namespaceToken, a.stableRegistry)
		} else {
			token, err = stableColumnAssetResourceTokenWithRegistryForPublish(a.file, ref, namespaceToken, a.stableRegistry)
		}
		if errors.Is(err, errColumnAssetStableTokenOmittedForTest) {
			continue
		}
		if err != nil {
			builder.Abandon()
			return nil, err
		}
		if err := builder.Add(token); err != nil {
			token.Release()
			builder.Abandon()
			return nil, err
		}
	}
	resources, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		return nil, err
	}
	// Keep construction authority until the caller has validated this complete
	// resource set. A fault-injected or future partial capture can freeze an
	// empty set, so the final tokens alone cannot protect the pre-visibility
	// rollback interval.
	return resources, nil
}

func (a *columnPhysicalAssetSegmentAppender) releaseStableConstructionAuthority() error {
	if a == nil {
		return nil
	}
	if a.stableConstructionPin != nil {
		a.stableConstructionPin.Release()
		a.stableConstructionPin = nil
	}
	if !a.stableConstructionObserved {
		return nil
	}
	if err := a.stableRegistry.Unobserve(a.stableChildIdentity); err != nil {
		return err
	}
	a.stableConstructionObserved = false
	return nil
}

func (a *columnPhysicalAssetSegmentAppender) close() error {
	return a.closeWithStableValidation(nil)
}

func (a *columnPhysicalAssetSegmentAppender) closeWithStableValidation(validate func(*rootpublication.StableResourceSet) error) error {
	if a == nil {
		return nil
	}
	a.closeStats = columnPhysicalAssetSegmentCloseStats{}
	a.closeStats.CloseCount = 1
	var appenderErr error
	var fileSyncErr error
	var fileCloseErr error
	var stableCaptureErr error
	var stableValidationErr error
	var stableRollbackErr error
	var stableRollbackAttempted bool
	var stableRollbackComplete bool
	var constructionReleaseErr error
	if a.failed {
		appenderErr = errors.New("collections: column physical asset appender is failed")
	}
	if a.file != nil && a.closeFile {
		if !a.failed {
			start := time.Now()
			a.closeStats.FileSyncCount++
			fileSyncErr = syncColumnAssetSegmentFileObserved(a.file, a.namespace.SegmentDir)
			a.closeStats.FileSync += time.Since(start)
		}
		if !a.failed && fileSyncErr == nil && a.stableRegistry != nil {
			start := time.Now()
			a.stableResources, stableCaptureErr = a.captureStableResources()
			if a.stableNamespaceNeedsSync {
				a.closeStats.DirSync += time.Since(start)
			}
		}
		if validate != nil {
			if fileSyncErr == nil && stableCaptureErr == nil {
				stableValidationErr = validate(a.stableResources)
			}
			if fileSyncErr != nil || stableCaptureErr != nil || stableValidationErr != nil {
				stableRollbackAttempted = true
				stableRollbackComplete, stableRollbackErr = a.rollbackStableAppend()
				if !stableRollbackComplete {
					stableRollbackErr = errors.Join(stableRollbackErr, a.retainStableRollbackForRecovery())
				}
			}
		}
		if !a.stableRecoveryRetained {
			start := time.Now()
			fileCloseErr = a.file.Close()
			a.closeStats.FileClose += time.Since(start)
			a.closeFile = false
			a.file = nil
		}
	}
	if !a.stableRecoveryRetained {
		constructionReleaseErr = a.releaseStableConstructionAuthority()
	}
	var dirSyncErr error
	if a.syncDirOnClose && a.stableRegistry == nil && appenderErr == nil && fileSyncErr == nil && fileCloseErr == nil {
		start := time.Now()
		dirSyncErr = syncColumnAssetDir(a.namespace.SegmentDir)
		a.closeStats.DirSync += time.Since(start)
	}
	if a.syncDirOnClose && a.stableRegistry == nil && dirSyncErr == nil && !a.failed && fileSyncErr == nil && fileCloseErr == nil {
		markColumnAssetSegmentDirSynced(a.assetPath)
	}
	if a.closeStats.FileSyncCount > 0 && !a.failed && fileSyncErr == nil && fileCloseErr == nil && dirSyncErr == nil && stableCaptureErr == nil {
		a.closeStats.SyncEpochCount = 1
	}
	if a.created && (appenderErr != nil || fileSyncErr != nil || fileCloseErr != nil || dirSyncErr != nil || stableCaptureErr != nil) {
		clearColumnAssetSegmentDirSyncKnown(a.assetPath)
	}
	var parentCloseErr error
	if a.stableParent != nil && !a.stableRecoveryRetained {
		parentCloseErr = a.stableParent.Close()
		a.stableParent = nil
	}
	closeErr := errors.Join(appenderErr, fileSyncErr, stableCaptureErr, stableValidationErr, stableRollbackErr, fileCloseErr, dirSyncErr, constructionReleaseErr, parentCloseErr)
	if closeErr != nil && a.stableResources != nil {
		a.stableResources.Release()
		a.stableResources = nil
	}
	if closeErr != nil && !stableRollbackAttempted && a.stableNamespaceProofAdded {
		closeErr = errors.Join(closeErr, a.stableRegistry.ForgetStableNamespaceLinkIdentity(
			a.stableParentIdentity, a.stableChildIdentity, a.stableChildName,
		))
		a.stableNamespaceProofAdded = false
	}
	if !a.stableRecoveryRetained {
		a.releaseLock()
	}
	return closeErr
}

func (a *columnPhysicalAssetSegmentAppender) rollbackStableAppend() (bool, error) {
	if a == nil || a.file == nil || a.stableParent == nil || a.stableRegistry == nil || a.stableChildName == "" {
		return false, errors.Join(
			fmt.Errorf("%w: stable column rollback lacks exact child or parent authority", rootpublication.ErrResourceOwnership),
			ErrRecoveryRequired,
		)
	}
	if a.appendStart < 0 || a.offset < a.appendStart {
		return false, errors.Join(
			fmt.Errorf("%w: stable column rollback frontier start=%d end=%d", rootpublication.ErrUnresolvedResource, a.appendStart, a.offset),
			ErrRecoveryRequired,
		)
	}
	if !a.stableRollbackHookRan {
		a.stableRollbackHookRan = true
		if hook := columnAssetStableBeforeRollbackTestHook(); hook != nil {
			hook(a.stableParent, a.file, a.stableChildName)
		}
	}
	info, err := a.file.Stat()
	if err != nil {
		return false, errors.Join(err, ErrRecoveryRequired)
	}
	expectedSize := a.offset
	if a.stableRollbackTruncated {
		expectedSize = a.appendStart
	}
	if info.Size() != expectedSize {
		return false, errors.Join(
			fmt.Errorf("%w: stable column rollback frontier changed from %d to %d", rootpublication.ErrResourceConflict, expectedSize, info.Size()),
			ErrRecoveryRequired,
		)
	}
	if !a.stableRollbackTruncated {
		if err := truncateStableColumnAssetForRollback(a.file, a.appendStart); err != nil {
			return false, errors.Join(err, ErrRecoveryRequired)
		}
		a.stableRollbackTruncated = true
	}
	if !a.stableRollbackContentSync {
		if err := syncStableColumnAssetForRollback(a.file); err != nil {
			return false, errors.Join(err, ErrRecoveryRequired)
		}
		a.stableRollbackContentSync = true
	}
	if !a.created {
		if err := rootpublication.ValidateStableChildLink(a.stableParent, a.file, a.stableChildName); err != nil {
			return false, errors.Join(err, ErrRecoveryRequired)
		}
		return true, nil
	}
	if !a.stableRollbackUnlinked {
		if err := rootpublication.ValidateStableChildLink(a.stableParent, a.file, a.stableChildName); err != nil {
			return false, errors.Join(err, ErrRecoveryRequired)
		}
		if err := removeStableColumnAssetForRollback(a.stableParent, a.stableChildName); err != nil {
			return false, errors.Join(err, ErrRecoveryRequired)
		}
		a.stableRollbackUnlinked = true
		clearColumnAssetSegmentDirSyncKnown(a.assetPath)
	}
	if !a.stableRollbackParentSync {
		if err := syncStableColumnAssetParentRollback(a.stableParent); err != nil {
			return false, errors.Join(err, ErrRecoveryRequired)
		}
		a.stableRollbackParentSync = true
	}
	if a.stableNamespaceProofAdded {
		if err := a.stableRegistry.ForgetStableNamespaceLinkIdentity(
			a.stableParentIdentity, a.stableChildIdentity, a.stableChildName,
		); err != nil {
			return false, errors.Join(err, ErrRecoveryRequired)
		}
		a.stableNamespaceProofAdded = false
	}
	return true, nil
}

func (a *columnPhysicalAssetSegmentAppender) retainStableRollbackForRecovery() error {
	if a == nil || a.stableRegistry == nil || a.stableRecoveryRetained {
		return nil
	}
	if a.stableRecoveryRetainer == nil {
		return errors.Join(
			fmt.Errorf("%w: stable column rollback lacks DB recovery retainer", rootpublication.ErrResourceOwnership),
			ErrRecoveryRequired,
		)
	}
	err := a.stableRecoveryRetainer.RetainStableResourceCaptureRecovery(func() error {
		complete, rollbackErr := a.rollbackStableAppend()
		if complete {
			rollbackErr = nil
		}
		return errors.Join(rollbackErr, a.releaseStableRollbackRecoveryAuthority())
	})
	if err != nil {
		return errors.Join(
			err,
			ErrRecoveryRequired,
		)
	}
	a.stableRecoveryRetained = true
	return nil
}

func (a *columnPhysicalAssetSegmentAppender) releaseStableRollbackRecoveryAuthority() error {
	if a == nil {
		return nil
	}
	var err error
	if a.file != nil && a.closeFile {
		err = errors.Join(err, a.file.Close())
		a.closeFile = false
		a.file = nil
	}
	err = errors.Join(err, a.releaseStableConstructionAuthority())
	if a.stableParent != nil {
		err = errors.Join(err, a.stableParent.Close())
		a.stableParent = nil
	}
	a.stableRecoveryRetained = false
	a.releaseLock()
	return err
}

func (a *columnPhysicalAssetSegmentAppender) abort() error {
	if a == nil {
		return nil
	}
	var closeErr error
	if a.file != nil && a.closeFile {
		closeErr = a.file.Close()
		a.closeFile = false
		a.file = nil
	}
	if a.created {
		clearColumnAssetSegmentDirSyncKnown(a.assetPath)
	}
	if a.stableResources != nil {
		a.stableResources.Release()
		a.stableResources = nil
	}
	closeErr = errors.Join(closeErr, a.releaseStableConstructionAuthority())
	if a.stableNamespaceProofAdded {
		closeErr = errors.Join(closeErr, a.stableRegistry.ForgetStableNamespaceLinkIdentity(
			a.stableParentIdentity, a.stableChildIdentity, a.stableChildName,
		))
		a.stableNamespaceProofAdded = false
	}
	if a.stableParent != nil {
		closeErr = errors.Join(closeErr, a.stableParent.Close())
		a.stableParent = nil
	}
	a.releaseLock()
	return closeErr
}

func (a *columnPhysicalAssetSegmentAppender) releaseLock() {
	if a != nil && a.lock != nil && a.unlockLock {
		a.lock.Unlock()
		a.unlockLock = false
	}
}

func readColumnPhysicalAssetFromManager(rootDir string, ref ColumnAssetRef) ([]byte, error) {
	return readColumnPhysicalAssetFromManagerInto(rootDir, ref, nil)
}

func readColumnPhysicalAssetFromManagerInto(rootDir string, ref ColumnAssetRef, dst []byte) ([]byte, error) {
	return readColumnPhysicalAssetFromManagerIntoWithIntegrity(rootDir, ref, dst, ColumnAssetReadIntegrityVerify)
}

func readColumnPhysicalAssetFromManagerIntoWithIntegrity(rootDir string, ref ColumnAssetRef, dst []byte, integrity ColumnAssetReadIntegrity) ([]byte, error) {
	if err := validateColumnAssetRefForPlan(ref); err != nil {
		return nil, err
	}
	verifyChecksum, err := columnAssetReadIntegrityVerifyChecksum(integrity)
	if err != nil {
		return nil, err
	}
	if ref.Length > int64(maxCollectionInt) {
		return nil, fmt.Errorf("collections: column physical asset length=%d overflows int", ref.Length)
	}
	assetPath, err := columnAssetSegmentPath(rootDir, ref)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(assetPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	if cap(dst) < int(ref.Length) {
		dst = make([]byte, int(ref.Length))
	}
	raw := dst[:int(ref.Length)]
	n, err := file.ReadAt(raw, ref.Offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n != len(raw) {
		return nil, io.ErrUnexpectedEOF
	}
	if err := verifyColumnPhysicalAssetReadChecksumWithIntegrityForSegment(raw, ref, verifyChecksum, integrity, rootDir, columnAssetVerifiedChecksumFileIdentityFromFile(file)); err != nil {
		return nil, err
	}
	return raw, nil
}

func columnAssetReadIntegrityVerifyChecksum(integrity ColumnAssetReadIntegrity) (bool, error) {
	switch integrity {
	case "", ColumnAssetReadIntegrityVerify:
		return true, nil
	case ColumnAssetReadIntegrityCachedVerify:
		return true, nil
	case ColumnAssetReadIntegritySkipChecksums:
		return false, nil
	default:
		return false, fmt.Errorf("collections: unsupported column asset read integrity %q", integrity)
	}
}

func columnAssetReadIntegrityLabel(integrity ColumnAssetReadIntegrity) string {
	switch integrity {
	case "", ColumnAssetReadIntegrityVerify:
		return string(ColumnAssetReadIntegrityVerify)
	case ColumnAssetReadIntegrityCachedVerify:
		return string(ColumnAssetReadIntegrityCachedVerify)
	case ColumnAssetReadIntegritySkipChecksums:
		return string(ColumnAssetReadIntegritySkipChecksums)
	default:
		return string(integrity)
	}
}

func verifyColumnPhysicalAssetReadChecksum(raw []byte, ref ColumnAssetRef, verify bool) error {
	return verifyColumnPhysicalAssetReadChecksumWithIntegrityForSegment(raw, ref, verify, ColumnAssetReadIntegrityVerify, "", columnAssetVerifiedChecksumFileIdentity{})
}

func verifyColumnPhysicalAssetReadChecksumWithIntegrityForSegment(raw []byte, ref ColumnAssetRef, verify bool, integrity ColumnAssetReadIntegrity, rootDir string, fileIdentity columnAssetVerifiedChecksumFileIdentity) error {
	if !verify {
		return nil
	}
	if integrity == ColumnAssetReadIntegrityCachedVerify && columnAssetVerifiedChecksumCacheContains(rootDir, ref, fileIdentity) {
		return nil
	}
	if checksum := page.Checksum(raw); checksum != ref.Checksum {
		return fmt.Errorf("collections: column physical asset checksum=%d does not match ref checksum=%d", checksum, ref.Checksum)
	}
	if integrity == ColumnAssetReadIntegrityCachedVerify {
		columnAssetVerifiedChecksumCacheStore(rootDir, ref, fileIdentity)
	}
	return nil
}

func columnAssetVerifiedChecksumCacheContains(rootDir string, ref ColumnAssetRef, fileIdentity columnAssetVerifiedChecksumFileIdentity) bool {
	if !fileIdentity.valid {
		return false
	}
	key := columnAssetVerifiedChecksumKeyForRef(rootDir, ref, fileIdentity)
	idx := columnAssetVerifiedChecksumCacheIndex(key)
	columnAssetVerifiedChecksumCache.Lock()
	entry := columnAssetVerifiedChecksumCache.entries[idx]
	columnAssetVerifiedChecksumCache.Unlock()
	return entry.valid && entry.key == key
}

func columnAssetVerifiedChecksumCacheStore(rootDir string, ref ColumnAssetRef, fileIdentity columnAssetVerifiedChecksumFileIdentity) {
	if !fileIdentity.valid {
		return
	}
	key := columnAssetVerifiedChecksumKeyForRef(rootDir, ref, fileIdentity)
	idx := columnAssetVerifiedChecksumCacheIndex(key)
	columnAssetVerifiedChecksumCache.Lock()
	columnAssetVerifiedChecksumCache.entries[idx] = columnAssetVerifiedChecksumEntry{key: key, valid: true}
	columnAssetVerifiedChecksumCache.Unlock()
}

func columnAssetVerifiedChecksumKeyForRef(rootDir string, ref ColumnAssetRef, fileIdentity columnAssetVerifiedChecksumFileIdentity) columnAssetVerifiedChecksumKey {
	return columnAssetVerifiedChecksumKey{
		rootDir:    rootDir,
		kind:       ref.Kind,
		namespace:  ref.Namespace,
		generation: ref.Generation,
		partID:     ref.PartID,
		fileID:     ref.FileID,
		offset:     ref.Offset,
		length:     ref.Length,
		checksum:   ref.Checksum,
		fileDev:    fileIdentity.dev,
		fileIno:    fileIdentity.ino,
		fileSize:   fileIdentity.size,
		fileModNS:  fileIdentity.modTimeUnixNano,
	}
}

func columnAssetVerifiedChecksumCacheIndex(key columnAssetVerifiedChecksumKey) int {
	h := uint64(1469598103934665603)
	h = columnAssetVerifiedChecksumCacheHashString(h, key.rootDir)
	h = columnAssetVerifiedChecksumCacheHashString(h, string(key.kind))
	h = columnAssetVerifiedChecksumCacheHashString(h, key.namespace)
	h = columnAssetVerifiedChecksumCacheHashUint64(h, key.generation)
	h = columnAssetVerifiedChecksumCacheHashUint64(h, key.partID)
	h = columnAssetVerifiedChecksumCacheHashUint64(h, uint64(key.fileID))
	h = columnAssetVerifiedChecksumCacheHashUint64(h, uint64(key.offset))
	h = columnAssetVerifiedChecksumCacheHashUint64(h, uint64(key.length))
	h = columnAssetVerifiedChecksumCacheHashUint64(h, uint64(key.checksum))
	h = columnAssetVerifiedChecksumCacheHashUint64(h, key.fileDev)
	h = columnAssetVerifiedChecksumCacheHashUint64(h, key.fileIno)
	h = columnAssetVerifiedChecksumCacheHashUint64(h, uint64(key.fileSize))
	h = columnAssetVerifiedChecksumCacheHashUint64(h, uint64(key.fileModNS))
	return int(h % columnAssetVerifiedChecksumCacheSlots)
}

func columnAssetVerifiedChecksumCacheHashString(h uint64, s string) uint64 {
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

func columnAssetVerifiedChecksumCacheHashUint64(h uint64, v uint64) uint64 {
	for i := 0; i < 8; i++ {
		h ^= uint64(byte(v >> (i * 8)))
		h *= 1099511628211
	}
	return h
}

type columnPhysicalAssetReadCache struct {
	namespace           string
	rootDir             string
	segmentDir          string
	readIntegrity       ColumnAssetReadIntegrity
	verifyChecksum      bool
	fileID              uint32
	file                *columnPhysicalAssetSegmentReader
	files               map[uint32]*columnPhysicalAssetSegmentReader
	scratch             []byte
	returnViews         bool
	forceReadAtFallback bool
	lastView            bool
	hits                uint64
	misses              uint64
	mmapHits            uint64
	readAtFallbacks     uint64
	fileOpens           uint64
	fileCloses          uint64
	// trustCachedVerifyFileIdentity lets explicit prepared lifetimes reuse the
	// identity captured when the segment reader was opened. Default read caches
	// keep refreshing identity so existing fail-closed tests and non-prepared
	// readers retain their stricter cached-verify behavior.
	trustCachedVerifyFileIdentity bool

	resourceManager *mappedresource.Manager
	resourceScope   mappedresource.Scope
	resourceReason  string
	resourceHandles []*mappedresource.Handle
}

type columnPhysicalAssetSegmentReader struct {
	file     *os.File
	mmap     []byte
	identity columnAssetVerifiedChecksumFileIdentity
}

func newColumnPhysicalAssetReadCache(rootDir string, namespace string) (columnPhysicalAssetReadCache, error) {
	return newColumnPhysicalAssetReadCacheWithIntegrity(rootDir, namespace, ColumnAssetReadIntegrityVerify)
}

func newColumnPhysicalAssetReadCacheWithIntegrity(rootDir string, namespace string, integrity ColumnAssetReadIntegrity) (columnPhysicalAssetReadCache, error) {
	if namespace == "" {
		return columnPhysicalAssetReadCache{}, errors.New("collections: column asset read cache namespace is required")
	}
	verifyChecksum, err := columnAssetReadIntegrityVerifyChecksum(integrity)
	if err != nil {
		return columnPhysicalAssetReadCache{}, err
	}
	managerNamespace, err := columnAssetManagerNamespaceForRoot(rootDir, namespace)
	if err != nil {
		return columnPhysicalAssetReadCache{}, err
	}
	return columnPhysicalAssetReadCache{
		namespace:      namespace,
		rootDir:        rootDir,
		segmentDir:     managerNamespace.SegmentDir,
		readIntegrity:  integrity,
		verifyChecksum: verifyChecksum,
	}, nil
}

func (c *columnPhysicalAssetReadCache) useMappedResourceManager(manager *mappedresource.Manager, scope mappedresource.Scope, reason string) error {
	if c == nil {
		return errors.New("collections: nil column physical asset read cache")
	}
	if manager != nil {
		if scope.Namespace == "" {
			scope.Namespace = c.namespace
		}
		if err := scope.Validate(); err != nil {
			return err
		}
	}
	c.resourceManager = manager
	c.resourceScope = scope
	c.resourceReason = reason
	return nil
}

type columnPhysicalAssetReadCacheLifecycleStats struct {
	MmapHits        uint64
	ReadAtFallbacks uint64
	FileOpens       uint64
	FileCloses      uint64
	ActiveHandles   int64
}

func (c *columnPhysicalAssetReadCache) lifecycleStats() columnPhysicalAssetReadCacheLifecycleStats {
	if c == nil {
		return columnPhysicalAssetReadCacheLifecycleStats{}
	}
	stats := columnPhysicalAssetReadCacheLifecycleStats{
		MmapHits:        c.mmapHits,
		ReadAtFallbacks: c.readAtFallbacks,
		FileOpens:       c.fileOpens,
		FileCloses:      c.fileCloses,
	}
	if c.resourceManager != nil {
		stats.ActiveHandles = c.resourceManager.Stats().ActiveHandles
	}
	return stats
}

func (c *columnPhysicalAssetReadCache) mappedResourceStats() mappedresource.Stats {
	if c == nil || c.resourceManager == nil {
		return mappedresource.Stats{}
	}
	return c.resourceManager.Stats()
}

func (c *columnPhysicalAssetReadCache) mappedResourcePins() []mappedresource.Pin {
	if c == nil || c.resourceManager == nil {
		return nil
	}
	return c.resourceManager.PinSummary()
}

func (c *columnPhysicalAssetReadCache) mappedResourcePinnedRefs() []ColumnAssetRef {
	if c == nil {
		return nil
	}
	return columnAssetRefsForMappedResourcePins(c.mappedResourcePins(), c.namespace)
}

func (c *columnPhysicalAssetReadCache) releaseResourceHandles() error {
	if c == nil || len(c.resourceHandles) == 0 {
		return nil
	}
	var releaseErr error
	for _, handle := range c.resourceHandles {
		if err := handle.Release(); err != nil && releaseErr == nil {
			releaseErr = err
		}
	}
	clear(c.resourceHandles)
	c.resourceHandles = c.resourceHandles[:0]
	return releaseErr
}

func (c *columnPhysicalAssetReadCache) releaseResourceHandlesBySource(source mappedresource.Source) error {
	if c == nil || len(c.resourceHandles) == 0 {
		return nil
	}
	var releaseErr error
	kept := c.resourceHandles[:0]
	for _, handle := range c.resourceHandles {
		if handle == nil {
			continue
		}
		if handle.Source() == source {
			if err := handle.Release(); err != nil && releaseErr == nil {
				releaseErr = err
			}
			continue
		}
		kept = append(kept, handle)
	}
	clear(c.resourceHandles[len(kept):])
	c.resourceHandles = kept
	return releaseErr
}

func (c *columnPhysicalAssetReadCache) close() error {
	var closeErr error
	if err := c.releaseResourceHandles(); err != nil {
		closeErr = err
	}
	if c.scratch != nil {
		putColumnPhysicalAssetReadScratch(c.scratch)
		c.scratch = nil
	}
	if c.file != nil {
		hadFile := c.file.file != nil
		if err := c.file.close(); err != nil && closeErr == nil {
			closeErr = err
		} else if err == nil && hadFile {
			c.fileCloses++
		}
		c.file = nil
	}
	for fileID, reader := range c.files {
		if reader == nil {
			continue
		}
		hadFile := reader.file != nil
		if err := reader.close(); err != nil && closeErr == nil {
			closeErr = err
		} else if err == nil && hadFile {
			c.fileCloses++
		}
		delete(c.files, fileID)
	}
	return closeErr
}

func (c *columnPhysicalAssetReadCache) read(ref ColumnAssetRef, dst []byte) ([]byte, error) {
	if c == nil {
		return nil, errors.New("collections: nil column physical asset read cache")
	}
	if ref.Namespace != c.namespace {
		return nil, fmt.Errorf("collections: column physical asset ref namespace=%q want %q", ref.Namespace, c.namespace)
	}
	reader, err := c.fileForRef(ref)
	if err != nil {
		return nil, err
	}
	if c.lastView {
		dst = nil
		c.lastView = false
	}
	if c.returnViews {
		if raw, ok, err := reader.readView(ref); err != nil {
			return nil, err
		} else if ok {
			if err := c.verifyReadChecksum(raw, ref, reader); err != nil {
				return nil, err
			}
			if _, err := c.trackResourceRead(ref, raw, mappedresource.SourceMapped, ""); err != nil {
				return nil, err
			}
			c.mmapHits++
			c.lastView = true
			return raw, nil
		}
		c.readAtFallbacks++
	}
	if ref.Length >= 0 && ref.Length <= int64(maxCollectionInt) && cap(dst) < int(ref.Length) {
		if cap(c.scratch) < int(ref.Length) {
			if c.scratch != nil {
				putColumnPhysicalAssetReadScratch(c.scratch)
			}
			c.scratch = getColumnPhysicalAssetReadScratch(int(ref.Length))
		}
		dst = c.scratch
	}
	raw, err := readColumnPhysicalAssetFromFileWithChecksum(reader.file, ref, dst, false)
	if err != nil {
		return nil, err
	}
	if err := c.verifyReadChecksum(raw, ref, reader); err != nil {
		return nil, err
	}
	var fallback mappedresource.FallbackReason
	if c.returnViews {
		fallback = mappedresource.FallbackReadAt
	}
	if _, err := c.trackResourceRead(ref, raw, mappedresource.SourceHeapCopy, fallback); err != nil {
		return nil, err
	}
	return raw, nil
}

func (c *columnPhysicalAssetReadCache) validateFullRef(ref ColumnAssetRef) (int, error) {
	if c == nil {
		return 0, errors.New("collections: nil column physical asset read cache")
	}
	if ref.Namespace != c.namespace {
		return 0, fmt.Errorf("collections: column physical asset ref namespace=%q want %q", ref.Namespace, c.namespace)
	}
	reader, err := c.fileForRef(ref)
	if err != nil {
		return 0, err
	}
	if c.returnViews {
		if raw, ok, err := reader.readView(ref); err != nil {
			return 0, err
		} else if ok {
			if err := c.verifyReadChecksum(raw, ref, reader); err != nil {
				return 0, err
			}
			return len(raw), nil
		}
	}
	raw, err := readColumnPhysicalAssetFromFileWithChecksum(reader.file, ref, nil, false)
	if err != nil {
		return 0, err
	}
	if err := c.verifyReadChecksum(raw, ref, reader); err != nil {
		return 0, err
	}
	return len(raw), nil
}

func (c *columnPhysicalAssetReadCache) readRange(ref ColumnAssetRef, relativeOffset int64, length int64) ([]byte, error) {
	raw, _, err := c.readRangeHandle(ref, relativeOffset, length)
	return raw, err
}

func (c *columnPhysicalAssetReadCache) readRangeHandle(ref ColumnAssetRef, relativeOffset int64, length int64) ([]byte, *mappedresource.Handle, error) {
	if c == nil {
		return nil, nil, errors.New("collections: nil column physical asset read cache")
	}
	if err := validateColumnAssetRefForPlan(ref); err != nil {
		return nil, nil, err
	}
	if ref.Namespace != c.namespace {
		return nil, nil, fmt.Errorf("collections: column physical asset ref namespace=%q want %q", ref.Namespace, c.namespace)
	}
	if relativeOffset < 0 || length <= 0 {
		return nil, nil, fmt.Errorf("collections: column physical asset range offset=%d length=%d is invalid", relativeOffset, length)
	}
	end := relativeOffset + length
	if end < relativeOffset || end > ref.Length {
		return nil, nil, fmt.Errorf("collections: column physical asset range offset=%d length=%d outside ref length=%d", relativeOffset, length, ref.Length)
	}
	if ref.Offset > int64(^uint64(0)>>1)-relativeOffset {
		return nil, nil, fmt.Errorf("collections: column physical asset range base offset=%d relative=%d overflows", ref.Offset, relativeOffset)
	}
	rangeRef := ref
	rangeRef.Offset = ref.Offset + relativeOffset
	rangeRef.Length = length
	reader, err := c.fileForRef(ref)
	if err != nil {
		return nil, nil, err
	}
	if c.lastView {
		c.lastView = false
	}
	if c.returnViews {
		if raw, ok, err := reader.readView(rangeRef); err != nil {
			return nil, nil, err
		} else if ok {
			handle, err := c.trackResourceRead(rangeRef, raw, mappedresource.SourceMapped, "")
			if err != nil {
				return nil, nil, err
			}
			c.mmapHits++
			c.lastView = true
			return raw, handle, nil
		}
		c.readAtFallbacks++
	}
	if length > int64(maxCollectionInt) {
		return nil, nil, fmt.Errorf("collections: column physical asset range length=%d overflows int", length)
	}
	raw, err := readColumnPhysicalAssetFromFileWithChecksum(reader.file, rangeRef, make([]byte, int(length)), false)
	if err != nil {
		return nil, nil, err
	}
	var fallback mappedresource.FallbackReason
	if c.returnViews {
		fallback = mappedresource.FallbackReadAt
	}
	handle, err := c.trackResourceRead(rangeRef, raw, mappedresource.SourceHeapCopy, fallback)
	if err != nil {
		return nil, nil, err
	}
	return raw, handle, nil
}

func (c *columnPhysicalAssetReadCache) verifyReadChecksum(raw []byte, ref ColumnAssetRef, reader *columnPhysicalAssetSegmentReader) error {
	if c == nil {
		return errors.New("collections: nil column physical asset read cache")
	}
	var fileIdentity columnAssetVerifiedChecksumFileIdentity
	if reader != nil {
		fileIdentity = reader.identity
		if c.readIntegrity == ColumnAssetReadIntegrityCachedVerify && !c.trustCachedVerifyFileIdentity {
			fileIdentity = columnAssetVerifiedChecksumFileIdentityFromFile(reader.file)
			reader.identity = fileIdentity
		}
	}
	return verifyColumnPhysicalAssetReadChecksumWithIntegrityForSegment(raw, ref, c.verifyChecksum, c.readIntegrity, c.rootDir, fileIdentity)
}

func (c *columnPhysicalAssetReadCache) trackResourceRead(ref ColumnAssetRef, raw []byte, source mappedresource.Source, fallback mappedresource.FallbackReason) (*mappedresource.Handle, error) {
	if c == nil || c.resourceManager == nil {
		return nil, nil
	}
	key := mappedResourceKeyForColumnAssetRef(ref)
	if key.Length != int64(len(raw)) {
		return nil, fmt.Errorf("collections: column asset mapped-resource key length=%d raw bytes=%d", key.Length, len(raw))
	}
	scope := c.resourceScope
	if scope.Kind == "" {
		scope = mappedresource.Scope{Kind: mappedresource.ScopeTypedRowReader, ID: "column-physical-asset-read-cache", Namespace: c.namespace}
	}
	if scope.Namespace == "" {
		scope.Namespace = c.namespace
	}
	reason := c.resourceReason
	if reason == "" {
		reason = "column_physical_asset_read"
	}
	if source == mappedresource.SourceMapped {
		for _, handle := range c.resourceHandles {
			if handle != nil && !handle.Released() && handle.Source() == source && handle.Key().Equal(key) {
				return handle, nil
			}
		}
	}
	handleBytes := raw
	if source == mappedresource.SourceHeapCopy {
		// Heap-copy reads use the cache scratch path, so their implicit lifetime
		// ends when the next heap read may reuse scratch. Keep at most the current
		// heap handle active. Mapped view handles remain pinned until cache close,
		// with duplicate keys deduplicated above.
		if err := c.releaseResourceHandlesBySource(mappedresource.SourceHeapCopy); err != nil {
			return nil, err
		}
		if len(raw) != 0 {
			// AcquireBytes requires immutable bytes for the handle lifetime. Syscall
			// reads may return c.scratch, which is reused on later reads, so the
			// accounting/pin handle owns a stable copy while it is active. The caller
			// still receives the original raw bytes.
			handleBytes = append([]byte(nil), raw...)
		}
	}
	handle, err := c.resourceManager.AcquireBytes(key, scope, source, handleBytes, mappedresource.AcquireOptions{
		Reason:         reason,
		ValidationMode: mappedResourceValidationModeForColumnAssetIntegrity(c.readIntegrity),
		FallbackReason: fallback,
		ResourceRoot:   c.rootDir,
	})
	if err != nil {
		return nil, err
	}
	c.resourceHandles = append(c.resourceHandles, handle)
	return handle, nil
}

func mappedResourceKeyForColumnAssetRef(ref ColumnAssetRef) mappedresource.Key {
	class := mappedresource.ClassTypedRowAsset
	switch ref.Kind {
	case ColumnAssetKindTCS1TypedColumnPart, ColumnAssetKindTCS1AggregateMetadata, ColumnAssetKindTCS1DictionaryCodes, ColumnAssetKindTCS1Int64Values, ColumnAssetKindTCS1HNSWSearchPack, ColumnAssetKindQueryReadyBase, ColumnAssetKindQueryReadyDelta, ColumnAssetKindQueryReadyConsolidatedBase:
		class = mappedresource.ClassTypedColumnAsset
	}
	return mappedresource.Key{
		Class:      class,
		Namespace:  ref.Namespace,
		Kind:       string(ref.Kind),
		Generation: ref.Generation,
		PartID:     ref.PartID,
		FileID:     ref.FileID,
		Offset:     ref.Offset,
		Length:     ref.Length,
		Checksum:   uint64(ref.Checksum),
	}
}

func columnAssetRefsForMappedResourcePins(pins []mappedresource.Pin, namespace string) []ColumnAssetRef {
	if len(pins) == 0 {
		return nil
	}
	refs := make([]ColumnAssetRef, 0, len(pins))
	for _, pin := range pins {
		ref, ok := columnAssetRefForMappedResourceKey(pin.Key)
		if !ok {
			continue
		}
		if namespace != "" && ref.Namespace != namespace {
			continue
		}
		refs = append(refs, ref)
	}
	return refs
}

func columnAssetRefForMappedResourceKey(key mappedresource.Key) (ColumnAssetRef, bool) {
	switch key.Class {
	case mappedresource.ClassTypedRowAsset, mappedresource.ClassTypedColumnAsset:
	default:
		return ColumnAssetRef{}, false
	}
	if key.Checksum != uint64(uint32(key.Checksum)) {
		return ColumnAssetRef{}, false
	}
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKind(key.Kind),
		Namespace:  key.Namespace,
		Generation: key.Generation,
		PartID:     key.PartID,
		FileID:     key.FileID,
		Offset:     key.Offset,
		Length:     key.Length,
		Checksum:   uint32(key.Checksum),
	}
	if err := validateColumnAssetRefForPlan(ref); err != nil {
		return ColumnAssetRef{}, false
	}
	return ref, true
}

func mappedResourceValidationModeForColumnAssetIntegrity(integrity ColumnAssetReadIntegrity) mappedresource.ValidationMode {
	switch integrity {
	case ColumnAssetReadIntegrityCachedVerify:
		return mappedresource.ValidationCachedVerify
	case ColumnAssetReadIntegritySkipChecksums:
		return mappedresource.ValidationSkipChecksum
	default:
		return mappedresource.ValidationVerify
	}
}

func getColumnPhysicalAssetReadScratch(minLen int) []byte {
	if minLen <= columnPhysicalAssetReadScratchPoolMaxRetainBytes {
		if pooled, ok := columnPhysicalAssetReadScratchPool.Get().([]byte); ok {
			if cap(pooled) >= minLen {
				return pooled[:0]
			}
			putColumnPhysicalAssetReadScratch(pooled)
		}
	}
	return make([]byte, 0, minLen)
}

func putColumnPhysicalAssetReadScratch(scratch []byte) {
	if scratch == nil || cap(scratch) > columnPhysicalAssetReadScratchPoolMaxRetainBytes {
		return
	}
	columnPhysicalAssetReadScratchPool.Put(scratch[:0])
}

func (c *columnPhysicalAssetReadCache) fileForRef(ref ColumnAssetRef) (*columnPhysicalAssetSegmentReader, error) {
	if err := validateColumnAssetRefForPlan(ref); err != nil {
		return nil, err
	}
	if c.file != nil && c.fileID == ref.FileID {
		c.hits++
		if c.resourceManager != nil {
			c.resourceManager.RecordHit()
		}
		return c.file, nil
	}
	if c.files != nil {
		if reader := c.files[ref.FileID]; reader != nil {
			c.hits++
			if c.resourceManager != nil {
				c.resourceManager.RecordHit()
			}
			return reader, nil
		}
	}
	c.misses++
	if c.resourceManager != nil {
		c.resourceManager.RecordMiss()
	}
	file, err := os.Open(filepath.Join(c.segmentDir, columnAssetSegmentFileName(ref.FileID)))
	if err != nil {
		return nil, err
	}
	c.fileOpens++
	reader := &columnPhysicalAssetSegmentReader{
		file:     file,
		identity: columnAssetVerifiedChecksumFileIdentityFromFile(file),
	}
	if c.returnViews && !c.forceReadAtFallback {
		if mapped, err := mmapColumnPhysicalAssetFile(file); err == nil {
			reader.mmap = mapped
		}
	}
	if c.file == nil && c.files == nil {
		c.fileID = ref.FileID
		c.file = reader
		return reader, nil
	}
	if c.files == nil {
		c.files = make(map[uint32]*columnPhysicalAssetSegmentReader, 2)
		if c.file != nil {
			c.files[c.fileID] = c.file
			c.file = nil
			c.fileID = 0
		}
	}
	c.files[ref.FileID] = reader
	return reader, nil
}

func (r *columnPhysicalAssetSegmentReader) close() error {
	if r == nil {
		return nil
	}
	unmapErr := munmapColumnPhysicalAssetFile(r.mmap)
	r.mmap = nil
	var closeErr error
	if r.file != nil {
		closeErr = r.file.Close()
		r.file = nil
	}
	return errors.Join(unmapErr, closeErr)
}

func (r *columnPhysicalAssetSegmentReader) readView(ref ColumnAssetRef) ([]byte, bool, error) {
	if r == nil || len(r.mmap) == 0 {
		return nil, false, nil
	}
	if ref.Offset < 0 || ref.Length < 0 || ref.Length > int64(maxCollectionInt) {
		return nil, false, nil
	}
	end := ref.Offset + ref.Length
	if end < ref.Offset {
		return nil, false, fmt.Errorf("collections: column physical asset offset=%d length=%d overflows", ref.Offset, ref.Length)
	}
	if end > int64(len(r.mmap)) {
		return nil, false, io.ErrUnexpectedEOF
	}
	return r.mmap[ref.Offset:end], true, nil
}

func readColumnPhysicalAssetFromFile(file *os.File, ref ColumnAssetRef, dst []byte) ([]byte, error) {
	return readColumnPhysicalAssetFromFileWithChecksum(file, ref, dst, true)
}

func readColumnPhysicalAssetFromFileWithChecksum(file *os.File, ref ColumnAssetRef, dst []byte, verifyChecksum bool) ([]byte, error) {
	if file == nil {
		return nil, errors.New("collections: nil column physical asset segment file")
	}
	if ref.Length > int64(maxCollectionInt) {
		return nil, fmt.Errorf("collections: column physical asset length=%d overflows int", ref.Length)
	}
	if cap(dst) < int(ref.Length) {
		dst = make([]byte, int(ref.Length))
	}
	raw := dst[:int(ref.Length)]
	n, err := file.ReadAt(raw, ref.Offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n != len(raw) {
		return nil, io.ErrUnexpectedEOF
	}
	if err := verifyColumnPhysicalAssetReadChecksum(raw, ref, verifyChecksum); err != nil {
		return nil, err
	}
	return raw, nil
}

func columnAssetSegmentPath(rootDir string, ref ColumnAssetRef) (string, error) {
	if ref.Namespace == "" {
		return "", errors.New("collections: column asset ref namespace is required")
	}
	if ref.FileID == 0 {
		return "", errors.New("collections: column asset ref file_id is required")
	}
	namespace, err := columnAssetManagerNamespaceForRoot(rootDir, ref.Namespace)
	if err != nil {
		return "", err
	}
	return filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(ref.FileID)), nil
}

func columnAssetSegmentFileName(fileID uint32) string {
	return fmt.Sprintf("%s%06d%s", columnAssetSegmentFilePrefix, fileID, columnAssetSegmentFileSuffix)
}

func columnAssetSegmentAllocationLock(segmentDir string) *sync.Mutex {
	return &columnAssetSegmentAllocationLocks[columnAssetSegmentAllocationLockIndex(segmentDir)]
}

func columnAssetSegmentAllocationLockIndex(segmentDir string) uint64 {
	return columnAssetSegmentLockIndex(filepath.Clean(segmentDir))
}

func columnAssetSegmentWriteLock(assetPath string) *sync.Mutex {
	return &columnAssetSegmentWriteLocks[columnAssetSegmentLockIndex(filepath.Clean(assetPath))]
}

func columnAssetSegmentDirSyncKnown(assetPath string) bool {
	cleanPath := filepath.Clean(assetPath)
	cache := &columnAssetSegmentDirSyncCaches[columnAssetSegmentLockIndex(cleanPath)]
	cache.Lock()
	defer cache.Unlock()
	return cache.known && cache.assetPath == cleanPath
}

func markColumnAssetSegmentDirSynced(assetPath string) {
	if assetPath == "" {
		return
	}
	cleanPath := filepath.Clean(assetPath)
	cache := &columnAssetSegmentDirSyncCaches[columnAssetSegmentLockIndex(cleanPath)]
	cache.Lock()
	cache.assetPath = cleanPath
	cache.known = true
	cache.Unlock()
}

func clearColumnAssetSegmentDirSyncKnown(assetPath string) {
	if assetPath == "" {
		return
	}
	cleanPath := filepath.Clean(assetPath)
	cache := &columnAssetSegmentDirSyncCaches[columnAssetSegmentLockIndex(cleanPath)]
	cache.Lock()
	if cache.known && cache.assetPath == cleanPath {
		cache.known = false
		cache.assetPath = ""
	}
	cache.Unlock()
}

func columnAssetSegmentLockIndex(name string) uint64 {
	const (
		fnvOffset64 = 14695981039346656037
		fnvPrime64  = 1099511628211
	)
	hash := uint64(fnvOffset64)
	for i := 0; i < len(name); i++ {
		hash ^= uint64(name[i])
		hash *= fnvPrime64
	}
	return hash % uint64(len(columnAssetSegmentWriteLocks))
}

func syncColumnAssetSegmentFileObserved(file *os.File, root string) error {
	if file == nil {
		return syncColumnAssetSegmentFileForPublish(file)
	}
	path := file.Name()
	if err := durabilitycut.EmitPath(durabilitycut.BeforeDependencyFileSync, durabilitycut.ResourceAuxiliary, root, path); err != nil {
		return err
	}
	if err := syncColumnAssetSegmentFileForPublish(file); err != nil {
		return err
	}
	return durabilitycut.EmitPath(durabilitycut.AfterDependencyFileSync, durabilitycut.ResourceAuxiliary, root, path)
}

func stabilizeColumnAssetNamespaceForPublish(token *rootpublication.StableNamespaceToken, namespace columnAssetManagerNamespace) error {
	if token == nil {
		return errors.New("collections: stable column asset namespace token is nil")
	}
	if err := durabilitycut.EmitPath(durabilitycut.BeforeNewFileDirectorySync, durabilitycut.ResourceAuxiliary, namespace.SegmentDir, namespace.SegmentDir); err != nil {
		return err
	}
	if err := token.Stabilize(); err != nil {
		return err
	}
	return durabilitycut.EmitPath(durabilitycut.AfterNewFileDirectorySync, durabilitycut.ResourceAuxiliary, namespace.SegmentDir, namespace.SegmentDir)
}

func syncColumnAssetDir(dir string) error {
	if runtime.GOOS == "windows" || dir == "" {
		return nil
	}
	if err := durabilitycut.EmitPath(durabilitycut.BeforeNewFileDirectorySync, durabilitycut.ResourceAuxiliary, dir, dir); err != nil {
		return err
	}
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	if err := file.Sync(); err != nil {
		// Some platforms/filesystems do not support directory fsync. The segment
		// payload itself is already fsync'd; keep directory sync best-effort rather
		// than making the column manager unusable there.
		lowerErr := strings.ToLower(err.Error())
		if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.EPERM) || strings.Contains(lowerErr, "not supported") {
			return nil
		}
		return err
	}
	return durabilitycut.EmitPath(durabilitycut.AfterNewFileDirectorySync, durabilitycut.ResourceAuxiliary, dir, dir)
}
