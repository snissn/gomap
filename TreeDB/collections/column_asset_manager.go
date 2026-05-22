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
	"strings"
	"sync"
	"syscall"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	columnAssetManagerAssetsDirName     = "assets"
	columnAssetManagerSegmentsDirName   = "segments"
	columnAssetManagerIndexesDirName    = "indexes"
	columnAssetManagerPreparedDirName   = "prepared"
	columnAssetManagerQuarantineDirName = "quarantine"
	columnAssetManagerTempDirName       = "tmp"
	columnAssetSegmentFilePrefix        = "segment-"
	columnAssetSegmentFileSuffix        = ".tca"
	columnAssetM12ASegmentFileID        = uint32(1)
)

const columnAssetSegmentWriteLockStripes = 64
const columnPhysicalAssetReadScratchPoolMaxRetainBytes = 16 << 20
const columnAssetVerifiedChecksumCacheSlots = 4096

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
	return writeColumnAssetToManagerSegment(rootDir, cfg, payload, kind, generation, partID, columnAssetM12ASegmentFileID)
}

func writeColumnPhysicalAssetToManagerSegment(rootDir string, cfg ColumnStoreConfig, payload []byte, generation, partID uint64, fileID uint32) (ColumnAssetRef, error) {
	return writeColumnAssetToManagerSegment(rootDir, cfg, payload, ColumnAssetKindTCS1PartImage, generation, partID, fileID)
}

func writeColumnAssetToManagerSegment(rootDir string, cfg ColumnStoreConfig, payload []byte, kind ColumnAssetKind, generation, partID uint64, fileID uint32) (ColumnAssetRef, error) {
	if cfg.AssetManager == nil {
		return ColumnAssetRef{}, errors.New("collections: column physical asset write requires asset manager")
	}
	if cfg.AssetManager.Kind != ColumnAssetManagerValueLogShaped {
		return ColumnAssetRef{}, fmt.Errorf("collections: unsupported column asset manager %q", cfg.AssetManager.Kind)
	}
	if !cfg.AssetManager.IsolatedNamespace {
		return ColumnAssetRef{}, errors.New("collections: column physical asset write requires isolated namespace")
	}
	if len(payload) == 0 {
		return ColumnAssetRef{}, errors.New("collections: column physical asset payload is empty")
	}
	if generation == 0 || partID == 0 {
		return ColumnAssetRef{}, errors.New("collections: column physical asset write requires generation and part_id")
	}
	if fileID == 0 {
		return ColumnAssetRef{}, errors.New("collections: column physical asset write requires file_id")
	}
	checksum := page.Checksum(payload)
	namespace, err := columnAssetManagerNamespaceForRoot(rootDir, cfg.AssetManager.Namespace)
	if err != nil {
		return ColumnAssetRef{}, err
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		return ColumnAssetRef{}, err
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
		return ColumnAssetRef{}, err
	}
	segmentLock := columnAssetSegmentWriteLock(assetPath)
	segmentLock.Lock()
	defer segmentLock.Unlock()
	file, err := os.OpenFile(assetPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return ColumnAssetRef{}, err
	}
	closeFile := true
	defer func() {
		if closeFile {
			_ = file.Close()
		}
	}()
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return ColumnAssetRef{}, err
	}
	written, err := writeColumnAssetSegmentPayload(file, payload)
	if err != nil {
		return ColumnAssetRef{}, err
	}
	if written != len(payload) {
		return ColumnAssetRef{}, io.ErrShortWrite
	}
	if err := file.Sync(); err != nil {
		return ColumnAssetRef{}, err
	}
	if err := file.Close(); err != nil {
		return ColumnAssetRef{}, err
	}
	closeFile = false
	if err := syncColumnAssetDir(namespace.SegmentDir); err != nil {
		return ColumnAssetRef{}, err
	}
	ref.Offset = offset
	return ref, nil
}

func nextColumnAssetSegmentFileID(namespace columnAssetManagerNamespace) (uint32, error) {
	segments, err := listColumnAssetReachabilitySegments(context.Background(), namespace.SegmentDir)
	if err != nil {
		return 0, err
	}
	maxFileID := uint32(0)
	for _, segment := range segments {
		if segment.fileID > maxFileID {
			maxFileID = segment.fileID
		}
	}
	if maxFileID < columnAssetM12ASegmentFileID {
		maxFileID = columnAssetM12ASegmentFileID
	}
	if maxFileID == ^uint32(0) {
		return 0, errors.New("collections: column asset segment file_id exhausted")
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

func nextColumnAssetSegmentFileIDCached(namespace columnAssetManagerNamespace, cleanSegmentDir string, cache *columnAssetSegmentAllocationCache) (uint32, error) {
	if cache != nil && cache.valid && cache.segmentDir == cleanSegmentDir {
		if cache.nextFileID == 0 {
			return 0, errors.New("collections: column asset segment file_id exhausted")
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
	if allocatedFileID == ^uint32(0) {
		cache.nextFileID = 0
		return
	}
	cache.nextFileID = allocatedFileID + 1
}

type columnPhysicalAssetSegmentAppender struct {
	cfg        ColumnStoreConfig
	namespace  columnAssetManagerNamespace
	fileID     uint32
	assetPath  string
	file       *os.File
	offset     int64
	failed     bool
	lock       *sync.Mutex
	closeFile  bool
	unlockLock bool
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
		cfg:        cfg,
		namespace:  namespace,
		fileID:     fileID,
		assetPath:  assetPath,
		lock:       segmentLock,
		unlockLock: true,
	}
	file, err := os.OpenFile(assetPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		appender.releaseLock()
		return nil, err
	}
	appender.file = file
	appender.closeFile = true
	return appender, nil
}

func (a *columnPhysicalAssetSegmentAppender) append(payload []byte, generation, partID uint64) (ColumnAssetRef, error) {
	return a.appendKind(payload, ColumnAssetKindTCS1PartImage, generation, partID)
}

func (a *columnPhysicalAssetSegmentAppender) appendKind(payload []byte, kind ColumnAssetKind, generation, partID uint64) (ColumnAssetRef, error) {
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
	return ref, nil
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

func (a *columnPhysicalAssetSegmentAppender) close() error {
	if a == nil {
		return nil
	}
	var appenderErr error
	var fileSyncErr error
	var fileCloseErr error
	if a.failed {
		appenderErr = errors.New("collections: column physical asset appender is failed")
	}
	if a.file != nil && a.closeFile {
		if !a.failed {
			fileSyncErr = a.file.Sync()
		}
		fileCloseErr = a.file.Close()
		a.closeFile = false
		a.file = nil
	}
	dirSyncErr := syncColumnAssetDir(a.namespace.SegmentDir)
	var removeErr error
	removeOnClose := columnPhysicalAssetSegmentAppenderRemoveOnClose(a.failed, fileSyncErr, fileCloseErr, dirSyncErr)
	if removeOnClose && a.assetPath != "" {
		removeErr = os.Remove(a.assetPath)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
	}
	var removeDirSyncErr error
	if removeOnClose && removeErr == nil {
		removeDirSyncErr = syncColumnAssetDir(a.namespace.SegmentDir)
	}
	a.releaseLock()
	return errors.Join(appenderErr, fileSyncErr, fileCloseErr, removeErr, dirSyncErr, removeDirSyncErr)
}

func columnPhysicalAssetSegmentAppenderRemoveOnClose(failed bool, fileSyncErr, fileCloseErr, dirSyncErr error) bool {
	return failed || fileSyncErr != nil || fileCloseErr != nil || dirSyncErr != nil
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
	var removeErr error
	if a.assetPath != "" {
		removeErr = os.Remove(a.assetPath)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
	}
	syncErr := syncColumnAssetDir(a.namespace.SegmentDir)
	a.releaseLock()
	return errors.Join(closeErr, removeErr, syncErr)
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
	namespace      string
	rootDir        string
	segmentDir     string
	readIntegrity  ColumnAssetReadIntegrity
	verifyChecksum bool
	fileID         uint32
	file           *columnPhysicalAssetSegmentReader
	files          map[uint32]*columnPhysicalAssetSegmentReader
	scratch        []byte
	returnViews    bool
	lastView       bool
	hits           uint64
	misses         uint64

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
		if err := c.file.close(); err != nil && closeErr == nil {
			closeErr = err
		}
		c.file = nil
	}
	for fileID, reader := range c.files {
		if reader == nil {
			continue
		}
		if err := reader.close(); err != nil && closeErr == nil {
			closeErr = err
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
			if err := c.trackResourceRead(ref, raw, mappedresource.SourceMapped, ""); err != nil {
				return nil, err
			}
			c.lastView = true
			return raw, nil
		}
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
	if err := c.trackResourceRead(ref, raw, mappedresource.SourceHeapCopy, fallback); err != nil {
		return nil, err
	}
	return raw, nil
}

func (c *columnPhysicalAssetReadCache) verifyReadChecksum(raw []byte, ref ColumnAssetRef, reader *columnPhysicalAssetSegmentReader) error {
	if c == nil {
		return errors.New("collections: nil column physical asset read cache")
	}
	var fileIdentity columnAssetVerifiedChecksumFileIdentity
	if reader != nil {
		fileIdentity = reader.identity
		if c.readIntegrity == ColumnAssetReadIntegrityCachedVerify {
			fileIdentity = columnAssetVerifiedChecksumFileIdentityFromFile(reader.file)
			reader.identity = fileIdentity
		}
	}
	return verifyColumnPhysicalAssetReadChecksumWithIntegrityForSegment(raw, ref, c.verifyChecksum, c.readIntegrity, c.rootDir, fileIdentity)
}

func (c *columnPhysicalAssetReadCache) trackResourceRead(ref ColumnAssetRef, raw []byte, source mappedresource.Source, fallback mappedresource.FallbackReason) error {
	if c == nil || c.resourceManager == nil {
		return nil
	}
	key := mappedResourceKeyForColumnAssetRef(ref)
	if key.Length != int64(len(raw)) {
		return fmt.Errorf("collections: column asset mapped-resource key length=%d raw bytes=%d", key.Length, len(raw))
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
	handleBytes := raw
	if source == mappedresource.SourceHeapCopy && len(raw) != 0 {
		// AcquireBytes requires immutable bytes for the handle lifetime. Syscall
		// reads may return c.scratch, which is reused on later reads, so the
		// accounting/pin handle owns a stable copy when mapped-resource reporting
		// is enabled. The caller still receives the original raw bytes.
		handleBytes = append([]byte(nil), raw...)
	}
	handle, err := c.resourceManager.AcquireBytes(key, scope, source, handleBytes, mappedresource.AcquireOptions{
		Reason:         reason,
		ValidationMode: mappedResourceValidationModeForColumnAssetIntegrity(c.readIntegrity),
		FallbackReason: fallback,
	})
	if err != nil {
		return err
	}
	c.resourceHandles = append(c.resourceHandles, handle)
	return nil
}

func mappedResourceKeyForColumnAssetRef(ref ColumnAssetRef) mappedresource.Key {
	return mappedresource.Key{
		Class:      mappedresource.ClassTypedRowAsset,
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
	reader := &columnPhysicalAssetSegmentReader{
		file:     file,
		identity: columnAssetVerifiedChecksumFileIdentityFromFile(file),
	}
	if c.returnViews {
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

func syncColumnAssetDir(dir string) error {
	if runtime.GOOS == "windows" || dir == "" {
		return nil
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
	return nil
}
