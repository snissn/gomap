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

// columnAssetSegmentWriteLocks is a bounded stripe set keyed by canonical
// segment path. Writers to the same segment share a process-local offset lock
// without retaining one mutex per temp dir or segment path forever.
var columnAssetSegmentWriteLocks [columnAssetSegmentWriteLockStripes]sync.Mutex

// columnAssetSegmentAllocationLocks serialize same-process segment file-id
// allocation by namespace without retaining one lock per temp dir forever.
// O_EXCL still protects against external process races.
var columnAssetSegmentAllocationLocks [columnAssetSegmentWriteLockStripes]sync.Mutex

// columnAssetSegmentAllocationCaches avoid repeated directory scans for hot
// same-namespace segment allocation while keeping cache memory bounded by the
// stripe count. Collisions only evict the cached hint; O_EXCL remains the source
// of truth for file-id ownership.
var columnAssetSegmentAllocationCaches [columnAssetSegmentWriteLockStripes]columnAssetSegmentAllocationCache

type columnAssetSegmentAllocationCache struct {
	segmentDir string
	nextFileID uint32
	valid      bool
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
	cleanNamespace, err := cleanColumnAssetNamespace(namespace)
	if err != nil {
		return columnAssetManagerNamespace{}, err
	}
	namespaceRoot := filepath.Join(rootDir, filepath.FromSlash(cleanNamespace))
	assetDir := filepath.Join(namespaceRoot, columnAssetManagerAssetsDirName)
	return columnAssetManagerNamespace{
		ManagerRootDir: rootDir,
		RootDir:        namespaceRoot,
		AssetDir:       assetDir,
		SegmentDir:     filepath.Join(assetDir, columnAssetManagerSegmentsDirName),
		IndexDir:       filepath.Join(assetDir, columnAssetManagerIndexesDirName),
		PreparedDir:    filepath.Join(namespaceRoot, columnAssetManagerPreparedDirName),
		QuarantineDir:  filepath.Join(namespaceRoot, columnAssetManagerQuarantineDirName),
		TempDir:        filepath.Join(namespaceRoot, columnAssetManagerTempDirName),
	}, nil
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
	return writeColumnPhysicalAssetToManagerSegment(rootDir, cfg, payload, generation, partID, columnAssetM12ASegmentFileID)
}

func writeColumnPhysicalAssetToManagerSegment(rootDir string, cfg ColumnStoreConfig, payload []byte, generation, partID uint64, fileID uint32) (ColumnAssetRef, error) {
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
		Kind:       ColumnAssetKindTCS1PartImage,
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
		Kind:       ColumnAssetKindTCS1PartImage,
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
	if err := validateColumnAssetRefForPlan(ref); err != nil {
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
	if checksum := page.Checksum(raw); checksum != ref.Checksum {
		return nil, fmt.Errorf("collections: column physical asset checksum=%d does not match ref checksum=%d", checksum, ref.Checksum)
	}
	return raw, nil
}

type columnPhysicalAssetReadCache struct {
	namespace  string
	segmentDir string
	fileID     uint32
	file       *os.File
	files      map[uint32]*os.File
	hits       uint64
	misses     uint64
}

func newColumnPhysicalAssetReadCache(rootDir string, namespace string) (columnPhysicalAssetReadCache, error) {
	if namespace == "" {
		return columnPhysicalAssetReadCache{}, errors.New("collections: column asset read cache namespace is required")
	}
	managerNamespace, err := columnAssetManagerNamespaceForRoot(rootDir, namespace)
	if err != nil {
		return columnPhysicalAssetReadCache{}, err
	}
	return columnPhysicalAssetReadCache{
		namespace:  namespace,
		segmentDir: managerNamespace.SegmentDir,
	}, nil
}

func (c *columnPhysicalAssetReadCache) close() error {
	var closeErr error
	if c.file != nil {
		if err := c.file.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
		c.file = nil
	}
	for fileID, file := range c.files {
		if file == nil {
			continue
		}
		if err := file.Close(); err != nil && closeErr == nil {
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
	file, err := c.fileForRef(ref)
	if err != nil {
		return nil, err
	}
	return readColumnPhysicalAssetFromFile(file, ref, dst)
}

func (c *columnPhysicalAssetReadCache) fileForRef(ref ColumnAssetRef) (*os.File, error) {
	if err := validateColumnAssetRefForPlan(ref); err != nil {
		return nil, err
	}
	if c.file != nil && c.fileID == ref.FileID {
		c.hits++
		return c.file, nil
	}
	if c.files != nil {
		if file := c.files[ref.FileID]; file != nil {
			c.hits++
			return file, nil
		}
	}
	file, err := os.Open(filepath.Join(c.segmentDir, columnAssetSegmentFileName(ref.FileID)))
	if err != nil {
		return nil, err
	}
	c.misses++
	if c.file == nil && c.files == nil {
		c.fileID = ref.FileID
		c.file = file
		return file, nil
	}
	if c.files == nil {
		c.files = make(map[uint32]*os.File, 2)
		if c.file != nil {
			c.files[c.fileID] = c.file
			c.file = nil
			c.fileID = 0
		}
	}
	c.files[ref.FileID] = file
	return file, nil
}

func readColumnPhysicalAssetFromFile(file *os.File, ref ColumnAssetRef, dst []byte) ([]byte, error) {
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
	if checksum := page.Checksum(raw); checksum != ref.Checksum {
		return nil, fmt.Errorf("collections: column physical asset checksum=%d does not match ref checksum=%d", checksum, ref.Checksum)
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
