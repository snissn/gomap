package collections

import (
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

type columnAssetManagerNamespace struct {
	RootDir       string
	AssetDir      string
	SegmentDir    string
	IndexDir      string
	PreparedDir   string
	QuarantineDir string
	TempDir       string
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
		RootDir:       namespaceRoot,
		AssetDir:      assetDir,
		SegmentDir:    filepath.Join(assetDir, columnAssetManagerSegmentsDirName),
		IndexDir:      filepath.Join(assetDir, columnAssetManagerIndexesDirName),
		PreparedDir:   filepath.Join(namespaceRoot, columnAssetManagerPreparedDirName),
		QuarantineDir: filepath.Join(namespaceRoot, columnAssetManagerQuarantineDirName),
		TempDir:       filepath.Join(namespaceRoot, columnAssetManagerTempDirName),
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
	dirs := []string{
		namespace.RootDir,
		namespace.AssetDir,
		namespace.SegmentDir,
		namespace.IndexDir,
		namespace.PreparedDir,
		namespace.QuarantineDir,
		namespace.TempDir,
	}
	for _, dir := range dirs {
		if dir == "" {
			return errors.New("collections: column asset manager namespace has empty dir")
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	return nil
}

func writeColumnPhysicalAssetToManager(rootDir string, cfg ColumnStoreConfig, payload []byte, generation, partID uint64) (ColumnAssetRef, error) {
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
	checksum := page.Checksum(payload)
	if checksum == 0 {
		return ColumnAssetRef{}, errors.New("collections: column physical asset checksum is zero")
	}
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
		FileID:     columnAssetM12ASegmentFileID,
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
	written, err := file.Write(payload)
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
		closeFile = false
		return ColumnAssetRef{}, err
	}
	closeFile = false
	if err := syncColumnAssetDir(namespace.SegmentDir); err != nil {
		return ColumnAssetRef{}, err
	}
	ref.Offset = offset
	return ref, nil
}

func readColumnPhysicalAssetFromManager(rootDir string, ref ColumnAssetRef) ([]byte, error) {
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
	raw := make([]byte, int(ref.Length))
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

func columnAssetSegmentWriteLock(assetPath string) *sync.Mutex {
	const (
		fnvOffset64 = 14695981039346656037
		fnvPrime64  = 1099511628211
	)
	hash := uint64(fnvOffset64)
	for i := 0; i < len(assetPath); i++ {
		hash ^= uint64(assetPath[i])
		hash *= fnvPrime64
	}
	return &columnAssetSegmentWriteLocks[hash%uint64(len(columnAssetSegmentWriteLocks))]
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
