package raftfsm

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	pathpkg "path"
	"path/filepath"
	"strings"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

const (
	raftSnapshotDBPrefixV1    = "db"
	raftSnapshotSidePrefixV1  = "side"
	raftSnapshotApplyPrefixV1 = "apply"

	raftSnapshotFormatConfigFileV1 = "format.json"
)

var raftSnapshotMainDBEntriesV1 = []string{
	"index.db",
	raftSnapshotFormatConfigFileV1,
	"wal",
	"value_vlog",
	"leaf_vlog",
	"column_assets",
}

var raftSnapshotSideStoreEntriesV1 = []string{
	"dictdb",
	"templatedb",
}

// ExportRaftSnapshotV1 exports a production snapshot archive for HashiCorp
// Raft. The archive contains TreeDB main storage, including persistent
// value-log segments, plus durable Raft apply metadata. It intentionally
// excludes HashiCorp Raft log/stable/snapshot directories.
func (f *FSM) ExportRaftSnapshotV1() (raftcluster.RaftSnapshotV1, error) {
	if f == nil {
		return raftcluster.RaftSnapshotV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.requireRaftSnapshotOpenV1(); err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	if err := f.db.Checkpoint(); err != nil {
		return raftcluster.RaftSnapshotV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "checkpoint before snapshot export: %v", err)
	}
	manifest, err := f.exportSnapshotManifestV1Locked(SnapshotManifestExportOptionsV1{})
	if err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	header := raftcluster.NewRaftSnapshotArchiveHeaderV1(manifest)
	headerBytes, err := raftcluster.EncodeRaftSnapshotArchiveHeaderV1(header)
	if err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := writeRaftSnapshotFileV1(tw, raftcluster.RaftSnapshotArchiveManifestPathV1, headerBytes, 0o600); err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	if err := appendRaftSnapshotTreeDBStorageV1(tw, raftSnapshotDBPrefixV1, raftcluster.MainDBDir(f.cluster.Dir)); err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	if err := appendRaftSnapshotTreeDBSideStoresV1(tw, raftSnapshotSidePrefixV1, raftcluster.MainDBDir(f.cluster.Dir)); err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	if err := appendRaftSnapshotDirV1(tw, raftSnapshotApplyPrefixV1, f.cluster.Layout.ApplyDir); err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	if err := tw.Close(); err != nil {
		return raftcluster.RaftSnapshotV1{}, fmt.Errorf("raftfsm: close snapshot archive: %w", err)
	}
	snapshot := raftcluster.RaftSnapshotV1{
		Manifest: manifest,
		Payload:  buf.Bytes(),
	}
	if err := snapshot.Validate(); err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	return snapshot, nil
}

// InstallRaftSnapshotV1 discards the local TreeDB state and installs a
// production snapshot archive. HashiCorp Raft calls Restore without concurrent
// Apply calls, so the FSM can safely close and reopen its local stores here.
func (f *FSM) InstallRaftSnapshotV1(reader io.Reader) error {
	if f == nil {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	if reader == nil {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "nil snapshot reader")
	}
	f.mu.RLock()
	if err := f.requireRaftSnapshotOpenV1(); err != nil {
		f.mu.RUnlock()
		return err
	}
	mainDir := raftcluster.MainDBDir(f.cluster.Dir)
	applyDir := f.cluster.Layout.ApplyDir
	f.mu.RUnlock()
	if mainDir == "" || applyDir == "" {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "missing snapshot install directories")
	}

	scratch, err := createRaftSnapshotScratchDirsV1(mainDir, applyDir)
	if err != nil {
		return err
	}
	tmpMain := scratch.main
	tmpApply := scratch.apply
	tmpSide := scratch.side
	cleanupMain := true
	cleanupApply := true
	cleanupSide := true
	defer func() {
		if cleanupMain {
			_ = os.RemoveAll(tmpMain)
		}
		if cleanupSide {
			_ = os.RemoveAll(tmpSide)
		}
		if cleanupApply {
			_ = os.RemoveAll(tmpApply)
		}
	}()

	header, err := extractRaftSnapshotArchiveV1(reader, tmpMain, tmpSide, tmpApply)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.requireRaftSnapshotOpenV1(); err != nil {
		return err
	}
	if err := header.Validate(f.snapshotScopeIdentityV1()); err != nil {
		return codedError(raftentry.ErrorRejectedConflictV1, "snapshot archive is not valid for FSM scope: %v", err)
	}
	if header.Manifest.GroupID != f.cluster.GroupID {
		return codedError(raftentry.ErrorRejectedConflictV1, "snapshot archive group %q does not match FSM group %q", header.Manifest.GroupID, f.cluster.GroupID)
	}
	if err := f.closeForRaftSnapshotRestoreV1(); err != nil {
		return err
	}
	if err := replaceSnapshotMainDBV1(mainDir, tmpMain); err != nil {
		return err
	}
	cleanupMain = false
	if err := replaceSnapshotSideStoresV1(mainDir, tmpSide); err != nil {
		return err
	}
	if err := replaceSnapshotDirV1(applyDir, tmpApply); err != nil {
		return err
	}
	cleanupApply = false
	if err := f.reopenAfterRaftSnapshotRestoreV1(); err != nil {
		return err
	}
	return f.verifyInstalledSnapshotManifestV1Locked(header.Manifest)
}

func (f *FSM) requireRaftSnapshotOpenV1() error {
	if f == nil {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	if f.closed {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is closed")
	}
	if f.db == nil || f.progress == nil || f.results == nil {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	return nil
}

func (f *FSM) closeForRaftSnapshotRestoreV1() error {
	var errs []error
	if f.progress != nil {
		errs = append(errs, f.progress.Close())
		f.progress = nil
	}
	if f.results != nil {
		errs = append(errs, f.results.Close())
		f.results = nil
	}
	if f.db != nil {
		errs = append(errs, f.db.Close())
		f.db = nil
	}
	if f.sideDBs != nil {
		errs = append(errs, f.sideDBs())
		f.sideDBs = nil
	}
	f.ownsDB = false
	return errors.Join(errs...)
}

func (f *FSM) reopenAfterRaftSnapshotRestoreV1() error {
	opts := f.restoreDB
	mainDir := raftcluster.MainDBDir(f.cluster.Dir)
	opts.Dir = mainDir
	opts.CommandWAL = true
	opts.CommandWALStatsScan = true
	if opts.CommandWALSegmentTargetBytes <= 0 {
		opts.CommandWALSegmentTargetBytes = 1 << 20
	}
	opts.DisableBackgroundPrune = true
	sideDBs, err := wireRaftSnapshotSideStoreLookupsV1(snapshotSideStoreRootV1(mainDir), &opts)
	if err != nil {
		return err
	}
	db, err := backenddb.Open(opts)
	if err != nil {
		_ = sideDBs()
		return err
	}
	progress, err := raftapply.OpenDurableApplyProgressStore(f.metadataDir, f.storeOptions)
	if err != nil {
		_ = errors.Join(db.Close(), sideDBs())
		return err
	}
	results, err := raftapply.OpenDurableApplyResultStore(f.metadataDir, f.storeOptions)
	if err != nil {
		_ = errors.Join(progress.Close(), db.Close(), sideDBs())
		return err
	}
	if err := validateProgressCoverage(db, progress, results); err != nil {
		_ = errors.Join(progress.Close(), results.Close(), db.Close(), sideDBs())
		return err
	}
	f.db = db
	f.progress = progress
	f.results = results
	f.sideDBs = sideDBs
	f.ownsDB = true
	return nil
}

func appendRaftSnapshotDirV1(tw *tar.Writer, prefix, root string) error {
	if tw == nil {
		return fmt.Errorf("raftfsm: nil snapshot archive writer")
	}
	root = filepath.Clean(root)
	info, err := os.Stat(root)
	if err != nil {
		return fmt.Errorf("raftfsm: stat snapshot directory %q: %w", root, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("raftfsm: snapshot path %q is not a directory", root)
	}
	return filepath.WalkDir(root, func(filePath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, filePath)
		if err != nil {
			return err
		}
		if rel == "." {
			return writeRaftSnapshotDirHeaderV1(tw, prefix)
		}
		if shouldSkipRaftSnapshotTreeDBFileV1(rel) {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			return fmt.Errorf("raftfsm: snapshot path %q is a symlink", filePath)
		}
		name := pathpkg.Join(prefix, filepath.ToSlash(rel))
		if entry.IsDir() {
			return writeRaftSnapshotDirHeaderV1(tw, name)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("raftfsm: snapshot path %q is not a regular file", filePath)
		}
		file, err := os.Open(filePath)
		if err != nil {
			return err
		}
		header := &tar.Header{
			Name:    name,
			Mode:    int64(info.Mode().Perm()),
			Size:    info.Size(),
			ModTime: info.ModTime(),
		}
		if err := tw.WriteHeader(header); err != nil {
			_ = file.Close()
			return err
		}
		_, copyErr := io.Copy(tw, file)
		closeErr := file.Close()
		return errors.Join(copyErr, closeErr)
	})
}

func shouldSkipRaftSnapshotTreeDBFileV1(rel string) bool {
	return filepath.Base(rel) == "command-wal-journal-owner.lock"
}

func appendRaftSnapshotTreeDBStorageV1(tw *tar.Writer, prefix, mainDir string) error {
	if err := writeRaftSnapshotDirHeaderV1(tw, prefix); err != nil {
		return err
	}
	for _, name := range raftSnapshotMainDBEntriesV1 {
		src := filepath.Join(mainDir, name)
		if err := appendRaftSnapshotStoragePathV1(tw, pathpkg.Join(prefix, name), src); err != nil {
			return err
		}
	}
	return nil
}

func appendRaftSnapshotTreeDBSideStoresV1(tw *tar.Writer, prefix, mainDir string) error {
	if err := writeRaftSnapshotDirHeaderV1(tw, prefix); err != nil {
		return err
	}
	root := snapshotSideStoreRootV1(mainDir)
	for _, name := range raftSnapshotSideStoreEntriesV1 {
		src := filepath.Join(root, name)
		if err := appendRaftSnapshotStoragePathV1(tw, pathpkg.Join(prefix, name), src); err != nil {
			return err
		}
	}
	return nil
}

func appendRaftSnapshotStoragePathV1(tw *tar.Writer, archiveName, src string) error {
	info, err := os.Lstat(src)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if info.Mode()&fs.ModeSymlink != 0 {
		return fmt.Errorf("raftfsm: snapshot path %q is a symlink", src)
	}
	if info.IsDir() {
		return appendRaftSnapshotDirV1(tw, archiveName, src)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("raftfsm: snapshot path %q is not a regular file", src)
	}
	file, err := os.Open(src)
	if err != nil {
		return err
	}
	if err := tw.WriteHeader(&tar.Header{
		Name:    pathpkg.Clean(archiveName),
		Mode:    int64(info.Mode().Perm()),
		Size:    info.Size(),
		ModTime: info.ModTime(),
	}); err != nil {
		_ = file.Close()
		return err
	}
	_, copyErr := io.Copy(tw, file)
	closeErr := file.Close()
	return errors.Join(copyErr, closeErr)
}

func writeRaftSnapshotDirHeaderV1(tw *tar.Writer, name string) error {
	return tw.WriteHeader(&tar.Header{
		Name:     pathpkg.Clean(name),
		Typeflag: tar.TypeDir,
		Mode:     0o700,
	})
}

func writeRaftSnapshotFileV1(tw *tar.Writer, name string, payload []byte, mode int64) error {
	if err := tw.WriteHeader(&tar.Header{
		Name: pathpkg.Clean(name),
		Mode: mode,
		Size: int64(len(payload)),
	}); err != nil {
		return err
	}
	_, err := tw.Write(payload)
	return err
}

func extractRaftSnapshotArchiveV1(reader io.Reader, mainDir, sideDir, applyDir string) (raftcluster.RaftSnapshotArchiveHeaderV1, error) {
	tr := tar.NewReader(reader)
	var (
		header       raftcluster.RaftSnapshotArchiveHeaderV1
		sawHeader    bool
		sawDBFile    bool
		sawApplyFile bool
	)
	for {
		tarHeader, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return raftcluster.RaftSnapshotArchiveHeaderV1{}, fmt.Errorf("raftfsm: read snapshot archive: %w", err)
		}
		if tarHeader == nil {
			continue
		}
		name := cleanSnapshotArchiveNameV1(tarHeader.Name)
		if name == raftcluster.RaftSnapshotArchiveManifestPathV1 {
			if tarHeader.Typeflag != tar.TypeReg && tarHeader.Typeflag != tar.TypeRegA {
				return raftcluster.RaftSnapshotArchiveHeaderV1{}, fmt.Errorf("raftfsm: snapshot archive header is not a regular file")
			}
			raw, err := io.ReadAll(tr)
			if err != nil {
				return raftcluster.RaftSnapshotArchiveHeaderV1{}, err
			}
			decoded, err := raftcluster.DecodeRaftSnapshotArchiveHeaderV1(raw, raftcluster.SnapshotScopeIdentityV1{})
			if err != nil {
				return raftcluster.RaftSnapshotArchiveHeaderV1{}, err
			}
			header = decoded
			sawHeader = true
			continue
		}
		if rel, ok, err := snapshotArchiveRelV1(name, raftSnapshotDBPrefixV1); err != nil {
			return raftcluster.RaftSnapshotArchiveHeaderV1{}, err
		} else if ok {
			file, err := extractRaftSnapshotEntryV1(tr, tarHeader, mainDir, rel)
			if err != nil {
				return raftcluster.RaftSnapshotArchiveHeaderV1{}, err
			}
			sawDBFile = sawDBFile || file
			continue
		}
		if rel, ok, err := snapshotArchiveRelV1(name, raftSnapshotSidePrefixV1); err != nil {
			return raftcluster.RaftSnapshotArchiveHeaderV1{}, err
		} else if ok {
			if _, err := extractRaftSnapshotEntryV1(tr, tarHeader, sideDir, rel); err != nil {
				return raftcluster.RaftSnapshotArchiveHeaderV1{}, err
			}
			continue
		}
		if rel, ok, err := snapshotArchiveRelV1(name, raftSnapshotApplyPrefixV1); err != nil {
			return raftcluster.RaftSnapshotArchiveHeaderV1{}, err
		} else if ok {
			file, err := extractRaftSnapshotEntryV1(tr, tarHeader, applyDir, rel)
			if err != nil {
				return raftcluster.RaftSnapshotArchiveHeaderV1{}, err
			}
			sawApplyFile = sawApplyFile || file
			continue
		}
		return raftcluster.RaftSnapshotArchiveHeaderV1{}, fmt.Errorf("raftfsm: unsupported snapshot archive path %q", tarHeader.Name)
	}
	if !sawHeader {
		return raftcluster.RaftSnapshotArchiveHeaderV1{}, fmt.Errorf("raftfsm: missing snapshot archive header")
	}
	if !sawDBFile {
		return raftcluster.RaftSnapshotArchiveHeaderV1{}, fmt.Errorf("raftfsm: snapshot archive has no TreeDB files")
	}
	if !sawApplyFile {
		return raftcluster.RaftSnapshotArchiveHeaderV1{}, fmt.Errorf("raftfsm: snapshot archive has no Raft apply metadata files")
	}
	return header, nil
}

func cleanSnapshotArchiveNameV1(name string) string {
	name = strings.TrimSpace(strings.ReplaceAll(name, "\\", "/"))
	name = strings.TrimPrefix(name, "/")
	return pathpkg.Clean(name)
}

func snapshotArchiveRelV1(name, prefix string) (string, bool, error) {
	if name == prefix {
		return "", true, nil
	}
	prefixSlash := prefix + "/"
	if !strings.HasPrefix(name, prefixSlash) {
		return "", false, nil
	}
	rel := strings.TrimPrefix(name, prefixSlash)
	if rel == "" || rel == "." || strings.HasPrefix(rel, "../") || strings.Contains(rel, "/../") {
		return "", false, fmt.Errorf("raftfsm: unsafe snapshot archive path %q", name)
	}
	return filepath.FromSlash(rel), true, nil
}

func extractRaftSnapshotEntryV1(tr *tar.Reader, header *tar.Header, root, rel string) (bool, error) {
	if rel == "" {
		if header.Typeflag == tar.TypeDir {
			return false, nil
		}
		return false, fmt.Errorf("raftfsm: snapshot archive root entry %q is not a directory", header.Name)
	}
	dest := filepath.Join(root, rel)
	if !sameOrUnderSnapshotDirV1(root, dest) {
		return false, fmt.Errorf("raftfsm: unsafe snapshot archive destination %q", dest)
	}
	switch header.Typeflag {
	case tar.TypeDir:
		return false, os.MkdirAll(dest, 0o700)
	case tar.TypeReg, tar.TypeRegA:
		if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
			return false, err
		}
		file, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return false, err
		}
		_, copyErr := io.Copy(file, tr)
		closeErr := file.Close()
		return true, errors.Join(copyErr, closeErr)
	default:
		return false, fmt.Errorf("raftfsm: unsupported snapshot archive entry type %d for %q", header.Typeflag, header.Name)
	}
}

func sameOrUnderSnapshotDirV1(parent, child string) bool {
	parent = filepath.Clean(parent)
	child = filepath.Clean(child)
	if parent == child {
		return true
	}
	rel, err := filepath.Rel(parent, child)
	return err == nil && rel != "." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) && rel != ".."
}

type raftSnapshotScratchDirsV1 struct {
	main  string
	side  string
	apply string
}

func createRaftSnapshotScratchDirsV1(mainDir, applyDir string) (raftSnapshotScratchDirsV1, error) {
	if mainDir == "" || applyDir == "" {
		return raftSnapshotScratchDirsV1{}, fmt.Errorf("raftfsm: empty snapshot scratch path")
	}
	mainParent := filepath.Dir(mainDir)
	applyParent := filepath.Dir(applyDir)
	if sameOrUnderSnapshotDirV1(mainDir, applyDir) {
		applyParent = mainParent
	}
	if err := os.MkdirAll(mainParent, 0o700); err != nil {
		return raftSnapshotScratchDirsV1{}, err
	}
	if err := os.MkdirAll(applyParent, 0o700); err != nil {
		return raftSnapshotScratchDirsV1{}, err
	}
	tmpMain, err := os.MkdirTemp(mainParent, ".raft-snapshot-main-*")
	if err != nil {
		return raftSnapshotScratchDirsV1{}, err
	}
	tmpApply, err := os.MkdirTemp(applyParent, ".raft-snapshot-apply-*")
	if err != nil {
		_ = os.RemoveAll(tmpMain)
		return raftSnapshotScratchDirsV1{}, err
	}
	tmpSide, err := os.MkdirTemp(mainParent, ".raft-snapshot-side-*")
	if err != nil {
		_ = os.RemoveAll(tmpMain)
		_ = os.RemoveAll(tmpApply)
		return raftSnapshotScratchDirsV1{}, err
	}
	return raftSnapshotScratchDirsV1{
		main:  tmpMain,
		side:  tmpSide,
		apply: tmpApply,
	}, nil
}

func replaceSnapshotDirV1(dest, src string) error {
	if dest == "" || src == "" {
		return fmt.Errorf("raftfsm: empty snapshot restore path")
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
		return err
	}
	if err := os.RemoveAll(dest); err != nil {
		return err
	}
	return os.Rename(src, dest)
}

func replaceSnapshotMainDBV1(destMain, srcMain string) error {
	if destMain == "" || srcMain == "" {
		return fmt.Errorf("raftfsm: empty snapshot restore DB path")
	}
	if err := os.MkdirAll(filepath.Dir(destMain), 0o700); err != nil {
		return err
	}
	if err := os.RemoveAll(destMain); err != nil {
		return err
	}
	return os.Rename(srcMain, destMain)
}

func replaceSnapshotSideStoresV1(mainDir, srcSide string) error {
	root := snapshotSideStoreRootV1(mainDir)
	if root == "" || srcSide == "" {
		return fmt.Errorf("raftfsm: empty snapshot restore side-store path")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	for _, name := range raftSnapshotSideStoreEntriesV1 {
		src := filepath.Join(srcSide, name)
		dest := filepath.Join(root, name)
		if err := os.RemoveAll(dest); err != nil {
			return err
		}
		if _, err := os.Stat(src); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		if err := os.Rename(src, dest); err != nil {
			return err
		}
	}
	return nil
}

func snapshotSideStoreRootV1(mainDir string) string {
	mainDir = filepath.Clean(mainDir)
	if filepath.Base(mainDir) == "maindb" {
		return filepath.Dir(mainDir)
	}
	return mainDir
}
