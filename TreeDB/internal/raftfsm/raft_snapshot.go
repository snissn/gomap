package raftfsm

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	pathpkg "path"
	"path/filepath"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

const (
	raftSnapshotDBPrefixV1    = "db"
	raftSnapshotSidePrefixV1  = "side"
	raftSnapshotApplyPrefixV1 = "apply"

	raftSnapshotFormatConfigFileV1 = "format.json"
	raftSnapshotStagingDirNameV1   = "staged"
	raftSnapshotArchiveGlobV1      = "treedb-snapshot-*.tar"
)

// raftSnapshotAfterExtractForTest blocks a restore after archive extraction
// while it still owns the storage barrier. It is test-only and makes the
// barrier-before-FSM-lock ordering regression deterministic.
var raftSnapshotAfterExtractForTest func()

// raftSnapshotBeforeOpenForTest makes the discovery/open boundary observable
// to deterministic no-follow regression tests.
var raftSnapshotBeforeOpenForTest func(string)

// raftSnapshotBeforeVectorPartitionRootOpenForTest makes the vector-partition
// root retained-open/path-revalidation boundary observable to deterministic
// replacement tests.
var raftSnapshotBeforeVectorPartitionRootOpenForTest func(string)

var raftSnapshotMainDBEntriesV1 = []string{
	"index.db",
	raftSnapshotFormatConfigFileV1,
	"wal",
	"value_vlog",
	"leaf_vlog",
	"column_assets",
	// M1 vector-partition manifests are durable derived state. Omitting this
	// namespace would make a restored ready generation silently partial.
	"vector_partitions",
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
	var snapshot raftcluster.RaftSnapshotV1
	err := collections.WithVectorPartitionStorageBarrierV1(raftcluster.MainDBDir(f.cluster.Dir), func() error {
		// All snapshot operations take the root barrier before f.mu. Install
		// extracts under the barrier and then takes f.mu, so reversing this
		// order here can deadlock export behind an in-flight install.
		f.mu.Lock()
		defer f.mu.Unlock()
		var inner error
		snapshot, inner = f.exportRaftSnapshotV1Locked()
		return inner
	})
	return snapshot, err
}

func (f *FSM) exportRaftSnapshotV1Locked() (raftcluster.RaftSnapshotV1, error) {
	if err := f.requireRaftSnapshotOpenV1(); err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	if err := f.db.Checkpoint(); err != nil {
		return raftcluster.RaftSnapshotV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "checkpoint before snapshot export: %v", err)
	}
	if err := f.syncDurableApplyMetadataForRaftSnapshotV1(); err != nil {
		return raftcluster.RaftSnapshotV1{}, err
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

	archiveFile, archivePath, err := createRaftSnapshotArchiveFileV1(f.cluster.Layout.SnapshotDir)
	if err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	keepArchive := false
	defer func() {
		if !keepArchive {
			_ = os.Remove(archivePath)
		}
	}()
	tw := tar.NewWriter(archiveFile)
	if err := writeRaftSnapshotFileV1(tw, raftcluster.RaftSnapshotArchiveManifestPathV1, headerBytes, 0o600); err != nil {
		_ = archiveFile.Close()
		return raftcluster.RaftSnapshotV1{}, err
	}
	if err := appendRaftSnapshotTreeDBStorageV1(tw, raftSnapshotDBPrefixV1, raftcluster.MainDBDir(f.cluster.Dir)); err != nil {
		_ = archiveFile.Close()
		return raftcluster.RaftSnapshotV1{}, err
	}
	if !f.cluster.DisableSideStores {
		if err := appendRaftSnapshotTreeDBSideStoresV1(tw, raftSnapshotSidePrefixV1, raftcluster.MainDBDir(f.cluster.Dir)); err != nil {
			_ = archiveFile.Close()
			return raftcluster.RaftSnapshotV1{}, err
		}
	}
	if err := appendRaftSnapshotDirV1(tw, raftSnapshotApplyPrefixV1, f.cluster.Layout.ApplyDir); err != nil {
		_ = archiveFile.Close()
		return raftcluster.RaftSnapshotV1{}, err
	}
	if err := tw.Close(); err != nil {
		_ = archiveFile.Close()
		return raftcluster.RaftSnapshotV1{}, fmt.Errorf("raftfsm: close snapshot archive: %w", err)
	}
	if err := archiveFile.Sync(); err != nil {
		_ = archiveFile.Close()
		return raftcluster.RaftSnapshotV1{}, fmt.Errorf("raftfsm: sync snapshot archive: %w", err)
	}
	if err := archiveFile.Close(); err != nil {
		return raftcluster.RaftSnapshotV1{}, fmt.Errorf("raftfsm: close snapshot archive file: %w", err)
	}
	snapshot := raftcluster.RaftSnapshotV1{
		Manifest:    manifest,
		ArchivePath: archivePath,
	}
	if err := snapshot.Validate(); err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	keepArchive = true
	return snapshot, nil
}

func createRaftSnapshotArchiveFileV1(snapshotDir string) (*os.File, string, error) {
	if strings.TrimSpace(snapshotDir) == "" {
		return nil, "", fmt.Errorf("raftfsm: missing snapshot directory (SnapshotDir is empty)")
	}
	stagingDir := raftSnapshotStagingDirV1(snapshotDir)
	if err := os.MkdirAll(stagingDir, 0o700); err != nil {
		return nil, "", fmt.Errorf("raftfsm: create snapshot staging directory: %w", err)
	}
	file, err := os.CreateTemp(stagingDir, raftSnapshotArchiveGlobV1)
	if err != nil {
		return nil, "", fmt.Errorf("raftfsm: create snapshot archive: %w", err)
	}
	return file, file.Name(), nil
}

func cleanupAbandonedRaftSnapshotArchivesV1(snapshotDir string) error {
	if strings.TrimSpace(snapshotDir) == "" {
		return nil
	}
	stagingDir := raftSnapshotStagingDirV1(snapshotDir)
	info, err := os.Stat(stagingDir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("raftfsm: stat snapshot staging directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("raftfsm: snapshot staging path %q is not a directory", stagingDir)
	}
	entries, err := os.ReadDir(stagingDir)
	if err != nil {
		return fmt.Errorf("raftfsm: read snapshot staging directory: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		matches, err := filepath.Match(raftSnapshotArchiveGlobV1, entry.Name())
		if err != nil {
			return fmt.Errorf("raftfsm: match snapshot archive pattern: %w", err)
		}
		if !matches {
			continue
		}
		path := filepath.Join(stagingDir, entry.Name())
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("raftfsm: remove abandoned staged snapshot archive %q: %w", path, err)
		}
	}
	return nil
}

func raftSnapshotStagingDirV1(snapshotDir string) string {
	return filepath.Join(snapshotDir, raftSnapshotStagingDirNameV1)
}

// InstallRaftSnapshotV1 discards the local TreeDB state and installs a
// production snapshot archive. HashiCorp Raft calls Restore without concurrent
// Apply calls, so the FSM can safely close and reopen its local stores here.
// Any caller-owned DB handle passed to Open is closed by a successful install
// and must be recreated before direct use.
func (f *FSM) InstallRaftSnapshotV1(reader io.Reader) error {
	if f == nil {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	return collections.WithVectorPartitionStorageBarrierV1(raftcluster.MainDBDir(f.cluster.Dir), func() error { return f.installRaftSnapshotV1Locked(reader) })
}
func (f *FSM) installRaftSnapshotV1Locked(reader io.Reader) error {
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
	if !rootpublication.StableRelativeNamespaceSupported() {
		f.mu.RUnlock()
		return codedError(
			raftentry.ErrorUnsafeDurabilityModeV1,
			"%w: Raft snapshot install requires durable rename and removal namespaces",
			rootpublication.ErrNamespacePersistenceUnsupported,
		)
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
	if hook := raftSnapshotAfterExtractForTest; hook != nil {
		hook()
	}
	if err := collections.ValidateVectorPartitionSnapshotNamespaceV1(tmpMain); err != nil {
		return codedError(raftentry.ErrorRejectedConflictV1, "validate extracted vector partition lifecycle snapshot: %v", err)
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
	if err := rebindExtractedRaftSnapshotDurableRootsV1(tmpMain, tmpSide, f.cluster.DisableSideStores); err != nil {
		return codedError(raftentry.ErrorRejectedConflictV1, "rebind extracted snapshot durable roots: %v", err)
	}
	if err := f.verifyExtractedRaftSnapshotV1(header.Manifest, tmpMain, tmpSide, tmpApply); err != nil {
		return err
	}
	if err := f.closeForRaftSnapshotRestoreV1(); err != nil {
		return err
	}
	if err := replaceSnapshotMainDBV1(mainDir, tmpMain, f.cluster.Layout.RootDir); err != nil {
		return err
	}
	cleanupMain = false
	if !f.cluster.DisableSideStores {
		if err := replaceSnapshotSideStoresV1(mainDir, tmpSide); err != nil {
			return err
		}
	}
	// Entry-wise installs can change parent directory identities even though
	// the dependency files themselves retain their handles across rename. Rebind
	// once more in the final layout before any restored meta is reopened.
	if err := rebindExtractedRaftSnapshotDurableRootsV1(mainDir, snapshotSideStoreRootV1(mainDir), f.cluster.DisableSideStores); err != nil {
		return codedError(raftentry.ErrorRejectedConflictV1, "rebind installed snapshot durable roots: %v", err)
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

func rebindExtractedRaftSnapshotDurableRootsV1(mainDir, sideDir string, disableSideStores bool) error {
	if !disableSideStores {
		// Rebinding a side store atomically replaces its index file. Do that
		// first so the main manifest captures the side store's final identity.
		for _, name := range raftSnapshotSideStoreEntriesV1 {
			dir := filepath.Join(sideDir, name)
			if _, err := os.Stat(filepath.Join(dir, "index.db")); err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return err
			}
			if err := backenddb.RebindDurableRootSnapshotV1(dir); err != nil {
				return fmt.Errorf("side store %s: %w", name, err)
			}
		}
	}
	return backenddb.RebindDurableRootSnapshotLayoutV1(mainDir, sideDir)
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

func (f *FSM) syncDurableApplyMetadataForRaftSnapshotV1() error {
	if f == nil || f.progress == nil || f.results == nil {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM has no durable apply metadata")
	}
	return errors.Join(f.progress.Sync(), f.results.Sync())
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
	mainDir := raftcluster.MainDBDir(f.cluster.Dir)
	opts := f.snapshotRestoreDBOptionsV1(mainDir)
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

func (f *FSM) verifyExtractedRaftSnapshotV1(manifest raftcluster.SnapshotManifestV1, mainDir, sideDir, applyDir string) error {
	opts := f.snapshotRestoreDBOptionsV1(mainDir)
	sideDBs, err := wireRaftSnapshotSideStoreLookupsV1(sideDir, &opts)
	if err != nil {
		return err
	}
	db, err := backenddb.Open(opts)
	if err != nil {
		_ = sideDBs()
		return err
	}
	progress, err := raftapply.OpenDurableApplyProgressStore(applyDir, f.storeOptions)
	if err != nil {
		_ = errors.Join(db.Close(), sideDBs())
		return err
	}
	results, err := raftapply.OpenDurableApplyResultStore(applyDir, f.storeOptions)
	if err != nil {
		_ = errors.Join(progress.Close(), db.Close(), sideDBs())
		return err
	}
	verifyFSM := &FSM{
		db:           db,
		progress:     progress,
		results:      results,
		cluster:      f.cluster,
		scopeRule:    f.scopeRule,
		database:     f.database,
		catalog:      f.catalog,
		storeOptions: f.storeOptions,
		restoreDB:    f.restoreDB,
	}
	verifyErr := verifyFSM.verifyInstalledSnapshotManifestV1Locked(manifest)
	closeErr := errors.Join(progress.Close(), results.Close(), db.Close(), sideDBs())
	if verifyErr != nil {
		return verifyErr
	}
	return closeErr
}

func (f *FSM) snapshotRestoreDBOptionsV1(dir string) backenddb.Options {
	opts := f.restoreDB
	opts.Dir = dir
	opts.CommandWAL = true
	opts.CommandWALStatsScan = true
	if opts.CommandWALSegmentTargetBytes <= 0 {
		opts.CommandWALSegmentTargetBytes = 1 << 20
	}
	opts.DisableBackgroundPrune = true
	if f.cluster.DisableSideStores {
		opts.DisableSideStores = true
	}
	return opts
}

func appendRaftSnapshotDirV1(tw *tar.Writer, prefix, root string) error {
	if tw == nil {
		return fmt.Errorf("raftfsm: nil snapshot archive writer")
	}
	root = filepath.Clean(root)
	info, err := os.Lstat(root)
	if err != nil {
		return fmt.Errorf("raftfsm: stat snapshot directory %q: %w", root, err)
	}
	if info.Mode()&fs.ModeSymlink != 0 {
		return fmt.Errorf("raftfsm: snapshot path %q is a symlink", root)
	}
	if !info.IsDir() {
		return fmt.Errorf("raftfsm: snapshot path %q is not a directory", root)
	}
	return appendRaftSnapshotDirWithRootInfoV1(tw, prefix, root, info)
}

func appendRaftSnapshotDirWithRootInfoV1(tw *tar.Writer, prefix, root string, expectedRoot fs.FileInfo) error {
	if tw == nil {
		return fmt.Errorf("raftfsm: nil snapshot archive writer")
	}
	if expectedRoot == nil {
		return fmt.Errorf("raftfsm: nil snapshot directory identity for %q", root)
	}
	if strings.HasSuffix(prefix, "/vector_partitions") {
		return appendRaftSnapshotVectorPartitionDirV1(tw, prefix, root, expectedRoot)
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
			if raftSnapshotVectorPartitionLifecycleEntryV1(prefix, filepath.ToSlash(rel)) {
				return fmt.Errorf("raftfsm: vector partition lifecycle path %q is not a regular file", filePath)
			}
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
		copyErr := copyRaftSnapshotFileContentV1(tw, file, header.Size)
		closeErr := file.Close()
		return errors.Join(copyErr, closeErr)
	})
}

// appendRaftSnapshotVectorPartitionDirV1 keeps discovery and child opens on
// one exact retained directory handle. The immutable lifecycle namespace is
// flat by contract.
func appendRaftSnapshotVectorPartitionDirV1(tw *tar.Writer, prefix, root string, expectedRoot fs.FileInfo) error {
	dir, err := rootpublication.OpenStableParent(root)
	if err != nil {
		return err
	}
	defer dir.Close()
	openedRoot, err := dir.Stat()
	if err != nil {
		return err
	}
	if !openedRoot.IsDir() || !os.SameFile(expectedRoot, openedRoot) {
		return fmt.Errorf("raftfsm: vector partition snapshot root %q changed while opening", root)
	}
	retainedIdentity, err := rootpublication.StableIdentityFromFile(dir)
	if err != nil {
		return fmt.Errorf("raftfsm: capture vector partition snapshot root %q identity: %w", root, err)
	}
	if raftSnapshotBeforeVectorPartitionRootOpenForTest != nil {
		raftSnapshotBeforeVectorPartitionRootOpenForTest(root)
	}
	current, err := rootpublication.OpenStableParent(root)
	if err != nil {
		return fmt.Errorf("raftfsm: reopen vector partition snapshot root %q: %w", root, err)
	}
	currentIdentity, identityErr := rootpublication.StableIdentityFromFile(current)
	closeErr := current.Close()
	if err := errors.Join(identityErr, closeErr); err != nil {
		return fmt.Errorf("raftfsm: revalidate vector partition snapshot root %q identity: %w", root, err)
	}
	if !rootpublication.SamePhysicalIdentity(retainedIdentity, currentIdentity) {
		return fmt.Errorf("raftfsm: vector partition snapshot root %q changed after retained open", root)
	}
	if err := writeRaftSnapshotDirHeaderV1(tw, prefix); err != nil {
		return err
	}
	entries, err := collections.VectorPartitionSnapshotEntriesV1(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if raftSnapshotBeforeOpenForTest != nil {
			raftSnapshotBeforeOpenForTest(filepath.Join(root, entry.Name))
		}
		file, err := rootpublication.OpenStableChildFile(dir, entry.Name, os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		exactInfo, statErr := file.Stat()
		exactIdentity, identityErr := rootpublication.StableIdentityFromFile(file)
		if statErr != nil || identityErr != nil || !exactInfo.Mode().IsRegular() || exactInfo.Size() < 0 ||
			uint64(exactInfo.Size()) != entry.Bytes ||
			!rootpublication.SamePhysicalIdentity(exactIdentity, entry.Identity) {
			_ = file.Close()
			if statErr != nil {
				return statErr
			}
			if identityErr != nil {
				return identityErr
			}
			return fmt.Errorf("raftfsm: vector partition snapshot entry %q changed while opening", entry.Name)
		}
		if err := rootpublication.ValidateStableChildLink(dir, file, entry.Name); err != nil {
			_ = file.Close()
			return err
		}
		header := &tar.Header{Name: pathpkg.Join(prefix, entry.Name), Mode: int64(exactInfo.Mode().Perm()), Size: exactInfo.Size(), ModTime: exactInfo.ModTime()}
		if err := tw.WriteHeader(header); err != nil {
			_ = file.Close()
			return err
		}
		copyErr := copyRaftSnapshotFileContentV1(tw, file, header.Size)
		validateErr := rootpublication.ValidateStableChildLink(dir, file, entry.Name)
		closeErr := file.Close()
		if err := errors.Join(copyErr, validateErr, closeErr); err != nil {
			return err
		}
	}
	return nil
}

func raftSnapshotVectorPartitionLifecycleEntryV1(prefix, rel string) bool {
	parts := strings.Split(pathpkg.Join(prefix, rel), "/")
	for i, part := range parts {
		if part == "vector_partitions" && i+2 == len(parts) {
			name := parts[i+1]
			return strings.HasSuffix(name, ".vpm") || strings.HasSuffix(name, ".active") || strings.HasSuffix(name, ".retired") || strings.HasSuffix(name, ".inactive") || strings.HasSuffix(name, ".deleting")
		}
	}
	return false
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
		if name == "vector_partitions" {
			if _, err := os.Lstat(src); os.IsNotExist(err) {
				if err := writeRaftSnapshotDirHeaderV1(tw, pathpkg.Join(prefix, name)); err != nil {
					return err
				}
				continue
			} else if err != nil {
				return err
			}
		}
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
		return appendRaftSnapshotDirWithRootInfoV1(tw, archiveName, filepath.Clean(src), info)
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
	copyErr := copyRaftSnapshotFileContentV1(tw, file, info.Size())
	closeErr := file.Close()
	return errors.Join(copyErr, closeErr)
}

func copyRaftSnapshotFileContentV1(dst io.Writer, src io.Reader, size int64) error {
	if size < 0 {
		return fmt.Errorf("raftfsm: negative snapshot file size %d", size)
	}
	copied, err := io.CopyN(dst, src, size)
	if errors.Is(err, io.EOF) {
		return fmt.Errorf("raftfsm: snapshot file changed while exporting: copied %d of %d bytes", copied, size)
	}
	return err
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
			if sawHeader {
				return raftcluster.RaftSnapshotArchiveHeaderV1{}, fmt.Errorf("raftfsm: duplicate snapshot archive header")
			}
			if tarHeader.Typeflag != tar.TypeReg && tarHeader.Typeflag != tar.TypeRegA {
				return raftcluster.RaftSnapshotArchiveHeaderV1{}, fmt.Errorf("raftfsm: snapshot archive header is not a regular file")
			}
			raw, err := readRaftSnapshotArchiveHeaderBytesV1(tr, tarHeader.Size)
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
	if err := syncExtractedRaftSnapshotRootsV1(mainDir, sideDir, applyDir); err != nil {
		return raftcluster.RaftSnapshotArchiveHeaderV1{}, err
	}
	return header, nil
}

func readRaftSnapshotArchiveHeaderBytesV1(reader io.Reader, size int64) ([]byte, error) {
	if size > raftcluster.RaftSnapshotArchiveHeaderMaxBytes {
		return nil, fmt.Errorf("raftfsm: snapshot archive header is %d bytes, max %d", size, raftcluster.RaftSnapshotArchiveHeaderMaxBytes)
	}
	raw, err := io.ReadAll(io.LimitReader(reader, raftcluster.RaftSnapshotArchiveHeaderMaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("raftfsm: read snapshot archive header: %w", err)
	}
	if len(raw) > raftcluster.RaftSnapshotArchiveHeaderMaxBytes {
		return nil, fmt.Errorf("raftfsm: snapshot archive header exceeds %d bytes", raftcluster.RaftSnapshotArchiveHeaderMaxBytes)
	}
	return raw, nil
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
		syncErr := error(nil)
		if copyErr == nil {
			syncErr = rootpublication.SyncStableFile(file)
		}
		closeErr := file.Close()
		return true, errors.Join(copyErr, syncErr, closeErr)
	default:
		return false, fmt.Errorf("raftfsm: unsupported snapshot archive entry type %d for %q", header.Typeflag, header.Name)
	}
}

func syncExtractedRaftSnapshotRootsV1(roots ...string) error {
	for _, root := range roots {
		if root == "" {
			continue
		}
		directories := make([]string, 0, 16)
		if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				directories = append(directories, path)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("raftfsm: inventory extracted snapshot namespaces: %w", err)
		}
		// Children must become durable before the name that links them from their
		// parent. WalkDir is parent-first, so synchronize in reverse order.
		for index := len(directories) - 1; index >= 0; index-- {
			dir, err := os.Open(directories[index])
			if err != nil {
				return fmt.Errorf("raftfsm: open extracted snapshot namespace %q: %w", directories[index], err)
			}
			syncErr := rootpublication.SyncStableNamespace(dir)
			closeErr := dir.Close()
			if syncErr != nil || closeErr != nil {
				return fmt.Errorf("raftfsm: sync extracted snapshot namespace %q: %w", directories[index], errors.Join(syncErr, closeErr))
			}
		}
	}
	return nil
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
	if err := os.Rename(src, dest); err != nil {
		return err
	}
	return syncRaftSnapshotRenameParentsV1(src, dest)
}

func replaceSnapshotMainDBV1(destMain, srcMain, preserveRoot string) error {
	if destMain == "" || srcMain == "" {
		return fmt.Errorf("raftfsm: empty snapshot restore DB path")
	}
	if err := os.MkdirAll(filepath.Dir(destMain), 0o700); err != nil {
		return err
	}
	if preserveRoot != "" && sameOrUnderSnapshotDirV1(destMain, preserveRoot) {
		return replaceSnapshotMainDBEntriesV1(destMain, srcMain, preserveRoot)
	}
	if err := os.RemoveAll(destMain); err != nil {
		return err
	}
	if err := os.Rename(srcMain, destMain); err != nil {
		return err
	}
	return syncRaftSnapshotRenameParentsV1(srcMain, destMain)
}

func replaceSnapshotMainDBEntriesV1(destMain, srcMain, preserveRoot string) error {
	if err := os.MkdirAll(destMain, 0o700); err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(srcMain) }()
	preserveName, hasPreserveName := snapshotTopLevelPreserveNameV1(destMain, preserveRoot)
	entries, err := os.ReadDir(destMain)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else {
		for _, entry := range entries {
			if hasPreserveName && entry.Name() == preserveName {
				continue
			}
			if err := os.RemoveAll(filepath.Join(destMain, entry.Name())); err != nil {
				return err
			}
		}
	}
	for _, name := range raftSnapshotMainDBEntriesV1 {
		src := filepath.Join(srcMain, name)
		dest := filepath.Join(destMain, name)
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
		if err := syncRaftSnapshotRenameParentsV1(src, dest); err != nil {
			return err
		}
	}
	return nil
}

func snapshotTopLevelPreserveNameV1(parent, preserveRoot string) (string, bool) {
	rel, err := filepath.Rel(filepath.Clean(parent), filepath.Clean(preserveRoot))
	if err != nil || rel == "." || rel == "" || rel == ".." || filepath.IsAbs(rel) || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", false
	}
	if i := strings.IndexRune(rel, os.PathSeparator); i >= 0 {
		rel = rel[:i]
	}
	if rel == "" || rel == "." || rel == ".." {
		return "", false
	}
	return rel, true
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
		if err := syncRaftSnapshotRenameParentsV1(src, dest); err != nil {
			return err
		}
	}
	return nil
}

func syncRaftSnapshotRenameParentsV1(src, dest string) error {
	parents := []string{filepath.Dir(src)}
	if destParent := filepath.Dir(dest); filepath.Clean(destParent) != filepath.Clean(parents[0]) {
		parents = append(parents, destParent)
	}
	for _, parent := range parents {
		dir, err := os.Open(parent)
		if err != nil {
			return fmt.Errorf("raftfsm: open snapshot rename parent %q: %w", parent, err)
		}
		syncErr := rootpublication.SyncStableNamespace(dir)
		closeErr := dir.Close()
		if syncErr != nil || closeErr != nil {
			return fmt.Errorf("raftfsm: sync snapshot rename parent %q: %w", parent, errors.Join(syncErr, closeErr))
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
