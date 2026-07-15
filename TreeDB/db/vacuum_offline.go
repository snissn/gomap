package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/collectionwal"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

type vacuumFailpoint string

const (
	vacuumFailNone           vacuumFailpoint = ""
	vacuumFailAfterNewSync   vacuumFailpoint = "after_new_sync"
	vacuumFailAfterReady     vacuumFailpoint = "after_ready"
	vacuumFailAfterRenameOld vacuumFailpoint = "after_rename_old"
	vacuumFailAfterRenameNew vacuumFailpoint = "after_rename_new"
)

var errVacuumFailpoint = errors.New("vacuum failpoint")

// VacuumIndexOffline rewrites index.db into a fresh file and swaps it in.
//
// This is intended to reclaim space (reduce `index.db` chunk count) and restore
// locality after long churn. It is an offline operation (requires exclusive
// open lock).
func VacuumIndexOffline(opts Options) error {
	return vacuumIndexOffline(opts, vacuumFailNone)
}

func vacuumIndexOffline(opts Options, fail vacuumFailpoint) error {
	if opts.Dir == "" {
		return errors.New("db dir required")
	}
	if err := applyFormatConfigForMaintenanceWithOptions(&opts, true); err != nil {
		return err
	}
	if opts.ChunkSize == 0 {
		opts.ChunkSize = defaultChunkSize
	}
	opts.DisableBackgroundPrune = true
	opts.ReadOnly = true

	lock, err := lockfile.Acquire(filepath.Join(opts.Dir, "LOCK"))
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()

	if err := recoverIndexSwap(opts.Dir); err != nil {
		return err
	}
	if err := collectionwal.RequireCleanForOfflineMaintenance(opts.Dir); err != nil {
		return err
	}

	// Open the DB without acquiring a second lock (we already hold it).
	d, err := openReadOnlyNoLock(opts)
	if err != nil {
		return err
	}

	state := d.State()
	if state == nil {
		_ = d.Close()
		return fmt.Errorf("vacuum: missing db state")
	}
	acquiredValueLogSet := state.ValueLogSet
	if acquiredValueLogSet != nil {
		d.valueLogManager.Acquire(acquiredValueLogSet)
		defer func() {
			if acquiredValueLogSet != nil {
				_ = d.valueLogManager.Release(acquiredValueLogSet)
			}
		}()
	}

	indexPath := filepath.Join(opts.Dir, indexFileName)
	newPath := filepath.Join(opts.Dir, indexNewFileName)
	bakPath := filepath.Join(opts.Dir, indexBakFileName)
	readyPath := filepath.Join(opts.Dir, indexReadyFileName)

	// Clean up any previous partial artifacts (best-effort).
	if err := removePersistentFileBestEffort(opts.Dir, newPath, durabilitycut.ResourceIndex); err != nil {
		_ = d.Close()
		return err
	}
	if err := removePersistentFileBestEffort(opts.Dir, readyPath, durabilitycut.ResourceIndex); err != nil {
		_ = d.Close()
		return err
	}

	_, newStatErr := os.Stat(newPath)
	newCreated := os.IsNotExist(newStatErr)
	newPager, err := pager.Open(newPath, opts.ChunkSize)
	if err != nil {
		_ = d.Close()
		return err
	}
	if err := observeCreatedPersistentFile(opts.Dir, newPath, durabilitycut.ResourceIndex, newCreated); err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return err
	}

	if _, err := newPager.Alloc(2); err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return err
	}

	alloc := &pagerAllocator{p: newPager}
	reader := newValueReader(state.ValueLogSet)

	collectionRootReplacements, err := vacuumRewriteCollectionRoots(d.Pager(), reader, state.SystemRootPageID, alloc, newPager)
	if err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return err
	}

	effectiveInternalBaseDelta := opts.IndexInternalBaseDelta && !opts.IndexOuterLeavesInValueLog

	buildOpts := bulk.BuildOptions{
		LeafPrefixCompression: opts.LeafPrefixCompression,
		LeafColumnar:          opts.IndexColumnarLeaves,
		PackedValuePtr:        opts.IndexPackedValuePtr,
		InternalBaseDelta:     effectiveInternalBaseDelta,
	}
	sysRoot, err := vacuumBuildSystemRoot(d.Pager(), reader, state.SystemRootPageID, alloc, newPager, buildOpts, collectionRootReplacements)
	if err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return err
	}

	var userRoot uint64
	if opts.IndexOuterLeavesInValueLog {
		rootData, err := d.Pager().Get(state.RootPageID)
		if err != nil {
			_ = newPager.Close()
			_ = d.Close()
			return err
		}
		rootNode := node.NewNode(rootData)
		if rootNode.Type() == page.PageTypeLeaf {
			userRoot, err = vacuumClonePagerTreeWithLeafRefs(d.Pager(), state.RootPageID, alloc, newPager, effectiveInternalBaseDelta)
			if err != nil {
				_ = newPager.Close()
				_ = d.Close()
				return err
			}
		} else {
			allLeafRefs, err := vacuumTreeAllLeafRefsIfComplete(d.Pager(), state.RootPageID)
			if err != nil {
				_ = newPager.Close()
				_ = d.Close()
				return err
			}
			if allLeafRefs {
				userRoot, err = vacuumBuildInternalTreeFromLeafRefs(d.Pager(), state.RootPageID, newPager, alloc, effectiveInternalBaseDelta)
			} else {
				userRoot, err = vacuumClonePagerTreeWithLeafRefs(d.Pager(), state.RootPageID, alloc, newPager, effectiveInternalBaseDelta)
			}
			if err != nil {
				_ = newPager.Close()
				_ = d.Close()
				return err
			}
		}
	} else {
		userIter := tree.New(d.Pager(), reader, state.RootPageID).
			IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
		userRoot, err = bulk.BuildWithOptions(userIter, alloc, newPager, buildOpts)
		_ = userIter.Close()
		if err != nil {
			_ = newPager.Close()
			_ = d.Close()
			return err
		}
	}

	meta := d.meta
	meta.CommitSeq++
	meta.UserRootPageID = userRoot
	meta.SystemRootPageID = sysRoot
	meta.FreelistHeadID = 0
	meta.TotalPages = newPager.PageCount()

	durableResources, err := d.captureRebuiltIndexDurableResourcesV1(newPager, meta)
	if err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return err
	}
	defer durableResources.Release()
	outputHasLeafLogRefs, err := vacuumOutputHasLeafLogRefs(newPager, reader, userRoot, sysRoot)
	if err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return err
	}
	if !outputHasLeafLogRefs {
		if err := writeLeafGenerationResetPendingAfterOfflineVacuum(opts.Dir); err != nil {
			_ = newPager.Close()
			_ = d.Close()
			return err
		}
	}

	if err := writeRebuiltDurableRootV1(opts.Dir, newPath, newPager, meta, durableResources); err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return err
	}
	if fail == vacuumFailAfterNewSync {
		_ = newPager.Close()
		_ = d.Close()
		return errVacuumFailpoint
	}

	if err := writePersistentFile(opts.Dir, readyPath, []byte("ready\n"), 0o644, durabilitycut.ResourceIndex); err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return err
	}
	if runtime.GOOS != "windows" {
		if err := syncNewFileNamespaceDirectory(opts.Dir, durabilitycut.ResourceIndex); err != nil {
			_ = newPager.Close()
			_ = d.Close()
			return err
		}
	}
	if fail == vacuumFailAfterReady {
		_ = newPager.Close()
		_ = d.Close()
		return errVacuumFailpoint
	}

	if err := newPager.Close(); err != nil {
		_ = d.Close()
		return err
	}
	if acquiredValueLogSet != nil {
		releaseSet := acquiredValueLogSet
		acquiredValueLogSet = nil
		if err := d.valueLogManager.Release(releaseSet); err != nil {
			_ = d.Close()
			return err
		}
	}
	if err := d.Close(); err != nil {
		return err
	}

	// Swap in the new index file.
	if err := removePersistentFileBestEffort(opts.Dir, bakPath, durabilitycut.ResourceIndex); err != nil {
		return err
	}
	if _, err := renamePersistentFile(opts.Dir, indexPath, bakPath, durabilitycut.ResourceIndex); err != nil {
		return err
	}
	if fail == vacuumFailAfterRenameOld {
		return errVacuumFailpoint
	}
	if renamed, err := renamePersistentFile(opts.Dir, newPath, indexPath, durabilitycut.ResourceIndex); err != nil {
		if !renamed {
			_, rollbackErr := renamePersistentFile(opts.Dir, bakPath, indexPath, durabilitycut.ResourceIndex)
			return errors.Join(err, rollbackErr)
		}
		return err
	}
	if fail == vacuumFailAfterRenameNew {
		return errVacuumFailpoint
	}

	if err := removePersistentFileBestEffort(opts.Dir, readyPath, durabilitycut.ResourceIndex); err != nil {
		return err
	}
	if err := removePersistentFileBestEffort(opts.Dir, bakPath, durabilitycut.ResourceIndex); err != nil {
		return err
	}
	if runtime.GOOS != "windows" {
		if err := syncNewFileNamespaceDirectory(opts.Dir, durabilitycut.ResourceIndex); err != nil {
			return err
		}
	}

	if !outputHasLeafLogRefs {
		if err := resetLeafGenerationAfterOfflineVacuum(opts.Dir, meta.CommitSeq, nil); err != nil {
			return err
		}
		if err := removeLeafGenerationResetPendingAfterOfflineVacuum(opts.Dir); err != nil {
			return err
		}
	}

	return nil
}

func vacuumOutputHasLeafLogRefs(p *pager.Pager, reader tree.SlabReader, userRoot, systemRoot uint64) (bool, error) {
	for _, rootID := range []uint64{userRoot, systemRoot} {
		hasRefs, err := vacuumTreeHasLeafLogRefs(p, rootID)
		if err != nil || hasRefs {
			return hasRefs, err
		}
	}
	descriptors, err := vacuumCollectCollectionRootDescriptors(p, reader, systemRoot)
	if err != nil {
		return false, err
	}
	seen := make(map[uint64]struct{}, len(descriptors))
	for _, descriptor := range descriptors {
		if descriptor.rootID == 0 {
			continue
		}
		if _, ok := seen[descriptor.rootID]; ok {
			continue
		}
		seen[descriptor.rootID] = struct{}{}
		hasRefs, err := vacuumTreeHasLeafLogRefs(p, descriptor.rootID)
		if err != nil || hasRefs {
			return hasRefs, err
		}
	}
	return false, nil
}

func vacuumTreeHasLeafLogRefs(p *pager.Pager, rootID uint64) (bool, error) {
	if p == nil || rootID == 0 {
		return false, nil
	}
	var walk func(uint64) (bool, error)
	walk = func(id uint64) (bool, error) {
		data, err := p.Get(id)
		if err != nil {
			return false, err
		}
		n := node.NewNode(data)
		switch n.Type() {
		case page.PageTypeInternal:
			count := n.Count()
			for i := uint16(0); i < count; i++ {
				_, childRef, err := n.GetInternalEntryRefView(i)
				if err != nil {
					return false, err
				}
				if childRef.Kind == page.ChildRefLeafLog {
					return true, nil
				}
				if childRef.Kind != page.ChildRefPage {
					return false, fmt.Errorf("vacuum: unexpected child ref kind %d at page %d", childRef.Kind, id)
				}
				hasRefs, err := walk(childRef.Page)
				if err != nil || hasRefs {
					return hasRefs, err
				}
			}
			return false, nil
		case page.PageTypeLeaf:
			return false, nil
		default:
			return false, fmt.Errorf("vacuum: unexpected page type %d at page %d", n.Type(), id)
		}
	}
	return walk(rootID)
}

func resetLeafGenerationAfterOfflineVacuum(dir string, commitSeq uint64, evictSegment func(uint32) error) error {
	leafDir := LeafLogDirPath(dir)
	if leafDir == "" {
		return nil
	}
	if _, err := os.Stat(leafDir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("vacuum: stat leaf_vlog dir: %w", err)
	}

	files, err := listLeafGenerationBootstrapFiles(leafDir)
	if err != nil {
		return fmt.Errorf("vacuum: list leaf_vlog files: %w", err)
	}
	_, manifestExists, err := loadLeafGenerationManifest(leafDir)
	if err != nil {
		return fmt.Errorf("vacuum: load leaf generation manifest: %w", err)
	}
	sidecarPaths, err := filepath.Glob(filepath.Join(leafDir, "value-l*.log"+leafGenerationRecordLengthIndexSuffix))
	if err != nil {
		return fmt.Errorf("vacuum: list leaf generation record-length indexes: %w", err)
	}
	if len(files) == 0 && !manifestExists && len(sidecarPaths) == 0 {
		return nil
	}
	if err := saveLeafGenerationManifest(leafDir, newLeafGenerationManifest(commitSeq)); err != nil {
		return fmt.Errorf("vacuum: reset leaf generation manifest: %w", err)
	}
	if err := syncNewFileNamespaceDirectory(leafDir, durabilitycut.ResourceAuxiliary); err != nil {
		return fmt.Errorf("vacuum: sync leaf_vlog dir after manifest reset: %w", err)
	}

	for _, file := range files {
		if evictSegment != nil {
			if err := evictSegment(page.ValueLogFileID(file.rawFileID)); err != nil {
				return fmt.Errorf("vacuum: evict stale leaf generation file %s: %w", leafGenerationFallbackPath(dir, file.rawFileID), err)
			}
		}
		path := leafGenerationFallbackPath(dir, file.rawFileID)
		if _, err := removePersistentFile(leafDir, path, durabilitycut.ResourceOuterLeaf); err != nil {
			return fmt.Errorf("vacuum: remove stale leaf generation file %s: %w", path, err)
		}
		indexPath := leafGenerationRecordLengthIndexPath(dir, file.rawFileID)
		if _, err := removePersistentFile(leafDir, indexPath, durabilitycut.ResourceAuxiliary); err != nil {
			return fmt.Errorf("vacuum: remove stale leaf generation record-length index %s: %w", indexPath, err)
		}
	}
	for _, indexPath := range sidecarPaths {
		if _, err := removePersistentFile(leafDir, indexPath, durabilitycut.ResourceAuxiliary); err != nil {
			return fmt.Errorf("vacuum: remove stale leaf generation record-length index %s: %w", indexPath, err)
		}
	}
	if err := syncDeletionNamespaceDirectory(leafDir, durabilitycut.ResourceOuterLeaf); err != nil {
		return fmt.Errorf("vacuum: sync leaf_vlog dir after stale file removal: %w", err)
	}
	return nil
}

const leafGenerationOfflineVacuumResetPendingFileName = "offline-vacuum-reset.pending"

func leafGenerationResetPendingAfterOfflineVacuumPath(dir string) string {
	return filepath.Join(LeafLogDirPath(dir), leafGenerationOfflineVacuumResetPendingFileName)
}

func writeLeafGenerationResetPendingAfterOfflineVacuum(dir string) error {
	leafDir := LeafLogDirPath(dir)
	if err := os.MkdirAll(leafDir, 0o700); err != nil {
		return fmt.Errorf("vacuum: create leaf_vlog dir for reset marker: %w", err)
	}
	path := leafGenerationResetPendingAfterOfflineVacuumPath(dir)
	if err := writePersistentFile(leafDir, path, []byte("v1\n"), 0o600, durabilitycut.ResourceAuxiliary); err != nil {
		return fmt.Errorf("vacuum: write leaf_vlog reset marker: %w", err)
	}
	if err := syncNewFileNamespaceDirectory(leafDir, durabilitycut.ResourceAuxiliary); err != nil {
		return fmt.Errorf("vacuum: sync leaf_vlog dir after reset marker: %w", err)
	}
	return nil
}

func removeLeafGenerationResetPendingAfterOfflineVacuum(dir string) error {
	leafDir := LeafLogDirPath(dir)
	path := leafGenerationResetPendingAfterOfflineVacuumPath(dir)
	if _, err := removePersistentFile(leafDir, path, durabilitycut.ResourceAuxiliary); err != nil {
		return fmt.Errorf("vacuum: remove leaf_vlog reset marker: %w", err)
	}
	if err := syncDeletionNamespaceDirectory(leafDir, durabilitycut.ResourceAuxiliary); err != nil {
		return fmt.Errorf("vacuum: sync leaf_vlog dir after reset marker removal: %w", err)
	}
	return nil
}

func recoverLeafGenerationResetAfterOfflineVacuum(dir string, p *pager.Pager, reader tree.SlabReader, userRoot, systemRoot, commitSeq uint64, beforeReset func() error, evictSegment func(uint32) error) (bool, error) {
	markerPath := leafGenerationResetPendingAfterOfflineVacuumPath(dir)
	if _, err := os.Stat(markerPath); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("vacuum: stat leaf_vlog reset marker: %w", err)
	}
	hasLeafLogRefs, err := vacuumOutputHasLeafLogRefs(p, reader, userRoot, systemRoot)
	if err != nil {
		return false, err
	}
	if !hasLeafLogRefs {
		if beforeReset != nil {
			if err := beforeReset(); err != nil {
				return false, err
			}
		}
		if err := resetLeafGenerationAfterOfflineVacuum(dir, commitSeq, evictSegment); err != nil {
			return false, err
		}
		return true, removeLeafGenerationResetPendingAfterOfflineVacuum(dir)
	}
	return false, removeLeafGenerationResetPendingAfterOfflineVacuum(dir)
}
