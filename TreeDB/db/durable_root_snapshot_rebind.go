package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

// RebindDurableRootSnapshotV1 rewrites the two bounded durable-root manifests
// in an extracted snapshot so their physical identities name the extracted
// files, not the source replica's files. It is intentionally an explicit
// restore operation: ordinary Open continues to reject copied or recreated
// dependencies whose exact identities differ from the published manifests.
//
// Both independently recoverable slot generations are rebound in a stable
// sibling copy that is atomically installed only after its metas are durable.
// Their commit sequences, roots, allocator generations, and logical dependency
// frontiers remain unchanged. Dictionary namespace epochs are
// derived from restored parent handles, matching fresh destination authority.
func RebindDurableRootSnapshotV1(dir string) error {
	return RebindDurableRootSnapshotLayoutV1(dir, "")
}

// RebindDurableRootSnapshotLayoutV1 is the staged-layout variant used by
// snapshot restore before the extracted main and side-store trees receive
// their final names. sideRoot contains dictdb/ and templatedb/ when non-empty.
func RebindDurableRootSnapshotLayoutV1(dir, sideRoot string) error {
	return RebindDurableRootSnapshotLayoutWithContextV1(context.Background(), dir, sideRoot)
}

// RebindDurableRootSnapshotLayoutWithContextV1 cancels between bounded copy
// chunks, dependency records and index page operations. It mutates only a private
// sibling and preserves the original staged extent after closing its mappings.
func RebindDurableRootSnapshotLayoutWithContextV1(ctx context.Context, dir, sideRoot string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if dir == "" {
		return errors.New("treedb: durable-root snapshot rebind directory is empty")
	}
	if !rootpublication.StableRelativeNamespaceSupported() {
		return fmt.Errorf("%w: durable-root snapshot rebind requires durable rename and removal namespaces", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	indexPath := filepath.Join(dir, indexFileName)
	source, err := os.Open(indexPath)
	if err != nil {
		return fmt.Errorf("treedb: open snapshot index for durable-root rebind copy: %w", err)
	}
	info, statErr := source.Stat()
	if statErr != nil {
		_ = source.Close()
		return fmt.Errorf("treedb: stat snapshot index for durable-root rebind copy: %w", statErr)
	}
	temporary, err := os.CreateTemp(dir, ".durable-root-rebind-*")
	if err != nil {
		_ = source.Close()
		return fmt.Errorf("treedb: create snapshot index durable-root rebind copy: %w", err)
	}
	temporaryPath := temporary.Name()
	cleanupTemporary := true
	defer func() {
		if cleanupTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	copyErr := temporary.Chmod(info.Mode().Perm())
	if copyErr == nil {
		var buffer [64 * 1024]byte
		for offset := int64(0); offset < info.Size(); {
			if copyErr = ctx.Err(); copyErr != nil {
				break
			}
			n := min(int64(len(buffer)), info.Size()-offset)
			if _, copyErr = source.ReadAt(buffer[:n], offset); copyErr != nil {
				break
			}
			var written int
			written, copyErr = temporary.Write(buffer[:n])
			if copyErr != nil {
				break
			}
			if int64(written) != n {
				copyErr = io.ErrShortWrite
				break
			}
			offset += n
		}
	}
	if copyErr == nil {
		copyErr = rootpublication.SyncStableFile(temporary)
	}
	copyErr = errors.Join(copyErr, temporary.Close(), source.Close())
	if copyErr != nil {
		return fmt.Errorf("treedb: create stable snapshot index durable-root rebind copy: %w", copyErr)
	}
	if err := rebindDurableRootSnapshotFileWithContextV1(ctx, dir, sideRoot, temporaryPath); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Writable pager opening rounds its private mmap backing up to chunk size.
	// Rebinding replaces fixed-width fields only; it does not allocate pages.
	// Drop that padding after every mapping closes, before installing the copy.
	rebound, err := os.OpenFile(temporaryPath, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	reboundInfo, extentErr := rebound.Stat()
	if extentErr == nil && reboundInfo.Size() < info.Size() {
		extentErr = errors.New("treedb: rebound snapshot unexpectedly shrank")
	}
	if extentErr == nil {
		extentErr = rebound.Truncate(info.Size())
	}
	if extentErr == nil {
		extentErr = rootpublication.SyncStableFile(rebound)
	}
	if err := errors.Join(extentErr, rebound.Close()); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, indexPath); err != nil {
		return fmt.Errorf("treedb: install rebound snapshot index: %w", err)
	}
	cleanupTemporary = false
	parent, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("treedb: open rebound snapshot index namespace: %w", err)
	}
	syncErr := rootpublication.SyncStableNamespace(parent)
	closeErr := parent.Close()
	if syncErr != nil || closeErr != nil {
		return fmt.Errorf("treedb: persist rebound snapshot index namespace: %w", errors.Join(syncErr, closeErr))
	}
	return nil
}

func rebindDurableRootSnapshotFileWithContextV1(ctx context.Context, dir, sideRoot, indexPath string) error {
	file, err := os.OpenFile(indexPath, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("treedb: open snapshot index for durable-root rebind: %w", err)
	}
	store, err := newSnapshotIndexPageStoreV1(file)
	if err != nil {
		_ = file.Close()
		return err
	}
	store.ctx = ctx
	indexPager, err := pager.Open(indexPath, 64<<20)
	if err != nil {
		return errors.Join(err, file.Close())
	}
	closeWith := func(operationErr error) error {
		return errors.Join(operationErr, indexPager.Close(), file.Close())
	}

	selected, err := selectDurableRootV1(store, store.pageCount, nil, dependencyDirectoryStructureValidatorWithContextV2(ctx, indexPager))
	if ctxErr := ctx.Err(); ctxErr != nil {
		return closeWith(ctxErr)
	}
	if err != nil {
		return closeWith(fmt.Errorf("treedb: select snapshot durable roots for identity rebind: %w", err))
	}

	type slotRebindV1 struct {
		slot     uint64
		meta     page.DurableMetaV1
		record   rootpublication.DurableRootRecordV1
		manifest *rootpublication.DependencyManifestV1
	}
	plans := make([]slotRebindV1, 0, 2)
	for slot := uint64(0); slot < 2; slot++ {
		if selected.SlotCommits[slot] == 0 {
			continue
		}
		record := selected.SlotRecords[slot]
		if record.Directory.RootPageID != 0 {
			plans = append(plans, slotRebindV1{slot: slot, meta: selected.SlotMetas[slot], record: record})
			continue
		}
		manifest, err := rootpublication.LoadDependencyManifestV1(store, record.Manifest)
		if err != nil {
			return closeWith(fmt.Errorf("treedb: load snapshot dependency manifest for slot %d: %w", slot, err))
		}
		entries := manifest.Entries()
		for index := range entries {
			if err := ctx.Err(); err != nil {
				return closeWith(err)
			}
			if err := rebindSnapshotManifestEntryV1(dir, sideRoot, &entries[index]); err != nil {
				return closeWith(fmt.Errorf("treedb: rebind snapshot dependency for slot %d: %w", slot, err))
			}
		}
		rebound, err := rootpublication.NewDependencyManifestV1(entries)
		if err != nil {
			return closeWith(fmt.Errorf("treedb: encode rebound snapshot dependency manifest for slot %d: %w", slot, err))
		}
		if rebound.PageCount() > record.Manifest.PageCount {
			return closeWith(fmt.Errorf("treedb: rebound snapshot dependency manifest for slot %d needs %d pages, reserved %d", slot, rebound.PageCount(), record.Manifest.PageCount))
		}
		plans = append(plans, slotRebindV1{slot: slot, meta: selected.SlotMetas[slot], record: record, manifest: rebound})
	}
	if len(plans) == 0 {
		return closeWith(errors.New("treedb: snapshot has no independently recoverable durable-root slot"))
	}

	// Root-record lineage points backward, so rewrite oldest to newest and carry
	// the rebound parent digest into any newer selected record.
	sort.Slice(plans, func(i, j int) bool { return plans[i].record.CommitSeq < plans[j].record.CommitSeq })
	reboundRecordDigests := make(map[uint64][32]byte, len(plans))
	for index := range plans {
		plan := &plans[index]
		if plan.record.Directory.RootPageID != 0 {
			if err := rebindSnapshotDependencyDirectoryWithContextV2(ctx, dir, sideRoot, indexPager, plan.record); err != nil {
				return closeWith(fmt.Errorf("treedb: rebind snapshot directory for slot %d: %w", plan.slot, err))
			}
		} else {
			manifestRef, err := plan.manifest.Materialize(plan.record.Manifest.FirstPageID, store)
			if err != nil {
				return closeWith(fmt.Errorf("treedb: materialize rebound snapshot dependency manifest for slot %d: %w", plan.slot, err))
			}
			plan.record.Manifest = manifestRef
		}
		if digest, ok := reboundRecordDigests[plan.record.ParentRecordPageID]; ok {
			plan.record.ParentRecordDigest = digest
		}
		recordImage, recordDigest, err := plan.record.EncodePage(plan.meta.RootRecordPageID)
		if err != nil {
			return closeWith(fmt.Errorf("treedb: encode rebound snapshot root record for slot %d: %w", plan.slot, err))
		}
		if err := store.WritePage(plan.meta.RootRecordPageID, recordImage); err != nil {
			return closeWith(fmt.Errorf("treedb: write rebound snapshot root record for slot %d: %w", plan.slot, err))
		}
		reboundRecordDigests[plan.meta.RootRecordPageID] = recordDigest
		plan.meta.RootRecordDigest = recordDigest
	}
	if err := indexPager.Sync(); err != nil {
		return closeWith(err)
	}
	// Publish each rebound slot through the sole durable-meta transaction. All
	// dependency identities and both slots' index pages were materialized above;
	// every recovery-selectable meta therefore follows a physical index barrier.
	for index := range plans {
		_, err := executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{
			syncIndex: func() error { return rootpublication.SyncStableFile(file) },
			sink:      store,
			target:    plans[index].slot,
			meta:      plans[index].meta,
			syncMeta:  func() error { return rootpublication.SyncStableFile(file) },
			dir:       dir,
			indexPath: indexPath,
		})
		if err != nil {
			return closeWith(fmt.Errorf("treedb: publish rebound snapshot meta slot %d: %w", plans[index].slot, err))
		}
	}
	rebound, err := selectDurableRootV1(store, store.pageCount, nil, dependencyDirectoryStructureValidatorWithContextV2(ctx, indexPager))
	if err != nil {
		return closeWith(fmt.Errorf("treedb: verify rebound snapshot durable roots: %w", err))
	}
	for _, plan := range plans {
		if rebound.SlotCommits[plan.slot] != plan.meta.CommitSeq {
			return closeWith(fmt.Errorf("treedb: rebound snapshot slot %d commit=%d, want %d", plan.slot, rebound.SlotCommits[plan.slot], plan.meta.CommitSeq))
		}
	}
	return closeWith(nil)
}

func rebindSnapshotManifestEntryV1(dir, sideRoot string, entry *rootpublication.DependencyManifestEntryV1) error {
	if entry == nil {
		return rootpublication.ErrDependencyManifestFormat
	}
	resourcePath, err := durableDependencyPathForKindV1(dir, sideRoot, entry.Kind, entry.DiagnosticPath)
	if err != nil {
		return fmt.Errorf("invalid dependency path %q: %w", entry.DiagnosticPath, err)
	}
	identity, err := stableSnapshotPathIdentityV1(resourcePath, entry.Generation, rootpublication.SyncStableFile)
	if err != nil {
		return fmt.Errorf("capture dependency identity for %q: %w", entry.DiagnosticPath, err)
	}
	entry.Identity = identity
	if entry.Namespace != nil {
		parentPath := filepath.Dir(resourcePath)
		parent, err := os.Open(parentPath)
		if err != nil {
			return fmt.Errorf("capture dependency namespace identity for %q: %w", entry.Namespace.DiagnosticPath, err)
		}
		// Dictionary producers derive the namespace epoch from the physical
		// parent. Other producers may instead bind an immutable manifest revision
		// or asset file generation, which must remain unchanged during restore.
		syncErr := rootpublication.SyncStableNamespace(parent)
		parentIdentity, identityErr := rootpublication.StableIdentityFromFile(parent)
		parentGeneration := entry.Namespace.ParentIdentity.Generation
		var generationErr error
		if entry.Kind == rootpublication.ResourceDictionary {
			parentGeneration, generationErr = rootpublication.StableNamespaceParentGeneration(parent)
		}
		closeErr := parent.Close()
		if err := errors.Join(syncErr, identityErr, generationErr, closeErr); err != nil {
			return fmt.Errorf("capture dependency namespace identity for %q: %w", entry.Namespace.DiagnosticPath, err)
		}
		parentIdentity.Generation = parentGeneration
		entry.Namespace.ParentIdentity = parentIdentity
	}
	return nil
}

func stableSnapshotPathIdentityV1(path string, generation uint64, syncFile func(*os.File) error) (rootpublication.StableIdentity, error) {
	if generation == 0 {
		return rootpublication.StableIdentity{}, rootpublication.ErrDependencyManifestFormat
	}
	if syncFile == nil {
		return rootpublication.StableIdentity{}, os.ErrInvalid
	}
	file, err := os.Open(path)
	if err != nil {
		return rootpublication.StableIdentity{}, err
	}
	syncErr := syncFile(file)
	identity, identityErr := rootpublication.StableIdentityFromFile(file)
	closeErr := file.Close()
	if syncErr != nil || identityErr != nil || closeErr != nil {
		return rootpublication.StableIdentity{}, errors.Join(syncErr, identityErr, closeErr)
	}
	identity.Generation = generation
	return identity, nil
}

type snapshotIndexPageStoreV1 struct {
	file      *os.File
	pageCount uint64
	ctx       context.Context
}

func newSnapshotIndexPageStoreV1(file *os.File) (*snapshotIndexPageStoreV1, error) {
	if file == nil {
		return nil, os.ErrInvalid
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() <= 0 || info.Size()%int64(page.PageSize) != 0 {
		return nil, fmt.Errorf("treedb: snapshot index size %d is not page aligned", info.Size())
	}
	return &snapshotIndexPageStoreV1{file: file, pageCount: uint64(info.Size() / int64(page.PageSize))}, nil
}

func (store *snapshotIndexPageStoreV1) ReadPage(pageID uint64) ([]byte, error) {
	if store == nil || store.file == nil || pageID >= store.pageCount {
		return nil, io.EOF
	}
	if store.ctx != nil {
		if err := store.ctx.Err(); err != nil {
			return nil, err
		}
	}
	image := make([]byte, page.PageSize)
	_, err := store.file.ReadAt(image, int64(pageID)*int64(page.PageSize))
	return image, err
}

func (store *snapshotIndexPageStoreV1) WritePage(pageID uint64, image []byte) error {
	if store == nil || store.file == nil || pageID >= store.pageCount || len(image) != page.PageSize {
		return io.ErrShortWrite
	}
	if store.ctx != nil {
		if err := store.ctx.Err(); err != nil {
			return err
		}
	}
	written, err := store.file.WriteAt(image, int64(pageID)*int64(page.PageSize))
	if err != nil {
		return err
	}
	if written != len(image) {
		return io.ErrShortWrite
	}
	return nil
}
