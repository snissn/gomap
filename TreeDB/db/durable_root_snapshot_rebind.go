package db

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
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
// frontiers remain unchanged.
func RebindDurableRootSnapshotV1(dir string) error {
	return RebindDurableRootSnapshotLayoutV1(dir, "")
}

// RebindDurableRootSnapshotLayoutV1 is the staged-layout variant used by
// snapshot restore before the extracted main and side-store trees receive
// their final names. sideRoot contains dictdb/ and templatedb/ when non-empty.
func RebindDurableRootSnapshotLayoutV1(dir, sideRoot string) error {
	if dir == "" {
		return errors.New("treedb: durable-root snapshot rebind directory is empty")
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
		_, copyErr = io.Copy(temporary, source)
	}
	if copyErr == nil {
		copyErr = rootpublication.SyncStableFile(temporary)
	}
	copyErr = errors.Join(copyErr, temporary.Close(), source.Close())
	if copyErr != nil {
		return fmt.Errorf("treedb: create stable snapshot index durable-root rebind copy: %w", copyErr)
	}
	if err := rebindDurableRootSnapshotFileV1(dir, sideRoot, temporaryPath); err != nil {
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

func rebindDurableRootSnapshotFileV1(dir, sideRoot, indexPath string) error {
	file, err := os.OpenFile(indexPath, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("treedb: open snapshot index for durable-root rebind: %w", err)
	}
	store, err := newSnapshotIndexPageStoreV1(file)
	if err != nil {
		_ = file.Close()
		return err
	}
	closeWith := func(operationErr error) error {
		return errors.Join(operationErr, file.Close())
	}

	selected, err := selectDurableRootV1(store, store.pageCount, nil)
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
		manifest, err := rootpublication.LoadDependencyManifestV1(store, record.Manifest)
		if err != nil {
			return closeWith(fmt.Errorf("treedb: load snapshot dependency manifest for slot %d: %w", slot, err))
		}
		entries := manifest.Entries()
		for index := range entries {
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
		manifestRef, err := plan.manifest.Materialize(plan.record.Manifest.FirstPageID, store)
		if err != nil {
			return closeWith(fmt.Errorf("treedb: materialize rebound snapshot dependency manifest for slot %d: %w", plan.slot, err))
		}
		plan.record.Manifest = manifestRef
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
	rebound, err := selectDurableRootV1(store, store.pageCount, nil)
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
		parentIdentity, err := stableSnapshotPathIdentityV1(parentPath, entry.Namespace.ParentIdentity.Generation, rootpublication.SyncStableNamespace)
		if err != nil {
			return fmt.Errorf("capture dependency namespace identity for %q: %w", entry.Namespace.DiagnosticPath, err)
		}
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
	image := make([]byte, page.PageSize)
	_, err := store.file.ReadAt(image, int64(pageID)*int64(page.PageSize))
	return image, err
}

func (store *snapshotIndexPageStoreV1) WritePage(pageID uint64, image []byte) error {
	if store == nil || store.file == nil || pageID >= store.pageCount || len(image) != page.PageSize {
		return io.ErrShortWrite
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
