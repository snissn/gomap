package db

import (
	"context"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"io"
	"os"
	"path/filepath"
	"sort"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

// Restore owns an unpublished extracted directory. Both index files are edited
// privately, flushed and validated before installation. No live DB may use this
// operation. A process crash between the two renames leaves a fail-closed staged
// directory; the snapshot installer never publishes it without successful rebind.
func rebindDurablePrimarySnapshotLayoutV5(ctx context.Context, dir, sideRoot string) error {
	paths := [2]string{filepath.Join(dir, indexFileName), filepath.Join(dir, primaryIndexFileName)}
	var copies [2]string
	var sizes [2]int64
	defer func() {
		for _, name := range copies {
			if name != "" {
				_ = os.Remove(name)
			}
		}
	}()
	for i, path := range paths {
		name, size, err := copyPrimarySnapshotIndexV5(ctx, dir, path)
		if err != nil {
			return err
		}
		copies[i], sizes[i] = name, size
	}
	if err := rebindDurablePrimarySnapshotFilesV5(ctx, dir, sideRoot, copies[0], copies[1]); err != nil {
		return err
	}
	for i, path := range copies {
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			return err
		}
		stat, err := file.Stat()
		if err == nil && stat.Size() < sizes[i] {
			err = errors.New("treedb: rebound primary snapshot unexpectedly shrank")
		}
		if err == nil {
			err = file.Truncate(sizes[i])
		}
		if err == nil {
			err = rootpublication.SyncStableFile(file)
		}
		if err = errors.Join(err, file.Close()); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Keep the original companion for an ordinary second-rename failure. This is
	// rollback custody, not a third recovery namespace or a live publication path.
	backup, err := os.CreateTemp(dir, ".primary-rebind-original-*")
	if err != nil {
		return err
	}
	backupPath := backup.Name()
	if err = errors.Join(backup.Close(), os.Remove(backupPath)); err != nil {
		return err
	}
	if err = os.Link(paths[1], backupPath); err != nil {
		return err
	}
	defer os.Remove(backupPath)
	if err = os.Rename(copies[1], paths[1]); err != nil {
		return err
	}
	copies[1] = ""
	if err = os.Rename(copies[0], paths[0]); err != nil {
		rollback := os.Rename(backupPath, paths[1])
		return errors.Join(err, rollback, syncPrimarySnapshotNamespaceV5(dir))
	}
	copies[0] = ""
	if err = os.Remove(backupPath); err != nil {
		return err
	}
	return syncPrimarySnapshotNamespaceV5(dir)
}

func syncPrimarySnapshotNamespaceV5(dir string) error {
	parent, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(rootpublication.SyncStableNamespace(parent), parent.Close())
}
func copyPrimarySnapshotIndexV5(ctx context.Context, dir, path string) (name string, size int64, result error) {
	source, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer func() { result = errors.Join(result, source.Close()) }()
	stat, err := source.Stat()
	if err != nil {
		return "", 0, err
	}
	if stat.Size() <= 0 || stat.Size()%page.PageSize != 0 {
		return "", 0, rootpublication.ErrDurableRootRecordFormat
	}
	target, err := os.CreateTemp(dir, ".primary-root-rebind-*")
	if err != nil {
		return "", 0, err
	}
	name, size = target.Name(), stat.Size()
	defer func() {
		result = errors.Join(result, target.Close())
		if result != nil {
			_ = os.Remove(name)
		}
	}()
	if err = target.Chmod(stat.Mode().Perm()); err != nil {
		return name, size, err
	}
	var buffer [64 * 1024]byte
	for offset := int64(0); offset < size; {
		if err = ctx.Err(); err != nil {
			return name, size, err
		}
		n := min(int64(len(buffer)), size-offset)
		if _, err = source.ReadAt(buffer[:n], offset); err != nil {
			return name, size, err
		}
		written, err := target.Write(buffer[:n])
		if err != nil {
			return name, size, err
		}
		if int64(written) != n {
			return name, size, io.ErrShortWrite
		}
		offset += n
	}
	return name, size, rootpublication.SyncStableFile(target)
}

type primarySnapshotBankStoreV5 struct{ local *snapshotIndexPageStoreV1 }

func (s primarySnapshotBankStoreV5) ReadPage(id uint64) ([]byte, error) {
	return (primaryBankPageSource{local: s.local, extent: s.local.pageCount}).ReadPage(id)
}
func (s primarySnapshotBankStoreV5) WritePage(id uint64, image []byte) error {
	if !primaryarena.IsPage(id) || primaryarena.Local(id) < 2 {
		return rootpublication.ErrDurableRootRecordFormat
	}
	return s.local.WritePage(primaryarena.Local(id), image)
}

// Private extraction owns the arena mapping for this whole validation scope.
func primaryDependencyStructureValidatorWithMetadataV5(ctx context.Context, a *primaryarena.Arena) durablePrimaryDirectoryValidatorV5 {
	return func(v rootpublication.DurablePrimaryRootRecordV5) (*rootpublication.StableResourceSet, error) {
		d, e := rootpublication.NewOwnedLocalPrimaryDependencyDirectoryV5(a.Pager(), v.Record.Directory, v.Primary.ArenaHighWater, a.MetadataOwner(), func() (bool, error) { return true, nil })
		if e != nil {
			return nil, e
		}
		defer d.Release()
		return nil, d.ValidateWithMetadataV2(a.MetadataOwner(), ctx.Err)
	}
}

type primaryRebindRecordV5 struct {
	id     uint64
	value  rootpublication.DurablePrimaryRootRecordV5
	digest [32]byte
}
type primaryRebindScratchV5 struct {
	records  [4]primaryRebindRecordV5
	count    int
	operands primaryRebindScratchV6
	images   [4][]byte
}

func (s *primaryRebindScratchV5) insert(id uint64, v rootpublication.DurablePrimaryRootRecordV5) error {
	for i := 0; i < s.count; i++ {
		if s.records[i].id == id {
			if s.records[i].value != v {
				return rootpublication.ErrDurableRootRecordFormat
			}
			return nil
		}
	}
	if s.count == len(s.records) {
		return rootpublication.ErrDurableRootRecordFormat
	}
	s.records[s.count] = primaryRebindRecordV5{id: id, value: v}
	s.count++
	return nil
}
func (s *primaryRebindScratchV5) digest(id uint64) ([32]byte, bool) {
	for i := 0; i < s.count; i++ {
		if s.records[i].id == id {
			return s.records[i].digest, s.records[i].digest != ([32]byte{})
		}
	}
	return [32]byte{}, false
}
func rebindDurablePrimarySnapshotFilesV5(ctx context.Context, dir, sideRoot, dataPath, bankPath string) (resultErr error) {
	dataFile, err := os.OpenFile(dataPath, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, dataFile.Close()) }()
	bankFile, err := os.OpenFile(bankPath, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, bankFile.Close()) }()
	dataStore, err := newSnapshotIndexPageStoreV1(dataFile)
	if err != nil {
		return err
	}
	bankStore, err := newSnapshotIndexPageStoreV1(bankFile)
	if err != nil {
		return err
	}
	dataStore.ctx, bankStore.ctx = ctx, ctx
	dataPager, err := pager.Open(dataPath, 64<<20)
	if err != nil {
		return err
	}
	arena, err := primaryarena.OpenExisting(bankPath, false)
	if err != nil {
		return errors.Join(err, dataPager.Close())
	}
	closeWith := func(err error) error { return errors.Join(err, dataPager.Close(), arena.Close()) }
	if err = arena.SetOwnedMetadataDecoder(rootpublication.PrimaryBankMetadataEdgesV5, rootpublication.PrimaryBankMetadataEdgesOwnedV5); err != nil {
		return closeWith(err)
	}
	if err = dataPager.AttachPrimaryBankPager(arena.Pager()); err != nil {
		return closeWith(err)
	}
	if arena.CapsuleFormatV6() {
		return closeWith(rebindPrimaryCapsuleSnapshotV6(ctx, dir, sideRoot, dataPager, arena, dataStore, bankStore, dataFile, bankFile))
	}
	validate := primaryDependencyStructureValidatorWithMetadataV5(ctx, arena)
	selected, err := selectDurablePrimaryRootWithMetadataV5(dataStore, bankStore, dataStore.pageCount, bankStore.pageCount, arena.UUID(), nil, validate, arena.MetadataOwner())
	if err != nil {
		return closeWith(fmt.Errorf("treedb: select primary snapshot for rebind: %w", err))
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryRebindScratchV5{}))) + 4*retainedalloc.AllocationCharge(page.PageSize)
	if err = arena.MetadataOwner().AddPending(charge); err != nil {
		return closeWith(err)
	}
	scratch := new(primaryRebindScratchV5)
	defer func() { *scratch = primaryRebindScratchV5{}; arena.MetadataOwner().RemovePending(charge) }()
	for slot := 0; slot < 2; slot++ {
		if selected.SlotCommits[slot] == 0 {
			continue
		}
		r := selected.SlotRecords[slot]
		if err = scratch.insert(selected.SlotMetas[slot].RootRecordPageID, rootpublication.DurablePrimaryRootRecordV5{Record: r, Primary: selected.SlotPrimary[slot]}); err != nil {
			return closeWith(err)
		}
		if r.ParentRecordPageID != 0 {
			if err = scratch.insert(r.ParentRecordPageID, selected.ParentRecords[slot]); err != nil {
				return closeWith(err)
			}
		}
	}
	sort.Slice(scratch.records[:scratch.count], func(i, j int) bool {
		return scratch.records[i].value.Record.CommitSeq < scratch.records[j].value.Record.CommitSeq
	})
	writer := primarySnapshotBankStoreV5{local: bankStore}
	for i := 0; i < scratch.count; i++ {
		id := scratch.records[i].id
		if err = ctx.Err(); err != nil {
			return closeWith(err)
		}
		v := scratch.records[i].value
		r := &v.Record
		if err = rebindPrimarySnapshotOperand(ctx, dir, sideRoot, dataPager, writer, &v, &scratch.operands, arena.MetadataOwner(), 2); err != nil {
			return closeWith(err)
		}
		if digest, ok := scratch.digest(r.ParentRecordPageID); ok {
			r.ParentRecordDigest = digest
		}
		image := make([]byte, page.PageSize) // within the admitted four-image backing
		scratch.images[i] = image
		digest, err := v.EncodePageInto(image, id)
		if err != nil {
			return closeWith(err)
		}
		if err = writer.WritePage(id, image); err != nil {
			return closeWith(err)
		}
		scratch.records[i].digest = digest
	}
	if err = arena.Pager().Sync(); err != nil {
		return closeWith(err)
	}
	if err = dataPager.Sync(); err != nil {
		return closeWith(err)
	}
	for slot := uint64(0); slot < 2; slot++ {
		if selected.SlotCommits[slot] == 0 {
			continue
		}
		meta := selected.SlotMetas[slot]
		digest, ok := scratch.digest(meta.RootRecordPageID)
		if !ok {
			return closeWith(rootpublication.ErrDurableRootRecordFormat)
		}
		meta.RootRecordDigest = digest
		_, err = executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{
			syncIndex: func() error {
				return errors.Join(rootpublication.SyncStableFile(bankFile), rootpublication.SyncStableFile(dataFile))
			},
			sink: dataStore, target: slot, meta: meta, syncMeta: func() error { return rootpublication.SyncStableFile(dataFile) }, dir: dir, indexPath: dataPath,
		})
		if err != nil {
			return closeWith(err)
		}
	}
	rebound, err := selectDurablePrimaryRootWithMetadataV5(dataStore, bankStore, dataStore.pageCount, bankStore.pageCount, arena.UUID(), nil, validate, arena.MetadataOwner())
	if err != nil {
		return closeWith(fmt.Errorf("treedb: validate rebound primary snapshot: %w", err))
	}
	for slot := 0; slot < 2; slot++ {
		if rebound.SlotCommits[slot] != selected.SlotCommits[slot] {
			return closeWith(errors.New("treedb: primary snapshot rebind lost independent slot"))
		}
	}
	return closeWith(nil)
}
