package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

// RetainedPhysicalRootV1 is the bounded recovery operand of an existing DATA
// publication. It is usable only in the SAME index inode and parent namespace,
// while a PhysicalSnapshotCutV1 retains its original page/dependency closure.
// It is not an archive, a root constructor, or permission to release that cut.
type RetainedPhysicalRootV1 struct {
	Version   int
	Index     rootpublication.StableIdentity
	Namespace rootpublication.StableIdentity
	Meta      page.DurableMetaV1
	State     StateToken
	HighWater uint64
	// Conservative per-root retained physical extent, including index high-water.
	ResourceBytes uint64
	ResourceFiles uint64
}

// RetainedRootV1 selects only the newest slot. Its immediate parent is still
// protected by the cut's ordinary two-slot floor; unlike a sequential archive,
// an in-place retained root must never restore a possibly reused ancestor page.
func (cut *PhysicalSnapshotCutV1) RetainedRootV1() (RetainedPhysicalRootV1, error) {
	cut.mu.Lock()
	defer cut.mu.Unlock()
	var result RetainedPhysicalRootV1
	if cut.file == nil || cut.primaryFile != nil {
		return result, errors.New("retained external root requires live DATA cut")
	}
	for slot := uint64(0); slot < 2; slot++ {
		meta, err := page.DecodeDurableMetaV1(cut.meta[slot][page.PageHeaderSize:])
		if err == nil && meta.CommitSeq == cut.state.CommitSeq {
			result.Meta = meta
			break
		}
	}
	if result.Meta.CommitSeq == 0 {
		return result, ErrRecoverableRootSetStale
	}
	identity, err := rootpublication.StableIdentityFromFile(cut.file)
	if err != nil {
		return result, err
	}
	parent, err := os.Open(cut.roots.db.dir)
	if err != nil {
		return result, err
	}
	namespace, identityErr := rootpublication.StableIdentityFromFile(parent)
	if err := errors.Join(identityErr, parent.Close()); err != nil {
		return result, err
	}
	// This API currently certifies the raw DATA surface, including persistent
	// value and outer-leaf producers. Auxiliary stores require their own owner.
	resources := cut.roots.resourcesForRoot(RecoverableRoot{CommitSeq: cut.state.CommitSeq, UserRootPageID: cut.state.RootPageID, SystemRootPageID: cut.state.SystemRootPageID, AppliedCommandLSN: cut.state.AppliedCommandLSN, MaxEntryRevision: uint64(cut.state.MaxEntryRevision)})
	result.ResourceBytes, result.ResourceFiles = uint64(cut.SizeBytes()), 1
	for _, descriptor := range resources.PhysicalDescriptors() {
		switch descriptor.Kind {
		case rootpublication.ResourceIndex, rootpublication.ResourceValueLog,
			rootpublication.ResourceOuterLeafLog, rootpublication.ResourceOuterLeafManifest, rootpublication.ResourceOuterLeafPack:
		default:
			return result, fmt.Errorf("unsupported retained external dependency: %s", descriptor.Kind)
		}
	}
	count, bytes := resources.PhysicalSummary()
	if bytes > ^uint64(0)-result.ResourceBytes || count == ^uint64(0) {
		return result, errors.New("retained resource extent overflow")
	}
	result.ResourceBytes += bytes
	result.ResourceFiles += count
	result.Version, result.Index, result.Namespace = 1, identity, namespace
	result.State, result.HighWater = cut.state, cut.generation.HighWater()
	result.State.LeafGenerationStateVersion = 0
	return result, nil
}

// RestoreRetainedPhysicalRootsV1 runs before opening any backend owner. The
// caller exclusively owns the external directory. Both certificate operands
// remain immutable throughout restore: after any torn slot write or failed sync,
// retry writes the same two complete meta images before startup can reuse pages.
// Normal Open then validates and pins both closures before accepting mutations.
func RestoreRetainedPhysicalRootsV1(dir string, current RetainedPhysicalRootV1, previous *RetainedPhysicalRootV1) error {
	return restoreRetainedPhysicalRootsV1(dir, current, previous, nil)
}

func restoreRetainedPhysicalRootsV1(dir string, current RetainedPhysicalRootV1, previous *RetainedPhysicalRootV1, afterSlot func(int) error) error {
	lock, err := lockfile.Acquire(filepath.Join(dir, "LOCK"))
	if err != nil {
		return err
	}
	defer lock.Close()
	parent, err := os.Open(dir)
	if err != nil {
		return err
	}
	namespace, identityErr := rootpublication.StableIdentityFromFile(parent)
	if err := errors.Join(identityErr, parent.Close()); err != nil {
		return err
	}
	path := filepath.Join(dir, "index.db")
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return errors.Join(err, errors.New("retained index must be a regular file"))
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		return err
	}
	store, err := newSnapshotIndexPageStoreV1(file)
	if err != nil {
		return err
	}
	roots := [2]RetainedPhysicalRootV1{current, current}
	if previous != nil {
		roots[1] = *previous
	}
	for _, root := range roots {
		if root.Version != 1 || root.Index != identity || root.Namespace != namespace || root.HighWater > store.pageCount || root.Meta.CommitSeq != root.State.CommitSeq || root.Meta.CommitSeq > current.Meta.CommitSeq {
			return errors.New("retained root identity/extent mismatch")
		}
		image, err := store.ReadPage(root.Meta.RootRecordPageID)
		if err != nil {
			return err
		}
		record, err := rootpublication.DecodeDurableRootRecordV1(image, root.Meta.RootRecordPageID, root.Meta.RootRecordDigest)
		if err != nil {
			return err
		}
		if record.CommitSeq != root.Meta.CommitSeq || record.TotalPages != root.HighWater || record.UserRootPageID != root.State.RootPageID || record.SystemRootPageID != root.State.SystemRootPageID {
			return errors.New("retained root projection mismatch")
		}
		if err := validateDurableRootLineageV1(store, record); err != nil {
			return err
		}
	}
	for slot, root := range roots {
		if err := writeDurableMetaSlotV1(store, uint64(slot), root.Meta); err != nil {
			return err
		}
		if afterSlot != nil {
			if err := afterSlot(slot); err != nil {
				return err
			}
		}
	}
	return rootpublication.SyncStableFile(file)
}
