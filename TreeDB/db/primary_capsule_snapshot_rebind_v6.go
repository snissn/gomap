package db

import (
	"context"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
	"os"
	"unsafe"
)

type reboundPrimaryManifestV6 struct {
	original, rebound rootpublication.DependencyManifestRefV1
}
type primaryRebindScratchV6 struct {
	manifests              [4]reboundPrimaryManifestV6
	count                  int
	images, rebound, after [2][]byte
}

// The same actual dependency rebinding is applied independently to each
// current/embedded-parent operand. Fixed capsule images are edited only in the
// unpublished two-file copies owned by the existing snapshot installer.
func rebindPrimaryCapsuleOperandV6(ctx context.Context, dir, sideRoot string, p *pager.Pager, writer primarySnapshotBankStoreV5, v *rootpublication.DurablePrimaryRootRecordV5, scratch *primaryRebindScratchV6, metadata *retainedalloc.Owner) error {
	return rebindPrimarySnapshotOperand(ctx, dir, sideRoot, p, writer, v, scratch, metadata, rootpublication.PrimaryCapsuleFirstBankV6)
}

func rebindPrimarySnapshotOperand(ctx context.Context, dir, sideRoot string, p *pager.Pager, writer primarySnapshotBankStoreV5, v *rootpublication.DurablePrimaryRootRecordV5, scratch *primaryRebindScratchV6, metadata *retainedalloc.Owner, firstBank uint64) error {
	r := &v.Record
	if r.Directory.RootPageID != 0 {
		charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(tree.Tree{})))
		if err := metadata.AddPending(charge); err != nil {
			return err
		}
		defer metadata.RemovePending(charge)
		tr := tree.NewWithPrimaryBankExtent(p, r.Directory.RootPageID, v.Primary.ArenaHighWater)
		return rebindSnapshotDependencyTreeWithMetadata(ctx, dir, sideRoot, p, tr, r.Directory.PhysicalCount, func(id uint64) bool {
			return primaryarena.IsPage(id) && primaryarena.Local(id) >= firstBank && primaryarena.Local(id) < v.Primary.ArenaHighWater
		}, metadata)
	}
	original := r.Manifest
	for _, value := range scratch.manifests[:scratch.count] {
		if value.original == original {
			r.Manifest = value.rebound
			return nil
		}
	}
	if scratch.count == len(scratch.manifests) {
		return rootpublication.ErrResourceOwnership
	}
	rebound, e := rebindPrimarySnapshotManifestV1(ctx, dir, sideRoot, writer, r.Manifest, metadata)
	r.Manifest = rebound
	if e == nil {
		scratch.manifests[scratch.count] = reboundPrimaryManifestV6{original, r.Manifest}
		scratch.count++
	}
	return e
}

func rebindPrimaryCapsuleSnapshotV6(ctx context.Context, dir, sideRoot string, p *pager.Pager, a *primaryarena.Arena, data, bank *snapshotIndexPageStoreV1, dataFile, bankFile *os.File) error {
	// Selection, edited images, and validation copies overlap. Reserve full
	// backing before any allocation; none escapes this unpublished-copy scope.
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryRebindScratchV6{}))) + 6*retainedalloc.AllocationCharge(rootpublication.PrimaryCapsuleSizeV6)
	if e := a.MetadataOwner().AddPending(charge); e != nil {
		return e
	}
	scratch := &primaryRebindScratchV6{}
	defer func() { *scratch = primaryRebindScratchV6{}; a.MetadataOwner().RemovePending(charge) }()
	validate := primaryDependencyStructureValidatorWithMetadataV5(ctx, a)
	selected, images, e := selectPrimaryCapsuleAuthorityV6(data, bank, data.pageCount, bank.pageCount, a.UUID(), nil, validate, a.MetadataOwner())
	if e != nil {
		return fmt.Errorf("select complete capsule snapshot: %w", e)
	}
	scratch.images = images
	writer := primarySnapshotBankStoreV5{local: bank}
	// Metadata changes must precede either eligible capsule fence. No DATA META
	// is installed: the two complete PRIMARY records remain sole authority.
	rebound := scratch.rebound[:]
	for slot, image := range images {
		if selected.SlotCommits[slot] == 0 {
			continue
		}
		if e = ctx.Err(); e != nil {
			return e
		}
		v, e := rootpublication.DecodePrimaryCapsuleV6(image, uint64(slot), a.UUID())
		if e != nil {
			return e
		}
		current, parent := v.Current(), v.Parent()
		if e = rebindPrimaryCapsuleOperandV6(ctx, dir, sideRoot, p, writer, &current, scratch, a.MetadataOwner()); e != nil {
			return e
		}
		var parentPtr *rootpublication.DurablePrimaryRootRecordV5
		if parent.Record.CommitSeq != 0 {
			if e = rebindPrimaryCapsuleOperandV6(ctx, dir, sideRoot, p, writer, &parent, scratch, a.MetadataOwner()); e != nil {
				return e
			}
			parentPtr = &parent
		}
		rebound[slot] = make([]byte, rootpublication.PrimaryCapsuleSizeV6)
		if e = rootpublication.EncodePrimaryCapsuleV6(rebound[slot], uint64(slot), current, v.Directory(), parentPtr, v.ParentDirectory()); e != nil {
			return e
		}
	}
	if e = errors.Join(p.Sync(), a.Pager().Sync(), rootpublication.SyncStableFile(dataFile), rootpublication.SyncStableFile(bankFile)); e != nil {
		return e
	}
	for slot, image := range rebound {
		if len(image) == 0 {
			continue
		}
		if e = ctx.Err(); e != nil {
			return e
		}
		if _, ok, e := a.Pager().WritePrimaryCapsuleViewWithWork(uint64(2+3*slot), image, nil); !ok || e != nil {
			return errors.Join(e, rootpublication.ErrDurableRootRecordFormat)
		}
	}
	after, afterImages, e := selectPrimaryCapsuleAuthorityV6(data, bank, data.pageCount, bank.pageCount, a.UUID(), nil, validate, a.MetadataOwner())
	scratch.after = afterImages
	if e != nil {
		return fmt.Errorf("validate rebound complete capsules: %w", e)
	}
	if after.SlotCommits != selected.SlotCommits {
		return errors.New("capsule rebind lost an independent eligible slot")
	}
	return nil
}

func rebindPrimarySnapshotManifestV1(ctx context.Context, dir, sideRoot string, writer primarySnapshotBankStoreV5, original rootpublication.DependencyManifestRefV1, metadata *retainedalloc.Owner) (rootpublication.DependencyManifestRefV1, error) {
	manifest, e := rootpublication.LoadDependencyManifestWithMetadataV1(writer, original, metadata)
	if e != nil {
		return rootpublication.DependencyManifestRefV1{}, e
	}
	defer manifest.ReleaseOwnedMetadataV1()
	e = manifest.RebindPhysicalIdentitiesV1(func(entry rootpublication.DependencyManifestEntryV1) (rootpublication.StableIdentity, rootpublication.StableIdentity, error) {
		if e := ctx.Err(); e != nil {
			return rootpublication.StableIdentity{}, rootpublication.StableIdentity{}, e
		}
		// The existing physical identity/sync core mutates only this local namespace
		// header. No selected manifest alias is exported or retained by the callback.
		var namespace rootpublication.DependencyManifestNamespaceV1
		if entry.Namespace != nil {
			namespace = *entry.Namespace
			entry.Namespace = &namespace
		}
		e := rebindSnapshotManifestEntryV1(dir, sideRoot, &entry)
		var parent rootpublication.StableIdentity
		if entry.Namespace != nil {
			parent = entry.Namespace.ParentIdentity
		}
		return entry.Identity, parent, e
	})
	if e != nil {
		return rootpublication.DependencyManifestRefV1{}, e
	}
	rebound := manifest
	if rebound.PageCount() != original.PageCount {
		return rootpublication.DependencyManifestRefV1{}, rootpublication.ErrDependencyManifestFormat
	}
	charge := retainedalloc.AllocationCharge(page.PageSize)
	if e = metadata.AddPending(charge); e != nil {
		return rootpublication.DependencyManifestRefV1{}, e
	}
	scratch := make([]byte, page.PageSize)
	defer func() { clear(scratch); scratch = nil; metadata.RemovePending(charge) }()
	ref, e := rebound.MaterializeWithScratchV1(original.FirstPageID, writer, scratch)
	return ref, e
}
