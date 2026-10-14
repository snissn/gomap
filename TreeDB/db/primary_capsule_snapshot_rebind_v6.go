package db

import (
	"context"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
	"os"
)

// The same actual dependency rebinding is applied independently to each
// current/embedded-parent operand. Fixed capsule images are edited only in the
// unpublished two-file copies owned by the existing snapshot installer.
func rebindPrimaryCapsuleOperandV6(ctx context.Context, dir, sideRoot string, p *pager.Pager, writer primarySnapshotBankStoreV5, v *rootpublication.DurablePrimaryRootRecordV5, reboundManifests map[rootpublication.DependencyManifestRefV1]rootpublication.DependencyManifestRefV1) error {
	r := &v.Record
	if r.Directory.RootPageID != 0 {
		tr := tree.NewWithPrimaryBankExtent(p, r.Directory.RootPageID, v.Primary.ArenaHighWater)
		return rebindSnapshotDependencyTree(ctx, dir, sideRoot, p, tr, r.Directory.PhysicalCount, func(id uint64) bool {
			return primaryarena.IsPage(id) && primaryarena.Local(id) >= rootpublication.PrimaryCapsuleFirstBankV6 && primaryarena.Local(id) < v.Primary.ArenaHighWater
		})
	}
	original := r.Manifest
	if ref, ok := reboundManifests[original]; ok {
		r.Manifest = ref
		return nil
	}
	manifest, e := rootpublication.LoadDependencyManifestV1(writer, r.Manifest)
	if e != nil {
		return e
	}
	entries := manifest.Entries()
	for i := range entries {
		if e = ctx.Err(); e != nil {
			return e
		}
		if e = rebindSnapshotManifestEntryV1(dir, sideRoot, &entries[i]); e != nil {
			return e
		}
	}
	rebound, e := rootpublication.NewDependencyManifestV1(entries)
	if e != nil {
		return e
	}
	if rebound.PageCount() != r.Manifest.PageCount {
		return rootpublication.ErrDependencyManifestFormat
	}
	r.Manifest, e = rebound.Materialize(r.Manifest.FirstPageID, writer)
	if e == nil {
		reboundManifests[original] = r.Manifest
	}
	return e
}

func rebindPrimaryCapsuleSnapshotV6(ctx context.Context, dir, sideRoot string, p *pager.Pager, a *primaryarena.Arena, data, bank *snapshotIndexPageStoreV1, dataFile, bankFile *os.File) error {
	validate := primaryDependencyStructureValidatorV5(ctx, p)
	selected, images, e := selectPrimaryCapsuleAuthorityV6(data, bank, data.pageCount, bank.pageCount, a.UUID(), nil, validate)
	if e != nil {
		return fmt.Errorf("select complete capsule snapshot: %w", e)
	}
	writer := primarySnapshotBankStoreV5{local: bank}
	// Metadata changes must precede either eligible capsule fence. No DATA META
	// is installed: the two complete PRIMARY records remain sole authority.
	var rebound [2][]byte
	reboundManifests := make(map[rootpublication.DependencyManifestRefV1]rootpublication.DependencyManifestRefV1)
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
		if e = rebindPrimaryCapsuleOperandV6(ctx, dir, sideRoot, p, writer, &current, reboundManifests); e != nil {
			return e
		}
		var parentPtr *rootpublication.DurablePrimaryRootRecordV5
		if parent.Record.CommitSeq != 0 {
			if e = rebindPrimaryCapsuleOperandV6(ctx, dir, sideRoot, p, writer, &parent, reboundManifests); e != nil {
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
	after, _, e := selectPrimaryCapsuleAuthorityV6(data, bank, data.pageCount, bank.pageCount, a.UUID(), nil, validate)
	if e != nil {
		return fmt.Errorf("validate rebound complete capsules: %w", e)
	}
	if after.SlotCommits != selected.SlotCommits {
		return errors.New("capsule rebind lost an independent eligible slot")
	}
	return nil
}
