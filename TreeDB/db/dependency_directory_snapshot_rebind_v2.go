package db

import (
	"bytes"
	"context"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

// This helper is only for the private staged snapshot copy, after selection
// validates BOTH slots' directories. It never grants writable access to live
// tree pages. Fixed-width identity replacement preserves leaf packing, keys,
// revisions, roots and allocator high-water, including shared slot subtrees.
func rebindSnapshotDependencyDirectoryWithContextV2(ctx context.Context, dir, sideRoot string, p *pager.Pager, record rootpublication.DurableRootRecordV1) error {
	tr := tree.NewWithPageLimit(p, nil, record.Directory.RootPageID, record.TotalPages)
	return rebindSnapshotDependencyTree(ctx, dir, sideRoot, p, tr, record.Directory.PhysicalCount, func(id uint64) bool { return id >= 2 && id < record.TotalPages })
}

func rebindSnapshotDependencyTree(ctx context.Context, dir, sideRoot string, p *pager.Pager, tr *tree.Tree, count uint64, inExtent func(uint64) bool) error {
	return rebindSnapshotDependencyTreeWithMetadata(ctx, dir, sideRoot, p, tr, count, inExtent, nil)
}

func rebindSnapshotDependencyTreeWithMetadata(ctx context.Context, dir, sideRoot string, p *pager.Pager, tr *tree.Tree, count uint64, inExtent func(uint64) bool, metadata *retainedalloc.Owner) error {
	var it iterator.UnsafeIterator
	var scratch *primaryDependencyRebindPageV2
	if metadata != nil {
		charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryDependencyRebindPageV2{}))) + retainedalloc.AllocationCharge(tree.OwnedPointerProjectionAllocationSize())
		if err := metadata.AddPending(charge); err != nil {
			return err
		}
		scratch = new(primaryDependencyRebindPageV2)
		defer func() { *scratch = primaryDependencyRebindPageV2{}; metadata.RemovePending(charge) }()
		it = tr.OwnedPointerProjectionIterator(nil, nil, nil)
	} else {
		it = tr.Iterator(nil, nil)
	}
	defer it.Close()
	location, ok := it.(*tree.Iterator)
	if !ok {
		return rootpublication.ErrDependencyManifestFormat
	}
	for physical := uint64(0); physical < count; physical++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !it.Valid() {
			if err := it.Error(); err != nil {
				return err
			}
			return rootpublication.ErrDependencyManifestFormat
		}
		var key []byte
		if scratch != nil {
			if len(it.UnsafeKey()) > len(scratch.key) {
				return rootpublication.ErrDependencyManifestFormat
			}
			key = scratch.key[:len(it.UnsafeKey())]
			copy(key, it.UnsafeKey())
			err := rootpublication.WithReboundDependencyPhysicalV2(metadata, key, it.UnsafeValue(), func(entry rootpublication.DependencyManifestEntryV1) (rootpublication.StableIdentity, rootpublication.StableIdentity, error) {
				var namespace rootpublication.DependencyManifestNamespaceV1
				if entry.Namespace != nil {
					namespace = *entry.Namespace
					entry.Namespace = &namespace
				}
				err := rebindSnapshotManifestEntryV1(dir, sideRoot, &entry)
				var parent rootpublication.StableIdentity
				if entry.Namespace != nil {
					parent = entry.Namespace.ParentIdentity
				}
				return entry.Identity, parent, err
			}, func(encoded []byte) error {
				return installReboundDependencyLeafV2(p, location, key, encoded, inExtent, scratch.image[:])
			})
			if err != nil {
				return err
			}
		} else {
			key = bytes.Clone(it.UnsafeKey())
			entry, err := rootpublication.DecodeDependencyPhysicalV2(key, it.UnsafeValue())
			if err != nil {
				return err
			}
			if err = rebindSnapshotManifestEntryV1(dir, sideRoot, &entry); err != nil {
				return err
			}
			if !bytes.Equal(key, rootpublication.DependencyPhysicalKeyV2(entry)) {
				return rootpublication.ErrResourceConflict
			}
			encoded, err := rootpublication.EncodeDependencyPhysicalV2(entry)
			if err != nil {
				return err
			}
			if err = installReboundDependencyLeafV2(p, location, key, encoded, inExtent, nil); err != nil {
				return err
			}
		}
		// Write invalidates the pager's verified bit. The current iterator borrows
		// mmap bytes, but fixed-width replacement changes no cursor/key structure.
		it.Next()
	}
	return it.Error()
}

type primaryDependencyRebindPageV2 struct{ key, image [page.PageSize]byte }

func installReboundDependencyLeafV2(p *pager.Pager, location *tree.Iterator, key, encoded []byte, inExtent func(uint64) bool, destination []byte) error {
	id, ok := location.CurrentIndexLeafPageID()
	if !ok || !inExtent(id) {
		return rootpublication.ErrDependencyManifestFormat
	}
	image, err := p.ReadPage(id)
	if err != nil {
		return err
	}
	if len(destination) == page.PageSize {
		copy(destination, image)
		image = destination
	} else {
		image = bytes.Clone(image)
	}
	n := node.NewNode(image)
	if n.Type() != page.PageTypeLeaf || !n.VerifyChecksum() {
		return rootpublication.ErrDependencyManifestFormat
	}
	index, found, err := n.SearchLeaf(key)
	if err != nil {
		return err
	}
	if !found {
		return rootpublication.ErrDependencyManifestFormat
	}
	actualKey, value, _, flags, _, err := n.GetLeafEntryViewWithRevision(index)
	if err != nil {
		return err
	}
	if !bytes.Equal(actualKey, key) || flags != node.FlagInline || len(value) != len(encoded) {
		return fmt.Errorf("%w: snapshot directory identity replacement changes leaf encoding or length", rootpublication.ErrDependencyManifestFormat)
	}
	copy(value, encoded)
	n.UpdateChecksum()
	return p.Write(id, image)
}
