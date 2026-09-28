package db

import (
	"bytes"
	"context"
	"fmt"

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
	it := tr.Iterator(nil, nil)
	defer it.Close()
	location, ok := it.(*tree.Iterator)
	if !ok {
		return rootpublication.ErrDependencyManifestFormat
	}
	for physical := uint64(0); physical < record.Directory.PhysicalCount; physical++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !it.Valid() {
			if err := it.Error(); err != nil {
				return err
			}
			return rootpublication.ErrDependencyManifestFormat
		}
		key := bytes.Clone(it.UnsafeKey())
		entry, err := rootpublication.DecodeDependencyPhysicalV2(key, it.UnsafeValue())
		if err != nil {
			return err
		}
		if err := rebindSnapshotManifestEntryV1(dir, sideRoot, &entry); err != nil {
			return err
		}
		if !bytes.Equal(key, rootpublication.DependencyPhysicalKeyV2(entry)) {
			return rootpublication.ErrResourceConflict
		}
		encoded, err := rootpublication.EncodeDependencyPhysicalV2(entry)
		if err != nil {
			return err
		}
		id, ok := location.CurrentIndexLeafPageID()
		if !ok || id < 2 || id >= record.TotalPages {
			return rootpublication.ErrDependencyManifestFormat
		}
		image, err := p.ReadPage(id)
		if err != nil {
			return err
		}
		image = bytes.Clone(image)
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
		if err := p.Write(id, image); err != nil {
			return err
		}
		// Write invalidates the pager's verified bit. The current iterator borrows
		// mmap bytes, but fixed-width replacement changes no cursor/key structure.
		it.Next()
	}
	return it.Error()
}
