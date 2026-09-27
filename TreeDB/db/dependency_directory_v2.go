package db

import (
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

// prepareDependencyDirectoryV2 runs while publication owns rootReuseMu and
// before PrepareCOWCandidateRetiringV1 freezes the allocator. The ordinary
// ordered-root zipper supplies changed-path retirement for this third tree.
func (db *DB) prepareDependencyDirectoryV2(idx *indexGen, sequence uint64, baseResources, resources *rootpublication.StableResourceSet) (*rootpublication.StableResourceSet, []uint64, error) {
	if db == nil || idx == nil || idx != db.idx.Load() || sequence == 0 {
		return nil, nil, errors.New("invalid dependency directory index generation")
	}
	base, err := baseResources.DependencyDirectoryV2()
	if err != nil {
		return nil, nil, err
	}
	var root uint64
	if base != nil {
		root = base.Reference().RootPageID
	}
	delta := batch.New(nil, page.PageSize)
	defer delta.Close()
	physical, logical, err := rootpublication.WalkDependencyDirectoryChangesV2(resources, base, func(key, value []byte, deleted bool) error {
		if deleted {
			return delta.Delete(key)
		}
		return delta.Set(key, value)
	})
	if err != nil {
		return nil, nil, err
	}
	var retired []uint64
	if root == 0 && delta.IsEmpty() {
		iter := newOrderedRootDeltaBatchIterator(delta, false)
		defer iter.Close()
		root, err = bulk.BuildWithOptions(iter, idx.allocator, idx.pager, bulk.BuildOptions{LeafPrefixCompression: true})
	} else {
		root, retired, _, err = db.publishOrderedRootDeltaBatchWithAllocator(idx, root, delta, orderedRootPublishOptions{
			leafPrefixCompression: true,
		}, idx.allocator, idx.allocator, false)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("apply dependency directory: %w", err)
	}
	directory, err := db.pinDependencyDirectoryV2(idx, sequence, rootpublication.DependencyDirectoryRefV2{
		RootPageID: root, PhysicalCount: physical, LogicalCount: logical,
	})
	if err != nil {
		return nil, nil, err
	}
	defer directory.Release()
	bound, err := rootpublication.BindDependencyDirectoryV2(resources, directory)
	if err != nil {
		return nil, nil, err
	}
	return bound, retired, nil
}

// The generation registry keeps both the exact directory root's reclaim
// frontier and its mmap alive. It is not a permanent vacuum exclusion: vacuum
// may install a rebound successor while older readers retain this generation.
// The caller must hold rootReuseMu while acquiring this lease.
func (db *DB) pinDependencyDirectoryV2(idx *indexGen, sequence uint64, ref rootpublication.DependencyDirectoryRefV2) (*rootpublication.DependencyDirectoryV2, error) {
	if db == nil || idx == nil || idx.pager == nil || sequence == 0 {
		return nil, rootpublication.ErrResourceOwnership
	}
	registryID := idx.registry.Register(sequence)
	release := func() {
		idx.registry.Unregister(registryID)
		if db.idx.Load() != idx {
			db.maybeReleaseRetiredIndex(idx)
		}
	}
	directory, err := rootpublication.NewDependencyDirectoryV2(idx.pager, ref, release)
	if err != nil {
		release()
		return nil, err
	}
	return directory, nil
}
