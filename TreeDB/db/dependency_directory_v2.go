package db

import (
	"errors"
	"fmt"
	"os"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/zipper"
)

// prepareDependencyDirectoryV2 runs while publication owns rootReuseMu and
// before PrepareCOWCandidateRetiringV1 freezes the allocator. The ordinary
// ordered-root zipper supplies changed-path retirement for this third tree.
func (db *DB) prepareDependencyDirectoryV2(idx *indexGen, sequence uint64, baseResources, resources *rootpublication.StableResourceSet) (result *rootpublication.StableResourceSet, retiredOut []uint64, err error) {
	if db == nil || idx == nil || idx != db.idx.Load() || sequence == 0 {
		return nil, nil, errors.New("invalid dependency directory index generation")
	}
	if resources == nil {
		var err error
		resources, err = rootpublication.NewStableResourceSetBuilder().Freeze()
		if err != nil {
			return nil, nil, err
		}
		defer resources.Release()
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
	// Count persisted changed keys/values (including deletion keys), not
	// comparison encodings or binding checks of unchanged physical records.
	var encodedBytes, encodedRecords uint64
	physical, logical, err := rootpublication.WalkDependencyDirectoryChangesV2(resources, base, func(key, value []byte, deleted bool) error {
		encodedBytes += uint64(len(key) + len(value))
		encodedRecords++
		if deleted {
			return delta.Delete(key)
		}
		return delta.Set(key, value)
	})
	if err != nil {
		return nil, nil, err
	}
	var alloc zipper.PageAllocator = idx.allocator
	var bankAlloc *primaryDependencyAllocatorV5
	if idx.primary != nil {
		bankAlloc, err = newPrimaryDependencyAllocatorV5(idx)
		if err != nil {
			return nil, nil, err
		}
		defer func() {
			cleanup := bankAlloc.release()
			if cleanup != nil {
				if result != nil {
					result.Release()
					result = nil
				}
				err = errors.Join(err, cleanup)
			}
		}()
		alloc = bankAlloc
	}
	var retired []uint64
	var metrics adaptive.Metrics
	var pagesWritten uint64
	if root == 0 && delta.IsEmpty() {
		iter := newOrderedRootDeltaBatchIterator(delta, false)
		defer iter.Close()
		root, err = bulk.BuildWithOptions(iter, alloc, idx.pager, bulk.BuildOptions{LeafPrefixCompression: true})
		pagesWritten = 1 // The sole empty initial leaf.
	} else {
		root, retired, metrics, err = db.publishOrderedRootDeltaBatchWithAllocator(idx, root, delta, orderedRootPublishOptions{
			leafPrefixCompression: true,
		}, alloc, alloc, false)
		pagesWritten = uint64(metrics.ZipperPagerLeafPagesWritten + metrics.ZipperInternalPagesWritten)
	}
	db.durableRootDirectoryBytesEncoded.Add(encodedBytes)
	db.durableRootDirectoryRecordsEncoded.Add(encodedRecords)
	db.durableRootDirectoryPagesWritten.Add(pagesWritten)
	if err != nil {
		return nil, nil, fmt.Errorf("apply dependency directory: %w", err)
	}
	if bankAlloc != nil {
		if err = bankAlloc.seal(root); err != nil {
			return nil, nil, err
		}
		retired = nil
	}
	directory, err := db.pinDependencyDirectoryV2(idx, sequence, rootpublication.DependencyDirectoryRefV2{
		RootPageID: root, PhysicalCount: physical, LogicalCount: logical,
	}, idx.pager.PageCount())
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

func (db *DB) dependencyDirectoryValidatorV2(idx *indexGen) durableDirectoryValidatorV2 {
	return func(record rootpublication.DurableRootRecordV1) (*rootpublication.StableResourceSet, error) {
		directory, err := db.pinDependencyDirectoryV2(idx, record.CommitSeq, record.Directory, record.TotalPages)
		if err != nil {
			return nil, err
		}
		defer directory.Release()
		return db.recoverDependencyDirectoryResourcesV2(directory)
	}
}

// The generation registry keeps both the exact directory root's reclaim
// frontier and its mmap alive. It is not a permanent vacuum exclusion: vacuum
// may install a rebound successor while older readers retain this generation.
// The caller must hold rootReuseMu while acquiring this lease.
func (db *DB) pinDependencyDirectoryV2(idx *indexGen, sequence uint64, ref rootpublication.DependencyDirectoryRefV2, totalPages uint64) (*rootpublication.DependencyDirectoryV2, error) {
	if db == nil || idx == nil || idx.pager == nil || sequence == 0 {
		return nil, rootpublication.ErrResourceOwnership
	}
	if idx.primary != nil && primaryarena.IsPage(ref.RootPageID) {
		return db.pinPrimaryDependencyV5(idx, ref, idx.primary.Pager().PageCount())
	}
	registryID := idx.registry.Register(sequence)
	release := func() {
		idx.registry.Unregister(registryID)
		if db.idx.Load() != idx {
			db.maybeReleaseRetiredIndex(idx)
		}
	}
	directory, err := rootpublication.NewDependencyDirectoryV2(idx.pager, ref, totalPages, release)
	if err != nil {
		release()
		return nil, err
	}
	return directory, nil
}

func (db *DB) recoverDependencyDirectoryResourcesV2(directory *rootpublication.DependencyDirectoryV2) (*rootpublication.StableResourceSet, error) {
	return rootpublication.RecoverDependencyDirectoryV2(directory, func(entry rootpublication.DependencyManifestEntryV1) (*rootpublication.StableResourceSet, error) {
		return db.validateDurableDependencyEntriesV1([]rootpublication.DependencyManifestEntryV1{entry}, true)
	}, func(file *os.File, entry rootpublication.DependencyManifestEntryV1, obligation rootpublication.StableLogicalObligation) error {
		switch entry.Kind {
		case rootpublication.ResourceColumnAsset, rootpublication.ResourceTypedColumnAsset, rootpublication.ResourceVectorGraphPack:
			entry.LogicalObligations = []rootpublication.StableLogicalObligation{obligation}
			return validateDurableDependencyContentV1(file, entry)
		case rootpublication.ResourceDictionary, rootpublication.ResourceTemplate, rootpublication.ResourceOuterLeafPack, rootpublication.ResourceOuterLeafManifest:
			// Directory decoding already checks exact owner/frontier/field.
			return nil
		default:
			return rootpublication.ErrResourceConflict
		}
	})
}

func (db *DB) primaryDependencyDirectoryValidatorV5(idx *indexGen) durablePrimaryDirectoryValidatorV5 {
	return func(record rootpublication.DurablePrimaryRootRecordV5) (*rootpublication.StableResourceSet, error) {
		directory, e := db.pinPrimaryDependencyV5(idx, record.Record.Directory, record.Primary.ArenaHighWater)
		if e != nil {
			return nil, e
		}
		defer directory.Release()
		return db.recoverDependencyDirectoryResourcesV2(directory)
	}
}
