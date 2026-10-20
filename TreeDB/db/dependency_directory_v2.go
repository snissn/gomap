package db

import (
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
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
	metadata := idx.metadata

	// Actual generation producers must supply the selected original owner.
	// The utility's generic path is reserved for unowned diagnostics.
	if metadata == nil || (resources != nil && resources.MetadataOwner() != metadata) || (baseResources != nil && baseResources.MetadataOwner() != metadata) {
		return nil, nil, rootpublication.ErrResourceOwnership
	}
	if resources == nil {
		var err error
		builder, buildErr := rootpublication.NewStableResourceSetBuilderWithMetadata(metadata)
		if buildErr != nil {
			return nil, nil, buildErr
		}
		resources, err = builder.Freeze()
		if err != nil {
			builder.Abandon()
		}
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
	var delta *batch.Batch
	var encodedBacking []byte
	var entries, payload uint64
	_, _, err = rootpublication.WalkDependencyDirectoryChangesV2(resources, base, func(key, value []byte, _ bool) error {
		if entries == uint64(int(^uint(0)>>1)) || uint64(len(key)) > ^uint64(0)-payload || uint64(len(value)) > ^uint64(0)-payload-uint64(len(key)) {
			return retainedalloc.ErrCapacity
		}
		entries++
		payload += uint64(len(key)) + uint64(len(value))
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	width := uint64(unsafe.Sizeof(batch.Entry{}))
	header := uint64(unsafe.Sizeof(batch.Batch{}))
	if payload > uint64(int(^uint(0)>>1)) || entries > (^uint64(0)-header)/width || payload > ^uint64(0)-header-entries*width {
		return nil, nil, retainedalloc.ErrCapacity
	}
	charge := retainedalloc.AllocationCharge(header)
	entryCharge := retainedalloc.AllocationCharge(entries * width)
	payloadCharge := retainedalloc.AllocationCharge(payload)
	if entryCharge > ^uint64(0)-charge || payloadCharge > ^uint64(0)-charge-entryCharge {
		return nil, nil, retainedalloc.ErrCapacity
	}
	charge += entryCharge + payloadCharge
	if err = metadata.AddPending(charge); err != nil {
		return nil, nil, err
	}
	encodedBacking = make([]byte, int(payload))
	delta = batch.NewBoundedRetainingLargeEntries(nil, page.PageSize, int(entries))
	defer func() {
		delta.CloseWithoutPooling()
		delta = nil
		clear(encodedBacking)
		encodedBacking = nil
		metadata.RemovePending(charge)
	}()
	var encodedOffset int
	// Count persisted changed keys/values (including deletion keys), not
	// comparison encodings or binding checks of unchanged physical records.
	var encodedBytes, encodedRecords uint64
	physical, logical, err := rootpublication.WalkDependencyDirectoryChangesV2(resources, base, func(key, value []byte, deleted bool) error {
		encodedBytes += uint64(len(key) + len(value))
		encodedRecords++
		if len(key) > len(encodedBacking)-encodedOffset || len(value) > len(encodedBacking)-encodedOffset-len(key) || delta.Len() >= delta.EntriesCap() {
			return retainedalloc.ErrCapacity
		}
		keyEnd := encodedOffset + len(key)
		valueEnd := keyEnd + len(value)
		copy(encodedBacking[encodedOffset:keyEnd], key)
		copy(encodedBacking[keyEnd:valueEnd], value)
		key = encodedBacking[encodedOffset:keyEnd:keyEnd]
		value = encodedBacking[keyEnd:valueEnd:valueEnd]
		encodedOffset = valueEnd
		if deleted {
			return delta.DeleteView(key)
		}
		return delta.SetView(key, value)
	})
	if err != nil {
		return nil, nil, err
	}
	var alloc zipper.PageAllocator = idx.allocator

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
			return nil, fmt.Errorf("pin durable dependency directory: %w", err)
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

	return db.pinDataDependencyDirectoryV6(idx, sequence, ref, totalPages)
}

func (db *DB) recoverDependencyDirectoryResourcesV2(directory *rootpublication.DependencyDirectoryV2) (_ *rootpublication.StableResourceSet, retErr error) {
	defer func() {
		if retErr != nil {
			retErr = fmt.Errorf("dependency directory recovery: %w", retErr)
		}
	}()
	return rootpublication.RecoverDependencyDirectoryV2(directory, func(entry rootpublication.DependencyManifestEntryV1) (*rootpublication.StableResourceSet, error) {
		return db.validateDurableDependencyEntriesWithMetadataV1([]rootpublication.DependencyManifestEntryV1{entry}, true, directory.MetadataOwner())
	}, func(file *os.File, entry rootpublication.DependencyManifestEntryV1, obligation rootpublication.StableLogicalObligation) error {
		switch entry.Kind {
		case rootpublication.ResourceColumnAsset, rootpublication.ResourceTypedColumnAsset, rootpublication.ResourceVectorGraphPack:
			entry.LogicalObligations = []rootpublication.StableLogicalObligation{obligation}
			return validateDurableDependencyContentV1(file, entry)
		case rootpublication.ResourceDictionary, rootpublication.ResourceTemplate, rootpublication.ResourceOuterLeafPack, rootpublication.ResourceOuterLeafManifest:
			// Directory decoding already checks exact owner/frontier/field.
			return nil
		default:
			return fmt.Errorf("%w: directory recovered logical kind=%q path=%q", rootpublication.ErrResourceConflict, entry.Kind, entry.DiagnosticPath)
		}
	})
}
