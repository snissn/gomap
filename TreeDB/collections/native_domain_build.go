package collections

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	nativeBuildBatchEntries    = 256
	nativeBuildMaxBatchEntries = 131072
)

// nativeOrderedRootBuilder is the existing physical group operation, with no
// publication or callback registration. The production owner is always the
// original complete-cut RootPublicationBuildGroup.
type nativeOrderedRootBuilder interface {
	BuildNativeOrderedRoot(backenddb.OrderedRootDeltaBatchPublishInput) (uint64, error)
}

type nativeOrderedRootColdScanner interface {
	BuildNativeOrderedRootColdScan(backenddb.OrderedRootColdScanPublishInput) (uint64, error)
}

type nativeSystemRootBuilder interface {
	BuildNativeSystemDelta(*batch.Batch) error
}

var _ nativeOrderedRootBuilder = (*backenddb.RootPublicationBuildGroup)(nil)

// nativeDomainBuild owns one captured descriptor edge and bounded temporary
// batch/source scratch. Its enclosing original build Work adopts it before any
// fallible materialization. It owns no engine, current metadata, or group edge.
// The first materialization failure is terminal on this exact original ledger.
type nativeDomainBuild struct {
	source   *nativeDomainState
	roots    []*memtable.COWRoot
	delta    *batch.Batch
	root     uint64
	failure  error
	started  bool
	complete bool
	catalog  bool
	refund   *cowbtree.OwnedMapRefund
}

// newNativeDomainBuild admits all temporary storage before allocating it.
// allocation is the original maintenance capability (or a split of an original
// preaccepted build promise); it never acquires producer capacity for traversal.
func newNativeDomainBuild(source *nativeDomainState, maxSources int, admit cowbtree.OwnedMapAdmit) (*nativeDomainBuild, error) {
	if source == nil || maxSources <= 0 || admit == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	charge, err := nativeDomainBuildCharge(maxSources)
	if err != nil {
		return nil, err
	}
	lease, err := admit(charge)
	if err != nil {
		return nil, err
	}
	refund := new(cowbtree.OwnedMapRefund)
	refund.Init(lease)
	(nativeDomainLifetime{}).Retain(source)
	return &nativeDomainBuild{
		source: source, roots: make([]*memtable.COWRoot, 0, maxSources),
		delta: batch.NewBoundedRetainingLargeEntries(nil, int(^uint(0)>>1), nativeBuildBatchEntries),
		root:  source.baseRoot, refund: refund,
	}, nil
}

func nativeDomainBuildCharge(maxSources int) (uint64, error) {
	ptr := uint64(unsafe.Sizeof(uintptr(0)))
	if maxSources <= 0 || uint64(maxSources) > ^uint64(0)/ptr {
		return 0, memtable.ErrCOWCapacity
	}
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(page.ValuePtr{}))) + // one synchronous physical append result
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeDomainBuild{}))) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{}))) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(batch.Batch{}))) +
		memtable.COWAllocationCharge(nativeBuildBatchEntries*uint64(unsafe.Sizeof(batch.Entry{})))
	scratch := memtable.COWAllocationCharge(uint64(maxSources) * ptr)
	if scratch > ^uint64(0)-charge {
		return 0, memtable.ErrCOWCapacity
	}
	return charge + scratch, nil
}

// materialize applies older captured sources before newer sources and the
// active root, preserving final replacements/deletions and exact revisions.
// Each synchronous group call receives at most nativeBuildBatchEntries rows
// unless the caller supplies a larger preadmitted shared delta batch.
// The descriptor edge covers every borrowed byte until Reset/Close scrubs it.
func (b *nativeDomainBuild) materialize(group nativeOrderedRootBuilder) (uint64, error) {
	return b.materializeWithCatalogAndDelta(group, nil, b.delta, nativeBuildBatchEntries)
}

func (b *nativeDomainBuild) materializeWithDelta(group nativeOrderedRootBuilder, delta *batch.Batch, batchEntries int) (uint64, error) {
	if delta == nil || batchEntries <= 0 {
		delta = b.delta
		batchEntries = nativeBuildBatchEntries
	}
	return b.materializeWithCatalogAndDelta(group, nil, delta, batchEntries)
}

// names is borrowed synchronously from the same captured engine that owns this
// build. It is never stored on the ledger or used for ordinary row domains.
func (b *nativeDomainBuild) materializeWithCatalog(group nativeOrderedRootBuilder, names *cowbtree.OwnedMap[string, *nativeCollectionState]) (uint64, error) {
	return b.materializeWithCatalogAndDelta(group, names, b.delta, nativeBuildBatchEntries)
}

func (b *nativeDomainBuild) materializeWithCatalogAndDelta(group nativeOrderedRootBuilder, names *cowbtree.OwnedMap[string, *nativeCollectionState], delta *batch.Batch, batchEntries int) (uint64, error) {
	if b.failure != nil {
		return 0, b.failure
	}
	if b.started || group == nil || delta == nil || batchEntries <= 0 {
		return 0, backenddb.ErrNativeSemanticUnsupported
	}
	if b.catalog {
		if _, ok := group.(nativeSystemRootBuilder); !ok {
			return 0, backenddb.ErrNativeSemanticUnsupported
		}
	}
	b.started = true
	for positions := b.source.captured; positions != nil; positions = positions.older {
		if len(b.roots) == cap(b.roots) {
			b.failure = memtable.ErrCOWCapacity
			break
		}
		b.roots = append(b.roots, positions.source.root)
	}
	coldScanner, hasColdScanner := group.(nativeOrderedRootColdScanner)
	for i := len(b.roots) - 1; i >= 0 && b.failure == nil; i-- {
		if !b.catalog && b.root == 0 && hasColdScanner {
			b.buildColdScan(coldScanner, b.roots[i])
		} else {
			b.buildSource(group, b.roots[i], names, delta, batchEntries)
		}
	}
	if b.failure == nil && b.source.pending != nil {
		if !b.catalog && b.root == 0 && hasColdScanner {
			b.buildColdScan(coldScanner, b.source.pending)
		} else {
			b.buildSource(group, b.source.pending, names, delta, batchEntries)
		}
	}
	clear(b.roots)
	b.roots = b.roots[:0]
	delta.Reset()
	if b.failure != nil {
		return 0, b.failure
	}
	b.complete = true
	return b.root, nil
}

func (b *nativeDomainBuild) buildColdScan(coldScanner nativeOrderedRootColdScanner, root *memtable.COWRoot) {
	entryCount := root.Len()
	if entryCount <= 0 {
		return
	}
	checkInlineThreshold := b.source.name == "primary" || b.source.name == "templates"
	next, err := coldScanner.BuildNativeOrderedRootColdScan(backenddb.OrderedRootColdScanPublishInput{
		StoragePolicy:        b.source.storagePolicy,
		EntryCount:           entryCount,
		CheckInlineThreshold: checkInlineThreshold,
		ImmutableSlices:      true,
		Scan: func(emit func(key, val []byte, ptr page.ValuePtr, flags byte, rev page.EntryRevision) error) error {
			var scanErr error
			root.ScanOwned(func(row memtable.COWRecord) bool {
				if row.Flags&node.FlagTombstone != 0 {
					return true
				}
				key := nativeBorrowedStringBytes(row.Key)
				if row.Flags&node.FlagPointer != 0 {
					scanErr = emit(key, nil, row.Ptr, node.FlagPointer, row.Revision)
					return scanErr == nil
				}
				value := nativeBorrowedStringBytes(row.Value)
				scanErr = emit(key, value, page.ValuePtr{}, node.FlagInline, row.Revision)
				return scanErr == nil
			})
			return scanErr
		},
	})
	if err != nil {
		b.failure = err
		return
	}
	b.root = next
}

func (b *nativeDomainBuild) buildSource(group nativeOrderedRootBuilder, root *memtable.COWRoot, names *cowbtree.OwnedMap[string, *nativeCollectionState], delta *batch.Batch, batchEntries int) {
	placement, hasPlacement := group.(interface {
		NativeInlineThresholdForKey([]byte) int
		AppendNativeValueLogValues([][]byte) ([]page.ValuePtr, error)
	})
	checkInlineThreshold := !b.catalog && hasPlacement && (b.source.name == "primary" || b.source.name == "templates")
	root.ScanOwned(func(row memtable.COWRecord) bool {
		key := nativeBorrowedStringBytes(row.Key)
		switch {
		case row.Flags&node.FlagTombstone != 0:
			b.failure = delta.AppendDeleteViewTrustedSortedUniqueWithRevision(key, row.Revision)
		case row.Flags&node.FlagPointer != 0:
			if b.catalog || !page.IsValueLogFileID(row.Ptr.FileID) {
				b.failure = backenddb.ErrNativeSemanticUnsupported
				break
			}
			delta.AppendPointerViewNoTouchTrustedSortedWithRevision(key, row.Ptr, row.Revision)
		default:
			value := nativeBorrowedStringBytes(row.Value)
			if b.catalog && names != nil {
				if meta := nativeCatalogProjectionMeta(names, row); meta != nil {
					value, b.failure = nativeCatalogProjectedValue(meta, row)
					if b.failure != nil {
						return false
					}
				}
			}
			threshold := max(0, page.PageSize-node.NodeHeaderSize-node.DirectoryEntrySize-7-page.EntryRevisionSize-len(key))
			if checkInlineThreshold {
				threshold = min(threshold, placement.NativeInlineThresholdForKey(key))
			}
			if !b.catalog && len(value) > threshold {
				if !hasPlacement {
					b.failure = backenddb.ErrValueLogAppenderUnavailable
					return false
				}
				var values [1][]byte
				values[0] = value
				ptrs, err := placement.AppendNativeValueLogValues(values[:])
				if err != nil {
					b.failure = err
					return false
				}
				delta.AppendPointerViewNoTouchTrustedSortedWithRevision(key, ptrs[0], row.Revision)
			} else {
				b.failure = delta.AppendViewTrustedSortedUniqueWithRevision(key, value, row.Revision)
			}
		}
		if b.failure == nil && delta.Len() == batchEntries {
			b.flushDelta(group, delta)
		}
		return b.failure == nil
	})
	if b.failure == nil {
		b.flushDelta(group, delta)
	}
}

func (b *nativeDomainBuild) flushDelta(group nativeOrderedRootBuilder, delta *batch.Batch) {
	if delta.IsEmpty() {
		return
	}
	var err error
	if b.catalog {
		err = group.(nativeSystemRootBuilder).BuildNativeSystemDelta(delta)
	} else {
		var next uint64
		next, err = group.BuildNativeOrderedRoot(backenddb.OrderedRootDeltaBatchPublishInput{
			BaseRoot: b.root, Delta: delta, StoragePolicy: b.source.storagePolicy,
		})
		if err == nil {
			b.root = next
		}
	}
	b.failure = err
	delta.Reset() // no borrowed entry survives synchronous physical apply
}

// finish runs only from original Work outside publication/cut gates. Failed
// original scratch/descriptor/charge stay parked, without destructor replay.
// A successful completion refunds after the enclosing Work detaches this cell.
func (b *nativeDomainBuild) finish() (done cowbtree.OwnedMapCompletion, err error) {
	if b == nil {
		return done, nil
	}
	if b.failure != nil {
		return done, b.failure
	}
	b.delta.CloseWithoutPooling() // The original build owns this exact admitted backing.
	b.delta = nil
	clear(b.roots)
	b.roots = nil
	done, err = (nativeDomainLifetime{}).Release(b.source)
	if err != nil {
		b.failure = err
		return done, err
	}
	b.source = nil
	refund := b.refund
	b.refund = nil
	done.Append(refund.Completion())
	return done, nil
}

// Like the existing immutable COW table adapter, this borrows read-only bytes
// from generation-owned strings. Only batch/view builders which never mutate
// input receive them; the original descriptor remains owned through scrub.
func nativeBorrowedStringBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
