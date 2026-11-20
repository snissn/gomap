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
	nativeBuildBatchEntries = 256
	// Reserve bounded cohort views, routing and per-lane append scratch
	// from spare physical-progress capacity before the startup append.
	// The shared appender routes at most eight records to distinct writers.
	nativeBuildInitialAppendScratch = 4096
)

// nativeOrderedRootBuilder is the existing physical group operation, with no
// publication or callback registration. The production owner is always the
// original complete-cut RootPublicationBuildGroup.
type nativeOrderedRootBuilder interface {
	BuildNativeOrderedRoot(backenddb.OrderedRootDeltaBatchPublishInput) (uint64, error)
}

type nativeSystemRootBuilder interface {
	BuildNativeSystemDelta(*batch.Batch) error
}

type nativeBuildValuePlacement interface {
	NativeInlineThresholdForKey([]byte) int
	AppendNativeValueLogValues([][]byte) ([]page.ValuePtr, error)
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
// Each synchronous group call receives at most nativeBuildBatchEntries rows.
// The descriptor edge covers every borrowed byte until Reset/Close scrubs it.
// names is borrowed synchronously from the same captured engine that owns this
// build. It is never stored on the ledger or used for ordinary row domains.
func (b *nativeDomainBuild) materialize(group nativeOrderedRootBuilder, names *cowbtree.OwnedMap[string, *nativeCollectionState], reserveStartup func(uint64) (*memtable.COWExternalLease, error)) (uint64, error) {
	if b.failure != nil {
		return 0, b.failure
	}
	if b.started || group == nil {
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
	for i := len(b.roots) - 1; i >= 0 && b.failure == nil; i-- {
		b.buildSource(group, b.roots[i], names, reserveStartup)
	}
	if b.failure == nil && b.source.pending != nil {
		b.buildSource(group, b.source.pending, names, reserveStartup)
	}
	clear(b.roots)
	b.roots = b.roots[:0]
	b.delta.Reset()
	if b.failure != nil {
		return 0, b.failure
	}
	b.complete = true
	return b.root, nil
}

func (b *nativeDomainBuild) buildSource(group nativeOrderedRootBuilder, root *memtable.COWRoot, names *cowbtree.OwnedMap[string, *nativeCollectionState], reserveStartup func(uint64) (*memtable.COWExternalLease, error)) {
	placement, hasPlacement := group.(nativeBuildValuePlacement)
	usePlacementThreshold := hasPlacement && (b.source.name == "primary" || b.source.name == "templates")
	startup, checkStartup := group.(interface{ ClaimNativeValueLogInitialAppendWidth() int })
	checkStartup = checkStartup && reserveStartup != nil && !b.catalog && b.source.name == "primary"
	var initial *[backenddb.NativeValueLogInitialAppendMax]memtable.COWRecord
	var startupLease *memtable.COWExternalLease
	var initialCount, initialWidth int
	appendInitial := func() bool {
		b.appendInitialValues(placement, initial[:initialCount])
		clear(initial[:initialCount])
		initial = nil
		startupLease.Close()
		initialCount, initialWidth = 0, 0
		return b.flushFull(group)
	}

	root.ScanOwned(func(row memtable.COWRecord) bool {
		key := nativeBorrowedStringBytes(row.Key)
		if !b.catalog && row.Flags&(node.FlagTombstone|node.FlagPointer) == 0 {
			threshold := max(0, page.PageSize-node.NodeHeaderSize-node.DirectoryEntrySize-7-page.EntryRevisionSize-len(key))
			if usePlacementThreshold {
				threshold = min(threshold, placement.NativeInlineThresholdForKey(key))
			}
			if len(row.Value) > threshold {
				if !hasPlacement {
					b.failure = backenddb.ErrValueLogAppenderUnavailable
					return false
				}
				if checkStartup {
					checkStartup = false
					width := min(startup.ClaimNativeValueLogInitialAppendWidth(), nativeBuildBatchEntries-b.delta.Len())
					if width > 1 && width <= backenddb.NativeValueLogInitialAppendMax {
						if lease, err := reserveStartup(nativeBuildInitialAppendScratch); err == nil {
							startupLease = lease
							initial = new([backenddb.NativeValueLogInitialAppendMax]memtable.COWRecord)
							initialWidth = width
						}
					}
				}
				if initialWidth > 1 {
					initial[initialCount] = row
					initialCount++
					if initialCount < initialWidth {
						return true
					}
					return appendInitial()
				}
				values := [1][]byte{nativeBorrowedStringBytes(row.Value)}
				ptrs, err := placement.AppendNativeValueLogValues(values[:])
				if err != nil {
					b.failure = err
					return false
				}
				b.delta.AppendPointerViewNoTouchTrustedSortedWithRevision(key, ptrs[0], row.Revision)
				return b.flushFull(group)
			}
		}
		if initialCount != 0 && !appendInitial() {
			return false
		}
		switch {
		case row.Flags&node.FlagTombstone != 0:
			b.failure = b.delta.AppendDeleteViewTrustedSortedUniqueWithRevision(key, row.Revision)
		case row.Flags&node.FlagPointer != 0:
			if b.catalog || !page.IsValueLogFileID(row.Ptr.FileID) {
				b.failure = backenddb.ErrNativeSemanticUnsupported
				break
			}
			b.delta.AppendPointerViewNoTouchTrustedSortedWithRevision(key, row.Ptr, row.Revision)
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
			b.failure = b.delta.AppendViewTrustedSortedUniqueWithRevision(key, value, row.Revision)
		}
		return b.flushFull(group)
	})
	if b.failure == nil && initialCount != 0 {
		appendInitial()
	}
	if b.failure == nil {
		b.flush(group)
	}
}

func (b *nativeDomainBuild) appendInitialValues(placement nativeBuildValuePlacement, rows []memtable.COWRecord) {
	var values [backenddb.NativeValueLogInitialAppendMax][]byte
	for i, row := range rows {
		values[i] = nativeBorrowedStringBytes(row.Value)
	}
	ptrs, err := placement.AppendNativeValueLogValues(values[:len(rows)])
	clear(values[:len(rows)])
	if err != nil {
		b.failure = err
		return
	}
	for i, row := range rows {
		b.delta.AppendPointerViewNoTouchTrustedSortedWithRevision(nativeBorrowedStringBytes(row.Key), ptrs[i], row.Revision)
	}
}

func (b *nativeDomainBuild) flushFull(group nativeOrderedRootBuilder) bool {
	if b.failure == nil && b.delta.Len() == nativeBuildBatchEntries {
		b.flush(group)
	}
	return b.failure == nil
}

func (b *nativeDomainBuild) flush(group nativeOrderedRootBuilder) {
	if b.delta.IsEmpty() {
		return
	}
	var err error
	if b.catalog {
		err = group.(nativeSystemRootBuilder).BuildNativeSystemDelta(b.delta)
	} else {
		var next uint64
		next, err = group.BuildNativeOrderedRoot(backenddb.OrderedRootDeltaBatchPublishInput{
			BaseRoot: b.root, Delta: b.delta, StoragePolicy: b.source.storagePolicy,
		})
		if err == nil {
			b.root = next
		}
	}
	b.failure = err
	b.delta.Reset() // no borrowed entry survives synchronous physical apply
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
