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

const nativeBuildBatchEntries = 256

// nativeOrderedRootBuilder is the existing physical group operation, with no
// publication or callback registration. The production owner is always the
// original complete-cut RootPublicationBuildGroup.
type nativeOrderedRootBuilder interface {
	BuildNativeOrderedRoot(backenddb.OrderedRootDeltaBatchPublishInput) (uint64, error)
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
	refund   *cowbtree.OwnedMapRefund
	// Match the existing batch's bounded small-file path. Flush before a
	// fifth distinct pointer file so this scratch never allocates a file map.
	pointerFiles [4]uint32
	pointerCount int
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
		delta: batch.NewOwnedBounded(nil, int(^uint(0)>>1), nativeBuildBatchEntries),
		root:  source.baseRoot, refund: refund,
	}, nil
}

func nativeDomainBuildCharge(maxSources int) (uint64, error) {
	ptr := uint64(unsafe.Sizeof(uintptr(0)))
	if maxSources <= 0 || uint64(maxSources) > ^uint64(0)/ptr {
		return 0, memtable.ErrCOWCapacity
	}
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeDomainBuild{}))) +
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
func (b *nativeDomainBuild) materialize(group nativeOrderedRootBuilder) (uint64, error) {
	if b.failure != nil {
		return 0, b.failure
	}
	if b.started || group == nil {
		return 0, backenddb.ErrNativeSemanticUnsupported
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
		b.buildSource(group, b.roots[i])
	}
	if b.failure == nil && b.source.pending != nil {
		b.buildSource(group, b.source.pending)
	}
	clear(b.roots)
	b.roots = b.roots[:0]
	b.delta.Reset()
	clear(b.pointerFiles[:])
	b.pointerCount = 0
	if b.failure != nil {
		return 0, b.failure
	}
	b.complete = true
	return b.root, nil
}

func (b *nativeDomainBuild) buildSource(group nativeOrderedRootBuilder, root *memtable.COWRoot) {
	root.ScanOwned(func(row memtable.COWRecord) bool {
		key := nativeBorrowedStringBytes(row.Key)
		switch {
		case row.Flags&node.FlagTombstone != 0:
			b.failure = b.delta.AppendDeleteViewTrustedSortedUniqueWithRevision(key, row.Revision)
		case row.Flags&node.FlagPointer != 0:
			if !page.IsValueLogFileID(row.Ptr.FileID) {
				b.failure = backenddb.ErrNativeSemanticUnsupported
				break
			}
			known := false
			for i := 0; i < b.pointerCount; i++ {
				known = known || b.pointerFiles[i] == row.Ptr.FileID
			}
			if !known {
				if b.pointerCount == len(b.pointerFiles) {
					b.flush(group)
					if b.failure != nil {
						return false
					}
				}
				b.pointerFiles[b.pointerCount] = row.Ptr.FileID
				b.pointerCount++
			}
			b.delta.AppendPointerViewNoTouchTrustedSortedWithRevision(key, row.Ptr, row.Revision)
			b.delta.NoteTouchedValueLogFileID(row.Ptr.FileID)
		default:
			b.failure = b.delta.AppendViewTrustedSortedUniqueWithRevision(key, nativeBorrowedStringBytes(row.Value), row.Revision)
		}
		if b.failure == nil && b.delta.Len() == nativeBuildBatchEntries {
			b.flush(group)
		}
		return b.failure == nil
	})
	if b.failure == nil {
		b.flush(group)
	}
}

func (b *nativeDomainBuild) flush(group nativeOrderedRootBuilder) {
	if b.delta.IsEmpty() {
		return
	}
	next, err := group.BuildNativeOrderedRoot(backenddb.OrderedRootDeltaBatchPublishInput{
		BaseRoot: b.root, Delta: b.delta, StoragePolicy: b.source.storagePolicy,
	})
	if err == nil {
		b.root = next
	}
	b.failure = err
	b.delta.Reset() // no borrowed entry survives synchronous physical apply
	clear(b.pointerFiles[:])
	b.pointerCount = 0
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
	b.delta.Close()
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
