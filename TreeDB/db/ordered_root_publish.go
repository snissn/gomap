package db

import (
	"bytes"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/TreeDB/zipper"
)

type orderedRootPublishPlan uint8

const (
	orderedRootPublishPlanColdBuild orderedRootPublishPlan = iota
	orderedRootPublishPlanWarmFallbackRebuild
	orderedRootPublishPlanWarmNativeApply
)

// orderedRootDeltaBatchInlineThreshold is intentionally not the page/value-log
// placement threshold. These batches are transient root-local mutation streams
// consumed by zipper.Apply; they do not decide durable value placement. Large
// collection documents must remain valid here because the destination ordered
// root policy decides whether rebuilt leaves live in pager pages or value-log
// LeafRefs. Pointer entries still flow through SetPointer, and non-stable inline
// iterators may copy values into this short-lived batch, so callers should keep
// delta streams bounded rather than using this as a bulk-load accumulator.
var orderedRootDeltaBatchInlineThreshold = int(^uint(0) >> 1)

const orderedRootOptimisticSystemDeltaRebaseMaxAttempts = 4

const orderedRootDeltaBatchGroupParallelApplyMinRoots = 2

var orderedRootReadOnlyPrepareResultPool = sync.Pool{
	New: func() any {
		return new(zipper.ReadOnlyPrepareResult)
	},
}

func acquireOrderedRootReadOnlyPrepareResult() *zipper.ReadOnlyPrepareResult {
	result, _ := orderedRootReadOnlyPrepareResultPool.Get().(*zipper.ReadOnlyPrepareResult)
	if result == nil {
		return new(zipper.ReadOnlyPrepareResult)
	}
	return result
}

func releaseOrderedRootReadOnlyPrepareResult(result *zipper.ReadOnlyPrepareResult) {
	if result == nil {
		return
	}
	result.ResetForReuse()
	orderedRootReadOnlyPrepareResultPool.Put(result)
}

type orderedRootPublishStats struct {
	warmAttempts                          uint64
	warmNativeApplyAttempts               uint64
	warmRebuildFallbacks                  uint64
	warmPreservedPages                    uint64
	warmRewrittenPages                    uint64
	collectionRootDescriptorBaseEntries   []orderedRootCollectionDescriptorEntry
	collectionRootDescriptorTargetEntries []orderedRootCollectionDescriptorEntry
}

type orderedRootPublishOptions struct {
	maxWarmDeltaOps              int
	leafPrefixCompression        bool
	leafColumnar                 bool
	packedValuePtr               bool
	internalBaseDelta            bool
	outerLeavesInValueLog        bool
	leafPageLog                  bulk.LeafPageAppender
	applyOptions                 zipper.ApplyOptions
	readOnlyPrepareSummary       *zipper.ReadOnlyLeafSpanSummary
	readOnlyPrepareWorkerSummary *zipper.ReadOnlyLeafSpanWorkerRangeSummary
	readOnlyPrepareCallerResult  *zipper.ReadOnlyPrepareResult
	readOnlyPrepareNs            *uint64
	readOnlyPrepareAttempted     *bool
	readOnlyPrepareWorkerCount   int
}

type orderedRootDeltaBatchGroupApplyResult struct {
	idx                          int
	rootID                       uint64
	outputID                     preparedOutputID
	output                       *preparedOutputSnapshot
	outputPages                  uint64
	outputLeafLogPtrs            uint64
	pendingRetiredPages          []uint64
	metrics                      adaptive.Metrics
	readOnlyPrepareSummary       zipper.ReadOnlyLeafSpanSummary
	readOnlyPrepareWorkerSummary zipper.ReadOnlyLeafSpanWorkerRangeSummary
	readOnlyPrepareNs            uint64
	readOnlyPrepareAttempted     bool
	err                          error
	attempted                    bool
}

type preparedLeafLogOutputRecorder interface {
	notePreparedLeafLogPtr(page.LeafLogPtr)
}

type preparedRootApplyOutputCounter struct {
	inner    zipper.PageAllocator
	recorder preparedLeafLogOutputRecorder

	pages       atomic.Uint64
	leafLogPtrs atomic.Uint64
}

func (c *preparedRootApplyOutputCounter) Alloc(hint uint64) (uint64, error) {
	id, err := c.inner.Alloc(hint)
	if err != nil {
		return 0, err
	}
	c.pages.Add(1)
	return id, nil
}

func (c *preparedRootApplyOutputCounter) notePreparedLeafLogPtr(ptr page.LeafLogPtr) {
	if c.recorder != nil {
		c.recorder.notePreparedLeafLogPtr(ptr)
	}
	c.leafLogPtrs.Add(1)
}

func (c *preparedRootApplyOutputCounter) counts() (pages, leafLogPtrs uint64) {
	if c == nil {
		return 0, 0
	}
	return c.pages.Load(), c.leafLogPtrs.Load()
}

// OrderedRootStoragePolicy selects the physical storage policy for a published
// ordered root. The zero value keeps the DB-level default.
type OrderedRootStoragePolicy uint8

const (
	OrderedRootStorageDefault OrderedRootStoragePolicy = iota
	// OrderedRootStoragePagerLeaves stores root leaves in index.db pages. It is
	// the fast index policy and can use internal base-delta child encodings.
	OrderedRootStoragePagerLeaves
	// OrderedRootStorageValueLogLeaves stores root leaves as leaf-log records.
	// It is the compressed policy; leaf-log child pages use explicit LogRecordRef
	// entries instead of base-delta page-child encoding.
	OrderedRootStorageValueLogLeaves
)

type OrderedRootPublishInput struct {
	BaseRoot      uint64
	Iter          iterator.UnsafeIterator
	StoragePolicy OrderedRootStoragePolicy
}

// OrderedRootDeltaPublishInput describes a sorted root-local mutation stream.
// Unlike OrderedRootPublishInput, Iter contains only keys changed by this
// publish; omitted base-root keys are preserved.
type OrderedRootDeltaPublishInput struct {
	BaseRoot      uint64
	Iter          iterator.UnsafeIterator
	StoragePolicy OrderedRootStoragePolicy
}

// OrderedRootDeltaBatchPublishInput describes a sorted root-local mutation
// batch whose iterator materialization has already been done by the caller.
// Callers retain ownership of Delta and must keep any borrowed key/value views
// immutable until the publish call returns.
type OrderedRootDeltaBatchPublishInput struct {
	BaseRoot      uint64
	Delta         *batch.Batch
	StoragePolicy OrderedRootStoragePolicy
	// IncludeDeletedOnColdBuild preserves tombstones when BaseRoot is zero.
	// Most cold root builds can omit deletes because there is no base tree to
	// hide, but collection overlay roots need tombstones to mask older overlay
	// or base-root entries during reads.
	IncludeDeletedOnColdBuild bool
	// ParallelApply allows this root-local batch apply to run concurrently with
	// other opted-in roots in the same group before the final commit validation.
	// Callers should opt in only when root deltas are already materialized and
	// benchmarked as large enough to amortize goroutine and shared backend costs.
	ParallelApply bool
	// PrepareReadOnly runs the read-only leaf-span preparation pass before warm
	// root apply and records planning stats. It is observability/planning only;
	// it does not change publish output or enable parallel leaf execution.
	PrepareReadOnly bool
	// ReadOnlyPrepareResult, when non-nil, is both the reuse source and output
	// destination for this root's optional preparation metadata. It must be
	// owned by this input within the group; sharing one result pointer across
	// group inputs is rejected. It is ignored unless PrepareReadOnly is true.
	ReadOnlyPrepareResult *zipper.ReadOnlyPrepareResult
	// ReadOnlyPrepareWorkerCount, when positive with PrepareReadOnly, records an
	// allocation-free summary of deterministic leaf-span worker ranges for this
	// target worker count. It is observability/planning only.
	ReadOnlyPrepareWorkerCount int
}

func closeUnconsumedOrderedRootPublishIterators(ordered []OrderedRootPublishInput, consumed []bool) {
	for idx := range ordered {
		if idx < len(consumed) && consumed[idx] {
			continue
		}
		if ordered[idx].Iter != nil {
			_ = ordered[idx].Iter.Close()
		}
	}
}

func closeUnconsumedOrderedRootDeltaPublishIterators(ordered []OrderedRootDeltaPublishInput, consumed []bool) {
	for idx := range ordered {
		if idx < len(consumed) && consumed[idx] {
			continue
		}
		if ordered[idx].Iter != nil {
			_ = ordered[idx].Iter.Close()
		}
	}
}

// OrderedRootGroupSystemBuilder builds a target system-root iterator after the
// non-system roots in a group have been built. The rootIDs slice is ordered to
// match the OrderedRootPublishInput slice passed to
// PublishOrderedRootGroupWithSystemBuilder.
type OrderedRootGroupSystemBuilder func(rootIDs []uint64) (iterator.UnsafeIterator, error)

// OrderedRootGroupPreflight validates that a root group can still be applied.
// It runs while the DB write lock is held and before root-local deltas are
// applied, so callers can reject stale base roots before old pages are read.
type OrderedRootGroupPreflight func() error

type orderedRootStableUnsafeIterator interface {
	StableUnsafeIteratorSlices() bool
}

type orderedRootTrustedSortedUniqueIterator interface {
	OrderedUniqueUnsafeIterator() bool
}

type orderedRootLenHintIterator interface {
	Len() int
}

func selectOrderedRootWarmPublishPlan(hasExistingEntries bool, deltaOps int, maxDeltaOps int) orderedRootPublishPlan {
	if !hasExistingEntries {
		return orderedRootPublishPlanColdBuild
	}
	if deltaOps <= maxDeltaOps {
		return orderedRootPublishPlanWarmNativeApply
	}
	return orderedRootPublishPlanWarmFallbackRebuild
}

func (db *DB) orderedRootPublishOptionsForPolicy(policy OrderedRootStoragePolicy) (orderedRootPublishOptions, error) {
	opts := systemRootOrderedPublishOptions(db)
	switch policy {
	case OrderedRootStorageDefault:
		return opts, nil
	case OrderedRootStoragePagerLeaves:
		opts.outerLeavesInValueLog = false
		opts.leafPageLog = nil
		opts.internalBaseDelta = true
		return opts, nil
	case OrderedRootStorageValueLogLeaves:
		opts.outerLeavesInValueLog = true
		opts.internalBaseDelta = false
		opts.leafPageLog = db.leafPageLog
		if opts.leafPageLog == nil {
			return opts, errors.New("ordered root value-log leaf storage requires a leaf page log")
		}
		return opts, nil
	default:
		return opts, errors.New("unknown ordered root storage policy")
	}
}

func (db *DB) orderedRootZipperForOptions(idx *indexGen, opts orderedRootPublishOptions) (*zipper.Zipper, error) {
	if idx == nil {
		return nil, errors.New("missing index")
	}
	return db.orderedRootZipperForOptionsWithAllocator(idx, opts, idx.allocator)
}

func (db *DB) orderedRootZipperForOptionsWithAllocator(idx *indexGen, opts orderedRootPublishOptions, alloc zipper.PageAllocator) (*zipper.Zipper, error) {
	if idx == nil || idx.zipper == nil {
		return nil, errors.New("missing index")
	}
	if alloc == nil {
		return nil, errors.New("missing allocator")
	}
	if db != nil && alloc == idx.allocator && db.orderedRootOptionsUseDefaultZipper(opts) {
		return idx.zipper, nil
	}
	z := idx.zipper.CloneWithAllocator(alloc)
	z.SetOuterLeavesInValueLog(opts.outerLeavesInValueLog)
	z.SetIndexInternalBaseDelta(opts.internalBaseDelta && !opts.outerLeavesInValueLog)
	if opts.outerLeavesInValueLog {
		if opts.leafPageLog == nil {
			return nil, errors.New("ordered root value-log leaf storage requires a leaf page log")
		}
		z.SetLeafPageLog(opts.leafPageLog)
	}
	return z, nil
}

func (db *DB) orderedRootOptionsUseDefaultZipper(opts orderedRootPublishOptions) bool {
	if db == nil {
		return false
	}
	if opts.leafPrefixCompression != db.leafPrefixCompression ||
		opts.leafColumnar != db.indexColumnarLeaves ||
		opts.packedValuePtr != db.indexPackedValuePtr ||
		opts.outerLeavesInValueLog != db.indexOuterLeavesInValueLog ||
		(opts.internalBaseDelta && !opts.outerLeavesInValueLog) != db.indexInternalBaseDelta {
		return false
	}
	if opts.outerLeavesInValueLog && opts.leafPageLog != db.leafPageLog {
		return false
	}
	return true
}

func materializeOrderedRootTable(iter iterator.UnsafeIterator) (memtable.Table, error) {
	entries := 0
	if hint, ok := iter.(orderedRootLenHintIterator); ok {
		entries = hint.Len()
	}
	table := memtable.NewAppendOnlyWithEntryCapacity(entries)
	for iter.Valid() {
		val, ptr, flags := iter.UnsafeEntry()
		table.SetEntry(iter.UnsafeKey(), val, ptr, flags)
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	table.Freeze()
	return table, nil
}

type orderedRootCollectionDescriptorEntry struct {
	key        []byte
	val        []byte
	ptr        page.ValuePtr
	flags      byte
	valueKnown bool
}

func orderedRootTableCollectionRootDescriptorEntries(table memtable.Table) ([]orderedRootCollectionDescriptorEntry, error) {
	rootIter := table.NewIterator(collectionRootDescriptorPrefixBytes, collectionRootDescriptorPrefixEnd())
	entries, err := orderedRootIteratorCollectionRootDescriptorEntriesForPrefix(rootIter, collectionRootDescriptorPrefixBytes)
	_ = rootIter.Close()
	if err != nil {
		return nil, err
	}
	overlayIter := table.NewIterator(collectionRootOverlayDescriptorPrefixBytes, collectionRootOverlayDescriptorPrefixEnd())
	overlayEntries, err := orderedRootIteratorCollectionRootDescriptorEntriesForPrefix(overlayIter, collectionRootOverlayDescriptorPrefixBytes)
	_ = overlayIter.Close()
	if err != nil {
		return nil, err
	}
	return append(entries, overlayEntries...), nil
}

func orderedRootIteratorCollectionRootDescriptorEntries(iter iterator.UnsafeIterator) ([]orderedRootCollectionDescriptorEntry, error) {
	entries, err := orderedRootIteratorCollectionRootDescriptorEntriesForPrefix(iter, collectionRootDescriptorPrefixBytes)
	if err != nil {
		return nil, err
	}
	overlayEntries, err := orderedRootIteratorCollectionRootDescriptorEntriesForPrefix(iter, collectionRootOverlayDescriptorPrefixBytes)
	if err != nil {
		return nil, err
	}
	return append(entries, overlayEntries...), nil
}

func orderedRootIteratorCollectionRootDescriptorEntriesForPrefix(iter iterator.UnsafeIterator, prefix []byte) ([]orderedRootCollectionDescriptorEntry, error) {
	if iter == nil {
		return nil, nil
	}
	var out []orderedRootCollectionDescriptorEntry
	iter.Seek(prefix)
	for iter.Valid() {
		key := iter.UnsafeKey()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		val, ptr, flags := iter.UnsafeEntry()
		if flags&node.FlagPointer != 0 && val == nil {
			val = iter.UnsafeValue()
		}
		entry := orderedRootCollectionDescriptorEntry{
			key:        append([]byte(nil), key...),
			ptr:        ptr,
			flags:      flags,
			valueKnown: flags&node.FlagPointer == 0 || val != nil,
		}
		if entry.valueKnown {
			entry.val = append([]byte(nil), val...)
		}
		out = append(out, entry)
		iter.Next()
	}
	return out, iter.Error()
}

func orderedRootCollectionRootDescriptorEntriesEqual(a, b []orderedRootCollectionDescriptorEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i].key, b[i].key) {
			return false
		}
		if a[i].valueKnown && b[i].valueKnown {
			if !bytes.Equal(a[i].val, b[i].val) {
				return false
			}
			continue
		}
		if a[i].flags != b[i].flags {
			return false
		}
		if a[i].flags&node.FlagPointer != 0 && a[i].ptr != b[i].ptr {
			return false
		}
	}
	return true
}

func (s orderedRootPublishStats) collectionRootDescriptorReachabilityMayChange() bool {
	return !orderedRootCollectionRootDescriptorEntriesEqual(s.collectionRootDescriptorBaseEntries, s.collectionRootDescriptorTargetEntries)
}

func orderedRootEntryEqual(baseIter, targetIter iterator.UnsafeIterator) bool {
	baseVal, basePtr, baseFlags := baseIter.UnsafeEntry()
	targetVal, targetPtr, targetFlags := targetIter.UnsafeEntry()
	if baseFlags != targetFlags {
		return false
	}
	if baseFlags&node.FlagPointer != 0 {
		return basePtr == targetPtr
	}
	return bytes.Equal(baseVal, targetVal)
}

func orderedRootEntryValueLogFileID(iter iterator.UnsafeIterator) (uint32, bool) {
	if iter == nil || !iter.Valid() {
		return 0, false
	}
	_, ptr, flags := iter.UnsafeEntry()
	if flags&node.FlagPointer == 0 || !page.IsValueLogFileID(ptr.FileID) {
		return 0, false
	}
	return ptr.FileID, true
}

func orderedRootBatchPut(delta *batch.Batch, iter iterator.UnsafeIterator, borrowEntryViews bool, trustedSortedUnique bool) error {
	if delta == nil || iter == nil || !iter.Valid() {
		return nil
	}
	val, ptr, flags := iter.UnsafeEntry()
	if flags&node.FlagPointer != 0 && page.IsValueLogFileID(ptr.FileID) {
		if borrowEntryViews && trustedSortedUnique {
			return delta.AppendPointerViewTrustedSortedUnique(iter.UnsafeKey(), ptr)
		}
		if borrowEntryViews {
			return delta.SetPointerView(iter.UnsafeKey(), ptr)
		}
		return delta.SetPointer(iter.UnsafeKey(), ptr)
	}
	if borrowEntryViews && trustedSortedUnique {
		return delta.AppendViewTrustedSortedUnique(iter.UnsafeKey(), val)
	}
	if borrowEntryViews {
		return delta.SetView(iter.UnsafeKey(), val)
	}
	return delta.Set(iter.UnsafeKey(), val)
}

func orderedRootDeltaBatchFromIterator(iter iterator.UnsafeIterator) (*batch.Batch, error) {
	if iter == nil {
		return nil, errors.New("nil ordered root delta iterator")
	}
	delta := batch.NewRetainingLargeEntries(nil, orderedRootDeltaBatchInlineThreshold)
	if hint, ok := iter.(orderedRootLenHintIterator); ok {
		delta.Reserve(hint.Len())
	}
	borrowEntryViews := false
	if stable, ok := iter.(orderedRootStableUnsafeIterator); ok {
		borrowEntryViews = stable.StableUnsafeIteratorSlices()
	}
	trustedSortedUnique := false
	if trusted, ok := iter.(orderedRootTrustedSortedUniqueIterator); ok {
		trustedSortedUnique = trusted.OrderedUniqueUnsafeIterator()
	}
	for iter.Valid() {
		if iter.IsDeleted() {
			var err error
			if borrowEntryViews && trustedSortedUnique {
				err = delta.AppendDeleteViewTrustedSortedUnique(iter.UnsafeKey())
			} else if borrowEntryViews {
				err = delta.DeleteView(iter.UnsafeKey())
			} else {
				err = delta.Delete(iter.UnsafeKey())
			}
			if err != nil {
				_ = delta.Close()
				return nil, err
			}
		} else if err := orderedRootBatchPut(delta, iter, borrowEntryViews, trustedSortedUnique); err != nil {
			_ = delta.Close()
			return nil, err
		}
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		_ = delta.Close()
		return nil, err
	}
	return delta, nil
}

// OrderedRootDeltaBatchFromIterator materializes a root-local mutation iterator
// into the same transient batch shape used by ordered-root delta publishing.
// It does not close iter.
func OrderedRootDeltaBatchFromIterator(iter iterator.UnsafeIterator) (*batch.Batch, error) {
	return orderedRootDeltaBatchFromIterator(iter)
}

type orderedRootDeltaBatchIterator struct {
	entries        []batch.Entry
	idx            int
	includeDeleted bool
}

func newOrderedRootDeltaBatchIterator(delta *batch.Batch, includeDeleted bool) *orderedRootDeltaBatchIterator {
	it := &orderedRootDeltaBatchIterator{includeDeleted: includeDeleted}
	if delta != nil {
		it.entries = delta.SortedEntries()
	}
	it.skipDeleted()
	return it
}

func (it *orderedRootDeltaBatchIterator) Valid() bool {
	return it != nil && it.idx < len(it.entries)
}

func (it *orderedRootDeltaBatchIterator) Next() {
	if !it.Valid() {
		return
	}
	it.idx++
	it.skipDeleted()
}

func (it *orderedRootDeltaBatchIterator) Seek(key []byte) {
	if it == nil {
		return
	}
	it.idx = sort.Search(len(it.entries), func(i int) bool {
		return bytes.Compare(it.entries[i].Key, key) >= 0
	})
	it.skipDeleted()
}

func (it *orderedRootDeltaBatchIterator) UnsafeKey() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.idx].Key
}

func (it *orderedRootDeltaBatchIterator) UnsafeValue() []byte {
	if !it.Valid() || it.entries[it.idx].Type == batch.OpDelete {
		return nil
	}
	return it.entries[it.idx].Value
}

func (it *orderedRootDeltaBatchIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.Valid() {
		return nil, page.ValuePtr{}, node.FlagInline
	}
	entry := it.entries[it.idx]
	if entry.Type == batch.OpDelete {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	if entry.IsPtr {
		return entry.Value, entry.ValuePtr, node.FlagPointer
	}
	return entry.Value, page.ValuePtr{}, node.FlagInline
}

func (it *orderedRootDeltaBatchIterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *orderedRootDeltaBatchIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *orderedRootDeltaBatchIterator) KeyCopy(dst []byte) []byte {
	return append(dst[:0], it.UnsafeKey()...)
}

func (it *orderedRootDeltaBatchIterator) ValueCopy(dst []byte) []byte {
	return append(dst[:0], it.UnsafeValue()...)
}

func (it *orderedRootDeltaBatchIterator) IsDeleted() bool {
	return it.Valid() && it.entries[it.idx].Type == batch.OpDelete
}

func (it *orderedRootDeltaBatchIterator) Error() error {
	return nil
}

func (it *orderedRootDeltaBatchIterator) Close() error {
	if it != nil {
		it.entries = nil
		it.idx = 0
	}
	return nil
}

func (it *orderedRootDeltaBatchIterator) Domain() (start, end []byte) {
	return nil, nil
}

func (it *orderedRootDeltaBatchIterator) StableUnsafeIteratorSlices() bool {
	return true
}

func (it *orderedRootDeltaBatchIterator) Len() int {
	if it == nil {
		return 0
	}
	if !it.includeDeleted {
		n := 0
		for idx := it.idx; idx < len(it.entries); idx++ {
			if it.entries[idx].Type != batch.OpDelete {
				n++
			}
		}
		return n
	}
	return len(it.entries) - it.idx
}

func (it *orderedRootDeltaBatchIterator) skipDeleted() {
	if it == nil || it.includeDeleted {
		return
	}
	for it.idx < len(it.entries) && it.entries[it.idx].Type == batch.OpDelete {
		it.idx++
	}
}

func collectValueLogRefDeltaFromIterator(iter iterator.UnsafeIterator) (*valueLogRefDelta, error) {
	if iter == nil {
		return nil, nil
	}
	defer iter.Close()
	delta := newValueLogRefDelta()
	for iter.Valid() {
		if fileID, ok := orderedRootEntryValueLogFileID(iter); ok {
			delta.add(fileID, 1)
		}
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return delta, nil
}

func (db *DB) publishOrderedRootDeltaIterator(baseRoot uint64, iter iterator.UnsafeIterator, opts orderedRootPublishOptions) (newRoot uint64, retired []uint64, metrics adaptive.Metrics, err error) {
	if db == nil {
		err = ErrClosed
		return
	}
	if iter == nil {
		err = errors.New("nil ordered root delta iterator")
		return
	}

	if baseRoot == 0 {
		newRoot, retired, metrics, _, _, err = db.publishOrderedRootIterator(0, iter, opts, false)
		return
	}
	defer iter.Close()

	idx := db.idx.Load()
	if idx == nil {
		err = errors.New("missing index")
		return
	}
	if opts.outerLeavesInValueLog && opts.leafPageLog == nil {
		err = errors.New("ordered root value-log leaf storage requires a leaf page log")
		return
	}
	delta, err := orderedRootDeltaBatchFromIterator(iter)
	if err != nil {
		return 0, nil, metrics, err
	}
	defer delta.Close()
	if delta.IsEmpty() {
		return baseRoot, nil, metrics, nil
	}
	rootZipper, err := db.orderedRootZipperForOptions(idx, opts)
	if err != nil {
		return 0, nil, metrics, err
	}
	applyOptions := opts.applyOptions
	var pooledResult *zipper.ReadOnlyPrepareResult
	if applyOptions.PrepareReadOnly && opts.readOnlyPrepareCallerResult == nil {
		pooledResult = acquireOrderedRootReadOnlyPrepareResult()
		applyOptions.ReadOnlyPrepare = pooledResult.ReuseOptions()
	}
	newRoot, retired, metrics, readOnlyPrepare, readOnlyPrepareNs, err := applyOrderedRootDeltaWithOptions(rootZipper, baseRoot, delta, applyOptions)
	if opts.applyOptions.PrepareReadOnly && opts.readOnlyPrepareSummary != nil {
		summary := readOnlyPrepare.LeafSpanSummary()
		*opts.readOnlyPrepareSummary = summary
	}
	if opts.readOnlyPrepareCallerResult != nil {
		*opts.readOnlyPrepareCallerResult = readOnlyPrepare
	}
	if opts.readOnlyPrepareNs != nil {
		*opts.readOnlyPrepareNs = readOnlyPrepareNs
	}
	if pooledResult != nil {
		*pooledResult = readOnlyPrepare
		releaseOrderedRootReadOnlyPrepareResult(pooledResult)
	}
	return newRoot, retired, metrics, err
}

func (db *DB) publishOrderedRootDeltaBatch(baseRoot uint64, delta *batch.Batch, opts orderedRootPublishOptions) (newRoot uint64, retired []uint64, metrics adaptive.Metrics, err error) {
	if db == nil {
		err = ErrClosed
		return
	}
	idx := db.idx.Load()
	if idx == nil {
		err = errors.New("missing index")
		return
	}
	return db.publishOrderedRootDeltaBatchWithAllocator(idx, baseRoot, delta, opts, idx.allocator, &pagerAllocator{p: idx.pager}, false)
}

func (db *DB) publishOrderedRootDeltaBatchWithAllocator(idx *indexGen, baseRoot uint64, delta *batch.Batch, opts orderedRootPublishOptions, alloc zipper.PageAllocator, coldBuildAlloc bulk.Allocator, includeDeletedOnColdBuild bool) (newRoot uint64, retired []uint64, metrics adaptive.Metrics, err error) {
	if db == nil {
		err = ErrClosed
		return
	}
	if idx == nil {
		err = errors.New("missing index")
		return
	}
	if delta == nil {
		err = errors.New("nil ordered root delta batch")
		return
	}
	if alloc == nil {
		err = errors.New("missing allocator")
		return
	}
	if opts.outerLeavesInValueLog && opts.leafPageLog == nil {
		err = errors.New("ordered root value-log leaf storage requires a leaf page log")
		return
	}
	if opts.outerLeavesInValueLog {
		if tracker := preparedOutputTrackerFromAlloc(alloc, coldBuildAlloc); tracker != nil {
			opts.leafPageLog = preparedOutputLeafPageLog{inner: opts.leafPageLog, tracker: tracker}
		}
	}
	if delta.IsEmpty() {
		if opts.applyOptions.PrepareReadOnly {
			if err = db.runOrderedRootReadOnlyPrepare(idx, baseRoot, delta, opts, alloc); err != nil {
				return 0, nil, metrics, err
			}
		}
		return baseRoot, nil, metrics, nil
	}
	if baseRoot == 0 {
		if opts.applyOptions.PrepareReadOnly {
			if err = db.runOrderedRootReadOnlyPrepare(idx, baseRoot, delta, opts, alloc); err != nil {
				return 0, nil, metrics, err
			}
		}
		iter := newOrderedRootDeltaBatchIterator(delta, includeDeletedOnColdBuild)
		defer func() { _ = iter.Close() }()
		if coldBuildAlloc == nil {
			coldBuildAlloc = alloc
		}
		var leafPageLog bulk.LeafPageAppender
		if opts.outerLeavesInValueLog {
			leafPageLog = opts.leafPageLog
		}
		newRoot, err = bulk.BuildWithOptions(iter, coldBuildAlloc, idx.pager, bulk.BuildOptions{
			LeafPrefixCompression: opts.leafPrefixCompression,
			LeafColumnar:          opts.leafColumnar,
			PackedValuePtr:        opts.packedValuePtr,
			InternalBaseDelta:     opts.internalBaseDelta && !opts.outerLeavesInValueLog,
			LeafPageLog:           leafPageLog,
		})
		return
	}

	rootZipper, err := db.orderedRootZipperForOptionsWithAllocator(idx, opts, alloc)
	if err != nil {
		return 0, nil, metrics, err
	}
	if opts.applyOptions.PrepareReadOnly {
		if err = runOrderedRootReadOnlyPrepare(rootZipper, baseRoot, delta, opts); err != nil {
			return 0, nil, metrics, err
		}
		opts.applyOptions.PrepareReadOnly = false
	}
	newRoot, retired, metrics, _, _, err = applyOrderedRootDeltaWithOptions(rootZipper, baseRoot, delta, opts.applyOptions)
	return newRoot, retired, metrics, err
}

func (db *DB) runOrderedRootReadOnlyPrepare(idx *indexGen, baseRoot uint64, delta *batch.Batch, opts orderedRootPublishOptions, alloc zipper.PageAllocator) error {
	rootZipper, err := db.orderedRootZipperForOptionsWithAllocator(idx, opts, alloc)
	if err != nil {
		return err
	}
	return runOrderedRootReadOnlyPrepare(rootZipper, baseRoot, delta, opts)
}

func runOrderedRootReadOnlyPrepare(rootZipper *zipper.Zipper, baseRoot uint64, delta *batch.Batch, opts orderedRootPublishOptions) error {
	if opts.readOnlyPrepareAttempted != nil {
		*opts.readOnlyPrepareAttempted = true
	}
	prepareOptions := opts.applyOptions.ReadOnlyPrepare
	var pooledResult *zipper.ReadOnlyPrepareResult
	if opts.readOnlyPrepareCallerResult == nil {
		pooledResult = acquireOrderedRootReadOnlyPrepareResult()
		prepareOptions = pooledResult.ReuseOptions()
	}
	prepareStart := time.Now()
	prepared, err := rootZipper.PrepareReadOnly(baseRoot, delta, prepareOptions)
	prepareNs := elapsedDurationNs(prepareStart)
	if pooledResult != nil {
		*pooledResult = prepared
		defer releaseOrderedRootReadOnlyPrepareResult(pooledResult)
	}
	if opts.readOnlyPrepareSummary != nil {
		summary := prepared.LeafSpanSummary()
		*opts.readOnlyPrepareSummary = summary
	}
	if opts.readOnlyPrepareWorkerSummary != nil {
		summary := prepared.LeafSpanWorkerRangeSummary(opts.readOnlyPrepareWorkerCount)
		*opts.readOnlyPrepareWorkerSummary = summary
	}
	if opts.readOnlyPrepareCallerResult != nil {
		*opts.readOnlyPrepareCallerResult = prepared
	}
	if opts.readOnlyPrepareNs != nil {
		*opts.readOnlyPrepareNs = prepareNs
	}
	return err
}

func preparedOutputTrackerFromAlloc(alloc zipper.PageAllocator, coldBuildAlloc bulk.Allocator) preparedLeafLogOutputRecorder {
	if tracker, ok := alloc.(preparedLeafLogOutputRecorder); ok && tracker != nil {
		return tracker
	}
	if tracker, ok := coldBuildAlloc.(preparedLeafLogOutputRecorder); ok && tracker != nil {
		return tracker
	}
	return nil
}

func applyOrderedRootDeltaWithOptions(rootZipper *zipper.Zipper, baseRoot uint64, delta *batch.Batch, opts zipper.ApplyOptions) (uint64, []uint64, adaptive.Metrics, zipper.ReadOnlyPrepareResult, uint64, error) {
	applyResult, err := rootZipper.ApplyWithOptions(baseRoot, delta, opts)
	// ApplyWithOptions returns its result by value and may include partial
	// metrics when err is non-nil; preserve metrics but do not return partial
	// root IDs or retired-page ownership on failure.
	if err != nil {
		return 0, nil, applyResult.Metrics, applyResult.ReadOnlyPrepare, applyResult.ReadOnlyPrepareNs, err
	}
	return applyResult.RootID, applyResult.PendingRetiredPages, applyResult.Metrics, applyResult.ReadOnlyPrepare, applyResult.ReadOnlyPrepareNs, nil
}

func buildOrderedRootDeltaBatch(baseIter, targetIter iterator.UnsafeIterator, trackRefs bool) (*batch.Batch, int, *valueLogRefDelta, error) {
	delta := batch.NewRetainingLargeEntries(nil, orderedRootDeltaBatchInlineThreshold)
	baseValid := baseIter.Valid()
	targetValid := targetIter.Valid()
	deltaOps := 0
	var vlogRefDelta *valueLogRefDelta
	if trackRefs {
		vlogRefDelta = newValueLogRefDelta()
	}
	for baseValid || targetValid {
		switch {
		case !targetValid:
			if vlogRefDelta != nil {
				if fileID, ok := orderedRootEntryValueLogFileID(baseIter); ok {
					vlogRefDelta.add(fileID, -1)
				}
			}
			if err := delta.Delete(baseIter.UnsafeKey()); err != nil {
				_ = delta.Close()
				return nil, 0, nil, err
			}
			deltaOps++
			baseIter.Next()
			baseValid = baseIter.Valid()
		case !baseValid:
			if vlogRefDelta != nil {
				if fileID, ok := orderedRootEntryValueLogFileID(targetIter); ok {
					vlogRefDelta.add(fileID, 1)
				}
			}
			if err := orderedRootBatchPut(delta, targetIter, false, false); err != nil {
				_ = delta.Close()
				return nil, 0, nil, err
			}
			deltaOps++
			targetIter.Next()
			targetValid = targetIter.Valid()
		default:
			switch cmp := bytes.Compare(baseIter.UnsafeKey(), targetIter.UnsafeKey()); {
			case cmp < 0:
				if vlogRefDelta != nil {
					if fileID, ok := orderedRootEntryValueLogFileID(baseIter); ok {
						vlogRefDelta.add(fileID, -1)
					}
				}
				if err := delta.Delete(baseIter.UnsafeKey()); err != nil {
					_ = delta.Close()
					return nil, 0, nil, err
				}
				deltaOps++
				baseIter.Next()
				baseValid = baseIter.Valid()
			case cmp > 0:
				if vlogRefDelta != nil {
					if fileID, ok := orderedRootEntryValueLogFileID(targetIter); ok {
						vlogRefDelta.add(fileID, 1)
					}
				}
				if err := orderedRootBatchPut(delta, targetIter, false, false); err != nil {
					_ = delta.Close()
					return nil, 0, nil, err
				}
				deltaOps++
				targetIter.Next()
				targetValid = targetIter.Valid()
			default:
				if !orderedRootEntryEqual(baseIter, targetIter) {
					if vlogRefDelta != nil {
						if fileID, ok := orderedRootEntryValueLogFileID(baseIter); ok {
							vlogRefDelta.add(fileID, -1)
						}
						if fileID, ok := orderedRootEntryValueLogFileID(targetIter); ok {
							vlogRefDelta.add(fileID, 1)
						}
					}
					if err := orderedRootBatchPut(delta, targetIter, false, false); err != nil {
						_ = delta.Close()
						return nil, 0, nil, err
					}
					deltaOps++
				}
				baseIter.Next()
				targetIter.Next()
				baseValid = baseIter.Valid()
				targetValid = targetIter.Valid()
			}
		}
	}
	if err := baseIter.Error(); err != nil {
		_ = delta.Close()
		return nil, 0, nil, err
	}
	if err := targetIter.Error(); err != nil {
		_ = delta.Close()
		return nil, 0, nil, err
	}
	return delta, deltaOps, vlogRefDelta, nil
}

func (db *DB) publishOrderedRootIterator(baseRoot uint64, iter iterator.UnsafeIterator, opts orderedRootPublishOptions, trackValueLogRefs bool) (newRoot uint64, retired []uint64, metrics adaptive.Metrics, stats orderedRootPublishStats, vlogRefDelta *valueLogRefDelta, err error) {
	if db == nil {
		err = ErrClosed
		return
	}
	if iter == nil {
		err = errors.New("nil ordered root iterator")
		return
	}
	if db.testOrderedRootPublishHook != nil {
		db.testOrderedRootPublishHook(baseRoot)
	}
	defer iter.Close()

	idx := db.idx.Load()
	if idx == nil {
		err = errors.New("missing index")
		return
	}
	state := db.state.Load()
	if state == nil {
		err = errors.New("missing backend state")
		return
	}

	newRoot = baseRoot
	var buildIter iterator.UnsafeIterator
	if opts.outerLeavesInValueLog && opts.leafPageLog == nil {
		err = errors.New("ordered root value-log leaf storage requires a leaf page log")
		return
	}
	trackValueLogRefs = trackValueLogRefs && db.valueLogRefTracker != nil && db.valueLogRefTracker.canTrack(db.currentCommitSeq()) && !opts.outerLeavesInValueLog
	if baseRoot != 0 {
		rootTree := tree.New(idx.pager, newValueReader(state.ValueLogSet), baseRoot)
		collectBasePageIDs := func() ([]uint64, error) {
			return rootTree.CollectPageIDs()
		}
		if trackValueLogRefs {
			baseDescriptorIter := rootTree.Iterator(nil, nil)
			stats.collectionRootDescriptorBaseEntries, err = orderedRootIteratorCollectionRootDescriptorEntries(baseDescriptorIter)
			_ = baseDescriptorIter.Close()
			if err != nil {
				return
			}
		}

		baseProbe := rootTree.Iterator(nil, nil)
		hasExistingEntries := baseProbe.Valid()
		iterErr := baseProbe.Error()
		baseProbe.Close()
		if iterErr != nil {
			err = iterErr
			return
		}

		targetTable, materializeErr := materializeOrderedRootTable(iter)
		if materializeErr != nil {
			err = materializeErr
			return
		}
		if trackValueLogRefs {
			stats.collectionRootDescriptorTargetEntries, err = orderedRootTableCollectionRootDescriptorEntries(targetTable)
			if err != nil {
				return
			}
		}
		if !hasExistingEntries {
			if trackValueLogRefs {
				vlogRefDelta, err = collectValueLogRefDeltaFromIterator(targetTable.NewIterator(nil, nil))
				if err != nil {
					return
				}
			}
			pageIDs, collectErr := collectBasePageIDs()
			if collectErr != nil {
				err = collectErr
				return
			}
			retired = append(retired, pageIDs...)
			buildIter = targetTable.NewIterator(nil, nil)
		} else {
			baseIter := rootTree.Iterator(nil, nil)
			targetIter := targetTable.NewIterator(nil, nil)
			delta, deltaOps, refDelta, deltaErr := buildOrderedRootDeltaBatch(baseIter, targetIter, trackValueLogRefs)
			baseIter.Close()
			targetIter.Close()
			if deltaErr != nil {
				err = deltaErr
				return
			}
			defer delta.Close()
			vlogRefDelta = refDelta
			switch selectOrderedRootWarmPublishPlan(hasExistingEntries, deltaOps, opts.maxWarmDeltaOps) {
			case orderedRootPublishPlanWarmNativeApply:
				stats.warmAttempts++
				stats.warmNativeApplyAttempts++
				rootZipper, zipperErr := db.orderedRootZipperForOptions(idx, opts)
				if zipperErr != nil {
					err = zipperErr
					return
				}
				newRoot, retired, metrics, _, _, err = applyOrderedRootDeltaWithOptions(rootZipper, baseRoot, delta, zipper.ApplyOptions{})
				if err != nil {
					return
				}
				// Avoid a full old-tree page scan on the warm apply path. The
				// retired page list is exact; preserved pages are tracked as a
				// lower bound so the public counter still proves warm apply
				// avoided a full rebuild without making every write walk the root.
				if newRoot != 0 {
					stats.warmPreservedPages = 1
				}
				stats.warmRewrittenPages = uint64(len(retired))
				return
			case orderedRootPublishPlanWarmFallbackRebuild:
				stats.warmAttempts++
				stats.warmRebuildFallbacks++
				if vlogRefDelta != nil {
					vlogRefDelta = nil
				}
				pageIDs, collectErr := collectBasePageIDs()
				if collectErr != nil {
					err = collectErr
					return
				}
				retired = append(retired, pageIDs...)
				buildIter = targetTable.NewIterator(nil, nil)
			case orderedRootPublishPlanColdBuild:
				err = errors.New("ordered root warm publish selected cold build for a non-empty base root")
				return
			}
		}
	} else {
		if trackValueLogRefs {
			targetTable, materializeErr := materializeOrderedRootTable(iter)
			if materializeErr != nil {
				err = materializeErr
				return
			}
			stats.collectionRootDescriptorTargetEntries, err = orderedRootTableCollectionRootDescriptorEntries(targetTable)
			if err != nil {
				return
			}
			vlogRefDelta, err = collectValueLogRefDeltaFromIterator(targetTable.NewIterator(nil, nil))
			if err != nil {
				return
			}
			buildIter = targetTable.NewIterator(nil, nil)
		} else {
			buildIter = iter
		}
	}

	if buildIter != nil && buildIter != iter {
		defer buildIter.Close()
	}
	if buildIter != nil {
		var leafPageLog bulk.LeafPageAppender
		if opts.outerLeavesInValueLog {
			leafPageLog = opts.leafPageLog
		}
		newRoot, err = bulk.BuildWithOptions(buildIter, &pagerAllocator{p: idx.pager}, idx.pager, bulk.BuildOptions{
			LeafPrefixCompression: opts.leafPrefixCompression,
			LeafColumnar:          opts.leafColumnar,
			PackedValuePtr:        opts.packedValuePtr,
			InternalBaseDelta:     opts.internalBaseDelta && !opts.outerLeavesInValueLog,
			LeafPageLog:           leafPageLog,
		})
	}
	return
}

func mergeOrderedRootPublishMetrics(dst *adaptive.Metrics, src adaptive.Metrics) {
	if dst == nil {
		return
	}
	if src.LeafFill > 0 {
		if dst.LeafFill == 0 {
			dst.LeafFill = src.LeafFill
		} else {
			dst.LeafFill = (dst.LeafFill + src.LeafFill) / 2
		}
	}
	dst.Splits += src.Splits
	dst.IndexWriteBytes += src.IndexWriteBytes
	dst.SlabWriteBytes += src.SlabWriteBytes
	dst.SlabDeadBytes += src.SlabDeadBytes
	dst.ZipperApplyOps += src.ZipperApplyOps
	dst.ZipperNodeLoads += src.ZipperNodeLoads
	dst.ZipperPagerNodeLoads += src.ZipperPagerNodeLoads
	dst.ZipperLeafLogNodeLoads += src.ZipperLeafLogNodeLoads
	dst.ZipperLeafLogCacheHits += src.ZipperLeafLogCacheHits
	dst.ZipperLeafLogReaderCalls += src.ZipperLeafLogReaderCalls
	dst.ZipperLeafLogViewReads += src.ZipperLeafLogViewReads
	dst.ZipperLeafLogScratchReads += src.ZipperLeafLogScratchReads
	dst.ZipperPagerNodeBytesRead += src.ZipperPagerNodeBytesRead
	dst.ZipperLeafLogNodeBytesRead += src.ZipperLeafLogNodeBytesRead
	dst.ZipperLeafLogRecordHintBytesRead += src.ZipperLeafLogRecordHintBytesRead
	dst.ZipperLeafMerges += src.ZipperLeafMerges
	dst.ZipperInternalMerges += src.ZipperInternalMerges
	dst.ZipperLeafPagesWritten += src.ZipperLeafPagesWritten
	dst.ZipperPagerLeafPagesWritten += src.ZipperPagerLeafPagesWritten
	dst.ZipperLeafLogPagesWritten += src.ZipperLeafLogPagesWritten
	dst.ZipperLeafPageBytesWritten += src.ZipperLeafPageBytesWritten
	dst.ZipperPagerLeafPageBytesWritten += src.ZipperPagerLeafPageBytesWritten
	dst.ZipperLeafLogPageBytesWritten += src.ZipperLeafLogPageBytesWritten
	dst.ZipperLeafLogRecordHintBytesWritten += src.ZipperLeafLogRecordHintBytesWritten
	dst.ZipperInternalPagesWritten += src.ZipperInternalPagesWritten
	dst.ZipperInternalPageBytesWritten += src.ZipperInternalPageBytesWritten
	dst.ZipperInternalChildRefs += src.ZipperInternalChildRefs
	dst.ZipperInternalPageChildRefs += src.ZipperInternalPageChildRefs
	dst.ZipperInternalLeafLogRefs += src.ZipperInternalLeafLogRefs
	dst.ZipperInternalLeafLogRefCopies += src.ZipperInternalLeafLogRefCopies
	dst.ZipperRootSplitLevels += src.ZipperRootSplitLevels
	if len(src.SlabWriteBytesByFile) != 0 {
		if dst.SlabWriteBytesByFile == nil {
			dst.SlabWriteBytesByFile = make(map[uint32]int64, len(src.SlabWriteBytesByFile))
		}
		for fileID, bytes := range src.SlabWriteBytesByFile {
			dst.SlabWriteBytesByFile[fileID] += bytes
		}
	}
	if len(src.SlabDeadBytesByFile) != 0 {
		if dst.SlabDeadBytesByFile == nil {
			dst.SlabDeadBytesByFile = make(map[uint32]int64, len(src.SlabDeadBytesByFile))
		}
		for fileID, bytes := range src.SlabDeadBytesByFile {
			dst.SlabDeadBytesByFile[fileID] += bytes
		}
	}
}

// PublishOrderedRootIterator builds and commits a non-meta root from an ordered
// iterator while preserving the current user and system roots in the commit.
func (db *DB) PublishOrderedRootIterator(baseRoot uint64, iter iterator.UnsafeIterator) (uint64, error) {
	if db == nil {
		return 0, ErrClosed
	}
	if db.closing.Load() {
		return 0, ErrClosed
	}
	if iter == nil {
		return 0, errors.New("nil ordered root iterator")
	}

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	if db.readOnly {
		return 0, ErrReadOnly
	}

	db.mu.RLock()
	userRoot := db.meta.UserRootPageID
	systemRoot := db.meta.SystemRootPageID
	baseSeq := db.meta.CommitSeq
	db.mu.RUnlock()

	newRoot, retired, metrics, _, _, err := db.publishOrderedRootIterator(baseRoot, iter, systemRootOrderedPublishOptions(db), false)
	if err != nil {
		return 0, err
	}

	db.mu.RLock()
	curUserRoot := db.meta.UserRootPageID
	curSystemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if curUserRoot != userRoot || curSystemRoot != systemRoot {
		return 0, errors.New("concurrent modification detected during ordered root publish")
	}

	vlogRefDelta := db.newNoopValueLogRefDeltaIfTrackable(baseSeq)
	defer func() {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
	}()

	if err := db.finalizeCommit(userRoot, systemRoot, retired, false, metrics, nil, true, vlogRefDelta, nil, nil); err != nil {
		return 0, err
	}
	vlogRefDelta = nil
	return newRoot, nil
}

// PublishOrderedRootGroup builds and commits a mixed system/non-system root
// group in one backend commit. Non-system roots are built from ordered
// iterators and become durable when the grouped commit finalizes.
func (db *DB) PublishOrderedRootGroup(systemIter iterator.UnsafeIterator, ordered []OrderedRootPublishInput) (uint64, []uint64, error) {
	return db.publishOrderedRootGroup(systemIter, ordered, nil)
}

// PublishOrderedRootGroupWithSystemBuilder builds non-system roots first, then
// calls buildSystemIter with the produced root IDs and commits the system root
// plus all non-system roots in one backend commit. This is intended for callers
// whose system descriptors must store the new root IDs produced by the group.
func (db *DB) PublishOrderedRootGroupWithSystemBuilder(ordered []OrderedRootPublishInput, buildSystemIter OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	if buildSystemIter == nil {
		return 0, nil, errors.New("nil ordered root group system builder")
	}
	return db.publishOrderedRootGroup(nil, ordered, buildSystemIter)
}

// PublishOrderedRootDeltaGroupWithSystemBuilder applies root-local mutation
// streams to non-system roots, then builds and commits a system-root iterator
// that can persist the produced root IDs in the same backend commit.
func (db *DB) PublishOrderedRootDeltaGroupWithSystemBuilder(ordered []OrderedRootDeltaPublishInput, buildSystemIter OrderedRootGroupSystemBuilder) (newSystemRoot uint64, rootIDs []uint64, err error) {
	if buildSystemIter == nil {
		return 0, nil, errors.New("nil ordered root group system builder")
	}
	if db == nil {
		return 0, nil, ErrClosed
	}
	if db.closing.Load() {
		return 0, nil, ErrClosed
	}

	lockStart := time.Now()
	db.writeMu.Lock()
	holdStart := time.Now()
	wait := holdStart.Sub(lockStart)
	rootsObserved := 0
	phaseStats := orderedRootDeltaGroupPublishPhaseStats{}
	finished := false
	finishPublish := func() {
		if finished {
			return
		}
		finished = true
		hold := time.Since(holdStart)
		db.writeMu.Unlock()
		db.observeOrderedRootDeltaGroupPublish(wait, hold, rootsObserved, phaseStats, err)
	}
	cleanupIterators := func() {}
	// finishPublish intentionally runs before iterator cleanup: caller-provided
	// iterator Close paths can do arbitrary cleanup and should not extend the
	// write-lock hold time reported for the publish itself.
	defer func() {
		finishPublish()
		cleanupIterators()
	}()

	if db.readOnly {
		err = ErrReadOnly
		return 0, nil, err
	}

	db.mu.RLock()
	userRoot := db.meta.UserRootPageID
	baseSystemRoot := db.meta.SystemRootPageID
	baseSeq := db.meta.CommitSeq
	db.mu.RUnlock()

	systemOpts := systemRootOrderedPublishOptions(db)
	rootIDs = make([]uint64, len(ordered))
	orderedConsumed := make([]bool, len(ordered))
	cleanupIterators = func() {
		closeUnconsumedOrderedRootDeltaPublishIterators(ordered, orderedConsumed)
	}
	var retired []uint64
	var merged adaptive.Metrics
	for idx := range ordered {
		opts, err := db.orderedRootPublishOptionsForPolicy(ordered[idx].StoragePolicy)
		if err != nil {
			return 0, nil, err
		}
		orderedConsumed[idx] = true
		phaseStart := time.Now()
		rootID, rootRetired, metrics, err := db.publishOrderedRootDeltaIterator(ordered[idx].BaseRoot, ordered[idx].Iter, opts)
		phaseStats.rootApplyNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
		phaseStats.rootApplyCalls++
		if err != nil {
			return 0, nil, err
		}
		rootIDs[idx] = rootID
		rootsObserved++
		retired = append(retired, rootRetired...)
		mergeOrderedRootPublishMetrics(&merged, metrics)
		phaseStats.rootApplyMetrics.add(metrics)
	}

	phaseStart := time.Now()
	iter, err := buildSystemIter(append([]uint64(nil), rootIDs...))
	phaseStats.systemBuildNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	if err != nil {
		return 0, nil, err
	}
	if iter == nil {
		return 0, nil, errors.New("nil system root iterator")
	}
	phaseStart = time.Now()
	rootID, rootRetired, metrics, publishStats, refDelta, err := db.publishOrderedRootIterator(baseSystemRoot, iter, systemOpts, true)
	phaseStats.systemApplyNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	phaseStats.systemApplyCalls++
	if err != nil {
		return 0, nil, err
	}
	newSystemRoot = rootID
	retired = append(retired, rootRetired...)
	mergeOrderedRootPublishMetrics(&merged, metrics)
	phaseStats.systemApplyMetrics.add(metrics)
	vlogRefDelta := refDelta
	forceRefTrackerRebuild := publishStats.collectionRootDescriptorReachabilityMayChange()
	if len(ordered) > 0 || forceRefTrackerRebuild {
		// Non-system roots were applied from deltas, so this commit has no
		// exact value-log ref delta for their pointer changes. Collection
		// descriptors also make non-system roots part of reachability, and the
		// system-root ref delta is not exact for those roots. Keep GC
		// reachability conservative by invalidating the tracker after commit.
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
		vlogRefDelta = nil
	}
	db.mu.RLock()
	curUserRoot := db.meta.UserRootPageID
	curSystemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if curUserRoot != userRoot || curSystemRoot != baseSystemRoot {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
			vlogRefDelta = nil
		}
		return 0, nil, errors.New("concurrent modification detected during ordered root group publish")
	}

	if len(ordered) == 0 && !forceRefTrackerRebuild && vlogRefDelta == nil {
		vlogRefDelta = db.newNoopValueLogRefDeltaIfTrackable(baseSeq)
	}
	phaseStart = time.Now()
	err = db.finalizeCommit(userRoot, newSystemRoot, retired, false, merged, nil, true, vlogRefDelta, nil, nil)
	phaseStats.finalizeNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	phaseStats.finalizeCalls++
	if err != nil {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
			vlogRefDelta = nil
		}
		return 0, nil, err
	}
	vlogRefDelta = nil
	return newSystemRoot, rootIDs, nil
}

// PublishOrderedRootDeltaGroupWithSystemDeltaBuilder applies root-local
// mutation streams to non-system roots, then applies a root-local mutation
// stream to the system root. The system delta should contain only changed
// system-root entries; omitted system entries are preserved.
func (db *DB) PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered []OrderedRootDeltaPublishInput, buildSystemDeltaIter OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered, nil, buildSystemDeltaIter)
}

// PublishOrderedRootDeltaGroupWithPreflightAndSystemDeltaBuilder is like
// PublishOrderedRootDeltaGroupWithSystemDeltaBuilder, but runs preflight under
// the DB write lock before applying root-local deltas.
func (db *DB) PublishOrderedRootDeltaGroupWithPreflightAndSystemDeltaBuilder(ordered []OrderedRootDeltaPublishInput, preflight OrderedRootGroupPreflight, buildSystemDeltaIter OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered, preflight, buildSystemDeltaIter)
}

// PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder is like
// PublishOrderedRootDeltaGroupWithSystemDeltaBuilder, but the root-local
// mutation batches have already been materialized by the caller. This lets
// collection flush paths do iterator-to-batch work before entering the DB write
// critical section.
func (db *DB) PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered []OrderedRootDeltaBatchPublishInput, buildSystemDeltaIter OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered, nil, buildSystemDeltaIter)
}

// PublishOrderedRootDeltaBatchGroupWithPreflightAndSystemDeltaBuilder is like
// PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder, but runs preflight
// under the DB write lock before applying root-local deltas.
func (db *DB) PublishOrderedRootDeltaBatchGroupWithPreflightAndSystemDeltaBuilder(ordered []OrderedRootDeltaBatchPublishInput, preflight OrderedRootGroupPreflight, buildSystemDeltaIter OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered, preflight, buildSystemDeltaIter)
}

func (db *DB) publishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered []OrderedRootDeltaPublishInput, preflight OrderedRootGroupPreflight, buildSystemDeltaIter OrderedRootGroupSystemBuilder) (newSystemRoot uint64, rootIDs []uint64, err error) {
	if buildSystemDeltaIter == nil {
		return 0, nil, errors.New("nil ordered root group system delta builder")
	}
	if db == nil {
		return 0, nil, ErrClosed
	}
	if db.closing.Load() {
		return 0, nil, ErrClosed
	}

	lockStart := time.Now()
	db.writeMu.Lock()
	holdStart := time.Now()
	wait := holdStart.Sub(lockStart)
	rootsObserved := 0
	phaseStats := orderedRootDeltaGroupPublishPhaseStats{}
	finished := false
	finishPublish := func() {
		if finished {
			return
		}
		finished = true
		hold := time.Since(holdStart)
		db.writeMu.Unlock()
		db.observeOrderedRootDeltaGroupPublish(wait, hold, rootsObserved, phaseStats, err)
	}
	rootIDs = make([]uint64, len(ordered))
	orderedConsumed := make([]bool, len(ordered))
	defer closeUnconsumedOrderedRootDeltaPublishIterators(ordered, orderedConsumed)
	defer finishPublish()

	if db.readOnly {
		err = ErrReadOnly
		return 0, nil, err
	}

	db.mu.RLock()
	userRoot := db.meta.UserRootPageID
	baseSystemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if preflight != nil {
		phaseStart := time.Now()
		if err = preflight(); err != nil {
			phaseStats.preflightNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
			return 0, nil, err
		}
		phaseStats.preflightNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	}

	systemOpts := systemRootOrderedPublishOptions(db)
	var retired []uint64
	var merged adaptive.Metrics
	for idx := range ordered {
		opts, err := db.orderedRootPublishOptionsForPolicy(ordered[idx].StoragePolicy)
		if err != nil {
			return 0, nil, err
		}
		orderedConsumed[idx] = true
		phaseStart := time.Now()
		rootID, rootRetired, metrics, err := db.publishOrderedRootDeltaIterator(ordered[idx].BaseRoot, ordered[idx].Iter, opts)
		phaseStats.rootApplyNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
		phaseStats.rootApplyCalls++
		if err != nil {
			return 0, nil, err
		}
		rootIDs[idx] = rootID
		rootsObserved++
		retired = append(retired, rootRetired...)
		mergeOrderedRootPublishMetrics(&merged, metrics)
		phaseStats.rootApplyMetrics.add(metrics)
	}

	phaseStart := time.Now()
	iter, err := buildSystemDeltaIter(append([]uint64(nil), rootIDs...))
	phaseStats.systemBuildNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	if err != nil {
		return 0, nil, err
	}
	if iter == nil {
		return 0, nil, errors.New("nil system root delta iterator")
	}
	phaseStart = time.Now()
	rootID, rootRetired, metrics, err := db.publishOrderedRootDeltaIterator(baseSystemRoot, iter, systemOpts)
	phaseStats.systemApplyNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	phaseStats.systemApplyCalls++
	if err != nil {
		return 0, nil, err
	}
	newSystemRoot = rootID
	retired = append(retired, rootRetired...)
	mergeOrderedRootPublishMetrics(&merged, metrics)
	phaseStats.systemApplyMetrics.add(metrics)

	db.mu.RLock()
	curUserRoot := db.meta.UserRootPageID
	curSystemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if curUserRoot != userRoot || curSystemRoot != baseSystemRoot {
		return 0, nil, errors.New("concurrent modification detected during ordered root group publish")
	}

	// The system root was applied as a delta, so we do not have an exact
	// value-log ref delta for system-root pointer changes. Passing nil keeps the
	// tracker conservative by invalidating it after commit.
	var vlogRefDelta *valueLogRefDelta
	phaseStart = time.Now()
	err = db.finalizeCommit(userRoot, newSystemRoot, retired, false, merged, nil, true, vlogRefDelta, nil, nil)
	phaseStats.finalizeNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	phaseStats.finalizeCalls++
	if err != nil {
		return 0, nil, err
	}
	return newSystemRoot, rootIDs, nil
}

func (db *DB) publishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered []OrderedRootDeltaBatchPublishInput, preflight OrderedRootGroupPreflight, buildSystemDeltaIter OrderedRootGroupSystemBuilder) (newSystemRoot uint64, rootIDs []uint64, err error) {
	if preflight == nil && db != nil && !db.closing.Load() {
		var retry bool
		newSystemRoot, rootIDs, retry, err = db.tryPublishOrderedRootDeltaBatchGroupOptimistic(ordered, buildSystemDeltaIter)
		if err != nil || !retry {
			return newSystemRoot, rootIDs, err
		}
	}
	return db.publishOrderedRootDeltaBatchGroupWithSystemDeltaBuilderSerialized(ordered, preflight, buildSystemDeltaIter)
}

func orderedRootDeltaBatchGroupParallelApplyEligible(ordered []OrderedRootDeltaBatchPublishInput) bool {
	parallelActive := 0
	for idx := range ordered {
		if ordered[idx].Delta == nil || ordered[idx].Delta.IsEmpty() {
			continue
		}
		if ordered[idx].ParallelApply {
			parallelActive++
		}
	}
	return parallelActive >= orderedRootDeltaBatchGroupParallelApplyMinRoots
}

func validateOrderedRootReadOnlyPrepareResultOwnership(ordered []OrderedRootDeltaBatchPublishInput) error {
	// Keep this validation allocation-free. Ordered root groups are expected to
	// be small, and the read-only prepare reuse path is allocation-sensitive.
	for idx := range ordered {
		result := ordered[idx].ReadOnlyPrepareResult
		if !ordered[idx].PrepareReadOnly || result == nil {
			continue
		}
		for otherIdx := idx + 1; otherIdx < len(ordered); otherIdx++ {
			if ordered[otherIdx].PrepareReadOnly && ordered[otherIdx].ReadOnlyPrepareResult == result {
				return errors.New("ordered root read-only prepare result reused by multiple inputs")
			}
		}
	}
	return nil
}

func (db *DB) applyOrderedRootDeltaBatchGroupRoots(idx *indexGen, ordered []OrderedRootDeltaBatchPublishInput, alloc zipper.PageAllocator, coldBuildAlloc bulk.Allocator, includeOutputSnapshot bool) ([]orderedRootDeltaBatchGroupApplyResult, bool) {
	results := make([]orderedRootDeltaBatchGroupApplyResult, len(ordered))
	if err := validateOrderedRootReadOnlyPrepareResultOwnership(ordered); err != nil {
		if len(results) > 0 {
			// recordOrderedRootDeltaBatchGroupApplyResults uses attempted to find
			// terminal per-input errors. No root apply metrics are recorded for
			// errored results.
			results[0] = orderedRootDeltaBatchGroupApplyResult{idx: 0, err: err, attempted: true}
		}
		return results, false
	}
	var outputID preparedOutputID
	var outputTracker *allocTracker
	if tracker, ok := alloc.(*allocTracker); ok {
		outputTracker = tracker
		outputID = tracker.PreparedOutputID()
	}
	captureOutputSnapshot := func() {
		if !includeOutputSnapshot || outputTracker == nil {
			return
		}
		output := outputTracker.PreparedOutputSnapshot()
		for resultIdx := range results {
			if !results[resultIdx].attempted || results[resultIdx].err != nil {
				continue
			}
			results[resultIdx].output = &output
			results[resultIdx].outputID = output.ID
		}
	}
	applyOne := func(orderedIdx int, isolateOutput bool) orderedRootDeltaBatchGroupApplyResult {
		result := orderedRootDeltaBatchGroupApplyResult{
			idx:       orderedIdx,
			outputID:  outputID,
			attempted: true,
		}
		opts, err := db.orderedRootPublishOptionsForPolicy(ordered[orderedIdx].StoragePolicy)
		if err != nil {
			result.err = err
			return result
		}
		if ordered[orderedIdx].PrepareReadOnly {
			opts.applyOptions.PrepareReadOnly = true
			if resultOut := ordered[orderedIdx].ReadOnlyPrepareResult; resultOut != nil {
				opts.applyOptions.ReadOnlyPrepare = resultOut.ReuseOptions()
			}
			opts.readOnlyPrepareSummary = &result.readOnlyPrepareSummary
			if ordered[orderedIdx].ReadOnlyPrepareWorkerCount > 0 {
				opts.readOnlyPrepareWorkerSummary = &result.readOnlyPrepareWorkerSummary
				opts.readOnlyPrepareWorkerCount = ordered[orderedIdx].ReadOnlyPrepareWorkerCount
			}
			opts.readOnlyPrepareCallerResult = ordered[orderedIdx].ReadOnlyPrepareResult
			opts.readOnlyPrepareNs = &result.readOnlyPrepareNs
			opts.readOnlyPrepareAttempted = &result.readOnlyPrepareAttempted
		}
		beforePages, beforeLeafLogPtrs := uint64(0), uint64(0)
		if outputTracker != nil {
			beforePages, beforeLeafLogPtrs = outputTracker.PreparedOutputCounts()
		}
		rootAlloc := alloc
		rootColdBuildAlloc := coldBuildAlloc
		var counters []*preparedRootApplyOutputCounter
		if isolateOutput && outputTracker != nil {
			counter := &preparedRootApplyOutputCounter{inner: alloc, recorder: outputTracker}
			rootAlloc = counter
			counters = append(counters, counter)
			if coldBuildAlloc == nil || coldBuildAlloc == alloc {
				rootColdBuildAlloc = counter
			} else {
				coldCounter := &preparedRootApplyOutputCounter{inner: coldBuildAlloc, recorder: outputTracker}
				rootColdBuildAlloc = coldCounter
				counters = append(counters, coldCounter)
			}
		}
		rootID, pendingRetiredPages, metrics, err := db.publishOrderedRootDeltaBatchWithAllocator(idx, ordered[orderedIdx].BaseRoot, ordered[orderedIdx].Delta, opts, rootAlloc, rootColdBuildAlloc, ordered[orderedIdx].IncludeDeletedOnColdBuild)
		result.rootID = rootID
		result.pendingRetiredPages = pendingRetiredPages
		result.metrics = metrics
		result.err = err
		if err == nil {
			if len(counters) == 0 {
				afterPages, afterLeafLogPtrs := uint64(0), uint64(0)
				if outputTracker != nil {
					afterPages, afterLeafLogPtrs = outputTracker.PreparedOutputCounts()
				}
				result.outputPages = afterPages - beforePages
				result.outputLeafLogPtrs = afterLeafLogPtrs - beforeLeafLogPtrs
			} else {
				for _, counter := range counters {
					pages, leafLogPtrs := counter.counts()
					result.outputPages += pages
					result.outputLeafLogPtrs += leafLogPtrs
				}
			}
		}
		return result
	}

	if !orderedRootDeltaBatchGroupParallelApplyEligible(ordered) {
		for orderedIdx := range ordered {
			results[orderedIdx] = applyOne(orderedIdx, false)
			if results[orderedIdx].err != nil {
				captureOutputSnapshot()
				return results, false
			}
		}
		captureOutputSnapshot()
		return results, false
	}

	parallelRoots := 0
	for orderedIdx := range ordered {
		if ordered[orderedIdx].ParallelApply && ordered[orderedIdx].Delta != nil && !ordered[orderedIdx].Delta.IsEmpty() {
			parallelRoots++
		}
	}

	var wg sync.WaitGroup
	for orderedIdx := range ordered {
		if !ordered[orderedIdx].ParallelApply || ordered[orderedIdx].Delta == nil || ordered[orderedIdx].Delta.IsEmpty() {
			continue
		}
		wg.Add(1)
		go func(orderedIdx int) {
			defer wg.Done()
			results[orderedIdx] = applyOne(orderedIdx, true)
		}(orderedIdx)
	}
	wg.Wait()
	for orderedIdx := range ordered {
		if ordered[orderedIdx].ParallelApply && ordered[orderedIdx].Delta != nil && !ordered[orderedIdx].Delta.IsEmpty() {
			if results[orderedIdx].err != nil {
				captureOutputSnapshot()
				return results, false
			}
			continue
		}
		results[orderedIdx] = applyOne(orderedIdx, false)
		if results[orderedIdx].err != nil {
			captureOutputSnapshot()
			return results, false
		}
	}
	captureOutputSnapshot()
	return results, parallelRoots >= orderedRootDeltaBatchGroupParallelApplyMinRoots
}

func recordOrderedRootDeltaBatchGroupApplyResults(
	preparedGroup *preparedRootApplyGroup,
	rootIDs []uint64,
	results []orderedRootDeltaBatchGroupApplyResult,
	pendingRetiredPages *[]uint64,
	mergedMetrics *adaptive.Metrics,
	phaseStats *orderedRootDeltaGroupPublishPhaseStats,
	rootsObserved *int,
) error {
	var firstErr error
	for orderedIdx := range results {
		result := results[orderedIdx]
		if !result.attempted {
			continue
		}
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
			}
			continue
		}
		if orderedIdx < len(rootIDs) {
			rootIDs[orderedIdx] = result.rootID
		}
		if preparedGroup != nil {
			if result.output != nil {
				preparedGroup.markPreparedOutput(orderedIdx, result.rootID, *result.output)
			} else {
				preparedGroup.markPrepared(orderedIdx, result.rootID, result.outputID)
			}
		}
		if rootsObserved != nil {
			(*rootsObserved)++
		}
		if pendingRetiredPages != nil {
			*pendingRetiredPages = append(*pendingRetiredPages, result.pendingRetiredPages...)
		}
		if mergedMetrics != nil {
			mergeOrderedRootPublishMetrics(mergedMetrics, result.metrics)
		}
		if phaseStats != nil {
			phaseStats.rootApplyMetrics.add(result.metrics)
			phaseStats.rootApplyCalls++
			if result.readOnlyPrepareAttempted {
				summary := result.readOnlyPrepareSummary
				phaseStats.rootApplyReadOnlyPrepareNs += result.readOnlyPrepareNs
				phaseStats.rootApplyReadOnlyPrepareCalls++
				phaseStats.rootApplyReadOnlyPrepareOps += uint64(summary.Ops)
				phaseStats.rootApplyReadOnlyPrepareLeafSpans += uint64(summary.Spans)
				workerSummary := result.readOnlyPrepareWorkerSummary
				phaseStats.rootApplyReadOnlyPrepareWorker.targets += uint64(workerSummary.TargetWorkers)
				phaseStats.rootApplyReadOnlyPrepareWorker.ranges += uint64(workerSummary.Ranges)
				phaseStats.rootApplyReadOnlyPrepareWorker.minOps += uint64(workerSummary.MinRangeOps)
				phaseStats.rootApplyReadOnlyPrepareWorker.maxOps += uint64(workerSummary.MaxRangeOps)
				phaseStats.rootApplyReadOnlyPrepareWorker.singleSpan += uint64(workerSummary.SingleSpanRanges)
				if summary.ExactLeafSpans {
					phaseStats.rootApplyReadOnlyPrepareExactPlans++
				}
				if summary.Maintenance {
					phaseStats.rootApplyReadOnlyPrepareMaintenance++
				}
				if summary.ColdBuild {
					phaseStats.rootApplyReadOnlyPrepareColdBuilds++
				}
			}
		}
	}
	return firstErr
}

func orderedRootDeltaBatchGroupPreparedOutputCounts(results []orderedRootDeltaBatchGroupApplyResult) (pages, leafLogPtrs uint64) {
	for idx := range results {
		result := results[idx]
		if !result.attempted || result.err != nil {
			continue
		}
		pages += result.outputPages
		leafLogPtrs += result.outputLeafLogPtrs
	}
	return pages, leafLogPtrs
}

func (db *DB) tryPublishOrderedRootDeltaBatchGroupOptimistic(ordered []OrderedRootDeltaBatchPublishInput, buildSystemDeltaIter OrderedRootGroupSystemBuilder) (newSystemRoot uint64, rootIDs []uint64, retrySerialized bool, err error) {
	if buildSystemDeltaIter == nil {
		return 0, nil, false, errors.New("nil ordered root group system delta builder")
	}
	if db == nil {
		return 0, nil, false, ErrClosed
	}
	if db.closing.Load() {
		return 0, nil, false, ErrClosed
	}

	phaseStats := orderedRootDeltaGroupPublishPhaseStats{}
	rootsObserved := 0

	db.writeMu.RLock()
	if db.readOnly {
		db.writeMu.RUnlock()
		err = ErrReadOnly
		return 0, nil, false, err
	}
	idx := db.idx.Load()
	if idx == nil {
		db.writeMu.RUnlock()
		err = errors.New("missing index")
		return 0, nil, false, err
	}

	db.mu.RLock()
	baseUserRoot := db.meta.UserRootPageID
	baseSystemRoot := db.meta.SystemRootPageID
	baseSeq := db.meta.CommitSeq
	regID := idx.registry.Register(baseSeq)
	db.mu.RUnlock()

	defer func() {
		idx.registry.Unregister(regID)
		if err != nil || retrySerialized {
			db.writeMu.RUnlock()
		}
	}()
	if baseSystemRoot == 0 {
		retrySerialized = true
		return 0, nil, retrySerialized, nil
	}

	phaseStart := time.Now()
	var preparedGroup preparedRootApplyGroup
	includePreparedChecksum := db.testPreparedRootApplyHook != nil
	initPreparedRootApplyGroup(&preparedGroup, baseUserRoot, baseSystemRoot, ordered, includePreparedChecksum)
	phaseStats.preparedRootPrepareNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	preparedGroupObserved := false
	observePreparedGroup := func(state preparedRootApplyState) {
		if preparedGroupObserved {
			return
		}
		preparedGroupObserved = true
		observePreparedRootApplyGroup(db, &phaseStats, &preparedGroup, state)
	}
	publishObserved := false
	observePublish := func(wait, hold time.Duration, publishErr error) {
		if publishObserved {
			return
		}
		publishObserved = true
		db.observeOrderedRootDeltaGroupPublish(wait, hold, rootsObserved, phaseStats, publishErr)
	}
	defer func() {
		if err == nil && !retrySerialized {
			return
		}
		// Optimistic attempts can prepare data/system roots before failing or
		// falling back to the serialized path. Record those roots as abandoned
		// before allocator cleanup discards the prepared output.
		if !preparedGroupObserved {
			observePreparedGroup(preparedRootApplyStateAbandoned)
		}
		if err != nil && !publishObserved {
			db.observeOrderedRootDeltaGroupPreparedRootApply(phaseStats.preparedRootPrepareNs, phaseStats.preparedRootStats)
			return
		}
		if retrySerialized && !publishObserved {
			db.observeOrderedRootDeltaGroupPreparedRootApply(phaseStats.preparedRootPrepareNs, phaseStats.preparedRootStats)
		}
	}()

	rootTracker := db.newPreparedOutputAllocTracker(idx.allocator)
	var systemTracker *allocTracker
	commitStarted := false
	freeTrackedPages := func() {
		if freeErr := rootTracker.FreeAll(); freeErr != nil && err == nil {
			err = freeErr
		}
		if systemTracker != nil {
			if freeErr := systemTracker.FreeAll(); freeErr != nil && err == nil {
				err = freeErr
			}
		}
	}
	defer func() {
		if (err != nil || retrySerialized) && !commitStarted {
			freeTrackedPages()
		}
	}()

	rootIDs = make([]uint64, len(ordered))
	systemOpts := systemRootOrderedPublishOptions(db)
	var nonSystemPendingRetiredPages []uint64
	var nonSystemMetrics adaptive.Metrics
	phaseStart = time.Now()
	rootApplyResults, parallelRootApply := db.applyOrderedRootDeltaBatchGroupRoots(idx, ordered, rootTracker, rootTracker, includePreparedChecksum)
	phaseStats.rootApplyNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	if parallelRootApply {
		phaseStats.rootApplyParallelGroups++
		for orderedIdx := range ordered {
			if ordered[orderedIdx].ParallelApply && ordered[orderedIdx].Delta != nil && !ordered[orderedIdx].Delta.IsEmpty() {
				phaseStats.rootApplyParallelRoots++
			}
		}
	}
	outputPages, outputLeafLogPtrs := orderedRootDeltaBatchGroupPreparedOutputCounts(rootApplyResults)
	preparedGroup.noteSharedOutputCounts(outputPages, outputLeafLogPtrs)
	if applyErr := recordOrderedRootDeltaBatchGroupApplyResults(&preparedGroup, rootIDs, rootApplyResults, &nonSystemPendingRetiredPages, &nonSystemMetrics, &phaseStats, &rootsObserved); applyErr != nil {
		return 0, nil, false, applyErr
	}

	systemBaseRoot := baseSystemRoot
	var committedRootPages []uint64
	var committedSystemPages []uint64
	for attempt := 0; ; attempt++ {
		systemTracker = db.newPreparedOutputAllocTracker(idx.allocator)
		phaseStart := time.Now()
		iter, err := buildSystemDeltaIter(append([]uint64(nil), rootIDs...))
		phaseStats.systemBuildNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
		if err != nil {
			return 0, nil, false, err
		}
		if iter == nil {
			return 0, nil, false, errors.New("nil system root delta iterator")
		}
		systemDelta, err := orderedRootDeltaBatchFromIterator(iter)
		_ = iter.Close()
		if err != nil {
			return 0, nil, false, err
		}
		phaseStart = time.Now()
		systemPreparedIdx := preparedGroup.setSystemRoot(systemBaseRoot, systemDelta, includePreparedChecksum)
		phaseStats.preparedRootPrepareNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
		phaseStart = time.Now()
		rootID, systemPendingRetiredPages, systemMetrics, applyErr := db.publishOrderedRootDeltaBatchWithAllocator(idx, systemBaseRoot, systemDelta, systemOpts, systemTracker, systemTracker, systemBaseRoot == 0)
		phaseStats.systemApplyNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
		phaseStats.systemApplyCalls++
		_ = systemDelta.Close()
		if applyErr != nil {
			err = applyErr
			return 0, nil, false, err
		}
		if includePreparedChecksum {
			preparedGroup.markPreparedOutput(systemPreparedIdx, rootID, systemTracker.PreparedOutputSnapshot())
		} else {
			outputPages, outputLeafLogPtrs := systemTracker.PreparedOutputCounts()
			preparedGroup.markPreparedOutputCounts(systemPreparedIdx, rootID, systemTracker.PreparedOutputID(), outputPages, outputLeafLogPtrs)
		}
		phaseStats.systemApplyMetrics.add(systemMetrics)

		lockStart := time.Now()
		db.commitMu.Lock()
		holdStart := time.Now()
		wait := holdStart.Sub(lockStart)
		db.mu.RLock()
		curUserRoot := db.meta.UserRootPageID
		curSystemRoot := db.meta.SystemRootPageID
		db.mu.RUnlock()
		if curSystemRoot != systemBaseRoot {
			db.commitMu.Unlock()
			if freeErr := systemTracker.FreeAll(); freeErr != nil {
				err = freeErr
				return 0, nil, false, err
			}
			systemTracker = nil
			if curSystemRoot == 0 || attempt+1 >= orderedRootOptimisticSystemDeltaRebaseMaxAttempts {
				retrySerialized = true
				return 0, nil, retrySerialized, nil
			}
			systemBaseRoot = curSystemRoot
			continue
		}
		if curUserRoot == 0 {
			curUserRoot = baseUserRoot
		}

		pendingRetiredPages := append([]uint64(nil), nonSystemPendingRetiredPages...)
		pendingRetiredPages = append(pendingRetiredPages, systemPendingRetiredPages...)
		merged := nonSystemMetrics
		mergeOrderedRootPublishMetrics(&merged, systemMetrics)
		newSystemRoot = rootID

		// Batch-based grouped deltas have the same value-log reachability shape as
		// iterator-based grouped deltas: non-system roots changed, and the system
		// delta can change collection descriptors. Keep the incremental ref tracker
		// conservative by invalidating it after commit.
		var vlogRefDelta *valueLogRefDelta
		preparedGroup.markInstalling()
		guardNs, guardErr := db.runInstallGuard(orderedRootDeltaGroupSystemInstallGuard(systemBaseRoot))
		phaseStats.installGuardNs += guardNs
		phaseStats.installGuardCalls++
		if guardErr != nil {
			phaseStats.installGuardFailures++
			hold := time.Since(holdStart)
			db.commitMu.Unlock()
			observePreparedGroup(preparedRootApplyStateAbandoned)
			observePublish(wait, hold, guardErr)
			err = guardErr
			return 0, nil, false, err
		}
		phaseStart = time.Now()
		var post finalizeCommitPost
		commitStarted = true
		post, err = db.finalizeCommitLocked(curUserRoot, newSystemRoot, pendingRetiredPages, false, merged, nil, true, vlogRefDelta, nil, nil)
		phaseStats.finalizeNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
		phaseStats.finalizeCalls++
		hold := time.Since(holdStart)
		committedRootPages = rootTracker.Pages()
		committedSystemPages = systemTracker.Pages()
		db.commitMu.Unlock()
		if err != nil {
			observePreparedGroup(preparedRootApplyStateAbandoned)
			observePublish(wait, hold, err)
			return 0, nil, false, err
		}
		rootTracker.MarkInstalled()
		systemTracker.MarkInstalled()
		db.invalidateLeafGenerationSubtreeStats(append(committedRootPages, committedSystemPages...))
		db.finalizeCommitPostWork(post)
		db.writeMu.RUnlock()
		observePreparedGroup(preparedRootApplyStateInstalled)
		observePublish(wait, hold, nil)
		return newSystemRoot, rootIDs, false, nil
	}
}

func (db *DB) publishOrderedRootDeltaBatchGroupWithSystemDeltaBuilderSerialized(ordered []OrderedRootDeltaBatchPublishInput, preflight OrderedRootGroupPreflight, buildSystemDeltaIter OrderedRootGroupSystemBuilder) (newSystemRoot uint64, rootIDs []uint64, err error) {
	if buildSystemDeltaIter == nil {
		return 0, nil, errors.New("nil ordered root group system delta builder")
	}
	if db == nil {
		return 0, nil, ErrClosed
	}
	if db.closing.Load() {
		return 0, nil, ErrClosed
	}

	lockStart := time.Now()
	db.writeMu.Lock()
	holdStart := time.Now()
	wait := holdStart.Sub(lockStart)
	rootsObserved := 0
	phaseStats := orderedRootDeltaGroupPublishPhaseStats{}
	finished := false
	finishPublish := func() {
		if finished {
			return
		}
		finished = true
		hold := time.Since(holdStart)
		db.writeMu.Unlock()
		db.observeOrderedRootDeltaGroupPublish(wait, hold, rootsObserved, phaseStats, err)
	}
	defer finishPublish()

	if db.readOnly {
		err = ErrReadOnly
		return 0, nil, err
	}
	idxGen := db.idx.Load()
	if idxGen == nil {
		err = errors.New("missing index")
		return 0, nil, err
	}

	db.mu.RLock()
	userRoot := db.meta.UserRootPageID
	baseSystemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if preflight != nil {
		phaseStart := time.Now()
		if err = preflight(); err != nil {
			phaseStats.preflightNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
			return 0, nil, err
		}
		phaseStats.preflightNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	}

	phaseStart := time.Now()
	var preparedGroup preparedRootApplyGroup
	includePreparedChecksum := db.testPreparedRootApplyHook != nil
	initPreparedRootApplyGroup(&preparedGroup, userRoot, baseSystemRoot, ordered, includePreparedChecksum)
	phaseStats.preparedRootPrepareNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	preparedGroupObserved := false
	observePreparedGroup := func(state preparedRootApplyState) {
		if preparedGroupObserved {
			return
		}
		preparedGroupObserved = true
		observePreparedRootApplyGroup(db, &phaseStats, &preparedGroup, state)
	}
	defer func() {
		if !preparedGroupObserved && err != nil {
			observePreparedGroup(preparedRootApplyStateAbandoned)
		}
	}()

	rootTracker := db.newPreparedOutputAllocTracker(idxGen.allocator)
	systemTracker := db.newPreparedOutputAllocTracker(idxGen.allocator)
	commitFinished := false
	defer func() {
		if err != nil && !commitFinished {
			_ = rootTracker.FreeAll()
			_ = systemTracker.FreeAll()
		}
	}()

	rootIDs = make([]uint64, len(ordered))
	systemOpts := systemRootOrderedPublishOptions(db)
	var pendingRetiredPages []uint64
	var merged adaptive.Metrics
	phaseStart = time.Now()
	rootApplyResults, parallelRootApply := db.applyOrderedRootDeltaBatchGroupRoots(idxGen, ordered, rootTracker, rootTracker, includePreparedChecksum)
	phaseStats.rootApplyNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	if parallelRootApply {
		phaseStats.rootApplyParallelGroups++
		for orderedIdx := range ordered {
			if ordered[orderedIdx].ParallelApply && ordered[orderedIdx].Delta != nil && !ordered[orderedIdx].Delta.IsEmpty() {
				phaseStats.rootApplyParallelRoots++
			}
		}
	}
	outputPages, outputLeafLogPtrs := orderedRootDeltaBatchGroupPreparedOutputCounts(rootApplyResults)
	preparedGroup.noteSharedOutputCounts(outputPages, outputLeafLogPtrs)
	if applyErr := recordOrderedRootDeltaBatchGroupApplyResults(&preparedGroup, rootIDs, rootApplyResults, &pendingRetiredPages, &merged, &phaseStats, &rootsObserved); applyErr != nil {
		return 0, nil, applyErr
	}

	phaseStart = time.Now()
	iter, err := buildSystemDeltaIter(append([]uint64(nil), rootIDs...))
	phaseStats.systemBuildNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	if err != nil {
		return 0, nil, err
	}
	if iter == nil {
		return 0, nil, errors.New("nil system root delta iterator")
	}
	systemDelta, err := orderedRootDeltaBatchFromIterator(iter)
	_ = iter.Close()
	if err != nil {
		return 0, nil, err
	}
	phaseStart = time.Now()
	systemPreparedIdx := preparedGroup.setSystemRoot(baseSystemRoot, systemDelta, includePreparedChecksum)
	phaseStats.preparedRootPrepareNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	phaseStart = time.Now()
	rootID, systemPendingRetiredPages, metrics, err := db.publishOrderedRootDeltaBatchWithAllocator(idxGen, baseSystemRoot, systemDelta, systemOpts, systemTracker, systemTracker, baseSystemRoot == 0)
	phaseStats.systemApplyNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	phaseStats.systemApplyCalls++
	_ = systemDelta.Close()
	if err != nil {
		return 0, nil, err
	}
	if includePreparedChecksum {
		preparedGroup.markPreparedOutput(systemPreparedIdx, rootID, systemTracker.PreparedOutputSnapshot())
	} else {
		outputPages, outputLeafLogPtrs := systemTracker.PreparedOutputCounts()
		preparedGroup.markPreparedOutputCounts(systemPreparedIdx, rootID, systemTracker.PreparedOutputID(), outputPages, outputLeafLogPtrs)
	}
	newSystemRoot = rootID
	pendingRetiredPages = append(pendingRetiredPages, systemPendingRetiredPages...)
	mergeOrderedRootPublishMetrics(&merged, metrics)
	phaseStats.systemApplyMetrics.add(metrics)

	preparedGroup.markInstalling()
	guardNs, guardErr := db.runInstallGuard(orderedRootDeltaGroupInstallGuard(userRoot, baseSystemRoot))
	phaseStats.installGuardNs += guardNs
	phaseStats.installGuardCalls++
	if guardErr != nil {
		phaseStats.installGuardFailures++
		return 0, nil, guardErr
	}

	// Batch-based grouped deltas have the same value-log reachability shape as
	// iterator-based grouped deltas: non-system roots changed, and the system
	// delta can change collection descriptors. Keep the incremental ref tracker
	// conservative by invalidating it after commit.
	var vlogRefDelta *valueLogRefDelta
	phaseStart = time.Now()
	committedRootPages := rootTracker.Pages()
	committedSystemPages := systemTracker.Pages()
	err = db.finalizeCommit(userRoot, newSystemRoot, pendingRetiredPages, false, merged, nil, true, vlogRefDelta, nil, nil)
	phaseStats.finalizeNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	phaseStats.finalizeCalls++
	if err != nil {
		return 0, nil, err
	}
	commitFinished = true
	rootTracker.MarkInstalled()
	systemTracker.MarkInstalled()
	db.invalidateLeafGenerationSubtreeStats(append(committedRootPages, committedSystemPages...))
	observePreparedGroup(preparedRootApplyStateInstalled)
	return newSystemRoot, rootIDs, nil
}

func (db *DB) publishOrderedRootGroup(systemIter iterator.UnsafeIterator, ordered []OrderedRootPublishInput, buildSystemIter OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	if systemIter != nil && buildSystemIter != nil {
		return 0, nil, errors.New("ordered root group cannot use both system iterator and system builder")
	}
	if db == nil {
		return 0, nil, ErrClosed
	}
	if db.closing.Load() {
		return 0, nil, ErrClosed
	}

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	if db.readOnly {
		return 0, nil, ErrReadOnly
	}

	db.mu.RLock()
	userRoot := db.meta.UserRootPageID
	baseSystemRoot := db.meta.SystemRootPageID
	baseSeq := db.meta.CommitSeq
	db.mu.RUnlock()

	systemOpts := systemRootOrderedPublishOptions(db)
	newSystemRoot := baseSystemRoot
	var retired []uint64
	var merged adaptive.Metrics
	var systemStats orderedRootPublishStats
	var vlogRefDelta *valueLogRefDelta
	defer func() {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
	}()

	if systemIter != nil {
		rootID, rootRetired, metrics, publishStats, refDelta, err := db.publishOrderedRootIterator(baseSystemRoot, systemIter, systemOpts, true)
		if err != nil {
			return 0, nil, err
		}
		newSystemRoot = rootID
		retired = append(retired, rootRetired...)
		mergeOrderedRootPublishMetrics(&merged, metrics)
		systemStats = publishStats
		vlogRefDelta = refDelta
	}

	rootIDs := make([]uint64, len(ordered))
	orderedConsumed := make([]bool, len(ordered))
	defer closeUnconsumedOrderedRootPublishIterators(ordered, orderedConsumed)
	for idx := range ordered {
		opts, err := db.orderedRootPublishOptionsForPolicy(ordered[idx].StoragePolicy)
		if err != nil {
			return 0, nil, err
		}
		orderedConsumed[idx] = true
		rootID, rootRetired, metrics, _, _, err := db.publishOrderedRootIterator(ordered[idx].BaseRoot, ordered[idx].Iter, opts, false)
		if err != nil {
			return 0, nil, err
		}
		rootIDs[idx] = rootID
		retired = append(retired, rootRetired...)
		mergeOrderedRootPublishMetrics(&merged, metrics)
	}

	if buildSystemIter != nil {
		builtRootIDs := append([]uint64(nil), rootIDs...)
		iter, err := buildSystemIter(builtRootIDs)
		if err != nil {
			return 0, nil, err
		}
		if iter == nil {
			return 0, nil, errors.New("nil system root iterator")
		}
		rootID, rootRetired, metrics, publishStats, refDelta, err := db.publishOrderedRootIterator(baseSystemRoot, iter, systemOpts, true)
		if err != nil {
			return 0, nil, err
		}
		newSystemRoot = rootID
		retired = append(retired, rootRetired...)
		mergeOrderedRootPublishMetrics(&merged, metrics)
		systemStats = publishStats
		vlogRefDelta = refDelta
	}

	db.mu.RLock()
	curUserRoot := db.meta.UserRootPageID
	curSystemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if curUserRoot != userRoot || curSystemRoot != baseSystemRoot {
		return 0, nil, errors.New("concurrent modification detected during ordered root group publish")
	}

	forceRefTrackerRebuild := systemStats.collectionRootDescriptorReachabilityMayChange()
	if forceRefTrackerRebuild {
		// Collection descriptors make non-system roots part of value-log
		// reachability. The system-root ref delta alone is not an exact commit
		// delta for those roots, so force the tracker to rebuild from the full
		// maintenance root set.
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
		vlogRefDelta = nil
	}
	if vlogRefDelta == nil && !forceRefTrackerRebuild {
		vlogRefDelta = db.newNoopValueLogRefDeltaIfTrackable(baseSeq)
	}
	if err := db.finalizeCommit(userRoot, newSystemRoot, retired, false, merged, nil, true, vlogRefDelta, nil, nil); err != nil {
		return 0, nil, err
	}
	vlogRefDelta = nil
	if systemIter != nil {
		db.systemRootWarmPublishAttempts.Add(systemStats.warmAttempts)
		db.systemRootWarmNativeApplyAttempts.Add(systemStats.warmNativeApplyAttempts)
		db.systemRootWarmPublishRebuildFallbacks.Add(systemStats.warmRebuildFallbacks)
		db.systemRootWarmPreservedPages.Add(systemStats.warmPreservedPages)
		db.systemRootWarmRewrittenPages.Add(systemStats.warmRewrittenPages)
	}
	return newSystemRoot, rootIDs, nil
}
