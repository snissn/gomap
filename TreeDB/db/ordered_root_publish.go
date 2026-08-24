package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/TreeDB/zipper"
)

type orderedRootPublishPlan uint8

type orderedRootDeltaGroupSystemPublishMode uint8

// ErrCommandWALContextMissingFrame reports a command-WAL context publish that
// was called without the command frame that defines the publish LSN.
var ErrCommandWALContextMissingFrame = errors.New("treedb: command WAL context publish requires a command frame")

// ErrOrderedRootGroupCommandWALContextNilSystemBuilder reports a nil system
// delta builder passed to an ordered-root command-WAL context publish API.
var ErrOrderedRootGroupCommandWALContextNilSystemBuilder = errors.New("treedb: PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder: nil system builder")

// ErrOrderedRootDeltaBatchGroupCommandWALContextNilSystemBuilder reports a nil
// system delta builder passed to the batch ordered-root command-WAL context
// publish API.
var ErrOrderedRootDeltaBatchGroupCommandWALContextNilSystemBuilder = errors.New("treedb: PublishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilder: nil system builder")

// ErrStorageMaintenancePlanMissing reports a maintenance ordered-root publish
// that was called without a recognized storage-maintenance plan token.
var ErrStorageMaintenancePlanMissing = errors.New("treedb: storage-maintenance publish requires a recognized maintenance plan")

// ErrStorageMaintenanceRootDeltaMissing reports a maintenance publish with no
// root delta to rewrite. System-only logical changes must use command-WAL
// covered publish APIs.
var ErrStorageMaintenanceRootDeltaMissing = errors.New("treedb: storage-maintenance publish requires at least one maintenance root delta")

// ErrStorageMaintenanceRootDeltaEmpty reports a maintenance publish whose
// marked root delta did not actually rewrite its root.
var ErrStorageMaintenanceRootDeltaEmpty = errors.New("treedb: storage-maintenance publish requires every maintenance root delta to rewrite its root")

// ErrStorageMaintenanceRootDeltaIteratorMissing reports a maintenance publish
// whose marked root delta has no iterator to apply.
var ErrStorageMaintenanceRootDeltaIteratorMissing = errors.New("treedb: storage-maintenance publish requires every maintenance root delta to include an iterator")

// ErrStorageMaintenanceSystemBuilderMissing reports a maintenance publish with
// no system-root delta builder to atomically publish rewritten root IDs.
var ErrStorageMaintenanceSystemBuilderMissing = errors.New("treedb: storage-maintenance publish requires a system-delta builder")

// ErrStorageMaintenancePublishPreApplyFailed marks a storage-maintenance
// publish failure that happened before any root or system-root delta was
// applied. Maintenance callers may use this to clean newly copied physical
// assets without risking removal of data that a partially-applied root can
// reach.
var ErrStorageMaintenancePublishPreApplyFailed = errors.New("treedb: storage-maintenance publish failed before root apply")

var (
	errCommandWALContextZeroLSN = errors.New("treedb: command WAL context publish appended zero LSN")

	errOrderedRootPublishMissingIndex = errors.New("treedb: ordered root publish: missing index")
)

const (
	orderedRootPublishPlanColdBuild orderedRootPublishPlan = iota
	orderedRootPublishPlanWarmFallbackRebuild
	orderedRootPublishPlanWarmNativeApply
)

const (
	orderedRootDeltaGroupSystemPublishLogical orderedRootDeltaGroupSystemPublishMode = iota
	orderedRootDeltaGroupSystemPublishStorageMaintenance
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
	maxWarmDeltaOps       int
	leafPrefixCompression bool
	leafColumnar          bool
	packedValuePtr        bool
	internalBaseDelta     bool
	outerLeavesInValueLog bool
	leafPageLog           bulk.LeafPageAppender
	spanNativeRoute       OrderedRootSpanNativeRoute
	spanNativeContext     string
	spanNativeFallback    string
	appendOnlyAllocation  bool
}

type orderedRootDeltaBatchGroupApplyResult struct {
	idx         int
	rootID      uint64
	retired     []uint64
	metrics     adaptive.Metrics
	applyResult zipper.ApplyResult
	err         error
}

func (opts orderedRootPublishOptions) withSpanNativeRoute(route OrderedRootSpanNativeRoute, context string) orderedRootPublishOptions {
	if _, ok := orderedRootSpanNativeRouteIndex(route); !ok {
		route = ""
	}
	opts.spanNativeRoute = route
	opts.spanNativeContext = context
	return opts
}

func (opts orderedRootPublishOptions) withSpanNativeFallback(reason FlushSpanRunFallbackReason) orderedRootPublishOptions {
	if reason.Valid() && reason != FlushSpanRunFallbackUnknown {
		opts.spanNativeFallback = reason.String()
	}
	return opts
}

func (opts orderedRootPublishOptions) orderedRootSpanNativeRouteContext(defaultRoute OrderedRootSpanNativeRoute, defaultContext string) (OrderedRootSpanNativeRoute, string) {
	route := opts.spanNativeRoute
	if _, ok := orderedRootSpanNativeRouteIndex(route); !ok {
		route = defaultRoute
	}
	context := opts.spanNativeContext
	if context == "" {
		context = defaultContext
	}
	return route, context
}

func orderedRootDeltaBatchInputSpanNativeRoute(input OrderedRootDeltaBatchPublishInput, defaultRoute OrderedRootSpanNativeRoute, defaultContext string) (OrderedRootSpanNativeRoute, string) {
	if defaultRoute == OrderedRootSpanNativeRouteCommandWALPublish {
		return defaultRoute, defaultContext
	}
	route := input.SpanNativeRoute
	if _, ok := orderedRootSpanNativeRouteIndex(route); !ok {
		route = defaultRoute
	}
	context := input.SpanNativeContext
	if context == "" {
		context = defaultContext
	}
	return route, context
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

// StorageMaintenanceRootDeltaPublishInput describes a root-local physical
// storage-maintenance rewrite. It is intentionally separate from
// OrderedRootDeltaPublishInput so ordinary logical root-delta callers do not
// inherit maintenance-only fields or semantics.
type StorageMaintenanceRootDeltaPublishInput struct {
	BaseRoot      uint64
	Iter          iterator.UnsafeIterator
	StoragePolicy OrderedRootStoragePolicy
	// DurableResources transfers exact external handles made reachable by this
	// maintenance rewrite. The publish consumes the closure on every return.
	DurableResources *rootpublication.StableResourceSet
	// DurableResourceRequirements is the complete logical-reference closure for
	// every scoped reachability field in the rewritten candidate root.
	DurableResourceRequirements rootpublication.StableLogicalObligationRequirements
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
	// PrepareReadOnly runs the side-effect-free leaf-span planning pass before
	// warm root apply and records planning stats. It is observability/planning
	// only; it does not change publish output or enable parallel leaf execution.
	PrepareReadOnly bool
	// ReadOnlyPrepareWorkers is the requested future worker count used to build
	// deterministic contiguous leaf-span ranges when PrepareReadOnly is true.
	// Values <=0 skip worker-range construction while still validating spans.
	ReadOnlyPrepareWorkers int
	// SpanNativeRoute tags runtime ordered-root span-native observations for this
	// root-local batch. Empty uses the publish helper's default route.
	SpanNativeRoute OrderedRootSpanNativeRoute
	// SpanNativeContext is optional human-readable route context for diagnostics.
	// Empty uses the route's default context.
	SpanNativeContext string
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

func closeOrderedRootDeltaBatchPublishDeltas(ordered []OrderedRootDeltaBatchPublishInput) {
	for idx := range ordered {
		if ordered[idx].Delta != nil {
			_ = ordered[idx].Delta.Close()
		}
	}
}

// OrderedRootGroupSystemBuilder builds a target system-root iterator after the
// non-system roots in a group have been built. The rootIDs slice is ordered to
// match the OrderedRootPublishInput slice passed to
// PublishOrderedRootGroupWithSystemBuilder.
type OrderedRootGroupSystemBuilder func(rootIDs []uint64) (iterator.UnsafeIterator, error)

// CommandWALPublishContext carries the command-WAL LSN assigned to a grouped
// root publish. Builders that need the LSN in durable metadata should use the
// context-aware grouped publish APIs so command append and root publication
// remain one fail-closed boundary.
type CommandWALPublishContext struct {
	AppliedCommandLSN           uint64
	durableResources            *rootpublication.StableResourceSetBuilder
	durableResourceRequirements *rootpublication.StableLogicalObligationRequirements
	durableResourceMutation     *rootpublication.StableLogicalObligationMutation
}

// RegisterDurableLogicalObligationMutation supplies the exact root-local
// addition/removal evidence corresponding to the published root delta. It does
// not replace the complete requirements oracle; it authorizes bounded retained
// closure derivation only when removals are explicitly empty.
func (ctx CommandWALPublishContext) RegisterDurableLogicalObligationMutation(mutation rootpublication.StableLogicalObligationMutation) error {
	if ctx.durableResourceMutation == nil {
		return rootpublication.ErrResourceOwnership
	}
	merged, err := rootpublication.MergeStableLogicalObligationMutations(*ctx.durableResourceMutation, mutation)
	if err != nil {
		return err
	}
	*ctx.durableResourceMutation = merged
	return nil
}

// RegisterDurableResources transfers a producer-owned exact resource closure
// into this command-WAL root publication. The DB retains it through external
// sync, durable-meta publication, fallback-slot recovery, and eventual slot
// replacement. Builders must register resources before returning the root
// delta that makes them reachable.
func (ctx CommandWALPublishContext) RegisterDurableResources(resources *rootpublication.StableResourceSet) error {
	if resources == nil {
		return nil
	}
	if ctx.durableResources == nil {
		resources.Release()
		return rootpublication.ErrResourceOwnership
	}
	if err := ctx.durableResources.Merge(resources); err != nil {
		resources.Release()
		return err
	}
	return nil
}

// RegisterDurableLogicalObligationRequirements supplies the complete logical
// reference set for selected reachability fields in the candidate root. It is
// independent of RegisterDurableResources: retained references may be
// satisfied by exact handles inherited from the selected fallback slot, while
// newly produced references are supplied by the builder's resource closure.
func (ctx CommandWALPublishContext) RegisterDurableLogicalObligationRequirements(requirements rootpublication.StableLogicalObligationRequirements) error {
	if ctx.durableResourceRequirements == nil {
		return rootpublication.ErrResourceOwnership
	}
	merged, err := rootpublication.MergeStableLogicalObligationRequirements(*ctx.durableResourceRequirements, requirements)
	if err != nil {
		return err
	}
	*ctx.durableResourceRequirements = merged
	return nil
}

// OrderedRootGroupCommandWALSystemBuilder builds a target system-root iterator
// after the command-WAL frame has been appended and the non-system roots have
// been built. For context-root publish APIs, rootIDs contains the original
// ordered inputs first, followed by any context-built roots in returned order,
// so it may be longer than the original ordered input slice. The rootIDs slice
// is borrowed for the duration of the call and must be treated as read-only.
// APIs without context-built roots receive only the original ordered root IDs.
type OrderedRootGroupCommandWALSystemBuilder func(CommandWALPublishContext, []uint64) (iterator.UnsafeIterator, error)

// OrderedRootGroupCommandWALDeltaBuilder builds additional root-local mutation
// streams after the command-WAL LSN has been assigned. It is for roots whose
// durable contents include the assigned AppliedCommandLSN. On a nil error, the
// DB publish path takes ownership of returned iterators and closes every
// unconsumed iterator. If a builder returns iterators with a non-nil error, the
// DB publish path closes those iterators before returning the error.
type OrderedRootGroupCommandWALDeltaBuilder func(CommandWALPublishContext) ([]OrderedRootDeltaPublishInput, error)

// OrderedRootDeltaBatchGroupCommandWALDeltaBuilder is the batch-materialized
// counterpart to OrderedRootGroupCommandWALDeltaBuilder. On success, returned
// batch deltas keep the normal OrderedRootDeltaBatchPublishInput ownership
// contract: the DB publish path does not close them, so builders that allocate
// batches must arrange cleanup after the enclosing publish call returns. If a
// builder returns deltas with a non-nil error, the DB publish path closes those
// deltas before returning the error. If context build succeeds but a later
// publish step fails, the DB closes the context-built batch deltas before
// returning the publish error.
type OrderedRootDeltaBatchGroupCommandWALDeltaBuilder func(CommandWALPublishContext) ([]OrderedRootDeltaBatchPublishInput, error)

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

func orderedRootIteratorLenHint(iter iterator.UnsafeIterator) int {
	if iter == nil {
		return 0
	}
	switch it := iter.(type) {
	case orderedRootLenHintIterator:
		if n := it.Len(); n > 0 {
			return n
		}
	case *pendingValueLogAppendPtrCollectingIterator:
		return orderedRootIteratorLenHint(it.UnsafeIterator)
	case *orderedRootTouchedIterator:
		return orderedRootIteratorLenHint(it.UnsafeIterator)
	}
	return 0
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

func (db *DB) orderedRootRewriteZipperForOptionsWithAllocator(idx *indexGen, opts orderedRootPublishOptions, alloc zipper.PageAllocator, state *DBState) (*zipper.Zipper, error) {
	z, err := db.orderedRootZipperForOptionsWithAllocator(idx, opts, alloc)
	if err != nil {
		return nil, err
	}
	if idx != nil && z == idx.zipper {
		// Value-log rewrite walks and rewrites leaf-log-backed trees while leaf-log
		// segments may be replaced/GCed. Do not let the process-wide leaf page read
		// cache feed stale entries back into the rewrite zipper; use an uncached
		// state-pinned reader for the duration of the maintenance apply instead.
		z = idx.zipper.CloneWithAllocator(alloc)
	}
	z.SetLeafPageReader(db.rewriteLeafPageReaderForState(state))
	return z, nil
}

func (db *DB) rewriteLeafPageReaderForState(state *DBState) zipper.LeafPageReader {
	if state != nil && state.ValueLogSet != nil {
		return newValueReader(state.ValueLogSet)
	}
	if db != nil && db.valueLogManager != nil {
		return db.valueLogManager
	}
	return nil
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
		val, ptr, flags, revision := iterator.UnsafeEntryWithRevision(iter)
		table.SetEntryWithRevision(iter.UnsafeKey(), val, ptr, flags, revision)
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
	baseVal, basePtr, baseFlags, baseRevision := iterator.UnsafeEntryWithRevision(baseIter)
	targetVal, targetPtr, targetFlags, targetRevision := iterator.UnsafeEntryWithRevision(targetIter)
	if baseRevision != targetRevision {
		return false
	}
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

const orderedRootTouchedValueLogSegmentLinearLimit = 8

type orderedRootTouchedIterator struct {
	iterator.UnsafeIterator
	touchedValueLogSegments   []uint32
	touchedValueLogSegmentSet map[uint32]struct{}
}

type orderedRootValueLogRefDeltaIterator struct {
	iterator.UnsafeIterator
	delta                          *valueLogRefDelta
	trackCollectionRootDescriptors bool
	currentCaptured                bool
}

func (it *orderedRootValueLogRefDeltaIterator) capture(key []byte, ptr page.ValuePtr, flags byte) {
	if it == nil || it.delta == nil || it.currentCaptured {
		return
	}
	it.currentCaptured = true
	if it.trackCollectionRootDescriptors &&
		(bytes.HasPrefix(key, collectionRootDescriptorPrefixBytes) ||
			bytes.HasPrefix(key, collectionRootOverlayDescriptorPrefixBytes)) {
		it.delta.requiresCandidateProjection = true
	}
	if flags&node.FlagPointer != 0 && page.IsValueLogFileID(ptr.FileID) {
		it.delta.add(ptr.FileID, 1)
	}
}

func (it *orderedRootValueLogRefDeltaIterator) UnsafeEntry() (val []byte, ptr page.ValuePtr, flags byte) {
	val, ptr, flags = it.UnsafeIterator.UnsafeEntry()
	it.capture(it.UnsafeIterator.UnsafeKey(), ptr, flags)
	return val, ptr, flags
}

func (it *orderedRootValueLogRefDeltaIterator) UnsafeEntryWithRevision() (val []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision) {
	val, ptr, flags, revision = iterator.UnsafeEntryWithRevision(it.UnsafeIterator)
	it.capture(it.UnsafeIterator.UnsafeKey(), ptr, flags)
	return val, ptr, flags, revision
}

func (it *orderedRootValueLogRefDeltaIterator) Next() {
	it.currentCaptured = false
	it.UnsafeIterator.Next()
}

func (it *orderedRootValueLogRefDeltaIterator) Seek(key []byte) {
	it.currentCaptured = false
	it.UnsafeIterator.Seek(key)
}

func (it *orderedRootTouchedIterator) capture() {
	if it == nil {
		return
	}
	fileID, ok := orderedRootEntryValueLogFileID(it.UnsafeIterator)
	if !ok {
		return
	}
	it.appendTouchedValueLogSegmentID(fileID)
}

func (it *orderedRootTouchedIterator) appendTouchedValueLogSegmentID(fileID uint32) {
	if it == nil || fileID == 0 {
		return
	}
	if it.touchedValueLogSegmentSet != nil {
		if _, ok := it.touchedValueLogSegmentSet[fileID]; ok {
			return
		}
		it.touchedValueLogSegmentSet[fileID] = struct{}{}
		it.touchedValueLogSegments = append(it.touchedValueLogSegments, fileID)
		return
	}
	for _, existing := range it.touchedValueLogSegments {
		if existing == fileID {
			return
		}
	}
	if len(it.touchedValueLogSegments) >= orderedRootTouchedValueLogSegmentLinearLimit {
		it.touchedValueLogSegmentSet = make(map[uint32]struct{}, len(it.touchedValueLogSegments)+1)
		for _, existing := range it.touchedValueLogSegments {
			if existing != 0 {
				it.touchedValueLogSegmentSet[existing] = struct{}{}
			}
		}
		it.touchedValueLogSegmentSet[fileID] = struct{}{}
	}
	it.touchedValueLogSegments = append(it.touchedValueLogSegments, fileID)
}

func (it *orderedRootTouchedIterator) Next() {
	it.capture()
	it.UnsafeIterator.Next()
}

func (it *orderedRootTouchedIterator) Seek(key []byte) {
	it.capture()
	it.UnsafeIterator.Seek(key)
}

func (it *orderedRootTouchedIterator) Close() error {
	it.capture()
	return it.UnsafeIterator.Close()
}

func appendOrderedRootDeltaBatchFinalTouchedValueLogSegments(delta *batch.Batch, dst []uint32) []uint32 {
	if delta == nil {
		return dst
	}
	var seen map[uint32]struct{}
	for _, entry := range delta.SortedEntries() {
		if entry.Type == batch.OpDelete || !entry.IsPtr || !page.IsValueLogFileID(entry.ValuePtr.FileID) {
			continue
		}
		if seen == nil {
			seen = make(map[uint32]struct{}, len(dst)+1)
			for _, existing := range dst {
				if existing != 0 {
					seen[existing] = struct{}{}
				}
			}
		}
		if _, ok := seen[entry.ValuePtr.FileID]; ok {
			continue
		}
		seen[entry.ValuePtr.FileID] = struct{}{}
		dst = append(dst, entry.ValuePtr.FileID)
	}
	return dst
}

func orderedRootBatchPut(delta *batch.Batch, iter iterator.UnsafeIterator, borrowEntryViews bool, trustedSortedUnique bool) error {
	if delta == nil || iter == nil || !iter.Valid() {
		return nil
	}
	val, ptr, flags, revision := iterator.UnsafeEntryWithRevision(iter)
	if flags&node.FlagTombstone != 0 {
		if borrowEntryViews && trustedSortedUnique {
			return delta.AppendDeleteViewTrustedSortedUniqueWithRevision(iter.UnsafeKey(), revision)
		}
		if borrowEntryViews {
			return delta.DeleteViewWithRevision(iter.UnsafeKey(), revision)
		}
		return delta.DeleteWithRevision(iter.UnsafeKey(), revision)
	}
	if flags&node.FlagPointer != 0 && page.IsValueLogFileID(ptr.FileID) {
		if borrowEntryViews && trustedSortedUnique {
			return delta.AppendPointerViewTrustedSortedUniqueWithRevision(iter.UnsafeKey(), ptr, revision)
		}
		if borrowEntryViews {
			return delta.SetPointerViewWithRevision(iter.UnsafeKey(), ptr, revision)
		}
		return delta.SetPointerWithRevision(iter.UnsafeKey(), ptr, revision)
	}
	if borrowEntryViews && trustedSortedUnique {
		return delta.AppendViewTrustedSortedUniqueWithRevision(iter.UnsafeKey(), val, revision)
	}
	if borrowEntryViews {
		return delta.SetViewWithRevision(iter.UnsafeKey(), val, revision)
	}
	return delta.SetWithRevision(iter.UnsafeKey(), val, revision)
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
			_, _, _, revision := iterator.UnsafeEntryWithRevision(iter)
			var err error
			if borrowEntryViews && trustedSortedUnique {
				err = delta.AppendDeleteViewTrustedSortedUniqueWithRevision(iter.UnsafeKey(), revision)
			} else if borrowEntryViews {
				err = delta.DeleteViewWithRevision(iter.UnsafeKey(), revision)
			} else {
				err = delta.DeleteWithRevision(iter.UnsafeKey(), revision)
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
	val, ptr, flags, _ := it.UnsafeEntryWithRevision()
	return val, ptr, flags
}

func (it *orderedRootDeltaBatchIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	if !it.Valid() {
		return nil, page.ValuePtr{}, node.FlagInline, page.LegacyEntryRevision
	}
	entry := it.entries[it.idx]
	if entry.Type == batch.OpDelete {
		return nil, page.ValuePtr{}, node.FlagTombstone, entry.Revision
	}
	if entry.IsPtr {
		return entry.Value, entry.ValuePtr, node.FlagPointer, entry.Revision
	}
	return entry.Value, page.ValuePtr{}, node.FlagInline, entry.Revision
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

func positiveValueLogRefDeltaFileIDs(delta *valueLogRefDelta, dst []uint32) []uint32 {
	if delta == nil {
		return dst
	}
	_ = delta.forEachChange(func(fileID uint32, change int64) error {
		if change > 0 {
			dst = append(dst, fileID)
		}
		return nil
	})
	return dst
}

func mergeValueLogRefDeltaInto(dst **valueLogRefDelta, src *valueLogRefDelta) {
	if src == nil {
		return
	}
	if *dst == nil {
		*dst = newValueLogRefDelta()
	}
	(*dst).requiresCandidateProjection = (*dst).requiresCandidateProjection || src.requiresCandidateProjection
	(*dst).allowEmptyDependencyReuse = (*dst).allowEmptyDependencyReuse || src.allowEmptyDependencyReuse
	(*dst).outerLeafDependencyReuse = (*dst).outerLeafDependencyReuse || src.outerLeafDependencyReuse
	_ = src.forEachChange(func(fileID uint32, change int64) error {
		(*dst).addChange(fileID, change)
		return nil
	})
	_ = src.forEachPositive(func(fileID uint32, count int64) error {
		(*dst).addPositive(fileID, count)
		return nil
	})
}

func addOrderedRootOuterLeafSegmentsToValueLogRefDelta(log LeafPageLog, delta *valueLogRefDelta) error {
	if log == nil || delta == nil || !delta.outerLeafDependencyReuse {
		return nil
	}
	created, err := leafPageLogCreatedSegments(log)
	if err != nil {
		return err
	}
	current, err := leafPageLogCurrentSegments(log)
	if err != nil {
		return err
	}
	segments := make([]LeafPageLogSegment, 0, len(created)+len(current))
	segments = append(segments, created...)
	segments = append(segments, current...)
	for _, segment := range dedupeLeafPageLogSegments(segments) {
		delta.addPositive(segment.FileID, 1)
	}
	return nil
}

func orderedRootDeltaMayChangeCollectionRootDescriptors(delta *batch.Batch) bool {
	if delta == nil {
		return false
	}
	entries, ranges := delta.ApplyPlan()
	for i := range entries {
		if bytes.HasPrefix(entries[i].Key, collectionRootDescriptorPrefixBytes) ||
			bytes.HasPrefix(entries[i].Key, collectionRootOverlayDescriptorPrefixBytes) {
			return true
		}
	}
	for i := range ranges {
		if orderedRootRangeOverlapsPrefix(ranges[i].Start, ranges[i].End, collectionRootDescriptorPrefixBytes, collectionRootDescriptorPrefixEnd()) ||
			orderedRootRangeOverlapsPrefix(ranges[i].Start, ranges[i].End, collectionRootOverlayDescriptorPrefixBytes, collectionRootOverlayDescriptorPrefixEnd()) {
			return true
		}
	}
	return false
}

func (db *DB) orderedRootCollectionDescriptorTransitionsCovered(idx *indexGen, userRoot, baseSystemRoot, newSystemRoot uint64, baseRoots, newRoots []uint64) bool {
	if db == nil || idx == nil || idx.pager == nil || baseSystemRoot == 0 || newSystemRoot == 0 || len(baseRoots) != len(newRoots) {
		return false
	}
	// Descriptor values may themselves be value-log pointers. The publication
	// path holds writeMu while both roots and all producer-reported segments are
	// stable, so the live manager can resolve them without refreshing inventory.
	// Missing, unregistered, or malformed pointers still fail this proof closed.
	baseEntries, err := vacuumCollectCollectionEntriesFromRoot(context.Background(), idx.pager, db.valueLogManager, baseSystemRoot)
	if err != nil {
		return false
	}
	newEntries, err := vacuumCollectCollectionEntriesFromRoot(context.Background(), idx.pager, db.valueLogManager, newSystemRoot)
	if err != nil {
		return false
	}
	return orderedRootCollectionDescriptorTransitionsCoveredEntries(
		baseEntries,
		newEntries,
		userRoot,
		baseSystemRoot,
		newSystemRoot,
		baseRoots,
		newRoots,
	)
}

func orderedRootCollectionDescriptorTransitionsCoveredEntries(
	baseEntries, newEntries []collectionEntry,
	userRoot, baseSystemRoot, newSystemRoot uint64,
	baseRoots, newRoots []uint64,
) bool {
	if baseSystemRoot == 0 || newSystemRoot == 0 || len(baseRoots) != len(newRoots) {
		return false
	}
	if len(baseEntries) == 0 || len(baseEntries) != len(newEntries) {
		// Cold attachment, removal, and key-set changes retain the exact
		// candidate scanner. The bounded proof only covers warm retargeting of
		// an already-attached descriptor set.
		return false
	}
	baseByKey := make(map[string][]uint64, len(baseEntries))
	baseReachable := make(map[uint64]struct{})
	for i := range baseEntries {
		baseByKey[string(baseEntries[i].key)] = baseEntries[i].sourceRootIDs
		for _, rootID := range baseEntries[i].sourceRootIDs {
			baseReachable[rootID] = struct{}{}
		}
	}
	newReachable := make(map[uint64]struct{})
	for i := range newEntries {
		for _, rootID := range newEntries[i].sourceRootIDs {
			newReachable[rootID] = struct{}{}
		}
	}
	if _, aliasesSystemRoot := baseReachable[baseSystemRoot]; aliasesSystemRoot {
		// The system-root delta and the grouped collection-root delta would
		// otherwise split one deduplicated maintenance root into two changes.
		return false
	}
	if _, aliasesSystemRoot := newReachable[newSystemRoot]; aliasesSystemRoot {
		// Likewise, joining a collection transition into the new system root
		// cannot be represented by independently merged ref deltas.
		return false
	}
	// Include the implicit system-root transition in the cross-role
	// reachability checks below. This also rejects transitions that reuse the
	// old system root as a new collection root or vice versa.
	baseReachable[baseSystemRoot] = struct{}{}
	newReachable[newSystemRoot] = struct{}{}
	if userRoot != 0 {
		// The primary user root is unchanged by these grouped collection
		// publications and participates in both maintenance closures. A
		// descriptor transition that aliases it cannot be represented by the
		// per-root replacement delta alone, so retain the exact projection.
		baseReachable[userRoot] = struct{}{}
		newReachable[userRoot] = struct{}{}
	}
	type rootTransition struct {
		base uint64
		next uint64
	}
	publishedTransitions := make(map[rootTransition]int, len(baseRoots))
	for i := range baseRoots {
		if baseRoots[i] != newRoots[i] {
			publishedTransitions[rootTransition{base: baseRoots[i], next: newRoots[i]}]++
		}
	}
	consumedTransitions := make(map[rootTransition]struct{}, len(publishedTransitions))
	baseToNew := make(map[uint64]uint64)
	newToBase := make(map[uint64]uint64)
	changed := false
	for i := range newEntries {
		baseRootIDs, ok := baseByKey[string(newEntries[i].key)]
		if !ok || len(baseRootIDs) != len(newEntries[i].sourceRootIDs) {
			return false
		}
		for rootIdx, newRootID := range newEntries[i].sourceRootIDs {
			baseRootID := baseRootIDs[rootIdx]
			if baseRootID == newRootID {
				continue
			}
			// The merged per-root ref deltas describe replacement of one
			// reachable root by one new reachable root. Reject alias splits,
			// joins, and partial retargets where either side remains reachable;
			// those set-level changes are not represented by a bijective delta.
			if _, remainsReachable := newReachable[baseRootID]; remainsReachable {
				return false
			}
			if _, alreadyReachable := baseReachable[newRootID]; alreadyReachable {
				return false
			}
			transition := rootTransition{base: baseRootID, next: newRootID}
			if publishedTransitions[transition] != 1 {
				return false
			}
			if mapped, ok := baseToNew[baseRootID]; ok && mapped != newRootID {
				return false
			}
			if mapped, ok := newToBase[newRootID]; ok && mapped != baseRootID {
				return false
			}
			baseToNew[baseRootID] = newRootID
			newToBase[newRootID] = baseRootID
			consumedTransitions[transition] = struct{}{}
			changed = true
		}
		delete(baseByKey, string(newEntries[i].key))
	}
	return changed &&
		len(baseByKey) == 0 &&
		len(consumedTransitions) == len(publishedTransitions)
}

func orderedRootRangeOverlapsPrefix(start, end, prefix, prefixEnd []byte) bool {
	return (len(end) == 0 || bytes.Compare(end, prefix) > 0) &&
		(len(start) == 0 || len(prefixEnd) == 0 || bytes.Compare(start, prefixEnd) < 0)
}

func (db *DB) publishOrderedRootDeltaIterator(baseRoot uint64, iter iterator.UnsafeIterator, opts orderedRootPublishOptions) (newRoot uint64, retired []uint64, metrics adaptive.Metrics, touchedValueLogSegments []uint32, err error) {
	newRoot, retired, metrics, touchedValueLogSegments, _, err = db.publishOrderedRootDeltaIteratorWithValueLogRefs(baseRoot, iter, opts, 0, false, false)
	return
}

func (db *DB) publishOrderedRootDeltaIteratorWithValueLogRefs(baseRoot uint64, iter iterator.UnsafeIterator, opts orderedRootPublishOptions, baseSeq uint64, trackValueLogRefs, trackCollectionRootDescriptors bool) (newRoot uint64, retired []uint64, metrics adaptive.Metrics, touchedValueLogSegments []uint32, vlogRefDelta *valueLogRefDelta, err error) {
	if db == nil {
		err = ErrClosed
		return
	}
	if iter == nil {
		err = errors.New("nil ordered root delta iterator")
		return
	}

	if baseRoot == 0 {
		route := OrderedRootSpanNativeRouteOverlayColdBuild
		context := "ordered-root delta iterator cold build"
		db.observeOrderedRootSpanNativeEligibility(db.orderedRootSpanNativeEligibility(orderedRootSpanNativeEligibilityRequest{
			Route:          route,
			Context:        context,
			DeltaOps:       orderedRootIteratorLenHint(iter),
			ForceColdBuild: true,
		}))
		buildIter := iter
		if trackValueLogRefs {
			vlogRefDelta = newValueLogRefDelta()
			vlogRefDelta.allowEmptyDependencyReuse = true
			vlogRefDelta.outerLeafDependencyReuse = opts.outerLeavesInValueLog
			buildIter = &orderedRootValueLogRefDeltaIterator{
				UnsafeIterator:                 buildIter,
				delta:                          vlogRefDelta,
				trackCollectionRootDescriptors: trackCollectionRootDescriptors,
			}
		}
		touchedIter := &orderedRootTouchedIterator{UnsafeIterator: buildIter}
		newRoot, retired, metrics, _, _, err = db.publishOrderedRootIterator(0, touchedIter, opts, false)
		touchedValueLogSegments = touchedIter.touchedValueLogSegments
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
		return 0, nil, metrics, nil, nil, err
	}
	defer delta.Close()
	if delta.IsEmpty() {
		db.observeOrderedRootSpanNativeNoopFallback(opts, OrderedRootSpanNativeRouteDeltaBatchPublish, "ordered-root delta iterator warm no-op")
		if trackValueLogRefs {
			vlogRefDelta = db.newNoopValueLogRefDeltaIfTrackable(baseSeq)
		}
		return baseRoot, nil, metrics, nil, vlogRefDelta, nil
	}
	rootZipper, err := db.orderedRootZipperForOptions(idx, opts)
	if err != nil {
		return 0, nil, metrics, nil, vlogRefDelta, err
	}
	applyOpts := db.orderedRootDeltaBatchApplyOptions(opts)
	applyOpts.CollectOldPointerRefs = trackValueLogRefs
	prepareBuf := db.acquireFlushApplyReadOnlyPrepareBuffer(applyOpts)
	if prepareBuf != nil {
		applyOpts.ReadOnlyPrepare = prepareBuf.opts
	}
	if flushApplyUseOptions(applyOpts) {
		result, applyErr := rootZipper.ApplyWithOptions(baseRoot, delta, applyOpts)
		db.observeFlushApplyPrepareResult(result, applyErr)
		route, context := opts.orderedRootSpanNativeRouteContext(OrderedRootSpanNativeRouteDeltaBatchPublish, "ordered-root delta iterator warm apply")
		db.observeOrderedRootSpanNativeApplyResult(
			route,
			context,
			result,
			applyErr,
			applyOpts.SpanNativeForceFallbackReason,
		)
		db.releaseFlushApplyReadOnlyPrepareBuffer(prepareBuf, &result)
		newRoot = result.RootID
		retired = result.PendingRetiredPages
		metrics = result.Metrics
		err = applyErr
		if err != nil {
			return
		}
		if trackValueLogRefs {
			entries, ranges := delta.ApplyPlan()
			vlogRefDelta, err = db.buildValueLogRefDeltaWithOptions(
				idx.pager, baseRoot, baseSeq, entries, ranges,
				&result.OldPointerRefs, result.OldEntriesRemoved, result.OldPointerRefsCollected,
				opts.outerLeavesInValueLog,
			)
			if err != nil {
				return 0, nil, metrics, touchedValueLogSegments, nil, err
			}
			if vlogRefDelta != nil {
				// This ordered multi-root path retains the predecessor raw-leaf
				// dependency set while admitting producer-reported current
				// segments. Logical ValuePtr removals remain represented by the
				// exact apply delta below. Ordinary DB-root publication does not
				// take this exception and continues to project destructively for
				// leaf-generation GC.
				vlogRefDelta.requiresCandidateProjection = false
				vlogRefDelta.allowEmptyDependencyReuse = true
				vlogRefDelta.outerLeafDependencyReuse = opts.outerLeavesInValueLog
				if trackCollectionRootDescriptors && orderedRootDeltaMayChangeCollectionRootDescriptors(delta) {
					// System collection descriptors attach non-system roots to
					// the durable reachability closure. Their nested value-log
					// references are not represented by the system tree's
					// apply-local pointer delta, so retain the exact candidate
					// projection fallback when descriptor reachability changes.
					vlogRefDelta.requiresCandidateProjection = true
				}
			}
		}
		touchedValueLogSegments = appendOrderedRootDeltaBatchFinalTouchedValueLogSegments(delta, nil)
		return
	}
	newRoot, retired, metrics, err = rootZipper.Apply(baseRoot, delta)
	if err != nil {
		return
	}
	touchedValueLogSegments = appendOrderedRootDeltaBatchFinalTouchedValueLogSegments(delta, nil)
	return
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
	return db.publishOrderedRootDeltaBatchWithAllocator(idx, baseRoot, delta, opts, idx.allocator, idx.allocator, false)
}

func (db *DB) buildOrderedRootDeltaBatchValueLogRefDelta(idx *indexGen, baseRoot, baseSeq uint64, delta *batch.Batch, outerLeavesInValueLog bool) (*valueLogRefDelta, error) {
	if db == nil || idx == nil || delta == nil || (!outerLeavesInValueLog && !db.shouldCollectValueLogRefDelta(baseSeq)) {
		return nil, nil
	}
	if baseRoot != 0 {
		// The batch-group apply API does not yet return apply-local old-pointer
		// evidence. Do not replace the candidate scan with one point lookup per
		// delta key; leave warm batches on the existing exact projection path.
		return nil, nil
	}
	entries, _ := delta.ApplyPlan()
	refDelta := newValueLogRefDelta()
	refDelta.allowEmptyDependencyReuse = true
	refDelta.outerLeafDependencyReuse = outerLeavesInValueLog
	for i := range entries {
		if entries[i].Type == batch.OpPut && entries[i].IsPtr && page.IsValueLogFileID(entries[i].ValuePtr.FileID) {
			refDelta.add(entries[i].ValuePtr.FileID, 1)
		}
	}
	return refDelta, nil
}

func (db *DB) orderedRootDeltaBatchApplyOptions(opts orderedRootPublishOptions) zipper.ApplyOptions {
	applyOpts := db.flushApplyOptions()
	if applyOpts.SpanNativeApply {
		applyOpts.SpanNativeAllowMaintenancePointOps = true
		if opts.spanNativeRoute != "" && !orderedRootSpanNativeRouteCanBeCandidate(opts.spanNativeRoute) {
			applyOpts.SpanNativeForceFallbackReason = FlushSpanRunFallbackRouteIneligible.String()
		}
	}
	if opts.spanNativeFallback != "" {
		applyOpts.SpanNativeForceFallbackReason = opts.spanNativeFallback
	}
	if applyOpts.ParallelApplyConcurrency <= 1 && !applyOpts.SpanNativeApply {
		applyOpts.PrepareReadOnly = false
		applyOpts.ReadOnlyPrepareWorkers = 0
	}
	return applyOpts
}

func (db *DB) observeOrderedRootSpanNativeNoopFallback(opts orderedRootPublishOptions, defaultRoute OrderedRootSpanNativeRoute, defaultContext string) {
	if db == nil {
		return
	}
	applyOpts := db.orderedRootDeltaBatchApplyOptions(opts)
	if !flushApplyUseOptions(applyOpts) {
		return
	}
	fallback := applyOpts.SpanNativeForceFallbackReason
	if fallback == "" {
		fallback = FlushSpanRunFallbackBelowThreshold.String()
	}
	route, context := opts.orderedRootSpanNativeRouteContext(defaultRoute, defaultContext)
	db.observeOrderedRootSpanNativeEligibility(db.orderedRootSpanNativeEligibility(orderedRootSpanNativeEligibilityRequest{
		Route:                  route,
		Context:                context,
		Summary:                zipper.ReadOnlyLeafSpanSummary{ExactLeafSpans: true},
		ExplicitFallbackReason: fallback,
	}))
}

func (db *DB) publishOrderedRootDeltaBatchWithAllocator(idx *indexGen, baseRoot uint64, delta *batch.Batch, opts orderedRootPublishOptions, alloc zipper.PageAllocator, coldBuildAlloc bulk.Allocator, includeDeletedOnColdBuild bool) (newRoot uint64, retired []uint64, metrics adaptive.Metrics, err error) {
	newRoot, retired, metrics, _, err = db.publishOrderedRootDeltaBatchWithAllocatorResult(idx, baseRoot, delta, opts, alloc, coldBuildAlloc, includeDeletedOnColdBuild, false)
	return
}

func (db *DB) publishOrderedRootDeltaBatchWithAllocatorResult(idx *indexGen, baseRoot uint64, delta *batch.Batch, opts orderedRootPublishOptions, alloc zipper.PageAllocator, coldBuildAlloc bulk.Allocator, includeDeletedOnColdBuild, collectOldPointerRefs bool) (newRoot uint64, retired []uint64, metrics adaptive.Metrics, applyResult zipper.ApplyResult, err error) {
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
	if delta.IsEmpty() {
		db.observeOrderedRootSpanNativeNoopFallback(opts, OrderedRootSpanNativeRouteDeltaBatchPublish, "ordered-root delta batch no-op")
		return baseRoot, nil, metrics, applyResult, nil
	}
	if baseRoot == 0 {
		route := OrderedRootSpanNativeRouteOverlayColdBuild
		context := "ordered-root delta batch cold build"
		db.observeOrderedRootSpanNativeEligibility(db.orderedRootSpanNativeEligibility(orderedRootSpanNativeEligibilityRequest{
			Route:          route,
			Context:        context,
			DeltaOps:       delta.Len(),
			ForceColdBuild: true,
		}))
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
		return 0, nil, metrics, applyResult, err
	}
	applyOpts := db.orderedRootDeltaBatchApplyOptions(opts)
	applyOpts.CollectOldPointerRefs = collectOldPointerRefs
	prepareBuf := db.acquireFlushApplyReadOnlyPrepareBuffer(applyOpts)
	if prepareBuf != nil {
		applyOpts.ReadOnlyPrepare = prepareBuf.opts
	}
	if flushApplyUseOptions(applyOpts) {
		applyResult, err = rootZipper.ApplyWithOptions(baseRoot, delta, applyOpts)
		db.observeFlushApplyPrepareResult(applyResult, err)
		route, context := opts.orderedRootSpanNativeRouteContext(OrderedRootSpanNativeRouteDeltaBatchPublish, "ordered-root delta batch warm apply")
		db.observeOrderedRootSpanNativeApplyResult(
			route,
			context,
			applyResult,
			err,
			applyOpts.SpanNativeForceFallbackReason,
		)
		db.releaseFlushApplyReadOnlyPrepareBuffer(prepareBuf, &applyResult)
		newRoot = applyResult.RootID
		retired = applyResult.PendingRetiredPages
		metrics = applyResult.Metrics
		return
	}
	newRoot, retired, metrics, err = rootZipper.Apply(baseRoot, delta)
	return
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
	alloc := zipper.PageAllocator(idx.allocator)
	if opts.appendOnlyAllocation {
		alloc = appendOnlyPageAllocator{alloc: idx.allocator}
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
				rootZipper, zipperErr := db.orderedRootZipperForOptionsWithAllocator(idx, opts, alloc)
				if zipperErr != nil {
					err = zipperErr
					return
				}
				newRoot, retired, metrics, err = rootZipper.Apply(baseRoot, delta)
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
		newRoot, err = bulk.BuildWithOptions(buildIter, alloc, idx.pager, bulk.BuildOptions{
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
	db.teardownMu.RLock()
	defer db.teardownMu.RUnlock()

	db.writeMu.Lock()
	writeLocked := true
	defer func() {
		if writeLocked {
			db.writeMu.Unlock()
		}
	}()
	if err := db.checkWriteAdmissionLocked(); err != nil {
		return 0, err
	}

	if db.readOnly {
		return 0, ErrReadOnly
	}
	if err := db.rejectUnloggedCommandWALRootPublish(); err != nil {
		return 0, err
	}

	db.mu.RLock()
	userRoot := db.meta.UserRootPageID
	systemRoot := db.meta.SystemRootPageID
	baseSeq := db.meta.CommitSeq
	db.mu.RUnlock()

	opts := systemRootOrderedPublishOptions(db)
	// baseRoot is supplied by the caller and can outlive a previous root
	// publication's write lock. Append-only allocation keeps this build from
	// reusing any page that the caller's tree still references.
	opts.appendOnlyAllocation = true
	newRoot, retired, metrics, _, _, err := db.publishOrderedRootIterator(baseRoot, iter, opts, false)
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

	post, err := db.finalizeCommitReleasingRootSerialization(
		userRoot, systemRoot, retired, false, metrics, nil, true, vlogRefDelta, nil, nil,
		finalizeCommitOptions{closeTeardownPinned: true},
		baseSeq,
		func() {
			db.writeMu.Unlock()
			writeLocked = false
		},
		nil,
	)
	if err != nil {
		return 0, err
	}
	vlogRefDelta = nil
	db.finalizeCommitPostWork(post)
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
	db.teardownMu.RLock()
	defer db.teardownMu.RUnlock()

	lockStart := time.Now()
	db.writeMu.Lock()
	holdStart := time.Now()
	wait := holdStart.Sub(lockStart)
	rootsObserved := 0
	phaseStats := orderedRootDeltaGroupPublishPhaseStats{}
	finished := false
	writeLocked := true
	var hold time.Duration
	releaseWrite := func() {
		if !writeLocked {
			return
		}
		hold = time.Since(holdStart)
		db.writeMu.Unlock()
		writeLocked = false
	}
	finishPublish := func() {
		if finished {
			return
		}
		finished = true
		releaseWrite()
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
	if err = db.checkWriteAdmissionLocked(); err != nil {
		return 0, nil, err
	}

	if db.readOnly {
		err = ErrReadOnly
		return 0, nil, err
	}
	if err = db.rejectUnloggedCommandWALRootPublish(); err != nil {
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
	var touchedValueLogSegments []uint32
	var ptrCollectors []*pendingValueLogAppendPtrCollectingIterator
	defer func() {
		for _, collector := range ptrCollectors {
			db.releasePendingValueLogAppendPtrCollector(collector)
		}
	}()
	for idx := range ordered {
		opts, err := db.orderedRootPublishOptionsForPolicy(ordered[idx].StoragePolicy)
		if err != nil {
			return 0, nil, err
		}
		orderedConsumed[idx] = true
		phaseStart := time.Now()
		ptrCollector, collectedIter := newPendingValueLogAppendPtrCollectingIterator(ordered[idx].Iter)
		ptrCollectors = append(ptrCollectors, ptrCollector)
		rootID, rootRetired, metrics, rootTouched, err := db.publishOrderedRootDeltaIterator(ordered[idx].BaseRoot, collectedIter, opts)
		phaseStats.rootApplyNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
		phaseStats.rootApplyCalls++
		if err != nil {
			return 0, nil, err
		}
		touchedValueLogSegments = append(touchedValueLogSegments, rootTouched...)
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
		if iter != nil {
			_ = iter.Close()
		}
		return 0, nil, err
	}
	if iter == nil {
		return 0, nil, errors.New("nil system root iterator")
	}
	phaseStart = time.Now()
	ptrCollector, collectedIter := newPendingValueLogAppendPtrCollectingIterator(iter)
	ptrCollectors = append(ptrCollectors, ptrCollector)
	rootID, rootRetired, metrics, publishStats, refDelta, err := db.publishOrderedRootIterator(baseSystemRoot, collectedIter, systemOpts, true)
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
	touchedValueLogSegments = positiveValueLogRefDeltaFileIDs(refDelta, touchedValueLogSegments)
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
	var post finalizeCommitPost
	post, err = db.finalizeCommitReleasingRootSerialization(
		userRoot, newSystemRoot, retired, false, merged, touchedValueLogSegments, true, vlogRefDelta, nil, nil,
		finalizeCommitOptions{closeTeardownPinned: true}, baseSeq, releaseWrite, nil,
	)
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
	db.finalizeCommitPostWork(post)
	return newSystemRoot, rootIDs, nil
}

// PublishOrderedRootDeltaGroupWithSystemDeltaBuilder applies root-local
// mutation streams to non-system roots, then applies a root-local mutation
// stream to the system root. The system delta should contain only changed
// system-root entries; omitted system entries are preserved.
func (db *DB) PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered []OrderedRootDeltaPublishInput, buildSystemDeltaIter OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered, nil, nil, buildSystemDeltaIter, orderedRootDeltaGroupSystemPublishLogical)
}

// PublishOrderedRootDeltaGroupWithCommandWALAndSystemDeltaBuilder applies the
// grouped root delta under one command-WAL LSN. The command frame is appended
// while commit serialization is held and before publishing the metadata root
// tuple that advances AppliedCommandLSN.
func (db *DB) PublishOrderedRootDeltaGroupWithCommandWALAndSystemDeltaBuilder(ordered []OrderedRootDeltaPublishInput, intent *CommandWALIntent, buildSystemDeltaIter OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered, nil, intent, buildSystemDeltaIter, orderedRootDeltaGroupSystemPublishLogical)
}

// PublishStagedOrderedRootDeltaGroupWithCommandWALAndSystemDeltaBuilder is
// like PublishOrderedRootDeltaGroupWithCommandWALAndSystemDeltaBuilder, but
// assumes the caller already holds the command-WAL raw publish lock and its
// teardown lease.
func (db *DB) PublishStagedOrderedRootDeltaGroupWithCommandWALAndSystemDeltaBuilder(ordered []OrderedRootDeltaPublishInput, intent *CommandWALIntent, buildSystemDeltaIter OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaGroupWithSystemDeltaBuilderWithOptions(ordered, nil, intent, buildSystemDeltaIter, orderedRootDeltaGroupSystemPublishLogical, orderedRootCommandWALPublishOptions{rawPublishLocked: true, teardownPinned: true})
}

// PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder is
// like PublishOrderedRootDeltaGroupWithCommandWALAndSystemDeltaBuilder, but the
// system-delta builder receives the command-WAL LSN assigned to this publish.
// The command frame is appended before any roots are published; if later root
// or system publication fails, the open handle is poisoned and must be reopened
// for recovery before more command-WAL publishes can proceed.
func (db *DB) PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(ordered []OrderedRootDeltaPublishInput, intent *CommandWALIntent, buildSystemDeltaIter OrderedRootGroupCommandWALSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(ordered, nil, intent, nil, buildSystemDeltaIter, orderedRootCommandWALPublishOptions{})
}

// PublishOrderedRootDeltaGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder
// is like PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder,
// but may append additional root-local mutation streams after the command-WAL
// LSN is assigned. The returned rootIDs slice contains the original ordered
// inputs first, then any context-built roots in returned order, so it may be
// longer than len(ordered).
func (db *DB) PublishOrderedRootDeltaGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder(ordered []OrderedRootDeltaPublishInput, intent *CommandWALIntent, buildContextDeltas OrderedRootGroupCommandWALDeltaBuilder, buildSystemDeltaIter OrderedRootGroupCommandWALSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(ordered, nil, intent, buildContextDeltas, buildSystemDeltaIter, orderedRootCommandWALPublishOptions{})
}

// PublishStagedOrderedRootDeltaGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder
// is like PublishOrderedRootDeltaGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder,
// but assumes the caller already holds the command-WAL raw publish lock and its
// teardown lease.
func (db *DB) PublishStagedOrderedRootDeltaGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder(ordered []OrderedRootDeltaPublishInput, intent *CommandWALIntent, buildContextDeltas OrderedRootGroupCommandWALDeltaBuilder, buildSystemDeltaIter OrderedRootGroupCommandWALSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(ordered, nil, intent, buildContextDeltas, buildSystemDeltaIter, orderedRootCommandWALPublishOptions{rawPublishLocked: true, teardownPinned: true})
}

// PublishOrderedRootDeltaGroupWithPreflightCommandWALContextAndSystemDeltaBuilder
// is like PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder,
// but runs preflight before the command frame is appended.
func (db *DB) PublishOrderedRootDeltaGroupWithPreflightCommandWALContextAndSystemDeltaBuilder(ordered []OrderedRootDeltaPublishInput, preflight OrderedRootGroupPreflight, intent *CommandWALIntent, buildSystemDeltaIter OrderedRootGroupCommandWALSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(ordered, preflight, intent, nil, buildSystemDeltaIter, orderedRootCommandWALPublishOptions{})
}

// PublishStagedOrderedRootDeltaGroupWithPreflightCommandWALContextAndSystemDeltaBuilder
// is like PublishOrderedRootDeltaGroupWithPreflightCommandWALContextAndSystemDeltaBuilder,
// but assumes the caller already holds the command-WAL raw publish lock and its
// teardown lease.
func (db *DB) PublishStagedOrderedRootDeltaGroupWithPreflightCommandWALContextAndSystemDeltaBuilder(ordered []OrderedRootDeltaPublishInput, preflight OrderedRootGroupPreflight, intent *CommandWALIntent, buildSystemDeltaIter OrderedRootGroupCommandWALSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(ordered, preflight, intent, nil, buildSystemDeltaIter, orderedRootCommandWALPublishOptions{rawPublishLocked: true, teardownPinned: true})
}

// PublishOrderedRootDeltaGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder
// is like PublishOrderedRootDeltaGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder,
// but runs preflight before the command frame is appended. The returned rootIDs
// slice contains the original ordered inputs first, then any context-built roots
// in returned order, so it may be longer than len(ordered).
func (db *DB) PublishOrderedRootDeltaGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder(ordered []OrderedRootDeltaPublishInput, preflight OrderedRootGroupPreflight, intent *CommandWALIntent, buildContextDeltas OrderedRootGroupCommandWALDeltaBuilder, buildSystemDeltaIter OrderedRootGroupCommandWALSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(ordered, preflight, intent, buildContextDeltas, buildSystemDeltaIter, orderedRootCommandWALPublishOptions{})
}

// PublishStagedOrderedRootDeltaGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder
// is like PublishOrderedRootDeltaGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder,
// but assumes the caller already holds the command-WAL raw publish lock and its
// teardown lease.
func (db *DB) PublishStagedOrderedRootDeltaGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder(ordered []OrderedRootDeltaPublishInput, preflight OrderedRootGroupPreflight, intent *CommandWALIntent, buildContextDeltas OrderedRootGroupCommandWALDeltaBuilder, buildSystemDeltaIter OrderedRootGroupCommandWALSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(ordered, preflight, intent, buildContextDeltas, buildSystemDeltaIter, orderedRootCommandWALPublishOptions{rawPublishLocked: true, teardownPinned: true})
}

// PublishOrderedRootDeltaGroupWithPreflightAndSystemDeltaBuilder is like
// PublishOrderedRootDeltaGroupWithSystemDeltaBuilder, but runs preflight under
// the DB write lock before applying root-local deltas.
func (db *DB) PublishOrderedRootDeltaGroupWithPreflightAndSystemDeltaBuilder(ordered []OrderedRootDeltaPublishInput, preflight OrderedRootGroupPreflight, buildSystemDeltaIter OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered, preflight, nil, buildSystemDeltaIter, orderedRootDeltaGroupSystemPublishLogical)
}

// PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder is like
// PublishOrderedRootDeltaGroupWithPreflightAndSystemDeltaBuilder, but permits
// TreeDB-internal storage-maintenance root rewrites while command-WAL mode is
// enabled. Callers must provide an internal maintenance plan and at least one
// maintenance root-delta input. System-only logical changes must use
// command-WAL-covered publish APIs. This path does not append or advance a
// command-WAL frame.
func (db *DB) PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(plan StorageMaintenancePlan, ordered []StorageMaintenanceRootDeltaPublishInput, preflight OrderedRootGroupPreflight, buildSystemDeltaIter OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	defer releaseStorageMaintenanceDurableResources(ordered)
	if buildSystemDeltaIter == nil {
		closeStorageMaintenanceRootDeltaPublishIterators(ordered)
		return 0, nil, storageMaintenancePreApplyError(ErrStorageMaintenanceSystemBuilderMissing)
	}
	if db == nil {
		closeStorageMaintenanceRootDeltaPublishIterators(ordered)
		return 0, nil, storageMaintenancePreApplyError(ErrClosed)
	}
	if db.closing.Load() {
		closeStorageMaintenanceRootDeltaPublishIterators(ordered)
		return 0, nil, storageMaintenancePreApplyError(ErrClosed)
	}
	if db.readOnly {
		closeStorageMaintenanceRootDeltaPublishIterators(ordered)
		return 0, nil, storageMaintenancePreApplyError(ErrReadOnly)
	}
	if err := validateStorageMaintenanceRootDeltaPublishInputs(plan, ordered); err != nil {
		closeStorageMaintenanceRootDeltaPublishIterators(ordered)
		return 0, nil, storageMaintenancePreApplyError(err)
	}
	durableResources, durableRequirements, err := collectStorageMaintenanceDurableClosure(ordered)
	if err != nil {
		closeStorageMaintenanceRootDeltaPublishIterators(ordered)
		return 0, nil, storageMaintenancePreApplyError(err)
	}
	// Collection transfers the producer sets into a new union. Keep that union
	// owned here as well as in the finalizer so every preflight/root-apply early
	// return releases its exact resource handles. Release is idempotent after a
	// successful ownership transfer to the durable-root candidate.
	defer durableResources.Release()
	return db.publishOrderedRootDeltaGroupWithSystemDeltaBuilderWithMaintenancePlan(plan, storageMaintenanceRootDeltaInputsToOrdered(ordered), preflight, nil, buildSystemDeltaIter, orderedRootDeltaGroupSystemPublishStorageMaintenance, orderedRootCommandWALPublishOptions{
		durableResources:            durableResources,
		durableResourceRequirements: durableRequirements,
	})
}

func collectStorageMaintenanceDurableClosure(ordered []StorageMaintenanceRootDeltaPublishInput) (*rootpublication.StableResourceSet, rootpublication.StableLogicalObligationRequirements, error) {
	requirements := rootpublication.StableLogicalObligationRequirements{}
	for idx := range ordered {
		merged, err := rootpublication.MergeStableLogicalObligationRequirements(requirements, ordered[idx].DurableResourceRequirements)
		if err != nil {
			return nil, rootpublication.StableLogicalObligationRequirements{}, fmt.Errorf("storage maintenance durable requirements input %d: %w", idx, err)
		}
		requirements = merged
	}
	builder := rootpublication.NewStableResourceSetBuilder()
	defer builder.Abandon()
	for idx := range ordered {
		resources := ordered[idx].DurableResources
		if resources == nil {
			continue
		}
		if err := builder.Merge(resources); err != nil {
			return nil, rootpublication.StableLogicalObligationRequirements{}, fmt.Errorf("storage maintenance durable resources input %d: %w", idx, err)
		}
	}
	resources, err := builder.Freeze()
	if err != nil {
		return nil, rootpublication.StableLogicalObligationRequirements{}, err
	}
	if resources.Len() == 0 {
		resources.Release()
		resources = nil
	}
	return resources, requirements, nil
}

func releaseStorageMaintenanceDurableResources(ordered []StorageMaintenanceRootDeltaPublishInput) {
	for idx := range ordered {
		ordered[idx].DurableResources.Release()
	}
}

func validateStorageMaintenanceOrderedRootDeltaInputs(plan StorageMaintenancePlan, ordered []OrderedRootDeltaPublishInput) error {
	return validateStorageMaintenanceRootDeltaInputs(plan, len(ordered), func(idx int) bool {
		return ordered[idx].Iter != nil
	})
}

func validateStorageMaintenanceRootDeltaPublishInputs(plan StorageMaintenancePlan, ordered []StorageMaintenanceRootDeltaPublishInput) error {
	return validateStorageMaintenanceRootDeltaInputs(plan, len(ordered), func(idx int) bool {
		return ordered[idx].Iter != nil
	})
}

func validateStorageMaintenanceRootDeltaInputs(plan StorageMaintenancePlan, rootDeltaCount int, hasIterator func(int) bool) error {
	if err := validateStorageMaintenanceRootDeltaCount(plan, rootDeltaCount); err != nil {
		return err
	}
	for idx := 0; idx < rootDeltaCount; idx++ {
		if !hasIterator(idx) {
			return fmt.Errorf("%w: ordered input %d", ErrStorageMaintenanceRootDeltaIteratorMissing, idx)
		}
	}
	return nil
}

func validateStorageMaintenanceRootDeltaCount(plan StorageMaintenancePlan, rootDeltaCount int) error {
	if !validStorageMaintenancePlan(plan) {
		return ErrStorageMaintenancePlanMissing
	}
	if rootDeltaCount == 0 {
		return ErrStorageMaintenanceRootDeltaMissing
	}
	return nil
}

func storageMaintenancePreApplyError(err error) error {
	if err == nil {
		return nil
	}
	return errors.Join(ErrStorageMaintenancePublishPreApplyFailed, err)
}

func closeStorageMaintenanceRootDeltaPublishIterators(ordered []StorageMaintenanceRootDeltaPublishInput) {
	for idx := range ordered {
		if ordered[idx].Iter != nil {
			_ = ordered[idx].Iter.Close()
		}
	}
}

func storageMaintenanceRootDeltaInputsToOrdered(ordered []StorageMaintenanceRootDeltaPublishInput) []OrderedRootDeltaPublishInput {
	if len(ordered) == 0 {
		return nil
	}
	converted := make([]OrderedRootDeltaPublishInput, len(ordered))
	for idx := range ordered {
		converted[idx] = OrderedRootDeltaPublishInput{
			BaseRoot:      ordered[idx].BaseRoot,
			Iter:          ordered[idx].Iter,
			StoragePolicy: ordered[idx].StoragePolicy,
		}
	}
	return converted
}

// PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder is like
// PublishOrderedRootDeltaGroupWithSystemDeltaBuilder, but the root-local
// mutation batches have already been materialized by the caller. This lets
// collection flush paths do iterator-to-batch work before entering the DB write
// critical section.
func (db *DB) PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered []OrderedRootDeltaBatchPublishInput, buildSystemDeltaIter OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered, nil, nil, buildSystemDeltaIter)
}

// PublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder is like
// PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder, but the root publish
// is covered by the supplied command-WAL intent.
func (db *DB) PublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder(ordered []OrderedRootDeltaBatchPublishInput, intent *CommandWALIntent, buildSystemDeltaIter OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered, nil, intent, buildSystemDeltaIter)
}

// PublishStagedOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder is
// like PublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder, but
// assumes the caller already holds the command-WAL raw publish lock and its
// teardown lease.
func (db *DB) PublishStagedOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder(ordered []OrderedRootDeltaBatchPublishInput, intent *CommandWALIntent, buildSystemDeltaIter OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaBatchGroupWithSystemDeltaBuilderWithOptions(ordered, nil, intent, buildSystemDeltaIter, orderedRootCommandWALPublishOptions{rawPublishLocked: true, teardownPinned: true})
}

// PublishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilder is
// like PublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder, but
// the system-delta builder receives the command-WAL LSN assigned to this
// publish.
func (db *DB) PublishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilder(ordered []OrderedRootDeltaBatchPublishInput, intent *CommandWALIntent, buildSystemDeltaIter OrderedRootGroupCommandWALSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilderSerialized(ordered, nil, intent, nil, buildSystemDeltaIter, orderedRootCommandWALPublishOptions{})
}

// PublishOrderedRootDeltaBatchGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder
// is like PublishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilder,
// but may append additional batch-materialized root deltas after the command-WAL
// LSN is assigned. The returned rootIDs slice contains the original ordered
// inputs first, then any context-built roots in returned order, so it may be
// longer than len(ordered).
func (db *DB) PublishOrderedRootDeltaBatchGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder(ordered []OrderedRootDeltaBatchPublishInput, intent *CommandWALIntent, buildContextDeltas OrderedRootDeltaBatchGroupCommandWALDeltaBuilder, buildSystemDeltaIter OrderedRootGroupCommandWALSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilderSerialized(ordered, nil, intent, buildContextDeltas, buildSystemDeltaIter, orderedRootCommandWALPublishOptions{})
}

// PublishStagedOrderedRootDeltaBatchGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder
// is like PublishOrderedRootDeltaBatchGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder,
// but assumes the caller already holds the command-WAL raw publish lock and its
// teardown lease.
func (db *DB) PublishStagedOrderedRootDeltaBatchGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder(ordered []OrderedRootDeltaBatchPublishInput, intent *CommandWALIntent, buildContextDeltas OrderedRootDeltaBatchGroupCommandWALDeltaBuilder, buildSystemDeltaIter OrderedRootGroupCommandWALSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilderSerialized(ordered, nil, intent, buildContextDeltas, buildSystemDeltaIter, orderedRootCommandWALPublishOptions{rawPublishLocked: true, teardownPinned: true})
}

// PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALAndSystemDeltaBuilder
// is like PublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder,
// but runs preflight under the DB write lock before appending the command-WAL
// frame or applying root-local deltas.
func (db *DB) PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALAndSystemDeltaBuilder(ordered []OrderedRootDeltaBatchPublishInput, preflight OrderedRootGroupPreflight, intent *CommandWALIntent, buildSystemDeltaIter OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered, preflight, intent, buildSystemDeltaIter)
}

// PublishStagedOrderedRootDeltaBatchGroupWithPreflightCommandWALAndSystemDeltaBuilder
// is like PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALAndSystemDeltaBuilder,
// but assumes the caller already holds the command-WAL raw publish lock and its
// teardown lease.
func (db *DB) PublishStagedOrderedRootDeltaBatchGroupWithPreflightCommandWALAndSystemDeltaBuilder(ordered []OrderedRootDeltaBatchPublishInput, preflight OrderedRootGroupPreflight, intent *CommandWALIntent, buildSystemDeltaIter OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaBatchGroupWithSystemDeltaBuilderWithOptions(ordered, preflight, intent, buildSystemDeltaIter, orderedRootCommandWALPublishOptions{rawPublishLocked: true, teardownPinned: true})
}

// PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALContextAndSystemDeltaBuilder
// is like PublishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilder,
// but runs preflight before the command frame is appended.
func (db *DB) PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALContextAndSystemDeltaBuilder(ordered []OrderedRootDeltaBatchPublishInput, preflight OrderedRootGroupPreflight, intent *CommandWALIntent, buildSystemDeltaIter OrderedRootGroupCommandWALSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilderSerialized(ordered, preflight, intent, nil, buildSystemDeltaIter, orderedRootCommandWALPublishOptions{})
}

// PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder
// is like PublishOrderedRootDeltaBatchGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder,
// but runs preflight before the command frame is appended. The returned rootIDs
// slice contains the original ordered inputs first, then any context-built roots
// in returned order, so it may be longer than len(ordered).
func (db *DB) PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder(ordered []OrderedRootDeltaBatchPublishInput, preflight OrderedRootGroupPreflight, intent *CommandWALIntent, buildContextDeltas OrderedRootDeltaBatchGroupCommandWALDeltaBuilder, buildSystemDeltaIter OrderedRootGroupCommandWALSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilderSerialized(ordered, preflight, intent, buildContextDeltas, buildSystemDeltaIter, orderedRootCommandWALPublishOptions{})
}

// PublishStagedOrderedRootDeltaBatchGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder
// is like PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder,
// but assumes the caller already holds the command-WAL raw publish lock and its
// teardown lease.
func (db *DB) PublishStagedOrderedRootDeltaBatchGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder(ordered []OrderedRootDeltaBatchPublishInput, preflight OrderedRootGroupPreflight, intent *CommandWALIntent, buildContextDeltas OrderedRootDeltaBatchGroupCommandWALDeltaBuilder, buildSystemDeltaIter OrderedRootGroupCommandWALSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilderSerialized(ordered, preflight, intent, buildContextDeltas, buildSystemDeltaIter, orderedRootCommandWALPublishOptions{rawPublishLocked: true, teardownPinned: true})
}

// PublishOrderedRootDeltaBatchGroupWithPreflightAndSystemDeltaBuilder is like
// PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder, but runs preflight
// under the DB write lock before applying root-local deltas.
func (db *DB) PublishOrderedRootDeltaBatchGroupWithPreflightAndSystemDeltaBuilder(ordered []OrderedRootDeltaBatchPublishInput, preflight OrderedRootGroupPreflight, buildSystemDeltaIter OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	return db.publishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered, preflight, nil, buildSystemDeltaIter)
}

func (db *DB) publishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered []OrderedRootDeltaPublishInput, preflight OrderedRootGroupPreflight, commandWALIntent *CommandWALIntent, buildSystemDeltaIter OrderedRootGroupSystemBuilder, mode orderedRootDeltaGroupSystemPublishMode) (newSystemRoot uint64, rootIDs []uint64, err error) {
	return db.publishOrderedRootDeltaGroupWithSystemDeltaBuilderWithOptions(ordered, preflight, commandWALIntent, buildSystemDeltaIter, mode, orderedRootCommandWALPublishOptions{})
}

func (db *DB) publishOrderedRootDeltaGroupWithSystemDeltaBuilderWithOptions(ordered []OrderedRootDeltaPublishInput, preflight OrderedRootGroupPreflight, commandWALIntent *CommandWALIntent, buildSystemDeltaIter OrderedRootGroupSystemBuilder, mode orderedRootDeltaGroupSystemPublishMode, opts orderedRootCommandWALPublishOptions) (newSystemRoot uint64, rootIDs []uint64, err error) {
	if commandWALIntent != nil {
		var commandBuilder OrderedRootGroupCommandWALSystemBuilder
		if buildSystemDeltaIter != nil {
			commandBuilder = func(_ CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
				return buildSystemDeltaIter(rootIDs)
			}
		}
		return db.publishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(ordered, preflight, commandWALIntent, nil, commandBuilder, opts)
	}
	return db.publishOrderedRootDeltaGroupWithSystemDeltaBuilderWithMaintenancePlan(nil, ordered, preflight, commandWALIntent, buildSystemDeltaIter, mode, opts)
}

func (db *DB) publishOrderedRootDeltaGroupWithSystemDeltaBuilderWithMaintenancePlan(plan StorageMaintenancePlan, ordered []OrderedRootDeltaPublishInput, preflight OrderedRootGroupPreflight, commandWALIntent *CommandWALIntent, buildSystemDeltaIter OrderedRootGroupSystemBuilder, mode orderedRootDeltaGroupSystemPublishMode, opts orderedRootCommandWALPublishOptions) (newSystemRoot uint64, rootIDs []uint64, err error) {
	storageMaintenance := mode == orderedRootDeltaGroupSystemPublishStorageMaintenance
	rootsObserved := 0
	preApplyErr := func(err error) error {
		if err == nil || !storageMaintenance || rootsObserved != 0 {
			return err
		}
		return errors.Join(ErrStorageMaintenancePublishPreApplyFailed, err)
	}
	if buildSystemDeltaIter == nil {
		return 0, nil, preApplyErr(errors.New("nil ordered root group system delta builder"))
	}
	if db == nil {
		return 0, nil, preApplyErr(ErrClosed)
	}
	if db.closing.Load() {
		return 0, nil, preApplyErr(ErrClosed)
	}
	if !opts.teardownPinned {
		db.teardownMu.RLock()
		defer db.teardownMu.RUnlock()
	}

	rawPublishLocked := !opts.rawPublishLocked && db.commandWALIntentNeedsPublicAppendLock(commandWALIntent, false)
	if rawPublishLocked {
		unlockCommandWALPublish, lockErr := db.lockCommandWALPublishWithBarriersTeardownPinned()
		if lockErr != nil {
			return 0, nil, preApplyErr(lockErr)
		}
		defer unlockCommandWALPublish()
		opts.rawPublishLocked = true
	}
	lockStart := time.Now()
	db.writeMu.Lock()
	holdStart := time.Now()
	wait := holdStart.Sub(lockStart)
	phaseStats := orderedRootDeltaGroupPublishPhaseStats{}
	finished := false
	writeLocked := true
	var hold time.Duration
	releaseWrite := func() {
		if !writeLocked {
			return
		}
		hold = time.Since(holdStart)
		db.writeMu.Unlock()
		writeLocked = false
	}
	finishPublish := func() {
		if finished {
			return
		}
		finished = true
		releaseWrite()
		db.observeOrderedRootDeltaGroupPublish(wait, hold, rootsObserved, phaseStats, err)
	}
	rootIDs = make([]uint64, len(ordered))
	orderedConsumed := make([]bool, len(ordered))
	defer closeUnconsumedOrderedRootDeltaPublishIterators(ordered, orderedConsumed)
	defer finishPublish()
	if err = db.checkWriteAdmissionLocked(); err != nil {
		return 0, nil, preApplyErr(err)
	}

	if db.readOnly {
		err = ErrReadOnly
		return 0, nil, preApplyErr(err)
	}
	if storageMaintenance {
		if err = validateStorageMaintenanceOrderedRootDeltaInputs(plan, ordered); err != nil {
			return 0, nil, preApplyErr(err)
		}
	}
	if commandWALIntent == nil {
		if storageMaintenance {
			err = db.commandWALPoisonedError()
		} else {
			err = db.rejectUnloggedCommandWALRootPublish()
		}
		if err != nil {
			return 0, nil, preApplyErr(err)
		}
	}

	db.mu.RLock()
	userRoot := db.meta.UserRootPageID
	baseSystemRoot := db.meta.SystemRootPageID
	baseSeq := db.meta.CommitSeq
	db.mu.RUnlock()
	if preflight != nil {
		phaseStart := time.Now()
		if err = preflight(); err != nil {
			phaseStats.preflightNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
			return 0, nil, preApplyErr(err)
		}
		phaseStats.preflightNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	}

	systemRoute := OrderedRootSpanNativeRouteSystemDeltaBuilderPublish
	systemContext := "ordered-root iterator delta group system delta apply"
	rootRoute := OrderedRootSpanNativeRouteMultiIndexGroupPublish
	rootContext := "multi-index ordered-root iterator group root apply"
	if commandWALIntent != nil {
		systemRoute = OrderedRootSpanNativeRouteCommandWALPublish
		systemContext = "command-WAL ordered-root iterator group system delta apply"
		rootRoute = OrderedRootSpanNativeRouteCommandWALPublish
		rootContext = "command-WAL ordered-root iterator group root apply"
	}
	systemOpts := systemRootOrderedPublishOptions(db).withSpanNativeRoute(systemRoute, systemContext)
	if storageMaintenance {
		systemOpts = systemOpts.withSpanNativeFallback(FlushSpanRunFallbackMaintenance)
	}
	var retired []uint64
	var merged adaptive.Metrics
	var touchedValueLogSegments []uint32
	var ptrCollectors []*pendingValueLogAppendPtrCollectingIterator
	defer func() {
		for _, collector := range ptrCollectors {
			db.releasePendingValueLogAppendPtrCollector(collector)
		}
	}()
	for idx := range ordered {
		opts, err := db.orderedRootPublishOptionsForPolicy(ordered[idx].StoragePolicy)
		if err != nil {
			return 0, nil, preApplyErr(err)
		}
		opts = opts.withSpanNativeRoute(rootRoute, rootContext)
		if storageMaintenance {
			opts = opts.withSpanNativeFallback(FlushSpanRunFallbackMaintenance)
		}
		orderedConsumed[idx] = true
		phaseStart := time.Now()
		ptrCollector, collectedIter := newPendingValueLogAppendPtrCollectingIterator(ordered[idx].Iter)
		ptrCollectors = append(ptrCollectors, ptrCollector)
		rootID, rootRetired, metrics, rootTouched, err := db.publishOrderedRootDeltaIterator(ordered[idx].BaseRoot, collectedIter, opts)
		phaseStats.rootApplyNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
		phaseStats.rootApplyCalls++
		if err != nil {
			return 0, nil, preApplyErr(err)
		}
		if storageMaintenance && rootID == ordered[idx].BaseRoot {
			return 0, nil, preApplyErr(fmt.Errorf("%w: ordered input %d", ErrStorageMaintenanceRootDeltaEmpty, idx))
		}
		touchedValueLogSegments = append(touchedValueLogSegments, rootTouched...)
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
		if iter != nil {
			_ = iter.Close()
		}
		return 0, nil, err
	}
	if iter == nil {
		return 0, nil, errors.New("nil system root delta iterator")
	}
	phaseStart = time.Now()
	ptrCollector, collectedIter := newPendingValueLogAppendPtrCollectingIterator(iter)
	ptrCollectors = append(ptrCollectors, ptrCollector)
	rootID, rootRetired, metrics, systemTouched, err := db.publishOrderedRootDeltaIterator(baseSystemRoot, collectedIter, systemOpts)
	phaseStats.systemApplyNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	phaseStats.systemApplyCalls++
	if err != nil {
		return 0, nil, err
	}
	touchedValueLogSegments = append(touchedValueLogSegments, systemTouched...)
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
	err = db.finalizeOrderedRootPublishWithCommandWALOptions(userRoot, newSystemRoot, retired, false, merged, touchedValueLogSegments, true, vlogRefDelta, nil, nil, baseSeq, commandWALIntent, opts, releaseWrite)
	phaseStats.finalizeNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	phaseStats.finalizeCalls++
	if err != nil {
		return 0, nil, err
	}
	return newSystemRoot, rootIDs, nil
}

func (db *DB) publishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(ordered []OrderedRootDeltaPublishInput, preflight OrderedRootGroupPreflight, commandWALIntent *CommandWALIntent, buildContextDeltas OrderedRootGroupCommandWALDeltaBuilder, buildSystemDeltaIter OrderedRootGroupCommandWALSystemBuilder, opts orderedRootCommandWALPublishOptions) (newSystemRoot uint64, rootIDs []uint64, err error) {
	if buildSystemDeltaIter == nil {
		return 0, nil, ErrOrderedRootGroupCommandWALContextNilSystemBuilder
	}
	if commandWALIntent == nil {
		return 0, nil, ErrCommandWALContextMissingFrame
	}
	if db == nil {
		return 0, nil, ErrClosed
	}
	if db.closing.Load() {
		return 0, nil, ErrClosed
	}

	materialize := func(inputs []OrderedRootDeltaPublishInput) ([]OrderedRootDeltaBatchPublishInput, func(), error) {
		converted := make([]OrderedRootDeltaBatchPublishInput, len(inputs))
		release := func() {
			for idx := range converted {
				if converted[idx].Delta != nil {
					_ = converted[idx].Delta.Close()
					converted[idx].Delta = nil
				}
			}
			closeUnconsumedOrderedRootDeltaPublishIterators(inputs, nil)
		}
		for idx := range inputs {
			delta, convertErr := orderedRootDeltaBatchFromIterator(inputs[idx].Iter)
			if convertErr != nil {
				release()
				return nil, nil, convertErr
			}
			converted[idx] = OrderedRootDeltaBatchPublishInput{
				BaseRoot:      inputs[idx].BaseRoot,
				Delta:         delta,
				StoragePolicy: inputs[idx].StoragePolicy,
			}
		}
		return converted, release, nil
	}

	converted, release, err := materialize(ordered)
	if err != nil {
		return 0, nil, err
	}
	defer release()

	var contextReleases []func()
	defer func() {
		for idx := len(contextReleases) - 1; idx >= 0; idx-- {
			contextReleases[idx]()
		}
	}()
	var batchContextBuilder OrderedRootDeltaBatchGroupCommandWALDeltaBuilder
	if buildContextDeltas != nil {
		batchContextBuilder = func(ctx CommandWALPublishContext) ([]OrderedRootDeltaBatchPublishInput, error) {
			contextInputs, buildErr := buildContextDeltas(ctx)
			if buildErr != nil {
				closeUnconsumedOrderedRootDeltaPublishIterators(contextInputs, nil)
				return nil, buildErr
			}
			contextConverted, contextRelease, convertErr := materialize(contextInputs)
			if convertErr != nil {
				return nil, convertErr
			}
			contextReleases = append(contextReleases, contextRelease)
			return contextConverted, nil
		}
	}
	return db.publishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilderSerialized(
		converted, preflight, commandWALIntent, batchContextBuilder, buildSystemDeltaIter, opts,
	)
}
func (db *DB) publishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered []OrderedRootDeltaBatchPublishInput, preflight OrderedRootGroupPreflight, commandWALIntent *CommandWALIntent, buildSystemDeltaIter OrderedRootGroupSystemBuilder) (newSystemRoot uint64, rootIDs []uint64, err error) {
	if commandWALIntent == nil && preflight == nil && db != nil && !db.closing.Load() {
		var retry bool
		newSystemRoot, rootIDs, retry, err = db.tryPublishOrderedRootDeltaBatchGroupOptimistic(ordered, buildSystemDeltaIter)
		if err != nil || !retry {
			return newSystemRoot, rootIDs, err
		}
	}
	return db.publishOrderedRootDeltaBatchGroupWithSystemDeltaBuilderWithOptions(ordered, preflight, commandWALIntent, buildSystemDeltaIter, orderedRootCommandWALPublishOptions{})
}

func (db *DB) publishOrderedRootDeltaBatchGroupWithSystemDeltaBuilderWithOptions(ordered []OrderedRootDeltaBatchPublishInput, preflight OrderedRootGroupPreflight, commandWALIntent *CommandWALIntent, buildSystemDeltaIter OrderedRootGroupSystemBuilder, opts orderedRootCommandWALPublishOptions) (newSystemRoot uint64, rootIDs []uint64, err error) {
	return db.publishOrderedRootDeltaBatchGroupWithSystemDeltaBuilderSerialized(ordered, preflight, commandWALIntent, buildSystemDeltaIter, opts)
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

func runOrderedRootDeltaBatchReadOnlyPrepare(rootZipper *zipper.Zipper, baseRoot uint64, delta *batch.Batch, workers int, prepareOpts zipper.ReadOnlyPrepareOptions) (zipper.ReadOnlyPrepareResult, zipper.ReadOnlyLeafSpanSummary, zipper.ReadOnlyLeafSpanWorkerRangeSummary, uint64, bool, error) {
	var prepared zipper.ReadOnlyPrepareResult
	var summary zipper.ReadOnlyLeafSpanSummary
	var workerSummary zipper.ReadOnlyLeafSpanWorkerRangeSummary
	if rootZipper == nil {
		return prepared, summary, workerSummary, 0, false, errors.New("missing ordered root zipper")
	}
	prepareStart := time.Now()
	prepared, err := rootZipper.PrepareReadOnly(baseRoot, delta, prepareOpts)
	prepareNs := orderedRootDeltaGroupPhaseDurationNs(prepareStart)
	summary = prepared.LeafSpanSummary()
	workerSummary = prepared.LeafSpanWorkerRangeSummary(workers)
	if err != nil {
		return prepared, summary, workerSummary, prepareNs, false, err
	}
	if validationErr := prepared.ValidateLeafSpans(); validationErr != nil {
		return prepared, summary, workerSummary, prepareNs, true, fmt.Errorf("treedb: invalid ordered-root read-only apply plan: %w", validationErr)
	}
	return prepared, summary, workerSummary, prepareNs, false, nil
}

func (db *DB) runOrderedRootDeltaBatchReadOnlyPrepare(rootZipper *zipper.Zipper, baseRoot uint64, delta *batch.Batch, workers int, applyOpts zipper.ApplyOptions) (zipper.ReadOnlyLeafSpanSummary, zipper.ReadOnlyLeafSpanWorkerRangeSummary, uint64, bool, error) {
	applyOpts.PrepareReadOnly = true
	applyOpts.ReadOnlyPrepareWorkers = workers
	prepareBuf := db.acquireFlushApplyReadOnlyPrepareBuffer(applyOpts)
	if prepareBuf != nil {
		applyOpts.ReadOnlyPrepare = prepareBuf.opts
	}
	if applyOpts.SpanNativeApply {
		applyOpts.ReadOnlyPrepare.OmitKeys = false
		if applyOpts.SpanNativeAllowMaintenancePointOps {
			applyOpts.ReadOnlyPrepare.AllowMaintenancePointLeafSpans = true
		}
	}
	prepared, summary, workerSummary, prepareNs, validationFailed, err := runOrderedRootDeltaBatchReadOnlyPrepare(rootZipper, baseRoot, delta, workers, applyOpts.ReadOnlyPrepare)
	if prepareBuf != nil {
		result := zipper.ApplyResult{ReadOnlyPrepare: prepared}
		db.releaseFlushApplyReadOnlyPrepareBuffer(prepareBuf, &result)
	}
	return summary, workerSummary, prepareNs, validationFailed, err
}

func addOrderedRootReadOnlyPreparePhaseStats(phases *orderedRootDeltaGroupPublishPhaseStats, summary zipper.ReadOnlyLeafSpanSummary, workerSummary zipper.ReadOnlyLeafSpanWorkerRangeSummary, prepareNs uint64, err error, validationFailed bool) {
	if phases == nil {
		return
	}
	phases.rootApplyReadOnlyPrepareNs += prepareNs
	phases.rootApplyReadOnlyPrepareCalls++
	if err != nil {
		phases.rootApplyReadOnlyPrepareErrors++
	}
	if validationFailed {
		phases.rootApplyReadOnlyPrepareValidationFail++
	}
	if workerSummary.TargetWorkers > 0 {
		requested := uint64(workerSummary.TargetWorkers)
		phases.rootApplyReadOnlyPrepareRequested += requested
		if requested > phases.rootApplyReadOnlyPrepareRequestedMax {
			phases.rootApplyReadOnlyPrepareRequestedMax = requested
		}
	}
	if summary.Spans > 0 {
		phases.rootApplyReadOnlyPrepareSpans += uint64(summary.Spans)
	}
	if summary.SpanOps > 0 {
		phases.rootApplyReadOnlyPrepareSpanOps += uint64(summary.SpanOps)
	}
	if summary.SpanBytes > 0 {
		phases.rootApplyReadOnlyPrepareSpanBytes += uint64(summary.SpanBytes)
	}
	if workerSummary.Ranges > 0 {
		phases.rootApplyReadOnlyPrepareWorkerRanges += uint64(workerSummary.Ranges)
	}
}

func (db *DB) prepareOrderedRootDeltaBatchGroupReadOnly(idx *indexGen, ordered []OrderedRootDeltaBatchPublishInput, alloc zipper.PageAllocator, phaseStats *orderedRootDeltaGroupPublishPhaseStats) error {
	for orderedIdx := range ordered {
		if !ordered[orderedIdx].PrepareReadOnly {
			continue
		}
		opts, err := db.orderedRootPublishOptionsForPolicy(ordered[orderedIdx].StoragePolicy)
		if err != nil {
			return err
		}
		rootZipper, err := db.orderedRootZipperForOptionsWithAllocator(idx, opts, alloc)
		if err != nil {
			return err
		}
		applyOpts := db.orderedRootDeltaBatchApplyOptions(opts)
		summary, workerSummary, prepareNs, validationFailed, err := db.runOrderedRootDeltaBatchReadOnlyPrepare(rootZipper, ordered[orderedIdx].BaseRoot, ordered[orderedIdx].Delta, ordered[orderedIdx].ReadOnlyPrepareWorkers, applyOpts)
		addOrderedRootReadOnlyPreparePhaseStats(phaseStats, summary, workerSummary, prepareNs, err, validationFailed)
		db.observeFlushApplyReadOnlyPrepare(summary, workerSummary, prepareNs, err, validationFailed)
		deltaOps := 0
		if ordered[orderedIdx].Delta != nil {
			deltaOps = ordered[orderedIdx].Delta.Len()
		}
		db.observeOrderedRootSpanNativeReadOnlyPrepare(summary, deltaOps, err, validationFailed, opts)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) applyOrderedRootDeltaBatchGroupRoots(idx *indexGen, ordered []OrderedRootDeltaBatchPublishInput, alloc zipper.PageAllocator, coldBuildAlloc bulk.Allocator, defaultRoute OrderedRootSpanNativeRoute, defaultContext string, collectOldPointerRefs bool) ([]orderedRootDeltaBatchGroupApplyResult, bool) {
	results := make([]orderedRootDeltaBatchGroupApplyResult, len(ordered))
	applyOne := func(orderedIdx int) orderedRootDeltaBatchGroupApplyResult {
		result := orderedRootDeltaBatchGroupApplyResult{idx: orderedIdx}
		opts, err := db.orderedRootPublishOptionsForPolicy(ordered[orderedIdx].StoragePolicy)
		if err != nil {
			result.err = err
			return result
		}
		route, context := orderedRootDeltaBatchInputSpanNativeRoute(ordered[orderedIdx], defaultRoute, defaultContext)
		opts = opts.withSpanNativeRoute(route, context)
		rootID, retired, metrics, applyResult, err := db.publishOrderedRootDeltaBatchWithAllocatorResult(idx, ordered[orderedIdx].BaseRoot, ordered[orderedIdx].Delta, opts, alloc, coldBuildAlloc, ordered[orderedIdx].IncludeDeletedOnColdBuild, collectOldPointerRefs)
		result.rootID = rootID
		result.retired = retired
		result.metrics = metrics
		result.applyResult = applyResult
		result.err = err
		return result
	}

	if !orderedRootDeltaBatchGroupParallelApplyEligible(ordered) {
		for orderedIdx := range ordered {
			results[orderedIdx] = applyOne(orderedIdx)
			if results[orderedIdx].err != nil {
				return results, false
			}
		}
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
			results[orderedIdx] = applyOne(orderedIdx)
		}(orderedIdx)
	}
	wg.Wait()
	for orderedIdx := range ordered {
		if ordered[orderedIdx].ParallelApply && ordered[orderedIdx].Delta != nil && !ordered[orderedIdx].Delta.IsEmpty() {
			if results[orderedIdx].err != nil {
				return results, false
			}
			continue
		}
		results[orderedIdx] = applyOne(orderedIdx)
		if results[orderedIdx].err != nil {
			return results, false
		}
	}
	return results, parallelRoots >= orderedRootDeltaBatchGroupParallelApplyMinRoots
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
	db.teardownMu.RLock()
	defer db.teardownMu.RUnlock()

	phaseStats := orderedRootDeltaGroupPublishPhaseStats{}
	rootsObserved := 0
	if hook := db.testOrderedRootBatchAfterClosePreflightHook; hook != nil {
		hook()
	}

	db.writeMu.RLock()
	if err = db.checkReadAdmissionLocked(); err != nil {
		db.writeMu.RUnlock()
		return 0, nil, false, err
	}
	if db.vacuumCutoverInProgress.Load() {
		db.writeMu.RUnlock()
		return 0, nil, true, nil
	}
	if db.readOnly {
		db.writeMu.RUnlock()
		err = ErrReadOnly
		return 0, nil, false, err
	}
	if err = db.rejectUnloggedCommandWALRootPublish(); err != nil {
		db.writeMu.RUnlock()
		return 0, nil, false, err
	}
	idx := db.idx.Load()
	if idx == nil {
		db.writeMu.RUnlock()
		err = errors.New("missing index")
		return 0, nil, false, err
	}

	db.rootReuseMu.RLock()
	db.mu.RLock()
	baseUserRoot := db.meta.UserRootPageID
	baseSystemRoot := db.meta.SystemRootPageID
	baseSeq := db.meta.CommitSeq
	regID := idx.registry.Register(baseSeq)
	db.mu.RUnlock()
	db.rootReuseMu.RUnlock()

	writeReadLocked := true
	defer func() {
		idx.registry.Unregister(regID)
		if writeReadLocked {
			db.writeMu.RUnlock()
		}
	}()
	if baseSystemRoot == 0 {
		retrySerialized = true
		return 0, nil, retrySerialized, nil
	}

	rootTracker := newAllocTracker(idx.allocator)
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
	var optimisticSystemDeltaReleaseEntries []batch.Entry
	defer func() {
		if retrySerialized {
			db.releasePendingValueLogAppendFileIDsFromEntries(optimisticSystemDeltaReleaseEntries)
			return
		}
		for idx := range ordered {
			db.releasePendingValueLogAppendFileIDsFromBatch(ordered[idx].Delta)
		}
		db.releasePendingValueLogAppendFileIDsFromEntries(optimisticSystemDeltaReleaseEntries)
	}()
	systemOpts := systemRootOrderedPublishOptions(db).withSpanNativeRoute(OrderedRootSpanNativeRouteSystemDeltaBuilderPublish, "ordered-root delta group system delta apply")
	var nonSystemRetired []uint64
	var nonSystemMetrics adaptive.Metrics
	var touchedValueLogSegments []uint32
	if err = db.prepareOrderedRootDeltaBatchGroupReadOnly(idx, ordered, rootTracker, &phaseStats); err != nil {
		return 0, nil, false, err
	}
	phaseStart := time.Now()
	rootApplyResults, parallelRootApply := db.applyOrderedRootDeltaBatchGroupRoots(idx, ordered, rootTracker, rootTracker, OrderedRootSpanNativeRouteMultiIndexGroupPublish, "multi-index ordered-root group root apply", false)
	phaseStats.rootApplyNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	if parallelRootApply {
		phaseStats.rootApplyParallelGroups++
		for orderedIdx := range ordered {
			if ordered[orderedIdx].ParallelApply && ordered[orderedIdx].Delta != nil && !ordered[orderedIdx].Delta.IsEmpty() {
				phaseStats.rootApplyParallelRoots++
			}
		}
	}
	for orderedIdx := range rootApplyResults {
		result := rootApplyResults[orderedIdx]
		if result.err != nil {
			return 0, nil, false, result.err
		}
		rootIDs[orderedIdx] = result.rootID
		rootsObserved++
		touchedValueLogSegments = appendOrderedRootDeltaBatchFinalTouchedValueLogSegments(ordered[orderedIdx].Delta, touchedValueLogSegments)
		nonSystemRetired = append(nonSystemRetired, result.retired...)
		mergeOrderedRootPublishMetrics(&nonSystemMetrics, result.metrics)
		phaseStats.rootApplyMetrics.add(result.metrics)
		phaseStats.rootApplyCalls++
	}

	systemBaseRoot := baseSystemRoot
	var committedRootPages []uint64
	var committedSystemPages []uint64
	for attempt := 0; ; attempt++ {
		systemTracker = newAllocTracker(idx.allocator)
		phaseStart := time.Now()
		iter, err := buildSystemDeltaIter(append([]uint64(nil), rootIDs...))
		phaseStats.systemBuildNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
		if err != nil {
			if iter != nil {
				_ = iter.Close()
			}
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
		optimisticSystemDeltaReleaseEntries = append(optimisticSystemDeltaReleaseEntries, systemDelta.OrderedEntries()...)
		phaseStart = time.Now()
		rootID, systemRetired, systemMetrics, applyErr := db.publishOrderedRootDeltaBatchWithAllocator(idx, systemBaseRoot, systemDelta, systemOpts, systemTracker, systemTracker, false)
		phaseStats.systemApplyNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
		phaseStats.systemApplyCalls++
		if applyErr != nil {
			_ = systemDelta.Close()
			err = applyErr
			return 0, nil, false, err
		}
		systemTouched := appendOrderedRootDeltaBatchFinalTouchedValueLogSegments(systemDelta, nil)
		_ = systemDelta.Close()
		phaseStats.systemApplyMetrics.add(systemMetrics)

		db.mu.RLock()
		observedSystemRoot := db.meta.SystemRootPageID
		db.mu.RUnlock()
		if observedSystemRoot != systemBaseRoot {
			if freeErr := systemTracker.FreeAll(); freeErr != nil {
				err = freeErr
				return 0, nil, false, err
			}
			systemTracker = nil
			if observedSystemRoot == 0 || attempt+1 >= orderedRootOptimisticSystemDeltaRebaseMaxAttempts {
				retrySerialized = true
				return 0, nil, retrySerialized, nil
			}
			systemBaseRoot = observedSystemRoot
			continue
		}
		phaseStart = time.Now()
		db.writeMu.RUnlock()
		writeReadLocked = false
		publishPrepareGuard, prepareErr := db.prepareFinalizeCommitDurability(false)
		db.writeMu.RLock()
		writeReadLocked = true
		publishPrepareNs := orderedRootDeltaGroupPhaseDurationNs(phaseStart)
		phaseStats.publishPrepareNs += publishPrepareNs
		phaseStats.publishPrepareCalls++
		if prepareErr != nil {
			phaseStats.publishPrepareErrors++
			db.orderedRootDeltaGroupPublishPrepareNs.Add(publishPrepareNs)
			db.orderedRootDeltaGroupPublishPrepareCalls.Add(1)
			db.orderedRootDeltaGroupPublishPrepareErrors.Add(1)
			err = prepareErr
			return 0, nil, false, err
		}
		if db.vacuumCutoverInProgress.Load() {
			publishPrepareGuard.Release()
			retrySerialized = true
			return 0, nil, retrySerialized, nil
		}
		releasePublishPrepare := func() {
			if publishPrepareGuard != nil {
				publishPrepareGuard.Release()
				publishPrepareGuard = nil
			}
		}

		lockStart := time.Now()
		db.commitMu.Lock()
		holdStart := time.Now()
		wait := holdStart.Sub(lockStart)
		db.mu.RLock()
		curUserRoot := db.meta.UserRootPageID
		curSystemRoot := db.meta.SystemRootPageID
		curSeq := db.meta.CommitSeq
		db.mu.RUnlock()
		if curSystemRoot != systemBaseRoot {
			db.commitMu.Unlock()
			releasePublishPrepare()
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

		retired := append([]uint64(nil), nonSystemRetired...)
		retired = append(retired, systemRetired...)
		merged := nonSystemMetrics
		mergeOrderedRootPublishMetrics(&merged, systemMetrics)
		newSystemRoot = rootID
		commitTouchedValueLogSegments := append([]uint32(nil), touchedValueLogSegments...)
		commitTouchedValueLogSegments = append(commitTouchedValueLogSegments, systemTouched...)

		// Batch-based grouped deltas have the same value-log reachability shape as
		// iterator-based grouped deltas: non-system roots changed, and the system
		// delta can change collection descriptors. Keep the incremental ref tracker
		// conservative by invalidating it after commit.
		var vlogRefDelta *valueLogRefDelta
		phaseStart = time.Now()
		var post finalizeCommitPost
		commitStarted = true
		commitLocked := true
		rootLocksReleased := false
		var hold time.Duration
		post, err = db.finalizeCommitLockedWithOptions(
			curUserRoot, newSystemRoot, retired, false, merged, commitTouchedValueLogSegments,
			true, vlogRefDelta, nil, nil,
			finalizeCommitOptions{
				skipPrePublishFlush:      true,
				closeTeardownPinned:      true,
				expectedBaseCommitSeq:    curSeq,
				hasExpectedBaseCommitSeq: true,
				releaseRootSerialization: func() {
					hold = time.Since(holdStart)
					db.commitMu.Unlock()
					commitLocked = false
					db.writeMu.RUnlock()
					writeReadLocked = false
					rootLocksReleased = true
				},
			},
		)
		phaseStats.finalizeNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
		phaseStats.finalizeCalls++
		committedRootPages = rootTracker.Pages()
		committedSystemPages = systemTracker.Pages()
		if commitLocked {
			hold = time.Since(holdStart)
			db.commitMu.Unlock()
		}
		releasePublishPrepare()
		if err != nil {
			return 0, nil, false, err
		}
		db.invalidateLeafGenerationSubtreeStats(append(committedRootPages, committedSystemPages...))
		db.finalizeCommitPostWork(post)
		if !rootLocksReleased {
			db.writeMu.RUnlock()
			writeReadLocked = false
		}
		db.observeOrderedRootDeltaGroupPublish(wait, hold, rootsObserved, phaseStats, nil)
		return newSystemRoot, rootIDs, false, nil
	}
}

func (db *DB) publishOrderedRootDeltaBatchGroupWithSystemDeltaBuilderSerialized(ordered []OrderedRootDeltaBatchPublishInput, preflight OrderedRootGroupPreflight, commandWALIntent *CommandWALIntent, buildSystemDeltaIter OrderedRootGroupSystemBuilder, opts orderedRootCommandWALPublishOptions) (newSystemRoot uint64, rootIDs []uint64, err error) {
	if buildSystemDeltaIter == nil {
		return 0, nil, errors.New("nil ordered root group system delta builder")
	}
	if db == nil {
		return 0, nil, ErrClosed
	}
	if db.closing.Load() {
		return 0, nil, ErrClosed
	}
	if !opts.teardownPinned {
		db.teardownMu.RLock()
		defer db.teardownMu.RUnlock()
	}

	if !opts.rawPublishLocked && db.commandWALIntentNeedsPublicAppendLock(commandWALIntent, false) {
		unlockCommandWALPublish, lockErr := db.lockCommandWALPublishWithBarriersTeardownPinned()
		if lockErr != nil {
			return 0, nil, lockErr
		}
		defer unlockCommandWALPublish()
		opts.rawPublishLocked = true
	}
	lockStart := time.Now()
	db.writeMu.Lock()
	holdStart := time.Now()
	wait := holdStart.Sub(lockStart)
	rootsObserved := 0
	phaseStats := orderedRootDeltaGroupPublishPhaseStats{}
	finished := false
	writeLocked := true
	var hold time.Duration
	releaseWrite := func() {
		if !writeLocked {
			return
		}
		hold = time.Since(holdStart)
		db.writeMu.Unlock()
		writeLocked = false
	}
	finishPublish := func() {
		if finished {
			return
		}
		finished = true
		releaseWrite()
		db.observeOrderedRootDeltaGroupPublish(wait, hold, rootsObserved, phaseStats, err)
	}
	defer finishPublish()
	if err = db.checkWriteAdmissionLocked(); err != nil {
		return 0, nil, err
	}

	if db.readOnly {
		err = ErrReadOnly
		return 0, nil, err
	}
	if commandWALIntent == nil {
		if err = db.rejectUnloggedCommandWALRootPublish(); err != nil {
			return 0, nil, err
		}
	}
	idxGen := db.idx.Load()
	if idxGen == nil {
		err = errOrderedRootPublishMissingIndex
		return 0, nil, err
	}

	db.mu.RLock()
	userRoot := db.meta.UserRootPageID
	baseSystemRoot := db.meta.SystemRootPageID
	baseSeq := db.meta.CommitSeq
	db.mu.RUnlock()
	if preflight != nil {
		phaseStart := time.Now()
		if err = preflight(); err != nil {
			phaseStats.preflightNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
			return 0, nil, err
		}
		phaseStats.preflightNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	}

	rootIDs = make([]uint64, len(ordered))
	systemRoute := OrderedRootSpanNativeRouteSystemDeltaBuilderPublish
	systemContext := "ordered-root delta group system delta apply"
	if commandWALIntent != nil {
		systemRoute = OrderedRootSpanNativeRouteCommandWALPublish
		systemContext = "command-WAL ordered-root group system delta apply"
	}
	systemOpts := systemRootOrderedPublishOptions(db).withSpanNativeRoute(systemRoute, systemContext)
	var retired []uint64
	var merged adaptive.Metrics
	var touchedValueLogSegments []uint32
	var ptrCollectors []*pendingValueLogAppendPtrCollectingIterator
	defer func() {
		for idx := range ordered {
			db.releasePendingValueLogAppendFileIDsFromBatch(ordered[idx].Delta)
		}
		for _, collector := range ptrCollectors {
			db.releasePendingValueLogAppendPtrCollector(collector)
		}
	}()
	if err = db.prepareOrderedRootDeltaBatchGroupReadOnly(idxGen, ordered, idxGen.allocator, &phaseStats); err != nil {
		return 0, nil, err
	}
	phaseStart := time.Now()
	defaultRoute := OrderedRootSpanNativeRouteMultiIndexGroupPublish
	defaultContext := "multi-index ordered-root group root apply"
	if commandWALIntent != nil {
		defaultRoute = OrderedRootSpanNativeRouteCommandWALPublish
		defaultContext = "command-WAL ordered-root group root apply"
	}
	rootApplyResults, parallelRootApply := db.applyOrderedRootDeltaBatchGroupRoots(idxGen, ordered, idxGen.allocator, idxGen.allocator, defaultRoute, defaultContext, false)
	phaseStats.rootApplyNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	if parallelRootApply {
		phaseStats.rootApplyParallelGroups++
		for idx := range ordered {
			if ordered[idx].ParallelApply && ordered[idx].Delta != nil && !ordered[idx].Delta.IsEmpty() {
				phaseStats.rootApplyParallelRoots++
			}
		}
	}
	for idx := range rootApplyResults {
		result := rootApplyResults[idx]
		if result.err != nil {
			return 0, nil, result.err
		}
		touchedValueLogSegments = appendOrderedRootDeltaBatchFinalTouchedValueLogSegments(ordered[idx].Delta, touchedValueLogSegments)
		rootIDs[idx] = result.rootID
		rootsObserved++
		retired = append(retired, result.retired...)
		mergeOrderedRootPublishMetrics(&merged, result.metrics)
		phaseStats.rootApplyMetrics.add(result.metrics)
		phaseStats.rootApplyCalls++
	}

	phaseStart = time.Now()
	iter, err := buildSystemDeltaIter(append([]uint64(nil), rootIDs...))
	phaseStats.systemBuildNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	if err != nil {
		if iter != nil {
			_ = iter.Close()
		}
		return 0, nil, err
	}
	if iter == nil {
		return 0, nil, errors.New("nil system root delta iterator")
	}
	phaseStart = time.Now()
	ptrCollector, collectedIter := newPendingValueLogAppendPtrCollectingIterator(iter)
	ptrCollectors = append(ptrCollectors, ptrCollector)
	rootID, rootRetired, metrics, systemTouched, err := db.publishOrderedRootDeltaIterator(baseSystemRoot, collectedIter, systemOpts)
	phaseStats.systemApplyNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	phaseStats.systemApplyCalls++
	if err != nil {
		return 0, nil, err
	}
	touchedValueLogSegments = append(touchedValueLogSegments, systemTouched...)
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

	// Batch-based grouped deltas have the same value-log reachability shape as
	// iterator-based grouped deltas: non-system roots changed, and the system
	// delta can change collection descriptors. Keep the incremental ref tracker
	// conservative by invalidating it after commit.
	var vlogRefDelta *valueLogRefDelta
	phaseStart = time.Now()
	err = db.finalizeOrderedRootPublishWithCommandWALOptions(userRoot, newSystemRoot, retired, false, merged, touchedValueLogSegments, true, vlogRefDelta, nil, nil, baseSeq, commandWALIntent, opts, releaseWrite)
	phaseStats.finalizeNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	phaseStats.finalizeCalls++
	if err != nil {
		return 0, nil, err
	}
	return newSystemRoot, rootIDs, nil
}

func (db *DB) publishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilderSerialized(ordered []OrderedRootDeltaBatchPublishInput, preflight OrderedRootGroupPreflight, commandWALIntent *CommandWALIntent, buildContextDeltas OrderedRootDeltaBatchGroupCommandWALDeltaBuilder, buildSystemDeltaIter OrderedRootGroupCommandWALSystemBuilder, opts orderedRootCommandWALPublishOptions) (newSystemRoot uint64, rootIDs []uint64, err error) {
	if buildSystemDeltaIter == nil {
		return 0, nil, ErrOrderedRootDeltaBatchGroupCommandWALContextNilSystemBuilder
	}
	if commandWALIntent == nil {
		return 0, nil, ErrCommandWALContextMissingFrame
	}
	if db == nil {
		return 0, nil, ErrClosed
	}
	if db.closing.Load() {
		return 0, nil, ErrClosed
	}
	if !opts.teardownPinned {
		db.teardownMu.RLock()
		defer db.teardownMu.RUnlock()
	}

	syncCommandWAL := commandWALIntentPublishSync(commandWALIntent, false)
	if !opts.rawPublishLocked && db.commandWALIntentNeedsPublicAppendLock(commandWALIntent, syncCommandWAL) {
		unlockCommandWALPublish, lockErr := db.lockCommandWALPublishWithBarriersTeardownPinned()
		if lockErr != nil {
			return 0, nil, lockErr
		}
		defer unlockCommandWALPublish()
		opts.rawPublishLocked = true
	}
	timing := commandWALIntent.publishTiming
	lockStart := time.Now()
	db.writeMu.Lock()
	holdStart := time.Now()
	wait := holdStart.Sub(lockStart)
	if timing != nil {
		timing.WriteLockWait += wait
	}
	rootsObserved := 0
	phaseStats := orderedRootDeltaGroupPublishPhaseStats{}
	finished := false
	writeLocked := true
	var hold time.Duration
	releaseWrite := func() {
		if !writeLocked {
			return
		}
		hold = time.Since(holdStart)
		db.writeMu.Unlock()
		writeLocked = false
	}
	finishPublish := func() {
		if finished {
			return
		}
		finished = true
		releaseWrite()
		db.observeOrderedRootDeltaGroupPublish(wait, hold, rootsObserved, phaseStats, err)
	}
	defer finishPublish()
	if err = db.checkWriteAdmissionLocked(); err != nil {
		return 0, nil, err
	}

	if db.readOnly {
		err = ErrReadOnly
		return 0, nil, err
	}
	if !db.CommandWALEnabled() {
		err = ErrCommandWALUnsupported
		return 0, nil, err
	}
	idxGen := db.idx.Load()
	if idxGen == nil {
		err = errOrderedRootPublishMissingIndex
		return 0, nil, err
	}

	db.mu.RLock()
	userRoot := db.meta.UserRootPageID
	baseSystemRoot := db.meta.SystemRootPageID
	baseSeq := db.meta.CommitSeq
	db.mu.RUnlock()
	if preflight != nil {
		phaseStart := time.Now()
		if err = preflight(); err != nil {
			phaseStats.preflightNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
			if timing != nil {
				timing.Preflight += time.Since(phaseStart)
			}
			return 0, nil, err
		}
		phaseStats.preflightNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
		if timing != nil {
			timing.Preflight += time.Since(phaseStart)
		}
	}

	db.commitMu.Lock()
	commitLocked := true
	commandAppended := false
	defer func() {
		if err != nil && commandAppended {
			db.poisonCommandWALAfterPublicPostAppendFailure(commandWALIntent)
		}
		if commitLocked {
			db.commitMu.Unlock()
		}
	}()
	appendStart := time.Now()
	lsn, err := db.appendPublicCommandWALIntent(commandWALIntent, syncCommandWAL)
	if timing != nil {
		timing.Append += time.Since(appendStart)
	}
	if err != nil {
		return 0, nil, err
	}
	commandAppended = true
	if lsn == 0 {
		err = errCommandWALContextZeroLSN
		return 0, nil, err
	}
	durableResourceBuilder := rootpublication.NewStableResourceSetBuilder()
	defer durableResourceBuilder.Abandon()
	durableResourceRequirements := rootpublication.StableLogicalObligationRequirements{}
	durableResourceMutation := rootpublication.StableLogicalObligationMutation{}
	ctx := CommandWALPublishContext{
		AppliedCommandLSN: lsn, durableResources: durableResourceBuilder,
		durableResourceRequirements: &durableResourceRequirements,
		durableResourceMutation:     &durableResourceMutation,
	}

	allOrdered := ordered
	var contextOrdered []OrderedRootDeltaBatchPublishInput
	defer func() {
		if err != nil {
			closeOrderedRootDeltaBatchPublishDeltas(contextOrdered)
		}
	}()
	if buildContextDeltas != nil {
		contextBuildStart := time.Now()
		var buildErr error
		contextOrdered, buildErr = buildContextDeltas(ctx)
		if timing != nil {
			timing.ContextBuild += time.Since(contextBuildStart)
		}
		if buildErr != nil {
			closeOrderedRootDeltaBatchPublishDeltas(contextOrdered)
			contextOrdered = nil
			err = buildErr
			return 0, nil, err
		}
		if len(contextOrdered) != 0 {
			allOrdered = make([]OrderedRootDeltaBatchPublishInput, 0, len(ordered)+len(contextOrdered))
			allOrdered = append(allOrdered, ordered...)
			allOrdered = append(allOrdered, contextOrdered...)
		}
	}

	rootIDs = make([]uint64, len(allOrdered))
	systemOpts := systemRootOrderedPublishOptions(db).withSpanNativeRoute(OrderedRootSpanNativeRouteCommandWALPublish, "command-WAL ordered-root context group system delta apply")
	var retired []uint64
	var merged adaptive.Metrics
	var touchedValueLogSegments []uint32
	var vlogRefDelta *valueLogRefDelta
	exactValueLogRefDelta := true
	var ptrCollectors []*pendingValueLogAppendPtrCollectingIterator
	defer func() {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
		for idx := range allOrdered {
			db.releasePendingValueLogAppendFileIDsFromBatch(allOrdered[idx].Delta)
		}
		for _, collector := range ptrCollectors {
			db.releasePendingValueLogAppendPtrCollector(collector)
		}
	}()
	if err = db.prepareOrderedRootDeltaBatchGroupReadOnly(idxGen, allOrdered, idxGen.allocator, &phaseStats); err != nil {
		return 0, nil, err
	}
	phaseStart := time.Now()
	rootApplyResults, parallelRootApply := db.applyOrderedRootDeltaBatchGroupRoots(idxGen, allOrdered, idxGen.allocator, idxGen.allocator, OrderedRootSpanNativeRouteCommandWALPublish, "command-WAL ordered-root context group root apply", true)
	phaseStats.rootApplyNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	if timing != nil {
		timing.RootApply += time.Since(phaseStart)
	}
	if parallelRootApply {
		phaseStats.rootApplyParallelGroups++
		for idx := range allOrdered {
			if allOrdered[idx].ParallelApply && allOrdered[idx].Delta != nil && !allOrdered[idx].Delta.IsEmpty() {
				phaseStats.rootApplyParallelRoots++
			}
		}
	}
	for idx := range rootApplyResults {
		result := rootApplyResults[idx]
		if result.err != nil {
			return 0, nil, fmt.Errorf(
				"treedb: command WAL ordered-root context apply root[%d] base=%d: %w",
				idx,
				allOrdered[idx].BaseRoot,
				result.err,
			)
		}
		rootOpts, optsErr := db.orderedRootPublishOptionsForPolicy(allOrdered[idx].StoragePolicy)
		if optsErr != nil {
			return 0, nil, optsErr
		}
		var refDelta *valueLogRefDelta
		switch {
		case allOrdered[idx].BaseRoot == 0:
			refDelta, err = db.buildOrderedRootDeltaBatchValueLogRefDelta(idxGen, 0, baseSeq, allOrdered[idx].Delta, rootOpts.outerLeavesInValueLog)
		case allOrdered[idx].Delta == nil || allOrdered[idx].Delta.IsEmpty():
			refDelta = db.newNoopValueLogRefDeltaIfTrackable(baseSeq)
		default:
			entries, ranges := allOrdered[idx].Delta.ApplyPlan()
			refPager := idxGen.pager
			if result.applyResult.OldPointerRefsCollected {
				refPager = nil
			}
			refDelta, err = db.buildValueLogRefDeltaWithOptions(
				refPager, allOrdered[idx].BaseRoot, baseSeq, entries, ranges,
				&result.applyResult.OldPointerRefs, result.applyResult.OldEntriesRemoved,
				result.applyResult.OldPointerRefsCollected, rootOpts.outerLeavesInValueLog,
			)
		}
		if err != nil {
			return 0, nil, fmt.Errorf(
				"treedb: command WAL ordered-root context ref delta root[%d] base=%d: %w",
				idx,
				allOrdered[idx].BaseRoot,
				err,
			)
		}
		if refDelta == nil {
			exactValueLogRefDelta = false
		} else {
			refDelta.requiresCandidateProjection = false
			refDelta.allowEmptyDependencyReuse = true
			refDelta.outerLeafDependencyReuse = rootOpts.outerLeavesInValueLog
			mergeValueLogRefDeltaInto(&vlogRefDelta, refDelta)
			releaseValueLogRefDelta(refDelta)
		}
		touchedValueLogSegments = appendOrderedRootDeltaBatchFinalTouchedValueLogSegments(allOrdered[idx].Delta, touchedValueLogSegments)
		rootIDs[idx] = result.rootID
		rootsObserved++
		retired = append(retired, result.retired...)
		mergeOrderedRootPublishMetrics(&merged, result.metrics)
		phaseStats.rootApplyMetrics.add(result.metrics)
		phaseStats.rootApplyCalls++
	}

	phaseStart = time.Now()
	iter, err := buildSystemDeltaIter(ctx, rootIDs)
	phaseStats.systemBuildNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	if timing != nil {
		timing.SystemBuild += time.Since(phaseStart)
	}
	if err != nil {
		if iter != nil {
			_ = iter.Close()
		}
		return 0, nil, err
	}
	if iter == nil {
		err = errOrderedRootCommandWALContextNilSystemDeltaIterator()
		return 0, nil, err
	}
	systemDelta, convertErr := orderedRootDeltaBatchFromIterator(iter)
	_ = iter.Close()
	if convertErr != nil {
		return 0, nil, convertErr
	}
	defer systemDelta.Close()
	var baseDescriptorEntries []collectionEntry
	if orderedRootDeltaMayChangeCollectionRootDescriptors(systemDelta) {
		baseDescriptorEntries, _ = vacuumCollectCollectionEntriesFromRoot(context.Background(), idxGen.pager, db.valueLogManager, baseSystemRoot)
	}
	iter = newOrderedRootDeltaBatchIterator(systemDelta, true)
	phaseStart = time.Now()
	ptrCollector, collectedIter := newPendingValueLogAppendPtrCollectingIterator(iter)
	ptrCollectors = append(ptrCollectors, ptrCollector)
	rootID, rootRetired, metrics, systemTouched, systemRefDelta, err := db.publishOrderedRootDeltaIteratorWithValueLogRefs(baseSystemRoot, collectedIter, systemOpts, baseSeq, true, true)
	phaseStats.systemApplyNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	if timing != nil {
		timing.SystemApply += time.Since(phaseStart)
	}
	phaseStats.systemApplyCalls++
	if err != nil {
		if systemRefDelta != nil {
			releaseValueLogRefDelta(systemRefDelta)
		}
		return 0, nil, err
	}
	if systemRefDelta == nil {
		exactValueLogRefDelta = false
	} else {
		if systemRefDelta.requiresCandidateProjection {
			baseRoots := make([]uint64, len(allOrdered))
			for idx := range allOrdered {
				baseRoots[idx] = allOrdered[idx].BaseRoot
			}
			newDescriptorEntries, collectErr := vacuumCollectCollectionEntriesFromRoot(context.Background(), idxGen.pager, db.valueLogManager, rootID)
			if collectErr == nil && orderedRootCollectionDescriptorTransitionsCoveredEntries(baseDescriptorEntries, newDescriptorEntries, userRoot, baseSystemRoot, rootID, baseRoots, rootIDs) {
				systemRefDelta.requiresCandidateProjection = false
			} else {
				exactValueLogRefDelta = false
			}
		}
		mergeValueLogRefDeltaInto(&vlogRefDelta, systemRefDelta)
		releaseValueLogRefDelta(systemRefDelta)
	}
	touchedValueLogSegments = append(touchedValueLogSegments, systemTouched...)
	newSystemRoot = rootID
	retired = append(retired, rootRetired...)
	mergeOrderedRootPublishMetrics(&merged, metrics)
	phaseStats.systemApplyMetrics.add(metrics)

	db.mu.RLock()
	curUserRoot := db.meta.UserRootPageID
	curSystemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if curUserRoot != userRoot || curSystemRoot != baseSystemRoot {
		err = errOrderedRootCommandWALContextConcurrentModification(userRoot, curUserRoot, baseSystemRoot, curSystemRoot)
		return 0, nil, err
	}

	if !exactValueLogRefDelta {
		releaseValueLogRefDelta(vlogRefDelta)
		vlogRefDelta = nil
	}
	if err := addOrderedRootOuterLeafSegmentsToValueLogRefDelta(db.leafPageLog, vlogRefDelta); err != nil {
		return 0, nil, err
	}
	phaseStart = time.Now()
	durableResources, err := durableResourceBuilder.Freeze()
	if err != nil {
		return 0, nil, err
	}
	if durableResources.Len() == 0 {
		durableResources.Release()
		durableResources = nil
	}
	finalizeOpts := commandWALFinalizeOptionsForPublicIntent(commandWALIntent)
	finalizeOpts.publishTiming = timing
	finalizeOpts.closeTeardownPinned = true
	finalizeOpts.durableResources = durableResources
	finalizeOpts.durableResourceRequirements = durableResourceRequirements
	finalizeOpts.durableResourceMutation = durableResourceMutation
	post, err := db.finalizeCommitReleasingRootSerialization(
		userRoot, newSystemRoot, retired, syncCommandWAL, merged, touchedValueLogSegments,
		true, vlogRefDelta, nil, nil, finalizeOpts,
		baseSeq,
		func() {
			db.commitMu.Unlock()
			commitLocked = false
			releaseWrite()
		},
		func(error) {
			db.poisonCommandWALAfterPublicPostAppendFailure(commandWALIntent)
			commandAppended = false
		},
	)
	phaseStats.finalizeNs += orderedRootDeltaGroupPhaseDurationNs(phaseStart)
	if timing != nil {
		timing.Finalize += time.Since(phaseStart)
	}
	phaseStats.finalizeCalls++
	if post.accepted {
		commandAppended = false
		commandWALIntent.inner.staged = false
	}
	if err != nil {
		return 0, nil, err
	}
	vlogRefDelta = nil
	postStart := time.Now()
	db.finalizeCommitPostWork(post)
	if timing != nil {
		timing.PostFinalize += time.Since(postStart)
	}
	return newSystemRoot, rootIDs, nil
}

var errOrderedRootCommandWALContextNilSystemDeltaIteratorSentinel = errors.New("treedb: command WAL ordered root publish system builder returned nil system root delta iterator")

func errOrderedRootCommandWALContextNilSystemDeltaIterator() error {
	return errOrderedRootCommandWALContextNilSystemDeltaIteratorSentinel
}

func errOrderedRootCommandWALContextConcurrentModification(wantUserRoot, gotUserRoot, wantSystemRoot, gotSystemRoot uint64) error {
	return fmt.Errorf("%w: command WAL ordered root publish: user_root want=%d got=%d system_root want=%d got=%d", ErrConcurrentModification, wantUserRoot, gotUserRoot, wantSystemRoot, gotSystemRoot)
}

type orderedRootCommandWALPublishOptions struct {
	rawPublishLocked bool
	// teardownPinned accompanies rawPublishLocked for staged public callers.
	// Internal root publishers leave both false and acquire their own leases.
	teardownPinned              bool
	durableResources            *rootpublication.StableResourceSet
	durableResourceRequirements rootpublication.StableLogicalObligationRequirements
}

func (db *DB) finalizeOrderedRootPublishWithCommandWALOptions(newRootID uint64, sysRootID uint64, retired []uint64, sync bool, metrics adaptive.Metrics, touchedValueLogSegments []uint32, forceValueLogRefresh bool, vlogRefDelta *valueLogRefDelta, leafManifest *leafGenerationManifest, leafManifestRawFileIDs []uint32, baseSeq uint64, intent *CommandWALIntent, opts orderedRootCommandWALPublishOptions, releaseRootSerialization func()) error {
	defer opts.durableResources.Release()
	if intent == nil {
		finalizeOpts := finalizeCommitOptions{
			durableResources:            opts.durableResources,
			durableResourceRequirements: opts.durableResourceRequirements,
			closeTeardownPinned:         true,
		}
		post, err := db.finalizeCommitReleasingRootSerialization(
			newRootID, sysRootID, retired, sync, metrics, touchedValueLogSegments,
			forceValueLogRefresh, vlogRefDelta, leafManifest, leafManifestRawFileIDs,
			finalizeOpts, baseSeq, releaseRootSerialization, nil,
		)
		if err != nil {
			return err
		}
		db.finalizeCommitPostWork(post)
		return nil
	}
	sync = commandWALIntentPublishSync(intent, sync)
	if !opts.rawPublishLocked && db.commandWALIntentNeedsPublicAppendLock(intent, sync) {
		unlockCommandWALPublish, err := db.lockCommandWALPublishWithBarriersTeardownPinned()
		if err != nil {
			return err
		}
		defer unlockCommandWALPublish()
	}
	db.commitMu.Lock()
	if _, err := db.appendPublicCommandWALIntent(intent, sync); err != nil {
		db.commitMu.Unlock()
		return err
	}
	commitLocked := true
	finalizeOpts := commandWALFinalizeOptionsForPublicIntent(intent)
	finalizeOpts.closeTeardownPinned = true
	finalizeOpts.durableResources = opts.durableResources
	finalizeOpts.durableResourceRequirements = opts.durableResourceRequirements
	post, err := db.finalizeCommitReleasingRootSerialization(
		newRootID, sysRootID, retired, sync, metrics, touchedValueLogSegments,
		forceValueLogRefresh, vlogRefDelta, leafManifest, leafManifestRawFileIDs,
		finalizeOpts, baseSeq,
		func() {
			db.commitMu.Unlock()
			commitLocked = false
			releaseRootSerialization()
		},
		func(error) { db.poisonCommandWALAfterPublicPostAppendFailure(intent) },
	)
	if post.accepted {
		intent.inner.staged = false
	}
	if err != nil {
		if commitLocked {
			db.commitMu.Unlock()
		}
		return err
	}
	if commitLocked {
		db.commitMu.Unlock()
	}
	db.finalizeCommitPostWork(post)
	return nil
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
	db.teardownMu.RLock()
	defer db.teardownMu.RUnlock()

	db.writeMu.Lock()
	writeLocked := true
	defer func() {
		if writeLocked {
			db.writeMu.Unlock()
		}
	}()
	if err := db.checkWriteAdmissionLocked(); err != nil {
		return 0, nil, err
	}

	if db.readOnly {
		return 0, nil, ErrReadOnly
	}
	if err := db.rejectUnloggedCommandWALRootPublish(); err != nil {
		return 0, nil, err
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
	var systemPtrCollectors []*pendingValueLogAppendPtrCollectingIterator
	var orderedPtrCollectors []*pendingValueLogAppendPtrCollectingIterator
	defer func() {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
		for _, collector := range systemPtrCollectors {
			db.releasePendingValueLogAppendPtrCollector(collector)
		}
		if buildSystemIter != nil {
			for _, collector := range orderedPtrCollectors {
				db.releasePendingValueLogAppendPtrCollector(collector)
			}
		}
	}()

	if systemIter != nil {
		ptrCollector, collectedIter := newPendingValueLogAppendPtrCollectingIterator(systemIter)
		systemPtrCollectors = append(systemPtrCollectors, ptrCollector)
		rootID, rootRetired, metrics, publishStats, refDelta, err := db.publishOrderedRootIterator(baseSystemRoot, collectedIter, systemOpts, true)
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
	var touchedValueLogSegments []uint32
	defer closeUnconsumedOrderedRootPublishIterators(ordered, orderedConsumed)
	for idx := range ordered {
		opts, err := db.orderedRootPublishOptionsForPolicy(ordered[idx].StoragePolicy)
		if err != nil {
			return 0, nil, err
		}
		orderedConsumed[idx] = true
		touchedIter := &orderedRootTouchedIterator{UnsafeIterator: ordered[idx].Iter}
		ptrCollector, collectedIter := newPendingValueLogAppendPtrCollectingIterator(touchedIter)
		orderedPtrCollectors = append(orderedPtrCollectors, ptrCollector)
		rootID, rootRetired, metrics, _, _, err := db.publishOrderedRootIterator(ordered[idx].BaseRoot, collectedIter, opts, false)
		if err != nil {
			return 0, nil, err
		}
		touchedValueLogSegments = append(touchedValueLogSegments, touchedIter.touchedValueLogSegments...)
		rootIDs[idx] = rootID
		retired = append(retired, rootRetired...)
		mergeOrderedRootPublishMetrics(&merged, metrics)
	}

	if buildSystemIter != nil {
		builtRootIDs := append([]uint64(nil), rootIDs...)
		iter, err := buildSystemIter(builtRootIDs)
		if err != nil {
			if iter != nil {
				_ = iter.Close()
			}
			return 0, nil, err
		}
		if iter == nil {
			return 0, nil, errors.New("nil system root iterator")
		}
		ptrCollector, collectedIter := newPendingValueLogAppendPtrCollectingIterator(iter)
		systemPtrCollectors = append(systemPtrCollectors, ptrCollector)
		rootID, rootRetired, metrics, publishStats, refDelta, err := db.publishOrderedRootIterator(baseSystemRoot, collectedIter, systemOpts, true)
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
	touchedValueLogSegments = positiveValueLogRefDeltaFileIDs(vlogRefDelta, touchedValueLogSegments)
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
	post, err := db.finalizeCommitReleasingRootSerialization(
		userRoot, newSystemRoot, retired, false, merged, touchedValueLogSegments,
		true, vlogRefDelta, nil, nil, finalizeCommitOptions{closeTeardownPinned: true},
		baseSeq,
		func() {
			db.writeMu.Unlock()
			writeLocked = false
		},
		nil,
	)
	if err != nil {
		return 0, nil, err
	}
	vlogRefDelta = nil
	db.finalizeCommitPostWork(post)
	if systemIter != nil {
		db.systemRootWarmPublishAttempts.Add(systemStats.warmAttempts)
		db.systemRootWarmNativeApplyAttempts.Add(systemStats.warmNativeApplyAttempts)
		db.systemRootWarmPublishRebuildFallbacks.Add(systemStats.warmRebuildFallbacks)
		db.systemRootWarmPreservedPages.Add(systemStats.warmPreservedPages)
		db.systemRootWarmRewrittenPages.Add(systemStats.warmRewrittenPages)
	}
	return newSystemRoot, rootIDs, nil
}
