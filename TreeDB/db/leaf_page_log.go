package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// LeafPageLog appends and flushes B+Tree leaf pages stored in the value log.
//
// This is used when Options.IndexOuterLeavesInValueLog is enabled.
// Implementations are expected to reuse the existing value-log record encoding
// and compression semantics (i.e. they should append normal value-log records
// and return LeafLogPtr references).
type LeafPageLog interface {
	AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error)
	Flush() error
	Sync() error
}

type LeafPageBatchLog interface {
	// AppendLeafPages appends every leaf page and returns one pointer per input
	// page in the same order. Callers use that positional relationship for
	// cache population and per-page record-length metadata.
	AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error)
}

// LeafPageStableLog is the identity-pinned counterpart to LeafPageLog. On a
// successful non-empty append, the returned set is non-nil unclaimed
// builder-owned authority for exactly the raw outer-leaf segments referenced
// by the returned pointer. Ownership transfers to the caller, which must
// transfer or release it exactly once. On error, implementations must release
// acquired authority and return a nil set.
type LeafPageStableLog interface {
	AppendLeafPageWithStableResources(leafPage []byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error)
}

// LeafPageStableBatchLog has the same success/error ownership contract as
// LeafPageStableLog, covering every unique segment in the returned batch.
type LeafPageStableBatchLog interface {
	AppendLeafPagesWithStableResources(leafPages [][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error)
}

type LeafPagePreparedLog interface {
	// AppendPreparedLeafPage appends one caller-prepared leaf-log payload. The
	// original leafPage is used for read-cache population and integrity checks;
	// preparedPayload contains the already-compacted value-log record payload.
	AppendPreparedLeafPage(leafPage []byte, preparedPayload []byte) (page.LeafLogPtr, error)
}

// LeafPagePreparedStableLog has the same success/error ownership contract as
// LeafPageStableLog for a caller-prepared payload.
type LeafPagePreparedStableLog interface {
	AppendPreparedLeafPageWithStableResources(leafPage []byte, preparedPayload []byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error)
}

type LeafPagePreparedAppendLog interface {
	PreparedLeafPageAppends() bool
}

type LeafPagePreparedBatchAppendLog interface {
	PreparedLeafPageBatchAppends() bool
}

type LeafPageConcurrentAppendLog interface {
	ConcurrentLeafPageAppends() bool
}

type LeafPagePreparedBatchLog interface {
	// AppendPreparedLeafPages appends caller-prepared leaf-log payloads while
	// preserving the positional relationship to the original leaf pages. The
	// original leafPages are used for read-cache population and integrity checks;
	// preparedPayloads contain the already-compacted value-log record payloads.
	AppendPreparedLeafPages(leafPages [][]byte, preparedPayloads [][]byte) ([]page.LeafLogPtr, error)
}

// LeafPagePreparedStableBatchLog has the same success/error ownership contract
// as LeafPageStableLog, covering every unique segment in the returned batch.
type LeafPagePreparedStableBatchLog interface {
	AppendPreparedLeafPagesWithStableResources(leafPages [][]byte, preparedPayloads [][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error)
}

type LeafPagePreparedChildRefBatchLog interface {
	// AppendPreparedLeafPageChildRefs is the ChildRef-returning counterpart to
	// AppendPreparedLeafPages. It lets hot paths avoid allocating an intermediate
	// []LeafLogPtr when the caller ultimately needs ChildRefs.
	AppendPreparedLeafPageChildRefs(leafPages [][]byte, preparedPayloads [][]byte, refs []page.ChildRef) ([]page.ChildRef, error)
}

// LeafPagePreparedChildRefStableBatchLog has the same success/error ownership
// contract as LeafPageStableLog, covering every unique segment referenced by
// the returned leaf-log child refs.
type LeafPagePreparedChildRefStableBatchLog interface {
	AppendPreparedLeafPageChildRefsWithStableResources(leafPages [][]byte, preparedPayloads [][]byte, refs []page.ChildRef) ([]page.ChildRef, *rootpublication.StableResourceSet, error)
}

func validateLeafPageStableResources(ptrs []page.LeafLogPtr, resources *rootpublication.StableResourceSet) error {
	if len(ptrs) == 0 {
		if resources != nil && resources.Len() != 0 {
			return fmt.Errorf("%w: empty leaf append returned stable resources", rootpublication.ErrResourceConflict)
		}
		return nil
	}
	if resources == nil {
		return fmt.Errorf("%w: leaf append returned nil stable resources", rootpublication.ErrUnresolvedResource)
	}
	if resources.Owner() != rootpublication.ResourceOwnerBuilder {
		return fmt.Errorf("%w: leaf append returned resources owned by %v", rootpublication.ErrResourceOwnership, resources.Owner())
	}
	required := make(map[uint64]uint64, len(ptrs))
	for i, ptr := range ptrs {
		if ptr.FileID == 0 {
			return fmt.Errorf("%w: leaf append returned zero file ID at %d", rootpublication.ErrUnresolvedResource, i)
		}
		generation := uint64(ptr.ValueLogFileID())
		recordLength := uint64(ptr.RecordLength())
		if recordLength == 0 || ptr.Offset > ^uint64(0)-recordLength {
			return fmt.Errorf("%w: leaf append returned invalid record frontier at %d", rootpublication.ErrUnresolvedResource, i)
		}
		minimumBytes := ptr.Offset + recordLength
		if minimumBytes > required[generation] {
			required[generation] = minimumBytes
		}
	}
	descriptors := resources.Descriptors()
	for _, descriptor := range descriptors {
		fields := descriptor.ReachabilityFields()
		if len(fields) != 1 {
			return fmt.Errorf("%w: leaf append returned stable reachability %v", rootpublication.ErrResourceConflict, fields)
		}
		if fields[0] == rootpublication.ReachabilityDictionaryGeneration {
			if descriptor.Kind() != rootpublication.ResourceDictionary || len(descriptor.LogicalObligations()) == 0 {
				return fmt.Errorf("%w: leaf append returned incomplete dictionary authority", rootpublication.ErrUnresolvedResource)
			}
			continue
		}
		if fields[0] == rootpublication.ReachabilityTemplateGeneration {
			if descriptor.Kind() != rootpublication.ResourceTemplate || len(descriptor.LogicalObligations()) == 0 {
				return fmt.Errorf("%w: leaf append returned incomplete template authority", rootpublication.ErrUnresolvedResource)
			}
			continue
		}
		if fields[0] != rootpublication.ReachabilityOuterLeafRawPointer || descriptor.Kind() != rootpublication.ResourceOuterLeafLog {
			return fmt.Errorf("%w: leaf append returned unsupported stable kind %q reachability %v", rootpublication.ErrResourceConflict, descriptor.Kind(), fields)
		}
		minimumBytes, ok := required[descriptor.Generation()]
		if !ok {
			return fmt.Errorf("%w: leaf append returned unreferenced stable generation %d", rootpublication.ErrResourceConflict, descriptor.Generation())
		}
		if descriptor.Frontier().Bytes < minimumBytes {
			return fmt.Errorf("%w: leaf append stable generation %d frontier %d does not cover pointer end %d", rootpublication.ErrUnresolvedResource, descriptor.Generation(), descriptor.Frontier().Bytes, minimumBytes)
		}
		delete(required, descriptor.Generation())
	}
	if len(required) != 0 {
		return fmt.Errorf("%w: leaf append omitted stable generations %v", rootpublication.ErrUnresolvedResource, required)
	}
	return nil
}

type LeafPageLogSegment struct {
	Path   string
	FileID uint32
}

type LeafPageLogCreatedSegmentProvider interface {
	CreatedLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error)
}

// LeafPageLogLaneProvider optionally exposes additional lane-specific leaf-page
// log appenders for concurrent writers.
type LeafPageLogLaneProvider interface {
	LeafPageLogLane(workerIndex int) (LeafPageLog, bool)
}

// leafPageLogLaneProvider is retained for internal callers that still use the
// unexported name.
type leafPageLogLaneProvider = LeafPageLogLaneProvider

// LeafPageLogCurrentSegmentProvider optionally reports every currently tracked
// leaf-log segment. Implementations may return multiple current segments while
// keeping the singular CurrentValueLogSegment compatibility path available.
type LeafPageLogCurrentSegmentProvider interface {
	CurrentLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error)
}

// LeafPageLogCachedWrapperOwner marks leaf-page logs installed by the public
// cached TreeDB wrapper. Exhaustive compact treats these separately from
// caller-owned logs with similar concurrency or segment-reporting capabilities
// because cached owners also need background-flush and backlog quiescence.
type LeafPageLogCachedWrapperOwner interface {
	CompactStorageCachedWrapperOwner() bool
}

type LeafPageLogSegmentRegistrationObserver interface {
	MarkLeafPageLogSegmentsRegistered([]LeafPageLogSegment)
}

// Optional interface implemented by leaf-page logs that can report their
// current value-log segment identity.
type leafPageLogCurrentSegmentProvider interface {
	CurrentValueLogSegment() (path string, fileID uint32, ok bool)
}

type leafPageLogProtectedRootProvider interface {
	ProtectedLeafGenerationRootIDs() []uint64
}

type leafPageLogProtectedSystemRootProvider interface {
	ProtectedLeafGenerationSystemRootIDs() []uint64
}

type leafPageLogProtectedRootPairProvider interface {
	ProtectedLeafGenerationRootIDPair() (rootIDs []uint64, systemRootIDs []uint64)
}

type leafPageLogProtectedRootPairSnapshotProvider interface {
	ProtectedLeafGenerationRootIDPairSnapshot() (rootIDs []uint64, systemRootIDs []uint64, version uint64)
}

type leafPageLogStableRegistryBinder interface {
	bindStableResourcePinRegistry(*rootpublication.IdentityPinRegistry) error
}

type leafPageLogStableDictionaryBinder interface {
	bindStableDictionaryResourceProvider(func() StableDictionaryResourceProvider) error
}

type leafPageLogRecordLengthProvider interface {
	LastLeafPageRecordLength() uint32
}

type leafPageLogWithRecordLengthHints struct {
	db    *DB
	inner LeafPageLog
}

func (l *leafPageLogWithRecordLengthHints) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if l == nil || l.inner == nil {
		return page.LeafLogPtr{}, errors.New("leaf page log unavailable")
	}
	if err := l.emitDependencyAppend(durabilitycut.BeforeDependencyAppend); err != nil {
		return page.LeafLogPtr{}, err
	}
	ptr, err := l.inner.AppendLeafPage(leafPage)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	if err := l.emitDependencyAppend(durabilitycut.AfterDependencyAppend, ptr); err != nil {
		return page.LeafLogPtr{}, err
	}
	if l.db != nil {
		l.db.storeLeafPageReadCache(ptr, leafPage)
		if provider, ok := l.inner.(leafPageLogRecordLengthProvider); ok {
			l.db.noteLeafGenerationRecordLengthRaw(ptr.FileID, ptr.Offset, provider.LastLeafPageRecordLength())
		}
	}
	return ptr, nil
}

func (l *leafPageLogWithRecordLengthHints) AppendLeafPageWithStableResources(leafPage []byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	if l == nil || l.inner == nil {
		return page.LeafLogPtr{}, nil, errors.New("leaf page log unavailable")
	}
	stable, ok := l.inner.(LeafPageStableLog)
	if !ok {
		return page.LeafLogPtr{}, nil, fmt.Errorf("%w: leaf page log lacks stable append", rootpublication.ErrUnresolvedResource)
	}
	if err := l.emitDependencyAppend(durabilitycut.BeforeDependencyAppend); err != nil {
		return page.LeafLogPtr{}, nil, err
	}
	ptr, resources, err := stable.AppendLeafPageWithStableResources(leafPage)
	if err != nil {
		if resources != nil {
			resources.Release()
		}
		return page.LeafLogPtr{}, nil, err
	}
	if err := validateLeafPageStableResources([]page.LeafLogPtr{ptr}, resources); err != nil {
		if resources != nil {
			resources.Release()
		}
		return page.LeafLogPtr{}, nil, err
	}
	if err := l.emitDependencyAppend(durabilitycut.AfterDependencyAppend, ptr); err != nil {
		resources.Release()
		return page.LeafLogPtr{}, nil, err
	}
	l.noteLeafPagePointer(leafPage, ptr)
	return ptr, resources, nil
}

func (l *leafPageLogWithRecordLengthHints) ConcurrentLeafPageAppends() bool {
	if l == nil || l.inner == nil {
		return false
	}
	concurrent, ok := l.inner.(LeafPageConcurrentAppendLog)
	return ok && concurrent.ConcurrentLeafPageAppends()
}

func (l *leafPageLogWithRecordLengthHints) PreparedLeafPageAppends() bool {
	if l == nil || l.inner == nil {
		return false
	}
	_, ok := l.inner.(LeafPagePreparedLog)
	return ok
}

func (l *leafPageLogWithRecordLengthHints) PreparedLeafPageBatchAppends() bool {
	if l == nil || l.inner == nil {
		return false
	}
	if _, ok := l.inner.(LeafPagePreparedBatchLog); ok {
		return true
	}
	_, ok := l.inner.(LeafPagePreparedChildRefBatchLog)
	return ok
}

func (l *leafPageLogWithRecordLengthHints) AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error) {
	if l == nil || l.inner == nil {
		return nil, errors.New("leaf page log unavailable")
	}
	if len(leafPages) == 0 {
		return nil, nil
	}
	var ptrs []page.LeafLogPtr
	if err := l.emitDependencyAppend(durabilitycut.BeforeDependencyAppend); err != nil {
		return nil, err
	}
	if batcher, ok := l.inner.(LeafPageBatchLog); ok {
		var err error
		ptrs, err = batcher.AppendLeafPages(leafPages)
		if err != nil {
			return nil, err
		}
	} else {
		ptrs = make([]page.LeafLogPtr, len(leafPages))
		for i, leafPage := range leafPages {
			ptr, err := l.inner.AppendLeafPage(leafPage)
			if err != nil {
				return nil, err
			}
			ptrs[i] = ptr
		}
	}
	if err := l.emitDependencyAppend(durabilitycut.AfterDependencyAppend, ptrs...); err != nil {
		return nil, err
	}
	if err := l.noteLeafPageBatchPointers(leafPages, ptrs); err != nil {
		return nil, err
	}
	return ptrs, nil
}

func (l *leafPageLogWithRecordLengthHints) AppendLeafPagesWithStableResources(leafPages [][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	if l == nil || l.inner == nil {
		return nil, nil, errors.New("leaf page log unavailable")
	}
	if len(leafPages) == 0 {
		return nil, nil, nil
	}
	stable, ok := l.inner.(LeafPageStableBatchLog)
	if !ok {
		return nil, nil, fmt.Errorf("%w: leaf page log lacks stable batch append", rootpublication.ErrUnresolvedResource)
	}
	if err := l.emitDependencyAppend(durabilitycut.BeforeDependencyAppend); err != nil {
		return nil, nil, err
	}
	ptrs, resources, err := stable.AppendLeafPagesWithStableResources(leafPages)
	if err != nil {
		if resources != nil {
			resources.Release()
		}
		return nil, nil, err
	}
	if err := validateLeafPageStableResources(ptrs, resources); err != nil {
		if resources != nil {
			resources.Release()
		}
		return nil, nil, err
	}
	if len(ptrs) != len(leafPages) {
		resources.Release()
		return nil, nil, fmt.Errorf("stable leaf batch returned %d ptrs for %d pages", len(ptrs), len(leafPages))
	}
	if err := l.emitDependencyAppend(durabilitycut.AfterDependencyAppend, ptrs...); err != nil {
		resources.Release()
		return nil, nil, err
	}
	if err := l.noteLeafPageBatchPointers(leafPages, ptrs); err != nil {
		resources.Release()
		return nil, nil, err
	}
	return ptrs, resources, nil
}

func (l *leafPageLogWithRecordLengthHints) AppendPreparedLeafPage(leafPage []byte, preparedPayload []byte) (page.LeafLogPtr, error) {
	if l == nil || l.inner == nil {
		return page.LeafLogPtr{}, errors.New("leaf page log unavailable")
	}
	prepared, ok := l.inner.(LeafPagePreparedLog)
	if !ok {
		return l.AppendLeafPage(leafPage)
	}
	if err := l.emitDependencyAppend(durabilitycut.BeforeDependencyAppend); err != nil {
		return page.LeafLogPtr{}, err
	}
	ptr, err := prepared.AppendPreparedLeafPage(leafPage, preparedPayload)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	if err := l.emitDependencyAppend(durabilitycut.AfterDependencyAppend, ptr); err != nil {
		return page.LeafLogPtr{}, err
	}
	l.noteLeafPagePointer(leafPage, ptr)
	return ptr, nil
}

func (l *leafPageLogWithRecordLengthHints) AppendPreparedLeafPageWithStableResources(leafPage []byte, preparedPayload []byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	if l == nil || l.inner == nil {
		return page.LeafLogPtr{}, nil, errors.New("leaf page log unavailable")
	}
	stable, ok := l.inner.(LeafPagePreparedStableLog)
	if !ok {
		return l.AppendLeafPageWithStableResources(leafPage)
	}
	if err := l.emitDependencyAppend(durabilitycut.BeforeDependencyAppend); err != nil {
		return page.LeafLogPtr{}, nil, err
	}
	ptr, resources, err := stable.AppendPreparedLeafPageWithStableResources(leafPage, preparedPayload)
	if err != nil {
		if resources != nil {
			resources.Release()
		}
		return page.LeafLogPtr{}, nil, err
	}
	if err := validateLeafPageStableResources([]page.LeafLogPtr{ptr}, resources); err != nil {
		if resources != nil {
			resources.Release()
		}
		return page.LeafLogPtr{}, nil, err
	}
	if err := l.emitDependencyAppend(durabilitycut.AfterDependencyAppend, ptr); err != nil {
		resources.Release()
		return page.LeafLogPtr{}, nil, err
	}
	l.noteLeafPagePointer(leafPage, ptr)
	return ptr, resources, nil
}

func (l *leafPageLogWithRecordLengthHints) AppendPreparedLeafPages(leafPages [][]byte, preparedPayloads [][]byte) ([]page.LeafLogPtr, error) {
	if l == nil || l.inner == nil {
		return nil, errors.New("leaf page log unavailable")
	}
	if len(leafPages) == 0 {
		return nil, nil
	}
	if len(preparedPayloads) != len(leafPages) {
		return nil, fmt.Errorf("leaf page prepared batch has %d payloads for %d leaf pages", len(preparedPayloads), len(leafPages))
	}
	prepared, ok := l.inner.(LeafPagePreparedBatchLog)
	if !ok {
		return l.AppendLeafPages(leafPages)
	}
	if err := l.emitDependencyAppend(durabilitycut.BeforeDependencyAppend); err != nil {
		return nil, err
	}
	ptrs, err := prepared.AppendPreparedLeafPages(leafPages, preparedPayloads)
	if err != nil {
		return nil, err
	}
	if err := l.emitDependencyAppend(durabilitycut.AfterDependencyAppend, ptrs...); err != nil {
		return nil, err
	}
	if err := l.noteLeafPageBatchPointers(leafPages, ptrs); err != nil {
		return nil, err
	}
	return ptrs, nil
}

func (l *leafPageLogWithRecordLengthHints) AppendPreparedLeafPagesWithStableResources(leafPages [][]byte, preparedPayloads [][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	if l == nil || l.inner == nil {
		return nil, nil, errors.New("leaf page log unavailable")
	}
	if len(leafPages) == 0 {
		return nil, nil, nil
	}
	if len(preparedPayloads) != len(leafPages) {
		return nil, nil, fmt.Errorf("leaf page prepared stable batch has %d payloads for %d leaf pages", len(preparedPayloads), len(leafPages))
	}
	stable, ok := l.inner.(LeafPagePreparedStableBatchLog)
	if !ok {
		return l.AppendLeafPagesWithStableResources(leafPages)
	}
	if err := l.emitDependencyAppend(durabilitycut.BeforeDependencyAppend); err != nil {
		return nil, nil, err
	}
	ptrs, resources, err := stable.AppendPreparedLeafPagesWithStableResources(leafPages, preparedPayloads)
	if err != nil {
		if resources != nil {
			resources.Release()
		}
		return nil, nil, err
	}
	if err := validateLeafPageStableResources(ptrs, resources); err != nil {
		if resources != nil {
			resources.Release()
		}
		return nil, nil, err
	}
	if len(ptrs) != len(leafPages) {
		resources.Release()
		return nil, nil, fmt.Errorf("stable prepared leaf batch returned %d ptrs for %d pages", len(ptrs), len(leafPages))
	}
	if err := l.emitDependencyAppend(durabilitycut.AfterDependencyAppend, ptrs...); err != nil {
		resources.Release()
		return nil, nil, err
	}
	if err := l.noteLeafPageBatchPointers(leafPages, ptrs); err != nil {
		resources.Release()
		return nil, nil, err
	}
	return ptrs, resources, nil
}

func (l *leafPageLogWithRecordLengthHints) AppendPreparedLeafPageChildRefs(leafPages [][]byte, preparedPayloads [][]byte, refs []page.ChildRef) ([]page.ChildRef, error) {
	refs = refs[:0]
	if l == nil || l.inner == nil {
		return nil, errors.New("leaf page log unavailable")
	}
	if len(leafPages) == 0 {
		return refs, nil
	}
	if len(preparedPayloads) != len(leafPages) {
		return nil, fmt.Errorf("leaf page prepared child-ref batch has %d payloads for %d leaf pages", len(preparedPayloads), len(leafPages))
	}
	if prepared, ok := l.inner.(LeafPagePreparedChildRefBatchLog); ok {
		if err := l.emitDependencyAppend(durabilitycut.BeforeDependencyAppend); err != nil {
			return nil, err
		}
		out, err := prepared.AppendPreparedLeafPageChildRefs(leafPages, preparedPayloads, refs)
		if err != nil {
			return nil, err
		}
		if len(out) != len(leafPages) {
			return nil, fmt.Errorf("leaf page prepared child-ref batch returned %d refs for %d leaf pages", len(out), len(leafPages))
		}
		for i, ref := range out {
			if !ref.IsLeafLog() {
				return nil, fmt.Errorf("leaf page prepared child-ref batch returned non-leaf-log ref at %d", i)
			}
			l.noteLeafPagePointer(leafPages[i], ref.Log)
		}
		if durabilitycut.Enabled() {
			ptrs := make([]page.LeafLogPtr, len(out))
			for i := range out {
				ptrs[i] = out[i].Log
			}
			if err := l.emitDependencyAppend(durabilitycut.AfterDependencyAppend, ptrs...); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	ptrs, err := l.AppendPreparedLeafPages(leafPages, preparedPayloads)
	if err != nil {
		return nil, err
	}
	if cap(refs) < len(ptrs) {
		refs = make([]page.ChildRef, len(ptrs))
	} else {
		refs = refs[:len(ptrs)]
	}
	for i, ptr := range ptrs {
		refs[i] = page.LeafLogChildRef(ptr)
	}
	return refs, nil
}

func (l *leafPageLogWithRecordLengthHints) AppendPreparedLeafPageChildRefsWithStableResources(leafPages [][]byte, preparedPayloads [][]byte, refs []page.ChildRef) ([]page.ChildRef, *rootpublication.StableResourceSet, error) {
	refs = refs[:0]
	if l == nil || l.inner == nil {
		return nil, nil, errors.New("leaf page log unavailable")
	}
	if len(leafPages) == 0 {
		return refs, nil, nil
	}
	if len(preparedPayloads) != len(leafPages) {
		return nil, nil, fmt.Errorf("leaf page prepared stable child-ref batch has %d payloads for %d leaf pages", len(preparedPayloads), len(leafPages))
	}
	stable, ok := l.inner.(LeafPagePreparedChildRefStableBatchLog)
	if !ok {
		ptrs, resources, err := l.AppendPreparedLeafPagesWithStableResources(leafPages, preparedPayloads)
		if err != nil {
			return nil, nil, err
		}
		if cap(refs) < len(ptrs) {
			refs = make([]page.ChildRef, len(ptrs))
		} else {
			refs = refs[:len(ptrs)]
		}
		for i, ptr := range ptrs {
			refs[i] = page.LeafLogChildRef(ptr)
		}
		return refs, resources, nil
	}
	if err := l.emitDependencyAppend(durabilitycut.BeforeDependencyAppend); err != nil {
		return nil, nil, err
	}
	out, resources, err := stable.AppendPreparedLeafPageChildRefsWithStableResources(leafPages, preparedPayloads, refs)
	if err != nil {
		if resources != nil {
			resources.Release()
		}
		return nil, nil, err
	}
	if len(out) != len(leafPages) {
		resources.Release()
		return nil, nil, fmt.Errorf("leaf page prepared stable child-ref batch returned %d refs for %d leaf pages", len(out), len(leafPages))
	}
	ptrs := make([]page.LeafLogPtr, len(out))
	for i, ref := range out {
		if !ref.IsLeafLog() {
			resources.Release()
			return nil, nil, fmt.Errorf("leaf page prepared stable child-ref batch returned non-leaf-log ref at %d", i)
		}
		ptrs[i] = ref.Log
	}
	if err := validateLeafPageStableResources(ptrs, resources); err != nil {
		if resources != nil {
			resources.Release()
		}
		return nil, nil, err
	}
	if err := l.emitDependencyAppend(durabilitycut.AfterDependencyAppend, ptrs...); err != nil {
		resources.Release()
		return nil, nil, err
	}
	for i, ptr := range ptrs {
		l.noteLeafPagePointer(leafPages[i], ptr)
	}
	return out, resources, nil
}

func (l *leafPageLogWithRecordLengthHints) emitDependencyAppend(point durabilitycut.Point, ptrs ...page.LeafLogPtr) error {
	root := ""
	if l != nil && l.db != nil {
		root = l.db.dir
	}
	if point != durabilitycut.AfterDependencyAppend || len(ptrs) == 0 || !durabilitycut.Enabled() {
		return durabilitycut.EmitBasic(point, durabilitycut.ResourceOuterLeaf, root)
	}
	paths := make([]string, 0, len(ptrs))
	seen := make(map[string]struct{}, len(ptrs))
	for _, ptr := range ptrs {
		path := leafGenerationFallbackPath(root, ptr.FileID)
		if path == "" {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		paths = append(paths, path)
	}
	return durabilitycut.Emit(durabilitycut.Event{Point: point, Resource: durabilitycut.ResourceOuterLeaf, Root: root, Paths: paths})
}

func (l *leafPageLogWithRecordLengthHints) noteLeafPageBatchPointers(leafPages [][]byte, ptrs []page.LeafLogPtr) error {
	if len(ptrs) != len(leafPages) {
		return fmt.Errorf("leaf page batch log returned %d ptrs for %d leaf pages", len(ptrs), len(leafPages))
	}
	for i, ptr := range ptrs {
		// Batch logs guarantee returned pointers are positional: ptrs[i]
		// references leafPages[i].
		l.noteLeafPagePointer(leafPages[i], ptr)
	}
	return nil
}

func (l *leafPageLogWithRecordLengthHints) noteLeafPagePointer(leafPage []byte, ptr page.LeafLogPtr) {
	if l.db == nil {
		return
	}
	l.db.storeLeafPageReadCache(ptr, leafPage)
	recordLen := ptr.RecordLength()
	if recordLen == 0 {
		if provider, ok := l.inner.(leafPageLogRecordLengthProvider); ok {
			recordLen = provider.LastLeafPageRecordLength()
		}
	}
	l.db.noteLeafGenerationRecordLengthRaw(ptr.FileID, ptr.Offset, recordLen)
}

func (l *leafPageLogWithRecordLengthHints) Flush() error {
	if l == nil || l.inner == nil {
		return nil
	}
	return l.inner.Flush()
}

func (l *leafPageLogWithRecordLengthHints) Sync() error {
	if l == nil || l.inner == nil {
		return nil
	}
	return l.inner.Sync()
}

func (l *leafPageLogWithRecordLengthHints) CurrentValueLogSegment() (path string, fileID uint32, ok bool) {
	if l == nil || l.inner == nil {
		return "", 0, false
	}
	provider, ok := l.inner.(leafPageLogCurrentSegmentProvider)
	if !ok {
		return "", 0, false
	}
	return provider.CurrentValueLogSegment()
}

func (l *leafPageLogWithRecordLengthHints) CurrentLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error) {
	if l == nil || l.inner == nil {
		return nil, nil
	}
	return leafPageLogCurrentSegments(l.inner)
}

func (l *leafPageLogWithRecordLengthHints) LeafPageLogLane(workerIndex int) (LeafPageLog, bool) {
	if l == nil || l.inner == nil {
		return nil, false
	}
	provider, ok := l.inner.(LeafPageLogLaneProvider)
	if !ok {
		if workerIndex <= 0 {
			return l, true
		}
		return nil, false
	}
	lane, ok := provider.LeafPageLogLane(workerIndex)
	if !ok || lane == nil {
		return nil, false
	}
	return wrapLeafPageLogWithRecordLengthHints(l.db, lane), true
}

func (l *leafPageLogWithRecordLengthHints) LeafPageLogLaneAny(workerIndex int) (any, bool) {
	return l.LeafPageLogLane(workerIndex)
}

func (l *leafPageLogWithRecordLengthHints) ProtectedLeafGenerationRootIDs() []uint64 {
	if l == nil || l.inner == nil {
		return nil
	}
	provider, ok := l.inner.(leafPageLogProtectedRootProvider)
	if !ok {
		return nil
	}
	return provider.ProtectedLeafGenerationRootIDs()
}

func (l *leafPageLogWithRecordLengthHints) ProtectedLeafGenerationSystemRootIDs() []uint64 {
	if l == nil || l.inner == nil {
		return nil
	}
	provider, ok := l.inner.(leafPageLogProtectedSystemRootProvider)
	if !ok {
		return nil
	}
	return provider.ProtectedLeafGenerationSystemRootIDs()
}

func (l *leafPageLogWithRecordLengthHints) ProtectedLeafGenerationRootIDPair() ([]uint64, []uint64) {
	if l == nil || l.inner == nil {
		return nil, nil
	}
	provider, ok := l.inner.(leafPageLogProtectedRootPairProvider)
	if !ok {
		return l.ProtectedLeafGenerationRootIDs(), l.ProtectedLeafGenerationSystemRootIDs()
	}
	return provider.ProtectedLeafGenerationRootIDPair()
}

func (l *leafPageLogWithRecordLengthHints) ProtectedLeafGenerationRootIDPairSnapshot() ([]uint64, []uint64, uint64) {
	if l == nil || l.inner == nil {
		return nil, nil, 0
	}
	provider, ok := l.inner.(leafPageLogProtectedRootPairSnapshotProvider)
	if !ok {
		rootIDs, systemRootIDs := l.ProtectedLeafGenerationRootIDPair()
		return rootIDs, systemRootIDs, 0
	}
	return provider.ProtectedLeafGenerationRootIDPairSnapshot()
}

func (db *DB) currentLeafPageLogSegment() (path string, fileID uint32, ok bool) {
	if db == nil || db.leafPageLog == nil {
		return "", 0, false
	}
	segments, err := leafPageLogCurrentSegments(db.leafPageLog)
	if err != nil || len(segments) == 0 {
		return "", 0, false
	}
	return segments[0].Path, segments[0].FileID, true
}

func (db *DB) protectedLeafGenerationRootIDsFromLeafPageLog() []uint64 {
	if db == nil || db.leafPageLog == nil {
		return nil
	}
	provider, ok := db.leafPageLog.(leafPageLogProtectedRootProvider)
	if !ok {
		return nil
	}
	return provider.ProtectedLeafGenerationRootIDs()
}

func (db *DB) protectedLeafGenerationSystemRootIDsFromLeafPageLog() []uint64 {
	if db == nil || db.leafPageLog == nil {
		return nil
	}
	provider, ok := db.leafPageLog.(leafPageLogProtectedSystemRootProvider)
	if !ok {
		return nil
	}
	return provider.ProtectedLeafGenerationSystemRootIDs()
}

func (db *DB) protectedLeafGenerationRootIDPairFromLeafPageLog() ([]uint64, []uint64) {
	if db == nil || db.leafPageLog == nil {
		return nil, nil
	}
	pairProvider, ok := db.leafPageLog.(leafPageLogProtectedRootPairProvider)
	if ok {
		return pairProvider.ProtectedLeafGenerationRootIDPair()
	}
	return db.protectedLeafGenerationRootIDsFromLeafPageLog(), db.protectedLeafGenerationSystemRootIDsFromLeafPageLog()
}

func leafPageLogCreatedSegments(log LeafPageLog) ([]LeafPageLogSegment, error) {
	if log == nil {
		return nil, nil
	}
	if wrapped, ok := log.(*leafPageLogWithRecordLengthHints); ok {
		return leafPageLogCreatedSegments(wrapped.inner)
	}
	if provider, ok := log.(LeafPageLogCreatedSegmentProvider); ok {
		created, err := provider.CreatedLeafPageLogSegmentsSnapshot()
		if err != nil || len(created) == 0 {
			return nil, err
		}
		return sanitizeLeafPageLogCreatedSegments(created), nil
	}
	provider, ok := log.(interface {
		createdSegmentsSnapshot() ([]rewriteCreatedSegment, error)
	})
	if !ok {
		return nil, nil
	}
	created, err := provider.createdSegmentsSnapshot()
	if err != nil || len(created) == 0 {
		return nil, err
	}
	out := make([]LeafPageLogSegment, 0, len(created))
	for _, seg := range created {
		if seg.path == "" || seg.fileID == 0 {
			continue
		}
		out = append(out, LeafPageLogSegment{Path: seg.path, FileID: seg.fileID})
	}
	return sanitizeLeafPageLogCreatedSegments(out), nil
}

func leafPageLogCurrentSegments(log LeafPageLog) ([]LeafPageLogSegment, error) {
	if log == nil {
		return nil, nil
	}
	if wrapped, ok := log.(*leafPageLogWithRecordLengthHints); ok {
		return leafPageLogCurrentSegments(wrapped.inner)
	}
	if provider, ok := log.(LeafPageLogCurrentSegmentProvider); ok {
		current, err := provider.CurrentLeafPageLogSegmentsSnapshot()
		if err != nil || len(current) == 0 {
			return nil, err
		}
		return dedupeLeafPageLogSegments(current), nil
	}
	provider, ok := log.(leafPageLogCurrentSegmentProvider)
	if !ok {
		return nil, nil
	}
	path, fileID, ok := provider.CurrentValueLogSegment()
	if !ok || path == "" || fileID == 0 {
		return nil, nil
	}
	return []LeafPageLogSegment{{Path: path, FileID: fileID}}, nil
}

func sanitizeLeafPageLogCreatedSegments(created []LeafPageLogSegment) []LeafPageLogSegment {
	return dedupeLeafPageLogSegments(created)
}

func dedupeLeafPageLogSegments(segments []LeafPageLogSegment) []LeafPageLogSegment {
	if len(segments) == 0 {
		return nil
	}
	out := make([]LeafPageLogSegment, 0, len(segments))
	seen := make(map[uint32]struct{}, len(segments))
	for _, seg := range segments {
		if seg.Path == "" || seg.FileID == 0 {
			continue
		}
		if _, ok := seen[seg.FileID]; ok {
			continue
		}
		seen[seg.FileID] = struct{}{}
		out = append(out, seg)
	}
	return out
}

func markLeafPageLogSegmentsRegistered(log LeafPageLog, segments []LeafPageLogSegment) {
	if log == nil || len(segments) == 0 {
		return
	}
	if wrapped, ok := log.(*leafPageLogWithRecordLengthHints); ok {
		markLeafPageLogSegmentsRegistered(wrapped.inner, segments)
		return
	}
	observer, ok := log.(LeafPageLogSegmentRegistrationObserver)
	if !ok {
		return
	}
	observer.MarkLeafPageLogSegmentsRegistered(segments)
}

func wrapLeafPageLogWithRecordLengthHints(db *DB, log LeafPageLog) LeafPageLog {
	if db == nil || log == nil || !db.indexOuterLeavesInValueLog {
		return log
	}
	if wrapped, ok := log.(*leafPageLogWithRecordLengthHints); ok {
		wrapped.db = db
		return wrapped
	}
	return &leafPageLogWithRecordLengthHints{db: db, inner: log}
}

// SetLeafPageLog installs the value-log appender used for value-log-backed leaf
// pages. It is typically wired by the cached layer after opening the backend.
func (db *DB) SetLeafPageLog(log LeafPageLog) {
	db.setLeafPageLog(log, true)
}

func (db *DB) setLeafPageLogRaw(log LeafPageLog) {
	db.setLeafPageLog(log, false)
}

func (db *DB) setLeafPageLog(log LeafPageLog, wrap bool) {
	if db == nil {
		return
	}
	if binder, ok := log.(leafPageLogStableRegistryBinder); ok {
		// SetLeafPageLog predates stable capture and cannot return an error. The
		// producer records a typed late-bind failure so ordinary compatibility
		// may continue while every stable sibling fails closed.
		_ = binder.bindStableResourcePinRegistry(db.valueLogIdentityPins)
	}
	if binder, ok := log.(leafPageLogStableDictionaryBinder); ok {
		_ = binder.bindStableDictionaryResourceProvider(db.stableDictionaryResourceProvider)
	}
	installed := log
	if wrap {
		installed = wrapLeafPageLogWithLaneSelection(wrapLeafPageLogWithRecordLengthHints(db, log))
	}
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.waitForVacuumCutoverWriteLocked()
	if db.closing.Load() && installed != nil {
		return
	}
	db.leafPageLog = installed
	db.leafPageLogVersion++
	if idx := db.idx.Load(); idx != nil && idx.zipper != nil {
		idx.zipper.SetLeafPageLog(installed)
	}
}

func (db *DB) leafValueLogLanes() []LeafPageLog {
	if db == nil || db.leafPageLog == nil {
		return nil
	}
	if provider, ok := db.leafPageLog.(interface{ leafValueLogLanes() []LeafPageLog }); ok {
		return provider.leafValueLogLanes()
	}
	return []LeafPageLog{db.leafPageLog}
}

func (db *DB) leafPageLogLaneForWorkerIndex(workerIndex int) (LeafPageLog, bool) {
	if db == nil || db.leafPageLog == nil {
		return nil, false
	}
	if provider, ok := db.leafPageLog.(LeafPageLogLaneProvider); ok {
		return provider.LeafPageLogLane(workerIndex)
	}
	if workerIndex <= 0 {
		return db.leafPageLog, true
	}
	return nil, false
}

// RegisterValueLogSegment registers a newly created value-log segment with the
// backend read manager without scanning the filesystem. Cached mode uses this
// when it rotates the shared value log so outer-leaf commits can publish a
// current ValueLogSet via CurrentSetNoRefresh.
func (db *DB) RegisterValueLogSegment(path string, fileID uint32) error {
	return db.RegisterValueLogSegmentReplacing(path, fileID, 0)
}

// SetMultiCurrentWritableValueLogLane allows cached mode to keep independent
// physical append writers current under one logical lane ID.
func (db *DB) SetMultiCurrentWritableValueLogLane(lane uint32, enabled bool) {
	if db == nil || db.valueLogManager == nil {
		return
	}
	db.valueLogManager.SetMultiCurrentWritableLane(lane, enabled)
}

// RegisterValueLogSegmentReplacing registers a newly created value-log segment
// and marks it as current writable, sealing previousFileID when it is the prior
// segment for the same physical writer. Cached leaf-log lanes use this because
// multiple physical writers share the reserved encoded leaf-log lane id.
func (db *DB) RegisterValueLogSegmentReplacing(path string, fileID, previousFileID uint32) error {
	if db == nil {
		return nil
	}
	if path == "" || fileID == 0 {
		return fmt.Errorf("invalid value-log segment registration: path=%q file_id=%d", path, fileID)
	}
	if db.valueLogManager == nil {
		return fmt.Errorf("value-log segment registration unavailable: manager not initialized")
	}
	if err := db.valueLogManager.RegisterSegment(path, fileID); err != nil {
		return err
	}
	if err := db.valueLogManager.PromoteCurrentWritableReplacing(fileID, previousFileID); err != nil {
		return err
	}
	if db.isLeafGenerationSegmentPath(path) {
		db.queueLeafGenerationWritableFileID(fileID)
	}
	return nil
}

// ensureLeafPageLogSegmentRegistered tries to keep the leaf-page log's current
// writable segment visible in the value-log manager without a full directory
// scan. Returns (true, nil) when registration is confirmed on the no-refresh
// path; callers should fall back to manager.Refresh() when it returns false.
func (db *DB) ensureLeafPageLogSegmentRegistered(commitSeq uint64) (bool, error) {
	path, fileID, ok := db.currentLeafPageLogSegment()
	if !ok || path == "" || fileID == 0 {
		return false, nil
	}
	return db.ensureLeafPageLogSegmentRegisteredAt(path, fileID, commitSeq)
}

func (db *DB) registerLeafPageLogSegmentsForPublish() (bool, error) {
	if db == nil || db.valueLogManager == nil || db.leafPageLog == nil {
		return true, nil
	}
	createdSegments, err := leafPageLogCreatedSegments(db.leafPageLog)
	if err != nil {
		return false, err
	}
	currentSegments, err := leafPageLogCurrentSegments(db.leafPageLog)
	if err != nil {
		return false, err
	}
	currentByID := make(map[uint32]struct{}, len(currentSegments))
	for _, seg := range currentSegments {
		if seg.FileID != 0 {
			currentByID[seg.FileID] = struct{}{}
		}
	}
	segments := dedupeLeafPageLogSegments(append(createdSegments, currentSegments...))
	registeredSegments := make([]LeafPageLogSegment, 0, len(segments))
	for _, seg := range segments {
		if err := db.valueLogManager.RegisterSegment(seg.Path, seg.FileID); err != nil {
			return false, err
		}
		if _, current := currentByID[seg.FileID]; current {
			if err := db.valueLogManager.PromoteCurrentWritable(seg.FileID); err != nil {
				return false, err
			}
		}
		registeredSegments = append(registeredSegments, seg)
		if db.isLeafGenerationSegmentPath(seg.Path) {
			db.queueLeafGenerationWritableFileID(seg.FileID)
		}
	}
	for _, id := range db.valueLogManager.CurrentWritableFileIDs() {
		lane, _ := valuelog.DecodeFileID(id)
		if lane != valuelog.ReservedLeafLogLaneID {
			continue
		}
		if _, current := currentByID[id]; !current {
			db.valueLogManager.DemoteCurrentWritable(id)
		}
	}
	if len(createdSegments) > 0 {
		markLeafPageLogSegmentsRegistered(db.leafPageLog, createdSegments)
	}
	return len(registeredSegments) > 0, nil
}

func (db *DB) ensureLeafPageLogSegmentRegisteredAt(path string, fileID uint32, commitSeq uint64) (bool, error) {
	if db == nil || db.valueLogManager == nil || path == "" || fileID == 0 {
		return false, nil
	}
	if db.valueLogManager.HasSegment(fileID) {
		if err := db.valueLogManager.PromoteCurrentWritable(fileID); err != nil {
			return false, err
		}
		if commitSeq > 0 {
			db.queueLeafGenerationWritableFileIDAtCommit(fileID, commitSeq)
		}
		return true, nil
	}
	if err := db.valueLogManager.RegisterSegment(path, fileID); err != nil {
		// Segment may have rotated/deleted between report and registration;
		// caller can fall back to a full refresh in this case.
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if err := db.valueLogManager.PromoteCurrentWritable(fileID); err != nil {
		return false, err
	}
	if commitSeq > 0 {
		db.queueLeafGenerationWritableFileIDAtCommit(fileID, commitSeq)
	}
	return true, nil
}

func (db *DB) ensureValueLogSegmentRegisteredAt(path string, fileID uint32) (bool, error) {
	if db == nil || db.valueLogManager == nil || path == "" || fileID == 0 {
		return false, nil
	}
	if db.valueLogManager.HasSegment(fileID) {
		if err := db.valueLogManager.PromoteCurrentWritable(fileID); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := db.RegisterValueLogSegment(path, fileID); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (db *DB) isLeafGenerationSegmentPath(path string) bool {
	if db == nil || path == "" || db.leafGenerationManifest == nil {
		return false
	}
	leafDir := LeafLogDirPath(db.dir)
	if leafDir == "" {
		return false
	}
	return filepath.Clean(filepath.Dir(path)) == filepath.Clean(leafDir)
}

// SetCurrentValueLogReadBarrier installs a callback that will be invoked before
// backend-internal reads of segments still marked currentWritable.
func (db *DB) SetCurrentValueLogReadBarrier(fn func(fileID uint32) error) {
	if db == nil || db.valueLogManager == nil {
		return
	}
	db.valueLogManager.SetCurrentWritableReadBarrier(fn)
}

// SetCurrentValueLogReadBarrierWithSize is like SetCurrentValueLogReadBarrier,
// but the callback can return the flushed file size. Current-writable mmap
// reads use that size hint to avoid a per-read file Stat on freshly flushed
// segments.
func (db *DB) SetCurrentValueLogReadBarrierWithSize(fn func(fileID uint32) (int64, error)) {
	if db == nil || db.valueLogManager == nil {
		return
	}
	db.valueLogManager.SetCurrentWritableReadBarrierWithSize(fn)
}
