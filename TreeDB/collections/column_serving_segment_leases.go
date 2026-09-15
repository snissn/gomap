package collections

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

var (
	errColumnServingSegmentUnauthorized    = errors.New("collections: serving segment range is not authorized")
	errColumnServingSegmentClosed          = errors.New("collections: serving segment lease set is closed")
	errColumnServingSegmentCleanupRetained = errors.New("collections: serving segment cleanup is retained")
)

// typedGraphPhysicalResourceAccounting is protected by the containing ledger.
// Potential is charged for a holder's complete immutable closure before any OS
// acquisition. Live and in-flight fields describe resources that may exist now.
type typedGraphPhysicalResourceAccounting struct {
	inventoryHolders, inventoryRefs, inventorySegments int
	inventoryBytes                                     int64
	potentialDescriptors, potentialSegments            int
	potentialMappedBytes, potentialFallbackBytes       int64
	descriptorsInFlight, descriptorsLive               int
	unconfirmedDescriptors                             int
	mappedBackings                                     int
	mappedBytesInFlight, mappedBytes                   int64
	fallbackSegments, fallbackBackings                 int
	fallbackBytesInFlight, fallbackBytes               int64
	cleanupBackings, cleanupQuarantines                int

	totalBuilds, totalBuildFailures             uint64
	totalOpens, totalCloseAttempts, totalCloses uint64
	totalMaps, totalUnmaps                      uint64
	totalFallbacks                              uint64
	totalCleanupFailures                        uint64
	totalQuarantines, totalCleanupRetries       uint64
	lastBuildError                              string
	lastPrimaryError                            string
	lastCleanupError                            string
}

// typedGraphPhysicalResourceLedger belongs to the per-DB/per-collection schema
// coordinator. All holder generations and Collection handles therefore reserve
// against one atomic ledger. Limits cannot change while a reservation exists.
type typedGraphPhysicalResourceLedger struct {
	sync.Mutex
	// cleanupMu serializes retry/quarantine ownership transitions and the
	// retry-before-admission gate. It is never held with set.mu and the ledger
	// mutex is released around OS cleanup and lifecycle-pin close calls.
	cleanupMu      sync.Mutex
	limits         typedGraphPhysicalResourceLimits
	stats          typedGraphPhysicalResourceAccounting
	activeHead     *columnServingSegmentLeaseSet
	quarantineHead *columnServingSegmentLeaseSet
	closedDB       bool
}

func (p typedGraphPhysicalResourceAccounting) empty() bool {
	return p.inventoryHolders == 0 && p.inventoryRefs == 0 && p.inventorySegments == 0 && p.inventoryBytes == 0 &&
		p.potentialDescriptors == 0 && p.potentialSegments == 0 && p.potentialMappedBytes == 0 && p.potentialFallbackBytes == 0 &&
		p.descriptorsInFlight == 0 && p.descriptorsLive == 0 && p.unconfirmedDescriptors == 0 && p.mappedBackings == 0 && p.mappedBytesInFlight == 0 && p.mappedBytes == 0 &&
		p.fallbackSegments == 0 && p.fallbackBackings == 0 && p.fallbackBytesInFlight == 0 && p.fallbackBytes == 0 && p.cleanupBackings == 0 && p.cleanupQuarantines == 0
}

func (ledger *typedGraphPhysicalResourceLedger) snapshot() typedGraphPhysicalResourceAccounting {
	stats, _ := ledger.snapshotWithLimits()
	return stats
}

func (ledger *typedGraphPhysicalResourceLedger) snapshotWithLimits() (typedGraphPhysicalResourceAccounting, typedGraphPhysicalResourceLimits) {
	if ledger == nil {
		return typedGraphPhysicalResourceAccounting{}, typedGraphPhysicalResourceLimits{}
	}
	ledger.Lock()
	stats := ledger.stats
	limits := ledger.limits
	ledger.Unlock()
	return stats, limits
}

func typedGraphReadOwnerLimitsValid(limits typedGraphReadOwnerLimits) bool {
	p := limits.Physical
	return limits.Owners > 0 && limits.States > 0 && limits.StateBytes > 0 && limits.AssetBytes > 0 &&
		limits.Cold.ManifestRecords > 0 && limits.Cold.ManifestBytes > 0 && limits.Cold.AssetBytes > 0 && limits.Cold.DecodedTermBytes > 0 &&
		p.Segments > 0 && p.Descriptors > 0 && p.MappedBytes > 0 && p.FallbackBytes > 0 && p.InventoryBytes > 0
}

type columnServingSegmentLeaseState uint8

const (
	columnServingSegmentUnopened columnServingSegmentLeaseState = iota
	columnServingSegmentBuilding
	columnServingSegmentMapped
	columnServingSegmentFallback
	columnServingSegmentCleanup
	columnServingSegmentClosed
)

type columnServingSegmentLeaseAttempt struct {
	ready chan struct{}
	err   error
}

type columnServingRefFallbackState uint8

const (
	columnServingRefFallbackUnopened columnServingRefFallbackState = iota
	columnServingRefFallbackBuilding
	columnServingRefFallbackReady
	columnServingRefFallbackClosed
)

type columnServingRefFallbackAttempt struct {
	ready chan struct{}
	err   error
}

// columnServingRefFallback is the holder-resource heap fallback. There is one
// entry for every exact authorized parent ref, and at most one immutable full-
// parent buffer may be retained. Section handles only borrow checked slices of
// that buffer. Materializer reads use the separate synchronous borrow seam and
// never populate these entries.
type columnServingRefFallback struct {
	state   columnServingRefFallbackState
	attempt *columnServingRefFallbackAttempt
	bytes   []byte
}

// All fields below are protected by the containing set.mu. A set has one state
// mutex rather than nested pool/segment locks: OS I/O and ledger calls happen
// outside it, and Close cannot interleave between a builder's closing check and
// state publication.
type columnServingSegmentLease struct {
	fileID          uint32
	authorizedEnd   int64
	mappedCharge    int64
	state           columnServingSegmentLeaseState
	attempt         *columnServingSegmentLeaseAttempt
	mapped          []byte
	file            *os.File
	identity        columnAssetVerifiedChecksumFileIdentity
	mapErr          error
	buildErr        error
	cleanupErr      error
	fileRetryable   bool
	fileUnconfirmed bool

	// Accounting flags survive partial cleanup and make every decrement exact.
	fallbackAccounted bool
	cleanupAccounted  bool
}

type columnServingSegmentLeaseSet struct {
	mu        sync.Mutex
	cleanupMu sync.Mutex

	closing             bool
	closed              bool
	closeDone           chan struct{}
	closeErr            error
	reservationReleased bool
	users               sync.WaitGroup
	builders            sync.WaitGroup

	rootDir    string
	collection string
	namespace  string
	logicalKey string
	refsDigest [32]byte
	refs       []ColumnAssetRef
	fallbacks  []columnServingRefFallback
	segments   []columnServingSegmentLease
	guardian   *ColumnAssetLifecyclePinSet

	ledger                *typedGraphPhysicalResourceLedger
	limits                typedGraphPhysicalResourceLimits
	reservation           columnServingSegmentLeaseReservation
	fallbackReservedBytes atomic.Int64

	// These fields are protected by ledger. The active list owns every admitted
	// set; the quarantine list additionally makes retained-cleanup state
	// discoverable and retryable. The set itself owns its exact-base guardian.
	activeNext           *columnServingSegmentLeaseSet
	activeRegistered     bool
	quarantinePrimaryErr error
	quarantineCleanupErr error
	quarantineNext       *columnServingSegmentLeaseSet
	quarantined          bool
}

// columnServingSegmentOpenOutcome always returns ownership of every possibly
// live OS resource, even when buildErr or cleanupErr is non-nil. A caller may
// roll back an attempt only when both mapped and file are nil.
type columnServingSegmentOpenOutcome struct {
	mapped          []byte
	file            *os.File
	identity        columnAssetVerifiedChecksumFileIdentity
	mapErr          error
	usable          columnServingSegmentLeaseState
	buildErr        error
	cleanupErr      error
	fileRetryable   bool
	fileUnconfirmed bool
}

// columnServingSegmentBuildAccounting owns the two actual reservations made
// before open. Each flag is consumed exactly once: a descriptor reservation is
// either refunded or converted to a live descriptor, and a mapped-byte
// reservation is either refunded or converted to a live mapping. Recording the
// mapping at mmap success keeps a later close+unmap composite failure visible.
type columnServingSegmentBuildAccounting struct {
	set                *columnServingSegmentLeaseSet
	mappedCharge       int64
	descriptorInFlight bool
	mappedInFlight     bool
	mappedLive         bool
}

func (o columnServingSegmentOpenOutcome) hasResources() bool {
	return len(o.mapped) != 0 || o.file != nil
}

func (o columnServingSegmentOpenOutcome) err() error {
	return errors.Join(o.buildErr, o.cleanupErr)
}

type columnServingSegmentCloseOutcome struct {
	confirmed bool
	retryable bool
	err       error
}

// columnServingRangeHandle leaves logical lifetime and validation semantics
// with mappedresource. It does not add a pool user ref: all holder-owned
// handles close before the holder pool, and materializer borrows are
// synchronous. A heap-backed handle borrows its pool-owned exact-parent buffer;
// releasing the logical handle therefore never refunds physical fallback
// accounting.
type columnServingRangeHandle struct {
	handle *mappedresource.Handle
	once   sync.Once
}

// columnServingBorrowedRange exists only for the duration of a
// withBorrowedRange callback. A materializer may use the mapped view directly
// during that callback or ReadAt into request-owned scratch. It must not retain
// file, bytes, or identity beyond the callback.
type columnServingBorrowedRange struct {
	bytes          []byte
	file           *os.File
	identity       columnAssetVerifiedChecksumFileIdentity
	absoluteOffset int64
	length         int64
}

func (r columnServingBorrowedRange) Source() mappedresource.Source {
	if r.bytes != nil {
		return mappedresource.SourceMapped
	}
	return mappedresource.SourceHeapCopy
}

func (r columnServingBorrowedRange) Bytes() []byte {
	return r.bytes
}

func (r columnServingBorrowedRange) Identity() columnAssetVerifiedChecksumFileIdentity {
	return r.identity
}

func (r columnServingBorrowedRange) ReadAt(dst []byte) (int, error) {
	if int64(len(dst)) != r.length {
		return 0, fmt.Errorf("collections: serving borrowed range destination bytes=%d want=%d", len(dst), r.length)
	}
	if r.bytes != nil {
		return copy(dst, r.bytes), nil
	}
	if r.file == nil {
		return 0, errors.New("collections: serving borrowed range has no backing")
	}
	return readAtColumnServingSegment(r.file, dst, r.absoluteOffset)
}

func (h *columnServingRangeHandle) Bytes() []byte {
	if h == nil || h.handle == nil {
		return nil
	}
	return h.handle.Bytes()
}

func (h *columnServingRangeHandle) Source() mappedresource.Source {
	if h == nil || h.handle == nil {
		return ""
	}
	return h.handle.Source()
}

func (h *columnServingRangeHandle) Release() error {
	if h == nil {
		return nil
	}
	var err error
	h.once.Do(func() {
		if h.handle != nil {
			err = h.handle.Release()
			h.handle = nil
		}
	})
	return err
}

type columnServingSegmentLeaseReservation struct {
	refs, segments, descriptors int
	inventoryBytes              int64
	mappedBytes, fallbackBytes  int64
}

func normalizeColumnServingRoot(rootDir string) (string, error) {
	if rootDir == "" {
		return "", errors.New("collections: serving segment root is required")
	}
	rootDir, err := filepath.Abs(filepath.Clean(rootDir))
	if err != nil {
		return "", err
	}
	return rootDir, nil
}

func newColumnServingSegmentLeaseSet(key columnVectorGraphSharedPreparedSearchKey, refs []ColumnAssetRef, guardian *ColumnAssetLifecyclePinSet, ledger *typedGraphPhysicalResourceLedger, limits typedGraphPhysicalResourceLimits) (*columnServingSegmentLeaseSet, error) {
	if ledger == nil || limits.Segments <= 0 || limits.Descriptors <= 0 || limits.MappedBytes <= 0 || limits.FallbackBytes <= 0 || limits.InventoryBytes <= 0 {
		return nil, errTypedGraphOwnerBudget
	}
	if key.family != columnVectorGraphSharedPreparedSearchKeyServing || !key.valid() || guardian == nil || guardian.Source() != ColumnAssetLifecyclePinSourcePreparedQuery || len(guardian.Refs()) == 0 {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	rootDir, err := normalizeColumnServingRoot(key.assetRoot)
	if err != nil {
		return nil, err
	}
	cleanNamespace, err := cleanColumnAssetNamespace(key.namespace)
	if err != nil || cleanNamespace != key.namespace {
		if err == nil {
			err = fmt.Errorf("collections: serving segment namespace %q is not canonical", key.namespace)
		}
		return nil, err
	}
	if len(refs) == 0 {
		return nil, errors.New("collections: serving segment authority is empty")
	}

	maxEnds := make(map[uint32]int64)
	physicalIdentity := make(map[struct {
		fileID         uint32
		offset, length int64
	}]ColumnAssetRef)
	var fallbackPotential int64
	for i, ref := range refs {
		if err := validateColumnAssetRefForPlan(ref); err != nil {
			return nil, fmt.Errorf("collections: serving segment authority ref[%d]: %w", i, err)
		}
		if ref.Namespace != key.namespace {
			return nil, fmt.Errorf("collections: serving segment authority ref[%d] namespace=%q want %q", i, ref.Namespace, key.namespace)
		}
		if i > 0 && compareColumnAssetRefs(refs[i-1], ref) >= 0 {
			return nil, fmt.Errorf("collections: serving segment authority must be strictly sorted and duplicate-free at ref[%d]", i)
		}
		if ref.Offset > math.MaxInt64-ref.Length || ref.Length > int64(maxCollectionInt) {
			return nil, fmt.Errorf("collections: serving segment authority ref[%d] range overflows", i)
		}
		end := ref.Offset + ref.Length
		if end <= 0 || end > int64(maxCollectionInt) {
			return nil, fmt.Errorf("collections: serving segment authority ref[%d] end=%d exceeds host mapping bound", i, end)
		}
		identity := struct {
			fileID         uint32
			offset, length int64
		}{ref.FileID, ref.Offset, ref.Length}
		if prior, ok := physicalIdentity[identity]; ok && compareColumnAssetRefs(prior, ref) != 0 {
			return nil, fmt.Errorf("collections: serving segment authority has conflicting identity for file=%d offset=%d length=%d", ref.FileID, ref.Offset, ref.Length)
		}
		physicalIdentity[identity] = ref
		if end > maxEnds[ref.FileID] {
			maxEnds[ref.FileID] = end
		}
		if fallbackPotential > math.MaxInt64-ref.Length {
			return nil, errors.New("collections: serving segment fallback potential overflows")
		}
		fallbackPotential += ref.Length
	}
	digest, err := digestTypedGraphServingBaseRefs(refs)
	if err != nil || digest != key.refsDigest || key.refsCount != len(refs) {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	if guardianRefs := guardian.Refs(); len(guardianRefs) != len(refs) {
		return nil, ErrVectorIndexSnapshotMismatch
	} else {
		for i := range refs {
			if compareColumnAssetRefs(guardianRefs[i], refs[i]) != 0 {
				return nil, ErrVectorIndexSnapshotMismatch
			}
		}
	}

	fileIDs := make([]uint32, 0, len(maxEnds))
	for fileID := range maxEnds {
		fileIDs = append(fileIDs, fileID)
	}
	sort.Slice(fileIDs, func(i, j int) bool { return fileIDs[i] < fileIDs[j] })
	segments := make([]columnServingSegmentLease, len(fileIDs))
	var mappedPotential int64
	for i, fileID := range fileIDs {
		end := maxEnds[fileID]
		charge, err := pageRoundedColumnServingBytes(end)
		if err != nil || mappedPotential > math.MaxInt64-charge {
			if err == nil {
				err = errors.New("collections: serving segment mapped potential overflows")
			}
			return nil, err
		}
		mappedPotential += charge
		segments[i] = columnServingSegmentLease{fileID: fileID, authorizedEnd: end, mappedCharge: charge}
	}

	inventoryBytes, err := columnServingSegmentInventoryBytes(key, refs, len(segments))
	if err != nil {
		return nil, err
	}
	reservation := columnServingSegmentLeaseReservation{
		refs: len(refs), segments: len(segments), descriptors: len(segments),
		inventoryBytes: inventoryBytes, mappedBytes: mappedPotential, fallbackBytes: fallbackPotential,
	}
	set := &columnServingSegmentLeaseSet{
		rootDir: rootDir, collection: key.collection, namespace: key.namespace,
		logicalKey: key.logical, refsDigest: key.refsDigest,
		refs: append([]ColumnAssetRef(nil), refs...), fallbacks: make([]columnServingRefFallback, len(refs)), segments: segments, guardian: guardian,
		ledger: ledger, limits: limits, reservation: reservation, closeDone: make(chan struct{}),
	}
	// Closed-DB quarantines are independent cleanup owners and cannot lend
	// capacity to this coordinator. Retry them best-effort before local
	// admission so confirmable stale ownership is not stranded indefinitely;
	// an unconfirmable old DB record remains observable but cannot block a new
	// DB identity's explicitly bounded ledger.
	_ = retryColumnGraphClosedDBPhysicalQuarantines()
	if err := ledger.retryAndReserve(limits, set); err != nil {
		return nil, err
	}
	return set, nil
}

func pageRoundedColumnServingBytes(n int64) (int64, error) {
	pageSize := int64(os.Getpagesize())
	if n <= 0 || pageSize <= 0 || n > math.MaxInt64-(pageSize-1) {
		return 0, errors.New("collections: serving segment page-rounded length overflows")
	}
	return ((n + pageSize - 1) / pageSize) * pageSize, nil
}

const columnServingSegmentChannelInventoryBound int64 = 128

// columnServingSegmentInventoryBytes computes a deterministic retained-metadata
// admission charge for one actual serving holder: lease set, segment and exact-
// ref fallback state tables, authority and guardian/pin registry copies, typed
// cache key/entry, retained strings, and possible in-flight attempt channels.
// The byte-shaped charge makes configured capacity legible; it is not a Go heap
// or RSS measurement and deliberately excludes allocator-class and shared map-
// bucket slack.
func columnServingSegmentInventoryBytes(key columnVectorGraphSharedPreparedSearchKey, refs []ColumnAssetRef, segments int) (int64, error) {
	total := int64(reflect.TypeFor[columnServingSegmentLeaseSet]().Size()) +
		int64(reflect.TypeFor[ColumnAssetLifecyclePinSet]().Size()) +
		int64(reflect.TypeFor[columnAssetLifecyclePinSetRecord]().Size()) +
		int64(reflect.TypeFor[columnVectorGraphSharedPreparedSearchCacheEntry]().Size()) +
		int64(reflect.TypeFor[columnVectorGraphSharedPreparedSearchKey]().Size()) +
		int64(2+segments+len(refs))*columnServingSegmentChannelInventoryBound +
		int64(len(key.assetRoot)+len(key.collection)+len(key.namespace)+len(key.logical))
	add := func(count int, size uintptr) bool {
		if count < 0 || size == 0 || int64(count) > (math.MaxInt64-total)/int64(size) {
			return false
		}
		total += int64(count) * int64(size)
		return true
	}
	// The set owns one ref slice. The guardian and lifecycle registry share a
	// second immutable slice acquired with acquire...Owned.
	if !add(2*len(refs), reflect.TypeFor[ColumnAssetRef]().Size()) ||
		!add(len(refs), reflect.TypeFor[columnServingRefFallback]().Size()) ||
		!add(segments, reflect.TypeFor[columnServingSegmentLease]().Size()) {
		return 0, errors.New("collections: serving segment inventory metadata overflows")
	}
	for _, ref := range refs {
		if int64(2*(len(ref.Namespace)+len(ref.Kind))) > math.MaxInt64-total {
			return 0, errors.New("collections: serving segment inventory string metadata overflows")
		}
		total += int64(2 * (len(ref.Namespace) + len(ref.Kind)))
	}
	return total, nil
}

func (ledger *typedGraphPhysicalResourceLedger) retryAndReserve(limits typedGraphPhysicalResourceLimits, set *columnServingSegmentLeaseSet) error {
	if ledger == nil {
		return errTypedGraphOwnerBudget
	}
	if set == nil {
		return ErrVectorIndexSnapshotMismatch
	}
	ledger.cleanupMu.Lock()
	defer ledger.cleanupMu.Unlock()
	if err := ledger.retryQuarantinedPhysicalCleanupLocked(); err != nil {
		return err
	}
	return ledger.reserve(limits, set)
}

func (ledger *typedGraphPhysicalResourceLedger) reserve(limits typedGraphPhysicalResourceLimits, set *columnServingSegmentLeaseSet) error {
	ledger.Lock()
	defer ledger.Unlock()
	p := &ledger.stats
	if ledger.closedDB || p.cleanupQuarantines != 0 || p.cleanupBackings != 0 {
		return ErrVectorIndexSnapshotMismatch
	}
	r := set.reservation
	if !p.empty() && ledger.limits != limits {
		return ErrVectorIndexSnapshotMismatch
	}
	if r.refs <= 0 || r.segments <= 0 || r.descriptors != r.segments || r.inventoryBytes <= 0 || r.mappedBytes <= 0 || r.fallbackBytes <= 0 ||
		r.segments > limits.Segments-p.potentialSegments || r.descriptors > limits.Descriptors-p.potentialDescriptors ||
		r.inventoryBytes > limits.InventoryBytes-p.inventoryBytes || r.mappedBytes > limits.MappedBytes-p.potentialMappedBytes || r.fallbackBytes > limits.FallbackBytes-p.potentialFallbackBytes {
		return errTypedGraphOwnerBudget
	}
	ledger.limits = limits
	p.inventoryHolders++
	p.inventoryRefs += r.refs
	p.inventorySegments += r.segments
	p.inventoryBytes += r.inventoryBytes
	p.potentialSegments += r.segments
	p.potentialDescriptors += r.descriptors
	p.potentialMappedBytes += r.mappedBytes
	p.potentialFallbackBytes += r.fallbackBytes
	set.activeNext = ledger.activeHead
	set.activeRegistered = true
	ledger.activeHead = set
	return nil
}

func releaseColumnServingSegmentLeaseSet(set *columnServingSegmentLeaseSet) {
	if set == nil || set.ledger == nil {
		return
	}
	ledger := set.ledger
	r := set.reservation
	ledger.Lock()
	if !set.activeRegistered {
		ledger.Unlock()
		return
	}
	p := &ledger.stats
	p.inventoryHolders--
	p.inventoryRefs -= r.refs
	p.inventorySegments -= r.segments
	p.inventoryBytes -= r.inventoryBytes
	p.potentialSegments -= r.segments
	p.potentialDescriptors -= r.descriptors
	p.potentialMappedBytes -= r.mappedBytes
	p.potentialFallbackBytes -= r.fallbackBytes
	for current, previous := ledger.activeHead, (*columnServingSegmentLeaseSet)(nil); current != nil; previous, current = current, current.activeNext {
		if current != set {
			continue
		}
		if previous == nil {
			ledger.activeHead = current.activeNext
		} else {
			previous.activeNext = current.activeNext
		}
		break
	}
	set.activeNext = nil
	set.activeRegistered = false
	if p.empty() {
		ledger.limits = typedGraphPhysicalResourceLimits{}
	}
	ledger.Unlock()
}

// quarantine indexes an already ledger-owned set as retained cleanup. While the
// DB is open the set's exact-base guardian is the last maintenance pin, so there
// is no split handoff that can lose it. DB teardown removes pin effectiveness
// and transfers the same Go cleanup owner to the closed-DB quarantine. Nothing
// closes the guardian object or refunds the reservation until retry confirms
// every physical cleanup.
func (ledger *typedGraphPhysicalResourceLedger) quarantine(set *columnServingSegmentLeaseSet, primaryErr, cleanupErr error) error {
	if ledger == nil || set == nil || cleanupErr == nil {
		return errors.New("collections: invalid serving segment cleanup quarantine")
	}
	set.mu.Lock()
	hasGuardian := set.guardian != nil
	set.mu.Unlock()
	if !hasGuardian {
		cleanupErr = errors.Join(cleanupErr, errors.New("collections: retained serving segment cleanup has no guardian pin"))
	}
	ledger.cleanupMu.Lock()
	defer ledger.cleanupMu.Unlock()
	ledger.Lock()
	defer ledger.Unlock()
	if set.quarantined {
		return errors.Join(set.quarantinePrimaryErr, set.quarantineCleanupErr)
	}
	set.quarantinePrimaryErr = primaryErr
	set.quarantineCleanupErr = cleanupErr
	set.quarantineNext = ledger.quarantineHead
	set.quarantined = true
	ledger.quarantineHead = set
	ledger.stats.cleanupQuarantines++
	ledger.stats.totalQuarantines++
	if primaryErr != nil {
		ledger.stats.lastPrimaryError = primaryErr.Error()
	}
	ledger.stats.lastCleanupError = cleanupErr.Error()
	return errors.Join(primaryErr, cleanupErr)
}

// retryQuarantinedPhysicalCleanup is deliberately coordinator-wide. One failed
// cleanup blocks fresh reservations until every retained OS owner is confirmed
// closed and its guardian can be released in physical-then-pin order.
func (ledger *typedGraphPhysicalResourceLedger) retryQuarantinedPhysicalCleanup() error {
	if ledger == nil {
		return nil
	}
	ledger.cleanupMu.Lock()
	defer ledger.cleanupMu.Unlock()
	return ledger.retryQuarantinedPhysicalCleanupLocked()
}

func (ledger *typedGraphPhysicalResourceLedger) retryQuarantinedPhysicalCleanupLocked() error {
	for {
		ledger.Lock()
		set := ledger.quarantineHead
		ledger.Unlock()
		if set == nil {
			return nil
		}
		ledger.Lock()
		ledger.stats.totalCleanupRetries++
		ledger.Unlock()
		if err := set.retryCleanup(); err != nil {
			ledger.Lock()
			ledger.stats.lastCleanupError = err.Error()
			ledger.Unlock()
			return err
		}
		ledger.Lock()
		if ledger.quarantineHead != set {
			ledger.Unlock()
			continue
		}
		ledger.quarantineHead = set.quarantineNext
		ledger.stats.cleanupQuarantines--
		set.quarantinePrimaryErr = nil
		set.quarantineCleanupErr = nil
		set.quarantineNext = nil
		set.quarantined = false
		if ledger.stats.empty() {
			ledger.limits = typedGraphPhysicalResourceLimits{}
		}
		ledger.Unlock()
	}
}

func (s *columnServingSegmentLeaseSet) matchesAuthority(rootDir, namespace string, refs []ColumnAssetRef) bool {
	if s == nil || namespace != s.namespace || len(refs) != len(s.refs) {
		return false
	}
	rootDir, err := normalizeColumnServingRoot(rootDir)
	if err != nil || rootDir != s.rootDir {
		return false
	}
	for i := range refs {
		if compareColumnAssetRefs(refs[i], s.refs[i]) != 0 {
			return false
		}
	}
	return true
}

func (s *columnServingSegmentLeaseSet) authorize(rootDir string, parent ColumnAssetRef) (*columnServingSegmentLease, *columnServingRefFallback, error) {
	if s == nil {
		return nil, nil, errColumnServingSegmentUnauthorized
	}
	rootDir, err := normalizeColumnServingRoot(rootDir)
	if err != nil || rootDir != s.rootDir || parent.Namespace != s.namespace {
		return nil, nil, fmt.Errorf("%w: root or namespace mismatch", errColumnServingSegmentUnauthorized)
	}
	i := sort.Search(len(s.refs), func(i int) bool { return compareColumnAssetRefs(s.refs[i], parent) >= 0 })
	if i == len(s.refs) || compareColumnAssetRefs(s.refs[i], parent) != 0 {
		return nil, nil, fmt.Errorf("%w: ref=%+v", errColumnServingSegmentUnauthorized, parent)
	}
	if i >= len(s.fallbacks) {
		return nil, nil, fmt.Errorf("%w: missing exact-ref fallback state", errColumnServingSegmentUnauthorized)
	}
	j := sort.Search(len(s.segments), func(i int) bool { return s.segments[i].fileID >= parent.FileID })
	if j == len(s.segments) || s.segments[j].fileID != parent.FileID {
		return nil, nil, fmt.Errorf("%w: segment file=%d", errColumnServingSegmentUnauthorized, parent.FileID)
	}
	return &s.segments[j], &s.fallbacks[i], nil
}

func validateColumnServingLogicalRange(parent ColumnAssetRef, relativeOffset, length int64, key mappedresource.Key, scope mappedresource.Scope) error {
	if err := validateColumnAssetRefForPlan(parent); err != nil {
		return err
	}
	if relativeOffset < 0 || length < 0 || relativeOffset > parent.Length || length > parent.Length-relativeOffset || parent.Offset > math.MaxInt64-relativeOffset {
		return fmt.Errorf("%w: relative offset=%d length=%d parent_length=%d", errColumnServingSegmentUnauthorized, relativeOffset, length, parent.Length)
	}
	absolute := parent.Offset + relativeOffset
	parentKey := mappedResourceKeyForColumnAssetRef(parent)
	if key.Class != parentKey.Class || key.Namespace != parent.Namespace || key.Kind != string(parent.Kind) || key.Generation != parent.Generation || key.PartID != parent.PartID || key.FileID != parent.FileID || key.Offset != absolute || key.Length != length {
		return fmt.Errorf("%w: logical key does not match exact parent subrange", errColumnServingSegmentUnauthorized)
	}
	return scope.ValidateForKey(key)
}

func (s *columnServingSegmentLeaseSet) acquireSegment(ctx context.Context, segment *columnServingSegmentLease) (columnServingSegmentOpenOutcome, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return columnServingSegmentOpenOutcome{}, err
		}
		s.mu.Lock()
		if s.closing || s.closed {
			s.mu.Unlock()
			return columnServingSegmentOpenOutcome{}, errColumnServingSegmentClosed
		}
		switch segment.state {
		case columnServingSegmentMapped:
			out := columnServingSegmentOpenOutcome{mapped: segment.mapped, identity: segment.identity, usable: columnServingSegmentMapped}
			s.mu.Unlock()
			return out, nil
		case columnServingSegmentFallback:
			out := columnServingSegmentOpenOutcome{file: segment.file, identity: segment.identity, mapErr: segment.mapErr, usable: columnServingSegmentFallback}
			s.mu.Unlock()
			return out, nil
		case columnServingSegmentCleanup:
			err := errors.Join(errColumnServingSegmentCleanupRetained, segment.buildErr, segment.cleanupErr)
			s.mu.Unlock()
			return columnServingSegmentOpenOutcome{}, err
		case columnServingSegmentClosed:
			s.mu.Unlock()
			return columnServingSegmentOpenOutcome{}, errColumnServingSegmentClosed
		case columnServingSegmentBuilding:
			attempt := segment.attempt
			s.mu.Unlock()
			select {
			case <-attempt.ready:
				if attempt.err != nil {
					return columnServingSegmentOpenOutcome{}, attempt.err
				}
			case <-ctx.Done():
				return columnServingSegmentOpenOutcome{}, ctx.Err()
			}
		case columnServingSegmentUnopened:
			attempt := &columnServingSegmentLeaseAttempt{ready: make(chan struct{})}
			segment.state, segment.attempt = columnServingSegmentBuilding, attempt
			s.builders.Add(1)
			s.mu.Unlock()
			go s.buildSegment(segment, attempt)
			select {
			case <-attempt.ready:
				if attempt.err != nil {
					return columnServingSegmentOpenOutcome{}, attempt.err
				}
			case <-ctx.Done():
				return columnServingSegmentOpenOutcome{}, ctx.Err()
			}
		default:
			s.mu.Unlock()
			return columnServingSegmentOpenOutcome{}, errors.New("collections: serving segment has invalid state")
		}
	}
}

func (s *columnServingSegmentLeaseSet) buildSegment(segment *columnServingSegmentLease, attempt *columnServingSegmentLeaseAttempt) {
	defer s.builders.Done()
	accounting, err := s.reserveSegmentBuild(segment.mappedCharge)
	if err != nil {
		s.finishSegmentBuild(segment, attempt, columnServingSegmentOpenOutcome{buildErr: err})
		return
	}
	out := s.openSegmentBacking(segment.fileID, segment.authorizedEnd, accounting)
	accounting.finish(&out)
	s.finishSegmentBuild(segment, attempt, out)
}

func (s *columnServingSegmentLeaseSet) finishSegmentBuild(segment *columnServingSegmentLease, attempt *columnServingSegmentLeaseAttempt, out columnServingSegmentOpenOutcome) {
	s.mu.Lock()
	closing := s.closing || s.closed
	segment.attempt = nil
	segment.mapped, segment.file = out.mapped, out.file
	segment.identity, segment.mapErr = out.identity, out.mapErr
	segment.buildErr, segment.cleanupErr = out.buildErr, out.cleanupErr
	segment.fileRetryable, segment.fileUnconfirmed = out.fileRetryable, out.fileUnconfirmed
	segment.fallbackAccounted = out.usable == columnServingSegmentFallback && out.file != nil
	segment.cleanupAccounted = false
	switch {
	case out.hasResources() && (out.err() != nil || closing):
		segment.state = columnServingSegmentCleanup
		attempt.err = errors.Join(out.err(), func() error {
			if closing {
				return errColumnServingSegmentClosed
			}
			return nil
		}())
	case out.err() != nil:
		if closing {
			segment.state = columnServingSegmentClosed
			attempt.err = errors.Join(out.err(), errColumnServingSegmentClosed)
		} else {
			segment.state = columnServingSegmentUnopened
			attempt.err = out.err()
		}
	case closing:
		segment.state = columnServingSegmentClosed
		attempt.err = errColumnServingSegmentClosed
	default:
		segment.state = out.usable
	}
	needsCleanupCharge := segment.state == columnServingSegmentCleanup && !segment.cleanupAccounted
	if needsCleanupCharge {
		segment.cleanupAccounted = true
	}
	s.mu.Unlock()
	if needsCleanupCharge {
		s.recordCleanupBacking()
	}
	close(attempt.ready)
}

var columnServingSegmentLeaseHooks struct {
	sync.RWMutex
	open     func(string) (*os.File, error)
	stat     func(*os.File) (os.FileInfo, error)
	identity func(*os.File) columnAssetVerifiedChecksumFileIdentity
	mmap     func(*os.File, int64) ([]byte, error)
	unmap    func([]byte) error
	readAt   func(*os.File, []byte, int64) (int, error)
	close    func(*os.File) columnServingSegmentCloseOutcome
}

func openColumnServingSegment(path string) (*os.File, error) {
	columnServingSegmentLeaseHooks.RLock()
	hook := columnServingSegmentLeaseHooks.open
	columnServingSegmentLeaseHooks.RUnlock()
	if hook != nil {
		return hook(path)
	}
	return os.Open(path)
}

func statColumnServingSegment(file *os.File) (os.FileInfo, error) {
	if file == nil {
		return nil, os.ErrInvalid
	}
	columnServingSegmentLeaseHooks.RLock()
	hook := columnServingSegmentLeaseHooks.stat
	columnServingSegmentLeaseHooks.RUnlock()
	if hook != nil {
		return hook(file)
	}
	return file.Stat()
}

func identityColumnServingSegment(file *os.File) columnAssetVerifiedChecksumFileIdentity {
	columnServingSegmentLeaseHooks.RLock()
	hook := columnServingSegmentLeaseHooks.identity
	columnServingSegmentLeaseHooks.RUnlock()
	if hook != nil {
		return hook(file)
	}
	return columnAssetVerifiedChecksumFileIdentityFromFile(file)
}

func mmapColumnServingSegment(file *os.File, size int64) ([]byte, error) {
	columnServingSegmentLeaseHooks.RLock()
	hook := columnServingSegmentLeaseHooks.mmap
	columnServingSegmentLeaseHooks.RUnlock()
	if hook != nil {
		return hook(file, size)
	}
	return mmapColumnPhysicalAssetFilePrefix(file, size)
}

func munmapColumnServingSegment(data []byte) error {
	columnServingSegmentLeaseHooks.RLock()
	hook := columnServingSegmentLeaseHooks.unmap
	columnServingSegmentLeaseHooks.RUnlock()
	if hook != nil {
		return hook(data)
	}
	return munmapColumnPhysicalAssetFile(data)
}

func closeColumnServingSegment(file *os.File) columnServingSegmentCloseOutcome {
	if file == nil {
		return columnServingSegmentCloseOutcome{confirmed: true}
	}
	columnServingSegmentLeaseHooks.RLock()
	hook := columnServingSegmentLeaseHooks.close
	columnServingSegmentLeaseHooks.RUnlock()
	if hook != nil {
		return hook(file)
	}
	err := file.Close()
	return columnServingSegmentCloseOutcome{confirmed: err == nil, err: err}
}

func readAtColumnServingSegment(file *os.File, dst []byte, offset int64) (int, error) {
	columnServingSegmentLeaseHooks.RLock()
	hook := columnServingSegmentLeaseHooks.readAt
	columnServingSegmentLeaseHooks.RUnlock()
	if hook != nil {
		return hook(file, dst, offset)
	}
	return file.ReadAt(dst, offset)
}

func (s *columnServingSegmentLeaseSet) openSegmentBacking(fileID uint32, authorizedEnd int64, accounting *columnServingSegmentBuildAccounting) columnServingSegmentOpenOutcome {
	path, err := columnAssetSegmentPath(s.rootDir, ColumnAssetRef{Namespace: s.namespace, FileID: fileID})
	if err != nil {
		return columnServingSegmentOpenOutcome{buildErr: err}
	}
	file, err := openColumnServingSegment(path)
	if file == nil {
		if err == nil {
			err = errors.New("collections: serving segment open returned no descriptor")
		}
		return columnServingSegmentOpenOutcome{buildErr: err}
	}
	s.recordPhysicalOpen()
	out := columnServingSegmentOpenOutcome{file: file}
	if err != nil {
		out.buildErr = err
		closeOutcome := accounting.closeDescriptor(file)
		if !closeOutcome.confirmed {
			out.cleanupErr = closeOutcome.err
			out.fileRetryable, out.fileUnconfirmed = closeOutcome.retryable, true
		} else {
			out.file = nil
		}
		return out
	}
	info, statErr := statColumnServingSegment(file)
	if statErr != nil || info == nil || info.Size() < authorizedEnd {
		if statErr != nil {
			out.buildErr = statErr
		} else if info == nil {
			out.buildErr = errors.New("collections: serving segment stat returned no file information")
		} else {
			out.buildErr = fmt.Errorf("collections: serving segment file=%d size=%d is below authorized end=%d", fileID, info.Size(), authorizedEnd)
		}
		closeOutcome := accounting.closeDescriptor(file)
		if !closeOutcome.confirmed {
			out.cleanupErr = closeOutcome.err
			out.fileRetryable, out.fileUnconfirmed = closeOutcome.retryable, true
		} else {
			out.file = nil
		}
		return out
	}
	// Stable cached-verify identity is separate from size authority. Portable
	// fallback platforms intentionally return an invalid identity and re-hash.
	out.identity = identityColumnServingSegment(file)
	mapped, mapErr := mmapColumnServingSegment(file, authorizedEnd)
	out.mapped, out.mapErr = mapped, mapErr
	if len(mapped) != 0 {
		accounting.mappingAcquired()
	}
	if mapErr != nil {
		if len(mapped) != 0 {
			if unmapErr := munmapColumnServingSegment(mapped); unmapErr != nil {
				out.cleanupErr = unmapErr
			} else {
				out.mapped = nil
				accounting.mappingReleased()
			}
			out.buildErr = mapErr
			closeOutcome := accounting.closeDescriptor(file)
			if !closeOutcome.confirmed {
				out.cleanupErr = errors.Join(out.cleanupErr, closeOutcome.err)
				out.fileRetryable, out.fileUnconfirmed = closeOutcome.retryable, true
			} else {
				out.file = nil
			}
			return out
		}
		out.usable = columnServingSegmentFallback
		accounting.retainDescriptor(false, true)
		return out
	}
	if int64(len(mapped)) != authorizedEnd {
		out.buildErr = fmt.Errorf("collections: serving segment mapped bytes=%d want=%d", len(mapped), authorizedEnd)
		if len(mapped) != 0 {
			if unmapErr := munmapColumnServingSegment(mapped); unmapErr != nil {
				out.cleanupErr = unmapErr
			} else {
				out.mapped = nil
				accounting.mappingReleased()
			}
		}
		closeOutcome := accounting.closeDescriptor(file)
		if !closeOutcome.confirmed {
			out.cleanupErr = errors.Join(out.cleanupErr, closeOutcome.err)
			out.fileRetryable, out.fileUnconfirmed = closeOutcome.retryable, true
		} else {
			out.file = nil
		}
		return out
	}
	closeOutcome := accounting.closeDescriptor(file)
	if !closeOutcome.confirmed {
		out.buildErr = errors.New("collections: mapped serving segment descriptor close was not confirmed")
		out.cleanupErr = closeOutcome.err
		out.fileRetryable, out.fileUnconfirmed = closeOutcome.retryable, true
		if unmapErr := munmapColumnServingSegment(mapped); unmapErr != nil {
			out.cleanupErr = errors.Join(out.cleanupErr, unmapErr)
		} else {
			out.mapped = nil
			accounting.mappingReleased()
		}
		return out
	}
	out.file = nil
	out.usable = columnServingSegmentMapped
	return out
}

func (s *columnServingSegmentLeaseSet) reserveSegmentBuild(mappedBytes int64) (*columnServingSegmentBuildAccounting, error) {
	s.ledger.Lock()
	defer s.ledger.Unlock()
	p := &s.ledger.stats
	if s.ledger.closedDB || !s.activeRegistered || mappedBytes <= 0 || p.descriptorsInFlight+p.descriptorsLive >= p.potentialDescriptors || mappedBytes > p.potentialMappedBytes-p.mappedBytes-p.mappedBytesInFlight {
		p.totalBuildFailures++
		p.lastBuildError = errTypedGraphOwnerBudget.Error()
		return nil, errTypedGraphOwnerBudget
	}
	p.descriptorsInFlight++
	p.mappedBytesInFlight += mappedBytes
	p.totalBuilds++
	return &columnServingSegmentBuildAccounting{set: s, mappedCharge: mappedBytes, descriptorInFlight: true, mappedInFlight: true}, nil
}

func (a *columnServingSegmentBuildAccounting) mappingAcquired() {
	if a == nil || !a.mappedInFlight {
		return
	}
	a.set.ledger.Lock()
	p := &a.set.ledger.stats
	p.mappedBytesInFlight -= a.mappedCharge
	p.mappedBackings++
	p.mappedBytes += a.mappedCharge
	p.totalMaps++
	a.set.ledger.Unlock()
	a.mappedInFlight, a.mappedLive = false, true
}

func (a *columnServingSegmentBuildAccounting) mappingReleased() {
	if a == nil || !a.mappedLive {
		return
	}
	a.set.ledger.Lock()
	p := &a.set.ledger.stats
	p.mappedBackings--
	p.mappedBytes -= a.mappedCharge
	p.totalUnmaps++
	a.set.ledger.Unlock()
	a.mappedLive = false
}

func (a *columnServingSegmentBuildAccounting) retainDescriptor(unconfirmed, fallback bool) {
	if a == nil || !a.descriptorInFlight {
		return
	}
	a.set.ledger.Lock()
	p := &a.set.ledger.stats
	p.descriptorsInFlight--
	p.descriptorsLive++
	if unconfirmed {
		p.unconfirmedDescriptors++
	}
	if fallback {
		p.fallbackSegments++
	}
	a.set.ledger.Unlock()
	a.descriptorInFlight = false
}

func (a *columnServingSegmentBuildAccounting) closeDescriptor(file *os.File) columnServingSegmentCloseOutcome {
	out := a.set.closeSegmentDescriptor(file)
	if out.confirmed {
		if a.descriptorInFlight {
			a.set.ledger.Lock()
			a.set.ledger.stats.descriptorsInFlight--
			a.set.ledger.Unlock()
			a.descriptorInFlight = false
		}
		return out
	}
	a.retainDescriptor(true, false)
	return out
}

func (a *columnServingSegmentBuildAccounting) finish(out *columnServingSegmentOpenOutcome) {
	if a == nil || out == nil {
		return
	}
	// Fail safe if a new error branch forgets to settle a reservation: infer
	// ownership from the returned outcome rather than refunding a live object.
	if a.descriptorInFlight {
		if out.file != nil {
			a.retainDescriptor(out.fileUnconfirmed, out.usable == columnServingSegmentFallback)
		} else {
			a.set.ledger.Lock()
			a.set.ledger.stats.descriptorsInFlight--
			a.set.ledger.Unlock()
			a.descriptorInFlight = false
		}
	}
	if a.mappedInFlight {
		if len(out.mapped) != 0 {
			a.mappingAcquired()
		} else {
			a.set.ledger.Lock()
			a.set.ledger.stats.mappedBytesInFlight -= a.mappedCharge
			a.set.ledger.Unlock()
			a.mappedInFlight = false
		}
	}
	a.set.ledger.Lock()
	p := &a.set.ledger.stats
	if err := out.err(); err != nil {
		p.totalBuildFailures++
		p.lastBuildError = err.Error()
	}
	if out.cleanupErr != nil {
		p.totalCleanupFailures++
		p.lastCleanupError = out.cleanupErr.Error()
	}
	a.set.ledger.Unlock()
}

func (s *columnServingSegmentLeaseSet) recordCleanupBacking() {
	s.ledger.Lock()
	s.ledger.stats.cleanupBackings++
	s.ledger.Unlock()
}

func (s *columnServingSegmentLeaseSet) recordPhysicalOpen() {
	s.ledger.Lock()
	s.ledger.stats.totalOpens++
	s.ledger.Unlock()
}

func (s *columnServingSegmentLeaseSet) closeSegmentDescriptor(file *os.File) columnServingSegmentCloseOutcome {
	out := closeColumnServingSegment(file)
	if !out.confirmed && out.err == nil {
		out.err = errors.New("collections: serving segment descriptor close was not confirmed")
	}
	if out.confirmed {
		// Confirmation is the ownership boundary. A platform-specific adapter
		// may prove closure despite a lower-level warning; it must not retain a
		// phantom descriptor or block reservation refund.
		out.retryable = false
		out.err = nil
	}
	s.ledger.Lock()
	s.ledger.stats.totalCloseAttempts++
	if out.confirmed {
		s.ledger.stats.totalCloses++
	}
	s.ledger.Unlock()
	return out
}

func (s *columnServingSegmentLeaseSet) reserveFallbackBytes(bytes int64) error {
	if bytes <= 0 {
		return errTypedGraphOwnerBudget
	}
	for {
		used := s.fallbackReservedBytes.Load()
		if bytes > s.reservation.fallbackBytes-used {
			return errTypedGraphOwnerBudget
		}
		if s.fallbackReservedBytes.CompareAndSwap(used, used+bytes) {
			break
		}
	}
	s.ledger.Lock()
	p := &s.ledger.stats
	if s.ledger.closedDB || !s.activeRegistered || bytes > p.potentialFallbackBytes-p.fallbackBytes-p.fallbackBytesInFlight {
		s.ledger.Unlock()
		s.fallbackReservedBytes.Add(-bytes)
		return ErrVectorIndexSnapshotMismatch
	}
	p.fallbackBytesInFlight += bytes
	s.ledger.Unlock()
	return nil
}

func (s *columnServingSegmentLeaseSet) commitFallbackBytes(bytes int64) {
	if s == nil || bytes <= 0 {
		return
	}
	s.ledger.Lock()
	s.ledger.stats.fallbackBytesInFlight -= bytes
	s.ledger.stats.fallbackBackings++
	s.ledger.stats.fallbackBytes += bytes
	s.ledger.stats.totalFallbacks++
	s.ledger.Unlock()
}

func (s *columnServingSegmentLeaseSet) refundFallbackBytes(bytes int64) {
	if s == nil || bytes <= 0 {
		return
	}
	s.ledger.Lock()
	s.ledger.stats.fallbackBytesInFlight -= bytes
	s.ledger.Unlock()
	s.fallbackReservedBytes.Add(-bytes)
}

func (s *columnServingSegmentLeaseSet) releaseFallbackBytes(bytes int64) {
	if s == nil || bytes <= 0 {
		return
	}
	s.ledger.Lock()
	s.ledger.stats.fallbackBackings--
	s.ledger.stats.fallbackBytes -= bytes
	s.ledger.Unlock()
	s.fallbackReservedBytes.Add(-bytes)
}

func (s *columnServingSegmentLeaseSet) recordFallbackBuildFailure(err error) {
	if s == nil || err == nil {
		return
	}
	s.ledger.Lock()
	s.ledger.stats.totalBuildFailures++
	s.ledger.stats.lastBuildError = err.Error()
	s.ledger.Unlock()
}

// acquireRefFallback returns the one pool-owned immutable buffer for parent.
// Its detached build captures only pool-owned immutable authority and a segment
// backing protected by the pool guardian. A canceled waiter leaves promptly;
// the build publishes for another waiter or rolls back to retryable unopened.
func (s *columnServingSegmentLeaseSet) acquireRefFallback(ctx context.Context, parent ColumnAssetRef, fallback *columnServingRefFallback, backing columnServingSegmentOpenOutcome) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.mu.Lock()
		if s.closing || s.closed {
			s.mu.Unlock()
			return nil, errColumnServingSegmentClosed
		}
		switch fallback.state {
		case columnServingRefFallbackReady:
			raw := fallback.bytes
			s.mu.Unlock()
			return raw, nil
		case columnServingRefFallbackBuilding:
			attempt := fallback.attempt
			s.mu.Unlock()
			select {
			case <-attempt.ready:
				if attempt.err != nil {
					return nil, attempt.err
				}
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		case columnServingRefFallbackUnopened:
			attempt := &columnServingRefFallbackAttempt{ready: make(chan struct{})}
			fallback.state, fallback.attempt = columnServingRefFallbackBuilding, attempt
			s.builders.Add(1)
			s.mu.Unlock()
			go s.buildRefFallback(parent, fallback, backing, attempt)
			select {
			case <-attempt.ready:
				if attempt.err != nil {
					return nil, attempt.err
				}
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		case columnServingRefFallbackClosed:
			s.mu.Unlock()
			return nil, errColumnServingSegmentClosed
		default:
			s.mu.Unlock()
			return nil, errors.New("collections: serving ref fallback has invalid state")
		}
	}
}

func (s *columnServingSegmentLeaseSet) buildRefFallback(parent ColumnAssetRef, fallback *columnServingRefFallback, backing columnServingSegmentOpenOutcome, attempt *columnServingRefFallbackAttempt) {
	defer s.builders.Done()
	err := s.reserveFallbackBytes(parent.Length)
	reserved := err == nil
	var raw []byte
	if err == nil {
		raw = make([]byte, int(parent.Length))
		end := parent.Offset + parent.Length
		switch {
		case backing.file != nil:
			n, readErr := readAtColumnServingSegment(backing.file, raw, parent.Offset)
			if readErr != nil && readErr != io.EOF {
				err = readErr
			} else if n != len(raw) {
				err = io.ErrUnexpectedEOF
			}
		case parent.Offset >= 0 && end >= parent.Offset && end <= int64(len(backing.mapped)):
			copy(raw, backing.mapped[parent.Offset:end])
		default:
			err = fmt.Errorf("%w: exact parent range has no complete segment backing", errColumnServingSegmentUnauthorized)
		}
	}
	if err == nil {
		// CachedVerify may reuse a prior proof only with the captured stable file
		// identity. Portable fallback identities are invalid, so this hashes and
		// remains fail closed there. The pool publishes this immutable buffer only
		// after the complete parent checksum is validated.
		err = verifyColumnPhysicalAssetReadChecksumWithIntegrityForSegment(raw, parent, true, ColumnAssetReadIntegrityCachedVerify, s.rootDir, backing.identity)
	}
	committed := err == nil
	if committed {
		s.commitFallbackBytes(parent.Length)
	} else if reserved {
		s.refundFallbackBytes(parent.Length)
		raw = nil
	}

	s.mu.Lock()
	closing := s.closing || s.closed
	fallback.attempt = nil
	switch {
	case err != nil:
		if closing {
			fallback.state = columnServingRefFallbackClosed
			attempt.err = errors.Join(err, errColumnServingSegmentClosed)
		} else {
			fallback.state = columnServingRefFallbackUnopened
			attempt.err = err
		}
	case closing:
		fallback.state = columnServingRefFallbackClosed
		attempt.err = errColumnServingSegmentClosed
	default:
		fallback.bytes = raw
		fallback.state = columnServingRefFallbackReady
	}
	published := fallback.state == columnServingRefFallbackReady
	s.mu.Unlock()
	if committed && !published {
		s.releaseFallbackBytes(parent.Length)
	}
	if err != nil {
		s.recordFallbackBuildFailure(err)
	}
	close(attempt.ready)
}

func (s *columnServingSegmentLeaseSet) validateAuthorizedRange(rootDir string, parent ColumnAssetRef, relativeOffset, length int64, key mappedresource.Key, scope mappedresource.Scope) (*columnServingSegmentLease, *columnServingRefFallback, string, error) {
	segment, fallback, err := s.authorize(rootDir, parent)
	if err != nil {
		return nil, nil, "", err
	}
	if err := validateColumnServingLogicalRange(parent, relativeOffset, length, key, scope); err != nil {
		return nil, nil, "", err
	}
	if scope.Collection != "" && scope.Collection != s.collection {
		return nil, nil, "", fmt.Errorf("%w: logical scope collection mismatch", errColumnServingSegmentUnauthorized)
	}
	if scope.Generation != 0 && scope.Generation != parent.Generation {
		return nil, nil, "", fmt.Errorf("%w: logical scope generation mismatch", errColumnServingSegmentUnauthorized)
	}
	path, err := columnAssetSegmentPath(s.rootDir, parent)
	if err != nil {
		return nil, nil, "", err
	}
	return segment, fallback, path, nil
}

func (s *columnServingSegmentLeaseSet) acquireRange(ctx context.Context, rootDir string, parent ColumnAssetRef, relativeOffset, length int64, manager *mappedresource.Manager, key mappedresource.Key, scope mappedresource.Scope, opts mappedresource.AcquireOptions) (*columnServingRangeHandle, error) {
	if manager == nil {
		return nil, errors.New("collections: serving segment logical range requires mappedresource manager")
	}
	if err := s.beginUse(); err != nil {
		return nil, err
	}
	defer s.endUse()
	segment, fallback, path, err := s.validateAuthorizedRange(rootDir, parent, relativeOffset, length, key, scope)
	if err != nil {
		return nil, err
	}
	if opts.ResourceRoot != "" {
		root, normalizeErr := normalizeColumnServingRoot(opts.ResourceRoot)
		if normalizeErr != nil || root != s.rootDir {
			return nil, fmt.Errorf("%w: logical resource root mismatch", errColumnServingSegmentUnauthorized)
		}
	}
	if opts.ResourcePath != "" && filepath.Clean(opts.ResourcePath) != filepath.Clean(path) {
		return nil, fmt.Errorf("%w: logical resource path mismatch", errColumnServingSegmentUnauthorized)
	}
	opts.ResourceRoot, opts.ResourcePath = s.rootDir, path
	backing, err := s.acquireSegment(ctx, segment)
	if err != nil {
		return nil, err
	}
	end := key.Offset + key.Length
	if key.Offset < 0 || end < key.Offset || end > segment.authorizedEnd {
		return nil, fmt.Errorf("%w: logical range exceeds authorized segment end", errColumnServingSegmentUnauthorized)
	}
	if len(backing.mapped) != 0 && opts.PreferMapped {
		handle, err := manager.AcquireBytes(key, scope, mappedresource.SourceMapped, backing.mapped[key.Offset:end], opts)
		if err != nil {
			return nil, err
		}
		return &columnServingRangeHandle{handle: handle}, nil
	}
	if !opts.AllowHeapCopy {
		if backing.mapErr != nil {
			return nil, backing.mapErr
		}
		return nil, errors.New("collections: serving segment heap-copy fallback is disabled")
	}
	parentBytes, err := s.acquireRefFallback(ctx, parent, fallback, backing)
	if err != nil {
		return nil, err
	}
	parentEnd := relativeOffset + length
	if relativeOffset < 0 || parentEnd < relativeOffset || parentEnd > int64(len(parentBytes)) {
		return nil, fmt.Errorf("%w: logical range exceeds exact parent fallback", errColumnServingSegmentUnauthorized)
	}
	raw := parentBytes[relativeOffset:parentEnd]
	if opts.FallbackReason == "" {
		if backing.mapErr != nil {
			opts.FallbackReason = mappedresource.FallbackMmapFailed
		} else {
			opts.FallbackReason = mappedresource.FallbackReadAt
		}
	}
	handle, err := manager.AcquireBytes(key, scope, mappedresource.SourceHeapCopy, raw, opts)
	if err != nil {
		return nil, err
	}
	return &columnServingRangeHandle{handle: handle}, nil
}

// withBorrowedRange is the materializer-only physical seam. It never populates
// a pool-owned ref fallback. The callback executes while a pool use protects
// either the exact mapped slice or the shared segment descriptor and identity;
// any ReadAt destination and retained logical handle remain request-owned.
func (s *columnServingSegmentLeaseSet) withBorrowedRange(ctx context.Context, rootDir string, parent ColumnAssetRef, relativeOffset, length int64, key mappedresource.Key, scope mappedresource.Scope, fn func(columnServingBorrowedRange) error) error {
	if fn == nil {
		return errors.New("collections: serving segment borrowed range callback is required")
	}
	if err := s.beginUse(); err != nil {
		return err
	}
	defer s.endUse()
	segment, _, _, err := s.validateAuthorizedRange(rootDir, parent, relativeOffset, length, key, scope)
	if err != nil {
		return err
	}
	backing, err := s.acquireSegment(ctx, segment)
	if err != nil {
		return err
	}
	end := key.Offset + key.Length
	if key.Offset < 0 || end < key.Offset || end > segment.authorizedEnd {
		return fmt.Errorf("%w: borrowed logical range exceeds authorized segment end", errColumnServingSegmentUnauthorized)
	}
	borrowed := columnServingBorrowedRange{file: backing.file, identity: backing.identity, absoluteOffset: key.Offset, length: key.Length}
	if len(backing.mapped) != 0 {
		borrowed.bytes = backing.mapped[key.Offset:end]
		borrowed.file = nil
	}
	return fn(borrowed)
}

func (s *columnServingSegmentLeaseSet) beginUse() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.closed {
		return errColumnServingSegmentClosed
	}
	s.users.Add(1)
	return nil
}

func (s *columnServingSegmentLeaseSet) endUse() {
	if s != nil {
		s.users.Done()
	}
}

func (s *columnServingSegmentLeaseSet) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		err := s.closeErr
		s.mu.Unlock()
		return err
	}
	if s.closing {
		done := s.closeDone
		s.mu.Unlock()
		<-done
		s.mu.Lock()
		err := s.closeErr
		s.mu.Unlock()
		return err
	}
	s.closing = true
	s.mu.Unlock()
	s.users.Wait()
	s.builders.Wait()

	cleanupErr := s.cleanupRefFallbacks()
	if cleanupErr == nil {
		cleanupErr = s.cleanupPhysical()
	}
	if cleanupErr == nil {
		fallbackBytes := s.fallbackReservedBytes.Load()
		if fallbackBytes != 0 {
			cleanupErr = fmt.Errorf("collections: serving segment pool closed with %d retained parent fallback bytes", fallbackBytes)
		}
	}
	if cleanupErr == nil {
		cleanupErr = s.closeGuardian()
	}
	if cleanupErr == nil {
		s.releaseReservation()
	}
	s.mu.Lock()
	s.closing = false
	s.closed = true
	s.closeErr = cleanupErr
	close(s.closeDone)
	s.mu.Unlock()
	return cleanupErr
}

func (s *columnServingSegmentLeaseSet) cleanupRefFallbacks() error {
	if s == nil {
		return nil
	}
	var releasedBytes []int64
	s.mu.Lock()
	for i := range s.fallbacks {
		fallback := &s.fallbacks[i]
		if fallback.state == columnServingRefFallbackBuilding {
			s.mu.Unlock()
			return errors.New("collections: serving ref fallback builder remained active during cleanup")
		}
		if fallback.state == columnServingRefFallbackReady && fallback.bytes == nil {
			s.mu.Unlock()
			return errors.New("collections: ready serving ref fallback has no bytes")
		}
		if fallback.state > columnServingRefFallbackClosed {
			s.mu.Unlock()
			return errors.New("collections: serving ref fallback has invalid cleanup state")
		}
	}
	for i := range s.fallbacks {
		fallback := &s.fallbacks[i]
		switch fallback.state {
		case columnServingRefFallbackUnopened:
			fallback.state = columnServingRefFallbackClosed
		case columnServingRefFallbackReady:
			releasedBytes = append(releasedBytes, int64(len(fallback.bytes)))
			fallback.bytes = nil
			fallback.state = columnServingRefFallbackClosed
		case columnServingRefFallbackClosed:
		}
	}
	s.mu.Unlock()
	for _, bytes := range releasedBytes {
		s.releaseFallbackBytes(bytes)
	}
	return nil
}

func (s *columnServingSegmentLeaseSet) cleanupPhysical() error {
	s.cleanupMu.Lock()
	defer s.cleanupMu.Unlock()
	var cleanupErr error
	for i := range s.segments {
		segment := &s.segments[i]
		s.mu.Lock()
		state := segment.state
		mapped, file := segment.mapped, segment.file
		fallbackAccounted, cleanupAccounted := segment.fallbackAccounted, segment.cleanupAccounted
		fileRetryable, fileUnconfirmed := segment.fileRetryable, segment.fileUnconfirmed
		priorCleanupErr := segment.cleanupErr
		s.mu.Unlock()
		if state == columnServingSegmentUnopened {
			s.mu.Lock()
			segment.state = columnServingSegmentClosed
			s.mu.Unlock()
			continue
		}
		if state == columnServingSegmentClosed {
			continue
		}
		if state == columnServingSegmentBuilding {
			cleanupErr = errors.Join(cleanupErr, errors.New("collections: serving segment builder remained active during cleanup"))
			continue
		}

		var unmapErr, closeErr error
		if len(mapped) != 0 {
			unmapErr = munmapColumnServingSegment(mapped)
			if unmapErr == nil {
				s.ledger.Lock()
				s.ledger.stats.mappedBackings--
				s.ledger.stats.mappedBytes -= segment.mappedCharge
				s.ledger.stats.totalUnmaps++
				s.ledger.Unlock()
				s.mu.Lock()
				segment.mapped = nil
				s.mu.Unlock()
			}
		}
		if file != nil {
			if fileUnconfirmed && !fileRetryable {
				closeErr = priorCleanupErr
				if closeErr == nil {
					closeErr = errors.New("collections: serving segment descriptor closure remains unconfirmed")
				}
			} else {
				closeOutcome := s.closeSegmentDescriptor(file)
				if closeOutcome.confirmed {
					s.ledger.Lock()
					s.ledger.stats.descriptorsLive--
					if fileUnconfirmed {
						s.ledger.stats.unconfirmedDescriptors--
					}
					if fallbackAccounted {
						s.ledger.stats.fallbackSegments--
					}
					s.ledger.Unlock()
					s.mu.Lock()
					segment.file = nil
					segment.fileRetryable = false
					segment.fileUnconfirmed = false
					segment.fallbackAccounted = false
					s.mu.Unlock()
				} else {
					closeErr = closeOutcome.err
					if !fileUnconfirmed {
						s.ledger.Lock()
						s.ledger.stats.unconfirmedDescriptors++
						s.ledger.Unlock()
					}
					s.mu.Lock()
					segment.fileRetryable = closeOutcome.retryable
					segment.fileUnconfirmed = true
					s.mu.Unlock()
				}
			}
		}
		segmentErr := errors.Join(unmapErr, closeErr)
		s.mu.Lock()
		resourcesRemain := len(segment.mapped) != 0 || segment.file != nil
		s.mu.Unlock()
		if !resourcesRemain {
			if cleanupAccounted {
				s.ledger.Lock()
				s.ledger.stats.cleanupBackings--
				s.ledger.Unlock()
			}
			s.mu.Lock()
			segment.state = columnServingSegmentClosed
			segment.cleanupErr = nil
			segment.cleanupAccounted = false
			s.mu.Unlock()
		} else {
			if segmentErr == nil {
				segmentErr = errors.New("collections: serving segment cleanup could not confirm resource release")
			}
			if !cleanupAccounted {
				s.ledger.Lock()
				s.ledger.stats.cleanupBackings++
				s.ledger.Unlock()
			}
			s.mu.Lock()
			segment.state = columnServingSegmentCleanup
			segment.cleanupErr = segmentErr
			segment.cleanupAccounted = true
			s.mu.Unlock()
		}
		cleanupErr = errors.Join(cleanupErr, segmentErr)
	}
	if cleanupErr != nil {
		s.ledger.Lock()
		s.ledger.stats.totalCleanupFailures++
		s.ledger.stats.lastCleanupError = cleanupErr.Error()
		s.ledger.Unlock()
		return errors.Join(errColumnServingSegmentCleanupRetained, cleanupErr)
	}
	return nil
}

func (s *columnServingSegmentLeaseSet) releaseReservation() {
	s.mu.Lock()
	if s.reservationReleased {
		s.mu.Unlock()
		return
	}
	s.reservationReleased = true
	s.mu.Unlock()
	releaseColumnServingSegmentLeaseSet(s)
}

func (s *columnServingSegmentLeaseSet) closeGuardian() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	guardian := s.guardian
	s.mu.Unlock()
	if guardian == nil {
		return nil
	}
	if err := guardian.Close(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.guardian == guardian {
		s.guardian = nil
	}
	s.mu.Unlock()
	return nil
}

func (s *columnServingSegmentLeaseSet) cleanupRetained() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	retained := s.closed && (!s.reservationReleased || s.guardian != nil)
	s.mu.Unlock()
	return retained
}

func (s *columnServingSegmentLeaseSet) retryCleanup() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if !s.closed {
		s.mu.Unlock()
		return nil
	}
	released := s.reservationReleased
	s.mu.Unlock()
	if released {
		return s.closeGuardian()
	}
	fallbackBytes := s.fallbackReservedBytes.Load()
	if fallbackBytes != 0 {
		if err := s.cleanupRefFallbacks(); err != nil {
			return err
		}
		fallbackBytes = s.fallbackReservedBytes.Load()
		if fallbackBytes != 0 {
			return fmt.Errorf("collections: serving segment cleanup retains %d parent fallback bytes", fallbackBytes)
		}
	}
	if err := s.cleanupPhysical(); err != nil {
		return err
	}
	if err := s.closeGuardian(); err != nil {
		return err
	}
	s.releaseReservation()
	return nil
}
