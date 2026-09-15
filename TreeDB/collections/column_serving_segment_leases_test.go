package collections

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/page"
)

var errColumnServingLeaseInjected = errors.New("collections: injected serving lease failure")

type columnServingLeaseTestFixture struct {
	root       string
	namespace  string
	collection string
	db         *backenddb.DB
	refs       []ColumnAssetRef
	key        columnVectorGraphSharedPreparedSearchKey
	guardian   *ColumnAssetLifecyclePinSet
	ledger     *typedGraphPhysicalResourceLedger
	limits     typedGraphPhysicalResourceLimits
	set        *columnServingSegmentLeaseSet
}

func newColumnServingLeaseTestFixture(t testing.TB, fileIDs ...uint32) *columnServingLeaseTestFixture {
	t.Helper()
	if len(fileIDs) == 0 {
		fileIDs = []uint32{1}
	}
	fixture := &columnServingLeaseTestFixture{
		root:       t.TempDir(),
		namespace:  "docs_assets",
		collection: "docs",
		db:         new(backenddb.DB),
		ledger:     new(typedGraphPhysicalResourceLedger),
		limits: typedGraphPhysicalResourceLimits{
			Segments: 64, Descriptors: 64, MappedBytes: 64 << 20,
			FallbackBytes: 64 << 20, InventoryBytes: 8 << 20,
		},
	}
	for i, fileID := range fileIDs {
		payload := make([]byte, 257+i*31)
		for j := range payload {
			payload[j] = byte(1 + (i*29+j*17)%251)
		}
		offset := int64(17 + i*3)
		length := int64(101 + i*7)
		ref := ColumnAssetRef{
			Kind:       ColumnAssetKindTCS1TypedColumnPart,
			Namespace:  fixture.namespace,
			Generation: uint64(i + 1),
			PartID:     uint64(i + 11),
			FileID:     fileID,
			Offset:     offset,
			Length:     length,
			Checksum:   page.Checksum(payload[offset : offset+length]),
		}
		path, err := columnAssetSegmentPath(fixture.root, ref)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, payload, 0o600); err != nil {
			t.Fatal(err)
		}
		fixture.refs = append(fixture.refs, ref)
	}
	sort.Slice(fixture.refs, func(i, j int) bool {
		return compareColumnAssetRefs(fixture.refs[i], fixture.refs[j]) < 0
	})
	digest, err := digestTypedGraphServingBaseRefs(fixture.refs)
	if err != nil {
		t.Fatal(err)
	}
	fixture.key = columnVectorGraphSharedPreparedSearchKey{
		family:     columnVectorGraphSharedPreparedSearchKeyServing,
		db:         fixture.db,
		assetRoot:  fixture.root,
		collection: fixture.collection,
		namespace:  fixture.namespace,
		logical:    "test-serving-holder",
		refsDigest: digest,
		refsCount:  len(fixture.refs),
	}
	fixture.guardian = &ColumnAssetLifecyclePinSet{
		source: ColumnAssetLifecyclePinSourcePreparedQuery,
		owner:  "test-serving-holder-guardian",
		refs:   append([]ColumnAssetRef(nil), fixture.refs...),
	}
	fixture.set, err = newColumnServingSegmentLeaseSet(fixture.key, fixture.refs, fixture.guardian, fixture.ledger, fixture.limits)
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

func (f *columnServingLeaseTestFixture) rangeIdentity(ref ColumnAssetRef, relativeOffset, length int64) (mappedresource.Key, mappedresource.Scope) {
	key := mappedResourceKeyForColumnAssetRef(ref)
	key.Offset += relativeOffset
	key.Length = length
	key.Checksum = 0
	scope := mappedresource.Scope{
		Kind: mappedresource.ScopePreparedSearch, ID: "test-serving-holder",
		Collection: f.collection, Namespace: f.namespace, Generation: ref.Generation,
	}
	return key, scope
}

func installColumnServingLeaseHooks(t testing.TB, update func()) {
	t.Helper()
	columnServingSegmentLeaseHooks.Lock()
	if columnServingSegmentLeaseHooks.open != nil || columnServingSegmentLeaseHooks.stat != nil || columnServingSegmentLeaseHooks.identity != nil || columnServingSegmentLeaseHooks.mmap != nil || columnServingSegmentLeaseHooks.unmap != nil || columnServingSegmentLeaseHooks.readAt != nil || columnServingSegmentLeaseHooks.close != nil {
		columnServingSegmentLeaseHooks.Unlock()
		t.Fatal("serving segment lease hooks already installed")
	}
	columnServingSegmentLeaseHooks.Unlock()
	update()
	t.Cleanup(func() {
		columnServingSegmentLeaseHooks.Lock()
		columnServingSegmentLeaseHooks.open = nil
		columnServingSegmentLeaseHooks.stat = nil
		columnServingSegmentLeaseHooks.identity = nil
		columnServingSegmentLeaseHooks.mmap = nil
		columnServingSegmentLeaseHooks.unmap = nil
		columnServingSegmentLeaseHooks.readAt = nil
		columnServingSegmentLeaseHooks.close = nil
		columnServingSegmentLeaseHooks.Unlock()
	})
}

func requireColumnServingLedgerEmpty(t testing.TB, ledger *typedGraphPhysicalResourceLedger) {
	t.Helper()
	stats := ledger.snapshot()
	if !stats.empty() {
		t.Fatalf("serving physical ledger retained current state: %+v", stats)
	}
}

func waitForColumnServingCondition(t testing.TB, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for serving lease condition")
		}
		runtime.Gosched()
	}
}

func TestColumnServingSegmentLeaseExactAuthorityAndMappedPrefix(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("exact-prefix mmap is unsupported")
	}
	fixture := newColumnServingLeaseTestFixture(t, 1)
	ref := fixture.refs[0]
	var opens atomic.Int64
	installColumnServingLeaseHooks(t, func() {
		columnServingSegmentLeaseHooks.Lock()
		columnServingSegmentLeaseHooks.open = func(path string) (*os.File, error) {
			opens.Add(1)
			return os.Open(path)
		}
		columnServingSegmentLeaseHooks.Unlock()
	})

	// A current/suffix ref in the same physical file is not base authority.
	suffix := ref
	suffix.Generation++
	suffix.PartID++
	suffix.Offset += 3
	suffix.Length -= 3
	key, scope := fixture.rangeIdentity(suffix, 0, suffix.Length)
	if _, err := fixture.set.acquireRange(context.Background(), fixture.root, suffix, 0, suffix.Length, mappedresource.NewManager(), key, scope, mappedresource.AcquireOptions{PreferMapped: true, AllowHeapCopy: true}); !errors.Is(err, errColumnServingSegmentUnauthorized) {
		t.Fatalf("same-file suffix acquisition error=%v", err)
	}
	if got := opens.Load(); got != 0 {
		t.Fatalf("unauthorized same-file ref opened segment %d times", got)
	}

	relativeOffset, length := int64(9), int64(37)
	key, scope = fixture.rangeIdentity(ref, relativeOffset, length)
	handle, err := fixture.set.acquireRange(context.Background(), fixture.root, ref, relativeOffset, length, mappedresource.NewManager(), key, scope, mappedresource.AcquireOptions{PreferMapped: true, AllowHeapCopy: true})
	if err != nil {
		t.Fatal(err)
	}
	if handle.Source() != mappedresource.SourceMapped || int64(len(handle.Bytes())) != length {
		t.Fatalf("mapped handle source=%q bytes=%d", handle.Source(), len(handle.Bytes()))
	}
	segment := &fixture.set.segments[0]
	if int64(len(segment.mapped)) != segment.authorizedEnd || segment.authorizedEnd != ref.Offset+ref.Length {
		t.Fatalf("mapped prefix=%d authorized_end=%d ref_end=%d", len(segment.mapped), segment.authorizedEnd, ref.Offset+ref.Length)
	}
	stats := fixture.ledger.snapshot()
	if stats.descriptorsLive != 0 || stats.mappedBackings != 1 || stats.mappedBytes != segment.mappedCharge || stats.mappedBytesInFlight != 0 || stats.totalOpens != 1 || stats.totalCloses != 1 {
		t.Fatalf("mapped physical stats=%+v", stats)
	}
	if err := handle.Release(); err != nil {
		t.Fatal(err)
	}
	if after := fixture.ledger.snapshot(); after.mappedBackings != 1 || after.mappedBytes != segment.mappedCharge {
		t.Fatalf("logical release changed physical residency: %+v", after)
	}
	if err := fixture.set.Close(); err != nil {
		t.Fatal(err)
	}
	requireColumnServingLedgerEmpty(t, fixture.ledger)
}

func TestColumnServingSegmentLeaseParentFallbackAndMaterializerBorrow(t *testing.T) {
	fixture := newColumnServingLeaseTestFixture(t, 1)
	ref := fixture.refs[0]
	var reads atomic.Int64
	installColumnServingLeaseHooks(t, func() {
		columnServingSegmentLeaseHooks.Lock()
		columnServingSegmentLeaseHooks.mmap = func(*os.File, int64) ([]byte, error) {
			return nil, errColumnServingLeaseInjected
		}
		columnServingSegmentLeaseHooks.readAt = func(file *os.File, dst []byte, offset int64) (int, error) {
			reads.Add(1)
			return file.ReadAt(dst, offset)
		}
		columnServingSegmentLeaseHooks.Unlock()
	})

	type acquired struct {
		handle *columnServingRangeHandle
		err    error
	}
	results := make(chan acquired, 2)
	for _, interval := range [][2]int64{{0, 60}, {41, 37}} {
		interval := interval
		go func() {
			key, scope := fixture.rangeIdentity(ref, interval[0], interval[1])
			handle, err := fixture.set.acquireRange(context.Background(), fixture.root, ref, interval[0], interval[1], mappedresource.NewManager(), key, scope, mappedresource.AcquireOptions{PreferMapped: true, AllowHeapCopy: true})
			results <- acquired{handle: handle, err: err}
		}()
	}
	var handles []*columnServingRangeHandle
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		handles = append(handles, result.handle)
	}
	if got := reads.Load(); got != 1 {
		t.Fatalf("overlapping holder sections materialized parent %d times", got)
	}
	stats := fixture.ledger.snapshot()
	if stats.descriptorsLive != 1 || stats.fallbackSegments != 1 || stats.fallbackBackings != 1 || stats.fallbackBytes != ref.Length || stats.fallbackBytesInFlight != 0 {
		t.Fatalf("fallback physical stats=%+v", stats)
	}

	key, scope := fixture.rangeIdentity(ref, 7, 19)
	if err := fixture.set.withBorrowedRange(context.Background(), fixture.root, ref, 7, 19, key, scope, func(borrowed columnServingBorrowedRange) error {
		if borrowed.Bytes() != nil || borrowed.file == nil {
			t.Fatalf("fallback materializer borrow bytes=%d file=%v", len(borrowed.Bytes()), borrowed.file)
		}
		dst := make([]byte, 19)
		n, err := borrowed.ReadAt(dst)
		if err != nil || n != len(dst) {
			t.Fatalf("borrowed ReadAt n=%d err=%v", n, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := reads.Load(); got != 2 {
		t.Fatalf("materializer borrow read count=%d want=2", got)
	}
	afterBorrow := fixture.ledger.snapshot()
	if afterBorrow.fallbackBackings != 1 || afterBorrow.fallbackBytes != ref.Length {
		t.Fatalf("materializer borrow populated holder fallback: %+v", afterBorrow)
	}
	for _, handle := range handles {
		if err := handle.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if err := fixture.set.Close(); err != nil {
		t.Fatal(err)
	}
	requireColumnServingLedgerEmpty(t, fixture.ledger)
}

func TestColumnServingSegmentLeaseFallbackCancelAndRetry(t *testing.T) {
	fixture := newColumnServingLeaseTestFixture(t, 1)
	ref := fixture.refs[0]
	entered := make(chan struct{})
	release := make(chan struct{})
	var reads atomic.Int64
	var failFirst atomic.Bool
	failFirst.Store(true)
	installColumnServingLeaseHooks(t, func() {
		columnServingSegmentLeaseHooks.Lock()
		columnServingSegmentLeaseHooks.mmap = func(*os.File, int64) ([]byte, error) { return nil, errColumnServingLeaseInjected }
		columnServingSegmentLeaseHooks.readAt = func(file *os.File, dst []byte, offset int64) (int, error) {
			attempt := reads.Add(1)
			if attempt == 1 {
				close(entered)
				<-release
				if failFirst.Swap(false) {
					return 0, errColumnServingLeaseInjected
				}
			}
			return file.ReadAt(dst, offset)
		}
		columnServingSegmentLeaseHooks.Unlock()
	})
	key, scope := fixture.rangeIdentity(ref, 0, ref.Length)
	ctx, cancel := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, err := fixture.set.acquireRange(ctx, fixture.root, ref, 0, ref.Length, mappedresource.NewManager(), key, scope, mappedresource.AcquireOptions{PreferMapped: true, AllowHeapCopy: true})
		firstDone <- err
	}()
	<-entered
	cancel()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled first waiter error=%v", err)
	}
	close(release)
	waitForColumnServingCondition(t, func() bool {
		fixture.set.mu.Lock()
		defer fixture.set.mu.Unlock()
		return fixture.set.fallbacks[0].state == columnServingRefFallbackUnopened
	})
	if stats := fixture.ledger.snapshot(); stats.fallbackBytes != 0 || stats.fallbackBytesInFlight != 0 {
		t.Fatalf("failed fallback did not refund actual reservation: %+v", stats)
	}
	handle, err := fixture.set.acquireRange(context.Background(), fixture.root, ref, 0, ref.Length, mappedresource.NewManager(), key, scope, mappedresource.AcquireOptions{PreferMapped: true, AllowHeapCopy: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := reads.Load(); got != 2 {
		t.Fatalf("fallback retry reads=%d want=2", got)
	}
	if err := handle.Release(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.set.Close(); err != nil {
		t.Fatal(err)
	}
	requireColumnServingLedgerEmpty(t, fixture.ledger)
}

func TestColumnServingSegmentLeaseCloseWinsBuilderPublication(t *testing.T) {
	fixture := newColumnServingLeaseTestFixture(t, 1)
	ref := fixture.refs[0]
	entered := make(chan struct{})
	release := make(chan struct{})
	installColumnServingLeaseHooks(t, func() {
		columnServingSegmentLeaseHooks.Lock()
		columnServingSegmentLeaseHooks.mmap = func(file *os.File, size int64) ([]byte, error) {
			close(entered)
			<-release
			return mmapColumnPhysicalAssetFilePrefix(file, size)
		}
		columnServingSegmentLeaseHooks.Unlock()
	})
	key, scope := fixture.rangeIdentity(ref, 0, ref.Length)
	acquireDone := make(chan error, 1)
	go func() {
		_, err := fixture.set.acquireRange(context.Background(), fixture.root, ref, 0, ref.Length, mappedresource.NewManager(), key, scope, mappedresource.AcquireOptions{PreferMapped: true, AllowHeapCopy: true})
		acquireDone <- err
	}()
	<-entered
	closeDone := make(chan error, 1)
	go func() { closeDone <- fixture.set.Close() }()
	waitForColumnServingCondition(t, func() bool {
		fixture.set.mu.Lock()
		defer fixture.set.mu.Unlock()
		return fixture.set.closing
	})
	close(release)
	if err := <-acquireDone; !errors.Is(err, errColumnServingSegmentClosed) {
		t.Fatalf("acquire during close error=%v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	fixture.set.mu.Lock()
	state := fixture.set.segments[0].state
	fixture.set.mu.Unlock()
	if state != columnServingSegmentClosed {
		t.Fatalf("late builder published state=%d", state)
	}
	requireColumnServingLedgerEmpty(t, fixture.ledger)
	if err := fixture.set.Close(); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}
}

func TestColumnServingSegmentLeasePotentialAdmissionIsAtomic(t *testing.T) {
	fixture := newColumnServingLeaseTestFixture(t, 1, 2)
	if err := fixture.set.Close(); err != nil {
		t.Fatal(err)
	}
	requireColumnServingLedgerEmpty(t, fixture.ledger)
	var opens atomic.Int64
	installColumnServingLeaseHooks(t, func() {
		columnServingSegmentLeaseHooks.Lock()
		columnServingSegmentLeaseHooks.open = func(path string) (*os.File, error) {
			opens.Add(1)
			return os.Open(path)
		}
		columnServingSegmentLeaseHooks.Unlock()
	})
	guardian := &ColumnAssetLifecyclePinSet{source: ColumnAssetLifecyclePinSourcePreparedQuery, refs: append([]ColumnAssetRef(nil), fixture.refs...)}
	tiny := fixture.limits
	var mappedPotential int64
	for _, segment := range fixture.set.segments {
		mappedPotential += segment.mappedCharge
	}
	tiny.MappedBytes = mappedPotential - 1
	if _, err := newColumnServingSegmentLeaseSet(fixture.key, fixture.refs, guardian, fixture.ledger, tiny); !errors.Is(err, errTypedGraphOwnerBudget) {
		t.Fatalf("undersized full-potential admission error=%v", err)
	}
	if got := opens.Load(); got != 0 {
		t.Fatalf("OS acquisition happened before potential admission: opens=%d", got)
	}
	if err := guardian.Close(); err != nil {
		t.Fatal(err)
	}
	requireColumnServingLedgerEmpty(t, fixture.ledger)
}

func TestColumnServingSegmentLeaseSharedLedgerGenerationOverlap(t *testing.T) {
	first := newColumnServingLeaseTestFixture(t, 1)
	secondRoot := first.root
	secondRef := first.refs[0]
	secondRef.Generation++
	secondRef.PartID++
	secondRef.FileID = 2
	path, err := columnAssetSegmentPath(secondRoot, secondRef)
	if err != nil {
		t.Fatal(err)
	}
	raw := make([]byte, int(secondRef.Offset+secondRef.Length)+13)
	for i := range raw {
		raw[i] = byte(1 + i%251)
	}
	secondRef.Checksum = page.Checksum(raw[secondRef.Offset : secondRef.Offset+secondRef.Length])
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	secondRefs := []ColumnAssetRef{secondRef}
	digest, err := digestTypedGraphServingBaseRefs(secondRefs)
	if err != nil {
		t.Fatal(err)
	}
	secondKey := first.key
	secondKey.logical = "test-serving-holder-generation-2"
	secondKey.refsDigest, secondKey.refsCount = digest, 1
	secondGuardian := &ColumnAssetLifecyclePinSet{source: ColumnAssetLifecyclePinSourcePreparedQuery, refs: append([]ColumnAssetRef(nil), secondRefs...)}
	second, err := newColumnServingSegmentLeaseSet(secondKey, secondRefs, secondGuardian, first.ledger, first.limits)
	if err != nil {
		t.Fatal(err)
	}
	stats := first.ledger.snapshot()
	if stats.inventoryHolders != 2 || stats.potentialSegments != 2 || stats.potentialDescriptors != 2 || stats.potentialMappedBytes != first.set.reservation.mappedBytes+second.reservation.mappedBytes || stats.potentialFallbackBytes != first.set.reservation.fallbackBytes+second.reservation.fallbackBytes {
		t.Fatalf("overlap physical potential=%+v", stats)
	}
	if err := first.set.Close(); err != nil {
		t.Fatal(err)
	}
	stats = first.ledger.snapshot()
	if stats.inventoryHolders != 1 || stats.potentialSegments != second.reservation.segments || stats.potentialMappedBytes != second.reservation.mappedBytes || stats.potentialFallbackBytes != second.reservation.fallbackBytes {
		t.Fatalf("first close refunded second generation: %+v", stats)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	requireColumnServingLedgerEmpty(t, first.ledger)
}

func TestColumnServingSegmentLeaseReadAtContract(t *testing.T) {
	fixture := newColumnServingLeaseTestFixture(t, 1)
	ref := fixture.refs[0]
	installColumnServingLeaseHooks(t, func() {
		columnServingSegmentLeaseHooks.Lock()
		columnServingSegmentLeaseHooks.mmap = func(*os.File, int64) ([]byte, error) { return nil, errColumnServingLeaseInjected }
		columnServingSegmentLeaseHooks.Unlock()
	})
	key, scope := fixture.rangeIdentity(ref, 0, 3)
	err := fixture.set.withBorrowedRange(context.Background(), fixture.root, ref, 0, 3, key, scope, func(borrowed columnServingBorrowedRange) error {
		if _, err := borrowed.ReadAt(make([]byte, 2)); err == nil {
			return errors.New("borrowed ReadAt accepted a partial destination")
		}
		buf := make([]byte, 3)
		n, err := borrowed.ReadAt(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n != 3 {
			return io.ErrUnexpectedEOF
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.set.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestColumnServingPinContainsExactBase(t *testing.T) {
	fixture := newColumnServingLeaseTestFixture(t, 1, 2)
	if err := typedGraphServingPinContainsBase(fixture.refs, fixture.refs); err != nil {
		t.Fatal(err)
	}
	superset := append([]ColumnAssetRef(nil), fixture.refs...)
	extra := fixture.refs[len(fixture.refs)-1]
	extra.Generation += 10
	extra.PartID += 10
	extra.FileID += 10
	superset = append(superset, extra)
	if err := typedGraphServingPinContainsBase(fixture.refs, superset); err != nil {
		t.Fatalf("superset request pin: %v", err)
	}
	if err := typedGraphServingPinContainsBase(fixture.refs, fixture.refs[1:]); !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
		t.Fatalf("missing-base pin error=%v", err)
	}
	if err := fixture.set.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestColumnServingKeyIncludesExactClosureAndFamily(t *testing.T) {
	fixture := newColumnServingLeaseTestFixture(t, 1)
	changed := append([]ColumnAssetRef(nil), fixture.refs...)
	changed[0].Checksum++
	digest, err := digestTypedGraphServingBaseRefs(changed)
	if err != nil {
		t.Fatal(err)
	}
	changedKey := fixture.key
	changedKey.refsDigest = digest
	if fixture.key == changedKey {
		t.Fatal("serving key ignored exact base closure")
	}
	generic := genericColumnVectorGraphSharedPreparedSearchKey(fixture.key.logical)
	if generic == fixture.key || generic.family == fixture.key.family {
		t.Fatal("generic and serving key families alias")
	}
	if err := fixture.set.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestColumnServingSegmentLeaseValidationFailsBeforeOpen(t *testing.T) {
	fixture := newColumnServingLeaseTestFixture(t, 1)
	if err := fixture.set.Close(); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		edit func(*columnVectorGraphSharedPreparedSearchKey, []ColumnAssetRef, *ColumnAssetLifecyclePinSet)
	}{
		{name: "namespace", edit: func(_ *columnVectorGraphSharedPreparedSearchKey, refs []ColumnAssetRef, _ *ColumnAssetLifecyclePinSet) {
			refs[0].Namespace = "other"
		}},
		{name: "negative-offset", edit: func(_ *columnVectorGraphSharedPreparedSearchKey, refs []ColumnAssetRef, _ *ColumnAssetLifecyclePinSet) {
			refs[0].Offset = -1
		}},
		{name: "overflow", edit: func(_ *columnVectorGraphSharedPreparedSearchKey, refs []ColumnAssetRef, _ *ColumnAssetLifecyclePinSet) {
			refs[0].Offset, refs[0].Length = int64(^uint64(0)>>1), 2
		}},
		{name: "guardian", edit: func(_ *columnVectorGraphSharedPreparedSearchKey, _ []ColumnAssetRef, guardian *ColumnAssetLifecyclePinSet) {
			guardian.refs = nil
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			refs := append([]ColumnAssetRef(nil), fixture.refs...)
			key := fixture.key
			guardian := &ColumnAssetLifecyclePinSet{source: ColumnAssetLifecyclePinSourcePreparedQuery, refs: append([]ColumnAssetRef(nil), refs...)}
			tc.edit(&key, refs, guardian)
			if _, err := newColumnServingSegmentLeaseSet(key, refs, guardian, new(typedGraphPhysicalResourceLedger), fixture.limits); err == nil {
				t.Fatal("invalid lease set was admitted")
			}
		})
	}
}

func TestColumnServingSegmentLeaseDBCloseRetainsAndPrunesLateHolder(t *testing.T) {
	before := ColumnGraphClosedDBPhysicalQuarantineSnapshot()
	fixture := newColumnServingLeaseTestFixture(t, 1)
	ref := fixture.refs[0]
	installColumnServingLeaseHooks(t, func() {
		columnServingSegmentLeaseHooks.Lock()
		columnServingSegmentLeaseHooks.mmap = func(*os.File, int64) ([]byte, error) { return nil, errColumnServingLeaseInjected }
		columnServingSegmentLeaseHooks.Unlock()
	})
	key, scope := fixture.rangeIdentity(ref, 0, 1)
	if err := fixture.set.withBorrowedRange(context.Background(), fixture.root, ref, 0, 1, key, scope, func(columnServingBorrowedRange) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := fixture.ledger.closeForDB(fixture.db, fixture.root, fixture.collection); !errors.Is(err, errColumnServingSegmentCleanupRetained) {
		t.Fatalf("DB close with external holder error=%v", err)
	}
	during := ColumnGraphClosedDBPhysicalQuarantineSnapshot()
	if len(during) != len(before)+1 || !during[len(during)-1].Physical.ClosedDB || during[len(during)-1].Physical.DescriptorsLive != 1 {
		t.Fatalf("closed-DB quarantine before=%d during=%+v", len(before), during)
	}

	// The same path with a distinct DB identity is a disjoint key and ledger.
	reopenedKey := fixture.key
	reopenedKey.db = new(backenddb.DB)
	if reopenedKey == fixture.key {
		t.Fatal("reopened DB reused serving key identity")
	}
	reopenedGuardian := &ColumnAssetLifecyclePinSet{source: ColumnAssetLifecyclePinSourcePreparedQuery, refs: append([]ColumnAssetRef(nil), fixture.refs...)}
	reopened, err := newColumnServingSegmentLeaseSet(reopenedKey, fixture.refs, reopenedGuardian, new(typedGraphPhysicalResourceLedger), fixture.limits)
	if err != nil {
		t.Fatalf("closed DB quarantine blocked independent reopen: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.set.Close(); err != nil {
		t.Fatal(err)
	}
	after := ColumnGraphClosedDBPhysicalQuarantineSnapshot()
	if len(after) != len(before) {
		t.Fatalf("late holder close did not prune global record: before=%d after=%+v", len(before), after)
	}
	requireColumnServingLedgerEmpty(t, fixture.ledger)
}

func TestColumnServingSegmentLeaseRetainedCleanupRetry(t *testing.T) {
	fixture := newColumnServingLeaseTestFixture(t, 1)
	ref := fixture.refs[0]
	var allowCleanup atomic.Bool
	installColumnServingLeaseHooks(t, func() {
		columnServingSegmentLeaseHooks.Lock()
		columnServingSegmentLeaseHooks.stat = func(*os.File) (os.FileInfo, error) { return nil, errColumnServingLeaseInjected }
		columnServingSegmentLeaseHooks.close = func(file *os.File) columnServingSegmentCloseOutcome {
			if !allowCleanup.Load() {
				return columnServingSegmentCloseOutcome{retryable: true, err: errColumnServingLeaseInjected}
			}
			return columnServingSegmentCloseOutcome{confirmed: file.Close() == nil}
		}
		columnServingSegmentLeaseHooks.Unlock()
	})
	key, scope := fixture.rangeIdentity(ref, 0, 1)
	if err := fixture.set.withBorrowedRange(context.Background(), fixture.root, ref, 0, 1, key, scope, func(columnServingBorrowedRange) error { return nil }); !errors.Is(err, errColumnServingLeaseInjected) {
		t.Fatalf("stat/close build error=%v", err)
	}
	stats := fixture.ledger.snapshot()
	if stats.descriptorsLive != 1 || stats.unconfirmedDescriptors != 1 || stats.cleanupBackings != 1 || stats.inventoryHolders != 1 || stats.totalCloseAttempts != 1 || stats.totalCloses != 0 {
		t.Fatalf("retained partial acquisition stats=%+v", stats)
	}
	closeErr := fixture.set.Close()
	if !errors.Is(closeErr, errColumnServingSegmentCleanupRetained) {
		t.Fatalf("retained close error=%v", closeErr)
	}
	if err := fixture.ledger.quarantine(fixture.set, errColumnServingLeaseInjected, closeErr); !errors.Is(err, errColumnServingLeaseInjected) {
		t.Fatalf("quarantine error=%v", err)
	}
	stats = fixture.ledger.snapshot()
	if stats.cleanupQuarantines != 1 || stats.descriptorsLive != 1 || stats.unconfirmedDescriptors != 1 || stats.potentialDescriptors != 1 || stats.lastPrimaryError == "" || stats.lastCleanupError == "" {
		t.Fatalf("quarantine lost ownership/accounting: %+v", stats)
	}
	allowCleanup.Store(true)
	if err := fixture.ledger.retryQuarantinedPhysicalCleanup(); err != nil {
		t.Fatal(err)
	}
	requireColumnServingLedgerEmpty(t, fixture.ledger)
	final := fixture.ledger.snapshot()
	if final.totalCleanupRetries != 1 || final.totalCloses != 1 || final.totalCloseAttempts != 3 {
		t.Fatalf("cleanup cumulative counters=%+v", final)
	}
}

func TestColumnServingSegmentLeaseCompositeMapCleanupRetry(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mappedSize func(int64) int
	}{
		{name: "length-mismatch", mappedSize: func(size int64) int { return int(size - 1) }},
		{name: "mmap-success-close-failure", mappedSize: func(size int64) int { return int(size) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newColumnServingLeaseTestFixture(t, 1)
			ref := fixture.refs[0]
			var allowCleanup atomic.Bool
			installColumnServingLeaseHooks(t, func() {
				columnServingSegmentLeaseHooks.Lock()
				columnServingSegmentLeaseHooks.mmap = func(_ *os.File, size int64) ([]byte, error) {
					return make([]byte, tc.mappedSize(size)), nil
				}
				columnServingSegmentLeaseHooks.unmap = func([]byte) error {
					if allowCleanup.Load() {
						return nil
					}
					return errColumnServingLeaseInjected
				}
				columnServingSegmentLeaseHooks.close = func(file *os.File) columnServingSegmentCloseOutcome {
					if !allowCleanup.Load() {
						return columnServingSegmentCloseOutcome{retryable: true, err: errColumnServingLeaseInjected}
					}
					return columnServingSegmentCloseOutcome{confirmed: file.Close() == nil}
				}
				columnServingSegmentLeaseHooks.Unlock()
			})
			key, scope := fixture.rangeIdentity(ref, 0, 1)
			if err := fixture.set.withBorrowedRange(context.Background(), fixture.root, ref, 0, 1, key, scope, func(columnServingBorrowedRange) error { return nil }); err == nil {
				t.Fatal("composite mmap cleanup unexpectedly succeeded")
			}
			stats := fixture.ledger.snapshot()
			if stats.mappedBackings != 1 || stats.mappedBytes != fixture.set.segments[0].mappedCharge || stats.descriptorsLive != 1 || stats.unconfirmedDescriptors != 1 || stats.cleanupBackings != 1 {
				t.Fatalf("composite partial acquisition stats=%+v", stats)
			}
			closeErr := fixture.set.Close()
			if !errors.Is(closeErr, errColumnServingSegmentCleanupRetained) {
				t.Fatalf("composite retained close error=%v", closeErr)
			}
			if err := fixture.ledger.quarantine(fixture.set, nil, closeErr); err == nil {
				t.Fatal("quarantine did not report cleanup error")
			}
			allowCleanup.Store(true)
			if err := fixture.ledger.retryQuarantinedPhysicalCleanup(); err != nil {
				t.Fatal(err)
			}
			requireColumnServingLedgerEmpty(t, fixture.ledger)
		})
	}
}

func TestColumnServingSegmentLeasePermanentUnconfirmedDescriptor(t *testing.T) {
	fixture := newColumnServingLeaseTestFixture(t, 1)
	ref := fixture.refs[0]
	installColumnServingLeaseHooks(t, func() {
		columnServingSegmentLeaseHooks.Lock()
		columnServingSegmentLeaseHooks.mmap = func(*os.File, int64) ([]byte, error) { return nil, errColumnServingLeaseInjected }
		columnServingSegmentLeaseHooks.close = func(file *os.File) columnServingSegmentCloseOutcome {
			// Model a production Close error: the Go object no longer offers a
			// trustworthy retry, even if this test closes the real descriptor.
			_ = file.Close()
			return columnServingSegmentCloseOutcome{err: errColumnServingLeaseInjected}
		}
		columnServingSegmentLeaseHooks.Unlock()
	})
	key, scope := fixture.rangeIdentity(ref, 0, 1)
	if err := fixture.set.withBorrowedRange(context.Background(), fixture.root, ref, 0, 1, key, scope, func(columnServingBorrowedRange) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := fixture.set.Close(); !errors.Is(err, errColumnServingSegmentCleanupRetained) {
		t.Fatalf("unconfirmed fallback close error=%v", err)
	}
	stats := fixture.ledger.snapshot()
	if stats.descriptorsLive != 1 || stats.unconfirmedDescriptors != 1 || stats.totalCloseAttempts != 1 || stats.totalCloses != 0 || stats.potentialDescriptors != 1 {
		t.Fatalf("unconfirmed descriptor accounting=%+v", stats)
	}
	if err := fixture.set.retryCleanup(); !errors.Is(err, errColumnServingSegmentCleanupRetained) {
		t.Fatalf("unretryable descriptor cleanup error=%v", err)
	}
}

func TestColumnServingSegmentLeaseFileTooShortRetry(t *testing.T) {
	fixture := newColumnServingLeaseTestFixture(t, 1)
	ref := fixture.refs[0]
	path, err := columnAssetSegmentPath(fixture.root, ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, ref.Offset+ref.Length-1); err != nil {
		t.Fatal(err)
	}
	key, scope := fixture.rangeIdentity(ref, 0, 1)
	if err := fixture.set.withBorrowedRange(context.Background(), fixture.root, ref, 0, 1, key, scope, func(columnServingBorrowedRange) error { return nil }); err == nil {
		t.Fatal("short segment was accepted")
	}
	fixture.set.mu.Lock()
	state := fixture.set.segments[0].state
	fixture.set.mu.Unlock()
	if state != columnServingSegmentUnopened {
		t.Fatalf("confirmed short-file rollback state=%d", state)
	}
	if stats := fixture.ledger.snapshot(); stats.descriptorsLive != 0 || stats.mappedBackings != 0 || stats.descriptorsInFlight != 0 || stats.mappedBytesInFlight != 0 {
		t.Fatalf("confirmed short-file rollback stats=%+v", stats)
	}
	if err := fixture.set.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestColumnServingSegmentLeaseConcurrentFirstSegmentUse(t *testing.T) {
	fixture := newColumnServingLeaseTestFixture(t, 1)
	ref := fixture.refs[0]
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var opens atomic.Int64
	installColumnServingLeaseHooks(t, func() {
		columnServingSegmentLeaseHooks.Lock()
		columnServingSegmentLeaseHooks.open = func(path string) (*os.File, error) {
			opens.Add(1)
			once.Do(func() { close(entered) })
			<-release
			return os.Open(path)
		}
		columnServingSegmentLeaseHooks.Unlock()
	})
	results := make(chan error, 8)
	for range 8 {
		go func() {
			key, scope := fixture.rangeIdentity(ref, 0, 1)
			err := fixture.set.withBorrowedRange(context.Background(), fixture.root, ref, 0, 1, key, scope, func(columnServingBorrowedRange) error { return nil })
			results <- err
		}()
	}
	<-entered
	stats := fixture.ledger.snapshot()
	if stats.descriptorsInFlight != 1 || stats.mappedBytesInFlight != fixture.set.segments[0].mappedCharge {
		t.Fatalf("concurrent build reservations=%+v", stats)
	}
	close(release)
	for range 8 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if got := opens.Load(); got != 1 {
		t.Fatalf("concurrent first segment users opened %d backings", got)
	}
	if err := fixture.set.Close(); err != nil {
		t.Fatal(err)
	}
	requireColumnServingLedgerEmpty(t, fixture.ledger)
}
