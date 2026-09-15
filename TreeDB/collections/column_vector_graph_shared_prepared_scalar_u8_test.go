package collections

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/quantizedasset"
)

func TestColumnGraphLegacyScalarU8ZeroRowValidationCache(t *testing.T) {
	q := QuantizedVectorIndexDefinition{Name: "q", Codec: QuantizedVectorCodecScalarU8, Version: 1}
	newCache := func() *columnVectorGraphLegacyScalarU8ZeroRowValidationCache {
		cache := &columnVectorGraphLegacyScalarU8ZeroRowValidationCache{
			descriptors: []columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor{{definition: q}},
			entries:     make([]columnVectorGraphLegacyScalarU8ZeroRowValidationEntry, 1),
		}
		cache.cond = sync.NewCond(&cache.mu)
		return cache
	}

	t.Run("first_success_is_single_flight_and_cached", func(t *testing.T) {
		cache := newCache()
		started := make(chan struct{})
		release := make(chan struct{})
		results := make(chan error, 2)
		var loads atomic.Int32
		validate := func(columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor) error {
			if loads.Add(1) == 1 {
				close(started)
				<-release
			}
			return nil
		}
		go func() { results <- cache.validate(q.Name, q, validate) }()
		<-started
		go func() { results <- cache.validate(q.Name, q, validate) }()
		close(release)
		for range 2 {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		if got := loads.Load(); got != 1 {
			t.Fatalf("zero-row successful validations=%d want one", got)
		}
	})

	t.Run("failure_retries_but_success_stays_cached", func(t *testing.T) {
		cache := newCache()
		want := errors.New("temporary asset read failure")
		var loads atomic.Int32
		if err := cache.validate(q.Name, q, func(columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor) error {
			loads.Add(1)
			return want
		}); !errors.Is(err, want) {
			t.Fatalf("first validation err=%v want %v", err, want)
		}
		if err := cache.validate(q.Name, q, func(columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor) error {
			loads.Add(1)
			return nil
		}); err != nil {
			t.Fatalf("retry validation: %v", err)
		}
		if err := cache.validate(q.Name, q, func(columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor) error {
			t.Fatal("successful zero-row validation was not cached")
			return nil
		}); err != nil {
			t.Fatalf("cached validation: %v", err)
		}
		if got := loads.Load(); got != 2 {
			t.Fatalf("zero-row validation attempts=%d want two", got)
		}
	})
}

func TestColumnGraphSharedPreparedLegacyScalarU8AssetAttachment(t *testing.T) {
	requireTypedGraphPreparedHolderTest(t)
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-0", vector: []float32{0, 1, 0}},
		{id: "doc-1", vector: []float32{-1, 0, 0}},
		{id: "doc-2", vector: []float32{1, 0, 0}},
	}
	_, db, collection, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = db.Close() }()
	if _, err := collection.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	qName := def.QuantizedIndexes[0].Name
	callerNameBacking := strings.Repeat("x", 8192) + qName
	selectedName := callerNameBacking[len(callerNameBacking)-len(qName):]
	open := func() *columnVectorGraphPhysicalRowReader {
		t.Helper()
		reader, err := collection.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{SkipQuantizedAssets: true})
		if err != nil {
			t.Fatalf("open reader: %v", err)
		}
		return reader
	}
	first := open()
	second := open()
	if first.sharedPreparedSearch == nil || first.sharedPreparedSearch.holder == nil || second.sharedPreparedSearch == nil || second.sharedPreparedSearch.holder != first.sharedPreparedSearch.holder {
		_ = first.Close()
		_ = second.Close()
		t.Fatal("readers did not retain one shared prepared holder")
	}
	if len(first.quantizedAssetStatus) != 0 || len(second.quantizedAssetStatus) != 0 {
		_ = first.Close()
		_ = second.Close()
		t.Fatal("SkipQuantizedAssets unexpectedly loaded a code plane")
	}
	if err := collection.requestAndAttachColumnVectorGraphSharedPreparedLegacyScalarU8Asset(first, selectedName); err != nil {
		_ = first.Close()
		_ = second.Close()
		t.Fatalf("first shared scalar_u8 attach: %v", err)
	}
	firstStatus, ok := first.quantizedAssetStatus[qName]
	if !ok || firstStatus.resource == nil || firstStatus.ownsResource || firstStatus.Prepared == nil {
		_ = first.Close()
		_ = second.Close()
		t.Fatalf("first attached status=%+v", firstStatus)
	}
	resource := firstStatus.resource
	holder := first.sharedPreparedSearch.holder
	if len(holder.legacyScalarU8Assets) != 1 {
		_ = first.Close()
		_ = second.Close()
		t.Fatalf("holder legacy assets=%d want one", len(holder.legacyScalarU8Assets))
	}
	canonicalName := holder.legacyScalarU8Assets[0].definition.Name
	var storedName string
	for key := range first.quantizedAssetStatus {
		storedName = key
	}
	if storedName != canonicalName || unsafe.StringData(storedName) != unsafe.StringData(canonicalName) {
		_ = first.Close()
		_ = second.Close()
		t.Fatalf("retained map key=%q is not canonical holder name=%q", storedName, canonicalName)
	}
	if unsafe.StringData(storedName) == unsafe.StringData(selectedName) {
		_ = first.Close()
		_ = second.Close()
		t.Fatal("retained caller-provided name backing")
	}
	holder.legacyScalarU8Mu.Lock()
	var entry *columnVectorGraphSharedPreparedLegacyScalarU8AssetEntry
	for _, candidate := range holder.legacyScalarU8Entries {
		if candidate != nil && candidate.descriptor.definition.Name == qName {
			entry = candidate
			break
		}
	}
	stored := columnVectorGraphQuantizedAssetLoadStatus{}
	if entry != nil {
		stored = entry.status
	}
	holder.legacyScalarU8Mu.Unlock()
	if entry == nil || stored.resource != resource || !stored.ownsResource || stored.Prepared == nil || stored.Err != nil {
		_ = first.Close()
		_ = second.Close()
		t.Fatalf("holder did not retain the sole scalar_u8 resource: %+v", stored)
	}
	if err := collection.requestAndAttachColumnVectorGraphSharedPreparedLegacyScalarU8Asset(second, qName); err != nil {
		_ = first.Close()
		_ = second.Close()
		t.Fatalf("second shared scalar_u8 attach: %v", err)
	}
	secondStatus := second.quantizedAssetStatus[qName]
	if secondStatus.resource != resource || secondStatus.ownsResource || secondStatus.Prepared != firstStatus.Prepared {
		_ = first.Close()
		_ = second.Close()
		t.Fatalf("second attach did not reuse holder status: first=%+v second=%+v", firstStatus, secondStatus)
	}
	if err := first.Close(); err != nil {
		_ = second.Close()
		t.Fatalf("first Close: %v", err)
	}
	resource.mu.Lock()
	closedAfterFirst := resource.closed
	resource.mu.Unlock()
	if closedAfterFirst {
		_ = second.Close()
		t.Fatal("first attached reader closed the holder-owned resource")
	}
	if err := second.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	resource.mu.Lock()
	closedAfterLast := resource.closed
	resource.mu.Unlock()
	if !closedAfterLast {
		t.Fatal("last shared ref did not close the holder-owned resource")
	}
}

func TestColumnGraphSharedPreparedLegacyScalarU8AssetFailureRetries(t *testing.T) {
	holder := &columnVectorGraphSharedPreparedSearch{}
	descriptor := columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor{
		definition: QuantizedVectorIndexDefinition{Name: "q", Codec: QuantizedVectorCodecScalarU8, Version: 1},
	}
	transient := errors.New("temporary scalar plane read failure")
	resource := &columnVectorGraphQuantizedAssetResource{}
	var loads atomic.Int32
	load := func() (columnVectorGraphQuantizedAssetLoadStatus, error) {
		switch loads.Add(1) {
		case 1:
			return columnVectorGraphQuantizedAssetLoadStatus{}, transient
		case 2:
			// A nil-error malformed result must be retryable too; otherwise a
			// broken loader seam would cache a permanently not-ready entry.
			return columnVectorGraphQuantizedAssetLoadStatus{}, nil
		default:
			return columnVectorGraphQuantizedAssetLoadStatus{
				Definition:   descriptor.definition,
				Asset:        descriptor.assets.Codes,
				Prepared:     &quantizedasset.Prepared{},
				resource:     resource,
				ownsResource: true,
			}, nil
		}
	}
	assertForgotten := func(label string) {
		t.Helper()
		holder.legacyScalarU8Mu.Lock()
		defer holder.legacyScalarU8Mu.Unlock()
		if got := len(holder.legacyScalarU8Entries); got != 0 {
			t.Fatalf("%s retained entries=%d want zero", label, got)
		}
		for _, entry := range holder.legacyScalarU8Entries[:cap(holder.legacyScalarU8Entries)] {
			if entry != nil {
				t.Fatalf("%s retained a failed entry in backing storage", label)
			}
		}
	}

	if _, err := holder.acquireLegacyScalarU8Asset(descriptor, load); !errors.Is(err, transient) {
		t.Fatalf("first load err=%v want %v", err, transient)
	}
	assertForgotten("transient failure")
	if _, err := holder.acquireLegacyScalarU8Asset(descriptor, load); !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) {
		t.Fatalf("malformed status err=%v want invalid asset", err)
	}
	assertForgotten("malformed status")
	status, err := holder.acquireLegacyScalarU8Asset(descriptor, load)
	if err != nil {
		t.Fatalf("successful retry: %v", err)
	}
	if !columnVectorGraphSharedPreparedLegacyScalarU8AssetStatusReady(status, descriptor) || status.resource != resource {
		t.Fatalf("successful retry status=%+v", status)
	}
	if got := loads.Load(); got != 3 {
		t.Fatalf("loads=%d want three", got)
	}
	if err := holder.close(); err != nil {
		t.Fatalf("holder close: %v", err)
	}
	resource.mu.Lock()
	closed := resource.closed
	resource.mu.Unlock()
	if !closed {
		t.Fatal("successful retry resource was not closed")
	}
}

func TestColumnGraphSharedPreparedLegacyScalarU8AssetFirstLoaderCancellationClosesBeforeRetry(t *testing.T) {
	holder := &columnVectorGraphSharedPreparedSearch{}
	descriptor := columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor{
		definition: QuantizedVectorIndexDefinition{Name: "q", Codec: QuantizedVectorCodecScalarU8, Version: 1},
	}
	ctx, cancel := context.WithCancel(context.Background())
	firstResource := &columnVectorGraphQuantizedAssetResource{}
	_, err := holder.acquireLegacyScalarU8AssetWithContext(ctx, descriptor, func() (columnVectorGraphQuantizedAssetLoadStatus, error) {
		// Model cancellation racing immediately after the physical resource and
		// prepared view were constructed. The single-flight seam owns the
		// completed status until it either publishes or closes it.
		cancel()
		return columnVectorGraphQuantizedAssetLoadStatus{
			Definition:   descriptor.definition,
			Asset:        descriptor.assets.Codes,
			Prepared:     &quantizedasset.Prepared{},
			resource:     firstResource,
			ownsResource: true,
		}, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled first loader err=%v want context.Canceled", err)
	}
	firstResource.mu.Lock()
	firstClosed := firstResource.closed
	firstResource.mu.Unlock()
	if !firstClosed {
		t.Fatal("canceled first loader leaked its completed resource")
	}

	holder.legacyScalarU8Mu.Lock()
	entries := len(holder.legacyScalarU8Entries)
	var retained bool
	for _, entry := range holder.legacyScalarU8Entries[:cap(holder.legacyScalarU8Entries)] {
		retained = retained || entry != nil
	}
	holder.legacyScalarU8Mu.Unlock()
	if entries != 0 || retained {
		t.Fatalf("canceled first loader retained entry: len=%d backing_retained=%v", entries, retained)
	}

	secondResource := &columnVectorGraphQuantizedAssetResource{}
	status, err := holder.acquireLegacyScalarU8AssetWithContext(context.Background(), descriptor, func() (columnVectorGraphQuantizedAssetLoadStatus, error) {
		return columnVectorGraphQuantizedAssetLoadStatus{
			Definition:   descriptor.definition,
			Asset:        descriptor.assets.Codes,
			Prepared:     &quantizedasset.Prepared{},
			resource:     secondResource,
			ownsResource: true,
		}, nil
	})
	if err != nil {
		t.Fatalf("retry after canceled first loader: %v", err)
	}
	if status.resource != secondResource || !status.ownsResource {
		t.Fatalf("retry status=%+v", status)
	}
	if err := holder.close(); err != nil {
		t.Fatalf("holder close: %v", err)
	}
	secondResource.mu.Lock()
	secondClosed := secondResource.closed
	secondResource.mu.Unlock()
	if !secondClosed {
		t.Fatal("holder close did not release successful retry resource")
	}
}

func TestColumnGraphSharedPreparedLegacyScalarU8AssetCloseDuringLoad(t *testing.T) {
	holder := &columnVectorGraphSharedPreparedSearch{}
	descriptor := columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor{
		definition: QuantizedVectorIndexDefinition{Name: "q", Codec: QuantizedVectorCodecScalarU8, Version: 1},
	}
	started := make(chan struct{})
	release := make(chan struct{})
	resource := &columnVectorGraphQuantizedAssetResource{}
	acquireDone := make(chan error, 1)
	go func() {
		_, err := holder.acquireLegacyScalarU8Asset(descriptor, func() (columnVectorGraphQuantizedAssetLoadStatus, error) {
			close(started)
			<-release
			return columnVectorGraphQuantizedAssetLoadStatus{Definition: descriptor.definition, Asset: descriptor.assets.Codes, resource: resource, ownsResource: true}, nil
		})
		acquireDone <- err
	}()
	<-started
	closeDone := make(chan error, 1)
	go func() { closeDone <- holder.close() }()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		holder.legacyScalarU8Mu.Lock()
		closed := holder.legacyScalarU8Closed
		holder.legacyScalarU8Mu.Unlock()
		if closed {
			break
		}
		select {
		case <-deadline.C:
			close(release)
			<-acquireDone
			<-closeDone
			t.Fatal("holder close did not mark scalar-u8 entries closed")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	close(release)
	if err := <-acquireDone; !errors.Is(err, errColumnVectorGraphQuantizedAssetClosed) {
		<-closeDone
		t.Fatalf("acquire error=%v want closed holder", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("holder close: %v", err)
	}
	resource.mu.Lock()
	closed := resource.closed
	resource.mu.Unlock()
	if !closed {
		t.Fatal("closed-during-load resource leaked")
	}
}

func TestColumnGraphSharedPreparedLegacyScalarU8AssetWaiterCancellation(t *testing.T) {
	holder := &columnVectorGraphSharedPreparedSearch{}
	descriptor := columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor{
		definition: QuantizedVectorIndexDefinition{Name: "q", Codec: QuantizedVectorCodecScalarU8, Version: 1},
	}
	started := make(chan struct{})
	release := make(chan struct{})
	resource := &columnVectorGraphQuantizedAssetResource{}
	load := func() (columnVectorGraphQuantizedAssetLoadStatus, error) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-release
		return columnVectorGraphQuantizedAssetLoadStatus{
			Definition:   descriptor.definition,
			Asset:        descriptor.assets.Codes,
			Prepared:     &quantizedasset.Prepared{},
			resource:     resource,
			ownsResource: true,
		}, nil
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := holder.acquireLegacyScalarU8AssetWithContext(context.Background(), descriptor, load)
		firstDone <- err
	}()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		_, err := holder.acquireLegacyScalarU8AssetWithContext(ctx, descriptor, load)
		secondDone <- err
	}()
	cancel()
	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled waiter err=%v want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled waiter remained blocked behind first asset load")
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first asset load: %v", err)
	}
	if err := holder.close(); err != nil {
		t.Fatalf("holder close: %v", err)
	}
}

func TestColumnGraphLegacyScalarU8ZeroRowValidationWaiterCancellation(t *testing.T) {
	q := QuantizedVectorIndexDefinition{Name: "q", Codec: QuantizedVectorCodecScalarU8, Version: 1}
	cache := &columnVectorGraphLegacyScalarU8ZeroRowValidationCache{
		descriptors: []columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor{{definition: q}},
		entries:     make([]columnVectorGraphLegacyScalarU8ZeroRowValidationEntry, 1),
	}
	cache.cond = sync.NewCond(&cache.mu)
	started := make(chan struct{})
	release := make(chan struct{})
	validate := func(columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor) error {
		close(started)
		<-release
		return nil
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- cache.validateWithContext(context.Background(), q.Name, q, validate) }()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() { secondDone <- cache.validateWithContext(ctx, q.Name, q, validate) }()
	cancel()
	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled zero-row waiter err=%v want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled zero-row waiter remained blocked behind validation")
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first zero-row validation: %v", err)
	}
}
