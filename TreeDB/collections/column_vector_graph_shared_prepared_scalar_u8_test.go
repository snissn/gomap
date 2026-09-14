package collections

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
	if err := collection.requestAndAttachColumnVectorGraphSharedPreparedLegacyScalarU8Asset(first, qName); err != nil {
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
