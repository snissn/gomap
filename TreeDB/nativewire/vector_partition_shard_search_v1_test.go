package nativewire

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

const vectorPartitionShardSearchDigestTestV1 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type fakeVectorPartitionReadCoordinatorV1 struct {
	mu       sync.Mutex
	calls    []raftcluster.ReadIndexBarrier
	proof    raftcluster.ReadIndexProof
	progress raftcluster.AppliedProgress
	err      error
	wait     bool
}

func (f *fakeVectorPartitionReadCoordinatorV1) CoordinateRoutedReadIndex(ctx context.Context, target raftcluster.ReadIndexBarrier) (raftcluster.ReadIndexProof, raftcluster.AppliedProgress, error) {
	f.mu.Lock()
	f.calls = append(f.calls, target)
	wait := f.wait
	err := f.err
	proof := f.proof
	progress := f.progress
	f.mu.Unlock()
	if wait {
		<-ctx.Done()
		return raftcluster.ReadIndexProof{}, raftcluster.AppliedProgress{}, ctx.Err()
	}
	return proof, progress, err
}

func (f *fakeVectorPartitionReadCoordinatorV1) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

type fakeVectorPartitionGenerationSourceV1 struct {
	mu                      sync.Mutex
	manifest                collections.VectorPartitionManifestV1
	assets                  map[uint32]collections.VectorPartitionSearchAssetV1
	openErr                 map[uint32]error
	openLease               map[uint32]*VectorPartitionPartitionSearchLeaseV1
	pins                    int
	releases                int
	opens                   int
	searchers               map[uint32]*collections.VectorPartitionLocalSearcherV1
	pinErr                  error
	nilPin                  bool
	pinStarted, pinContinue chan struct{}
}

func (f *fakeVectorPartitionGenerationSourceV1) PinVectorPartitionGenerationV1(ctx context.Context, index string, generation uint64) (VectorPartitionPinnedGenerationV1, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.pinStarted != nil {
		close(f.pinStarted)
		select {
		case <-f.pinContinue:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pinErr != nil {
		return nil, f.pinErr
	}
	f.pins++
	if f.nilPin {
		return nil, nil
	}
	return &fakeVectorPartitionPinnedGenerationV1{
		source:   f,
		manifest: pinnedVectorPartitionManifestV1(f.manifest),
	}, nil
}

type fakeVectorPartitionPinnedGenerationV1 struct {
	source   *fakeVectorPartitionGenerationSourceV1
	manifest VectorPartitionPinnedManifestV1
	once     sync.Once
}

func (p *fakeVectorPartitionPinnedGenerationV1) Manifest() VectorPartitionPinnedManifestV1 {
	return clonePinnedVectorPartitionManifestV1(p.manifest)
}

func (p *fakeVectorPartitionPinnedGenerationV1) OpenPartition(ctx context.Context, partition uint32) (*VectorPartitionPartitionSearchLeaseV1, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.source.mu.Lock()
	defer p.source.mu.Unlock()
	p.source.opens++
	if err := p.source.openErr[partition]; err != nil {
		return nil, err
	}
	if lease := p.source.openLease[partition]; lease != nil {
		return lease, nil
	}
	asset, ok := p.source.assets[partition]
	if !ok {
		return nil, collections.ErrVectorPartitionSearchUnavailable
	}
	searcher, err := collections.OpenVectorPartitionLocalSearcherV1(asset)
	if err == nil {
		if p.source.searchers == nil {
			p.source.searchers = make(map[uint32]*collections.VectorPartitionLocalSearcherV1)
		}
		p.source.searchers[partition] = searcher
	}
	if err != nil {
		return nil, err
	}
	return NewVectorPartitionPartitionSearchLeaseV1(searcher, false, searcher.Close)
}

func (p *fakeVectorPartitionPinnedGenerationV1) Close() error {
	p.once.Do(func() {
		p.source.mu.Lock()
		p.source.releases++
		p.source.mu.Unlock()
	})
	return nil
}

func (f *fakeVectorPartitionGenerationSourceV1) counts() (pins, releases, opens int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pins, f.releases, f.opens
}

func TestVectorPartitionSearchLeaseClosePreservesErrorV1(t *testing.T) {
	searcher, err := collections.OpenVectorPartitionLocalSearcherV1(
		vectorPartitionShardSearchAssetTestV1(0, []string{"a"}, [][]float32{{1, 0}}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = searcher.Close() })
	closeFailure := errors.New("injected close failure")
	closeCalls := 0
	lease, err := NewVectorPartitionPartitionSearchLeaseV1(searcher, false, func() error {
		closeCalls++
		return closeFailure
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); !errors.Is(err, closeFailure) {
		t.Fatalf("first close err=%v", err)
	}
	if err := lease.Close(); !errors.Is(err, closeFailure) {
		t.Fatalf("idempotent close err=%v", err)
	}
	if closeCalls != 1 {
		t.Fatalf("close calls=%d", closeCalls)
	}
}

func TestCollectionVectorPartitionGenerationCacheInvalidationWaitsForRequestLeaseV1(t *testing.T) {
	searcher, err := collections.OpenVectorPartitionLocalSearcherV1(
		vectorPartitionShardSearchAssetTestV1(0, []string{"a"}, [][]float32{{1, 0}}),
	)
	if err != nil {
		t.Fatal(err)
	}
	key := collectionVectorPartitionGenerationKeyV1{index: "embedding", generation: 7}
	entry := &collectionVectorPartitionGenerationCacheV1{
		index:      key.index,
		generation: key.generation,
		refs:       1,
		searchers:  map[uint32]*collections.VectorPartitionLocalSearcherV1{0: searcher},
		opening:    make(map[uint32]*collectionVectorPartitionSearchLoadV1),
	}
	source := &CollectionVectorPartitionGenerationSourceV1{
		Collection: new(collections.Collection),
		entries:    map[collectionVectorPartitionGenerationKeyV1]*collectionVectorPartitionGenerationCacheV1{key: entry},
	}
	pinned := newCollectionVectorPartitionGenerationLeaseV1(source, entry)

	if err := source.InvalidateVectorPartitionGenerationV1(key.index, key.generation); err != nil {
		t.Fatal(err)
	}
	if status := searcher.Status(); status.Retired {
		t.Fatalf("invalidation retired an in-flight request searcher: %+v", status)
	}
	partition, err := pinned.OpenPartition(context.Background(), 0)
	if err != nil {
		t.Fatalf("in-flight pinned open: %v", err)
	}
	if !partition.CacheHit {
		t.Fatal("expected cached partition lease")
	}
	got, _, err := partition.Searcher.SearchWithOptionsV1(context.Background(), []float32{1, 0}, collections.VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 1})
	if err != nil || len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("search=%+v err=%v", got, err)
	}
	if err := partition.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := source.PinVectorPartitionGenerationV1(context.Background(), key.index, key.generation); !errors.Is(err, ErrVectorPartitionShardSearchGenerationMismatch) {
		t.Fatalf("stale generation pin err=%v", err)
	}
	if err := pinned.Close(); err != nil {
		t.Fatal(err)
	}
	if status := searcher.Status(); !status.Retired || status.ActivePins != 0 {
		t.Fatalf("last request release did not retire cached searcher: %+v", status)
	}
	if err := pinned.Close(); err != nil {
		t.Fatalf("idempotent pinned close: %v", err)
	}
	stats := source.Stats()
	if stats.Invalidations != 1 || stats.PartitionHits != 1 {
		t.Fatalf("cache stats=%+v", stats)
	}
}

func TestCollectionVectorPartitionGenerationCacheCloseLetsPinnedRequestsDrainV1(t *testing.T) {
	searcher, err := collections.OpenVectorPartitionLocalSearcherV1(
		vectorPartitionShardSearchAssetTestV1(0, []string{"a"}, [][]float32{{1, 0}}),
	)
	if err != nil {
		t.Fatal(err)
	}
	key := collectionVectorPartitionGenerationKeyV1{index: "embedding", generation: 7}
	entry := &collectionVectorPartitionGenerationCacheV1{
		index:      key.index,
		generation: key.generation,
		refs:       1,
		searchers:  map[uint32]*collections.VectorPartitionLocalSearcherV1{0: searcher},
		opening:    make(map[uint32]*collectionVectorPartitionSearchLoadV1),
	}
	source := &CollectionVectorPartitionGenerationSourceV1{
		Collection: new(collections.Collection),
		entries:    map[collectionVectorPartitionGenerationKeyV1]*collectionVectorPartitionGenerationCacheV1{key: entry},
	}
	pinned := newCollectionVectorPartitionGenerationLeaseV1(source, entry)
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	partition, err := pinned.OpenPartition(context.Background(), 0)
	if err != nil {
		t.Fatalf("pinned request could not drain after source close: %v", err)
	}
	if _, err := source.PinVectorPartitionGenerationV1(context.Background(), key.index, key.generation); !errors.Is(err, ErrVectorPartitionShardSearchAssetsUnavailable) {
		t.Fatalf("new pin after close err=%v", err)
	}
	if err := partition.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pinned.Close(); err != nil {
		t.Fatal(err)
	}
	if status := searcher.Status(); !status.Retired {
		t.Fatalf("drained source did not close searcher: %+v", status)
	}
}

func TestCollectionVectorPartitionGenerationCacheRevalidatesWarmHitV1(t *testing.T) {
	searcher, err := collections.OpenVectorPartitionLocalSearcherV1(
		vectorPartitionShardSearchAssetTestV1(0, []string{"a"}, [][]float32{{1, 0}}),
	)
	if err != nil {
		t.Fatal(err)
	}
	key := collectionVectorPartitionGenerationKeyV1{index: "embedding", generation: 7}
	entry := &collectionVectorPartitionGenerationCacheV1{
		index:      key.index,
		generation: key.generation,
		manifest:   VectorPartitionPinnedManifestV1{Generation: key.generation},
		searchers:  map[uint32]*collections.VectorPartitionLocalSearcherV1{0: searcher},
		opening:    make(map[uint32]*collectionVectorPartitionSearchLoadV1),
	}
	source := &CollectionVectorPartitionGenerationSourceV1{
		Collection: new(collections.Collection),
		entries:    map[collectionVectorPartitionGenerationKeyV1]*collectionVectorPartitionGenerationCacheV1{key: entry},
		testValidateActive: func(context.Context, collectionVectorPartitionGenerationKeyV1) error {
			return errors.New("generation replaced")
		},
	}
	if _, err := source.PinVectorPartitionGenerationV1(context.Background(), key.index, key.generation); !errors.Is(err, ErrVectorPartitionShardSearchGenerationMismatch) {
		t.Fatalf("warm stale generation err=%v", err)
	}
	if !searcher.Status().Retired {
		t.Fatal("stale warm generation remained cached and open")
	}
	if _, ok := source.invalidated[key]; !ok {
		t.Fatal("stale warm generation was not permanently invalidated")
	}
}

type recordingVectorPartitionReplicatedLifecycleAuthorityV1 struct {
	calls          int
	args           []any
	readySetDigest string
	err            error
}

func TestVectorPartitionReplicatedLifecycleValidationErrorPreservesCancellationV1(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want error
	}{
		{name: "authority canceled", ctx: context.Background(), err: context.Canceled, want: context.Canceled},
		{name: "authority deadline", ctx: context.Background(), err: context.DeadlineExceeded, want: context.DeadlineExceeded},
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	tests = append(tests, struct {
		name string
		ctx  context.Context
		err  error
		want error
	}{name: "caller canceled", ctx: canceled, err: errors.New("authority unavailable"), want: context.Canceled})
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := vectorPartitionReplicatedLifecycleValidationErrorV1(test.ctx, test.err); !errors.Is(got, test.want) || errors.Is(got, ErrVectorPartitionShardSearchGenerationMismatch) {
				t.Fatalf("validation error=%v want direct %v", got, test.want)
			}
		})
	}
	plain := errors.New("generation replaced")
	if got := vectorPartitionReplicatedLifecycleValidationErrorV1(context.Background(), plain); !errors.Is(got, ErrVectorPartitionShardSearchGenerationMismatch) || errors.Is(got, plain) {
		t.Fatalf("ordinary validation error=%v", got)
	}
}

func (a *recordingVectorPartitionReplicatedLifecycleAuthorityV1) ValidateVectorPartitionGenerationSearchV1(
	_ context.Context,
	collection raftplacement.CollectionRefV1,
	index string,
	generation uint64,
	indexDigest string,
	sourceGeneration uint64,
	sourceChecksum uint64,
	sourceSchemaHash uint64,
	sourceRowCount uint64,
) (string, error) {
	a.calls++
	a.args = []any{collection, index, generation, indexDigest, sourceGeneration, sourceChecksum, sourceSchemaHash, sourceRowCount}
	readySetDigest := a.readySetDigest
	if readySetDigest == "" {
		readySetDigest = strings.Repeat("b", 64)
	}
	return readySetDigest, a.err
}

func TestCollectionVectorPartitionGenerationCacheUsesReplicatedAuthorityOnWarmHitV1(t *testing.T) {
	key := collectionVectorPartitionGenerationKeyV1{index: "embedding", generation: 7}
	manifest := VectorPartitionPinnedManifestV1{
		Collection: "users", IndexName: key.index, Generation: key.generation,
		IndexDefinitionDigest: strings.Repeat("a", 64),
		SourceGeneration:      3,
		SourceChecksum:        4,
		SourceSchemaHash:      5,
		SourceRowCount:        6,
		ReadySetDigest:        strings.Repeat("b", 64),
	}
	authority := &recordingVectorPartitionReplicatedLifecycleAuthorityV1{err: errors.New("generation invalidated")}
	entry := &collectionVectorPartitionGenerationCacheV1{
		index: key.index, generation: key.generation, manifest: manifest,
		searchers: make(map[uint32]*collections.VectorPartitionLocalSearcherV1),
		opening:   make(map[uint32]*collectionVectorPartitionSearchLoadV1),
	}
	source := &CollectionVectorPartitionGenerationSourceV1{
		Collection:           new(collections.Collection),
		replicatedCollection: raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "users"},
		replicatedLifecycle:  authority,
		entries:              map[collectionVectorPartitionGenerationKeyV1]*collectionVectorPartitionGenerationCacheV1{key: entry},
	}

	if _, err := source.PinVectorPartitionGenerationV1(t.Context(), key.index, key.generation); !errors.Is(err, ErrVectorPartitionShardSearchGenerationMismatch) {
		t.Fatalf("warm invalidated generation err=%v", err)
	}
	if authority.calls != 1 {
		t.Fatalf("replicated authority calls=%d want 1", authority.calls)
	}
	want := []any{raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "users"}, "embedding", uint64(7), strings.Repeat("a", 64), uint64(3), uint64(4), uint64(5), uint64(6)}
	if !reflect.DeepEqual(authority.args, want) {
		t.Fatalf("replicated authority args=%#v want %#v", authority.args, want)
	}
	if _, ok := source.invalidated[key]; !ok {
		t.Fatal("replicated invalidation did not permanently evict stale cache entry")
	}
}

func TestCollectionVectorPartitionGenerationCacheRejectsChangedReplicatedReadySetV1(t *testing.T) {
	key := collectionVectorPartitionGenerationKeyV1{index: "embedding", generation: 7}
	manifest := VectorPartitionPinnedManifestV1{
		Collection: "users", IndexName: key.index, Generation: key.generation,
		IndexDefinitionDigest: strings.Repeat("a", 64),
		SourceGeneration:      3,
		SourceChecksum:        4,
		SourceSchemaHash:      5,
		SourceRowCount:        6,
		ReadySetDigest:        strings.Repeat("b", 64),
	}
	entry := &collectionVectorPartitionGenerationCacheV1{
		index: key.index, generation: key.generation, manifest: manifest,
		searchers: make(map[uint32]*collections.VectorPartitionLocalSearcherV1),
		opening:   make(map[uint32]*collectionVectorPartitionSearchLoadV1),
	}
	source := &CollectionVectorPartitionGenerationSourceV1{
		Collection:           new(collections.Collection),
		replicatedCollection: raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "users"},
		replicatedLifecycle:  &recordingVectorPartitionReplicatedLifecycleAuthorityV1{readySetDigest: strings.Repeat("c", 64)},
		entries:              map[collectionVectorPartitionGenerationKeyV1]*collectionVectorPartitionGenerationCacheV1{key: entry},
	}
	if _, err := source.PinVectorPartitionGenerationV1(t.Context(), key.index, key.generation); !errors.Is(err, ErrVectorPartitionShardSearchGenerationMismatch) {
		t.Fatalf("changed replicated ready-set err=%v", err)
	}
	if _, invalidated := source.invalidated[key]; !invalidated {
		t.Fatal("changed replicated ready-set did not retire the cached generation")
	}
}

func TestCollectionVectorPartitionGenerationCacheRefreshIsNotPermanentInvalidationV1(t *testing.T) {
	oldSearcher, err := collections.OpenVectorPartitionLocalSearcherV1(
		vectorPartitionShardSearchAssetTestV1(0, []string{"old"}, [][]float32{{1, 0}}),
	)
	if err != nil {
		t.Fatal(err)
	}
	key := collectionVectorPartitionGenerationKeyV1{index: "embedding", generation: 7}
	oldEntry := &collectionVectorPartitionGenerationCacheV1{
		index:      key.index,
		generation: key.generation,
		manifest:   VectorPartitionPinnedManifestV1{Generation: key.generation},
		searchers:  map[uint32]*collections.VectorPartitionLocalSearcherV1{0: oldSearcher},
		opening:    make(map[uint32]*collectionVectorPartitionSearchLoadV1),
	}
	loads := 0
	source := &CollectionVectorPartitionGenerationSourceV1{
		Collection: new(collections.Collection),
		entries:    map[collectionVectorPartitionGenerationKeyV1]*collectionVectorPartitionGenerationCacheV1{key: oldEntry},
		testValidateActive: func(context.Context, collectionVectorPartitionGenerationKeyV1) error {
			return collections.ErrVectorPartitionAuthorityRefreshRequiredV1
		},
		testLoadGeneration: func(context.Context, collectionVectorPartitionGenerationKeyV1) (*collectionVectorPartitionGenerationCacheV1, error) {
			loads++
			return &collectionVectorPartitionGenerationCacheV1{
				index:      key.index,
				generation: key.generation,
				manifest:   VectorPartitionPinnedManifestV1{Generation: key.generation},
				searchers:  make(map[uint32]*collections.VectorPartitionLocalSearcherV1),
				opening:    make(map[uint32]*collectionVectorPartitionSearchLoadV1),
			}, nil
		},
	}
	pinned, err := source.PinVectorPartitionGenerationV1(t.Context(), key.index, key.generation)
	if err != nil {
		t.Fatalf("refresh pin: %v", err)
	}
	if loads != 1 {
		t.Fatalf("refresh loads=%d want 1", loads)
	}
	if !oldSearcher.Status().Retired {
		t.Fatal("authority refresh did not retire the old cache entry")
	}
	if _, permanentlyInvalidated := source.invalidated[key]; permanentlyInvalidated {
		t.Fatal("ordinary DB authority refresh permanently invalidated generation")
	}
	if err := pinned.Close(); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCollectionVectorPartitionGenerationCacheBoundsAuthorityRefreshV1(t *testing.T) {
	key := collectionVectorPartitionGenerationKeyV1{index: "embedding", generation: 7}
	newEntry := func() *collectionVectorPartitionGenerationCacheV1 {
		return &collectionVectorPartitionGenerationCacheV1{
			index:      key.index,
			generation: key.generation,
			manifest:   VectorPartitionPinnedManifestV1{Generation: key.generation},
			searchers:  make(map[uint32]*collections.VectorPartitionLocalSearcherV1),
			opening:    make(map[uint32]*collectionVectorPartitionSearchLoadV1),
		}
	}
	source := &CollectionVectorPartitionGenerationSourceV1{
		Collection: new(collections.Collection),
		entries:    map[collectionVectorPartitionGenerationKeyV1]*collectionVectorPartitionGenerationCacheV1{key: newEntry()},
	}
	validations := 0
	source.testValidateActive = func(context.Context, collectionVectorPartitionGenerationKeyV1) error {
		validations++
		return collections.ErrVectorPartitionAuthorityRefreshRequiredV1
	}
	refreshEvictions := 0
	source.testAfterAuthorityRefreshEvict = func() {
		refreshEvictions++
		if refreshEvictions <= collectionVectorPartitionMaxAuthorityRefreshesV1 {
			source.mu.Lock()
			source.entries[key] = newEntry()
			source.mu.Unlock()
		}
	}

	_, err := source.PinVectorPartitionGenerationV1(t.Context(), key.index, key.generation)
	if !errors.Is(err, ErrVectorPartitionShardSearchAssetsUnavailable) ||
		!strings.Contains(err.Error(), "authority refresh retry budget exhausted") {
		t.Fatalf("refresh exhaustion err=%v", err)
	}
	if validations != collectionVectorPartitionMaxAuthorityRefreshesV1+1 {
		t.Fatalf("validations=%d want %d", validations, collectionVectorPartitionMaxAuthorityRefreshesV1+1)
	}
	source.mu.Lock()
	_, cached := source.entries[key]
	_, permanentlyInvalidated := source.invalidated[key]
	source.mu.Unlock()
	if cached || permanentlyInvalidated {
		t.Fatalf("refresh exhaustion cached=%v permanently_invalidated=%v", cached, permanentlyInvalidated)
	}
}

func TestCollectionVectorPartitionGenerationManifestHandoffIsDefensiveV1(t *testing.T) {
	entry := &collectionVectorPartitionGenerationCacheV1{
		manifest: VectorPartitionPinnedManifestV1{
			Generation: 7,
			Placements: []collections.VectorPartitionPlacementV1{{
				PartitionID: 0,
				GroupID:     "group-a",
			}},
		},
	}
	lease := newCollectionVectorPartitionGenerationLeaseV1(&CollectionVectorPartitionGenerationSourceV1{}, entry)
	manifest := lease.Manifest()
	manifest.Placements[0].GroupID = "mutated"
	if entry.manifest.Placements[0].GroupID != "group-a" {
		t.Fatal("public manifest handoff aliased immutable cache memory")
	}
	if view := lease.immutableManifestViewV1(); view.Placements[0].GroupID != "group-a" {
		t.Fatalf("internal immutable view=%+v", view)
	}
}

func TestCollectionVectorPartitionGenerationSingleflightDoesNotShareCallerCancellationV1(t *testing.T) {
	key := collectionVectorPartitionGenerationKeyV1{index: "embedding", generation: 7}
	firstLoadStarted := make(chan struct{})
	releaseFirstLoad := make(chan struct{})
	waiterAttached := make(chan struct{})
	var waitOnce sync.Once
	loadCalls := 0
	source := &CollectionVectorPartitionGenerationSourceV1{
		Collection: new(collections.Collection),
		testBeforeLoadWait: func() {
			waitOnce.Do(func() { close(waiterAttached) })
		},
		testLoadGeneration: func(ctx context.Context, got collectionVectorPartitionGenerationKeyV1) (*collectionVectorPartitionGenerationCacheV1, error) {
			if got != key {
				return nil, fmt.Errorf("key=%+v", got)
			}
			loadCalls++
			if loadCalls == 1 {
				close(firstLoadStarted)
				<-releaseFirstLoad
				return nil, ctx.Err()
			}
			return &collectionVectorPartitionGenerationCacheV1{
				index:      key.index,
				generation: key.generation,
				manifest:   VectorPartitionPinnedManifestV1{Generation: key.generation},
				searchers:  make(map[uint32]*collections.VectorPartitionLocalSearcherV1),
				opening:    make(map[uint32]*collectionVectorPartitionSearchLoadV1),
			}, nil
		},
	}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() {
		_, err := source.PinVectorPartitionGenerationV1(firstCtx, key.index, key.generation)
		firstResult <- err
	}()
	<-firstLoadStarted
	secondResult := make(chan struct {
		pin VectorPartitionPinnedGenerationV1
		err error
	}, 1)
	go func() {
		pin, err := source.PinVectorPartitionGenerationV1(context.Background(), key.index, key.generation)
		secondResult <- struct {
			pin VectorPartitionPinnedGenerationV1
			err error
		}{pin: pin, err: err}
	}()
	<-waiterAttached
	cancelFirst()
	close(releaseFirstLoad)
	if err := <-firstResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("first caller err=%v", err)
	}
	second := <-secondResult
	if second.err != nil || second.pin == nil {
		t.Fatalf("independent waiter pin=%v err=%v", second.pin, second.err)
	}
	if loadCalls != 2 {
		t.Fatalf("load calls=%d want canceled caller plus independent retry", loadCalls)
	}
	if err := second.pin.Close(); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestVectorPartitionShardSearchConstructorRejectsNonMemberLocalNodeV1(t *testing.T) {
	catalog, err := raftplacement.Validate(raftplacement.CatalogV1{
		Features: raftplacement.DefaultFeatureSet(),
		Groups: []raftplacement.GroupV1{{
			ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a",
		}},
		Placements: []raftplacement.CollectionPlacementV1{{
			Collection: raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "docs"},
			GroupID:    "group-a",
			Mode:       raftplacement.PlacementModeCollectionV1,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	placement := raftplacement.VectorPartitionPlacementRecordV1{
		Collection:            raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "docs"},
		IndexName:             "embedding",
		IndexDefinitionDigest: vectorPartitionShardSearchDigestTestV1,
		SourceGeneration:      11,
		SourceChecksum:        22,
		SourceSchemaHash:      33,
		SourceRowCount:        5,
		PartitionGeneration:   7,
		PartitionCount:        1,
		Partitions:            []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}},
	}
	_, err = NewVectorPartitionShardSearchServiceV1(VectorPartitionShardSearchServiceOptionsV1{
		Catalog:          catalog,
		Placement:        placement,
		LocalNodeID:      "node-b",
		LocalGroupID:     "group-a",
		ReadCoordinator:  &fakeVectorPartitionReadCoordinatorV1{},
		GenerationSource: &fakeVectorPartitionGenerationSourceV1{},
	})
	if !errors.Is(err, ErrVectorPartitionShardSearchRouteMismatch) {
		t.Fatalf("non-member constructor err=%v", err)
	}
}

func TestVectorPartitionShardSearchLeaderGroupLocalReturnsOracleAndProofV1(t *testing.T) {
	service, source, coordinator := newVectorPartitionShardSearchTestServiceV1(t, []raftplacement.VectorPartitionGroupV1{
		{PartitionID: 0, GroupID: "group-a"},
		{PartitionID: 1, GroupID: "group-a"},
	}, map[uint32]collections.VectorPartitionSearchAssetV1{
		0: vectorPartitionShardSearchAssetTestV1(0, []string{"a", "b", "c"}, [][]float32{{1, 0}, {0.8, 0.2}, {0, 1}}),
		1: vectorPartitionShardSearchAssetTestV1(1, []string{"d", "e"}, [][]float32{{0.7, 0.3}, {-1, 0}}),
	})
	request := vectorPartitionShardSearchRequestTestV1([]uint32{0, 1})
	response, err := service.Search(context.Background(), request)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if response.Version != 1 || response.RequestID != request.RequestID || len(response.Partials) != 2 {
		t.Fatalf("response identity=%+v", response)
	}
	if got := response.Proof; got.ServingNode != "node-a" || got.LeaderNode != "node-a" || got.GroupID != "group-a" ||
		got.ReadySetDigest != request.ReadySetDigest ||
		got.ReadTerm != 3 || got.ReadIndex != 41 || got.AppliedTerm != 3 || got.AppliedIndex != 43 ||
		got.PartitionGeneration != 7 || got.RouterGeneration != 7 || got.SourceGeneration != 11 {
		t.Fatalf("proof=%+v", got)
	}
	if got := response.Partials[0].Neighbors; len(got) != 2 || got[0].ID != "a" || got[1].ID != "b" {
		t.Fatalf("partition 0 neighbors=%+v", got)
	}
	if got := response.Partials[1].Neighbors; len(got) != 2 || got[0].ID != "d" || got[1].ID != "e" {
		t.Fatalf("partition 1 neighbors=%+v", got)
	}
	if response.Candidates != 5 || response.ResponseBytes != 580 || response.Timing.ReadIndexApplyNanos == 0 {
		t.Fatalf("response accounting=%+v", response)
	}
	if coordinator.callCount() != 1 {
		t.Fatalf("read coordinator calls=%d", coordinator.callCount())
	}
	if pins, releases, opens := source.counts(); pins != 1 || releases != 1 || opens != 2 {
		t.Fatalf("source counts pins=%d releases=%d opens=%d", pins, releases, opens)
	}
	stats := service.Stats()
	if stats.Requests != 1 || stats.Successes != 1 || stats.Errors != 0 || stats.Partitions != 2 ||
		stats.ReadProofs != 1 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestVectorPartitionShardSearchTimingExcludesResponseMaterializationFromSearchV1(t *testing.T) {
	service, _, _ := newVectorPartitionShardSearchTestServiceV1(t,
		[]raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}},
		map[uint32]collections.VectorPartitionSearchAssetV1{
			0: vectorPartitionShardSearchAssetTestV1(0, []string{"a"}, [][]float32{{1, 0}}),
		},
	)
	const materializationDelay = 25 * time.Millisecond
	service.testBeforePartialMaterialization = func() {
		time.Sleep(materializationDelay)
	}
	response, err := service.Search(context.Background(), vectorPartitionShardSearchRequestTestV1([]uint32{0}))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if response.Timing.ResponseCopyNanos < uint64(materializationDelay) {
		t.Fatalf("response materialization timing=%d want >=%d", response.Timing.ResponseCopyNanos, materializationDelay)
	}
	if response.Timing.SearchNanos >= response.Timing.ResponseCopyNanos {
		t.Fatalf("search timing includes response materialization: %+v", response.Timing)
	}
}

func TestVectorPartitionShardSearchCancellationBeforePartialMaterializationV1(t *testing.T) {
	service, _, _ := newVectorPartitionShardSearchTestServiceV1(t,
		[]raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}},
		map[uint32]collections.VectorPartitionSearchAssetV1{
			0: vectorPartitionShardSearchAssetTestV1(0, []string{"a"}, [][]float32{{1, 0}}),
		},
	)
	ctx, cancel := context.WithCancel(context.Background())
	service.testBeforePartialMaterialization = cancel
	response, err := service.Search(ctx, vectorPartitionShardSearchRequestTestV1([]uint32{0}))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Search cancellation err=%v", err)
	}
	if response.Version != 0 || response.RequestID != "" || len(response.Partials) != 0 {
		t.Fatalf("canceled response materialized: %+v", response)
	}
}

func TestVectorPartitionShardSearchPinnedManifestDirectIndexFailsClosedV1(t *testing.T) {
	service, source, _ := newVectorPartitionShardSearchTestServiceV1(t,
		[]raftplacement.VectorPartitionGroupV1{
			{PartitionID: 0, GroupID: "group-a"},
			{PartitionID: 1, GroupID: "group-a"},
		},
		map[uint32]collections.VectorPartitionSearchAssetV1{},
	)
	request := vectorPartitionShardSearchRequestTestV1([]uint32{1})
	manifest := pinnedVectorPartitionManifestV1(source.manifest)
	if err := service.validatePinnedManifest(request, manifest); err != nil {
		t.Fatalf("valid pinned manifest: %v", err)
	}
	tests := []struct {
		name string
		edit func(*VectorPartitionPinnedManifestV1)
	}{
		{
			name: "missing direct slot",
			edit: func(m *VectorPartitionPinnedManifestV1) {
				m.Placements = m.Placements[:1]
			},
		},
		{
			name: "noncanonical direct slot",
			edit: func(m *VectorPartitionPinnedManifestV1) {
				m.Placements[0], m.Placements[1] = m.Placements[1], m.Placements[0]
			},
		},
		{
			name: "wrong owner",
			edit: func(m *VectorPartitionPinnedManifestV1) {
				m.Placements[1].GroupID = "group-b"
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate := clonePinnedVectorPartitionManifestV1(manifest)
			tt.edit(&candidate)
			if err := service.validatePinnedManifest(request, candidate); !errors.Is(err, ErrVectorPartitionShardSearchRouteMismatch) {
				t.Fatalf("malformed pinned manifest err=%v", err)
			}
		})
	}
}

func TestVectorPartitionShardSearchResponseRejectsLargeDuplicateSetV1(t *testing.T) {
	neighbors := make([]VectorPartitionShardSearchNeighborV1, vectorPartitionDuplicateLinearThresholdV1+2)
	for i := range neighbors {
		neighbors[i] = VectorPartitionShardSearchNeighborV1{ID: fmt.Sprintf("id-%d", i), Score: float32(i)}
	}
	neighbors[len(neighbors)-1].ID = neighbors[0].ID
	service := &VectorPartitionShardSearchServiceV1{limits: DefaultVectorPartitionShardSearchLimitsV1()}
	request := VectorPartitionShardSearchRequestV1{
		PartitionIDs:       []uint32{0},
		TopK:               len(neighbors),
		ResponseBytesLimit: 1 << 20,
	}
	_, err := service.validateResponse(t.Context(), request, []VectorPartitionShardSearchPartialV1{{
		PartitionID: 0,
		Neighbors:   neighbors,
		SearchRoute: collections.VectorPartitionSearchRouteHNSWSearchPackV1,
	}})
	if !errors.Is(err, ErrVectorPartitionShardSearchAssetsUnavailable) ||
		!strings.Contains(err.Error(), "duplicate stable ID") {
		t.Fatalf("large duplicate response err=%v", err)
	}
}

func TestVectorPartitionShardSearchResponseBloomCollisionStillExactV1(t *testing.T) {
	var first, second string
	seen := make(map[uint64]string)
	for i := 0; second == ""; i++ {
		id := fmt.Sprintf("collision-%d", i)
		bit := vectorPartitionStableIDBloomBitV1(id)
		if previous, ok := seen[bit]; ok {
			first, second = previous, id
			break
		}
		seen[bit] = id
	}
	service := &VectorPartitionShardSearchServiceV1{limits: DefaultVectorPartitionShardSearchLimitsV1()}
	request := VectorPartitionShardSearchRequestV1{
		PartitionIDs:       []uint32{0},
		TopK:               3,
		ResponseBytesLimit: 1 << 20,
	}
	partial := VectorPartitionShardSearchPartialV1{
		PartitionID: 0,
		Neighbors: []VectorPartitionShardSearchNeighborV1{
			{ID: first, Score: 2},
			{ID: second, Score: 1},
		},
		SearchRoute: collections.VectorPartitionSearchRouteHNSWSearchPackV1,
	}
	if _, err := service.validateResponse(t.Context(), request, []VectorPartitionShardSearchPartialV1{partial}); err != nil {
		t.Fatalf("distinct bloom collision rejected: %v", err)
	}
	partial.Neighbors = append(partial.Neighbors, VectorPartitionShardSearchNeighborV1{ID: second, Score: 0})
	if _, err := service.validateResponse(t.Context(), request, []VectorPartitionShardSearchPartialV1{partial}); !errors.Is(err, ErrVectorPartitionShardSearchAssetsUnavailable) ||
		!strings.Contains(err.Error(), "duplicate stable ID") {
		t.Fatalf("duplicate after bloom collision err=%v", err)
	}
}

func TestVectorPartitionShardSearchRouteFailuresNeverOpenOrSearchLocalStateV1(t *testing.T) {
	tests := []struct {
		name      string
		parts     []raftplacement.VectorPartitionGroupV1
		request   func(VectorPartitionShardSearchRequestV1) VectorPartitionShardSearchRequestV1
		coordErr  error
		wantCode  VectorPartitionShardSearchErrorCodeV1
		wantCoord int
	}{
		{
			name:  "missing_group",
			parts: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}},
			request: func(r VectorPartitionShardSearchRequestV1) VectorPartitionShardSearchRequestV1 {
				r.TargetGroupID = ""
				return r
			},
			wantCode: VectorPartitionShardSearchErrorMissingOwnerV1,
		},
		{
			name:  "remote_owner",
			parts: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-b"}},
			request: func(r VectorPartitionShardSearchRequestV1) VectorPartitionShardSearchRequestV1 {
				r.TargetGroupID = "group-b"
				return r
			},
			wantCode: VectorPartitionShardSearchErrorRemoteOwnerV1,
		},
		{
			name:  "mixed_groups",
			parts: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}, {PartitionID: 1, GroupID: "group-b"}},
			request: func(r VectorPartitionShardSearchRequestV1) VectorPartitionShardSearchRequestV1 {
				r.PartitionIDs = []uint32{0, 1}
				return r
			},
			wantCode: VectorPartitionShardSearchErrorRouteMismatchV1,
		},
		{
			name:  "unknown_partition",
			parts: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}},
			request: func(r VectorPartitionShardSearchRequestV1) VectorPartitionShardSearchRequestV1 {
				r.PartitionIDs = []uint32{1}
				return r
			},
			wantCode: VectorPartitionShardSearchErrorUnknownOwnerV1,
		},
		{
			name:  "stale_leader_hint",
			parts: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}},
			request: func(r VectorPartitionShardSearchRequestV1) VectorPartitionShardSearchRequestV1 {
				r.TargetNodeID = "node-old"
				return r
			},
			coordErr:  raftcluster.ErrReadBarrierTargetMismatch,
			wantCode:  VectorPartitionShardSearchErrorRouteMismatchV1,
			wantCoord: 1,
		},
		{
			name:  "follower",
			parts: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}},
			request: func(r VectorPartitionShardSearchRequestV1) VectorPartitionShardSearchRequestV1 {
				return r
			},
			coordErr:  raftcluster.ErrNotLeader,
			wantCode:  VectorPartitionShardSearchErrorNotLeaderV1,
			wantCoord: 1,
		},
		{
			name:  "unavailable_quorum",
			parts: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}},
			request: func(r VectorPartitionShardSearchRequestV1) VectorPartitionShardSearchRequestV1 {
				return r
			},
			coordErr:  raftcluster.ErrReadBarrierNotSatisfied,
			wantCode:  VectorPartitionShardSearchErrorGroupUnavailableV1,
			wantCoord: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assets := map[uint32]collections.VectorPartitionSearchAssetV1{
				0: vectorPartitionShardSearchAssetTestV1(0, []string{"a"}, [][]float32{{1, 0}}),
				1: vectorPartitionShardSearchAssetTestV1(1, []string{"b"}, [][]float32{{1, 0}}),
			}
			service, source, coordinator := newVectorPartitionShardSearchTestServiceV1(t, test.parts, assets)
			coordinator.err = test.coordErr
			request := test.request(vectorPartitionShardSearchRequestTestV1([]uint32{0}))
			response, err := service.Search(context.Background(), request)
			assertVectorPartitionShardSearchCodeV1(t, err, test.wantCode)
			if !vectorPartitionShardSearchResponseZeroTestV1(response) {
				t.Fatalf("failure returned partial response=%+v", response)
			}
			if got := coordinator.callCount(); got != test.wantCoord {
				t.Fatalf("coordinator calls=%d want %d", got, test.wantCoord)
			}
			if pins, releases, opens := source.counts(); pins != 0 || releases != 0 || opens != 0 {
				t.Fatalf("local state observed pins=%d releases=%d opens=%d", pins, releases, opens)
			}
		})
	}
}

func TestVectorPartitionShardSearchGenerationAndAssetFailuresAreWholeRequestV1(t *testing.T) {
	t.Run("generation_identity", func(t *testing.T) {
		service, source, coordinator := newVectorPartitionShardSearchTestServiceV1(t,
			[]raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}},
			map[uint32]collections.VectorPartitionSearchAssetV1{0: vectorPartitionShardSearchAssetTestV1(0, []string{"a"}, [][]float32{{1, 0}})},
		)
		request := vectorPartitionShardSearchRequestTestV1([]uint32{0})
		request.SourceGeneration++
		_, err := service.Search(context.Background(), request)
		assertVectorPartitionShardSearchCodeV1(t, err, VectorPartitionShardSearchErrorGenerationMismatchV1)
		if coordinator.callCount() != 0 {
			t.Fatal("generation mismatch reached read proof")
		}
		if pins, releases, opens := source.counts(); pins != 0 || releases != 0 || opens != 0 {
			t.Fatalf("generation mismatch observed local state %d/%d/%d", pins, releases, opens)
		}
	})

	t.Run("active_manifest_changed", func(t *testing.T) {
		service, source, _ := newVectorPartitionShardSearchTestServiceV1(t,
			[]raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}},
			map[uint32]collections.VectorPartitionSearchAssetV1{0: vectorPartitionShardSearchAssetTestV1(0, []string{"a"}, [][]float32{{1, 0}})},
		)
		source.manifest.Generation = 8
		_, err := service.Search(context.Background(), vectorPartitionShardSearchRequestTestV1([]uint32{0}))
		assertVectorPartitionShardSearchCodeV1(t, err, VectorPartitionShardSearchErrorGenerationMismatchV1)
		if pins, releases, opens := source.counts(); pins != 1 || releases != 1 || opens != 0 {
			t.Fatalf("pin lifetime=%d/%d opens=%d", pins, releases, opens)
		}
	})

	t.Run("second_asset_missing_no_first_partial", func(t *testing.T) {
		service, source, _ := newVectorPartitionShardSearchTestServiceV1(t,
			[]raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}, {PartitionID: 1, GroupID: "group-a"}},
			map[uint32]collections.VectorPartitionSearchAssetV1{0: vectorPartitionShardSearchAssetTestV1(0, []string{"a"}, [][]float32{{1, 0}})},
		)
		source.openErr = map[uint32]error{1: errors.New("corrupt pack checksum")}
		response, err := service.Search(context.Background(), vectorPartitionShardSearchRequestTestV1([]uint32{0, 1}))
		assertVectorPartitionShardSearchCodeV1(t, err, VectorPartitionShardSearchErrorAssetsUnavailableV1)
		if !vectorPartitionShardSearchResponseZeroTestV1(response) {
			t.Fatalf("failure returned partial response=%+v", response)
		}
		if pins, releases, opens := source.counts(); pins != 1 || releases != 1 || opens != 2 {
			t.Fatalf("pin lifetime=%d/%d opens=%d", pins, releases, opens)
		}
		source.mu.Lock()
		first := source.searchers[0]
		source.mu.Unlock()
		if first == nil {
			t.Fatal("first searcher not opened")
		}
		status := first.Status()
		if status.Searches != 0 || !status.Retired || status.ActivePins != 0 {
			t.Fatalf("first partition status after whole-request failure=%+v", status)
		}
	})

	t.Run("oversized_stable_id_rejected_before_search", func(t *testing.T) {
		service, source, _ := newVectorPartitionShardSearchTestServiceV1(t,
			[]raftplacement.VectorPartitionGroupV1{
				{PartitionID: 0, GroupID: "group-a"},
				{PartitionID: 1, GroupID: "group-a"},
			},
			map[uint32]collections.VectorPartitionSearchAssetV1{
				0: vectorPartitionShardSearchAssetTestV1(0, []string{"valid"}, [][]float32{{1, 0}}),
				1: vectorPartitionShardSearchAssetTestV1(1, []string{strings.Repeat("x", DefaultVectorPartitionShardSearchLimitsV1().MaxStableIDBytes+1)}, [][]float32{{1, 0}}),
			},
		)
		response, err := service.Search(context.Background(), vectorPartitionShardSearchRequestTestV1([]uint32{0, 1}))
		assertVectorPartitionShardSearchCodeV1(t, err, VectorPartitionShardSearchErrorAssetsUnavailableV1)
		if !vectorPartitionShardSearchResponseZeroTestV1(response) {
			t.Fatalf("oversized stable ID returned partial response=%+v", response)
		}
		if pins, releases, opens := source.counts(); pins != 1 || releases != 1 || opens != 2 {
			t.Fatalf("oversized stable ID pin lifetime=%d/%d opens=%d", pins, releases, opens)
		}
		source.mu.Lock()
		searchers := []*collections.VectorPartitionLocalSearcherV1{source.searchers[0], source.searchers[1]}
		source.mu.Unlock()
		for partition, searcher := range searchers {
			if searcher == nil {
				t.Fatalf("partition %d searcher was not opened", partition)
			}
			if status := searcher.Status(); status.Searches != 0 || status.Failures != 0 || status.ActivePins != 0 || !status.Retired {
				t.Fatalf("partition %d oversized stable ID reached traversal or leaked resources: %+v", partition, status)
			}
		}
	})
}

func TestVectorPartitionShardSearchBoundsFailBeforeProofOrAllocationV1(t *testing.T) {
	service, source, coordinator := newVectorPartitionShardSearchTestServiceV1(t,
		[]raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}},
		map[uint32]collections.VectorPartitionSearchAssetV1{0: vectorPartitionShardSearchAssetTestV1(0, []string{"a"}, [][]float32{{1, 0}})},
	)
	base := vectorPartitionShardSearchRequestTestV1([]uint32{0})
	tests := []struct {
		name string
		edit func(*VectorPartitionShardSearchRequestV1)
		code VectorPartitionShardSearchErrorCodeV1
	}{
		{name: "dimension", edit: func(r *VectorPartitionShardSearchRequestV1) {
			r.Query = make([]float32, service.limits.MaxDimensions+1)
		}},
		{name: "nan", edit: func(r *VectorPartitionShardSearchRequestV1) { r.Query[0] = float32(math.NaN()) }},
		{name: "infinity", edit: func(r *VectorPartitionShardSearchRequestV1) { r.Query[0] = float32(math.Inf(1)) }},
		{name: "zero", edit: func(r *VectorPartitionShardSearchRequestV1) { r.Query = []float32{0, 0} }},
		{name: "top_k", edit: func(r *VectorPartitionShardSearchRequestV1) { r.TopK = service.limits.MaxTopK + 1 }},
		{name: "ef_search", edit: func(r *VectorPartitionShardSearchRequestV1) { r.EfSearch = service.limits.MaxEfSearch + 1 }},
		{name: "partition_order", edit: func(r *VectorPartitionShardSearchRequestV1) { r.PartitionIDs = []uint32{0, 0} }},
		{name: "ready_set_digest", edit: func(r *VectorPartitionShardSearchRequestV1) { r.ReadySetDigest = "" }},
		{name: "request_bytes", edit: func(r *VectorPartitionShardSearchRequestV1) { r.RequestBytesLimit = 1 }},
		{name: "candidate_bytes", edit: func(r *VectorPartitionShardSearchRequestV1) { r.CandidateBytesLimit = 1 }},
		{name: "response_bytes", edit: func(r *VectorPartitionShardSearchRequestV1) { r.ResponseBytesLimit = 1 }, code: VectorPartitionShardSearchErrorResponseTooLargeV1},
		{name: "exact_mode_cannot_silently_use_hnsw", edit: func(r *VectorPartitionShardSearchRequestV1) { r.Mode = "exact_no_document" }},
		{name: "latest_vector_claim", edit: func(r *VectorPartitionShardSearchRequestV1) { r.Consistency = "linearizable_latest_vector" }, code: VectorPartitionShardSearchErrorUnsupportedConsistencyV1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := base
			request.Query = append([]float32(nil), base.Query...)
			request.PartitionIDs = append([]uint32(nil), base.PartitionIDs...)
			test.edit(&request)
			_, err := service.Search(context.Background(), request)
			want := test.code
			if want == "" {
				want = VectorPartitionShardSearchErrorInvalidRequestV1
			}
			assertVectorPartitionShardSearchCodeV1(t, err, want)
		})
	}
	if coordinator.callCount() != 0 {
		t.Fatalf("invalid requests reached proof %d times", coordinator.callCount())
	}
	if pins, releases, opens := source.counts(); pins != 0 || releases != 0 || opens != 0 {
		t.Fatalf("invalid requests observed local state %d/%d/%d", pins, releases, opens)
	}
}

func TestVectorPartitionShardSearchCancellationReleasesPinsWithoutPartialV1(t *testing.T) {
	t.Run("before_read_proof", func(t *testing.T) {
		service, source, coordinator := newVectorPartitionShardSearchTestServiceV1(t,
			[]raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}},
			map[uint32]collections.VectorPartitionSearchAssetV1{0: vectorPartitionShardSearchAssetTestV1(0, []string{"a"}, [][]float32{{1, 0}})},
		)
		coordinator.wait = true
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			response, err := service.Search(ctx, vectorPartitionShardSearchRequestTestV1([]uint32{0}))
			if !vectorPartitionShardSearchResponseZeroTestV1(response) {
				done <- fmt.Errorf("partial response: %+v", response)
				return
			}
			done <- err
		}()
		waitVectorPartitionShardConditionV1(t, func() bool { return coordinator.callCount() == 1 })
		cancel()
		assertVectorPartitionShardSearchCodeV1(t, <-done, VectorPartitionShardSearchErrorCanceledV1)
		if pins, releases, opens := source.counts(); pins != 0 || releases != 0 || opens != 0 {
			t.Fatalf("pre-proof cancellation observed state %d/%d/%d", pins, releases, opens)
		}
	})

	t.Run("during_search", func(t *testing.T) {
		service, source, _ := newVectorPartitionShardSearchTestServiceV1(t,
			[]raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}},
			map[uint32]collections.VectorPartitionSearchAssetV1{0: vectorPartitionShardSearchAssetTestV1(0, []string{"a"}, [][]float32{{1, 0}})},
		)
		started := make(chan struct{})
		service.testSearchPartition = func(ctx context.Context, searcher *collections.VectorPartitionLocalSearcherV1, _ []float32, _ collections.VectorPartitionSearchOptionsV1) ([]collections.VectorPartitionSearchResultV1, collections.VectorPartitionSearchMetricsV1, error) {
			if err := searcher.Acquire(); err != nil {
				return nil, collections.VectorPartitionSearchMetricsV1{}, err
			}
			close(started)
			<-ctx.Done()
			searcher.Release()
			return nil, collections.VectorPartitionSearchMetricsV1{}, ctx.Err()
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			response, err := service.Search(ctx, vectorPartitionShardSearchRequestTestV1([]uint32{0}))
			if !vectorPartitionShardSearchResponseZeroTestV1(response) {
				done <- fmt.Errorf("partial response: %+v", response)
				return
			}
			done <- err
		}()
		<-started
		cancel()
		assertVectorPartitionShardSearchCodeV1(t, <-done, VectorPartitionShardSearchErrorCanceledV1)
		if pins, releases, opens := source.counts(); pins != 1 || releases != 1 || opens != 1 {
			t.Fatalf("search cancellation pin lifetime=%d/%d opens=%d", pins, releases, opens)
		}
		source.mu.Lock()
		searcher := source.searchers[0]
		source.mu.Unlock()
		if status := searcher.Status(); status.ActivePins != 0 || !status.Retired {
			t.Fatalf("searcher pins leaked: %+v", status)
		}
	})

	t.Run("during_response", func(t *testing.T) {
		service, source, _ := newVectorPartitionShardSearchTestServiceV1(t,
			[]raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}},
			map[uint32]collections.VectorPartitionSearchAssetV1{0: vectorPartitionShardSearchAssetTestV1(0, []string{"a"}, [][]float32{{1, 0}})},
		)
		ctx, cancel := context.WithCancel(context.Background())
		service.testBeforeResponseCopy = cancel
		response, err := service.Search(ctx, vectorPartitionShardSearchRequestTestV1([]uint32{0}))
		assertVectorPartitionShardSearchCodeV1(t, err, VectorPartitionShardSearchErrorCanceledV1)
		if !vectorPartitionShardSearchResponseZeroTestV1(response) {
			t.Fatalf("response cancellation returned partial=%+v", response)
		}
		if pins, releases, opens := source.counts(); pins != 1 || releases != 1 || opens != 1 {
			t.Fatalf("response cancellation pin lifetime=%d/%d opens=%d", pins, releases, opens)
		}
	})
}

func TestRunVectorPartitionOwnedSearchWithContextV1OwnsQueryAndJoinsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	query := []float32{1, 2}
	started := make(chan struct{})
	finish := make(chan struct{})
	observedQuery := make(chan []float32, 1)
	done := make(chan error, 1)

	go func() {
		_, _, err := runVectorPartitionOwnedSearchWithContextV1(
			ctx,
			query,
			collections.VectorPartitionSearchOptionsV1{TopK: 1},
			func(ctx context.Context, owned []float32, _ collections.VectorPartitionSearchOptionsV1) ([]collections.VectorPartitionSearchResultV1, collections.VectorPartitionSearchMetricsV1, error) {
				close(started)
				<-ctx.Done()
				<-finish
				observedQuery <- append([]float32(nil), owned...)
				return nil, collections.VectorPartitionSearchMetricsV1{}, ctx.Err()
			},
		)
		done <- err
	}()

	<-started
	cancel()
	select {
	case err := <-done:
		t.Fatalf("canceled search returned before worker joined: %v", err)
	default:
	}
	query[0] = 99
	close(finish)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled search err=%v", err)
	}
	if got := <-observedQuery; len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("worker query=%v want owned [1 2]", got)
	}
}

func TestVectorPartitionShardSearchPinsExactGenerationAcrossConcurrentActivationV1(t *testing.T) {
	service, source, _ := newVectorPartitionShardSearchTestServiceV1(t,
		[]raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}},
		map[uint32]collections.VectorPartitionSearchAssetV1{0: vectorPartitionShardSearchAssetTestV1(0, []string{"old"}, [][]float32{{1, 0}})},
	)
	service.testSearchPartition = func(ctx context.Context, searcher *collections.VectorPartitionLocalSearcherV1, query []float32, opts collections.VectorPartitionSearchOptionsV1) ([]collections.VectorPartitionSearchResultV1, collections.VectorPartitionSearchMetricsV1, error) {
		source.mu.Lock()
		source.manifest.Generation = 8
		source.manifest.RouterGeneration = 8
		source.assets[0] = vectorPartitionShardSearchAssetTestV1(0, []string{"new"}, [][]float32{{1, 0}})
		source.assets[0] = func(asset collections.VectorPartitionSearchAssetV1) collections.VectorPartitionSearchAssetV1 {
			asset.Generation = 8
			return asset
		}(source.assets[0])
		source.mu.Unlock()
		return searcher.SearchWithOptionsV1(ctx, query, opts)
	}
	response, err := service.Search(context.Background(), vectorPartitionShardSearchRequestTestV1([]uint32{0}))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if got := response.Partials[0].Neighbors[0].ID; got != "old" {
		t.Fatalf("result=%q want pinned old generation", got)
	}
	if response.Proof.PartitionGeneration != 7 || response.Proof.RouterGeneration != 7 {
		t.Fatalf("proof mixed generations: %+v", response.Proof)
	}
}

func TestVectorPartitionShardSearchDeadlineIsStableAndNoMutationV1(t *testing.T) {
	service, source, coordinator := newVectorPartitionShardSearchTestServiceV1(t,
		[]raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}},
		map[uint32]collections.VectorPartitionSearchAssetV1{0: vectorPartitionShardSearchAssetTestV1(0, []string{"a"}, [][]float32{{1, 0}})},
	)
	request := vectorPartitionShardSearchRequestTestV1([]uint32{0})
	request.DeadlineUnixNano = time.Now().Add(-time.Second).UnixNano()
	_, err := service.Search(context.Background(), request)
	assertVectorPartitionShardSearchCodeV1(t, err, VectorPartitionShardSearchErrorDeadlineV1)
	if coordinator.callCount() != 0 {
		t.Fatal("expired request reached read proof")
	}
	if pins, releases, opens := source.counts(); pins != 0 || releases != 0 || opens != 0 {
		t.Fatalf("expired request observed local state %d/%d/%d", pins, releases, opens)
	}
	if stats := service.Stats(); stats.TimedOut != 1 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestVectorPartitionShardSearchPreservesPartitionOpenContextErrorsV1(t *testing.T) {
	tests := []struct {
		name         string
		openErr      error
		wantCode     VectorPartitionShardSearchErrorCodeV1
		wantCanceled uint64
		wantTimedOut uint64
	}{
		{name: "canceled", openErr: context.Canceled, wantCode: VectorPartitionShardSearchErrorCanceledV1, wantCanceled: 1},
		{name: "deadline", openErr: context.DeadlineExceeded, wantCode: VectorPartitionShardSearchErrorDeadlineV1, wantTimedOut: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service, source, _ := newVectorPartitionShardSearchTestServiceV1(t,
				[]raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}},
				map[uint32]collections.VectorPartitionSearchAssetV1{0: vectorPartitionShardSearchAssetTestV1(0, []string{"a"}, [][]float32{{1, 0}})},
			)
			source.openErr[0] = test.openErr
			response, err := service.Search(context.Background(), vectorPartitionShardSearchRequestTestV1([]uint32{0}))
			assertVectorPartitionShardSearchCodeV1(t, err, test.wantCode)
			if !errors.Is(err, test.openErr) {
				t.Fatalf("partition open err=%v does not preserve %v", err, test.openErr)
			}
			if errors.Is(err, ErrVectorPartitionShardSearchAssetsUnavailable) {
				t.Fatalf("partition open context error also classified as assets unavailable: %v", err)
			}
			if !vectorPartitionShardSearchResponseZeroTestV1(response) {
				t.Fatalf("partition open response=%+v", response)
			}
			if pins, releases, opens := source.counts(); pins != 1 || releases != 1 || opens != 1 {
				t.Fatalf("partition open lifecycle pins=%d releases=%d opens=%d", pins, releases, opens)
			}
			stats := service.Stats()
			if stats.Canceled != test.wantCanceled || stats.TimedOut != test.wantTimedOut || stats.AssetsUnavailable != 0 {
				t.Fatalf("partition open stats=%+v", stats)
			}
		})
	}
}

func TestVectorPartitionShardSearchContextWithoutDeadlineReusesCallerV1(t *testing.T) {
	caller, cancelCaller := context.WithCancel(context.Background())
	got, cancelRequest, err := vectorPartitionShardSearchContextV1(caller, 0)
	if err != nil {
		t.Fatalf("context: %v", err)
	}
	if got != caller {
		t.Fatal("request without a deadline did not reuse the caller context")
	}
	cancelRequest()
	if err := got.Err(); err != nil {
		t.Fatalf("no-op request cancel changed caller context: %v", err)
	}
	cancelCaller()
	if err := got.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation=%v want context.Canceled", err)
	}
}

func BenchmarkVectorPartitionShardSearchServiceV1(b *testing.B) {
	service, fakeSource, _ := newVectorPartitionShardSearchTestServiceV1(b,
		[]raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}},
		map[uint32]collections.VectorPartitionSearchAssetV1{
			0: vectorPartitionShardSearchAssetTestV1(0,
				[]string{"a", "b", "c", "d", "e", "f", "g", "h"},
				[][]float32{{1, 0}, {.9, .1}, {.8, .2}, {.7, .3}, {.6, .4}, {.5, .5}, {.4, .6}, {0, 1}},
			),
		},
	)
	searcher, err := collections.OpenVectorPartitionLocalSearcherV1(fakeSource.assets[0])
	if err != nil {
		b.Fatal(err)
	}
	key := collectionVectorPartitionGenerationKeyV1{index: "embedding", generation: 7}
	entry := &collectionVectorPartitionGenerationCacheV1{
		index:      key.index,
		generation: key.generation,
		manifest:   pinnedVectorPartitionManifestV1(fakeSource.manifest),
		searchers:  map[uint32]*collections.VectorPartitionLocalSearcherV1{0: searcher},
		opening:    make(map[uint32]*collectionVectorPartitionSearchLoadV1),
	}
	cachedSource := &CollectionVectorPartitionGenerationSourceV1{
		Collection: new(collections.Collection),
		entries:    map[collectionVectorPartitionGenerationKeyV1]*collectionVectorPartitionGenerationCacheV1{key: entry},
		testValidateActive: func(context.Context, collectionVectorPartitionGenerationKeyV1) error {
			return nil
		},
	}
	service.generationSource = cachedSource
	b.Cleanup(func() {
		if err := cachedSource.Close(); err != nil {
			b.Error(err)
		}
	})
	request := vectorPartitionShardSearchRequestTestV1([]uint32{0})
	b.ReportAllocs()
	b.SetBytes(int64(len(request.Query)*4 + len(request.PartitionIDs)*4))
	latencies := make([]uint64, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		started := time.Now()
		response, err := service.Search(context.Background(), request)
		if err != nil {
			b.Fatal(err)
		}
		latencies[i] = uint64(time.Since(started).Nanoseconds())
		if response.ResponseBytes == 0 {
			b.Fatal("empty response accounting")
		}
	}
	b.StopTimer()
	stats := service.Stats()
	n := uint64(b.N)
	if n != 0 {
		cacheStats := cachedSource.Stats()
		if cacheStats.GenerationHits != n || cacheStats.PartitionHits != n {
			b.Fatalf("warm cache stats=%+v requests=%d", cacheStats, n)
		}
		b.ReportMetric(float64(stats.RouteOwnerNanos)/float64(n), "route-ns/op")
		b.ReportMetric(float64(stats.ReadIndexApplyNanos)/float64(n), "read-index-apply-ns/op")
		b.ReportMetric(float64(stats.GenerationOpenNanos)/float64(n), "generation-open-ns/op")
		b.ReportMetric(float64(stats.SearchNanos)/float64(n), "partition-search-ns/op")
		b.ReportMetric(float64(stats.ResponseCopyNanos)/float64(n), "response-copy-ns/op")
		b.ReportMetric(float64(stats.ResponseBytes)/float64(n), "response-B/op")
		b.ReportMetric(float64(stats.Candidates)/float64(n), "candidates/op")
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		b.ReportMetric(float64(vectorPartitionShardPercentileTestV1(latencies, 50)), "p50-ns")
		b.ReportMetric(float64(vectorPartitionShardPercentileTestV1(latencies, 95)), "p95-ns")
		b.ReportMetric(float64(vectorPartitionShardPercentileTestV1(latencies, 99)), "p99-ns")
		totalSeconds := float64(stats.TotalNanos) / float64(time.Second)
		if totalSeconds > 0 {
			b.ReportMetric(float64(n)/totalSeconds, "service-qps")
		}
	}
}

func newVectorPartitionShardSearchTestServiceV1(tb testing.TB, parts []raftplacement.VectorPartitionGroupV1, assets map[uint32]collections.VectorPartitionSearchAssetV1) (*VectorPartitionShardSearchServiceV1, *fakeVectorPartitionGenerationSourceV1, *fakeVectorPartitionReadCoordinatorV1) {
	tb.Helper()
	catalog, err := raftplacement.Validate(raftplacement.CatalogV1{
		Features: raftplacement.DefaultFeatureSet(),
		Groups: []raftplacement.GroupV1{
			{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"},
			{ID: "group-b", Members: []raftcluster.NodeID{"node-b"}, LeaderHint: "node-b"},
		},
		Placements: []raftplacement.CollectionPlacementV1{{
			Collection: raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "docs"},
			GroupID:    "group-a",
			Mode:       raftplacement.PlacementModeCollectionV1,
		}},
	})
	if err != nil {
		tb.Fatalf("catalog: %v", err)
	}
	placement := raftplacement.VectorPartitionPlacementRecordV1{
		Collection:            raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "docs"},
		IndexName:             "embedding",
		IndexDefinitionDigest: vectorPartitionShardSearchDigestTestV1,
		SourceGeneration:      11,
		SourceChecksum:        22,
		SourceSchemaHash:      33,
		SourceRowCount:        5,
		PartitionGeneration:   7,
		PartitionCount:        uint32(len(parts)),
		Partitions:            append([]raftplacement.VectorPartitionGroupV1(nil), parts...),
	}
	manifestParts := make([]collections.VectorPartitionPlacementV1, len(parts))
	for i, part := range parts {
		manifestParts[i] = collections.VectorPartitionPlacementV1{PartitionID: part.PartitionID, GroupID: string(part.GroupID)}
	}
	source := &fakeVectorPartitionGenerationSourceV1{
		manifest: collections.VectorPartitionManifestV1{
			Format:                collections.VectorPartitionManifestFormatV1,
			State:                 "ready",
			Collection:            "docs",
			IndexName:             "embedding",
			IndexDefinitionDigest: vectorPartitionShardSearchDigestTestV1,
			SourceGeneration:      11,
			SourceChecksum:        22,
			SourceSchemaHash:      33,
			SourceRowCount:        5,
			Generation:            7,
			RouterGeneration:      7,
			ReadySetDigest:        vectorPartitionShardSearchDigestTestV1,
			PartitionCount:        uint32(len(parts)),
			Placements:            manifestParts,
		},
		assets:    assets,
		openErr:   make(map[uint32]error),
		searchers: make(map[uint32]*collections.VectorPartitionLocalSearcherV1),
	}
	coordinator := &fakeVectorPartitionReadCoordinatorV1{
		proof: raftcluster.ReadIndexProof{
			NodeID: "node-a", GroupID: "group-a", Term: 3, Index: 41,
			HasQuorum: true, EvidenceKind: raftcluster.ReadIndexEvidenceProduction,
		},
		progress: raftcluster.AppliedProgress{
			NodeID: "node-a", GroupID: "group-a", Term: 3, Index: 43, HasApplied: true,
		},
	}
	service, err := NewVectorPartitionShardSearchServiceV1(VectorPartitionShardSearchServiceOptionsV1{
		Catalog:          catalog,
		Placement:        placement,
		LocalNodeID:      "node-a",
		LocalGroupID:     "group-a",
		ReadCoordinator:  coordinator,
		GenerationSource: source,
	})
	if err != nil {
		tb.Fatalf("service: %v", err)
	}
	return service, source, coordinator
}

func vectorPartitionShardSearchAssetTestV1(partition uint32, ids []string, vectors [][]float32) collections.VectorPartitionSearchAssetV1 {
	return collections.VectorPartitionSearchAssetV1{
		ManifestChecksum: vectorPartitionShardSearchDigestTestV1,
		Generation:       7,
		PartitionID:      partition,
		Dimensions:       len(vectors[0]),
		IDs:              append([]string(nil), ids...),
		Vectors:          vectors,
		Kinds:            repeatVectorPartitionMembershipKindTestV1(len(ids), collections.VectorPartitionMembershipHomeV1),
	}
}

func repeatVectorPartitionMembershipKindTestV1(count int, kind collections.VectorPartitionMembershipKindV1) []collections.VectorPartitionMembershipKindV1 {
	out := make([]collections.VectorPartitionMembershipKindV1, count)
	for i := range out {
		out[i] = kind
	}
	return out
}

func vectorPartitionShardSearchRequestTestV1(partitions []uint32) VectorPartitionShardSearchRequestV1 {
	return VectorPartitionShardSearchRequestV1{
		Version:               1,
		RequestID:             "request-1",
		CancellationID:        "cancel-1",
		Database:              "default",
		Catalog:               "default",
		Collection:            "docs",
		IndexName:             "embedding",
		IndexDefinitionDigest: vectorPartitionShardSearchDigestTestV1,
		SourceGeneration:      11,
		SourceChecksum:        22,
		SourceSchemaHash:      33,
		SourceRowCount:        5,
		PartitionGeneration:   7,
		RouterGeneration:      7,
		ReadySetDigest:        vectorPartitionShardSearchDigestTestV1,
		TargetGroupID:         "group-a",
		TargetNodeID:          "node-a",
		PartitionIDs:          append([]uint32(nil), partitions...),
		Query:                 []float32{1, 0},
		Metric:                VectorPartitionShardSearchMetricCosineV1,
		Mode:                  VectorPartitionShardSearchModeNoDocumentV1,
		Consistency:           VectorPartitionShardSearchConsistencySnapshotV1,
		StatsMode:             VectorPartitionShardSearchStatsBasicV1,
		TopK:                  2,
		EfSearch:              4,
		RequestBytesLimit:     64 << 10,
		CandidateBytesLimit:   1 << 20,
		ResponseBytesLimit:    1 << 20,
	}
}

func assertVectorPartitionShardSearchCodeV1(tb testing.TB, err error, want VectorPartitionShardSearchErrorCodeV1) {
	tb.Helper()
	if err == nil {
		tb.Fatalf("err=nil want code %q", want)
	}
	var serviceErr *VectorPartitionShardSearchErrorV1
	if !errors.As(err, &serviceErr) || serviceErr.Code != want {
		tb.Fatalf("err=%v code=%q want %q", err, serviceErr.Code, want)
	}
	if strings.TrimSpace(err.Error()) == "" {
		tb.Fatal("empty stable error")
	}
}

func TestMeasureVectorPartitionShardSearchResponseBytesV1MatchesServiceAccounting(t *testing.T) {
	partials := []VectorPartitionShardSearchPartialV1{
		{
			PartitionID: 1,
			Neighbors: []VectorPartitionShardSearchNeighborV1{
				{ID: "alpha", Score: 1},
				{ID: "beta", Score: .5},
			},
		},
		{PartitionID: 2},
	}
	got, err := MeasureVectorPartitionShardSearchResponseBytesV1(partials)
	if err != nil {
		t.Fatal(err)
	}
	want := vectorPartitionShardSearchResponseEnvelopeBytesV1 +
		2*vectorPartitionShardSearchPartialEnvelopeBytesV1 +
		uint64(len("alpha")+16+len("beta")+16)
	if got != want {
		t.Fatalf("response bytes=%d want=%d", got, want)
	}
}

func vectorPartitionShardSearchResponseZeroTestV1(response VectorPartitionShardSearchResponseV1) bool {
	return response.Version == 0 && response.RequestID == "" && len(response.Partials) == 0 &&
		response.Partitions == 0 && response.Candidates == 0 && response.Edges == 0 &&
		response.ResponseBytes == 0 && response.Proof == (VectorPartitionShardSearchProofV1{})
}

func vectorPartitionShardPercentileTestV1(sortedValues []uint64, percentile int) uint64 {
	if len(sortedValues) == 0 {
		return 0
	}
	index := (len(sortedValues)*percentile + 99) / 100
	if index < 1 {
		index = 1
	}
	if index > len(sortedValues) {
		index = len(sortedValues)
	}
	return sortedValues[index-1]
}

func waitVectorPartitionShardConditionV1(tb testing.TB, condition func() bool) {
	tb.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	tb.Fatal("condition did not become true")
}
