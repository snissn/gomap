package nativewire

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

const collectionVectorPartitionMaxAuthorityRefreshesV1 = 3

// VectorPartitionReplicatedLifecycleAuthorityV1 is the constant-time M7
// serving guard. Local prepared assets are evidence, not activation authority;
// every cold load and cache hit must match the single replicated active record.
// Successful validation returns the replicated lifecycle ready-set digest,
// which is distinct from the local manifest's asset/placement digest.
type VectorPartitionReplicatedLifecycleAuthorityV1 interface {
	ValidateVectorPartitionGenerationSearchV1(
		context.Context,
		raftplacement.CollectionRefV1,
		string,
		uint64,
		string,
		uint64,
		uint64,
		uint64,
		uint64,
	) (string, error)
}

// CollectionVectorPartitionGenerationSourceV1 binds M5 to one actual local
// collection store. It keeps immutable ready generations and their opened
// partition packs cached until explicit invalidation or Close. A request lease
// prevents invalidation from closing a mapped pack while that request is
// searching it.
//
// The catalog/lifecycle owner must invalidate an old generation when it swaps
// the service's static placement to a replacement generation. Invalidation is
// permanent for this source instance, so a stale service cannot reload the old
// generation after the cutover.
type CollectionVectorPartitionGenerationSourceV1 struct {
	Collection           *collections.Collection
	replicatedCollection raftplacement.CollectionRefV1
	replicatedLifecycle  VectorPartitionReplicatedLifecycleAuthorityV1

	mu          sync.Mutex
	entries     map[collectionVectorPartitionGenerationKeyV1]*collectionVectorPartitionGenerationCacheV1
	loads       map[collectionVectorPartitionGenerationKeyV1]*collectionVectorPartitionGenerationLoadV1
	invalidated map[collectionVectorPartitionGenerationKeyV1]struct{}
	closed      bool
	stats       CollectionVectorPartitionGenerationCacheStatsV1
	closeOnce   sync.Once
	closeErr    error

	// Narrow package-test seams for shared-load cancellation and cached-active
	// validation. Production always uses the collection lifecycle authority.
	testLoadGeneration             func(context.Context, collectionVectorPartitionGenerationKeyV1) (*collectionVectorPartitionGenerationCacheV1, error)
	testValidateActive             func(context.Context, collectionVectorPartitionGenerationKeyV1) error
	testBeforeLoadWait             func()
	testAfterAuthorityRefreshEvict func()
}

type CollectionVectorPartitionGenerationCacheStatsV1 struct {
	GenerationHits, GenerationMisses uint64
	PartitionHits, PartitionMisses   uint64
	Invalidations                    uint64
}

func NewCollectionVectorPartitionGenerationSourceV1(collection *collections.Collection) (*CollectionVectorPartitionGenerationSourceV1, error) {
	if collection == nil {
		return nil, ErrVectorPartitionShardSearchAssetsUnavailable
	}
	return &CollectionVectorPartitionGenerationSourceV1{Collection: collection}, nil
}

// NewCollectionVectorPartitionGenerationSourceForReplicatedLifecycleV1 opens
// locally prepared generations and admits them only through replicated M7
// lifecycle authority. It deliberately does not consult or mutate M1's
// standalone LocalActivate pointer.
func NewCollectionVectorPartitionGenerationSourceForReplicatedLifecycleV1(
	collection *collections.Collection,
	collectionRef raftplacement.CollectionRefV1,
	authority VectorPartitionReplicatedLifecycleAuthorityV1,
) (*CollectionVectorPartitionGenerationSourceV1, error) {
	if collection == nil || authority == nil || collectionRef.Database == "" || collectionRef.Catalog == "" || collectionRef.Collection == "" {
		return nil, ErrVectorPartitionShardSearchAssetsUnavailable
	}
	return &CollectionVectorPartitionGenerationSourceV1{
		Collection:           collection,
		replicatedCollection: collectionRef,
		replicatedLifecycle:  authority,
	}, nil
}

type collectionVectorPartitionGenerationKeyV1 struct {
	index      string
	generation uint64
}

type collectionVectorPartitionGenerationLoadV1 struct {
	ready chan struct{}
	err   error
}

func (s *CollectionVectorPartitionGenerationSourceV1) PinVectorPartitionGenerationV1(ctx context.Context, index string, generation uint64) (VectorPartitionPinnedGenerationV1, error) {
	if s == nil || s.Collection == nil || index == "" || generation == 0 {
		return nil, ErrVectorPartitionShardSearchAssetsUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key := collectionVectorPartitionGenerationKeyV1{index: index, generation: generation}
	authorityRefreshes := 0

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil, fmt.Errorf("%w: generation source closed", ErrVectorPartitionShardSearchAssetsUnavailable)
		}
		if _, invalidated := s.invalidated[key]; invalidated {
			s.mu.Unlock()
			return nil, fmt.Errorf("%w: generation cache invalidated", ErrVectorPartitionShardSearchGenerationMismatch)
		}
		if entry := s.entries[key]; entry != nil {
			entry.refs++
			s.mu.Unlock()
			if err := s.validateActive(ctx, key, entry); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					_ = s.release(entry)
					return nil, err
				}
				if errors.Is(err, collections.ErrVectorPartitionAuthorityRefreshRequiredV1) {
					authorityRefreshes++
					if evictErr := s.evictForAuthorityRefresh(entry); evictErr != nil {
						return nil, evictErr
					}
					if s.testAfterAuthorityRefreshEvict != nil {
						s.testAfterAuthorityRefreshEvict()
					}
					if authorityRefreshes > collectionVectorPartitionMaxAuthorityRefreshesV1 {
						return nil, fmt.Errorf("%w: authority refresh retry budget exhausted", ErrVectorPartitionShardSearchAssetsUnavailable)
					}
					continue
				}
				invalidateErr := s.InvalidateVectorPartitionGenerationV1(key.index, key.generation)
				releaseErr := s.release(entry)
				return nil, errors.Join(
					fmt.Errorf("%w: cached generation is no longer active: %v", ErrVectorPartitionShardSearchGenerationMismatch, err),
					invalidateErr,
					releaseErr,
				)
			}
			s.mu.Lock()
			s.stats.GenerationHits++
			s.mu.Unlock()
			return newCollectionVectorPartitionGenerationLeaseV1(s, entry), nil
		}
		if load := s.loads[key]; load != nil {
			s.mu.Unlock()
			if s.testBeforeLoadWait != nil {
				s.testBeforeLoadWait()
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-load.ready:
				if load.err != nil {
					if (errors.Is(load.err, context.Canceled) || errors.Is(load.err, context.DeadlineExceeded)) && ctx.Err() == nil {
						continue
					}
					return nil, load.err
				}
				continue
			}
		}
		if s.entries == nil {
			s.entries = make(map[collectionVectorPartitionGenerationKeyV1]*collectionVectorPartitionGenerationCacheV1)
		}
		if s.loads == nil {
			s.loads = make(map[collectionVectorPartitionGenerationKeyV1]*collectionVectorPartitionGenerationLoadV1)
		}
		load := &collectionVectorPartitionGenerationLoadV1{ready: make(chan struct{})}
		s.loads[key] = load
		s.mu.Unlock()

		entry, err := s.loadGeneration(ctx, key)
		s.mu.Lock()
		switch {
		case err != nil:
			load.err = err
		case s.closed:
			load.err = fmt.Errorf("%w: generation source closed", ErrVectorPartitionShardSearchAssetsUnavailable)
		case s.isInvalidatedLocked(key):
			load.err = fmt.Errorf("%w: generation cache invalidated", ErrVectorPartitionShardSearchGenerationMismatch)
		default:
			entry.refs = 1
			s.entries[key] = entry
			s.stats.GenerationMisses++
		}
		needsClose := load.err != nil && entry != nil
		s.mu.Unlock()
		if needsClose {
			load.err = errors.Join(load.err, entry.close())
		}
		s.mu.Lock()
		delete(s.loads, key)
		close(load.ready)
		loadErr := load.err
		s.mu.Unlock()

		if loadErr != nil {
			return nil, loadErr
		}
		if err := ctx.Err(); err != nil {
			_ = s.release(entry)
			return nil, err
		}
		return newCollectionVectorPartitionGenerationLeaseV1(s, entry), nil
	}
}

func (s *CollectionVectorPartitionGenerationSourceV1) loadGeneration(ctx context.Context, key collectionVectorPartitionGenerationKeyV1) (*collectionVectorPartitionGenerationCacheV1, error) {
	if s.testLoadGeneration != nil {
		return s.testLoadGeneration(ctx, key)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pin, err := s.Collection.AcquireVectorPartitionReaderPinWithContextV1(ctx, key.index, key.generation)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: generation pin: %v", ErrVectorPartitionShardSearchAssetsUnavailable, err)
	}
	release := true
	defer func() {
		if release {
			pin.Release()
		}
	}()
	var manifest collections.VectorPartitionManifestV1
	var authorityToken collections.VectorPartitionActiveAuthorityTokenV1
	if s.replicatedLifecycle != nil {
		manifest, err = s.Collection.PreparedVectorPartitionManifestWithContextV1(ctx, key.index, key.generation)
	} else {
		manifest, authorityToken, err = s.Collection.ActiveVectorPartitionManifestAndAuthorityTokenWithContextV1(ctx, key.index, key.generation)
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: generation status: %v", ErrVectorPartitionShardSearchGenerationMismatch, err)
	}
	replicatedReadySetDigest, err := s.validateReplicatedLifecycle(ctx, key, manifest)
	if err != nil {
		authorityToken.Release()
		return nil, vectorPartitionReplicatedLifecycleValidationErrorV1(ctx, err)
	}
	openPlan, err := collections.NewVectorPartitionGenerationSearchOpenPlanWithContextV1(ctx, manifest)
	if err != nil {
		authorityToken.Release()
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: generation search-open plan: %v", ErrVectorPartitionShardSearchAssetsUnavailable, err)
	}
	if err := ctx.Err(); err != nil {
		authorityToken.Release()
		return nil, err
	}
	release = false
	pinnedManifest := pinnedVectorPartitionManifestV1(manifest)
	pinnedManifest.ReadySetDigest = replicatedReadySetDigest
	return &collectionVectorPartitionGenerationCacheV1{
		collection:     s.Collection,
		index:          key.index,
		generation:     key.generation,
		manifest:       pinnedManifest,
		openPlan:       openPlan,
		authorityToken: authorityToken,
		pin:            pin,
		searchers:      make(map[uint32]*collections.VectorPartitionLocalSearcherV1),
		opening:        make(map[uint32]*collectionVectorPartitionSearchLoadV1),
	}, nil
}

func vectorPartitionReplicatedLifecycleValidationErrorV1(ctx context.Context, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return err
	}
	return fmt.Errorf("%w: replicated lifecycle: %v", ErrVectorPartitionShardSearchGenerationMismatch, err)
}

func (s *CollectionVectorPartitionGenerationSourceV1) validateActive(ctx context.Context, key collectionVectorPartitionGenerationKeyV1, entry *collectionVectorPartitionGenerationCacheV1) error {
	if s.testValidateActive != nil {
		return s.testValidateActive(ctx, key)
	}
	if entry == nil {
		return ErrVectorPartitionShardSearchAssetsUnavailable
	}
	if s.replicatedLifecycle != nil {
		readySetDigest, err := s.replicatedLifecycle.ValidateVectorPartitionGenerationSearchV1(
			raftcluster.WithCatalogMetaReadSourceV1(ctx, raftcluster.CatalogMetaReadSourceShardLifecycleV1),
			s.replicatedCollection,
			entry.manifest.IndexName,
			entry.manifest.Generation,
			entry.manifest.IndexDefinitionDigest,
			entry.manifest.SourceGeneration,
			entry.manifest.SourceChecksum,
			entry.manifest.SourceSchemaHash,
			entry.manifest.SourceRowCount,
		)
		if err != nil {
			return err
		}
		if readySetDigest != entry.manifest.ReadySetDigest {
			return ErrVectorPartitionShardSearchGenerationMismatch
		}
		return nil
	}
	return s.Collection.ValidateActiveVectorPartitionAuthorityTokenWithContextV1(ctx, key.index, key.generation, entry.authorityToken)
}

func (s *CollectionVectorPartitionGenerationSourceV1) validateReplicatedLifecycle(ctx context.Context, key collectionVectorPartitionGenerationKeyV1, manifest collections.VectorPartitionManifestV1) (string, error) {
	if s.replicatedLifecycle == nil {
		return manifest.ReadySetDigest, nil
	}
	if manifest.Collection != s.replicatedCollection.Collection {
		return "", ErrVectorPartitionShardSearchGenerationMismatch
	}
	return s.replicatedLifecycle.ValidateVectorPartitionGenerationSearchV1(
		raftcluster.WithCatalogMetaReadSourceV1(ctx, raftcluster.CatalogMetaReadSourceShardLifecycleV1),
		s.replicatedCollection,
		key.index,
		key.generation,
		manifest.IndexDefinitionDigest,
		manifest.SourceGeneration,
		manifest.SourceChecksum,
		manifest.SourceSchemaHash,
		manifest.SourceRowCount,
	)
}

func (s *CollectionVectorPartitionGenerationSourceV1) isInvalidatedLocked(key collectionVectorPartitionGenerationKeyV1) bool {
	_, ok := s.invalidated[key]
	return ok
}

// InvalidateVectorPartitionGenerationV1 prevents new requests from pinning the
// named generation. Existing request leases finish against their exact pinned
// generation; the cached manifest, packs, and lifecycle pin are released after
// the last such request closes.
func (s *CollectionVectorPartitionGenerationSourceV1) InvalidateVectorPartitionGenerationV1(index string, generation uint64) error {
	if s == nil || index == "" || generation == 0 {
		return ErrVectorPartitionShardSearchAssetsUnavailable
	}
	key := collectionVectorPartitionGenerationKeyV1{index: index, generation: generation}
	var closeEntry *collectionVectorPartitionGenerationCacheV1
	var wait <-chan struct{}
	s.mu.Lock()
	if s.invalidated == nil {
		s.invalidated = make(map[collectionVectorPartitionGenerationKeyV1]struct{})
	}
	if _, exists := s.invalidated[key]; !exists {
		s.invalidated[key] = struct{}{}
		s.stats.Invalidations++
	}
	if entry := s.entries[key]; entry != nil {
		delete(s.entries, key)
		entry.retire = true
		if entry.refs == 0 {
			closeEntry = entry
		}
	}
	if load := s.loads[key]; load != nil {
		wait = load.ready
	}
	s.mu.Unlock()
	if closeEntry != nil {
		if err := closeEntry.close(); err != nil {
			return err
		}
	}
	if wait != nil {
		<-wait
	}
	return nil
}

// Close rejects new pins and retires every cached generation. Existing request
// leases remain usable and perform the final close when released.
func (s *CollectionVectorPartitionGenerationSourceV1) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		var closeEntries []*collectionVectorPartitionGenerationCacheV1
		var waits []<-chan struct{}
		s.mu.Lock()
		s.closed = true
		for key, entry := range s.entries {
			delete(s.entries, key)
			entry.retire = true
			if entry.refs == 0 {
				closeEntries = append(closeEntries, entry)
			}
		}
		for _, load := range s.loads {
			waits = append(waits, load.ready)
		}
		s.mu.Unlock()
		for _, entry := range closeEntries {
			s.closeErr = errors.Join(s.closeErr, entry.close())
		}
		for _, wait := range waits {
			<-wait
		}
	})
	return s.closeErr
}

func (s *CollectionVectorPartitionGenerationSourceV1) Stats() CollectionVectorPartitionGenerationCacheStatsV1 {
	if s == nil {
		return CollectionVectorPartitionGenerationCacheStatsV1{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func (s *CollectionVectorPartitionGenerationSourceV1) release(entry *collectionVectorPartitionGenerationCacheV1) error {
	var closeEntry bool
	s.mu.Lock()
	if entry.refs != 0 {
		entry.refs--
	}
	closeEntry = entry.refs == 0 && entry.retire
	s.mu.Unlock()
	if closeEntry {
		return entry.close()
	}
	return nil
}

func (s *CollectionVectorPartitionGenerationSourceV1) evictForAuthorityRefresh(entry *collectionVectorPartitionGenerationCacheV1) error {
	if s == nil || entry == nil {
		return nil
	}
	var closeEntry bool
	key := collectionVectorPartitionGenerationKeyV1{index: entry.index, generation: entry.generation}
	s.mu.Lock()
	if s.entries[key] == entry {
		delete(s.entries, key)
		entry.retire = true
	}
	if entry.refs != 0 {
		entry.refs--
	}
	closeEntry = entry.refs == 0 && entry.retire
	s.mu.Unlock()
	if closeEntry {
		return entry.close()
	}
	return nil
}

func (s *CollectionVectorPartitionGenerationSourceV1) partitionOpen(cacheHit bool) {
	s.mu.Lock()
	if cacheHit {
		s.stats.PartitionHits++
	} else {
		s.stats.PartitionMisses++
	}
	s.mu.Unlock()
}

type collectionVectorPartitionGenerationCacheV1 struct {
	collection     *collections.Collection
	index          string
	generation     uint64
	manifest       VectorPartitionPinnedManifestV1
	openPlan       *collections.VectorPartitionGenerationSearchOpenPlanV1
	authorityToken collections.VectorPartitionActiveAuthorityTokenV1
	pin            *collections.VectorPartitionReaderPinV1

	// refs and retire are protected by the source mutex.
	refs   uint64
	retire bool

	mu        sync.Mutex
	searchers map[uint32]*collections.VectorPartitionLocalSearcherV1
	opening   map[uint32]*collectionVectorPartitionSearchLoadV1
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

type collectionVectorPartitionSearchLoadV1 struct {
	ready chan struct{}
	err   error
}

func (e *collectionVectorPartitionGenerationCacheV1) openPartition(ctx context.Context, source *CollectionVectorPartitionGenerationSourceV1, partition uint32) (*VectorPartitionPartitionSearchLeaseV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		e.mu.Lock()
		if e.closed {
			e.mu.Unlock()
			return nil, fmt.Errorf("%w: generation cache closed", ErrVectorPartitionShardSearchAssetsUnavailable)
		}
		if searcher := e.searchers[partition]; searcher != nil {
			e.mu.Unlock()
			source.partitionOpen(true)
			return newCachedVectorPartitionSearchLeaseV1(searcher, true)
		}
		if load := e.opening[partition]; load != nil {
			e.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-load.ready:
				if load.err != nil {
					if (errors.Is(load.err, context.Canceled) || errors.Is(load.err, context.DeadlineExceeded)) && ctx.Err() == nil {
						continue
					}
					return nil, load.err
				}
				continue
			}
		}
		load := &collectionVectorPartitionSearchLoadV1{ready: make(chan struct{})}
		e.opening[partition] = load
		e.mu.Unlock()

		searcher, err := e.collection.OpenVectorPartitionLocalSearcherForGenerationSearchPlanWithContextV1(ctx, e.index, e.generation, partition, e.openPlan, e.pin)
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				err = fmt.Errorf("%w: %v", ErrVectorPartitionShardSearchAssetsUnavailable, err)
			}
		}
		e.mu.Lock()
		if err == nil && e.closed {
			err = fmt.Errorf("%w: generation cache closed", ErrVectorPartitionShardSearchAssetsUnavailable)
		}
		if err == nil {
			e.searchers[partition] = searcher
		} else if searcher != nil {
			_ = searcher.Close()
		}
		load.err = err
		delete(e.opening, partition)
		close(load.ready)
		e.mu.Unlock()
		if err != nil {
			return nil, err
		}
		source.partitionOpen(false)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return newCachedVectorPartitionSearchLeaseV1(searcher, false)
	}
}

func newCachedVectorPartitionSearchLeaseV1(searcher *collections.VectorPartitionLocalSearcherV1, cacheHit bool) (*VectorPartitionPartitionSearchLeaseV1, error) {
	return NewVectorPartitionPartitionSearchLeaseV1(searcher, cacheHit, func() error { return nil })
}

func (e *collectionVectorPartitionGenerationCacheV1) close() error {
	if e == nil {
		return nil
	}
	e.closeOnce.Do(func() {
		e.mu.Lock()
		e.closed = true
		searchers := make([]*collections.VectorPartitionLocalSearcherV1, 0, len(e.searchers))
		for partition, searcher := range e.searchers {
			searchers = append(searchers, searcher)
			delete(e.searchers, partition)
		}
		e.mu.Unlock()
		for _, searcher := range searchers {
			e.closeErr = errors.Join(e.closeErr, searcher.Close())
		}
		e.authorityToken.Release()
		if e.pin != nil {
			e.pin.Release()
		}
	})
	return e.closeErr
}

type collectionVectorPartitionGenerationLeaseV1 struct {
	source   *CollectionVectorPartitionGenerationSourceV1
	entry    *collectionVectorPartitionGenerationCacheV1
	manifest VectorPartitionPinnedManifestV1
	once     sync.Once
	closeErr error
}

func newCollectionVectorPartitionGenerationLeaseV1(source *CollectionVectorPartitionGenerationSourceV1, entry *collectionVectorPartitionGenerationCacheV1) *collectionVectorPartitionGenerationLeaseV1 {
	return &collectionVectorPartitionGenerationLeaseV1{
		source: source,
		entry:  entry,
		// entry.manifest is immutable for the cache entry's lifetime. Manifest
		// clones on handoff, so retaining the entry snapshot here avoids a
		// redundant per-request placements copy without exposing cache memory.
		manifest: entry.manifest,
	}
}

func (p *collectionVectorPartitionGenerationLeaseV1) Manifest() VectorPartitionPinnedManifestV1 {
	if p == nil {
		return VectorPartitionPinnedManifestV1{}
	}
	return clonePinnedVectorPartitionManifestV1(p.manifest)
}

func (p *collectionVectorPartitionGenerationLeaseV1) immutableManifestViewV1() VectorPartitionPinnedManifestV1 {
	if p == nil {
		return VectorPartitionPinnedManifestV1{}
	}
	return p.manifest
}

func (p *collectionVectorPartitionGenerationLeaseV1) OpenPartition(ctx context.Context, partition uint32) (*VectorPartitionPartitionSearchLeaseV1, error) {
	if p == nil || p.source == nil || p.entry == nil {
		return nil, ErrVectorPartitionShardSearchAssetsUnavailable
	}
	return p.entry.openPartition(ctx, p.source, partition)
}

func (p *collectionVectorPartitionGenerationLeaseV1) Close() error {
	if p == nil {
		return nil
	}
	p.once.Do(func() {
		p.closeErr = p.source.release(p.entry)
	})
	return p.closeErr
}

func pinnedVectorPartitionManifestV1(m collections.VectorPartitionManifestV1) VectorPartitionPinnedManifestV1 {
	return VectorPartitionPinnedManifestV1{
		State:                 m.State,
		Collection:            m.Collection,
		IndexName:             m.IndexName,
		IndexDefinitionDigest: m.IndexDefinitionDigest,
		IntegrityDigest:       m.IntegrityDigest,
		SourceGeneration:      m.SourceGeneration,
		SourceChecksum:        m.SourceChecksum,
		SourceSchemaHash:      m.SourceSchemaHash,
		SourceRowCount:        m.SourceRowCount,
		Generation:            m.Generation,
		RouterGeneration:      m.RouterGeneration,
		ReadySetDigest:        m.ReadySetDigest,
		PartitionCount:        m.PartitionCount,
		Placements:            slices.Clone(m.Placements),
	}
}

func clonePinnedVectorPartitionManifestV1(m VectorPartitionPinnedManifestV1) VectorPartitionPinnedManifestV1 {
	m.Placements = slices.Clone(m.Placements)
	return m
}
