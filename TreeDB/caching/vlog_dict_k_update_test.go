package caching

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
)

type legacyDictStoreForClassPublishTest struct {
	nextID    uint64
	currentID uint64
	dicts     map[uint64][]byte
}

type classWriterOnlyDictStoreForClassPublishTest struct {
	*legacyDictStoreForClassPublishTest
	classCurrent map[string]uint64
}

type classReadWriteDictStoreForClassPublishTest struct {
	*legacyDictStoreForClassPublishTest
	classCurrent map[string]uint64
}

type blockingDictStoreForConcurrentPublishTest struct {
	mu            sync.Mutex
	nextID        uint64
	currentID     uint64
	dicts         map[uint64][]byte
	putCalls      int
	inPut         int
	concurrentPut bool
	firstEntered  chan struct{}
	secondEntered chan struct{}
	releaseFirst  chan struct{}
}

type staleOnceDictStoreForPublishTest struct {
	*legacyDictStoreForClassPublishTest
	putCalls int
}

type staleKOnceDictStoreForPublishTest struct {
	*legacyDictStoreForClassPublishTest
	putCalls  int
	setKCalls int
	kByID     map[uint64]int
}

func (s *staleOnceDictStoreForPublishTest) PutDictBytes(ctx context.Context, dict []byte) (uint64, error) {
	s.putCalls++
	if s.putCalls == 1 {
		return 0, db.ErrDurableWALCleanupProofStale
	}
	return s.legacyDictStoreForClassPublishTest.PutDictBytes(ctx, dict)
}

func (s *staleKOnceDictStoreForPublishTest) PutDictBytes(ctx context.Context, dict []byte) (uint64, error) {
	s.putCalls++
	return s.legacyDictStoreForClassPublishTest.PutDictBytes(ctx, dict)
}

func (s *staleKOnceDictStoreForPublishTest) SetK(_ context.Context, dictID uint64, k int) error {
	s.setKCalls++
	if s.setKCalls == 1 {
		return db.ErrDurableWALCleanupProofStale
	}
	if s.kByID == nil {
		s.kByID = make(map[uint64]int)
	}
	s.kByID[dictID] = k
	return nil
}

func (s *staleKOnceDictStoreForPublishTest) GetK(_ context.Context, dictID uint64) (int, error) {
	return s.kByID[dictID], nil
}

func newBlockingDictStoreForConcurrentPublishTest() *blockingDictStoreForConcurrentPublishTest {
	return &blockingDictStoreForConcurrentPublishTest{
		dicts:         make(map[uint64][]byte),
		firstEntered:  make(chan struct{}),
		secondEntered: make(chan struct{}),
		releaseFirst:  make(chan struct{}),
	}
}

func (s *blockingDictStoreForConcurrentPublishTest) GetCurrent(context.Context) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentID, nil
}

func (s *blockingDictStoreForConcurrentPublishTest) GetDictBytes(_ context.Context, dictID uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.dicts[dictID]...), nil
}

func (s *blockingDictStoreForConcurrentPublishTest) PutDictBytes(_ context.Context, dict []byte) (uint64, error) {
	s.mu.Lock()
	s.putCalls++
	call := s.putCalls
	s.inPut++
	if s.inPut > 1 {
		s.concurrentPut = true
	}
	if call == 1 {
		close(s.firstEntered)
	} else if call == 2 {
		close(s.secondEntered)
	}
	s.mu.Unlock()

	if call == 1 {
		<-s.releaseFirst
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.inPut--
	s.nextID++
	id := s.nextID
	s.dicts[id] = append([]byte(nil), dict...)
	return id, nil
}

func (s *blockingDictStoreForConcurrentPublishTest) SetCurrent(_ context.Context, dictID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentID = dictID
	return nil
}

func (s *blockingDictStoreForConcurrentPublishTest) snapshot() (putCalls int, concurrent bool, currentID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putCalls, s.concurrentPut, s.currentID
}

func (s *legacyDictStoreForClassPublishTest) GetCurrent(context.Context) (uint64, error) {
	if s == nil {
		return 0, errors.New("nil store")
	}
	return s.currentID, nil
}

func (s *legacyDictStoreForClassPublishTest) GetDictBytes(_ context.Context, dictID uint64) ([]byte, error) {
	if s == nil {
		return nil, errors.New("nil store")
	}
	if dictID == 0 {
		return nil, nil
	}
	b, ok := s.dicts[dictID]
	if !ok {
		return nil, nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out, nil
}

func (s *legacyDictStoreForClassPublishTest) PutDictBytes(_ context.Context, dict []byte) (uint64, error) {
	if s == nil {
		return 0, errors.New("nil store")
	}
	if s.dicts == nil {
		s.dicts = make(map[uint64][]byte)
	}
	s.nextID++
	id := s.nextID
	out := make([]byte, len(dict))
	copy(out, dict)
	s.dicts[id] = out
	return id, nil
}

func (s *legacyDictStoreForClassPublishTest) SetCurrent(_ context.Context, dictID uint64) error {
	if s == nil {
		return errors.New("nil store")
	}
	s.currentID = dictID
	return nil
}

func (s *classWriterOnlyDictStoreForClassPublishTest) SetCurrentForClass(_ context.Context, class string, dictID uint64) error {
	if s == nil {
		return errors.New("nil store")
	}
	if s.classCurrent == nil {
		s.classCurrent = make(map[string]uint64)
	}
	s.classCurrent[class] = dictID
	return nil
}

func (s *classReadWriteDictStoreForClassPublishTest) SetCurrentForClass(_ context.Context, class string, dictID uint64) error {
	if s == nil {
		return errors.New("nil store")
	}
	if s.classCurrent == nil {
		s.classCurrent = make(map[string]uint64)
	}
	s.classCurrent[class] = dictID
	return nil
}

func (s *classReadWriteDictStoreForClassPublishTest) GetCurrentForClass(_ context.Context, class string) (uint64, error) {
	if s == nil {
		return 0, errors.New("nil store")
	}
	if s.classCurrent == nil {
		return 0, nil
	}
	return s.classCurrent[class], nil
}

func TestApplyValueLogDictProfileUpdatesKForSameDict(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("open dictdb: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	store := dictdb.New(backend)

	ctx := context.Background()
	dictBytes := []byte("dictionary-bytes")
	dictID, err := store.PutDictBytes(ctx, dictBytes)
	if err != nil {
		t.Fatalf("PutDictBytes: %v", err)
	}
	if err := store.SetCurrent(ctx, dictID); err != nil {
		t.Fatalf("SetCurrent: %v", err)
	}

	tr := &compression.Trainer{}
	dictHash := xxhash.Sum64(dictBytes)
	tr.AcceptProfile(&compression.ActiveProfile{
		DictHash:     dictHash,
		DictBytes:    len(dictBytes),
		Dict:         dictBytes,
		K:            16,
		PayloadRatio: 0.5,
		TotalRatio:   0.5,
		Timestamp:    time.Now(),
	})

	db := &DB{
		dictStore: store,
	}
	db.valueLogDictTrainer = tr
	db.valueLogDictLastAppliedDictHash.Store(dictHash)
	db.valueLogDictLastAppliedDictID.Store(dictID)
	db.dictCurrentCached.Store(dictID)
	db.valueLogDictCurrentK.Store(8)

	db.applyValueLogDictProfile()

	gotK, err := store.GetK(ctx, dictID)
	if err != nil {
		t.Fatalf("GetK: %v", err)
	}
	if gotK != 16 {
		t.Fatalf("expected k=16, got %d", gotK)
	}
	if curK := int(db.valueLogDictCurrentK.Load()); curK != 16 {
		t.Fatalf("expected currentK=16, got %d", curK)
	}
}

func TestApplyValueLogDictProfileForClass_SerializesConcurrentPublishers(t *testing.T) {
	tr := &compression.Trainer{}
	dictBytes := []byte("concurrent-publish-dictionary")
	dictHash := xxhash.Sum64(dictBytes)
	tr.AcceptProfile(&compression.ActiveProfile{
		DictHash:     dictHash,
		DictBytes:    len(dictBytes),
		Dict:         dictBytes,
		K:            8,
		PayloadRatio: 0.6,
		TotalRatio:   0.6,
		Timestamp:    time.Now(),
	})

	store := newBlockingDictStoreForConcurrentPublishTest()
	database := &DB{dictStore: store}
	database.valueLogDictTrainerByClass[vlogDictClassSingleValue] = tr

	var publishers sync.WaitGroup
	publishers.Add(2)
	go func() {
		defer publishers.Done()
		database.applyValueLogDictProfileForClass(vlogDictClassSingleValue)
	}()
	select {
	case <-store.firstEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("first dictionary publisher did not enter the store")
	}

	go func() {
		defer publishers.Done()
		database.applyValueLogDictProfileForClass(vlogDictClassSingleValue)
	}()
	select {
	case <-store.secondEntered:
		close(store.releaseFirst)
		publishers.Wait()
		t.Fatal("second publisher entered before the first completed")
	case <-time.After(100 * time.Millisecond):
	}

	close(store.releaseFirst)
	publishers.Wait()
	putCalls, concurrent, currentID := store.snapshot()
	if putCalls != 1 {
		t.Fatalf("dictionary PutDictBytes calls=%d want 1", putCalls)
	}
	if concurrent {
		t.Fatal("dictionary store observed concurrent PutDictBytes calls")
	}
	if currentID != 1 {
		t.Fatalf("current dictionary id=%d want 1", currentID)
	}
	if got := database.valueLogDictLastAppliedDictHashByClass[vlogDictClassSingleValue].Load(); got != dictHash {
		t.Fatalf("last applied dictionary hash=%x want %x", got, dictHash)
	}
}

func TestApplyValueLogDictProfileForClass_RetriesStaleCleanupWithoutBackgroundError(t *testing.T) {
	tr := &compression.Trainer{}
	dictBytes := []byte("retryable-dictionary")
	dictHash := xxhash.Sum64(dictBytes)
	tr.AcceptProfile(&compression.ActiveProfile{
		DictHash:     dictHash,
		DictBytes:    len(dictBytes),
		Dict:         dictBytes,
		K:            8,
		PayloadRatio: 0.6,
		TotalRatio:   0.6,
		Timestamp:    time.Now(),
	})

	store := &staleOnceDictStoreForPublishTest{
		legacyDictStoreForClassPublishTest: &legacyDictStoreForClassPublishTest{
			dicts: make(map[uint64][]byte),
		},
	}
	notifications := 0
	database := &DB{
		dictStore: store,
		notifyError: func(error) {
			notifications++
		},
	}
	database.valueLogDictTrainerByClass[vlogDictClassSingleValue] = tr

	database.applyValueLogDictProfileForClass(vlogDictClassSingleValue)
	if notifications != 0 || database.backgroundError() != nil {
		t.Fatalf("stale cleanup poisoned background state: notifications=%d err=%v", notifications, database.backgroundError())
	}
	if got := database.valueLogDictLastAppliedDictHashByClass[vlogDictClassSingleValue].Load(); got != 0 {
		t.Fatalf("stale cleanup consumed candidate hash=%x want 0", got)
	}

	database.applyValueLogDictProfileForClass(vlogDictClassSingleValue)
	if store.putCalls != 2 {
		t.Fatalf("dictionary PutDictBytes calls=%d want 2", store.putCalls)
	}
	if got := database.valueLogDictLastAppliedDictHashByClass[vlogDictClassSingleValue].Load(); got != dictHash {
		t.Fatalf("retried dictionary hash=%x want %x", got, dictHash)
	}
}

func TestApplyValueLogDictProfileForClass_RetriesStaleKWithoutApplyingProfile(t *testing.T) {
	tr := &compression.Trainer{}
	dictBytes := []byte("retryable-dictionary-k")
	dictHash := xxhash.Sum64(dictBytes)
	tr.AcceptProfile(&compression.ActiveProfile{
		DictHash:     dictHash,
		DictBytes:    len(dictBytes),
		Dict:         dictBytes,
		K:            8,
		PayloadRatio: 0.6,
		TotalRatio:   0.6,
		Timestamp:    time.Now(),
	})

	store := &staleKOnceDictStoreForPublishTest{
		legacyDictStoreForClassPublishTest: &legacyDictStoreForClassPublishTest{
			dicts: make(map[uint64][]byte),
		},
	}
	notifications := 0
	database := &DB{
		dictStore: store,
		notifyError: func(error) {
			notifications++
		},
	}
	database.valueLogDictTrainerByClass[vlogDictClassSingleValue] = tr

	database.applyValueLogDictProfileForClass(vlogDictClassSingleValue)
	if notifications != 0 || database.backgroundError() != nil {
		t.Fatalf("stale K cleanup poisoned background state: notifications=%d err=%v", notifications, database.backgroundError())
	}
	if got := database.valueLogDictLastAppliedDictHashByClass[vlogDictClassSingleValue].Load(); got != 0 {
		t.Fatalf("stale K cleanup applied candidate hash=%x want 0", got)
	}

	database.applyValueLogDictProfileForClass(vlogDictClassSingleValue)
	if store.putCalls != 2 || store.setKCalls != 2 {
		t.Fatalf("publish calls PutDictBytes=%d SetK=%d want 2 each", store.putCalls, store.setKCalls)
	}
	if got := database.valueLogDictLastAppliedDictHashByClass[vlogDictClassSingleValue].Load(); got != dictHash {
		t.Fatalf("retried dictionary hash=%x want %x", got, dictHash)
	}
	if got := store.kByID[store.currentID]; got != 8 {
		t.Fatalf("persisted K for current dictionary=%d want 8", got)
	}
}

func TestApplyValueLogDictProfileForClass_ClosingSkipsPublish(t *testing.T) {
	tr := &compression.Trainer{}
	dictBytes := []byte("single-value-dictionary")
	dictHash := xxhash.Sum64(dictBytes)
	tr.AcceptProfile(&compression.ActiveProfile{
		DictHash:     dictHash,
		DictBytes:    len(dictBytes),
		Dict:         dictBytes,
		K:            8,
		PayloadRatio: 0.6,
		TotalRatio:   0.6,
		Timestamp:    time.Now(),
	})

	store := &legacyDictStoreForClassPublishTest{
		currentID: 41,
		dicts:     map[uint64][]byte{41: []byte("old-dict")},
		nextID:    41,
	}
	db := &DB{dictStore: store}
	db.valueLogDictTrainerByClass[vlogDictClassSingleValue] = tr
	db.closing.Store(true)

	db.applyValueLogDictProfileForClass(vlogDictClassSingleValue)

	if got := store.currentID; got != 41 {
		t.Fatalf("expected current dict id to remain unchanged while closing, got %d", got)
	}
	if got := store.nextID; got != 41 {
		t.Fatalf("expected no new dict publish while closing, nextID=%d", got)
	}
}

func TestApplyValueLogDictProfileForClass_LegacyStoreFallbackRefreshesGlobalCache(t *testing.T) {
	tr := &compression.Trainer{}
	dictBytes := []byte("outer-leaf-dictionary")
	dictHash := xxhash.Sum64(dictBytes)
	tr.AcceptProfile(&compression.ActiveProfile{
		DictHash:     dictHash,
		DictBytes:    len(dictBytes),
		Dict:         dictBytes,
		K:            8,
		PayloadRatio: 0.6,
		TotalRatio:   0.6,
		Timestamp:    time.Now(),
	})

	store := &legacyDictStoreForClassPublishTest{
		currentID: 41,
		dicts:     map[uint64][]byte{41: []byte("old-dict")},
		nextID:    41,
	}
	db := &DB{
		dictStore:             store,
		valueLogDictClassMode: uint8(vlogDictClassModeSplitOuterLeaf),
	}
	db.valueLogDictTrainerByClass[vlogDictClassOuterLeaf] = tr
	db.dictCurrentCached.Store(41)
	db.dictCurrentOps.Store(77)

	db.applyValueLogDictProfileForClass(vlogDictClassOuterLeaf)

	if got := db.dictCurrentCached.Load(); got == 41 {
		t.Fatalf("expected global dict cache to refresh on legacy fallback publish, got stale=%d", got)
	}
	if got := db.dictCurrentOps.Load(); got != 0 {
		t.Fatalf("expected global dict cache ops to reset, got=%d", got)
	}
	if got := store.currentID; got != db.dictCurrentCached.Load() {
		t.Fatalf("expected global cache to mirror store current dict id, store=%d cached=%d", got, db.dictCurrentCached.Load())
	}
}

func TestApplyValueLogDictProfileForClass_ClassWriterWithoutClassReaderFallsBackToGlobalCurrent(t *testing.T) {
	tr := &compression.Trainer{}
	dictBytes := []byte("outer-leaf-dictionary")
	dictHash := xxhash.Sum64(dictBytes)
	tr.AcceptProfile(&compression.ActiveProfile{
		DictHash:     dictHash,
		DictBytes:    len(dictBytes),
		Dict:         dictBytes,
		K:            8,
		PayloadRatio: 0.6,
		TotalRatio:   0.6,
		Timestamp:    time.Now(),
	})

	store := &classWriterOnlyDictStoreForClassPublishTest{
		legacyDictStoreForClassPublishTest: &legacyDictStoreForClassPublishTest{
			currentID: 41,
			dicts:     map[uint64][]byte{41: []byte("old-dict")},
			nextID:    41,
		},
	}
	db := &DB{
		dictStore:             store,
		valueLogDictClassMode: uint8(vlogDictClassModeSplitOuterLeaf),
	}
	db.valueLogDictTrainerByClass[vlogDictClassOuterLeaf] = tr
	db.dictCurrentCached.Store(41)
	db.dictCurrentOps.Store(99)

	db.applyValueLogDictProfileForClass(vlogDictClassOuterLeaf)

	if got := store.currentID; got == 41 {
		t.Fatalf("expected global current marker update, got stale=%d", got)
	}
	if got := db.dictCurrentCached.Load(); got != store.currentID {
		t.Fatalf("expected global cache to match updated store current, cached=%d store=%d", got, store.currentID)
	}
	if got := db.dictCurrentOps.Load(); got != 0 {
		t.Fatalf("expected global cache ops reset, got=%d", got)
	}
	if got := len(store.classCurrent); got != 0 {
		t.Fatalf("expected class-specific marker not to be used without class reader, writes=%d", got)
	}
}

func TestApplyValueLogDictProfileForClass_SingleClassUpdatesGlobalAndClassCurrent(t *testing.T) {
	tr := &compression.Trainer{}
	dictBytes := []byte("single-value-dictionary")
	dictHash := xxhash.Sum64(dictBytes)
	tr.AcceptProfile(&compression.ActiveProfile{
		DictHash:     dictHash,
		DictBytes:    len(dictBytes),
		Dict:         dictBytes,
		K:            8,
		PayloadRatio: 0.6,
		TotalRatio:   0.6,
		Timestamp:    time.Now(),
	})

	store := &classReadWriteDictStoreForClassPublishTest{
		legacyDictStoreForClassPublishTest: &legacyDictStoreForClassPublishTest{
			currentID: 55,
			dicts:     map[uint64][]byte{55: []byte("old-dict")},
			nextID:    55,
		},
		classCurrent: map[string]uint64{vlogDictClassSuffix(vlogDictClassSingleValue): 55},
	}
	db := &DB{
		dictStore:             store,
		valueLogDictClassMode: uint8(vlogDictClassModeSplitOuterLeaf),
	}
	db.valueLogDictTrainerByClass[vlogDictClassSingleValue] = tr

	db.applyValueLogDictProfileForClass(vlogDictClassSingleValue)

	if store.currentID == 55 {
		t.Fatalf("expected global current marker to update for single class publish")
	}
	classKey := vlogDictClassSuffix(vlogDictClassSingleValue)
	if got := store.classCurrent[classKey]; got != store.currentID {
		t.Fatalf("expected class current to match global current, class=%d global=%d", got, store.currentID)
	}
}

func TestSetDictStore_SplitModeClassZeroUsesGlobalCurrent(t *testing.T) {
	store := &classReadWriteDictStoreForClassPublishTest{
		legacyDictStoreForClassPublishTest: &legacyDictStoreForClassPublishTest{
			currentID: 77,
			dicts:     map[uint64][]byte{77: []byte("global-dict")},
			nextID:    77,
		},
		classCurrent: map[string]uint64{},
	}
	db := &DB{valueLogDictClassMode: uint8(vlogDictClassModeSplitOuterLeaf)}

	db.SetDictStore(store)

	if got := db.dictCurrentCached.Load(); got != 77 {
		t.Fatalf("expected global dict current=77, got %d", got)
	}
	if got := db.dictCurrentCachedByClass[vlogDictClassSingleValue].Load(); got != 77 {
		t.Fatalf("expected single-value class current to fall back to global=77, got %d", got)
	}
	if got := db.dictCurrentCachedByClass[vlogDictClassOuterLeaf].Load(); got != 77 {
		t.Fatalf("expected outer-leaf class current to fall back to global=77, got %d", got)
	}
}

func TestCurrentDictIDForClass_SplitModeClassZeroRefreshesGlobalCurrent(t *testing.T) {
	store := &classReadWriteDictStoreForClassPublishTest{
		legacyDictStoreForClassPublishTest: &legacyDictStoreForClassPublishTest{
			currentID: 99,
			dicts:     map[uint64][]byte{99: []byte("global-dict")},
			nextID:    99,
		},
		classCurrent: map[string]uint64{},
	}
	db := &DB{
		dictStore:             store,
		valueLogDictClassMode: uint8(vlogDictClassModeSplitOuterLeaf),
	}
	db.dictCurrentCached.Store(88)
	db.dictCurrentCachedByClass[vlogDictClassOuterLeaf].Store(88)
	db.dictCurrentOpsByClass[vlogDictClassOuterLeaf].Store((1 << 16) - 1) // force refresh

	dictID, err := db.currentDictIDForClass(context.Background(), vlogDictClassOuterLeaf)
	if err != nil {
		t.Fatalf("currentDictIDForClass: %v", err)
	}
	if dictID != 99 {
		t.Fatalf("expected class refresh to read global current=99 on class miss, got %d", dictID)
	}
	if got := db.dictCurrentCachedByClass[vlogDictClassOuterLeaf].Load(); got != 99 {
		t.Fatalf("expected outer-leaf cache to refresh to global current=99, got %d", got)
	}
	if got := db.dictCurrentCached.Load(); got != 99 {
		t.Fatalf("expected global cache to refresh to store current=99, got %d", got)
	}
}
