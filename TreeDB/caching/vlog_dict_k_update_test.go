package caching

import (
	"context"
	"errors"
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
