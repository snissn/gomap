package template

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type trainerNoopStore struct{}

func (trainerNoopStore) GetCandidates(context.Context, uint64, int) ([]Candidate, error) {
	return nil, nil
}
func (trainerNoopStore) GetTemplateDef(context.Context, uint64) ([]byte, error) {
	return nil, ErrMissingTemplate
}
func (trainerNoopStore) PutTemplateDef(context.Context, []byte, []uint64) (uint64, error) {
	return 1, nil
}

type trainerFallbackStore struct {
	ids  []uint64
	errs []error

	putCalls int
}

func (s *trainerFallbackStore) GetCandidates(context.Context, uint64, int) ([]Candidate, error) {
	return nil, nil
}
func (s *trainerFallbackStore) GetTemplateDef(context.Context, uint64) ([]byte, error) {
	return nil, ErrMissingTemplate
}
func (s *trainerFallbackStore) PutTemplateDef(context.Context, []byte, []uint64) (uint64, error) {
	i := s.putCalls
	s.putCalls++
	if i < len(s.errs) && s.errs[i] != nil {
		return 0, s.errs[i]
	}
	if i < len(s.ids) {
		return s.ids[i], nil
	}
	return uint64(i + 1), nil
}

type trainerBatchStore struct {
	ids      []uint64
	batchErr error

	batchCalls int
	putCalls   int
}

func (s *trainerBatchStore) GetCandidates(context.Context, uint64, int) ([]Candidate, error) {
	return nil, nil
}
func (s *trainerBatchStore) GetTemplateDef(context.Context, uint64) ([]byte, error) {
	return nil, ErrMissingTemplate
}
func (s *trainerBatchStore) PutTemplateDef(context.Context, []byte, []uint64) (uint64, error) {
	s.putCalls++
	return 999, nil
}
func (s *trainerBatchStore) PutTemplateDefs(context.Context, []PublishSpec) ([]uint64, error) {
	s.batchCalls++
	if s.batchErr != nil {
		return nil, s.batchErr
	}
	return append([]uint64(nil), s.ids...), nil
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(1 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition not met within %v", timeout)
	}
}

func TestEngineObserveTrainingStrideAndTooLarge(t *testing.T) {
	store := trainerNoopStore{}

	e := &Engine{cfg: NormalizeConfig(Config{
		FingerprintK:       4,
		TrainSampleStride:  2,
		TrainMaxValueBytes: 64,
	})}
	e.observeTraining([]byte("abcd"), store) // seq=1: dropped by stride
	if got := e.stats.TrainEnqueueAttempts.Load(); got != 0 {
		t.Fatalf("TrainEnqueueAttempts=%d, want 0 on stride miss", got)
	}

	e.observeTraining([]byte("abcd"), store) // seq=2: accepted
	if got := e.stats.TrainEnqueueAttempts.Load(); got != 1 {
		t.Fatalf("TrainEnqueueAttempts=%d, want 1", got)
	}

	e2 := &Engine{cfg: NormalizeConfig(Config{
		FingerprintK:       4,
		TrainSampleStride:  1,
		TrainMaxValueBytes: 3,
	})}
	e2.observeTraining([]byte("abcd"), store)
	if got := e2.stats.TrainEnqueueAttempts.Load(); got != 1 {
		t.Fatalf("TrainEnqueueAttempts=%d, want 1", got)
	}
	if got := e2.stats.TrainDroppedTooLarge.Load(); got != 1 {
		t.Fatalf("TrainDroppedTooLarge=%d, want 1", got)
	}
}

func TestTrainerEnqueueCopiesInputAndDropsWhenQueueFull(t *testing.T) {
	stats := &TemplateStats{}
	tr := &trainer{
		cfg:      NormalizeConfig(Config{FingerprintK: 4}),
		stats:    stats,
		ingestCh: make(chan trainSample, 1),
	}
	store := trainerNoopStore{}

	value := []byte("abcd")
	tr.Enqueue(store, value)
	value[0] = 'z'

	sample := <-tr.ingestCh
	if string(sample.value) != "abcd" {
		t.Fatalf("enqueue did not copy input, got %q", sample.value)
	}
	if got := stats.TrainEnqueued.Load(); got != 1 {
		t.Fatalf("TrainEnqueued=%d, want 1", got)
	}

	tr.ingestCh <- trainSample{store: store, value: []byte("full")}
	tr.Enqueue(store, []byte("abcd"))
	if got := stats.TrainDroppedQueueFull.Load(); got != 1 {
		t.Fatalf("TrainDroppedQueueFull=%d, want 1", got)
	}
}

func TestTrainShardEvictOldestBucketTieBreak(t *testing.T) {
	s := trainShard{
		buckets: map[uint64]*bucket{
			5: {lastSeen: 1},
			3: {lastSeen: 1},
			9: {lastSeen: 2},
		},
	}
	s.evictOldestBucket()
	if _, ok := s.buckets[3]; ok {
		t.Fatalf("expected bucket key 3 to be evicted (tie-break on lower key)")
	}
	if len(s.buckets) != 2 {
		t.Fatalf("len(buckets)=%d, want 2", len(s.buckets))
	}
}

func TestTrainShardProcessCapsBucketSizeAndBytes(t *testing.T) {
	stats := &TemplateStats{}
	total := &atomic.Uint64{}
	s := trainShard{
		cfg: NormalizeConfig(Config{
			MaxValuesPerBucket:     2,
			MaxBytesPerBucket:      5,
			SynthesizeEverySamples: 1000,
		}),
		stats:          stats,
		totalTemplates: total,
		buckets:        make(map[uint64]*bucket),
		maxBuckets:     4,
	}
	store := trainerNoopStore{}

	s.process(trainTask{store: store, bucketKey: 1, value: []byte("aaa")})
	s.process(trainTask{store: store, bucketKey: 1, value: []byte("bbb")})
	s.process(trainTask{store: store, bucketKey: 1, value: []byte("ccc")})

	b := s.buckets[1]
	if b == nil {
		t.Fatalf("expected bucket 1")
	}
	if b.samplesSeen != 3 {
		t.Fatalf("samplesSeen=%d, want 3", b.samplesSeen)
	}
	if len(b.samples) != 1 {
		t.Fatalf("len(samples)=%d, want 1 due to byte cap", len(b.samples))
	}
	if b.sampleBytes != 3 {
		t.Fatalf("sampleBytes=%d, want 3", b.sampleBytes)
	}
	if got := stats.TrainProcessed.Load(); got != 3 {
		t.Fatalf("TrainProcessed=%d, want 3", got)
	}
}

func TestTrainShardPublishBatch_BatchPublisherSuccessAndFailure(t *testing.T) {
	stats := &TemplateStats{}
	total := &atomic.Uint64{}
	cache := newDefCache(8)
	s := trainShard{
		cfg:            NormalizeConfig(Config{}),
		stats:          stats,
		defCache:       cache,
		totalTemplates: total,
	}

	def := TemplateDef{Kind: TemplateAnchors, Anchors: [][]byte{[]byte("aa")}}
	b1 := &bucket{publishPending: true}
	b2 := &bucket{publishPending: true}
	items := []pendingPublish{
		{store: &trainerBatchStore{}, bucket: b1, samplesSeen: 10, def: def, defBytes: []byte("d1"), routeFPs: []uint64{1}},
		{store: &trainerBatchStore{}, bucket: b2, samplesSeen: 11, def: def, defBytes: []byte("d2"), routeFPs: []uint64{2}},
	}

	okStore := &trainerBatchStore{ids: []uint64{11, 12}}
	s.publishBatch(context.Background(), okStore, items)
	if okStore.batchCalls != 1 {
		t.Fatalf("batchCalls=%d, want 1", okStore.batchCalls)
	}
	if got := stats.TemplatesPublished.Load(); got != 2 {
		t.Fatalf("TemplatesPublished=%d, want 2", got)
	}
	if got := total.Load(); got != 2 {
		t.Fatalf("totalTemplates=%d, want 2", got)
	}
	if b1.publishPending || b2.publishPending {
		t.Fatalf("publishPending should be reset on success")
	}
	if b1.templatesPublished != 1 || b2.templatesPublished != 1 {
		t.Fatalf("templatesPublished per bucket not updated: %d %d", b1.templatesPublished, b2.templatesPublished)
	}
	if _, ok := cache.Get(11); !ok {
		t.Fatalf("expected defCache entry for id 11")
	}

	b3 := &bucket{publishPending: true}
	b4 := &bucket{publishPending: true}
	failItems := []pendingPublish{
		{store: &trainerBatchStore{}, bucket: b3, def: def, defBytes: []byte("d3"), routeFPs: []uint64{3}},
		{store: &trainerBatchStore{}, bucket: b4, def: def, defBytes: []byte("d4"), routeFPs: []uint64{4}},
	}
	badStore := &trainerBatchStore{ids: []uint64{33}} // length mismatch triggers failure path
	s.publishBatch(context.Background(), badStore, failItems)
	if got := stats.PublishErrors.Load(); got != 1 {
		t.Fatalf("PublishErrors=%d, want 1", got)
	}
	if b3.publishPending || b4.publishPending {
		t.Fatalf("publishPending should be reset on batch failure")
	}
}

func TestTrainShardPublishBatch_FallbackStorePath(t *testing.T) {
	stats := &TemplateStats{}
	total := &atomic.Uint64{}
	s := trainShard{
		cfg:            NormalizeConfig(Config{}),
		stats:          stats,
		defCache:       newDefCache(4),
		totalTemplates: total,
	}

	store := &trainerFallbackStore{
		ids:  []uint64{0, 21},
		errs: []error{errors.New("put-fail"), nil},
	}
	b1 := &bucket{publishPending: true}
	b2 := &bucket{publishPending: true}
	def := TemplateDef{Kind: TemplateAnchors, Anchors: [][]byte{[]byte("aa")}}
	items := []pendingPublish{
		{store: store, bucket: b1, samplesSeen: 3, def: def, defBytes: []byte("a"), routeFPs: []uint64{1}},
		{store: store, bucket: b2, samplesSeen: 4, def: def, defBytes: []byte("b"), routeFPs: []uint64{2}},
	}

	s.publishBatch(context.Background(), store, items)

	if got := stats.PublishErrors.Load(); got != 1 {
		t.Fatalf("PublishErrors=%d, want 1", got)
	}
	if got := stats.TemplatesPublished.Load(); got != 1 {
		t.Fatalf("TemplatesPublished=%d, want 1", got)
	}
	if got := total.Load(); got != 1 {
		t.Fatalf("totalTemplates=%d, want 1", got)
	}
	if b1.publishPending {
		t.Fatalf("b1 publishPending should reset after error")
	}
	if b2.publishPending {
		t.Fatalf("b2 publishPending should reset after success")
	}
}

func TestTrainerRunRouterRoutesAndDropsWhenShardFull(t *testing.T) {
	stats := &TemplateStats{}
	done := make(chan struct{})
	t1 := &trainer{
		cfg:      NormalizeConfig(Config{FingerprintK: 4}),
		stats:    stats,
		done:     done,
		ingestCh: make(chan trainSample, 1),
		shards: []trainShard{
			{in: make(chan trainTask, 1)},
		},
	}
	store := trainerNoopStore{}
	t1.ingestCh <- trainSample{store: store, value: []byte("abcd")}
	t1.wg.Add(1)
	go t1.runRouter()
	waitUntil(t, 250*time.Millisecond, func() bool {
		return stats.TrainRouted.Load() == 1 && len(t1.shards[0].in) == 1
	})
	close(done)
	t1.wg.Wait()

	stats2 := &TemplateStats{}
	done2 := make(chan struct{})
	// Unbuffered shard input with no receiver forces default/drop path.
	t2 := &trainer{
		cfg:      NormalizeConfig(Config{FingerprintK: 4}),
		stats:    stats2,
		done:     done2,
		ingestCh: make(chan trainSample, 1),
		shards: []trainShard{
			{in: make(chan trainTask)},
		},
	}
	t2.ingestCh <- trainSample{store: store, value: []byte("wxyz")}
	t2.wg.Add(1)
	go t2.runRouter()
	waitUntil(t, 250*time.Millisecond, func() bool {
		return stats2.TrainDroppedShardFull.Load() == 1
	})
	close(done2)
	t2.wg.Wait()
}
