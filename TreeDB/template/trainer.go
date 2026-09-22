package template

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type trainer struct {
	cfg            Config
	stats          *TemplateStats
	defCache       *defCache
	totalTemplates *atomic.Uint64

	ingestCh chan trainSample
	done     chan struct{}
	closed   atomic.Bool
	wg       sync.WaitGroup
	once     sync.Once

	shards []trainShard
}

type trainSample struct {
	store Store
	value []byte
}

type trainTask struct {
	store     Store
	bucketKey uint64
	value     []byte
}

type pendingPublish struct {
	store       Store
	bucket      *bucket
	samplesSeen int
	def         TemplateDef
	defBytes    []byte
	routeFPs    []uint64
}

type trainShard struct {
	cfg            Config
	stats          *TemplateStats
	defCache       *defCache
	totalTemplates *atomic.Uint64

	in         chan trainTask
	buckets    map[uint64]*bucket
	maxBuckets int
	bucketSeq  uint64

	pending []pendingPublish
}

func newTrainer(e *Engine) *trainer {
	if e == nil {
		return nil
	}
	cfg := NormalizeConfig(e.cfg)
	t := &trainer{
		cfg:            cfg,
		stats:          &e.stats,
		defCache:       e.defCache,
		totalTemplates: &e.totalTemplates,
		ingestCh:       make(chan trainSample, cfg.TrainQueueSize),
		done:           make(chan struct{}),
	}

	shards := make([]trainShard, cfg.TrainShards)
	maxBuckets := cfg.MaxBuckets
	if maxBuckets < 1 {
		maxBuckets = 1
	}
	if cfg.TrainShards < 1 {
		cfg.TrainShards = 1
	}
	baseBuckets := maxBuckets / cfg.TrainShards
	rem := maxBuckets % cfg.TrainShards
	for i := range shards {
		limit := baseBuckets
		if i < rem {
			limit++
		}
		if limit < 1 {
			limit = 1
		}
		shards[i] = trainShard{
			cfg:            cfg,
			stats:          &e.stats,
			defCache:       e.defCache,
			totalTemplates: &e.totalTemplates,
			in:             make(chan trainTask, cfg.TrainShardQueueSize),
			buckets:        make(map[uint64]*bucket),
			maxBuckets:     limit,
		}
	}
	t.shards = shards

	for i := 0; i < cfg.TrainRouters; i++ {
		t.wg.Add(1)
		go t.runRouter()
	}
	for i := range t.shards {
		shard := &t.shards[i]
		t.wg.Add(1)
		go shard.run(t.done, &t.wg)
	}
	return t
}

func (t *trainer) Close() {
	if t == nil {
		return
	}
	t.once.Do(func() {
		t.closed.Store(true)
		close(t.done)
	})
	t.wg.Wait()
}

func (t *trainer) Enqueue(store Store, value []byte) {
	if t == nil || store == nil || t.closed.Load() {
		return
	}
	cfg := t.cfg
	if cfg.FingerprintK <= 0 || len(value) < cfg.FingerprintK {
		return
	}
	if cap(t.ingestCh) > 0 && len(t.ingestCh) == cap(t.ingestCh) {
		t.stats.TrainDroppedQueueFull.Add(1)
		return
	}
	cp := append([]byte(nil), value...)
	select {
	case t.ingestCh <- trainSample{store: store, value: cp}:
		t.stats.TrainEnqueued.Add(1)
	default:
		t.stats.TrainDroppedQueueFull.Add(1)
	}
}

func (t *trainer) runRouter() {
	defer t.wg.Done()
	cfg := t.cfg
	shardCount := len(t.shards)
	if shardCount == 0 {
		return
	}
	for {
		select {
		case <-t.done:
			// Drain to avoid holding references to large byte slices after Close.
			for {
				select {
				case sample := <-t.ingestCh:
					_ = sample
				default:
					return
				}
			}
		case sample := <-t.ingestCh:
			if sample.store == nil || len(sample.value) < cfg.FingerprintK {
				continue
			}
			fps := BucketFingerprints(sample.value, cfg)
			if len(fps) == 0 {
				fps = Fingerprints(sample.value, cfg)
			}
			if len(fps) == 0 {
				continue
			}
			key := BucketKey(fps)
			if key == 0 {
				continue
			}
			idx := int(key % uint64(shardCount))
			task := trainTask{store: sample.store, bucketKey: key, value: sample.value}
			select {
			case t.shards[idx].in <- task:
				t.stats.TrainRouted.Add(1)
			default:
				t.stats.TrainDroppedShardFull.Add(1)
			}
		}
	}
}

func (s *trainShard) run(done <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(s.cfg.PublishFlushEvery)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			for {
				select {
				case task := <-s.in:
					_ = task
				default:
					goto drained
				}
			}
		drained:
			s.flushPublishes(context.Background())
			return
		case <-ticker.C:
			s.flushPublishes(context.Background())
		case task := <-s.in:
			s.process(task)
		}
	}
}

func (s *trainShard) process(task trainTask) {
	cfg := s.cfg
	if task.store == nil {
		return
	}
	b := s.buckets[task.bucketKey]
	if b == nil {
		if len(s.buckets) >= s.maxBuckets {
			s.evictOldestBucket()
		}
		b = &bucket{key: task.bucketKey}
		s.buckets[task.bucketKey] = b
	}
	s.bucketSeq++
	b.lastSeen = s.bucketSeq

	// Add sample (already copied).
	b.samples = append(b.samples, sample{value: task.value})
	b.sampleBytes += len(task.value)
	b.samplesSeen++
	for (cfg.MaxValuesPerBucket > 0 && len(b.samples) > cfg.MaxValuesPerBucket) || (cfg.MaxBytesPerBucket > 0 && b.sampleBytes > cfg.MaxBytesPerBucket) {
		old := b.samples[0]
		b.samples = b.samples[1:]
		b.sampleBytes -= len(old.value)
	}
	s.stats.TrainProcessed.Add(1)

	if cfg.SynthesizeEverySamples <= 0 || b.samplesSeen%cfg.SynthesizeEverySamples != 0 {
		return
	}
	if b.publishPending {
		return
	}
	if cfg.CooldownValues > 0 && b.samplesSeen-b.lastPublishSample < cfg.CooldownValues {
		return
	}
	if cfg.MaxTemplatesPerBucket > 0 && b.templatesPublished >= cfg.MaxTemplatesPerBucket {
		return
	}
	if cfg.MaxTemplatesTotal > 0 && s.totalTemplates.Load() >= uint64(cfg.MaxTemplatesTotal) {
		return
	}
	def, routeValue, activated, ok := synthesizeTemplate(b.samples, cfg)
	if !ok || !activated {
		return
	}
	defBytes, err := EncodeTemplateDef(def, cfg)
	if err != nil {
		return
	}
	routeFPs := RoutingFingerprints(routeValue, cfg)
	if len(routeFPs) == 0 {
		return
	}
	routeFPs = append(routeFPs, RoutingFingerprintsLegacy(routeValue, cfg)...)

	b.publishPending = true
	s.pending = append(s.pending, pendingPublish{
		store:       task.store,
		bucket:      b,
		samplesSeen: b.samplesSeen,
		def:         def,
		defBytes:    defBytes,
		routeFPs:    routeFPs,
	})
	if len(s.pending) >= cfg.PublishBatchSize {
		s.flushPublishes(context.Background())
	}
}

func (s *trainShard) flushPublishes(ctx context.Context) {
	if len(s.pending) == 0 {
		return
	}
	pending := s.pending
	s.pending = nil

	s.stats.PublishBatches.Add(1)
	s.stats.PublishDefs.Add(uint64(len(pending)))

	// Fast path: common case is a single store per engine.
	firstStore := pending[0].store
	sameStore := true
	for i := 1; i < len(pending); i++ {
		if pending[i].store != firstStore {
			sameStore = false
			break
		}
	}
	if sameStore {
		s.publishBatch(ctx, firstStore, pending)
		return
	}
	for _, item := range pending {
		s.publishBatch(ctx, item.store, []pendingPublish{item})
	}
}

func (s *trainShard) publishBatch(ctx context.Context, store Store, items []pendingPublish) {
	if store == nil || len(items) == 0 {
		for i := range items {
			if items[i].bucket != nil {
				items[i].bucket.publishPending = false
			}
		}
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if bp, ok := store.(BatchPublisher); ok && len(items) > 1 {
		specs := make([]PublishSpec, len(items))
		for i := range items {
			specs[i] = PublishSpec{DefBytes: items[i].defBytes, RouteFPs: items[i].routeFPs}
		}
		ids, err := bp.PutTemplateDefs(ctx, specs)
		if err != nil || len(ids) != len(items) {
			s.stats.PublishErrors.Add(1)
			for i := range items {
				if items[i].bucket != nil {
					items[i].bucket.publishPending = false
				}
			}
			return
		}
		for i := range ids {
			s.onPublished(ids[i], items[i])
		}
		return
	}
	for i := range items {
		id, err := store.PutTemplateDef(ctx, items[i].defBytes, items[i].routeFPs)
		if err != nil {
			s.stats.PublishErrors.Add(1)
			if items[i].bucket != nil {
				items[i].bucket.publishPending = false
			}
			continue
		}
		s.onPublished(id, items[i])
	}
}

func (s *trainShard) onPublished(id uint64, pub pendingPublish) {
	if id != 0 {
		if cache := s.defCache; cache != nil {
			cache.Add(id, pub.def)
		}
	}
	if pub.bucket != nil {
		pub.bucket.lastPublishSample = pub.samplesSeen
		pub.bucket.templatesPublished++
		pub.bucket.publishPending = false
	}
	s.totalTemplates.Add(1)
	s.stats.TemplatesPublished.Add(1)
}

func (s *trainShard) evictOldestBucket() {
	var (
		oldestKey  uint64
		oldestSeen uint64
		set        bool
	)
	for k, b := range s.buckets {
		if b == nil {
			continue
		}
		if !set || b.lastSeen < oldestSeen || (b.lastSeen == oldestSeen && k < oldestKey) {
			oldestKey = k
			oldestSeen = b.lastSeen
			set = true
		}
	}
	if set {
		delete(s.buckets, oldestKey)
	}
}
