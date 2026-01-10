package slab

import (
	"log"
	"math"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
)

const (
	defaultCompressionTrainBytes          = 1 << 20
	defaultCompressionTrainDictBytes      = 32 << 10
	defaultCompressionTrainMinRecords     = 64
	defaultCompressionTrainMaxRecordBytes = 64 << 10
	defaultCompressionTrainQueue          = 128
)

type compressionTrainer struct {
	enabled     atomic.Bool
	collecting  atomic.Bool
	training    atomic.Bool
	targetBytes uint64
	minRecords  uint64
	maxRecord   int
	dictBytes   int
	level       zstd.EncoderLevel
	logged      atomic.Bool

	mu            sync.Mutex
	sampleBytes   uint64
	sampleRecords uint64
	samples       []pooledSample
	lastSlabID    uint32

	sampleGen  atomic.Uint64
	sampleCh   chan trainerSample
	samplePool sync.Pool
	closed     atomic.Bool
	closeOnce  sync.Once

	enqueued         atomic.Uint64
	dropped          atomic.Uint64
	maxQueueLen      atomic.Uint64
	trainCount       atomic.Uint64
	lastTrainRatio   atomic.Uint64
	lastTrainSamples atomic.Uint64
	lastTrainDict    atomic.Uint64
}

type trainerSample struct {
	gen    uint64
	sample []byte
	buf    []byte
}

type pooledSample struct {
	data []byte
	buf  []byte
}

type CompressionTrainerStats struct {
	Enabled          bool
	Collecting       bool
	Training         bool
	QueueLen         int
	QueueCap         int
	Enqueued         uint64
	Dropped          uint64
	MaxQueueLen      uint64
	TrainCount       uint64
	LastTrainRatio   float64
	LastTrainSamples uint64
	LastTrainDict    uint64
}

func newCompressionTrainer(opts Options, cfg compressionConfig, readOnly bool) *compressionTrainer {
	if readOnly || opts.CompressionAdaptiveTrainBytes == 0 {
		return nil
	}
	if cfg.kind != CompressionZSTD {
		return nil
	}
	target := opts.CompressionAdaptiveTrainBytes
	if target < 0 {
		return nil
	}
	if target == 0 {
		target = defaultCompressionTrainBytes
	}
	dictBytes := opts.CompressionAdaptiveTrainDictBytes
	if dictBytes <= 0 {
		dictBytes = defaultCompressionTrainDictBytes
	}
	minRecords := opts.CompressionAdaptiveTrainMinRecords
	if minRecords <= 0 {
		minRecords = defaultCompressionTrainMinRecords
	}
	maxRecord := opts.CompressionAdaptiveTrainMaxRecordBytes
	if maxRecord <= 0 {
		maxRecord = defaultCompressionTrainMaxRecordBytes
	}

	trainer := &compressionTrainer{
		targetBytes: uint64(target),
		minRecords:  uint64(minRecords),
		maxRecord:   maxRecord,
		dictBytes:   dictBytes,
		level:       cfg.level,
		sampleCh:    make(chan trainerSample, defaultCompressionTrainQueue),
	}
	trainer.samplePool.New = func() any {
		return make([]byte, 0, maxRecord)
	}
	trainer.enabled.Store(true)
	go trainer.run()
	return trainer
}

func (t *compressionTrainer) Close() {
	if t == nil {
		return
	}
	t.closeOnce.Do(func() {
		t.closed.Store(true)
		close(t.sampleCh)
	})
}

func (t *compressionTrainer) signalDegraded(slabID uint32) {
	if t == nil || !t.enabled.Load() {
		return
	}
	if t.training.Load() || t.collecting.Load() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.training.Load() || t.collecting.Load() {
		return
	}
	t.samples = nil
	t.sampleBytes = 0
	t.sampleRecords = 0
	t.lastSlabID = slabID
	t.sampleGen.Add(1)
	t.collecting.Store(true)
}

func (t *compressionTrainer) collect(value []byte) {
	if t == nil || t.closed.Load() || !t.collecting.Load() || len(value) == 0 {
		return
	}
	if len(t.sampleCh) == cap(t.sampleCh) {
		t.dropped.Add(1)
		return
	}
	sample := value
	if len(sample) > t.maxRecord {
		sample = sample[:t.maxRecord]
	}
	buf := t.samplePool.Get().([]byte)
	if cap(buf) < len(sample) {
		buf = make([]byte, 0, t.maxRecord)
		if cap(buf) < len(sample) {
			buf = make([]byte, 0, len(sample))
		}
	}
	cp := buf[:len(sample)]
	copy(cp, sample)
	gen := t.sampleGen.Load()
	select {
	case t.sampleCh <- trainerSample{gen: gen, sample: cp, buf: buf[:0]}:
		t.enqueued.Add(1)
	default:
		t.dropped.Add(1)
		t.recycleBuffer(buf)
	}
	t.updateMaxQueueLen()
}

func (t *compressionTrainer) run() {
	for sample := range t.sampleCh {
		t.appendSample(sample)
	}
}

func (t *compressionTrainer) appendSample(sample trainerSample) {
	if t == nil || !t.collecting.Load() {
		t.recycleSample(sample)
		return
	}
	if sample.gen != t.sampleGen.Load() {
		t.recycleSample(sample)
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.collecting.Load() || sample.gen != t.sampleGen.Load() {
		t.recycleSample(sample)
		return
	}
	t.samples = append(t.samples, pooledSample{data: sample.sample, buf: sample.buf})
	t.sampleBytes += uint64(len(sample.sample))
	t.sampleRecords++

	if t.sampleBytes < t.targetBytes || t.sampleRecords < t.minRecords {
		return
	}
	samples := t.samples
	slabID := t.lastSlabID
	dictBytes := t.dictBytes
	level := t.level
	t.samples = nil
	t.collecting.Store(false)
	t.training.Store(true)
	go t.train(samples, dictBytes, level, slabID)
}

func (t *compressionTrainer) train(samples []pooledSample, dictBytes int, level zstd.EncoderLevel, slabID uint32) {
	defer t.training.Store(false)
	defer t.recycleSamples(samples)
	if len(samples) == 0 {
		return
	}

	rawTotal := 0
	for _, sample := range samples {
		rawTotal += len(sample.data)
	}
	if rawTotal < 8 {
		return
	}
	if dictBytes > rawTotal {
		dictBytes = rawTotal
	}
	history := make([]byte, 0, dictBytes)
	for _, sample := range samples {
		if len(history) >= dictBytes {
			break
		}
		need := dictBytes - len(history)
		if len(sample.data) > need {
			history = append(history, sample.data[:need]...)
		} else {
			history = append(history, sample.data...)
		}
	}
	if len(history) < 8 {
		return
	}

	t.trainCount.Add(1)

	dictID := slabID + 1
	if dictID == 0 {
		dictID = 1
	}
	contents := make([][]byte, 0, len(samples))
	for _, sample := range samples {
		contents = append(contents, sample.data)
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       dictID,
		Contents: contents,
		History:  history,
		Level:    level,
	})
	if err != nil {
		log.Printf("treedb: slab compression training failed slab=%d err=%v", slabID, err)
		return
	}

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level), zstd.WithEncoderCRC(false), zstd.WithEncoderDict(dict))
	if err != nil {
		log.Printf("treedb: slab compression training encode setup failed slab=%d err=%v", slabID, err)
		return
	}
	defer enc.Close()

	storedTotal := 0
	for _, sample := range samples {
		storedTotal += len(enc.EncodeAll(sample.data, nil))
	}
	ratio := float64(storedTotal) / float64(rawTotal)
	t.lastTrainRatio.Store(math.Float64bits(ratio))
	t.lastTrainSamples.Store(uint64(len(samples)))
	t.lastTrainDict.Store(uint64(len(dict)))
	if t.logOnce() {
		log.Printf("treedb: slab compression trained dict slab=%d dict_bytes=%d samples=%d raw=%d stored=%d ratio=%.3f",
			slabID,
			len(dict),
			len(samples),
			rawTotal,
			storedTotal,
			ratio,
		)
	}
}

func (t *compressionTrainer) logOnce() bool {
	if t == nil {
		return false
	}
	return t.logged.CompareAndSwap(false, true)
}

func (t *compressionTrainer) recycleSample(sample trainerSample) {
	if t == nil || sample.buf == nil {
		return
	}
	t.samplePool.Put(sample.buf[:0])
}

func (t *compressionTrainer) recycleBuffer(buf []byte) {
	if t == nil || buf == nil {
		return
	}
	t.samplePool.Put(buf[:0])
}

func (t *compressionTrainer) recycleSamples(samples []pooledSample) {
	if t == nil {
		return
	}
	for _, sample := range samples {
		if sample.buf == nil {
			continue
		}
		t.samplePool.Put(sample.buf[:0])
	}
}

func (t *compressionTrainer) updateMaxQueueLen() {
	if t == nil {
		return
	}
	cur := uint64(len(t.sampleCh))
	for {
		prev := t.maxQueueLen.Load()
		if cur <= prev {
			return
		}
		if t.maxQueueLen.CompareAndSwap(prev, cur) {
			return
		}
	}
}

func (t *compressionTrainer) stats() CompressionTrainerStats {
	if t == nil {
		return CompressionTrainerStats{}
	}
	return CompressionTrainerStats{
		Enabled:          t.enabled.Load(),
		Collecting:       t.collecting.Load(),
		Training:         t.training.Load(),
		QueueLen:         len(t.sampleCh),
		QueueCap:         cap(t.sampleCh),
		Enqueued:         t.enqueued.Load(),
		Dropped:          t.dropped.Load(),
		MaxQueueLen:      t.maxQueueLen.Load(),
		TrainCount:       t.trainCount.Load(),
		LastTrainRatio:   math.Float64frombits(t.lastTrainRatio.Load()),
		LastTrainSamples: t.lastTrainSamples.Load(),
		LastTrainDict:    t.lastTrainDict.Load(),
	}
}
