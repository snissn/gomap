package caching

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/template"
)

// VlogAutotuneBenchMode controls the deterministic bench mode.
type VlogAutotuneBenchMode string

const (
	VlogAutotuneBenchOff         VlogAutotuneBenchMode = "off"
	VlogAutotuneBenchNoDictFixed VlogAutotuneBenchMode = "no_dict_fixed"
	VlogAutotuneBenchDictFixed   VlogAutotuneBenchMode = "dict_fixed"
	VlogAutotuneBenchAutotune    VlogAutotuneBenchMode = "autotune"
	VlogAutotuneBenchTemplate    VlogAutotuneBenchMode = "template_fixed"
)

type VlogAutotuneBenchSegment struct {
	Name               string
	Workload           valuelog.AutotuneWorkload
	ValueSize          int
	Records            int
	EncodeNsPerRawByte float64
	IoNsPerStoredByte  float64
}

type VlogAutotuneBenchRequest struct {
	Mode     VlogAutotuneBenchMode
	FixedK   int
	Segments []VlogAutotuneBenchSegment
}

type VlogAutotuneBenchSegmentResult struct {
	Name               string
	RawBytes           uint64
	StoredBytes        uint64
	WallTimeNs         int64
	ThroughputRawMBps  float64
	AttemptedFrac      float64
	KeptFrac           float64
	ObservedRatio      float64
	FramesTotal        uint64
	FramesAttempted    uint64
	FramesKept         uint64
	EncodeNsTotal      int64
	IoNsTotal          int64
	State              string
	DictID             uint64
	DictHash           uint64
	HistoryBytes       int
	K                  int
	PublishOrderingOK  bool
	TrainerProfileOK   bool
	TrainerProfileK    int
	TrainerProfileHash uint64
}

type VlogAutotuneBenchResult struct {
	Mode         VlogAutotuneBenchMode
	Segments     []VlogAutotuneBenchSegmentResult
	RawBytes     uint64
	StoredBytes  uint64
	WallTimeNs   int64
	ThroughputMB float64
	TrainerStats compression.TrainerStats
}

type templateBenchKV struct {
	db *DB
}

func (kv templateBenchKV) Get(key []byte) ([]byte, error) {
	return kv.db.Get(key)
}

func (kv templateBenchKV) SetSync(key, value []byte) error {
	return kv.db.SetSync(key, value)
}

func (kv templateBenchKV) DeleteSync(key []byte) error {
	return kv.db.DeleteSync(key)
}

func (kv templateBenchKV) NewBatch() templatedb.Batch {
	b := kv.db.NewBatch()
	if b == nil {
		return nil
	}
	return templateBenchBatch{b: b}
}

type templateBenchBatch struct {
	b *Batch
}

func (tb templateBenchBatch) Set(key, value []byte) error { return tb.b.Set(key, value) }
func (tb templateBenchBatch) Delete(key []byte) error     { return tb.b.Delete(key) }
func (tb templateBenchBatch) WriteSync() error            { return tb.b.WriteSync() }
func (tb templateBenchBatch) Close() error                { return tb.b.Close() }

func RunVlogAutotuneBench(req VlogAutotuneBenchRequest) (*VlogAutotuneBenchResult, error) {
	if len(req.Segments) == 0 {
		return nil, errors.New("no segments")
	}
	root, err := os.MkdirTemp("", "treedb-autotune-bench-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(root)

	dictdbDir := filepath.Join(root, "dictdb")
	if err := os.MkdirAll(dictdbDir, 0755); err != nil {
		return nil, err
	}

	backend, err := db.Open(db.Options{Dir: root})
	if err != nil {
		return nil, err
	}
	defer backend.Close()

	dictBackend, err := db.Open(db.Options{Dir: dictdbDir, DisableBackgroundPrune: true})
	if err != nil {
		return nil, err
	}
	defer dictBackend.Close()

	store := dictdb.New(dictBackend)
	opts := benchOptionsForMode(req.Mode, req.FixedK)
	var templateStore *templatedb.Store
	if opts.ValueLogTemplateMode != template.TemplateOff {
		templatedbDir := filepath.Join(root, "templatedb")
		if err := os.MkdirAll(templatedbDir, 0755); err != nil {
			return nil, err
		}
		templateBackend, err := db.Open(db.Options{Dir: templatedbDir, DisableBackgroundPrune: true})
		if err != nil {
			return nil, err
		}
		defer templateBackend.Close()
		templateCached, err := Open(templatedbDir, templateBackend, Options{
			FlushThreshold: 1 << 30,
			DisableWAL:     true,
			AllowUnsafe:    true,
		})
		if err != nil {
			_ = templateBackend.Close()
			return nil, err
		}
		defer templateCached.Close()
		tcfg := template.NormalizeConfig(opts.ValueLogTemplateConfig)
		templateStore = templatedb.New(templateBenchKV{db: templateCached}, templatedb.Config{
			MaxCandidatesPerFP:    tcfg.MaxCandidatesPerFP,
			MaxCandidateListBytes: tcfg.MaxCandidateListBytes,
		})
	}

	cached, err := Open(root, backend, opts)
	if err != nil {
		return nil, err
	}
	defer cached.Close()
	cached.SetDictStore(store)
	if templateStore != nil {
		cached.SetTemplateStore(templateStore)
	}

	ioClock := valuelog.NewVirtualClock(time.Unix(0, 0))
	cached.valueLogAutotuneMetrics.setClock(ioClock)

	benchWriter, ioSink, err := installBenchWriter(cached, ioClock)
	if err != nil {
		return nil, err
	}

	if req.Mode == VlogAutotuneBenchDictFixed || req.Mode == VlogAutotuneBenchAutotune {
		if err := warmupDictTrainer(cached, benchWriter, ioSink, req.Segments[0]); err != nil {
			return nil, err
		}
	}

	var result VlogAutotuneBenchResult
	result.Mode = req.Mode
	for idx, seg := range req.Segments {
		segRes, err := runBenchSegment(cached, benchWriter, ioSink, seg, uint64(idx))
		if err != nil {
			return nil, err
		}
		result.Segments = append(result.Segments, segRes)
		result.RawBytes += segRes.RawBytes
		result.StoredBytes += segRes.StoredBytes
		result.WallTimeNs += segRes.WallTimeNs
	}
	if result.WallTimeNs > 0 {
		result.ThroughputMB = float64(result.RawBytes) * 1e3 / float64(result.WallTimeNs)
	}
	if tr := cached.valueLogDictTrainer; tr != nil {
		result.TrainerStats = tr.Stats()
	}
	return &result, nil
}

func benchOptionsForMode(mode VlogAutotuneBenchMode, fixedK int) Options {
	if fixedK <= 0 {
		fixedK = 4
	}
	const (
		benchTrainBytes     = 128 << 10
		benchTrainMinRecord = 32
		benchDictBytes      = 40 << 10
		benchWindowBytes    = 64 << 10
		benchPauseBytes     = 256 << 10
		benchEncodeNsPerRaw = 5.0
		benchDecodeNsPerRaw = 1.0
	)

	autotuneMode := valuelog.AutotuneOff
	switch mode {
	case VlogAutotuneBenchAutotune:
		autotuneMode = valuelog.AutotuneMedium
	case VlogAutotuneBenchDictFixed:
		autotuneMode = valuelog.AutotuneOff
	case VlogAutotuneBenchNoDictFixed, VlogAutotuneBenchOff, VlogAutotuneBenchTemplate:
		autotuneMode = valuelog.AutotuneOff
	}

	trainCfg := compression.TrainConfig{}
	if mode == VlogAutotuneBenchDictFixed || mode == VlogAutotuneBenchAutotune {
		trainCfg = compression.TrainConfig{
			TrainBytes:         benchTrainBytes,
			MinRecords:         benchTrainMinRecord,
			DictBytes:          benchDictBytes,
			SampleStride:       1,
			EncodeNsPerRawByte: benchEncodeNsPerRaw,
			DecodeNsPerRawByte: benchDecodeNsPerRaw,
			MaxRecordBytes:     64 << 10,
			DedupWindow:        compression.DefaultTrainDedupWindow,
		}
	}

	candidateK := []int{1, 2, 4, 8, 16, 32}
	if mode == VlogAutotuneBenchDictFixed {
		candidateK = []int{fixedK}
	}

	return Options{
		FlushThreshold:                 1 << 30,
		DisableWAL:                     true,
		AllowUnsafe:                    true,
		ValueLogPointerThreshold:       1,
		ValueLogDictTrain:              trainCfg,
		ValueLogDictAdaptiveRatio:      0.98,
		ValueLogDictMetricsWindowBytes: benchWindowBytes,
		ValueLogDictMetricsMinRecords:  benchTrainMinRecord,
		ValueLogDictMetricsPauseBytes:  benchPauseBytes,
		ValueLogTemplateMode: func() template.Mode {
			if mode == VlogAutotuneBenchTemplate {
				return template.TemplateOnly
			}
			return template.TemplateOff
		}(),
		ValueLogTemplateReadStrict: true,
		ValueLogTemplateConfig: template.Config{
			MinSavingsBytes:        1,
			MaxAnchorsPerTemplate:  16,
			MinAnchorLen:           16,
			MaxAnchorLen:           64,
			MaxAnchorBytesTotal:    1024,
			MaxGaps:                32,
			MaxTemplateFetch:       16,
			MaxCandidatesPerFP:     16,
			TrainSampleStride:      2,
			SynthesizeEverySamples: 64,
			MinAnchorFreq:          16,
			MinPresenceRatio:       0.9,
			MinPublishSavingsBytes: 16,
		},
		ValueLogCompressionAutotune: valuelog.AutotuneOptions{
			Mode:                   autotuneMode,
			CandidateK:             candidateK,
			CandidateHistoryBytes:  []int{64 << 10, 96 << 10, 128 << 10},
			CandidateDictBytes:     []int{40 << 10, 64 << 10, 96 << 10},
			MinGainToSwitch:        0.05,
			MinDwellFrames:         256,
			SampleStride:           2,
			MaxSampleBytes:         256 << 10,
			TrainCPUFraction:       0.02,
			ProbeBytes:             64 << 10,
			PauseBytes:             256 << 10,
			DisableBelowValueBytes: 0,
		},
	}
}

type benchValueWriter struct {
	*valuelog.Writer
	stats benchFrameStats
}

func (w *benchValueWriter) Append(dictID uint64, dict []byte, rid uint64, value []byte) (page.ValuePtr, error) {
	ptr, err := w.Writer.Append(dictID, dict, rid, value)
	w.stats.observeRaw(value)
	return ptr, err
}

func (w *benchValueWriter) AppendFrame(dictID uint64, dict []byte, records []valuelog.Record) ([]page.ValuePtr, error) {
	ptrs, err := w.Writer.AppendFrame(dictID, dict, records)
	w.stats.observeRecords(records)
	return ptrs, err
}

func (w *benchValueWriter) AppendFrameWithStats(dictID uint64, dict []byte, records []valuelog.Record) ([]page.ValuePtr, valuelog.FrameStats, error) {
	ptrs, stats, err := w.Writer.AppendFrameWithStats(dictID, dict, records)
	w.stats.observe(stats)
	return ptrs, stats, err
}

func (w *benchValueWriter) AppendFrameWithStatsInto(dictID uint64, dict []byte, records []valuelog.Record, dst []page.ValuePtr) ([]page.ValuePtr, valuelog.FrameStats, error) {
	ptrs, stats, err := w.Writer.AppendFrameWithStatsInto(dictID, dict, records, dst)
	w.stats.observe(stats)
	return ptrs, stats, err
}

func (w *benchValueWriter) AppendRawFramesWritevInto(records []valuelog.Record, k int, dst []page.ValuePtr) ([]page.ValuePtr, valuelog.FrameStats, error) {
	ptrs, stats, err := w.Writer.AppendRawFramesWritevInto(records, k, dst)
	frames := 1
	if k > 0 && len(records) > 0 {
		frames = (len(records) + k - 1) / k
	}
	w.stats.observeBatch(stats, frames)
	return ptrs, stats, err
}

func (w *benchValueWriter) resetStats() {
	w.stats = benchFrameStats{}
}

type benchFrameStats struct {
	frames       uint64
	attempted    uint64
	kept         uint64
	rawBytes     uint64
	storedBytes  uint64
	encodeNs     int64
	recordFrames uint64
}

func (s *benchFrameStats) observe(stats valuelog.FrameStats) {
	if stats.Records <= 0 {
		return
	}
	s.frames++
	s.rawBytes += uint64(stats.RawPayloadBytes)
	s.storedBytes += uint64(stats.StoredPayloadBytes)
	s.encodeNs += stats.EncodeNs
	if stats.Attempted {
		s.attempted++
	}
	if stats.Kept {
		s.kept++
	}
}

func (s *benchFrameStats) observeBatch(stats valuelog.FrameStats, frames int) {
	if stats.Records <= 0 || frames <= 0 {
		return
	}
	s.frames += uint64(frames)
	s.rawBytes += uint64(stats.RawPayloadBytes)
	s.storedBytes += uint64(stats.StoredPayloadBytes)
	s.encodeNs += stats.EncodeNs
	if stats.Attempted {
		s.attempted += uint64(frames)
	}
	if stats.Kept {
		s.kept += uint64(frames)
	}
}

func (s *benchFrameStats) observeRecords(records []valuelog.Record) {
	raw := 0
	for i := range records {
		raw += len(records[i].Value)
	}
	s.frames++
	s.rawBytes += uint64(raw)
	s.storedBytes += uint64(raw)
}

func (s *benchFrameStats) observeRaw(value []byte) {
	if len(value) == 0 {
		return
	}
	s.frames++
	s.rawBytes += uint64(len(value))
	s.storedBytes += uint64(len(value))
}

func installBenchWriter(db *DB, clock *valuelog.VirtualClock) (*benchValueWriter, *valuelog.VirtualSink, error) {
	if db == nil {
		return nil, nil, errors.New("nil db")
	}
	fileID, _ := valuelog.EncodeFileID(0, 1)
	ioSink := &valuelog.VirtualSink{Clock: clock}
	writer := valuelog.NewWriterWithSink(ioSink, fileID)
	writer.SetEncodeSampleStride(1)
	bw := &benchValueWriter{Writer: writer}
	if len(db.lanes) == 0 {
		return nil, nil, errors.New("no lanes")
	}
	l := &db.lanes[0]
	l.vlogMu.Lock()
	l.vlog = bw
	l.vlogPath = "bench://vlog"
	l.vlogSeq = 1
	l.vlogMu.Unlock()
	return bw, ioSink, nil
}

func warmupDictTrainer(db *DB, writer *benchValueWriter, sink *valuelog.VirtualSink, seg VlogAutotuneBenchSegment) error {
	if db == nil {
		return errors.New("nil db")
	}
	db.ensureValueLogDictTrainer()
	if writer == nil {
		return errors.New("nil writer")
	}
	if sink != nil && seg.IoNsPerStoredByte > 0 {
		sink.NsPerByte = int64(seg.IoNsPerStoredByte)
	}
	writer.SetEncodeCostModel(valuelog.EncodeCostModelFunc(func(rawPayloadBytes int, records int) int64 {
		if seg.EncodeNsPerRawByte <= 0 {
			return 0
		}
		return int64(math.Round(seg.EncodeNsPerRawByte * float64(rawPayloadBytes)))
	}))

	values := valuelog.GenerateAutotuneValues(seg.Workload, seg.ValueSize, 256, 42)
	wantDict := false
	for i := 0; i < len(values) && i < 8; i++ {
		if likelyCompressibleSample(values[i]) {
			wantDict = true
			break
		}
	}
	lane := &db.lanes[0]
	rid := uint64(1)
	records := make([]valuelog.Record, 0, 128)
	for i := 0; i < len(values); i++ {
		records = append(records, valuelog.Record{RID: rid, Value: values[i]})
		rid++
		if len(records) == cap(records) || i == len(values)-1 {
			ptrs, err := db.appendValueLog(lane, 0, nil, records, journalDurabilityNone)
			if err != nil {
				return err
			}
			putValueLogPtrs(ptrs)
			records = records[:0]
		}
	}
	for i := 0; i < 512; i++ {
		db.applyValueLogDictProfile()
		if db.valueLogDictLastAppliedDictID.Load() != 0 {
			return nil
		}
		time.Sleep(1 * time.Millisecond)
	}
	if !wantDict {
		return nil
	}
	if tr := db.valueLogDictTrainer; tr != nil {
		if prof, ok := tr.ActiveProfile(); ok && prof != nil {
			minSavings := db.valueLogDictMinSavingsRatio()
			if prof.PayloadRatio >= 1.0-minSavings {
				return nil
			}
		}
	}
	return errors.New("dict warmup failed")
}

func runBenchSegment(db *DB, writer *benchValueWriter, sink *valuelog.VirtualSink, seg VlogAutotuneBenchSegment, seed uint64) (VlogAutotuneBenchSegmentResult, error) {
	if db == nil || writer == nil {
		return VlogAutotuneBenchSegmentResult{}, errors.New("nil db/writer")
	}
	if sink != nil {
		sink.NsPerByte = int64(seg.IoNsPerStoredByte)
	}
	writer.SetEncodeCostModel(valuelog.EncodeCostModelFunc(func(rawPayloadBytes int, records int) int64 {
		if seg.EncodeNsPerRawByte <= 0 {
			return 0
		}
		return int64(math.Round(seg.EncodeNsPerRawByte * float64(rawPayloadBytes)))
	}))
	writer.ResetCompressionHints()
	writer.resetStats()
	snap := db.valueLogAutotuneMetrics.snapshot()
	if snap.EncodeNsPerRawByte <= 0 || snap.IoNsPerStoredByte <= 0 {
		db.valueLogAutotuneMetrics.seed(seg.EncodeNsPerRawByte, seg.IoNsPerStoredByte)
	}

	ioClock, ok := db.valueLogAutotuneMetrics.clock.(*valuelog.VirtualClock)
	if !ok || ioClock == nil {
		return VlogAutotuneBenchSegmentResult{}, errors.New("missing virtual clock")
	}
	ioStart := ioClock.Now()

	values := valuelog.GenerateAutotuneValues(seg.Workload, seg.ValueSize, seg.Records, int64(1000+seed))
	rawBytesOriginal := uint64(0)
	for i := range values {
		rawBytesOriginal += uint64(len(values[i]))
	}
	lane := &db.lanes[0]
	rid := uint64(1)
	batch := make([]valuelog.Record, 0, 256)
	for i := 0; i < len(values); i++ {
		batch = append(batch, valuelog.Record{RID: rid, Value: values[i]})
		rid++
		if len(batch) == cap(batch) || i == len(values)-1 {
			dictID := uint64(0)
			if db.valueLogDictTrain.TrainBytes > 0 {
				if id, err := db.currentDictID(context.Background()); err == nil {
					dictID = id
				}
			}
			ptrs, err := db.appendValueLog(lane, dictID, nil, batch, journalDurabilityNone)
			if err != nil {
				return VlogAutotuneBenchSegmentResult{}, err
			}
			putValueLogPtrs(ptrs)
			db.applyValueLogDictProfile()
			batch = batch[:0]
		}
	}

	ioEnd := ioClock.Now()
	ioNs := ioEnd.Sub(ioStart).Nanoseconds()
	raw := writer.stats.rawBytes
	if rawBytesOriginal > 0 {
		raw = rawBytesOriginal
	}
	stored := writer.stats.storedBytes
	if stored == 0 {
		stored = raw
	}
	wallNs := ioNs + writer.stats.encodeNs
	throughput := 0.0
	if wallNs > 0 {
		throughput = float64(raw) * 1e3 / float64(wallNs)
	}
	attemptedFrac := 0.0
	keptFrac := 0.0
	if writer.stats.frames > 0 {
		attemptedFrac = float64(writer.stats.attempted) / float64(writer.stats.frames)
		keptFrac = float64(writer.stats.kept) / float64(writer.stats.frames)
	}
	observedRatio := 1.0
	if raw > 0 {
		observedRatio = float64(stored) / float64(raw)
	}

	state := benchAutotuneState(db)
	dictID := db.valueLogDictLastAppliedDictID.Load()
	dictHash := db.valueLogDictLastAppliedDictHash.Load()
	historyBytes := 0
	k := int(db.valueLogDictCurrentK.Load())
	if p, ok := db.valueLogAutotuneLastProfile.Load().(*vlogAutotuneProfile); ok && p != nil {
		historyBytes = p.HistoryBytes
		if p.K > 0 {
			k = p.K
		}
	}
	publishOK := true
	if dictID != 0 && db.dictStore != nil {
		if dictBytes, err := db.dictStore.GetDictBytes(context.Background(), dictID); err != nil || len(dictBytes) == 0 {
			publishOK = false
		}
	}
	trProfileOK := false
	trProfileK := 0
	trProfileHash := uint64(0)
	if tr := db.valueLogDictTrainer; tr != nil {
		if prof, ok := tr.ActiveProfile(); ok && prof != nil {
			trProfileOK = true
			trProfileK = prof.K
			trProfileHash = prof.DictHash
		}
	}

	return VlogAutotuneBenchSegmentResult{
		Name:               seg.Name,
		RawBytes:           raw,
		StoredBytes:        stored,
		WallTimeNs:         wallNs,
		ThroughputRawMBps:  throughput,
		AttemptedFrac:      attemptedFrac,
		KeptFrac:           keptFrac,
		ObservedRatio:      observedRatio,
		FramesTotal:        writer.stats.frames,
		FramesAttempted:    writer.stats.attempted,
		FramesKept:         writer.stats.kept,
		EncodeNsTotal:      writer.stats.encodeNs,
		IoNsTotal:          ioNs,
		State:              state,
		DictID:             dictID,
		DictHash:           dictHash,
		HistoryBytes:       historyBytes,
		K:                  k,
		PublishOrderingOK:  publishOK,
		TrainerProfileOK:   trProfileOK,
		TrainerProfileK:    trProfileK,
		TrainerProfileHash: trProfileHash,
	}, nil
}

func benchAutotuneState(db *DB) string {
	if db == nil {
		return "OFF"
	}
	if db.valueLogAutotuneOptions.Mode == valuelog.AutotuneOff {
		return "OFF"
	}
	if db.valueLogDictPaused() {
		return "PAUSED"
	}
	if db.valueLogDictLastAppliedDictID.Load() == 0 {
		return "WARMUP"
	}
	return "ACTIVE"
}
