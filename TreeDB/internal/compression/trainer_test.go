package compression

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/snissn/compress/zstd"
)

func TestBuildAndValidateDict_DefaultOffsetsAvoidInvalidOffset(t *testing.T) {
	// Regression coverage for #117: If we pass zero offsets into zstd.BuildDict,
	// it can return a dictionary that fails to load with
	// "invalid offset in dictionary" (especially with small/degenerate inputs).

	hist := bytes.Repeat([]byte("abcd"), (40<<10)/4)
	sample := append([]byte("prefix:"), bytes.Repeat([]byte("abcd"), (1024-7)/4)...)
	sample = sample[:1024]

	dict, err := buildAndValidateDict(1, [][]byte{sample}, hist, zstd.SpeedFastest, nil, nil)
	if err != nil {
		t.Fatalf("buildAndValidateDict failed: %v", err)
	}
	if len(dict) == 0 {
		t.Fatalf("unexpected empty dictionary")
	}
	info, err := zstd.InspectDictionary(dict)
	if err != nil {
		t.Fatalf("InspectDictionary failed: %v", err)
	}
	offsets := info.Offsets()
	for i, off := range offsets {
		if off <= 0 {
			t.Fatalf("invalid offsets: idx=%d offsets=%v", i, offsets)
		}
	}
}

func TestTrainerDedupWindowZeroDoesNotPanic(t *testing.T) {
	tr := &Trainer{}
	tr.dictDedupWindow = 0

	if mode, ref := tr.recordDictHash(123); mode != dictDedupNone || ref != -1 {
		t.Fatalf("unexpected recordDictHash result: mode=%v ref=%d", mode, ref)
	}

	tr.storeCachedDict(1, 2, []byte{1, 2, 3})
}

func TestTrainerRestartsCollectingWhenDegradedAndThrottled(t *testing.T) {
	tr := NewTrainer(TrainConfig{
		TrainBytes:     64 << 10,
		DictBytes:      40 << 10,
		MinRecords:     8,
		MaxRecordBytes: 1024,
		SampleStride:   1,
	}, Config{Kind: KindZSTD, Level: zstd.SpeedFastest}, false, false)
	if tr == nil {
		t.Fatalf("expected non-nil trainer")
	}
	defer tr.Close()
	tr.collecting.Store(false)
	tr.training.Store(true)
	tr.degraded.Store(true)
	tr.lastAcceptTime.Store(time.Now())
	tr.lastAcceptBytes.Store(0)
	tr.lastAcceptRecords.Store(0)
	tr.rollingRatioBase.Store(math.Float64bits(0.5))
	tr.rollingRatioCur.Store(math.Float64bits(0.5))

	samples := make([][]byte, 8)
	for i := range samples {
		samples[i] = []byte("sample")
	}
	tr.train(samples, 40<<10, zstd.SpeedFastest, 1)
	if !tr.collecting.Load() {
		t.Fatalf("expected collecting to restart")
	}
}

func TestTrainerSetOnAccept_CallsCallback(t *testing.T) {
	tr := &Trainer{}
	called := make(chan struct{}, 1)
	tr.SetOnAccept(func(*ActiveProfile) {
		select {
		case called <- struct{}{}:
		default:
		}
	})

	tr.AcceptProfile(&ActiveProfile{Timestamp: time.Now(), TotalRatio: 0.5})

	select {
	case <-called:
	default:
		t.Fatalf("expected onAccept callback to be invoked")
	}
}

func TestTrainerAcceptProfile_BootstrapUpgradeResumesCollecting(t *testing.T) {
	tr := &Trainer{
		targetBytes:         64 << 10,
		minRecords:          64,
		dictBytes:           32 << 10,
		bootstrapBytes:      32 << 10,
		bootstrapMinRecords: 8,
		bootstrapDictBytes:  8 << 10,
	}
	tr.collecting.Store(false)

	tr.AcceptProfile(&ActiveProfile{Timestamp: time.Now(), TotalRatio: 0.5})

	if !tr.upgradePending.Load() {
		t.Fatalf("expected upgradePending to be set after first bootstrap profile")
	}
	if !tr.collecting.Load() {
		t.Fatalf("expected collecting to resume after first bootstrap profile")
	}
}

func TestTrainerCanSelectExpandedDictCandidates(t *testing.T) {
	tr := NewTrainer(TrainConfig{
		TrainBytes:     64 << 10,
		DictBytes:      32 << 10,
		MinRecords:     8,
		MaxRecordBytes: 64 << 10,
		SampleStride:   1,
	}, Config{Kind: KindZSTD, Level: zstd.SpeedFastest}, false, false)
	if tr == nil {
		t.Fatalf("expected non-nil trainer")
	}
	defer tr.Close()
	tr.SetAutotuneCandidates([]int{32}, []int{96 << 10}, []int{96 << 10})

	samples := make([][]byte, 128)
	base := bytes.Repeat([]byte("validator-snapshot-entry/"), 2048)
	for i := range samples {
		buf := append([]byte(nil), base[:43635]...)
		copy(buf[len(buf)-16:], []byte(fmt.Sprintf("%016x", i)))
		samples[i] = buf
	}

	tr.train(samples, 32<<10, zstd.SpeedFastest, 1)

	p, ok := tr.lastProfile.Load().(*ActiveProfile)
	if !ok || p == nil {
		t.Fatalf("expected accepted profile")
	}
	if p.HistoryBytes != 96<<10 {
		t.Fatalf("history=%d want=%d", p.HistoryBytes, 96<<10)
	}
	if p.DictBytes != 96<<10 {
		t.Fatalf("dict bytes=%d want=%d", p.DictBytes, 96<<10)
	}
}

func TestNewTrainer_BootstrapMinRecordsClamp(t *testing.T) {
	tr := NewTrainer(TrainConfig{
		TrainBytes:     1 << 20,
		DictBytes:      32 << 10,
		MinRecords:     64,
		MaxRecordBytes: 64 << 10,
		SampleStride:   1,
	}, Config{Kind: KindZSTD, Level: zstd.SpeedFastest}, false, false)
	if tr == nil {
		t.Fatalf("expected non-nil trainer")
	}
	defer tr.Close()
	if got, want := tr.bootstrapMinRecords, uint64(DefaultTrainBootstrapMinRecords); got != want {
		t.Fatalf("bootstrapMinRecords clamp: got=%d want=%d", got, want)
	}
}

func TestNewTrainer_BootstrapMinRecordsRespectsLowerMinRecords(t *testing.T) {
	tr := NewTrainer(TrainConfig{
		TrainBytes:     1 << 20,
		DictBytes:      32 << 10,
		MinRecords:     8,
		MaxRecordBytes: 64 << 10,
		SampleStride:   1,
	}, Config{Kind: KindZSTD, Level: zstd.SpeedFastest}, false, false)
	if tr == nil {
		t.Fatalf("expected non-nil trainer")
	}
	defer tr.Close()
	if got, want := tr.bootstrapMinRecords, uint64(8); got != want {
		t.Fatalf("bootstrapMinRecords respect lower minRecords: got=%d want=%d", got, want)
	}
}

func TestTrainerTrain_HoldoutPreventsInSampleMemorizationOnRandomNoise(t *testing.T) {
	tr := NewTrainer(TrainConfig{
		TrainBytes:     64 << 10,
		DictBytes:      128 << 10,
		MinRecords:     8,
		MaxRecordBytes: 8 << 10,
		SampleStride:   1,
	}, Config{Kind: KindZSTD, Level: zstd.SpeedFastest}, false, false)
	if tr == nil {
		t.Fatalf("expected non-nil trainer")
	}
	defer tr.Close()
	tr.SetAutotuneCandidates([]int{1, 2, 4, 8}, []int{64 << 10, 128 << 10}, []int{64 << 10, 128 << 10})

	// 32 x 4 KiB = 128 KiB of pure incompressible cryptographic noise.
	// Previously, historyMax copied all 128 KiB and evaluated on the same
	// samples, reporting a bogus ~0.005 ratio and publishing a 128 KiB dict.
	samples := make([][]byte, 32)
	for i := range samples {
		buf := make([]byte, 4096)
		if _, err := rand.Read(buf); err != nil {
			t.Fatalf("rand.Read: %v", err)
		}
		samples[i] = buf
	}

	tr.train(samples, 128<<10, zstd.SpeedFastest, 1)

	p, ok := tr.ActiveProfile()
	if !ok || p == nil {
		return
	}
	if p.TotalRatio < 0.98 {
		t.Fatalf("expected incompressible random noise to evaluate >=0.98 on holdout, got TotalRatio=%.4f (PayloadRatio=%.4f, HistoryBytes=%d, DictBytes=%d)",
			p.TotalRatio, p.PayloadRatio, p.HistoryBytes, p.DictBytes)
	}
	if p.HistoryBytes > (16*4096)/4 {
		t.Fatalf("expected history capped at <=25%% of disjoint train corpus (%d B), got %d B", (16*4096)/4, p.HistoryBytes)
	}
}

func TestShapeAndValidateDict_DoesNotZeroPadShorterDict(t *testing.T) {
	hist := bytes.Repeat([]byte("structured-json-field:"), 256)
	sample := append([]byte("doc:"), bytes.Repeat([]byte("structured-json-field:"), 64)...)
	dict, err := buildAndValidateDict(1, [][]byte{sample}, hist, zstd.SpeedFastest, nil, nil)
	if err != nil {
		t.Fatalf("buildAndValidateDict: %v", err)
	}
	target := len(dict) + 16<<10
	shaped, err := shapeAndValidateDict(dict, target, zstd.SpeedFastest, nil)
	if err != nil {
		t.Fatalf("shapeAndValidateDict: %v", err)
	}
	if len(shaped) != len(dict) {
		t.Fatalf("shapeAndValidateDict zero-padded shorter dict: got len=%d want=%d", len(shaped), len(dict))
	}
}

func TestTrainerTrain_AlternatingSampleSizesBalancePartitions(t *testing.T) {
	tr := NewTrainer(TrainConfig{
		TrainBytes:     32 << 10,
		DictBytes:      16 << 10,
		MinRecords:     8,
		MaxRecordBytes: 4 << 10,
		SampleStride:   1,
	}, Config{Kind: KindZSTD, Level: zstd.SpeedFastest}, false, false)
	if tr == nil {
		t.Fatalf("expected non-nil trainer")
	}
	defer tr.Close()

	// Alternate 64-byte and 2048-byte structured records (32 records = 33792 B).
	// Naive even/odd splitting would place only 1024 B in trainSamples and drop
	// the batch; byte-balanced partitioning trains a valid dictionary.
	smallBase := bytes.Repeat([]byte(`{"k":"v","t":1},`), 4)
	largeBase := bytes.Repeat([]byte(`{"collection":"orders","status":"confirmed","region":"us-west-2","payload":"structured-field-data"},`), 22)[:2048]
	samples := make([][]byte, 32)
	for i := range samples {
		if i%2 == 0 {
			buf := append([]byte(nil), smallBase[:64]...)
			copy(buf[len(buf)-4:], fmt.Sprintf("%04d", i))
			samples[i] = buf
		} else {
			buf := append([]byte(nil), largeBase...)
			copy(buf[len(buf)-4:], fmt.Sprintf("%04d", i))
			samples[i] = buf
		}
	}

	tr.train(samples, 16<<10, zstd.SpeedFastest, 1)

	p, ok := tr.ActiveProfile()
	if !ok || p == nil {
		t.Fatalf("expected accepted profile for alternating 64B/2048B corpus")
	}
	if p.TotalRatio >= 0.5 {
		t.Fatalf("expected strong holdout compression on structured alternating corpus, got TotalRatio=%.4f", p.TotalRatio)
	}
}
