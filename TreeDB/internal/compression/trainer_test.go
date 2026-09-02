package compression

import (
	"bytes"
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
	if got, want := tr.bootstrapMinRecords, uint64(8); got != want {
		t.Fatalf("bootstrapMinRecords respect lower minRecords: got=%d want=%d", got, want)
	}
}
