package compression

import (
	"bytes"
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

	dict, err := buildAndValidateDict(1, [][]byte{sample}, hist, zstd.SpeedFastest)
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
