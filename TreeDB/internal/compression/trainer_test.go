package compression

import (
	"math"
	"testing"
	"time"

	"github.com/snissn/compress/zstd"
)

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
