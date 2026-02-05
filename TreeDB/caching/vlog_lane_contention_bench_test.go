package caching

import (
	"context"
	"math/rand"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

type benchDictStore struct {
	dictID uint64
	dict   []byte
}

func (s *benchDictStore) GetCurrent(ctx context.Context) (uint64, error) {
	if s == nil {
		return 0, nil
	}
	return s.dictID, nil
}

func (s *benchDictStore) GetDictBytes(ctx context.Context, dictID uint64) ([]byte, error) {
	if s == nil {
		return nil, valuelog.ErrMissingDict
	}
	if dictID != s.dictID {
		return nil, valuelog.ErrMissingDict
	}
	if s.dictID == 0 || len(s.dict) == 0 {
		return nil, valuelog.ErrMissingDict
	}
	return s.dict, nil
}

func fillBenchRepeatTail(rng *rand.Rand, dst []byte, tail int, pattern []byte) {
	if len(dst) == 0 {
		return
	}
	for off := 0; off < len(dst); {
		off += copy(dst[off:], pattern)
	}
	if tail <= 0 {
		return
	}
	if tail > len(dst) {
		tail = len(dst)
	}
	_, _ = rng.Read(dst[len(dst)-tail:])
}

func buildBenchZstdDict(b *testing.B, dictID uint32, valueSize int) []byte {
	b.Helper()
	if valueSize <= 0 {
		b.Fatalf("invalid valueSize=%d", valueSize)
	}
	rng := rand.New(rand.NewSource(1))
	samples := make([][]byte, 128)
	pattern := []byte("{\"key\":\"value\",\"active\":true}")
	for i := range samples {
		buf := make([]byte, valueSize)
		fillBenchRepeatTail(rng, buf, 64, pattern)
		samples[i] = buf
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       dictID,
		Contents: samples,
		History:  pattern,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		b.Fatalf("BuildDict: %v", err)
	}
	if len(dict) == 0 {
		b.Fatalf("BuildDict: empty dictionary")
	}
	return dict
}

func BenchmarkAppendValueLogOneParallel(b *testing.B) {
	bench := func(b *testing.B, startWorker bool, valueSize int, dictOn bool) {
		clock := valuelog.NewVirtualClock(time.Unix(0, 0))
		sink := &valuelog.VirtualSink{Clock: clock}
		fileID, _ := valuelog.EncodeFileID(0, 1)
		writer := valuelog.NewWriterWithSink(sink, fileID)
		// Avoid encode sampling overhead; this benchmark focuses on caching-layer coordination.
		writer.SetEncodeSampleStride(0)

		dictID := uint64(0)
		if dictOn {
			dictID = 1
		}

		db := &DB{
			closeCh: make(chan struct{}),
			valueLogAutotuneOptions: valuelog.AutotuneOptions{
				Mode: valuelog.AutotuneOff,
			},
			disableJournal:        true,
			forceValueLogPointers: true,
			lanes:                 []lane{{id: 0, vlog: writer}},
		}
		if dictOn {
			db.dictStore = &benchDictStore{
				dictID: dictID,
				dict:   buildBenchZstdDict(b, uint32(dictID), valueSize),
			}
			db.valueLogDictLastAppliedDictID.Store(dictID)
			db.valueLogDictCurrentK.Store(16)
			db.valueLogDictFramePipelineWorkers = 4
		}
		if startWorker {
			db.startVlogWriter(&db.lanes[0])
			db.lanes[0].vlogQueueing.Store(true)
		}
		defer func() {
			close(db.closeCh)
			db.wg.Wait()
		}()

		value := make([]byte, valueSize)
		pattern := []byte("{\"key\":\"value\",\"active\":true}")
		fillBenchRepeatTail(rand.New(rand.NewSource(2)), value, 64, pattern)
		var rid atomic.Uint64

		// Increase contention to make lane coordination visible.
		b.SetParallelism(8)
		b.ReportAllocs()
		b.SetBytes(int64(len(value)))
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				id := rid.Add(1)
				if _, _, err := db.appendValueLogOne(&db.lanes[0], dictID, nil, id, value, journalDurabilityNone); err != nil {
					panic(err)
				}
			}
		})
	}

	b.Run("small/raw/direct", func(b *testing.B) { bench(b, false, 128, false) })
	b.Run("small/raw/queued", func(b *testing.B) { bench(b, true, 128, false) })
	b.Run("large/raw/direct", func(b *testing.B) { bench(b, false, vlogQueueMinValueSize, false) })
	b.Run("large/raw/queued", func(b *testing.B) { bench(b, true, vlogQueueMinValueSize, false) })
	b.Run("large/dict/direct", func(b *testing.B) { bench(b, false, vlogQueueMinValueSize, true) })
	b.Run("large/dict/queued", func(b *testing.B) { bench(b, true, vlogQueueMinValueSize, true) })
}

func BenchmarkAppendValueLogOneParallelFile(b *testing.B) {
	bench := func(b *testing.B, startWorker bool, valueSize int) {
		dir := b.TempDir()

		fileID, _ := valuelog.EncodeFileID(0, 1)
		path := filepath.Join(dir, "value.log")
		writer, err := valuelog.NewWriter(path, fileID)
		if err != nil {
			b.Fatalf("NewWriter: %v", err)
		}
		// Avoid encode sampling overhead; this benchmark focuses on caching-layer coordination.
		writer.SetEncodeSampleStride(0)

		db := &DB{
			closeCh: make(chan struct{}),
			valueLogAutotuneOptions: valuelog.AutotuneOptions{
				Mode: valuelog.AutotuneOff,
			},
			disableJournal:        true,
			forceValueLogPointers: true,
			lanes:                 []lane{{id: 0, vlog: writer}},
		}
		if startWorker {
			db.startVlogWriter(&db.lanes[0])
			db.lanes[0].vlogQueueing.Store(true)
		}

		value := make([]byte, valueSize)
		var rid atomic.Uint64

		b.SetParallelism(8)
		b.ReportAllocs()
		b.SetBytes(int64(len(value)))
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				id := rid.Add(1)
				if _, _, err := db.appendValueLogOne(&db.lanes[0], 0, nil, id, value, journalDurabilityNone); err != nil {
					panic(err)
				}
			}
		})
		b.StopTimer()

		close(db.closeCh)
		db.wg.Wait()
		if err := writer.Close(); err != nil {
			b.Fatalf("writer.Close: %v", err)
		}
	}

	b.Run("small/direct", func(b *testing.B) { bench(b, false, 128) })
	b.Run("small/queued", func(b *testing.B) { bench(b, true, 128) })
	b.Run("large/direct", func(b *testing.B) { bench(b, false, 32<<10) })
	b.Run("large/queued", func(b *testing.B) { bench(b, true, 32<<10) })
}
