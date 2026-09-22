package caching

import (
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func BenchmarkAppendValueLogOneParallel(b *testing.B) {
	bench := func(b *testing.B, startWorker bool, valueSize int, dictID uint64) {
		clock := valuelog.NewVirtualClock(time.Unix(0, 0))
		sink := &valuelog.VirtualSink{Clock: clock}
		fileID, _ := valuelog.EncodeFileID(0, 1)
		writer := valuelog.NewWriterWithSink(sink, fileID)
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
		if dictID != 0 {
			samples := [][]byte{
				queueTestValue('q', valueSize, 1),
				queueTestValue('q', valueSize, 2),
				queueTestValue('q', valueSize, 3),
				queueTestValue('q', valueSize, 4),
			}
			db.dictStore = &queueDictStore{
				current: dictID,
				dicts: map[uint64][]byte{
					dictID: buildQueueTestDict(b, dictID, samples),
				},
			}
			db.valueLogDictCurrentK.Store(8)
			db.valueLogDictLastAppliedDictID.Store(dictID)
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
		if dictID != 0 {
			value = queueTestValue('q', valueSize, 99)
		}
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

	b.Run("small/direct", func(b *testing.B) { bench(b, false, 128, 0) })
	b.Run("small/queued", func(b *testing.B) { bench(b, true, 128, 0) })
	b.Run("large/direct", func(b *testing.B) { bench(b, false, vlogQueueMinValueSize, 0) })
	b.Run("large/queued", func(b *testing.B) { bench(b, true, vlogQueueMinValueSize, 0) })
	b.Run("large/direct_dict_on", func(b *testing.B) { bench(b, false, vlogQueueMinValueSize, 1) })
	b.Run("large/queued_dict_on", func(b *testing.B) { bench(b, true, vlogQueueMinValueSize, 1) })
}

func BenchmarkAppendValueLogOneParallelFile(b *testing.B) {
	bench := func(b *testing.B, startWorker bool, valueSize int, dictID uint64) {
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
		if dictID != 0 {
			samples := [][]byte{
				queueTestValue('q', valueSize, 1),
				queueTestValue('q', valueSize, 2),
				queueTestValue('q', valueSize, 3),
				queueTestValue('q', valueSize, 4),
			}
			db.dictStore = &queueDictStore{
				current: dictID,
				dicts: map[uint64][]byte{
					dictID: buildQueueTestDict(b, dictID, samples),
				},
			}
			db.valueLogDictCurrentK.Store(8)
			db.valueLogDictLastAppliedDictID.Store(dictID)
		}
		if startWorker {
			db.startVlogWriter(&db.lanes[0])
			db.lanes[0].vlogQueueing.Store(true)
		}

		value := make([]byte, valueSize)
		if dictID != 0 {
			value = queueTestValue('q', valueSize, 99)
		}
		var rid atomic.Uint64

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
		b.StopTimer()

		close(db.closeCh)
		db.wg.Wait()
		if err := writer.Close(); err != nil {
			b.Fatalf("writer.Close: %v", err)
		}
	}

	b.Run("small/direct", func(b *testing.B) { bench(b, false, 128, 0) })
	b.Run("small/queued", func(b *testing.B) { bench(b, true, 128, 0) })
	b.Run("large/direct", func(b *testing.B) { bench(b, false, 32<<10, 0) })
	b.Run("large/queued", func(b *testing.B) { bench(b, true, 32<<10, 0) })
	b.Run("large/direct_dict_on", func(b *testing.B) { bench(b, false, 32<<10, 1) })
	b.Run("large/queued_dict_on", func(b *testing.B) { bench(b, true, 32<<10, 1) })
}
