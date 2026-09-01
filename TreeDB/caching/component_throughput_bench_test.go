package caching

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

type benchMemtableShard struct {
	mu sync.Mutex
	mt memtable.Table
}

// BenchmarkComponentMemtableShardedSetParallel isolates mutable-memtable write
// throughput (sharded hash + shard mutex + memtable Set) without WAL/index work.
func BenchmarkComponentMemtableShardedSetParallel(b *testing.B) {
	const (
		shardCount = 16
		keySpace   = 1 << 16
		valueSize  = 256
	)

	indexer := memtable.NewHashSortedIndexer()
	shards := make([]benchMemtableShard, shardCount)
	for i := range shards {
		mt, err := memtable.NewWithCapacityModeAndIndexer(0, memtable.ModeHashSorted, indexer)
		if err != nil {
			b.Fatalf("new memtable: %v", err)
		}
		shards[i] = benchMemtableShard{mt: mt}
	}

	keys := make([][]byte, keySpace)
	for i := 0; i < keySpace; i++ {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		keys[i] = k
	}
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte(i)
	}

	var opSeq atomic.Uint64
	mask := uint64(shardCount - 1)

	b.ReportAllocs()
	b.SetBytes(int64(valueSize))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := opSeq.Add(1)
			key := keys[int(n%keySpace)]
			shard := &shards[int(xxhash.Sum64(key)&mask)]
			shard.mu.Lock()
			shard.mt.Set(key, value)
			shard.mu.Unlock()
		}
	})
}

// BenchmarkComponentValueLaneAppendQueuedParallel isolates the queued value-lane
// append path (single lane + worker + appendValueLogOne), excluding index work.
func BenchmarkComponentValueLaneAppendQueuedParallel(b *testing.B) {
	clock := valuelog.NewVirtualClock(time.Unix(0, 0))
	sink := &valuelog.VirtualSink{Clock: clock}
	fileID, _ := valuelog.EncodeFileID(0, 1)
	writer := valuelog.NewWriterWithSink(sink, fileID)
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
	db.startVlogWriter(&db.lanes[0])
	db.lanes[0].vlogQueueing.Store(true)
	defer func() {
		close(db.closeCh)
		db.wg.Wait()
	}()

	value := make([]byte, vlogQueueMinValueSize)
	for i := range value {
		value[i] = byte(i)
	}
	var rid atomic.Uint64

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
}

func BenchmarkComponentValueLanesParallel(b *testing.B) {
	for _, queueOn := range []bool{false, true} {
		queueName := "queue_off"
		if queueOn {
			queueName = "queue_on"
		}
		for _, laneCount := range []int{1, 2, 4, 8} {
			b.Run(fmt.Sprintf("%s/lanes_%d", queueName, laneCount), func(b *testing.B) {
				clock := valuelog.NewVirtualClock(time.Unix(0, 0))
				sink := &valuelog.VirtualSink{Clock: clock}
				lanes := make([]lane, laneCount)
				for i := range lanes {
					fileID, _ := valuelog.EncodeFileID(uint32(i), 1)
					writer := valuelog.NewWriterWithSink(sink, fileID)
					writer.SetEncodeSampleStride(0)
					lanes[i] = lane{id: i, vlog: writer}
				}

				db := &DB{
					closeCh: make(chan struct{}),
					valueLogAutotuneOptions: valuelog.AutotuneOptions{
						Mode: valuelog.AutotuneOff,
					},
					disableJournal:        true,
					forceValueLogPointers: true,
					lanes:                 lanes,
				}
				if queueOn {
					for i := range db.lanes {
						db.startVlogWriter(&db.lanes[i])
						db.lanes[i].vlogQueueing.Store(true)
					}
				}
				defer func() {
					close(db.closeCh)
					db.wg.Wait()
				}()

				value := make([]byte, vlogQueueMinValueSize)
				for i := range value {
					value[i] = byte(i)
				}
				var rid atomic.Uint64
				var lanePick atomic.Uint64

				b.ReportAllocs()
				b.SetBytes(int64(len(value)))
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						id := rid.Add(1)
						idx := int(lanePick.Add(1)-1) % laneCount
						if _, _, err := db.appendValueLogOne(&db.lanes[idx], 0, nil, id, value, journalDurabilityNone); err != nil {
							panic(err)
						}
					}
				})
			})
		}
	}
}

func BenchmarkComponentNativeRootValueLogAppendWriters(b *testing.B) {
	const (
		valuesPerBatch = 480
		valueBytes     = 8_704
	)
	records := make([]valuelog.Record, valuesPerBatch)
	for i := range records {
		value := make([]byte, valueBytes)
		state := uint32(i + 1)
		for j := range value[:valueBytes/2] {
			state ^= state << 13
			state ^= state >> 17
			state ^= state << 5
			value[j] = byte(state)
		}
		for j := valueBytes / 2; j < len(value); j++ {
			value[j] = byte((i*31 + j*17 + j>>5) % 251)
		}
		records[i] = valuelog.Record{RID: uint64(i + 1), Value: value}
	}

	for _, width := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("writers_%d", width), func(b *testing.B) {
			db := &DB{
				closeCh:                  make(chan struct{}),
				splitValueLog:            true,
				disableJournal:           true,
				forceValueLogPointers:    true,
				valueLogCompressionMode:  uint8(vlogCompressionBlock),
				valueLogBlockCodec:       valuelog.BlockCodecZSTD,
				valueLogBlockTargetBytes: 256,
				valueLogThreshold:        1 << 30,
				valueLogAutotuneOptions: valuelog.AutotuneOptions{
					Mode: valuelog.AutotuneOff,
				},
				lanes: []lane{{id: 0}},
			}
			db.nativeRootValueLogAppendLanes = make([]*lane, width)
			for i := 0; i < width; i++ {
				fileID, _ := valuelog.EncodeFileID(0, uint32(i+1))
				writer := valuelog.NewWriterWithSink(&valuelog.VirtualSink{Clock: valuelog.NewVirtualClock(time.Unix(0, 0))}, fileID)
				writer.SetEncodeSampleStride(0)
				l := &db.lanes[0]
				if i > 0 {
					l = &lane{id: 0}
				}
				l.vlog = writer
				l.vlogSeq = i + 1
				db.nativeRootValueLogAppendLanes[i] = l
			}
			db.nativeRootValueLogAppendShared = width > 1
			appender := &cachingValueLogAppender{db: db, lane: &db.lanes[0]}

			b.ReportAllocs()
			b.SetBytes(valuesPerBatch * valueBytes)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := appender.appendRecords(records); err != nil {
						panic(err)
					}
				}
			})
			b.StopTimer()
			for _, l := range db.nativeRootValueLogAppendLanes {
				if err := l.vlog.Close(); err != nil {
					b.Fatalf("writer close: %v", err)
				}
			}
		})
	}
}
