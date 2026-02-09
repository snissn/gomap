package caching

import (
	"encoding/binary"
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
