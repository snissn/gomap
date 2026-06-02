package collections

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func BenchmarkCollectionUpdateBSONSetYCSBNoIndexParallel16(b *testing.B) {
	for _, tc := range []struct {
		name    string
		profile treedb.Profile
	}{
		{name: "command_wal_relaxed", profile: treedb.ProfileCommandWALRelaxed},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.Run("direct", func(b *testing.B) {
				benchmarkCollectionUpdateBSONSetYCSBNoIndexParallel16(b, tc.profile, true)
			})
			b.Run("public_combiner", func(b *testing.B) {
				benchmarkCollectionUpdateBSONSetYCSBNoIndexParallel16(b, tc.profile, false)
			})
		})
	}
}

func benchmarkCollectionUpdateBSONSetYCSBNoIndexParallel16(b *testing.B, profile treedb.Profile, direct bool) {
	b.Helper()
	const (
		docCount         = 10000
		fieldCount       = 10
		fieldLength      = 100
		preloadBatchSize = 1000
		workers          = 16
	)

	dbDir := b.TempDir()
	opts := treedb.OptionsFor(profile, dbDir)
	opts.IndexOuterLeavesInValueLog = true
	backend, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		b.Fatalf("open backend: %v", err)
	}
	defer func() { _ = cleanup() }()

	manager := NewCollectionManager(backend)
	manager.SetUpdateBatchDetailedStatsEnabled(true)
	meta := &CollectionMeta{
		Name: "bench.docs",
		Options: CollectionOptions{
			DocumentFormat:          DocumentFormatBSON,
			DataRootStoragePolicy:   RootStorageCompressed,
			IndexStateStoragePolicy: RootStorageCompressed,
		},
	}
	if _, err := manager.CreateCollection(meta); err != nil {
		b.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("bench.docs")
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}

	ids := make([][]byte, docCount)
	docs := make([][]byte, docCount)
	for i := 0; i < docCount; i++ {
		id := "user" + strconv.Itoa(i)
		ids[i] = []byte(id)
		doc, err := bson.Marshal(benchmarkBSONSetCombinerYCSBDocument(id, i, fieldCount, fieldLength))
		if err != nil {
			b.Fatalf("marshal doc: %v", err)
		}
		docs[i] = doc
	}
	for i := 0; i < docCount; i += preloadBatchSize {
		end := i + preloadBatchSize
		if end > docCount {
			end = docCount
		}
		if _, err := collection.InsertBatchValidatedBSON(ids[i:end], docs[i:end]); err != nil {
			b.Fatalf("insert docs [%d:%d]: %v", i, end, err)
		}
	}
	if err := manager.FlushAll(); err != nil {
		b.Fatalf("flush preload docs: %v", err)
	}
	manager.ResetUpdateCombinersForProfiling()
	manager.ResetUpdateCombineQueueDepthMax()

	valueA := bytes.Repeat([]byte{0xA5}, fieldLength)
	valueB := bytes.Repeat([]byte{0x5A}, fieldLength)
	typeA, rawA, err := bson.MarshalValue(valueA)
	if err != nil {
		b.Fatalf("marshal value A: %v", err)
	}
	typeB, rawB, err := bson.MarshalValue(valueB)
	if err != nil {
		b.Fatalf("marshal value B: %v", err)
	}
	values := []bson.RawValue{
		{Type: typeA, Value: rawA},
		{Type: typeB, Value: rawB},
	}

	if !direct && collection.writeDomain != nil {
		collection.writeDomain.updateCombineLastRequestUnixNano.Store(time.Now().Add(time.Hour).UnixNano())
	}

	var next atomic.Uint64
	var stop atomic.Bool
	start := make(chan struct{})
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			fields := []BSONSetField{{
				Key:   "field0",
				Value: values[0],
			}}
			for {
				if stop.Load() {
					return
				}
				n := int(next.Add(1) - 1)
				if n >= b.N {
					return
				}
				fields[0].Value = values[(n/docCount)&1]
				id := ids[n%docCount]
				var matched bool
				var err error
				if direct {
					matched, _, err = collection.updateBSONSetDirect(id, mustBenchmarkBSONSetUpdate(fields))
				} else {
					matched, _, err = collection.UpdateBSONSet(id, fields)
				}
				if err != nil {
					if stop.CompareAndSwap(false, true) {
						errCh <- fmt.Errorf("update %d id=%q: %w", n, string(id), err)
					}
					return
				}
				if !matched {
					if stop.CompareAndSwap(false, true) {
						errCh <- fmt.Errorf("update %d missing id %q", n, string(id))
					}
					return
				}
			}
		}()
	}

	statsBefore := manager.StatsSnapshot()
	b.ReportAllocs()
	b.ResetTimer()
	startTime := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(startTime)
	b.StopTimer()
	select {
	case err := <-errCh:
		if err != nil {
			b.Fatalf("worker update: %v", err)
		}
	default:
	}

	stats := collectionManagerStatsBenchmarkDelta(manager.StatsSnapshot(), statsBefore)
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "updates/sec")
	}
	reportCollectionUpdateStatsForBenchmark(b, stats, b.N)
	if err := manager.FlushAll(); err != nil {
		b.Fatalf("flush updates: %v", err)
	}
}

func benchmarkBSONSetCombinerYCSBDocument(id string, ordinal int, fieldCount int, fieldLength int) bson.D {
	doc := make(bson.D, 0, fieldCount+1)
	doc = append(doc, bson.E{Key: "_id", Value: id})
	for field := 0; field < fieldCount; field++ {
		doc = append(doc, bson.E{
			Key:   "field" + strconv.Itoa(field),
			Value: benchmarkBSONSetCombinerYCSBFieldValue(ordinal, field, fieldLength),
		})
	}
	return doc
}

func benchmarkBSONSetCombinerYCSBFieldValue(ordinal int, field int, length int) []byte {
	out := make([]byte, length)
	seed := byte((ordinal + field*17) & 0xff)
	for i := range out {
		out[i] = seed + byte(i)
	}
	return out
}

func mustBenchmarkBSONSetUpdate(fields []BSONSetField) bsonSetUpdate {
	spec, err := newBSONSetUpdate(fields)
	if err != nil {
		panic(err)
	}
	return spec
}
