package collections_test

import (
	"bytes"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func BenchmarkCollectionUpdate_CallbackVsUpdateBatchJSON_NoIndex_Size1(b *testing.B) {
	benchmarkCollectionUpdateCallbackVsUpdateBatch(b, 0)
}

func BenchmarkCollectionUpdate_CallbackVsUpdateBatchJSON_OneIndex_Size1(b *testing.B) {
	benchmarkCollectionUpdateCallbackVsUpdateBatch(b, 1)
}

func benchmarkCollectionUpdateCallbackVsUpdateBatch(b *testing.B, secondaryIndexes int) {
	b.Helper()
	if secondaryIndexes < 0 || secondaryIndexes > 1 {
		b.Fatalf("unsupported secondaryIndexes=%d", secondaryIndexes)
	}

	const (
		docCount         = 10000
		preloadBatchSize = 1000
	)
	indexes := []collections.IndexDefinition{}
	if secondaryIndexes == 1 {
		indexes = append(indexes, collections.IndexDefinition{
			Name:      "pad_1",
			Field:     "pad",
			ValueType: collections.IndexValueString,
		})
	}

	openCollection := func() (*collections.Collection, [][]byte, func()) {
		dbDir := b.TempDir()
		opts := treedb.OptionsFor(treedb.ProfileFast, dbDir)
		opts.IndexOuterLeavesInValueLog = true
		opts.Durability = treedb.DurabilityWALOffRelaxed
		backend, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
		if err != nil {
			b.Fatalf("open backend: %v", err)
		}
		manager := collections.NewCollectionManager(backend)
		meta := &collections.CollectionMeta{
			Name: "bench.docs",
			Options: collections.CollectionOptions{
				DocumentFormat:          collections.DocumentFormatJSON,
				DataRootStoragePolicy:   collections.RootStorageCompressed,
				IndexStateStoragePolicy: collections.RootStorageCompressed,
				BufferedIndexedWrites:   secondaryIndexes > 0,
			},
			Indexes: make([]collections.IndexDefinition, len(indexes)),
		}
		copy(meta.Indexes, indexes)
		if _, err := manager.CreateCollection(meta); err != nil {
			_ = cleanup()
			b.Fatalf("create collection: %v", err)
		}
		collection, err := manager.OpenCollection("bench.docs")
		if err != nil {
			_ = cleanup()
			b.Fatalf("open collection: %v", err)
		}

		ids := make([][]byte, docCount)
		docs := make([][]byte, docCount)
		for i := 0; i < docCount; i++ {
			ids[i] = []byte("doc-" + strconv.Itoa(i))
			docs[i] = []byte(`{"_id":"` + string(ids[i]) + `","pad":"static","count":0}`)
		}
		for i := 0; i < docCount; i += preloadBatchSize {
			end := i + preloadBatchSize
			if end > docCount {
				end = docCount
			}
			if _, err := collection.InsertBatch(ids[i:end], docs[i:end]); err != nil {
				_ = cleanup()
				b.Fatalf("insert batch [%d:%d]: %v", i, end, err)
			}
		}
		if err := manager.FlushAll(); err != nil {
			_ = cleanup()
			b.Fatalf("flush preload docs: %v", err)
		}
		cleanupClosure := func() { _ = cleanup() }
		return collection, ids, cleanupClosure
	}

	updateFn := func(current []byte) ([]byte, bool, error) {
		next := append([]byte{}, current...)
		if bytes.Contains(next, []byte(`"count":0`)) {
			next = bytes.Replace(next, []byte(`"count":0`), []byte(`"count":1`), 1)
		} else {
			next = bytes.Replace(next, []byte(`"count":1`), []byte(`"count":0`), 1)
		}
		return next, true, nil
	}

	b.Run("Update", func(b *testing.B) {
		collection, ids, cleanup := openCollection()
		b.Cleanup(cleanup)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := ids[i%docCount]
			if _, _, err := collection.Update(id, updateFn); err != nil {
				b.Fatalf("update: %v", err)
			}
		}
	})

	b.Run("UpdateBatchSize1", func(b *testing.B) {
		collection, ids, cleanup := openCollection()
		b.Cleanup(cleanup)
		items := []collections.UpdateBatchItem{
			{
				DocumentID: ids[0],
				Update:     updateFn,
			},
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			items[0].DocumentID = ids[i%docCount]
			if _, err := collection.UpdateBatch(items); err != nil {
				b.Fatalf("update batch: %v", err)
			}
		}
	})

}

func BenchmarkCollectionUpdateBSONSetVsBatch_NoIndex_Size1(b *testing.B) {
	const docCount = 10000
	openCollection := func() (*collections.Collection, [][]byte, func()) {
		dbDir := b.TempDir()
		opts := treedb.OptionsFor(treedb.ProfileFast, dbDir)
		opts.IndexOuterLeavesInValueLog = true
		opts.Durability = treedb.DurabilityWALOffRelaxed
		backend, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
		if err != nil {
			b.Fatalf("open backend: %v", err)
		}
		manager := collections.NewCollectionManager(backend)
		meta := &collections.CollectionMeta{
			Name: "bench.docs",
			Options: collections.CollectionOptions{
				DocumentFormat:          collections.DocumentFormatBSON,
				DataRootStoragePolicy:   collections.RootStorageCompressed,
				IndexStateStoragePolicy: collections.RootStorageCompressed,
			},
		}
		if _, err := manager.CreateCollection(meta); err != nil {
			_ = cleanup()
			b.Fatalf("create collection: %v", err)
		}
		collection, err := manager.OpenCollection("bench.docs")
		if err != nil {
			_ = cleanup()
			b.Fatalf("open collection: %v", err)
		}
		ids := make([][]byte, docCount)
		docs := make([][]byte, docCount)
		for i := 0; i < docCount; i++ {
			id := "doc-" + strconv.Itoa(i)
			ids[i] = []byte(id)
			doc, err := bson.Marshal(bson.D{
				{Key: "_id", Value: id},
				{Key: "city", Value: "hnl"},
			})
			if err != nil {
				_ = cleanup()
				b.Fatalf("marshal doc: %v", err)
			}
			docs[i] = doc
		}
		if _, err := collection.InsertBatchValidatedBSON(ids, docs); err != nil {
			_ = cleanup()
			b.Fatalf("insert docs: %v", err)
		}
		if err := manager.FlushAll(); err != nil {
			_ = cleanup()
			b.Fatalf("flush preload docs: %v", err)
		}
		cleanupClosure := func() { _ = cleanup() }
		return collection, ids, cleanupClosure
	}

	seaType, seaRaw, err := bson.MarshalValue("sea")
	if err != nil {
		b.Fatalf("marshal sea value: %v", err)
	}
	sfoType, sfoRaw, err := bson.MarshalValue("sfo")
	if err != nil {
		b.Fatalf("marshal sfo value: %v", err)
	}
	values := []bson.RawValue{
		{Type: seaType, Value: seaRaw},
		{Type: sfoType, Value: sfoRaw},
	}

	b.Run("UpdateBSONSet", func(b *testing.B) {
		collection, ids, cleanup := openCollection()
		b.Cleanup(cleanup)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := ids[i%docCount]
			_, _, err := collection.UpdateBSONSet(id, []collections.BSONSetField{{
				Key:   "city",
				Value: values[i%2],
			}})
			if err != nil {
				b.Fatalf("UpdateBSONSet: %v", err)
			}
		}
	})

	b.Run("UpdateBSONSetBatchSize1", func(b *testing.B) {
		collection, ids, cleanup := openCollection()
		b.Cleanup(cleanup)
		item := []collections.BSONSetUpdateBatchItem{{
			DocumentID: ids[0],
			Fields: []collections.BSONSetField{{
				Key:   "city",
				Value: values[0],
			}},
		}}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			item[0].DocumentID = ids[i%docCount]
			item[0].Fields[0].Value = values[i%2]
			if _, _, err := collection.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges(item); err != nil {
				b.Fatalf("UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges: %v", err)
			}
		}
	})
}

func BenchmarkCollectionUpdateBSONSetYCSB_NoIndex_Size1(b *testing.B) {
	profiles := []struct {
		name    string
		profile treedb.Profile
	}{
		{name: "bench_no_wal", profile: treedb.ProfileBenchUnsafe},
		{name: "command_wal_relaxed", profile: treedb.ProfileCommandWALRelaxed},
	}
	for _, profile := range profiles {
		b.Run(profile.name, func(b *testing.B) {
			benchmarkCollectionUpdateBSONSetYCSBNoIndexSize1(b, profile.profile)
		})
	}
}

func benchmarkCollectionUpdateBSONSetYCSBNoIndexSize1(b *testing.B, profile treedb.Profile) {
	b.Helper()
	const (
		docCount         = 10000
		fieldCount       = 10
		fieldLength      = 100
		preloadBatchSize = 1000
	)
	openCollection := func() (*collections.Collection, [][]byte, func()) {
		dbDir := b.TempDir()
		opts := treedb.OptionsForBenchmark(profile, dbDir)
		opts.IndexOuterLeavesInValueLog = true
		backend, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
		if err != nil {
			b.Fatalf("open backend: %v", err)
		}
		manager := collections.NewCollectionManager(backend)
		meta := &collections.CollectionMeta{
			Name: "bench.docs",
			Options: collections.CollectionOptions{
				DocumentFormat:          collections.DocumentFormatBSON,
				DataRootStoragePolicy:   collections.RootStorageCompressed,
				IndexStateStoragePolicy: collections.RootStorageCompressed,
			},
		}
		if _, err := manager.CreateCollection(meta); err != nil {
			_ = cleanup()
			b.Fatalf("create collection: %v", err)
		}
		collection, err := manager.OpenCollection("bench.docs")
		if err != nil {
			_ = cleanup()
			b.Fatalf("open collection: %v", err)
		}
		ids := make([][]byte, docCount)
		docs := make([][]byte, docCount)
		for i := 0; i < docCount; i++ {
			id := "user" + strconv.Itoa(i)
			ids[i] = []byte(id)
			doc, err := bson.Marshal(benchmarkUpdateYCSBDocument(id, i, fieldCount, fieldLength))
			if err != nil {
				_ = cleanup()
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
				_ = cleanup()
				b.Fatalf("insert docs [%d:%d]: %v", i, end, err)
			}
		}
		if err := manager.FlushAll(); err != nil {
			_ = cleanup()
			b.Fatalf("flush preload docs: %v", err)
		}
		cleanupClosure := func() { _ = cleanup() }
		return collection, ids, cleanupClosure
	}

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
	collection, ids, cleanup := openCollection()
	b.Cleanup(cleanup)
	fields := []collections.BSONSetField{{
		Key:   "field0",
		Value: values[0],
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fields[0].Value = values[i&1]
		matched, _, err := collection.UpdateBSONSet(ids[i%docCount], fields)
		if err != nil {
			b.Fatalf("UpdateBSONSet: %v", err)
		}
		if !matched {
			b.Fatalf("UpdateBSONSet missing id %q", string(ids[i%docCount]))
		}
	}
}

func benchmarkUpdateYCSBDocument(id string, ordinal int, fieldCount int, fieldLength int) bson.D {
	doc := make(bson.D, 0, fieldCount+1)
	doc = append(doc, bson.E{Key: "_id", Value: id})
	for field := 0; field < fieldCount; field++ {
		doc = append(doc, bson.E{
			Key:   "field" + strconv.Itoa(field),
			Value: benchmarkUpdateYCSBFieldValue(ordinal, field, fieldLength),
		})
	}
	return doc
}

func benchmarkUpdateYCSBFieldValue(ordinal int, field int, length int) []byte {
	out := make([]byte, length)
	seed := byte((ordinal + field*17) & 0xff)
	for i := range out {
		out[i] = seed + byte(i)
	}
	return out
}
