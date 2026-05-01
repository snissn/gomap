package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	mongogateway "github.com/snissn/gomap/TreeDB/mongo_gateway"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

var (
	errUpdatedPrimaryKey      = errors.New("profile benchmark update changed _id")
	errProfileBenchUpdateMiss = errors.New("profile benchmark update missed document")
)

const (
	// Keep update payloads bounded so the benchmark stresses collection update
	// concurrency instead of BSON allocation variety.
	profileBenchUpdateDocPoolSize = 4096
	// Preferred deterministic stride for scrambling sequential operation
	// numbers across the preloaded ID set.
	profileBenchPreferredUpdateIDStride = 37
)

func profileBenchUpdateIDStride(documentCount int) int {
	if documentCount <= 1 {
		return 1
	}
	stride := profileBenchPreferredUpdateIDStride
	if stride >= documentCount {
		stride %= documentCount
	}
	if stride <= 0 {
		stride = 1
	}
	for profileBenchGCD(stride, documentCount) != 1 {
		stride++
		if stride >= documentCount {
			stride = 1
		}
	}
	return stride
}

func profileBenchGCD(a, b int) int {
	if a < 0 {
		a = -a
	}
	if b < 0 {
		b = -b
	}
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func TestProfileBenchUpdateIDStrideCoversDocumentSet(t *testing.T) {
	for _, documentCount := range []int{1, 2, 37, 74, 1000} {
		stride := profileBenchUpdateIDStride(documentCount)
		seen := make(map[int]struct{}, documentCount)
		for op := 0; op < documentCount; op++ {
			seen[(op*stride)%documentCount] = struct{}{}
		}
		if len(seen) != documentCount {
			t.Fatalf("documentCount=%d stride=%d covered=%d", documentCount, stride, len(seen))
		}
	}
}

func BenchmarkTreeDBGatewayLoadBSONIndexes2(b *testing.B) {
	benchmarkTreeDBGatewayLoad(b, collections.DocumentFormatBSON, 2, false)
}

func BenchmarkTreeDBGatewayLoadGeneratedIDBSONIndexes2(b *testing.B) {
	benchmarkTreeDBGatewayLoadWithDocument(b, collections.DocumentFormatBSON, 2, false, benchmarkDocumentWithoutID)
}

func BenchmarkTreeDBGatewayLoadObjectIDBSONIndexes2(b *testing.B) {
	benchmarkTreeDBGatewayLoadWithDocument(b, collections.DocumentFormatBSON, 2, false, benchmarkDocumentWithObjectID)
}

func BenchmarkTreeDBGatewayLoadUnackBSONIndexes2(b *testing.B) {
	benchmarkTreeDBGatewayLoadWithDocument(b, collections.DocumentFormatBSON, 2, false, benchmarkDocument,
		options.Collection().SetWriteConcern(writeconcern.Unacknowledged()))
}

func BenchmarkTreeDBGatewayLoadRawBSONIndexes2(b *testing.B) {
	benchmarkTreeDBGatewayLoad(b, collections.DocumentFormatBSON, 2, true)
}

func BenchmarkTreeDBGatewayRunCommandLoadBSONIndexes2(b *testing.B) {
	benchmarkTreeDBGatewayRunCommandLoad(b, collections.DocumentFormatBSON, 2)
}

func BenchmarkTreeDBGatewayRunRawCommandLoadBSONIndexes2(b *testing.B) {
	benchmarkTreeDBGatewayRunRawCommandLoad(b, collections.DocumentFormatBSON, 2)
}

func BenchmarkTreeDBGatewayRawWireLoadBSONIndexes2(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "treedb")
	opts := treedb.OptionsFor(treedb.ProfileWALOnFast, dir)
	opts.IndexOuterLeavesInValueLog = true
	opts.IndexInternalBaseDelta = false
	backend, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		b.Fatalf("open backend: %v", err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			b.Fatalf("close backend: %v", err)
		}
	}()
	manager := collections.NewCollectionManager(backend)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "bench.docs",
		Options: collections.CollectionOptions{
			DocumentFormat:          collections.DocumentFormatBSON,
			DataRootStoragePolicy:   collections.RootStorageCompressed,
			IndexStateStoragePolicy: collections.RootStorageCompressed,
		},
	}); err != nil {
		b.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("bench.docs")
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}
	if _, err := collection.CreateIndex(collections.IndexDefinition{
		Name:          "email_1",
		Field:         "email",
		Unique:        true,
		StoragePolicy: collections.RootStorageCompressed,
	}); err != nil {
		b.Fatalf("create email index: %v", err)
	}
	if _, err := collection.CreateIndex(collections.IndexDefinition{
		Name:          "city_1",
		Field:         "city",
		StoragePolicy: collections.RootStorageCompressed,
	}); err != nil {
		b.Fatalf("create city index: %v", err)
	}
	server := mongogateway.NewServer()
	server.Collections = manager
	server.MaxFindScanDocuments = b.N
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat:          collections.DocumentFormatBSON,
		DataRootStoragePolicy:   collections.RootStorageCompressed,
		IndexStateStoragePolicy: collections.RootStorageCompressed,
	}
	server.DefaultIndexStoragePolicy = collections.RootStorageCompressed
	commandDoc := mustProfileBenchDocument(b, bson.D{
		{Key: "insert", Value: "docs"},
		{Key: "ordered", Value: true},
		{Key: "$db", Value: "bench"},
	})

	batchSize := profileBenchBatchSize(b)
	b.ReportAllocs()
	var timedElapsed time.Duration
	var requestID int32
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		count := batchSize
		if remaining := b.N - inserted; remaining < count {
			count = remaining
		}
		docs := make([]wire.Document, count)
		for i := 0; i < count; i++ {
			raw, err := bson.Marshal(benchmarkDocument(inserted + i))
			if err != nil {
				b.Fatalf("marshal BSON document: %v", err)
			}
			docs[i] = wire.Document(raw)
		}
		requestID++
		b.StartTimer()
		batchStart := time.Now()
		msg, err := wire.AppendMsgMessageWithSequences(nil, requestID, 0, 0, commandDoc, []wire.DocumentSequence{{
			Identifier: "documents",
			Documents:  docs,
		}})
		if err != nil {
			b.Fatalf("append raw wire insert message: %v", err)
		}
		rw := profileBenchReadWriter{r: bytes.NewReader(msg)}
		if err := server.ServeOneWithOwner(&rw, 1); err != nil {
			b.Fatalf("serve raw wire insert: %v", err)
		}
		if err := assertRawWireInsertOK(rw.w.Bytes(), count); err != nil {
			b.Fatalf("raw wire insert response: %v", err)
		}
		timedElapsed += time.Since(batchStart)
		inserted += count
	}
	b.StopTimer()
	reportDocsPerSecond(b, b.N, timedElapsed)
}

func BenchmarkTreeDBGatewayRawWireTCPLoadBSONIndexes2(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()
	cfg := config{
		Target:                      "treedb",
		TreeDBDir:                   dir,
		Database:                    "bench",
		Collection:                  "docs",
		BatchSize:                   profileBenchBatchSize(b),
		SecondaryIndexes:            2,
		TreeDBProfile:               treedb.ProfileWALOnFast,
		TreeDBDocumentFormat:        collections.DocumentFormatBSON,
		TreeDBDataRootStorage:       collections.RootStorageCompressed,
		TreeDBIndexStateRootStorage: collections.RootStorageCompressed,
		TreeDBIndexRootStorage:      collections.RootStorageCompressed,
		TreeDBMaintenance:           treeDBMaintenanceNone,
		Timeout:                     0,
	}
	target, err := openTreeDBTarget(ctx, cfg)
	if err != nil {
		b.Fatalf("open target: %v", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := closeBenchTarget(cleanupCtx, target); err != nil {
			b.Fatalf("close target: %v", err)
		}
	}()
	coll := target.client.Database(cfg.Database).Collection(cfg.Collection)
	if err := createIndexes(ctx, coll, cfg.SecondaryIndexes); err != nil {
		b.Fatalf("create indexes: %v", err)
	}
	commandDoc := mustProfileBenchDocument(b, bson.D{
		{Key: "insert", Value: cfg.Collection},
		{Key: "ordered", Value: true},
		{Key: "$db", Value: cfg.Database},
	})
	conn, err := net.Dial("tcp", target.mongoAddr)
	if err != nil {
		b.Fatalf("dial raw-wire tcp: %v", err)
	}
	defer conn.Close()

	batchSize := profileBenchBatchSize(b)
	b.ReportAllocs()
	var timedElapsed time.Duration
	var requestID int32
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		count := batchSize
		if remaining := b.N - inserted; remaining < count {
			count = remaining
		}
		docs := make([]wire.Document, count)
		for i := 0; i < count; i++ {
			raw, err := bson.Marshal(benchmarkDocument(inserted + i))
			if err != nil {
				b.Fatalf("marshal BSON document: %v", err)
			}
			docs[i] = wire.Document(raw)
		}
		requestID++
		b.StartTimer()
		batchStart := time.Now()
		if err := serveRawWireTCPInsert(conn, requestID, commandDoc, docs); err != nil {
			b.Fatalf("serve raw wire tcp insert: %v", err)
		}
		timedElapsed += time.Since(batchStart)
		inserted += count
	}
	b.StopTimer()
	reportDocsPerSecond(b, b.N, timedElapsed)
}

func BenchmarkDirectCollectionLoadBSONIndexes2(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "treedb")
	opts := treedb.OptionsFor(treedb.ProfileWALOnFast, dir)
	opts.IndexOuterLeavesInValueLog = true
	opts.IndexInternalBaseDelta = false
	backend, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		b.Fatalf("open backend: %v", err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			b.Fatalf("close backend: %v", err)
		}
	}()
	manager := collections.NewCollectionManager(backend)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "bench.docs",
		Options: collections.CollectionOptions{
			DocumentFormat:          collections.DocumentFormatBSON,
			DataRootStoragePolicy:   collections.RootStorageCompressed,
			IndexStateStoragePolicy: collections.RootStorageCompressed,
		},
	}); err != nil {
		b.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("bench.docs")
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}
	if _, err := collection.CreateIndex(collections.IndexDefinition{
		Name:          "email_1",
		Field:         "email",
		Unique:        true,
		StoragePolicy: collections.RootStorageCompressed,
	}); err != nil {
		b.Fatalf("create email index: %v", err)
	}
	if _, err := collection.CreateIndex(collections.IndexDefinition{
		Name:          "city_1",
		Field:         "city",
		StoragePolicy: collections.RootStorageCompressed,
	}); err != nil {
		b.Fatalf("create city index: %v", err)
	}

	batchSize := profileBenchBatchSize(b)
	b.ReportAllocs()
	var timedElapsed time.Duration
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		count := batchSize
		if remaining := b.N - inserted; remaining < count {
			count = remaining
		}
		ids := make([][]byte, count)
		docs := make([][]byte, count)
		for i := 0; i < count; i++ {
			docNum := inserted + i
			ids[i] = []byte(benchmarkID(docNum))
			raw, err := bson.Marshal(benchmarkDocument(docNum))
			if err != nil {
				b.Fatalf("marshal BSON document: %v", err)
			}
			docs[i] = raw
		}
		b.StartTimer()
		batchStart := time.Now()
		if _, err := collection.InsertBatchValidatedBSON(ids, docs); err != nil {
			b.Fatalf("insert batch: %v", err)
		}
		timedElapsed += time.Since(batchStart)
		inserted += count
	}
	b.StopTimer()
	if err := manager.FlushAll(); err != nil {
		b.Fatalf("flush collections: %v", err)
	}
	if err := backend.Checkpoint(); err != nil {
		b.Fatalf("checkpoint backend: %v", err)
	}
	reportDocsPerSecond(b, b.N, timedElapsed)
}

func BenchmarkDirectCollectionConcurrentUpdateBSONIndexes2(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "treedb")
	opts := treedb.OptionsFor(treedb.ProfileWALOnFast, dir)
	opts.IndexOuterLeavesInValueLog = true
	opts.IndexInternalBaseDelta = false
	backend, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		b.Fatalf("open backend: %v", err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			b.Fatalf("close backend: %v", err)
		}
	}()
	manager := collections.NewCollectionManager(backend)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "bench.docs",
		Options: collections.CollectionOptions{
			DocumentFormat:          collections.DocumentFormatBSON,
			DataRootStoragePolicy:   collections.RootStorageCompressed,
			IndexStateStoragePolicy: collections.RootStorageCompressed,
		},
	}); err != nil {
		b.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("bench.docs")
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}
	if _, err := collection.CreateIndex(collections.IndexDefinition{
		Name:          "email_1",
		Field:         "email",
		Unique:        true,
		StoragePolicy: collections.RootStorageCompressed,
	}); err != nil {
		b.Fatalf("create email index: %v", err)
	}
	if _, err := collection.CreateIndex(collections.IndexDefinition{
		Name:          "city_1",
		Field:         "city",
		StoragePolicy: collections.RootStorageCompressed,
	}); err != nil {
		b.Fatalf("create city index: %v", err)
	}

	documentCount := profileBenchUpdateDocumentCount(b)
	batchSize := profileBenchBatchSize(b)
	for inserted := 0; inserted < documentCount; {
		count := batchSize
		if remaining := documentCount - inserted; remaining < count {
			count = remaining
		}
		ids := make([][]byte, count)
		docs := make([][]byte, count)
		for i := 0; i < count; i++ {
			docNum := inserted + i
			ids[i] = []byte(benchmarkID(docNum))
			raw, err := bson.Marshal(benchmarkDocument(docNum))
			if err != nil {
				b.Fatalf("marshal BSON document: %v", err)
			}
			docs[i] = raw
		}
		if _, err := collection.InsertBatchValidatedBSON(ids, docs); err != nil {
			b.Fatalf("insert preload batch: %v", err)
		}
		inserted += count
	}
	if err := manager.FlushAll(); err != nil {
		b.Fatalf("flush preload: %v", err)
	}
	if err := backend.Checkpoint(); err != nil {
		b.Fatalf("checkpoint preload: %v", err)
	}
	updateDocs := make([]bson.Raw, profileBenchUpdateDocPoolSize)
	for i := range updateDocs {
		updateRaw, err := bson.Marshal(bson.D{{Key: "$set", Value: bson.D{
			{Key: "concurrent_updated", Value: true},
			{Key: "concurrent_update_seq", Value: int64(i)},
		}}})
		if err != nil {
			b.Fatalf("marshal update document: %v", err)
		}
		updateDocs[i] = bson.Raw(updateRaw)
	}
	ids := make([][]byte, documentCount)
	for i := range ids {
		ids[i] = []byte(benchmarkID(i))
	}
	idStride := profileBenchUpdateIDStride(documentCount)

	writers := profileBenchConcurrentWriters(b)
	b.ReportAllocs()
	b.ResetTimer()
	started := time.Now()
	err = runConcurrentOperations(context.Background(), writers, b.N, func(op int) error {
		id := ids[(op*idStride)%documentCount]
		updateRaw := updateDocs[op%len(updateDocs)]
		matched, _, err := collection.Update(id, func(stored []byte) ([]byte, bool, error) {
			raw := bson.Raw(stored)
			originalID := raw.Lookup("_id")
			updated, shouldWrite, err := profileBenchApplySetUpdate(raw, updateRaw)
			if err != nil {
				return nil, false, err
			}
			if !updated.Lookup("_id").Equal(originalID) {
				return nil, false, errUpdatedPrimaryKey
			}
			return []byte(updated), shouldWrite, nil
		})
		if err != nil {
			return err
		}
		if !matched {
			return errProfileBenchUpdateMiss
		}
		return nil
	})
	timedElapsed := time.Since(started)
	b.StopTimer()
	if err != nil {
		b.Fatalf("run concurrent updates: %v", err)
	}
	b.ReportMetric(float64(writers), "writers")
	reportDocsPerSecond(b, b.N, timedElapsed)
}

func profileBenchApplySetUpdate(doc bson.Raw, update bson.Raw) (bson.Raw, bool, error) {
	updateElements, err := update.Elements()
	if err != nil {
		return nil, false, err
	}
	if len(updateElements) != 1 {
		return nil, false, errors.New("profile benchmark update supports exactly one $set operator")
	}
	operator, err := updateElements[0].KeyErr()
	if err != nil {
		return nil, false, err
	}
	if operator != "$set" {
		return nil, false, errors.New("profile benchmark update supports $set only")
	}
	setDoc, ok := updateElements[0].Value().DocumentOK()
	if !ok {
		return nil, false, errors.New("profile benchmark $set value must be a document")
	}
	setElements, err := setDoc.Elements()
	if err != nil {
		return nil, false, err
	}
	if len(setElements) == 0 {
		return doc, false, nil
	}
	sets := make(map[string]bson.RawValue, len(setElements))
	setOrder := make([]string, 0, len(setElements))
	for _, elem := range setElements {
		key, err := elem.KeyErr()
		if err != nil {
			return nil, false, err
		}
		if key == "" {
			return nil, false, errors.New("profile benchmark $set field name cannot be empty")
		}
		if key == "_id" {
			return nil, false, errors.New("profile benchmark update cannot modify _id")
		}
		if strings.Contains(key, ".") {
			return nil, false, errors.New("profile benchmark $set currently supports top-level fields only")
		}
		if strings.HasPrefix(key, "$") {
			return nil, false, errors.New("profile benchmark $set field names cannot start with $")
		}
		if _, exists := sets[key]; !exists {
			setOrder = append(setOrder, key)
		}
		sets[key] = elem.Value()
	}

	elements, err := doc.Elements()
	if err != nil {
		return nil, false, err
	}
	out := make(bson.D, 0, len(elements)+len(sets))
	used := make(map[string]struct{}, len(sets))
	// Force the benchmark down the write/index update path for any non-empty $set.
	forceWrite := true
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return nil, false, err
		}
		value := elem.Value()
		if replacement, ok := sets[key]; ok {
			value = replacement
			used[key] = struct{}{}
		}
		out = append(out, bson.E{Key: key, Value: value})
	}
	for _, key := range setOrder {
		if _, ok := used[key]; ok {
			continue
		}
		out = append(out, bson.E{Key: key, Value: sets[key]})
	}
	raw, err := bson.Marshal(out)
	if err != nil {
		return nil, false, err
	}
	return bson.Raw(raw), forceWrite, nil
}

func TestProfileBenchApplySetUpdateHappyPath(t *testing.T) {
	docRaw, err := bson.Marshal(bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "name", Value: "ada"},
		{Key: "count", Value: int32(1)},
	})
	if err != nil {
		t.Fatalf("marshal doc: %v", err)
	}
	updateRaw, err := bson.Marshal(bson.D{{Key: "$set", Value: bson.D{
		{Key: "name", Value: "grace"},
		{Key: "city", Value: "hnl"},
		{Key: "count", Value: int32(1)},
	}}})
	if err != nil {
		t.Fatalf("marshal update: %v", err)
	}
	updated, changed, err := profileBenchApplySetUpdate(bson.Raw(docRaw), bson.Raw(updateRaw))
	if err != nil {
		t.Fatalf("apply update: %v", err)
	}
	if !changed {
		t.Fatal("changed=false want true for non-empty $set")
	}
	if got, ok := updated.Lookup("_id").StringValueOK(); !ok || got != "u1" {
		t.Fatalf("_id=%q ok=%v want u1", got, ok)
	}
	if got, ok := updated.Lookup("name").StringValueOK(); !ok || got != "grace" {
		t.Fatalf("name=%q ok=%v want grace", got, ok)
	}
	if got, ok := updated.Lookup("city").StringValueOK(); !ok || got != "hnl" {
		t.Fatalf("city=%q ok=%v want hnl", got, ok)
	}
	if got, ok := updated.Lookup("count").Int32OK(); !ok || got != 1 {
		t.Fatalf("count=%d ok=%v want 1", got, ok)
	}
	_, changed, err = profileBenchApplySetUpdate(updated, bson.Raw(updateRaw))
	if err != nil {
		t.Fatalf("reapply update: %v", err)
	}
	if !changed {
		t.Fatal("reapply changed=false want true so benchmark exercises write path")
	}
}

func TestProfileBenchApplySetUpdateRejectsGatewayInvalidSetFields(t *testing.T) {
	docRaw, err := bson.Marshal(bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "ada"}})
	if err != nil {
		t.Fatalf("marshal doc: %v", err)
	}
	tests := []struct {
		name string
		key  string
	}{
		{name: "empty", key: ""},
		{name: "id", key: "_id"},
		{name: "dotted", key: "profile.name"},
		{name: "dollar", key: "$name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRaw, err := bson.Marshal(bson.D{{Key: "$set", Value: bson.D{{Key: tt.key, Value: "grace"}}}})
			if err != nil {
				t.Fatalf("marshal update: %v", err)
			}
			if _, _, err := profileBenchApplySetUpdate(bson.Raw(docRaw), bson.Raw(updateRaw)); err == nil {
				t.Fatalf("profileBenchApplySetUpdate accepted key %q", tt.key)
			}
		})
	}
}

type profileBenchReadWriter struct {
	r *bytes.Reader
	w bytes.Buffer
}

func (rw *profileBenchReadWriter) Read(p []byte) (int, error) {
	return rw.r.Read(p)
}

func (rw *profileBenchReadWriter) Write(p []byte) (int, error) {
	return rw.w.Write(p)
}

func mustProfileBenchDocument(tb testing.TB, doc bson.D) wire.Document {
	tb.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		tb.Fatalf("marshal profile benchmark document: %v", err)
	}
	return wire.Document(raw)
}

func BenchmarkClientBSONBatchEncode(b *testing.B) {
	docs := make([]bson.D, profileBenchBatchSize(b))
	for i := range docs {
		docs[i] = benchmarkDocument(i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := bson.Marshal(docs[i%len(docs)]); err != nil {
			b.Fatalf("marshal BSON document: %v", err)
		}
	}
}

func benchmarkTreeDBGatewayLoad(b *testing.B, format collections.DocumentFormat, secondaryIndexes int, rawDocs bool) {
	benchmarkTreeDBGatewayLoadWithDocument(b, format, secondaryIndexes, rawDocs, benchmarkDocument)
}

func benchmarkTreeDBGatewayLoadWithDocument(b *testing.B, format collections.DocumentFormat, secondaryIndexes int, rawDocs bool, document func(int) bson.D, collectionOptions ...options.Lister[options.CollectionOptions]) {
	ctx := context.Background()
	dir := b.TempDir()
	cfg := config{
		Target:                      "treedb",
		TreeDBDir:                   dir,
		Database:                    "bench",
		Collection:                  "docs",
		BatchSize:                   profileBenchBatchSize(b),
		SecondaryIndexes:            secondaryIndexes,
		TreeDBProfile:               treedb.ProfileWALOnFast,
		TreeDBDocumentFormat:        format,
		TreeDBDataRootStorage:       collections.RootStorageCompressed,
		TreeDBIndexStateRootStorage: collections.RootStorageCompressed,
		TreeDBIndexRootStorage:      collections.RootStorageCompressed,
		TreeDBMaintenance:           treeDBMaintenanceNone,
		Timeout:                     0,
	}
	target, err := openTreeDBTarget(ctx, cfg)
	if err != nil {
		b.Fatalf("open target: %v", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := closeBenchTarget(cleanupCtx, target); err != nil {
			b.Fatalf("close target: %v", err)
		}
	}()
	coll := target.client.Database(cfg.Database).Collection(cfg.Collection, collectionOptions...)
	if err := createIndexes(ctx, coll, secondaryIndexes); err != nil {
		b.Fatalf("create indexes: %v", err)
	}

	b.ReportAllocs()
	var timedElapsed time.Duration
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		count := cfg.BatchSize
		if remaining := b.N - inserted; remaining < count {
			count = remaining
		}
		docs := make([]any, 0, count)
		for i := 0; i < count; i++ {
			doc := document(inserted + i)
			if rawDocs {
				raw, err := bson.Marshal(doc)
				if err != nil {
					b.Fatalf("marshal BSON document: %v", err)
				}
				docs = append(docs, bson.Raw(raw))
			} else {
				docs = append(docs, doc)
			}
		}
		b.StartTimer()
		batchStart := time.Now()
		if _, err := coll.InsertMany(ctx, docs); err != nil {
			b.Fatalf("insert many: %v", err)
		}
		timedElapsed += time.Since(batchStart)
		inserted += count
	}
	b.StopTimer()
	reportDocsPerSecond(b, b.N, timedElapsed)
}

func benchmarkDocumentWithoutID(i int) bson.D {
	doc := benchmarkDocument(i)
	return doc[1:]
}

func benchmarkDocumentWithObjectID(i int) bson.D {
	doc := benchmarkDocument(i)
	doc[0].Value = benchmarkObjectID(i)
	return doc
}

func benchmarkObjectID(i int) bson.ObjectID {
	var id bson.ObjectID
	binary.BigEndian.PutUint32(id[0:4], 1)
	binary.BigEndian.PutUint64(id[4:12], uint64(i))
	return id
}

func benchmarkTreeDBGatewayRunCommandLoad(b *testing.B, format collections.DocumentFormat, secondaryIndexes int) {
	ctx := context.Background()
	dir := b.TempDir()
	cfg := config{
		Target:                      "treedb",
		TreeDBDir:                   dir,
		Database:                    "bench",
		Collection:                  "docs",
		BatchSize:                   profileBenchBatchSize(b),
		SecondaryIndexes:            secondaryIndexes,
		TreeDBProfile:               treedb.ProfileWALOnFast,
		TreeDBDocumentFormat:        format,
		TreeDBDataRootStorage:       collections.RootStorageCompressed,
		TreeDBIndexStateRootStorage: collections.RootStorageCompressed,
		TreeDBIndexRootStorage:      collections.RootStorageCompressed,
		TreeDBMaintenance:           treeDBMaintenanceNone,
		Timeout:                     0,
	}
	target, err := openTreeDBTarget(ctx, cfg)
	if err != nil {
		b.Fatalf("open target: %v", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := closeBenchTarget(cleanupCtx, target); err != nil {
			b.Fatalf("close target: %v", err)
		}
	}()
	coll := target.client.Database(cfg.Database).Collection(cfg.Collection)
	if err := createIndexes(ctx, coll, secondaryIndexes); err != nil {
		b.Fatalf("create indexes: %v", err)
	}
	db := target.client.Database(cfg.Database)

	b.ReportAllocs()
	var timedElapsed time.Duration
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		count := cfg.BatchSize
		if remaining := b.N - inserted; remaining < count {
			count = remaining
		}
		docs := make(bson.A, 0, count)
		for i := 0; i < count; i++ {
			docs = append(docs, benchmarkDocument(inserted+i))
		}
		b.StartTimer()
		batchStart := time.Now()
		err := db.RunCommand(ctx, bson.D{
			{Key: "insert", Value: cfg.Collection},
			{Key: "documents", Value: docs},
			{Key: "ordered", Value: true},
		}).Err()
		timedElapsed += time.Since(batchStart)
		if err != nil {
			b.Fatalf("run insert command: %v", err)
		}
		inserted += count
	}
	b.StopTimer()
	reportDocsPerSecond(b, b.N, timedElapsed)
}

func benchmarkTreeDBGatewayRunRawCommandLoad(b *testing.B, format collections.DocumentFormat, secondaryIndexes int) {
	ctx := context.Background()
	dir := b.TempDir()
	cfg := config{
		Target:                      "treedb",
		TreeDBDir:                   dir,
		Database:                    "bench",
		Collection:                  "docs",
		BatchSize:                   profileBenchBatchSize(b),
		SecondaryIndexes:            secondaryIndexes,
		TreeDBProfile:               treedb.ProfileWALOnFast,
		TreeDBDocumentFormat:        format,
		TreeDBDataRootStorage:       collections.RootStorageCompressed,
		TreeDBIndexStateRootStorage: collections.RootStorageCompressed,
		TreeDBIndexRootStorage:      collections.RootStorageCompressed,
		TreeDBMaintenance:           treeDBMaintenanceNone,
		Timeout:                     0,
	}
	target, err := openTreeDBTarget(ctx, cfg)
	if err != nil {
		b.Fatalf("open target: %v", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := closeBenchTarget(cleanupCtx, target); err != nil {
			b.Fatalf("close target: %v", err)
		}
	}()
	coll := target.client.Database(cfg.Database).Collection(cfg.Collection)
	if err := createIndexes(ctx, coll, secondaryIndexes); err != nil {
		b.Fatalf("create indexes: %v", err)
	}
	db := target.client.Database(cfg.Database)

	b.ReportAllocs()
	var timedElapsed time.Duration
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		count := cfg.BatchSize
		if remaining := b.N - inserted; remaining < count {
			count = remaining
		}
		rawDocs := make([]bson.Raw, count)
		for i := 0; i < count; i++ {
			raw, err := bson.Marshal(benchmarkDocument(inserted + i))
			if err != nil {
				b.Fatalf("marshal BSON document: %v", err)
			}
			rawDocs[i] = raw
		}
		command, err := rawInsertCommand(cfg.Collection, 0, count, nil, rawDocs)
		if err != nil {
			b.Fatalf("build raw insert command: %v", err)
		}
		b.StartTimer()
		batchStart := time.Now()
		err = db.RunCommand(ctx, command).Err()
		timedElapsed += time.Since(batchStart)
		if err != nil {
			b.Fatalf("run raw insert command: %v", err)
		}
		inserted += count
	}
	b.StopTimer()
	reportDocsPerSecond(b, b.N, timedElapsed)
}

func profileBenchBatchSize(tb testing.TB) int {
	return profileBenchPositiveEnvInt(tb, "MONGO_GATEWAY_PROFILE_BENCH_BATCH_SIZE", 10000)
}

func profileBenchUpdateDocumentCount(tb testing.TB) int {
	return profileBenchPositiveEnvInt(tb, "MONGO_GATEWAY_PROFILE_BENCH_UPDATE_DOCUMENTS", 100000)
}

func profileBenchConcurrentWriters(tb testing.TB) int {
	return profileBenchPositiveEnvInt(tb, "MONGO_GATEWAY_PROFILE_BENCH_WRITERS", 8)
}

func profileBenchPositiveEnvInt(tb testing.TB, name string, defaultValue int) int {
	tb.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		tb.Fatalf("invalid %s=%q", name, raw)
	}
	return value
}

func reportDocsPerSecond(b *testing.B, docs int, elapsed time.Duration) {
	b.Helper()
	if elapsed <= 0 {
		return
	}
	b.ReportMetric(float64(docs)/elapsed.Seconds(), "docs/sec")
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(docs), "ns/doc")
}
