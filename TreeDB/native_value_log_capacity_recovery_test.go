package treedb_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	mongogateway "github.com/snissn/gomap/TreeDB/mongo_gateway"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestStandaloneNativeBSONRecoverValueLogCapacity(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_CHECKPOINT_KICK", "1")
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.COWMemtableLimits = treedb.StandaloneCOWMemtableLimits()
	opts.FreshSystemPagerLeaves = true
	opts.FreshRelaxedStandaloneWALStream = true

	const name = "test.capacity"
	documents := []bson.D{
		{{Key: "_id", Value: "first"}, {Key: "data", Value: bson.Binary{Data: bytes.Repeat([]byte{1}, 8192)}}},
		{{Key: "_id", Value: "second"}, {Key: "data", Value: bson.Binary{Data: bytes.Repeat([]byte{2}, 8192)}}},
	}
	var ids, raw [][]byte
	for _, document := range documents {
		encoded, err := bson.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		id, err := mongogateway.EncodePrimaryKey(bson.Raw(encoded).Lookup("_id"))
		if err != nil {
			t.Fatal(err)
		}
		ids, raw = append(ids, id), append(raw, encoded)
	}

	run := func(options treedb.Options, operate func(*treedb.DB, *collections.CollectionManager)) {
		t.Helper()
		database, err := treedb.Open(options)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}()
		backend, err := database.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		manager := collections.NewCollectionManager(backend)
		defer func() {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}()
		operate(database, manager)
	}
	checkDocuments := func(manager *collections.CollectionManager) {
		t.Helper()
		collection, err := manager.OpenCollection(name)
		if err != nil {
			t.Fatal(err)
		}
		view, err := collection.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
		result, err := view.FetchDocumentsByID(ids, collections.DocumentFetchOptions{Format: collections.DocumentFormatBSON})
		if err = errors.Join(err, view.Close()); err != nil {
			t.Fatal(err)
		}
		if len(result.Results) != len(raw) {
			t.Fatalf("typed rows = %d, want %d", len(result.Results), len(raw))
		}
		for i, row := range result.Results {
			if !row.Found || !bytes.Equal(row.Document, raw[i]) {
				t.Fatalf("typed row %d missing or changed", i)
			}
		}
	}

	var valueDir string
	run(opts, func(database *treedb.DB, manager *collections.CollectionManager) {
		backend, err := database.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		valueDir = backenddb.ValueLogDirPath(backend.Dir())
		_, err = manager.CreateCollection(&collections.CollectionMeta{
			Name: name,
			Options: collections.CollectionOptions{
				DocumentFormat:        collections.DocumentFormatBSON,
				DataRootStoragePolicy: collections.RootStoragePager,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		collection, err := manager.OpenCollection(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := collection.InsertBatch(ids, raw); err != nil {
			t.Fatal(err)
		}
		if err := database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	})

	entries, err := os.ReadDir(valueDir)
	if err != nil {
		t.Fatal(err)
	}
	type durableFile struct {
		info os.FileInfo
		data []byte
	}
	durable := make(map[string]durableFile)
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() > 0 {
			path := filepath.Join(valueDir, entry.Name())
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			durable[path] = durableFile{info, data}
		}
	}
	if len(durable) == 0 {
		t.Fatal("typed seed did not create persistent value-log records")
	}

	// Model the stopped legacy inventory without depending on the old writer
	// rotation defect. Preserve the seed database and its original inodes.
	capacity := opts.COWMemtableLimits.MaxResources
	for i := range capacity {
		fileID, err := valuelog.EncodeFileID(0, uint32(1024+i))
		if err != nil {
			t.Fatal(err)
		}
		file, err := os.OpenFile(valuelog.SegmentPath(valueDir, fileID), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	count := func() int {
		t.Helper()
		entries, err := os.ReadDir(valueDir)
		if err != nil {
			t.Fatal(err)
		}
		return len(entries)
	}
	baseline := count()
	if baseline <= capacity || baseline >= 2*capacity {
		t.Fatalf("legacy inventory = %d, want greater than %d and below %d", baseline, capacity, 2*capacity)
	}

	run(opts, func(_ *treedb.DB, manager *collections.CollectionManager) {
		checkDocuments(manager)
	})
	if got := count(); got != baseline {
		t.Fatalf("ordinary reopen changed inventory from %d to %d", baseline, got)
	}

	repair := opts
	repair.COWMemtableLimits.MaxResources = 2 * capacity
	run(repair, func(database *treedb.DB, manager *collections.CollectionManager) {
		checkDocuments(manager)
		stats, err := database.ValueLogGC(t.Context(), treedb.ValueLogGCOptions{Mode: treedb.ValueLogGCModeStrict})
		if err != nil {
			t.Fatal(err)
		}
		if stats.SegmentsTotal <= capacity || stats.SegmentsReferenced == 0 || stats.SegmentsDeleted == 0 {
			t.Fatalf("strict GC did not process the over-limit inventory with live references: %+v", stats)
		}
		checkDocuments(manager)
	})
	if got := count(); got >= capacity {
		t.Fatalf("after checked maintenance Close, inventory = %d, want below %d", got, capacity)
	}
	run(opts, func(_ *treedb.DB, manager *collections.CollectionManager) {
		checkDocuments(manager)
	})
	for path, original := range durable {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !os.SameFile(original.info, info) || !bytes.Equal(original.data, data) {
			t.Fatalf("durable value-log file changed: %s", path)
		}
	}
}
