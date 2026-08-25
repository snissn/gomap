package treedb

import (
	"strconv"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestNativeRootCollectionWriteAdvancesCachedForegroundActivity(t *testing.T) {
	opts := OptionsFor(ProfileCommandWALDurable, t.TempDir())
	opts.IndexOuterLeavesInValueLog = true
	database, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	manager := collections.NewCollectionManager(database.backend)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "native-root-foreground",
		Options: collections.CollectionOptions{
			DocumentFormat:          collections.DocumentFormatJSON,
			DataRootStoragePolicy:   collections.RootStorageCompressed,
			IndexStateStoragePolicy: collections.RootStorageCompressed,
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	collection, err := manager.OpenCollection("native-root-foreground")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	const stat = "treedb.cache.vlog_generation.maintenance.foreground_last_write_unix_nano"
	before, err := strconv.ParseInt(database.cached.Stats()[stat], 10, 64)
	if err != nil {
		t.Fatalf("parse %s before write: %v", stat, err)
	}
	time.Sleep(time.Millisecond)
	if _, err := collection.InsertBatch(
		[][]byte{[]byte("doc-1")},
		[][]byte{[]byte(`{"_id":"doc-1","value":1}`)},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	after, err := strconv.ParseInt(database.cached.Stats()[stat], 10, 64)
	if err != nil {
		t.Fatalf("parse %s after write: %v", stat, err)
	}
	if after <= before {
		t.Fatalf("foreground write timestamp did not advance: before=%d after=%d", before, after)
	}
}
