package treedb

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestCachedCollectionsCreateCollection_SystemMetadataBufferedUntilCheckpoint(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.OpenCollection(meta.Name); err != nil {
		t.Fatalf("open collection from cached view: %v", err)
	}

	metaKey, err := collections.SystemCollectionMetaKey(meta.Name)
	if err != nil {
		t.Fatalf("system meta key: %v", err)
	}
	primaryRootKey, err := collections.SystemCollectionRootKey(meta.PrimaryRoot)
	if err != nil {
		t.Fatalf("primary root key: %v", err)
	}

	rawMeta, err := d.backend.GetSystem(metaKey)
	if err != nil {
		t.Fatalf("backend get system meta before checkpoint: %v", err)
	}
	if len(rawMeta) != 0 {
		t.Fatalf("expected backend system meta to remain buffered before checkpoint")
	}
	rawRoot, err := d.backend.GetSystem(primaryRootKey)
	if err != nil {
		t.Fatalf("backend get root descriptor before checkpoint: %v", err)
	}
	if len(rawRoot) != 0 {
		t.Fatalf("expected backend root descriptor to remain buffered before checkpoint")
	}

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	rawMeta, err = d.backend.GetSystem(metaKey)
	if err != nil {
		t.Fatalf("backend get system meta after checkpoint: %v", err)
	}
	if len(rawMeta) == 0 {
		t.Fatalf("expected backend system meta after checkpoint")
	}
	rawRoot, err = d.backend.GetSystem(primaryRootKey)
	if err != nil {
		t.Fatalf("backend get root descriptor after checkpoint: %v", err)
	}
	if len(rawRoot) == 0 {
		t.Fatalf("expected backend root descriptor after checkpoint")
	}
}

func TestCachedCollectionsCreateIndex_SystemMetadataBufferedUntilCheckpoint(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint collection create: %v", err)
	}

	indexDef, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true})
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection from cached view: %v", err)
	}
	found := false
	for _, idx := range col.Meta().Indexes {
		if idx.Name == "email_idx" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected cached view to see email_idx before checkpoint")
	}

	indexKey, err := collections.SystemIndexKey(meta.Name, indexDef.Name)
	if err != nil {
		t.Fatalf("system index key: %v", err)
	}
	indexRootKey, err := collections.SystemCollectionRootKey(indexDef.RootName)
	if err != nil {
		t.Fatalf("index root key: %v", err)
	}

	rawIndex, err := d.backend.GetSystem(indexKey)
	if err != nil {
		t.Fatalf("backend get index meta before checkpoint: %v", err)
	}
	if len(rawIndex) != 0 {
		t.Fatalf("expected backend index metadata to remain buffered before checkpoint")
	}
	rawRoot, err := d.backend.GetSystem(indexRootKey)
	if err != nil {
		t.Fatalf("backend get index root descriptor before checkpoint: %v", err)
	}
	if len(rawRoot) != 0 {
		t.Fatalf("expected backend index root descriptor to remain buffered before checkpoint")
	}

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint index create: %v", err)
	}

	rawIndex, err = d.backend.GetSystem(indexKey)
	if err != nil {
		t.Fatalf("backend get index meta after checkpoint: %v", err)
	}
	if len(rawIndex) == 0 {
		t.Fatalf("expected backend index metadata after checkpoint")
	}
	rawRoot, err = d.backend.GetSystem(indexRootKey)
	if err != nil {
		t.Fatalf("backend get index root descriptor after checkpoint: %v", err)
	}
	if len(rawRoot) == 0 {
		t.Fatalf("expected backend index root descriptor after checkpoint")
	}
}

func TestCachedCollectionsDropCollection_SystemMetadataBufferedUntilCheckpoint(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint collection create: %v", err)
	}

	metaKey, err := collections.SystemCollectionMetaKey(meta.Name)
	if err != nil {
		t.Fatalf("system meta key: %v", err)
	}
	beforeDrop, err := d.backend.GetSystem(metaKey)
	if err != nil {
		t.Fatalf("backend get system meta before drop: %v", err)
	}
	if len(beforeDrop) == 0 {
		t.Fatalf("expected backend collection metadata before drop")
	}

	if err := mgr.DropCollection(meta.Name); err != nil {
		t.Fatalf("drop collection: %v", err)
	}
	if _, err := mgr.OpenCollection(meta.Name); err == nil {
		t.Fatalf("expected cached view to hide dropped collection before checkpoint")
	}
	listed, err := mgr.ListCollections()
	if err != nil {
		t.Fatalf("list collections: %v", err)
	}
	for _, existing := range listed {
		if existing.Name == meta.Name {
			t.Fatalf("expected dropped collection to disappear from cached view before checkpoint")
		}
	}

	stillPersisted, err := d.backend.GetSystem(metaKey)
	if err != nil {
		t.Fatalf("backend get system meta after drop before checkpoint: %v", err)
	}
	if len(stillPersisted) == 0 {
		t.Fatalf("expected backend collection metadata to remain until checkpoint")
	}

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint drop: %v", err)
	}

	afterDrop, err := d.backend.GetSystem(metaKey)
	if err != nil {
		t.Fatalf("backend get system meta after checkpoint: %v", err)
	}
	if len(afterDrop) != 0 {
		t.Fatalf("expected backend collection metadata to be removed after checkpoint")
	}
}
