package collections

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestInsertBatchDirectScratchReuseAndReopenDurability(t *testing.T) {
	dir := t.TempDir()
	openDB := func() (*backenddb.DB, *CollectionManager) {
		d, err := backenddb.Open(backenddb.Options{
			Dir:                    dir,
			Durability:             backenddb.DurabilityWALOnRelaxed,
			DisableBackgroundPrune: true,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return d, NewCollectionManager(d)
	}

	d, mgr := openDB()
	meta := &CollectionMeta{
		Name: "scratch_two_idx",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatJSON,
			BufferedIndexedWrites: false,
		},
		Indexes: []IndexDefinition{
			{Name: "by_email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "by_region", Field: "region", ValueType: IndexValueString, Unique: false},
		},
	}
	if _, err := mgr.CreateCollection(meta); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection: %v", err)
	}

	regions := []string{"us-west", "eu-central", "ap-south", "us-east"}
	const (
		batches   = 4
		batchSize = 64
	)
	for b := 0; b < batches; b++ {
		ids := make([][]byte, batchSize)
		docs := make([][]byte, batchSize)
		for i := 0; i < batchSize; i++ {
			seq := b*batchSize + i
			ids[i] = []byte(fmt.Sprintf("doc-%06d", seq))
			docs[i] = []byte(fmt.Sprintf(`{"email":"u%06d@example.com","region":"%s","seq":%d}`, seq, regions[seq%len(regions)], seq))
		}
		gotIDs, err := col.InsertBatch(ids, docs)
		if err != nil {
			_ = d.Close()
			t.Fatalf("InsertBatch batch=%d: %v", b, err)
		}
		if len(gotIDs) != batchSize {
			_ = d.Close()
			t.Fatalf("InsertBatch batch=%d returned %d ids, want %d", b, len(gotIDs), batchSize)
		}
		for i := range ids {
			if !bytes.Equal(gotIDs[i], ids[i]) {
				_ = d.Close()
				t.Fatalf("InsertBatch batch=%d id[%d]=%q want %q", b, i, gotIDs[i], ids[i])
			}
		}
	}

	// Verify unique conflict error path cleanly releases scratch without corrupting subsequent inserts.
	_, conflictErr := col.InsertBatch(
		[][]byte{[]byte("doc-conflict-1")},
		[][]byte{[]byte(`{"email":"u000010@example.com","region":"us-west","seq":999}`)},
	)
	if !errors.Is(conflictErr, ErrUniqueIndexConflict) {
		_ = d.Close()
		t.Fatalf("unique conflict err=%v, want %v", conflictErr, ErrUniqueIndexConflict)
	}

	// Subsequent insert after error must succeed cleanly.
	extraID := []byte("doc-999999")
	extraDoc := []byte(`{"email":"u999999@example.com","region":"eu-central","seq":999999}`)
	if _, err := col.InsertBatch([][]byte{extraID}, [][]byte{extraDoc}); err != nil {
		_ = d.Close()
		t.Fatalf("post-conflict InsertBatch: %v", err)
	}

	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify every document + both indexes.
	d2, mgr2 := openDB()
	defer func() { _ = d2.Close() }()
	col2, err := mgr2.OpenCollection("scratch_two_idx")
	if err != nil {
		t.Fatalf("OpenCollection after reopen: %v", err)
	}
	total := batches * batchSize
	for seq := 0; seq < total; seq++ {
		id := []byte(fmt.Sprintf("doc-%06d", seq))
		wantDoc := fmt.Sprintf(`{"email":"u%06d@example.com","region":"%s","seq":%d}`, seq, regions[seq%len(regions)], seq)
		gotDoc, err := col2.Get(id)
		if err != nil {
			t.Fatalf("Get(%q) after reopen: %v", id, err)
		}
		if string(gotDoc) != wantDoc {
			t.Fatalf("Get(%q) got=%q want=%q", id, gotDoc, wantDoc)
		}
	}
	gotExtra, err := col2.Get(extraID)
	if err != nil || !bytes.Equal(gotExtra, extraDoc) {
		t.Fatalf("Get(extra) got=%q err=%v want=%q", gotExtra, err, extraDoc)
	}
	euIDs, err := col2.FindByIndex("by_region", "eu-central")
	if err != nil {
		t.Fatalf("FindByIndex(by_region): %v", err)
	}
	wantEU := total/len(regions) + 1
	if len(euIDs) != wantEU {
		t.Fatalf("FindByIndex(by_region=eu-central) len=%d want=%d", len(euIDs), wantEU)
	}
	uniqueIDs, err := col2.FindByIndex("by_email", "u000123@example.com")
	if err != nil || len(uniqueIDs) != 1 || string(uniqueIDs[0]) != "doc-000123" {
		t.Fatalf("FindByIndex(by_email) got=%q err=%v want=[doc-000123]", uniqueIDs, err)
	}
}
