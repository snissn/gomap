package db_test

import (
	"bytes"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/compaction"
)

func TestCompaction_OmitKeys_RequiresIndexSwap(t *testing.T) {
	// This test verifies that we've added a safety check to prevent data loss
	// when TREEDB_SLAB_OMIT_KEYS=1 is used with the default compactor.
	dir := t.TempDir()
	opts := treedb.Options{Dir: dir}
	opts.OmitSlabKeys = true
	opts.SlabCompression.Kind = 0 // CompressionNone

	d, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	val := []byte("secret-value")
	key := []byte("z-key")
	if err := d.SetSync(key, val); err != nil {
		t.Fatal(err)
	}

	// Rotate slab
	if _, err := d.Backend().SlabManager().Rotate(); err != nil {
		t.Fatal(err)
	}

	// 1. Verify that compaction FAILS without IndexSwap
	cOpts := compaction.Options{
		IndexSwap: false,
		MaxSlabs:  1,
	}
	err = d.CompactCandidates(cOpts)
	if err == nil || err.Error() != "compaction: IndexSwap required when OmitSlabKeys is enabled" {
		t.Fatalf("Expected IndexSwap safety error, got: %v", err)
	}

	// 2. Verify that compaction SUCCEEDS with IndexSwap
	cOpts.IndexSwap = true
	if err := d.CompactCandidates(cOpts); err != nil {
		t.Fatalf("Expected IndexSwap to work, got error: %v", err)
	}

	// Verify data is still there
	got, err := d.Get(key)
	if err != nil {
		t.Fatalf("Get failed after IndexSwap compaction: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("Data mismatch after IndexSwap compaction")
	}
}
