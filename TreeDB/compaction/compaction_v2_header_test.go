package compaction

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/slab"
)

func TestCompaction_V2HeaderOnly(t *testing.T) {
	dir := t.TempDir()

	opts := db.Options{
		Dir:                               dir,
		SlabCompressionAdaptiveTrainBytes: -1,
		SlabCompression: slab.CompressionOptions{
			Kind:            slab.CompressionZSTD,
			MinBytes:        1,
			MinSavingsBytes: 0,
		},
		AllowUnsafe: true,
	}
	d, err := db.Open(opts)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	defer d.Close()

	slabID := d.SlabManager().ActiveSlabID()
	if _, err := d.SlabManager().Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	c := New(d)
	if err := c.CompactSlab(slabID); err != nil {
		t.Fatalf("CompactSlab: %v", err)
	}
}
