package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestLeafPrefixCompression_Efficiency(t *testing.T) {
	// Write many keys with shared prefix. Compare size with and without compression.

	writeDB := func(compress bool) int64 {
		dir := t.TempDir()
		opts := DefaultOptions(dir)
		opts.ChunkSize = 16384 // Granular allocation
		opts.LeafPrefixCompression = compress
		opts.DisableWAL = true // Speed up
		opts.AllowUnsafe = true

		db, err := Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		batch := db.NewBatch().(*Batch)
		// Write 50,000 keys: "prefix/key/0000" ...
		prefix := []byte("common/prefix/for/testing/")
		for i := 0; i < 50000; i++ {
			key := append(prefix, []byte(fmt.Sprintf("%04d", i))...)
			batch.Set(key, []byte("v"))
		}
		if err := batch.Write(); err != nil {
			t.Fatal(err)
		}

		// Get size of index.db
		info, _ := os.Stat(filepath.Join(dir, "index.db"))
		return info.Size()
	}

	sizeOff := writeDB(false)
	sizeOn := writeDB(true)

	t.Logf("Size Off: %d", sizeOff)
	t.Logf("Size On:  %d", sizeOn)

	if sizeOn >= sizeOff {
		t.Errorf("Prefix compression did not reduce size! On: %d, Off: %d", sizeOn, sizeOff)
	} else {
		t.Logf("Reduction: %.2f%%", 100*float64(sizeOff-sizeOn)/float64(sizeOff))
	}
}
