package caching

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

// Diagnostic: show how much of the "underfilled live pages" comes from seeding
// the initial dataset via many small batches vs one big batch.
func TestCachedSeed_BatchSizeAffectsFillAndPageCount(t *testing.T) {
	const keys = 20000
	val := bytes.Repeat([]byte("a"), 128)

	type result struct {
		label string
		rep   map[string]string
	}
	run := func(label string, seed func(*DB)) result {
		t.Helper()
		dir := t.TempDir()
		backend, err := db.Open(db.Options{
			Dir:               dir,
			PreferAppendAlloc: false,
			KeepRecent:        1,
		})
		if err != nil {
			t.Fatalf("%s: backend open: %v", label, err)
		}
		cached, err := Open(dir, backend, Options{FlushThreshold: 1 << 20})
		if err != nil {
			_ = backend.Close()
			t.Fatalf("%s: cached open: %v", label, err)
		}

		seed(cached)
		if err := cached.Checkpoint(); err != nil {
			t.Fatalf("%s: checkpoint: %v", label, err)
		}
		rep, err := backend.FragmentationReport()
		if err != nil {
			t.Fatalf("%s: FragmentationReport: %v", label, err)
		}
		_ = cached.Close()
		_ = backend.Close()
		return result{label: label, rep: rep}
	}

	manySmall := run("many_small_batches", func(c *DB) {
		seedBatches(t, c, keys, val)
	})
	oneBig := run("one_big_batch", func(c *DB) {
		b := c.NewBatch()
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, val); err != nil {
				t.Fatalf("set: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = b.Close()
	})

	for _, r := range []result{manySmall, oneBig} {
		t.Logf("%s: pages.total=%s user.pages=%s leaf.pages=%s internal.pages=%s leaf.avg=%s leaf.p50=%s leaf.min=%s span_ratio_ppm=%s",
			r.label,
			r.rep["treedb.pages.total"],
			r.rep["treedb.user.pages"],
			r.rep["treedb.user.pages.leaf"],
			r.rep["treedb.user.pages.internal"],
			r.rep["treedb.user.leaf_fill_ppm_avg"],
			r.rep["treedb.user.leaf_fill_ppm_p50"],
			r.rep["treedb.user.leaf_fill_ppm_min"],
			r.rep["treedb.user.pages.span_ratio_ppm"],
		)
	}
}
