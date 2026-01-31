package caching

import (
	"bytes"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

// Diagnostic: sweep commit batch sizes and log how they affect live-tree
// page count and fill. This helps identify the "knee" where behavior becomes
// vacuum-like, and whether bloat is strongly correlated with commit size.
//
// This test is expected to stay green; it only logs.
//
// Env:
// - TREEDB_TEST_KEYS: override key count (default 20000)
// - TREEDB_TEST_BATCH_SIZES: CSV of batch sizes (default "256,512,1024,4096,20000")
func TestCachedSeed_BatchSizeSweepDiagnostics(t *testing.T) {
	keys := 20000
	if v := os.Getenv("TREEDB_TEST_KEYS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			keys = n
		}
	}

	batchSizes := []int{256, 512, 1024, 4096, 20000}
	if v := os.Getenv("TREEDB_TEST_BATCH_SIZES"); v != "" {
		var parsed []int
		for _, part := range strings.Split(v, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			n, err := strconv.Atoi(part)
			if err != nil || n <= 0 {
				t.Fatalf("invalid TREEDB_TEST_BATCH_SIZES entry %q", part)
			}
			parsed = append(parsed, n)
		}
		if len(parsed) > 0 {
			batchSizes = parsed
		}
	}

	val := bytes.Repeat([]byte("a"), 128)

	type row struct {
		batchSize int
		rep       map[string]string
	}
	var rows []row

	run := func(batchSize int) map[string]string {
		dir := t.TempDir()
		backend, err := db.Open(db.Options{
			Dir:               dir,
			PreferAppendAlloc: false,
			KeepRecent:        1,
		})
		if err != nil {
			t.Fatalf("backend open (batch=%d): %v", batchSize, err)
		}

		cached, err := Open(dir, backend, Options{FlushThreshold: 1 << 20})
		if err != nil {
			_ = backend.Close()
			t.Fatalf("cached open (batch=%d): %v", batchSize, err)
		}

		for base := 0; base < keys; base += batchSize {
			b := cached.NewBatch()
			limit := base + batchSize
			if limit > keys {
				limit = keys
			}
			for i := base; i < limit; i++ {
				k := []byte{byte(i >> 8), byte(i)}
				if err := b.Set(k, val); err != nil {
					t.Fatalf("set (batch=%d): %v", batchSize, err)
				}
			}
			if err := b.WriteSync(); err != nil {
				t.Fatalf("write (batch=%d): %v", batchSize, err)
			}
			_ = b.Close()
		}

		if err := cached.Checkpoint(); err != nil {
			t.Fatalf("checkpoint (batch=%d): %v", batchSize, err)
		}

		rep, err := backend.FragmentationReport()
		if err != nil {
			t.Fatalf("FragmentationReport (batch=%d): %v", batchSize, err)
		}

		_ = cached.Close()
		_ = backend.Close()
		return rep
	}

	for _, bs := range batchSizes {
		rep := run(bs)
		rows = append(rows, row{batchSize: bs, rep: rep})
	}

	sort.Slice(rows, func(i, j int) bool { return rows[i].batchSize < rows[j].batchSize })

	for _, r := range rows {
		rep := r.rep
		t.Logf("batch=%-6d user.pages=%-6s leaf.avg=%-6s leaf.p50=%-6s leaf.min=%-6s pages.total=%-6s span_ratio_ppm=%s",
			r.batchSize,
			rep["treedb.user.pages"],
			rep["treedb.user.leaf_fill_ppm_avg"],
			rep["treedb.user.leaf_fill_ppm_p50"],
			rep["treedb.user.leaf_fill_ppm_min"],
			rep["treedb.pages.total"],
			rep["treedb.user.pages.span_ratio_ppm"],
		)
	}
}
