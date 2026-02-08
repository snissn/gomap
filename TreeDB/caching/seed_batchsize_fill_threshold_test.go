package caching

import (
	"bytes"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

// This is a regression test that encodes the expectation that seeding a dataset
// should yield reasonably full leaves across *all* commit batch sizes, not only
// a single huge commit. The current behavior (small batches produce ~0.43 avg
// leaf fill) is treated as a bug to fix.
//
// Env:
// - TREEDB_TEST_KEYS: override key count (default 20000)
// - TREEDB_TEST_BATCH_SIZES: CSV of batch sizes (default "256,512,1024,4096,20000")
func TestCachedSeed_BatchSizeFillShouldBeHigh(t *testing.T) {
	requireTreeDBStress(t)

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

	type bad struct {
		batchSize int
		leafAvg   string
		userPages string
	}
	var bads []bad

	for _, batchSize := range batchSizes {
		dir := t.TempDir()
		backend, err := db.Open(db.Options{
			Dir:               dir,
			PreferAppendAlloc: false,
			KeepRecent:        1,
		})
		if err != nil {
			t.Fatalf("backend open (batch=%d): %v", batchSize, err)
		}

		cached, err := Open(dir, backend, Options{
			FlushThreshold: 1 << 20,
			JournalLanes:   1,
		})
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

		// Close early to avoid tempdir buildup.
		_ = cached.Close()
		_ = backend.Close()

		leafAvgStr := rep["treedb.user.leaf_fill_ppm_avg"]
		userPagesStr := rep["treedb.user.pages"]
		if leafAvgStr == "" || userPagesStr == "" {
			t.Fatalf("missing fill stats (batch=%d): leaf_avg=%q user_pages=%q", batchSize, leafAvgStr, userPagesStr)
		}

		leafAvg, err := strconv.ParseUint(leafAvgStr, 10, 64)
		if err != nil {
			t.Fatalf("parse leaf fill avg (batch=%d): %v", batchSize, err)
		}

		// Target: >= 800k ppm (~80% average fill). This is intentionally strict
		// to force us to fix small-batch structural bloat.
		if leafAvg < 800_000 {
			bads = append(bads, bad{
				batchSize: batchSize,
				leafAvg:   leafAvgStr,
				userPages: userPagesStr,
			})
		}
	}

	if len(bads) > 0 {
		for _, b := range bads {
			t.Logf("bad batch: size=%d leaf_fill_ppm_avg=%s user_pages=%s", b.batchSize, b.leafAvg, b.userPages)
		}
		t.Fatalf("leaf fill avg too low for %d batch sizes (target >= 800000 ppm)", len(bads))
	}
}
