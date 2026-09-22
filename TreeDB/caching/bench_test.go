package caching

import (
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func BenchmarkRepeatedIterator(b *testing.B) {
	dir, err := os.MkdirTemp("", "treedb-bench-iter-")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer backend.Close()

	// Default flush threshold
	cached, err := Open(dir, backend, Options{FlushThreshold: 64 * 1024 * 1024})
	if err != nil {
		b.Fatal(err)
	}
	defer cached.Close()

	// Preload some data to ensure rotation logic is exercised at least once
	// (if memtable is empty, rotation is skipped entirely, which is also a valid optimization).
	// But we want to ensure that even if we DO rotate (or check), it's fast.
	// Actually, the fix was "skip rotation if empty".
	// So we should test that case.
	// But we also want to test that if we HAVE data, we don't thrash IO if we don't need to.
	// But `Iterator` forces rotation if data exists.
	// So `Iterator` IS slow if data exists (flush).
	// But `prefix_scan` is fast because subsequent iterators see EMPTY memtable.

	// So this benchmark tests the "Empty Memtable" path.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it, err := cached.Iterator(nil, nil)
		if err != nil {
			b.Fatal(err)
		}
		it.Close()
	}
}
