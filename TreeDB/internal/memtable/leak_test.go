package memtable

import (
	"testing"

	"go.uber.org/goleak"
)

// sharedLoop appears in the stacks of the documented process-global fallback indexer
// (see sharedHashSortedIndexer). It intentionally runs for the process
// lifetime once started; owned indexers use loop and are still reported.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreAnyFunction("github.com/snissn/gomap/TreeDB/internal/memtable.(*HashSortedIndexer).sharedLoop"),
	)
}
