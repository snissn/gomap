package raftentry

import (
	"testing"

	"go.uber.org/goleak"
)

// sharedLoop appears in the stacks of the memtable package's documented
// process-global fallback indexer; see TreeDB/internal/memtable/leak_test.go.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreAnyFunction("github.com/snissn/gomap/TreeDB/internal/memtable.(*HashSortedIndexer).sharedLoop"),
	)
}
