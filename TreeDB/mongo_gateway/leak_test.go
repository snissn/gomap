package mongogateway

import (
	"testing"

	"go.uber.org/goleak"
)

// sharedLoop appears in the stacks of the memtable package's documented
// process-global fallback indexer; see TreeDB/internal/memtable/leak_test.go.
// The insert/update coalescer workers are per-collection and retire on their
// own after the configured idle TTL; Server.Close stops them immediately
// (covered by TestServerCloseStopsInsertCoalescers and
// TestServerCloseStopsUpdateCoalescers). Tests that never close their server
// would otherwise report these bounded-lifetime workers as leaks.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreAnyFunction("github.com/snissn/gomap/TreeDB/internal/memtable.(*HashSortedIndexer).sharedLoop"),
		goleak.IgnoreAnyFunction("github.com/snissn/gomap/TreeDB/mongo_gateway.(*mongoInsertCoalescer).run"),
		goleak.IgnoreAnyFunction("github.com/snissn/gomap/TreeDB/mongo_gateway.(*mongoUpdateCoalescer).run"),
	)
}
