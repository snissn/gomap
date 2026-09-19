package main

import (
	"testing"

	"go.uber.org/goleak"
)

// sharedLoop appears in the stacks of the memtable package's documented
// process-global fallback indexer; see TreeDB/internal/memtable/leak_test.go.
// registerSignalCloser installs a process-lifetime OS-signal waiter that runs
// the registered DB closers and exits the process; it intentionally has no
// shutdown path.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreAnyFunction("github.com/snissn/gomap/TreeDB/internal/memtable.(*HashSortedIndexer).sharedLoop"),
		goleak.IgnoreAnyFunction("github.com/snissn/gomap/TreeDB/cmd/treemap.registerSignalCloser.func1"),
		goleak.IgnoreAnyFunction("github.com/snissn/gomap/TreeDB/cmd/treemap.registerSignalCloser.func1.1"),
	)
}
