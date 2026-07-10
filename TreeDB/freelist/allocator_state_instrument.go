//go:build treedb_freelist_instrument

package freelist

import (
	"sync"

	"github.com/snissn/gomap/TreeDB/pager"
)

const allocatorInstrumentationEnabled = true

type allocatorPageOperationCounts struct {
	gets     int
	verifies int
	updates  int
}

type allocatorInstrumentation struct {
	mu               sync.Mutex
	operations       map[uint64]allocatorPageOperationCounts
	failAfterGets    int
	failGetForWrite  error
	injectedFailures int
}

type Allocator struct {
	pager *pager.Pager
	head  uint64
	mu    sync.Mutex

	lastAlloc    uint64
	regionPages  uint64
	regionRadius int

	stats Stats

	// preferAppend makes Alloc ignore the freelist and allocate new pages by
	// extending the file. This improves locality at the cost of reclaiming space
	// later via vacuum.
	preferAppend bool

	instrumentation allocatorInstrumentation
}

func (a *Allocator) observePageOperation(operation allocatorPageOperation, pageID uint64) {
	a.instrumentation.mu.Lock()
	defer a.instrumentation.mu.Unlock()
	if a.instrumentation.operations == nil {
		a.instrumentation.operations = make(map[uint64]allocatorPageOperationCounts)
	}
	counts := a.instrumentation.operations[pageID]
	switch operation {
	case allocatorPageGetForWrite:
		counts.gets++
	case allocatorPageVerifyChecksum:
		counts.verifies++
	case allocatorPageUpdateChecksum:
		counts.updates++
	}
	a.instrumentation.operations[pageID] = counts
}

func (a *Allocator) injectedGetForWriteError(_ uint64) error {
	a.instrumentation.mu.Lock()
	defer a.instrumentation.mu.Unlock()
	if a.instrumentation.failGetForWrite == nil {
		return nil
	}
	if a.instrumentation.failAfterGets > 0 {
		a.instrumentation.failAfterGets--
		return nil
	}
	err := a.instrumentation.failGetForWrite
	a.instrumentation.failGetForWrite = nil
	a.instrumentation.injectedFailures++
	return err
}

// TestInjectGetForWriteFailureAfter injects a one-shot batch GetForWrite
// failure after successfulAttempts subsequent attempts. It exists only in
// treedb_freelist_instrument builds.
func (a *Allocator) TestInjectGetForWriteFailureAfter(successfulAttempts int, err error) {
	a.instrumentation.mu.Lock()
	defer a.instrumentation.mu.Unlock()
	a.instrumentation.failAfterGets = successfulAttempts
	a.instrumentation.failGetForWrite = err
}

// TestInjectedGetForWriteFailures returns the number of one-shot failures
// consumed by this allocator. It exists only in instrumented builds.
func (a *Allocator) TestInjectedGetForWriteFailures() int {
	a.instrumentation.mu.Lock()
	defer a.instrumentation.mu.Unlock()
	return a.instrumentation.injectedFailures
}

func (a *Allocator) resetPageOperationCounts() {
	a.instrumentation.mu.Lock()
	defer a.instrumentation.mu.Unlock()
	a.instrumentation.operations = nil
}

func (a *Allocator) pageOperationCounts(pageID uint64) allocatorPageOperationCounts {
	a.instrumentation.mu.Lock()
	defer a.instrumentation.mu.Unlock()
	return a.instrumentation.operations[pageID]
}

func (a *Allocator) allPageOperationCounts() map[uint64]allocatorPageOperationCounts {
	a.instrumentation.mu.Lock()
	defer a.instrumentation.mu.Unlock()
	out := make(map[uint64]allocatorPageOperationCounts, len(a.instrumentation.operations))
	for pageID, counts := range a.instrumentation.operations {
		out[pageID] = counts
	}
	return out
}
