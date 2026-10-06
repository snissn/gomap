package caching

import (
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Observe the real blocking drain at its wait, rather than release the private
// prefix after an arbitrary delay that might precede the assist's selection.
func ownedPointWaitForBlockingAssist(t *testing.T) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	stacks := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		n := runtime.Stack(stacks, true)
		for _, stack := range strings.Split(string(stacks[:n]), "\n\n") {
			if !strings.Contains(stack, ".flushSomeBlocking(") {
				continue
			}
			header := strings.SplitN(stack, "\n", 2)[0]
			if strings.Contains(header, "[sync.Mutex.Lock]") || strings.Contains(header, "[select]") || strings.Contains(header, "[chan receive]") {
				return stack
			}
		}
		runtime.Gosched()
	}
	t.Fatal("blocking assist did not reach its wait")
	return ""
}

func TestOwnedPointPrefixBlockingStopDrainWaitsBeforeSelectingSources(t *testing.T) {
	db, backend := ownedPointFixture(t, 1)
	enqueueOwnedPointLane(t, db, 0, "a", "first", "c", "third")
	enqueueOwnedPointLane(t, db, 1, "b", "second")
	db.mu.Lock()
	frontier := db.captureCheckpointFrontierLocked()
	if db.hasDirtyRootPublishGroupLocked() {
		db.mu.Unlock()
		t.Fatal("fixture would take dirty root publication instead of the lane drain")
	}
	db.publishMemtablesLocked()
	db.mu.Unlock()
	db.checkpointing.Store(true)
	db.setActiveCheckpointFrontier(frontier)
	defer func() { db.clearActiveCheckpointFrontier(); db.checkpointing.Store(false) }()
	old := db.AcquireSnapshot()
	defer old.Close()
	seq := backend.State().CommitSeq
	entered, resume := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	defer release()
	var paused atomic.Bool
	backend.afterWrite = func(final bool) {
		if !final && paused.CompareAndSwap(false, true) {
			close(entered)
			<-resume
		}
	}
	prefixDone := make(chan struct{})
	go func() {
		// The production shared checkpoint runs this drain without flushMu and
		// reacquires it only after the helper has released its lane claims.
		db.flushCheckpointFrontierLocked(true, nil, frontier)
		db.flushMu.Lock()
		db.flushMu.Unlock()
		close(prefixDone)
	}()
	ownedPointWait(t, entered)
	enqueueOwnedPointLane(t, db, 0, "a", "newer")
	db.mu.Lock()
	newerID := db.queueIDs[len(db.queueIDs)-1]
	if err := db.enqueueRangeSpanLayerLocked([]batch.DeleteRange{{Start: []byte("a"), End: []byte("d")}}); err != nil {
		db.mu.Unlock()
		t.Fatal(err)
	}
	rangeID := db.queueIDs[len(db.queueIDs)-1]
	db.publishMemtablesLocked()
	db.mu.Unlock()
	enqueueOwnedPointLane(t, db, 1, "late", "later")
	db.mu.RLock()
	lateID := db.queueIDs[len(db.queueIDs)-1]
	db.mu.RUnlock()
	assistDone := make(chan struct{})
	var flushed int
	go func() { flushed = db.flushSomeBlocking(false, 1, 0); close(assistDone) }()
	stack := ownedPointWaitForBlockingAssist(t)
	selectedBeforeRetirement := db.flushCoordinatorActiveWorkers.Load()
	release()
	ownedPointWait(t, assistDone)
	ownedPointWait(t, prefixDone)
	if selectedBeforeRetirement != 1 || strings.Contains(stack, ".flushLaneOnceWithCollectionMode") {
		t.Fatalf("blocking stop drain selected a claimed source before prefix retirement: active workers=%d\n%s", selectedBeforeRetirement, stack)
	}
	if flushed != 0 {
		t.Fatalf("blocking assist replayed %d retired frontier units", flushed)
	}
	if got := backend.writes.Load(); got != 3 {
		t.Fatalf("physical writes=%d want 3 private-prefix chunks without replay", got)
	}
	if got := backend.State().CommitSeq - seq; got != 1 {
		t.Fatalf("published roots=%d want 1", got)
	}
	db.mu.RLock()
	remaining := append([]uint64(nil), db.queueIDs...)
	db.mu.RUnlock()
	if len(remaining) != 3 || remaining[0] != newerID || remaining[1] != rangeID || remaining[2] != lateID {
		t.Fatalf("post-frontier queue IDs changed: %v", remaining)
	}
	if got, err := backend.Get(ownedPointKey(t, db, 0, "a")); err != nil || string(got) != "first" {
		t.Fatalf("private frontier result=(%q,%v)", got, err)
	}
	if got, err := old.Get(ownedPointKey(t, db, 0, "a")); err != nil || string(got) != "first" {
		t.Fatalf("retained old source snapshot=(%q,%v)", got, err)
	}
	if got, err := old.Get(ownedPointKey(t, db, 1, "late")); (err != nil && !errors.Is(err, tree.ErrKeyNotFound)) || got != nil {
		t.Fatalf("old snapshot exposed newer point=(%q,%v)", got, err)
	}
	// After shared checkpoint ownership ends, stop-backpressure can make actual
	// progress on the newer queue, preserving the point/range/point ordering.
	db.clearActiveCheckpointFrontier()
	db.checkpointing.Store(false)
	if got := db.flushSomeBlocking(true, 8, 0); got != 3 {
		t.Fatalf("post-frontier blocking progress=%d want 3", got)
	}
	if got, err := backend.Get(ownedPointKey(t, db, 0, "a")); err != nil || got != nil {
		t.Fatalf("newer same-key point/range order=(%q,%v)", got, err)
	}
	if got, err := backend.Get(ownedPointKey(t, db, 1, "late")); err != nil || string(got) != "later" {
		t.Fatalf("post-range point=(%q,%v)", got, err)
	}
}
