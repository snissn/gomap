package memtable

import (
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	btree "github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func cowOrderedLifecycleMutation(t *testing.T, logical string, timestamp uint64, value string) COWMutation {
	t.Helper()
	physical, err := mvcckey.Encode([]byte(logical), timestamp)
	if err != nil {
		t.Fatal(err)
	}
	return COWMutation{
		Key: physical, Value: []byte(value), Flags: node.FlagInline,
		Revision: page.EntryRevision(timestamp + 17), MVCC: true, Timestamp: timestamp,
	}
}

func cowOrderedLifecycleWriter(t *testing.T) (*COWBudget, *COWWriter) {
	t.Helper()
	limits := DefaultCOWLimits()
	limits.MaxGenerationBytes = 1 << 20
	limits.MaxTotalBytes, limits.MaxRetiredBytes, limits.MaxInFlightBytes = 4<<20, 4<<20, 4<<20
	budget, err := NewCOWBudget(limits)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewCOWOrderedWriter(budget)
	if err != nil {
		t.Fatal(err)
	}
	if writer.ordered == nil {
		t.Fatal("ordered constructor activated the compatibility tree")
	}
	return budget, writer
}

func cowOrderedLifecycleFill(t *testing.T, budget *COWBudget) *COWExternalLease {
	t.Helper()
	header := COWAllocationCharge(uint64(unsafe.Sizeof(COWExternalLease{})))
	remaining := budget.Limits().MaxTotalBytes - budget.Stats().TotalBytes
	if remaining < header {
		t.Fatal("fixture has no room for the filler lease")
	}
	filler, err := budget.AcquireExternal(remaining - header)
	if err != nil {
		t.Fatal(err)
	}
	if got := budget.Stats().TotalBytes; got != budget.Limits().MaxTotalBytes {
		t.Fatalf("fixture did not saturate total admission: %d", got)
	}
	return filler
}

func cowOrderedLifecycleWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("lifecycle completion did not reach its deterministic boundary")
	}
}

func TestCOWOrderedPreparedPublishNeedsNoFurtherAdmission(t *testing.T) {
	budget, writer := cowOrderedLifecycleWriter(t)
	other, err := NewCOWOrderedWriter(budget)
	if err != nil {
		t.Fatal(err)
	}
	low := cowOrderedLifecycleMutation(t, "shared", 50, "old low")
	high := cowOrderedLifecycleMutation(t, "shared", 100, "old high")
	old := cowTestPublish(t, writer, []COWMutation{low, high})
	oldKey, found := old.SeekGE(high.Key, nil)
	if !found || oldKey.Key != string(high.Key) {
		t.Fatal("opening physical key missing")
	}
	replacement := cowOrderedLifecycleMutation(t, "shared", 50, "replacement")
	backfill := cowOrderedLifecycleMutation(t, "shared", 75, "backfill")
	erase := high
	erase.Remove = true
	prepared, err := writer.Prepare([]COWMutation{replacement, backfill, erase}, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	filler := cowOrderedLifecycleFill(t, budget)
	blocked := cowOrderedLifecycleMutation(t, "independent", 1, "retry")
	full := budget.Stats()
	if failed, err := other.Prepare([]COWMutation{blocked}, COWPrepareOptions{}); !errors.Is(err, ErrCOWCapacity) || failed != nil {
		t.Fatalf("new preparation at full admission=(%p,%v)", failed, err)
	}
	if got := budget.Stats(); got != full {
		t.Fatalf("failed preparation changed ownership: got %+v want %+v", got, full)
	}
	// The retained opening root still owns its old physical version, while all
	// postimages and allocations for this publication already exist privately.
	if record, ok := old.Get(high.Key); !ok || record.Value != "old high" {
		t.Fatal("private physical erase changed the captured opening root")
	}
	current := prepared.Publish()
	if record, ok := current.Get(replacement.Key); !ok || record.Value != "replacement" || record.Revision != replacement.Revision {
		t.Fatal("prepared replacement did not publish")
	}
	if record, ok := current.Get(backfill.Key); !ok || record.Value != "backfill" {
		t.Fatal("prepared backfill did not publish")
	}
	if _, ok := current.Get(high.Key); ok {
		t.Fatal("prepared physical erase did not publish")
	}
	if record, ok := old.Get(low.Key); !ok || record.Value != "old low" || oldKey.Key != string(high.Key) {
		t.Fatal("publication rewrote a retained history or physical key alias")
	}
	filler.Close()
	retried, err := other.Prepare([]COWMutation{blocked}, COWPrepareOptions{})
	if err != nil {
		t.Fatal("capacity was not available after unrelated owner release", err)
	}
	otherRoot := retried.Publish()
	cowTestRelease(otherRoot)
	cowTestClose(other)
	cowTestRelease(old)
	cowTestRelease(current)
	cowTestClose(writer)
	budget.Close()
	if got := budget.Stats(); got.TotalBytes != 0 || got.ReservedBytes != 0 || got.Generations != 0 {
		t.Fatalf("publication/capacity retry leaked original owners: %+v", got)
	}
}

func TestCOWOrderedCancelledResourcesRefundAfterLastCleanupOnly(t *testing.T) {
	budget, writer := cowOrderedLifecycleWriter(t)
	mutation := cowOrderedLifecycleMutation(t, "shared", 100, "retained")
	old := cowTestPublish(t, writer, []COWMutation{mutation})
	baseline := budget.Stats()
	erase := mutation
	erase.Remove = true
	prepared, err := writer.Prepare([]COWMutation{erase}, COWPrepareOptions{ResourceSlots: 1, ResourceBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-resume:
		default:
			close(resume)
		}
	})
	var calls atomic.Int32
	if err := prepared.AttachResources([]COWResourceID{{Kind: 1, ID: 887}}, func() {
		calls.Add(1)
		// Both locks must be available to the real owner callback.
		_ = budget.Stats()
		writer.mu.Lock()
		writer.mu.Unlock()
		close(entered)
		<-resume
	}); err != nil {
		t.Fatal(err)
	}
	retirement := prepared.Cancel()
	if calls.Load() != 0 || budget.Stats().DeferredBytes < 1024 {
		t.Fatal("Cancel disposed or refunded a still-owned physical resource")
	}
	if record, ok := old.Get(mutation.Key); !ok || record.Value != "retained" {
		t.Fatal("cancelled private history changed the published source")
	}
	filler := cowOrderedLifecycleFill(t, budget)
	charged := budget.Stats()
	copy := retirement
	go func() { retirement.Drain(); close(done) }()
	cowOrderedLifecycleWait(t, entered)
	// Copies share the cleanup claim, including while the first owner pauses.
	copy.Drain()
	if got := budget.Stats(); got != charged {
		t.Fatalf("resource charge disappeared while callback was paused: %+v want %+v", got, charged)
	}
	if _, err := budget.AcquireExternal(1); !errors.Is(err, ErrCOWCapacity) {
		t.Fatalf("reused cleanup-owned admission: %v", err)
	}
	close(resume)
	cowOrderedLifecycleWait(t, done)
	copy.Drain()
	retirement.Drain()
	if calls.Load() != 1 {
		t.Fatalf("cleanup callback ran %d times", calls.Load())
	}
	newOwner, err := budget.AcquireExternal(1)
	if err != nil {
		t.Fatal("completed cleanup did not return admission", err)
	}
	newOwner.Close()
	filler.Close()
	if got := budget.Stats(); got.TotalBytes != baseline.TotalBytes || got.ReservedBytes != baseline.ReservedBytes || got.DeferredBytes != 0 {
		t.Fatalf("cancelled postimage retained admission after cleanup: %+v baseline %+v", got, baseline)
	}
	cowTestRelease(old)
	cowTestClose(writer)
	budget.Close()
	if got := budget.Stats(); got.TotalBytes != 0 || got.Generations != 0 {
		t.Fatalf("last physical owner leaked: %+v", got)
	}
}

func TestCOWOrderedForkAndOwnedScanKeepPhysicalAliasesAfterBudgetClose(t *testing.T) {
	budget, writer := cowOrderedLifecycleWriter(t)
	mutations := make([]COWMutation, 80)
	for i := range mutations {
		mutations[i] = cowOrderedLifecycleMutation(t, "binary\x00logical", uint64(i+1), "old")
	}
	old := cowTestPublish(t, writer, mutations)
	if old.ordered == nil || old.Len() != len(mutations) {
		t.Fatal("published root did not retain all ordered physical versions")
	}
	borrowed, found := old.SeekGE(mutations[79].Key, nil)
	if !found || borrowed.Key != string(mutations[79].Key) {
		t.Fatal("physical seeker returned the wrong version")
	}
	if next, ok := old.SeekGE(mutations[20].Key, nil); !ok || next.Key != string(mutations[20].Key) || borrowed.Key != string(mutations[79].Key) {
		t.Fatal("another point seek overwrote a borrowed physical key")
	}
	cursor, err := old.Cursor(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, found, err := cursor.Record()
	if err != nil || !found {
		t.Fatalf("opening cursor=(%+v,%v,%v)", first, found, err)
	}
	for range 40 {
		if err := cursor.Next(); err != nil {
			t.Fatal(err)
		}
	}
	if first.Key != string(mutations[79].Key) || borrowed.Key != first.Key {
		t.Fatal("cursor movement overwrote a generation-owned physical key")
	}
	fork, err := ForkCOWWriter(old)
	if err != nil {
		t.Fatal(err)
	}
	if fork.ordered == nil {
		t.Fatal("fork lost the ordered engine")
	}
	erase := mutations[79]
	erase.Remove = true
	replacement := mutations[20]
	replacement.Value = []byte("new fork")
	current := cowTestPublish(t, fork, []COWMutation{erase, replacement})
	if current.Len() != len(mutations)-1 || current.ordered == nil {
		t.Fatal("fork physical erase produced the wrong root")
	}
	if record, ok := old.Get(erase.Key); !ok || record.Value != "old" || first.Key != record.Key {
		t.Fatal("fork changed a retained physical version or alias")
	}
	if record, ok := current.Get(replacement.Key); !ok || record.Value != "new fork" {
		t.Fatal("fork replacement missing")
	}
	cowTestClose(writer)
	cowTestClose(fork)
	cowTestRelease(current)
	budget.Close()
	if old.Retain() || !old.RetainOwned() {
		t.Fatal("closed-budget admission and existing-owner sharing were conflated")
	}
	// Release the original edge. The already-owned cursor and transferred edge
	// continue to pin the original generation without obtaining new admission.
	cowTestRelease(old)
	before := budget.Stats()
	count := 0
	old.ScanOwned(func(record COWRecord) bool {
		if record.Value != "old" {
			t.Error("owned scan observed a fork postimage")
		}
		count++
		return true
	})
	if count != len(mutations) || budget.Stats() != before || first.Key != string(mutations[79].Key) {
		t.Fatal("owned scan changed admission, physical aliases, or retained history")
	}
	cursor.Close()
	if budget.Stats().TotalBytes == 0 {
		t.Fatal("cursor close overtook the last transferred root owner")
	}
	cowTestRelease(old)
	if old.RetainOwned() {
		t.Fatal("released physical source was revived")
	}
	if got := budget.Stats(); got.TotalBytes != 0 || got.Views != 0 || got.Generations != 0 {
		t.Fatalf("last transferred source owner leaked: %+v", got)
	}
}

func TestCOWOrderedTraversalReservesAllConstructorWorkBeforeView(t *testing.T) {
	budget, writer := cowOrderedLifecycleWriter(t)
	root := cowTestPublish(t, writer, []COWMutation{cowOrderedLifecycleMutation(t, "logical", 10, "value")})
	before := budget.Stats()
	for _, limit := range []uint64{3, 4, 5} {
		work := COWTraversalWork{RecordLimit: limit, ByteLimit: 1 << 20}
		cursor, err := root.NewTraversal(&work, 0)
		if cursor != nil {
			retirement := traversalClose(t, cursor)
			retirement.Drain()
			t.Fatalf("constructor accepted insufficient work limit %d", limit)
		}
		if !errors.Is(err, btree.ErrWork) || work.Records != 2 || budget.Stats() != before {
			t.Fatalf("limit %d performed partial construction: work=%+v stats=%+v error=%v", limit, work, budget.Stats(), err)
		}
	}
	work := COWTraversalWork{RecordLimit: 6, ByteLimit: 1 << 20}
	cursor, err := root.NewTraversal(&work, 0)
	if err != nil || cursor == nil || work.Records != 6 {
		t.Fatalf("exact constructor work refused: records=%d error=%v", work.Records, err)
	}
	retirement := traversalClose(t, cursor)
	retirement.Drain()
	cowTestRelease(root)
	cowTestClose(writer)
	budget.Close()
	if got := budget.Stats(); got.TotalBytes != 0 || got.Views != 0 {
		t.Fatalf("work-boundary construction leaked: %+v", got)
	}
}

func TestCOWOrderedPrepareChargeCoversActualAllocations(t *testing.T) {
	for _, shape := range []string{"singleton", "many_logical_keys", "one_long_history", "mixed_replace_backfill_erase"} {
		t.Run(shape, func(t *testing.T) {
			cowTestIsolateAllocationWitness(t)
			budget, writer := cowOrderedLifecycleWriter(t)
			var seed, mutations []COWMutation
			switch shape {
			case "singleton":
				mutations = []COWMutation{cowOrderedLifecycleMutation(t, "singleton", 1, string(make([]byte, 4096)))}
			case "many_logical_keys":
				for i := range 32 {
					mutations = append(mutations, cowOrderedLifecycleMutation(t, string([]byte{'k', byte(i)}), uint64(i+1), "value"))
				}
			case "one_long_history", "mixed_replace_backfill_erase":
				for i := range 80 {
					mutations = append(mutations, cowOrderedLifecycleMutation(t, "shared", uint64(2*i+2), "old"))
				}
				if shape == "mixed_replace_backfill_erase" {
					seed, mutations = mutations, nil
					replace := seed[20]
					replace.Value = []byte("replacement")
					erase := seed[40]
					erase.Remove = true
					mutations = []COWMutation{
						replace, erase,
						cowOrderedLifecycleMutation(t, "shared", 41, "backfill"),
						cowOrderedLifecycleMutation(t, "new logical", 1, "new value"),
					}
				}
			}
			var old *COWRoot
			if len(seed) != 0 {
				old = cowTestPublish(t, writer, seed)
			}
			// Inputs, the writer, and any opening source are outside this witness.
			// Only private preparation allocations are promised by Charge.Total.
			runtime.GC()
			accounting := budget.Stats()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			prepared, err := writer.Prepare(mutations, COWPrepareOptions{})
			runtime.ReadMemStats(&after)
			if err != nil {
				t.Fatal(err)
			}
			charge := prepared.Charge()
			if actual := after.TotalAlloc - before.TotalAlloc; actual > charge.Total() {
				t.Errorf("private preparation allocated %d bytes above original charge %+v (total %d)", actual, charge, charge.Total())
			}
			retirement := prepared.Cancel()
			retirement.Drain()
			if got := budget.Stats(); got.TotalBytes != accounting.TotalBytes || got.ReservedBytes != accounting.ReservedBytes {
				t.Errorf("private preparation cancellation did not restore original charge: %+v want %+v", got, accounting)
			}
			if old != nil {
				cowTestRelease(old)
			}
			cowTestClose(writer)
			budget.Close()
			if got := budget.Stats(); got.TotalBytes != 0 || got.Generations != 0 {
				t.Fatalf("allocation witness retained owners: %+v", got)
			}
		})
	}
}

func TestCOWOrderedCursorLifetimeChargeCoversRepeatedTraversal(t *testing.T) {
	cowTestIsolateAllocationWitness(t)
	budget, writer := cowOrderedLifecycleWriter(t)
	var mutations []COWMutation
	for key := range 16 {
		versions := 1
		if key%2 == 0 {
			versions = 8
		}
		for version := 1; version <= versions; version++ {
			mutations = append(mutations, cowOrderedLifecycleMutation(t, string([]byte{'k', byte(key)}), uint64(version), "value"))
		}
	}
	root := cowTestPublish(t, writer, mutations)
	for _, reverse := range []bool{false, true} {
		// This includes initial stack growth, not only a warmed iterator. The
		// cursor's original lease must cover every allocation over its lifetime.
		beforeCharge := budget.Stats().TotalBytes
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		var cursor *COWCursor
		var err error
		if reverse {
			cursor, err = root.ReverseCursorWithExtraBytes(nil, nil, 0)
		} else {
			cursor, err = root.Cursor(nil, nil)
		}
		if err != nil {
			t.Fatal(err)
		}
		for range 8 {
			if err = cursor.Seek(nil); err != nil {
				t.Fatal(err)
			}
			count := 0
			for {
				_, valid, recordErr := cursor.Record()
				if recordErr != nil {
					t.Fatal(recordErr)
				}
				if !valid {
					break
				}
				count++
				if err = cursor.Next(); err != nil {
					t.Fatal(err)
				}
			}
			if count != len(mutations) {
				t.Fatalf("reverse=%t traversal count %d want %d", reverse, count, len(mutations))
			}
			for i := range mutations {
				if err = cursor.Seek(mutations[i].Key); err != nil {
					t.Fatal(err)
				}
				record, valid, recordErr := cursor.Record()
				if recordErr != nil || !valid || record.Key != string(mutations[i].Key) {
					t.Fatalf("reverse=%t exact seek %d failed: valid=%t error=%v", reverse, i, valid, recordErr)
				}
			}
		}
		runtime.ReadMemStats(&after)
		charge := budget.Stats().TotalBytes - beforeCharge
		if actual := after.TotalAlloc - before.TotalAlloc; actual > charge {
			t.Errorf("reverse=%t cursor allocated %d bytes above original lifetime charge %d", reverse, actual, charge)
		}
		cursor.Close()
		if got := budget.Stats().TotalBytes; got != beforeCharge {
			t.Fatalf("cursor close left %d bytes above original source charge %d", got, beforeCharge)
		}
	}
	cowTestRelease(root)
	cowTestClose(writer)
	budget.Close()
	if got := budget.Stats(); got.TotalBytes != 0 || got.Views != 0 || got.Generations != 0 {
		t.Fatalf("cursor allocation witness retained owners: %+v", got)
	}
}
