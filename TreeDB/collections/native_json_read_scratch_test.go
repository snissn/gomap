package collections

import (
	"bytes"
	"errors"
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
)

type nativeReadRefusingAllocator struct {
	backenddb.NativeSemanticCapture
	allocator backenddb.NativeSemanticAllocator
}

func (c nativeReadRefusingAllocator) NativeAllocator() backenddb.NativeSemanticAllocator {
	return c.allocator
}

// Representation-only optimization: public behavior was already correct. This
// control exercises original budget refusal, scope reuse and borrowed txn probes
// rather than inventing a semantic failing oracle for the former eager layout.
func TestNativeJSONReadLazyScratchOwnership(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewNativeJSONManager(backend)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = manager.CreateCollection(CollectionMeta{Name: "rows"}); err != nil {
		t.Fatal(err)
	}
	value := []byte(`{"value":"owned output with spare-capacity length"}`)
	if err = manager.InsertBatch("rows", [][]byte{[]byte("a")}, [][]byte{value}); err != nil {
		t.Fatal(err)
	}
	held, err := manager.Capture("rows")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := held.Close(); err != nil {
			t.Error(err)
		}
	})
	check := func(r *NativeJSONRead) {
		t.Helper()
		got, err := r.Get([]byte("a"))
		if err != nil || !bytes.Equal(got, value) || cap(got) != len(got) {
			t.Fatalf("owned value %q cap=%d: %v", got, cap(got), err)
		}
		got[0] = '!'
		got, err = r.Get([]byte("a"))
		if err != nil || !bytes.Equal(got, value) {
			t.Fatalf("caller changed captured value: %q %v", got, err)
		}
	}
	check(held)
	if held.scratch != nil || held.scratchLease != nil {
		t.Fatal("logical hit allocated physical scratch")
	}
	if _, err = held.Get([]byte("absent")); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatal(err)
	}
	if held.scratch != nil {
		t.Fatal("no-base absence allocated scratch")
	}
	if err = database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	check(held)
	if held.scratch != nil {
		t.Fatal("held logical source changed after checkpoint")
	}

	base, err := manager.Capture("rows")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := base.Close(); err != nil {
			t.Error(err)
		}
	})
	if base.scratch != nil {
		t.Fatal("live-state Capture eagerly allocated scratch")
	}
	check(base)
	if base.scratch != nil || base.scratchLease != nil {
		t.Fatal("shared basis physical read allocated duplicate scope scratch")
	}
	refused, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	refused.Close()
	original := base.capture
	base.capture = nativeReadRefusingAllocator{original, refused}
	if _, err = base.Get([]byte("a")); !errors.Is(err, memtable.ErrCOWClosed) {
		t.Fatalf("scratch refusal: %v", err)
	}
	if base.scratch != nil || base.scratchLease != nil || refused.Stats().TotalBytes != 0 {
		t.Fatal("refusal left partial scratch ownership")
	}
	base.capture = nativeReadRefusingAllocator{original, original.NativeAllocator()}
	check(base)
	if base.scratch == nil || base.scratchLease == nil {
		t.Fatal("physical read lacks original scratch admission")
	}
	scratch := base.scratch
	receipt := base.scratchLease
	budget := original.NativeAllocator().(*memtable.COWBudget)
	before := budget.Stats()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 16; j++ {
				got, err := base.Get([]byte("a"))
				if err != nil || !bytes.Equal(got, value) {
					t.Errorf("concurrent held read: %q %v", got, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	after := budget.Stats()
	// Existing asynchronous retirement may reduce the global totals. Reuse
	// belongs to this scope's exact workspace and independent original receipt.
	if base.scratch != scratch || base.scratchLease != receipt {
		t.Fatal("held read failed to reuse admitted scratch", before, after)
	}
	scratch = nil
	// Closing and a final point read share the original scope mutex. A read
	// either finishes with owned bytes or observes Close, never stale scratch.
	start := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		got, err := base.Get([]byte("a"))
		if err != nil && !errors.Is(err, backenddb.ErrClosed) || err == nil && !bytes.Equal(got, value) {
			t.Errorf("read/Close overlap: %q %v", got, err)
		}
	}()
	close(start)
	if err = base.Close(); err != nil {
		t.Error(err)
	}
	wg.Wait()
	if base.scratch != nil || base.scratchLease != nil || !errors.Is(receipt.Resize(1), memtable.ErrCOWClosed) {
		t.Fatal("closed read retained scratch receipt")
	}
	receipt = nil

	for _, commit := range []bool{false, true} {
		txn, err := manager.Begin("rows")
		if err != nil {
			t.Fatal(err)
		}
		txn.bindings[0].probe.capture = nativeReadRefusingAllocator{txn.bindings[0].probe.capture, txn.core.NativeAllocator()}
		if _, err = txn.Get([]byte("a")); err != nil {
			t.Fatal(err)
		}
		receipt = txn.bindings[0].probe.scratchLease
		if receipt == nil {
			t.Fatal("borrowed txn read did not admit physical scratch")
		}
		if commit {
			if err = txn.InsertBatch([][]byte{[]byte("b")}, [][]byte{value}); err != nil {
				t.Fatal(err)
			}
			if err = txn.Commit(); err != nil {
				t.Fatal(err)
			}
		}
		if err = txn.Close(); err != nil {
			t.Fatal(err)
		}
		if !errors.Is(receipt.Resize(1), memtable.ErrCOWClosed) {
			t.Fatal("borrowed txn retained scratch on commit/cancel")
		}
		receipt = nil
	}
	if err = held.Close(); err != nil {
		t.Fatal(err)
	}
	original = nil
	closed = true
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := budget.Stats(); stats.TotalBytes != 0 || stats.Sources != 0 {
		t.Fatal("original owners survived final Close", stats)
	}
}
