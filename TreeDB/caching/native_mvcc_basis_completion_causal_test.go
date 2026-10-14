package caching
import("reflect";"testing";backenddb "github.com/snissn/gomap/TreeDB/db";"github.com/snissn/gomap/TreeDB/internal/memtable")
func TestNativeMVCCCOWBasisDataCompletionCausalState(t *testing.T) {
	for _, primary := range []bool{false} {
		name := "DATA"
		if primary {
			name = "PRIMARY"
		}
		t.Run(name, func(t *testing.T) {
			backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), IndexPrimaryDirectory: primary})
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			if err = backend.SetSync([]byte("disk"), []byte("basis")); err != nil {
				t.Fatal(err)
			}
			c, err := newCOWCache(backend, 2, memtable.DefaultCOWLimits())
			if err != nil {
				t.Fatal(err)
			}
			cache := &DB{backend: backend, cow: c, mutableShards: make([]memShard, 2), mutableShardMask: 1}
			outer := cache.AcquireSnapshot()
			if outer == nil {
				t.Fatal("outer capture")
			}
			original := outer.cowCut.basis
			generation := reflect.ValueOf(original.snapshot).Elem().FieldByName("idx").Elem()
 metadata := original.metadata
			owner := reflect.ValueOf(metadata).Elem().FieldByName("owner").Elem()
			reader, err := original.snapshot.Iterator(nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.closeWithBudget(false); err != nil {
				t.Fatal(err)
			}
			if err = backend.Close(); err != nil {
				t.Fatal(err)
			}
			c.budget.Close()
			if err = outer.Close(); err != nil {
				t.Fatal(err)
			}
			original.completionMu.Lock()
			pending := !original.completed && original.snapshot != nil
			original.completionMu.Unlock()
			before := c.budget.Stats()
			if !pending || before.ExternalBytes == 0 || before.ExternalLeases == 0 {
				t.Fatalf("Close request refunded original pending payload: %+v", before)
			}
			if err = outer.Close(); err != nil {
				t.Fatal(err)
			}
			if got := c.budget.Stats(); got != before {
				t.Fatal("repeat outer Close changed pending capacity")
			}
			if err = reader.Close(); err != nil {
				t.Fatal(err)
			}
			original.completionMu.Lock()
			completed := original.completed && original.cleanupErr == nil && original.snapshot == nil && original.pins == nil
			original.completionMu.Unlock()
			if got := c.budget.Stats(); !completed || got.ExternalBytes != 0 || got.ExternalLeases != 0 {
				t.Fatalf("actual completion retained original payload or charge: completed=%v err=%v snapshot=%v pins=%d lease=%+v owner(bytes=%d pending=%d controls=%d closed=%v failed=%v) head=%v gen=%v stats=%+v", completed, original.cleanupErr, original.snapshot != nil, len(original.pins), original.lease, owner.FieldByName("bytes").Uint(), owner.FieldByName("pending").Uint(), owner.FieldByName("controls").Uint(), owner.FieldByName("closed").Bool(), owner.FieldByName("cleanupFailed").Bool(), owner.FieldByName("head"), generation, got)
			}
		})
	}
}
