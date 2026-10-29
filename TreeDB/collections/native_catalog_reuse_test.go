package collections

import (
	"bytes"
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

// A fresh opener has no published semantic state. Reads must reuse only an
// immutable projection of THAT original cut, not install a semantic publication.
func TestNativeColdCatalogProjectionReuse(t *testing.T) {
	dir := t.TempDir()
	owner, manager, col := openNativeTextPublic(t, dir, treedb.ProfileNoWALFast, true)
	oldDoc := []byte(`{"tenant":"old","content":"original policy"}`)
	newDoc := []byte(`{"tenant":"new","content":"updated policy"}`)
	if _, err := col.Insert([]byte("a"), oldDoc); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	owner, manager, col = openNativeTextPublic(t, dir, treedb.ProfileNoWALFast, false)
	var budget nativeHNSWAdmissionBudget
	// Registered after opener cleanup deliberately executes first only to close
	// our held reads; the budget-zero check is registered before the final reopen.
	var reads []*NativeJSONRead
	defer func() {
		for _, r := range reads {
			if r != nil {
				if err := r.Close(); err != nil {
					t.Error(err)
				}
			}
		}
	}()
	first, err := manager.nativeJSON.Capture("docs")
	if err != nil {
		t.Fatal(err)
	}
	reads = append(reads, first)
	if first.capture.State() != nil {
		t.Fatal("cold read published semantic authority")
	}
	budget = first.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
	backend := col.nativeJSON.db
	sibling, err := NewNativeJSONManager(backend)
	if err != nil {
		t.Fatal(err)
	}
	const n = 8
	results := make(chan *NativeJSONRead, n)
	failures := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := sibling.Capture("docs")
			if err != nil {
				failures <- err
				return
			}
			results <- r
		}()
	}
	wg.Wait()
	close(results)
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	for r := range results {
		reads = append(reads, r)
	}
	if t.Failed() {
		return
	}
	shared := true
	for _, r := range reads {
		got, err := r.Get([]byte("a"))
		if err != nil || !bytes.Equal(got, oldDoc) {
			t.Fatalf("cold read: %q %v", got, err)
		}
		if r.capture.State() != nil || r.capture.LogicalSequence() != first.capture.LogicalSequence() {
			t.Fatal("projection changed captured authority")
		}
		shared = shared && r.collection == first.collection
	}
	// Closing one reader cannot destroy another reader's descriptor/backing.
	if err := reads[1].Close(); err != nil {
		t.Fatal(err)
	}
	reads[1] = nil
	if _, err := col.UpdateBatch([]UpdateBatchItem{{DocumentID: []byte("a"), Update: func([]byte) ([]byte, bool, error) { return newDoc, true, nil }}}); err != nil {
		t.Fatal(err)
	}
	if _, err := col.DropIndex("tenant"); err != nil {
		t.Fatal(err)
	}
	current, err := manager.nativeJSON.Capture("docs")
	if err != nil {
		t.Fatal(err)
	}
	reads = append(reads, current)
	if len(first.meta.Indexes) != 1 || len(current.meta.Indexes) != 0 {
		t.Fatal("old or current schema crossed cut")
	}
	for _, tc := range []struct {
		r    *NativeJSONRead
		want []byte
	}{{first, oldDoc}, {current, newDoc}} {
		got, err := tc.r.Get([]byte("a"))
		if err != nil || !bytes.Equal(got, tc.want) {
			t.Fatalf("held/current read %q %v", got, err)
		}
	}
	if current.collection == first.collection {
		t.Fatal("new cut reused stale catalog descriptor")
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Held old-cut readers retain their authority after current publication and
	// physical progress, then all receipts must retire at the actual last owner.
	got, err := first.Get([]byte("a"))
	if err != nil || !bytes.Equal(got, oldDoc) {
		t.Fatal("checkpoint invalidated held cut", err)
	}
	for i, r := range reads {
		if r != nil {
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			reads[i] = nil
		}
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if s := budget.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 {
		t.Fatalf("Close retained catalog receipts: %+v", s)
	}
	owner, manager, col = openNativeTextPublic(t, dir, treedb.ProfileNoWALFast, false)
	got, err = col.Get([]byte("a"))
	if err != nil || !bytes.Equal(got, newDoc) {
		t.Fatalf("reopen: %q %v", got, err)
	}
	reopened, err := manager.nativeJSON.Capture("docs")
	if err != nil {
		t.Fatal(err)
	}
	if len(reopened.meta.Indexes) != 0 {
		t.Fatal("reopen restored stale scalar definition")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	// Baseline must reach all semantic/held/reopen/Close assertions first.
	if !shared {
		t.Fatal("same original cold cut rebuilt independent catalog descriptors")
	}
}
