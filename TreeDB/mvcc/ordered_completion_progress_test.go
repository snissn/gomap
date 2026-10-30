package mvcc

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// The first completion must make progress even when a later submitted handle
// was elected while its predecessor sync was in progress. The caller retains
// custody of B until A returns, as a single FIFO completion worker does.
func TestOrderedCompletionFollowerBeforeElectedSuccessor(t *testing.T) {
	for _, successors := range []int{1, 70} {
		t.Run(fmt.Sprintf("successors-%d", successors), func(t *testing.T) {
			testOrderedCompletionFollowerBeforeSuccessors(t, successors)
		})
	}
}

func testOrderedCompletionFollowerBeforeSuccessors(t *testing.T, successors int) {
	opts := orderedCompletionOptions(t.TempDir())
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	store := New(db)
	if err := store.CommitAt(1, []Mutation{{Key: []byte("anchor"), Value: []byte("v")}}, CommitDurable); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var block sync.Once
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
			block.Do(func() { close(entered); <-release })
		}
		return nil
	})
	defer restore()
	var p, a CommitCompletion
	bs := make([]CommitCompletion, successors)
	submit := func(c *CommitCompletion, n uint64) {
		t.Helper()
		err := store.SubmitCommitGroupAt([]CommitGroup{{Timestamp: n, Mutations: []Mutation{{Key: []byte(fmt.Sprintf("key-%d", n)), Value: []byte("value")}}}}, CommitDurable, c)
		if err != nil {
			t.Fatal(err)
		}
	}
	submit(&p, 2)
	pdone := make(chan error, 1)
	go func() { pdone <- p.Complete() }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("P did not enter sync")
	}
	submit(&a, 3)
	close(release)
	select {
	case err := <-pdone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("P did not complete")
	}
	for i := range bs {
		submit(&bs[i], uint64(i+4))
	}
	adone := make(chan error, 1)
	go func() { adone <- a.Complete() }()
	var blocked bool
	var stack []byte
	select {
	case err := <-adone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		blocked = true
		stack = make([]byte, 1<<20)
		stack = stack[:runtime.Stack(stack, true)]
	}
	bdone := make(chan error, 1)
	go func() { bdone <- bs[0].Complete() }()
	if blocked {
		select {
		case err := <-adone:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("A stayed blocked after B completed")
		}
	}
	select {
	case err := <-bdone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("B did not finish")
	}
	for i := 1; i < len(bs); i++ {
		if err := bs[i].Complete(); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store = New(db)
	for n := uint64(2); n < uint64(successors+4); n++ {
		requireResult(t, store, []byte(fmt.Sprintf("key-%d", n)), n, Present, n, []byte("value"))
	}
	if blocked {
		t.Fatalf("A completion waited on later B; B must be drivable independently of FIFO A\n%s", stack)
	}
}

// A later completion may drive the sync, but it must not acknowledge before
// the earlier publication. Both callers eventually return without duplicating
// the group driver or skipping the earlier publication ticket.
func TestOrderedCompletionLaterWaiterCannotOvertakePublication(t *testing.T) {
	opts := orderedCompletionOptions(t.TempDir())
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	store := New(db)
	var a, b CommitCompletion
	for i, completion := range []*CommitCompletion{&a, &b} {
		if err := store.SubmitCommitGroupAt([]CommitGroup{{Timestamp: uint64(i + 1), Mutations: []Mutation{{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")}}}}, CommitDurable, completion); err != nil {
			t.Fatal(err)
		}
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var active, maximum atomic.Int32
	var hold sync.Once
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource != durabilitycut.ResourceCommandWAL {
			return nil
		}
		switch e.Point {
		case durabilitycut.BeforeDependencyFileSync:
			n := active.Add(1)
			for old := maximum.Load(); n > old && !maximum.CompareAndSwap(old, n); old = maximum.Load() {
			}
			hold.Do(func() { close(entered); <-release })
		case durabilitycut.AfterDependencyFileSync:
			active.Add(-1)
		}
		return nil
	})
	var restoreOnce sync.Once
	cleanup := func() { restoreOnce.Do(restore) }
	defer cleanup()
	bdone := make(chan error, 1)
	go func() { bdone <- b.Complete() }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("later completion did not drive sync")
	}
	adone := make(chan error, 1)
	go func() { adone <- a.Complete() }()
	select {
	case err := <-bdone:
		close(release)
		t.Fatalf("later completion overtook the held sync: %v", err)
	case err := <-adone:
		close(release)
		t.Fatalf("earlier completion overtook the held sync: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	for _, done := range []<-chan error{adone, bdone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent completions did not drain")
		}
	}
	if maximum.Load() != 1 || active.Load() != 0 {
		t.Fatalf("physical sync concurrency max=%d active=%d", maximum.Load(), active.Load())
	}
	cleanup()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store = New(db)
	requireResult(t, store, []byte("k0"), 1, Present, 1, []byte("v"))
	requireResult(t, store, []byte("k1"), 2, Present, 2, []byte("v"))
}

// A failed sync driven by a later handle remains available to an earlier
// handle completed afterward; both failures release lifecycle custody.
func TestOrderedCompletionLaterDriverSyncFailureRetainedForEarlier(t *testing.T) {
	db, err := treedb.Open(orderedCompletionOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	store := New(db)
	var a, b CommitCompletion
	for i, completion := range []*CommitCompletion{&a, &b} {
		if err := store.SubmitCommitGroupAt([]CommitGroup{{Timestamp: uint64(i + 1), Mutations: []Mutation{{Key: []byte(fmt.Sprintf("failure-%d", i)), Value: []byte("v")}}}}, CommitDurable, completion); err != nil {
			t.Fatal(err)
		}
	}
	injected := errors.New("controlled group sync failure")
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
			return injected
		}
		return nil
	})
	var restoreOnce sync.Once
	cleanup := func() { restoreOnce.Do(restore) }
	defer cleanup()
	bdone := make(chan error, 1)
	go func() { bdone <- b.Complete() }()
	select {
	case err := <-bdone:
		if !errors.Is(err, injected) {
			t.Fatalf("later completion: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("sync failure did not wake driver")
	}
	adone := make(chan error, 1)
	go func() { adone <- a.Complete() }()
	select {
	case err := <-adone:
		if !errors.Is(err, injected) {
			t.Fatalf("earlier completion: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stored sync failure did not return to earlier completion")
	}
	cleanup()
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("failed group retained close custody")
	}
}

// A completion that drove a shared sync still must wait for an earlier
// handle to publish; sync coverage alone does not authorize out-of-order ACK.
func TestOrderedCompletionLaterWaiterWaitsForEarlierPublication(t *testing.T) {
	db, err := treedb.Open(orderedCompletionOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	store := New(db)
	var a, b CommitCompletion
	for i, c := range []*CommitCompletion{&a, &b} {
		if err := store.SubmitCommitGroupAt([]CommitGroup{{Timestamp: uint64(i + 1), Mutations: []Mutation{{Key: []byte(fmt.Sprintf("p%d", i)), Value: []byte("v")}}}}, CommitDurable, c); err != nil {
			t.Fatal(err)
		}
	}
	bdone := make(chan error, 1)
	go func() { bdone <- b.Complete() }()
	select {
	case err := <-bdone:
		t.Fatalf("later ACK overtook earlier publication: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := a.Complete(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-bdone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("later completion did not drain")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
