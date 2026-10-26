package mvcc

import (
	"bytes"
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

func TestOrderedBuilderIncrementalNativeWorkBeforeSeal(t *testing.T) {
	opts := orderedCompletionOptions(t.TempDir())
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()
	store := New(db)
	if err := store.CommitAt(1, []Mutation{{Key: []byte("anchor"), Value: []byte("accepted")}}, CommitDurable); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	var stages, preparations atomic.Int32
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		switch e.Point {
		case durabilitycut.AfterLogicalMVCCStaging:
			stages.Add(1)
		case durabilitycut.BeforeCOWPreparation:
			preparations.Add(1)
		case durabilitycut.BeforeDependencyFileSync:
			if e.Resource == durabilitycut.ResourceCommandWAL {
				once.Do(func() { close(entered); <-release })
			}
		}
		return nil
	})
	defer func() { restore() }()
	defer unblock()
	var first CommitCompletion
	if err := store.SubmitCommitGroupAt([]CommitGroup{{Timestamp: 2, Mutations: []Mutation{{Key: []byte("first"), Value: []byte("one")}}}}, CommitDurable, &first); err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.Complete() }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("physical sync not held")
	}
	prepBefore := preparations.Load()
	var builder CommitBuilder
	if err := store.BeginCommitGroup(&builder); err != nil {
		t.Fatal(err)
	}
	defer builder.Abort()
	key, value := []byte{'x', 0, 'a'}, []byte("owned-before-seal")
	for i, group := range []CommitGroup{
		{Timestamp: 3, Mutations: []Mutation{{Key: key, Value: value}}},
		{Timestamp: 4, Mutations: []Mutation{{Key: []byte("second"), Value: []byte("four")}}},
	} {
		done := make(chan error, 1)
		go func() { done <- builder.AppendGroup(group) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("native staging blocked on predecessor sync")
		}
		if got := stages.Load(); got != int32(i+1) {
			t.Fatalf("owned staging events=%d", got)
		}
		if preparations.Load() != prepBefore {
			t.Fatal("root-dependent preparation occurred before seal")
		}
	}
	copy(key, []byte("zzz"))
	copy(value, []byte("mutated-after-copy"))
	var second CommitCompletion
	submitted := make(chan error, 1)
	go func() { submitted <- builder.Submit(CommitDurable, &second) }()
	select {
	case err := <-submitted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("seal blocked on prior physical barrier")
	}
	if preparations.Load() <= prepBefore {
		t.Fatal("seal omitted canonical preparation")
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.Complete() }()
	select {
	case err := <-firstDone:
		t.Fatalf("early ACK %v", err)
	default:
	}
	select {
	case err := <-secondDone:
		t.Fatalf("early successor ACK %v", err)
	default:
	}
	unblock()
	for _, done := range []<-chan error{firstDone, secondDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("completion stalled")
		}
	}
	restore()
	restore = func() {}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	db, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store = New(db)
	requireResult(t, store, []byte{'x', 0, 'a'}, 3, Present, 3, []byte("owned-before-seal"))
	requireResult(t, store, []byte("second"), 4, Present, 4, []byte("four"))
}

func TestOrderedBuilderValidationAbortAndReuse(t *testing.T) {
	db, err := treedb.Open(orderedCompletionOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	var b CommitBuilder
	if err := store.BeginCommitGroup(&b); err != nil {
		t.Fatal(err)
	}
	defer b.Abort()
	group := CommitGroup{Timestamp: 1, Mutations: []Mutation{{Key: []byte("a"), Value: []byte("one")}}}
	if err := b.AppendGroup(group); err != nil {
		t.Fatal(err)
	}
	for _, group := range []CommitGroup{{Timestamp: 0}, group, {Timestamp: 2, Mutations: []Mutation{{Key: bytes.Repeat([]byte("k"), 65536)}}}} {
		err := b.AppendGroup(group)
		if err == nil || errors.Is(err, ErrStorage) {
			t.Fatalf("preappend validation %v", err)
		}
	}
	if err := b.AppendGroup(CommitGroup{Timestamp: 2, Mutations: []Mutation{{Key: []byte("b"), Value: []byte("two")}}}); err != nil {
		t.Fatal(err)
	}
	var c CommitCompletion
	if err := b.Submit(CommitDurable, &c); err != nil {
		t.Fatal(err)
	}
	if err := c.Complete(); err != nil {
		t.Fatal(err)
	}
	if err := store.BeginCommitGroup(&b); err != nil {
		t.Fatal(err)
	}
	if err := b.AppendGroup(CommitGroup{Timestamp: 3, Mutations: []Mutation{{Key: []byte("abort"), Value: []byte("never")}}}); err != nil {
		t.Fatal(err)
	}
	if err := b.Abort(); err != nil {
		t.Fatal(err)
	}
	if err := b.Abort(); err != nil {
		t.Fatal(err)
	}
	requireResult(t, store, []byte("abort"), 3, Absent, 0, nil)
	if err := store.BeginCommitGroup(&b); err != nil {
		t.Fatal(err)
	}
	if err := b.Submit(CommitDurable, &c); err != nil {
		t.Fatal(err)
	}
	if err := c.Complete(); err != nil {
		t.Fatal(err)
	}
}

func TestOrderedBuilderTransfersQueuedFloorWriter(t *testing.T) {
	db, err := treedb.Open(orderedCompletionOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	var b CommitBuilder
	if err := store.BeginCommitGroup(&b); err != nil {
		t.Fatal(err)
	}
	defer b.Abort()
	if err := b.AppendGroup(CommitGroup{Timestamp: 1, Mutations: []Mutation{{Key: []byte("a"), Value: []byte("one")}}}); err != nil {
		t.Fatal(err)
	}
	writerDone := make(chan struct{})
	go func() { store.mu.Lock(); store.mu.Unlock(); close(writerDone) }()
	// Wait for actual RWMutex writer exclusion, without changing the floor or
	// sleeping past a timing assumption. The builder already owns the read lock.
	deadline := time.Now().Add(2 * time.Second)
	for store.mu.TryRLock() {
		store.mu.RUnlock()
		if time.Now().After(deadline) {
			t.Fatal("floor writer did not queue")
		}
		runtime.Gosched()
	}
	var c CommitCompletion
	done := make(chan error, 1)
	go func() { done <- b.Submit(CommitDurable, &c) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("seal recursively acquired queued floor RLock")
	}
	select {
	case <-writerDone:
		t.Fatal("floor custody released before ACK")
	default:
	}
	if err := c.Complete(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-writerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("completion did not release floor custody")
	}
}

func TestOrderedBuilderManyIncrementalGroupsFitWorkBound(t *testing.T) {
	db, err := treedb.Open(orderedCompletionOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	var b CommitBuilder
	if err := store.BeginCommitGroup(&b); err != nil {
		t.Fatal(err)
	}
	defer b.Abort()
	n := int(db.MVCCWriteWorkLimit() / db.EstimateMVCCWriteWork(16, 64))
	for i := 0; i < n; i++ {
		if err := b.AppendGroup(CommitGroup{Timestamp: uint64(i + 1), Mutations: []Mutation{{Key: []byte(fmt.Sprintf("%016x", i)), Value: bytes.Repeat([]byte("x"), 64)}}}); err != nil {
			t.Fatalf("incremental stage %d/%d: %v", i, n, err)
		}
	}
	var c CommitCompletion
	if err := b.Submit(CommitDurable, &c); err != nil {
		t.Fatal(err)
	}
	if err := c.Complete(); err != nil {
		t.Fatal(err)
	}
}

func TestOrderedBuilderSubmitMisuseReleasesUnassignedCustody(t *testing.T) {
	db, err := treedb.Open(orderedCompletionOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	store := New(db)
	var active CommitCompletion
	if err := store.SubmitCommitGroupAt([]CommitGroup{{Timestamp: 1, Mutations: []Mutation{{Key: []byte("prior"), Value: []byte("accepted")}}}}, CommitDurable, &active); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"invalid-mode", "nil-completion", "active-completion"} {
		var b CommitBuilder
		if err := store.BeginCommitGroup(&b); err != nil {
			t.Fatal(err)
		}
		if err := b.AppendGroup(CommitGroup{Timestamp: 2, Mutations: []Mutation{{Key: []byte(kind), Value: []byte("unassigned")}}}); err != nil {
			t.Fatal(err)
		}
		var c CommitCompletion
		var err error
		switch kind {
		case "invalid-mode":
			err = b.Submit(CommitMode(255), &c)
		case "nil-completion":
			err = b.Submit(CommitDurable, nil)
		case "active-completion":
			err = b.Submit(CommitDurable, &active)
		}
		if err == nil {
			t.Fatalf("%s accepted", kind)
		}
		if b.store != nil {
			t.Fatalf("%s retained builder", kind)
		}
	}
	if err := active.Complete(); err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("submit misuse retained lifecycle custody")
	}
}

func TestOrderedBuilderExactRouteEligibility(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile treedb.Profile
		mode    string
		phases  bool
		want    bool
	}{
		{"durable-ordered", treedb.ProfileCommandWALDurable, "cow_ordered", false, true},
		{"relaxed-ordered", treedb.ProfileCommandWALRelaxed, "cow_ordered", false, true},
		{"durable-phases", treedb.ProfileCommandWALDurable, "cow_ordered", true, false},
		{"no-wal", treedb.ProfileNoWALFast, "cow_ordered", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := treedb.OptionsFor(tc.profile, t.TempDir())
			opts.MemtableMode = tc.mode
			opts.PublicBatchWriteSyncPhaseStats = tc.phases
			db, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			store := New(db)
			if got := store.SupportsCommitGroupStaging(); got != tc.want {
				t.Fatalf("support=%v want%v", got, tc.want)
			}
			if got := db.SupportsLogicalMVCCStaging(); got != tc.want {
				t.Fatalf("DB support=%v want%v", got, tc.want)
			}
			if tc.want {
				var b CommitBuilder
				if err := store.BeginCommitGroup(&b); err != nil {
					t.Fatal(err)
				}
				if err := b.Abort(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
