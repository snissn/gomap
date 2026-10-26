package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func orderedCompletionOptions(dir string) treedb.Options {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.MemtableMode = "cow_ordered"
	return opts
}

func TestOrderedSubmissionSuccessorWhileCapturedSyncHeld(t *testing.T) {
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
	var once sync.Once
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.BeforeDependencyFileSync {
			once.Do(func() { close(entered); <-release })
		}
		return nil
	})
	defer restore()
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var first, second CommitCompletion
	key, value := []byte{'a', 0, 'b'}, []byte("first-owned")
	if err := store.SubmitCommitGroupAt([]CommitGroup{{Timestamp: 2, Mutations: []Mutation{{Key: key, Value: value}}}}, CommitDurable, &first); err != nil {
		t.Fatal(err)
	}
	// Submission has copied/canonically installed the caller's operands.
	copy(key, []byte("zzz"))
	copy(value, []byte("overwritten"))
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.Complete() }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("completion failed to drive physical sync without application Wait")
	}
	secondSubmitted := make(chan error, 1)
	go func() {
		secondSubmitted <- store.SubmitCommitGroupAt([]CommitGroup{{Timestamp: 3, Mutations: []Mutation{{Key: []byte("second"), Value: []byte("successor")}}}}, CommitDurable, &second)
	}()
	select {
	case err := <-secondSubmitted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("successor preparation serialized behind predecessor sync")
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.Complete() }()
	select {
	case err := <-firstDone:
		t.Fatalf("acknowledged while sync held: %v", err)
	default:
	}
	select {
	case err := <-secondDone:
		t.Fatalf("successor acknowledged before prefix: %v", err)
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
			t.Fatal("completion stranded after barrier release")
		}
	}
	if err := first.Complete(); err != nil {
		t.Fatal(err)
	}
	if err := store.SubmitCommitGroupAt([]CommitGroup{{Timestamp: 4, Mutations: []Mutation{{Key: []byte("reuse"), Value: []byte("record")}}}}, CommitDurable, &first); err != nil {
		t.Fatal(err)
	}
	if err := first.Complete(); err != nil {
		t.Fatal(err)
	}
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
	requireResult(t, store, []byte{'a', 0, 'b'}, 2, Present, 2, []byte("first-owned"))
	requireResult(t, store, []byte("second"), 3, Present, 3, []byte("successor"))
	requireResult(t, store, []byte("reuse"), 4, Present, 4, []byte("record"))
}

func TestOrderedSubmissionFailureCustodyAndReuse(t *testing.T) {
	db, err := treedb.Open(orderedCompletionOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	var completion CommitCompletion
	if err := completion.Complete(); err != nil {
		t.Fatal(err)
	}
	if err := store.SubmitCommitGroupAt([]CommitGroup{{Timestamp: 0}}, CommitDurable, &completion); !errors.Is(err, ErrZeroTimestamp) {
		t.Fatalf("validation = %v", err)
	}
	if err := completion.Complete(); !errors.Is(err, ErrZeroTimestamp) {
		t.Fatalf("completed failed record = %v", err)
	}
	ms := []Mutation{{Key: []byte("a"), Value: []byte("one")}}
	if err := store.SubmitCommitGroupAt([]CommitGroup{{Timestamp: 1, Mutations: ms}}, CommitDurable, &completion); err != nil {
		t.Fatal(err)
	}
	if err := store.SubmitCommitGroupAt([]CommitGroup{{Timestamp: 2, Mutations: ms}}, CommitDurable, &completion); err == nil {
		t.Fatal("reused a live completion record")
	}
	if err := completion.Complete(); err != nil {
		t.Fatal(err)
	}
	if err := store.SubmitCommitGroupAt([]CommitGroup{{Timestamp: 2, Mutations: ms}}, CommitRelaxed, &completion); err != nil {
		t.Fatal(err)
	}
	if err := completion.Complete(); err != nil {
		t.Fatal(err)
	}
}

func TestOrderedPhysicalWorkBoundsAndOversizedAtomicRefusal(t *testing.T) {
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
	limit := db.MVCCWriteWorkLimit()
	work := db.EstimateMVCCWriteWork(16, 64)
	if work == 0 || work > limit || work == math.MaxUint64 {
		t.Fatalf("ordinary entry cannot fit work=%d limit=%d", work, limit)
	}
	if uint64(100000) <= limit/work {
		t.Fatal("physical work recombines unsafe100k command")
	}
	for _, sizes := range [][2]int{{-1, 1}, {1, -1}, {math.MaxInt, math.MaxInt}} {
		if got := db.EstimateMVCCWriteWork(sizes[0], sizes[1]); got != math.MaxUint64 {
			t.Fatalf("overflow sizes=%v work=%d", sizes, got)
		}
	}
	// A conservative producer-sized atomic group fits without changing defaults.
	n := int(limit / work)
	ms := make([]Mutation, n)
	for i := range ms {
		ms[i] = Mutation{Key: []byte(fmt.Sprintf("small/%08x", i)), Value: bytes.Repeat([]byte("v"), 64)}
	}
	if err := store.CommitAt(1, ms, CommitDurable); err != nil {
		t.Fatalf("bounded command n=%d work=%d limit=%d: %v", n, work, limit, err)
	}
	t.Logf("configured work: per-entry=%d limit=%d bounded-rows=%d", work, limit, n)
	ms = make([]Mutation, 100000)
	for i := range ms {
		ms[i] = Mutation{Key: []byte(fmt.Sprintf("oversize/%016x", i)), Value: []byte("next-edge")}
	}
	var completion CommitCompletion
	if err := store.SubmitCommitGroupAt([]CommitGroup{{Timestamp: 2, Mutations: ms}}, CommitDurable, &completion); err == nil || !errors.Is(err, ErrStorage) {
		t.Fatalf("oversized raw command = %v", err)
	}
	// Failed submission requires no completion call and must release every owner
	// needed by Close; the prior accepted history remains durable after reopen.
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
	requireResult(t, store, []byte("small/00000000"), 1, Present, 1, bytes.Repeat([]byte("v"), 64))
	requireResult(t, store, []byte("oversize/0000000000000000"), 2, Absent, 0, nil)
}

func TestOrderedSubmissionSyncFailureReleasesCustody(t *testing.T) {
	opts := orderedCompletionOptions(t.TempDir())
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	store := New(db)
	if err := store.CommitAt(1, []Mutation{{Key: []byte("anchor"), Value: []byte("stable")}}, CommitDurable); err != nil {
		t.Fatal(err)
	}
	var completion CommitCompletion
	if err := store.SubmitCommitGroupAt([]CommitGroup{{Timestamp: 2, Mutations: []Mutation{{Key: []byte("uncertain"), Value: []byte("assigned")}}}}, CommitDurable, &completion); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("controlled captured-prefix sync refusal")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.BeforeDependencyFileSync {
			return injected
		}
		return nil
	})
	err = completion.Complete()
	restore()
	if !errors.Is(err, injected) || !errors.Is(err, ErrStorage) {
		t.Fatalf("ambiguous completion = %v", err)
	}
	if again := completion.Complete(); again != err {
		t.Fatalf("completion error identity changed: %v != %v", again, err)
	}
	if err := store.SubmitCommitGroupAt([]CommitGroup{{Timestamp: 3, Mutations: []Mutation{{Key: []byte("later"), Value: []byte("refuse")}}}}, CommitDurable, &completion); err == nil {
		t.Fatal("assigned sync failure did not poison later producer")
	}
	// Failed Submit needs no Complete, and neither failure retains teardown/floor.
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("failed completion/submission retained lifecycle custody")
	}
}
