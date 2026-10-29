package mvcc

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// Each failure stays in a bounded child: the original bug retains lifecycle
// custody, so a deferred Close would otherwise hang the test runner itself.
func TestOrderedCompletionPanicCustody(t *testing.T) {
	if mode := os.Getenv("GOMAP_COMPLETION_PANIC_CHILD"); mode != "" {
		orderedCompletionPanicChild(t, mode)
		return
	}
	for _, point := range []durabilitycut.Point{durabilitycut.BeforeUserspaceFlush, durabilitycut.BeforeDependencyFileSync} {
		for _, check := range []string{"custody", "repeat"} {
			mode := string(point) + "/" + check
			t.Run(mode, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOrderedCompletionPanicCustody$", "-test.v", "-test.timeout=12s")
				cmd.Env = append(os.Environ(), "GOMAP_COMPLETION_PANIC_CHILD="+mode)
				out, err := cmd.CombinedOutput() // Wait reaps the sole child on every path.
				if err != nil {
					t.Fatalf("child %s: %v\n%s", mode, err, out)
				}
				t.Logf("%s", out)
			})
		}
	}
}

func orderedCompletionPanicChild(t *testing.T, mode string) {
	var point durabilitycut.Point
	check := ""
	for _, p := range []durabilitycut.Point{durabilitycut.BeforeUserspaceFlush, durabilitycut.BeforeDependencyFileSync} {
		for _, c := range []string{"custody", "repeat"} {
			if mode == string(p)+"/"+c {
				point, check = p, c
			}
		}
	}
	if check == "" {
		t.Fatal("invalid child mode")
	}
	opts := orderedCompletionOptions(t.TempDir())
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	store := New(db)
	if err := store.CommitAt(1, []Mutation{{Key: []byte("anchor"), Value: []byte("accepted")}}, CommitDurable); err != nil {
		t.Fatal(err)
	}
	var completion CommitCompletion
	if err := store.SubmitCommitGroupAt([]CommitGroup{{Timestamp: 2, Mutations: []Mutation{{Key: []byte("panic-owned"), Value: []byte("uncertain")}}}}, CommitDurable, &completion); err != nil {
		t.Fatal(err)
	}
	var peer CommitCompletion
	if err := store.SubmitCommitGroupAt([]CommitGroup{{Timestamp: 3, Mutations: []Mutation{{Key: []byte("peer"), Value: []byte("uncertain")}}}}, CommitDurable, &peer); err != nil {
		t.Fatal(err)
	}
	sentinel := new(int)
	fired := false
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if !fired && e.Resource == durabilitycut.ResourceCommandWAL && e.Point == point {
			fired = true
			panic(sentinel)
		}
		return nil
	})
	var recovered any
	func() { defer func() { recovered = recover() }(); _ = completion.Complete() }()
	restore()
	if !fired || recovered != sentinel {
		t.Fatalf("physical panic not preserved: fired=%t recovered=%v", fired, recovered)
	}
	if err := peer.Complete(); !errors.Is(err, treedb.ErrRecoveryRequired) {
		t.Fatalf("peer failed to retire: %v", err)
	}
	if check == "custody" {
		if !store.mu.TryLock() {
			t.Fatal("panic retained Store discard-floor custody")
		}
		store.mu.Unlock()
	} else {
		err := completion.Complete()
		if !errors.Is(err, ErrStorage) || !errors.Is(err, treedb.ErrRecoveryRequired) {
			t.Fatalf("repeat completion lost uncertain outcome: %v", err)
		}
		if again := completion.Complete(); again != err {
			t.Fatalf("repeat error not stable: %v vs %v", again, err)
		}
	}

	// Concurrent repeated owners must see the same stored terminal error.
	var owners sync.WaitGroup
	errorsSeen := make(chan error, 4)
	for i := 0; i < 4; i++ {
		owners.Add(1)
		go func() { defer owners.Done(); errorsSeen <- completion.Complete() }()
	}
	owners.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if !errors.Is(err, treedb.ErrRecoveryRequired) {
			t.Fatalf("concurrent completion: %v", err)
		}
	}
	if err := db.SetSync([]byte("later"), []byte("must-fail")); !errors.Is(err, treedb.ErrRecoveryRequired) {
		t.Fatalf("later write did not fail closed: %v", err)
	}
	// No other goroutine is created here: child timeout bounds Close as well.
	_ = db.Close()
	reopened, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	requireResult(t, New(reopened), []byte("anchor"), 1, Present, 1, []byte("accepted"))
	// The interrupted commands may replay: failure is ambiguous, not rollback.
}
