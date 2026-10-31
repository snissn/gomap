package treedb

import (
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
)

type completionPanicCloseBatch struct {
	Batch
	panicValue  any
	calls       int
	goexit      bool
	beforePanic func()
}

func (b *completionPanicCloseBatch) Close() error {
	b.calls++
	_ = b.Batch.Close()
	if b.beforePanic != nil {
		b.beforePanic()
	}
	if b.goexit {
		runtime.Goexit()
	}
	panic(b.panicValue)
}

func TestOrderedCompletionCleanupPanic(t *testing.T) {
	for _, nilPanic := range []bool{false, true} {
		for _, nilCleanup := range []bool{false, true} {
			for _, physicalPanic := range []bool{false, true} {
				t.Run(map[bool]string{false: "body-nonnil/", true: "body-nil/"}[nilPanic]+map[bool]string{false: "cleanup-nonnil/", true: "cleanup-nil/"}[nilCleanup]+map[bool]string{false: "cleanup-only", true: "original-and-cleanup"}[physicalPanic], func(t *testing.T) {
					opts := OptionsFor(ProfileCommandWALDurable, t.TempDir())
					opts.MemtableMode = "cow_ordered"
					db, err := Open(opts)
					if err != nil {
						t.Fatal(err)
					}
					rows := []mvccadmission.LogicalMutation{{Key: []byte("cleanup"), Value: []byte("uncertain"), Timestamp: 1}}
					var c LogicalMVCCCompletion
					if err := db.SubmitLogicalMVCC(rows, db.NewMVCCAdmission().LogicalInput(rows, 0), true, &c); err != nil {
						t.Fatal(err)
					}
					var original, secondary any = new(int), new(int)
					if nilPanic {
						original = nil
					}
					if nilCleanup {
						secondary = nil
					}
					wrapper := &completionPanicCloseBatch{Batch: c.batch.inner, panicValue: secondary}
					fired := false
					restore := durabilitycut.Install(func(e durabilitycut.Event) error {
						if !fired && e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
							fired = true
							c.batch.inner = wrapper
							if physicalPanic {
								panic(original)
							}
						}
						return nil
					})
					var recovered any
					returned := false
					func() { defer func() { recovered = recover() }(); _ = c.Complete(); returned = true }()
					restore()
					want := secondary
					if physicalPanic {
						want = original
					}
					matches := recovered == want
					if want == nil && recovered != nil {
						_, matches = recovered.(*runtime.PanicNilError)
					}
					if !fired || returned || !matches {
						t.Fatalf("panic fired=%t got=%v want=%v", fired, recovered, want)
					}
					if wrapper.calls != 1 || c.batch != nil || c.db != nil {
						t.Fatalf("cleanup calls=%d batch=%v db=%v", wrapper.calls, c.batch, c.db)
					}
					if err := c.Complete(); !errors.Is(err, ErrRecoveryRequired) {
						t.Fatalf("repeat=%v", err)
					}
					if !db.lifecycleMu.TryLock() {
						t.Fatal("lifecycle custody retained")
					}
					db.lifecycleMu.Unlock()
					if got := db.commandWALGroupCommit.errors.Load(); got != 1 {
						t.Fatalf("panic counted more than once: %d", got)
					}
					// Completion poisoning cannot claim an unrelated physical leader's custody.
					db.commandWALGroupCommit.mu.Lock()
					db.commandWALGroupCommit.leaderActive = true
					db.commandWALGroupCommit.mu.Unlock()
					db.commandWALGroupCommit.failPanickedCompletion(db)
					db.commandWALGroupCommit.mu.Lock()
					active := db.commandWALGroupCommit.leaderActive
					db.commandWALGroupCommit.leaderActive = false
					db.commandWALGroupCommit.mu.Unlock()
					if !active || db.commandWALGroupCommit.errors.Load() != 1 {
						t.Fatal("poisoning is not idempotent/retired unrelated leader")
					}
					_ = db.Close()
				})
			}
		}
	}
}

func TestOrderedCompletionCleanupGoexit(t *testing.T) {
	opts := OptionsFor(ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_ordered"
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	rows := []mvccadmission.LogicalMutation{{Key: []byte("exit"), Value: []byte("uncertain"), Timestamp: 1}}
	var c LogicalMVCCCompletion
	if err := db.SubmitLogicalMVCC(rows, db.NewMVCCAdmission().LogicalInput(rows, 0), true, &c); err != nil {
		t.Fatal(err)
	}
	wrapper := &completionPanicCloseBatch{Batch: c.batch.inner, goexit: true}
	fired := false
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if !fired && e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
			fired = true
			c.batch.inner = wrapper
		}
		return nil
	})
	done := make(chan struct{})
	returned := false
	go func() { defer close(done); _ = c.Complete(); returned = true }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup Goexit did not unwind")
	}
	restore()
	if returned || !fired || wrapper.calls != 1 {
		t.Fatalf("exit returned=%t fired=%t close=%d", returned, fired, wrapper.calls)
	}
	if err := c.Complete(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("repeat=%v", err)
	}
	if !db.lifecycleMu.TryLock() {
		t.Fatal("lifecycle retained")
	}
	db.lifecycleMu.Unlock()
	_ = db.Close()
}

// A cleanup failure on an older ACK cannot retire a newer physical leader or
// let Close release that leader's lifecycle custody before its I/O unwinds.
func TestOrderedCompletionCleanupPanicWithHeldLeader(t *testing.T) {
	opts := OptionsFor(ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_ordered"
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	submit := func(ts uint64, key string, c *LogicalMVCCCompletion) {
		t.Helper()
		rows := []mvccadmission.LogicalMutation{{Key: []byte(key), Value: []byte("uncertain"), Timestamp: ts}}
		if err := db.SubmitLogicalMVCC(rows, db.NewMVCCAdmission().LogicalInput(rows, 0), true, c); err != nil {
			t.Fatal(err)
		}
	}
	var first, leader, peer LogicalMVCCCompletion
	submit(1, "older", &first)
	cleanupEntered := make(chan struct{})
	cleanupRelease := make(chan struct{})
	physicalEntered := make(chan struct{})
	physicalRelease := make(chan struct{})
	var releaseCleanup, releasePhysical sync.Once
	defer releaseCleanup.Do(func() { close(cleanupRelease) })
	defer releasePhysical.Do(func() { close(physicalRelease) })
	sentinel := new(int)
	wrapper := &completionPanicCloseBatch{Batch: first.batch.inner, panicValue: sentinel, beforePanic: func() { close(cleanupEntered); <-cleanupRelease }}
	installed := false
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if !installed && e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
			installed = true
			first.batch.inner = wrapper
		}
		return nil
	})
	firstDone := make(chan any, 1)
	go func() { defer func() { firstDone <- recover() }(); _ = first.Complete() }()
	select {
	case <-cleanupEntered:
	case <-time.After(2 * time.Second):
		restore()
		t.Fatal("older cleanup not reached")
	}
	restore()
	submit(2, "leader", &leader)
	submit(3, "peer", &peer)
	held := false
	restore = durabilitycut.Install(func(e durabilitycut.Event) error {
		if !held && e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
			held = true
			close(physicalEntered)
			<-physicalRelease
		}
		return nil
	})
	defer restore()
	leaderDone := make(chan error, 1)
	go func() { leaderDone <- leader.Complete() }()
	select {
	case <-physicalEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("new leader not held")
	}
	releaseCleanup.Do(func() { close(cleanupRelease) })
	select {
	case got := <-firstDone:
		if got != sentinel {
			t.Fatalf("cleanup panic=%v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("older cleanup did not retire")
	}
	db.commandWALGroupCommit.mu.Lock()
	active := db.commandWALGroupCommit.leaderActive
	terminal := db.commandWALGroupCommit.terminalErr
	db.commandWALGroupCommit.mu.Unlock()
	if !active || !errors.Is(terminal, ErrRecoveryRequired) {
		t.Fatalf("new leader custody=%v terminal=%v", active, terminal)
	}
	if err := peer.Complete(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("peer=%v", err)
	}
	closeDone := make(chan struct{})
	go func() { _ = db.Close(); close(closeDone) }()
	select {
	case <-closeDone:
		t.Fatal("Close escaped physical leader custody")
	case <-time.After(20 * time.Millisecond):
	}
	releasePhysical.Do(func() { close(physicalRelease) })
	select {
	case err := <-leaderDone:
		if !errors.Is(err, ErrRecoveryRequired) {
			t.Fatalf("held leader=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("leader did not retire")
	}
	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not finish after physical release")
	}
	if wrapper.calls != 1 {
		t.Fatalf("close calls=%d", wrapper.calls)
	}
}
