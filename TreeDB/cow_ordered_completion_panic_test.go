package treedb

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
)

type completionPanicCloseBatch struct {
	Batch
	panicValue any
	calls      int
}

func (b *completionPanicCloseBatch) Close() error {
	b.calls++
	_ = b.Batch.Close()
	panic(b.panicValue)
}

func TestOrderedCompletionCleanupPanic(t *testing.T) {
	for _, physicalPanic := range []bool{false, true} {
		t.Run(map[bool]string{false: "cleanup-only", true: "original-and-cleanup"}[physicalPanic], func(t *testing.T) {
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
			original, secondary := new(int), new(int)
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
			func() { defer func() { recovered = recover() }(); _ = c.Complete() }()
			restore()
			want := secondary
			if physicalPanic {
				want = original
			}
			if !fired || recovered != want {
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
