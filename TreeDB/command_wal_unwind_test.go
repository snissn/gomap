package treedb

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// A child bounds a regression that leaves Close or publication custody pinned.
func TestPublicCommandWALUnwind(t *testing.T) {
	if mode := os.Getenv("GOMAP_ORDINARY_UNWIND_CHILD"); mode != "" {
		publicCommandWALUnwindChild(t, mode)
		return
	}
	for _, mode := range []string{"append-nil", "capture-nil", "sync-nil", "capture-goexit", "sync-goexit"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPublicCommandWALUnwind$", "-test.timeout=12s", "-test.v")
			cmd.Env = append(os.Environ(), "GOMAP_ORDINARY_UNWIND_CHILD="+mode, "GODEBUG=panicnil=1")
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("%s: %v\n%s", mode, err, out)
			}
		})
	}
}

func publicCommandWALUnwindChild(t *testing.T, mode string) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	fired := false
	returned := false
	var recovered any
	action := func() {
		if mode == "append-nil" {
			_, _ = db.appendAndRegisterPublicCommandWAL(1, nil, true, nil, nil, nil, func(bool) (uint64, error) { fired = true; panic(nil) })
		} else {
			point := durabilitycut.BeforeDependencyFileSync
			if mode == "capture-nil" || mode == "capture-goexit" {
				point = durabilitycut.BeforeUserspaceFlush
			}
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				if !fired && e.Resource == durabilitycut.ResourceCommandWAL && e.Point == point {
					fired = true
					if mode == "capture-goexit" || mode == "sync-goexit" {
						runtime.Goexit()
					}
					panic(nil)
				}
				return nil
			})
			defer restore()
			_ = db.SetSync([]byte("ordinary-unwind"), []byte("uncertain"))
		}
	}
	done := make(chan struct{})
	go func() { defer close(done); defer func() { recovered = recover() }(); action(); returned = true }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("unwind did not complete")
	}
	if !fired || returned || recovered != nil {
		t.Fatalf("fired=%v returned=%v recovered=%v", fired, returned, recovered)
	}
	if !db.commandWALPublicOperationGate.TryLock() {
		t.Fatal("operation gate retained")
	}
	db.commandWALPublicOperationGate.Unlock()
	if !db.commandWALPublicPublishMu.TryLock() {
		t.Fatal("publish gate retained")
	}
	db.commandWALPublicPublishMu.Unlock()
	if db.commandWALGroupCommit.durableIntents.Load() != 0 {
		t.Fatal("intent retained")
	}
	if mode != "append-nil" {
		db.commandWALGroupCommit.mu.Lock()
		active, terminal := db.commandWALGroupCommit.leaderActive, db.commandWALGroupCommit.terminalErr
		db.commandWALGroupCommit.mu.Unlock()
		if active || !errors.Is(terminal, ErrRecoveryRequired) {
			t.Fatalf("active=%v terminal=%v", active, terminal)
		}
		if err := db.SetSync([]byte("later"), []byte("must-fail")); !errors.Is(err, ErrRecoveryRequired) {
			t.Fatalf("later=%v", err)
		}
	}
	_ = db.Close()
}
