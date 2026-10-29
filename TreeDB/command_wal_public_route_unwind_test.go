package treedb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commandwalbarrier"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// These are public writes. Backend barrier registration and durabilitycut are
// deterministic observers/fault injectors, not substitute writer entrypoints.
func TestPublicCommandWALRouteUnwind(t *testing.T) {
	if spec := os.Getenv("GOMAP_PUBLIC_ROUTE_UNWIND"); spec != "" {
		publicCommandWALRouteUnwindChild(t, spec)
		return
	}
	for _, route := range []string{"point-flush", "point-sync", "batch-flush", "batch-postappend-hook", "batch-assigned-hook", "batch-assigned-drain", "batch-assigned-flush", "range-flush", "range-postappend-hook", "mixed-flush"} {
		for _, mode := range []string{"modern-nil", "legacy-nil", "goexit"} {
			t.Run(route+"/"+mode, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPublicCommandWALRouteUnwind$", "-test.timeout=17s", "-test.v")
				debug := "panicnil=0"
				if mode != "modern-nil" {
					debug = "panicnil=1"
				}
				cmd.Env = append(os.Environ(), "GOMAP_PUBLIC_ROUTE_UNWIND="+route+"/"+mode, "GODEBUG="+debug)
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("%v\n%s", err, out)
				}
				t.Logf("%s", out)
			})
		}
	}
}

func publicCommandWALRouteUnwindChild(t *testing.T, spec string) {
	route, mode, _ := strings.Cut(spec, "/")
	opts := commandWALDurabilityProofOptions(t.TempDir())
	opts.MemtableMode = "btree"
	opts.MemtableShards = 2
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	if db.ResolvedProfile() != ProfileCommandWALDurable || db.cached.COWMode() {
		t.Fatal("ordinary command WAL fixture inactive")
	}
	anchor, value := []byte("0-durable-anchor"), []byte("stable")
	if err = db.SetSync(anchor, value); err != nil {
		t.Fatal(err)
	}
	var stack []string
	fired, returned := false, false
	var recovered any
	fault := func() {
		if fired {
			panic("fault unexpectedly repeated")
		}
		fired = true
		pcs := make([]uintptr, 64)
		n := runtime.Callers(1, pcs)
		frames := runtime.CallersFrames(pcs[:n])
		for {
			f, more := frames.Next()
			stack = append(stack, fmt.Sprintf("%s:%d", f.Function, f.Line))
			if !more {
				break
			}
		}
		if mode == "goexit" {
			runtime.Goexit()
		}
		panic(nil)
	}
	calls := 0
	unregister := db.backend.RegisterCommandWALRawPublishBarrier(func() error {
		calls++
		if (route == "batch-postappend-hook" || route == "range-postappend-hook") && calls == 2 {
			fault()
		}
		if strings.HasPrefix(route, "batch-assigned-") {
			if calls == 2 {
				return &commandwalbarrier.PendingDrain{Drain: func() error { return nil }}
			}
			if calls == 3 {
				if route == "batch-assigned-hook" {
					fault()
				}
				if route == "batch-assigned-drain" {
					return &commandwalbarrier.PendingDrain{Drain: func() error { fault(); return nil }}
				}
			}
		}
		return nil
	})
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if !fired && e.Resource == durabilitycut.ResourceCommandWAL {
			if (strings.HasSuffix(route, "-flush") && e.Point == durabilitycut.BeforeUserspaceFlush) || (route == "point-sync" && e.Point == durabilitycut.BeforeDependencyFileSync) {
				fault()
			}
		}
		return nil
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { recovered = recover() }()
		if strings.HasPrefix(route, "point-") {
			err = db.SetSync([]byte("uncertain-point"), []byte("pending"))
		} else {
			b := db.NewBatch()
			defer b.Close()
			if strings.HasPrefix(route, "range-") || strings.HasPrefix(route, "mixed-") {
				if e := b.DeleteRange([]byte("a"), []byte("z")); e != nil {
					err = e
					returned = true
					return
				}
			}
			if !strings.HasPrefix(route, "range-") {
				if e := b.Set([]byte("uncertain-batch"), []byte("pending")); e != nil {
					err = e
					returned = true
					return
				}
			}
			err = b.WriteSync()
		}
		returned = true
	}()
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("public write unwind stuck")
	}
	restore()
	unregister()
	if !fired || returned {
		t.Fatalf("fired=%v returned=%v err=%v calls=%d", fired, returned, err, calls)
	}
	if mode == "modern-nil" {
		if _, ok := recovered.(*runtime.PanicNilError); !ok {
			t.Fatalf("changed modern nil panic: %T %v", recovered, recovered)
		}
	} else if recovered != nil {
		t.Fatalf("changed legacy nil/Goexit: %T %v", recovered, recovered)
	}
	proof := strings.Join(stack, "\n")
	require := []string{"appendAndCaptureOrdinaryPublicCommandWAL"}
	if !strings.HasPrefix(route, "point-") {
		require = append(require, "WriteSync")
		if strings.HasPrefix(route, "batch-") {
			require = append(require, "appendRawKVBatchPayloadCommandWALPreparedCapture")
		}
	} else {
		require = append(require, "SetSync")
	}
	if strings.HasPrefix(route, "range-") {
		require = append(require, "writeRangeSpanBatch")
	}
	if strings.HasPrefix(route, "mixed-") {
		require = append(require, "writeRangeBatch", "writeRegularLocked")
	}
	if strings.HasPrefix(route, "batch-assigned-") {
		require = append(require, "captureCommandWALPrefixAfterAppend")
	}
	if route == "batch-assigned-hook" || route == "batch-assigned-drain" {
		require = append(require, "acquireCommandWALPublishWithBarriersAfterAppend")
	}
	if route == "point-flush" || route == "batch-flush" || route == "batch-assigned-flush" {
		require = append(require, "captureCommandWALPrefixLocked")
	}
	for _, name := range require {
		if !strings.Contains(proof, name) {
			t.Fatalf("missing live public-route frame %s:\n%s", name, proof)
		}
	}
	t.Logf("route=%s mode=%s barriers=%d live fault stack:\n%s", route, mode, calls, proof)
	if !db.commandWALPublicOperationGate.TryLock() {
		t.Fatal("public operation gate retained")
	}
	db.commandWALPublicOperationGate.Unlock()
	if !db.commandWALPublicPublishMu.TryLock() {
		t.Fatal("public publish gate retained")
	}
	db.commandWALPublicPublishMu.Unlock()
	if db.commandWALGroupCommit.durableIntents.Load() != 0 {
		t.Fatal("durable intent retained")
	}
	db.commandWALGroupCommit.mu.Lock()
	active, terminal := db.commandWALGroupCommit.leaderActive, db.commandWALGroupCommit.terminalErr
	db.commandWALGroupCommit.mu.Unlock()
	if active || !errors.Is(terminal, ErrRecoveryRequired) {
		t.Fatalf("active=%v terminal=%v", active, terminal)
	}
	// Public backend lock APIs test all three transferred leases, without
	// appending a synthetic backend command or bypassing the public writer.
	unlock, ok := db.backend.TryLockCommandWALPreparedPublish()
	if !ok {
		t.Fatal("backend teardown/admission retained")
	}
	rawDone := make(chan struct{})
	go func() { release := db.backend.LockCommandWALPreparedPublish(); release(); close(rawDone) }()
	select {
	case <-rawDone:
	case <-time.After(2 * time.Second):
		t.Fatal("backend raw publication retained")
	}
	unlock()
	if e := db.SetSync([]byte("later"), []byte("must-fail")); !errors.Is(e, ErrRecoveryRequired) {
		t.Fatalf("later public write=%v", e)
	}
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case e := <-closed:
		if e != nil && !errors.Is(e, ErrRecoveryRequired) {
			t.Fatalf("Close: %v", e)
		}
	case <-time.After(3 * time.Second):
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Fatalf("Close retained ownership; blocked goroutines:\n%s", buf[:n])
	}
	reopened, e := Open(opts)
	if e != nil {
		t.Fatalf("reopen: %v", e)
	}
	defer reopened.Close()
	got, e := reopened.Get(anchor)
	if e != nil || string(got) != string(value) {
		t.Fatalf("durable anchor=%q err=%v", got, e)
	}
}

// Return an actual append/flush error through the public writer, rather than
// failing validation before the callback. Child processes bound lock regressions.
func TestPublicCommandWALBatchAppendReturnedError(t *testing.T) {
	if route := os.Getenv("GOMAP_PUBLIC_BATCH_ERROR_CHILD"); route != "" {
		opts := commandWALDurabilityProofOptions(t.TempDir())
		opts.MemtableMode = "btree"
		db, err := Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		if db.cached.COWMode() {
			t.Fatal("ordinary fixture inactive")
		}
		b := db.NewBatch()
		defer b.Close()
		if route != "point" {
			if err := b.DeleteRange([]byte("a"), []byte("z")); err != nil {
				t.Fatal(err)
			}
		}
		if route != "range" {
			if err := b.Set([]byte("key"), []byte("value")); err != nil {
				t.Fatal(err)
			}
		}
		sentinel := errors.New("injected public append flush failure")
		fired := false
		restore := durabilitycut.Install(func(e durabilitycut.Event) error {
			if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeUserspaceFlush {
				fired = true
				return sentinel
			}
			return nil
		})
		err = b.WriteSync()
		restore()
		if !fired || !errors.Is(err, sentinel) {
			t.Fatalf("fired=%v error=%v; lost sentinel", fired, err)
		}
		if err := db.SetSync([]byte("later"), []byte("no")); !errors.Is(err, ErrRecoveryRequired) {
			t.Fatalf("later write=%v", err)
		}
		done := make(chan error, 1)
		go func() { done <- db.Close() }()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, ErrRecoveryRequired) && !errors.Is(err, sentinel) {
				t.Fatalf("Close=%v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("returned append error retained cache custody")
		}
		return
	}
	for _, route := range []string{"point", "range", "mixed"} {
		t.Run(route, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPublicCommandWALBatchAppendReturnedError$", "-test.timeout=12s", "-test.v")
			cmd.Env = append(os.Environ(), "GOMAP_PUBLIC_BATCH_ERROR_CHILD="+route)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
		})
	}
}
