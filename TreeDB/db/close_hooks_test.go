package db

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunCloseHooksAllowsNestedRunCloseHooks(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	var calls atomic.Int32
	d.RegisterCloseHook(func() error {
		calls.Add(1)
		return d.RunCloseHooks()
	})
	done := make(chan error, 1)
	go func() { done <- d.RunCloseHooks() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunCloseHooks: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("nested RunCloseHooks deadlocked")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("close hook calls=%d, want 1", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestCloseHookMayCallClose(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	var calls atomic.Int32
	d.RegisterCloseHook(func() error {
		calls.Add(1)
		return d.Close()
	})
	done := make(chan error, 1)
	go func() { done <- d.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close hook calling Close deadlocked")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("close hook calls=%d, want 1", got)
	}
}

func TestCloseHookMayCallCloseThroughDeepHelpers(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	d.RegisterCloseHook(func() error {
		return closeThroughHelpers(d, 64)
	})
	done := make(chan error, 1)
	go func() { done <- d.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close hook calling Close through deep helpers deadlocked")
	}
}

func TestStandaloneRunCloseHooksCompletesReentrantClose(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	d.RegisterCloseHook(func() error {
		return d.Close()
	})
	done := make(chan error, 1)
	go func() { done <- d.RunCloseHooks() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunCloseHooks: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("standalone close-hook drain deadlocked")
	}
	if !d.IsClosing() {
		t.Fatal("reentrant Close returned without completing DB teardown")
	}
	if err := d.Set([]byte("after-close"), []byte("value")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Set after reentrant Close error=%v, want %v", err, ErrClosed)
	}
}

func TestConcurrentRunCloseHooksWaitsForPendingCloseDrain(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	closeRequested := make(chan struct{})
	releaseHook := make(chan struct{})
	released := false
	release := func() {
		if !released {
			close(releaseHook)
			released = true
		}
	}
	defer release()
	d.RegisterCloseHook(func() error {
		if err := d.Close(); err != nil {
			return err
		}
		close(closeRequested)
		<-releaseHook
		return nil
	})
	d.RegisterCloseHook(func() error {
		return d.Set([]byte("later-hook"), []byte("still-open"))
	})

	ownerDone := make(chan error, 1)
	go func() { ownerDone <- d.RunCloseHooks() }()
	<-closeRequested

	waiting := make(chan struct{})
	d.closeHooksMu.Lock()
	d.closeHooksWaitHook = func() { close(waiting) }
	d.closeHooksMu.Unlock()
	otherDone := make(chan error, 1)
	go func() { otherDone <- d.RunCloseHooks() }()
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("concurrent RunCloseHooks did not wait for the active drain")
	}
	if d.IsClosing() {
		t.Fatal("concurrent RunCloseHooks began teardown before callbacks completed")
	}
	select {
	case err := <-otherDone:
		t.Fatalf("concurrent RunCloseHooks returned before callbacks completed: %v", err)
	default:
	}

	release()
	if err := <-ownerDone; err != nil {
		t.Fatalf("owner RunCloseHooks: %v", err)
	}
	if err := <-otherDone; err != nil {
		t.Fatalf("concurrent RunCloseHooks: %v", err)
	}
	if !d.IsClosing() {
		t.Fatal("pending Close was not completed after callbacks returned")
	}
}

// Recursive calls cannot be inlined, keeping the regression independent of
// ordinary helper inlining decisions.
func closeThroughHelpers(d *DB, depth int) error {
	if depth == 0 {
		return d.Close()
	}
	return closeThroughHelpers(d, depth-1)
}

func TestCloseWaitsForStandaloneCloseHookDrain(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	hookStarted := make(chan struct{})
	releaseHook := make(chan struct{})
	d.RegisterCloseHook(func() error {
		close(hookStarted)
		<-releaseHook
		return nil
	})

	hookDone := make(chan error, 1)
	go func() { hookDone <- d.RunCloseHooks() }()
	<-hookStarted

	closeWaiting := make(chan struct{})
	d.closeHooksMu.Lock()
	d.closeHooksWaitHook = func() { close(closeWaiting) }
	d.closeHooksMu.Unlock()

	closeDone := make(chan error, 1)
	go func() { closeDone <- d.Close() }()
	<-closeWaiting
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before standalone hook drain completed: %v", err)
	default:
	}

	close(releaseHook)
	if err := <-hookDone; err != nil {
		t.Fatalf("RunCloseHooks: %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !d.IsClosing() {
		t.Fatal("Close returned without marking the DB closing")
	}
}

func TestCloseHookOnlineVacuumObservesClosedAdmission(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := d.Set([]byte("hook/key"), []byte("value")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	d.RegisterCloseHook(func() error {
		err := d.VacuumIndexOnline(context.Background())
		if !errors.Is(err, ErrClosed) {
			return err
		}
		return nil
	})
	done := make(chan error, 1)
	go func() { done <- d.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close hook maintenance call deadlocked")
	}
}

func TestRegisterCloseHookAfterRunCloseHooksIsIgnored(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	ran := 0
	unregister := d.RegisterCloseHook(func() error {
		ran++
		return nil
	})
	if err := d.RunCloseHooks(); err != nil {
		t.Fatalf("RunCloseHooks: %v", err)
	}
	if ran != 1 {
		t.Fatalf("close hook ran %d times, want 1", ran)
	}

	lateRan := false
	lateUnregister := d.RegisterCloseHook(func() error {
		lateRan = true
		return nil
	})
	lateUnregister()
	unregister()
	if err := d.RunCloseHooks(); err != nil {
		t.Fatalf("second RunCloseHooks: %v", err)
	}
	if lateRan {
		t.Fatalf("late close hook ran after hook drain started")
	}
	if ran != 1 {
		t.Fatalf("close hook ran %d times after second drain, want 1", ran)
	}
}

func TestRegisterCloseHookBeforeRunsBeforeOrdinaryHooks(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	var order []string
	d.RegisterCloseHook(func() error {
		order = append(order, "ordinary")
		return nil
	})
	d.RegisterCloseHookBefore(func() error {
		order = append(order, "before")
		return nil
	})
	if err := d.RunCloseHooks(); err != nil {
		t.Fatalf("RunCloseHooks: %v", err)
	}
	if len(order) != 2 || order[0] != "before" || order[1] != "ordinary" {
		t.Fatalf("close hook order=%v, want [before ordinary]", order)
	}
}

func TestRegisterCloseHookIfOpenAfterSetupRunsUnderAcceptedRegistration(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	published := false
	hookRan := false
	if _, ok := d.RegisterCloseHookIfOpenAfter(func() bool {
		published = true
		return true
	}, func() error {
		if !published {
			t.Fatalf("close hook ran before setup published state")
		}
		hookRan = true
		return nil
	}); !ok {
		t.Fatalf("RegisterCloseHookIfOpenAfter rejected open DB")
	}
	if !published {
		t.Fatalf("setup did not run for accepted close hook registration")
	}
	if err := d.RunCloseHooks(); err != nil {
		t.Fatalf("RunCloseHooks: %v", err)
	}
	if !hookRan {
		t.Fatalf("close hook did not run")
	}

	lateSetupRan := false
	if _, ok := d.RegisterCloseHookIfOpenAfter(func() bool {
		lateSetupRan = true
		return true
	}, func() error {
		t.Fatalf("late close hook ran after hook drain")
		return nil
	}); ok {
		t.Fatalf("RegisterCloseHookIfOpenAfter accepted drained DB")
	}
	if lateSetupRan {
		t.Fatalf("setup ran after close hooks drained")
	}
}

func TestRegisterCloseHookIfOpenAfterSetupCanDeclineHook(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	setupRan := false
	hookRan := false
	unregister, ok := d.RegisterCloseHookIfOpenAfter(func() bool {
		setupRan = true
		return false
	}, func() error {
		hookRan = true
		return nil
	})
	if !ok {
		t.Fatalf("RegisterCloseHookIfOpenAfter rejected open DB")
	}
	unregister()
	if !setupRan {
		t.Fatalf("setup did not run")
	}
	if err := d.RunCloseHooks(); err != nil {
		t.Fatalf("RunCloseHooks: %v", err)
	}
	if hookRan {
		t.Fatalf("close hook ran after setup declined registration")
	}
}
