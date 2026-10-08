package treedb

import (
	"errors"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"testing"
	"time"
)

func TestPublicCloseDropsLifecycleGateAndRefusesReentrantCloseRead(t *testing.T) {
	// A real backend hook runs inside the public physical child Close, after
	// facade admission closes. The callback can probe and reenter the facade.
	backend, err := backenddb.Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	database := &DB{backend: backend}
	defer database.Close()
	if err := database.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	backend.RegisterCloseHook(func() error {
		if !database.lifecycleMu.TryLock() {
			return errors.New("physical Close retained facade gate")
		}
		database.lifecycleMu.Unlock()
		if err := database.Close(); !errors.Is(err, ErrClosed) {
			return errors.New("reentrant Close did not refuse")
		}
		close(entered)
		<-release
		return nil
	})
	done := make(chan error, 1)
	go func() { done <- database.Close() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("physical callback self-joined")
	}
	if _, err := database.Get([]byte("key")); !errors.Is(err, ErrClosed) {
		close(release)
		t.Fatalf("read admitted during terminal: %v", err)
	}
	if err := database.Close(); !errors.Is(err, ErrClosed) {
		close(release)
		t.Fatalf("concurrent terminal admitted: %v", err)
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close failed to finish")
	}
	if database.backend != nil {
		t.Fatal("completed backend not discharged")
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPublicPendingConstructorRetainsActualChildOnCloseError(t *testing.T) {
	backend, err := backenddb.Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	fault := errors.New("actual child cleanup error")
	hookCalls := 0
	backend.RegisterCloseHook(func() error { hookCalls++; return fault })
	pending, err := failedPublicConstructor(nil, backend, nil, nil, fault)
	if pending == nil || pending.backend != backend || !errors.Is(err, fault) {
		t.Fatalf("pending actual child lost: %p %v", pending, err)
	}
	defer pending.Close()
	if hookCalls != 1 {
		t.Fatalf("constructor cleanup hook calls=%d want 1", hookCalls)
	}
	if err := pending.Close(); err != nil || pending.backend != nil {
		t.Fatalf("completed retry retained child: %v", err)
	}
	if err := pending.Close(); err != nil {
		t.Fatalf("completed Close is not idempotent: %v", err)
	}
	if hookCalls != 1 {
		t.Fatalf("consumed cleanup hook replayed: calls=%d", hookCalls)
	}
}
