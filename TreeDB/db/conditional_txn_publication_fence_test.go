package db

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConditionalTxnWaitsForVisibleHistoryCompletion(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.SetSync([]byte("guard"), []byte("before")); err != nil {
		t.Fatal(err)
	}
	tx, err := d.NewConditionalTxn()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err := tx.Get([]byte("guard")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Set([]byte("target"), []byte("stale")); err != nil {
		t.Fatal(err)
	}
	visible, resume := make(chan struct{}), make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(resume) })
	var installs atomic.Int32
	d.testAfterDurableRootVisibleInstallHook = func() {
		if installs.Add(1) == 1 {
			close(visible)
			<-resume
		}
	}
	captured := make(chan struct{})
	var captures atomic.Int32
	d.testAfterOptimisticBaseCaptureHook = func() {
		if captures.Add(1) == 2 {
			close(captured)
		}
	}
	writer, commit := make(chan error, 1), make(chan error, 1)
	writerDone, commitDone := make(chan struct{}), make(chan struct{})
	commitStarted := false
	defer func() {
		release.Do(func() { close(resume) })
		<-writerDone
		if commitStarted {
			<-commitDone
		}
		d.testAfterDurableRootVisibleInstallHook = nil
		d.testAfterOptimisticBaseCaptureHook = nil
	}()
	go func() {
		defer close(writerDone)
		writer <- d.Set([]byte("guard"), []byte("after"))
	}()
	select {
	case <-visible:
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not reach visible install")
	}
	// The metadata is installed, but its original root-reuse write fence still
	// excludes both snapshots and optimistic base capture until history catches up.
	d.mu.RLock()
	sequence := d.meta.CommitSeq
	d.mu.RUnlock()
	if sequence <= tx.startCommitSeq {
		t.Fatal("fixture did not install a successor")
	}
	if d.rootReuseMu.TryRLock() {
		d.rootReuseMu.RUnlock()
		t.Fatal("visible root escaped the history-completion fence")
	}
	commitStarted = true
	go func() {
		defer close(commitDone)
		commit <- tx.Commit()
	}()
	select {
	case <-captured:
		t.Fatal("conditional writer captured visible root before history completion")
	case err := <-commit:
		t.Fatalf("conditional writer escaped incomplete history: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	release.Do(func() { close(resume) })
	for name, result := range map[string]<-chan error{"writer": writer, "conditional": commit} {
		select {
		case err := <-result:
			if name == "writer" && err != nil || name == "conditional" && !errors.Is(err, ErrConcurrentModification) {
				t.Fatalf("%s result=%v", name, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s did not finish", name)
		}
	}
	if got, err := d.Get([]byte("target")); err != nil || got != nil {
		t.Fatalf("stale write became visible: %q %v", got, err)
	}
}
