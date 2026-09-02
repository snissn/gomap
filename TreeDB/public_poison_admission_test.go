package treedb_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestPublicCachedWriterWaitingBehindPostMetaFailureRejectsAfterPoison(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.DisableSideStores = true
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	if err := database.SetSync([]byte("stable/old"), []byte("old-value")); err != nil {
		t.Fatal(err)
	}
	if err := database.Set([]byte("dirty/new"), []byte("new-value")); err != nil {
		t.Fatal(err)
	}

	cutErr := errors.New("public admission race: stop after meta dirty")
	cutReached := make(chan struct{})
	releaseCut := make(chan struct{})
	var cutOnce sync.Once
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point != durabilitycut.AfterMetaWrite {
			return nil
		}
		cutOnce.Do(func() {
			close(cutReached)
			<-releaseCut
		})
		return cutErr
	})
	defer restore()

	checkpointErr := make(chan error, 1)
	go func() { checkpointErr <- database.Checkpoint() }()
	select {
	case <-cutReached:
	case <-time.After(2 * time.Second):
		close(releaseCut)
		t.Fatal("checkpoint did not reach post-meta cut")
	}

	writeErr := make(chan error, 1)
	go func() { writeErr <- database.Set([]byte("after/preflight"), []byte("must-reopen")) }()
	deadline := time.Now().Add(2 * time.Second)
	for database.Stats()["treedb.cache.write.wait_for_checkpoint.active"] != "1" {
		if time.Now().After(deadline) {
			close(releaseCut)
			t.Fatal("writer did not wait behind checkpoint after public preflight")
		}
		time.Sleep(time.Millisecond)
	}
	close(releaseCut)

	if err := <-checkpointErr; !errors.Is(err, cutErr) || !errors.Is(err, treedb.ErrRecoveryRequired) {
		t.Fatalf("post-meta checkpoint error=%v, want injected cut and ErrRecoveryRequired", err)
	}
	if err := <-writeErr; !errors.Is(err, treedb.ErrRecoveryRequired) {
		t.Fatalf("writer admitted after waiting behind poison error=%v, want ErrRecoveryRequired", err)
	}
}

func TestPublicCachedHandleRejectsMutationAfterPostMetaFailure(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.DisableSideStores = true
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	if err := database.SetSync([]byte("stable/old"), []byte("old-value")); err != nil {
		t.Fatal(err)
	}
	if err := database.Set([]byte("dirty/new"), []byte("new-value")); err != nil {
		t.Fatal(err)
	}

	cutErr := errors.New("public admission: stop after meta dirty")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point == durabilitycut.AfterMetaWrite {
			return cutErr
		}
		return nil
	})
	err = database.Checkpoint()
	restore()
	if !errors.Is(err, cutErr) || !errors.Is(err, treedb.ErrRecoveryRequired) {
		t.Fatalf("post-meta checkpoint error=%v, want injected cut and ErrRecoveryRequired", err)
	}
	if err := database.Set([]byte("after/poison"), []byte("must-reopen")); !errors.Is(err, treedb.ErrRecoveryRequired) {
		t.Fatalf("public Set after post-meta failure error=%v, want ErrRecoveryRequired", err)
	}
	if batch := database.NewBatch(); batch != nil {
		_ = batch.Close()
		t.Fatal("public NewBatch after post-meta failure returned a writable batch")
	}
	if _, err := database.CompactStorage(context.Background(), treedb.CompactStorageOptions{}); !errors.Is(err, treedb.ErrRecoveryRequired) {
		t.Fatalf("public CompactStorage after post-meta failure error=%v, want ErrRecoveryRequired", err)
	}
}

func TestPublicCachedHandleRetainsAdmissionAfterPreMetaFailure(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.DisableSideStores = true
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	if err := database.SetSync([]byte("stable/old"), []byte("old-value")); err != nil {
		t.Fatal(err)
	}
	if err := database.Set([]byte("dirty/new"), []byte("new-value")); err != nil {
		t.Fatal(err)
	}

	cutErr := errors.New("public admission: stop before meta write")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point == durabilitycut.BeforeMetaWrite {
			return cutErr
		}
		return nil
	})
	err = database.Checkpoint()
	restore()
	if !errors.Is(err, cutErr) || errors.Is(err, treedb.ErrRecoveryRequired) {
		t.Fatalf("pre-meta checkpoint error=%v, want injected cut without ErrRecoveryRequired", err)
	}
	if err := database.Set([]byte("after/retryable"), []byte("accepted")); err != nil {
		t.Fatalf("public Set after pre-meta failure: %v", err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatalf("retry checkpoint after pre-meta failure: %v", err)
	}
}
