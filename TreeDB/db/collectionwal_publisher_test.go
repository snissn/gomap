package db

import (
	"errors"
	"testing"
	"time"
)

func TestCollectionWALPublisherSerializesCallbacks(t *testing.T) {
	var db DB
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- db.RunCollectionWALPublisher(func() error {
			close(firstEntered)
			<-releaseFirst
			return nil
		})
	}()
	select {
	case <-firstEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("first publisher callback did not enter")
	}

	secondEntered := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- db.RunCollectionWALPublisher(func() error {
			close(secondEntered)
			return nil
		})
	}()
	select {
	case <-secondEntered:
		t.Fatal("second publisher callback entered before first released")
	case <-time.After(25 * time.Millisecond):
	}

	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first publisher callback: %v", err)
	}
	select {
	case <-secondEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("second publisher callback did not enter after release")
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second publisher callback: %v", err)
	}
}

func TestCollectionWALCheckpointWaitsForPublisher(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	entered := make(chan struct{})
	release := make(chan struct{})
	publisherDone := make(chan error, 1)
	go func() {
		publisherDone <- db.RunCollectionWALPublisher(func() error {
			close(entered)
			<-release
			return nil
		})
	}()
	waitClosed(t, entered, "publisher entered")

	checkpointDone := make(chan error, 1)
	go func() {
		checkpointDone <- db.Checkpoint()
	}()
	assertStillBlocked(t, checkpointDone, "Checkpoint returned while collection WAL publisher was active")

	close(release)
	if err := waitErr(t, publisherDone, "publisher done"); err != nil {
		t.Fatalf("publisher: %v", err)
	}
	if err := waitErr(t, checkpointDone, "checkpoint done"); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
}

func TestCollectionWALCloseWaitsForPublisherAndRejectsFollower(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	publisherDone := make(chan error, 1)
	go func() {
		publisherDone <- db.RunCollectionWALPublisher(func() error {
			close(entered)
			<-release
			return nil
		})
	}()
	waitClosed(t, entered, "publisher entered")

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- db.Close()
	}()
	assertStillBlocked(t, closeDone, "Close returned while collection WAL publisher was active")

	close(release)
	if err := waitErr(t, publisherDone, "publisher done"); err != nil {
		t.Fatalf("publisher: %v", err)
	}
	if err := waitErr(t, closeDone, "close done"); err != nil {
		t.Fatalf("Close: %v", err)
	}
	followerRan := false
	err = db.RunCollectionWALPublisher(func() error {
		followerRan = true
		return nil
	})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("RunCollectionWALPublisher after Close err=%v want ErrClosed", err)
	}
	if followerRan {
		t.Fatal("publisher callback ran after close admission cut")
	}
}

func TestCollectionWALPublisherRejectsWhenRecoveryRequired(t *testing.T) {
	var db DB
	db.MarkCollectionWALRecoveryRequired()

	ran := false
	err := db.RunCollectionWALPublisher(func() error {
		ran = true
		return nil
	})
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("RunCollectionWALPublisher err=%v want ErrRecoveryRequired", err)
	}
	if ran {
		t.Fatal("publisher callback ran after collection WAL recovery-required state")
	}
}

func TestCollectionWALCheckpointRejectsWhenRecoveryRequired(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.MarkCollectionWALRecoveryRequired()

	if err := db.Checkpoint(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Checkpoint err=%v want ErrRecoveryRequired", err)
	}
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func waitErr(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

func assertStillBlocked[T any](t *testing.T, ch <-chan T, msg string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal(msg)
	case <-time.After(25 * time.Millisecond):
	}
}
