package treedb

import (
	"bytes"
	"testing"
)

func TestAcquireSnapshot_SeesJustWrittenBatch(t *testing.T) {
	opts := OptionsFor(ProfileBench, t.TempDir())
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	key := []byte("k")
	want := []byte("v")

	b := db.NewBatch()
	if b == nil {
		t.Fatal("NewBatch returned nil")
	}
	if err := b.Set(key, want); err != nil {
		t.Fatalf("batch.Set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("batch.Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch.Close: %v", err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer snap.Close()

	got, err := snap.GetUnsafe(key)
	if err != nil {
		t.Fatalf("snapshot.GetUnsafe: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("snapshot mismatch: got=%q want=%q", got, want)
	}
}

func TestAcquireSnapshot_SeesJustWrittenBatch_AfterCheckpoint(t *testing.T) {
	opts := OptionsFor(ProfileBench, t.TempDir())
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	key := []byte("k")
	want := []byte("v")

	b := db.NewBatch()
	if b == nil {
		t.Fatal("NewBatch returned nil")
	}
	if err := b.Set(key, want); err != nil {
		t.Fatalf("batch.Set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("batch.Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch.Close: %v", err)
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer snap.Close()

	got, err := snap.GetUnsafe(key)
	if err != nil {
		t.Fatalf("snapshot.GetUnsafe: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("snapshot mismatch: got=%q want=%q", got, want)
	}
}

