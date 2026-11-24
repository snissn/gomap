package treedb

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"testing"
)

func TestWALOnFastCheckpointsAvoidCandidateScan(t *testing.T) {
	dir := t.TempDir()
	opts := OptionsFor(ProfileWALOnFast, dir)
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open(ProfileWALOnFast): %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = d.Close()
		}
	}()

	writeBatch := func(round, count int) {
		t.Helper()
		b := d.NewBatch()
		for i := 0; i < count; i++ {
			if err := b.Set([]byte(fmt.Sprintf("k/%05d", i)), bytes.Repeat([]byte{byte(round)}, 128)); err != nil {
				_ = b.Close()
				t.Fatal(err)
			}
		}
		if err := b.Write(); err != nil {
			_ = b.Close()
			t.Fatal(err)
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// Bootstrap initial data and checkpoint once.
	writeBatch(1, 2000)
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("bootstrap Checkpoint: %v", err)
	}

	var scans atomic.Int64
	restore := d.backend.SetScanCandidateExternalReferencesHookForTesting(func() {
		scans.Add(1)
	})
	defer restore()

	// Interleave point writes, batch overwrites, batch deletes, point deletes,
	// and Checkpoints (the exact sequence run by unified-bench wal_on_fast).
	for round := 2; round <= 4; round++ {
		for i := 0; i < 64; i++ {
			if err := d.Set([]byte(fmt.Sprintf("p/%05d", i)), bytes.Repeat([]byte{byte(round)}, 128)); err != nil {
				t.Fatalf("round %d Set: %v", round, err)
			}
		}
		if err := d.Checkpoint(); err != nil {
			t.Fatalf("round %d post-point Checkpoint: %v", round, err)
		}

		writeBatch(round, 2000)
		if err := d.Checkpoint(); err != nil {
			t.Fatalf("round %d post-batch Checkpoint: %v", round, err)
		}

		delBatch := d.NewBatch()
		for i := 0; i < 200; i++ {
			if err := delBatch.Delete([]byte(fmt.Sprintf("k/%05d", i))); err != nil {
				_ = delBatch.Close()
				t.Fatal(err)
			}
		}
		if err := delBatch.Write(); err != nil {
			_ = delBatch.Close()
			t.Fatal(err)
		}
		if err := delBatch.Close(); err != nil {
			t.Fatal(err)
		}
		if err := d.Checkpoint(); err != nil {
			t.Fatalf("round %d post-delete Checkpoint: %v", round, err)
		}

		for i := 0; i < 16; i++ {
			if err := d.Delete([]byte(fmt.Sprintf("p/%05d", i))); err != nil {
				t.Fatalf("round %d Delete: %v", round, err)
			}
		}
		if err := d.Checkpoint(); err != nil {
			t.Fatalf("round %d post-point-delete Checkpoint: %v", round, err)
		}
	}

	if got := scans.Load(); got != 0 {
		t.Fatalf("wal_on_fast overwrite/delete/checkpoint rounds performed %d full candidate scans, want 0", got)
	}
	restore()

	checkData := func(db *DB, label string) {
		t.Helper()
		for i := 0; i < 200; i++ {
			got, err := db.Get([]byte(fmt.Sprintf("k/%05d", i)))
			if err != nil || got != nil {
				t.Fatalf("%s deleted k/%05d: got=%x err=%v", label, i, got, err)
			}
		}
		want := bytes.Repeat([]byte{4}, 128)
		for i := 200; i < 2000; i++ {
			got, err := db.Get([]byte(fmt.Sprintf("k/%05d", i)))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("%s live k/%05d: got len=%d err=%v", label, i, len(got), err)
			}
		}
		for i := 0; i < 16; i++ {
			got, err := db.Get([]byte(fmt.Sprintf("p/%05d", i)))
			if err != nil || got != nil {
				t.Fatalf("%s deleted p/%05d: got=%x err=%v", label, i, got, err)
			}
		}
		for i := 16; i < 64; i++ {
			got, err := db.Get([]byte(fmt.Sprintf("p/%05d", i)))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("%s live p/%05d: got len=%d err=%v", label, i, len(got), err)
			}
		}
	}

	checkData(d, "pre-close")
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closed = true

	reopen, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen(ProfileWALOnFast): %v", err)
	}
	defer func() {
		if err := reopen.Close(); err != nil {
			t.Fatalf("reopen Close: %v", err)
		}
	}()
	checkData(reopen, "reopen")
}
