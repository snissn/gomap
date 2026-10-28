package db

import (
	"context"
	"crypto/sha256"
	"os"
	"testing"
)

func TestInspectClosedCommandWALPreservesRetentionAndReopen(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err := database.InspectClosedCommandWALV1(); err == nil {
		t.Fatal("accepted open handle")
	}
	batch := database.NewBatch()
	if err := batch.Set([]byte("retained"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.rootPublication.coordinator.WaitThrough(context.Background(), database.State().CommitSeq); err != nil {
		t.Fatal(err)
	}
	if database.commandWALDurableLSN.Load() >= database.State().AppliedCommandLSN {
		t.Fatal("fixture did not create unavailable cleanup proof")
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	segments, err := listWALSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	before := make(map[string][32]byte)
	for _, segment := range segments {
		raw, err := os.ReadFile(segment.path)
		if err != nil {
			t.Fatal(err)
		}
		before[segment.path] = sha256.Sum256(raw)
	}
	out, err := database.InspectClosedCommandWALV1()
	if err != nil {
		t.Fatal(err)
	}
	if out.ProofError == "" || out.LastCleanupError == "" || len(out.Segments) == 0 || out.CleanupCounters["treedb.command_wal.cleanup.retries_total"] == "0" {
		t.Fatalf("lost conservative retention evidence: %+v", out)
	}
	if out.Roots[out.SelectedSlot].AppliedCommandLSN != database.State().AppliedCommandLSN {
		t.Fatalf("slot identity=%+v", out)
	}
	for path, digest := range before {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if sha256.Sum256(raw) != digest {
			t.Fatalf("diagnostics changed %s", path)
		}
	}
	reopened, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if value, err := reopened.Get([]byte("retained")); err != nil || string(value) != "value" {
		t.Fatalf("reopen value=%q error=%v", value, err)
	}
}
