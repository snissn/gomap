package db

import (
	"crypto/sha256"
	"encoding/json"
	"os"
	"testing"
)

// A single real durable append leaves the initial recovery-selectable root at
// LSN zero. Empty predecessors contain no commands needed by that fallback.
func TestCommandWALZeroFrontierRetiresEmptyReopenSegments(t *testing.T) {
	dir := t.TempDir()
	options := Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true}
	database, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.SetSync([]byte("retained"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	source, err := zeroFrontierSnapshotForTest(database)
	if err != nil || source.ProofError != "" || source.CleanupThrough != 0 || source.DurableWALLSN != 1 {
		t.Fatalf("source is not a valid zero-frontier fixture: %+v; %v", source, err)
	}
	if raw, err := json.Marshal(source); err == nil {
		t.Logf("source: %s", raw)
	}
	if len(source.Segments) != 2 {
		t.Fatalf("source segments=%+v, want framed predecessor and active empty", source.Segments)
	}
	var framedPath, emptyPath string
	for _, segment := range source.Segments {
		if segment.Frames == 1 && segment.MaxLSN == 1 {
			framedPath = segment.Path
		}
		if segment.Frames == 0 && segment.Active {
			emptyPath = segment.Path
		}
	}
	if framedPath == "" || emptyPath == "" {
		t.Fatalf("unexpected source layout: %+v", source.Segments)
	}
	raw, err := os.ReadFile(framedPath)
	if err != nil {
		t.Fatal(err)
	}
	framedDigest := sha256.Sum256(raw)
	for cycle := 0; cycle < 3; cycle++ {
		database, err = Open(options)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := database.Get([]byte("retained")); err != nil || string(got) != "value" {
			t.Fatalf("cycle %d reopen value=%q: %v", cycle, got, err)
		}
		if err := database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		final, err := zeroFrontierSnapshotForTest(database)
		if err != nil || final.ProofError != "" || final.CleanupThrough != 0 {
			t.Fatalf("cycle %d proof=%+v: %v", cycle, final, err)
		}
		if raw, err := json.Marshal(final); err == nil {
			t.Logf("cycle %d final: %s", cycle, raw)
		}
		if _, err := os.Stat(emptyPath); !os.IsNotExist(err) {
			t.Fatalf("cycle %d empty predecessor retained with valid zero-frontier proof: %s; %v", cycle, emptyPath, err)
		}
		if len(final.Segments) != len(source.Segments) {
			t.Fatalf("cycle %d WAL grew: source=%+v final=%+v", cycle, source.Segments, final.Segments)
		}
		raw, err := os.ReadFile(framedPath)
		if err != nil || sha256.Sum256(raw) != framedDigest {
			t.Fatalf("cycle %d replay predecessor changed: %v", cycle, err)
		}
		for _, segment := range final.Segments {
			if segment.Active && segment.Frames == 0 {
				emptyPath = segment.Path
			}
		}
	}
}

// Snapshot reads the closed backend and the existing read-only cleanup scanner;
// it needs no production diagnostic API or altered cleanup authority.
type zeroFrontierSnapshot struct {
	Roots          [2]uint64
	DurableWALLSN  uint64
	CleanupThrough uint64
	ProofError     string
	Segments       []commandWALSegmentCleanupDecision
}

func zeroFrontierSnapshotForTest(database *DB) (zeroFrontierSnapshot, error) {
	out := zeroFrontierSnapshot{DurableWALLSN: database.commandWALDurableLSN.Load()}
	for i := range out.Roots {
		out.Roots[i] = database.durableRoot.slotRecord[i].AppliedCommandLSN
	}
	proof, err := captureDurableWALCleanupProofFromRuntimeV1(database.durableRoot, out.DurableWALLSN)
	if err != nil {
		out.ProofError = err.Error()
		return out, err
	}
	out.CleanupThrough = proof.cleanupThrough
	out.Segments, err = scanCommandWALSegmentsForCleanupProof(database.dir, proof.cleanupThrough, proof.durableWALLSN, database.walMaxSegmentBytes)
	closeCommandWALCleanupDecisions(out.Segments)
	return out, err
}
