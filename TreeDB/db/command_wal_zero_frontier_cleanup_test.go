package db

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestCommandWALCleanupEmptyFixedAtZeroRootFrontier(t *testing.T) {
	dir := t.TempDir()
	key, value := []byte("replay-from-initial-root"), []byte("retained-value")
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: key, Value: value}})
	const emptyCapacity = int64(4_199_424)
	empty := writeEmptyFixedCleanupSegment(t, dir, 2, emptyCapacity)
	database := &DB{dir: dir, commandWAL: true, durability: DurabilityDurable}
	// Both roots remain independently recovery-selectable. The selected root
	// covers LSN1, but the initial fallback still needs the complete replay frame.
	installCommandWALCleanupTwoRootRuntimeForTest(database, 1, 1)
	fallback := database.durableRoot.slot ^ 1
	record := database.durableRoot.slotRecord[fallback]
	record.AppliedCommandLSN = 0
	database.durableRoot.slotRecord[fallback] = record
	installCommandWALCleanupJournalOwnerForTest(t, database, 1)
	proof, err := database.captureDurableWALCleanupProofV1()
	if err != nil {
		t.Fatal(err)
	}
	if proof.rootCount != 2 || proof.cleanupThrough != 0 || proof.durableWALLSN != 1 {
		t.Fatalf("fixture lost zero-frontier proof: %+v", proof)
	}
	active := database.commandJournal.WriterBufferStats().ActivePath
	replay := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1))
	if active == empty || active == replay {
		t.Fatal("fixture did not create fresh active successor")
	}
	syncs := 0
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point == durabilitycut.AfterDeletionDirectorySync && event.Resource == durabilitycut.ResourceCommandWAL {
			syncs++
		}
		return nil
	})
	err = database.CleanupCommandWALCoveredSegments(true)
	restore()
	if err != nil {
		t.Fatal(err)
	}
	// Cleanup may not delete either the current empty active file or the
	// nonempty replay required by the older root, even though the newer root
	// already covers that frame. Verify its exact payload after reader reopen.
	if _, err := os.Stat(active); err != nil {
		t.Fatalf("active successor removed: %v", err)
	}
	r, err := commitlog.NewReader(replay)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	frame, err := r.ReadCommandFrame()
	if err != nil || frame.LSN != 1 {
		t.Fatalf("retained replay lsn=%d err=%v", frame.LSN, err)
	}
	ops, err := commitlog.DecodeRawKVBatchPayload(frame.Payload)
	if err != nil || len(ops) != 1 || !bytes.Equal(ops[0].Key, key) || !bytes.Equal(ops[0].Value, value) {
		t.Fatalf("retained replay payload changed: ops=%d err=%v", len(ops), err)
	}
	if _, err := r.ReadCommandFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("retained replay EOF: %v", err)
	}
	if _, err := os.Stat(empty); !os.IsNotExist(err) {
		t.Fatalf("validated closed empty fixed WAL retained at zero root frontier: %v", err)
	}
	if syncs != 1 || database.commandWALCleanupRemoved.Load() != 1 || database.commandWALCleanupBytes.Load() != uint64(emptyCapacity) {
		t.Fatalf("empty deletion lost namespace sync/physical accounting: syncs=%d removed=%d bytes=%d", syncs, database.commandWALCleanupRemoved.Load(), database.commandWALCleanupBytes.Load())
	}
}
