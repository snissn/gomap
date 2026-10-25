package db

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// A legal physical-pressure rotation can close a positively empty fixed file
// before assigning LSN1. Debt must witness the retained exact files, including
// this predecessor: pathname-only cuts cannot capture its persistence.
func TestCommandWALDebtEmptyFixedPressureRotationRetainsSyncHandle(t *testing.T) {
	if !commitlog.FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	root := t.TempDir()
	walDir := WALDirPath(root)
	if err := os.MkdirAll(walDir, 0700); err != nil {
		t.Fatal(err)
	}
	emptyPath := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	seed, err := commitlog.NewFixedWriterWithCapacity(emptyPath, 4352)
	if err != nil {
		t.Fatal(err)
	}
	if err = seed.Close(); err != nil {
		t.Fatal(err)
	}
	emptyReader, err := commitlog.NewReaderWithOptions(emptyPath, commitlog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !emptyReader.IsEmptyFixedContainer() {
		t.Fatal("seed is not a positively validated empty fixed container")
	}
	if err = emptyReader.Close(); err != nil {
		t.Fatal(err)
	}
	original, err := os.Stat(emptyPath)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := commitlog.OpenCommandJournal(walDir, commitlog.CommandJournalOptions{SegmentSeq: 1, SegmentTargetBytes: 1 << 20, CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("pressure"), Value: bytes.Repeat([]byte("v"), 512)}})
	if err != nil {
		t.Fatal(err)
	}
	lsn, err := journal.AppendCommandObserved(commitlog.CommandEnvelope{Version: commitlog.CommandFrameVersionV2, DurabilityClass: commitlog.CommandDurabilityRelaxed, Kind: commitlog.CommandKindRawKVBatch, Scope: commitlog.CommandScopeRawKV, PayloadFormat: commitlog.PayloadFormatRawKVBatchV1, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	active, _ := journal.ActiveSegmentSnapshot()
	if lsn != 1 || active == emptyPath {
		t.Fatalf("first-input pressure rotation lsn=%d active=%s", lsn, active)
	}
	rotations, err := journal.TakePendingStableRotations()
	if err != nil {
		t.Fatal(err)
	}
	if len(rotations) != 1 {
		for _, r := range rotations {
			r.Release()
		}
		t.Fatalf("rotations=%d want1", len(rotations))
	}
	var debt CommandWALDependencyDebt
	if err = debt.add(lsn, commandWALStableRotationTokens(rotations)); err != nil {
		t.Fatal(err)
	}
	defer debt.releaseThrough(lsn)
	counts := map[string][2]int{}
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource != durabilitycut.ResourceCommandWAL {
			return nil
		}
		index := 0
		switch event.Point {
		case durabilitycut.BeforeDependencyFileSync:
		case durabilitycut.AfterDependencyFileSync:
			index = 1
		default:
			return nil
		}
		if event.StablePath == nil {
			t.Fatalf("physical sync %s has no retained handle", event.Path)
		}
		linked, e := os.Stat(event.Path)
		if e != nil {
			return e
		}
		retained, e := event.StablePath.Stat()
		if e != nil {
			return e
		}
		if !os.SameFile(linked, retained) {
			t.Fatal("sync handle does not match actual segment")
		}
		if event.Path == emptyPath && !os.SameFile(original, retained) {
			t.Fatal("empty predecessor identity changed")
		}
		v := counts[event.Path]
		v[index]++
		counts[event.Path] = v
		return nil
	})
	defer restore()
	if err = debt.syncRotationFilesThrough(root, walDir, lsn); err != nil {
		t.Fatal(err)
	}
	if len(counts) != 2 || counts[emptyPath] != [2]int{1, 1} || counts[active] != [2]int{1, 1} {
		t.Fatalf("actual physical event pairs=%v", counts)
	}

	// The same real rotation keeps the empty predecessor pinned in post-cut
	// dependency debt. Newly positive empty-file cleanup must refuse that exact
	// identity, rather than silently deleting it merely because MaxLSN is zero.
	decisions, err := scanCommandWALSegmentsForCleanupProof(root, lsn, lsn, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer closeCommandWALCleanupDecisions(decisions)
	emptyDecision := emptyFixedCleanupDecision(t, decisions, emptyPath)
	if !emptyDecision.Covered || emptyDecision.Active || emptyDecision.Frames != 0 {
		t.Fatalf("empty pressure predecessor=%+v", emptyDecision)
	}
	tokens, err := debt.rotationFileViewThrough(lsn)
	if err != nil {
		t.Fatal(err)
	}
	pinned := false
	for _, token := range tokens {
		if rootpublication.SamePhysicalIdentity(token.Identity(), emptyDecision.identity) {
			pinned = true
		}
	}
	if !pinned {
		t.Fatal("actual empty predecessor identity is not retained by assigned debt")
	}
	snapshot, err := journal.CaptureCleanupSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	err = journal.WithCleanupSnapshot(snapshot, func(registry *rootpublication.IdentityPinRegistry, _ commitlog.CommandJournalCleanupSnapshot) (bool, error) {
		_, err := removeCoveredCommandWALSegmentsWithRegistry(decisions, registry)
		return false, err
	})
	if !errors.Is(err, rootpublication.ErrResourcePinned) {
		t.Fatalf("pinned empty pressure predecessor cleanup=%v", err)
	}
	if _, err = os.Stat(emptyPath); err != nil {
		t.Fatalf("pinned predecessor removed: %v", err)
	}
	t.Logf("actual empty pressure predecessor retained: path=%s identity=%+v debtLSN=%d cleanup=%v", emptyPath, emptyDecision.identity, lsn, rootpublication.ErrResourcePinned)
}
