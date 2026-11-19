package commitlog

import (
	"errors"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func openClosedSegmentSyncJournal(t *testing.T) *CommandJournal {
	t.Helper()
	dir := t.TempDir()
	file, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ext4 := IsExt4WALFilesystem(file)
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if !ext4 {
		t.Skip("requires ext4 for the production stream selector")
	}
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{
		SegmentSeq: 1, SegmentTargetBytes: 1024,
		CaptureStableResources: true, PreferExt4Stream: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if journal.writer.fixed != nil || !journal.writer.containerSelection.ext4Stream {
		journal.Close()
		t.Fatal("missing ext4 stream")
	}
	env := CommandEnvelope{
		Version: CommandFrameVersionV2, DurabilityClass: CommandDurabilityRelaxed,
		Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
	}
	if lsn, err := journal.AppendCommand(env); lsn != 1 || err != nil {
		journal.Close()
		t.Fatalf("first append LSN=%d, err=%v", lsn, err)
	}
	return journal
}

func TestClosedSegmentSyncReceiptCoversOnlyRetainedMatchingFile(t *testing.T) {
	journal := openClosedSegmentSyncJournal(t)
	defer journal.Close()
	if err := journal.Flush(); err != nil {
		t.Fatal(err)
	}
	need := rootpublication.DurableFrontier{Bytes: uint64(journal.writer.ActiveBytes()), MaxLSN: 1}
	newToken := func(seq uint64, frontier rootpublication.DurableFrontier) *rootpublication.StableResourceToken {
		t.Helper()
		token, err := journal.stableSegmentToken(journal.writer.f, journal.path, seq, rootpublication.ReachabilityCommandWALActive, frontier, nil, false)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(token.Release)
		return token
	}
	target := newToken(1, need)
	otherSequence := newToken(2, need)
	beyondLSN := need
	beyondLSN.MaxLSN++
	otherLSN := newToken(1, beyondLSN)

	receipt, err := journal.RotateActiveSegmentWithClosedSyncReceipt()
	if err != nil || !receipt.Valid() {
		t.Fatalf("checked cutover receipt: valid=%t err=%v", receipt.Valid(), err)
	}
	if covered, err := receipt.CoversRetainedFile(journal, target); !covered || err != nil {
		t.Fatalf("terminal debt file not covered: covered=%t err=%v", covered, err)
	}
	active := newToken(2, rootpublication.DurableFrontier{})
	sameSequenceDifferentObject := newToken(1, rootpublication.DurableFrontier{})
	for name, token := range map[string]*rootpublication.StableResourceToken{
		"another-generation": otherSequence, "higher-lsn": otherLSN,
		"successor": active, "different-identity": sameSequenceDifferentObject, "no-lease": nil,
	} {
		if covered, err := receipt.CoversRetainedFile(journal, token); covered || err != nil {
			t.Errorf("%s: covered=%t err=%v", name, covered, err)
		}
	}
	short := receipt
	short.bytes--
	if covered, err := short.CoversRetainedFile(journal, target); covered || err != nil {
		t.Fatalf("required extent exceeds proof: covered=%t err=%v", covered, err)
	}
	failure := errors.New("later journal sync failed")
	journal.writer.syncFn = func(*os.File) error { return failure }
	if err := journal.Sync(); !errors.Is(err, failure) {
		t.Fatalf("later physical sync error=%v", err)
	}
	if covered, err := receipt.CoversRetainedFile(journal, target); !covered || !errors.Is(err, failure) {
		t.Fatalf("terminal file proof masked sticky failure: covered=%t err=%v", covered, err)
	}
	if err := journal.Close(); !errors.Is(err, failure) {
		t.Fatalf("checked Close lost the physical sync failure: %v", err)
	}
	reopen, err := OpenCommandJournal(journal.walDir, CommandJournalOptions{
		InitialLSN: 1, SegmentSeq: 2, SegmentTargetBytes: 1024,
		CaptureStableResources: true, PreferExt4Stream: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reopen.Close()
	if covered, err := receipt.CoversRetainedFile(reopen, target); covered || err != nil {
		t.Fatalf("old proof applied across reopen: covered=%t err=%v", covered, err)
	}
	target.Release()
	if covered, err := receipt.CoversRetainedFile(journal, target); !covered || !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("released lease: covered=%t err=%v", covered, err)
	}
}

func TestClosedSegmentSyncReceiptRejectsFailedCutover(t *testing.T) {
	for _, phase := range []string{"physical-sync", "after-file-sync", "successor-directory", "close-original"} {
		t.Run(phase, func(t *testing.T) {
			journal := openClosedSegmentSyncJournal(t)
			defer journal.Close()
			fail := errors.New("cutover " + phase)
			if phase == "physical-sync" {
				journal.writer.syncFn = func(*os.File) error { return fail }
			}
			if phase == "close-original" {
				journal.writer.closeRotateFn = func(file *os.File) error {
					return errors.Join(file.Close(), fail)
				}
			}
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Resource != durabilitycut.ResourceCommandWAL {
					return nil
				}
				if phase == "after-file-sync" && event.Point == durabilitycut.AfterDependencyFileSync {
					return fail
				}
				if phase == "successor-directory" && event.Point == durabilitycut.BeforeNewFileDirectorySync {
					return fail
				}
				return nil
			})
			receipt, err := journal.RotateActiveSegmentWithClosedSyncReceipt()
			restore()
			if !errors.Is(err, fail) || receipt.Valid() {
				t.Fatalf("failed cutover supplied proof: valid=%t err=%v", receipt.Valid(), err)
			}
			wantSeq := uint64(1)
			if phase == "close-original" {
				wantSeq++
			}
			if journal.segmentSeq != wantSeq {
				t.Fatalf("failed rotation seq=%d, want %d", journal.segmentSeq, wantSeq)
			}
			if err := journal.Close(); (phase == "physical-sync" && !errors.Is(err, fail)) || (phase != "physical-sync" && err != nil) {
				t.Fatalf("checked Close err=%v (injected %v)", err, fail)
			}
		})
	}
}
