package commitlog

import (
	"testing"
	"time"
)

func TestCapturedSyncPinsPrefixAcrossAppendRotationAndClose(t *testing.T) {
	dir := t.TempDir()
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	appendFrame := func() uint64 {
		t.Helper()
		lsn, err := journal.AppendCommand(CommandEnvelope{Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1})
		if err != nil {
			t.Fatal(err)
		}
		return lsn
	}
	first := appendFrame()
	target, err := journal.CaptureSync(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Release()
	capturedBytes := target.Bytes
	rotation, err := journal.RotateActiveSegmentWithStableResources(false)
	if err != nil {
		t.Fatal(err)
	}
	defer rotation.Release()
	if got := appendFrame(); got != first+1 {
		t.Fatalf("successor LSN=%d", got)
	}
	if target.LSN != first || target.Bytes != capturedBytes {
		t.Fatal("capture frontier mutated after append/rotation")
	}
	closed := make(chan error, 1)
	go func() { closed <- journal.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close crossed live captured pin: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	if err := target.Sync(); err != nil {
		t.Fatal(err)
	}
	target.Release()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not wake after release")
	}
	reopened, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.NextLSN(); got != first+2 {
		t.Fatalf("recovered nextLSN=%d", got)
	}
}

// Each capture must perform fresh physical I/O even while the active segment's
// exact handle is retained between captures. Captures retain distinct immutable
// frontiers and remain usable in either order after rotation.
func TestCapturedSyncSharedSegmentProgressAndOutOfOrderRelease(t *testing.T) {
	journal, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	appendFrame := func() uint64 {
		t.Helper()
		lsn, err := journal.AppendCommand(CommandEnvelope{Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1})
		if err != nil {
			t.Fatal(err)
		}
		return lsn
	}
	firstLSN := appendFrame()
	first, err := journal.CaptureSync(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	secondLSN := appendFrame()
	second, err := journal.CaptureSync(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	if first.LSN != firstLSN || second.LSN != secondLSN || second.Bytes <= first.Bytes {
		t.Fatal("capture did not retain distinct increasing complete prefixes")
	}
	if first.active != second.active {
		t.Fatal("same segment did not share its exact retained handle")
	}
	before := journal.WriterDurabilityStats().FileSyncCalls
	if err := second.Sync(); err != nil {
		t.Fatal(err)
	}
	second.Release()
	if err := first.Sync(); err != nil {
		t.Fatal(err)
	}
	first.Release()
	if got := journal.WriterDurabilityStats().FileSyncCalls - before; got != 2 {
		t.Fatalf("successive capture syncs=%d want 2; shared retention must not cache durability", got)
	}
	if journal.syncPins != 0 || journal.capturedFile.refs != 1 {
		t.Fatalf("released captures retain pins=%d refs=%d", journal.syncPins, journal.capturedFile.refs)
	}
	thirdLSN := appendFrame()
	third, err := journal.CaptureSync(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer third.Release()
	oldHandle := third.active
	rotation, err := journal.RotateActiveSegmentWithStableResources(false)
	if err != nil {
		t.Fatal(err)
	}
	defer rotation.Release()
	if journal.capturedFile != nil || oldHandle.refs != 1 {
		t.Fatal("rotation failed to release only the journal's old-handle reference")
	}
	fourthLSN := appendFrame()
	fourth, err := journal.CaptureSync(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer fourth.Release()
	if fourth.active == oldHandle || third.LSN != thirdLSN || fourth.LSN != fourthLSN {
		t.Fatal("rotation aliased capture identity/frontier")
	}
	if err := fourth.Sync(); err != nil {
		t.Fatal(err)
	}
	fourth.Release()
	if err := third.Sync(); err != nil {
		t.Fatal(err)
	}
	third.Release()
	if oldHandle.refs != 0 || journal.syncPins != 0 {
		t.Fatalf("retired handle leaked refs=%d pins=%d", oldHandle.refs, journal.syncPins)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	if journal.capturedFile != nil {
		t.Fatal("Close retained active handle")
	}
}

func TestCapturedSyncFinalLegalLSN(t *testing.T) {
	journal, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{InitialLSN: ^uint64(0) - 1, CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if got := journal.AppendedLSN(); got != ^uint64(0)-1 {
		t.Fatalf("initial prefix=%d", got)
	}
	lsn, err := journal.AppendCommand(CommandEnvelope{Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1})
	if err != nil {
		t.Fatal(err)
	}
	if lsn != ^uint64(0) || journal.AppendedLSN() != lsn {
		t.Fatal("last legal LSN excluded from complete prefix")
	}
	target, err := journal.CaptureSync(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Release()
	if target.LSN != lsn {
		t.Fatal("capture omitted terminal LSN")
	}
	if err := target.Sync(); err != nil {
		t.Fatal(err)
	}
}
