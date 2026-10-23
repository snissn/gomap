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
