package caching

import (
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"testing"
)

func TestCOWCoalescedBoundarySurvivesWritesBeforeKernelCapture(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, true)
	cache.SetCommandWALCheckpointCutoverHook(func() {})
	write := func(value string) {
		t.Helper()
		b := cache.NewBatch()
		defer b.Close()
		if err := b.AuthorizeCOWAppend(cache.cow.txnOwner); err != nil {
			t.Fatal(err)
		}
		if err := b.Set([]byte("key"), []byte(value)); err != nil {
			t.Fatal(err)
		}
		if err := b.WriteAfterCommandWALAppendWithPreparedRevision(false, func() error {
			lsn, err := backend.AppendRawKVCommandWALOrderedEntryScanWithHintPreparedFinalizedAndMode(b.PrepareExternalCommandWALPublication, b.finalizeCOWPublication, b.Replay, 1, backenddb.RawKVCommandWALAppendRelaxed)
			if receiptErr := b.RecordCOWAppend(cache.cow.txnOwner, lsn, err, false); err == nil {
				err = receiptErr
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 4097; i++ {
		write("prefix")
	}
	held := cache.AcquireSnapshot()
	if held == nil {
		t.Fatal("missing retained opening")
	}
	defer held.Close()
	prefix := cache.cow.prefix.LogicalFrontiers()
	cache.testAfterCheckpointFrontierCapture = func() {
		// The original writer fence has ended, but CaptureAcceptedCut has not run.
		for i := 0; i < 4097; i++ {
			write("suffix")
		}
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	cache.testAfterCheckpointFrontierCapture = nil
	disk := backend.AcquireSnapshot()
	if disk == nil {
		t.Fatal("missing physical prefix")
	}
	token, ok := disk.StateToken()
	if !ok || token.AppliedCommandLSN != prefix.C {
		t.Fatalf("physical exact cut %+v prefix%+v", token, prefix)
	}
	if value, err := disk.Get([]byte("key")); err != nil || string(value) != "prefix" {
		t.Fatalf("physical suffix imported %q: %v", value, err)
	}
	if err := disk.Close(); err != nil {
		t.Fatal(err)
	}
	if value, err := held.Get([]byte("key")); err != nil || string(value) != "prefix" {
		t.Fatalf("held prefix changed %q: %v", value, err)
	}
	if value, err := cache.Get([]byte("key")); err != nil || string(value) != "suffix" {
		t.Fatalf("latest suffix %q: %v", value, err)
	}
	if got := cache.cow.prefix.LogicalFrontiers(); got.S.CanonicalLSN != prefix.C || got.C != prefix.C+4097 {
		t.Fatalf("frontiers%+v prefix%+v", got, prefix)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	if cache.cow.budget.Stats().TotalBytes != 0 {
		t.Fatal("final charge retained")
	}
}
