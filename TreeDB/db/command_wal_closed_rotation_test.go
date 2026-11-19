package db

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// File coverage cannot discharge retained namespace ownership or authorize W.
// A later attempt without the original cutover receipt still syncs its debt.
func TestCommandWALClosedRotationReceiptPreservesNamespaceFailure(t *testing.T) {
	opts := Options{
		Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true,
		ResolvedProfile: ProfileCommandWALRelaxed, Durability: DurabilityWALOnRelaxed,
		MemtableMode: "cow_btree", FreshRelaxedStandaloneWALStream: true,
		CommandWALSegmentTargetBytes: 1,
	}
	database, err := OpenForNativeSemanticOwner(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	}()
	if stats := database.commandJournal.WriterBufferStats(); stats.Container != "stream" || stats.ContainerReason != "ext4_stream" {
		t.Skipf("production selector requires an ext4 stream: %+v", stats)
	}
	key := []byte("row")
	for _, value := range []string{"one", "two"} {
		if err := database.Set(key, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	path, _ := database.commandWALActiveSegmentSnapshot()
	if filepath.Base(path) != commitlog.CommandSegmentName(0, 2) {
		t.Fatalf("fixture did not rotate on the second relaxed frame: %s", path)
	}
	closed, err := database.RotateCommandWALActiveSegmentWithClosedSyncReceipt()
	if err != nil || !closed.Valid() {
		t.Fatalf("checked rotation valid=%t err=%v", closed.Valid(), err)
	}
	beforeW := database.commandWALDurableLSN.Load()
	if beforeW != 0 {
		t.Fatalf("rotation alone promoted durable WAL to %d", beforeW)
	}
	fail := errors.New("retained WAL namespace failure")
	failNamespace := true
	var targetSyncs, namespaceChecks int
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource != durabilitycut.ResourceCommandWAL {
			return nil
		}
		switch {
		case event.Point == durabilitycut.AfterDependencyFileSync && event.Path == path:
			targetSyncs++
		case event.Point == durabilitycut.BeforeNewFileDirectorySync && event.Path == database.commandWALDir:
			namespaceChecks++
			if failNamespace {
				return fail
			}
		}
		return nil
	})
	defer restore()
	if pending, err := database.PrepareCommandWALCoveredPrefixCleanupWithClosedSyncReceipt(closed); pending || !errors.Is(err, fail) {
		t.Fatalf("retained namespace failure pending=%t err=%v", pending, err)
	}
	if got := database.commandWALDurableLSN.Load(); got != beforeW || namespaceChecks != 1 || targetSyncs != 0 {
		t.Fatalf("failed namespace: W=%d (before %d), checks=%d, target syncs=%d", got, beforeW, namespaceChecks, targetSyncs)
	}
	failNamespace = false
	if pending, err := database.PrepareCommandWALCoveredPrefixCleanup(); !pending || err != nil {
		t.Fatalf("subsequent attempt without receipt pending=%t err=%v", pending, err)
	}
	if got := database.commandWALDurableLSN.Load(); got != 2 || namespaceChecks != 2 || targetSyncs != 1 {
		t.Fatalf("successful retry: W=%d, namespace checks=%d, target syncs=%d", got, namespaceChecks, targetSyncs)
	}
}
