package caching

import (
	"bytes"
	"context"
	"os"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCachingValueLogStableResource_RotationPinsCapturedWriterUntilDurableRelease(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		Durability:             backenddb.DurabilityDurable,
		CommandWAL:             true,
		DisableBackgroundPrune: true,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold: 1,
			ForcePointers:    true,
		},
	})
	if err != nil {
		t.Fatalf("backend Open: %v", err)
	}
	db, err := Open(dir, backend, Options{
		ExternalCommandWAL:       true,
		FlushThreshold:           1 << 30,
		JournalLanes:             1,
		ValueLogPointerThreshold: 1,
		ForceValueLogPointers:    true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache Open: %v", err)
	}
	defer func() {
		_ = db.Close()
		_ = backend.Close()
	}()

	ptrs, err := backend.AppendValueLogValues([][]byte{bytes.Repeat([]byte("stable-value|"), 128)})
	if err != nil {
		t.Fatalf("AppendValueLogValues: %v", err)
	}
	if len(ptrs) != 1 {
		t.Fatalf("pointers=%d want 1", len(ptrs))
	}
	token, err := backend.CaptureValueLogStableResourceToken("system_root.value_log")
	if err != nil {
		t.Fatalf("CaptureValueLogStableResourceToken: %v", err)
	}
	if token.NamespaceToken.Required {
		t.Fatal("ordinary append unexpectedly requires namespace persistence")
	}
	capturedPath := token.DiagnosticPath
	capturedFileID := ptrs[0].FileID
	if token.Generation != uint64(capturedFileID) {
		t.Fatalf("token generation=%d want file ID %d", token.Generation, capturedFileID)
	}
	if err := backend.ValidateValueLogStableResource(token); err != nil {
		t.Fatalf("ValidateValueLogStableResource: %v", err)
	}
	invalidFrontier := token
	invalidFrontier.Frontier++
	if err := backend.ValidateValueLogStableResource(invalidFrontier); err == nil {
		t.Fatal("validator accepted frontier beyond captured file length")
	}

	if err := db.rotateValueLogLocked(&db.lanes[0]); err != nil {
		t.Fatalf("first rotateValueLogLocked: %v", err)
	}
	if err := db.rotateValueLogLocked(&db.lanes[0]); err != nil {
		t.Fatalf("second rotateValueLogLocked: %v", err)
	}
	backend.ReleaseValueLogValues(ptrs)

	stats, err := backend.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC while stable-pinned: %v", err)
	}
	if stats.SegmentsDeleted == 0 {
		t.Fatalf("expected an unpinned rotated segment to be deleted: %+v", stats)
	}
	if _, err := os.Stat(capturedPath); err != nil {
		t.Fatalf("captured segment removed while pinned: %v", err)
	}

	set, err := rootpublication.NewStableResourceSet([]rootpublication.StableResourceToken{token})
	if err != nil {
		t.Fatalf("NewStableResourceSet: %v", err)
	}
	if err := set.FlushAndSync(context.Background()); err != nil {
		t.Fatalf("FlushAndSync after rotation: %v", err)
	}
	debt := rootpublication.NewStableResourceDebt(set)
	debt.Retry()
	debt.Poison()
	debt.Shutdown()
	transferred := debt.Supersede()
	debt.DurableCoverage() // Ownership moved; the original debt cannot release it.
	if _, err := backend.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{}); err != nil {
		t.Fatalf("ValueLogGC after retained transitions: %v", err)
	}
	if _, err := os.Stat(capturedPath); err != nil {
		t.Fatalf("captured segment removed after retry/poison/supersession: %v", err)
	}
	rootpublication.NewStableResourceDebt(transferred).DurableCoverage()

	stats, err = backend.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC after durable release: %v", err)
	}
	if stats.SegmentsDeleted != 1 {
		t.Fatalf("segments deleted after durable release=%d want 1: %+v", stats.SegmentsDeleted, stats)
	}
	if _, err := os.Stat(capturedPath); !os.IsNotExist(err) {
		t.Fatalf("captured segment still exists after durable release: %v", err)
	}
}
