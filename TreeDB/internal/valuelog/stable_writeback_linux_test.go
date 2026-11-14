//go:build linux

package valuelog

import (
	"errors"
	"os"
	"syscall"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestStableValueLogWritebackReadOnlyManagerRetainsFullSync(t *testing.T) {
	dir := t.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(SegmentPath(dir, fileID), []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	token, err := manager.StableResourceToken(fileID, StableResourceRegistration{MetadataOwner: &owner, Kind: rootpublication.ResourceValueLog, LogicalLane: "main", Generation: 1, DiagnosticPath: "value_vlog/value-l0-000001.log", Reachability: rootpublication.ReachabilityValueLogPointer})
	if err != nil {
		t.Fatal(err)
	}
	// Capture is metadata-owned and contentSynced=false on the real manager path.
	// A write attempt verifies that duplication retained read-only access rights.
	if err = token.WithPinnedFile(func(file *os.File) error {
		_, err := file.WriteAt([]byte("x"), 0)
		if !errors.Is(err, syscall.EBADF) {
			t.Fatalf("manager handle is not read-only: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = token.FlushThrough(); err != nil {
		t.Fatalf("read-only mapping hint: %v", err)
	}
	before, after := 0, 0
	if err = token.SyncThroughWithFileSyncObserver(func(*os.File) error { before++; return nil }, func(*os.File) error { after++; return nil }); err != nil {
		t.Fatal(err)
	}
	if before != 1 || after != 1 {
		t.Fatalf("full sync observers=%d/%d", before, after)
	}
	token.Release()
	if owner.Bytes() != 0 {
		t.Fatalf("metadata retained=%d", owner.Bytes())
	}
}
