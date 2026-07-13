//go:build linux

package valuelog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestStableResourcePinBlocksDeleteThroughSecondManager(t *testing.T) {
	dir := t.TempDir()
	fileID, err := EncodeFileID(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "value-l1-000001.log")
	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	ptr, err := writer.Append(0, nil, 1, []byte("stable value"))
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	first, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	registry := rootpublication.NewIdentityPinRegistry()
	if err := first.SetStableResourcePinRegistry(registry); err != nil {
		t.Fatal(err)
	}
	if err := second.SetStableResourcePinRegistry(registry); err != nil {
		t.Fatal(err)
	}
	frontier := ptr.Offset + uint64(page.ValuePtrRecordLength(ptr))
	token, err := first.RegisterStableResourceToken(fileID, func(*os.File, uint64) error { return nil }, rootpublication.StableResourceSpec{
		Kind: rootpublication.ResourceValueLogSegment, LogicalNamespace: "test/value",
		ResourceID: "segment/1", DiagnosticPath: "value_vlog/value-l1-000001.log",
		Generation: 1, RequiredFrontier: frontier, ReachabilityField: "test.value_ptr",
	})
	if err != nil {
		t.Fatalf("RegisterStableResourceToken: %v", err)
	}
	if err := second.RemoveSegment(fileID); !errors.Is(err, ErrFilePinned) {
		t.Fatalf("RemoveSegment while token pinned = %v, want ErrFilePinned", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("pinned segment disappeared: %v", err)
	}
	if err := token.Release(); err != nil {
		t.Fatalf("token Release: %v", err)
	}
	if err := second.RemoveSegment(fileID); err != nil {
		t.Fatalf("RemoveSegment after release: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("segment still exists after release/delete: %v", err)
	}
	if _, err := first.RegisterStableResourceToken(fileID, func(*os.File, uint64) error { return nil }, rootpublication.StableResourceSpec{
		Kind: rootpublication.ResourceValueLogSegment, LogicalNamespace: "test/value",
		ResourceID: "segment/stale", DiagnosticPath: "value_vlog/value-l1-000001.log",
		Generation: 1, RequiredFrontier: frontier, ReachabilityField: "test.value_ptr",
	}); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("stale manager registration after delete = %v, want ErrResourceConflict", err)
	}
}
