//go:build linux

package caching

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestStableValueLogRegistrationRetainsExactWriterAcrossRotation(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "value-l1-000001.log")
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	ptr, err := w.Append(0, nil, 1, []byte("stable value"))
	if err != nil {
		t.Fatal(err)
	}
	db := &DB{dir: dir, stableResourcePins: rootpublication.NewIdentityPinRegistry()}
	l := &lane{id: 1, vlog: w, vlogPath: path, vlogSeq: 1}
	a := &cachingValueLogAppender{db: db, lane: l}
	token, err := a.RegisterStableValueLogSegment([]page.ValuePtr{ptr}, rootpublication.StableResourceSpec{
		ReachabilityField: "meta.value_ptr",
	})
	if err != nil {
		t.Fatalf("RegisterStableValueLogSegment: %v", err)
	}
	wantFrontier := ptr.Offset + uint64(page.ValuePtrRecordLength(ptr))
	if token.Kind() != rootpublication.ResourceValueLogSegment || token.RequiredFrontier() != wantFrontier {
		t.Fatalf("token kind/frontier = %s/%d, want %s/%d", token.Kind(), token.RequiredFrontier(), rootpublication.ResourceValueLogSegment, wantFrontier)
	}
	oldInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	nextID, err := valuelog.EncodeFileID(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	nextPath := filepath.Join(dir, "value-l1-000002.log")
	if err := w.RotateTo(nextPath, nextID); err != nil {
		t.Fatal(err)
	}
	l.vlogPath = nextPath
	l.vlogSeq = 2
	if err := token.FlushThrough(); err != nil {
		t.Fatalf("FlushThrough retained writer: %v", err)
	}
	if err := token.SyncThrough(); err != nil {
		t.Fatalf("SyncThrough retained writer: %v", err)
	}
	afterInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(oldInfo, afterInfo) {
		t.Fatal("retained token stopped naming original writer inode")
	}
	if err := token.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := a.RegisterStableValueLogSegment([]page.ValuePtr{ptr}, rootpublication.StableResourceSpec{
		ReachabilityField: "meta.stale_value_ptr",
	}); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("registration after rotation = %v, want ErrResourceConflict", err)
	}
}

func TestStableOuterLeafRegistrationUsesExactLogRecordFrontier(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(valuelog.ReservedLeafLogLaneID, 7)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "outer-leaf.log")
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	ptr, err := w.Append(0, nil, 1, []byte("outer leaf"))
	if err != nil {
		t.Fatal(err)
	}
	ref, err := page.LeafLogPtrFromValuePtr(ptr)
	if err != nil {
		t.Fatal(err)
	}
	db := &DB{dir: dir, stableResourcePins: rootpublication.NewIdentityPinRegistry()}
	lane := &lane{id: valuelog.ReservedLeafLogLaneID, vlog: w, vlogPath: path, vlogSeq: 7}
	leafLog := &cachingLeafPageLog{db: db, lane: lane}
	token, err := leafLog.RegisterStableOuterLeafSegment([]page.LogRecordRef{ref}, rootpublication.StableResourceSpec{
		ReachabilityField: "tree.child.log",
	})
	if err != nil {
		t.Fatalf("RegisterStableOuterLeafSegment: %v", err)
	}
	wantFrontier := ref.Offset + uint64(ref.RecordLength())
	if token.Kind() != rootpublication.ResourceOuterLeafSegment || token.RequiredFrontier() != wantFrontier {
		t.Fatalf("token kind/frontier = %s/%d, want %s/%d", token.Kind(), token.RequiredFrontier(), rootpublication.ResourceOuterLeafSegment, wantFrontier)
	}
	if token.Identity() == (rootpublication.StableIdentity{}) {
		t.Fatal("outer-leaf token has empty writer identity")
	}
	if err := token.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestStableWriterTokenBlocksManagerDeletion(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "value-l2-000001.log")
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	ptr, err := w.Append(0, nil, 1, []byte("delete gate"))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	manager, err := valuelog.NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	registry := rootpublication.NewIdentityPinRegistry()
	if err := manager.SetStableResourcePinRegistry(registry); err != nil {
		t.Fatal(err)
	}
	db := &DB{dir: dir, stableResourcePins: registry}
	lane := &lane{id: 2, vlog: w, vlogPath: path, vlogSeq: 1}
	token, err := (&cachingValueLogAppender{db: db, lane: lane}).RegisterStableValueLogSegment(
		[]page.ValuePtr{ptr},
		rootpublication.StableResourceSpec{ReachabilityField: "meta.value_ptr"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.RemoveSegment(fileID); !errors.Is(err, valuelog.ErrFilePinned) {
		t.Fatalf("RemoveSegment while writer token pinned = %v, want ErrFilePinned", err)
	}
	if err := token.Release(); err != nil {
		t.Fatal(err)
	}
	if err := manager.RemoveSegment(fileID); err != nil {
		t.Fatalf("RemoveSegment after release: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("segment still exists after release/delete: %v", err)
	}
}

func TestExactStableFrontiersRejectIncompleteOrMixedReferences(t *testing.T) {
	fileID, err := valuelog.EncodeFileID(3, 1)
	if err != nil {
		t.Fatal(err)
	}
	otherID, err := valuelog.EncodeFileID(3, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := exactValuePtrFrontier([]page.ValuePtr{{FileID: fileID, Offset: 1}}); !errors.Is(err, rootpublication.ErrInvalidStableResource) {
		t.Fatalf("zero record hint = %v, want ErrInvalidStableResource", err)
	}
	if _, _, err := exactValuePtrFrontier([]page.ValuePtr{{FileID: fileID, Offset: 1, Length: 2}, {FileID: otherID, Offset: 3, Length: 4}}); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("mixed file ids = %v, want ErrResourceConflict", err)
	}
	if _, _, err := exactLogRecordRefFrontier([]page.LogRecordRef{{FileID: page.ValueLogSegmentID(fileID), Offset: 1}}); !errors.Is(err, rootpublication.ErrInvalidStableResource) {
		t.Fatalf("zero outer-leaf record hint = %v, want ErrInvalidStableResource", err)
	}
}

type stableRegistryRecordingBackend struct {
	*MockBackend
	registry *rootpublication.IdentityPinRegistry
}

func (b *stableRegistryRecordingBackend) SetValueLogStableResourcePinRegistry(registry *rootpublication.IdentityPinRegistry) error {
	b.registry = registry
	return nil
}

func TestOpenInjectsOneStableRegistryIntoReaderAndBackend(t *testing.T) {
	backend := &stableRegistryRecordingBackend{MockBackend: NewMockBackend()}
	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 20,
		JournalLanes:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.stableResourcePins == nil || backend.registry != db.stableResourcePins {
		t.Fatalf("registry injection backend=%p db=%p", backend.registry, db.stableResourcePins)
	}
}

func TestBackendCandidateBoundaryReachesExactCachingProducer(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	cached, err := Open(dir, backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 20,
		JournalLanes:   1,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	defer cached.Close()
	if backend.StableResourcePinRegistry() == nil {
		t.Fatal("direct backend DB did not own a stable resource registry")
	}
	if cached.stableResourcePins != backend.StableResourcePinRegistry() {
		t.Fatalf("cached reader registry %p differs from backend-owned registry %p", cached.stableResourcePins, backend.StableResourcePinRegistry())
	}
	ptrs, err := backend.AppendValueLogValues([][]byte{[]byte("candidate-bound value")})
	if err != nil {
		t.Fatal(err)
	}
	token, err := backend.RegisterStableValueLogSegment(ptrs, rootpublication.StableResourceSpec{
		ReachabilityField: "prepared_root.value_ptr",
	})
	if err != nil {
		t.Fatalf("backend RegisterStableValueLogSegment: %v", err)
	}
	if token.Kind() != rootpublication.ResourceValueLogSegment {
		t.Fatalf("backend token kind = %s", token.Kind())
	}
	if err := token.Release(); err != nil {
		t.Fatal(err)
	}
}
