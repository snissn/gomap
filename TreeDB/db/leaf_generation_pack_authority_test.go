//go:build darwin || linux || freebsd || netbsd || openbsd

package db

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type leafPackAuthorityFixture struct {
	db          *DB
	registry    *rootpublication.IdentityPinRegistry
	stagingDir  string
	destination string
	path        string
	fileID      uint32
	writer      *valuelog.Writer
	created     rewriteCreatedSegment
	pointer     page.LeafLogPtr
}

func newLeafPackAuthorityFixture(t *testing.T) leafPackAuthorityFixture {
	t.Helper()
	root := t.TempDir()
	destination := filepath.Join(root, leafVLogDirName)
	stagingDir := filepath.Join(destination, ".leaf-pack-copy-test", leafVLogDirName)
	for _, dir := range []string{destination, stagingDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatalf("MkdirAll %s: %v", dir, err)
		}
	}
	fileID, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := valuelog.SegmentPath(stagingDir, fileID)
	writer, err := valuelog.NewStagingWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewStagingWriter: %v", err)
	}
	valuePtr, err := writer.Append(0, nil, 1, []byte("packed-outer-leaf"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := writer.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	identity, err := writer.StableIdentity()
	if err != nil {
		t.Fatal(err)
	}
	pointer, err := page.LeafLogPtrFromValuePtr(valuePtr)
	if err != nil {
		t.Fatal(err)
	}
	registry := rootpublication.NewIdentityPinRegistry()
	return leafPackAuthorityFixture{
		db: &DB{dir: root, valueLogIdentityPins: registry}, registry: registry,
		stagingDir: stagingDir, destination: destination, path: path, fileID: fileID, writer: writer,
		created: rewriteCreatedSegment{path: path, fileID: fileID, identity: identity}, pointer: pointer,
	}
}

func (fixture *leafPackAuthorityFixture) close(t *testing.T) {
	t.Helper()
	if fixture.writer != nil {
		if err := fixture.writer.Close(); err != nil {
			t.Fatalf("Close writer: %v", err)
		}
		fixture.writer = nil
	}
}

func TestLeafGenerationPackPromotionAuthorityRetainsExactPackedResourceThroughRegistration(t *testing.T) {
	fixture := newLeafPackAuthorityFixture(t)
	defer fixture.close(t)
	authority, err := newLeafGenerationPackPromotionAuthority(fixture.db, fixture.stagingDir, fixture.destination)
	if err != nil {
		t.Fatal(err)
	}
	defer authority.release()
	if err := authority.capture([]rewriteCreatedSegment{fixture.created}, []page.LeafLogPtr{fixture.pointer}); err != nil {
		t.Fatalf("capture: %v", err)
	}
	fixture.close(t)
	if got := fixture.registry.ActivePins(); got != 1 {
		t.Fatalf("capture pins after writer close=%d want 1", got)
	}
	if _, err := fixture.registry.BeginDelete(fixture.created.identity); !errors.Is(err, rootpublication.ErrResourcePinned) {
		t.Fatalf("BeginDelete during capture=%v want pinned", err)
	}

	var events []durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		events = append(events, event)
		return nil
	})
	promoted, mutated, err := authority.promote()
	restore()
	if err != nil || !mutated || len(promoted) != 1 {
		t.Fatalf("promote mutated=%v promoted=%v err=%v", mutated, promoted, err)
	}
	destinationPath := filepath.Join(fixture.destination, filepath.Base(fixture.path))
	if promoted[0].path != destinationPath {
		t.Fatalf("promoted path=%q want %q", promoted[0].path, destinationPath)
	}
	if _, err := os.Stat(fixture.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging path remains: %v", err)
	}
	if got := fixture.registry.ActivePins(); got != 2 {
		t.Fatalf("capture+frozen-set pins=%d want 2", got)
	}
	descriptors := authority.resources.Descriptors()
	if len(descriptors) != 1 || descriptors[0].Kind() != rootpublication.ResourceOuterLeafPack ||
		descriptors[0].Generation() != uint64(fixture.fileID) || descriptors[0].Frontier().Bytes < fixture.pointer.Offset+uint64(fixture.pointer.RecordLength()) {
		t.Fatalf("packed descriptors=%+v", descriptors)
	}
	var renameEvents, beforeSync, afterSync int
	for _, event := range events {
		if event.Namespace == durabilitycut.NamespaceRename {
			renameEvents++
		}
		if event.Point == durabilitycut.BeforeNewFileDirectorySync {
			beforeSync++
		}
		if event.Point == durabilitycut.AfterNewFileDirectorySync {
			afterSync++
		}
	}
	if renameEvents != 1 || beforeSync != 2 || afterSync != 2 {
		t.Fatalf("namespace evidence rename=%d before-sync=%d after-sync=%d", renameEvents, beforeSync, afterSync)
	}

	manager, err := valuelog.NewManagerWithStableResourcePinRegistry(fixture.destination, fixture.registry)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer manager.Close()
	fixture.db.valueLogManager = manager
	if err := authority.verifyManagerRegistration(); err != nil {
		t.Fatalf("verifyManagerRegistration: %v", err)
	}
	if err := authority.release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	if got := fixture.registry.ActivePins(); got != 0 {
		t.Fatalf("pins after manager handoff=%d want 0", got)
	}
	if _, ok := manager.StableSegmentIdentity(fixture.fileID); !ok {
		t.Fatal("manager lost promoted identity after authority release")
	}
}

func TestLeafGenerationPackPromotionAuthorityMultipleSegmentsShareParentSyncs(t *testing.T) {
	fixture := newLeafPackAuthorityFixture(t)
	defer fixture.close(t)
	secondFileID, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 2)
	if err != nil {
		t.Fatal(err)
	}
	secondPath := valuelog.SegmentPath(fixture.stagingDir, secondFileID)
	secondWriter, err := valuelog.NewStagingWriter(secondPath, secondFileID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if secondWriter != nil {
			_ = secondWriter.Close()
		}
	}()
	secondValuePtr, err := secondWriter.Append(0, nil, 2, []byte("second-packed-outer-leaf"))
	if err != nil {
		t.Fatal(err)
	}
	if err := secondWriter.Sync(); err != nil {
		t.Fatal(err)
	}
	secondIdentity, err := secondWriter.StableIdentity()
	if err != nil {
		t.Fatal(err)
	}
	secondPointer, err := page.LeafLogPtrFromValuePtr(secondValuePtr)
	if err != nil {
		t.Fatal(err)
	}
	secondCreated := rewriteCreatedSegment{path: secondPath, fileID: secondFileID, identity: secondIdentity}

	authority, err := newLeafGenerationPackPromotionAuthority(fixture.db, fixture.stagingDir, fixture.destination)
	if err != nil {
		t.Fatal(err)
	}
	defer authority.release()
	if err := authority.capture(
		[]rewriteCreatedSegment{fixture.created, secondCreated},
		[]page.LeafLogPtr{fixture.pointer, secondPointer},
	); err != nil {
		t.Fatal(err)
	}
	if got := fixture.writer.DurabilityStats().FileSyncCalls; got != 1 {
		t.Fatalf("first content syncs=%d want 1", got)
	}
	if got := secondWriter.DurabilityStats().FileSyncCalls; got != 1 {
		t.Fatalf("second content syncs=%d want 1", got)
	}
	fixture.close(t)
	if err := secondWriter.Close(); err != nil {
		t.Fatal(err)
	}
	secondWriter = nil

	var events []durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		events = append(events, event)
		return nil
	})
	promoted, mutated, err := authority.promote()
	restore()
	if err != nil || !mutated || len(promoted) != 2 {
		t.Fatalf("promote mutated=%v promoted=%v err=%v", mutated, promoted, err)
	}
	if got := fixture.registry.ActivePins(); got != 4 {
		t.Fatalf("two capture and two resource-set pins=%d want 4", got)
	}
	if descriptors := authority.resources.Descriptors(); len(descriptors) != 2 {
		t.Fatalf("packed descriptors=%d want 2", len(descriptors))
	}
	var renames, beforeSyncs, afterSyncs int
	for _, event := range events {
		if event.Namespace == durabilitycut.NamespaceRename {
			renames++
		}
		if event.Point == durabilitycut.BeforeNewFileDirectorySync {
			beforeSyncs++
		}
		if event.Point == durabilitycut.AfterNewFileDirectorySync {
			afterSyncs++
		}
	}
	if renames != 2 || beforeSyncs != 2 || afterSyncs != 2 {
		t.Fatalf("two files share exact parents: renames=%d before-sync=%d after-sync=%d", renames, beforeSyncs, afterSyncs)
	}
}

func TestLeafGenerationPackPromotionAuthorityRejectsExistingDestinationBeforeMutation(t *testing.T) {
	fixture := newLeafPackAuthorityFixture(t)
	defer fixture.close(t)
	authority, err := newLeafGenerationPackPromotionAuthority(fixture.db, fixture.stagingDir, fixture.destination)
	if err != nil {
		t.Fatal(err)
	}
	defer authority.release()
	if err := authority.capture([]rewriteCreatedSegment{fixture.created}, []page.LeafLogPtr{fixture.pointer}); err != nil {
		t.Fatal(err)
	}
	fixture.close(t)
	replacement := filepath.Join(fixture.destination, filepath.Base(fixture.path))
	if err := os.WriteFile(replacement, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, mutated, err := authority.promote()
	if mutated || !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("promote mutated=%v err=%v want pre-mutation conflict", mutated, err)
	}
	if err := rootpublication.ValidateStableChildLink(authority.stagingParent, authority.segments[0].handle, filepath.Base(fixture.path)); err != nil {
		t.Fatalf("staging identity lost: %v", err)
	}
	data, err := os.ReadFile(replacement)
	if err != nil || string(data) != "replacement" {
		t.Fatalf("replacement changed data=%q err=%v", data, err)
	}
}

func TestLeafGenerationPackPromotionAuthorityRejectsMalformedPointers(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(page.LeafLogPtr) page.LeafLogPtr
		wantErr error
	}{
		{name: "zero-length", mutate: func(ptr page.LeafLogPtr) page.LeafLogPtr { ptr.RecordLengthHint = 0; return ptr }, wantErr: rootpublication.ErrUnresolvedResource},
		{name: "foreign-file", mutate: func(ptr page.LeafLogPtr) page.LeafLogPtr { ptr.FileID++; return ptr }, wantErr: rootpublication.ErrResourceConflict},
		{name: "overflow", mutate: func(ptr page.LeafLogPtr) page.LeafLogPtr { ptr.Offset = ^uint64(0) - 1; return ptr }, wantErr: rootpublication.ErrFrontierBeyondResource},
		{name: "past-frontier", mutate: func(ptr page.LeafLogPtr) page.LeafLogPtr { ptr.Offset += 1 << 20; return ptr }, wantErr: rootpublication.ErrFrontierBeyondResource},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newLeafPackAuthorityFixture(t)
			defer fixture.close(t)
			authority, err := newLeafGenerationPackPromotionAuthority(fixture.db, fixture.stagingDir, fixture.destination)
			if err != nil {
				t.Fatal(err)
			}
			defer authority.release()
			err = authority.capture([]rewriteCreatedSegment{fixture.created}, []page.LeafLogPtr{test.mutate(fixture.pointer)})
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("capture error=%v want %v", err, test.wantErr)
			}
			if fixture.registry.ActivePins() != 0 || fixture.registry.ActiveIdentities() != 0 {
				t.Fatalf("failed capture leaked pins=%d identities=%d", fixture.registry.ActivePins(), fixture.registry.ActiveIdentities())
			}
		})
	}
}

func TestCleanupRewriteCreatedSegmentRejectsReboundPath(t *testing.T) {
	fixture := newLeafPackAuthorityFixture(t)
	fixture.close(t)
	if err := os.Rename(fixture.path, fixture.path+".original"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixture.path, []byte("rebound"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := fixture.db.cleanupRewriteCreatedSegments([]rewriteCreatedSegment{fixture.created})
	if !errors.Is(err, rootpublication.ErrResourceConflict) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("cleanup error=%v want conflict and recovery-required", err)
	}
	data, readErr := os.ReadFile(fixture.path)
	if readErr != nil || string(data) != "rebound" {
		t.Fatalf("rebound path was deleted data=%q err=%v", data, readErr)
	}
}

func TestCleanupRewriteCreatedSegmentDropsRegisteredMissingPath(t *testing.T) {
	fixture := newLeafPackAuthorityFixture(t)
	defer fixture.close(t)
	authority, err := newLeafGenerationPackPromotionAuthority(fixture.db, fixture.stagingDir, fixture.destination)
	if err != nil {
		t.Fatal(err)
	}
	defer authority.release()
	if err := authority.capture([]rewriteCreatedSegment{fixture.created}, []page.LeafLogPtr{fixture.pointer}); err != nil {
		t.Fatal(err)
	}
	fixture.close(t)
	promoted, _, err := authority.promote()
	if err != nil || len(promoted) != 1 {
		t.Fatalf("promote segments=%v err=%v", promoted, err)
	}
	manager, err := valuelog.NewManagerWithStableResourcePinRegistry(fixture.destination, fixture.registry)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	fixture.db.valueLogManager = manager
	if err := authority.verifyManagerRegistration(); err != nil {
		t.Fatal(err)
	}
	if err := authority.release(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(promoted[0].path); err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.cleanupRewriteCreatedSegments(promoted); err != nil {
		t.Fatalf("cleanup missing registered segment: %v", err)
	}
	if manager.HasSegment(fixture.fileID) {
		t.Fatalf("manager retained missing rewrite-created segment %d", fixture.fileID)
	}
	if pins, identities := fixture.registry.ActivePins(), fixture.registry.ActiveIdentities(); pins != 0 || identities != 0 {
		t.Fatalf("cleanup leaked registry state pins=%d identities=%d", pins, identities)
	}
}
