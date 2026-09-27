//go:build windows

package raftfsm

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestRaftSnapshotV1CaptureFailsClosedBeforeMutationWindows(t *testing.T) {
	dir := t.TempDir()
	db := openRaftSnapshotFSMTestDB(t, dir, false)
	defer func() { _ = db.Close() }()
	fsm := openRaftSnapshotFSMForTest(t, db, dir, true)
	defer func() { _ = fsm.Close() }()
	applySnapshotSourceEntries(t, fsm, []byte(`{"_id":"u-large","name":"unchanged"}`))
	before, err := fsm.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatal(err)
	}
	for _, capture := range []struct {
		name string
		run  func() error
	}{
		{"capture", func() error { _, err := fsm.CaptureRaftSnapshotV1(); return err }},
		{"export", func() error { _, err := fsm.ExportRaftSnapshotV1(); return err }},
	} {
		t.Run(capture.name, func(t *testing.T) {
			err := capture.run()
			if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
				t.Fatalf("snapshot error=%v want ErrNamespacePersistenceUnsupported", err)
			}
			if code, ok := ErrorCodeOf(err); !ok || code != raftentry.ErrorUnsafeDurabilityModeV1 {
				t.Fatalf("snapshot code=(%s,%t) want %s", code, ok, raftentry.ErrorUnsafeDurabilityModeV1)
			}
			if !fsm.SnapshotCarrierReleasedV1() {
				t.Fatal("unsupported capture retained snapshot admission")
			}
			after, err := fsm.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
			if err != nil || after != before {
				t.Fatalf("unsupported capture changed live digest: %v", err)
			}
			staged, err := filepath.Glob(filepath.Join(raftSnapshotStagingDirV1(fsm.cluster.Layout.SnapshotDir), "treedb-*"))
			if err != nil || len(staged) != 0 {
				t.Fatalf("unsupported capture staged files: %v %v", staged, err)
			}
			if _, err := os.Stat(filepath.Join(fsm.db.Dir(), "index.db")); err != nil {
				t.Fatalf("live index unavailable after refusal: %v", err)
			}
		})
	}
}

func TestRaftSnapshotV1InstallFailsClosedBeforeLiveStateMutationWindows(t *testing.T) {
	dir := t.TempDir()
	db := openRaftSnapshotFSMTestDB(t, dir, false)
	defer func() { _ = db.Close() }()
	fsm := openRaftSnapshotFSMForTest(t, db, dir, true)
	defer func() { _ = fsm.Close() }()

	err := fsm.InstallRaftSnapshotV1(strings.NewReader("not inspected on an unsupported platform"))
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("InstallRaftSnapshotV1 error=%v want ErrNamespacePersistenceUnsupported", err)
	}
	if code, ok := ErrorCodeOf(err); !ok || code != raftentry.ErrorUnsafeDurabilityModeV1 {
		t.Fatalf("InstallRaftSnapshotV1 code=(%s,%t) want %s", code, ok, raftentry.ErrorUnsafeDurabilityModeV1)
	}
	fsm.mu.RLock()
	stateErr := fsm.requireRaftSnapshotOpenV1()
	fsm.mu.RUnlock()
	if stateErr != nil {
		t.Fatalf("unsupported install mutated live FSM state: %v", stateErr)
	}
}
