//go:build windows

package raftfsm

import (
	"errors"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

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
