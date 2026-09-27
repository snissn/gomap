package nativewire

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func replacementReceiverBeginForTestV1() raftplacement.ReplicaReplacementBeginV1 {
	return raftplacement.ReplicaReplacementBeginV1{Format: 1, Kind: raftplacement.ReplicaReplacementBeginKindV1, OperationID: "../logical/operation", ConfigDigest: strings.Repeat("a", 64), ExpectedEpoch: 1, CatalogDigest: strings.Repeat("b", 64), GroupID: "group-a", OldNodeID: "old", NewPeer: raftcluster.Peer{ID: "target", Address: "127.0.0.1:19002"}}
}
func TestReplacementReceiverDurablePhaseRestartAndBindingV1(t *testing.T) {
	seed, _, _ := replacementGateSeedForTestV1()
	begin := replacementReceiverBeginForTestV1()
	dir := t.TempDir()
	owner, err := openReplacementReceiverOwnerV1(dir, begin, seed)
	if !rootpublication.StableRelativeNamespaceSupported() {
		if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
			t.Fatalf("unsupported receiver=%v", err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := openReplacementReceiverOwnerV1(dir, begin, seed); !errors.Is(err, lockfile.ErrLocked) {
		t.Fatalf("second owner=%v", err)
	}
	if err := owner.persistPhase(replacementReceiverAddIntentV1); err == nil {
		t.Fatal("skipped native install")
	}
	for _, phase := range []replacementReceiverPhaseV1{replacementReceiverInstallingV1, replacementReceiverInstalledV1, replacementReceiverAddIntentV1} {
		if err := owner.persistPhase(phase); err != nil {
			t.Fatal(err)
		}
		if err := owner.Close(); err != nil {
			t.Fatal(err)
		}
		owner, err = openReplacementReceiverOwnerV1(dir, begin, seed)
		if err != nil {
			t.Fatal(err)
		}
		if owner.record.Phase != phase {
			t.Fatalf("restart phase=%s want %s", owner.record.Phase, phase)
		}
	}
	if err := owner.persistPhase(replacementReceiverPreparedV1); err == nil {
		t.Fatal("restart reopened seed install")
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	changed := seed
	changed.ArchiveSHA256 = strings.Repeat("c", 64)
	if _, err := openReplacementReceiverOwnerV1(dir, begin, changed); !errors.Is(err, raftplacement.ErrCatalogMetaConflict) {
		t.Fatalf("changed seed=%v", err)
	}
	begin.OperationID = "another"
	if _, err := openReplacementReceiverOwnerV1(dir, begin, seed); !errors.Is(err, raftplacement.ErrCatalogMetaConflict) {
		t.Fatalf("changed operation=%v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("phase updates grew files: %v", entries)
	}
}

func TestReplacementReceiverLockRejectsReboundParentV1(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		return
	}
	base := t.TempDir()
	dir := filepath.Join(base, "receiver")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	parent, err := rootpublication.OpenStableParent(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	// The original parent already has a distinct lock entry, as on a restart.
	original, err := os.OpenFile(filepath.Join(dir, "replacement-receiver-v1.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	original.Close()
	if err := os.Rename(dir, filepath.Join(base, "old")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	rebound, err := lockfile.Acquire(filepath.Join(dir, "replacement-receiver-v1.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer rebound.Close()
	if err := validateReplacementReceiverLockV1(parent, rebound); !errors.Is(err, raftcluster.ErrInvalidConfig) {
		t.Fatalf("rebound lock accepted: %v", err)
	}
	// Moving the old pathname back cannot turn the wrong locked FD into proof.
	if err := os.Rename(dir, filepath.Join(base, "new")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(base, "old"), dir); err != nil {
		t.Fatal(err)
	}
	if err := validateReplacementReceiverLockV1(parent, rebound); !errors.Is(err, raftcluster.ErrInvalidConfig) {
		t.Fatalf("rename-back accepted wrong FD: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, replacementReceiverRecordNameV1)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("refused owner published phase: %v", err)
	}
}
