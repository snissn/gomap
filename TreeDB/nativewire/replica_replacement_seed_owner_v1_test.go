package nativewire

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestReplacementSeedOwnerSuccessorAndInterruptedCleanupV1(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("requires durable relative namespace")
	}
	command := replacementReceiverTestBeginV1(t)
	root := filepath.Join(t.TempDir(), "replacement-seed")
	first, err := prepareReplacementSeedDirectoryV1(root, command)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(first, "snapshots", "partial.tmp"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(first, "snapshots", "partial.tmp", "state.bin"), []byte("old retained bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	if repeated, err := prepareReplacementSeedDirectoryV1(root, command); err != nil || repeated != first {
		t.Fatalf("exact retry %q %v", repeated, err)
	}
	changed := command
	changed.OperationID += "/../next"
	if _, err := prepareReplacementSeedDirectoryV1(root, changed); err == nil {
		t.Fatal("same epoch superseded owner")
	}
	changed.ExpectedEpoch++
	second, err := prepareReplacementSeedDirectoryV1(root, changed)
	if err != nil || second == first {
		t.Fatalf("successor %q %v", second, err)
	}
	if _, err := os.Lstat(first); !os.IsNotExist(err) {
		t.Fatalf("old seed remains: %v", err)
	}
	// A crash after cleanup but before native store creation keeps a canonical
	// owner with no seed directory. The identical BEGIN resumes deterministically.
	if repeated, err := prepareReplacementSeedDirectoryV1(root, changed); err != nil || repeated != second {
		t.Fatalf("restart after cleanup %q %v", repeated, err)
	}
	if _, err := prepareReplacementSeedDirectoryV1(root, command); err == nil {
		t.Fatal("older owner returned")
	}
	if err := os.WriteFile(filepath.Join(root, replacementSeedOwnerTempV1), []byte("interrupted write"), 0600); err != nil {
		t.Fatal(err)
	}
	third := changed
	third.OperationID += "-third"
	third.ExpectedEpoch++
	if _, err := prepareReplacementSeedDirectoryV1(root, third); err != nil {
		t.Fatal(err)
	}
}

func TestReplacementSeedOwnerRefusesUnownedOrMismatchedBytesV1(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("requires durable relative namespace")
	}
	command := replacementReceiverTestBeginV1(t)
	for _, shape := range []string{"unowned", "wrong-directory", "symlink", "malformed"} {
		t.Run(shape, func(t *testing.T) {
			root := t.TempDir()
			if shape != "unowned" {
				if _, err := prepareReplacementSeedDirectoryV1(root, command); err != nil {
					t.Fatal(err)
				}
			}
			switch shape {
			case "unowned", "wrong-directory":
				if err := os.Mkdir(filepath.Join(root, "unowned"), 0700); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(t.TempDir(), filepath.Join(root, "linked")); err != nil {
					t.Fatal(err)
				}
			case "malformed":
				if err := os.WriteFile(filepath.Join(root, replacementSeedOwnerV1), []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			command.ExpectedEpoch++
			if _, err := prepareReplacementSeedDirectoryV1(root, command); err == nil {
				t.Fatal("unowned state accepted")
			}
		})
	}
}

func replacementReceiverTestBeginV1(t *testing.T) raftplacement.ReplicaReplacementBeginV1 {
	t.Helper()
	return raftplacement.ReplicaReplacementBeginV1{Format: 1, Kind: "replica-replacement-begin-v1", OperationID: "seed-owner", ConfigDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ExpectedEpoch: 1, CatalogDigest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", GroupID: "g", OldNodeID: "old", NewPeer: raftcluster.Peer{ID: "new", Address: "127.0.0.1:7002"}}
}

func TestReplacementSeedOwnerRefusesSameOwnerSnapshotSymlinkV1(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("requires durable relative namespace")
	}
	command := replacementReceiverTestBeginV1(t)
	root := t.TempDir()
	selected, err := prepareReplacementSeedDirectoryV1(root, command)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(selected, 0700); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(selected, "snapshots")); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareReplacementSeedDirectoryV1(root, command); err == nil {
		t.Fatal("same owner followed snapshots symlink")
	}
	if entries, err := os.ReadDir(outside); err != nil || len(entries) != 0 {
		t.Fatalf("outside namespace touched: %v %v", entries, err)
	}
}
