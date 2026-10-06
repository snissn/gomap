package treedb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/node"
)

// These are already encoded physical point commands, not MVCC Store admission.
// Store's visibility/discard fences remain owned by the later integration node.
func TestCOWPublicEncodedMVCCProcessHelper(t *testing.T) {
	if os.Getenv("TREEDB_COW_ENCODED_HELPER") != "1" {
		t.Skip("subprocess only")
	}
	directory := os.Getenv("TREEDB_COW_ENCODED_DIR")
	profile := Profile(os.Getenv("TREEDB_COW_ENCODED_PROFILE"))
	database, err := Open(cowPublicCrashOptions(profile, directory))
	if err != nil {
		t.Fatal(err)
	}
	const oldTS, newTS uint64 = 770000000001, 880000000001
	encode := func(logical string, ts uint64) []byte {
		k, err := mvcckey.Encode([]byte(logical), ts)
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	aOld, dOld, aNew, dNew := encode("a", oldTS), encode("d", oldTS), encode("a", newTS), encode("d", newTS)
	oldValue := append([]byte{0x01}, bytes.Repeat([]byte("encoded-old/"), 512)...)
	if err := database.Set(aOld, oldValue); err != nil {
		t.Fatal(err)
	}
	if err := database.Set(dOld, oldValue); err != nil {
		t.Fatal(err)
	}
	old := database.AcquireSnapshot()
	if old == nil {
		t.Fatal("old cut unavailable")
	}
	group := database.NewBatchWithSize(2)
	if err := group.Set(aNew, []byte{0x02}); err != nil {
		t.Fatal(err)
	}
	if err := group.Set(dNew, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	if err := group.WriteSync(); err != nil {
		t.Fatal(err)
	}
	_ = group.Close()
	if err := database.DeleteSync(aOld); err != nil {
		t.Fatal(err)
	}
	if got, err := old.Get(aOld); err != nil || !bytes.Equal(got, oldValue) {
		t.Fatalf("raw physical deletion changed old cut: %v", err)
	}
	_ = old.Close()
	// Timestamp and native commit revision have independent authorities.
	expected := []cowPublicCrashEntry{{Key: string(aOld), Found: false}, {Key: string(dOld), Value: oldValue, Found: true}, {Key: string(aNew), Value: []byte{0x02}, Found: true}, {Key: string(dNew), Value: []byte{0x01}, Found: true}}
	snap := database.AcquireSnapshot()
	if snap == nil {
		t.Fatal("new cut unavailable")
	}
	for i := range expected {
		value, rev, err := database.GetVersioned([]byte(expected[i].Key))
		if !expected[i].Found {
			if err != nil || value != nil {
				t.Fatal("physical delete visible")
			}
			expected[i].Revision = rev
			continue
		}
		if err != nil || !bytes.Equal(value, expected[i].Value) || rev == LegacyEntryRevision || uint64(rev) == oldTS || uint64(rev) == newTS {
			t.Fatalf("encoded/live metadata %x: %d %v", expected[i].Key, rev, err)
		}
		expected[i].Revision = rev
		entry, err := snap.GetEntry([]byte(expected[i].Key))
		if err != nil {
			t.Fatal(err)
		}
		expected[i].IsPtr = entry.Flags&node.FlagPointer != 0
		expected[i].Pointer = entry.ValuePtr
	}
	_ = snap.Close()
	upper := append(bytes.Clone(aNew[:len(aNew)-8]), bytes.Repeat([]byte{0xff}, 8)...)
	key, value, found, err := database.SeekGEVersionRange(aNew, upper)
	if err != nil || !found || !bytes.Equal(key, aNew) || !bytes.Equal(value, []byte{0x02}) {
		t.Fatalf("encoded version successor: %x %x %t %v", key, value, found, err)
	}
	receipt := cowPublicCrashReceipt{Profile: profile, ExplicitSync: true, Entries: expected}
	if profile != ProfileNoWALFast {
		receipt.Frames = cowPublicCrashCaptureFrames(t, directory)
		latest := make(map[string]commitlog.RawKVOperation)
		grouped := false
		for _, frame := range receipt.Frames {
			for _, op := range frame.Operations {
				latest[string(op.Key)] = op
			}
			if len(frame.Operations) == 2 && bytes.Equal(frame.Operations[0].Key, aNew) && bytes.Equal(frame.Operations[1].Key, dNew) {
				grouped = true
			}
		}
		if !grouped {
			t.Fatal("encoded group not one canonical frame")
		}
		for i := range expected {
			op, ok := latest[expected[i].Key]
			if !ok || op.Revision != uint64(expected[i].Revision) {
				t.Fatalf("encoded/live/frame revision mismatch: %+v", op)
			}
			if !expected[i].Found {
				if op.Op != commitlog.RawKVOpDelete {
					t.Fatal("physical delete changed format")
				}
			} else {
				if op.Op != commitlog.RawKVOpSetRID && op.Op != commitlog.RawKVOpSetMaterializedRID {
					t.Fatal("forced pointer missing canonical RID")
				}
				receipt.Entries[i].RID = op.RID
			}
		}
	}
	keys := make([][]byte, len(receipt.Entries))
	for i := range keys {
		keys[i] = []byte(receipt.Entries[i].Key)
	}
	encoded, err := json.Marshal(struct {
		Receipt cowPublicCrashReceipt
		Keys    [][]byte
	}{receipt, keys})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(directory, "cow-encoded-receipt.json"), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("COW_ENCODED_MVCC_RECEIPT %s\n", encoded)
	os.Exit(0) // Actual process exit before Close; replay is not checkpointed by cleanup.
}

func TestCOWPublicEncodedMVCCPointsGroupsPhysicalDeleteReplay(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			directory := t.TempDir()
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, binary, "-test.run=^TestCOWPublicEncodedMVCCProcessHelper$", "-test.v")
			command.Env = append(os.Environ(), "TREEDB_COW_ENCODED_HELPER=1", "TREEDB_COW_ENCODED_DIR="+directory, "TREEDB_COW_ENCODED_PROFILE="+string(profile))
			out, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("encoded process exit: %v\n%s", err, out)
			}
			encoded, err := os.ReadFile(filepath.Join(directory, "cow-encoded-receipt.json"))
			if err != nil {
				t.Fatal(err)
			}
			var packet struct {
				Receipt cowPublicCrashReceipt
				Keys    [][]byte
			}
			if err = json.Unmarshal(encoded, &packet); err != nil {
				t.Fatal(err)
			}
			receipt := packet.Receipt
			for i, key := range packet.Keys {
				receipt.Entries[i].Key = string(key)
			}
			if receipt.Profile != profile {
				t.Fatal("receipt profile mismatch")
			}
			cowPublicCrashVerifyReopen(t, directory, receipt)
			cowPublicCrashVerifyReopen(t, directory, receipt)
		})
	}
}
