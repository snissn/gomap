package treedb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type cowPublicCrashEntry struct {
	Key      string
	Value    []byte
	Found    bool
	Revision EntryRevision
	IsPtr    bool
	Pointer  page.ValuePtr
	RID      uint64
}

type cowPublicCrashFrame struct {
	LSN           uint64
	Version       uint16
	Durability    commitlog.CommandDurabilityClass
	PayloadSHA256 string
	Operations    []commitlog.RawKVOperation
}

type cowPublicCrashReceipt struct {
	Profile           Profile
	ExplicitSync      bool
	AppliedBeforeExit uint64
	MaxLSN            uint64
	CommitBefore      uint64
	CommitAfter       uint64
	GroupKeys         []string
	Entries           []cowPublicCrashEntry
	Frames            []cowPublicCrashFrame
}

func cowPublicCrashOptions(profile Profile, directory string) Options {
	opts := OptionsFor(profile, directory)
	opts.MemtableMode = "cow_btree"
	opts.MemtableShards = 2
	opts.FlushThreshold = 1 << 30
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.DisableBackgroundPrune = true
	return opts
}

func cowPublicCrashShardKeys() (string, string) {
	var keys [2]string
	for i := 0; keys[0] == "" || keys[1] == ""; i++ {
		key := fmt.Sprintf("cow-crash-group-%03d", i)
		keys[xxhash.Sum64String(key)&1] = key
	}
	return keys[0], keys[1]
}

func cowPublicCrashCaptureFrames(t *testing.T, directory string) []cowPublicCrashFrame {
	t.Helper()
	layout, err := resolveOpenDirLayout(directory, false)
	if err != nil {
		t.Fatalf("resolve actual public storage layout: %v", err)
	}
	paths, err := filepath.Glob(filepath.Join(backenddb.WALDirPath(layout.mainDir), "commit-*.log"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("command WAL paths=%v err=%v", paths, err)
	}
	frames, err := commitlog.ScanCommandFrameSegments(paths, commitlog.Options{})
	if err != nil {
		t.Fatalf("scan acknowledged command WAL: %v", err)
	}
	var recorded []cowPublicCrashFrame
	for _, frame := range frames {
		if frame.PayloadFormat != commitlog.PayloadFormatRawKVBatchV1 && frame.PayloadFormat != commitlog.PayloadFormatRawKVBatchV2 {
			continue
		}
		ops, err := commitlog.DecodeRawKVBatchPayload(frame.Payload)
		if err != nil {
			t.Fatalf("decode LSN%d: %v", frame.LSN, err)
		}
		sum := sha256.Sum256(frame.Payload)
		recorded = append(recorded, cowPublicCrashFrame{frame.LSN, frame.Version, frame.DurabilityClass, hex.EncodeToString(sum[:]), ops})
	}
	return recorded
}

// This helper exits immediately after its public acknowledgements and diagnostic
// receipt. It deliberately never invokes DB.Close or an explicit Checkpoint.
func TestCOWPublicCrashHelper(t *testing.T) {
	if os.Getenv("TREEDB_COW_PUBLIC_CRASH_HELPER") != "1" {
		return
	}
	directory := os.Getenv("TREEDB_COW_PUBLIC_CRASH_DIR")
	profile := Profile(os.Getenv("TREEDB_COW_PUBLIC_CRASH_PROFILE"))
	explicit := os.Getenv("TREEDB_COW_PUBLIC_CRASH_SYNC") == "1"
	if directory == "" {
		t.Fatal("missing subprocess database directory")
	}
	database, err := Open(cowPublicCrashOptions(profile, directory))
	if err != nil {
		t.Fatalf("actual public cow_btree Open: %v", err)
	}
	receipt := cowPublicCrashReceipt{Profile: profile, ExplicitSync: explicit, CommitBefore: database.backend.State().CommitSeq}
	seed := bytes.Repeat([]byte("seed-pointer/"), 1024)
	if explicit {
		err = database.SetSync([]byte("seed"), seed)
	} else {
		err = database.Set([]byte("seed"), seed)
	}
	if err != nil {
		t.Fatalf("acknowledged public seed write: %v", err)
	}
	for _, key := range []string{"overwrite", "deleted"} {
		if err := database.Set([]byte(key), bytes.Repeat([]byte("older/"), 1024)); err != nil {
			t.Fatal(err)
		}
	}
	left, right := cowPublicCrashShardKeys()
	leftValue := bytes.Repeat([]byte("left-group-pointer/"), 512)
	rightValue := bytes.Repeat([]byte("right-group-pointer/"), 512)
	overwritten := bytes.Repeat([]byte("overwrite-pointer/"), 512)
	group := database.NewBatch()
	if group == nil {
		t.Fatal("public batch unavailable")
	}
	expected := []cowPublicCrashEntry{
		{Key: left, Value: leftValue, Found: true},
		{Key: right, Value: rightValue, Found: true},
		{Key: "overwrite", Value: overwritten, Found: true},
		{Key: "empty", Value: []byte{}, Found: true},
		{Key: "deleted", Found: false},
	}
	for _, entry := range expected {
		receipt.GroupKeys = append(receipt.GroupKeys, entry.Key)
		if entry.Found {
			err = group.Set([]byte(entry.Key), entry.Value)
		} else {
			err = group.Delete([]byte(entry.Key))
		}
		if err != nil {
			t.Fatalf("stage public group: %v", err)
		}
	}
	if explicit {
		err = group.WriteSync()
	} else {
		err = group.Write()
	}
	if err != nil {
		t.Fatalf("acknowledged public group: %v", err)
	}
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	expected = append(expected, cowPublicCrashEntry{Key: "seed", Value: seed, Found: true})
	snapshot := database.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("acknowledged public snapshot unavailable")
	}
	for i := range expected {
		if !expected[i].Found {
			continue
		}
		entry, err := snapshot.GetEntry([]byte(expected[i].Key))
		if err != nil {
			t.Fatalf("acknowledged public pointer metadata %q: %v", expected[i].Key, err)
		}
		expected[i].IsPtr, expected[i].Pointer = entry.Flags&node.FlagPointer != 0, entry.ValuePtr
		if len(expected[i].Value) > 0 && (!expected[i].IsPtr || entry.ValuePtr == (page.ValuePtr{})) {
			t.Fatalf("forced pointer metadata missing for %q: %+v", expected[i].Key, entry)
		}
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	for i := range expected {
		value, revision, err := database.GetVersioned([]byte(expected[i].Key))
		if expected[i].Found {
			if err != nil || !bytes.Equal(value, expected[i].Value) || (len(value) == 0 && value == nil) || revision == LegacyEntryRevision {
				t.Fatalf("acknowledged live entry %q value len%d revision%d err%v", expected[i].Key, len(value), revision, err)
			}
		} else {
			// DB reads map absence to nil/nil; snapshot reads return ErrKeyNotFound.
			if err != nil || value != nil {
				t.Fatalf("acknowledged physical delete %q value%q err%v", expected[i].Key, value, err)
			}
			if found, err := database.Has([]byte(expected[i].Key)); err != nil || found {
				t.Fatalf("acknowledged delete Has=%t err%v", found, err)
			}
		}
		expected[i].Revision = revision
	}
	state := database.backend.State()
	receipt.AppliedBeforeExit, receipt.CommitAfter = state.AppliedCommandLSN, state.CommitSeq
	if profile != ProfileNoWALFast {
		receipt.MaxLSN, err = strconv.ParseUint(database.Stats()["treedb.command_wal.max_lsn"], 10, 64)
		if err != nil || receipt.MaxLSN <= receipt.AppliedBeforeExit {
			t.Fatalf("fixture not uncheckpointed: maxLSN%d applied%d err%v", receipt.MaxLSN, receipt.AppliedBeforeExit, err)
		}
		receipt.Frames = cowPublicCrashCaptureFrames(t, directory)
		latest := make(map[string]commitlog.RawKVOperation)
		groupSeen := false
		for _, frame := range receipt.Frames {
			for _, op := range frame.Operations {
				latest[string(op.Key)] = op
			}
			if len(frame.Operations) == len(receipt.GroupKeys) {
				all := true
				for _, key := range receipt.GroupKeys {
					found := false
					for _, op := range frame.Operations {
						if string(op.Key) == key {
							found = true
						}
					}
					all = all && found
				}
				if all {
					groupSeen = true
					want := commitlog.CommandDurabilityRelaxed
					if explicit || profile == ProfileCommandWALDurable {
						want = commitlog.CommandDurabilityDurable
					}
					if frame.Version != commitlog.CommandFrameVersionV2 || frame.Durability != want {
						t.Fatalf("group frame version%d durability%d want%d", frame.Version, frame.Durability, want)
					}
				}
			}
		}
		if !groupSeen {
			t.Fatal("mixed-shard group was not one canonical WAL frame")
		}
		for i := range expected {
			op, ok := latest[expected[i].Key]
			if !ok || op.Revision != uint64(expected[i].Revision) {
				t.Fatalf("live/frame revision mismatch %q live%d op%+v", expected[i].Key, expected[i].Revision, op)
			}
			if !expected[i].Found {
				if op.Op != commitlog.RawKVOpDelete {
					t.Fatalf("physical delete frame=%+v", op)
				}
			} else if len(expected[i].Value) > 0 {
				if op.RID == 0 || (op.Op != commitlog.RawKVOpSetRID && op.Op != commitlog.RawKVOpSetMaterializedRID) {
					t.Fatalf("pointer canonical RID missing %q op%+v", expected[i].Key, op)
				}
				if op.Op == commitlog.RawKVOpSetMaterializedRID && !bytes.Equal(op.Value, expected[i].Value) {
					t.Fatalf("materialized pointer payload mismatch %q", expected[i].Key)
				}
				expected[i].RID = op.RID
			}
		}
	} else if receipt.CommitAfter <= receipt.CommitBefore {
		t.Fatal("NoWAL sync did not publish a backend root")
	}
	receipt.Entries = expected
	encoded, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "cow-process-crash-receipt.json"), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("COW_PROCESS_CRASH_RECEIPT %s\n", encoded)
	os.Exit(0)
}

func cowPublicCrashVerifyReopen(t *testing.T, directory string, receipt cowPublicCrashReceipt) {
	t.Helper()
	database, err := Open(cowPublicCrashOptions(receipt.Profile, directory))
	if err != nil {
		t.Fatalf("actual public reopen after process exit: %v", err)
	}
	defer database.Close()
	snapshot := database.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("public recovered snapshot unavailable")
	}
	defer snapshot.Close()
	persisted := database.backend.AcquireSnapshot()
	defer persisted.Close()
	for _, expected := range receipt.Entries {
		value, revision, err := snapshot.GetVersioned([]byte(expected.Key))
		if !expected.Found {
			if !errors.Is(err, ErrKeyNotFound) {
				t.Fatalf("recovered delete %q value%q err%v", expected.Key, value, err)
			}
			if found, err := snapshot.Has([]byte(expected.Key)); err != nil || found {
				t.Fatalf("recovered delete Has=%t err%v", found, err)
			}
			continue // Physical replay may remove the stored tombstone/revision entirely.
		}
		if err != nil || !bytes.Equal(value, expected.Value) || (len(value) == 0 && value == nil) || revision != expected.Revision {
			t.Fatalf("recovered %q value len%d revision%d want%d err%v", expected.Key, len(value), revision, expected.Revision, err)
		}
		if len(value) > 0 {
			entry, err := persisted.GetEntry([]byte(expected.Key))
			if err != nil || entry.Flags&node.FlagPointer == 0 || entry.ValuePtr == (page.ValuePtr{}) || entry.Revision != expected.Revision {
				t.Fatalf("recovered persisted pointer %q entry%+v err%v", expected.Key, entry, err)
			}
			if expected.RID != 0 {
				file := persisted.State().ValueLogSet.Files[entry.ValuePtr.FileID]
				if file == nil {
					t.Fatalf("recovered pointer %q lacks registered file", expected.Key)
				}
				rid, err := file.ReadRIDUnverified(entry.ValuePtr)
				if err != nil || rid != expected.RID {
					t.Fatalf("recovered pointer RID %q=%d want%d err%v", expected.Key, rid, expected.RID, err)
				}
			}
		}
	}
	if receipt.Profile != ProfileNoWALFast && database.backend.State().AppliedCommandLSN < receipt.MaxLSN {
		t.Fatalf("replay AppliedCommandLSN%d < acknowledged maxLSN%d", database.backend.State().AppliedCommandLSN, receipt.MaxLSN)
	}
}

func TestCOWPublicCrashAcknowledgedSyncReopen(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) { cowPublicCrashRun(t, profile, true) })
	}
}

// This is a process/kernel-drain witness. Ordinary relaxed acknowledgement is
// deliberately not described as surviving physical power loss.
func TestCOWPublicCrashRelaxedOrdinaryReopen(t *testing.T) {
	cowPublicCrashRun(t, ProfileCommandWALRelaxed, false)
}

func cowPublicCrashRun(t *testing.T, profile Profile, explicit bool) {
	t.Helper()
	directory := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^TestCOWPublicCrashHelper$", "-test.v")
	syncFlag := "0"
	if explicit {
		syncFlag = "1"
	}
	command.Env = append(os.Environ(), "TREEDB_COW_PUBLIC_CRASH_HELPER=1", "TREEDB_COW_PUBLIC_CRASH_DIR="+directory, "TREEDB_COW_PUBLIC_CRASH_PROFILE="+string(profile), "TREEDB_COW_PUBLIC_CRASH_SYNC="+syncFlag)
	output, err := command.CombinedOutput()
	t.Logf("process-exit helper profile%s explicitSync%t output:\n%s", profile, explicit, output)
	if err != nil {
		t.Fatalf("process-exit helper: %v context%v", err, ctx.Err())
	}
	encoded, err := os.ReadFile(filepath.Join(directory, "cow-process-crash-receipt.json"))
	if err != nil {
		t.Fatal(err)
	}
	var receipt cowPublicCrashReceipt
	if err := json.Unmarshal(encoded, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Profile != profile || receipt.ExplicitSync != explicit {
		t.Fatalf("subprocess receipt identity mismatch: %+v", receipt)
	}
	cowPublicCrashVerifyReopen(t, directory, receipt)
	cowPublicCrashVerifyReopen(t, directory, receipt)
}
