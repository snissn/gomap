package caching

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

func TestCOWAutomaticCheckpointSealsWALSuffixForRecovery(t *testing.T) {
	mount, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ext4 := commitlog.IsExt4WALFilesystem(mount)
	if err := mount.Close(); err != nil {
		t.Fatal(err)
	}
	if !ext4 {
		t.Skip("requires the relaxed standalone ext4 stream policy")
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"intact", "torn-sealed", "torn-later-relaxed"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(exe, "-test.run=^TestHelperCOWAutomaticCheckpointCrash$")
			cmd.Env = append(os.Environ(), "TREEDB_TEST_COW_CHECKPOINT_CRASH_DIR="+dir, "TREEDB_TEST_COW_CHECKPOINT_CRASH_MODE="+mode)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("checkpoint crash writer: %v\n%s", err, out)
			}
			receipt, err := os.ReadFile(filepath.Join(dir, "target-lsn"))
			if err != nil {
				t.Fatal(err)
			}
			target, err := strconv.ParseUint(string(receipt), 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			opts := cowCheckpointRecoveryOptions(dir)
			path, start, end := findCOWCheckpointRecoveryFrame(t, backenddb.WALDirPath(opts.Dir), target)
			var torn []byte
			if mode != "intact" {
				if err := os.Truncate(path, end-1); err != nil {
					t.Fatal(err)
				}
				env, _, err := commitlog.InspectCommandFrameV2TerminalTail(path, start)
				if err != nil || env.LSN != target || env.DurabilityClass != commitlog.CommandDurabilityRelaxed {
					t.Fatalf("fixture did not create an identifiable torn relaxed frame at LSN %d: got LSN=%d durability=%d, err=%v", target, env.LSN, env.DurabilityClass, err)
				}
				torn, err = os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
			}

			reopened, openErr := backenddb.OpenForNativeSemanticOwner(opts)
			if reopened != nil {
				defer func() {
					if err := reopened.Close(); err != nil {
						t.Error(err)
					}
				}()
			}
			if mode == "torn-sealed" {
				if !errors.Is(openErr, commitlog.ErrCommandWALTerminalTail) && !errors.Is(openErr, commitlog.ErrCorrupt) {
					if reopened != nil {
						value, err := reopened.Get([]byte("key"))
						t.Errorf("read-write reopen accepted torn checkpoint-synced LSN %d: value=%q, read err=%v", target, value, err)
					} else {
						t.Errorf("read-write reopen of torn checkpoint-synced LSN %d returned %v, want corruption", target, openErr)
					}
				}
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(after, torn) {
					t.Fatalf("reopen changed torn WAL bytes at LSN %d: before=%d after=%d, err=%v", target, len(torn), len(after), err)
				}
				return
			}
			if openErr != nil {
				t.Fatal(openErr)
			}
			if value, err := reopened.Get([]byte("key")); err != nil || string(value) != "suffix" {
				t.Fatalf("read-write reopen value=%q, want completed checkpoint suffix, err=%v", value, err)
			}
		})
	}
}

func TestHelperCOWAutomaticCheckpointCrash(t *testing.T) {
	dir := os.Getenv("TREEDB_TEST_COW_CHECKPOINT_CRASH_DIR")
	if dir == "" {
		t.Skip("subprocess helper")
	}
	t.Setenv(envDisableVlogGenerationLoop, "1")
	opts := cowCheckpointRecoveryOptions(dir)
	backend, err := backenddb.OpenForNativeSemanticOwner(opts)
	if err != nil {
		t.Fatal(err)
	}
	owner := transactionowner.New()
	if err := backend.BindPrivateCOWOwner(owner); err != nil {
		t.Fatal(err)
	}
	cache, err := Open(dir, backend, Options{
		TransactionOwner: owner, MemtableMode: opts.MemtableMode, ExternalCommandWAL: true,
		MemtableShards: 2, FlushThreshold: 1 << 30, ValueLogPointerThreshold: 1, ValueLogCompression: uint8(vlogCompressionOff),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.BindNativeSemanticBridge(owner, cache); err != nil {
		t.Fatal(err)
	}
	cache.SetCommandWALCheckpointCutoverHook(func() {})
	writeCOWAutoFallbackRaw(t, cache, backend, "first")
	if err := backend.RotateCommandWALActiveSegment(true); err != nil {
		t.Fatal(err)
	}
	writeCOWAutoFallbackRaw(t, cache, backend, "prefix")
	prefix := backend.CommandWALNextLSN() - 1
	var suffix uint64
	cache.testAfterCheckpointFrontierCapture = func() {
		writeCOWAutoFallbackRaw(t, cache, backend, "suffix")
		suffix = backend.CommandWALNextLSN() - 1
	}
	if err := cache.checkpointContext(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	cache.testAfterCheckpointFrontierCapture = nil
	cache.runPendingCommandWALCleanup()
	w, err := strconv.ParseUint(backend.Stats()["treedb.command_wal.durable_wal_lsn"], 10, 64)
	if state, live := backend.StateToken(); err != nil || !live || state.AppliedCommandLSN != prefix || suffix <= prefix || w < suffix {
		t.Fatalf("checkpoint did not sync the WAL-only suffix: root=%+v prefix=%d suffix=%d W=%d, err=%v", state, prefix, suffix, w, err)
	}
	target := suffix
	if os.Getenv("TREEDB_TEST_COW_CHECKPOINT_CRASH_MODE") == "torn-later-relaxed" {
		writeCOWAutoFallbackRaw(t, cache, backend, "later")
		target = backend.CommandWALNextLSN() - 1
		w, err := strconv.ParseUint(backend.Stats()["treedb.command_wal.durable_wal_lsn"], 10, 64)
		if err != nil || target <= w {
			t.Fatalf("later relaxed frame LSN=%d W=%d, err=%v", target, w, err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "target-lsn"), []byte(strconv.FormatUint(target, 10)), 0600); err != nil {
		t.Fatal(err)
	}
	// Exit before Close can checkpoint the suffix and hide the recovery path.
	os.Exit(0)
}

func cowCheckpointRecoveryOptions(dir string) backenddb.Options {
	opts := backenddb.Options{
		Dir: filepath.Join(dir, "backend"), CommandWAL: true, DisableBackgroundPrune: true,
		ResolvedProfile: backenddb.ProfileCommandWALRelaxed, Durability: backenddb.DurabilityWALOnRelaxed,
		MemtableMode: "cow_btree", FreshRelaxedStandaloneWALStream: true,
	}
	opts.ValueLog.PointerThreshold = 4096
	return opts
}

func findCOWCheckpointRecoveryFrame(t *testing.T, dir string, target uint64) (string, int64, int64) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !commitlog.IsCommandSegmentName(entry.Name()) {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if start, end, found := cowCheckpointRecoveryFrameOffsets(t, path, target); found {
			return path, start, end
		}
	}
	t.Fatalf("WAL frame LSN %d is missing", target)
	return "", 0, 0
}

func cowCheckpointRecoveryFrameOffsets(t *testing.T, path string, target uint64) (int64, int64, bool) {
	t.Helper()
	r, err := commitlog.NewReaderWithOptions(path, commitlog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	}()
	var start int64
	for {
		env, err := r.ReadCommandFrameV2()
		if errors.Is(err, io.EOF) {
			return 0, 0, false
		}
		if err != nil {
			t.Fatal(err)
		}
		end, err := r.Offset()
		if err != nil {
			t.Fatal(err)
		}
		if env.LSN == target {
			if env.Kind != commitlog.CommandKindRawKVBatch || env.DurabilityClass != commitlog.CommandDurabilityRelaxed {
				t.Fatalf("expected relaxed raw frame at LSN %d, got kind=%d durability=%d", target, env.Kind, env.DurabilityClass)
			}
			return start, end, true
		}
		start = end
	}
}
