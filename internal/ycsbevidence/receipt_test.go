package ycsbevidence

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func requireLinuxProcessEvidence(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("live owner process evidence requires Linux procfs")
	}
}

func TestDisabledPreservesOriginalClose(t *testing.T) {
	t.Setenv("YCSB_SERVER_RECEIPT_DIR", "")
	observer, err := OpenFromEnv("", "", nil, nil)
	if err != nil || observer != nil {
		t.Fatal(observer, err)
	}
	cause := errors.New("original close failed")
	count := 0
	if err := observer.Close(func() error { count++; return cause }); !errors.Is(err, cause) || count != 1 {
		t.Fatal(count, err)
	}
}

func TestActualLiveOwnerReceiptAndOriginalClose(t *testing.T) {
	requireLinuxProcessEvidence(t)
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "cow", true: "legacy"}[legacy], func(t *testing.T) {
			dataDir, evidenceDir := t.TempDir(), t.TempDir()
			t.Setenv("YCSB_SERVER_RECEIPT_DIR", evidenceDir)
			t.Setenv("GOMAXPROCS", "") // Actual presence is recorded, never inferred from value.
			opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dataDir)
			opts.MemtableMode = "cow_btree"
			var observer *Observer
			var closeDB func() error
			if legacy {
				backend, cleanup, stats, err := treedb.OpenBackendWithCachedLeafLogStats(opts)
				if err != nil {
					t.Fatal(err)
				}
				closeDB = cleanup
				observer, err = OpenFromEnv("command_wal_relaxed", dataDir, backend, stats)
				if err != nil {
					cleanup()
					t.Fatal(err)
				}
			} else {
				database, err := treedb.Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				closeDB = database.Close
				backend, err := database.NativeSemanticBackend()
				if err != nil {
					database.Close()
					t.Fatal(err)
				}
				observer, err = OpenFromEnv("command_wal_relaxed", dataDir, backend, database.Stats)
				if err != nil {
					database.Close()
					t.Fatal(err)
				}
			}
			var opened Observation
			raw, err := os.ReadFile(filepath.Join(evidenceDir, "owner.open.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(raw, &opened); err != nil {
				t.Fatal(err)
			}
			expected := map[bool]string{false: "cow", true: "legacy"}[legacy]
			if opened.Backend != expected || opened.Process.PID != os.Getpid() || opened.Process.StartTicks == 0 || len(opened.Process.EXESHA256) != 64 || opened.Process.GOMAXPROCSUnset || opened.DataInode == 0 || opened.OwnerID == "" || opened.ObservedGOMAXPROCS < 1 {
				t.Fatalf("invalid observation %+v", opened)
			}
			count := 0
			if err = observer.Close(func() error { count++; return closeDB() }); err != nil || count != 1 {
				t.Fatal(count, err)
			}
			if _, err = os.Stat(filepath.Join(evidenceDir, "owner.close.json")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestObservationFailureStillClosesOriginalOnce(t *testing.T) {
	requireLinuxProcessEvidence(t)
	dataDir, evidenceDir := t.TempDir(), t.TempDir()
	t.Setenv("YCSB_SERVER_RECEIPT_DIR", evidenceDir)
	database, err := treedb.Open(func() treedb.Options {
		opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dataDir)
		opts.MemtableMode = "cow_btree"
		return opts
	}())
	if err != nil {
		t.Fatal(err)
	}
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	observer, err := OpenFromEnv("command_wal_relaxed", dataDir, backend, database.Stats)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	observer.stats = func() map[string]string { return map[string]string{"treedb.profile.resolved": "forged"} }
	count := 0
	cause := errors.New("original failure")
	err = observer.Close(func() error { count++; return errors.Join(database.Close(), cause) })
	if count != 1 || !errors.Is(err, cause) {
		t.Fatal(count, err)
	}
	if err == cause {
		t.Fatal("lost observation failure")
	}
}
