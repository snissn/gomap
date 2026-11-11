//go:build linux

package main

import (
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/kvstore"
)

type processUsageCheckpointDB struct {
	*batchDeleteRangeMemoryDB
	checkpointErr error
	closeErr      error
}

func (d *processUsageCheckpointDB) Checkpoint() error {
	*d.events = append(*d.events, "checkpoint")
	return d.checkpointErr
}

func (d *processUsageCheckpointDB) Close() error {
	*d.events = append(*d.events, "db_close")
	return errors.Join(d.batchDeleteRangeMemoryDB.Close(), d.closeErr)
}

func runProcessUsageFixture(t *testing.T, selected bool, failRead int, checkpointErr, writeErr, closeErr error, keepDir ...bool) ([]string, *processUsageReceipt, int, error) {
	t.Helper()
	var events []string
	var receipt *processUsageReceipt
	reads := 0
	var openedDir string
	keep := len(keepDir) == 1 && keepDir[0]
	t.Setenv("TMPDIR", t.TempDir())
	name := "process_usage_" + strings.ToLower(strings.ReplaceAll(t.Name(), "/", "_"))
	RegisterHiddenDB(name, func(dir string) (kvstore.DB, error) {
		openedDir = dir
		d := newBatchDeleteRangeMemoryDB(name)
		d.events = &events
		return &processUsageCheckpointDB{batchDeleteRangeMemoryDB: d, checkpointErr: checkpointErr, closeErr: closeErr}, nil
	})
	path := ""
	if selected {
		path = filepath.Join(t.TempDir(), "usage.json")
	}
	t.Setenv(processUsageEnv, path)
	hooks := &benchmarkProfileHooks{
		startCPUProfile: func(io.Writer) error {
			events = append(events, "cpu_start")
			return nil
		},
		stopCPUProfile: func() {
			events = append(events, "cpu_stop")
		},
		readProcessUsage: func() (processUsageCounters, error) {
			reads++
			events = append(events, "usage_read")
			if reads == failRead {
				return processUsageCounters{}, errProcessUsageFixtureRead
			}
			return processUsageCounters{UserCPUNS: int64(reads), SystemCPUNS: int64(reads)}, nil
		},
		writeProcessUsage: func(_ string, r *processUsageReceipt) error {
			events = append(events, "usage_write")
			copy := *r
			receipt = &copy
			return writeErr
		},
	}
	_, err := runBenchmark(BenchConfig{Keys: 64, ValueSize: 8, BatchSize: 16, DBsArg: name, TestsArg: "batch_write_steady", SeedUsed: 1, KeepDir: keep, CPUProfile: filepath.Join(t.TempDir(), "cpu"), profileHooks: hooks})
	if selected {
		closes := 0
		for _, event := range events {
			if event == "db_close" {
				closes++
			}
		}
		if closes != 1 {
			t.Fatalf("selected mode must attempt original Close once: %v", events)
		}
		_, statErr := os.Stat(openedDir)
		if keep || closeErr != nil {
			if statErr != nil {
				t.Fatalf("kept or failed-Close directory must remain: %v", statErr)
			}
		} else if !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("successfully closed directory leaked: %s: %v", openedDir, statErr)
		}
	}
	return events, receipt, reads, err
}

var errProcessUsageFixtureRead = errors.New("injected usage reader error")

func TestRunBenchmarkProcessUsageDefaultZero(t *testing.T) {
	events, receipt, reads, err := runProcessUsageFixture(t, false, 1, nil, nil, nil)
	if err != nil || reads != 0 || receipt != nil {
		t.Fatalf("default reads=%d receipt=%+v err=%v", reads, receipt, err)
	}
	if len(events) == 0 {
		t.Fatal("original work did not run")
	}
}

func TestRunBenchmarkProcessUsageTwoReadsCheckpointOrder(t *testing.T) {
	events, receipt, reads, err := runProcessUsageFixture(t, true, 0, nil, nil, nil)
	if err != nil || reads != 2 || receipt == nil || !receipt.Completed || receipt.Delta == nil || receipt.Delta.UserCPUNS != 1 || receipt.IntervalWallLowerNS == nil || receipt.IntervalWallUpperNS == nil || *receipt.IntervalWallUpperNS < *receipt.IntervalWallLowerNS {
		t.Fatalf("reads=%d receipt=%+v err=%v", reads, receipt, err)
	}
	var selected []string
	sets := 0
	for _, event := range events {
		if event == "set" {
			sets++
			continue
		}
		selected = append(selected, event)
		if event == "usage_write" {
			break // Original post-run maintenance is outside this receipt.
		}
	}
	want := []string{"cpu_start", "usage_read", "checkpoint", "usage_read", "cpu_stop", "usage_write"}
	if sets != 64 || !reflect.DeepEqual(selected, want) {
		t.Fatalf("original sets=%d order=%v, want=%v", sets, selected, want)
	}
	for _, sample := range receipt.Samples {
		if sample == nil || sample.Counters == nil || sample.ReadFailed || sample.ReadWallNS < 0 || sample.ReadFinishedUnixNS < sample.ReadStartedUnixNS {
			t.Fatalf("unbounded/missing successful read: %+v", sample)
		}
	}
}

func TestRunBenchmarkProcessUsageOriginalFailureWins(t *testing.T) {
	original := errors.New("original final checkpoint failure")
	writeFailure := errors.New("secondary receipt write failure")
	closeFailure := errors.New("secondary receipt close failure")
	removeFailure := errors.New("secondary receipt remove failure")
	writer := errors.Join(writeFailure, closeFailure, removeFailure)
	events, receipt, reads, err := runProcessUsageFixture(t, true, 0, original, writer, nil)
	if !errors.Is(err, original) || !errors.Is(err, writer) || reads != 1 || receipt == nil || receipt.Completed || receipt.Delta != nil || receipt.Samples[1] != nil {
		t.Fatalf("reads=%d receipt=%+v err=%v", reads, receipt, err)
	}
	// This tests propagation of cleanup errors from the existing hook, not OS failure injection.
	for _, secondary := range []error{writeFailure, closeFailure, removeFailure} {
		if !errors.Is(err, secondary) || strings.Index(err.Error(), original.Error()) >= strings.Index(err.Error(), secondary.Error()) {
			t.Fatalf("original error must be first and each secondary retained: %v", err)
		}
	}
	var stop, write int
	for i, event := range events {
		if event == "cpu_stop" {
			stop = i
		}
		if event == "usage_write" {
			write = i
		}
	}
	if stop == 0 || write <= stop {
		t.Fatalf("original CPU stop/write order=%v", events)
	}
}

func TestRunBenchmarkProcessUsageReaderError(t *testing.T) {
	for _, attempt := range []int{1, 2} {
		name := "first"
		if attempt == 2 {
			name = "second"
		}
		t.Run(name, func(t *testing.T) {
			events, receipt, reads, err := runProcessUsageFixture(t, true, attempt, nil, nil, nil)
			if !errors.Is(err, errProcessUsageFixtureRead) || reads != attempt || receipt == nil || receipt.Completed || receipt.Delta != nil || !receipt.Samples[attempt-1].ReadFailed || receipt.Samples[attempt-1].Counters != nil {
				t.Fatalf("reads=%d receipt=%+v err=%v", reads, receipt, err)
			}
			stops, sets, checkpoints := 0, 0, 0
			for _, event := range events {
				if event == "cpu_stop" {
					stops++
				}
				if event == "set" {
					sets++
				}
				if event == "checkpoint" {
					checkpoints++
				}
			}
			if stops != 1 || (attempt == 1 && sets != 0) || (attempt == 2 && (sets != 64 || checkpoints != 1)) {
				t.Fatalf("read failure changed original order: %v", events)
			}
		})
	}
}

func TestProcessUsageTimevalBounds(t *testing.T) {
	if got, err := processUsageTimevalNS(1, 2); err != nil || got != 1000002000 {
		t.Fatalf("timeval=(%d,%v)", got, err)
	}
	for _, pair := range [][2]int64{{-1, 0}, {0, -1}, {0, 1000000}, {math.MaxInt64, 0}} {
		if _, err := processUsageTimevalNS(pair[0], pair[1]); err == nil {
			t.Fatalf("invalid timeval accepted: %v", pair)
		}
	}
}

func TestRunBenchmarkProcessUsageCloseFailureIsNotLifecycleSuccess(t *testing.T) {
	originalClose := errors.New("original DB Close failure")
	events, receipt, reads, err := runProcessUsageFixture(t, true, 0, nil, nil, originalClose)
	if !errors.Is(err, originalClose) || reads != 2 || receipt == nil || !receipt.Completed || receipt.Scope != "WORK_INTERVAL" || receipt.Status != "COMPLETE_WORK_INTERVAL_DIAGNOSTIC_ONLY" || receipt.Delta == nil {
		t.Fatalf("interval completion must coexist with failed original Close: reads=%d receipt=%+v err=%v", reads, receipt, err)
	}
	writeAt, closeAt := -1, -1
	for i, event := range events {
		if event == "usage_write" {
			writeAt = i
		}
		if event == "db_close" {
			closeAt = i
		}
	}
	if writeAt < 0 || closeAt <= writeAt {
		t.Fatalf("receipt must not move or certify original DB Close: %v", events)
	}
}

func TestRunBenchmarkProcessUsageCleanup(t *testing.T) {
	for _, name := range []string{"write_failure", "write_and_close_failure", "read_and_close_failure", "keep_dir", "normal"} {
		t.Run(name, func(t *testing.T) {
			var writeErr, closeErr error
			if name == "write_failure" || name == "write_and_close_failure" {
				writeErr = errors.New("receipt write failure")
			}
			if name == "write_and_close_failure" || name == "read_and_close_failure" {
				closeErr = errors.New("DB Close failure")
			}
			failRead := 0
			primary := writeErr
			if name == "read_and_close_failure" {
				failRead = 1
				primary = errProcessUsageFixtureRead
			}
			_, receipt, reads, err := runProcessUsageFixture(t, true, failRead, nil, writeErr, closeErr, name == "keep_dir")
			if receipt == nil || (failRead == 0 && (!receipt.Completed || reads != 2)) || (failRead != 0 && (receipt.Completed || reads != 1)) {
				t.Fatalf("unexpected interval: reads=%d receipt=%+v", reads, receipt)
			}
			if writeErr == nil && closeErr == nil && err != nil {
				t.Fatal(err)
			}
			if primary != nil && !errors.Is(err, primary) {
				t.Fatalf("lost original diagnostic failure: %v", err)
			}
			if closeErr != nil && (!errors.Is(err, closeErr) || strings.Index(err.Error(), primary.Error()) >= strings.Index(err.Error(), closeErr.Error())) {
				t.Fatalf("original error must precede cleanup Close failure: %v", err)
			}
		})
	}
}

func TestProcessUsageCleanupRemoveFailure(t *testing.T) {
	var events []string
	db := newBatchDeleteRangeMemoryDB("usage_remove")
	db.events = &events
	owner := &processUsageDBOwner{instance: &DBInstance{Name: "usage_remove", Wrapper: &processUsageCheckpointDB{batchDeleteRangeMemoryDB: db}, Dir: "\x00"}}
	if err := owner.cleanup(false); err == nil || !owner.closeOK || !owner.removeAttempted {
		t.Fatalf("actual invalid-path RemoveAll error lost: %v", err)
	}
	if err := owner.cleanup(false); err != nil {
		t.Fatalf("cleanup operation was retried: %v", err)
	}
	closes := 0
	for _, event := range events {
		if event == "db_close" {
			closes++
		}
	}
	if closes != 1 {
		t.Fatalf("Close attempts=%d", closes)
	}
}

func TestRunBenchmarkProcessUsageCollisionBeforeOpen(t *testing.T) {
	name := "usage_collision_before_open"
	opens := 0
	RegisterHiddenDB(name, func(string) (kvstore.DB, error) {
		opens++
		return nil, errors.New("factory must not be entered")
	})
	prefix := filepath.Join(t.TempDir(), "cpu")
	t.Setenv(processUsageEnv, prefix+"_batch_write_steady_"+name+".pprof")
	_, err := runBenchmark(BenchConfig{Keys: 64, ValueSize: 8, BatchSize: 16, DBsArg: name, TestsArg: "batch_write_steady", CPUProfile: prefix})
	if err == nil || opens != 0 {
		t.Fatalf("collision reached factory: opens=%d err=%v", opens, err)
	}
}

func TestProcessUsageOutputAlias(t *testing.T) {
	dir := t.TempDir()
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(dir, alias); err != nil {
		t.Fatal(err)
	}
	cfg := BenchConfig{CPUProfile: filepath.Join(alias, "cpu")}
	t.Setenv(processUsageEnv, filepath.Join(dir, "cpu_batch_write_steady_one.pprof"))
	if u, err := selectedProcessUsage(cfg, []string{"one"}, []string{"batch_write_steady"}, true, func() (processUsageCounters, error) { return processUsageCounters{}, nil }); err == nil || u != nil {
		t.Fatalf("aliased future output accepted: %v %v", u, err)
	}
}
