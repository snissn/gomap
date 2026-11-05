package main

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
)

func TestProcessUsageSelectionRefusal(t *testing.T) {
	for _, name := range []string{"unsupported", "relative", "multi_db", "multi_test", "no_cpu", "existing"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "receipt.json")
			cfg := BenchConfig{CPUProfile: filepath.Join(t.TempDir(), "cpu")}
			dbs, tests, supported := []string{"one"}, []string{"batch_write_steady"}, true
			switch name {
			case "unsupported":
				supported = false
			case "relative":
				path = "relative.json"
			case "multi_db":
				dbs = append(dbs, "two")
			case "multi_test":
				tests = append(tests, "batch_write")
			case "no_cpu":
				cfg.CPUProfile = ""
			case "existing":
				if err := writeProcessUsageReceipt(path, &processUsageReceipt{}); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv(processUsageEnv, path)
			reads := 0
			u, err := selectedProcessUsage(cfg, dbs, tests, supported, func() (processUsageCounters, error) {
				reads++
				return processUsageCounters{}, nil
			})
			if err == nil || u != nil || reads != 0 {
				t.Fatalf("selection=(%v,%v) reads=%d", u, err, reads)
			}
		})
	}
}

func TestProcessUsageCounterRegression(t *testing.T) {
	base := processUsageCounters{10, 10, 10, 10, 10, 10}
	for i, name := range []string{"user_cpu", "system_cpu", "minor_faults", "major_faults", "voluntary", "involuntary"} {
		t.Run(name, func(t *testing.T) {
			after := base
			fields := []*int64{&after.UserCPUNS, &after.SystemCPUNS, &after.MinorFaults, &after.MajorFaults, &after.VoluntaryContextSwitches, &after.InvoluntaryContextSwitches}
			*fields[i] = 9
			if _, err := processUsageDelta(base, after); err == nil {
				t.Fatal("regression accepted")
			}
		})
	}
	if _, err := processUsageDelta(processUsageCounters{UserCPUNS: -1}, base); err == nil {
		t.Fatal("negative counter accepted")
	}
	if delta, err := processUsageDelta(base, base); err != nil || delta != (processUsageCounters{}) {
		t.Fatalf("honest zero delta=(%+v,%v)", delta, err)
	}
}

func TestProcessUsageIncompleteAndReadLimit(t *testing.T) {
	reads := 0
	u := &steadyProcessUsage{read: func() (processUsageCounters, error) {
		reads++
		return processUsageCounters{}, nil
	}}
	if err := u.finish(nil); err == nil || u.receipt.Completed || u.receipt.Delta != nil {
		t.Fatal("missing reads presented as complete")
	}
	for i := 0; i < 2; i++ {
		if err := u.sample(); err != nil {
			t.Fatal(err)
		}
	}
	if err := u.sample(); err == nil || reads != 2 {
		t.Fatalf("third read not refused: reads=%d err=%v", reads, err)
	}
	original := errors.New("original failure")
	if err := u.finish(original); err != original || u.receipt.Completed || u.receipt.Delta != nil {
		t.Fatal("diagnostic failure replaced original error or produced complete delta")
	}
}

func TestProcessUsageSampleRegressionFailsClosed(t *testing.T) {
	reads := 0
	u := &steadyProcessUsage{read: func() (processUsageCounters, error) {
		reads++
		return processUsageCounters{UserCPUNS: int64(3 - reads)}, nil
	}}
	if err := u.sample(); err != nil {
		t.Fatal(err)
	}
	if err := u.sample(); err == nil {
		t.Fatal("regressing reader accepted")
	}
	if err := u.finish(nil); err == nil || u.receipt.Completed || u.receipt.Delta != nil || reads != 2 || u.receipt.Samples[1].Counters == nil {
		t.Fatalf("regression not retained/fail-closed: receipt=%+v reads=%d err=%v", u.receipt, reads, err)
	}
}

func TestProcessUsageOutputCollision(t *testing.T) {
	for _, name := range []string{"cpu", "allocs", "checkpoint", "block", "block_delta", "mutex", "mutex_delta", "trace", "results_json", "results_md", "insights_json", "insights_md", "insights_html", "output_parent", "distinct"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			oldProfileDir := *profileDir
			*profileDir = dir
			t.Cleanup(func() { *profileDir = oldProfileDir })
			cfg := BenchConfig{CPUProfile: filepath.Join(dir, "cpu"), AllocsProfile: filepath.Join(dir, "allocs"), CheckpointCPUProfile: filepath.Join(dir, "checkpoint_cpu"), BlockProfile: filepath.Join(dir, "block.pprof"), MutexProfile: filepath.Join(dir, "mutex.pprof"), TraceProfile: filepath.Join(dir, "trace.out")}
			paths := map[string]string{
				"cpu":           "cpu_batch_write_steady_one.pprof",
				"allocs":        "allocs_batch_write_steady_one.pprof",
				"checkpoint":    "checkpoint_cpu_checkpoint_post-run_different_wrapper.pprof",
				"block":         "block.pprof",
				"block_delta":   "block_batch_write_steady_one.pprof",
				"mutex":         "mutex.pprof",
				"mutex_delta":   "mutex_batch_write_steady_one.pprof",
				"trace":         "trace.out",
				"results_json":  "benchprof_results.json",
				"results_md":    "benchprof_results.md",
				"insights_json": "insights.json",
				"insights_md":   "insights.md",
				"insights_html": "insights.html",
				"output_parent": "future",
				"distinct":      "usage.json",
			}
			if name == "output_parent" {
				cfg.CPUProfile = filepath.Join(dir, "future", "cpu")
			}
			t.Setenv(processUsageEnv, filepath.Join(dir, paths[name]))
			reads := 0
			u, err := selectedProcessUsage(cfg, []string{"one"}, []string{"batch_write_steady"}, true, func() (processUsageCounters, error) {
				reads++
				return processUsageCounters{}, nil
			})
			if reads != 0 || (name == "distinct" && (err != nil || u == nil)) || (name != "distinct" && (err == nil || u != nil)) {
				t.Fatalf("collision %s: usage=%v err=%v reads=%d", name, u, err, reads)
			}
		})
	}
}

func TestProcessUsageOutputPathAliases(t *testing.T) {
	for _, name := range []string{"trim_block", "trim_mutex", "trim_trace", "trim_block_delta", "parent_component", "receipt_parent_component", "checkpoint_literal_symlink", "checkpoint_generated_symlink", "checkpoint_nonmatching_symlink", "dangling_output_symlink", "distinct"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			oldProfileDir := *profileDir
			*profileDir = ""
			t.Cleanup(func() { *profileDir = oldProfileDir })
			receipt := filepath.Join(dir, "usage.json")
			cfg := BenchConfig{CPUProfile: filepath.Join(dir, "cpu")}
			switch name {
			case "trim_block":
				cfg.BlockProfile = "  " + receipt + "  "
			case "trim_mutex":
				cfg.MutexProfile = "  " + receipt + "  "
			case "trim_trace":
				cfg.TraceProfile = "  " + receipt + "  "
			case "trim_block_delta":
				cfg.BlockProfile = "  " + filepath.Join(dir, "block.pprof") + "  "
				receipt = filepath.Join(dir, "block_batch_write_steady_one.pprof")
			case "parent_component":
				real := filepath.Join(dir, "real")
				sub := filepath.Join(real, "sub")
				if err := os.MkdirAll(sub, 0700); err != nil {
					t.Fatal(err)
				}
				link := filepath.Join(dir, "link")
				if err := os.Symlink(sub, link); err != nil {
					if runtime.GOOS == "windows" && (os.IsPermission(err) || errors.Is(err, syscall.Errno(1314))) {
						t.Skipf("Windows symlink unavailable: %v", err)
					}
					t.Fatalf("create symlink: %v", err)
				}
				receipt = filepath.Join(real, "usage.json")
				cfg.TraceProfile = link + string(filepath.Separator) + ".." + string(filepath.Separator) + "usage.json"
			case "receipt_parent_component":
				receipt = dir + "/sub/../usage.json"
			case "checkpoint_literal_symlink":
				prefix := filepath.Join(dir, "checkpoint")
				target := filepath.Join(dir, "other")
				if err := os.WriteFile(target, []byte("existing"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, prefix); err != nil {
					if runtime.GOOS == "windows" && (os.IsPermission(err) || errors.Is(err, syscall.Errno(1314))) {
						t.Skipf("Windows symlink unavailable: %v", err)
					}
					t.Fatalf("create symlink: %v", err)
				}
				cfg.CheckpointCPUProfile = prefix
				receipt = prefix + "_checkpoint_post-run_wrapper.pprof"
			case "checkpoint_generated_symlink", "checkpoint_nonmatching_symlink":
				cfg.CheckpointCPUProfile = filepath.Join(dir, "checkpoint")
				filename := "checkpoint_checkpoint_post-run_wrapper.pprof"
				if name == "checkpoint_nonmatching_symlink" {
					filename = "other_checkpoint_post-run_wrapper.pprof"
				}
				if err := os.Symlink(receipt, filepath.Join(dir, filename)); err != nil {
					if runtime.GOOS == "windows" && (os.IsPermission(err) || errors.Is(err, syscall.Errno(1314))) {
						t.Skipf("Windows symlink unavailable: %v", err)
					}
					t.Fatalf("create symlink: %v", err)
				}
			case "dangling_output_symlink":
				output := filepath.Join(dir, "trace")
				if err := os.Symlink(receipt, output); err != nil {
					if runtime.GOOS == "windows" && (os.IsPermission(err) || errors.Is(err, syscall.Errno(1314))) {
						t.Skipf("Windows symlink unavailable: %v", err)
					}
					t.Fatalf("create symlink: %v", err)
				}
				cfg.TraceProfile = output
			}
			err := validateProcessUsageOutputPath(receipt, cfg, "one")
			if (name == "distinct" || name == "checkpoint_nonmatching_symlink") != (err == nil) {
				t.Fatalf("%s: err=%v", name, err)
			}
		})
	}
}
