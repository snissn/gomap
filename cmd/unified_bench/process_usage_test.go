package main

import (
	"errors"
	"path/filepath"
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
