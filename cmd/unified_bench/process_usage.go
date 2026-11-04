package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Private diagnostic opt-in. It adds no benchmark/gate schema or CLI option.
const processUsageEnv = "GOMAP_DIAGNOSTIC_STEADY_RUSAGE"

// RUSAGE_SELF covers this process's threads, not its children or pprof samples.
type processUsageCounters struct {
	UserCPUNS                  int64 `json:"user_cpu_ns"`
	SystemCPUNS                int64 `json:"system_cpu_ns"`
	MinorFaults                int64 `json:"minor_faults"`
	MajorFaults                int64 `json:"major_faults"`
	VoluntaryContextSwitches   int64 `json:"voluntary_context_switches"`
	InvoluntaryContextSwitches int64 `json:"involuntary_context_switches"`
}

type processUsageSample struct {
	ReadStartedUnixNS  int64                 `json:"read_started_unix_ns"`
	ReadFinishedUnixNS int64                 `json:"read_finished_unix_ns"`
	ReadWallNS         int64                 `json:"read_wall_ns"`
	ReadFailed         bool                  `json:"read_failed"`
	Counters           *processUsageCounters `json:"counters"`
}

type processUsageReceipt struct {
	Schema              int                    `json:"schema"`
	Scope               string                 `json:"scope"`
	Completed           bool                   `json:"completed"`
	Status              string                 `json:"status"`
	Attempts            int                    `json:"read_attempts"`
	Samples             [2]*processUsageSample `json:"samples"`
	Delta               *processUsageCounters  `json:"delta"`
	IntervalWallLowerNS *int64                 `json:"interval_wall_lower_ns"`
	IntervalWallUpperNS *int64                 `json:"interval_wall_upper_ns"`
}

type steadyProcessUsage struct {
	path string
	read func() (processUsageCounters, error)

	receipt         processUsageReceipt
	failure         error
	firstReadStart  time.Time
	firstReadFinish time.Time
}

func selectedProcessUsage(cfg BenchConfig, dbs, tests []string, supported bool, read func() (processUsageCounters, error)) (*steadyProcessUsage, error) {
	path := os.Getenv(processUsageEnv)
	if path == "" {
		return nil, nil
	}
	if !supported {
		return nil, errors.New("process usage diagnostic is supported only on Linux")
	}
	if !filepath.IsAbs(path) || len(dbs) != 1 || len(tests) != 1 || tests[0] != "batch_write_steady" || !shouldCPUProfile(cfg, "batch_write_steady") {
		return nil, errors.New("process usage diagnostic requires an absolute new receipt path, one DB, only batch_write_steady, and its CPU profile")
	}
	if _, err := os.Lstat(path); err == nil || !errors.Is(err, os.ErrNotExist) {
		return nil, errors.New("process usage receipt must not already exist and its path must be accessible")
	}
	if read == nil {
		return nil, errors.New("process usage reader unavailable")
	}
	return &steadyProcessUsage{
		path:    path,
		read:    read,
		receipt: processUsageReceipt{Schema: 1, Scope: "WORK_INTERVAL", Status: "INCOMPLETE"},
	}, nil
}

func (c processUsageCounters) valid() bool {
	return c.UserCPUNS >= 0 && c.SystemCPUNS >= 0 && c.MinorFaults >= 0 && c.MajorFaults >= 0 && c.VoluntaryContextSwitches >= 0 && c.InvoluntaryContextSwitches >= 0
}

func processUsageDelta(a, b processUsageCounters) (processUsageCounters, error) {
	if !a.valid() || !b.valid() || b.UserCPUNS < a.UserCPUNS || b.SystemCPUNS < a.SystemCPUNS || b.MinorFaults < a.MinorFaults || b.MajorFaults < a.MajorFaults || b.VoluntaryContextSwitches < a.VoluntaryContextSwitches || b.InvoluntaryContextSwitches < a.InvoluntaryContextSwitches {
		return processUsageCounters{}, errors.New("invalid or regressing process usage counters")
	}
	return processUsageCounters{
		UserCPUNS:                  b.UserCPUNS - a.UserCPUNS,
		SystemCPUNS:                b.SystemCPUNS - a.SystemCPUNS,
		MinorFaults:                b.MinorFaults - a.MinorFaults,
		MajorFaults:                b.MajorFaults - a.MajorFaults,
		VoluntaryContextSwitches:   b.VoluntaryContextSwitches - a.VoluntaryContextSwitches,
		InvoluntaryContextSwitches: b.InvoluntaryContextSwitches - a.InvoluntaryContextSwitches,
	}, nil
}

func (u *steadyProcessUsage) sample() error {
	if u.failure != nil {
		return u.failure
	}
	if u.receipt.Attempts >= len(u.receipt.Samples) {
		u.failure = errors.New("process usage diagnostic permits at most two reads")
		return u.failure
	}
	i := u.receipt.Attempts
	u.receipt.Attempts++
	before := time.Now()
	counters, err := u.read()
	after := time.Now()
	s := &processUsageSample{ReadStartedUnixNS: before.UnixNano(), ReadFinishedUnixNS: after.UnixNano(), ReadWallNS: after.Sub(before).Nanoseconds()}
	u.receipt.Samples[i] = s
	if i == 0 {
		u.firstReadStart, u.firstReadFinish = before, after
	}
	if err != nil {
		s.ReadFailed = true
		u.failure = fmt.Errorf("process usage read %d: %w", i+1, err)
		return u.failure
	}
	s.Counters = &counters
	if !counters.valid() || s.ReadWallNS < 0 || s.ReadFinishedUnixNS < s.ReadStartedUnixNS || (i == 1 && s.ReadStartedUnixNS < u.receipt.Samples[0].ReadFinishedUnixNS) {
		u.failure = errors.New("invalid process usage counters or wall bounds")
		return u.failure
	}
	if i == 1 {
		lower := before.Sub(u.firstReadFinish).Nanoseconds()
		upper := after.Sub(u.firstReadStart).Nanoseconds()
		if lower < 0 || upper < lower {
			u.failure = errors.New("invalid process usage interval wall bounds")
			return u.failure
		}
		u.receipt.IntervalWallLowerNS, u.receipt.IntervalWallUpperNS = &lower, &upper
		delta, deltaErr := processUsageDelta(*u.receipt.Samples[0].Counters, counters)
		if deltaErr != nil {
			u.failure = deltaErr
			return u.failure
		}
		u.receipt.Delta = &delta
	}
	return nil
}

// Called after finishCPUProfile. The original operation/checkpoint error wins.
func (u *steadyProcessUsage) finish(workErr error) error {
	u.receipt.Completed = false
	u.receipt.Status = "INCOMPLETE"
	if workErr != nil {
		u.receipt.Status = "ORIGINAL_WORK_OR_DIAGNOSTIC_FAILED"
		u.receipt.Delta = nil
		return workErr
	}
	if u.failure != nil {
		u.receipt.Delta = nil
		return u.failure
	}
	if u.receipt.Attempts != 2 || u.receipt.Samples[0] == nil || u.receipt.Samples[1] == nil || u.receipt.Delta == nil {
		return errors.New("incomplete process usage diagnostic")
	}
	u.receipt.Completed = true
	u.receipt.Status = "COMPLETE_WORK_INTERVAL_DIAGNOSTIC_ONLY"
	return nil
}

func writeProcessUsageReceipt(path string, receipt *processUsageReceipt) error {
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	if len(data) > 4096 {
		return errors.New("process usage receipt exceeds fixed 4096-byte bound")
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		return errors.Join(writeErr, closeErr, os.Remove(path))
	}
	return nil
}
