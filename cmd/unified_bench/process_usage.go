package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	if err := validateProcessUsageOutputPath(path, cfg, dbs[0]); err != nil {
		return nil, err
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

// Resolve existing parent aliases without creating any output. Later profile
// directories may not exist yet, so resolve their nearest existing ancestor.
func processUsageCanonicalPath(path string) (string, error) {
	return processUsageCanonicalPathDepth(path, 0)
}

func processUsageCanonicalPathDepth(path string, depth int) (string, error) {
	if depth > 40 {
		return "", errors.New("too many process usage path symlinks")
	}
	for _, component := range strings.Split(filepath.ToSlash(path), "/") {
		if component == ".." {
			return "", errors.New("process usage paths must not contain parent components")
		}
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	var tail []string
	for current := filepath.Clean(absolute); ; current = filepath.Dir(current) {
		resolved, err := filepath.EvalSymlinks(current)
		if err == nil {
			for i := len(tail) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, tail[i])
			}
			return resolved, nil
		}
		if !errors.Is(err, os.ErrNotExist) || filepath.Dir(current) == current {
			return "", err
		}
		info, statErr := os.Lstat(current)
		if statErr == nil && info.Mode()&os.ModeSymlink != 0 {
			target, readErr := os.Readlink(current)
			if readErr != nil {
				return "", readErr
			}
			for _, component := range strings.Split(filepath.ToSlash(target), "/") {
				if component == ".." {
					return "", errors.New("process usage symlink target contains parent components")
				}
			}
			if !filepath.IsAbs(target) {
				target = filepath.Join(filepath.Dir(current), target)
			}
			resolved, resolveErr := processUsageCanonicalPathDepth(target, depth+1)
			if resolveErr != nil {
				return "", resolveErr
			}
			for i := len(tail) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, tail[i])
			}
			return resolved, nil
		}
		if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return "", statErr
		}
		tail = append(tail, filepath.Base(current))
	}
}

func validateProcessUsageOutputPath(path string, cfg BenchConfig, dbName string) error {
	for _, raw := range []string{path, cfg.CPUProfile, cfg.AllocsProfile, cfg.CheckpointCPUProfile, cfg.BlockProfile, cfg.MutexProfile, cfg.TraceProfile, *profileDir} {
		for _, component := range strings.Split(filepath.ToSlash(strings.TrimSpace(raw)), "/") {
			if component == ".." {
				return errors.New("process usage paths must not contain parent components")
			}
		}
	}
	parent, err := os.Stat(filepath.Dir(path))
	if err != nil || !parent.IsDir() {
		return errors.New("process usage receipt requires an existing parent directory")
	}
	receipt, err := processUsageCanonicalPath(path)
	if err != nil {
		return fmt.Errorf("process usage receipt path: %w", err)
	}
	const testName = "batch_write_steady"
	paths := []string{strings.TrimSpace(cfg.BlockProfile), strings.TrimSpace(cfg.MutexProfile), strings.TrimSpace(cfg.TraceProfile)}
	if shouldCPUProfile(cfg, testName) {
		paths = append(paths, strings.TrimSpace(cfg.CPUProfile)+"_"+testName+"_"+dbName+".pprof")
	}
	if shouldAllocsProfile(cfg, testName) {
		paths = append(paths, strings.TrimSpace(cfg.AllocsProfile)+"_"+testName+"_"+dbName+".pprof")
	}
	for _, pair := range [][2]string{{cfg.BlockProfile, "block"}, {cfg.MutexProfile, "mutex"}} {
		if strings.TrimSpace(pair[0]) != "" {
			paths = append(paths, contentionProfilePath(strings.TrimSpace(pair[0]), pair[1], testName, dbName))
		}
	}
	// Checkpoint profiles use the wrapper's runtime Name. Reserve the configured
	// checkpoint namespace before opening it; do not guess that Name from dbName.
	if prefix := strings.TrimSpace(cfg.CheckpointCPUProfile); prefix != "" {
		parent, err := processUsageCanonicalPath(filepath.Dir(prefix))
		if err != nil {
			return err
		}
		canonical := filepath.Join(parent, filepath.Base(prefix))
		if (strings.HasPrefix(receipt, canonical+"_checkpoint_") && strings.HasSuffix(receipt, ".pprof")) || strings.HasPrefix(canonical, receipt+string(filepath.Separator)) {
			return errors.New("process usage receipt collides with checkpoint profile namespace")
		}
	}
	if dir := strings.TrimSpace(*profileDir); dir != "" {
		for _, name := range []string{"benchprof_results.json", "benchprof_results.md", "insights.json", "insights.md", "insights.html"} {
			paths = append(paths, filepath.Join(dir, name))
		}
	}
	for _, output := range paths {
		if strings.TrimSpace(output) == "" {
			continue
		}
		canonical, err := processUsageCanonicalPath(output)
		if err != nil {
			return fmt.Errorf("process usage output path: %w", err)
		}
		if receipt == canonical || strings.HasPrefix(canonical, receipt+string(filepath.Separator)) {
			return fmt.Errorf("process usage receipt collides with benchmark output %s", output)
		}
	}
	return nil
}

// Selected-mode ownership only. A failed original Close is never retried and
// its directory is retained because release of the DB's resources is unproved.
type processUsageDBOwner struct {
	instance        *DBInstance
	closeAttempted  bool
	closeOK         bool
	removeAttempted bool
}

func (o *processUsageDBOwner) close() error {
	if o.closeAttempted {
		return nil
	}
	o.closeAttempted = true
	if err := o.instance.Wrapper.Close(); err != nil {
		return fmt.Errorf("close %s: %w", o.instance.Name, err)
	}
	o.closeOK = true
	return nil
}

func (o *processUsageDBOwner) removeDir() error {
	if !o.closeOK || o.removeAttempted {
		return nil
	}
	o.removeAttempted = true
	if err := os.RemoveAll(o.instance.Dir); err != nil {
		return fmt.Errorf("remove %s directory: %w", o.instance.Name, err)
	}
	return nil
}

func (o *processUsageDBOwner) cleanup(keepDir bool) error {
	closeErr := o.close()
	if keepDir {
		return closeErr
	}
	return errors.Join(closeErr, o.removeDir())
}
