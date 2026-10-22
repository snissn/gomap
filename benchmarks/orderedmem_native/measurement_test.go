package orderedmemnative

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/mvcc"
)

type config struct {
	Case                       string `json:"case"`
	Engine                     string `json:"engine"`
	Profile                    string `json:"profile"`
	MemtableMode               string `json:"memtable_mode"`
	Dir                        string `json:"dir"`
	Operations                 int    `json:"operations"`
	BatchSize                  int    `json:"batch_size"`
	ValueBytes                 int    `json:"value_bytes"`
	KeyCount                   int    `json:"key_count"`
	HistoryDepth               int    `json:"history_depth"`
	Workers                    int    `json:"workers"`
	RangeWidth                 int    `json:"range_width"`
	FlushThreshold             int64  `json:"flush_threshold"`
	CheckpointEvery            int    `json:"checkpoint_every"`
	HoldCut                    bool   `json:"hold_cut"`
	Seed                       uint64 `json:"seed"`
	PointerThreshold           int    `json:"pointer_threshold"`
	BadgerMemtableSize         int64  `json:"badger_memtable_size"`
	BadgerValueThreshold       int64  `json:"badger_value_threshold"`
	OperationalMinRotations    uint64 `json:"operational_min_rotations"`
	OperationalMinPayloadBytes uint64 `json:"operational_min_payload_bytes"`
	RetainedDiagnostics        bool   `json:"retained_diagnostics"`
	AllocationProfileRate      int    `json:"allocation_profile_rate,omitempty"`
	ProfileDir                 string `json:"profile_dir,omitempty"`
}

type latency struct {
	P50     int64 `json:"p50"`
	P95     int64 `json:"p95"`
	P99     int64 `json:"p99"`
	Max     int64 `json:"max"`
	Samples int   `json:"samples"`
}
type phase struct {
	endedAt                time.Time
	Name                   string    `json:"name"`
	Nanoseconds            int64     `json:"nanoseconds"`
	PlannedOperations      int       `json:"planned_operations"`
	PlannedMutations       int       `json:"planned_mutations"`
	PlannedOutputRecords   int       `json:"planned_output_records"`
	Completed              bool      `json:"completed"`
	Operations             int       `json:"operations"`
	Mutations              int       `json:"mutations"`
	OutputRecords          int       `json:"output_records"`
	AllocatedBytes         uint64    `json:"allocated_bytes"`
	Allocations            uint64    `json:"allocations"`
	NumGC                  uint32    `json:"num_gc"`
	HeapAlloc              uint64    `json:"heap_alloc"`
	HeapObjects            uint64    `json:"heap_objects"`
	LatencyNS              latency   `json:"latency_ns"`
	MaintenanceCalls       int       `json:"maintenance_calls"`
	MaintenanceNanoseconds int64     `json:"maintenance_nanoseconds"`
	Process                resources `json:"process_delta"`
}

// CPU and kernel counters cover the process, including engine background work.
// Linux /proc I/O bytes describe process accounting, not stable device writes.
type resources struct {
	UserCPUNS           int64 `json:"user_cpu_ns"`
	SystemCPUNS         int64 `json:"system_cpu_ns"`
	VoluntarySwitches   int64 `json:"voluntary_switches"`
	InvoluntarySwitches int64 `json:"involuntary_switches"`
	ReadBytes           int64 `json:"read_bytes"`
	WriteBytes          int64 `json:"write_bytes"`
	ReadChars           int64 `json:"read_chars"`
	WriteChars          int64 `json:"write_chars"`
	IOAvailable         bool  `json:"io_available"`
}
type files struct {
	LogicalBytes     int64  `json:"logical_bytes"`
	Files            int    `json:"files"`
	CommandWAL       int64  `json:"treedb_command_wal_bytes"`
	ValueLog         int64  `json:"value_log_bytes"`
	BadgerSST        int64  `json:"badger_sst_bytes"`
	BadgerWAL        int64  `json:"badger_wal_bytes"`
	MaxBadgerMemFID  uint64 `json:"max_badger_mem_fid"`
	TransientMissing int    `json:"transient_missing"`
}
type retained struct {
	Name        string `json:"name"`
	HeapAlloc   uint64 `json:"heap_alloc"`
	HeapObjects uint64 `json:"heap_objects"`
	NumGC       uint32 `json:"num_gc"`
}
type packet struct {
	SchemaVersion            string              `json:"schema_version"`
	Success                  bool                `json:"success"`
	Error                    string              `json:"error,omitempty"`
	Config                   config              `json:"config"`
	TraceSHA256              string              `json:"trace_sha256"`
	Runtime                  map[string]any      `json:"runtime"`
	Route                    route               `json:"route"`
	ReopenedRoute            route               `json:"reopened_route"`
	Phases                   []phase             `json:"phases"`
	CycleNS                  int64               `json:"cycle_ns"`
	FilesBeforeClose         files               `json:"files_before_close"`
	FilesAfterClose          files               `json:"files_after_close"`
	StatsBefore              map[string]string   `json:"stats_before"`
	StatsAfter               map[string]string   `json:"stats_after"`
	ActivationStats          map[string]string   `json:"activation_stats"`
	StatsPersisted           map[string]string   `json:"stats_persisted"`
	StatsReleased            map[string]string   `json:"stats_released"`
	FilesBeforeForeground    files               `json:"files_before_foreground"`
	FilesAfterForeground     files               `json:"files_after_foreground"`
	Retained                 []retained          `json:"retained"`
	AllocationProfiles       []allocationProfile `json:"allocation_profiles,omitempty"`
	FailureStateStats        map[string]string   `json:"failure_state_stats,omitempty"`
	FailureStateFiles        *files              `json:"failure_state_files,omitempty"`
	FailureStateCaptureError string              `json:"failure_state_capture_error,omitempty"`
	FailureStateDelayNS      int64               `json:"failure_state_delay_nanoseconds,omitempty"`
	Verification             map[string]any      `json:"verification"`
	Notes                    []string            `json:"notes"`
}

// No environment opt-in means no DB, benchmark, or measurement work. The runner
// starts one process per config and preserves failure JSON as well as stdout.
func TestOrderedMemNativeMeasurement(t *testing.T) {
	input := os.Getenv("ORDEREDMEM_NATIVE_CONFIG")
	if input == "" {
		t.Skip("opt-in public measurement; coordinator owns benchmark execution")
	}
	out := os.Getenv("ORDEREDMEM_NATIVE_OUT")
	if out == "" {
		t.Fatal("ORDEREDMEM_NATIVE_OUT is required")
	}
	var c config
	data, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &c); err != nil {
		t.Fatal(err)
	}
	if c.AllocationProfileRate > 0 {
		// One fresh process serves one cell. Set the sampler once before the
		// fixture, open, and seed so cumulative profile weights use one rate.
		runtime.MemProfileRate = c.AllocationProfileRate
	}
	p := &packet{SchemaVersion: "orderedmem-native-v1", Config: c, Runtime: runtimeIdentity(), Verification: make(map[string]any)}
	err = runCell(c, p)
	p.Success = err == nil
	if err != nil {
		p.Error = err.Error()
	}
	b, encodeErr := json.MarshalIndent(p, "", "  ")
	if encodeErr == nil {
		encodeErr = os.WriteFile(out, append(b, '\n'), 0o644)
	}
	if encodeErr != nil {
		t.Fatalf("write result: %v (workload error: %v)", encodeErr, err)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func (c config) validate() error {
	if c.Engine != "treedb" && c.Engine != "badger" {
		return fmt.Errorf("unsupported engine %q", c.Engine)
	}
	if c.Profile != "no_wal_fast" && c.Profile != "command_wal_relaxed" && c.Profile != "command_wal_durable" {
		return fmt.Errorf("unsupported profile %q", c.Profile)
	}
	switch c.Case {
	case "commit", "history", "range", "lifecycle", "mixed", "shared":
	default:
		return fmt.Errorf("unsupported case %q", c.Case)
	}
	if !filepath.IsAbs(c.Dir) || c.Operations <= 0 || c.BatchSize <= 0 || c.KeyCount < c.BatchSize || c.ValueBytes < 16 || c.HistoryDepth <= 0 || c.Workers <= 0 || c.RangeWidth <= 0 {
		return errors.New("absolute fresh dir, positive counts/workers/depth/width, value>=16 and keys>=batch are required")
	}
	if c.Case != "shared" && c.Workers != 1 {
		return errors.New("only shared case permits multiple workers")
	}
	if c.Case == "shared" && c.CheckpointEvery != 0 {
		return errors.New("shared foreground does not run periodic checkpoints")
	}
	if c.Case != "lifecycle" && (c.CheckpointEvery != 0 || c.HoldCut) {
		return errors.New("periodic persistence/held cut are lifecycle-only")
	}
	if c.Case == "mixed" && c.BatchSize != 1 {
		return errors.New("the mixed 60/20/20 trace uses singleton mutations")
	}
	if (c.OperationalMinRotations > 0 || c.OperationalMinPayloadBytes > 0) && (c.Case != "lifecycle" || c.BadgerMemtableSize <= 0 || c.BadgerValueThreshold <= 0) {
		return errors.New("operational activation requires lifecycle plus explicit Badger memtable/value thresholds")
	}
	if c.AllocationProfileRate < 0 || (c.AllocationProfileRate > 0 && c.ProfileDir == "") {
		return errors.New("allocation profile rate must be nonnegative and a positive rate requires profile_dir")
	}
	return nil
}

func runCell(c config, p *packet) (retErr error) {
	if err := c.validate(); err != nil {
		return err
	}
	if entries, err := os.ReadDir(c.Dir); err == nil && len(entries) != 0 {
		return errors.New("database directory must be fresh/empty; reuse is forbidden")
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(c.Dir, 0o755); err != nil {
		return err
	}
	f, err := makeFixture(c)
	if err != nil {
		return err
	}
	p.TraceSHA256 = f.trace
	p.Notes = []string{
		"Foreground includes public input ownership, preparation, acceptance/publication, owned reads and exact output checks; excludes fixture construction/open/seeding/final drain/close/reopen.",
		"cycle_ns is complete elapsed wall time from open through verified reopen and final close, including validation, stats/file inspection and retained diagnostics if enabled; it is not pure write service time.",
		"Lifecycle foreground includes explicitly requested periodic persistence. Badger.Sync is not a TreeDB sealed-root checkpoint equivalent.",
		"Latency samples are closed-loop service times including shared-key waiting, not offered-load response times or a wall-clock progress guarantee.",
		"Point API normalizes tombstone to absent for Badger compatibility; retained-history validation preserves all timestamps and tombstones.",
		"Held-cut comparison uses only later timestamps above the held read timestamp. Backfill publication-cut guarantees require separate TreeDB semantic tests.",
		"Natural-GC allocation totals are not retained memory or RSS; fixture/oracle storage remains live throughout both engine arms.",
		"Per-operation latency excludes periodic persistence calls; lifecycle foreground elapsed includes those calls. Quantile sorting and sample-buffer allocation are outside foreground.",
		"Process CPU and Linux I/O deltas include engine background work; /proc read_bytes/write_bytes are kernel process accounting, not proof of durable device traffic.",
		"Process resource snapshots bracket allocation samplers outside each elapsed phase; CPU/I/O counter boundaries are broader than phase nanoseconds, especially for short phases.",
	}
	if c.RetainedDiagnostics {
		p.Notes = append(p.Notes, "Forced-GC retained diagnostics perturb this run; do not pool its timings with natural-GC qualification.")
	}
	if c.ProfileDir != "" {
		if c.AllocationProfileRate > 0 {
			p.Notes = append(p.Notes, "Allocation/heap profiling is diagnostic and replaces CPU profiling in this cell. The explicitly configured sampling rate is set before fixture construction; two forced GCs outside each foreground boundary flush profile samples. Subtract before/after cumulative alloc_space/alloc_objects for attribution, keeping runtime sampling lag and background work in scope; snapshots are not RSS or a pure engine memory bound. Never pool these timings with natural-GC qualification.")
		} else {
			p.Notes = append(p.Notes, "CPU profiling is diagnostic and perturbs this run; do not pool its timings with uninstrumented qualification.")
		}
	}
	p.Notes = append(p.Notes, "Phase operations/mutations count successfully completed work; planned_operations/planned_mutations retain the requested denominator. A failed batch contributes no completed mutations. Failed foreground state is inspected before held-cut/DB-close unwind, after CPU-profile stop if enabled, with capture delay recorded; background maintenance may advance before inspection.")
	retain := func(name string) {
		if !c.RetainedDiagnostics {
			return
		}
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		p.Retained = append(p.Retained, retained{Name: name, HeapAlloc: m.HeapAlloc, HeapObjects: m.HeapObjects, NumGC: m.NumGC})
	}
	retain("fixture_before_open")
	cycleStart := time.Now()
	defer func() { p.CycleNS = time.Since(cycleStart).Nanoseconds(); runtime.KeepAlive(f) }()
	var e publicEngine
	closed := false
	defer func() {
		if e != nil && !closed {
			retErr = errors.Join(retErr, e.Close())
		}
	}()
	if err := measure(p, "open", 1, 0, func(_ *phase) error {
		var err error
		e, p.Route, err = openEngine(c)
		return err
	}); err != nil {
		return err
	}
	retain("open")
	seedMutations := 0
	for _, op := range f.seed {
		seedMutations += len(op.mutations)
	}
	if err := measure(p, "seed", len(f.seed), seedMutations, func(ph *phase) error {
		for i, op := range f.seed {
			if err := e.Commit(op.timestamp, op.mutations); err != nil {
				return fmt.Errorf("seed timestamp %d: %w", op.timestamp, err)
			}
			ph.Operations++
			ph.Mutations += len(op.mutations)
			if i == 0 {
				// Check representation before periodic checkpoints or later
				// flushing can drain this first small mutable generation.
				p.ActivationStats = e.Stats()
				if c.Engine == "treedb" && c.MemtableMode == "cow_ordered" {
					for _, key := range []string{"treedb.cache.cow.ordered.logical_keys", "treedb.cache.cow.ordered.physical_versions"} {
						n, err := strconv.ParseUint(p.ActivationStats[key], 10, 64)
						if err != nil || n == 0 {
							return fmt.Errorf("ordered representation did not activate: %s=%q", key, p.ActivationStats[key])
						}
					}
					p.Verification["ordered_entries_observed"] = true
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	var held historyCursor
	if c.HoldCut {
		if err := measure(p, "capture_held", 1, 0, func(_ *phase) error {
			var err error
			held, err = e.History(f.seedTime)
			return err
		}); err != nil {
			return err
		}
		defer func() {
			if held != nil {
				retErr = errors.Join(retErr, held.Close())
			}
		}()
	}
	retain("seed_and_optional_held")
	p.StatsBefore = e.Stats()
	p.FilesBeforeForeground, err = directoryFiles(c.Dir)
	if err != nil {
		return err
	}
	mutations := 0
	outputRecords := 0
	for _, op := range f.operations {
		mutations += len(op.mutations)
		outputRecords += len(op.rows)
		if op.kind == 'g' && op.expected.State == mvcc.Present {
			outputRecords++
		}
	}
	latencies := make([]int64, len(f.operations))
	if c.AllocationProfileRate > 0 {
		if err := captureAllocationProfiles(c, p, "before_foreground"); err != nil {
			return err
		}
	}
	var cpuFile *os.File
	stopCPU := func() error {
		if cpuFile == nil {
			return nil
		}
		pprof.StopCPUProfile()
		err := cpuFile.Close()
		cpuFile = nil
		return err
	}
	if c.ProfileDir != "" && c.AllocationProfileRate == 0 {
		if err := os.MkdirAll(c.ProfileDir, 0o755); err != nil {
			return err
		}
		cpuFile, err = os.Create(filepath.Join(c.ProfileDir, "foreground.cpu.pprof"))
		if err != nil {
			return err
		}
		if err := pprof.StartCPUProfile(cpuFile); err != nil {
			_ = cpuFile.Close()
			return err
		}
		defer func() {
			retErr = errors.Join(retErr, stopCPU())
		}()
	}
	foregroundErr := measure(p, "foreground", len(f.operations), mutations, func(ph *phase) error {
		ph.PlannedOutputRecords = outputRecords
		return execute(e, f.operations, c, ph, latencies)
	})
	foregroundEnd := p.Phases[len(p.Phases)-1].endedAt
	profileErr := stopCPU()
	if foregroundErr != nil {
		// Preserve the original workload error and the engine state before any
		// held cut or DB is closed. Profile stop is outside foreground timing.
		p.FailureStateDelayNS = time.Since(foregroundEnd).Nanoseconds()
		p.FailureStateStats = e.Stats()
		stateFiles, inspectErr := directoryFiles(c.Dir)
		p.FailureStateFiles = &stateFiles
		if inspectErr != nil {
			p.FailureStateCaptureError = inspectErr.Error()
		}
	}
	if c.AllocationProfileRate > 0 {
		profileErr = errors.Join(profileErr, captureAllocationProfiles(c, p, "after_foreground"))
	}
	// Only validated operations have a nonzero latency. Failed attempts are
	// retained in the workload error and excluded from successful-work quantiles.
	completedLatencies := latencies[:0]
	for _, value := range latencies {
		if value > 0 {
			completedLatencies = append(completedLatencies, value)
		}
	}
	p.Phases[len(p.Phases)-1].LatencyNS = quantiles(completedLatencies)
	if err := errors.Join(foregroundErr, profileErr); err != nil {
		return err
	}
	p.Verification["foreground_outputs"] = true
	p.StatsAfter = e.Stats()
	p.FilesAfterForeground, err = directoryFiles(c.Dir)
	if err != nil {
		return err
	}
	retain("foreground_with_optional_held")
	if err := measure(p, "final_persist", 1, 0, func(_ *phase) error { return e.Persist() }); err != nil {
		return err
	}
	p.StatsPersisted = e.Stats()
	if c.HoldCut {
		if err := measure(p, "verify_held_after_persist", len(f.held), 0, func(_ *phase) error {
			count, err := checkHistory(held, f.held)
			p.Verification["held_records"] = count
			return err
		}); err != nil {
			return err
		}
		p.Verification["held_cut"] = true
	}
	if err := measure(p, "release_held", 0, 0, func(_ *phase) error {
		if held == nil {
			return nil
		}
		err := held.Close()
		held = nil
		return err
	}); err != nil {
		return err
	}
	retain("persisted_held_released")
	p.StatsReleased = e.Stats()
	p.FilesBeforeClose, err = directoryFiles(c.Dir)
	if err != nil {
		return err
	}
	if err := checkOperational(c, f, p); err != nil {
		return err
	}
	if err := measure(p, "close", 1, 0, func(_ *phase) error {
		err := e.Close()
		closed = true
		return err
	}); err != nil {
		return err
	}
	e = nil
	retain("closed_no_engine_reference")
	p.FilesAfterClose, err = directoryFiles(c.Dir)
	if err != nil {
		return err
	}
	if err := measure(p, "reopen", 1, 0, func(_ *phase) error {
		var err error
		e, p.ReopenedRoute, err = openEngine(c)
		closed = false
		return err
	}); err != nil {
		return err
	}
	expected := f.allHistory()
	if err := measure(p, "verify_reopened_history", len(expected), 0, func(_ *phase) error {
		cursor, err := e.History(^uint64(0))
		if err != nil {
			return err
		}
		count, err := checkHistory(cursor, expected)
		p.Verification["reopened_records"] = count
		return errors.Join(err, cursor.Close())
	}); err != nil {
		return err
	}
	p.Verification["distinct_values_and_complete_history_after_reopen"] = true
	p.Verification["expected_records"] = len(expected)
	if err := measure(p, "close_reopened", 1, 0, func(_ *phase) error {
		err := e.Close()
		closed = true
		return err
	}); err != nil {
		return err
	}
	e = nil
	retain("reopened_closed_no_engine_reference")
	return nil
}

func measure(p *packet, name string, operations, mutations int, f func(*phase) error) error {
	var before, after runtime.MemStats
	resourceBefore := processResources()
	runtime.ReadMemStats(&before)
	ph := phase{Name: name, PlannedOperations: operations, PlannedMutations: mutations}
	start := time.Now()
	err := f(&ph)
	ph.endedAt = time.Now()
	ph.Nanoseconds = ph.endedAt.Sub(start).Nanoseconds()
	ph.Completed = err == nil
	if err == nil && name != "foreground" {
		ph.Operations, ph.Mutations = operations, mutations
	}
	runtime.ReadMemStats(&after)
	ph.Process = resourceDelta(resourceBefore, processResources())
	ph.AllocatedBytes, ph.Allocations = after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs
	ph.NumGC, ph.HeapAlloc, ph.HeapObjects = after.NumGC-before.NumGC, after.HeapAlloc, after.HeapObjects
	p.Phases = append(p.Phases, ph)
	if err != nil {
		return fmt.Errorf("phase %s: %w", name, err)
	}
	return nil
}

func execute(e publicEngine, ops []operation, c config, ph *phase, latencies []int64) error {
	type progress struct{ operations, mutations, outputs int }
	var completed progress
	defer func() {
		ph.Operations, ph.Mutations, ph.OutputRecords = completed.operations, completed.mutations, completed.outputs
	}()
	runOne := func(i int, done *progress) error {
		op := &ops[i]
		start := time.Now()
		var err error
		switch op.kind {
		case 'c':
			err = e.Commit(op.timestamp, op.mutations)
		case 'g':
			var got record
			got, err = e.Get(op.key, op.timestamp)
			if err == nil {
				err = checkRecord(got, op.expected, false)
			}
		case 'r':
			var rows []record
			rows, err = e.Range(op.key, nil, op.timestamp, c.RangeWidth)
			if err == nil && len(rows) != len(op.rows) {
				err = fmt.Errorf("range length=%d want=%d", len(rows), len(op.rows))
			}
			if err == nil {
				for j := range rows {
					if err = checkRecord(rows[j], op.rows[j], true); err != nil {
						break
					}
				}
			}
		default:
			err = fmt.Errorf("unknown operation %q", op.kind)
		}
		if err == nil {
			latencies[i] = max(1, time.Since(start).Nanoseconds())
			done.operations++
			done.mutations += len(op.mutations)
			done.outputs += len(op.rows)
			if op.kind == 'g' && op.expected.State == mvcc.Present {
				done.outputs++
			}
		}
		return err
	}
	if c.Case == "shared" {
		// Fixed aggregate work, not operations per worker. Each worker's own
		// timestamp stream increases, while concurrent completion may reorder.
		var wg sync.WaitGroup
		errs := make(chan error, c.Workers)
		workerProgress := make([]progress, c.Workers)
		for worker := 0; worker < c.Workers; worker++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				var done progress
				defer func() { workerProgress[worker] = done }()
				for i := worker; i < len(ops); i += c.Workers {
					if err := runOne(i, &done); err != nil {
						errs <- fmt.Errorf("worker%d operation%d: %w", worker, i, err)
						return
					}
				}
			}(worker)
		}
		wg.Wait()
		for _, done := range workerProgress {
			completed.operations += done.operations
			completed.mutations += done.mutations
			completed.outputs += done.outputs
		}
		close(errs)
		for err := range errs {
			return err
		}
	} else {
		for i := range ops {
			if err := runOne(i, &completed); err != nil {
				return fmt.Errorf("operation%d: %w", i, err)
			}
			if c.CheckpointEvery > 0 && (i+1)%c.CheckpointEvery == 0 {
				start := time.Now()
				err := e.Persist()
				ph.MaintenanceNanoseconds += time.Since(start).Nanoseconds()
				ph.MaintenanceCalls++
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func processResources() (r resources) {
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) == nil {
		r.UserCPUNS = usage.Utime.Nano()
		r.SystemCPUNS = usage.Stime.Nano()
		r.VoluntarySwitches = usage.Nvcsw
		r.InvoluntarySwitches = usage.Nivcsw
	}
	if data, err := os.ReadFile("/proc/self/io"); err == nil {
		r.IOAvailable = true
		for _, line := range strings.Split(string(data), "\n") {
			key, value, ok := strings.Cut(line, ":")
			if !ok {
				continue
			}
			n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
			if err != nil {
				continue
			}
			switch key {
			case "read_bytes":
				r.ReadBytes = n
			case "write_bytes":
				r.WriteBytes = n
			case "rchar":
				r.ReadChars = n
			case "wchar":
				r.WriteChars = n
			}
		}
	}
	return r
}

func resourceDelta(a, b resources) resources {
	return resources{UserCPUNS: b.UserCPUNS - a.UserCPUNS, SystemCPUNS: b.SystemCPUNS - a.SystemCPUNS,
		VoluntarySwitches: b.VoluntarySwitches - a.VoluntarySwitches, InvoluntarySwitches: b.InvoluntarySwitches - a.InvoluntarySwitches,
		ReadBytes: b.ReadBytes - a.ReadBytes, WriteBytes: b.WriteBytes - a.WriteBytes,
		ReadChars: b.ReadChars - a.ReadChars, WriteChars: b.WriteChars - a.WriteChars,
		IOAvailable: a.IOAvailable && b.IOAvailable}
}

func quantiles(values []int64) latency {
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	if len(values) == 0 {
		return latency{}
	}
	q := func(p int) int64 { return values[min((len(values)*p+99)/100-1, len(values)-1)] }
	return latency{P50: q(50), P95: q(95), P99: q(99), Max: values[len(values)-1], Samples: len(values)}
}

func runtimeIdentity() map[string]any {
	r := map[string]any{"go_version": runtime.Version(), "gomaxprocs": runtime.GOMAXPROCS(0),
		"goos": runtime.GOOS, "goarch": runtime.GOARCH, "num_cpu": runtime.NumCPU(), "mem_profile_rate": runtime.MemProfileRate}
	if build, ok := debug.ReadBuildInfo(); ok {
		r["build_info"] = build
	}
	env := make(map[string]string)
	for _, key := range []string{"GOMAXPROCS", "GOGC", "GOMEMLIMIT", "GOWORK", "GOTOOLCHAIN", "GOROOT", "CGO_ENABLED", "GOFLAGS"} {
		env[key] = os.Getenv(key)
	}
	r["environment"] = env
	if b, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "Cpus_allowed_list:") {
				r["cpu_affinity"] = strings.TrimSpace(strings.TrimPrefix(line, "Cpus_allowed_list:"))
			}
		}
	}
	return r
}

func directoryFiles(dir string) (out files, err error) {
	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if os.IsNotExist(err) {
			out.TransientMissing++
			return nil
		}
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if os.IsNotExist(err) {
			out.TransientMissing++
			return nil
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		out.LogicalBytes += info.Size()
		out.Files++
		switch name := d.Name(); {
		case strings.HasPrefix(name, "commit-l"):
			out.CommandWAL += info.Size()
		case strings.HasPrefix(name, "value-l"), strings.HasSuffix(name, ".vlog"):
			out.ValueLog += info.Size()
		case strings.HasSuffix(name, ".sst"):
			out.BadgerSST += info.Size()
		case strings.HasSuffix(name, ".mem"):
			out.BadgerWAL += info.Size()
			if id, err := strconv.ParseUint(strings.TrimSuffix(name, ".mem"), 10, 64); err == nil {
				out.MaxBadgerMemFID = max(out.MaxBadgerMemFID, id)
			}
		}
		return nil
	})
	return out, err
}
