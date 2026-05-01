package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strings"
	"sync"
	"time"
)

const (
	profileManifestFile = "profile_manifest.json"
	profileResultFile   = "benchmark_result.json"
)

type profileRecorder struct {
	dir                  string
	blockRate            int
	mutexFraction        int
	traceEnabled         bool
	createdAt            time.Time
	resultPath           string
	manifestPath         string
	mu                   sync.Mutex
	seenNames            map[string]int
	artifacts            []profilePhaseArtifact
	profilingRatesActive bool
}

type profileManifest struct {
	Version              int                    `json:"version"`
	CreatedAt            string                 `json:"created_at"`
	ProfileDir           string                 `json:"profile_dir"`
	BlockRate            int                    `json:"block_rate,omitempty"`
	MutexFraction        int                    `json:"mutex_fraction,omitempty"`
	TraceEnabled         bool                   `json:"trace_enabled,omitempty"`
	ResultFile           string                 `json:"result_file,omitempty"`
	RunError             string                 `json:"run_error,omitempty"`
	Artifacts            []profilePhaseArtifact `json:"artifacts"`
	RecommendedPprofArgs []string               `json:"recommended_pprof_args,omitempty"`
}

type profilePhaseArtifact struct {
	Phase          string  `json:"phase"`
	Prefix         string  `json:"prefix"`
	StartedAt      string  `json:"started_at"`
	DurationMillis float64 `json:"duration_ms"`
	CPUProfile     string  `json:"cpu_profile,omitempty"`
	HeapProfile    string  `json:"heap_profile,omitempty"`
	AllocsProfile  string  `json:"allocs_profile,omitempty"`
	BlockProfile   string  `json:"block_profile,omitempty"`
	MutexProfile   string  `json:"mutex_profile,omitempty"`
	GoroutineDump  string  `json:"goroutine_dump,omitempty"`
	Trace          string  `json:"trace,omitempty"`
	Error          string  `json:"error,omitempty"`
}

type activeProfilePhase struct {
	recorder  *profileRecorder
	artifact  profilePhaseArtifact
	started   time.Time
	cpuFile   *os.File
	traceFile *os.File
	cpuActive bool
	traceOn   bool
}

func newProfileRecorder(cfg config) (*profileRecorder, error) {
	if strings.TrimSpace(cfg.ProfileDir) == "" {
		return nil, nil
	}
	abs, err := filepath.Abs(cfg.ProfileDir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return nil, err
	}
	recorder := &profileRecorder{
		dir:           abs,
		blockRate:     cfg.ProfileBlockRate,
		mutexFraction: cfg.ProfileMutexFraction,
		traceEnabled:  cfg.ProfileTrace,
		createdAt:     time.Now(),
		resultPath:    filepath.Join(abs, profileResultFile),
		manifestPath:  filepath.Join(abs, profileManifestFile),
		seenNames:     make(map[string]int),
		artifacts:     make([]profilePhaseArtifact, 0, 8),
	}
	if recorder.blockRate > 0 {
		runtime.SetBlockProfileRate(recorder.blockRate)
		recorder.profilingRatesActive = true
	}
	if recorder.mutexFraction > 0 {
		runtime.SetMutexProfileFraction(recorder.mutexFraction)
		recorder.profilingRatesActive = true
	}
	return recorder, nil
}

func (r *profileRecorder) Dir() string {
	if r == nil {
		return ""
	}
	return r.dir
}

func (r *profileRecorder) ManifestPath() string {
	if r == nil {
		return ""
	}
	return r.manifestPath
}

func (r *profileRecorder) ResultPath() string {
	if r == nil {
		return ""
	}
	return r.resultPath
}

func (r *profileRecorder) Close() {
	if r == nil || !r.profilingRatesActive {
		return
	}
	runtime.SetBlockProfileRate(0)
	runtime.SetMutexProfileFraction(0)
	r.profilingRatesActive = false
}

func (r *profileRecorder) RunPhase(name string, run func() (phaseResult, error)) (phaseResult, error) {
	phase, startErr := r.startPhase(name)
	if startErr != nil {
		return phaseResult{}, startErr
	}
	result, runErr := run()
	stopErr := phase.stop(runErr)
	return result, errors.Join(runErr, stopErr)
}

func (r *profileRecorder) WriteResult(result *benchmarkResult) error {
	if r == nil || result == nil {
		return nil
	}
	return writeProfileJSONFile(r.resultPath, result)
}

func (r *profileRecorder) WriteManifest(result *benchmarkResult, runErr error) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	artifacts := append([]profilePhaseArtifact(nil), r.artifacts...)
	r.mu.Unlock()
	manifest := profileManifest{
		Version:       1,
		CreatedAt:     r.createdAt.Format(time.RFC3339Nano),
		ProfileDir:    r.dir,
		BlockRate:     r.blockRate,
		MutexFraction: r.mutexFraction,
		TraceEnabled:  r.traceEnabled,
		Artifacts:     artifacts,
		RecommendedPprofArgs: []string{
			"go tool pprof -top -cum <binary> <profile.cpu.pprof>",
			"go tool pprof -http=:0 <binary> <profile.cpu.pprof>",
		},
	}
	if result != nil {
		manifest.ResultFile = profileResultFile
	}
	if runErr != nil {
		manifest.RunError = runErr.Error()
	}
	return writeProfileJSONFile(r.manifestPath, manifest)
}

func (r *profileRecorder) startPhase(name string) (*activeProfilePhase, error) {
	prefix := r.nextPrefix(name)
	started := time.Now()
	artifact := profilePhaseArtifact{
		Phase:     name,
		Prefix:    prefix,
		StartedAt: started.Format(time.RFC3339Nano),
	}
	phase := &activeProfilePhase{
		recorder: r,
		artifact: artifact,
		started:  started,
	}

	cpuPath := filepath.Join(r.dir, prefix+".cpu.pprof")
	cpuFile, err := os.Create(cpuPath)
	if err != nil {
		return nil, err
	}
	phase.cpuFile = cpuFile
	phase.artifact.CPUProfile = filepath.Base(cpuPath)
	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		_ = cpuFile.Close()
		return nil, err
	}
	phase.cpuActive = true

	if r.traceEnabled {
		tracePath := filepath.Join(r.dir, prefix+".trace.out")
		traceFile, err := os.Create(tracePath)
		if err != nil {
			phase.stop(err)
			return nil, err
		}
		phase.traceFile = traceFile
		phase.artifact.Trace = filepath.Base(tracePath)
		if err := trace.Start(traceFile); err != nil {
			phase.stop(err)
			return nil, err
		}
		phase.traceOn = true
	}
	return phase, nil
}

func (r *profileRecorder) nextPrefix(name string) string {
	base := sanitizeProfileName(name)
	r.mu.Lock()
	defer r.mu.Unlock()
	count := r.seenNames[base]
	r.seenNames[base] = count + 1
	if count == 0 {
		return base
	}
	return fmt.Sprintf("%s_%d", base, count+1)
}

func (p *activeProfilePhase) stop(runErr error) error {
	if p == nil {
		return nil
	}
	if p.traceOn {
		trace.Stop()
		p.traceOn = false
	}
	if p.cpuActive {
		pprof.StopCPUProfile()
		p.cpuActive = false
	}
	var errs []error
	if p.traceFile != nil {
		errs = append(errs, p.traceFile.Close())
		p.traceFile = nil
	}
	if p.cpuFile != nil {
		errs = append(errs, p.cpuFile.Close())
		p.cpuFile = nil
	}
	p.artifact.DurationMillis = float64(time.Since(p.started).Nanoseconds()) / 1e6
	if runErr != nil {
		p.artifact.Error = runErr.Error()
	}
	errs = append(errs, p.writeRuntimeProfiles()...)
	p.recorder.mu.Lock()
	p.recorder.artifacts = append(p.recorder.artifacts, p.artifact)
	p.recorder.mu.Unlock()
	return errors.Join(errs...)
}

func (p *activeProfilePhase) writeRuntimeProfiles() []error {
	profiles := []struct {
		name string
		set  func(string)
		want bool
	}{
		{name: "heap", set: func(path string) { p.artifact.HeapProfile = path }, want: true},
		{name: "allocs", set: func(path string) { p.artifact.AllocsProfile = path }, want: true},
		{name: "block", set: func(path string) { p.artifact.BlockProfile = path }, want: p.recorder.blockRate > 0},
		{name: "mutex", set: func(path string) { p.artifact.MutexProfile = path }, want: p.recorder.mutexFraction > 0},
		{name: "goroutine", set: func(path string) { p.artifact.GoroutineDump = path }, want: true},
	}
	var errs []error
	for _, profile := range profiles {
		if !profile.want {
			continue
		}
		if profile.name == "heap" {
			runtime.GC()
		}
		outPath := filepath.Join(p.recorder.dir, p.artifact.Prefix+"."+profile.name+".pprof")
		if err := writeRuntimeProfile(profile.name, outPath); err != nil {
			errs = append(errs, err)
			continue
		}
		profile.set(filepath.Base(outPath))
	}
	return errs
}

func writeRuntimeProfile(name, path string) error {
	profile := pprof.Lookup(name)
	if profile == nil {
		return fmt.Errorf("runtime profile %q is unavailable", name)
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return profile.WriteTo(file, 0)
}

func writeProfileJSONFile(path string, value any) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func sanitizeProfileName(name string) string {
	var builder strings.Builder
	builder.Grow(len(name))
	lastUnderscore := false
	for _, r := range strings.ToLower(strings.TrimSpace(name)) {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '.'
		if ok {
			builder.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			builder.WriteByte('_')
			lastUnderscore = true
		}
	}
	out := strings.Trim(builder.String(), "_")
	if out == "" {
		return "phase"
	}
	return out
}
