package collections

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"testing"
)

const collectionsVectorAllocationArtifactsEnv = "TREEDB_COLLECTIONS_VECTOR_ALLOCATION_ARTIFACTS"

// captureVectorAllocationFailure is called only after a strict allocation gate
// has already failed, while its fixture owners are still open. These sampled,
// process-wide snapshots cannot attribute the AllocsPerRun delta to a caller.
// No profiling rate changes, forced GC, environment dumps, or DB data are used.
func captureVectorAllocationFailure(t *testing.T, gate, phase, fixture string, runs int, allocs float64) {
	t.Helper()
	if allocs <= 0 || os.Getenv(collectionsVectorAllocationGateEnv) != gate {
		return
	}
	// Preserve the measured result even if later diagnostic work stalls until
	// the child timeout. Stderr is collected immediately by the parent, unlike
	// non-verbose testing logs that can remain buffered until test completion.
	t.Fail()
	fmt.Fprintf(os.Stderr, "ALLOCATION_GATE_FAILURE test=%q gate=%q phase=%q runs=%d allocs/run=%v target=0\n", t.Name(), gate, phase, runs, allocs)
	root := os.Getenv(collectionsVectorAllocationArtifactsEnv)
	if root == "" {
		return
	}
	// Diagnostics must never replace the original allocation failure, including
	// when an unexpected capture panic occurs.
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Logf("vector allocation failure capture panicked (%T); original failure follows", recovered)
		}
	}()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Logf("vector allocation failure capture root: %v", err)
		return
	}
	dir, err := os.MkdirTemp(root, "vector-allocation-")
	if err != nil {
		t.Logf("vector allocation failure capture directory: %v", err)
		return
	}
	metadata := struct {
		Test             string
		Gate             string
		Phase            string
		Fixture          string
		Runs             int
		AllocsPerRun     float64
		GoVersion        string
		GOOS             string
		GOARCH           string
		MemProfileRate   int
		MemStats         runtime.MemStats
		BuildSettings    map[string]string
		BinarySHA256     string
		CaptureErrors    []string
		ProfileSemantics string
	}{
		Test: t.Name(), Gate: gate, Phase: phase, Fixture: fixture,
		Runs: runs, AllocsPerRun: allocs, GoVersion: runtime.Version(),
		GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		MemProfileRate: runtime.MemProfileRate, MemStats: stats,
		BuildSettings:    make(map[string]string),
		ProfileSemantics: "After-failure process snapshots; sampled memory records may lag GC cycles; no exact allocation-counter attribution. Stack files use basenames; no heap contents, environment, command line, DB files, or executable copy.",
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs", "vcs.revision", "vcs.modified", "GOOS", "GOARCH", "CGO_ENABLED", "-race", "-trimpath":
				metadata.BuildSettings[setting.Key] = setting.Value
			}
		}
	}
	recordError := func(name string, err error) {
		if err != nil {
			metadata.CaptureErrors = append(metadata.CaptureErrors, name+": "+err.Error())
			t.Logf("vector allocation failure capture %s: %v", name, err)
		}
	}
	// Fixed record and output caps bound capture memory and disk usage. If the
	// runtime needs more records, retain that omission rather than retrying.
	memory := make([]runtime.MemProfileRecord, 2048)
	if n, ok := runtime.MemProfile(memory, true); ok {
		recordError("sampled-memory.txt", writeVectorAllocationArtifact(dir, "sampled-memory.txt", 2<<20, func(w io.Writer) error {
			for _, record := range memory[:n] {
				if _, err := fmt.Fprintf(w, "alloc_objects=%d alloc_bytes=%d free_objects=%d free_bytes=%d\n", record.AllocObjects, record.AllocBytes, record.FreeObjects, record.FreeBytes); err != nil {
					return err
				}
				if err := writeVectorAllocationStack(w, record.Stack(), len(record.Stack0)); err != nil {
					return err
				}
			}
			return nil
		}))
	} else {
		recordError("sampled-memory.txt", fmt.Errorf("requires %d records; cap is %d", n, len(memory)))
	}
	goroutines := make([]runtime.StackRecord, 1024)
	if n, ok := runtime.GoroutineProfile(goroutines); ok {
		recordError("goroutines.txt", writeVectorAllocationArtifact(dir, "goroutines.txt", 2<<20, func(w io.Writer) error {
			for i, record := range goroutines[:n] {
				if _, err := fmt.Fprintf(w, "goroutine_snapshot=%d\n", i); err != nil {
					return err
				}
				if err := writeVectorAllocationStack(w, record.Stack(), len(record.Stack0)); err != nil {
					return err
				}
			}
			return nil
		}))
	} else {
		recordError("goroutines.txt", fmt.Errorf("requires %d records; cap is %d", n, len(goroutines)))
	}
	// Bind PCs to the exact child binary without uploading its embedded data.
	// Hash streaming is capped as well; an oversized binary gets no partial hash.
	metadata.BinarySHA256, err = vectorAllocationBinaryHash()
	recordError("binary-sha256", err)
	recordError("metadata.json", writeVectorAllocationArtifact(dir, "metadata.json", 64<<10, func(w io.Writer) error {
		return json.NewEncoder(w).Encode(metadata)
	}))
	t.Logf("after-failure vector allocation snapshots: %s (sampled process evidence only)", dir)
}

func writeVectorAllocationStack(w io.Writer, pcs []uintptr, capacity int) error {
	if _, err := fmt.Fprintf(w, "raw_pc_count=%d raw_pc_capacity=%d possibly_truncated=%t\n", len(pcs), capacity, len(pcs) == capacity); err != nil {
		return err
	}
	frames := runtime.CallersFrames(pcs)
	for {
		frame, more := frames.Next()
		if _, err := fmt.Fprintf(w, "  %#x %s %s:%d\n", frame.PC, frame.Function, filepath.Base(frame.File), frame.Line); err != nil {
			return err
		}
		if !more {
			return nil
		}
	}
}

type vectorAllocationBoundedWriter struct {
	writer    io.Writer
	remaining int
}

func (w *vectorAllocationBoundedWriter) Write(p []byte) (int, error) {
	if len(p) > w.remaining {
		return 0, fmt.Errorf("artifact byte cap reached")
	}
	n, err := w.writer.Write(p)
	w.remaining -= n
	return n, err
}

func writeVectorAllocationArtifact(dir, name string, limit int, write func(io.Writer) error) error {
	file, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	writeErr := write(&vectorAllocationBoundedWriter{writer: file, remaining: limit})
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func vectorAllocationBinaryHash() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", err
	}
	return vectorAllocationFileHash(path, 128<<20)
}

func vectorAllocationFileHash(path string, limit int64) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(file, limit+1))
	if err != nil {
		return "", err
	}
	if n > limit {
		return "", fmt.Errorf("binary exceeds %d-byte hash cap", limit)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
