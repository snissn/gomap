package collections

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const vectorAllocationCaptureTestModeEnv = "TREEDB_VECTOR_ALLOCATION_CAPTURE_TEST_MODE"

// Like enterIsolatedVectorAllocationGate, this exercises the same test binary
// in a fresh process. The positive value is a helper input, not a reproduced
// AllocsPerRun failure or evidence about the production allocation gate.
func TestVectorAllocationFailureCaptureProcess(t *testing.T) {
	const gate = "capture-helper-test"
	if mode := os.Getenv(vectorAllocationCaptureTestModeEnv); mode != "" {
		allocs := float64(2)
		if mode == "zero" {
			allocs = 0
		}
		captureVectorAllocationFailure(t, gate, "probe", "synthetic helper fixture", 7, allocs)
		if mode == "positive" || mode == "capture-error" {
			t.Fatal("original allocation failure sentinel")
		}
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"positive", "capture-error", "zero", "other-selector"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			fixtureDir := filepath.Join(root, "temporary fixture with spaces")
			artifactRoot := filepath.Join(root, "retained artifacts with spaces")
			if err := os.Mkdir(fixtureDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if mode == "capture-error" {
				// A regular file is an invalid directory on Unix and Windows,
				// including when tests have permission to bypass mode bits.
				if err := os.WriteFile(artifactRoot, []byte("keep"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestVectorAllocationFailureCaptureProcess$", "-test.count=1", "-test.timeout=20s")
			cmd.Dir = fixtureDir
			selector := gate
			if mode == "other-selector" {
				selector = "unselected-gate"
			}
			// Replace selectors case-insensitively for Windows, where environment
			// variable names are case-insensitive. No shell or executable copy.
			for _, entry := range os.Environ() {
				key, _, _ := strings.Cut(entry, "=")
				if strings.EqualFold(key, vectorAllocationCaptureTestModeEnv) || strings.EqualFold(key, collectionsVectorAllocationGateEnv) || strings.EqualFold(key, collectionsVectorAllocationArtifactsEnv) {
					continue
				}
				cmd.Env = append(cmd.Env, entry)
			}
			cmd.Env = append(cmd.Env, vectorAllocationCaptureTestModeEnv+"="+mode, collectionsVectorAllocationGateEnv+"="+selector, collectionsVectorAllocationArtifactsEnv+"="+artifactRoot)
			output, runErr := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("capture helper child timed out: %v\n%s", ctx.Err(), output)
			}
			// Model the parent removing its fixture/shard temporary path before
			// uploading the separate artifact root. This is not a CI job run.
			if err := os.RemoveAll(fixtureDir); err != nil {
				t.Fatal(err)
			}
			if mode == "zero" || mode == "other-selector" {
				if runErr != nil || bytes.Contains(output, []byte("ALLOCATION_GATE_FAILURE")) {
					t.Fatalf("non-failure capture changed child result: %v\n%s", runErr, output)
				}
				if _, err := os.Stat(artifactRoot); !os.IsNotExist(err) {
					t.Fatalf("non-failure capture created artifacts: %v", err)
				}
				return
			}
			if runErr == nil {
				t.Fatalf("positive helper child passed:\n%s", output)
			}
			early := bytes.Index(output, []byte("ALLOCATION_GATE_FAILURE"))
			original := bytes.Index(output, []byte("original allocation failure sentinel"))
			if early < 0 || original < early || !bytes.Contains(output, []byte("runs=7 allocs/run=2 target=0")) {
				t.Fatalf("missing early measured result or original failure:\n%s", output)
			}
			if mode == "capture-error" {
				captureErr := bytes.Index(output, []byte("vector allocation failure capture root:"))
				if captureErr < early {
					t.Fatalf("capture error preceded or lost early result:\n%s", output)
				}
				if content, err := os.ReadFile(artifactRoot); err != nil || string(content) != "keep" {
					t.Fatalf("capture error overwrote existing data: %q %v", content, err)
				}
				return
			}
			entries, err := os.ReadDir(artifactRoot)
			if err != nil || len(entries) != 1 || !entries[0].IsDir() {
				t.Fatalf("retained capture directory: entries=%v err=%v", entries, err)
			}
			dir := filepath.Join(artifactRoot, entries[0].Name())
			metadataBytes, err := os.ReadFile(filepath.Join(dir, "metadata.json"))
			if err != nil {
				t.Fatal(err)
			}
			var metadata struct {
				Gate, Phase, Fixture, BinarySHA256 string
				Runs                               int
				AllocsPerRun                       float64
				MemStats                           runtime.MemStats
				CaptureErrors                      []string
			}
			if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
				t.Fatal(err)
			}
			if metadata.Gate != gate || metadata.Phase != "probe" || metadata.Fixture != "synthetic helper fixture" || metadata.Runs != 7 || metadata.AllocsPerRun != 2 || metadata.MemStats.Mallocs == 0 {
				t.Fatalf("wrong captured identity/result: %+v", metadata)
			}
			for name, capBytes := range map[string]int64{"sampled-memory.txt": 2 << 20, "goroutines.txt": 2 << 20, "metadata.json": 64 << 10} {
				info, err := os.Stat(filepath.Join(dir, name))
				if os.IsNotExist(err) && strings.Contains(strings.Join(metadata.CaptureErrors, "\n"), name+":") {
					continue // A fixed record cap omission must remain explicit.
				}
				if err != nil || info.Size() > capBytes {
					t.Fatalf("capture file %s exceeds cap or is missing: %v %v", name, info, err)
				}
			}
			if len(metadata.BinarySHA256) != sha256.Size*2 && !(metadata.BinarySHA256 == "" && strings.Contains(strings.Join(metadata.CaptureErrors, "\n"), "binary-sha256:")) {
				t.Fatalf("binary hash is partial or missing without a capture error: %q", metadata.BinarySHA256)
			}
		})
	}
}

func TestVectorAllocationFailureArtifactBounds(t *testing.T) {
	dir := t.TempDir()
	err := writeVectorAllocationArtifact(dir, "bounded.txt", 5, func(w io.Writer) error {
		if _, err := io.WriteString(w, "1234"); err != nil {
			return err
		}
		_, err := io.WriteString(w, "67")
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "cap") {
		t.Fatalf("overflow err=%v", err)
	}
	content, err := os.ReadFile(filepath.Join(dir, "bounded.txt"))
	if err != nil || string(content) != "1234" {
		t.Fatalf("bounded partial output=%q err=%v", content, err)
	}
	if err := writeVectorAllocationArtifact(dir, "bounded.txt", 5, func(w io.Writer) error { _, err := io.WriteString(w, "new"); return err }); err == nil {
		t.Fatal("existing artifact was opened for overwrite")
	}
	sentinel := errors.New("capture writer failed")
	if err := writeVectorAllocationArtifact(dir, "error.txt", 5, func(io.Writer) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("capture write error lost: %v", err)
	}
}

func TestVectorAllocationFailureStackCapacityLabels(t *testing.T) {
	var sample [1]uintptr
	runtime.Callers(0, sample[:])
	for _, depth := range []int{1, 32} {
		pcs := make([]uintptr, depth)
		for i := range pcs {
			pcs[i] = sample[0]
		}
		var output bytes.Buffer
		if err := writeVectorAllocationStack(&output, pcs, 32); err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf("raw_pc_count=%d raw_pc_capacity=32 possibly_truncated=%t", depth, depth == 32)
		if !strings.Contains(output.String(), want) {
			t.Fatalf("stack depth completeness label missing: %s", output.String())
		}
	}
	writer := &vectorAllocationBoundedWriter{writer: io.Discard, remaining: 1}
	if err := writeVectorAllocationStack(writer, sample[:], 32); err == nil {
		t.Fatal("stack output cap error was lost")
	}
}

func TestVectorAllocationFailureBinaryHashBound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "child binary with spaces.exe")
	data := []byte("exact child bytes")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256(data)
	got, err := vectorAllocationFileHash(path, int64(len(data)))
	if err != nil || got != hex.EncodeToString(want[:]) {
		t.Fatalf("exact binary hash=%q err=%v", got, err)
	}
	if got, err := vectorAllocationFileHash(path, int64(len(data)-1)); err == nil || got != "" {
		t.Fatalf("oversized binary produced partial hash=%q err=%v", got, err)
	}
	if got, err := vectorAllocationFileHash(path+".missing", 1); err == nil || got != "" {
		t.Fatalf("missing binary hash=%q err=%v", got, err)
	}
}
